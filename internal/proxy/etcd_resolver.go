package proxy

import (
	"context"
	"encoding/json"
	"path"
	"strings"
	"sync"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const DefaultEtcdPrefix = "/knodeproxy/services/"

type EtcdResolver struct {
	client *clientv3.Client
	prefix string
	store  *endpointStore

	ctx    context.Context
	cancel context.CancelFunc
}

func NewEtcdResolver(ctx context.Context, client *clientv3.Client, prefix string) (*EtcdResolver, error) {
	if prefix == "" {
		prefix = DefaultEtcdPrefix
	}
	prefix = normalizePrefix(prefix)
	watchCtx, cancel := context.WithCancel(ctx)
	r := &EtcdResolver{
		client: client,
		prefix: prefix,
		store:  newEndpointStore(prefix),
		ctx:    watchCtx,
		cancel: cancel,
	}
	if err := r.loadAndWatch(ctx); err != nil {
		cancel()
		return nil, err
	}
	return r, nil
}

func (r *EtcdResolver) Close() {
	r.cancel()
}

func (r *EtcdResolver) Snapshot(service string) Snapshot {
	return r.store.snapshot(service)
}

func (r *EtcdResolver) Subscribe(ctx context.Context, service string) (<-chan Snapshot, func(), error) {
	return r.store.subscribe(ctx, service)
}

func (r *EtcdResolver) loadAndWatch(ctx context.Context) error {
	resp, err := r.client.Get(ctx, r.prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	r.store.applyFull(resp.Kvs)
	go r.watch(resp.Header.Revision + 1)
	return nil
}

func (r *EtcdResolver) watch(revision int64) {
	watchCh := r.client.Watch(r.ctx, r.prefix, clientv3.WithPrefix(), clientv3.WithRev(revision))
	for resp := range watchCh {
		if resp.Err() != nil {
			continue
		}
		for _, event := range resp.Events {
			switch event.Type {
			case clientv3.EventTypePut:
				r.store.put(string(event.Kv.Key), event.Kv.Value)
			case clientv3.EventTypeDelete:
				r.store.delete(string(event.Kv.Key))
			}
		}
	}
}

type endpointStore struct {
	prefix string

	mu       sync.RWMutex
	version  uint64
	services map[string]map[string]Endpoint
	subs     map[string]map[chan Snapshot]struct{}
}

func newEndpointStore(prefix string) *endpointStore {
	return &endpointStore{
		prefix:   normalizePrefix(prefix),
		services: make(map[string]map[string]Endpoint),
		subs:     make(map[string]map[chan Snapshot]struct{}),
	}
}

func (s *endpointStore) applyFull(kvs []*mvccpb.KeyValue) {
	changed := make(map[string]struct{})
	s.mu.Lock()
	for _, kv := range kvs {
		service, nodeID, endpoint, ok := s.parseKV(string(kv.Key), kv.Value)
		if !ok {
			continue
		}
		if s.services[service] == nil {
			s.services[service] = make(map[string]Endpoint)
		}
		s.services[service][nodeID] = endpoint
		changed[service] = struct{}{}
	}
	s.version++
	snapshots := s.snapshotsLocked(changed)
	s.mu.Unlock()
	s.publishSnapshots(snapshots)
}

func (s *endpointStore) put(key string, value []byte) {
	service, nodeID, endpoint, ok := s.parseKV(key, value)
	if !ok {
		return
	}
	s.mu.Lock()
	if s.services[service] == nil {
		s.services[service] = make(map[string]Endpoint)
	}
	s.services[service][nodeID] = endpoint
	s.version++
	snapshot := s.snapshotLocked(service)
	s.mu.Unlock()
	s.publishSnapshots([]Snapshot{snapshot})
}

func (s *endpointStore) delete(key string) {
	service, nodeID, ok := s.parseKey(key)
	if !ok {
		return
	}
	s.mu.Lock()
	if nodes := s.services[service]; nodes != nil {
		delete(nodes, nodeID)
		if len(nodes) == 0 {
			delete(s.services, service)
		}
	}
	s.version++
	snapshot := s.snapshotLocked(service)
	s.mu.Unlock()
	s.publishSnapshots([]Snapshot{snapshot})
}

func (s *endpointStore) snapshot(service string) Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotLocked(service)
}

func (s *endpointStore) subscribe(ctx context.Context, service string) (<-chan Snapshot, func(), error) {
	ch := make(chan Snapshot, 1)
	s.mu.Lock()
	if s.subs[service] == nil {
		s.subs[service] = make(map[chan Snapshot]struct{})
	}
	s.subs[service][ch] = struct{}{}
	snapshot := s.snapshotLocked(service)
	s.mu.Unlock()

	sendSnapshot(ch, snapshot)
	unsubscribe := func() {
		s.mu.Lock()
		if subs := s.subs[service]; subs != nil {
			delete(subs, ch)
		}
		s.mu.Unlock()
	}
	go func() {
		<-ctx.Done()
		unsubscribe()
		close(ch)
	}()
	return ch, unsubscribe, nil
}

func (s *endpointStore) snapshotLocked(service string) Snapshot {
	nodes := s.services[service]
	endpoints := make([]Endpoint, 0, len(nodes))
	for _, endpoint := range nodes {
		endpoints = append(endpoints, endpoint)
	}
	return Snapshot{Service: service, Endpoints: endpoints, Version: s.version}
}

func (s *endpointStore) snapshotsLocked(services map[string]struct{}) []Snapshot {
	snapshots := make([]Snapshot, 0, len(services))
	for service := range services {
		snapshots = append(snapshots, s.snapshotLocked(service))
	}
	return snapshots
}

func (s *endpointStore) publishSnapshots(snapshots []Snapshot) {
	for _, snapshot := range snapshots {
		s.mu.RLock()
		subs := make([]chan Snapshot, 0, len(s.subs[snapshot.Service]))
		for ch := range s.subs[snapshot.Service] {
			subs = append(subs, ch)
		}
		s.mu.RUnlock()
		for _, ch := range subs {
			sendSnapshot(ch, snapshot)
		}
	}
}

func (s *endpointStore) parseKV(key string, value []byte) (string, string, Endpoint, bool) {
	service, nodeID, ok := s.parseKey(key)
	if !ok {
		return "", "", Endpoint{}, false
	}
	var endpoint Endpoint
	if err := json.Unmarshal(value, &endpoint); err != nil || endpoint.Addr == "" {
		return "", "", Endpoint{}, false
	}
	return service, nodeID, endpoint, true
}

func (s *endpointStore) parseKey(key string) (string, string, bool) {
	if !strings.HasPrefix(key, s.prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(key, s.prefix)
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func normalizePrefix(prefix string) string {
	clean := path.Clean("/" + strings.TrimPrefix(prefix, "/"))
	return strings.TrimSuffix(clean, "/") + "/"
}

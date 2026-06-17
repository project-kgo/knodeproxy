package proxy

import (
	"context"
	"sync"
)

type StaticResolver struct {
	mu        sync.RWMutex
	version   uint64
	snapshots map[string]Snapshot
	subs      map[string]map[chan Snapshot]struct{}
}

func NewStaticResolver() *StaticResolver {
	return &StaticResolver{
		snapshots: make(map[string]Snapshot),
		subs:      make(map[string]map[chan Snapshot]struct{}),
	}
}

func (r *StaticResolver) Set(service string, endpoints []Endpoint) {
	r.mu.Lock()
	r.version++
	snapshot := Snapshot{
		Service:   service,
		Endpoints: append([]Endpoint(nil), endpoints...),
		Version:   r.version,
	}
	r.snapshots[service] = snapshot
	subs := make([]chan Snapshot, 0, len(r.subs[service]))
	for ch := range r.subs[service] {
		subs = append(subs, ch)
	}
	r.mu.Unlock()

	for _, ch := range subs {
		sendSnapshot(ch, snapshot)
	}
}

func (r *StaticResolver) Snapshot(service string) Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return cloneSnapshot(r.snapshots[service])
}

func (r *StaticResolver) Subscribe(ctx context.Context, service string) (<-chan Snapshot, func(), error) {
	ch := make(chan Snapshot, 1)
	r.mu.Lock()
	if r.subs[service] == nil {
		r.subs[service] = make(map[chan Snapshot]struct{})
	}
	r.subs[service][ch] = struct{}{}
	snapshot := cloneSnapshot(r.snapshots[service])
	r.mu.Unlock()

	sendSnapshot(ch, snapshot)
	unsubscribe := func() {
		r.mu.Lock()
		if subs := r.subs[service]; subs != nil {
			delete(subs, ch)
		}
		r.mu.Unlock()
	}
	go func() {
		<-ctx.Done()
		unsubscribe()
		close(ch)
	}()
	return ch, unsubscribe, nil
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	snapshot.Endpoints = append([]Endpoint(nil), snapshot.Endpoints...)
	return snapshot
}

func sendSnapshot(ch chan Snapshot, snapshot Snapshot) {
	select {
	case ch <- snapshot:
	default:
		<-ch
		ch <- snapshot
	}
}

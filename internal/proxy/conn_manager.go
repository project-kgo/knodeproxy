package proxy

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/credentials/insecure"
	grpcresolver "google.golang.org/grpc/resolver"
)

const resolverScheme = "knodeproxy"

type ConnManager struct {
	resolver Resolver
	opts     []grpc.DialOption

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewConnManager(resolver Resolver, opts ...grpc.DialOption) *ConnManager {
	return &ConnManager{
		resolver: resolver,
		opts:     opts,
		conns:    make(map[string]*grpc.ClientConn),
	}
}

func (m *ConnManager) GetConn(ctx context.Context, service string) (*grpc.ClientConn, error) {
	if strings.TrimSpace(service) == "" {
		return nil, fmt.Errorf("proxy: empty target service")
	}
	snapshot := m.resolver.Snapshot(service)
	if len(snapshot.Endpoints) == 0 {
		return nil, fmt.Errorf("proxy: service %q has no endpoints", service)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if conn := m.conns[service]; conn != nil {
		return conn, nil
	}

	target := (&url.URL{Scheme: resolverScheme, Path: "/" + service}).String()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
		grpc.WithResolvers(&connResolverBuilder{manager: m}),
	}
	opts = append(opts, m.opts...)
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	m.conns[service] = conn
	return conn, nil
}

func (m *ConnManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for service, conn := range m.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(m.conns, service)
	}
	return firstErr
}

type connResolverBuilder struct {
	manager *ConnManager
}

func (b *connResolverBuilder) Scheme() string {
	return resolverScheme
}

func (b *connResolverBuilder) Build(target grpcresolver.Target, cc grpcresolver.ClientConn, _ grpcresolver.BuildOptions) (grpcresolver.Resolver, error) {
	service := strings.TrimPrefix(target.URL.Path, "/")
	if service == "" {
		service = target.URL.Host
	}
	if service == "" {
		return nil, fmt.Errorf("proxy resolver: empty service in target %q", target.URL.String())
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch, unsubscribe, err := b.manager.resolver.Subscribe(ctx, service)
	if err != nil {
		cancel()
		return nil, err
	}
	r := &connResolver{
		cancel:      cancel,
		unsubscribe: unsubscribe,
	}
	go r.watch(cc, ch)
	return r, nil
}

type connResolver struct {
	cancel      context.CancelFunc
	unsubscribe func()
}

func (r *connResolver) ResolveNow(grpcresolver.ResolveNowOptions) {}

func (r *connResolver) Close() {
	r.cancel()
	if r.unsubscribe != nil {
		r.unsubscribe()
	}
}

func (r *connResolver) watch(cc grpcresolver.ClientConn, ch <-chan Snapshot) {
	for snapshot := range ch {
		addresses := make([]grpcresolver.Address, 0, len(snapshot.Endpoints))
		for _, endpoint := range snapshot.Endpoints {
			if endpoint.Addr == "" {
				continue
			}
			addresses = append(addresses, grpcresolver.Address{Addr: endpoint.Addr})
		}
		if err := cc.UpdateState(grpcresolver.State{Addresses: addresses}); err != nil {
			cc.ReportError(err)
		}
	}
}

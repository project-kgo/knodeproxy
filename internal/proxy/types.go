package proxy

import "context"

const TargetServiceMetadataKey = "x-knodeproxy-target-service"

type Endpoint struct {
	Addr     string            `json:"addr"`
	Weight   int               `json:"weight,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type Snapshot struct {
	Service   string
	Endpoints []Endpoint
	Version   uint64
}

type Resolver interface {
	Snapshot(service string) Snapshot
	Subscribe(ctx context.Context, service string) (<-chan Snapshot, func(), error)
}

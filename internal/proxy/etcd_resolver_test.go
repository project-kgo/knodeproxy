package proxy

import "testing"

func TestEndpointStorePutDelete(t *testing.T) {
	store := newEndpointStore(DefaultEtcdPrefix)
	store.put("/knodeproxy/services/billing/node-a", []byte(`{"addr":"127.0.0.1:50051","weight":1}`))
	store.put("/knodeproxy/services/billing/node-b", []byte(`{"addr":"127.0.0.1:50052"}`))

	snapshot := store.snapshot("billing")
	if len(snapshot.Endpoints) != 2 {
		t.Fatalf("len(Endpoints) = %d, want 2", len(snapshot.Endpoints))
	}

	store.delete("/knodeproxy/services/billing/node-a")
	snapshot = store.snapshot("billing")
	if len(snapshot.Endpoints) != 1 {
		t.Fatalf("len(Endpoints) = %d, want 1", len(snapshot.Endpoints))
	}
	if snapshot.Endpoints[0].Addr != "127.0.0.1:50052" {
		t.Fatalf("remaining Addr = %q, want 127.0.0.1:50052", snapshot.Endpoints[0].Addr)
	}
}

func TestEndpointStoreIgnoresInvalidValues(t *testing.T) {
	store := newEndpointStore(DefaultEtcdPrefix)
	store.put("/knodeproxy/services/billing/node-a", []byte(`{"weight":1}`))
	store.put("/knodeproxy/services/billing", []byte(`{"addr":"127.0.0.1:50051"}`))
	if got := len(store.snapshot("billing").Endpoints); got != 0 {
		t.Fatalf("len(Endpoints) = %d, want 0", got)
	}
}

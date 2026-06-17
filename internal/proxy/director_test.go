package proxy

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestDefaultDirectorMetadataWins(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(TargetServiceMetadataKey, "billing"))
	route, err := NewDefaultDirector().Resolve(ctx, "/package.User/Get")
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if route.Service != "billing" {
		t.Fatalf("Service = %q, want billing", route.Service)
	}
}

func TestDefaultDirectorFallsBackToMethodService(t *testing.T) {
	route, err := NewDefaultDirector().Resolve(context.Background(), "/package.User/Get")
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if route.Service != "package.User" {
		t.Fatalf("Service = %q, want package.User", route.Service)
	}
}

func TestDefaultDirectorRejectsInvalidMethod(t *testing.T) {
	if _, err := NewDefaultDirector().Resolve(context.Background(), "package.User/Get"); err == nil {
		t.Fatal("Resolve() error = nil, want error")
	}
}

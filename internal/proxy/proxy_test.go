package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestProxyPassesRawPayload(t *testing.T) {
	backendAddr, stopBackend := startRawBackend(t, func(_ any, server grpc.ServerStream) error {
		var msg []byte
		if err := server.RecvMsg(&msg); err != nil {
			return err
		}
		return server.SendMsg(append([]byte("echo:"), msg...))
	})
	defer stopBackend()

	resolver := NewStaticResolver()
	resolver.Set("test.Echo", []Endpoint{{Addr: backendAddr}})
	manager := NewConnManager(resolver)
	defer manager.Close()
	proxyAddr, stopProxy := startProxy(t, NewProxy(NewDefaultDirector(), manager))
	defer stopProxy()

	conn, err := grpc.Dial(proxyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})))
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/test.Echo/Ping")
	if err != nil {
		t.Fatalf("NewStream() error = %v", err)
	}
	if err := stream.SendMsg([]byte("payload")); err != nil {
		t.Fatalf("SendMsg() error = %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend() error = %v", err)
	}
	var out []byte
	if err := stream.RecvMsg(&out); err != nil {
		t.Fatalf("RecvMsg() error = %v", err)
	}
	if string(out) != "echo:payload" {
		t.Fatalf("response = %q, want echo:payload", out)
	}
}

func startProxy(t *testing.T, p *Proxy) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	server := grpc.NewServer(ServerOptions(p.Handler())...)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("proxy Serve() error = %v", err)
		}
	}()
	return listener.Addr().String(), server.Stop
}

func startRawBackend(t *testing.T, handler grpc.StreamHandler) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	server := grpc.NewServer(ServerOptions(handler)...)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("backend Serve() error = %v", err)
		}
	}()
	return listener.Addr().String(), server.Stop
}

func TestProxyBidiStream(t *testing.T) {
	backendAddr, stopBackend := startRawBackend(t, func(_ any, server grpc.ServerStream) error {
		for {
			var msg []byte
			err := server.RecvMsg(&msg)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := server.SendMsg(append([]byte("ack:"), msg...)); err != nil {
				return err
			}
		}
	})
	defer stopBackend()

	resolver := NewStaticResolver()
	resolver.Set("test.Chat", []Endpoint{{Addr: backendAddr}})
	manager := NewConnManager(resolver)
	defer manager.Close()
	proxyAddr, stopProxy := startProxy(t, NewProxy(NewDefaultDirector(), manager))
	defer stopProxy()

	conn, err := grpc.Dial(proxyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})))
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/test.Chat/Talk")
	if err != nil {
		t.Fatalf("NewStream() error = %v", err)
	}

	for _, payload := range []string{"one", "two"} {
		if err := stream.SendMsg([]byte(payload)); err != nil {
			t.Fatalf("SendMsg(%q) error = %v", payload, err)
		}
		var out []byte
		if err := stream.RecvMsg(&out); err != nil {
			t.Fatalf("RecvMsg() error = %v", err)
		}
		if string(out) != "ack:"+payload {
			t.Fatalf("response = %q, want %q", out, "ack:"+payload)
		}
	}
}

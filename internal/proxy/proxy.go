package proxy

import (
	"context"
	"errors"
	"io"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Proxy struct {
	director Director
	conns    *ConnManager
}

func NewProxy(director Director, conns *ConnManager) *Proxy {
	if director == nil {
		director = NewDefaultDirector()
	}
	return &Proxy{director: director, conns: conns}
}

func ServerOptions(handler grpc.StreamHandler) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(handler),
	}
}

func (p *Proxy) Handler() grpc.StreamHandler {
	return func(_ any, serverStream grpc.ServerStream) error {
		return p.handle(serverStream)
	}
}

func (p *Proxy) handle(serverStream grpc.ServerStream) error {
	fullMethod, ok := grpc.MethodFromServerStream(serverStream)
	if !ok || fullMethod == "" {
		return status.Error(codes.InvalidArgument, "proxy: missing grpc method")
	}

	route, err := p.director.Resolve(serverStream.Context(), fullMethod)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	conn, err := p.conns.GetConn(serverStream.Context(), route.Service)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}

	ctx := outgoingContext(serverStream.Context())
	clientStream, err := conn.NewStream(
		ctx,
		&grpc.StreamDesc{StreamName: route.Method, ClientStreams: true, ServerStreams: true},
		route.Method,
		grpc.ForceCodec(rawCodec{}),
	)
	if err != nil {
		return err
	}

	errCh := make(chan error, 2)
	go copyClientToBackend(serverStream, clientStream, errCh)
	go copyBackendToClient(serverStream, clientStream, errCh)

	var finalErr error
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil && finalErr == nil {
			finalErr = err
		}
	}
	serverStream.SetTrailer(clientStream.Trailer())
	return finalErr
}

func copyClientToBackend(serverStream grpc.ServerStream, clientStream grpc.ClientStream, errCh chan<- error) {
	for {
		var msg []byte
		err := serverStream.RecvMsg(&msg)
		if errors.Is(err, io.EOF) {
			errCh <- clientStream.CloseSend()
			return
		}
		if err != nil {
			_ = clientStream.CloseSend()
			errCh <- err
			return
		}
		if err := clientStream.SendMsg(msg); err != nil {
			errCh <- err
			return
		}
	}
}

func copyBackendToClient(serverStream grpc.ServerStream, clientStream grpc.ClientStream, errCh chan<- error) {
	header, err := clientStream.Header()
	if err != nil {
		errCh <- err
		return
	}
	if len(header) > 0 {
		if err := serverStream.SetHeader(header); err != nil {
			errCh <- err
			return
		}
	}

	for {
		var msg []byte
		err := clientStream.RecvMsg(&msg)
		if errors.Is(err, io.EOF) {
			errCh <- nil
			return
		}
		if err != nil {
			errCh <- err
			return
		}
		if err := serverStream.SendMsg(msg); err != nil {
			errCh <- err
			return
		}
	}
}

func outgoingContext(ctx context.Context) context.Context {
	incoming, _ := metadata.FromIncomingContext(ctx)
	outgoing := metadata.MD{}
	for key, values := range incoming {
		lower := strings.ToLower(key)
		if lower == TargetServiceMetadataKey || strings.HasPrefix(lower, "grpc-") {
			continue
		}
		outgoing[lower] = append([]string(nil), values...)
	}
	return metadata.NewOutgoingContext(ctx, outgoing)
}

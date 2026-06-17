package proxy

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"
)

type Route struct {
	Service string
	Method  string
}

type Director interface {
	Resolve(ctx context.Context, fullMethod string) (Route, error)
}

type DefaultDirector struct {
	TargetServiceKey string
}

func NewDefaultDirector() DefaultDirector {
	return DefaultDirector{TargetServiceKey: TargetServiceMetadataKey}
}

func (d DefaultDirector) Resolve(ctx context.Context, fullMethod string) (Route, error) {
	service := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		key := d.TargetServiceKey
		if key == "" {
			key = TargetServiceMetadataKey
		}
		values := md.Get(key)
		if len(values) > 0 {
			service = strings.TrimSpace(values[0])
		}
	}
	if service == "" {
		var err error
		service, err = serviceFromMethod(fullMethod)
		if err != nil {
			return Route{}, err
		}
	}
	return Route{Service: service, Method: fullMethod}, nil
}

func serviceFromMethod(fullMethod string) (string, error) {
	if !strings.HasPrefix(fullMethod, "/") {
		return "", fmt.Errorf("invalid grpc method %q: missing leading slash", fullMethod)
	}
	trimmed := strings.TrimPrefix(fullMethod, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("invalid grpc method %q: expected /service/method", fullMethod)
	}
	return parts[0], nil
}

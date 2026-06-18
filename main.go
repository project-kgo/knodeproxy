package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/project-kgo/knodeproxy/internal/proxy"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := loadConfig()
	if err != nil {
		logger.Error("load config", slog.Any("error", err))
		os.Exit(1)
	}
	ctx := context.Background()

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: cfg.EtcdDialTimeout,
	})
	if err != nil {
		logger.Error("create etcd client", slog.Any("error", err))
		os.Exit(1)
	}
	defer etcdClient.Close()

	resolver, err := proxy.NewEtcdResolver(ctx, etcdClient, cfg.EtcdPrefix)
	if err != nil {
		logger.Error("create etcd resolver", slog.Any("error", err))
		os.Exit(1)
	}
	defer resolver.Close()

	connManager := proxy.NewConnManager(resolver)
	defer connManager.Close()

	p := proxy.NewProxy(proxy.NewDefaultDirector(), connManager)
	server := grpc.NewServer(proxy.ServerOptions(p.Handler())...)
	listener, err := listen(cfg)
	if err != nil {
		logger.Error("listen", slog.String("network", listenNetwork(cfg)), slog.String("addr", listenAddr(cfg)), slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info(
		"knodeproxy listening",
		slog.String("network", listener.Addr().Network()),
		slog.String("addr", listener.Addr().String()),
		slog.Any("etcd_endpoints", cfg.EtcdEndpoints),
		slog.String("etcd_prefix", cfg.EtcdPrefix),
	)
	if err := server.Serve(listener); err != nil {
		logger.Error("serve grpc proxy", slog.Any("error", err))
		os.Exit(1)
	}
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/project-kgo/knodeproxy/internal/proxy"
	"github.com/spf13/viper"
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
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Error("listen", slog.String("addr", cfg.ListenAddr), slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info(
		"knodeproxy listening",
		slog.String("addr", cfg.ListenAddr),
		slog.Any("etcd_endpoints", cfg.EtcdEndpoints),
		slog.String("etcd_prefix", cfg.EtcdPrefix),
	)
	if err := server.Serve(listener); err != nil {
		logger.Error("serve grpc proxy", slog.Any("error", err))
		os.Exit(1)
	}
}

type config struct {
	ListenAddr      string
	EtcdEndpoints   []string
	EtcdPrefix      string
	EtcdDialTimeout time.Duration
}

func loadConfig() (config, error) {
	configFile := flag.String("config", "", "path to .yml/.yaml config file")
	flag.Parse()

	v := viper.New()
	v.SetEnvPrefix("KNODEPROXY")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	v.SetDefault("listen.addr", ":8080")
	v.SetDefault("etcd.endpoints", []string{"127.0.0.1:2379"})
	v.SetDefault("etcd.prefix", proxy.DefaultEtcdPrefix)
	v.SetDefault("etcd.dial_timeout", 5*time.Second)

	if err := readConfigFile(v, *configFile); err != nil {
		return config{}, err
	}

	endpoints := v.GetStringSlice("etcd.endpoints")
	if len(endpoints) == 0 {
		endpoints = splitCSV(v.GetString("etcd.endpoints"))
	}
	if len(endpoints) == 0 {
		endpoints = []string{"127.0.0.1:2379"}
	}

	return config{
		ListenAddr:      v.GetString("listen.addr"),
		EtcdEndpoints:   endpoints,
		EtcdPrefix:      v.GetString("etcd.prefix"),
		EtcdDialTimeout: v.GetDuration("etcd.dial_timeout"),
	}, nil
}

func readConfigFile(v *viper.Viper, explicitPath string) error {
	configFile := strings.TrimSpace(explicitPath)
	if configFile == "" {
		configFile = strings.TrimSpace(os.Getenv("KNODEPROXY_CONFIG"))
	}
	if configFile == "" {
		return nil
	}

	ext := strings.ToLower(filepath.Ext(configFile))
	if ext != ".yml" && ext != ".yaml" {
		return fmt.Errorf("config file %q must use .yml or .yaml extension", configFile)
	}

	v.SetConfigFile(configFile)
	v.SetConfigType("yaml")
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("read config file %q: %w", configFile, err)
	}
	return nil
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

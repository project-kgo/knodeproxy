package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/project-kgo/knodeproxy/internal/proxy"
	"github.com/spf13/viper"
)

type config struct {
	ListenAddr      string
	ListenUDSPath   string
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
	v.SetDefault("listen.uds_path", "")
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
		ListenUDSPath:   strings.TrimSpace(v.GetString("listen.uds_path")),
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

func listen(cfg config) (net.Listener, error) {
	if cfg.ListenUDSPath == "" {
		return net.Listen("tcp", cfg.ListenAddr)
	}
	if err := prepareUDSPath(cfg.ListenUDSPath); err != nil {
		return nil, err
	}
	return net.Listen("unix", cfg.ListenUDSPath)
}

func listenNetwork(cfg config) string {
	if cfg.ListenUDSPath != "" {
		return "unix"
	}
	return "tcp"
}

func listenAddr(cfg config) string {
	if cfg.ListenUDSPath != "" {
		return cfg.ListenUDSPath
	}
	return cfg.ListenAddr
}

func prepareUDSPath(socketPath string) error {
	dir := filepath.Dir(socketPath)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create uds directory %q: %w", dir, err)
		}
	}

	info, err := os.Stat(socketPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat uds path %q: %w", socketPath, err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("uds path %q already exists and is not a socket", socketPath)
	}
	if err := os.Remove(socketPath); err != nil {
		return fmt.Errorf("remove stale uds socket %q: %w", socketPath, err)
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

package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigReadsYAMLFile(t *testing.T) {
	resetFlags(t)
	configFile := filepath.Join(t.TempDir(), "knodeproxy.yaml")
	if err := os.WriteFile(configFile, []byte(`
listen:
  addr: ":9090"
etcd:
  endpoints:
    - "127.0.0.1:12379"
    - "127.0.0.1:22379"
  prefix: "/custom/services/"
  dial_timeout: "7s"
`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv("KNODEPROXY_CONFIG", configFile)

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.ListenAddr != ":9090" {
		t.Fatalf("ListenAddr = %q, want :9090", cfg.ListenAddr)
	}
	if len(cfg.EtcdEndpoints) != 2 || cfg.EtcdEndpoints[0] != "127.0.0.1:12379" || cfg.EtcdEndpoints[1] != "127.0.0.1:22379" {
		t.Fatalf("EtcdEndpoints = %#v, want two configured endpoints", cfg.EtcdEndpoints)
	}
	if cfg.EtcdPrefix != "/custom/services/" {
		t.Fatalf("EtcdPrefix = %q, want /custom/services/", cfg.EtcdPrefix)
	}
	if cfg.EtcdDialTimeout != 7*time.Second {
		t.Fatalf("EtcdDialTimeout = %s, want 7s", cfg.EtcdDialTimeout)
	}
}

func TestLoadConfigEnvOverridesYAMLFile(t *testing.T) {
	resetFlags(t)
	configFile := filepath.Join(t.TempDir(), "knodeproxy.yml")
	if err := os.WriteFile(configFile, []byte(`
listen:
  addr: ":9090"
`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv("KNODEPROXY_CONFIG", configFile)
	t.Setenv("KNODEPROXY_LISTEN_ADDR", ":10000")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.ListenAddr != ":10000" {
		t.Fatalf("ListenAddr = %q, want env override :10000", cfg.ListenAddr)
	}
}

func TestLoadConfigRejectsNonYAMLFile(t *testing.T) {
	resetFlags(t)
	t.Setenv("KNODEPROXY_CONFIG", filepath.Join(t.TempDir(), "knodeproxy.json"))
	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig() error = nil, want error")
	}
}

func resetFlags(t *testing.T) {
	t.Helper()
	oldCommandLine := flag.CommandLine
	oldArgs := os.Args
	flag.CommandLine = flag.NewFlagSet(oldArgs[0], flag.ContinueOnError)
	os.Args = []string{oldArgs[0]}
	t.Cleanup(func() {
		flag.CommandLine = oldCommandLine
		os.Args = oldArgs
	})
}

package main

import (
	"flag"
	"net"
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
  uds_path: "/tmp/knodeproxy-test.sock"
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
	if cfg.ListenUDSPath != "/tmp/knodeproxy-test.sock" {
		t.Fatalf("ListenUDSPath = %q, want /tmp/knodeproxy-test.sock", cfg.ListenUDSPath)
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

func TestLoadConfigReadsUDSPathFromEnv(t *testing.T) {
	resetFlags(t)
	t.Setenv("KNODEPROXY_LISTEN_UDS_PATH", "/tmp/knodeproxy-env.sock")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.ListenUDSPath != "/tmp/knodeproxy-env.sock" {
		t.Fatalf("ListenUDSPath = %q, want /tmp/knodeproxy-env.sock", cfg.ListenUDSPath)
	}
}

func TestLoadConfigRejectsNonYAMLFile(t *testing.T) {
	resetFlags(t)
	t.Setenv("KNODEPROXY_CONFIG", filepath.Join(t.TempDir(), "knodeproxy.json"))
	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig() error = nil, want error")
	}
}

func TestListenUsesUDSWhenConfigured(t *testing.T) {
	socketPath := testSocketPath(t, "listen.sock")
	listener, err := listen(config{ListenAddr: "127.0.0.1:0", ListenUDSPath: socketPath})
	if err != nil {
		t.Fatalf("listen() error = %v", err)
	}
	defer listener.Close()

	if listener.Addr().Network() != "unix" {
		t.Fatalf("Network() = %q, want unix", listener.Addr().Network())
	}
	if listener.Addr().String() != socketPath {
		t.Fatalf("Addr() = %q, want %q", listener.Addr().String(), socketPath)
	}
}

func TestListenRejectsExistingNonSocketUDSPath(t *testing.T) {
	socketPath := testSocketPath(t, "regular.sock")
	if err := os.WriteFile(socketPath, []byte("not a socket"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := listen(config{ListenUDSPath: socketPath}); err == nil {
		t.Fatal("listen() error = nil, want error")
	}
}

func TestPrepareUDSPathRemovesStaleSocket(t *testing.T) {
	socketPath := testSocketPath(t, "stale.sock")
	stale, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	if err := stale.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := prepareUDSPath(socketPath); err != nil {
		t.Fatalf("prepareUDSPath() error = %v", err)
	}
	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Fatalf("Stat() error = %v, want not exist", err)
	}
}

func testSocketPath(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "knodeproxy-test-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() {
		_ = os.Remove(filepath.Join(dir, name))
		_ = os.Remove(dir)
	})
	return filepath.Join(dir, name)
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

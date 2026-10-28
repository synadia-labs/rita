package testutil

import (
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
)

const storeDirRemoveTimeout = 2 * time.Second

func NewNatsServerWithDir(dir string) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = dir
	return natsserver.RunServer(&opts)
}

func NewNatsServer(tb testing.TB) *server.Server {
	return NewNatsServerWithDir(storeDir(tb))
}

func NewNatsServerWithDomain(tb testing.TB, domain string) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.JetStreamDomain = domain
	opts.StoreDir = storeDir(tb)
	return natsserver.RunServer(&opts)
}

func ShutdownNatsServer(s *server.Server) {
	s.Shutdown()
	s.WaitForShutdown()
}

func storeDir(tb testing.TB) string {
	tb.Helper()
	dir, err := os.MkdirTemp("", "rita-nats-*")
	if err != nil {
		tb.Fatalf("create nats store dir: %v", err)
	}
	tb.Cleanup(func() {
		if err := removeStoreDir(dir, storeDirRemoveTimeout); err != nil {
			tb.Errorf("remove nats store dir: %v", err)
		}
	})
	return dir
}

func removeStoreDir(dir string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		err := os.RemoveAll(dir)
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

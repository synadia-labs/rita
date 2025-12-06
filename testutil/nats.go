package testutil

import (
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
)

func NewNatsServer(tb testing.TB) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = tb.TempDir()
	return natsserver.RunServer(&opts)
}

func ShutdownNatsServer(s *server.Server) {
	s.Shutdown()
	s.WaitForShutdown()
}

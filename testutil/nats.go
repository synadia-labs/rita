package testutil

import (
	"os"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
)

func NewNatsServer() *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return natsserver.RunServer(&opts)
}

func ShutdownNatsServer(s *server.Server) {
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != "" {
		_ = os.RemoveAll(sd)
	}
	s.WaitForShutdown()
}

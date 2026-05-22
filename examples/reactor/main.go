// Reactor demonstrates a minimal end-to-end Rita reactor: it spins up an embedded JetStream server,
// appends a few order events, and starts a durable reactor that handles OrderShipped events via a subject filter.
//
// Run with: go run ./examples/reactor
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/synadia-labs/rita"
	"github.com/synadia-labs/rita/testutil"
	"github.com/synadia-labs/rita/types"
)

type OrderPlaced struct {
	OrderID string
	Amount  int
}

type OrderShipped struct {
	OrderID string
	Carrier string
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "rita-example-*")
	if err != nil {
		return fmt.Errorf("temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	srv := testutil.NewNatsServerWithDir(dir)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer nc.Close()

	registry, err := types.NewRegistry(map[string]*types.Type{
		"order-placed":  {Init: func() any { return &OrderPlaced{} }},
		"order-shipped": {Init: func() any { return &OrderShipped{} }},
	})
	if err != nil {
		return fmt.Errorf("registry: %w", err)
	}

	mgr, err := rita.New(nc, rita.WithRegistry(registry))
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}

	ctx := context.Background()
	es, err := mgr.CreateEventStore(ctx, rita.EventStoreConfig{Name: "orders"})
	if err != nil {
		return fmt.Errorf("create store: %w", err)
	}

	if _, err := es.Append(ctx, []*rita.Event{
		{Entity: "order.1001", Data: &OrderPlaced{OrderID: "1001", Amount: 50}},
		{Entity: "order.1001", Data: &OrderShipped{OrderID: "1001", Carrier: "UPS"}},
		{Entity: "order.1002", Data: &OrderPlaced{OrderID: "1002", Amount: 75}},
		{Entity: "order.1002", Data: &OrderShipped{OrderID: "1002", Carrier: "FedEx"}},
	}); err != nil {
		return fmt.Errorf("append: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	handler := func(_ context.Context, ev *rita.Event) error {
		shipped, ok := ev.Data.(*OrderShipped)
		if !ok {
			return rita.ErrUnprocessable
		}
		fmt.Printf("shipped: order=%s carrier=%s\n", shipped.OrderID, shipped.Carrier)
		wg.Done()
		return nil
	}

	r, err := es.CreateOrUpdateReactor(ctx, rita.ReactorConfig{
		Name:    "shipping-notifier",
		Filters: []string{"*.*.order-shipped"},
	})
	if err != nil {
		return fmt.Errorf("create reactor: %w", err)
	}

	if err := r.Bind(ctx, handler); err != nil {
		return fmt.Errorf("bind: %w", err)
	}

	wg.Wait()

	unbindCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return r.Unbind(unbindCtx)
}

// Projection demonstrates Watch: a model kept continuously up to date as events
// are appended. Because events are applied from a background goroutine, the
// model must be thread-safe, so it is wrapped with NewModel and read via View.
//
// Run with: go run ./examples/projection
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/synadia-labs/rita"
	"github.com/synadia-labs/rita/testutil"
	"github.com/synadia-labs/rita/types"
)

type OrderPlaced struct{ Amount int }
type OrderShipped struct{ Carrier string }

// Stats is a read model aggregating across all orders.
type Stats struct {
	Placed  int
	Shipped int
}

func (s *Stats) Evolve(_ context.Context, e *rita.Event) error {
	switch e.Data.(type) {
	case *OrderPlaced:
		s.Placed++
	case *OrderShipped:
		s.Shipped++
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "rita-example-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	srv := testutil.NewNatsServerWithDir(dir)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		return err
	}
	defer nc.Close()

	registry, err := types.NewRegistry(map[string]*types.Type{
		"order-placed":  {Init: func() any { return &OrderPlaced{} }},
		"order-shipped": {Init: func() any { return &OrderShipped{} }},
	})
	if err != nil {
		return err
	}

	mgr, err := rita.New(nc, rita.WithRegistry(registry))
	if err != nil {
		return err
	}

	ctx := context.Background()
	es, err := mgr.CreateEventStore(ctx, rita.EventStoreConfig{Name: "orders"})
	if err != nil {
		return err
	}

	// Some events already exist before the projection starts.
	if _, err := es.Append(ctx, []*rita.Event{
		{Entity: "order.1", Data: &OrderPlaced{Amount: 50}},
		{Entity: "order.2", Data: &OrderPlaced{Amount: 75}},
	}); err != nil {
		return err
	}

	// Watch catches up on existing events before returning, then keeps applying
	// new ones in the background until Stop.
	model := rita.NewModel(&Stats{})
	w, err := es.Watch(ctx, model)
	if err != nil {
		return err
	}
	defer w.Stop()

	// Append more events after the watch is live.
	if _, err := es.Append(ctx, []*rita.Event{
		{Entity: "order.1", Data: &OrderShipped{Carrier: "UPS"}},
		{Entity: "order.2", Data: &OrderShipped{Carrier: "FedEx"}},
	}); err != nil {
		return err
	}

	// Delivery is asynchronous, so poll the view until it reflects all events.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var done bool
		if err := model.View(ctx, func(s *Stats) error {
			done = s.Placed == 2 && s.Shipped == 2
			return nil
		}); err != nil {
			return err
		}
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return model.View(ctx, func(s *Stats) error {
		fmt.Printf("placed=%d shipped=%d\n", s.Placed, s.Shipped)
		return nil
	})
}

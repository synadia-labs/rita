// Quickstart appends events for an order and rebuilds the order's state by
// folding those events through a model. It is the example referenced by the
// project README and docs/.
//
// Run with: go run ./examples/quickstart
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/nats-io/nats.go"

	"github.com/synadia-labs/rita"
	"github.com/synadia-labs/rita/testutil"
	"github.com/synadia-labs/rita/types"
)

// Events are facts that have already happened.
type OrderPlaced struct {
	OrderID string
	Amount  int
}

type OrderShipped struct {
	OrderID string
	Carrier string
}

// Order is a model. It implements Evolver to fold events into state.
type Order struct {
	Placed  bool
	Shipped bool
	Amount  int
}

func (o *Order) Evolve(_ context.Context, e *rita.Event) error {
	switch d := e.Data.(type) {
	case *OrderPlaced:
		o.Placed, o.Amount = true, d.Amount
	case *OrderShipped:
		o.Shipped = true
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Embedded JetStream server so the example is self-contained.
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

	// A registry maps event names to Go types so Rita can (de)serialize them.
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

	// An event store is backed by a JetStream stream.
	es, err := mgr.CreateEventStore(ctx, rita.EventStoreConfig{Name: "orders"})
	if err != nil {
		return err
	}

	// Append events for an entity, identified as "<type>.<id>".
	if _, err := es.Append(ctx, []*rita.Event{
		{Entity: "order.1001", Data: &OrderPlaced{OrderID: "1001", Amount: 50}},
		{Entity: "order.1001", Data: &OrderShipped{OrderID: "1001", Carrier: "UPS"}},
	}); err != nil {
		return err
	}

	// Rebuild state by replaying the entity's events through the model.
	var order Order
	if _, err := es.Evolve(ctx, &order, rita.WithFilters("order.1001")); err != nil {
		return err
	}

	fmt.Printf("placed=%v shipped=%v amount=%d\n", order.Placed, order.Shipped, order.Amount)
	return nil
}

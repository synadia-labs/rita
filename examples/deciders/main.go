// Deciders demonstrates the write-side patterns: a model that both decides
// (command -> events) and evolves (event -> state), driven through a thread-safe
// Model with DecideAndEvolve, and read back through View. It also shows a
// decision being rejected when it would violate an invariant.
//
// Run with: go run ./examples/deciders
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/nats-io/nats.go"

	"github.com/synadia-labs/rita"
	"github.com/synadia-labs/rita/testutil"
	"github.com/synadia-labs/rita/types"
)

// Commands are intentions; they may be refused.
type PlaceOrder struct{ Amount int }
type ShipOrder struct{ Carrier string }

// Events are facts; they already happened.
type OrderPlaced struct{ Amount int }
type OrderShipped struct{ Carrier string }

// Order is an aggregate: it decides what events a command produces (validating
// against current state) and evolves its state from those events.
type Order struct {
	Placed  bool
	Shipped bool
	Amount  int
}

func (o *Order) Decide(_ context.Context, cmd *rita.Command) ([]*rita.Event, error) {
	switch c := cmd.Data.(type) {
	case *PlaceOrder:
		if o.Placed {
			return nil, errors.New("order already placed")
		}
		return []*rita.Event{{Entity: "order.1", Data: &OrderPlaced{Amount: c.Amount}}}, nil
	case *ShipOrder:
		if !o.Placed {
			return nil, errors.New("cannot ship an order that was never placed")
		}
		if o.Shipped {
			return nil, errors.New("order already shipped")
		}
		return []*rita.Event{{Entity: "order.1", Data: &OrderShipped{Carrier: c.Carrier}}}, nil
	}
	return nil, nil
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

	// Only events are registered: events are serialized to and from the log,
	// commands never are. EventStore.Decide passes the *Command straight to the
	// model, so registering command types would wrongly imply they are stored.
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

	// NewModel wraps the aggregate with thread-safety and per-entity sequence
	// tracking. DecideAndEvolve decides, appends the events, and applies them
	// back to the in-memory model in one step.
	model := rita.NewModel(&Order{})

	if _, _, err := es.DecideAndEvolve(ctx, model, &rita.Command{Data: &PlaceOrder{Amount: 50}}); err != nil {
		return err
	}
	if _, _, err := es.DecideAndEvolve(ctx, model, &rita.Command{Data: &ShipOrder{Carrier: "UPS"}}); err != nil {
		return err
	}

	// View reads the model's current state under a read lock.
	if err := model.View(ctx, func(o *Order) error {
		fmt.Printf("placed=%v shipped=%v amount=%d\n", o.Placed, o.Shipped, o.Amount)
		return nil
	}); err != nil {
		return err
	}

	// A decision that would violate an invariant is refused, and nothing is
	// appended.
	if _, _, err := es.DecideAndEvolve(ctx, model, &rita.Command{Data: &ShipOrder{Carrier: "FedEx"}}); err != nil {
		fmt.Printf("second ship rejected: %v\n", err)
	}

	return nil
}

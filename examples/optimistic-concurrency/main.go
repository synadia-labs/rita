// Optimistic-concurrency demonstrates guarding an append with an expected
// sequence so a concurrent writer cannot clobber another's change. The losing
// writer gets ErrSequenceConflict, reloads current state, and retries.
//
// Run with: go run ./examples/optimistic-concurrency
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

type OrderPlaced struct{ Amount int }
type OrderShipped struct{ Carrier string }
type OrderNoteAdded struct{ Note string }

// order counts applied events; its only job here is to surface the entity's
// last sequence from Evolve.
type order struct{ events int }

func (o *order) Evolve(_ context.Context, _ *rita.Event) error {
	o.events++
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
		"order-placed":     {Init: func() any { return &OrderPlaced{} }},
		"order-shipped":    {Init: func() any { return &OrderShipped{} }},
		"order-note-added": {Init: func() any { return &OrderNoteAdded{} }},
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

	// Seed the entity. Expecting sequence 0 asserts the entity is brand new.
	seq, err := es.Append(ctx, []*rita.Event{
		{Entity: "order.1", Data: &OrderPlaced{Amount: 50}, Expect: rita.ExpectSequence(0)},
	})
	if err != nil {
		return err
	}
	fmt.Printf("placed at seq=%d\n", seq)

	// Two writers both observed the entity's last sequence as 1 and each guard
	// their append with ExpectSequence(1). Only the first can win.
	const observed = 1

	if _, err := es.Append(ctx, []*rita.Event{
		{Entity: "order.1", Data: &OrderShipped{Carrier: "UPS"}, Expect: rita.ExpectSequence(observed)},
	}); err != nil {
		return err
	}
	fmt.Println("writer A: shipped (won)")

	_, err = es.Append(ctx, []*rita.Event{
		{Entity: "order.1", Data: &OrderNoteAdded{Note: "gift wrap"}, Expect: rita.ExpectSequence(observed)},
	})
	if !errors.Is(err, rita.ErrSequenceConflict) {
		return fmt.Errorf("expected sequence conflict, got %v", err)
	}
	fmt.Println("writer B: conflict (lost)")

	// Writer B recovers: reload current state to learn the new last sequence,
	// then retry the append guarded by that sequence.
	var current order
	last, err := es.Evolve(ctx, &current, rita.WithFilters("order.1"))
	if err != nil {
		return err
	}
	if _, err := es.Append(ctx, []*rita.Event{
		{Entity: "order.1", Data: &OrderNoteAdded{Note: "gift wrap"}, Expect: rita.ExpectSequence(last)},
	}); err != nil {
		return err
	}
	fmt.Printf("writer B: retried at seq=%d (won)\n", last)

	return nil
}

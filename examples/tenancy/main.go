// Tenancy demonstrates a tenant store: one event store serving many isolated
// tenants. Every operation goes through a tenant-scoped handle, and each
// tenant's events occupy a disjoint slice of the subject space.
//
// Run with: go run ./examples/tenancy
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

// orders counts the events it sees.
type orders struct{ count int }

func (o *orders) Evolve(_ context.Context, _ *rita.Event) error {
	o.count++
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
		"order-placed": {Init: func() any { return &OrderPlaced{} }},
	})
	if err != nil {
		return err
	}

	mgr, err := rita.New(nc, rita.WithRegistry(registry))
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Tenancy is fixed at creation and recorded in stream metadata.
	es, err := mgr.CreateEventStore(ctx, rita.EventStoreConfig{Name: "orders", Tenancy: true})
	if err != nil {
		return err
	}

	// The bare handle is unscoped: store operations require a tenant scope.
	if _, err := es.Append(ctx, []*rita.Event{
		{Entity: "order.1", Data: &OrderPlaced{Amount: 10}},
	}); !errors.Is(err, rita.ErrTenantRequired) {
		return fmt.Errorf("expected ErrTenantRequired, got %v", err)
	}
	fmt.Println("unscoped append rejected")

	// Derive a scoped handle per tenant. The receiver is unchanged, so handles
	// are cheap and immutable.
	acme, err := es.Tenant("acme")
	if err != nil {
		return err
	}
	beta, err := es.Tenant("beta")
	if err != nil {
		return err
	}

	if _, err := acme.Append(ctx, []*rita.Event{
		{Entity: "order.1", Data: &OrderPlaced{Amount: 50}},
		{Entity: "order.2", Data: &OrderPlaced{Amount: 75}},
	}); err != nil {
		return err
	}
	if _, err := beta.Append(ctx, []*rita.Event{
		{Entity: "order.1", Data: &OrderPlaced{Amount: 99}},
	}); err != nil {
		return err
	}

	// Each tenant sees only its own events. A scoped handle with no filters is
	// confined to its tenant, never the whole stream.
	var acmeOrders orders
	if _, err := acme.Evolve(ctx, &acmeOrders); err != nil {
		return err
	}
	var betaOrders orders
	if _, err := beta.Evolve(ctx, &betaOrders); err != nil {
		return err
	}

	fmt.Printf("acme orders=%d beta orders=%d\n", acmeOrders.count, betaOrders.count)
	return nil
}

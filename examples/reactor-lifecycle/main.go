// Reactor-lifecycle demonstrates the Rita reactor lifecycle methods on
// *EventStore: CreateReactor, GetReactor, UpdateReactor, ListReactors,
// React (runtime), and DeleteReactor. CreateOrUpdateReactor is intentionally
// omitted see ./examples/reactor for the CreateOrUpdateReactor example.
//
// Run with: go run ./examples/reactor-lifecycle
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"

	"github.com/synadia-labs/rita"
	"github.com/synadia-labs/rita/types"
)

const name = "shipping-notifier"

type OrderPlaced struct {
	OrderID string
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
	srv, dir, err := startEmbeddedNATS()
	if err != nil {
		return fmt.Errorf("start nats: %w", err)
	}
	defer func() {
		srv.Shutdown()
		srv.WaitForShutdown()
		_ = os.RemoveAll(dir)
	}()

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

	fmt.Println("== CreateReactor ==")
	if err := es.CreateReactor(ctx, rita.ReactorConfig{
		Name:        name,
		Description: "logs shipped orders",
		Filters:     []string{"*.*.order-shipped"},
		AckWait:     5 * time.Second,
	}); err != nil {
		return fmt.Errorf("create reactor: %w", err)
	}
	fmt.Printf("created %q\n\n", name)

	fmt.Println("== GetReactor ==")
	info, err := es.GetReactor(ctx, name)
	if err != nil {
		return fmt.Errorf("get reactor: %w", err)
	}
	fmt.Printf("name=%s ack_wait=%s max_ack_pending=%d filters=%v\n\n",
		info.Name, info.Config.AckWait, info.Config.MaxAckPending, info.Config.Filters)

	fmt.Println("== UpdateReactor ==")
	cfg := info.Config
	cfg.AckWait = 10 * time.Second
	cfg.Description = "logs shipped orders"
	if err := es.UpdateReactor(ctx, cfg); err != nil {
		return fmt.Errorf("update reactor: %w", err)
	}
	updated, err := es.GetReactor(ctx, name)
	if err != nil {
		return fmt.Errorf("get reactor after update: %w", err)
	}
	fmt.Printf("updated ack_wait=%s description=%q\n\n", updated.Config.AckWait, updated.Config.Description)

	fmt.Println("== ListReactors ==")
	reactors, err := es.ListReactors(ctx)
	if err != nil {
		return fmt.Errorf("list reactors: %w", err)
	}
	for _, r := range reactors {
		fmt.Printf("- %s (pending=%d)\n\n", r.Name, r.NumPending)
	}

	fmt.Println("== GetReactor ==")
	reactorGet, err := es.GetReactor(ctx, name)
	if err != nil {
		return fmt.Errorf("get reactor err: %w", err)
	}
	fmt.Printf(
		"name=%s\nack_wait=%s\nmax_ack_pending=%d\nfilters=%v\ncreated=%v\n\n",
		reactorGet.Name,
		reactorGet.Config.AckWait,
		reactorGet.Config.MaxAckPending,
		reactorGet.Config.Filters,
		reactorGet.Created,
	)

	fmt.Println("== React ==")
	if _, err := es.Append(ctx, []*rita.Event{
		{Entity: "order.1001", Data: &OrderPlaced{OrderID: "1001"}},
		{Entity: "order.1001", Data: &OrderShipped{OrderID: "1001", Carrier: "UPS"}},
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
		fmt.Printf("handled: order=%s carrier=%s\n", shipped.OrderID, shipped.Carrier)
		wg.Done()
		return nil
	}

	r, err := es.React(ctx, name, handler)
	if err != nil {
		return fmt.Errorf("react: %w", err)
	}
	wg.Wait()

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Stop(stopCtx); err != nil {
		return fmt.Errorf("stop: %w", err)
	}
	fmt.Printf("reactor stopped (durable still exists)\n")

	fmt.Println("== DeleteReactor ==")
	if err := es.DeleteReactor(ctx, name); err != nil {
		return fmt.Errorf("delete reactor: %w", err)
	}

	if _, err := es.GetReactor(ctx, name); !errors.Is(err, rita.ErrReactorNotFound) {
		return fmt.Errorf("expected ErrReactorNotFound after delete, got %v", err)
	}
	fmt.Printf("deleted %q\n\n", name)

	fmt.Println("== GetReactor after delete ==")
	_, err = es.GetReactor(ctx, name)
	if err != nil {
		if errors.Is(err, rita.ErrReactorNotFound) {
			fmt.Printf("reactor %q deleted and no longer exists (as expected)\n\n", name)
			return nil
		}
		return fmt.Errorf("get reactor err: %w", err)
	}

	return nil
}

func startEmbeddedNATS() (*server.Server, string, error) {
	dir, err := os.MkdirTemp("", "rita-example-*")
	if err != nil {
		return nil, "", err
	}
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = dir
	return natsserver.RunServer(&opts), dir, nil
}

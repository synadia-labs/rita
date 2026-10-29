// Snapshots demonstrates explicit save, fresh-state recovery, tail replay,
// and the next command using restored optimistic concurrency.
//
// Run with: go run ./examples/snapshots
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/nats-io/nats.go"

	"github.com/synadia-labs/rita"
	"github.com/synadia-labs/rita/codec"
	"github.com/synadia-labs/rita/testutil"
	"github.com/synadia-labs/rita/types"
)

type Deposit struct{ Amount int }
type Deposited struct{ Amount int }

type Account struct {
	Balance int `json:"balance"`
}

func (a *Account) Decide(_ context.Context, cmd *rita.Command) ([]*rita.Event, error) {
	c := cmd.Data.(*Deposit)
	return []*rita.Event{{Entity: "account.1", Data: &Deposited{Amount: c.Amount}}}, nil
}

func (a *Account) Evolve(_ context.Context, e *rita.Event) error {
	d := e.Data.(*Deposited)
	a.Balance += d.Amount
	fmt.Printf("  evolve seq=%d: +%d -> balance=%d\n", e.Sequence(), d.Amount, a.Balance)
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "rita-snapshots-*")
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
		"deposited": {Init: func() any { return &Deposited{} }},
	})
	if err != nil {
		return err
	}
	mgr, err := rita.New(nc, rita.WithRegistry(registry))
	if err != nil {
		return err
	}
	ctx := context.Background()
	es, err := mgr.CreateEventStore(ctx, rita.EventStoreConfig{Name: "accounts"})
	if err != nil {
		return err
	}
	config := rita.SnapshotConfig{
		Name: "account-1", Version: "v1", Filters: []string{"account.1"},
		Codec: codec.JSON,
	}
	model, err := rita.LoadModel(ctx, es, &Account{}, config)
	if err != nil {
		return err
	}

	fmt.Println("deposit 10")
	if _, _, err := es.DecideAndEvolve(ctx, model, &rita.Command{Data: &Deposit{Amount: 10}}); err != nil {
		return err
	}
	// Decisions advance entity sequences; ordered replay advances the snapshot cursor.
	if _, err := es.Evolve(ctx, model); err != nil {
		return err
	}
	if err := model.SaveSnapshot(ctx); err != nil {
		return err
	}
	printBalance(ctx, "save snapshot", model)

	fmt.Println("deposit 5 (not in the snapshot)")
	if _, _, err := es.DecideAndEvolve(ctx, model, &rita.Command{Data: &Deposit{Amount: 5}}); err != nil {
		return err
	}

	fmt.Println("restart: load snapshot, replay the tail")
	// Simulate an application restart: never reuse the original state pointer.
	restored, err := rita.LoadModel(ctx, es, &Account{}, config)
	if err != nil {
		return err
	}
	printBalance(ctx, "loaded", restored)

	fmt.Println("deposit 2 on the restored model")
	events, _, err := es.DecideAndEvolve(ctx, restored, &rita.Command{Data: &Deposit{Amount: 2}})
	if err != nil {
		return err
	}
	fmt.Printf("  expected last seq=%d, accepted\n", events[0].Expect.Sequence)
	printBalance(ctx, "final", restored)
	return nil
}

func printBalance(ctx context.Context, label string, m *rita.Model[*Account]) {
	_ = m.View(ctx, func(a *Account) error {
		fmt.Printf("%s: balance=%d\n", label, a.Balance)
		return nil
	})
}

// Snapshots demonstrates explicit save, fresh-state recovery, tail replay,
// and the next command using restored optimistic concurrency.
//
// Run with: go run ./examples/snapshots
package main

import (
	"context"
	"errors"
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
	Balance  int `json:"balance"`
	Replayed int `json:"-"`
}

func (a *Account) Decide(_ context.Context, cmd *rita.Command) ([]*rita.Event, error) {
	c, ok := cmd.Data.(*Deposit)
	if !ok {
		return nil, nil
	}
	if c.Amount <= 0 {
		return nil, errors.New("deposit must be positive")
	}
	return []*rita.Event{{Entity: "account.1", Data: &Deposited{Amount: c.Amount}}}, nil
}

func (a *Account) Evolve(_ context.Context, e *rita.Event) error {
	if d, ok := e.Data.(*Deposited); ok {
		if d.Amount <= 0 {
			return errors.New("invalid deposit event")
		}
		a.Balance += d.Amount
		a.Replayed++
	}
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
	if err := model.View(ctx, func(a *Account) error {
		if a.Balance != 10 {
			return fmt.Errorf("saved balance: got %d, want 10", a.Balance)
		}
		state, err := codec.JSON.Marshal(a)
		if err != nil {
			return err
		}
		fmt.Printf("saved snapshot state: %s\n", state)
		return nil
	}); err != nil {
		return err
	}

	if _, _, err := es.DecideAndEvolve(ctx, model, &rita.Command{Data: &Deposit{Amount: 5}}); err != nil {
		return err
	}
	// Simulate an application restart: never reuse the original state pointer.
	restored, err := rita.LoadModel(ctx, es, &Account{}, config)
	if err != nil {
		return err
	}
	if err := restored.View(ctx, func(a *Account) error {
		if a.Balance != 15 || a.Replayed != 1 {
			return fmt.Errorf("restored: got balance=%d replayed=%d, want balance=15 replayed=1", a.Balance, a.Replayed)
		}
		fmt.Printf("restored snapshot + %d tail event: balance=%d\n", a.Replayed, a.Balance)
		return nil
	}); err != nil {
		return err
	}
	if _, _, err := es.DecideAndEvolve(ctx, restored, &rita.Command{Data: &Deposit{Amount: 2}}); err != nil {
		return err
	}
	return restored.View(ctx, func(a *Account) error {
		if a.Balance != 17 {
			return fmt.Errorf("next decision balance: got %d, want 17", a.Balance)
		}
		fmt.Printf("next decision accepted: balance=%d\n", a.Balance)
		return nil
	})
}

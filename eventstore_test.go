package rita

import (
	"context"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-labs/rita/id"
	"github.com/synadia-labs/rita/testutil"
	"github.com/synadia-labs/rita/types"
)

type OrderPlaced struct {
	ID string
}

type OrderShipped struct {
	ID string
}

type OrderStats struct {
	OrdersPlaced  int
	OrdersShipped int
}

func (s *OrderStats) Evolve(event *Event) error {
	switch event.Data.(type) {
	case *OrderPlaced:
		s.OrdersPlaced++
	case *OrderShipped:
		s.OrdersShipped++
	}
	return nil
}

func TestEventStoreNoRegistry(t *testing.T) {
	is := testutil.NewIs(t)

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, _ := nats.Connect(srv.ClientURL())

	r, err := New(t.Context(), nc)
	is.NoErr(err)

	es := r.EventStore("orders")
	err = es.Create(&jetstream.StreamConfig{
		Storage: jetstream.MemoryStorage,
	})
	is.NoErr(err)

	ctx := context.Background()

	seq, err := es.Append(ctx, "orders.1", []*Event{{
		Type: "foo",
		Data: []byte("hello"),
	}})
	is.NoErr(err)
	is.Equal(seq, uint64(1))

	events, _, err := es.Load(ctx, "orders.1")
	is.NoErr(err)
	is.Equal(events[0].Type, "foo")
	is.Equal(events[0].Data, []byte("hello"))
}

func TestEventStoreWithRegistry(t *testing.T) {
	is := testutil.NewIs(t)

	tests := []struct {
		Name string
		Run  func(t *testing.T, es *EventStore, subject string)
	}{
		{
			"append-load-no-occ",
			func(t *testing.T, es *EventStore, subject string) {
				ctx := context.Background()
				devent := OrderPlaced{ID: "123"}
				seq, err := es.Append(ctx, subject, []*Event{{
					Data: &devent,
				}})
				is.NoErr(err)
				is.Equal(seq, uint64(1))

				events, lseq, err := es.Load(ctx, subject)
				is.NoErr(err)

				is.Equal(seq, lseq)
				is.Equal(len(events), 1)

				is.True(events[0].ID != "")
				is.True(!events[0].Time.IsZero())
				is.Equal(events[0].Type, "order-placed")
				data, ok := events[0].Data.(*OrderPlaced)
				is.True(ok)
				is.Equal(*data, devent)
			},
		},
		{
			"append-load-with-occ",
			func(t *testing.T, es *EventStore, subject string) {
				ctx := context.Background()

				event1 := &Event{
					Data: &OrderPlaced{ID: "123"},
					Meta: map[string]string{
						"geo": "eu",
					},
				}

				seq, err := es.Append(ctx, subject, []*Event{event1}, ExpectSequence(0))
				is.NoErr(err)
				is.Equal(seq, uint64(1))

				event2 := &Event{
					Data: &OrderShipped{ID: "123"},
				}

				seq, err = es.Append(ctx, subject, []*Event{event2}, ExpectSequence(1))
				is.NoErr(err)
				is.Equal(seq, uint64(2))

				events, lseq, err := es.Load(ctx, subject)
				is.NoErr(err)

				is.Equal(seq, lseq)
				is.Equal(len(events), 2)
			},
		},
		{
			"append-load-partial",
			func(t *testing.T, es *EventStore, subject string) {
				ctx := context.Background()

				seq, err := es.Append(ctx, subject, []*Event{{Data: &OrderPlaced{ID: "123"}}}, ExpectSequence(0))
				is.NoErr(err)
				is.Equal(seq, uint64(1))

				seq, err = es.Append(ctx, subject, []*Event{{Data: &OrderShipped{ID: "123"}}}, ExpectSequence(1))
				is.NoErr(err)
				is.Equal(seq, uint64(2))

				events, lseq, err := es.Load(ctx, subject, AfterSequence(1))
				is.NoErr(err)

				is.Equal(seq, lseq)
				is.Equal(len(events), 1)
				is.Equal(events[0].Type, "order-shipped")
			},
		},
		{
			"duplicate-append",
			func(t *testing.T, es *EventStore, subject string) {
				ctx := context.Background()

				e := &Event{
					ID:   id.NUID.New(),
					Data: &OrderPlaced{ID: "123"},
				}

				seq, err := es.Append(ctx, subject, []*Event{e})
				is.NoErr(err)
				is.Equal(seq, uint64(1))

				// Append same event with same ID, expect the same response.
				seq, err = es.Append(ctx, subject, []*Event{e})
				is.NoErr(err)
				is.Equal(seq, uint64(1))

				// Append same event with same ID, expect the same response... again.
				seq, err = es.Append(ctx, subject, []*Event{e})
				is.NoErr(err)
				is.Equal(seq, uint64(1))
			},
		},
		{
			"state",
			func(t *testing.T, es *EventStore, _ string) {
				ctx := context.Background()

				events := []*Event{
					{Data: &OrderPlaced{ID: "1"}},
					{Data: &OrderPlaced{ID: "2"}},
					{Data: &OrderPlaced{ID: "3"}},
					{Data: &OrderShipped{ID: "2"}},
				}

				seq, err := es.Append(ctx, "orders.*", events)
				is.NoErr(err)
				is.Equal(seq, uint64(4))

				var stats OrderStats
				seq2, err := es.Evolve(ctx, "orders.*", &stats)
				is.NoErr(err)
				is.Equal(seq, seq2)

				is.Equal(stats.OrdersPlaced, 3)
				is.Equal(stats.OrdersShipped, 1)

				// New event to test out AfterSequence.
				e5 := &Event{Data: &OrderShipped{ID: "1"}}
				seq, err = es.Append(ctx, "orders.*", []*Event{e5})
				is.NoErr(err)
				is.Equal(seq, uint64(5))

				seq2, err = es.Evolve(ctx, "orders.*", &stats, AfterSequence(seq2))
				is.NoErr(err)
				is.Equal(seq, seq2)

				is.Equal(stats.OrdersPlaced, 3)
				is.Equal(stats.OrdersShipped, 2)
			},
		},
	}

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, _ := nats.Connect(srv.ClientURL())

	tr, err := types.NewRegistry(map[string]*types.Type{
		"order-placed": {
			Init: func() any { return &OrderPlaced{} },
		},
		"order-shipped": {
			Init: func() any { return &OrderShipped{} },
		},
	})
	is.NoErr(err)

	r, err := New(t.Context(), nc, TypeRegistry(tr))
	is.NoErr(err)

	for i, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			es := r.EventStore("orders")

			// Recreate the store for each test.
			_ = es.Delete()
			err := es.Create(&jetstream.StreamConfig{
				Storage: jetstream.MemoryStorage,
			})
			is.NoErr(err)

			subject := fmt.Sprintf("orders.%d", i)
			test.Run(t, es, subject)
		})
	}
}

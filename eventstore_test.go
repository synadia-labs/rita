package rita

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-labs/rita/codec"
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

var (
	_ Evolver      = (*OrderStats)(nil)
	_ Snapshotable = (*OrderStats)(nil)
)

type OrderStats struct {
	OrdersPlaced  int `json:"orders_placed"`
	OrdersShipped int `json:"orders_shipped"`

	codec.Codec `json:"-"`
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

func TestNewEventConstructor(t *testing.T) {
	is := testutil.NewIs(t)

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, _ := nats.Connect(srv.ClientURL())

	r, err := New(t.Context(), nc)
	is.NoErr(err)

	es, err := r.EventStore("orders")
	is.NoErr(err)
	err = es.Create(&jetstream.StreamConfig{
		Storage: jetstream.MemoryStorage,
	})
	is.NoErr(err)

	eventTime := time.Now()
	metadata := map[string]string{"foo": "bar"}

	// Test with a valid type.
	e, err := NewEvent(&OrderPlaced{ID: "123"},
		NewEventID("abc123"),
		NewEventType("placed-order"),
		NewEventTime(eventTime),
		NewEventMetadata(metadata),
	)
	is.NoErr(err)
	is.Equal(e.ID, "abc123")
	is.Equal(e.Type, "placed-order")
	is.Equal(e.Data.(*OrderPlaced).ID, "123")
	is.Equal(e.Time, eventTime)
	is.Equal(e.Meta["foo"], "bar")
}

func TestEventStoreSnapshot(t *testing.T) {
	is := testutil.NewIs(t)

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

	ctx := t.Context()

	es, err := r.EventStore("orders", WithSnapshotSettings("snapshots"))
	is.NoErr(err)
	err = es.Create(&jetstream.StreamConfig{
		Storage: jetstream.MemoryStorage,
	})
	is.NoErr(err)

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
	stats.Codec = codec.Default

	seq2, err := es.Evolve(ctx, "orders.*", &stats, WithSnapshot("orderstats"))
	is.NoErr(err)
	is.Equal(seq, seq2)

	is.Equal(stats.OrdersPlaced, 3)
	is.Equal(stats.OrdersShipped, 1)

	jsCtx, err := jetstream.New(nc)
	is.NoErr(err)

	jss, err := jsCtx.Stream(ctx, "orders")
	is.NoErr(err)

	err = jss.Purge(ctx)
	is.NoErr(err)

	events = []*Event{
		{Data: &OrderPlaced{ID: "4"}},
		{Data: &OrderShipped{ID: "1"}},
	}

	_, err = es.Append(ctx, "orders.*", events)
	is.NoErr(err)

	seq, err = es.Evolve(ctx, "orders.*", &stats)
	is.NoErr(err)
	is.Equal(stats.OrdersPlaced, 4)
	is.Equal(stats.OrdersShipped, 2)
	is.Equal(uint64(6), seq)

	var loadStats OrderStats
	loadStats.Codec = codec.Default

	seqLoad, err := es.Evolve(ctx, "orders.*", &loadStats, FromSnapshot("orderstats", 0))
	is.NoErr(err)
	is.Equal(loadStats.OrdersPlaced, 4)
	is.Equal(loadStats.OrdersShipped, 2)
	is.Equal(uint64(6), seqLoad)

	is.Equal(stats, loadStats)
}

func TestEventStoreNoRegistry(t *testing.T) {
	is := testutil.NewIs(t)

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, _ := nats.Connect(srv.ClientURL())

	r, err := New(t.Context(), nc)
	is.NoErr(err)

	es, err := r.EventStore("orders")
	is.NoErr(err)
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

				seq2, err = es.Evolve(ctx, "orders.*", &stats, LoadAfterSequence(seq2))
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
			es, err := r.EventStore("orders")
			is.NoErr(err)

			// Recreate the store for each test.
			_ = es.Delete()
			err = es.Create(&jetstream.StreamConfig{
				Storage: jetstream.MemoryStorage,
			})
			is.NoErr(err)

			subject := fmt.Sprintf("orders.%d", i)
			test.Run(t, es, subject)
		})
	}
}

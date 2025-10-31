package rita

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-labs/rita/testutil"
	"github.com/synadia-labs/rita/types"
)

type OrderPlaced struct{}

type OrderShipped struct{}

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

type eventSlice []*Event

func (es *eventSlice) Evolve(event *Event) error {
	*es = append(*es, event)
	return nil
}

func TestEventStoreNoRegistry(t *testing.T) {
	is := testutil.NewIs(t)

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	r, err := New(t.Context(), nc)
	is.NoErr(err)

	ctx := context.Background()
	es, err := r.EventStore(ctx, "store")
	is.NoErr(err)

	err = es.Create(ctx, &jetstream.StreamConfig{
		Storage: jetstream.MemoryStorage,
	})
	is.NoErr(err)

	seq, err := es.Append(ctx, []*Event{{
		Entity: "order.1",
		Type:   "foo",
		Data:   []byte("hello"),
	}})
	is.NoErr(err)
	is.Equal(seq, uint64(1))

	var events eventSlice

	_, err = es.Evolve(ctx, &events)
	is.NoErr(err)
	is.Equal(events[0].Type, "foo")
	is.Equal(events[0].Data, []byte("hello"))
}

func TestEventStoreWithRegistry(t *testing.T) {
	is := testutil.NewIs(t)

	tests := []struct {
		Name string
		Run  func(t *testing.T, es *EventStore)
	}{
		{
			"append",
			func(t *testing.T, es *EventStore) {
				ctx := context.Background()
				devent := OrderPlaced{}
				seq, err := es.Append(ctx, []*Event{{
					Entity: "order.1",
					Data:   &devent,
					Meta: map[string]string{
						"geo": "eu",
					},
				}})
				is.NoErr(err)
				is.Equal(seq, uint64(1))

				var events eventSlice
				lseq, err := es.Evolve(ctx, &events)
				is.NoErr(err)

				is.Equal(seq, lseq)
				is.Equal(len(events), 1)

				is.True(events[0].ID != "")
				is.True(!events[0].Time.IsZero())
				is.Equal(events[0].Type, "order-placed")
				is.Equal(events[0].Meta["geo"], "eu")
				data, ok := events[0].Data.(*OrderPlaced)
				is.True(ok)
				is.Equal(*data, devent)
			},
		},
		{
			"append-expect-sequence",
			func(t *testing.T, es *EventStore) {
				ctx := context.Background()

				seq, err := es.Append(ctx, []*Event{{
					Entity: "order.1",
					Data:   &OrderPlaced{},
				}}, ExpectSequence(0))
				is.NoErr(err)
				is.Equal(seq, uint64(1))

				seq, err = es.Append(ctx, []*Event{{
					Entity: "order.1",
					Data:   &OrderShipped{},
				}}, ExpectSequence(1))
				is.NoErr(err)
				is.Equal(seq, uint64(2))
			},
		},
		{
			"append-expect-sequence-subject",
			func(t *testing.T, es *EventStore) {
				ctx := context.Background()

				seq, err := es.Append(ctx, []*Event{{
					Entity: "order.1",
					Data:   &OrderPlaced{},
				}})
				is.NoErr(err)
				is.Equal(seq, uint64(1))

				seq, err = es.Append(ctx, []*Event{{
					Entity: "order.1",
					Data:   &OrderShipped{},
				}}, ExpectSequence(1)) // specific entity
				is.NoErr(err)
				is.Equal(seq, uint64(2))

				seq, err = es.Append(ctx, []*Event{{
					Entity: "order.2",
					Data:   &OrderPlaced{},
				}}, ExpectSequenceSubject(2, "order")) // for all orders
				is.NoErr(err)
				is.Equal(seq, uint64(3))

				seq, err = es.Append(ctx, []*Event{{
					Entity: "order.3",
					Data:   &OrderPlaced{},
				}}, ExpectSequenceSubject(0, "order.3")) // relative to event entity (default)
				is.NoErr(err)
				is.Equal(seq, uint64(4))

				seq, err = es.Append(ctx, []*Event{{
					Entity: "order.4",
					Data:   &OrderPlaced{},
				}}, ExpectSequenceSubject(4, "order.3")) // relative to a different entity
				is.NoErr(err)
				is.Equal(seq, uint64(5))

				seq, err = es.Append(ctx, []*Event{{
					Entity: "order.5",
					Data:   &OrderPlaced{},
				}}, ExpectSequenceSubject(2, "*.*.order-shipped")) // relative to a type
				is.NoErr(err)
				is.Equal(seq, uint64(6))
			},
		},
		/* TODO
		{
			"append-duplicate",
			func(t *testing.T, es *EventStore) {
				ctx := context.Background()

				e := &Event{
					ID:     id.NUID.New(),
					Entity: "order.1",
					Data:   &OrderPlaced{},
				}

				seq, err := es.Append(ctx, []*Event{e})
				is.NoErr(err)
				is.Equal(seq, uint64(1))

				// Append same event with same ID, expect the same response.
				seq, err = es.Append(ctx, []*Event{e})
				is.NoErr(err)
				is.Equal(seq, uint64(1))

				// Append same event with same ID, expect the same response... again.
				seq, err = es.Append(ctx, []*Event{e})
				is.NoErr(err)
				is.Equal(seq, uint64(1))
			},
		},
		*/
		{
			"evolve-after-sequence",
			func(t *testing.T, es *EventStore) {
				ctx := context.Background()

				events := []*Event{
					{Entity: "order.1", Data: &OrderPlaced{}},
					{Entity: "order.2", Data: &OrderPlaced{}},
					{Entity: "order.3", Data: &OrderPlaced{}},
					{Entity: "order.2", Data: &OrderShipped{}},
				}

				seq, err := es.Append(ctx, events)
				is.NoErr(err)
				is.Equal(seq, uint64(4))

				var stats OrderStats
				seq2, err := es.Evolve(ctx, &stats)
				is.NoErr(err)
				is.Equal(seq, seq2)

				is.Equal(stats.OrdersPlaced, 3)
				is.Equal(stats.OrdersShipped, 1)

				// New event to test out AfterSequence.
				e5 := &Event{Entity: "order.1", Data: &OrderShipped{}}
				seq, err = es.Append(ctx, []*Event{e5})
				is.NoErr(err)
				is.Equal(seq, uint64(5))

				seq2, err = es.Evolve(ctx, &stats, AfterSequence(seq2))
				is.NoErr(err)
				is.Equal(seq, seq2)

				is.Equal(stats.OrdersPlaced, 3)
				is.Equal(stats.OrdersShipped, 2)
			},
		},
		{
			"evolve-up-to-sequence",
			func(t *testing.T, es *EventStore) {
				ctx := context.Background()

				events := []*Event{
					{Entity: "order.1", Data: &OrderPlaced{}},
					{Entity: "order.2", Data: &OrderPlaced{}},
					{Entity: "order.3", Data: &OrderPlaced{}},
					{Entity: "order.2", Data: &OrderShipped{}},
				}

				_, err := es.Append(ctx, events)
				is.NoErr(err)

				var stats OrderStats
				seq, err := es.Evolve(ctx, &stats, UpToSequence(2))
				is.NoErr(err)
				is.Equal(seq, uint64(2))

				is.Equal(stats.OrdersPlaced, 2)
				is.Equal(stats.OrdersShipped, 0)

				var stats2 OrderStats
				seq, err = es.Evolve(ctx, &stats2, AfterSequence(1), UpToSequence(3))
				is.NoErr(err)
				is.Equal(seq, uint64(3))

				is.Equal(stats2.OrdersPlaced, 2)
				is.Equal(stats2.OrdersShipped, 0)
			},
		},
		{
			"evolve-patterns",
			func(t *testing.T, es *EventStore) {
				ctx := context.Background()

				events := []*Event{
					{Entity: "order.1", Data: &OrderPlaced{}},
					{Entity: "order.2", Data: &OrderPlaced{}},
					{Entity: "order.3", Data: &OrderPlaced{}},
					{Entity: "order.2", Data: &OrderShipped{}},
				}

				_, err := es.Append(ctx, events)
				is.NoErr(err)

				var stats OrderStats
				_, err = es.Evolve(ctx, &stats, Filters("*.*.order-shipped"))
				is.NoErr(err)

				is.Equal(stats.OrdersPlaced, 0)
				is.Equal(stats.OrdersShipped, 1)
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

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			ctx := context.Background()
			es, err := r.EventStore(ctx, "store")
			is.NoErr(err)

			// Recreate the store for each test.
			_ = es.Delete(ctx)
			err = es.Create(ctx, &jetstream.StreamConfig{
				Storage: jetstream.MemoryStorage,
			})
			is.NoErr(err)

			test.Run(t, es)
		})
	}
}

func TestParseSubjectPrefix(t *testing.T) {
	is := testutil.NewIs(t)

	prefix, err := parseSubjectPrefix("events.>")
	is.NoErr(err)
	is.Equal(prefix, "events")

	prefix, err = parseSubjectPrefix("events.*.*.*")
	is.NoErr(err)
	is.Equal(prefix, "events")

	_, err = parseSubjectPrefix("events")
	is.Err(err, nil)

	_, err = parseSubjectPrefix("events.>.*")
	is.Err(err, nil)

	_, err = parseSubjectPrefix("events.*.*")
	is.Err(err, nil)

	_, err = parseSubjectPrefix("events.*.*.*.*")
	is.Err(err, nil)
}

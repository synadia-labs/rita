package rita

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/synadia-labs/rita/id"
	"github.com/synadia-labs/rita/testutil"
	"github.com/synadia-labs/rita/types"
)

// waitFor polls fn until it returns true or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

var (
	registry = map[string]*types.Type{
		"place-order": {
			Init: func() any { return &PlaceOrder{} },
		},
		"order-placed": {
			Init: func() any { return &OrderPlaced{} },
		},
		"ship-order": {
			Init: func() any { return &ShipOrder{} },
		},
		"order-shipped": {
			Init: func() any { return &OrderShipped{} },
		},
		"cancel-order": {
			Init: func() any { return &CancelOrder{} },
		},
		"order-canceled": {
			Init: func() any { return &OrderCanceled{} },
		},
		"order-address-changed": {
			Init: func() any { return &OrderAddressChanged{} },
		},
	}
)

// Command and Event types for testing
type PlaceOrder struct{}

type CancelOrder struct{}

type ShipOrder struct{}

type OrderPlaced struct{}

type OrderCanceled struct{}

type OrderShipped struct{}

type OrderAddressChanged struct {
	Address string
}

// OrderStats is an example state that can evolve based on events.
type OrderStats struct {
	Placed   int
	Canceled int
	Shipped  int
}

func (s *OrderStats) Decide(ctx context.Context, cmd *Command) ([]*Event, error) {
	switch cmd.Data.(type) {
	case *PlaceOrder:
		return []*Event{
			{Entity: "store.1", Data: &OrderPlaced{}},
		}, nil

	case *CancelOrder:
		return []*Event{
			{Entity: "store.1", Data: &OrderCanceled{}},
		}, nil

	case *ShipOrder:
		return []*Event{
			{Entity: "store.1", Data: &OrderShipped{}},
		}, nil
	}
	return nil, nil
}

func (s *OrderStats) Evolve(ctx context.Context, event *Event) error {
	switch event.Data.(type) {
	case *OrderPlaced:
		s.Placed++
	case *OrderCanceled:
		s.Canceled++
	case *OrderShipped:
		s.Shipped++
	}
	return nil
}

type eventSlice []*Event

func (es *eventSlice) Evolve(ctx context.Context, event *Event) error {
	*es = append(*es, event)
	return nil
}

func TestEventStoreNoRegistry(t *testing.T) {
	is := testutil.NewIs(t)

	srv := testutil.NewNatsServer(t)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	m, err := New(nc)
	is.NoErr(err)

	ctx := context.Background()
	es, err := m.CreateEventStore(ctx, EventStoreConfig{
		Name: "store",
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

// binValue exercises the encoding.BinaryMarshaler path of the binary codec.
type binValue struct{ b []byte }

func (v binValue) MarshalBinary() ([]byte, error) { return v.b, nil }

// TestNoRegistryWireCompat pins the no-registry wire format: bodies are the
// caller's raw bytes (or MarshalBinary output), the codec header says
// "binary", and the caller owns the type name. Any change to the
// degenerate-registry path must keep these bytes identical or stored events
// become unreadable across versions.
func TestNoRegistryWireCompat(t *testing.T) {
	is := testutil.NewIs(t)

	srv := testutil.NewNatsServer(t)
	t.Cleanup(func() { testutil.ShutdownNatsServer(srv) })

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)
	t.Cleanup(nc.Close)

	m, err := New(nc)
	is.NoErr(err)

	ctx := context.Background()
	es, err := m.CreateEventStore(ctx, EventStoreConfig{Name: "store"})
	is.NoErr(err)

	// Type is caller-owned with no registry: absent means error, not lookup.
	_, err = es.Append(ctx, []*Event{{Entity: "order.1", Data: []byte("x")}})
	is.Err(err, ErrEventTypeRequired)

	// Nor may the type carry subject syntax — it becomes the subject's final
	// token, so a wildcard or separator would leak into the published subject.
	_, err = es.Append(ctx, []*Event{{Entity: "order.1", Type: "order.*", Data: []byte("x")}})
	is.Err(err, ErrEventTypeInvalid)

	seq, err := es.Append(ctx, []*Event{{Entity: "order.1", Type: "order-placed", Data: []byte("payload")}})
	is.NoErr(err)

	str, err := es.js.Stream(ctx, es.stream)
	is.NoErr(err)
	raw, err := str.GetMsg(ctx, seq)
	is.NoErr(err)

	is.Equal(raw.Subject, "$ES.store.order.1.order-placed")
	is.Equal(raw.Header.Get(eventCodecHdr), "binary")
	is.Equal(raw.Header.Get(eventTypeHdr), "order-placed")
	is.Equal(raw.Header.Get(eventEntityHdr), "order.1")
	is.Equal(raw.Data, []byte("payload"))

	// Values implementing encoding.BinaryMarshaler serialize via MarshalBinary.
	seq, err = es.Append(ctx, []*Event{{Entity: "order.1", Type: "order-shipped", Data: binValue{b: []byte{0x01, 0x02}}}})
	is.NoErr(err)
	raw, err = str.GetMsg(ctx, seq)
	is.NoErr(err)
	is.Equal(raw.Header.Get(eventCodecHdr), "binary")
	is.Equal(raw.Data, []byte{0x01, 0x02})

	// Consumed events surface Data as []byte — the shape callers assert on.
	var events eventSlice
	_, err = es.Evolve(ctx, &events)
	is.NoErr(err)
	is.Equal(len(events), 2)
	b, ok := events[0].Data.([]byte)
	is.True(ok)
	is.Equal(b, []byte("payload"))
}

func TestWithAPIPrefix(t *testing.T) {
	is := testutil.NewIs(t)

	srv := testutil.NewNatsServerWithDomain(t, "test")
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	m, err := New(nc, WithAPIPrefix("$JS.test.API"))
	is.NoErr(err)

	ctx := context.Background()
	es, err := m.CreateEventStore(ctx, EventStoreConfig{
		Name: "store",
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
					Expect: ExpectSequence(0),
				}})
				is.NoErr(err)
				is.Equal(seq, uint64(1))

				seq, err = es.Append(ctx, []*Event{{
					Entity: "order.1",
					Data:   &OrderShipped{},
					Expect: ExpectSequence(1),
				}})
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
					Expect: ExpectSequence(1),
				}})
				is.NoErr(err)
				is.Equal(seq, uint64(2))

				seq, err = es.Append(ctx, []*Event{{
					Entity: "order.2",
					Data:   &OrderPlaced{},
					Expect: ExpectSequenceSubject(2, "order"), // relative to entity type
				}})
				is.NoErr(err)
				is.Equal(seq, uint64(3))

				seq, err = es.Append(ctx, []*Event{{
					Entity: "order.3",
					Data:   &OrderPlaced{},
					Expect: ExpectSequenceSubject(0, "order.3"), // relative to event entity (default)
				}})
				is.NoErr(err)
				is.Equal(seq, uint64(4))

				seq, err = es.Append(ctx, []*Event{{
					Entity: "order.4",
					Data:   &OrderPlaced{},
					Expect: ExpectSequenceSubject(4, "order.3"), // relative to a different entity
				}})
				is.NoErr(err)
				is.Equal(seq, uint64(5))

				seq, err = es.Append(ctx, []*Event{{
					Entity: "order.5",
					Data:   &OrderPlaced{},
					Expect: ExpectSequenceSubject(2, "*.*.order-shipped"), // relative to an event type
				}})
				is.NoErr(err)
				is.Equal(seq, uint64(6))
			},
		},
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

				is.Equal(stats.Placed, 3)
				is.Equal(stats.Shipped, 1)

				// New event to test out AfterSequence.
				e5 := &Event{Entity: "order.1", Data: &OrderShipped{}}
				seq, err = es.Append(ctx, []*Event{e5})
				is.NoErr(err)
				is.Equal(seq, uint64(5))

				seq2, err = es.Evolve(ctx, &stats, WithAfterSequence(seq2))
				is.NoErr(err)
				is.Equal(seq, seq2)

				is.Equal(stats.Placed, 3)
				is.Equal(stats.Shipped, 2)
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
				seq, err := es.Evolve(ctx, &stats, WithStopSequence(2))
				is.NoErr(err)
				is.Equal(seq, uint64(2))

				is.Equal(stats.Placed, 2)
				is.Equal(stats.Shipped, 0)

				var stats2 OrderStats
				seq, err = es.Evolve(ctx, &stats2, WithAfterSequence(1), WithStopSequence(3))
				is.NoErr(err)
				is.Equal(seq, uint64(3))

				is.Equal(stats2.Placed, 2)
				is.Equal(stats2.Shipped, 0)
			},
		},
		{
			"evolve-filters",
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
				_, err = es.Evolve(ctx, &stats, WithFilters("*.*.order-shipped"))
				is.NoErr(err)

				is.Equal(stats.Placed, 0)
				is.Equal(stats.Shipped, 1)
			},
		},
		{
			// Multiple filters map to multiple consumer filter subjects.
			"evolve-multi-filters",
			func(t *testing.T, es *EventStore) {
				ctx := context.Background()

				events := []*Event{
					{Entity: "order.1", Data: &OrderPlaced{}},
					{Entity: "order.2", Data: &OrderPlaced{}},
					{Entity: "order.2", Data: &OrderShipped{}},
					{Entity: "order.3", Data: &OrderCanceled{}},
				}

				_, err := es.Append(ctx, events)
				is.NoErr(err)

				var stats OrderStats
				_, err = es.Evolve(ctx, &stats, WithFilters("*.*.order-shipped", "*.*.order-canceled"))
				is.NoErr(err)

				is.Equal(stats.Placed, 0)
				is.Equal(stats.Shipped, 1)
				is.Equal(stats.Canceled, 1)
			},
		},
	}

	m, _ := newTestManager(t)

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			ctx := context.Background()
			// Use a per-subtest stream name so the JetStream filestore cleanup
			// from one subtest's deferred delete can't race the next subtest's
			// create on slow runners.
			storeName := test.Name
			es, err := m.CreateEventStore(ctx, EventStoreConfig{
				Name: storeName,
			})
			is.NoErr(err)
			defer func() {
				_ = m.DeleteEventStore(ctx, storeName)
			}()

			test.Run(t, es)
		})
	}
}

func TestEventStoreDecide(t *testing.T) {
	is := testutil.NewIs(t)

	es := newTestStore(t)
	ctx := context.Background()

	cmd := &Command{
		Data: &PlaceOrder{},
	}

	state := &OrderStats{}

	_, seq, err := es.Decide(ctx, state, cmd)
	is.NoErr(err)
	is.Equal(seq, uint64(1))

	var events eventSlice
	_, err = es.Evolve(ctx, &events)
	is.NoErr(err)
	is.Equal(len(events), 1)
	is.Equal(events[0].Type, "order-placed")
}

func TestEventStoreDecideAndEvolve(t *testing.T) {
	is := testutil.NewIs(t)

	es := newTestStore(t)
	ctx := context.Background()

	state := &OrderStats{}
	model := NewModel(state)

	// First command: place an order
	events, seq, err := es.DecideAndEvolve(ctx, model, &Command{
		Data: &PlaceOrder{},
	})
	is.NoErr(err)
	is.Equal(seq, uint64(1))
	is.Equal(len(events), 1)
	is.Equal(state.Placed, 1)
	is.Equal(state.Shipped, 0)
	is.Equal(state.Canceled, 0)

	// Second command: ship an order
	events, seq, err = es.DecideAndEvolve(ctx, model, &Command{
		Data: &ShipOrder{},
	})
	is.NoErr(err)
	is.Equal(seq, uint64(2))
	is.Equal(len(events), 1)
	is.Equal(state.Placed, 1)
	is.Equal(state.Shipped, 1)
	is.Equal(state.Canceled, 0)

	// Third command: cancel an order
	events, seq, err = es.DecideAndEvolve(ctx, model, &Command{
		Data: &CancelOrder{},
	})
	is.NoErr(err)
	is.Equal(seq, uint64(3))
	is.Equal(len(events), 1)
	is.Equal(state.Placed, 1)
	is.Equal(state.Shipped, 1)
	is.Equal(state.Canceled, 1)

	// Verify events are also persisted by evolving a fresh state
	var freshState OrderStats
	lastSeq, err := es.Evolve(ctx, &freshState)
	is.NoErr(err)
	is.Equal(lastSeq, uint64(3))
	is.Equal(freshState.Placed, 1)
	is.Equal(freshState.Shipped, 1)
	is.Equal(freshState.Canceled, 1)
}

func TestModelWatcher(t *testing.T) {
	is := testutil.NewIs(t)

	es := newTestStore(t)
	ctx := context.Background()

	// Create a model for OrderStats.
	m := NewModel(&OrderStats{})

	w, err := es.Watch(ctx, m)
	is.NoErr(err)
	defer w.Stop()

	// Decide a command to place an order.
	_, _, err = es.Decide(ctx, m, &Command{
		Data: &PlaceOrder{},
	})
	is.NoErr(err)

	// Wait for the watcher to evolve the model.
	waitFor(t, 2*time.Second, func() bool {
		var placed int
		_ = m.View(ctx, func(s *OrderStats) error {
			placed = s.Placed
			return nil
		})
		return placed == 1
	})

	// Decide a command to ship an order.
	_, _, err = es.Decide(ctx, m, &Command{
		Data: &ShipOrder{},
	})
	is.NoErr(err)

	// Wait for the watcher to evolve the model.
	waitFor(t, 2*time.Second, func() bool {
		var shipped int
		_ = m.View(ctx, func(s *OrderStats) error {
			shipped = s.Shipped
			return nil
		})
		return shipped == 1
	})
}

// batchOrderModel returns 3 OrderPlaced events for the same entity in one Decide call.
type batchOrderModel struct {
	Placed int
}

func (m *batchOrderModel) Decide(ctx context.Context, cmd *Command) ([]*Event, error) {
	return []*Event{
		{Entity: "store.1", Data: &OrderPlaced{}},
		{Entity: "store.1", Data: &OrderPlaced{}},
		{Entity: "store.1", Data: &OrderPlaced{}},
	}, nil
}

func (m *batchOrderModel) Evolve(ctx context.Context, event *Event) error {
	switch event.Data.(type) {
	case *OrderPlaced:
		m.Placed++
	}
	return nil
}

func TestDecideAndEvolveBatchSequences(t *testing.T) {
	is := testutil.NewIs(t)

	es := newTestStore(t)
	ctx := context.Background()

	state := &batchOrderModel{}
	model := NewModel(state)

	events, seq, err := es.DecideAndEvolve(ctx, model, &Command{
		Data: &PlaceOrder{},
	})
	is.NoErr(err)
	is.Equal(seq, uint64(3))
	is.Equal(len(events), 3)

	// Each event must have a distinct, incrementing sequence.
	is.Equal(events[0].sequence, uint64(1))
	is.Equal(events[1].sequence, uint64(2))
	is.Equal(events[2].sequence, uint64(3))

	// All 3 events must have been evolved into the state.
	is.Equal(state.Placed, 3)
}

type mixedEntitiesModel struct{}

func (m *mixedEntitiesModel) Decide(ctx context.Context, cmd *Command) ([]*Event, error) {
	return []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
		{Entity: "order.2", Data: &OrderPlaced{}},
		{Entity: "order.2", Data: &OrderPlaced{}},
	}, nil
}

func (m *mixedEntitiesModel) Evolve(ctx context.Context, event *Event) error {
	return nil
}

func TestMixedEntities(t *testing.T) {
	is := testutil.NewIs(t)

	es := newTestStore(t)
	ctx := context.Background()

	m := NewModel(&mixedEntitiesModel{})
	w, err := es.Watch(ctx, m)
	is.NoErr(err)
	defer w.Stop()

	events, seq, err := es.Decide(ctx, m, nil)
	is.NoErr(err)
	is.Equal(seq, uint64(3))
	is.Equal(len(events), 3)

	// Wait for the watcher to evolve all events before the next Decide
	// so that auto-Expect reflects the correct sequences.
	waitFor(t, 2*time.Second, func() bool {
		testEvents, _ := m.Decide(ctx, nil)
		for _, e := range testEvents {
			if e.Entity == "order.2" && e.Expect != nil && e.Expect.Sequence == 3 {
				return true
			}
		}
		return false
	})

	events, seq, err = es.Decide(ctx, m, nil)
	is.NoErr(err)
	is.Equal(seq, uint64(6))
	is.Equal(len(events), 3)
}

func TestFiltersToSubjects(t *testing.T) {
	es := storeHandle("demo")

	tests := []struct {
		name    string
		filters []string
		want    []string
		wantErr error
	}{
		{
			name:    "nil filters",
			filters: nil,
			want:    []string{},
		},
		{
			name:    "empty pattern expands to triple wildcard",
			filters: []string{""},
			want:    []string{"$ES.demo.*.*.*"},
		},
		{
			name:    "single token pads with wildcards",
			filters: []string{"order"},
			want:    []string{"$ES.demo.order.*.*"},
		},
		{
			name:    "two tokens pad once",
			filters: []string{"order.1"},
			want:    []string{"$ES.demo.order.1.*"},
		},
		{
			name:    "three tokens kept verbatim",
			filters: []string{"order.1.order-placed"},
			want:    []string{"$ES.demo.order.1.order-placed"},
		},
		{
			name:    "wildcards preserved",
			filters: []string{"*.*.order-shipped"},
			want:    []string{"$ES.demo.*.*.order-shipped"},
		},
		{
			name:    "multiple filters",
			filters: []string{"order", "*.*.order-shipped"},
			want:    []string{"$ES.demo.order.*.*", "$ES.demo.*.*.order-shipped"},
		},
		{
			name:    "too many tokens errors",
			filters: []string{"a.b.c.d"},
			wantErr: ErrSubjectTooManyTokens,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := testutil.NewIs(t)
			got, err := es.filtersToSubjects(tc.filters)
			if tc.wantErr != nil {
				is.Err(err, tc.wantErr)
				return
			}
			is.NoErr(err)
			is.Equal(got, tc.want)
		})
	}
}

func TestSubjectsToFilters(t *testing.T) {
	es := storeHandle("demo")

	tests := []struct {
		name     string
		subjects []string
		want     []string
	}{
		{
			name:     "nil subjects",
			subjects: nil,
			want:     []string{},
		},
		{
			name:     "prefix stripped",
			subjects: []string{"$ES.demo.*.*.order-shipped"},
			want:     []string{"*.*.order-shipped"},
		},
		{
			name:     "non-prefixed subject surfaced verbatim",
			subjects: []string{"other.stream.subject"},
			want:     []string{"other.stream.subject"},
		},
		{
			name: "mixed prefixed and non-prefixed",
			subjects: []string{
				"$ES.demo.order.1.order-placed",
				"foreign.subject",
				"$ES.demo.*.*.order-shipped",
			},
			want: []string{
				"order.1.order-placed",
				"foreign.subject",
				"*.*.order-shipped",
			},
		},
		{
			name:     "different store name does not match",
			subjects: []string{"$ES.other.foo.bar.baz"},
			want:     []string{"$ES.other.foo.bar.baz"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := testutil.NewIs(t)
			got := es.subjectsToFilters(tc.subjects)
			is.Equal(got, tc.want)
		})
	}
}

// TestAppendStampsSequences pins that Append records each event's assigned
// stream sequence on the event itself, on both publish paths. Callers that
// publish through bare Append or Decide need the per-event sequences (e.g. to
// resume with WithAfterSequence); previously only DecideAndEvolve derived
// them, restating the batch-contiguity assumption away from the publish.
func TestAppendStampsSequences(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	single := &Event{Entity: "order.1", Data: &OrderPlaced{}}
	seq, err := es.Append(ctx, []*Event{single})
	is.NoErr(err)
	is.Equal(seq, uint64(1))
	is.Equal(single.Sequence(), uint64(1))

	batch := []*Event{
		{Entity: "order.1", Data: &OrderShipped{}},
		{Entity: "order.2", Data: &OrderPlaced{}},
	}
	seq, err = es.Append(ctx, batch)
	is.NoErr(err)
	is.Equal(seq, uint64(3))
	is.Equal(batch[0].Sequence(), uint64(2))
	is.Equal(batch[1].Sequence(), uint64(3))
}

// TestAppendSequenceConflict pins that a stale Expect surfaces as
// ErrSequenceConflict on BOTH publish paths. Callers retry on
// errors.Is(err, ErrSequenceConflict), so the JetStream error must be mapped
// on the single-publish path (structured APIError) and the batch path
// (previously a brittle message-text match) alike.
func TestAppendSequenceConflict(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	// Seed the entity's subject so an ExpectSequence(0) below is stale.
	_, err := es.Append(ctx, []*Event{{Entity: "order.1", Data: &OrderPlaced{}}})
	is.NoErr(err)

	// Single-event append takes the PublishMsg path.
	_, err = es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderShipped{}, Expect: ExpectSequence(0)},
	})
	is.Err(err, ErrSequenceConflict)

	// Multi-event append takes the atomic batch path.
	_, err = es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderShipped{}, Expect: ExpectSequence(0)},
		{Entity: "order.1", Data: &OrderShipped{}},
	})
	is.Err(err, ErrSequenceConflict)
}

// TestEntityValidation pins that an entity is exactly two subject tokens and
// that neither token may smuggle in a wildcard or whitespace. This matters
// because the entity is interpolated straight into the published subject: a '*'
// or '>' would turn a concrete subject into a wildcard, and whitespace is an
// invalid token NATS would reject downstream with a far less obvious error.
func TestEntityValidation(t *testing.T) {
	is := testutil.NewIs(t)

	for _, good := range []string{"order.1", "order-type.abc123", "a.b"} {
		is.True(validEntity(good))
	}

	for _, bad := range []string{
		"order",     // one token
		"a.b.c",     // three tokens
		"order.",    // empty id
		".1",        // empty type
		"order.*",   // wildcard token
		"order.>",   // wildcard token
		"order *.1", // whitespace
		"order.\t1", // whitespace
	} {
		is.True(!validEntity(bad))
	}
}

// TestTypeValidation pins that the event type must be exactly one subject
// token: it becomes the published subject's final token, so a separator,
// wildcard, or whitespace would inject subject tokens, hide the event from
// three-token filter patterns, and misalign the default Expect pattern.
func TestTypeValidation(t *testing.T) {
	is := testutil.NewIs(t)

	for _, good := range []string{"order-placed", "OrderPlaced", "v2_order"} {
		is.True(validType(good))
	}

	for _, bad := range []string{
		"",              // empty
		"order.placed",  // separator: injects a subject token
		"order*",        // wildcard
		">",             // wildcard
		"order placed",  // whitespace
		"order\tplaced", // whitespace
	} {
		is.True(!validType(bad))
	}
}

// TestDottedRegistryTypeRejected pins the conflict decision between the type
// registry's name rules (dots allowed) and the subject grammar (the type is
// one token): a dotted registered type is rejected at append rather than
// published on a subject that three-token filters and Expect patterns cannot
// address.
func TestDottedRegistryTypeRejected(t *testing.T) {
	is := testutil.NewIs(t)

	reg, err := types.NewRegistry(map[string]*types.Type{
		"com.example.order-placed": {Init: func() any { return &OrderPlaced{} }},
	})
	is.NoErr(err)

	es := storeHandle("demo")
	es.types = registryTypes{r: reg}

	_, err = es.wrapEvent(&Event{Entity: "order.1", Data: &OrderPlaced{}})
	is.Err(err, ErrEventTypeInvalid)
}

// TestUpdateMergesCustomMetadata pins that an update preserves custom metadata
// keys set by an earlier create even when it does not re-supply them. JetStream
// replaces a stream's metadata wholesale, so without the merge a caller would
// have to echo every prior key on every update or silently lose it.
func TestUpdateMergesCustomMetadata(t *testing.T) {
	is := testutil.NewIs(t)
	m, ctx := newTestManager(t)

	_, err := m.CreateEventStore(ctx, EventStoreConfig{
		Name:     "meta",
		Metadata: map[string]string{"team": "platform", "tier": "gold"},
	})
	is.NoErr(err)

	// Update touches a different field and re-supplies only one key (changed),
	// plus a new key. The omitted "tier" must survive.
	err = m.UpdateEventStore(ctx, EventStoreConfig{
		Name:        "meta",
		Description: "updated",
		Metadata:    map[string]string{"team": "infra", "region": "us-east"},
	})
	is.NoErr(err)

	str, err := m.js.Stream(ctx, "ES_meta")
	is.NoErr(err)
	md := str.CachedInfo().Config.Metadata
	is.Equal(md["team"], "infra")     // re-supplied: overridden
	is.Equal(md["tier"], "gold")      // omitted: preserved
	is.Equal(md["region"], "us-east") // new: added
}

type flakyCounter struct {
	Count     int
	failOnce  uint64
	attempted map[uint64]int
}

func (c *flakyCounter) Evolve(_ context.Context, event *Event) error {
	c.attempted[event.Sequence()]++
	if event.Sequence() == c.failOnce && c.attempted[event.Sequence()] == 1 {
		return errEvolverRejected
	}
	c.Count++
	return nil
}

func TestEvolveReplayReappliesFailedEvent(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	_, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
		{Entity: "order.1", Data: &OrderShipped{}},
	})
	is.NoErr(err)

	state := &flakyCounter{failOnce: 2, attempted: map[uint64]int{}}
	model := NewModel(state)

	_, err = es.Evolve(ctx, model)
	is.Err(err, errEvolverRejected)

	seq, err := es.Evolve(ctx, model, WithAfterSequence(0))
	is.NoErr(err)
	is.Equal(seq, uint64(2))
	is.Equal(state.attempted[2], 2)
	is.Equal(state.Count, 2)
}

var (
	errUnknownOrderEvent  = errors.New("unknown order event")
	errOrderCanceled      = errors.New("order was canceled")
	errAddressServiceDown = errors.New("address service unavailable")
)

func releaseOneTypes() map[string]*types.Type {
	reg := map[string]*types.Type{}
	for name, typ := range registry {
		if name != "order-canceled" {
			reg[name] = typ
		}
	}
	return reg
}

type OrderReleaseOne struct {
	Address string
}

func (o *OrderReleaseOne) Evolve(ctx context.Context, event *Event) error {
	switch e := event.Data.(type) {
	case *OrderPlaced, *OrderShipped:
	case *OrderAddressChanged:
		o.Address = e.Address
	default:
		return fmt.Errorf("%w: %T", errUnknownOrderEvent, event.Data)
	}
	return nil
}

func (o *OrderReleaseOne) Decide(ctx context.Context, cmd *Command) ([]*Event, error) {
	return []*Event{{Entity: "order.1", Data: &OrderShipped{}}}, nil
}

type OrderReleaseTwo struct {
	OrderReleaseOne
	Canceled bool
}

func (o *OrderReleaseTwo) Evolve(ctx context.Context, event *Event) error {
	if _, ok := event.Data.(*OrderCanceled); ok {
		o.Canceled = true
		return nil
	}
	return o.OrderReleaseOne.Evolve(ctx, event)
}

func (o *OrderReleaseTwo) Decide(ctx context.Context, cmd *Command) ([]*Event, error) {
	if o.Canceled {
		return nil, errOrderCanceled
	}
	return o.OrderReleaseOne.Decide(ctx, cmd)
}

type OrderWithFlakyEvolve struct {
	OrderReleaseTwo
	failNext bool
}

func (o *OrderWithFlakyEvolve) Evolve(ctx context.Context, event *Event) error {
	if o.failNext {
		o.failNext = false
		return errAddressServiceDown
	}
	return o.OrderReleaseTwo.Evolve(ctx, event)
}

type orderDeploy struct {
	t  *testing.T
	nc *nats.Conn
}

func newCanceledOrderDeploy(t *testing.T) *orderDeploy {
	t.Helper()
	srv := testutil.NewNatsServer(t)
	t.Cleanup(func() { testutil.ShutdownNatsServer(srv) })

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	d := &orderDeploy{t: t, nc: nc}
	m, err := New(nc, WithRegistry(d.registry(registry)))
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	es, err := m.CreateEventStore(context.Background(), EventStoreConfig{Name: "orders"})
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	_, err = es.Append(context.Background(), []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
		{Entity: "order.1", Data: &OrderCanceled{}},
		{Entity: "order.1", Data: &OrderAddressChanged{Address: "2 New St"}},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	return d
}

func (d *orderDeploy) registry(reg map[string]*types.Type) *types.Registry {
	d.t.Helper()
	tr, err := types.NewRegistry(reg)
	if err != nil {
		d.t.Fatalf("registry: %v", err)
	}
	return tr
}

func (d *orderDeploy) store(reg map[string]*types.Type) *EventStore {
	d.t.Helper()
	m, err := New(d.nc, WithRegistry(d.registry(reg)))
	if err != nil {
		d.t.Fatalf("manager: %v", err)
	}
	es, err := m.GetEventStore(context.Background(), "orders")
	if err != nil {
		d.t.Fatalf("get store: %v", err)
	}
	return es
}

func watchModel(t *testing.T, es *EventStore, model Evolver) {
	t.Helper()
	w, err := es.Watch(context.Background(), model)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	t.Cleanup(w.Stop)
}

func TestFailedEvolve_WatcherCannotDecodeNewEventType_RefusesDecide(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	deploy := newCanceledOrderDeploy(t)

	releaseOne := deploy.store(releaseOneTypes())
	stale := NewModel(&OrderReleaseOne{})
	watchModel(t, releaseOne, stale)

	is.Equal(stale.lseq["order.1"], uint64(3))
	is.Err(stale.EvolveErr(), types.ErrTypeNotRegistered)

	_, _, err := releaseOne.Decide(ctx, stale, &Command{Data: &ShipOrder{}})
	is.Err(err, ErrEvolveFailed)
	is.Err(err, types.ErrTypeNotRegistered)

	rebuiltOnReleaseOne := NewModel(&OrderReleaseOne{})
	watchModel(t, releaseOne, rebuiltOnReleaseOne)
	is.Err(rebuiltOnReleaseOne.EvolveErr(), types.ErrTypeNotRegistered)

	releaseTwo := deploy.store(registry)
	rebuiltOnReleaseTwo := NewModel(&OrderReleaseTwo{})
	watchModel(t, releaseTwo, rebuiltOnReleaseTwo)
	is.NoErr(rebuiltOnReleaseTwo.EvolveErr())

	_, _, err = releaseTwo.Decide(ctx, rebuiltOnReleaseTwo, &Command{Data: &ShipOrder{}})
	is.Err(err, errOrderCanceled)
}

func TestFailedEvolve_ReplayCannotDecodeNewEventType_RefusesDecide(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	deploy := newCanceledOrderDeploy(t)

	releaseOne := deploy.store(releaseOneTypes())
	model := NewModel(&OrderReleaseOne{})
	_, err := releaseOne.Evolve(ctx, model)
	is.Err(err, types.ErrTypeNotRegistered)

	_, _, err = releaseOne.Decide(ctx, model, &Command{Data: &ShipOrder{}})
	is.Err(err, ErrEvolveFailed)
	is.Err(err, types.ErrTypeNotRegistered)
}

func TestFailedEvolve_TemporaryEvolverError_RebuildRecovers(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	deploy := newCanceledOrderDeploy(t)
	releaseTwo := deploy.store(registry)

	flaky := NewModel(&OrderWithFlakyEvolve{failNext: true})
	_, err := releaseTwo.Evolve(ctx, flaky)
	is.Err(err, errAddressServiceDown)
	is.Err(flaky.EvolveErr(), errAddressServiceDown)

	rebuilt := NewModel(&OrderWithFlakyEvolve{})
	_, err = releaseTwo.Evolve(ctx, rebuilt)
	is.NoErr(err)
	is.NoErr(rebuilt.EvolveErr())

	_, _, err = releaseTwo.Decide(ctx, rebuilt, &Command{Data: &ShipOrder{}})
	is.Err(err, errOrderCanceled)
}

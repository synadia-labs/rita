package rita

import (
	"context"
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
	}
)

// Command and Event types for testing
type PlaceOrder struct{}

type CancelOrder struct{}

type ShipOrder struct{}

type OrderPlaced struct{}

type OrderCanceled struct{}

type OrderShipped struct{}

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
	}

	srv := testutil.NewNatsServer(t)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	tr, err := types.NewRegistry(registry)
	is.NoErr(err)

	m, err := New(nc, WithRegistry(tr))
	is.NoErr(err)

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

	srv := testutil.NewNatsServer(t)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	tr, err := types.NewRegistry(registry)
	is.NoErr(err)

	m, err := New(nc, WithRegistry(tr))
	is.NoErr(err)

	ctx := context.Background()
	es, err := m.CreateEventStore(ctx, EventStoreConfig{
		Name: "store",
	})
	is.NoErr(err)

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

	srv := testutil.NewNatsServer(t)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	tr, err := types.NewRegistry(registry)
	is.NoErr(err)

	m, err := New(nc, WithRegistry(tr))
	is.NoErr(err)

	ctx := context.Background()
	es, err := m.CreateEventStore(ctx, EventStoreConfig{
		Name: "store",
	})
	is.NoErr(err)

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

	srv := testutil.NewNatsServer(t)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	tr, err := types.NewRegistry(registry)
	is.NoErr(err)

	mgr, err := New(nc, WithRegistry(tr))
	is.NoErr(err)

	ctx := context.Background()
	es, err := mgr.CreateEventStore(ctx, EventStoreConfig{
		Name: "store",
	})
	is.NoErr(err)

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

	srv := testutil.NewNatsServer(t)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	tr, err := types.NewRegistry(registry)
	is.NoErr(err)

	mgr, err := New(nc, WithRegistry(tr))
	is.NoErr(err)

	ctx := context.Background()
	es, err := mgr.CreateEventStore(ctx, EventStoreConfig{
		Name: "store",
	})
	is.NoErr(err)

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

	srv := testutil.NewNatsServer(t)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	tr, err := types.NewRegistry(registry)
	is.NoErr(err)

	mgr, err := New(nc, WithRegistry(tr))
	is.NoErr(err)

	ctx := context.Background()
	es, err := mgr.CreateEventStore(ctx, EventStoreConfig{
		Name: "store",
	})
	is.NoErr(err)

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
	es := &EventStore{name: "demo"}

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
	es := &EventStore{name: "demo"}

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

// TestEntityValidation pins that an entity is exactly two subject tokens and
// that neither token may smuggle in a wildcard or whitespace. This matters
// because the entity is interpolated straight into the published subject: a '*'
// or '>' would turn a concrete subject into a wildcard, and whitespace is an
// invalid token NATS would reject downstream with a far less obvious error.
func TestEntityValidation(t *testing.T) {
	is := testutil.NewIs(t)

	for _, good := range []string{"order.1", "order-type.abc123", "a.b"} {
		is.True(entityRegex.MatchString(good))
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
		is.True(!entityRegex.MatchString(bad))
	}
}

// TestUpdateMergesCustomMetadata pins that an update preserves custom metadata
// keys set by an earlier create even when it does not re-supply them. JetStream
// replaces a stream's metadata wholesale, so without the merge a caller would
// have to echo every prior key on every update or silently lose it.
func TestUpdateMergesCustomMetadata(t *testing.T) {
	is := testutil.NewIs(t)
	m, ctx := tenantTestManager(t)

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

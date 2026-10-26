package rita

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-labs/rita/codec"
	"github.com/synadia-labs/rita/testutil"
)

type namelessCodec struct{ codec.Codec }

func (namelessCodec) Name() string { return "" }

func TestSnapshotLoadModel_RejectsInvalidConfig(t *testing.T) {
	es := newTestStore(t)
	ctx := context.Background()
	valid := SnapshotConfig{Name: "stats", Version: "1", Codec: codec.JSON}

	t.Run("non-pointer state", func(t *testing.T) {
		_, err := LoadModel(ctx, es, OrderStats{}, valid)
		testutil.NewIs(t).Err(err, ErrSnapshotConfigInvalid)
	})

	t.Run("nil pointer state", func(t *testing.T) {
		_, err := LoadModel(ctx, es, (*OrderStats)(nil), valid)
		testutil.NewIs(t).Err(err, ErrSnapshotConfigInvalid)
	})

	t.Run("nil codec", func(t *testing.T) {
		cfg := valid
		cfg.Codec = nil
		_, err := LoadModel(ctx, es, &OrderStats{}, cfg)
		testutil.NewIs(t).Err(err, ErrSnapshotConfigInvalid)
	})

	t.Run("codec without a name", func(t *testing.T) {
		cfg := valid
		cfg.Codec = namelessCodec{codec.JSON}
		_, err := LoadModel(ctx, es, &OrderStats{}, cfg)
		testutil.NewIs(t).Err(err, ErrSnapshotConfigInvalid)
	})

	t.Run("empty name", func(t *testing.T) {
		cfg := valid
		cfg.Name = ""
		_, err := LoadModel(ctx, es, &OrderStats{}, cfg)
		testutil.NewIs(t).Err(err, ErrSnapshotConfigInvalid)
	})
}

func appendOrderEvents(t *testing.T, es *EventStore, entities ...string) []*Event {
	t.Helper()
	events := make([]*Event, len(entities))
	for i, entity := range entities {
		events[i] = &Event{Entity: entity, Data: &OrderPlaced{}}
		if _, err := es.Append(context.Background(), []*Event{events[i]}); err != nil {
			t.Fatalf("append %s: %v", entity, err)
		}
	}
	return events
}

func nextExpect[T any](t *testing.T, m *Model[T], entity string) uint64 {
	t.Helper()
	events, err := m.Decide(context.Background(), &Command{Data: entity})
	if err != nil {
		t.Fatalf("decide %s: %v", entity, err)
	}
	return events[0].Expect.Sequence
}

func TestSnapshotLoadModel_WithoutSnapshotReplaysConfiguredScope(t *testing.T) {
	es := newTestStore(t)
	ctx := context.Background()

	evs := appendOrderEvents(t, es, "order.1", "order.2", "order.1", "user.1", "order.2", "user.1")

	t.Run("aggregate", func(t *testing.T) {
		is := testutil.NewIs(t)
		state := &entityCounter{Counts: map[string]int{}}
		m, err := LoadModel(ctx, es, state, SnapshotConfig{
			Name: "order-1", Version: "1", Filters: []string{"order.1"}, Codec: codec.JSON,
		})
		is.NoErr(err)
		is.Equal(state.Counts, map[string]int{"order.1": 2})
		is.Equal(nextExpect(t, m, "order.1"), evs[2].Sequence())
	})

	t.Run("projection", func(t *testing.T) {
		is := testutil.NewIs(t)
		state := &entityCounter{Counts: map[string]int{}}
		m, err := LoadModel(ctx, es, state, SnapshotConfig{
			Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON,
		})
		is.NoErr(err)
		is.Equal(state.Counts, map[string]int{"order.1": 2, "order.2": 2})
		is.Equal(nextExpect(t, m, "order.1"), evs[2].Sequence())
		is.Equal(nextExpect(t, m, "order.2"), evs[4].Sequence())
		is.Equal(nextExpect(t, m, "user.1"), uint64(0))
	})
}

func TestSnapshotBinding_IdentifiesStoreByStreamAndTenant(t *testing.T) {
	is := testutil.NewIs(t)
	mgr, ctx := newTestManager(t)

	base, err := mgr.CreateEventStore(ctx, EventStoreConfig{Name: "bound", Tenancy: true})
	is.NoErr(err)
	other, err := mgr.CreateEventStore(ctx, EventStoreConfig{Name: "other", Tenancy: true})
	is.NoErr(err)

	tenantA, err := base.Tenant("a")
	is.NoErr(err)
	m, err := LoadModel(ctx, tenantA, &entityCounter{Counts: map[string]int{}}, SnapshotConfig{
		Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON,
	})
	is.NoErr(err)

	rederived, err := base.Tenant("a")
	is.NoErr(err)
	_, err = rederived.Evolve(ctx, m)
	is.NoErr(err)

	fetched, err := mgr.GetEventStore(ctx, "bound")
	is.NoErr(err)
	fetchedA, err := fetched.Tenant("a")
	is.NoErr(err)
	_, err = fetchedA.Evolve(ctx, m)
	is.NoErr(err)

	tenantB, err := base.Tenant("b")
	is.NoErr(err)
	_, err = tenantB.Evolve(ctx, m)
	is.Err(err, ErrSnapshotWrongStore)

	otherA, err := other.Tenant("a")
	is.NoErr(err)
	_, err = otherA.Evolve(ctx, m)
	is.Err(err, ErrSnapshotWrongStore)
}

func TestSnapshotBinding_RejectsOptionsThatChangeScopeOrSkipEvents(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	evs := appendOrderEvents(t, es, "order.1", "user.1", "order.2")
	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, SnapshotConfig{
		Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON,
	})
	is.NoErr(err)
	cursor := evs[2].Sequence()

	_, err = es.Evolve(ctx, m, WithFilters("user"))
	is.Err(err, ErrSnapshotScopeChanged)

	_, err = es.Evolve(ctx, m, WithFilters("order.*.*", "order"))
	is.NoErr(err)

	_, err = es.Evolve(ctx, m, WithAfterSequence(cursor+1))
	is.Err(err, ErrSnapshotSkipsEvents)

	_, err = es.Evolve(ctx, m, WithAfterSequence(0))
	is.NoErr(err)
}

func counts(t *testing.T, m *Model[*entityCounter]) map[string]int {
	t.Helper()
	out := map[string]int{}
	err := m.View(context.Background(), func(c *entityCounter) error {
		for k, v := range c.Counts {
			out[k] = v
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view: %v", err)
	}
	return out
}

func TestSnapshotWatch_InheritsBindingAndCursor(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	appendOrderEvents(t, es, "order.1", "user.1")
	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, SnapshotConfig{
		Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON,
	})
	is.NoErr(err)

	_, err = es.Watch(ctx, m, WithFilters("user"))
	is.Err(err, ErrSnapshotScopeChanged)
	_, err = es.Watch(ctx, m, WithAfterSequence(m.snapshotCursor()+1))
	is.Err(err, ErrSnapshotSkipsEvents)

	w, err := es.Watch(ctx, m)
	is.NoErr(err)
	defer w.Stop()

	evs := appendOrderEvents(t, es, "order.2", "user.2", "order.1")
	waitFor(t, 2*time.Second, func() bool { return m.snapshotCursor() == evs[2].Sequence() })
	is.Equal(counts(t, m), map[string]int{"order.1": 2, "order.2": 1})
}

func TestSnapshotWatch_OneStorePathAtATime(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, SnapshotConfig{
		Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON,
	})
	is.NoErr(err)

	first, err := es.Watch(ctx, m)
	is.NoErr(err)

	_, err = es.Watch(ctx, m)
	is.Err(err, ErrModelBusy)
	_, err = es.Evolve(ctx, m)
	is.Err(err, ErrModelBusy)

	first.Stop()
	second, err := es.Watch(ctx, m)
	is.NoErr(err)
	defer second.Stop()

	evs := appendOrderEvents(t, es, "order.1")
	waitFor(t, 2*time.Second, func() bool { return m.snapshotCursor() == evs[0].Sequence() })
}

func TestSnapshotDecideAndEvolve_AdvancesEntityButNotCursor(t *testing.T) {
	is := testutil.NewIs(t)
	mgr, ctx := newTestManager(t)
	es, err := mgr.CreateEventStore(ctx, EventStoreConfig{Name: "store"})
	is.NoErr(err)
	other, err := mgr.CreateEventStore(ctx, EventStoreConfig{Name: "other"})
	is.NoErr(err)

	appendOrderEvents(t, es, "order.1")
	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, SnapshotConfig{
		Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON,
	})
	is.NoErr(err)
	cursor := m.snapshotCursor()

	_, _, err = other.DecideAndEvolve(ctx, m, &Command{Data: "order.2"})
	is.Err(err, ErrSnapshotWrongStore)

	events, _, err := es.DecideAndEvolve(ctx, m, &Command{Data: "order.2"})
	is.NoErr(err)
	is.Equal(m.snapshotCursor(), cursor)
	is.Equal(nextExpect(t, m, "order.2"), events[0].Sequence())

	w, err := es.Watch(ctx, m)
	is.NoErr(err)
	defer w.Stop()

	events, _, err = es.DecideAndEvolve(ctx, m, &Command{Data: "order.1"})
	is.NoErr(err)
	waitFor(t, 2*time.Second, func() bool { return m.snapshotCursor() == events[0].Sequence() })

	is.Equal(counts(t, m), map[string]int{"order.1": 2, "order.2": 1})
	is.NoErr(m.evolveFailure())
}

func TestSnapshotDecideAndEvolve_FailedEvolveIsRecorded(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	evs := appendOrderEvents(t, es, "order.1")
	state := &entityCounter{Counts: map[string]int{}}
	m, err := LoadModel(ctx, es, state, SnapshotConfig{
		Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON,
	})
	is.NoErr(err)
	is.NoErr(m.evolveFailure())

	state.failOn = evs[0].Sequence() + 1
	_, _, err = es.DecideAndEvolve(ctx, m, &Command{Data: "order.1"})
	is.Err(err, errEvolverRejected)

	is.Equal(nextExpect(t, m, "order.1"), evs[0].Sequence())
	is.Err(m.evolveFailure(), errEvolverRejected)
}

func TestSnapshotModel_DirectEvolveIsRecorded(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, SnapshotConfig{
		Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON,
	})
	is.NoErr(err)

	is.NoErr(m.Evolve(ctx, &Event{Entity: "order.1", sequence: 1}))
	is.Err(m.evolveFailure(), ErrEvolvedOutsideStore)
}

func publishUndecodable(t *testing.T, es *EventStore, entity string) {
	t.Helper()
	msg := nats.NewMsg(es.prefix + entity + ".order-placed")
	msg.Header.Set(eventCodecHdr, "no-such-codec")
	msg.Header.Set(eventEntityHdr, entity)
	if _, err := es.js.PublishMsg(context.Background(), msg); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func TestSnapshotWatch_UndecodableEventHaltsBoundWatcher(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	bound, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, SnapshotConfig{
		Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON,
	})
	is.NoErr(err)
	ordinary := NewModel(&entityCounter{Counts: map[string]int{}})

	var reported atomic.Int32
	bw, err := es.Watch(ctx, bound, WithErrHandler(func(error, *Event, jetstream.Msg) { reported.Add(1) }))
	is.NoErr(err)
	defer bw.Stop()
	ow, err := es.Watch(ctx, ordinary, WithFilters("order"), WithErrHandler(func(error, *Event, jetstream.Msg) {}))
	is.NoErr(err)
	defer ow.Stop()

	publishUndecodable(t, es, "order.9")
	appendOrderEvents(t, es, "order.1")

	waitFor(t, 2*time.Second, func() bool { return counts(t, ordinary)["order.1"] == 1 })
	waitFor(t, 2*time.Second, func() bool { return reported.Load() == 1 })
	time.Sleep(100 * time.Millisecond)

	is.Equal(counts(t, bound), map[string]int{})
	is.Equal(bound.snapshotCursor(), uint64(0))
	is.Err(bound.evolveFailure(), codec.ErrCodecNotRegistered)
	is.Equal(reported.Load(), int32(1))
}

type gatedCounter struct {
	Counts  map[string]int
	entered chan struct{}
	gate    chan struct{}
}

func (c *gatedCounter) Evolve(ctx context.Context, event *Event) error {
	if c.gate != nil {
		select {
		case c.entered <- struct{}{}:
		default:
		}
		<-c.gate
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Counts[event.Entity]++
	return nil
}

func claimHeld(b *snapshotBinding) bool {
	b.claim.mu.Lock()
	defer b.claim.mu.Unlock()
	return b.claim.held
}

func TestSnapshotWatch_StopWithBufferedEventsIsNotAFailure(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	state := &gatedCounter{Counts: map[string]int{}}
	m, err := LoadModel(ctx, es, state, SnapshotConfig{
		Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON,
	})
	is.NoErr(err)
	state.entered, state.gate = make(chan struct{}, 1), make(chan struct{})

	w, err := es.Watch(ctx, m)
	is.NoErr(err)
	evs := appendOrderEvents(t, es, "order.1", "order.1", "order.1")

	select {
	case <-state.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first event never reached the evolver")
	}
	w.Stop()
	close(state.gate)
	waitFor(t, 2*time.Second, func() bool { return !claimHeld(m.snap) })

	is.NoErr(m.evolveFailure())
	is.Equal(m.snapshotCursor(), evs[0].Sequence())
	is.Equal(state.Counts, map[string]int{"order.1": 1})
}

func TestSnapshotWatch_RepeatedStopDoesNotStopReplacement(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, SnapshotConfig{
		Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON,
	})
	is.NoErr(err)

	first, err := es.Watch(ctx, m)
	is.NoErr(err)
	first.Stop()
	second, err := es.Watch(ctx, m)
	is.NoErr(err)
	defer second.Stop()

	first.Stop()
	bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	_, err = es.Evolve(bounded, m)
	is.Err(err, ErrModelBusy)
}

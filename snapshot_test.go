package rita

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-labs/rita/codec"
	"github.com/synadia-labs/rita/testutil"
	"google.golang.org/protobuf/types/known/wrapperspb"
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

func TestSnapshotLoadModel_RejectsStoreNameUnfitForBucket(t *testing.T) {
	is := testutil.NewIs(t)
	mgr, ctx := newTestManager(t)
	es, err := mgr.CreateEventStore(ctx, EventStoreConfig{Name: "orders~v1"})
	is.NoErr(err)

	_, err = LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, SnapshotConfig{Name: "orders", Version: "1", Codec: codec.JSON})
	is.Err(err, ErrSnapshotConfigInvalid)
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

func failsIfReplayed(seq uint64) *entityCounter {
	return &entityCounter{Counts: map[string]int{}, failOn: seq}
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
	is.Err(m.SaveSnapshot(ctx), errEvolverRejected)
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
	is.Err(bound.SaveSnapshot(ctx), codec.ErrCodecNotRegistered)
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

func TestSnapshotSave_RestoresStateAndReplaysTail(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	cfg := SnapshotConfig{Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON}
	appendOrderEvents(t, es, "order.1", "user.1", "order.2", "order.1")
	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
	appendOrderEvents(t, es, "order.2", "user.2")

	state := failsIfReplayed(1)
	restored, err := LoadModel(ctx, es, state, cfg)
	is.NoErr(err)
	is.Equal(counts(t, restored), map[string]int{"order.1": 2, "order.2": 2})
	_, _, err = es.DecideAndEvolve(ctx, restored, &Command{Data: "order.1"})
	is.NoErr(err)
	is.Equal(counts(t, restored), map[string]int{"order.1": 3, "order.2": 2})
	is.NoErr(restored.SaveSnapshot(ctx))
	_, err = LoadModel(ctx, es, failsIfReplayed(1), cfg)
	is.NoErr(err)
}

func TestSnapshotLoad_RejectsTrimmedTail(t *testing.T) {
	for _, tailEntity := range []string{"order.1", "user.1"} {
		t.Run(tailEntity, func(t *testing.T) {
			is := testutil.NewIs(t)
			mgr, ctx := newTestManager(t)
			es, err := mgr.CreateEventStore(ctx, EventStoreConfig{Name: "limited", MaxMsgs: 3})
			is.NoErr(err)
			cfg := SnapshotConfig{Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON}
			appendOrderEvents(t, es, "order.1")
			m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
			is.NoErr(err)
			is.NoErr(m.SaveSnapshot(ctx))
			appendOrderEvents(t, es, tailEntity, tailEntity, tailEntity, tailEntity)
			_, err = LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
			is.Err(err, ErrSnapshotHistoryGap)
		})
	}
}

func TestSnapshotLoad_EntityAheadOfCursorAppliesSkippedEvents(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	cfg := SnapshotConfig{Name: "orders", Version: "1", Filters: []string{"order"}, Codec: codec.JSON}
	appendOrderEvents(t, es, "order.1")

	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
	appendOrderEvents(t, es, "order.2")
	_, _, err = es.DecideAndEvolve(ctx, m, &Command{Data: "order.1"})
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))

	restored, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
	full, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, SnapshotConfig{Name: "orders-full", Version: "1", Filters: []string{"order"}, Codec: codec.JSON})
	is.NoErr(err)
	is.Equal(counts(t, restored), counts(t, full))
	is.Equal(counts(t, restored), map[string]int{"order.1": 2, "order.2": 1})
	_, _, err = es.DecideAndEvolve(ctx, restored, &Command{Data: "order.2"})
	is.NoErr(err)
}

func TestSnapshotLoad_EmptyScopeOnTrimmedStreamStaysUsable(t *testing.T) {
	is := testutil.NewIs(t)
	mgr, ctx := newTestManager(t)
	es, err := mgr.CreateEventStore(ctx, EventStoreConfig{Name: "limited", MaxMsgs: 3})
	is.NoErr(err)
	appendOrderEvents(t, es, "user.1", "user.1", "user.1", "user.1", "user.1")
	cfg := SnapshotConfig{Name: "invoices", Version: "1", Filters: []string{"invoice"}, Codec: codec.JSON}

	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
	w, err := es.Watch(ctx, m)
	is.NoErr(err)
	w.Stop()

	is.NoErr(m.SaveSnapshot(ctx))
	_, err = LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
}

func TestSnapshotSave_ConcurrentWritersDoNotRegress(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	cfg := SnapshotConfig{Name: "orders", Version: "1", Codec: codec.JSON}
	appendOrderEvents(t, es, "order.1")
	older, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
	appendOrderEvents(t, es, "order.2", "order.1")
	newer, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
	is.NoErr(newer.SaveSnapshot(ctx))
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for i := range 12 {
		wg.Go(func() {
			m := older
			if i%2 == 0 {
				m = newer
			}
			errs <- m.SaveSnapshot(ctx)
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		is.NoErr(err)
	}
	state := failsIfReplayed(2)
	restored, err := LoadModel(ctx, es, state, cfg)
	is.NoErr(err)
	is.Equal(counts(t, restored), map[string]int{"order.1": 2, "order.2": 1})
}

func TestSnapshotSave_ConflictWithOlderWriterIsReported(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	cfg := SnapshotConfig{Name: "orders", Version: "1", Codec: codec.JSON}
	load := func() *Model[*entityCounter] {
		appendOrderEvents(t, es, "order.1")
		m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
		is.NoErr(err)
		return m
	}
	first, second, third := load(), load(), load()

	is.NoErr(first.SaveSnapshot(ctx))
	entry, err := first.snap.kv.Get(ctx, first.snap.key)
	is.NoErr(err)
	is.NoErr(second.SaveSnapshot(ctx))

	candidate, err := third.captureSnapshot()
	is.NoErr(err)
	data, err := json.Marshal(candidate)
	is.NoErr(err)
	stream, err := es.js.Stream(ctx, es.stream)
	is.NoErr(err)
	info := stream.CachedInfo()
	err = third.snap.write(ctx, info, data, candidate.Cursor, entry.Revision())
	is.Err(err, ErrSnapshotConflict)
	is.Err(err, jetstream.ErrKeyRevisionMismatch)

	stored, _, err := third.snap.read(ctx, info)
	is.NoErr(err)
	is.Equal(stored.Cursor, second.snapshotCursor())
	is.NoErr(third.SaveSnapshot(ctx))
	stored, _, err = third.snap.read(ctx, info)
	is.NoErr(err)
	is.Equal(stored.Cursor, third.snapshotCursor())
}

func TestSnapshotLoad_RejectsInvalidEnvelopes(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*snapshotEnvelope)
		want error
	}{
		{"identity", func(e *snapshotEnvelope) { e.Identity.Tenant = "other" }, ErrSnapshotInvalid},
		{"generation", func(e *snapshotEnvelope) { e.Created = time.Time{} }, ErrSnapshotStale},
		{"future cursor", func(e *snapshotEnvelope) { e.Cursor = 20 }, ErrSnapshotStale},
		{"future entity", func(e *snapshotEnvelope) { e.Sequences["order.1"] = 20 }, ErrSnapshotStale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := testutil.NewIs(t)
			es := newTestStore(t)
			ctx := context.Background()
			cfg := SnapshotConfig{Name: "orders", Version: "1", Codec: codec.JSON}
			appendOrderEvents(t, es, "order.1")
			m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
			is.NoErr(err)
			is.NoErr(m.SaveSnapshot(ctx))
			entry, err := m.snap.kv.Get(ctx, m.snap.key)
			is.NoErr(err)
			var env snapshotEnvelope
			is.NoErr(json.Unmarshal(entry.Value(), &env))
			tc.edit(&env)
			data, err := json.Marshal(env)
			is.NoErr(err)
			_, err = m.snap.kv.Put(ctx, m.snap.key, data)
			is.NoErr(err)
			_, err = LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
			is.Err(err, tc.want)
		})
	}
}

func TestSnapshotLoad_RejectsCorruptValues(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value func(t *testing.T, stored []byte) []byte
	}{
		{"not an envelope", func(*testing.T, []byte) []byte { return []byte("not json") }},
		{"undecodable state", func(t *testing.T, stored []byte) []byte {
			var env snapshotEnvelope
			if err := json.Unmarshal(stored, &env); err != nil {
				t.Fatal(err)
			}
			env.State = []byte("not json")
			data, err := json.Marshal(env)
			if err != nil {
				t.Fatal(err)
			}
			return data
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := testutil.NewIs(t)
			es := newTestStore(t)
			ctx := context.Background()
			cfg := SnapshotConfig{Name: "orders", Version: "1", Codec: codec.JSON}
			appendOrderEvents(t, es, "order.1")
			m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
			is.NoErr(err)
			is.NoErr(m.SaveSnapshot(ctx))
			entry, err := m.snap.kv.Get(ctx, m.snap.key)
			is.NoErr(err)
			_, err = m.snap.kv.Put(ctx, m.snap.key, tc.value(t, entry.Value()))
			is.NoErr(err)

			_, err = LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
			is.Err(err, ErrSnapshotInvalid)
		})
	}
}

func TestSnapshotSave_RefusesUnsafeModels(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	is.Err(NewModel(&entityCounter{}).SaveSnapshot(ctx), ErrSnapshotNotConfigured)
	es := newTestStore(t)
	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, SnapshotConfig{Name: "orders", Codec: codec.JSON})
	is.NoErr(err)
	is.NoErr(m.Evolve(ctx, &Event{Entity: "order.1", sequence: 1}))
	is.Err(m.SaveSnapshot(ctx), ErrEvolvedOutsideStore)
	is.Err(m.SaveSnapshot(ctx), ErrEvolveFailed)
}

func TestSnapshotLoad_SeparatesTenantsAndVersions(t *testing.T) {
	is := testutil.NewIs(t)
	mgr, ctx := newTestManager(t)
	base, err := mgr.CreateEventStore(ctx, EventStoreConfig{Name: "tenants", Tenancy: true})
	is.NoErr(err)
	a, err := base.Tenant("a")
	is.NoErr(err)
	b, err := base.Tenant("b")
	is.NoErr(err)
	appendOrderEvents(t, a, "order.1", "order.1")
	appendOrderEvents(t, b, "order.1")
	cfg := SnapshotConfig{Name: "orders", Version: "1", Codec: codec.JSON}
	m, err := LoadModel(ctx, a, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
	other, err := LoadModel(ctx, b, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
	is.Equal(counts(t, other), map[string]int{"order.1": 1})
	cfg.Version = "2"
	_, err = LoadModel(ctx, a, failsIfReplayed(1), cfg)
	is.Err(err, errEvolverRejected)
}

func TestSnapshotLoad_RecreatedStreamIsStale(t *testing.T) {
	is := testutil.NewIs(t)
	mgr, ctx := newTestManager(t)
	es, err := mgr.CreateEventStore(ctx, EventStoreConfig{Name: "recreated"})
	is.NoErr(err)
	cfg := SnapshotConfig{Name: "orders", Codec: codec.JSON}
	appendOrderEvents(t, es, "order.1")
	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
	is.NoErr(mgr.DeleteEventStore(ctx, "recreated"))
	es, err = mgr.CreateEventStore(ctx, EventStoreConfig{Name: "recreated"})
	is.NoErr(err)
	_, err = LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
	is.Err(err, ErrSnapshotStale)
	is.Err(m.SaveSnapshot(ctx), ErrSnapshotStale)
}

func TestSnapshotSave_OversizePreservesPrevious(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	_, err := es.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "ES_SNAP_store", MaxValueSize: 1024})
	is.NoErr(err)
	cfg := SnapshotConfig{Name: "orders", Codec: codec.JSON}
	appendOrderEvents(t, es, "order.1")
	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
	for i := range 100 {
		appendOrderEvents(t, es, "order."+strconv.Itoa(i))
	}
	_, err = es.Evolve(ctx, m)
	is.NoErr(err)
	if err := m.SaveSnapshot(ctx); err == nil {
		t.Fatal("oversized save succeeded")
	}
	_, err = LoadModel(ctx, es, failsIfReplayed(1), cfg)
	is.NoErr(err)
}

type binarySnapshotCounter struct {
	count  int
	failOn uint64
}

func (c *binarySnapshotCounter) Evolve(_ context.Context, event *Event) error {
	if event.Sequence() == c.failOn {
		return errEvolverRejected
	}
	c.count++
	return nil
}

func (c *binarySnapshotCounter) MarshalBinary() ([]byte, error) {
	return json.Marshal(c.count)
}

func (c *binarySnapshotCounter) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, &c.count)
}

type protoSnapshotCounter struct {
	*wrapperspb.Int64Value
	failOn uint64
}

func (c *protoSnapshotCounter) Evolve(_ context.Context, event *Event) error {
	if event.Sequence() == c.failOn {
		return errEvolverRejected
	}
	c.Value++
	return nil
}

func TestSnapshotLoad_CodecsRestoreIntoOriginalState(t *testing.T) {
	es := newTestStore(t)
	ctx := context.Background()
	appendOrderEvents(t, es, "order.1", "order.1")
	t.Run("binary private state", func(t *testing.T) {
		is := testutil.NewIs(t)
		cfg := SnapshotConfig{Name: "binary", Codec: codec.Binary}
		m, err := LoadModel(ctx, es, &binarySnapshotCounter{}, cfg)
		is.NoErr(err)
		is.NoErr(m.SaveSnapshot(ctx))
		state := &binarySnapshotCounter{failOn: 1}
		restored, err := LoadModel(ctx, es, state, cfg)
		is.NoErr(err)
		is.NoErr(restored.View(ctx, func(c *binarySnapshotCounter) error {
			is.Equal(c == state, true)
			is.Equal(c.count, 2)
			return nil
		}))
	})
	t.Run("protobuf", func(t *testing.T) {
		is := testutil.NewIs(t)
		cfg := SnapshotConfig{Name: "proto", Codec: codec.ProtoBuf}
		m, err := LoadModel(ctx, es, &protoSnapshotCounter{Int64Value: &wrapperspb.Int64Value{}}, cfg)
		is.NoErr(err)
		is.NoErr(m.SaveSnapshot(ctx))
		state := &protoSnapshotCounter{Int64Value: &wrapperspb.Int64Value{}, failOn: 1}
		restored, err := LoadModel(ctx, es, state, cfg)
		is.NoErr(err)
		is.NoErr(restored.View(ctx, func(c *protoSnapshotCounter) error {
			is.Equal(c == state, true)
			is.Equal(c.Value, int64(2))
			return nil
		}))
	})
	t.Run("protobuf zero state", func(t *testing.T) {
		is := testutil.NewIs(t)
		cfg := SnapshotConfig{Name: "empty-proto", Filters: []string{"user"}, Codec: codec.ProtoBuf}
		m, err := LoadModel(ctx, es, &protoSnapshotCounter{Int64Value: &wrapperspb.Int64Value{}}, cfg)
		is.NoErr(err)
		is.NoErr(m.SaveSnapshot(ctx))
		state := &protoSnapshotCounter{Int64Value: &wrapperspb.Int64Value{Value: 99}}
		_, err = LoadModel(ctx, es, state, cfg)
		is.NoErr(err)
		is.Equal(state.Value, int64(0))
	})
}

func TestSnapshotDecisions_DoNotNeedSnapshotBucket(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, SnapshotConfig{Name: "orders", Codec: codec.JSON})
	is.NoErr(err)
	is.NoErr(es.js.DeleteKeyValue(ctx, "ES_SNAP_store"))
	_, _, err = es.Decide(ctx, m, &Command{Data: "order.1"})
	is.NoErr(err)
	_, _, err = es.DecideAndEvolve(ctx, m, &Command{Data: "order.2"})
	is.NoErr(err)
	_, err = es.Evolve(ctx, m)
	is.NoErr(err)
	is.Equal(counts(t, m), map[string]int{"order.1": 1, "order.2": 1})
	if err := m.SaveSnapshot(ctx); err == nil {
		t.Fatal("save succeeded after deleting snapshot storage")
	}
}

func TestSnapshotSave_LiveWatcher(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	cfg := SnapshotConfig{Name: "orders", Codec: codec.JSON}
	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
	w, err := es.Watch(ctx, m)
	is.NoErr(err)
	defer w.Stop()
	appended := make(chan error, 1)
	go func() {
		for range 20 {
			_, err := es.Append(ctx, []*Event{
				{Entity: "order.1", Data: &OrderPlaced{}},
				{Entity: "order.2", Data: &OrderPlaced{}},
				{Entity: "order.1", Data: &OrderPlaced{}},
			})
			if err != nil {
				appended <- err
				return
			}
		}
		appended <- nil
	}()
	for range 12 {
		is.NoErr(m.SaveSnapshot(ctx))
	}
	is.NoErr(<-appended)
	restored, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
	is.Equal(counts(t, restored), map[string]int{"order.1": 40, "order.2": 20})
	_, _, err = es.DecideAndEvolve(ctx, restored, &Command{Data: "order.2"})
	is.NoErr(err)
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

func TestSnapshotLoad_WhileCheckpointAdvances(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	cfg := SnapshotConfig{Name: "orders", Codec: codec.JSON}
	appendOrderEvents(t, es, "order.1")
	m, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg)
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
	errs := make(chan error, 2)
	go func() {
		for range 40 {
			if _, _, err := es.DecideAndEvolve(ctx, m, &Command{Data: "order.1"}); err != nil {
				errs <- err
				return
			}
			if _, err := es.Evolve(ctx, m); err != nil {
				errs <- err
				return
			}
			if err := m.SaveSnapshot(ctx); err != nil {
				errs <- err
				return
			}
		}
		errs <- nil
	}()
	go func() {
		for range 40 {
			if _, err := LoadModel(ctx, es, &entityCounter{Counts: map[string]int{}}, cfg); err != nil {
				errs <- err
				return
			}
		}
		errs <- nil
	}()
	is.NoErr(<-errs)
	is.NoErr(<-errs)
	restored, err := LoadModel(ctx, es, failsIfReplayed(1), cfg)
	is.NoErr(err)
	is.Equal(counts(t, restored), map[string]int{"order.1": 41})
}

package rita

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
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

	_, err = LoadModel(ctx, es, newCounter(), ordersConfig())
	is.Err(err, ErrSnapshotConfigInvalid)
}

func TestSnapshotBuckets_ManifestBucketNeverNamesAnotherStoresDataBucket(t *testing.T) {
	is := testutil.NewIs(t)
	manifests, _, err := snapshotBuckets("DATA_orders")
	is.NoErr(err)
	_, objects, err := snapshotBuckets("orders")
	is.NoErr(err)
	is.True(manifests != objects)
}

func TestSnapshotLoadModel_WithoutSnapshotReplaysConfiguredScope(t *testing.T) {
	es := newTestStore(t)
	ctx := context.Background()

	evs := appendOrderEvents(t, es, "order.1", "order.2", "order.1", "user.1", "order.2", "user.1")

	t.Run("aggregate", func(t *testing.T) {
		is := testutil.NewIs(t)
		state := newCounter()
		m, err := LoadModel(ctx, es, state, SnapshotConfig{
			Name: "order-1", Version: "1", Filters: []string{"order.1"}, Codec: codec.JSON,
		})
		is.NoErr(err)
		is.Equal(state.Counts, map[string]int{"order.1": 2})
		is.Equal(nextExpect(t, m, "order.1"), evs[2].Sequence())
	})

	t.Run("projection", func(t *testing.T) {
		is := testutil.NewIs(t)
		state := newCounter()
		m, err := LoadModel(ctx, es, state, ordersConfig("order"))
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
	m, err := LoadModel(ctx, tenantA, newCounter(), ordersConfig("order"))
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
	m, err := LoadModel(ctx, es, newCounter(), ordersConfig("order"))
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
		maps.Copy(out, c.Counts)
		return nil
	})
	if err != nil {
		t.Fatalf("view: %v", err)
	}
	return out
}

func newCounter() *entityCounter {
	return &entityCounter{Counts: map[string]int{}}
}

func ordersConfig(filters ...string) SnapshotConfig {
	return SnapshotConfig{Name: "orders", Version: "1", Filters: filters, Codec: codec.JSON}
}

func failsIfReplayed(seq uint64) *entityCounter {
	return &entityCounter{Counts: map[string]int{}, failOn: seq}
}

func TestSnapshotWatch_InheritsBindingAndCursor(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	appendOrderEvents(t, es, "order.1", "user.1")
	m, err := LoadModel(ctx, es, newCounter(), ordersConfig("order"))
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

	m, err := LoadModel(ctx, es, newCounter(), ordersConfig("order"))
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
	m, err := LoadModel(ctx, es, newCounter(), ordersConfig("order"))
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
	state := newCounter()
	m, err := LoadModel(ctx, es, state, ordersConfig("order"))
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

	m, err := LoadModel(ctx, es, newCounter(), ordersConfig("order"))
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

	bound, err := LoadModel(ctx, es, newCounter(), ordersConfig("order"))
	is.NoErr(err)
	ordinary := NewModel(newCounter())

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

func TestSnapshotWatch_EvolveErrorHaltsBoundWatcher(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	bound, err := LoadModel(ctx, es, failsIfReplayed(1), ordersConfig("order"))
	is.NoErr(err)

	var reported atomic.Int32
	w, err := es.Watch(ctx, bound, WithErrHandler(func(error, *Event, jetstream.Msg) { reported.Add(1) }))
	is.NoErr(err)
	defer w.Stop()

	appendOrderEvents(t, es, "order.1", "order.2")

	waitFor(t, 2*time.Second, func() bool { return reported.Load() == 1 })
	time.Sleep(100 * time.Millisecond)

	is.Equal(counts(t, bound), map[string]int{})
	is.Equal(bound.snapshotCursor(), uint64(0))
	is.Err(bound.evolveFailure(), errEvolverRejected)
	is.Err(bound.SaveSnapshot(ctx), errEvolverRejected)
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
	m, err := LoadModel(ctx, es, state, ordersConfig("order"))
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
	cfg := ordersConfig("order")
	appendOrderEvents(t, es, "order.1", "user.1", "order.2", "order.1")
	m, err := LoadModel(ctx, es, newCounter(), cfg)
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
			cfg := ordersConfig("order")
			appendOrderEvents(t, es, "order.1")
			m, err := LoadModel(ctx, es, newCounter(), cfg)
			is.NoErr(err)
			is.NoErr(m.SaveSnapshot(ctx))
			appendOrderEvents(t, es, tailEntity, tailEntity, tailEntity, tailEntity)
			_, err = LoadModel(ctx, es, newCounter(), cfg)
			is.Err(err, ErrSnapshotHistoryGap)
		})
	}
}

func TestSnapshotLoad_EntityAheadOfCursorAppliesSkippedEvents(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	cfg := ordersConfig("order")
	appendOrderEvents(t, es, "order.1")

	m, err := LoadModel(ctx, es, newCounter(), cfg)
	is.NoErr(err)
	appendOrderEvents(t, es, "order.2")
	_, _, err = es.DecideAndEvolve(ctx, m, &Command{Data: "order.1"})
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))

	restored, err := LoadModel(ctx, es, newCounter(), cfg)
	is.NoErr(err)
	full, err := LoadModel(ctx, es, newCounter(), SnapshotConfig{Name: "orders-full", Version: "1", Filters: []string{"order"}, Codec: codec.JSON})
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

	m, err := LoadModel(ctx, es, newCounter(), cfg)
	is.NoErr(err)
	w, err := es.Watch(ctx, m)
	is.NoErr(err)
	w.Stop()

	is.NoErr(m.SaveSnapshot(ctx))
	_, err = LoadModel(ctx, es, newCounter(), cfg)
	is.NoErr(err)
}

func TestSnapshotSave_ConcurrentWritersDoNotRegress(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	cfg := ordersConfig()
	appendOrderEvents(t, es, "order.1")
	older, err := LoadModel(ctx, es, newCounter(), cfg)
	is.NoErr(err)
	appendOrderEvents(t, es, "order.2", "order.1")
	newer, err := LoadModel(ctx, es, newCounter(), cfg)
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
	cfg := ordersConfig()
	load := func() *Model[*entityCounter] {
		appendOrderEvents(t, es, "order.1")
		m, err := LoadModel(ctx, es, newCounter(), cfg)
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
	next, err := third.snap.upload(ctx, candidate)
	is.NoErr(err)
	err = third.snap.publish(ctx, next, entry.Revision(), nil)
	is.Err(err, ErrSnapshotConflict)
	is.Err(err, jetstream.ErrKeyRevisionMismatch)
	_, err = third.snap.objects.GetInfo(ctx, next.Current)
	is.Err(err, jetstream.ErrObjectNotFound)

	is.Equal(storedManifest(t, third).Cursor, second.snapshotCursor())
	is.NoErr(third.SaveSnapshot(ctx))
	is.Equal(storedManifest(t, third).Cursor, third.snapshotCursor())
}

func TestSnapshotSave_StreamRecreatedDuringUploadIsStale(t *testing.T) {
	is := testutil.NewIs(t)
	mgr, ctx := newTestManager(t)
	es, err := mgr.CreateEventStore(ctx, EventStoreConfig{Name: "recreated"})
	is.NoErr(err)
	cfg := ordersConfig()
	appendOrderEvents(t, es, "order.1")
	m, err := LoadModel(ctx, es, newCounter(), cfg)
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
	before, err := m.snap.kv.Get(ctx, m.snap.key)
	is.NoErr(err)
	appendOrderEvents(t, es, "order.1")
	_, err = es.Evolve(ctx, m)
	is.NoErr(err)

	candidate, err := m.captureSnapshot()
	is.NoErr(err)
	next, err := m.snap.upload(ctx, candidate)
	is.NoErr(err)
	is.NoErr(mgr.DeleteEventStore(ctx, "recreated"))
	_, err = mgr.CreateEventStore(ctx, EventStoreConfig{Name: "recreated"})
	is.NoErr(err)

	err = m.snap.publish(ctx, next, before.Revision(), nil)
	is.Err(err, ErrSnapshotStale)
	_, err = m.snap.objects.GetInfo(ctx, next.Current)
	is.Err(err, jetstream.ErrObjectNotFound)
	after, err := m.snap.kv.Get(ctx, m.snap.key)
	is.NoErr(err)
	is.Equal(after.Revision(), before.Revision())
}

func TestSnapshotPublishRejected_OnlyDefiniteRejectionsAllowCleanup(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"key exists", jetstream.ErrKeyExists, true},
		{"revision mismatch", jetstream.ErrKeyRevisionMismatch, true},
		{"wrapped revision mismatch", fmt.Errorf("update: %w", jetstream.ErrKeyRevisionMismatch), true},
		{"no error", nil, false},
		{"timeout", nats.ErrTimeout, false},
		{"deadline", context.DeadlineExceeded, false},
		{"no responders", nats.ErrNoResponders, false},
		{"other", errors.New("connection reset"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testutil.NewIs(t).Equal(publishRejected(tc.err), tc.want)
		})
	}
}

func TestSnapshotSave_CoveredWriterDeletesOnlyItsObject(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	cfg := ordersConfig()
	appendOrderEvents(t, es, "order.1")
	older, err := LoadModel(ctx, es, newCounter(), cfg)
	is.NoErr(err)
	appendOrderEvents(t, es, "order.1")
	newer, err := LoadModel(ctx, es, newCounter(), cfg)
	is.NoErr(err)
	is.NoErr(newer.SaveSnapshot(ctx))
	current := storedManifest(t, newer)

	candidate, err := older.captureSnapshot()
	is.NoErr(err)
	next, err := older.snap.upload(ctx, candidate)
	is.NoErr(err)
	is.NoErr(older.snap.publish(ctx, next, 0, nil))
	_, err = older.snap.objects.GetInfo(ctx, next.Current)
	is.Err(err, jetstream.ErrObjectNotFound)
	_, err = older.snap.objects.GetInfo(ctx, current.Current)
	is.NoErr(err)
}

func TestSnapshotManifest_StaysSmallRegardlessOfState(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	appendOrderEvents(t, es, "order.1", "order.1")
	manifestSize := func(name string, blob []byte) int {
		m, err := LoadModel(ctx, es, &blobState{Blob: blob}, SnapshotConfig{Name: name, Codec: codec.JSON})
		is.NoErr(err)
		is.NoErr(m.SaveSnapshot(ctx))
		entry, err := m.snap.kv.Get(ctx, m.snap.key)
		is.NoErr(err)
		return len(entry.Value())
	}

	small := manifestSize("small", nil)
	large := manifestSize("large", bytes.Repeat([]byte("x"), 2*int(es.js.Conn().MaxPayload())))
	is.Equal(large, small)
	is.True(large < 1024)
}

func TestSnapshotSave_StoresCodecStateVerbatim(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	_, m, manifest := savedOrdersSnapshot(t)
	var state []byte
	is.NoErr(m.View(ctx, func(c *entityCounter) error {
		var err error
		state, err = m.snap.config.Codec.Marshal(c)
		return err
	}))

	stored, err := m.snap.objects.GetBytes(ctx, manifest.Current)
	is.NoErr(err)
	is.True(bytes.HasSuffix(stored, state))
}

func TestSnapshotSave_ObjectNamesCarryManifestKey(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	appendOrderEvents(t, es, "order.1")
	current := map[string]string{}
	var objects jetstream.ObjectStore
	for _, name := range []string{"first", "second"} {
		m, err := LoadModel(ctx, es, newCounter(), SnapshotConfig{Name: name, Codec: codec.JSON})
		is.NoErr(err)
		is.NoErr(m.SaveSnapshot(ctx))
		current[m.snap.key] = storedManifest(t, m).Current
		objects = m.snap.objects
	}

	listed, err := objects.List(ctx)
	is.NoErr(err)
	is.Equal(len(listed), len(current))
	for _, object := range listed {
		key, _, ok := strings.Cut(object.Name, ".")
		is.True(ok)
		is.Equal(object.Name, current[key])
	}
}

func TestSnapshotSave_RetainsCurrentAndPrevious(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	es, m, _ := savedOrdersSnapshot(t)

	advanceAndSave(t, es, m)
	b := storedManifest(t, m)
	advanceAndSave(t, es, m)
	c := storedManifest(t, m)
	is.Equal(c.Previous, b.Current)
	listed, err := m.snap.objects.List(ctx)
	is.NoErr(err)
	names := make([]string, 0, len(listed))
	for _, object := range listed {
		names = append(names, object.Name)
	}
	slices.Sort(names)
	want := []string{b.Current, c.Current}
	slices.Sort(want)
	is.Equal(names, want)
}

type countingCodec struct {
	codec.Codec
	marshals *atomic.Int64
}

func (c countingCodec) Marshal(v any) ([]byte, error) {
	c.marshals.Add(1)
	return c.Codec.Marshal(v)
}

func TestSnapshotSave_CoveredSaveDoesNotEncodeState(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	appendOrderEvents(t, es, "order.1")
	marshals := &atomic.Int64{}
	m, err := LoadModel(ctx, es, newCounter(), SnapshotConfig{Name: "orders", Codec: countingCodec{codec.JSON, marshals}})
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
	is.NoErr(m.SaveSnapshot(ctx))
	is.Equal(marshals.Load(), int64(1))
}

func TestSnapshotSave_SequenceMapLargerThanMaxPayloadRoundTrips(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	cfg := SnapshotConfig{Name: "entities", Codec: codec.JSON}
	maxPayload := int(es.js.Conn().MaxPayload())
	id := strings.Repeat("x", 250)
	entities := 2 * maxPayload / len(id)
	for i := range entities {
		_, err := es.Append(ctx, []*Event{{Entity: fmt.Sprintf("order.%s%d", id, i), Data: &OrderPlaced{}}})
		is.NoErr(err)
	}
	m, err := LoadModel(ctx, es, &blobState{}, cfg)
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
	object, err := m.snap.objects.GetInfo(ctx, storedManifest(t, m).Current)
	is.NoErr(err)
	is.True(object.Size > uint64(maxPayload))

	loaded, err := LoadModel(ctx, es, &blobState{failOn: 1}, cfg)
	is.NoErr(err)
	is.Equal(len(loaded.lseq), entities)
	is.NoErr(loaded.View(ctx, func(s *blobState) error {
		is.Equal(s.Events, entities)
		return nil
	}))
}

func editStored[T any](edit func(*T)) func(*testing.T, []byte) []byte {
	return func(t *testing.T, stored []byte) []byte {
		var v T
		if err := json.Unmarshal(stored, &v); err != nil {
			t.Fatal(err)
		}
		edit(&v)
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
}

func editEnvelope(edit func(*snapshotEnvelope)) func(*testing.T, []byte) []byte {
	return func(t *testing.T, stored []byte) []byte {
		env, err := decodeSnapshotEnvelope(stored)
		if err != nil {
			t.Fatal(err)
		}
		edit(env)
		header, err := env.encodeHeader()
		if err != nil {
			t.Fatal(err)
		}
		return append(header, env.State...)
	}
}

func notJSON(*testing.T, []byte) []byte { return []byte("not json") }

func savedOrdersSnapshot(t *testing.T) (*EventStore, *Model[*entityCounter], *snapshotManifest) {
	t.Helper()
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	appendOrderEvents(t, es, "order.1")
	m, err := LoadModel(ctx, es, newCounter(), ordersConfig())
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
	return es, m, storedManifest(t, m)
}

func currentInfo[T any](t testing.TB, m *Model[T]) *jetstream.StreamInfo {
	t.Helper()
	info, err := m.snap.streamInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func storedManifest[T any](t testing.TB, m *Model[T]) *snapshotManifest {
	t.Helper()
	manifest, _, err := m.snap.read(context.Background(), currentInfo(t, m))
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func advanceAndSave(t *testing.T, es *EventStore, m *Model[*entityCounter]) {
	t.Helper()
	is := testutil.NewIs(t)
	ctx := context.Background()
	appendOrderEvents(t, es, "order.1")
	_, err := es.Evolve(ctx, m)
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
}

func TestSnapshotLoad_RejectsBadManifests(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tamper func(*testing.T, []byte) []byte
		want   error
	}{
		{"not a manifest", notJSON, ErrSnapshotInvalid},
		{"identity", editStored(func(m *snapshotManifest) { m.Identity.Tenant = "other" }), ErrSnapshotInvalid},
		{"format", editStored(func(m *snapshotManifest) { m.Identity.Format = 0 }), ErrSnapshotInvalid},
		{"no object", editStored(func(m *snapshotManifest) { m.Current = "" }), ErrSnapshotInvalid},
		{"generation", editStored(func(m *snapshotManifest) { m.Created = time.Time{} }), ErrSnapshotStale},
		{"future cursor", editStored(func(m *snapshotManifest) { m.Cursor = 20 }), ErrSnapshotStale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := testutil.NewIs(t)
			ctx := context.Background()
			es, m, _ := savedOrdersSnapshot(t)
			entry, err := m.snap.kv.Get(ctx, m.snap.key)
			is.NoErr(err)
			_, err = m.snap.kv.Put(ctx, m.snap.key, tc.tamper(t, entry.Value()))
			is.NoErr(err)

			_, err = LoadModel(ctx, es, newCounter(), ordersConfig())
			is.Err(err, tc.want)
		})
	}
}

func TestSnapshotLoad_RejectsBadObjects(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tamper func(*testing.T, []byte) []byte
		want   error
	}{
		{"not an envelope", notJSON, ErrSnapshotInvalid},
		{"shorter than header length", func(*testing.T, []byte) []byte { return []byte{0, 0, 1} }, ErrSnapshotInvalid},
		{"header length past end", func(_ *testing.T, stored []byte) []byte { return stored[:len(stored)/2] }, ErrSnapshotInvalid},
		{"header length overflows", func(_ *testing.T, stored []byte) []byte {
			return append(bytes.Repeat([]byte{0xff}, snapshotHeaderLenSize), stored[snapshotHeaderLenSize:]...)
		}, ErrSnapshotInvalid},
		{"undecodable state", editEnvelope(func(e *snapshotEnvelope) { e.State = []byte("not json") }), ErrSnapshotInvalid},
		{"different identity", editEnvelope(func(e *snapshotEnvelope) { e.Identity.Tenant = "other" }), ErrSnapshotInvalid},
		{"different cursor", editEnvelope(func(e *snapshotEnvelope) { e.Cursor-- }), ErrSnapshotInvalid},
		{"no sequences", editEnvelope(func(e *snapshotEnvelope) { e.Sequences = nil }), ErrSnapshotInvalid},
		{"future entity", editEnvelope(func(e *snapshotEnvelope) { e.Sequences["order.1"] = 20 }), ErrSnapshotStale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := testutil.NewIs(t)
			ctx := context.Background()
			es, m, manifest := savedOrdersSnapshot(t)
			stored, err := m.snap.objects.GetBytes(ctx, manifest.Current)
			is.NoErr(err)
			_, err = m.snap.objects.PutBytes(ctx, manifest.Current, tc.tamper(t, stored))
			is.NoErr(err)

			_, err = LoadModel(ctx, es, newCounter(), ordersConfig())
			is.Err(err, tc.want)
		})
	}
}

func TestSnapshotLoad_RejectsMalformedObjectSize(t *testing.T) {
	for _, name := range []string{"understated", "overstated", "large", "overflow", "max uint64", "zero"} {
		t.Run(name, func(t *testing.T) {
			is := testutil.NewIs(t)
			ctx := context.Background()
			es, m, manifest := savedOrdersSnapshot(t)
			info, err := m.snap.objects.GetInfo(ctx, manifest.Current)
			is.NoErr(err)
			switch name {
			case "understated":
				info.Size--
			case "overstated":
				info.Size++
			case "large":
				info.Size = 1 << 40
			case "overflow":
				info.Size = 1 << 63
			case "max uint64":
				info.Size = ^uint64(0)
			case "zero":
				info.Size = 0
			}
			data, err := json.Marshal(info)
			is.NoErr(err)
			subject := "$O." + info.Bucket + ".M." + base64.URLEncoding.EncodeToString([]byte(manifest.Current))
			_, err = es.js.Publish(ctx, subject, data)
			is.NoErr(err)

			_, err = LoadModel(ctx, es, newCounter(), ordersConfig())
			is.Err(err, ErrSnapshotInvalid)
		})
	}
}

func TestSnapshotLoad_RetriesWhenManifestReplacedDuringFetch(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	es, m, _ := savedOrdersSnapshot(t)
	attempts := 0
	env, _, err := m.snap.load(ctx, currentInfo(t, m), func(ctx context.Context, info *jetstream.StreamInfo, manifest *snapshotManifest) (*snapshotEnvelope, error) {
		attempts++
		if attempts == 1 {
			advanceAndSave(t, es, m)
			advanceAndSave(t, es, m)
		}
		return m.snap.fetch(ctx, info, manifest)
	})
	is.NoErr(err)
	is.Equal(attempts, 2)
	is.Equal(env.Cursor, m.snapshotCursor())
}

func TestSnapshotLoad_RetriesWhenObjectRetiredDuringDownload(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	es := newTestStore(t)
	appendOrderEvents(t, es, "order.1")
	blob := bytes.Repeat([]byte("x"), 16<<20)
	m, err := LoadModel(ctx, es, &blobState{Blob: blob}, SnapshotConfig{Name: "retired", Codec: codec.JSON})
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
	first := true
	env, _, err := m.snap.load(ctx, currentInfo(t, m), func(fetchCtx context.Context, info *jetstream.StreamInfo, manifest *snapshotManifest) (*snapshotEnvelope, error) {
		if !first {
			return m.snap.fetch(fetchCtx, info, manifest)
		}
		first = false
		result, err := m.snap.objects.Get(fetchCtx, manifest.Current)
		if err != nil {
			return nil, err
		}
		defer func() { _ = result.Close() }()
		for range 2 {
			appendOrderEvents(t, es, "order.1")
			_, err := es.Evolve(ctx, m)
			is.NoErr(err)
			is.NoErr(m.SaveSnapshot(ctx))
		}
		_, err = io.ReadAll(result)
		if err == nil {
			t.Fatal("retired object download unexpectedly completed")
		}
		return nil, err
	})
	is.NoErr(err)
	var restored blobState
	is.NoErr(codec.JSON.Unmarshal(env.State, &restored))
	is.Equal(restored.Events, 3)
	is.True(bytes.Equal(restored.Blob, blob))
}

func TestSnapshotLoad_InvalidObjectWithUnchangedManifestFailsAtOnce(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	_, m, manifest := savedOrdersSnapshot(t)
	_, err := m.snap.objects.PutBytes(ctx, manifest.Current, []byte("not json"))
	is.NoErr(err)
	attempts := 0
	_, _, err = m.snap.load(ctx, currentInfo(t, m), func(ctx context.Context, info *jetstream.StreamInfo, manifest *snapshotManifest) (*snapshotEnvelope, error) {
		attempts++
		return m.snap.fetch(ctx, info, manifest)
	})
	is.Err(err, ErrSnapshotInvalid)
	is.Equal(attempts, 1)
}

func TestSnapshotLoad_MissingObjectWithUnchangedManifestRetriesForReplicaLag(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	_, m, manifest := savedOrdersSnapshot(t)
	data, err := m.snap.objects.GetBytes(ctx, manifest.Current)
	is.NoErr(err)
	is.NoErr(m.snap.objects.Delete(ctx, manifest.Current))
	attempts := 0
	env, _, err := m.snap.load(ctx, currentInfo(t, m), func(ctx context.Context, info *jetstream.StreamInfo, manifest *snapshotManifest) (*snapshotEnvelope, error) {
		attempts++
		if attempts == snapshotFetchAttempts {
			_, err := m.snap.objects.PutBytes(ctx, manifest.Current, data)
			is.NoErr(err)
		}
		return m.snap.fetch(ctx, info, manifest)
	})
	is.NoErr(err)
	is.Equal(attempts, snapshotFetchAttempts)
	is.Equal(env.Cursor, manifest.Cursor)
}

func TestSnapshotLoad_StopsRetryingAfterThreeReplacedManifests(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	es, m, _ := savedOrdersSnapshot(t)
	errUnreadable := errors.New("unreadable")
	attempts := 0
	_, _, err := m.snap.load(ctx, currentInfo(t, m), func(context.Context, *jetstream.StreamInfo, *snapshotManifest) (*snapshotEnvelope, error) {
		attempts++
		advanceAndSave(t, es, m)
		return nil, errUnreadable
	})
	is.Err(err, errUnreadable)
	is.Equal(attempts, snapshotFetchAttempts)
}

func TestSnapshotFetchContext_DefaultsTo30sAndHonoursCallerDeadline(t *testing.T) {
	is := testutil.NewIs(t)
	start := time.Now()
	ctx, cancel := snapshotFetchContext(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	is.True(ok)
	is.True(!deadline.Before(start.Add(snapshotFetchTimeout)))
	is.True(deadline.Before(time.Now().Add(snapshotFetchTimeout + time.Second)))
	is.Equal(snapshotFetchTimeout, 30*time.Second)

	want := time.Now().Add(time.Hour)
	caller, cancelCaller := context.WithDeadline(context.Background(), want)
	defer cancelCaller()
	ctx, cancel = snapshotFetchContext(caller)
	defer cancel()
	deadline, ok = ctx.Deadline()
	is.True(ok)
	is.Equal(deadline, want)
}

func TestSnapshotLoad_AttemptsShareCallerDeadline(t *testing.T) {
	is := testutil.NewIs(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	es, m, _ := savedOrdersSnapshot(t)
	var deadlines []time.Time
	_, _, err := m.snap.load(ctx, currentInfo(t, m), func(fetchCtx context.Context, _ *jetstream.StreamInfo, _ *snapshotManifest) (*snapshotEnvelope, error) {
		deadline, ok := fetchCtx.Deadline()
		is.True(ok)
		deadlines = append(deadlines, deadline)
		advanceAndSave(t, es, m)
		return nil, errors.New("unreadable")
	})
	is.True(err != nil)
	is.Equal(len(deadlines), snapshotFetchAttempts)
	for _, deadline := range deadlines {
		is.Equal(deadline, deadlines[0])
	}
}

func TestSnapshotLoad_MissingObjectIsInvalid(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	es, m, manifest := savedOrdersSnapshot(t)
	is.NoErr(m.snap.objects.Delete(ctx, manifest.Current))

	_, err := LoadModel(ctx, es, newCounter(), ordersConfig())
	is.Err(err, ErrSnapshotInvalid)
	is.Err(err, jetstream.ErrObjectNotFound)
}

func TestSnapshotSave_RefusesUnsafeModels(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	is.Err(NewModel(&entityCounter{}).SaveSnapshot(ctx), ErrSnapshotNotConfigured)
	es := newTestStore(t)
	m, err := LoadModel(ctx, es, newCounter(), SnapshotConfig{Name: "orders", Codec: codec.JSON})
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
	cfg := ordersConfig()
	m, err := LoadModel(ctx, a, newCounter(), cfg)
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
	other, err := LoadModel(ctx, b, newCounter(), cfg)
	is.NoErr(err)
	is.Equal(counts(t, other), map[string]int{"order.1": 1})
	cfg.Version = "2"
	_, err = LoadModel(ctx, a, failsIfReplayed(1), cfg)
	is.Err(err, errEvolverRejected)
}

type blobState struct {
	Blob   []byte
	Events int
	failOn uint64
}

func (s *blobState) Evolve(_ context.Context, event *Event) error {
	if event.Sequence() == s.failOn {
		return errEvolverRejected
	}
	s.Events++
	return nil
}

func TestSnapshotSave_StateLargerThanMaxPayloadRoundTrips(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	cfg := SnapshotConfig{Name: "blob", Codec: codec.JSON}
	appendOrderEvents(t, es, "order.1", "order.1")
	blob := bytes.Repeat([]byte("x"), 2*int(es.js.Conn().MaxPayload()))
	m, err := LoadModel(ctx, es, &blobState{Blob: blob}, cfg)
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))

	loaded, err := LoadModel(ctx, es, &blobState{failOn: 1}, cfg)
	is.NoErr(err)
	is.NoErr(loaded.View(ctx, func(s *blobState) error {
		is.Equal(s.Events, 2)
		is.True(bytes.Equal(s.Blob, blob))
		return nil
	}))
}

func TestSnapshotLoad_RecreatedStreamIsStale(t *testing.T) {
	is := testutil.NewIs(t)
	mgr, ctx := newTestManager(t)
	es, err := mgr.CreateEventStore(ctx, EventStoreConfig{Name: "recreated"})
	is.NoErr(err)
	cfg := SnapshotConfig{Name: "orders", Codec: codec.JSON}
	appendOrderEvents(t, es, "order.1")
	m, err := LoadModel(ctx, es, newCounter(), cfg)
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
	is.NoErr(mgr.DeleteEventStore(ctx, "recreated"))
	es, err = mgr.CreateEventStore(ctx, EventStoreConfig{Name: "recreated"})
	is.NoErr(err)
	_, err = LoadModel(ctx, es, newCounter(), cfg)
	is.Err(err, ErrSnapshotStale)
	is.Err(m.SaveSnapshot(ctx), ErrSnapshotStale)
}

func TestSnapshotSave_FailedUploadPreservesPrevious(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	_, err := es.js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "ES_SNAP_DATA_store", MaxBytes: 2048})
	is.NoErr(err)
	cfg := SnapshotConfig{Name: "orders", Codec: codec.JSON}
	appendOrderEvents(t, es, "order.1")
	m, err := LoadModel(ctx, es, newCounter(), cfg)
	is.NoErr(err)
	is.NoErr(m.SaveSnapshot(ctx))
	before, err := m.snap.kv.Get(ctx, m.snap.key)
	is.NoErr(err)
	for i := range 200 {
		appendOrderEvents(t, es, "order."+strconv.Itoa(i))
	}
	_, err = es.Evolve(ctx, m)
	is.NoErr(err)
	if err := m.SaveSnapshot(ctx); err == nil {
		t.Fatal("save larger than the data bucket succeeded")
	}
	after, err := m.snap.kv.Get(ctx, m.snap.key)
	is.NoErr(err)
	is.Equal(after.Revision(), before.Revision())
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
	t.Run("user codec", func(t *testing.T) {
		is := testutil.NewIs(t)
		cfg := SnapshotConfig{Name: "tagged", Codec: taggedCodec{}}
		m, err := LoadModel(ctx, es, newCounter(), cfg)
		is.NoErr(err)
		is.NoErr(m.SaveSnapshot(ctx))
		restored, err := LoadModel(ctx, es, failsIfReplayed(1), cfg)
		is.NoErr(err)
		is.Equal(counts(t, restored), map[string]int{"order.1": 2})
	})
}

type taggedCodec struct{}

func (taggedCodec) Name() string { return "tagged" }

func (taggedCodec) Marshal(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	return append([]byte("tagged:"), data...), err
}

func (taggedCodec) Unmarshal(data []byte, v any) error {
	rest, ok := bytes.CutPrefix(data, []byte("tagged:"))
	if !ok {
		return errors.New("missing tag")
	}
	return json.Unmarshal(rest, v)
}

func TestSnapshotDecisions_DoNotNeedSnapshotBucket(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()
	m, err := LoadModel(ctx, es, newCounter(), SnapshotConfig{Name: "orders", Codec: codec.JSON})
	is.NoErr(err)
	is.NoErr(es.js.DeleteKeyValue(ctx, "ES_SNAP_META_store"))
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
	m, err := LoadModel(ctx, es, newCounter(), cfg)
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
	restored, err := LoadModel(ctx, es, newCounter(), cfg)
	is.NoErr(err)
	is.Equal(counts(t, restored), map[string]int{"order.1": 40, "order.2": 20})
	_, _, err = es.DecideAndEvolve(ctx, restored, &Command{Data: "order.2"})
	is.NoErr(err)
}

func TestSnapshotWatch_RepeatedStopDoesNotStopReplacement(t *testing.T) {
	is := testutil.NewIs(t)
	es := newTestStore(t)
	ctx := context.Background()

	m, err := LoadModel(ctx, es, newCounter(), ordersConfig("order"))
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
	m, err := LoadModel(ctx, es, newCounter(), cfg)
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
			if _, err := LoadModel(ctx, es, newCounter(), cfg); err != nil {
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

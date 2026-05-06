package rita

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/synadia-labs/rita/testutil"
	"github.com/synadia-labs/rita/types"
)

func newReactTestStore(t *testing.T) *EventStore {
	t.Helper()

	srv := testutil.NewNatsServer(t)
	t.Cleanup(func() { testutil.ShutdownNatsServer(srv) })

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	tr, err := types.NewRegistry(registry)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}

	mgr, err := New(nc, WithRegistry(tr))
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	es, err := mgr.CreateEventStore(context.Background(), EventStoreConfig{Name: "store"})
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	return es
}

func TestReact_EmptyDurable(t *testing.T) {
	es := newReactTestStore(t)

	_, err := es.React(context.Background(), "", func(_ context.Context, _ *Event) error { return nil })
	if !errors.Is(err, ErrReactorDurableRequired) {
		t.Fatalf("expected ErrReactorDurableRequired, got %v", err)
	}
}

func TestReact_NilHandler(t *testing.T) {
	es := newReactTestStore(t)

	_, err := es.React(context.Background(), "durable", nil)
	if !errors.Is(err, ErrReactorHandlerRequired) {
		t.Fatalf("expected ErrReactorHandlerRequired, got %v", err)
	}
}

func TestReact_BasicDelivery(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
	}); err != nil {
		t.Fatal(err)
	}

	var got atomic.Int32
	r, err := es.React(ctx, "basic", func(_ context.Context, ev *Event) error {
		if ev.Type == "order-placed" {
			got.Add(1)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Stop(context.Background()) }()

	waitFor(t, 2*time.Second, func() bool { return got.Load() >= 1 })
}

func TestReact_ResumesFromStoredPosition(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
	}); err != nil {
		t.Fatal(err)
	}

	var first atomic.Int32
	r1, err := es.React(ctx, "resume", func(_ context.Context, _ *Event) error {
		first.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, 2*time.Second, func() bool { return first.Load() >= 1 })

	if err := r1.Stop(context.Background()); err != nil {
		t.Fatalf("stop r1: %v", err)
	}

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderShipped{}},
	}); err != nil {
		t.Fatal(err)
	}

	var second atomic.Int32
	var seenType atomic.Value
	r2, err := es.React(ctx, "resume", func(_ context.Context, ev *Event) error {
		seenType.Store(ev.Type)
		second.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r2.Stop(context.Background()) }()

	waitFor(t, 2*time.Second, func() bool { return second.Load() >= 1 })

	if got, _ := seenType.Load().(string); got != "order-shipped" {
		t.Fatalf("expected order-shipped, got %q", got)
	}
}

func TestReact_BackOffAppliedOnNak(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
	}); err != nil {
		t.Fatal(err)
	}

	const backoff = 150 * time.Millisecond

	var (
		mu         sync.Mutex
		deliveries []time.Time
	)
	r, err := es.React(ctx, "backoff", func(_ context.Context, _ *Event) error {
		mu.Lock()
		deliveries = append(deliveries, time.Now())
		mu.Unlock()
		return errors.New("force nak")
	},
		WithBackOff(backoff),
		WithMaxDeliver(3),
		WithAckWait(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Stop(context.Background()) }()

	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(deliveries) >= 2
	})

	mu.Lock()
	gap := deliveries[1].Sub(deliveries[0])
	mu.Unlock()

	// Allow some scheduler leeway but require we actually paced backoff apart.
	if gap < backoff-50*time.Millisecond {
		t.Fatalf("expected backoff >= %v, got %v", backoff-50*time.Millisecond, gap)
	}
}

func TestReact_HandlerCtxCancelledOnStopDeadline(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
	}); err != nil {
		t.Fatal(err)
	}

	handlerEntered := make(chan struct{})
	handlerCtxErr := make(chan error, 1)
	r, err := es.React(ctx, "stop-cancel", func(hctx context.Context, _ *Event) error {
		close(handlerEntered)
		<-hctx.Done()
		handlerCtxErr <- hctx.Err()
		return nil
	}, WithAckWait(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-handlerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := r.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded from Stop, got %v", err)
	}

	select {
	case got := <-handlerCtxErr:
		if !errors.Is(got, context.Canceled) {
			t.Fatalf("expected handler ctx Canceled, got %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler ctx was not cancelled after Stop deadline")
	}

	if err := r.Stop(context.Background()); err != nil {
		t.Fatalf("final stop: %v", err)
	}
}

func TestReact_WithFiltersTranslation(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
		{Entity: "order.1", Data: &OrderShipped{}},
		{Entity: "order.2", Data: &OrderPlaced{}},
	}); err != nil {
		t.Fatal(err)
	}

	var got atomic.Int32
	var lastType atomic.Value
	r, err := es.React(ctx, "filtered", func(_ context.Context, ev *Event) error {
		lastType.Store(ev.Type)
		got.Add(1)
		return nil
	}, WithFilters("*.*.order-shipped"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Stop(context.Background()) }()

	waitFor(t, 2*time.Second, func() bool { return got.Load() >= 1 })

	if v, _ := lastType.Load().(string); v != "order-shipped" {
		t.Fatalf("expected order-shipped, got %q", v)
	}
}

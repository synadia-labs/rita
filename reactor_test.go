package rita

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-labs/rita/testutil"
	"github.com/synadia-labs/rita/types"
)

func newReactTestStore(t *testing.T) *EventStore {
	t.Helper()
	return newReactTestStoreWithLogger(t, nil)
}

func newReactTestStoreWithLogger(t *testing.T, logger *slog.Logger) *EventStore {
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

	opts := []ManagerOption{WithRegistry(tr)}
	if logger != nil {
		opts = append(opts, WithLogger(logger))
	}
	mgr, err := New(nc, opts...)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	es, err := mgr.CreateEventStore(context.Background(), EventStoreConfig{Name: "store"})
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	return es
}

func mustCreateReactor(t *testing.T, es *EventStore, cfg ReactorConfig) Reactor {
	t.Helper()
	r, err := es.CreateReactor(context.Background(), cfg)
	if err != nil {
		t.Fatalf("create reactor %q: %v", cfg.Name, err)
	}
	return r
}

func mustBindReactor(t *testing.T, es *EventStore, name string, handler ReactorHandler) Reactor {
	t.Helper()
	r, err := es.GetReactor(context.Background(), name)
	if err != nil {
		t.Fatalf("get reactor %q: %v", name, err)
	}
	if err := r.Bind(context.Background(), handler); err != nil {
		t.Fatalf("bind reactor %q: %v", name, err)
	}
	return r
}

func TestGetReactor_EmptyName(t *testing.T) {
	es := newReactTestStore(t)

	_, err := es.GetReactor(context.Background(), "")
	if !errors.Is(err, ErrReactorNameRequired) {
		t.Fatalf("expected ErrReactorNameRequired, got %v", err)
	}
}

func TestBind_NilHandler(t *testing.T) {
	es := newReactTestStore(t)
	mustCreateReactor(t, es, ReactorConfig{Name: "nil-handler"})

	r, err := es.GetReactor(context.Background(), "nil-handler")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Bind(context.Background(), nil); !errors.Is(err, ErrReactorHandlerRequired) {
		t.Fatalf("expected ErrReactorHandlerRequired, got %v", err)
	}
}

func TestGetReactor_NotFound(t *testing.T) {
	es := newReactTestStore(t)

	_, err := es.GetReactor(context.Background(), "missing")
	if !errors.Is(err, ErrReactorNotFound) {
		t.Fatalf("expected ErrReactorNotFound, got %v", err)
	}
	if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDoesNotExist) {
		t.Fatalf("expected JetStream sentinel to stay internal, got %v", err)
	}
}

func TestReactor_BasicDelivery(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
	}); err != nil {
		t.Fatal(err)
	}

	mustCreateReactor(t, es, ReactorConfig{Name: "basic"})

	var got atomic.Int32
	r := mustBindReactor(t, es, "basic", func(_ context.Context, ev *Event) error {
		if ev.Type == "order-placed" {
			got.Add(1)
		}
		return nil
	})
	defer func() { _ = r.Unbind(context.Background()) }()

	waitFor(t, 2*time.Second, func() bool { return got.Load() >= 1 })
}

func TestReactor_ResumesFromStoredPosition(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
	}); err != nil {
		t.Fatal(err)
	}

	mustCreateReactor(t, es, ReactorConfig{Name: "resume"})

	var first atomic.Int32
	r1 := mustBindReactor(t, es, "resume", func(_ context.Context, _ *Event) error {
		first.Add(1)
		return nil
	})

	waitFor(t, 2*time.Second, func() bool { return first.Load() >= 1 })

	if err := r1.Unbind(context.Background()); err != nil {
		t.Fatalf("unbind r1: %v", err)
	}

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderShipped{}},
	}); err != nil {
		t.Fatal(err)
	}

	var second atomic.Int32
	var seenType atomic.Value
	r2 := mustBindReactor(t, es, "resume", func(_ context.Context, ev *Event) error {
		seenType.Store(ev.Type)
		second.Add(1)
		return nil
	})
	defer func() { _ = r2.Unbind(context.Background()) }()

	waitFor(t, 2*time.Second, func() bool { return second.Load() >= 1 })

	if got, _ := seenType.Load().(string); got != "order-shipped" {
		t.Fatalf("expected order-shipped, got %q", got)
	}
}

func TestReactor_BackOffAppliedOnNak(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
	}); err != nil {
		t.Fatal(err)
	}

	const backoff = 150 * time.Millisecond

	mustCreateReactor(t, es, ReactorConfig{
		Name:       "backoff",
		BackOff:    []time.Duration{backoff},
		MaxDeliver: 3,
		AckWait:    2 * time.Second,
	})

	var (
		mu         sync.Mutex
		deliveries []time.Time
	)
	r := mustBindReactor(t, es, "backoff", func(_ context.Context, _ *Event) error {
		mu.Lock()
		deliveries = append(deliveries, time.Now())
		mu.Unlock()
		return errors.New("force nak")
	})
	defer func() { _ = r.Unbind(context.Background()) }()

	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(deliveries) >= 2
	})

	mu.Lock()
	gap := deliveries[1].Sub(deliveries[0])
	mu.Unlock()

	if gap < backoff-50*time.Millisecond {
		t.Fatalf("expected backoff >= %v, got %v", backoff-50*time.Millisecond, gap)
	}
}

func TestReactor_HandlerCtxCancelledOnUnbindDeadline(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
	}); err != nil {
		t.Fatal(err)
	}

	mustCreateReactor(t, es, ReactorConfig{Name: "unbind-cancel", AckWait: 10 * time.Second})

	handlerEntered := make(chan struct{})
	handlerCtxErr := make(chan error, 1)
	r := mustBindReactor(t, es, "unbind-cancel", func(hctx context.Context, _ *Event) error {
		close(handlerEntered)
		<-hctx.Done()
		handlerCtxErr <- hctx.Err()
		return nil
	})

	select {
	case <-handlerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	unbindCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := r.Unbind(unbindCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded from Unbind, got %v", err)
	}

	select {
	case got := <-handlerCtxErr:
		if !errors.Is(got, context.Canceled) {
			t.Fatalf("expected handler ctx Canceled, got %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler ctx was not cancelled after Unbind deadline")
	}

	if err := r.Unbind(context.Background()); err != nil {
		t.Fatalf("final unbind: %v", err)
	}
}

func TestReactor_FiltersTranslation(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
		{Entity: "order.1", Data: &OrderShipped{}},
		{Entity: "order.2", Data: &OrderPlaced{}},
	}); err != nil {
		t.Fatal(err)
	}

	mustCreateReactor(t, es, ReactorConfig{Name: "filtered", Filters: []string{"*.*.order-shipped"}})

	var got atomic.Int32
	var lastType atomic.Value
	r := mustBindReactor(t, es, "filtered", func(_ context.Context, ev *Event) error {
		lastType.Store(ev.Type)
		got.Add(1)
		return nil
	})
	defer func() { _ = r.Unbind(context.Background()) }()

	waitFor(t, 2*time.Second, func() bool { return got.Load() >= 1 })

	if v, _ := lastType.Load().(string); v != "order-shipped" {
		t.Fatalf("expected order-shipped, got %q", v)
	}
}

func TestCreateReactor_EmptyName(t *testing.T) {
	es := newReactTestStore(t)
	if _, err := es.CreateReactor(context.Background(), ReactorConfig{}); !errors.Is(err, ErrReactorNameRequired) {
		t.Fatalf("expected ErrReactorNameRequired, got %v", err)
	}
}

func TestCreateReactor_GetReactor_Roundtrip(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	cfg := ReactorConfig{
		Name:          "rt",
		Description:   "round trip",
		Filters:       []string{"*.*.order-shipped"},
		MaxAckPending: 4,
		MaxDeliver:    5,
		AckWait:       7 * time.Second,
	}
	if _, err := es.CreateReactor(ctx, cfg); err != nil {
		t.Fatalf("create: %v", err)
	}

	r, err := es.GetReactor(ctx, "rt")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	info, err := r.Info(ctx)
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if info.Name != "rt" {
		t.Fatalf("name: got %q want: %q", info.Name, "rt")
	}
	got := info.Config
	if got.Description != cfg.Description {
		t.Fatalf("description: got %q want %q", got.Description, cfg.Description)
	}
	if got.MaxAckPending != cfg.MaxAckPending {
		t.Fatalf("max ack pending: got %d want %d", got.MaxAckPending, cfg.MaxAckPending)
	}
	if got.MaxDeliver != cfg.MaxDeliver {
		t.Fatalf("max deliver: got %d want %d", got.MaxDeliver, cfg.MaxDeliver)
	}
	if got.AckWait != cfg.AckWait {
		t.Fatalf("ack wait: got %v want %v", got.AckWait, cfg.AckWait)
	}
	if len(got.Filters) != 1 || got.Filters[0] != "*.*.order-shipped" {
		t.Fatalf("filters: got %v", got.Filters)
	}
}

// JetStream normalises AckWait to BackOff[0] when BackOff is set; this test
// pins both fields independently so the BackOff round-trip is verified.
func TestCreateReactor_BackOffRoundtrip(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	cfg := ReactorConfig{
		Name:    "bo",
		BackOff: []time.Duration{100 * time.Millisecond, 200 * time.Millisecond},
	}
	if _, err := es.CreateReactor(ctx, cfg); err != nil {
		t.Fatalf("create: %v", err)
	}
	r, err := es.GetReactor(ctx, "bo")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	info, err := r.Info(ctx)
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if len(info.Config.BackOff) != 2 ||
		info.Config.BackOff[0] != 100*time.Millisecond ||
		info.Config.BackOff[1] != 200*time.Millisecond {
		t.Fatalf("backoff: got %v", info.Config.BackOff)
	}
}

func TestCreateReactor_IdempotentOnMatchingConfig(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	cfg := ReactorConfig{Name: "idem", AckWait: 5 * time.Second}
	if _, err := es.CreateReactor(ctx, cfg); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if _, err := es.CreateReactor(ctx, cfg); err != nil {
		t.Fatalf("second create with same config: %v", err)
	}
}

func TestCreateReactor_DifferentConfigReturnsExists(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.CreateReactor(ctx, ReactorConfig{Name: "conflict", AckWait: 5 * time.Second}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err := es.CreateReactor(ctx, ReactorConfig{Name: "conflict", AckWait: 10 * time.Second})
	if !errors.Is(err, ErrReactorExists) {
		t.Fatalf("expected ErrReactorExists, got %v", err)
	}
	if errors.Is(err, jetstream.ErrConsumerExists) {
		t.Fatalf("expected JetStream sentinel to stay internal, got %v", err)
	}
}

func TestUpdateReactor_NotFound(t *testing.T) {
	es := newReactTestStore(t)
	_, err := es.UpdateReactor(context.Background(), ReactorConfig{Name: "missing"})
	if !errors.Is(err, ErrReactorNotFound) {
		t.Fatalf("expected ErrReactorNotFound, got %v", err)
	}
	if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDoesNotExist) {
		t.Fatalf("expected JetStream sentinel to stay internal, got %v", err)
	}
}

func TestUpdateReactor_GetModifyUpdate(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.CreateReactor(ctx, ReactorConfig{Name: "upd", AckWait: 5 * time.Second, MaxAckPending: 3}); err != nil {
		t.Fatal(err)
	}
	r, err := es.GetReactor(ctx, "upd")
	if err != nil {
		t.Fatal(err)
	}
	info, err := r.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg := info.Config
	cfg.AckWait = 12 * time.Second
	if _, err := es.UpdateReactor(ctx, cfg); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, err := es.GetReactor(ctx, "upd")
	if err != nil {
		t.Fatal(err)
	}
	gotInfo, err := got.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if gotInfo.Config.AckWait != 12*time.Second {
		t.Fatalf("ack wait: got %v want 12s", gotInfo.Config.AckWait)
	}
	if gotInfo.Config.MaxAckPending != 3 {
		t.Fatalf("max ack pending should round-trip via Get→Update: got %d want 3", gotInfo.Config.MaxAckPending)
	}
}

func TestUpdateReactor_ReplaceWritesDefaults(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.CreateReactor(ctx, ReactorConfig{Name: "replace", MaxAckPending: 9, AckWait: 8 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := es.UpdateReactor(ctx, ReactorConfig{Name: "replace"}); err != nil {
		t.Fatalf("update with partial config: %v", err)
	}
	r, err := es.GetReactor(ctx, "replace")
	if err != nil {
		t.Fatal(err)
	}
	info, err := r.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxAckPending != 1 {
		t.Fatalf("expected default MaxAckPending=1 written through, got %d", info.Config.MaxAckPending)
	}
	if info.Config.AckWait != 30*time.Second {
		t.Fatalf("expected default AckWait=30s written through, got %v", info.Config.AckWait)
	}
}

func TestCreateOrUpdateReactor_Idempotent(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	cfg := ReactorConfig{Name: "cou", AckWait: 5 * time.Second}
	if _, err := es.CreateOrUpdateReactor(ctx, cfg); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := es.CreateOrUpdateReactor(ctx, cfg); err != nil {
		t.Fatalf("second: %v", err)
	}
	cfg.AckWait = 9 * time.Second
	if _, err := es.CreateOrUpdateReactor(ctx, cfg); err != nil {
		t.Fatalf("third with different config: %v", err)
	}
}

func TestDeleteReactor_NotFound(t *testing.T) {
	es := newReactTestStore(t)
	err := es.DeleteReactor(context.Background(), "nope")
	if !errors.Is(err, ErrReactorNotFound) {
		t.Fatalf("expected ErrReactorNotFound, got %v", err)
	}
	if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDoesNotExist) {
		t.Fatalf("expected JetStream sentinel to stay internal, got %v", err)
	}
}

func TestDeleteReactor_ThenGetReturnsNotFound(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.CreateReactor(ctx, ReactorConfig{Name: "del"}); err != nil {
		t.Fatal(err)
	}
	if err := es.DeleteReactor(ctx, "del"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err := es.GetReactor(ctx, "del")
	if !errors.Is(err, ErrReactorNotFound) {
		t.Fatalf("expected ErrReactorNotFound, got %v", err)
	}
	if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDoesNotExist) {
		t.Fatalf("expected JetStream sentinel to stay internal, got %v", err)
	}
}

func TestListReactors_ReturnsCreated(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.CreateReactor(ctx, ReactorConfig{Name: "list-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := es.CreateReactor(ctx, ReactorConfig{Name: "list-b"}); err != nil {
		t.Fatal(err)
	}
	infos, err := es.ListReactors(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	seen := map[string]bool{}
	for _, info := range infos {
		seen[info.Name] = true
	}
	if !seen["list-a"] || !seen["list-b"] {
		t.Fatalf("expected list-a and list-b in %v", seen)
	}
}

func TestListReactors_ExcludesEphemeralConsumers(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()

	if _, err := es.CreateReactor(ctx, ReactorConfig{Name: "durable-one"}); err != nil {
		t.Fatal(err)
	}

	var events eventSlice
	w, err := es.Watch(ctx, &events, WithNoWait())
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer w.Stop()

	infos, err := es.ListReactors(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(infos) != 1 {
		names := make([]string, 0, len(infos))
		for _, info := range infos {
			names = append(names, info.Name)
		}
		t.Fatalf("expected exactly 1 reactor, got %d: %v", len(infos), names)
	}
	if infos[0].Name != "durable-one" {
		t.Fatalf("expected durable-one, got %q", infos[0].Name)
	}
}

func TestReactor_ConsumeErrHandlerLogsAfterDelete(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	w := &lockedWriter{mu: &mu, buf: &buf}
	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))

	es := newReactTestStoreWithLogger(t, logger)
	ctx := context.Background()

	if _, err := es.CreateReactor(ctx, ReactorConfig{Name: "errh"}); err != nil {
		t.Fatal(err)
	}
	var seen atomic.Int32
	r := mustBindReactor(t, es, "errh", func(_ context.Context, _ *Event) error {
		seen.Add(1)
		return nil
	})
	defer func() { _ = r.Unbind(context.Background()) }()

	if _, err := es.Append(ctx, []*Event{{Entity: "order.1", Data: &OrderPlaced{}}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return seen.Load() >= 1 })

	if err := es.DeleteReactor(ctx, "errh"); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return bytes.Contains(buf.Bytes(), []byte("reactor consume error"))
	})
}

type lockedWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func TestReactor_Bind_ErrorWhenAlreadyBound(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()
	mustCreateReactor(t, es, ReactorConfig{Name: "double-bind"})

	r := mustBindReactor(t, es, "double-bind", func(_ context.Context, _ *Event) error { return nil })
	defer func() { _ = r.Unbind(context.Background()) }()

	err := r.Bind(ctx, func(_ context.Context, _ *Event) error { return nil })
	if !errors.Is(err, ErrReactorAlreadyBound) {
		t.Fatalf("second Bind: want ErrReactorAlreadyBound, got %v", err)
	}
}

func TestReactor_Rebind_AfterUnbind(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()
	mustCreateReactor(t, es, ReactorConfig{Name: "rebind"})

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.1", Data: &OrderPlaced{}},
	}); err != nil {
		t.Fatal(err)
	}

	var hits1 atomic.Int32
	r := mustBindReactor(t, es, "rebind", func(_ context.Context, _ *Event) error {
		hits1.Add(1)
		return nil
	})
	waitFor(t, 2*time.Second, func() bool { return hits1.Load() >= 1 })

	if err := r.Unbind(context.Background()); err != nil {
		t.Fatalf("first Unbind: %v", err)
	}

	if _, err := es.Append(ctx, []*Event{
		{Entity: "order.2", Data: &OrderPlaced{}},
	}); err != nil {
		t.Fatal(err)
	}

	var hits2 atomic.Int32
	if err := r.Bind(ctx, func(_ context.Context, _ *Event) error {
		hits2.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("rebind: %v", err)
	}
	defer func() { _ = r.Unbind(context.Background()) }()
	waitFor(t, 2*time.Second, func() bool { return hits2.Load() >= 1 })
}

func TestReactor_Info_FreshSnapshot(t *testing.T) {
	es := newReactTestStore(t)
	ctx := context.Background()
	mustCreateReactor(t, es, ReactorConfig{Name: "info-test", AckWait: 7 * time.Second})

	r := mustBindReactor(t, es, "info-test", func(_ context.Context, _ *Event) error { return nil })
	defer func() { _ = r.Unbind(context.Background()) }()

	info, err := r.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Name != "info-test" {
		t.Fatalf("Info().Name: want %q, got %q", "info-test", info.Name)
	}
	if info.Config.AckWait != 7*time.Second {
		t.Fatalf("Info().Config.AckWait: want 7s, got %v", info.Config.AckWait)
	}
}

func TestReactor_Name(t *testing.T) {
	es := newReactTestStore(t)
	mustCreateReactor(t, es, ReactorConfig{Name: "named"})

	r := mustBindReactor(t, es, "named", func(_ context.Context, _ *Event) error { return nil })
	defer func() { _ = r.Unbind(context.Background()) }()

	if got := r.Name(); got != "named" {
		t.Fatalf("Name(): want %q, got %q", "named", got)
	}
}

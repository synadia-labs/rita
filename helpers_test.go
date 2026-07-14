package rita

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-labs/rita/testutil"
	"github.com/synadia-labs/rita/types"
)

// storeHandle builds an unscoped handle the way the Manager constructors do,
// for unit tests that exercise subject construction without a server.
func storeHandle(name string) *EventStore {
	return &EventStore{name: name, stream: streamName(name), prefix: subjectRoot(name)}
}

// tenantHandle builds a tenant-scoped handle through the production Tenant()
// path so prefix derivation is exercised rather than restated.
func tenantHandle(t *testing.T, name, tenant string) *EventStore {
	t.Helper()
	base := storeHandle(name)
	base.tenantMode = true
	ten, err := base.Tenant(tenant)
	if err != nil {
		t.Fatal(err)
	}
	return ten
}

// newTestManager starts an embedded NATS server and returns a Manager built
// with the shared test registry.
func newTestManager(tb testing.TB, opts ...ManagerOption) (*Manager, context.Context) {
	tb.Helper()

	srv := testutil.NewNatsServer(tb)
	tb.Cleanup(func() { testutil.ShutdownNatsServer(srv) })

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		tb.Fatalf("connect: %v", err)
	}
	tb.Cleanup(nc.Close)

	tr, err := types.NewRegistry(registry)
	if err != nil {
		tb.Fatalf("registry: %v", err)
	}

	m, err := New(nc, append([]ManagerOption{WithRegistry(tr)}, opts...)...)
	if err != nil {
		tb.Fatalf("manager: %v", err)
	}

	return m, context.Background()
}

// newTestStore returns a store named "store" backed by a fresh embedded server.
func newTestStore(tb testing.TB, opts ...ManagerOption) *EventStore {
	tb.Helper()
	m, ctx := newTestManager(tb, opts...)
	es, err := m.CreateEventStore(ctx, EventStoreConfig{Name: "store"})
	if err != nil {
		tb.Fatalf("create store: %v", err)
	}
	return es
}

// assertNoJetStreamSentinel pins that rita-level errors do not leak the
// underlying JetStream consumer-not-found sentinels into callers' errors.Is
// checks.
func assertNoJetStreamSentinel(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDoesNotExist) {
		t.Fatalf("expected JetStream sentinel to stay internal, got %v", err)
	}
}

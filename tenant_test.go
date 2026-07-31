package rita

import (
	"sort"
	"testing"

	"github.com/synadia-labs/rita/testutil"
)

// --- Unit tests: subject construction & validation (no server) ---

// TestTenantSubjectScoping pins that the untenanted path is byte-identical to
// today and the tenant token is inserted between the store name and the body.
func TestTenantSubjectScoping(t *testing.T) {
	is := testutil.NewIs(t)

	base := storeHandle("demo") // untenanted
	is.Equal(base.subjectPrefix(""), "$ES.demo.")
	is.Equal(base.subjectPrefix("*.*.*"), "$ES.demo.*.*.*")
	is.Equal(base.eventSubject(&Event{Entity: "order.1", Type: "order-placed"}), "$ES.demo.order.1.order-placed")

	ten := tenantHandle(t, "demo", "acme")
	is.Equal(ten.subjectPrefix(""), "$ES.demo.acme.")
	is.Equal(ten.subjectPrefix("*.*.*"), "$ES.demo.acme.*.*.*")
	is.Equal(ten.eventSubject(&Event{Entity: "order.1", Type: "order-placed"}), "$ES.demo.acme.order.1.order-placed")
}

// TestFiltersToSubjectsTenantScoped verifies a tenant handle confines filters to
// its tenant, defaults empty filters to the tenant pattern rather than the whole
// stream, and leaves the untenanted "no filters means whole stream" untouched.
func TestFiltersToSubjectsTenantScoped(t *testing.T) {
	is := testutil.NewIs(t)
	ten := tenantHandle(t, "demo", "acme")

	got, err := ten.filtersToSubjects(nil)
	is.NoErr(err)
	is.Equal(got, []string{"$ES.demo.acme.*.*.*"})

	got, err = ten.filtersToSubjects([]string{"order.1"})
	is.NoErr(err)
	is.Equal(got, []string{"$ES.demo.acme.order.1.*"})

	// Untenanted empty stays whole-stream (byte-identical to today).
	base := storeHandle("demo")
	got, err = base.filtersToSubjects(nil)
	is.NoErr(err)
	is.Equal(got, []string{})
}

// TestSubjectsToFiltersTenantScoped verifies the reactor round-trip strips the
// tenant-aware prefix back to the user-facing filter form.
func TestSubjectsToFiltersTenantScoped(t *testing.T) {
	is := testutil.NewIs(t)
	ten := tenantHandle(t, "demo", "acme")

	got := ten.subjectsToFilters([]string{"$ES.demo.acme.*.*.order-shipped"})
	is.Equal(got, []string{"*.*.order-shipped"})

	// The tenant-wide pattern collapses back to empty, inverting the empty-filter
	// default so a reactor created with no filters round-trips to no filters
	// rather than appearing to carry an explicit "*.*.*".
	roundTrip, err := ten.filtersToSubjects(nil)
	is.NoErr(err)
	is.Equal(roundTrip, []string{"$ES.demo.acme.*.*.*"})
	is.Equal(ten.subjectsToFilters(roundTrip), []string(nil))
}

// TestTenantValidation pins the charset rules and mode guard on Tenant().
func TestTenantValidation(t *testing.T) {
	is := testutil.NewIs(t)
	es := storeHandle("demo")
	es.tenantMode = true

	for _, bad := range []string{"", "a.b", "a*", "a>", "a b", "a\tb"} {
		_, err := es.Tenant(bad)
		is.Err(err, ErrTenantInvalid)
	}

	sc, err := es.Tenant("acme")
	is.NoErr(err)
	is.Equal(sc.tenant, "acme")

	// Re-scoping replaces rather than nests.
	sc2, err := sc.Tenant("beta")
	is.NoErr(err)
	is.Equal(sc2.tenant, "beta")
	// Original handle is left unchanged (immutable scope).
	is.Equal(sc.tenant, "acme")

	// A non-tenant store cannot be scoped.
	base := storeHandle("demo")
	_, err = base.Tenant("acme")
	is.Err(err, ErrTenantNotSupported)
}

// --- Integration tests (with server) ---

// TestTenantStoreEnforcement verifies two-way enforcement: every store-building
// operation on an unscoped tenant handle is rejected, a scoped handle works, and
// a non-tenant store refuses scoping.
func TestTenantStoreEnforcement(t *testing.T) {
	is := testutil.NewIs(t)
	m, ctx := newTestManager(t)

	es, err := m.CreateEventStore(ctx, EventStoreConfig{Name: "tstore", Tenancy: true})
	is.NoErr(err)

	// Unscoped operations on a tenant store are all rejected.
	_, err = es.Append(ctx, []*Event{{Entity: "order.1", Data: &OrderPlaced{}}})
	is.Err(err, ErrTenantRequired)

	var sink eventSlice
	_, err = es.Evolve(ctx, &sink)
	is.Err(err, ErrTenantRequired)

	_, err = es.Watch(ctx, &sink)
	is.Err(err, ErrTenantRequired)

	_, _, err = es.Decide(ctx, &OrderStats{}, &Command{Data: &PlaceOrder{}})
	is.Err(err, ErrTenantRequired)

	_, _, err = es.DecideAndEvolve(ctx, &OrderStats{}, &Command{Data: &PlaceOrder{}})
	is.Err(err, ErrTenantRequired)

	_, err = es.CreateReactor(ctx, ReactorConfig{Name: "r", Filters: []string{"*.*.order-placed"}})
	is.Err(err, ErrTenantRequired)

	_, err = es.UpdateReactor(ctx, ReactorConfig{Name: "r", Filters: []string{"*.*.order-placed"}})
	is.Err(err, ErrTenantRequired)

	_, err = es.CreateOrUpdateReactor(ctx, ReactorConfig{Name: "r", Filters: []string{"*.*.order-placed"}})
	is.Err(err, ErrTenantRequired)

	err = es.DeleteReactor(ctx, "r")
	is.Err(err, ErrTenantRequired)

	// A scoped handle works.
	acme, err := es.Tenant("acme")
	is.NoErr(err)
	seq, err := acme.Append(ctx, []*Event{{Entity: "order.1", Data: &OrderPlaced{}}})
	is.NoErr(err)
	is.Equal(seq, uint64(1))

	// A non-tenant store refuses scoping.
	plain, err := m.CreateEventStore(ctx, EventStoreConfig{Name: "pstore"})
	is.NoErr(err)
	_, err = plain.Tenant("acme")
	is.Err(err, ErrTenantNotSupported)
}

// TestTenantIsolation proves two tenants sharing an entity id are isolated on the
// wire and in the optimistic-concurrency sequence space.
func TestTenantIsolation(t *testing.T) {
	is := testutil.NewIs(t)
	m, ctx := newTestManager(t)

	es, err := m.CreateEventStore(ctx, EventStoreConfig{Name: "iso", Tenancy: true})
	is.NoErr(err)

	acme, err := es.Tenant("acme")
	is.NoErr(err)
	beta, err := es.Tenant("beta")
	is.NoErr(err)

	// Both tenants append to the "same" entity order.1 expecting an empty
	// subject sequence; isolation means neither conflicts with the other.
	s1, err := acme.Append(ctx, []*Event{{Entity: "order.1", Data: &OrderPlaced{}, Expect: ExpectSequence(0)}})
	is.NoErr(err)
	is.Equal(s1, uint64(1))

	s2, err := beta.Append(ctx, []*Event{{Entity: "order.1", Data: &OrderPlaced{}, Expect: ExpectSequence(0)}})
	is.NoErr(err)
	is.Equal(s2, uint64(2))

	// Each tenant evolves only its own event, at its own subject.
	var av eventSlice
	_, err = acme.Evolve(ctx, &av)
	is.NoErr(err)
	is.Equal(len(av), 1)
	is.Equal(av[0].Subject(), "$ES.iso.acme.order.1.order-placed")

	var bv eventSlice
	_, err = beta.Evolve(ctx, &bv)
	is.NoErr(err)
	is.Equal(len(bv), 1)
	is.Equal(bv[0].Subject(), "$ES.iso.beta.order.1.order-placed")
}

// TestUntenantedSubjectUnchanged confirms an untenanted store publishes the exact
// subject it does today.
func TestUntenantedSubjectUnchanged(t *testing.T) {
	is := testutil.NewIs(t)
	m, ctx := newTestManager(t)

	es, err := m.CreateEventStore(ctx, EventStoreConfig{Name: "plain"})
	is.NoErr(err)

	_, err = es.Append(ctx, []*Event{{Entity: "order.1", Data: &OrderPlaced{}}})
	is.NoErr(err)

	var ev eventSlice
	_, err = es.Evolve(ctx, &ev)
	is.NoErr(err)
	is.Equal(len(ev), 1)
	is.Equal(ev[0].Subject(), "$ES.plain.order.1.order-placed")
}

// TestTenantModePersisted verifies the mode is discovered from stream metadata by
// a fresh handle and is not demoted by an update that omits Tenancy.
func TestTenantModePersisted(t *testing.T) {
	is := testutil.NewIs(t)
	m, ctx := newTestManager(t)

	_, err := m.CreateEventStore(ctx, EventStoreConfig{Name: "persist", Tenancy: true})
	is.NoErr(err)

	// A fresh handle via Get detects tenant mode from metadata.
	got, err := m.GetEventStore(ctx, "persist")
	is.NoErr(err)
	_, err = got.Append(ctx, []*Event{{Entity: "order.1", Data: &OrderPlaced{}}})
	is.Err(err, ErrTenantRequired)

	acme, err := got.Tenant("acme")
	is.NoErr(err)
	_, err = acme.Append(ctx, []*Event{{Entity: "order.1", Data: &OrderPlaced{}}})
	is.NoErr(err)

	// An update that forgets Tenancy must NOT demote the store to untenanted.
	err = m.UpdateEventStore(ctx, EventStoreConfig{Name: "persist", Description: "updated"})
	is.NoErr(err)

	got2, err := m.GetEventStore(ctx, "persist")
	is.NoErr(err)
	_, err = got2.Append(ctx, []*Event{{Entity: "order.2", Data: &OrderPlaced{}}})
	is.Err(err, ErrTenantRequired)
}

// TestTenantReactorRoundTrip verifies a reactor created on a tenant handle scopes
// its filter subjects to the tenant and round-trips back to the user form.
func TestTenantReactorRoundTrip(t *testing.T) {
	is := testutil.NewIs(t)
	m, ctx := newTestManager(t)

	es, err := m.CreateEventStore(ctx, EventStoreConfig{Name: "rstore", Tenancy: true})
	is.NoErr(err)

	acme, err := es.Tenant("acme")
	is.NoErr(err)

	r, err := acme.CreateReactor(ctx, ReactorConfig{Name: "shipper", Filters: []string{"*.*.order-shipped"}})
	is.NoErr(err)

	info, err := r.Info(ctx)
	is.NoErr(err)
	is.Equal(info.Config.Filters, []string{"*.*.order-shipped"})

	// A scoped handle can delete its reactor.
	err = acme.DeleteReactor(ctx, "shipper")
	is.NoErr(err)
}

// TestTenantReactorLookupScoped verifies that reactor lookups are tenant-scoped:
// a scoped handle sees only its own reactors and cannot fetch another tenant's,
// while the unscoped handle sees every reactor across tenants. This is the
// isolation that lets each tenant manage its reactors without observing or
// fetching durables belonging to other tenants.
func TestTenantReactorLookupScoped(t *testing.T) {
	is := testutil.NewIs(t)
	m, ctx := newTestManager(t)

	es, err := m.CreateEventStore(ctx, EventStoreConfig{Name: "rlookup", Tenancy: true})
	is.NoErr(err)

	acme, err := es.Tenant("acme")
	is.NoErr(err)
	beta, err := es.Tenant("beta")
	is.NoErr(err)

	_, err = acme.CreateReactor(ctx, ReactorConfig{Name: "acme-shipper", Filters: []string{"*.*.order-shipped"}})
	is.NoErr(err)
	_, err = beta.CreateReactor(ctx, ReactorConfig{Name: "beta-shipper", Filters: []string{"*.*.order-shipped"}})
	is.NoErr(err)

	// Each scoped handle lists only its own reactor.
	acmeList, err := acme.ListReactors(ctx)
	is.NoErr(err)
	is.Equal(reactorNames(acmeList), []string{"acme-shipper"})

	betaList, err := beta.ListReactors(ctx)
	is.NoErr(err)
	is.Equal(reactorNames(betaList), []string{"beta-shipper"})

	// The unscoped handle sees both, across tenants.
	allList, err := es.ListReactors(ctx)
	is.NoErr(err)
	names := reactorNames(allList)
	sort.Strings(names)
	is.Equal(names, []string{"acme-shipper", "beta-shipper"})

	// GetReactor is invisible across tenants, but works within scope and unscoped.
	_, err = acme.GetReactor(ctx, "beta-shipper")
	is.Err(err, ErrReactorNotFound)

	_, err = acme.GetReactor(ctx, "acme-shipper")
	is.NoErr(err)

	_, err = es.GetReactor(ctx, "beta-shipper")
	is.NoErr(err)
}

func reactorNames(rs []*ReactorInfo) []string {
	names := make([]string, len(rs))
	for i, r := range rs {
		names[i] = r.Name
	}
	return names
}

// TestTenantReactorMutationScoped pins write-side isolation: lookup scoping
// makes foreign reactors invisible, so mutations must refuse them too —
// otherwise a scoped handle could delete or overwrite a durable it cannot even
// see, by guessing its name. Delete/Update surface the foreign durable as
// absent (matching GetReactor); the upsert surfaces the name as taken rather
// than silently rewriting the foreign durable's filters to its own tenant.
func TestTenantReactorMutationScoped(t *testing.T) {
	is := testutil.NewIs(t)
	m, ctx := newTestManager(t)

	es, err := m.CreateEventStore(ctx, EventStoreConfig{Name: "rmut", Tenancy: true})
	is.NoErr(err)

	acme, err := es.Tenant("acme")
	is.NoErr(err)
	beta, err := es.Tenant("beta")
	is.NoErr(err)

	_, err = beta.CreateReactor(ctx, ReactorConfig{Name: "beta-shipper", Filters: []string{"*.*.order-shipped"}})
	is.NoErr(err)

	// A foreign durable cannot be deleted or updated; it reads as absent.
	err = acme.DeleteReactor(ctx, "beta-shipper")
	is.Err(err, ErrReactorNotFound)

	_, err = acme.UpdateReactor(ctx, ReactorConfig{Name: "beta-shipper", Filters: []string{"*.*.order-placed"}})
	is.Err(err, ErrReactorNotFound)

	// An upsert cannot steal it either; the name reads as taken.
	_, err = acme.CreateOrUpdateReactor(ctx, ReactorConfig{Name: "beta-shipper", Filters: []string{"*.*.order-placed"}})
	is.Err(err, ErrReactorExists)

	// The foreign durable is untouched: still beta's, with its original filter.
	r, err := beta.GetReactor(ctx, "beta-shipper")
	is.NoErr(err)
	info, err := r.Info(ctx)
	is.NoErr(err)
	is.Equal(info.Config.Filters, []string{"*.*.order-shipped"})

	// The owner retains full mutation rights.
	_, err = beta.CreateOrUpdateReactor(ctx, ReactorConfig{Name: "beta-shipper", Filters: []string{"*.*.order-returned"}})
	is.NoErr(err)
	err = beta.DeleteReactor(ctx, "beta-shipper")
	is.NoErr(err)
}

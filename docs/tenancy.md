# Multi-tenancy

A **tenant store** lets one event store serve many isolated tenants without a
stream per tenant. Tenancy inserts a tenant token into every event subject, so
each tenant's events occupy a disjoint slice of the subject space within the
same JetStream stream.

```
 Untenanted:  $ES.orders.order.1001.order-shipped
 Tenant:      $ES.orders.acme.order.1001.order-shipped
                         ^^^^ tenant token
```

## Enabling tenancy

Set `Tenancy` when creating the store:

```go
es, err := mgr.CreateEventStore(ctx, rita.EventStoreConfig{
	Name:    "orders",
	Tenancy: true,
})
```

This records a marker in the stream's metadata. **Tenancy is fixed at creation
and immutable** — `UpdateEventStore` always preserves the existing mode, so an
update that forgets the flag cannot silently demote a tenant store, and you
cannot convert an untenanted store into a tenant one. `GetEventStore` reads the
marker back, so a handle always knows whether it is tenanted.

## Scoping a handle

On a tenant store, the bare handle returned by `CreateEventStore`/`GetEventStore`
is **unscoped** and cannot perform store operations. Derive a scoped handle with
`Tenant`:

```go
acme, err := es.Tenant("acme")
if err != nil {
	return err
}

acme.Append(ctx, []*rita.Event{
	{Entity: "order.1001", Data: &OrderPlaced{}},
})
```

`Tenant` returns a derived handle and leaves the receiver unchanged, so scopes
are immutable and cheap to derive — hand each request its own scoped handle.
Re-scoping replaces rather than nests: `es.Tenant("a").Tenant("b")` is tenant
`"b"`.

A tenant token is a single subject token: it must be non-empty and may not
contain `.`, `*`, `>`, or whitespace. An invalid token returns
`ErrTenantInvalid`.

> **Full example:** [`examples/tenancy/main.go`](../examples/tenancy/main.go) —
> creating the tenant store is lines 67–68, the unscoped-handle guard
> (`ErrTenantRequired`) is 73–79, deriving per-tenant handles is 81–90, and the
> per-tenant read isolation is 104–115.

## How scoping affects operations

Every store operation enforces the rule that **a tenant store requires a tenant
scope**:

| Situation | Result |
| --- | --- |
| Operation on a scoped handle (`es.Tenant("acme")`) | Confined to that tenant. |
| Operation on an unscoped handle of a tenant store | `ErrTenantRequired` |
| `Tenant(...)` on an untenanted store | `ErrTenantNotSupported` |

Scoping flows through transparently:

- **Appends** publish under the tenant's subject prefix.
- **`Evolve` / `Watch`** are confined to the tenant. A scoped handle with no
  explicit filters defaults to the tenant-wide pattern (not the whole stream),
  so you can never accidentally read across tenants.
- **Filters** are still written in their normal
  [user form](./reading-state.md#filters) (`order.1001`,
  `*.*.order-shipped`); Rita inserts the tenant token for you. You never write
  the tenant into a filter yourself.

The untenanted code path is byte-for-byte unchanged: on a store created without
tenancy, subjects and filters look exactly as they would without this feature.

## Reactors are stream-global

Tenancy scopes *subjects*, but durable consumer **names** share a single
stream-wide namespace. This has a deliberate, sharp edge:

- Reactor **mutations** (`CreateReactor`, `UpdateReactor`,
  `CreateOrUpdateReactor`, `DeleteReactor`) require a tenant scope, because they
  build tenant-scoped filter subjects.
- Reactor **lookups** (`GetReactor`, `ListReactors`) are **not** tenant-filtered.
  `ListReactors` on a tenant store returns durables created under every tenant.

Because the lookup is stream-global, `ListReactors` decodes each durable's
filters by stripping the *calling* handle's tenant prefix. A durable that
belongs to the calling tenant comes back in user form
(`*.*.order-shipped`); a durable from another tenant doesn't match the prefix
and is surfaced verbatim
(`$ES.orders.other-tenant.*.*.order-shipped`).

**If you need per-tenant reactor isolation, namespace the durable names
yourself**, e.g. `"acme-shipping-notifier"`:

```go
acme, _ := es.Tenant("acme")
acme.CreateReactor(ctx, rita.ReactorConfig{
	Name:    "acme-shipping-notifier", // tenant-prefixed name
	Filters: []string{"*.*.order-shipped"},
})
```

Library-side name namespacing is a possible future addition.

## Choosing tenancy vs. separate stores

Tenancy keeps every tenant in one stream, which means shared retention limits,
shared replication, and a shared consumer namespace. Reach for it when tenants
are numerous and individually small, and when uniform configuration is
acceptable. When tenants need independent retention, placement, or per-tenant
operational isolation, a stream (event store) per tenant is the better fit.

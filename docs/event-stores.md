# Event Stores

An **event store** is a JetStream stream plus the operations Rita layers on top
of it: appending events, replaying them into state, watching them live, and
running reactors. You obtain event stores from a **Manager**.

## The manager

A `Manager` is the entry point. It holds the shared dependencies every store it
creates will use, so they are configured once:

```go
mgr, err := rita.New(nc,
	rita.WithRegistry(registry), // type registry (see events-and-types.md)
	rita.WithLogger(logger),     // *slog.Logger; defaults to slog.Default()
)
```

`New` takes an established `*nats.Conn` and a set of options:

| Option | Purpose | Default |
| --- | --- | --- |
| `WithRegistry(*types.Registry)` | Type registry used to (de)serialize event data. | none — raw `[]byte` data only |
| `WithClock(clock.Clock)` | Source of event timestamps. | `clock.Time` (wall clock) |
| `WithIDer(id.ID)` | Generates event IDs when one is not supplied. | `id.NUID` |
| `WithLogger(*slog.Logger)` | Logger for background errors (watchers, reactors). | `slog.Default()` |
| `WithAPIPrefix(string)` | Custom JetStream API prefix (e.g. for a leaf/domain). | none |

The clock and ID generator are injectable so tests can make event timestamps
and IDs deterministic; the [`testutil`](../testutil) package provides fakes.

> **Without a registry.** A manager created without `WithRegistry` still works,
> but event `Data` must be raw `[]byte` and each event must set `Type`
> explicitly. See [Events & types](./events-and-types.md#without-a-registry).

## Store lifecycle

### Create

```go
es, err := mgr.CreateEventStore(ctx, rita.EventStoreConfig{
	Name:     "orders",
	Storage:  jetstream.FileStorage,
	Replicas: 3,
	MaxAge:   90 * 24 * time.Hour,
})
```

`CreateEventStore` provisions the underlying stream (named `ES_orders`) and
returns a handle. It returns an error if a stream with that name already exists.

> **Full example:** constructing the manager and creating a store is
> [`examples/quickstart/main.go`](../examples/quickstart/main.go), lines 80–91.

`EventStoreConfig` maps directly onto JetStream stream settings:

| Field | Notes |
| --- | --- |
| `Name` | Required. Becomes stream `ES_<name>` with subjects `$ES.<name>.>`. |
| `Description` | Free-form stream description. |
| `Metadata` | Custom stream metadata (key/value). |
| `Replicas` | RAFT replica count. |
| `Storage` | `jetstream.FileStorage` or `jetstream.MemoryStorage`. |
| `Placement` | Cluster placement constraints. |
| `RePublish` | JetStream re-publish configuration. |
| `MaxMsgs` / `MaxAge` / `MaxBytes` | Retention limits. |
| `Tenancy` | Create as a [tenant store](./tenancy.md). Immutable after creation. |

Rita always creates the stream with `AllowAtomicPublish` and `AllowDirect`
enabled — the former backs [atomic batch appends](./deciders-and-evolvers.md#append),
the latter enables efficient direct reads.

### Get

```go
es, err := mgr.GetEventStore(ctx, "orders")
```

`GetEventStore` returns a handle to an existing store. It verifies the stream
exists (returning an error otherwise) and detects whether the store was created
with [tenancy](./tenancy.md) enabled, so the returned handle enforces the right
mode.

### Update

```go
err := mgr.UpdateEventStore(ctx, rita.EventStoreConfig{
	Name:   "orders",
	MaxAge: 180 * 24 * time.Hour,
})
```

`UpdateEventStore` reconfigures the stream. Two behaviors are worth knowing:

- **Tenancy is immutable.** The existing stream's tenancy mode is preserved
  regardless of `config.Tenancy`, so an update that forgets to set the flag
  cannot silently demote a tenant store.
- **Metadata is merged, not replaced.** Keys present in `config.Metadata` are
  written (overriding any prior value); keys you omit are preserved. Removing a
  metadata key is therefore not expressible through `Update` today.

### Delete

```go
err := mgr.DeleteEventStore(ctx, "orders")
```

Deletes the stream and everything in it. This is irreversible.

## What you do with a store

Once you hold an `*EventStore`, the rest of Rita's API hangs off it:

| Operation | Doc |
| --- | --- |
| `Append` events | [Deciders & evolvers](./deciders-and-evolvers.md#append) |
| `Decide` / `DecideAndEvolve` | [Deciders & evolvers](./deciders-and-evolvers.md) |
| `Evolve` state on demand | [Reading state](./reading-state.md#evolve) |
| `Watch` events live | [Reading state](./reading-state.md#watch) |
| Reactor management (`CreateReactor`, …) | [Reactors](./reactors.md) |
| `Tenant` scoping | [Multi-tenancy](./tenancy.md) |

A store handle is cheap and safe to share across goroutines.

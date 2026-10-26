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

Deletes the stream and all its events. You cannot undo this. Rita does **not**
delete the snapshot bucket. Delete it yourself if you do not need it.

If you create a store with the same name again, Rita does not use the old
snapshots. `LoadModel` returns `ErrSnapshotStale`. To continue, change
`Version` or delete the snapshot bucket.

## Snapshot storage

Each store keeps its snapshots in a KV bucket named `ES_SNAP_<store name>`.
`LoadModel` creates the bucket if it does not exist. Each snapshot config
has one key in the bucket.

The store name must contain only letters, digits, `-` and `_`. Other names
cannot make a valid bucket name, and `LoadModel` returns
`ErrSnapshotConfigInvalid`.

The new bucket uses the same storage type and replica count as the stream. It
keeps one revision per key and has no TTL. `UpdateEventStore` does not change
the bucket.

Your credentials need permission to read stream info and to read and write the
bucket. If your credentials cannot create buckets, create the bucket before
you start.

Each snapshot must fit in one KV value. The server `max_payload` and the
bucket `MaxValueSize` set the limit. If a snapshot is too large, the save
fails and the previous snapshot stays.

Snapshots do not make it safe to remove events from the stream. See
[When a snapshot is rejected](./reading-state.md#when-a-snapshot-is-rejected).

These errors mean that the code uses the API incorrectly:

| Error | Cause |
| --- | --- |
| `ErrSnapshotConfigInvalid` | The state is nil or not a pointer, `Name` or `Codec` is missing, the codec has no name, or the store name is not valid. |
| `ErrSnapshotNotConfigured` | You called `SaveSnapshot` on a model from `NewModel`. |
| `ErrSnapshotWrongStore` | You used the model with a different store or tenant. |
| `ErrSnapshotSkipsEvents` | A start sequence would skip events that the model did not apply. |
| `ErrModelBusy` | Another `Evolve` or `Watch` already uses the model. |

## What you do with a store

Once you hold an `*EventStore`, the rest of Rita's API hangs off it:

| Operation | Doc |
| --- | --- |
| `Append` events | [Deciders & evolvers](./deciders-and-evolvers.md#append) |
| `Decide` / `DecideAndEvolve` | [Deciders & evolvers](./deciders-and-evolvers.md) |
| `Evolve` state on demand | [Reading state](./reading-state.md#evolve) |
| `Watch` events live | [Reading state](./reading-state.md#watch) |
| `LoadModel` / `SaveSnapshot` | [Snapshots](./reading-state.md#snapshots) |
| Reactor management (`CreateReactor`, …) | [Reactors](./reactors.md) |
| `Tenant` scoping | [Multi-tenancy](./tenancy.md) |

A store handle is cheap and safe to share across goroutines.

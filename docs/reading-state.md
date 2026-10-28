# Reading State

State in an event-sourced system is derived from events. Rita gives you two ways
to derive it from the log:

- **`Evolve`** — replay events once, on demand, to build state up to *now*.
- **`Watch`** — subscribe and keep a model continuously up to date as new events
  arrive.

Both apply events to an [`Evolver`](./deciders-and-evolvers.md#the-interfaces)
and both accept the same [filters](#filters).

## Evolve

`Evolve` folds the matching events through your model and returns the sequence
of the last event applied:

```go
var order Order
lastSeq, err := es.Evolve(ctx, &order, rita.WithFilters("order.1001"))
```

> **Full example:** [`examples/quickstart/main.go`](../examples/quickstart/main.go) —
> appending events and then rebuilding state with `Evolve` is lines 93–107.

It builds an ephemeral consumer, applies exactly the currently-pending events,
then tears the consumer down. It is a one-shot, synchronous catch-up: when it
returns, `order` reflects every event in scope at the moment the call started.
If nothing matches, it returns `0` and leaves the model untouched.

Use `Evolve` for request/response work: load an entity's state, make a decision,
move on. For a model you keep in memory across requests, pair it with
[`DecideAndEvolve`](./deciders-and-evolvers.md#decideandevolve) so each write
advances the same in-memory state.

## Watch

`Watch` runs continuously. It applies all existing events (catching up first),
then keeps applying new ones as they are appended, until you `Stop` it:

```go
model := rita.NewModel(&Stats{})

w, err := es.Watch(ctx, model, rita.WithFilters("*.*.order-shipped"))
if err != nil {
	return err
}
defer w.Stop()
```

Because events are applied from a background goroutine, **the model must be
thread-safe**. Use [`NewModel`](./deciders-and-evolvers.md#model), which guards
state with a mutex, and read through `View`:

```go
model.View(ctx, func(s *Stats) error {
	fmt.Println("shipped so far:", s.Shipped)
	return nil
})
```

By default `Watch` blocks until the initial catch-up is complete, so the model
is current before the call returns.

> **Full example:** [`examples/projection/main.go`](../examples/projection/main.go) —
> the `Stats` read model is lines 25–39, starting the watch is 91–96, and reading
> the live projection through `View` is 106–128.

Watch-specific options:

| Option | Effect |
| --- | --- |
| `WithNoWait()` | Return immediately without waiting for catch-up. |
| `WithErrHandler(fn)` | Handle unpack/evolve errors yourself. Default logs via the store's logger. |

The error handler signature is `func(err error, ev *Event, msg jetstream.Msg)`,
giving you the failing event and raw message for logging or dead-lettering.

`ctx` governs setup and the initial catch-up; once `Watch` returns, the
watcher's lifetime is owned by `Stop()`, which cancels delivery and drains any
buffered events.

### Watch vs. reactors

`Watch` keeps **in-process** read models current — it has no acknowledgements
and no durability, so a restart re-reads from the beginning (or from a sequence
you provide). When you need **durable, at-least-once** processing for side
effects that must not be lost across restarts, use a
[reactor](./reactors.md) instead.

## Snapshots

A snapshot keeps a copy of a model's state outside the event stream. When you
load the model again, Rita only replays the events after the snapshot.
Snapshots are optional. `NewModel` does not use them.

```go
model, err := rita.LoadModel(ctx, es, &Order{}, rita.SnapshotConfig{
	Name:    "order-1001",
	Version: "v1",
	Filters: []string{"order.1001"},
	Codec:   codec.JSON,
})
if err != nil {
	return err
}

if err := model.SaveSnapshot(ctx); err != nil {
	return err
}
```

- **Set a codec.** The codec encodes your state. It does not encode events.
  JSON and MsgPack do not save unexported fields. To save them, use
  `codec.Binary` with `MarshalBinary` and `UnmarshalBinary`.
- **Give `LoadModel` new, empty state.** Do not change that pointer after
  the call. Use `View` to read the state.
- **Change `Version`** when you change the state fields or the `Evolve` logic.
  Rita then ignores the old snapshot and replays all events.

### Where snapshots are stored

Snapshots live in an Object Store beside the stream, so a snapshot can be
larger than the server's maximum message size. See
[Snapshot storage](./event-stores.md#snapshot-storage) for the bucket names,
permissions, and which snapshots Rita keeps.

`LoadModel` gives each snapshot download attempt 30 seconds when `ctx` has
no deadline. If a save replaces the snapshot during a download, Rita tries
again. It makes up to three attempts. A deadline on `ctx` limits the whole
load, including all attempts. For a large snapshot, pass a `ctx` with a longer
deadline.

### What to snapshot

Use a snapshot when a model gets many events but its state stays a size you
can hold in memory. Each save encodes the whole state, and each load reads
the whole snapshot into memory.

A model that watches the whole store also keeps a sequence for every entity
it sees, even for events it ignores. The snapshot stores these sequences too.
Set `Filters` to keep that list to the model's own entities.

Do not use a snapshot for:

- **State that grows without limit**, such as a full history. Use a
  [reactor](./reactors.md) that writes one KV key for each entity.
- **Secrets or data you must be able to erase.** A snapshot is a second copy
  of the persisted state, outside the stream, and Rita does not encrypt it.

If a save fails, the previous snapshot stays. `LoadModel` then replays more
events at each start.

### Keep a snapshot current

`DecideAndEvolve` changes the state, but it does not move the snapshot
position. Call `es.Evolve` before you save:

```go
if _, err := es.Evolve(ctx, model); err != nil {
	return err
}
if err := model.SaveSnapshot(ctx); err != nil {
	return err
}
```

`es.Watch(ctx, model)` also moves the position, and it uses the saved
filters. Run only one `Evolve` or `Watch` at a time on a model. At shutdown,
stop the watch before you save.

`es.Evolve` and the catch-up of `es.Watch` move the position to the end of
the stream, also past events that your filters do not match. After the
catch-up, a watch moves the position only for events that your filters match.
If the stream has retention limits and your entities get few events, call
`es.Evolve` before you save. Then retention of other events cannot cause
`ErrSnapshotHistoryGap`.

Rita does not save snapshots automatically. Your application calls
`SaveSnapshot`. If the stored snapshot is the same or newer, the save does
nothing. If two writers save at the same time, one gets `ErrSnapshotConflict`.
Try the save again.

### When a snapshot is rejected

If there is no snapshot, Rita replays all events. In all other cases, a
problem with the snapshot returns an error:

| Error | Cause |
| --- | --- |
| `ErrSnapshotInvalid` | Rita cannot decode the stored snapshot, or the snapshot it names is missing. |
| `ErrSnapshotStale` | Someone deleted and created the stream again, or the stream is behind the snapshot. Change `Version`. |
| `ErrSnapshotHistoryGap` | Retention or a purge removed events after the snapshot. This applies to all events in the stream, not only the model's filters. |
| `ErrSnapshotScopeChanged` | You gave `Evolve` or `Watch` different filters. |
| `ErrEvolvedOutsideStore` | You called `model.Evolve` directly. Use `es.Evolve` or `es.Watch`. |

If `Evolve` fails, or a watch cannot apply an event, the model stops. It
cannot decide or save, and `EvolveErr` returns the failure. Load a new model.
See [failed evolves](./deciders-and-evolvers.md#failed-evolves).

> **Full runnable example:** [`examples/snapshots/main.go`](../examples/snapshots/main.go)

## Filters

Both `Evolve` and `Watch` accept `WithFilters(patterns...)`. A pattern matches
against the three trailing subject tokens —
`<entity-type>.<entity-id>.<event-type>` — and may be given at any depth, with
NATS wildcards (`*` for one token, `>` for the tail):

| Pattern | Matches |
| --- | --- |
| (no filter) | Every event in the store. |
| `order` | All events for every `order` entity. |
| `order.1001` | All events for entity `order.1001`. |
| `order.1001.order-shipped` | Only `order-shipped` events for that entity. |
| `*.*.order-shipped` | `order-shipped` events across all entities. |

A pattern shorter than three tokens is padded with `*`, so `order` is equivalent
to `order.*.*`. A pattern with more than three tokens is rejected with
`ErrSubjectTooManyTokens`. Multiple patterns are OR'd together.

## Sequence windows

Two options bound *which* events are replayed by `Evolve` (and `WithAfterSequence`
also applies to `Watch`):

| Option | Meaning |
| --- | --- |
| `WithAfterSequence(seq)` | Start *after* `seq` — replay only events newer than one you've already applied. |
| `WithStopSequence(seq)` | Stop *at* `seq` — replay no further than that point. |

`WithAfterSequence` resumes replay when you already have matching state and
sequence metadata:

```go
// state already reflects events through lastSeq
_, err := es.Evolve(ctx, &state, rita.WithAfterSequence(lastSeq))
```

Do not use this to restore a `Model` from your own saved state. The model
also needs its entity sequences, and only [`LoadModel`](#snapshots) restores
them. After a failed `Evolve`, see
[failed evolves](./deciders-and-evolvers.md#failed-evolves).

`WithStopSequence` lets you reconstruct historical state — what the entity looked
like as of a particular point in the log — which is useful for debugging,
auditing, or "as-of" queries.

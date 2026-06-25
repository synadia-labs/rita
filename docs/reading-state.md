# Reading State

State in an event-sourced system is derived, not stored. Rita gives you two ways
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
returns, `stats` reflects every event in scope at the moment the call started.
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
> the live projection through `View` is 106–125.

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

`WithAfterSequence` is how you resume cheaply. If you persist a model's state
alongside the last sequence it reflects (a snapshot), you can rebuild by loading
the snapshot and replaying only what came after:

```go
// state + lastSeq were restored from a snapshot
_, err := es.Evolve(ctx, &state, rita.WithAfterSequence(lastSeq))
```

It is also the documented recovery path when
[`DecideAndEvolve` fails mid-evolve](./deciders-and-evolvers.md#decideandevolve):
replay from the last sequence the model successfully applied.

`WithStopSequence` lets you reconstruct historical state — what the entity looked
like as of a particular point in the log — which is useful for debugging,
auditing, or "as-of" queries.

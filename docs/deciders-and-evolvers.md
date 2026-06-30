# Deciders & Evolvers

This is the heart of event sourcing in Rita. State changes are expressed as two
small, pure-ish functions:

- A **Decider** answers *"given this command and the current state, what events
  should happen?"*
- An **Evolver** answers *"given this event, how does the state change?"*

Everything else — appending, replaying, concurrency control — is built on these
two ideas. The pattern comes from
[functional event sourcing](https://thinkbeforecoding.com/post/2021/12/17/functional-event-sourcing-decider):
small folds you can test in isolation, with the infrastructure kept at the edges.

## The interfaces

```go
type Decider interface {
	Decide(context.Context, *Command) ([]*Event, error)
}

type Evolver interface {
	Evolve(context.Context, *Event) error
}

type Viewer[T any] interface {
	View(context.Context, func(T) error) error
}
```

A model usually implements both `Decide` and `Evolve` on the same struct, so the
state it folds up is exactly the state its decisions are validated against. The
decision logic is the meaningful part:

```go
// Decide turns a command into events, validating against current state.
func (o *Order) Decide(_ context.Context, cmd *rita.Command) ([]*rita.Event, error) {
	switch c := cmd.Data.(type) {
	case *PlaceOrder:
		if o.Placed {
			return nil, errors.New("order already placed")
		}
		return []*rita.Event{{Entity: "order.1", Data: &OrderPlaced{Amount: c.Amount}}}, nil
	case *ShipOrder:
		if !o.Placed {
			return nil, errors.New("cannot ship an order that was never placed")
		}
		return []*rita.Event{{Entity: "order.1", Data: &OrderShipped{Carrier: c.Carrier}}}, nil
	}
	return nil, nil
}
```

Returning no events (`nil, nil`) is valid — the command was a no-op against
current state.

> **Full example:** the `Order` aggregate, with both `Decide` and `Evolve` on one
> struct, is [`examples/deciders/main.go`](../examples/deciders/main.go), lines
> 31–66.

## Commands

A `Command` is the input to a Decider: a request to change something, which may
be accepted or rejected. It mirrors `Event` but is never stored:

```go
type Command struct {
	ID   string
	Time time.Time
	Type string
	Data any               // a registered type, or []byte
	Meta map[string]string
}
```

Commands and events share the same [type registry](./events-and-types.md#the-type-registry).
The crucial distinction is intent: a command is a request ("place this order")
that may fail; an event is a fact ("order was placed") that already happened and
cannot fail.

## Append

`Append`, `Decide`, and `DecideAndEvolve` below are methods on the `EventStore` —
the store's write side. They orchestrate the model you defined above (calling its
`Decider.Decide` and `Evolver.Evolve`), so note the overlapping names:
`EventStore.Decide` is a store method, distinct from your model's `Decider.Decide`.

The lowest-level write is `Append`. It validates and enriches each event (see
[the event](./events-and-types.md#the-event)), publishes them, and returns the
stream sequence of the last one:

```go
seq, err := es.Append(ctx, []*rita.Event{
	{Entity: "order.1001", Data: &OrderPlaced{OrderID: "1001"}},
	{Entity: "order.1001", Data: &OrderShipped{OrderID: "1001"}},
})
```

A single event is published directly. **Multiple events are published as one
atomic batch** — either all of them are stored or none are — which lets a single
`Decide` emit several events without risk of a partial write. Appending an empty
slice returns `ErrNoEvents`.

## Decide

`EventStore.Decide` is a convenience that runs your model's `Decider.Decide` and
then `Append`s the resulting events in one call:

```go
events, seq, err := es.Decide(ctx, model, &rita.Command{Data: &PlaceOrder{}})
```

It is exactly `model.Decide(...)` followed by `es.Append(...)`, with the errors
threaded through. Use it when the deciding model and the stored events are the
only things you need.

## DecideAndEvolve

`DecideAndEvolve` goes one step further: decide, append, **and** apply the new
events back to the in-memory model so it reflects the write immediately:

```go
model := rita.NewModel(&Order{})

events, seq, err := es.DecideAndEvolve(ctx, model, &rita.Command{Data: &PlaceOrder{}})
// model now reflects OrderPlaced; no replay needed.
```

This is the workhorse for a command handler that keeps a model in memory across
requests: each command advances both the stored log and the live model in lock
step.

> **Full example:** [`examples/deciders/main.go`](../examples/deciders/main.go) —
> `NewModel` and the `DecideAndEvolve` command loop are lines 114–121, `View` is
> 123–129, and a rejected decision is 131–135.

> **Partial-failure note.** If the evolve step fails *after* the append
> succeeded (including a context cancellation between events), the events are
> already durably stored but the in-memory model has only advanced up to the
> last successfully-applied event. Recover by calling
> [`Evolve` with `WithAfterSequence`](./reading-state.md#sequence-windows) to
> replay the remainder onto the model.

## Model

`Model[T]` wraps a value that implements any of `Decider`, `Evolver`, and/or
`Viewer`, and adds two things you almost always want:

```go
model := rita.NewModel(&Order{})
```

**1. Thread-safety.** A bare model is fine for synchronous `Decide`/`Evolve`,
but a [`Watch`](./reading-state.md#watch) applies events from a background
goroutine while your request handlers read state. `Model[T]` guards all access
with a read/write mutex, so a watched model can be safely read concurrently via
`View`:

```go
err := model.View(ctx, func(o *Order) error {
	fmt.Println("shipped:", o.Shipped)
	return nil
})
```

**2. Per-entity sequence tracking**, which buys two behaviors:

- **Idempotent evolves.** The model records the last sequence applied per
  entity and skips any event whose sequence it has already seen, so replaying or
  overlapping a live watch with a catch-up cannot double-count.
- **Automatic optimistic concurrency.** On `Decide`, the model stamps each
  emitted event's `Expect` with the last sequence it has observed for that
  entity (unless you set `Expect` yourself) — see below.

`NewModel` detects which interfaces the wrapped type implements; calling a method
the type doesn't support returns `ErrDeciderNotImplemented` or
`ErrEvolverNotImplemented`.

## Optimistic concurrency

When two writers act on the same entity concurrently, you want the second write
to fail rather than clobber the first. Rita expresses this with an expected
sequence on the event:

```go
// Reject the append unless the entity's last event is at sequence 7.
{Entity: "order.1001", Data: &OrderShipped{}, Expect: rita.ExpectSequence(7)}
```

On a mismatch, `Append` returns `ErrSequenceConflict`; the caller re-reads
current state and retries. Under the hood this maps to JetStream's
expected-last-subject-sequence check.

There are two constructors:

| Constructor | Guards against |
| --- | --- |
| `ExpectSequence(seq)` | Concurrent writes to **this entity** (the default scope: all event types for the entity). |
| `ExpectSequenceSubject(seq, pattern)` | A custom subject scope, e.g. a single event type or an entity-type-wide stream. |

When you drive writes through a [`Model[T]`](#model), you usually don't set
`Expect` at all: the model fills it in from the sequence it last observed for the
entity, so the read-decide-write cycle is concurrency-safe by construction.

```go
model := rita.NewModel(&Order{})
es.Evolve(ctx, model, rita.WithFilters("order.1001")) // load current state + seq
events, _, err := es.DecideAndEvolve(ctx, model, cmd) // Expect set automatically
if errors.Is(err, rita.ErrSequenceConflict) {
	// someone else wrote first; reload and retry
}
```

> **Full example:** [`examples/optimistic-concurrency/main.go`](../examples/optimistic-concurrency/main.go)
> races two writers on one entity — the conflicting append is lines 85–102, and
> the reload-and-retry recovery is 104–116.

## Choosing among Append / Decide / DecideAndEvolve

| You have… | Use |
| --- | --- |
| Pre-built events, no decision logic | `Append` |
| A command and a decider, and you'll rebuild read models elsewhere | `Decide` |
| A command and a long-lived in-memory model to keep current | `DecideAndEvolve` |

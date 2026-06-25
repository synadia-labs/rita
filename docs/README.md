# Rita Documentation

Rita is a library for building **event-sourced** applications on top of NATS
JetStream. Before reaching for the API, it's worth slowing down on the idea
underneath it — the patterns here land a lot more naturally once the concept
clicks.

## Thinking in events

Picture a bank account. The balance you see isn't really the truth of the
account; it's a summary. The truth is the ledger underneath it — *deposited
$50*, *withdrew $20*, *deposited $100* — and the balance is just what you get
when you add those up. Hand someone only the final number and throw the ledger
away, and you've lost something you can't get back: not just the history, but
the reason behind every change.

Most software does exactly that. A row in a table holds the current state, and
each update overwrites what was there a moment ago. The previous value, and the
intent behind the change, are gone the instant the new value lands.

Event sourcing turns this inside out. You store the **events** — the things that
happened — and treat them as the source of truth. State is no longer something
you keep; it's something you *derive* by replaying events, the same way a
balance is derived from a ledger. An event is final: it can be read and
interpreted a dozen ways downstream, but the fact that it happened is not up for
debate.

Three words recur throughout these docs, and they're worth pinning down:

- An **event** is something that *already happened* — `order-placed`,
  `order-shipped`. Past tense, and indisputable.
- A **command** is an *intention to do something* — `place-order`, `ship-order`.
  Imperative, and — unlike an event — a command can be refused.
- **State** is the point-in-time information you need in order to evaluate the
  next command.

How they fit together is the whole idea: **as events occur, you derive the state
needed to evaluate the next command, and honoring that command produces new
events.** And it only ever flows one way — you can always rebuild state from the
log, but you can never rebuild the log from state. So Rita keeps the one thing
you can't recompute (the events) and lets you recompute everything else (your
read models, your views, your projections) whenever you need it.

## Why NATS

None of this requires NATS, but NATS fits unusually well. JetStream is a
messaging-first system where durable, ordered, append-only streams aren't a
feature bolted on the side — they're the substrate. An event store really only
needs four things, and JetStream offers each of them natively: streams that
persist indefinitely, subjects that key granular sequences of events, an
expected-sequence check for [optimistic
concurrency](./deciders-and-evolvers.md#optimistic-concurrency), and consumers
for replaying a stream or following it live. Rita is a thin layer that maps
event sourcing onto those primitives, so you can think in events instead of in
streams and consumers.

This directory documents the patterns Rita provides for doing that well.

## Contents

- [Architecture overview](#architecture-overview) (this page)
- [Event stores](./event-stores.md) — the manager and the event store lifecycle.
- [Events & types](./events-and-types.md) — events, entities, the type registry, and codecs.
- [Deciders & evolvers](./deciders-and-evolvers.md) — the core write-side patterns and optimistic concurrency.
- [Reading state](./reading-state.md) — `Evolve`, `Watch`, filters, and sequence windows.
- [Reactors](./reactors.md) — durable consumers for side effects.
- [Multi-tenancy](./tenancy.md) — scoping a single store to many tenants.

## Architecture overview

Rita has a small surface area built around a few collaborating pieces.

```
  Command                Events                  Stream                  State
 ┌─────────┐  Decide   ┌──────────┐   Append   ┌────────────┐  Evolve  ┌──────────┐
 │ Command │ ────────► │ Decider  │ ─────────► │ EventStore │ ───────► │ Evolver  │
 └─────────┘           │ (model)  │            │ (JetStream)│          │ (model)  │
                       └──────────┘            └─────┬──────┘          └──────────┘
                                                     │
                                       Watch ────────┤──────── Reactor
                                  (in-process views)  │   (durable side effects)
                                                      ▼
                                                external systems
```

- A [**Manager**](./event-stores.md) holds shared dependencies (a type
  registry, an ID generator, a clock, a logger) and creates **EventStores**.
- An [**EventStore**](./event-stores.md) is backed by exactly one JetStream
  stream. It is where you `Append` events and from which you `Evolve` or
  `Watch` to rebuild state.
- An [**Event**](./events-and-types.md) is an immutable fact about an
  **entity**. Events are the source of truth.
- A [**Decider**](./deciders-and-evolvers.md) turns a **Command** (a request to
  change something) into zero or more events.
- An [**Evolver**](./deciders-and-evolvers.md) folds events into in-memory
  **state**. A **Viewer** reads that state. The generic
  [`Model[T]`](./deciders-and-evolvers.md#model) combines all three with
  thread-safety and per-entity sequence tracking.
- A [**Reactor**](./reactors.md) is a durable consumer that runs side effects
  (send an email, call an API) as events arrive.

The write side (commands → events) and the read side (events → state) are
deliberately separate. Events are the contract between them.

## The CQRS / event-sourcing flow

A typical request flows through Rita like this:

1. A command arrives (`PlaceOrder`).
2. A **Decider** validates it against current state and emits events
   (`OrderPlaced`).
3. Rita **Appends** those events to the stream — atomically, and optionally
   guarded by an [expected sequence](./deciders-and-evolvers.md#optimistic-concurrency)
   for optimistic concurrency.
4. Read models rebuild state by **Evolving** over the events, either on demand
   (`Evolve`) or continuously (`Watch`).
5. **Reactors** independently observe the same events to trigger side effects,
   durably and at-least-once.

Steps 2–3 are the write side; steps 4–5 are the read side. They only ever
communicate through stored events.

## Subject & storage layout

Every event store maps to one JetStream stream and a hierarchical subject space.
Understanding the layout makes the [filtering](./reading-state.md#filters) rules
intuitive.

| Concept | Pattern | Example |
| --- | --- | --- |
| Stream name | `ES_<name>` | `ES_orders` |
| Stream subjects | `$ES.<name>.>` | `$ES.orders.>` |
| Event subject | `$ES.<name>.<entity-type>.<entity-id>.<event-type>` | `$ES.orders.order.1001.order-shipped` |
| Tenant event subject | `$ES.<name>.<tenant>.<entity-type>.<entity-id>.<event-type>` | `$ES.orders.acme.order.1001.order-shipped` |

The three trailing tokens — `<entity-type>.<entity-id>.<event-type>` — are what
you match against with [filters](./reading-state.md#filters). The
[`Entity`](./events-and-types.md#entities) field supplies the first two; the
event type supplies the third.

Each event is stored as a single JetStream message:

- The encoded event **data** is the message body.
- The event **envelope** (id, entity, type, time, codec, custom metadata) is
  stored in NATS message **headers**, which lets consumers fetch headers without
  the body when that is all they need.

See [Events & types](./events-and-types.md#on-the-wire) for the exact header
names.

## Glossary

| Term | Meaning |
| --- | --- |
| **Manager** | Factory for event stores; carries shared registry/clock/id/logger. |
| **EventStore** | A JetStream-backed log of events plus operations over it. |
| **Event** | An immutable fact about an entity; the unit of storage. |
| **Entity** | The thing an event is about, identified as `<type>.<id>` (e.g. `order.1001`). |
| **Command** | A request to change state; input to a Decider. |
| **Decider** | `Decide(cmd) -> events`. The write-side decision logic. |
| **Evolver** | `Evolve(event)` mutating in-memory state. The read-side fold. |
| **Viewer** | Read-only access to a model's state. |
| **Model[T]** | Thread-safe combinator of Decider/Evolver/Viewer with sequence tracking. |
| **Reactor** | A durable consumer that runs side effects per event. |
| **Type registry** | Maps event/command type names to Go structs and a codec. |
| **Tenant** | An isolated subject scope within a single tenant-enabled store. |

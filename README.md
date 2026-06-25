# Rita

Rita is a library for building event-sourced applications on top of NATS.

**NOTE: This package is under heavy development, so breaking changes may be introduced.**

[![GoDoc][GoDoc-Image]][GoDoc-URL] [![ReportCard][ReportCard-Image]][ReportCard-URL] [![GitHub Actions][GitHubActions-Image]][GitHubActions-URL]

[GoDoc-Image]: https://pkg.go.dev/badge/github.com/synadia-labs/rita
[GoDoc-URL]: https://pkg.go.dev/github.com/synadia-labs/rita
[ReportCard-Image]: https://goreportcard.com/badge/github.com/synadia-labs/rita
[ReportCard-URL]: https://goreportcard.com/report/github.com/synadia-labs/rita
[GitHubActions-Image]: https://github.com/synadia-labs/rita/actions/workflows/ci.yaml/badge.svg?branch=main
[GitHubActions-URL]: https://github.com/synadia-labs/rita/actions?query=branch%3Amain

## Install

Requires Go 1.24+ and NATS 2.12+

```bash
go get github.com/synadia-labs/rita
```

## Getting Started

Rita persists your application's **events** to a JetStream stream and gives you
the tools to turn those events back into **state**. You write small models that
either *decide* what events a command produces or *evolve* state from events;
Rita handles serialization, storage, ordering, concurrency, and delivery.

You need a NATS server with JetStream enabled:

```bash
nats-server -js
```

Given two event types, `OrderPlaced` and `OrderShipped`, a **model** folds them
into state by implementing `Evolve`:

```go
type Order struct {
	Placed  bool
	Shipped bool
	Amount  int
}

func (o *Order) Evolve(_ context.Context, e *rita.Event) error {
	switch d := e.Data.(type) {
	case *OrderPlaced:
		o.Placed, o.Amount = true, d.Amount
	case *OrderShipped:
		o.Shipped = true
	}
	return nil
}
```

Append events for an entity (identified as `<type>.<id>`), then rebuild its
state by replaying them through the model:

```go
es.Append(ctx, []*rita.Event{
	{Entity: "order.1001", Data: &OrderPlaced{OrderID: "1001", Amount: 50}},
	{Entity: "order.1001", Data: &OrderShipped{OrderID: "1001", Carrier: "UPS"}},
})

var order Order
es.Evolve(ctx, &order, rita.WithFilters("order.1001"))
// order.Placed == true, order.Shipped == true, order.Amount == 50
```

> **Full runnable example** — with the embedded server, registry, and error
> handling: [`examples/quickstart/main.go`](./examples/quickstart/main.go). The
> event types and model are lines 21–47; appending and rebuilding state is lines
> 93–107.

## Examples

Each pattern has a complete, runnable program under [`examples/`](./examples):

| Example | Pattern |
| --- | --- |
| [`quickstart`](./examples/quickstart) | Append events and rebuild state. |
| [`deciders`](./examples/deciders) | Command → events → state through a model. |
| [`optimistic-concurrency`](./examples/optimistic-concurrency) | Expected-sequence guarding and retry. |
| [`projection`](./examples/projection) | A live read model via `Watch`. |
| [`tenancy`](./examples/tenancy) | One store, many isolated tenants. |
| [`reactor`](./examples/reactor) | A minimal durable side-effect consumer. |
| [`reactor-lifecycle`](./examples/reactor-lifecycle) | The full reactor management API. |

## Documentation

The [`docs/`](./docs) directory breaks down each pattern in depth:

- [Concepts & architecture](./docs/README.md) — new to event sourcing? Start here: how to think in events, then how Rita's pieces fit and its subject layout.
- [Event stores](./docs/event-stores.md) — the manager and event store lifecycle.
- [Events & types](./docs/events-and-types.md) — events, entities, the type registry, and codecs.
- [Deciders & evolvers](./docs/deciders-and-evolvers.md) — the core write-side patterns and optimistic concurrency.
- [Reading state](./docs/reading-state.md) — `Evolve`, `Watch`, filters, and sequence windows.
- [Reactors](./docs/reactors.md) — durable consumers for side effects.
- [Multi-tenancy](./docs/tenancy.md) — scoping a single store to many tenants.


# Reactors

A **reactor** runs side effects in response to events: send a shipping
notification, call a payment API, update an external search index. Where
[`Watch`](./reading-state.md#watch) keeps in-process state current with no
durability, a reactor is a **durable JetStream consumer** that processes each
event **at least once** and survives restarts by resuming from its stored
position.

## The shape of a reactor

There are two halves:

1. A **durable consumer** on the stream, provisioned and managed through the
   `*EventStore` (`CreateReactor`, `UpdateReactor`, …). This is server-side state
   that persists independently of your process.
2. A **`Reactor` handle**, which you `Bind` a handler to in order to start
   dispatching events, and `Unbind` to stop.

Separating the two means a reactor's *progress* lives in JetStream, while a
*handler* is just a function in your process. Restart the process, bind again,
and you resume where you left off.

## A minimal reactor

A reactor is mostly its handler — the function that decides each event's fate.
The return value is the entire contract (detailed [below](#handler-return-semantics)):

```go
handler := func(ctx context.Context, ev *rita.Event) error {
	shipped, ok := ev.Data.(*OrderShipped)
	if !ok {
		return rita.ErrUnprocessable // not for us — drop it (Term)
	}
	return notify(ctx, shipped) // nil -> Ack; error -> Nak (retry)
}
```

The surrounding wiring is a few lines: provision the durable with
`CreateOrUpdateReactor`, `Bind` the handler to start dispatching, and `Unbind`
on shutdown.

> **Full examples:** [`examples/reactor/main.go`](../examples/reactor/main.go) is
> a complete end-to-end reactor — `CreateOrUpdateReactor` is lines 94–100, the
> handler 84–92, `Bind` 102–104, and `Unbind` 108–110.
> [`examples/reactor-lifecycle/main.go`](../examples/reactor-lifecycle/main.go)
> walks the full management API.

## Handler return semantics

The handler's return value decides what happens to the message. This is the
contract that gives you at-least-once delivery with explicit poison-message
handling:

| Return | Action | Meaning |
| --- | --- | --- |
| `nil` | **Ack** | Processed successfully; advance past it. |
| `errors.Is(err, ErrUnprocessable)` | **Term** | This event can never be processed; drop it permanently (no redelivery). |
| any other error | **Nak** | Transient failure; redeliver and try again. |

`Term` is the right response to an event the handler should never have received
(wrong type, structurally invalid) — retrying would loop forever. `Nak` is for
failures that might succeed later (a downstream service is briefly down).

## Retry and backoff

By default a Nak'd message is redelivered immediately and indefinitely
(`MaxDeliver: -1`). Configure `BackOff` to space out retries:

```go
es.CreateReactor(ctx, rita.ReactorConfig{
	Name:    "shipping-notifier",
	Filters: []string{"*.*.order-shipped"},
	BackOff: []time.Duration{time.Second, 5 * time.Second, 30 * time.Second},
})
```

The `BackOff` slice gives the delay per delivery attempt; the last entry repeats
for all further attempts. Rita applies these delays to explicit Naks itself (it
delays the Nak by the attempt's backoff), so backoff governs both handler-error
retries and ack-timeout retries.

## Configuration & defaults

```go
type ReactorConfig struct {
	Name          string
	Description   string
	Metadata      map[string]string
	Filters       []string
	MaxAckPending int
	MaxDeliver    int
	AckWait       time.Duration
	BackOff       []time.Duration
}
```

Zero-valued knobs are replaced with Rita's defaults on every create/update, so
an omitted field never silently inherits a surprising JetStream server default:

| Field | Default | Why |
| --- | --- | --- |
| `MaxAckPending` | `1` | Serial delivery — one event in flight at a time, which preserves ordering for side effects. Raise it to process concurrently. |
| `MaxDeliver` | `-1` | Unlimited redelivery. |
| `AckWait` | `30s` | How long before an un-acked message is redelivered. |

`Filters` uses the same [pattern syntax](./reading-state.md#filters) as `Evolve`
and `Watch`.

## Managing reactors

All of these hang off the `*EventStore`:

| Method | Purpose |
| --- | --- |
| `CreateReactor(cfg)` | Provision a new durable. Idempotent if the config matches; `ErrReactorExists` if a durable of that name exists with a *different* config. |
| `UpdateReactor(cfg)` | Replace an existing durable's config. `cfg` is the complete desired state, not a patch. |
| `CreateOrUpdateReactor(cfg)` | Provision or reconfigure unconditionally — good for declarative startup hooks. |
| `GetReactor(name)` | An unbound handle to an existing durable. |
| `ListReactors()` | All durable consumers on the stream. |
| `DeleteReactor(name)` | Remove the durable consumer. |

`UpdateReactor` is replace-semantics: read the current config, mutate, write it
back.

```go
r, _ := es.GetReactor(ctx, "shipping-notifier")
info, _ := r.Info(ctx)
cfg := info.Config
cfg.AckWait = 10 * time.Second
es.UpdateReactor(ctx, cfg)
```

Updating the durable does **not** hot-reload a currently-bound reactor — runtime
behavior such as `BackOff` is snapshotted at `Bind`. Unbind and bind again to
pick up changes.

> **Full example:** [`examples/reactor-lifecycle/main.go`](../examples/reactor-lifecycle/main.go)
> exercises each operation in order — `CreateReactor` is lines 77–85,
> `GetReactor`/`Info` 89–98, `UpdateReactor` 101–115, `ListReactors` 118–124, and
> `DeleteReactor` 178–185.

## Lifecycle: bind and unbind

```go
r.Bind(ctx, handler)   // start dispatching; ErrReactorAlreadyBound if already bound
// ...
r.Unbind(ctx)          // drain in-flight messages within ctx's deadline, then stop
```

- **`Bind`** attaches a handler and starts the consume loop. A handle can only be
  bound once at a time.
- **`Unbind`** drains in-flight handler work within `ctx`'s deadline, then stops
  the consumer. The **durable persists** — a later `Bind` (on the same or a fresh
  handle) resumes from the stored position. `Unbind` is idempotent. If the
  deadline expires before the drain finishes, it cancels the handler context
  (so in-flight work can abort), returns `ctx.Err()`, and can be called again to
  wait for final shutdown.

Unbinding is *not* deletion. To remove the consumer entirely, call
`DeleteReactor`. If a reactor is still bound when its durable is deleted, the
consume loop receives a terminal error (logged via the store's logger); you
still call `Unbind` to release the in-process handle.

## Tenancy note

Reactor *mutations* (`Create`/`Update`/`CreateOrUpdate`/`Delete`) require a
[tenant scope](./tenancy.md) on a tenant store so they build correctly scoped
filter subjects. But durable **names share a single stream-wide namespace** —
`GetReactor` and `ListReactors` are not tenant-filtered. If you need per-tenant
reactor isolation, namespace the durable names yourself (e.g.
`"<tenant>-shipping-notifier"`). See [Multi-tenancy](./tenancy.md#reactors-are-stream-global)
for details.

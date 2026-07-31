# Events & Types

Events are the unit of storage in Rita and the contract between the write side
and the read side. This page covers the anatomy of an event, how entities are
identified, and how Rita turns your Go types into bytes on the wire.

## The event

```go
type Event struct {
	ID     string            // NATS Msg-Id, used for de-duplication
	Entity string            // "<entity-type>.<entity-id>", e.g. "order.1001"
	Time   time.Time         // when the event occurred
	Type   string            // unique event name, e.g. "order-placed"
	Data   any               // the payload (registered struct or []byte)
	Meta   map[string]string // application-defined metadata
	Expect *Expect           // optimistic-concurrency expectation (append only)
}
```

When you `Append`, Rita fills in sensible defaults so you usually only set
`Entity` and `Data`:

- **`ID`** — if empty, a fresh ID is generated (NUID by default). The ID becomes
  the NATS `Msg-Id`, so JetStream's de-duplication window will reject a second
  append of the same ID. Set it yourself to make appends idempotent.
- **`Type`** — if a [type registry](#the-type-registry) is configured, the type
  name is inferred from the Go type of `Data`. Without a registry it is
  required. If you set both, they must agree.
- **`Time`** — defaults to the manager's clock (`time.Now()` in local time).

Two fields are read-only and populated by Rita when events come back from the
store:

- **`Event.Sequence()`** — the stream sequence number of the event.
- **`Event.Subject()`** — the full NATS subject the event was published to.

### Entities

Every event is *about* an entity, identified by the `Entity` field in the form
`<entity-type>.<entity-id>` — exactly two tokens joined by a dot:

```go
{Entity: "order.1001", Data: &OrderShipped{...}}
//        ^^^^^ ^^^^
//        type  id
```

The two tokens become the middle of the event's subject
(`$ES.orders.order.1001.order-shipped`), which is what gives you cheap
[per-entity and per-type filtering](./reading-state.md#filters). Neither token
may contain `.`, `*`, `>`, or whitespace — those are the characters that would
let an entity inject extra subject tokens or wildcards. An entity that is empty
or not two tokens is rejected with `ErrEventEntityRequired` /
`ErrEventEntityInvalid`.

### Metadata

`Meta` carries application-defined key/value annotations — a correlation ID, the
acting user, a trace span — that travel with the event but are not part of its
payload. Each entry is stored as a NATS header prefixed with `Rita-Meta-` and is
restored into `Meta` on read.

## The type registry

A registry maps **type names** (the strings stored on the wire) to **Go types**,
and selects a **codec**. With a registry, you work in terms of your own structs
and Rita handles serialization transparently.

```go
registry, err := types.NewRegistry(map[string]*types.Type{
	"order-placed":  {Init: func() any { return &OrderPlaced{} }},
	"order-shipped": {Init: func() any { return &OrderShipped{} }},
}, types.Codec("json"))
```

> **Full example:** the registry, manager, and store setup is
> [`examples/quickstart/main.go`](../examples/quickstart/main.go), lines 71–91.

Each `Type` provides an `Init` function that returns a **pointer to a struct**.
Rita uses it two ways:

- On **write**, it reflects the Go type of your `Data` to look up the registered
  name, which becomes the event `Type`.
- On **read**, it calls `Init` to allocate a fresh value of the right type, then
  decodes the message body into it. `Event.Data` comes back as your concrete
  pointer type, ready for a type switch.

Type names must match `^[\w-]+(\.[\w-]+)*$` (word characters and dashes, with
optional dotted segments). `NewRegistry` validates every type up front — that
`Init` returns a non-nil pointer to a struct and that a value round-trips
through the codec — so misconfiguration fails at startup, not at runtime.

For event types, use the undotted form: the name becomes the event subject's
final token, so a dotted name is rejected at append with `ErrEventTypeInvalid` —
it would inject extra subject tokens that per-type filters and `Expect` patterns
cannot address.

The registry serializes events; command payload types do not need to be
registered, since [commands](./deciders-and-evolvers.md#commands) are never
serialized.

### Validation

If a registered type implements `Validate() error`, the codec calls it before
marshaling. A non-nil error aborts the append, so invalid events never reach the
store:

```go
func (o *OrderPlaced) Validate() error {
	if o.Amount <= 0 {
		return errors.New("amount must be positive")
	}
	return nil
}
```

`Validate()` runs on the write path only, when the codec marshals an event during
`Append`. Commands are never serialized — `EventStore.Decide` hands the
`*Command` straight to your model — so a command type's `Validate()` is **not**
called automatically. Validate a command inside your `Decide` method (or before
calling it) if you need that check.

## Codecs

Rita ships four codecs, registered by name in the [`codec`](../codec) package:

| Name | Use |
| --- | --- |
| `json` | Default. Human-readable, broad interop. |
| `msgpack` | Compact binary, schema-less. |
| `protobuf` | For `proto.Message` types; compact and schema'd. |
| `binary` | Pass-through for `[]byte` / `encoding.BinaryMarshaler`. |

Select one per registry with `types.Codec("msgpack")`; the default is `json`.
The codec name is recorded on each event (the `Rita-Codec` header) so reads
decode with the same codec that wrote the data, even if the registry's default
later changes.

## Without a registry

A registry is optional. Without one, Rita treats event data as opaque bytes:

```go
seq, err := es.Append(ctx, []*rita.Event{{
	Entity: "order.1001",
	Type:   "order-placed", // required: no type can be inferred
	Data:   []byte(`{"amount":50}`),
}})
```

In this mode `Data` must be a `[]byte` (the `binary` codec is used), `Type` is
mandatory, and reads return `Data` as `[]byte`. This is useful when you manage
serialization yourself or are bridging an existing wire format.

## On the wire

Each event becomes one JetStream message. The payload is the message body; the
envelope is carried in headers so a consumer can request headers without the
body when that is all it needs:

| Header | Contents |
| --- | --- |
| `Nats-Msg-Id` | Event `ID` (drives de-duplication). |
| `Rita-Entity` | The `<type>.<id>` entity. |
| `Rita-Type` | Event type name. |
| `Rita-Time` | Event time, RFC3339Nano. |
| `Rita-Codec` | Codec used for the body. |
| `Rita-Meta-<key>` | One header per `Meta` entry. |

The subject the message is published to is
`$ES.<store>.<entity-type>.<entity-id>.<event-type>` (see the
[subject layout](./README.md#subject--storage-layout)).

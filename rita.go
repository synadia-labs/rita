package rita

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-labs/rita/clock"
	"github.com/synadia-labs/rita/codec"
	"github.com/synadia-labs/rita/id"
	"github.com/synadia-labs/rita/types"
)

type ritaOption func(o *Rita) error

func (f ritaOption) addOption(o *Rita) error {
	return f(o)
}

// RitaOption models a option when creating a type registry.
type RitaOption interface {
	addOption(o *Rita) error
}

// TypeRegistry sets an explicit type registry.
func TypeRegistry(types types.Registry) RitaOption {
	return ritaOption(func(o *Rita) error {
		o.types = types
		return nil
	})
}

// Clock sets a clock implementation. Default it clock.Time.
func Clock(clock clock.Clock) RitaOption {
	return ritaOption(func(o *Rita) error {
		o.clock = clock
		return nil
	})
}

// ID sets a unique ID generator implementation. Default is id.NUID.
func ID(id id.ID) RitaOption {
	return ritaOption(func(o *Rita) error {
		o.id = id
		return nil
	})
}

func Logger(logger *slog.Logger) RitaOption {
	return ritaOption(func(o *Rita) error {
		o.logger = logger
		return nil
	})
}

type Rita struct {
	ctx    context.Context
	logger *slog.Logger
	nc     *nats.Conn
	js     jetstream.JetStream

	id    id.ID
	clock clock.Clock
	types types.Registry
}

// UnpackEvent unpacks an Event from a NATS message.
func (r *Rita) UnpackEvent(msg jetstream.Msg) (*Event, error) {
	eventType := msg.Headers().Get(eventTypeHdr)
	codecName := msg.Headers().Get(eventCodecHdr)

	var (
		data interface{}
		err  error
	)

	c, ok := codec.Codecs[codecName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", codec.ErrCodecNotRegistered, codecName)
	}

	// No type registry, so assume byte slice.
	if r.types == nil {
		var b []byte
		err = c.Unmarshal(msg.Data(), &b)
		data = b
	} else {
		var v any
		v, err = r.types.Init(eventType)
		if err == nil {
			err = c.Unmarshal(msg.Data(), v)
			data = v
		}
	}
	if err != nil {
		return nil, err
	}

	var seq uint64
	// If this message is not from a native JS subscription, the reply will not
	// be set. This is where metadata is parsed from. In cases where a message is
	// re-published, we don't want to fail if we can't get the sequence.
	if msg.Reply() != "" {
		md, err := msg.Metadata()
		if err != nil {
			return nil, fmt.Errorf("unpack: failed to get metadata: %s", err)
		}
		seq = md.Sequence.Stream
	}

	eventTime, err := time.Parse(eventTimeFormat, msg.Headers().Get(eventTimeHdr))
	if err != nil {
		return nil, fmt.Errorf("unpack: failed to parse event time: %s", err)
	}

	meta := make(map[string]string)

	for h := range msg.Headers() {
		if strings.HasPrefix(h, eventMetaPrefixHdr) {
			key := h[len(eventMetaPrefixHdr):]
			meta[key] = msg.Headers().Get(h)
		}
	}

	return &Event{
		ID:       msg.Headers().Get(nats.MsgIdHdr),
		Type:     msg.Headers().Get(eventTypeHdr),
		Time:     eventTime,
		Data:     data,
		Meta:     meta,
		subject:  msg.Subject(),
		sequence: seq,
	}, nil
}

func (r *Rita) EventStore(name string) *EventStore {
	return &EventStore{
		name: name,
		rt:   r,
	}
}

// New initializes a new Rita instance with a NATS connection.
func New(ctx context.Context, nc *nats.Conn, opts ...RitaOption) (*Rita, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	rt := &Rita{
		ctx:   ctx,
		nc:    nc,
		js:    js,
		id:    id.NUID,
		clock: clock.Time,
	}

	for _, o := range opts {
		if err := o.addOption(rt); err != nil {
			return nil, err
		}
	}

	return rt, nil
}

package rita

import (
	"fmt"
	"strings"
	"time"

	"github.com/bruth/rita/clock"
	"github.com/bruth/rita/codec"
	"github.com/bruth/rita/id"
	"github.com/bruth/rita/types"
	"github.com/nats-io/nats.go"
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
func TypeRegistry(types *types.Registry) RitaOption {
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

type Rita struct {
	nc *nats.Conn
	js nats.JetStreamContext

	id    id.ID
	clock clock.Clock
	types *types.Registry
}

// UnpackEvent unpacks an Event from a NATS message.
func (r *Rita) UnpackEvent(msg *nats.Msg) (*Event, error) {
	eventType := msg.Header.Get(eventTypeHdr)
	codecName := msg.Header.Get(eventCodecHdr)

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
		err = c.Unmarshal(msg.Data, &b)
		data = b
	} else {
		var v any
		v, err = r.types.Init(eventType)
		if err == nil {
			err = c.Unmarshal(msg.Data, v)
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
	if msg.Reply != "" {
		md, err := msg.Metadata()
		if err != nil {
			return nil, fmt.Errorf("unpack: failed to get metadata: %s", err)
		}
		seq = md.Sequence.Stream
	}

	eventTime, err := time.Parse(eventTimeFormat, msg.Header.Get(eventTimeHdr))
	if err != nil {
		return nil, fmt.Errorf("unpack: failed to parse event time: %s", err)
	}

	meta := make(map[string]string)

	for h := range msg.Header {
		if strings.HasPrefix(h, eventMetaPrefixHdr) {
			key := h[len(eventMetaPrefixHdr):]
			meta[key] = msg.Header.Get(h)
		}
	}

	return &Event{
		ID:       msg.Header.Get(nats.MsgIdHdr),
		Type:     msg.Header.Get(eventTypeHdr),
		Time:     eventTime,
		Data:     data,
		Meta:     meta,
		Subject:  msg.Subject,
		Sequence: seq,
	}, nil
}

func (r *Rita) EventStore(name string) *EventStore {
	return &EventStore{
		name: name,
		rt:   r,
	}
}

// New initializes a new Rita instance with a NATS connection.
func New(nc *nats.Conn, opts ...RitaOption) (*Rita, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	rt := &Rita{
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

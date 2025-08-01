package rita

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-labs/rita/codec"
)

const (
	eventTypeHdr       = "rita-type"
	eventTimeHdr       = "rita-time"
	eventCodecHdr      = "rita-codec"
	eventMetaPrefixHdr = "rita-meta-"
	eventTimeFormat    = time.RFC3339Nano
)

var (
	ErrSequenceConflict  = errors.New("rita: sequence conflict")
	ErrEventIDRequired   = errors.New("rita: event id required")
	ErrEventTypeRequired = errors.New("rita: event type required")
)

// Validator can be optionally implemented by user-defined types and will be
// validated in different contexts, such as before appending an event to a stream.
type validator interface {
	Validate() error
}

type Evolver interface {
	Evolve(event *Event) error
}

// Event is a wrapper for application-defined events.
type Event struct {
	// ID of the event. This will be used as the NATS msg ID
	// for de-duplication.
	ID string

	// Time is the time of when the event occurred which may be different
	// from the time the event is appended to the store. If no time is provided,
	// the current local time will be used.
	Time time.Time

	// Type is a unique name for the event itself. This can be ommitted
	// if a type registry is being used, otherwise it must be set explicitly
	// to identity the encoded data.
	Type string

	// Data is the event data. This must be a byte slice (pre-encoded) or a value
	// of a type registered in the type registry.
	Data any

	// Metadata is application-defined metadata about the event.
	Meta map[string]string

	// Subject is the the subject the event is associated with. Read-only.
	Subject string

	// Sequence is the sequence where this event exists in the stream. Read-only.
	Sequence uint64
}

type appendOpts struct {
	expSeq *uint64
}

type appendOptFn func(o *appendOpts) error

func (f appendOptFn) appendOpt(o *appendOpts) error {
	return f(o)
}

// AppendOption is an option for the event store Append operation.
type AppendOption interface {
	appendOpt(o *appendOpts) error
}

// ExpectSequence indicates that the expected sequence of the subject sequence should
// be the value provided. If not, a conflict is indicated.
func ExpectSequence(seq uint64) AppendOption {
	return appendOptFn(func(o *appendOpts) error {
		o.expSeq = &seq
		return nil
	})
}

type loadOpts struct {
	afterSeq *uint64
}

type loadOptFn func(o *loadOpts) error

func (f loadOptFn) loadOpt(o *loadOpts) error {
	return f(o)
}

// LoadOption is an option for the event store Load operation.
type LoadOption interface {
	loadOpt(o *loadOpts) error
}

// AfterSequence specifies the sequence of the first event that should be fetched
// from the sequence up to the end of the sequence. This useful when partially applied
// state has been derived up to a specific sequence and only the latest events need
// to be fetched.
func AfterSequence(seq uint64) LoadOption {
	return loadOptFn(func(o *loadOpts) error {
		o.afterSeq = &seq
		return nil
	})
}

type natsApiError struct {
	Code        int    `json:"code"`
	ErrCode     uint16 `json:"err_code"`
	Description string `json:"description"`
}

type natsGetMsgRequest struct {
	LastBySubject string `json:"last_by_subj"`
}

type natsGetMsgResponse struct {
	Type    string         `json:"type"`
	Error   *natsApiError  `json:"error"`
	Message *natsStoredMsg `json:"message"`
}

type natsStoredMsg struct {
	Sequence uint64 `json:"seq"`
}

// EventStore provides event store semantics over a NATS stream.
type EventStore struct {
	name string
	rt   *Rita
}

// wrapEvent wraps a user-defined event into the Event envelope. It performs
// validation to ensure all the properties are either defined or defaults are set.
func (s *EventStore) wrapEvent(event *Event) (*Event, error) {
	if event.Data == nil {
		return nil, fmt.Errorf("event data is nil")
	}

	if s.rt.types == nil {
		if event.Type == "" {
			return nil, errors.New("event type is not defined")
		}
	} else {
		t, err := s.rt.types.Lookup(event.Data)
		if err != nil {
			return nil, err
		}

		if event.Type == "" {
			event.Type = t
		} else if event.Type != t {
			return nil, fmt.Errorf("wrong type for event data: %s", event.Type)
		}
	}

	if v, ok := event.Data.(validator); ok {
		if err := v.Validate(); err != nil {
			return nil, err
		}
	}

	// Set ID if empty.
	if event.ID == "" {
		event.ID = s.rt.id.New()
	}

	// Set time if empty.
	if event.Time.IsZero() {
		event.Time = s.rt.clock.Now().Local()
	}

	return event, nil
}

// packEvent pack an event into a NATS message. The advantage of using NATS headers
// is that the server supports creating a consumer that _only_ gets the headers
// without the data as an optimization for some use cases.
func (s *EventStore) packEvent(subject string, event *Event) (*nats.Msg, error) {
	// Marshal the data.
	var (
		data      []byte
		err       error
		codecName string
	)

	if s.rt.types == nil {
		data, err = codec.Binary.Marshal(event.Data)
		codecName = codec.Binary.Name()
	} else {
		data, err = s.rt.types.Marshal(event.Data)
		codecName = s.rt.types.Codec().Name()
	}
	if err != nil {
		return nil, err
	}

	msg := nats.NewMsg(subject)
	msg.Data = data

	// Map event envelope to NATS header.
	msg.Header.Set(nats.MsgIdHdr, event.ID)
	msg.Header.Set(eventTypeHdr, event.Type)
	msg.Header.Set(eventTimeHdr, event.Time.Format(eventTimeFormat))
	msg.Header.Set(eventCodecHdr, codecName)

	for k, v := range event.Meta {
		msg.Header.Set(fmt.Sprintf("%s%s", eventMetaPrefixHdr, k), v)
	}

	return msg, nil
}

// lastSeqForSubject queries the JS API to identify the current latest sequence for a subject.
// This is used as an best-guess indicator of the current end of the even history.
func (s *EventStore) lastMsgForSubject(ctx context.Context, subject string) (*natsStoredMsg, error) {
	rsubject := fmt.Sprintf("$JS.API.STREAM.MSG.GET.%s", s.name)

	data, _ := json.Marshal(&natsGetMsgRequest{
		LastBySubject: subject,
	})

	msg, err := s.rt.nc.RequestWithContext(ctx, rsubject, data)
	if err != nil {
		return nil, err
	}

	var rep natsGetMsgResponse
	err = json.Unmarshal(msg.Data, &rep)
	if err != nil {
		return nil, err
	}

	if rep.Error != nil {
		if rep.Error.Code == 404 {
			return &natsStoredMsg{}, nil
		}
		return nil, fmt.Errorf("%s (%d)", rep.Error.Description, rep.Error.Code)
	}

	return rep.Message, nil
}

// Load fetches all events for a specific subject. The primary use case
// is to use a concrete subject, e.g. "orders.1" corresponding to an
// aggregate/entity identifier. The second use case is to load events for
// a cross-cutting view which can use subject wildcards.
func (s *EventStore) Load(ctx context.Context, subject string, opts ...LoadOption) ([]*Event, uint64, error) {
	// Configure opts.
	var o loadOpts
	for _, opt := range opts {
		if err := opt.loadOpt(&o); err != nil {
			return nil, 0, err
		}
	}

	lastMsg, err := s.lastMsgForSubject(ctx, subject)
	if err != nil {
		return nil, 0, err
	}

	if lastMsg.Sequence == 0 {
		return nil, 0, nil
	}

	// Ephemeral ordered consumer.. read as fast as possible with least overhead.
	sopts := jetstream.ConsumerConfig{}

	// Don't bother creating the consumer if the last seq is smaller than start.
	if o.afterSeq != nil {
		if lastMsg.Sequence <= *o.afterSeq {
			return nil, 0, nil
		}
		sopts.DeliverPolicy = jetstream.DeliverLastPolicy
	} else {
		sopts.DeliverPolicy = jetstream.DeliverAllPolicy
	}

	consumer, err := s.rt.js.CreateConsumer(s.rt.ctx, s.name, jetstream.ConsumerConfig{})
	if err != nil {
		return nil, 0, err
	}

	pmopts := []jetstream.PullMessagesOpt{}
	msgCtx, err := consumer.Messages(pmopts...)
	if err != nil {
		return nil, 0, err
	}
	defer msgCtx.Stop()

	// Skip first.
	if o.afterSeq != nil {
		_, err := msgCtx.Next()
		if err != nil {
			return nil, 0, err
		}
	}

	var events []*Event
	for {
		msg, err := msgCtx.Next()
		if err != nil {
			return nil, 0, err
		}

		event, err := s.rt.UnpackEvent(msg)
		if err != nil {
			return nil, 0, err
		}

		events = append(events, event)

		if event.Sequence == lastMsg.Sequence {
			break
		}
	}

	return events, lastMsg.Sequence, nil
}

// Append appends a one or more events to the subject's event sequence.
// It returns the resulting sequence number of the last appended event.
func (s *EventStore) Append(ctx context.Context, subject string, events []*Event, opts ...AppendOption) (uint64, error) {
	// Configure opts.
	var o appendOpts
	for _, opt := range opts {
		if err := opt.appendOpt(&o); err != nil {
			return 0, err
		}
	}

	var ack *jetstream.PubAck

	for i, event := range events {
		popts := []jetstream.PublishOpt{
			jetstream.WithExpectStream(s.name),
		}

		if i == 0 && o.expSeq != nil {
			popts = append(popts, jetstream.WithExpectLastSequencePerSubject(*o.expSeq))
		}

		e, err := s.wrapEvent(event)
		if err != nil {
			return 0, err
		}

		msg, err := s.packEvent(subject, e)
		if err != nil {
			return 0, err
		}

		// TODO: add retry logic in case of intermittent errors?
		ack, err = s.rt.js.PublishMsg(s.rt.ctx, msg, popts...)
		if err != nil {
			if strings.Contains(err.Error(), "wrong last sequence") {
				return 0, ErrSequenceConflict
			}
			return 0, err
		}
	}

	return ack.Sequence, nil
}

// Evolve loads events and evolves a model of state. The sequence of the
// last event that evolved the state is returned, including when an error
// occurs.
func (s *EventStore) Evolve(ctx context.Context, subject string, model Evolver, opts ...LoadOption) (uint64, error) {
	events, _, err := s.Load(ctx, subject, opts...)
	if err != nil {
		return 0, err
	}

	var lastSeq uint64
	for _, e := range events {
		if err := model.Evolve(e); err != nil {
			return lastSeq, err
		}
		lastSeq = e.Sequence
	}

	return lastSeq, nil
}

// Create creates the event store given the configuration. The stream
// name is the name of the store and the subjects default to "{name}.>".
func (s *EventStore) Create(config *jetstream.StreamConfig) error {
	if config == nil {
		config = &jetstream.StreamConfig{}
	}
	config.Name = s.name

	if len(config.Subjects) == 0 {
		config.Subjects = []string{fmt.Sprintf("%s.>", s.name)}
	}

	_, err := s.rt.js.CreateStream(s.rt.ctx, *config)
	return err
}

// Update updates the event store configuration.
func (s *EventStore) Update(config *jetstream.StreamConfig) error {
	if config == nil {
		config = &jetstream.StreamConfig{}
	}
	config.Name = s.name
	_, err := s.rt.js.UpdateStream(s.rt.ctx, *config)
	return err
}

// Delete deletes the event store.
func (s *EventStore) Delete() error {
	return s.rt.js.DeleteStream(s.rt.ctx, s.name)
}

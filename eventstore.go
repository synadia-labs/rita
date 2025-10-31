package rita

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
	"github.com/synadia-labs/rita/codec"
)

const (
	eventEntityHdr     = "rita-entity"
	eventTypeHdr       = "rita-type"
	eventTimeHdr       = "rita-time"
	eventCodecHdr      = "rita-codec"
	eventMetaPrefixHdr = "rita-meta-"
	eventTimeFormat    = time.RFC3339Nano
)

var (
	ErrSequenceConflict    = errors.New("rita: sequence conflict")
	ErrEventDataRequired   = errors.New("rita: event data required")
	ErrEventEntityRequired = errors.New("rita: event entity required")
	ErrEventEntityInvalid  = errors.New("rita: event entity invalid")
	ErrEventTypeRequired   = errors.New("rita: event type required")
)

// validator can be optionally implemented by user-defined types and will be
// validated in different contexts, such as before appending an event to a stream.
type validator interface {
	Validate() error
}

// Evolver is an interface that application-defined models can implement
// to evolve their state based on events.
type Evolver interface {
	Evolve(event *Event) error
}

// Event is a wrapper for application-defined events.
type Event struct {
	// ID of the event. This will be used as the NATS msg ID
	// for de-duplication.
	ID string

	// Identifier for specific entities.  Can be used to determine if
	// an is related to a specific entity/node/endpoint/agent/etc.
	// The format must be two tokens, e.g. "node.1234".
	Entity string

	// Time is the time of when the event occurred which may be different
	// from the time the event is appended to the store. If no time is provided,
	// the current local time will be used.
	Time time.Time

	// Type is a unique name for the event itself. This can be omitted
	// if a type registry is being used, otherwise it must be set explicitly
	// to identity the encoded data.
	Type string

	// Data is the event data. This must be a byte slice (pre-encoded) or a value
	// of a type registered in the type registry.
	Data any

	// Metadata is application-defined metadata about the event.
	Meta map[string]string

	// subject is the the subject the event is associated with. Read-only.
	subject string

	// sequence is the sequence where this event exists in the stream. Read-only.
	sequence uint64
}

type appendOpts struct {
	expSubj string
	expSeq  *uint64
}

type appendOptFn func(o *appendOpts) error

func (f appendOptFn) appendOpt(o *appendOpts) error {
	return f(o)
}

// AppendOption is an option for the event store Append operation.
type AppendOption interface {
	appendOpt(o *appendOpts) error
}

// ExpectSequence indicates that the expected sequence of the implicit subject should
// be the value provided. If not, a conflict is indicated.
func ExpectSequence(seq uint64) AppendOption {
	return appendOptFn(func(o *appendOpts) error {
		o.expSeq = &seq
		return nil
	})
}

// ExpectSequenceSubject indicates that the expected sequence of the explicit subject should
// be the value provided. If not, a conflict is indicated.
func ExpectSequenceSubject(seq uint64, subject string) AppendOption {
	return appendOptFn(func(o *appendOpts) error {
		pattern, err := parsePattern(subject)
		if err != nil {
			return err
		}
		o.expSeq = &seq
		o.expSubj = pattern
		return nil
	})
}

type evolveOpts struct {
	fillters []string
	afterSeq *uint64
	upToSeq  *uint64
}

type evolveOptFn func(o *evolveOpts) error

func (f evolveOptFn) evolveOpt(o *evolveOpts) error {
	return f(o)
}

// EvolveOption is an option for the event store Evolve operation.
type EvolveOption interface {
	evolveOpt(o *evolveOpts) error
}

// AfterSequence specifies the sequence of the first event that should be fetched
// from the sequence up to the end of the sequence. This useful when partially applied
// state has been derived up to a specific sequence and only the latest events need
// to be fetched.
func AfterSequence(seq uint64) EvolveOption {
	return evolveOptFn(func(o *evolveOpts) error {
		o.afterSeq = &seq
		return nil
	})
}

// UpToSequence specifies the sequence of the last event that should be fetched.
// This is useful to control how much replay is performed when evolving a state.
func UpToSequence(seq uint64) EvolveOption {
	return evolveOptFn(func(o *evolveOpts) error {
		o.upToSeq = &seq
		return nil
	})
}

// Filters specifies the subject filter to use when evolving state.
// The filter can be in the form of `<entity-type>`, `<entity-type>.<entity-id>`,
// or `<entity-type>.<entity-id>.<event-type>`. Wildcards can be used as well.
func Filters(filters ...string) EvolveOption {
	return evolveOptFn(func(o *evolveOpts) error {
		o.fillters = filters
		return nil
	})
}

// EventStore provides event store semantics over a NATS stream.
type EventStore struct {
	name          string
	rt            *Rita
	subjectPrefix string
	subjectFunc   func(event *Event) string
}

var entityRegex = regexp.MustCompile(`^[^.]+\.[^.]+$`)

// wrapEvent wraps a user-defined event into the Event envelope. It performs
// validation to ensure all the properties are either defined or defaults are set.
func (s *EventStore) wrapEvent(event *Event) (*Event, error) {
	if event.Data == nil {
		return nil, ErrEventDataRequired
	}

	if event.Entity == "" {
		return nil, ErrEventEntityRequired
	}
	if !entityRegex.MatchString(event.Entity) {
		return nil, ErrEventEntityInvalid
	}

	if s.rt.types == nil {
		if event.Type == "" {
			return nil, ErrEventTypeRequired
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
	msg.Header.Set(eventEntityHdr, event.Entity)

	for k, v := range event.Meta {
		msg.Header.Set(fmt.Sprintf("%s%s", eventMetaPrefixHdr, k), v)
	}

	return msg, nil
}

// Decide is a convenience methods that combines the Decide and Append operations.
func (s *EventStore) Decide(ctx context.Context, model Decider, cmd *Command) error {
	events, err := model.Decide(cmd)
	if err != nil {
		return err
	}

	_, err = s.Append(ctx, events)
	return err
}

// parsePattern parses a subject pattern into the full form
func parsePattern(subject string) (string, error) {
	if subject == "" {
		return "*.*.*", nil
	}

	toks := strings.Split(subject, ".")
	if len(toks) > 3 {
		return "", fmt.Errorf("subject can have at most three tokens")
	}

	ntoks := make([]string, 3)
	// Individual tokens are not validated since this will downstream.
	for i := range ntoks {
		if i < len(toks) {
			ntoks[i] = toks[i]
		} else {
			ntoks[i] = "*"
		}
	}

	return fmt.Sprintf("%s.%s.%s", ntoks[0], ntoks[1], ntoks[2]), nil
}

// Evolve loads events and evolves a model of state. The sequence of the
// last event that evolved the state is returned, including when an error
// occurs. Note, the pattern can be several forms depending on the need.
// The full template is `<entity-type>.<entity-id>.<event-type>`. If only
// the entity type is provided, all events for all entities of that type
// will be loaded. If the entity type and entity ID are provided, all events
// for that specific entity will be loaded. If the full subject is provided,
// only events of that specific type for that specific entity will be loaded.
// Wildcards can be used as well.
func (s *EventStore) Evolve(ctx context.Context, model Evolver, opts ...EvolveOption) (uint64, error) {
	// Configure opts.
	var o evolveOpts
	for _, opt := range opts {
		if err := opt.evolveOpt(&o); err != nil {
			return 0, err
		}
	}

	// If still no patterns, default to all.
	if len(o.fillters) == 0 {
		o.fillters = []string{"*.*.*"}
	}

	// Build subjects from patterns.
	subjects := make([]string, len(o.fillters))
	for i, p := range o.fillters {
		pp, err := parsePattern(p)
		if err != nil {
			return 0, err
		}
		subjects[i] = fmt.Sprintf("%s.%s", s.subjectPrefix, pp)
	}

	// Ephemeral ordered consumer.. read as fast as possible with least overhead.
	sopts := jetstream.OrderedConsumerConfig{
		FilterSubjects: subjects,
	}

	// Set starting point.
	if o.afterSeq != nil {
		if *o.afterSeq == 0 {
			sopts.DeliverPolicy = jetstream.DeliverAllPolicy
		} else {
			sopts.OptStartSeq = *o.afterSeq + 1
			sopts.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		}
	} else {
		sopts.DeliverPolicy = jetstream.DeliverAllPolicy
	}

	con, err := s.rt.js.OrderedConsumer(s.rt.ctx, s.name, sopts)
	if err != nil {
		return 0, err
	}

	// The number of messages to consume until we are caught up
	// to the current known state.
	pending := con.CachedInfo().NumPending

	if pending == 0 {
		return 0, nil
	}

	msgCtx, err := con.Messages()
	if err != nil {
		return 0, err
	}
	defer msgCtx.Stop()

	var lastSeq uint64
	var count uint64
	for {
		msg, err := msgCtx.Next()
		if err != nil {
			return 0, err
		}

		event, err := s.rt.UnpackEvent(msg)
		if err != nil {
			return 0, err
		}

		// If up to sequence is set, break if the event sequence is greater than the up to sequence.
		// This check is here in case there is a gap between sequence numbers.
		if o.upToSeq != nil && event.sequence > *o.upToSeq {
			break
		}

		if err := model.Evolve(event); err != nil {
			return lastSeq, err
		}
		lastSeq = event.sequence

		// Check if we've reached the up to sequence.
		if o.upToSeq != nil && lastSeq == *o.upToSeq {
			break
		}

		count++
		if count == pending {
			break
		}
	}

	return lastSeq, nil
}

func is212(nc *nats.Conn) bool {
	v := nc.ConnectedServerVersion()
	return strings.HasPrefix(v, "2.12.")
}

// Append appends a one or more events to the subject's event sequence.
// It returns the resulting sequence number of the last appended event.
func (s *EventStore) Append(ctx context.Context, events []*Event, opts ...AppendOption) (uint64, error) {
	// Configure opts.
	var o appendOpts
	for _, opt := range opts {
		if err := opt.appendOpt(&o); err != nil {
			return 0, err
		}
	}

	// Prepare messages.
	var msgs []*nats.Msg

	for i, event := range events {
		e, err := s.wrapEvent(event)
		if err != nil {
			return 0, err
		}

		subject := s.subjectFunc(e)
		msg, err := s.packEvent(subject, e)
		if err != nil {
			return 0, err
		}
		msg.Header.Set(nats.ExpectedStreamHdr, s.name)

		if i == 0 {
			if o.expSeq != nil {
				if o.expSubj == "" {
					idx := strings.LastIndex(subject, ".")
					expSubj := fmt.Sprintf("%s.*", subject[:idx])
					msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, fmt.Sprintf("%d", *o.expSeq))
					msg.Header.Set(jetstream.ExpectedLastSubjSeqSubjHeader, expSubj)
				} else {
					expSubj := fmt.Sprintf("%s.%s", s.subjectPrefix, o.expSubj)
					msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, fmt.Sprintf("%d", *o.expSeq))
					msg.Header.Set(jetstream.ExpectedLastSubjSeqSubjHeader, expSubj)
				}
			}
		}

		msgs = append(msgs, msg)
	}

	// Use atomic publish for NATS Server 2.12+
	if is212(s.rt.nc) {
		ack, err := jetstreamext.PublishMsgBatch(ctx, s.rt.js, msgs)
		if err != nil {
			if strings.Contains(err.Error(), "wrong last sequence") {
				return 0, ErrSequenceConflict
			}
			return 0, err
		}

		return ack.Sequence, nil
	}

	// Fallback to individual publishes for older servers.
	var ack *jetstream.PubAck
	var err error
	for _, msg := range msgs {
		ack, err = s.rt.js.PublishMsg(s.rt.ctx, msg)
		if err != nil {
			if strings.Contains(err.Error(), "wrong last sequence") {
				return 0, ErrSequenceConflict
			}
			return 0, err
		}
	}

	return ack.Sequence, nil
}

// parseSubjectPrefix parses and validates that the subject prefix
// ends with "*.*.*" or ">".
func parseSubjectPrefix(s string) (string, error) {
	toks := strings.Split(s, ".")
	if len(toks) < 2 {
		return "", fmt.Errorf("subject must end with '*.*.*' or '>'")
	}

	// Can be the only wildcard.
	if toks[len(toks)-1] == ">" {
		if slices.Contains(toks[:len(toks)-1], "*") {
			return "", fmt.Errorf("wildcards not allowed before '>'")
		}

		return s[:len(s)-2], nil
	}

	if len(toks) < 4 {
		return "", fmt.Errorf("subject must have a prefix before '*.*.*'")
	}

	if toks[len(toks)-3] == "*" && toks[len(toks)-2] == "*" && toks[len(toks)-1] == "*" {
		if slices.Contains(toks[:len(toks)-3], "*") {
			return "", fmt.Errorf("wildcards not allowed before '*.*.*'")
		}

		// Three wildcard tokens and dots.
		return s[:len(s)-6], nil
	}

	return "", fmt.Errorf("subject must end with '*.*.*' or '>'")
}

// Create creates the event store given the configuration. The stream
// name is the name of the store and the subjects default to "{name}.>".
func (s *EventStore) Create(ctx context.Context, config *jetstream.StreamConfig) error {
	if config == nil {
		config = &jetstream.StreamConfig{}
	}

	config.Name = s.name
	switch len(config.Subjects) {
	case 1:
	case 0:
		config.Subjects = []string{fmt.Sprintf("%s.>", s.name)}
	default:
		return fmt.Errorf("only one subject is supported for event stores")
	}

	if is212(s.rt.nc) {
		config.AllowAtomicPublish = true
	}

	prefix, err := parseSubjectPrefix(config.Subjects[0])
	if err != nil {
		return err
	}
	s.subjectPrefix = prefix
	s.subjectFunc = func(event *Event) string {
		return fmt.Sprintf("%s.%s.%s", prefix, event.Entity, event.Type)
	}

	_, err = s.rt.js.CreateStream(ctx, *config)
	return err
}

// Update updates the event store configuration.
func (s *EventStore) Update(ctx context.Context, config *jetstream.StreamConfig) error {
	if config == nil {
		config = &jetstream.StreamConfig{}
	}
	config.Name = s.name
	_, err := s.rt.js.UpdateStream(ctx, *config)
	return err
}

// Delete deletes the event store.
func (s *EventStore) Delete(ctx context.Context) error {
	return s.rt.js.DeleteStream(ctx, s.name)
}

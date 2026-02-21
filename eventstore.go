package rita

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
	"github.com/synadia-labs/rita/clock"
	"github.com/synadia-labs/rita/codec"
	"github.com/synadia-labs/rita/id"
	"github.com/synadia-labs/rita/types"
)

const (
	// Event metadata stored in NATS message headers for efficient filtering and header-only consumption.
	eventEntityHdr     = "Rita-Entity" // Entity identifier (two-token format: "type.id")
	eventTypeHdr       = "Rita-Type"   // Event type name
	eventTimeHdr       = "Rita-Time"   // Event timestamp (RFC3339Nano)
	eventCodecHdr      = "Rita-Codec"  // Codec used to serialize event data
	eventMetaPrefixHdr = "Rita-Meta-"  // Prefix for custom metadata headers
	eventTimeFormat    = time.RFC3339Nano
)

var (
	ErrSequenceConflict       = errors.New("rita: sequence conflict")
	ErrEventDataRequired      = errors.New("rita: event data required")
	ErrEventEntityRequired    = errors.New("rita: event entity required")
	ErrEventEntityInvalid     = errors.New("rita: event entity invalid")
	ErrEventTypeRequired      = errors.New("rita: event type required")
	ErrNoEvents               = errors.New("rita: no events provided")
	ErrEventStoreNameRequired = errors.New("rita: event store name is required")
	ErrSubjectTooManyTokens   = errors.New("rita: subject can have at most three tokens")

	// Entity regex: <entity-type>.<entity-id>. Note this is just a basic validation
	// to ensure there are two tokens separated by a dot. Invalid characters will be
	// caught by NATS server when publishing.
	entityRegex = regexp.MustCompile(`^[^.]+\.[^.]+$`)
)

// parsePattern parses a subject pattern into the full form with exactly three tokens.
// Empty patterns are expanded to "*.*.*". Individual tokens are not validated
// as validation will happen downstream.
func parsePattern(subject string) (string, error) {
	if subject == "" {
		return "*.*.*", nil
	}

	toks := strings.Split(subject, ".")
	if len(toks) > 3 {
		return "", ErrSubjectTooManyTokens
	}

	// Pad with wildcards to reach three tokens
	for len(toks) < 3 {
		toks = append(toks, "*")
	}

	return strings.Join(toks, "."), nil
}

// subjectPrefix returns the subject prefix for this EventStore combined with the given pattern.
func (s *EventStore) subjectPrefix(pattern string) string {
	return fmt.Sprintf(eventStoreSubjectTmpl, s.name) + pattern
}

type options struct {
	filters  []string
	afterSeq *uint64

	// Evolve only
	stopSeq *uint64

	// Watch only
	noWait     bool
	errHandler func(error, *Event, jetstream.Msg)
}

type evolveOptFn func(o *options) error

func (f evolveOptFn) setOpt(o *options) error {
	return f(o)
}

// EvolveOption is an option for the event store Evolve operation.
type EvolveOption interface {
	setOpt(o *options) error
}

// WithAfterSequence specifies the sequence of the first event that should be fetched
// from the sequence up to the end of the sequence. This useful when partially applied
// state has been derived up to a specific sequence and only the latest events need
// to be fetched.
// This can be passed in `Evolve` and `Watch`.
func WithAfterSequence(seq uint64) EvolveOption {
	return evolveOptFn(func(o *options) error {
		o.afterSeq = &seq
		return nil
	})
}

// WithStopSequence specifies the sequence of the last event that should be fetched.
// This is useful to control how much replay is performed when evolving a state.
func WithStopSequence(seq uint64) EvolveOption {
	return evolveOptFn(func(o *options) error {
		o.stopSeq = &seq
		return nil
	})
}

// WithFilters specifies the subject filter to use when evolving state.
// The filter can be in the form of `<entity-type>`, `<entity-type>.<entity-id>`,
// or `<entity-type>.<entity-id>.<event-type>`. Wildcards can be used as well at
// any token position.
// This can be passed in `Evolve` and `Watch`.
func WithFilters(filters ...string) EvolveOption {
	return evolveOptFn(func(o *options) error {
		o.filters = filters
		return nil
	})
}

// Watcher represents an active event subscription. Call Stop to
// drain pending messages and stop the underlying consumer.
type Watcher interface {
	Stop()
}

type watcher struct {
	model  Evolver
	conCtx jetstream.ConsumeContext
	con    jetstream.Consumer

	opts *options
}

func (w *watcher) Stop() {
	w.conCtx.Drain()
}

// WatchOption is an option for the event store Watch operation.
type WatchOption interface {
	setOpt(*options) error
}

type watchOptFn func(o *options) error

func (f watchOptFn) setOpt(o *options) error {
	return f(o)
}

// WithErrHandler sets the error handler function for the watcher.
func WithErrHandler(fn func(error, *Event, jetstream.Msg)) WatchOption {
	return watchOptFn(func(o *options) error {
		o.errHandler = fn
		return nil
	})
}

// WithNoWait configures the watcher to not wait for catch-up before returning.
func WithNoWait() WatchOption {
	return watchOptFn(func(o *options) error {
		o.noWait = true
		return nil
	})
}

// EventStore persists events to a JetStream stream and provides operations
// to append, evolve, and watch events.
type EventStore struct {
	name string

	nc *nats.Conn
	js jetstream.JetStream

	id     id.ID
	clock  clock.Clock
	types  *types.Registry
	logger *slog.Logger
}

// wrapEvent validates and enriches an event with defaults. It ensures the event has
// required fields (data, entity, type), validates the entity format, and sets ID and
// timestamp if not provided.
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

	if s.types == nil {
		if event.Type == "" {
			return nil, ErrEventTypeRequired
		}
	} else {
		t, err := s.types.Lookup(event.Data)
		if err != nil {
			return nil, err
		}

		if event.Type == "" {
			event.Type = t
		} else if event.Type != t {
			return nil, fmt.Errorf("wrong type for event data: %s", event.Type)
		}
	}

	// Set ID if empty.
	if event.ID == "" {
		event.ID = s.id.New()
	}

	// Set time if empty.
	if event.Time.IsZero() {
		event.Time = s.clock.Now().Local()
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

	if s.types == nil {
		data, err = codec.Binary.Marshal(event.Data)
		codecName = codec.Binary.Name()
	} else {
		data, err = s.types.Marshal(event.Data)
		codecName = s.types.Codec().Name()
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
		msg.Header.Set(eventMetaPrefixHdr+k, v)
	}

	return msg, nil
}

// unpackEvent unpacks an Event from a NATS message.
func (s *EventStore) unpackEvent(msg jetstream.Msg) (*Event, error) {
	eventType := msg.Headers().Get(eventTypeHdr)
	codecName := msg.Headers().Get(eventCodecHdr)

	var (
		data any
		err  error
	)

	c, ok := codec.Codecs[codecName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", codec.ErrCodecNotRegistered, codecName)
	}

	// No type registry, so assume byte slice.
	if s.types == nil {
		var b []byte
		err = c.Unmarshal(msg.Data(), &b)
		data = b
	} else {
		var v any
		v, err = s.types.Init(eventType)
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
			return nil, fmt.Errorf("unpack: failed to get metadata: %w", err)
		}
		seq = md.Sequence.Stream
	}

	eventTime, err := time.Parse(eventTimeFormat, msg.Headers().Get(eventTimeHdr))
	if err != nil {
		return nil, fmt.Errorf("unpack: failed to parse event time: %w", err)
	}

	var meta map[string]string

	headers := msg.Headers()
	for h := range headers {
		if strings.HasPrefix(h, eventMetaPrefixHdr) {
			if meta == nil {
				meta = make(map[string]string)
			}
			key := h[len(eventMetaPrefixHdr):]
			meta[key] = headers.Get(h)
		}
	}

	return &Event{
		ID:       headers.Get(nats.MsgIdHdr),
		Entity:   headers.Get(eventEntityHdr),
		Type:     headers.Get(eventTypeHdr),
		Time:     eventTime,
		Data:     data,
		Meta:     meta,
		subject:  msg.Subject(),
		sequence: seq,
	}, nil
}

// Decide is a convenience method that combines a model's Decide invocation
// followed by an Append. If either step fails, an error is returned.
func (s *EventStore) Decide(ctx context.Context, model Decider, cmd *Command) ([]*Event, uint64, error) {
	events, err := model.Decide(cmd)
	if err != nil {
		return nil, 0, err
	}

	seq, err := s.Append(ctx, events)
	if err != nil {
		return events, 0, err
	}

	return events, seq, nil
}

// DecideAndEvolve is a convenience method that decides, stores, and evolves a model
// in one operation. If any step fails, an error is returned. Note, that if the evolve
// step fails, the events have already been stored.
func (s *EventStore) DecideAndEvolve(ctx context.Context, model DeciderEvolver, cmd *Command) ([]*Event, uint64, error) {
	events, err := model.Decide(cmd)
	if err != nil {
		return nil, 0, err
	}

	seq, err := s.Append(ctx, events)
	if err != nil {
		return events, 0, err
	}

	for i, ev := range events {
		ev.sequence = seq - uint64(len(events)) + uint64(i) + 1

		if err := model.Evolve(ev); err != nil {
			return events, seq, err
		}
	}

	return events, seq, nil
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
	var o options

	// Configure opts.
	for _, opt := range opts {
		if err := opt.setOpt(&o); err != nil {
			return 0, err
		}
	}

	// Build subjects from patterns.
	subjects := make([]string, len(o.filters))
	for i, p := range o.filters {
		pp, err := parsePattern(p)
		if err != nil {
			return 0, err
		}
		subjects[i] = s.subjectPrefix(pp)
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

	name := fmt.Sprintf(eventStoreNameTmpl, s.name)
	con, err := s.js.OrderedConsumer(ctx, name, sopts)
	if err != nil {
		return 0, err
	}

	// The number of messages to consume until we are caught up
	// to the current known state.
	info := con.CachedInfo()
	defer func() {
		_ = s.js.DeleteConsumer(ctx, name, info.Name)
	}()

	pending := info.NumPending
	if pending == 0 {
		return 0, nil
	}

	// TODO: more efficient way to do this?
	msgCtx, err := con.Messages()
	if err != nil {
		return 0, err
	}
	defer msgCtx.Stop()

	var lastSeq uint64
	var count uint64
	for {
		// Check if context has been cancelled
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		msg, err := msgCtx.Next()
		if err != nil {
			return 0, err
		}

		event, err := s.unpackEvent(msg)
		if err != nil {
			return 0, err
		}

		// If up to sequence is set, break if the event sequence is greater than the up to sequence.
		// This check is here in case there is a gap between sequence numbers.
		if o.stopSeq != nil && event.sequence > *o.stopSeq {
			break
		}

		if err := model.Evolve(event); err != nil {
			return lastSeq, err
		}
		lastSeq = event.sequence

		// Check if we've reached the up to sequence.
		if o.stopSeq != nil && lastSeq == *o.stopSeq {
			break
		}

		count++
		if count == pending {
			break
		}
	}

	return lastSeq, nil
}

// Append appends a one or more events to the subject's event sequence.
// It returns the resulting sequence number of the last appended event.
func (s *EventStore) Append(ctx context.Context, events []*Event) (uint64, error) {
	if len(events) == 0 {
		return 0, ErrNoEvents
	}

	// Prepare messages.
	var msgs []*nats.Msg

	for _, event := range events {
		e, err := s.wrapEvent(event)
		if err != nil {
			return 0, err
		}

		subject := eventSubject(s.name, e)
		msg, err := s.packEvent(subject, e)
		if err != nil {
			return 0, err
		}

		if event.Expect != nil {
			var expSubj string
			if event.Expect.Pattern != "" {
				pattern, err := parsePattern(event.Expect.Pattern)
				if err != nil {
					return 0, err
				}
				expSubj = s.subjectPrefix(pattern)
			} else {
				// Get the subject up to the last token.
				idx := strings.LastIndex(subject, ".")
				expSubj = fmt.Sprintf("%s.*", subject[:idx])
			}
			msg.Header.Set(jetstream.ExpectedLastSubjSeqSubjHeader, expSubj)
			msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, fmt.Sprintf("%d", event.Expect.Sequence))
		}

		msgs = append(msgs, msg)
	}

	if len(msgs) == 1 {
		ack, err := s.js.PublishMsg(ctx, msgs[0])
		if err != nil {
			if strings.Contains(err.Error(), "wrong last sequence") {
				return 0, ErrSequenceConflict
			}
			return 0, err
		}
		return ack.Sequence, nil
	}

	// Atomic batch publish.
	ack, err := jetstreamext.PublishMsgBatch(ctx, s.js, msgs)
	if err != nil {
		if strings.Contains(err.Error(), "wrong last sequence") {
			return 0, ErrSequenceConflict
		}
		return 0, err
	}

	return ack.Sequence, nil
}

// Watch creates a watcher that asynchronously consumes events from the event store
// and applies them to the provided Evolver. The watcher can be configured with
// various options such as error handling and subject patterns to filter events.
// Since this will update the Evolver asynchronously, the Evolver implementation must be
// thread-safe. Use the `NewModel()` helper to create a thread-safe model.
func (s *EventStore) Watch(ctx context.Context, model Evolver, opts ...WatchOption) (Watcher, error) {
	var o options
	for _, opt := range opts {
		if err := opt.setOpt(&o); err != nil {
			return nil, err
		}
	}

	if o.errHandler == nil {
		o.errHandler = func(err error, ev *Event, msg jetstream.Msg) {
			s.logger.Error("watcher error", "error", err, "event", ev)
		}
	}

	// Build subjects from patterns.
	subjects := make([]string, len(o.filters))
	for i, p := range o.filters {
		pp, err := parsePattern(p)
		if err != nil {
			return nil, err
		}
		subjects[i] = s.subjectPrefix(pp)
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

	name := fmt.Sprintf(eventStoreNameTmpl, s.name)
	con, err := s.js.OrderedConsumer(ctx, name, sopts)
	if err != nil {
		return nil, err
	}

	// The number of messages to consume until we are caught up
	// to the current known state.
	info := con.CachedInfo()

	// Determine if we need to wait for catch-up.
	var catchup bool
	var pending uint64
	done := make(chan struct{})
	if !o.noWait {
		pending = info.NumPending
		catchup = pending > 0
	} else {
		catchup = false
	}
	if !catchup {
		close(done)
	}

	conCtx, err := con.Consume(func(m jetstream.Msg) {
		ev, err := s.unpackEvent(m)
		if err != nil {
			o.errHandler(fmt.Errorf("failed to unpack event: %w", err), nil, m)
			return
		}

		if err := model.Evolve(ev); err != nil {
			o.errHandler(fmt.Errorf("failed to evolve event: %w", err), ev, m)
			return
		}

		if catchup {
			pending--
			if pending == 0 {
				close(done)
				catchup = false
			}
		}
	})
	if err != nil {
		return nil, err
	}

	<-done

	w := &watcher{
		model:  model,
		con:    con,
		conCtx: conCtx,
		opts:   &o,
	}

	return w, nil
}

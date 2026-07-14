package rita

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	ErrTenantInvalid          = errors.New("rita: tenant invalid")
	ErrTenantNotSupported     = errors.New("rita: store is not tenant-enabled")
	ErrTenantRequired         = errors.New("rita: store requires a tenant scope")

	// isSequenceConflict checks if the error is a JetStream wrong last sequence error.
	isSequenceConflict = func(err error) bool {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) {
			return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
		}
		return false
	}

	// subjectToken matches a single NATS subject token: one or more characters
	// that are not the token separator '.', a wildcard ('*' or '>'), or
	// whitespace. These are the characters NATS does not allow inside a token, so
	// rejecting them here stops an invalid entity or tenant from injecting extra
	// tokens or wildcards into a subject.
	subjectToken = `[^.*>\s]+`

	// Tenant regex: a single subject token.
	tenantRegex = regexp.MustCompile(`^` + subjectToken + `$`)
)

// validEntity reports whether entity is two subject tokens joined by a dot
// (<entity-type>.<entity-id>). It is the scan equivalent of
// `^[^.*>\s]+\.[^.*>\s]+$` — validation runs per appended event, so it avoids
// the regexp engine on that path. The rejected characters mirror subjectToken:
// a second separator, wildcards, or whitespace (the regexp \s class) would let
// an entity inject extra tokens or wildcards into a subject.
func validEntity(entity string) bool {
	dot := -1
	for i := 0; i < len(entity); i++ {
		switch entity[i] {
		case '.':
			if dot >= 0 {
				return false
			}
			dot = i
		case '*', '>', ' ', '\t', '\n', '\f', '\r':
			return false
		}
	}
	return dot > 0 && dot < len(entity)-1
}

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

// subjectPrefix returns the subject prefix for this EventStore combined with
// the given pattern. The prefix carries the tenant token when the handle is
// tenant-scoped (set at construction / Tenant re-scoping); otherwise the
// result is byte-identical to the untenanted form "$ES.<name>.<pattern>".
func (s *EventStore) subjectPrefix(pattern string) string {
	return s.prefix + pattern
}

// filtersToSubjects expands user-supplied filter patterns to fully-qualified
// JetStream subjects scoped to this store.
func (s *EventStore) filtersToSubjects(filters []string) ([]string, error) {
	// A tenant-scoped handle with no explicit filters must still be confined to
	// its tenant, so default to the tenant's full pattern ("*.*.*") rather than
	// the whole stream. Untenanted stores keep the historical "no filters means
	// whole stream" behavior (empty FilterSubjects).
	if len(filters) == 0 && s.tenant != "" {
		filters = []string{"*.*.*"}
	}
	subjects := make([]string, len(filters))
	for i, p := range filters {
		pp, err := parsePattern(p)
		if err != nil {
			return nil, err
		}
		subjects[i] = s.subjectPrefix(pp)
	}
	return subjects, nil
}

// subjectsToFilters strips this store's subject prefix from a list of
// fully-qualified JetStream subjects, returning the original filter patterns.
// Subjects that don't carry the prefix are surfaced verbatim (e.g. consumers
// created outside Rita).
//
// On a tenant handle it inverts filtersToSubjects' empty-filter default: a
// decoded set of exactly the tenant-wide pattern ("*.*.*") collapses back to
// empty, so a reactor created with no filters round-trips to no filters rather
// than appearing to carry an explicit one.
func (s *EventStore) subjectsToFilters(subjects []string) []string {
	prefix := s.subjectPrefix("")
	filters := make([]string, 0, len(subjects))
	for _, fs := range subjects {
		if rest, ok := strings.CutPrefix(fs, prefix); ok {
			filters = append(filters, rest)
		} else {
			filters = append(filters, fs)
		}
	}
	if s.tenant != "" && len(filters) == 1 && filters[0] == "*.*.*" {
		return nil
	}
	return filters
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

// optionFunc is the single adapter behind every option constructor. It
// satisfies EvolveOption and WatchOption, which share the same method set.
type optionFunc func(o *options) error

func (f optionFunc) setOpt(o *options) error {
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
	return optionFunc(func(o *options) error {
		o.afterSeq = &seq
		return nil
	})
}

// WithStopSequence specifies the sequence of the last event that should be fetched.
// This is useful to control how much replay is performed when evolving a state.
func WithStopSequence(seq uint64) EvolveOption {
	return optionFunc(func(o *options) error {
		o.stopSeq = &seq
		return nil
	})
}

// FilterOption is the return type of WithFilters; it satisfies the option
// interfaces accepted by Evolve and Watch. Filter knobs for reactors live on
// ReactorConfig instead.
type FilterOption interface {
	EvolveOption
}

// WithFilters specifies the subject filter to use when evolving state or
// watching. The filter can be in the form of `<entity-type>`,
// `<entity-type>.<entity-id>`, or `<entity-type>.<entity-id>.<event-type>`.
// Wildcards can be used as well at any token position. For reactors, set
// filters on ReactorConfig.Filters at Create/Update time.
func WithFilters(filters ...string) FilterOption {
	return optionFunc(func(o *options) error {
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
	conCtx jetstream.ConsumeContext
	cancel context.CancelFunc
}

func (w *watcher) Stop() {
	// Cancel first so any in-flight Evolve call observes the shutdown; then
	// drain remaining buffered messages.
	w.cancel()
	w.conCtx.Drain()
}

// WatchOption is an option for the event store Watch operation.
type WatchOption interface {
	setOpt(*options) error
}

// WithErrHandler sets the error handler function for the watcher.
func WithErrHandler(fn func(error, *Event, jetstream.Msg)) WatchOption {
	return optionFunc(func(o *options) error {
		o.errHandler = fn
		return nil
	})
}

// WithNoWait configures the watcher to not wait for catch-up before returning.
func WithNoWait() WatchOption {
	return optionFunc(func(o *options) error {
		o.noWait = true
		return nil
	})
}

// EventStore persists events to a JetStream stream and provides operations
// to append, evolve, and watch events.
type EventStore struct {
	name string

	// stream is the JetStream stream name backing this store, and prefix is
	// the handle's full subject prefix including any tenant token. Both are
	// fixed for the handle's lifetime, so they are computed once at
	// construction (and on Tenant re-scoping) to keep them off the per-event
	// append path.
	stream string
	prefix string

	// tenant is the active tenant scope for this handle. Empty means unscoped:
	// on a tenant store the store-building operations return ErrTenantRequired;
	// on an untenanted store it is always empty.
	tenant string
	// tenantMode is true when the backing store was created with Tenancy enabled.
	tenantMode bool

	js jetstream.JetStream

	id     id.ID
	clock  clock.Clock
	types  *types.Registry
	logger *slog.Logger
}

// Tenant returns a derived handle scoped to the given tenant. The receiver is
// left unchanged, so scopes are immutable and cheap to derive. Re-scoping
// replaces rather than nests: es.Tenant("a").Tenant("b") is tenant "b".
//
// Tenant returns ErrTenantNotSupported if the store was not created with
// Tenancy enabled, and ErrTenantInvalid if the token is empty or contains
// '.', '*', '>', or whitespace.
func (s *EventStore) Tenant(tenant string) (*EventStore, error) {
	if !s.tenantMode {
		return nil, ErrTenantNotSupported
	}
	if !tenantRegex.MatchString(tenant) {
		return nil, ErrTenantInvalid
	}
	clone := *s
	clone.tenant = tenant
	// Rebuild from the untenanted root so re-scoping replaces rather than
	// nests any prior tenant token.
	clone.prefix = subjectRoot(s.name) + tenant + "."
	return &clone, nil
}

// requireTenant guards store-building operations: on a tenant store a concrete
// tenant scope is mandatory so events are published to — and read from — a
// single tenant. It is a no-op on untenanted stores.
func (s *EventStore) requireTenant() error {
	if s.tenantMode && s.tenant == "" {
		return ErrTenantRequired
	}
	return nil
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
	if !validEntity(event.Entity) {
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

// unpackEvent unpacks an Event from a consumed NATS message.
func (s *EventStore) unpackEvent(msg jetstream.Msg) (*Event, error) {
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
	return s.unpackEventFrom(msg.Subject(), seq, msg.Headers(), msg.Data())
}

// unpackEventFrom builds an Event from the parts every message
// representation shares, so alternative transports (e.g. direct get) can
// reuse the exact unpack path.
func (s *EventStore) unpackEventFrom(subject string, seq uint64, headers nats.Header, data []byte) (*Event, error) {
	eventType := headers.Get(eventTypeHdr)
	codecName := headers.Get(eventCodecHdr)

	var (
		val any
		err error
	)

	c, ok := codec.Codecs[codecName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", codec.ErrCodecNotRegistered, codecName)
	}

	// No type registry, so assume byte slice.
	if s.types == nil {
		var b []byte
		err = c.Unmarshal(data, &b)
		val = b
	} else {
		val, err = s.types.Init(eventType)
		if err == nil {
			err = c.Unmarshal(data, val)
		}
	}
	if err != nil {
		return nil, err
	}

	eventTime, err := time.Parse(eventTimeFormat, headers.Get(eventTimeHdr))
	if err != nil {
		return nil, fmt.Errorf("unpack: failed to parse event time: %w", err)
	}

	var meta map[string]string

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
		Type:     eventType,
		Time:     eventTime,
		Data:     val,
		Meta:     meta,
		subject:  subject,
		sequence: seq,
	}, nil
}

// Decide is a convenience method that combines a model's Decide invocation
// followed by an Append. If either step fails, an error is returned.
func (s *EventStore) Decide(ctx context.Context, model Decider, cmd *Command) ([]*Event, uint64, error) {
	// Guard before invoking the model so an unscoped handle cannot run the
	// decider's side effects only to fail at Append (which re-checks).
	if err := s.requireTenant(); err != nil {
		return nil, 0, err
	}

	events, err := model.Decide(ctx, cmd)
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
// step fails — including via ctx cancellation between events — the events have
// already been stored, and the in-memory model is advanced only up to the event
// prior to the failure. Recovery is to call Evolve with WithAfterSequence to
// replay the remaining events.
func (s *EventStore) DecideAndEvolve(ctx context.Context, model DeciderEvolver, cmd *Command) ([]*Event, uint64, error) {
	// Guard before invoking the model so an unscoped handle cannot run the
	// decider's side effects only to fail at Append (which re-checks).
	if err := s.requireTenant(); err != nil {
		return nil, 0, err
	}

	events, err := model.Decide(ctx, cmd)
	if err != nil {
		return nil, 0, err
	}

	seq, err := s.Append(ctx, events)
	if err != nil {
		return events, 0, err
	}

	for _, ev := range events {
		if err := model.Evolve(ctx, ev); err != nil {
			return events, seq, err
		}
	}

	return events, seq, nil
}

// orderedConsumer builds an ordered consumer from the given options.
func (s *EventStore) orderedConsumer(ctx context.Context, o *options) (jetstream.Consumer, error) {
	subjects, err := s.filtersToSubjects(o.filters)
	if err != nil {
		return nil, err
	}

	sopts := jetstream.OrderedConsumerConfig{
		FilterSubjects: subjects,
	}

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

	return s.js.OrderedConsumer(ctx, s.stream, sopts)
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
	if err := s.requireTenant(); err != nil {
		return 0, err
	}

	var o options
	for _, opt := range opts {
		if err := opt.setOpt(&o); err != nil {
			return 0, err
		}
	}

	con, err := s.orderedConsumer(ctx, &o)
	if err != nil {
		return 0, err
	}

	// The number of messages to consume until we are caught up
	// to the current known state.
	info := con.CachedInfo()
	defer func() {
		_ = s.js.DeleteConsumer(ctx, s.stream, info.Name)
	}()

	pending := info.NumPending
	if pending == 0 {
		return 0, nil
	}

	// Replaying with batched direct gets instead of this ephemeral consumer
	// was evaluated and rejected: it saves the consumer create/delete RPCs
	// (~70µs, visible only on tiny replays) but consumes 2.5-3x slower at
	// 1k+ events and is a wash on per-entity reloads. See
	// BenchmarkEvolveReplay and its directGetReplay prototype.
	msgCtx, err := con.Messages()
	if err != nil {
		return 0, err
	}
	defer msgCtx.Stop()

	// msgCtx.Next() does not observe ctx; unblock it when the caller cancels.
	stopOnCancel := context.AfterFunc(ctx, func() { msgCtx.Stop() })
	defer stopOnCancel()

	var lastSeq uint64
	var count uint64
	for {
		// Check if context has been cancelled
		if err := ctx.Err(); err != nil {
			return lastSeq, err
		}

		msg, err := msgCtx.Next()
		if err != nil {
			// A cancelled ctx surfaces here as ErrMsgIteratorClosed via the
			// AfterFunc above; report it as cancellation so callers can match.
			if cerr := ctx.Err(); cerr != nil {
				return lastSeq, cerr
			}
			return lastSeq, err
		}

		event, err := s.unpackEvent(msg)
		if err != nil {
			return lastSeq, err
		}

		// If up to sequence is set, break if the event sequence is greater than the up to sequence.
		// This check is here in case there is a gap between sequence numbers.
		if o.stopSeq != nil && event.sequence > *o.stopSeq {
			break
		}

		if err := model.Evolve(ctx, event); err != nil {
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
// It returns the resulting sequence number of the last appended event and
// stamps every event's assigned stream sequence, retrievable via
// (*Event).Sequence.
func (s *EventStore) Append(ctx context.Context, events []*Event) (uint64, error) {
	if len(events) == 0 {
		return 0, ErrNoEvents
	}
	if err := s.requireTenant(); err != nil {
		return 0, err
	}

	// Prepare messages.
	msgs := make([]*nats.Msg, 0, len(events))

	for _, event := range events {
		e, err := s.wrapEvent(event)
		if err != nil {
			return 0, err
		}

		subject := s.eventSubject(e)
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
				expSubj = subject[:idx] + ".*"
			}
			msg.Header.Set(jetstream.ExpectedLastSubjSeqSubjHeader, expSubj)
			msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(event.Expect.Sequence, 10))
		}

		msgs = append(msgs, msg)
	}

	var seq uint64
	if len(msgs) == 1 {
		ack, err := s.js.PublishMsg(ctx, msgs[0])
		if err != nil {
			if isSequenceConflict(err) {
				return 0, ErrSequenceConflict
			}
			return 0, err
		}
		seq = ack.Sequence
	} else {
		// Atomic batch publish. Batch rejections surface the same structured
		// *jetstream.APIError as single publishes, so one predicate serves both.
		ack, err := jetstreamext.PublishMsgBatch(ctx, s.js, msgs)
		if err != nil {
			if isSequenceConflict(err) {
				return 0, ErrSequenceConflict
			}
			return 0, err
		}
		seq = ack.Sequence
	}

	// Acks report only the last sequence. The batch is committed atomically,
	// so the events occupy the contiguous run ending at seq; stamping here
	// keeps that assumption in the one place that owns the publish.
	for i := range events {
		events[i].sequence = seq - uint64(len(events)) + uint64(i) + 1
	}

	return seq, nil
}

// Watch creates a watcher that asynchronously consumes events from the event store
// and applies them to the provided Evolver. The watcher can be configured with
// various options such as error handling and subject patterns to filter events.
// Since this will update the Evolver asynchronously, the Evolver implementation must be
// thread-safe. Use the `NewModel()` helper to create a thread-safe model.
func (s *EventStore) Watch(ctx context.Context, model Evolver, opts ...WatchOption) (Watcher, error) {
	if err := s.requireTenant(); err != nil {
		return nil, err
	}

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

	con, err := s.orderedConsumer(ctx, &o)
	if err != nil {
		return nil, err
	}

	// Derive a watcher-scoped context from the caller's. The caller's ctx
	// governs setup only; the watcher's lifetime is governed by Stop(). Without
	// WithoutCancel, a request-scoped ctx cancelled after Watch returns would
	// silently feed cancelled contexts to every subsequent model.Evolve call.
	wctx, wcancel := context.WithCancel(context.WithoutCancel(ctx))

	// The number of messages to consume until we are caught up
	// to the current known state.
	info := con.CachedInfo()

	// Determine if we need to wait for catch-up.
	var pending atomic.Int64
	var closeOnce sync.Once
	done := make(chan struct{})
	if !o.noWait && info.NumPending > 0 {
		pending.Store(int64(info.NumPending))
	} else {
		close(done)
	}

	conCtx, err := con.Consume(func(m jetstream.Msg) {
		// Always decrement, even on unpack/evolve failure, so a single error
		// during catch-up cannot wedge the waiter below. closeOnce ensures we
		// only signal once even when pending crosses zero post-catch-up.
		defer func() {
			if pending.Add(-1) == 0 {
				closeOnce.Do(func() { close(done) })
			}
		}()

		ev, err := s.unpackEvent(m)
		if err != nil {
			o.errHandler(fmt.Errorf("failed to unpack event: %w", err), nil, m)
			return
		}

		if err := model.Evolve(wctx, ev); err != nil {
			o.errHandler(fmt.Errorf("failed to evolve event: %w", err), ev, m)
			return
		}
	})
	if err != nil {
		wcancel()
		return nil, err
	}

	// Wait for catch-up, but let the caller abort via ctx.
	select {
	case <-done:
	case <-ctx.Done():
		wcancel()
		conCtx.Stop()
		return nil, ctx.Err()
	}

	return &watcher{conCtx: conCtx, cancel: wcancel}, nil
}

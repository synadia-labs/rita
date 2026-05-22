package rita

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrUnprocessable          = errors.New("rita: unprocessable event")
	ErrReactorNameRequired    = errors.New("rita: reactor name is required")
	ErrReactorHandlerRequired = errors.New("rita: reactor handler is required")
	ErrReactorNotFound        = errors.New("rita: reactor not found")
	ErrReactorExists          = errors.New("rita: reactor already exists with a different config")
	ErrReactorAlreadyBound    = errors.New("rita: reactor already bound")
)

// ReactorHandler processes a single event for side effects.
//
// Return semantics:
//   - nil                                       -> rita Acks the message.
//   - errors.Is(err, ErrUnprocessable) == true  -> rita Terms the message.
//   - any other non-nil error                   -> rita Naks the message.
type ReactorHandler func(ctx context.Context, ev *Event) error

// Reactor is a handle to a durable consumer. A fresh handle is unbound:
// call Bind to attach a handler and start dispatching events. Unbind
// drains in-flight messages within ctx's deadline; the durable persists
// in JetStream, so a subsequent Bind on the same (or a fresh) handle
// resumes from the stored position. If ctx's deadline expires first,
// Unbind returns ctx.Err(), cancels handler work so in-flight operations
// can abort, and may be called again to wait for final shutdown.
//
// Unbind does not delete the durable consumer. To remove the underlying
// JetStream consumer, call (*EventStore).DeleteReactor. If DeleteReactor
// is called while this Reactor is bound, the consume loop receives a
// terminal error logged via the EventStore's logger; the caller still
// needs to invoke Unbind to release runtime resources.
type Reactor interface {
	Bind(ctx context.Context, handler ReactorHandler) error
	Unbind(ctx context.Context) error
	Name() string
	Info(ctx context.Context) (*ReactorInfo, error)
}

// ReactorConfig describes a durable consumer used to drive a reactor.
//
// Zero-valued options are replaced with Rita defaults on every Create/Update.
//   - MaxAckPending: 1   (serial delivery; protects ordering for side effects)
//   - MaxDeliver:   -1   (unlimited)
//   - AckWait:      30s
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

// ReactorInfo is a point-in-time snapshot of a reactor durable, mirrored from
// JetStream's ConsumerInfo.
type ReactorInfo struct {
	Name           string
	Config         ReactorConfig
	NumPending     uint64
	NumAckPending  int
	NumRedelivered int
	NumWaiting     int
	Created        time.Time
}

// wrapConsumerNotFound translates JetStream's not-found sentinels into the
// rita-level ErrReactorNotFound for the caller, returning the original error
// unchanged for any other failure mode.
func wrapConsumerNotFound(err error) error {
	if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDoesNotExist) {
		return fmt.Errorf("%w: %v", ErrReactorNotFound, err)
	}
	return err
}

func (c *ReactorConfig) applyDefaults() {
	if c.MaxAckPending == 0 {
		c.MaxAckPending = 1
	}
	if c.MaxDeliver == 0 {
		c.MaxDeliver = -1
	}
	if c.AckWait == 0 {
		c.AckWait = 30 * time.Second
	}
}

func (s *EventStore) reactorConsumerConfig(cfg ReactorConfig) (jetstream.ConsumerConfig, error) {
	subjects, err := s.filtersToSubjects(cfg.Filters)
	if err != nil {
		return jetstream.ConsumerConfig{}, err
	}
	return jetstream.ConsumerConfig{
		Durable:        cfg.Name,
		Description:    cfg.Description,
		Metadata:       cfg.Metadata,
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxAckPending:  cfg.MaxAckPending,
		MaxDeliver:     cfg.MaxDeliver,
		BackOff:        cfg.BackOff,
		AckWait:        cfg.AckWait,
		FilterSubjects: subjects,
	}, nil
}

func (s *EventStore) reactorConfigFromConsumer(cc jetstream.ConsumerConfig) ReactorConfig {
	return ReactorConfig{
		Name:          cc.Durable,
		Description:   cc.Description,
		Metadata:      cc.Metadata,
		Filters:       s.subjectsToFilters(cc.FilterSubjects),
		MaxAckPending: cc.MaxAckPending,
		MaxDeliver:    cc.MaxDeliver,
		AckWait:       cc.AckWait,
		BackOff:       cc.BackOff,
	}
}

// CreateReactor provisions a durable consumer for the reactor and returns
// an unbound Reactor handle. Call Bind on the handle to start dispatching
// events. Idempotent on matching config: if a durable with this name
// already exists and its config matches, the call succeeds silently and
// returns a handle for the existing durable. If the config differs,
// returns ErrReactorExists.
func (s *EventStore) CreateReactor(ctx context.Context, cfg ReactorConfig) (Reactor, error) {
	if cfg.Name == "" {
		return nil, ErrReactorNameRequired
	}
	cfg.applyDefaults()
	cc, err := s.reactorConsumerConfig(cfg)
	if err != nil {
		return nil, err
	}
	if _, err := s.js.CreateConsumer(ctx, s.streamName(), cc); err != nil {
		if errors.Is(err, jetstream.ErrConsumerExists) {
			return nil, fmt.Errorf("%w: %v", ErrReactorExists, err)
		}
		return nil, fmt.Errorf("rita: create reactor: %w", err)
	}
	return s.newReactorHandle(ctx, cfg.Name)
}

// UpdateReactor replaces the configuration of an existing reactor durable
// and returns an unbound Reactor handle for the updated durable.
//
// Replace semantics. The supplied ReactorConfig is the complete desired state,
// not a patch. To change one field, call GetReactor, read the current config
// via Info, mutate it, then call UpdateReactor:
//
//	r, err := es.GetReactor(ctx, "shipping-notifier")
//	if err != nil { return err }
//	info, err := r.Info(ctx)
//	if err != nil { return err }
//	cfg := info.Config
//	cfg.AckWait = 10 * time.Second
//	if _, err := es.UpdateReactor(ctx, cfg); err != nil { return err }
//
// Rita's defaults (MaxAckPending=1, MaxDeliver=-1, AckWait=30s) are applied
// at the boundary on every call, so an empty MaxAckPending does not silently
// flip the durable to the JetStream server default of 1000. Filter and
// BackOff slices are taken at face value: nil/empty means "no filters"/"no backoff".
// UpdateReactor persists the durable config only; currently-bound Reactor
// instances do not hot-reload updated settings. Unbind and Bind again to
// pick up changed runtime retry behavior such as BackOff.
//
// Returns ErrReactorNotFound if no durable with this name exists.
func (s *EventStore) UpdateReactor(ctx context.Context, cfg ReactorConfig) (Reactor, error) {
	if cfg.Name == "" {
		return nil, ErrReactorNameRequired
	}
	cfg.applyDefaults()
	cc, err := s.reactorConsumerConfig(cfg)
	if err != nil {
		return nil, err
	}
	if _, err := s.js.UpdateConsumer(ctx, s.streamName(), cc); err != nil {
		if mapped := wrapConsumerNotFound(err); errors.Is(mapped, ErrReactorNotFound) {
			return nil, mapped
		}
		return nil, fmt.Errorf("rita: update reactor: %w", err)
	}
	return s.newReactorHandle(ctx, cfg.Name)
}

// CreateOrUpdateReactor provisions or updates a reactor and returns an
// unbound Reactor handle. Use this for declarative service-startup hooks
// where idempotent provisioning is desired regardless of prior config.
func (s *EventStore) CreateOrUpdateReactor(ctx context.Context, cfg ReactorConfig) (Reactor, error) {
	if cfg.Name == "" {
		return nil, ErrReactorNameRequired
	}
	cfg.applyDefaults()
	cc, err := s.reactorConsumerConfig(cfg)
	if err != nil {
		return nil, err
	}
	if _, err := s.js.CreateOrUpdateConsumer(ctx, s.streamName(), cc); err != nil {
		return nil, fmt.Errorf("rita: create-or-update reactor: %w", err)
	}
	return s.newReactorHandle(ctx, cfg.Name)
}

// DeleteReactor removes the JetStream consumer backing this reactor.
//
// If a Reactor instance is bound when this is called, its Consume loop
// receives a terminal error from JetStream that is logged via the
// EventStore's logger. The caller is still responsible for calling
// (Reactor).Unbind to release the runtime handle.
//
// Returns ErrReactorNotFound if no reactor with this name exists.
func (s *EventStore) DeleteReactor(ctx context.Context, name string) error {
	if name == "" {
		return ErrReactorNameRequired
	}
	if err := s.js.DeleteConsumer(ctx, s.streamName(), name); err != nil {
		if mapped := wrapConsumerNotFound(err); errors.Is(mapped, ErrReactorNotFound) {
			return mapped
		}
		return fmt.Errorf("rita: delete reactor: %w", err)
	}
	return nil
}

// GetReactor returns an unbound Reactor handle for an existing durable.
// Use r.Info(ctx) for a point-in-time snapshot of the durable's
// configuration and stats.
//
// Durable consumers created outside Rita are visible; there is no Rita-specific
// marker to distinguish them.
//
// Returns ErrReactorNotFound if no durable with this name exists.
func (s *EventStore) GetReactor(ctx context.Context, name string) (Reactor, error) {
	if name == "" {
		return nil, ErrReactorNameRequired
	}
	return s.newReactorHandle(ctx, name)
}

// ListReactors returns durable consumers on the stream backing this EventStore.
// Ephemeral consumers - including those created internally by Evolve and Watch -
// are excluded. Durable consumers created outside Rita are included: the filter
// is on whether the consumer has a Durable name, not on any Rita-specific marker.
func (s *EventStore) ListReactors(ctx context.Context) ([]*ReactorInfo, error) {
	stream, err := s.js.Stream(ctx, s.streamName())
	if err != nil {
		return nil, fmt.Errorf("rita: list reactors: %w", err)
	}
	lister := stream.ListConsumers(ctx)
	var out []*ReactorInfo
	for info := range lister.Info() {
		if info.Config.Durable == "" {
			continue
		}
		out = append(out, s.reactorInfoFromJS(info))
	}
	if err := lister.Err(); err != nil {
		return nil, fmt.Errorf("rita: list reactors: %w", err)
	}
	return out, nil
}

func (s *EventStore) reactorInfoFromJS(info *jetstream.ConsumerInfo) *ReactorInfo {
	return &ReactorInfo{
		Name:           info.Name,
		Config:         s.reactorConfigFromConsumer(info.Config),
		NumPending:     info.NumPending,
		NumAckPending:  info.NumAckPending,
		NumRedelivered: info.NumRedelivered,
		NumWaiting:     info.NumWaiting,
		Created:        info.Created,
	}
}

type reactor struct {
	es      *EventStore
	durable string
	cons    jetstream.Consumer

	mu sync.Mutex
	// bound is true between a successful Bind and the corresponding
	// Unbind completion. Bind on a bound reactor returns ErrReactorAlreadyBound.
	bound bool
	// drainStarted gates the one-shot Drain initiation inside Unbind so
	// concurrent callers cooperate on a single drain.
	drainStarted bool
	handler      ReactorHandler
	backOff      []time.Duration
	// handlerCtx is cancelled when Unbind gives up waiting or completes so handlers can abort and release resources.
	handlerCtx    context.Context
	cancelHandler context.CancelFunc
	cc            jetstream.ConsumeContext
	stopped       chan struct{}
}

// newReactorHandle returns an unbound Reactor handle for an existing durable.
// The consumer is fetched eagerly so Bind can read BackOff from the cached
// ConsumerInfo without an extra round-trip.
func (s *EventStore) newReactorHandle(ctx context.Context, name string) (*reactor, error) {
	cons, err := s.js.Consumer(ctx, s.streamName(), name)
	if err != nil {
		if mapped := wrapConsumerNotFound(err); errors.Is(mapped, ErrReactorNotFound) {
			return nil, mapped
		}
		return nil, fmt.Errorf("rita: load reactor handle: %w", err)
	}
	return &reactor{es: s, durable: name, cons: cons}, nil
}

func (r *reactor) dispatch(msg jetstream.Msg) {
	ev, err := r.es.unpackEvent(msg)
	if err != nil {
		r.es.logger.Error("reactor unpack failed", "durable", r.durable, "error", err)
		r.applyMsgAction(msg.Term, "term")
		return
	}

	switch herr := r.handler(r.handlerCtx, ev); {
	case herr == nil:
		r.applyMsgAction(msg.Ack, "ack")
	case errors.Is(herr, ErrUnprocessable):
		r.applyMsgAction(msg.Term, "term")
	default:
		r.nak(msg)
	}
}

func (r *reactor) nak(msg jetstream.Msg) {
	if len(r.backOff) == 0 {
		r.applyMsgAction(msg.Nak, "nak")
		return
	}
	// JetStream's BackOff config only applies to AckWait timeouts, not explicit Naks.
	delay := r.backOff[len(r.backOff)-1]
	md, err := msg.Metadata()
	if err != nil {
		r.es.logger.Warn("reactor metadata read failed", "durable", r.durable, "error", err)
	} else if md.NumDelivered > 0 {
		idx := int(md.NumDelivered) - 1
		if idx < len(r.backOff) {
			delay = r.backOff[idx]
		}
	}
	r.applyMsgAction(func() error { return msg.NakWithDelay(delay) }, "nak")
}

// applyMsgAction runs an ack/nak/term operation and logs any failures.
func (r *reactor) applyMsgAction(action func() error, label string) {
	if err := action(); err != nil {
		r.es.logger.Error("reactor action failed", "action", label, "durable", r.durable, "error", err)
	}
}

func (r *reactor) Name() string {
	return r.durable
}

func (r *reactor) Info(ctx context.Context) (*ReactorInfo, error) {
	info, err := r.cons.Info(ctx)
	if err != nil {
		if mapped := wrapConsumerNotFound(err); errors.Is(mapped, ErrReactorNotFound) {
			return nil, mapped
		}
		return nil, fmt.Errorf("rita: reactor info: %w", err)
	}
	return r.es.reactorInfoFromJS(info), nil
}

// Bind attaches a handler to the reactor and starts dispatching events.
// Bind returns ErrReactorAlreadyBound if the reactor is already bound;
// call Unbind first to rebind with a different handler. BackOff is
// snapshotted at Bind time, so a subsequent UpdateReactor that changes
// BackOff does not affect a currently-bound reactor.
func (r *reactor) Bind(ctx context.Context, handler ReactorHandler) error {
	if handler == nil {
		return ErrReactorHandlerRequired
	}

	r.mu.Lock()
	if r.bound {
		r.mu.Unlock()
		return ErrReactorAlreadyBound
	}

	info, err := r.cons.Info(ctx)
	if err != nil {
		r.mu.Unlock()
		if mapped := wrapConsumerNotFound(err); errors.Is(mapped, ErrReactorNotFound) {
			return mapped
		}
		return fmt.Errorf("rita: bind reactor: %w", err)
	}

	hctx, cancelHandler := context.WithCancel(context.Background())
	r.handler = handler
	r.backOff = info.Config.BackOff
	r.handlerCtx = hctx
	r.cancelHandler = cancelHandler
	r.stopped = make(chan struct{})
	r.drainStarted = false

	cc, err := r.cons.Consume(r.dispatch, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, cerr error) {
		r.es.logger.Error("reactor consume error", "durable", r.durable, "error", cerr)
	}))
	if err != nil {
		cancelHandler()
		r.handler = nil
		r.handlerCtx = nil
		r.cancelHandler = nil
		r.stopped = nil
		r.mu.Unlock()
		return fmt.Errorf("rita: bind reactor consume: %w", err)
	}
	r.cc = cc
	r.bound = true
	r.mu.Unlock()
	return nil
}

// Unbind drains in-flight messages within ctx's deadline and stops the
// underlying consumer. Unbind is idempotent: calling it on an already-
// unbound reactor returns nil immediately. The durable consumer persists
// in JetStream; a subsequent Bind on the same handle resumes from the
// stored position.
//
// Reentrant: if ctx's deadline expires before drain completes, Unbind
// cancels handler work, returns ctx.Err(), and may be called again to
// wait for final shutdown.
func (r *reactor) Unbind(ctx context.Context) error {
	r.mu.Lock()
	if !r.bound {
		r.mu.Unlock()
		return nil
	}

	if !r.drainStarted {
		r.drainStarted = true
		cc := r.cc
		stopped := r.stopped
		r.mu.Unlock()
		cc.Drain()
		go func() {
			<-cc.Closed()
			close(stopped)
		}()
		r.mu.Lock()
	}

	stopped := r.stopped
	cancelHandler := r.cancelHandler
	r.mu.Unlock()

	select {
	case <-stopped:
		cancelHandler()
		r.mu.Lock()
		r.bound = false
		r.mu.Unlock()
		return nil
	case <-ctx.Done():
		cancelHandler()
		return ctx.Err()
	}
}

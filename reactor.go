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
)

// ReactorHandler processes a single event for side effects.
//
// Return semantics:
//   - nil                                       -> rita Acks the message.
//   - errors.Is(err, ErrUnprocessable) == true  -> rita Terms the message.
//   - any other non-nil error                   -> rita Naks the message.
type ReactorHandler func(ctx context.Context, ev *Event) error

// Reactor is an active durable subscription. Stop drains in-flight messages
// within ctx's deadline; the durable persists in JetStream, so a subsequent
// React call with the same name resumes from the stored position. If ctx's
// deadline expires first, Stop returns ctx.Err(), cancels handler work so
// in-flight operations can abort, and may be called again to wait for final
// shutdown.
//
// Stop does not delete the durable consumer. To remove the underlying
// JetStream consumer, call (*EventStore).DeleteReactor. If DeleteReactor is
// called while this Reactor is consuming, the consume loop receives a
// terminal error logged via the EventStore's logger; the caller still needs
// to invoke Stop to release runtime resources.
type Reactor interface {
	Stop(ctx context.Context) error
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

// CreateReactor provisions a durable consumer for the reactor. Idempotent on
// matching config: if a durable with this name already exists and its config
// matches, the call succeeds silently. If the config differs, returns ErrReactorExists.
func (s *EventStore) CreateReactor(ctx context.Context, cfg ReactorConfig) error {
	if cfg.Name == "" {
		return ErrReactorNameRequired
	}
	cfg.applyDefaults()
	cc, err := s.reactorConsumerConfig(cfg)
	if err != nil {
		return err
	}
	if _, err := s.js.CreateConsumer(ctx, s.streamName(), cc); err != nil {
		if errors.Is(err, jetstream.ErrConsumerExists) {
			return fmt.Errorf("%w: %v", ErrReactorExists, err)
		}
		return fmt.Errorf("rita: create reactor: %w", err)
	}
	return nil
}

// UpdateReactor replaces the configuration of an existing reactor durable.
//
// Replace semantics. The supplied ReactorConfig is the complete desired state,
// not a patch. To change one field, call GetReactor, mutate the returned
// config, then call UpdateReactor:
//
//	info, err := es.GetReactor(ctx, "shipping-notifier")
//	if err != nil { return err }
//	cfg := info.Config
//	cfg.AckWait = 10 * time.Second
//	if err := es.UpdateReactor(ctx, cfg); err != nil { return err }
//
// Rita's defaults (MaxAckPending=1, MaxDeliver=-1, AckWait=30s) are applied
// at the boundary on every call, so an empty MaxAckPending does not silently
// flip the durable to the JetStream server default of 1000. Filter and
// BackOff slices are taken at face value: nil/empty means "no filters"/"no backoff".
// UpdateReactor persists the durable config only; running Reactor instances do
// not hot-reload updated settings. Restart React to apply changed runtime retry
// behavior such as BackOff.
//
// Returns ErrReactorNotFound if no durable with this name exists.
func (s *EventStore) UpdateReactor(ctx context.Context, cfg ReactorConfig) error {
	if cfg.Name == "" {
		return ErrReactorNameRequired
	}
	cfg.applyDefaults()
	cc, err := s.reactorConsumerConfig(cfg)
	if err != nil {
		return err
	}
	if _, err := s.js.UpdateConsumer(ctx, s.streamName(), cc); err != nil {
		if errors.Is(err, jetstream.ErrConsumerDoesNotExist) || errors.Is(err, jetstream.ErrConsumerNotFound) {
			return fmt.Errorf("%w: %v", ErrReactorNotFound, err)
		}
		return fmt.Errorf("rita: update reactor: %w", err)
	}
	return nil
}

// CreateOrUpdateReactor provisions or updates a reactor. Use this for declarative service-startup hooks where
// idempotent provisioning is desired regardless of prior config.
func (s *EventStore) CreateOrUpdateReactor(ctx context.Context, cfg ReactorConfig) error {
	if cfg.Name == "" {
		return ErrReactorNameRequired
	}
	cfg.applyDefaults()
	cc, err := s.reactorConsumerConfig(cfg)
	if err != nil {
		return err
	}
	if _, err := s.js.CreateOrUpdateConsumer(ctx, s.streamName(), cc); err != nil {
		return fmt.Errorf("rita: create-or-update reactor: %w", err)
	}
	return nil
}

// DeleteReactor removes the JetStream consumer backing this reactor.
//
// If a Reactor instance is actively consuming when this is called, the
// Consume loop will receive a terminal error from JetStream - that error is
// logged via the EventStore's logger. The caller is still responsible for calling
// (Reactor).Stop to release the runtime handle.
//
// Returns ErrReactorNotFound if no reactor with this name exists.
func (s *EventStore) DeleteReactor(ctx context.Context, name string) error {
	if name == "" {
		return ErrReactorNameRequired
	}
	if err := s.js.DeleteConsumer(ctx, s.streamName(), name); err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDoesNotExist) {
			return fmt.Errorf("%w: %v", ErrReactorNotFound, err)
		}
		return fmt.Errorf("rita: delete reactor: %w", err)
	}
	return nil
}

// GetReactor returns a point-in-time snapshot of the reactor durable.
//
// Durable consumers created outside Rita are visible; there is no Rita-specific
// marker to distinguish them.
//
// Returns ErrReactorNotFound if no durable with this name exists.
func (s *EventStore) GetReactor(ctx context.Context, name string) (*ReactorInfo, error) {
	if name == "" {
		return nil, ErrReactorNameRequired
	}
	cons, err := s.js.Consumer(ctx, s.streamName(), name)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDoesNotExist) {
			return nil, fmt.Errorf("%w: %v", ErrReactorNotFound, err)
		}
		return nil, fmt.Errorf("rita: get reactor: %w", err)
	}
	return s.reactorInfoFromJS(cons.CachedInfo()), nil
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
	handler ReactorHandler
	backOff []time.Duration
	// handlerCtx is cancelled when Stop gives up waiting or completes so handlers can abort and release resources.
	handlerCtx    context.Context
	cancelHandler context.CancelFunc
	cc            jetstream.ConsumeContext
	stopped       chan struct{}
	once          sync.Once
}

// React attaches a handler to an existing reactor durable and starts
// dispatching events.
//
// The durable must already exist (via CreateReactor or CreateOrUpdateReactor);
// React does not create or modify the consumer. Runtime settings used by the
// returned Reactor, such as BackOff, are snapshotted when React starts; later
// UpdateReactor calls affect the durable and will be observed on the next React
// start, not by an already-running Reactor. Returns ErrReactorNotFound if no
// durable with this name exists on the EventStore's stream.
func (s *EventStore) React(ctx context.Context, name string, handler ReactorHandler) (Reactor, error) {
	if name == "" {
		return nil, ErrReactorNameRequired
	}
	if handler == nil {
		return nil, ErrReactorHandlerRequired
	}

	cons, err := s.js.Consumer(ctx, s.streamName(), name)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDoesNotExist) {
			return nil, fmt.Errorf("%w: %v", ErrReactorNotFound, err)
		}
		return nil, fmt.Errorf("rita: lookup reactor consumer: %w", err)
	}

	hctx, cancelHandler := context.WithCancel(context.Background())
	r := &reactor{
		es:            s,
		durable:       name,
		handler:       handler,
		backOff:       cons.CachedInfo().Config.BackOff,
		handlerCtx:    hctx,
		cancelHandler: cancelHandler,
		stopped:       make(chan struct{}),
	}

	cc, err := cons.Consume(r.dispatch, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, cerr error) {
		s.logger.Error("reactor consume error", "durable", name, "error", cerr)
	}))
	if err != nil {
		return nil, fmt.Errorf("rita: start reactor consume: %w", err)
	}
	r.cc = cc

	return r, nil
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

func (r *reactor) Stop(ctx context.Context) error {
	r.once.Do(func() {
		r.cc.Drain()
		go func() {
			<-r.cc.Closed()
			close(r.stopped)
		}()
	})

	select {
	case <-r.stopped:
		r.cancelHandler()
		return nil
	case <-ctx.Done():
		r.cancelHandler()
		return ctx.Err()
	}
}

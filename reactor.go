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
	ErrReactorDurableRequired = errors.New("rita: reactor durable name is required")
	ErrReactorHandlerRequired = errors.New("rita: reactor handler is required")
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
// React call with the same name resumes from the stored position.
type Reactor interface {
	Stop(ctx context.Context) error
}

type reactOptions struct {
	maxAckPending int
	maxDeliver    int
	backOff       []time.Duration
	ackWait       time.Duration
	filters       []string
}

func defaultReactOptions() reactOptions {
	return reactOptions{
		// maxAckPending=1 enforces serial delivery per durable so side-effect
		// handlers observe a strict event order. Override with WithMaxAckPending
		// when concurrent delivery is acceptable.
		maxAckPending: 1,
		maxDeliver:    -1,
		ackWait:       30 * time.Second,
	}
}

// ReactOption configures a React invocation.
type ReactOption interface {
	setReactOpt(o *reactOptions) error
}

type reactOptFn func(o *reactOptions) error

func (f reactOptFn) setReactOpt(o *reactOptions) error {
	return f(o)
}

// WithMaxAckPending sets the maximum number of unacknowledged messages
// in flight cluster-wide for this reactor. Defaults to 1.
func WithMaxAckPending(n int) ReactOption {
	return reactOptFn(func(o *reactOptions) error {
		o.maxAckPending = n
		return nil
	})
}

// WithMaxDeliver sets the maximum number of delivery attempts before
// JetStream stops redelivering. Defaults to -1 (unlimited).
func WithMaxDeliver(n int) ReactOption {
	return reactOptFn(func(o *reactOptions) error {
		o.maxDeliver = n
		return nil
	})
}

// WithBackOff sets a per-attempt redelivery delay schedule.
func WithBackOff(durs ...time.Duration) ReactOption {
	return reactOptFn(func(o *reactOptions) error {
		o.backOff = durs
		return nil
	})
}

// WithAckWait sets the message acknowledgement deadline. Defaults to 30s.
// NOTE: changing AckWait on a redeploy updates the existing durable's
// configuration via CreateOrUpdateConsumer.
func WithAckWait(d time.Duration) ReactOption {
	return reactOptFn(func(o *reactOptions) error {
		o.ackWait = d
		return nil
	})
}

type reactor struct {
	es      *EventStore
	durable string
	handler ReactorHandler
	backOff []time.Duration
	// handlerCtx is cancelled when Stop's own ctx fires so in-flight handlers
	// can abort I/O once the graceful-shutdown deadline is exceeded.
	handlerCtx    context.Context
	cancelHandler context.CancelFunc
	cc            jetstream.ConsumeContext
	wg            sync.WaitGroup
	stopped       chan struct{}
	once          sync.Once
}

// React provisions or resumes a durable consumer keyed on the given durable
// name and starts dispatching events to handler.
func (s *EventStore) React(ctx context.Context, durable string, handler ReactorHandler, opts ...ReactOption) (Reactor, error) {
	if durable == "" {
		return nil, ErrReactorDurableRequired
	}
	if handler == nil {
		return nil, ErrReactorHandlerRequired
	}

	o := defaultReactOptions()
	for _, opt := range opts {
		if err := opt.setReactOpt(&o); err != nil {
			return nil, err
		}
	}

	subjects, err := s.filtersToSubjects(o.filters)
	if err != nil {
		return nil, err
	}

	cfg := jetstream.ConsumerConfig{
		Durable:        durable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxAckPending:  o.maxAckPending,
		MaxDeliver:     o.maxDeliver,
		BackOff:        o.backOff,
		AckWait:        o.ackWait,
		FilterSubjects: subjects,
	}

	cons, err := s.js.CreateOrUpdateConsumer(ctx, s.streamName(), cfg)
	if err != nil {
		return nil, fmt.Errorf("rita: create reactor consumer: %w", err)
	}

	hctx, cancelHandler := context.WithCancel(context.Background())
	r := &reactor{
		es:            s,
		durable:       durable,
		handler:       handler,
		backOff:       o.backOff,
		handlerCtx:    hctx,
		cancelHandler: cancelHandler,
		stopped:       make(chan struct{}),
	}

	cc, err := cons.Consume(r.dispatch)
	if err != nil {
		return nil, fmt.Errorf("rita: start reactor consume: %w", err)
	}
	r.cc = cc

	return r, nil
}

func (r *reactor) dispatch(msg jetstream.Msg) {
	r.wg.Add(1)
	defer r.wg.Done()

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
	if md, err := msg.Metadata(); err == nil && md.NumDelivered > 0 {
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
			r.wg.Wait()
			close(r.stopped)
		}()
	})

	defer r.cancelHandler()
	select {
	case <-r.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

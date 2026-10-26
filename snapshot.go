package rita

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/synadia-labs/rita/codec"
)

var (
	ErrSnapshotConfigInvalid = errors.New("rita: snapshot config invalid")
	ErrSnapshotWrongStore    = errors.New("rita: model is bound to a different event store")
	ErrSnapshotScopeChanged  = errors.New("rita: filters differ from the snapshot binding")
	ErrSnapshotSkipsEvents   = errors.New("rita: start sequence would skip events the model has not applied")
	ErrModelBusy             = errors.New("rita: model is already being evolved or watched by the store")
	ErrEvolvedOutsideStore   = errors.New("rita: snapshot-bound model was evolved outside the event store")
)

type SnapshotConfig struct {
	Name    string
	Version string
	Filters []string
	Codec   codec.Codec
}

func LoadModel[T any](ctx context.Context, store *EventStore, initial T, config SnapshotConfig) (*Model[T], error) {
	if err := validateSnapshotConfig(initial, config); err != nil {
		return nil, err
	}
	filters, err := normalizeFilters(config.Filters)
	if err != nil {
		return nil, err
	}

	m := NewModel(initial)
	m.snap = &snapshotBinding{
		stream:  store.stream,
		tenant:  store.tenant,
		config:  config,
		filters: filters,
	}

	if _, err := store.Evolve(ctx, m); err != nil {
		return nil, err
	}
	return m, nil
}

func validateSnapshotConfig(initial any, config SnapshotConfig) error {
	v := reflect.ValueOf(initial)
	switch {
	case v.Kind() != reflect.Pointer:
		return fmt.Errorf("%w: state must be a pointer, got %T", ErrSnapshotConfigInvalid, initial)
	case v.IsNil():
		return fmt.Errorf("%w: state is a nil pointer", ErrSnapshotConfigInvalid)
	case config.Name == "":
		return fmt.Errorf("%w: name is required", ErrSnapshotConfigInvalid)
	case config.Codec == nil:
		return fmt.Errorf("%w: codec is required", ErrSnapshotConfigInvalid)
	case config.Codec.Name() == "":
		return fmt.Errorf("%w: codec has no name", ErrSnapshotConfigInvalid)
	}
	return nil
}

type snapshotBinding struct {
	stream  string
	tenant  string
	config  SnapshotConfig
	filters []string
	cursor  uint64
	claim   modelClaim
}

func normalizeFilters(filters []string) ([]string, error) {
	if len(filters) == 0 {
		filters = []string{""}
	}
	out := make([]string, len(filters))
	for i, f := range filters {
		p, err := parsePattern(f)
		if err != nil {
			return nil, err
		}
		out[i] = p
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

type snapshotEvolver interface {
	snapshotBinding() *snapshotBinding
	snapshotCursor() uint64
	evolveOrdered(context.Context, *Event) error
	evolveDecision(context.Context, *Event) error
	recordEvolveErr(error)
}

func (m *Model[T]) snapshotBinding() *snapshotBinding {
	return m.snap
}

func (m *Model[T]) snapshotCursor() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snap.cursor
}

func (m *Model[T]) evolveOrdered(ctx context.Context, event *Event) error {
	if m.e == nil {
		return ErrEvolverNotImplemented
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.applyLocked(ctx, event); err != nil {
		return err
	}
	m.snap.cursor = max(m.snap.cursor, event.sequence)
	return nil
}

func (b *snapshotBinding) resolve(cursor uint64, o *options) error {
	if o.filters != nil {
		filters, err := normalizeFilters(o.filters)
		if err != nil {
			return err
		}
		if !slices.Equal(filters, b.filters) {
			return fmt.Errorf("%w: bound to %v, got %v", ErrSnapshotScopeChanged, b.filters, filters)
		}
	}
	o.filters = b.filters

	if o.afterSeq != nil && *o.afterSeq > cursor {
		return fmt.Errorf("%w: model has applied up to %d, asked to start after %d", ErrSnapshotSkipsEvents, cursor, *o.afterSeq)
	}
	if o.afterSeq == nil {
		o.afterSeq = &cursor
	}
	return nil
}

func (b *snapshotBinding) checkStore(s *EventStore) error {
	if s.stream != b.stream || s.tenant != b.tenant {
		return fmt.Errorf("%w: bound to stream %q tenant %q, used with stream %q tenant %q",
			ErrSnapshotWrongStore, b.stream, b.tenant, s.stream, s.tenant)
	}
	return nil
}

type orderedPath struct {
	evolve  func(context.Context, *Event) error
	claim   *modelClaim
	gen     uint64
	model   snapshotEvolver
	halted  atomic.Bool
	stopped atomic.Bool
}

func (p *orderedPath) stop() {
	p.stopped.Store(true)
	p.claim.stop(p.gen)
}

func (p *orderedPath) skip() bool {
	return p.halted.Load() || (p.model != nil && p.stopped.Load())
}

func (p *orderedPath) evolveContext(watchCtx context.Context) context.Context {
	if p.model == nil {
		return watchCtx
	}
	return context.WithoutCancel(watchCtx)
}

func (p *orderedPath) halt(err error) {
	if p.model == nil {
		return
	}
	p.halted.Store(true)
	p.model.recordEvolveErr(err)
}

func (s *EventStore) orderedPath(ctx context.Context, model Evolver, o *options) (*orderedPath, error) {
	sm, ok := model.(snapshotEvolver)
	if !ok || sm.snapshotBinding() == nil {
		return &orderedPath{evolve: model.Evolve}, nil
	}
	b := sm.snapshotBinding()
	if err := b.checkStore(s); err != nil {
		return nil, err
	}
	gen, err := b.claim.acquire(ctx)
	if err != nil {
		return nil, err
	}
	if err := b.resolve(sm.snapshotCursor(), o); err != nil {
		b.claim.release(gen)
		return nil, err
	}
	return &orderedPath{evolve: sm.evolveOrdered, claim: &b.claim, gen: gen, model: sm}, nil
}

type modelClaim struct {
	mu       sync.Mutex
	held     bool
	stopped  bool
	released chan struct{}
	gen      uint64
}

func (l *modelClaim) acquire(ctx context.Context) (uint64, error) {
	for {
		l.mu.Lock()
		if !l.held {
			l.gen++
			l.held, l.stopped, l.released = true, false, make(chan struct{})
			gen := l.gen
			l.mu.Unlock()
			return gen, nil
		}
		if !l.stopped {
			l.mu.Unlock()
			return 0, ErrModelBusy
		}
		released := l.released
		l.mu.Unlock()

		select {
		case <-released:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

func (l *modelClaim) stop(gen uint64) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held && l.gen == gen {
		l.stopped = true
	}
}

func (l *modelClaim) release(gen uint64) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.held || l.gen != gen {
		return
	}
	l.held = false
	close(l.released)
}

func (s *EventStore) decisionEvolve(model Evolver) (func(context.Context, *Event) error, error) {
	sm, ok := model.(snapshotEvolver)
	if !ok || sm.snapshotBinding() == nil {
		return model.Evolve, nil
	}
	if err := sm.snapshotBinding().checkStore(s); err != nil {
		return nil, err
	}
	return sm.evolveDecision, nil
}

func (m *Model[T]) evolveDecision(ctx context.Context, event *Event) error {
	if m.e == nil {
		return ErrEvolverNotImplemented
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applyLocked(ctx, event)
}

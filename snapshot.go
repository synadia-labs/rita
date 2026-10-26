package rita

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-labs/rita/codec"
)

var (
	ErrSnapshotConfigInvalid = errors.New("rita: snapshot config is not valid")
	ErrSnapshotWrongStore    = errors.New("rita: model uses a different event store")
	ErrSnapshotScopeChanged  = errors.New("rita: filters are different from the snapshot filters")
	ErrSnapshotSkipsEvents   = errors.New("rita: start sequence skips events that the model did not apply")
	ErrModelBusy             = errors.New("rita: another Evolve or Watch uses this model")
	ErrEvolvedOutsideStore   = errors.New("rita: snapshot model received events outside the event store")
	ErrSnapshotNotConfigured = errors.New("rita: model has no snapshot config")
	ErrSnapshotInvalid       = errors.New("rita: snapshot is not valid")
	ErrSnapshotStale         = errors.New("rita: snapshot does not match the stream")
	ErrSnapshotHistoryGap    = errors.New("rita: events after the snapshot are missing from the stream")
	ErrSnapshotConflict      = errors.New("rita: another writer saved the snapshot at the same time")

	snapshotBucketRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
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
		store:   store,
		config:  config,
		filters: filters,
	}

	info, err := m.snap.open(ctx)
	if err != nil {
		return nil, err
	}
	m.snap.created = info.Created
	env, _, err := m.snap.read(ctx, info)
	if err != nil {
		return nil, err
	}
	if env != nil {
		if err := config.Codec.Unmarshal(env.State, m.t); err != nil {
			return nil, fmt.Errorf("%w: cannot decode state: %w", ErrSnapshotInvalid, err)
		}
		m.lseq, m.snap.cursor = env.Sequences, env.Cursor
	} else {
		m.snap.cursor = max(info.State.FirstSeq, 1) - 1
	}

	if _, err := store.Evolve(ctx, m); err != nil {
		return nil, err
	}
	m.snap.cursor = max(m.snap.cursor, info.State.LastSeq)
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
	store   *EventStore
	kv      jetstream.KeyValue
	key     string
	created time.Time
	config  SnapshotConfig
	filters []string
	cursor  uint64
	claim   modelClaim
}

type snapshotIdentity struct {
	Stream  string
	Tenant  string
	Name    string
	Version string
	Codec   string
	Filters []string
}

type snapshotEnvelope struct {
	Identity  snapshotIdentity
	Created   time.Time
	Cursor    uint64
	Sequences map[string]uint64
	State     []byte
}

func (b *snapshotBinding) identity() snapshotIdentity {
	return snapshotIdentity{b.store.stream, b.store.tenant, b.config.Name, b.config.Version, b.config.Codec.Name(), b.filters}
}

func snapshotBucket(storeName string) (string, error) {
	bucket := "ES_SNAP_" + storeName
	if !snapshotBucketRegex.MatchString(bucket) {
		return "", fmt.Errorf("%w: store name %q is not a valid KV bucket name", ErrSnapshotConfigInvalid, storeName)
	}
	return bucket, nil
}

func (b *snapshotBinding) open(ctx context.Context) (*jetstream.StreamInfo, error) {
	if err := b.store.requireTenant(); err != nil {
		return nil, err
	}
	bucket, err := snapshotBucket(b.store.name)
	if err != nil {
		return nil, err
	}
	stream, err := b.store.js.Stream(ctx, b.store.stream)
	if err != nil {
		return nil, fmt.Errorf("read stream info: %w", err)
	}
	info := stream.CachedInfo()
	b.kv, err = b.store.js.KeyValue(ctx, bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		b.kv, err = b.store.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket: bucket, Storage: info.Config.Storage, Replicas: info.Config.Replicas, History: 1,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("open snapshot bucket: %w", err)
	}
	identity, err := json.Marshal(b.identity())
	if err != nil {
		return nil, err
	}
	b.key = fmt.Sprintf("%x", sha256.Sum256(identity))
	return info, nil
}

func (b *snapshotBinding) read(ctx context.Context, info *jetstream.StreamInfo) (*snapshotEnvelope, uint64, error) {
	entry, err := b.kv.Get(ctx, b.key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("read snapshot: %w", err)
	}
	var env snapshotEnvelope
	if err := json.Unmarshal(entry.Value(), &env); err != nil {
		return nil, 0, fmt.Errorf("%w: cannot decode stored value: %w", ErrSnapshotInvalid, err)
	}
	switch {
	case !reflect.DeepEqual(env.Identity, b.identity()):
		return nil, 0, fmt.Errorf("%w: stored value is for a different snapshot config", ErrSnapshotInvalid)
	case env.Sequences == nil:
		return nil, 0, fmt.Errorf("%w: stored value has no entity sequences", ErrSnapshotInvalid)
	}
	if !env.Created.Equal(info.Created) || env.Cursor > info.State.LastSeq {
		return nil, 0, ErrSnapshotStale
	}
	for entity, seq := range env.Sequences {
		if !validEntity(entity) {
			return nil, 0, fmt.Errorf("%w: entity %q is not valid", ErrSnapshotInvalid, entity)
		}
		if seq > info.State.LastSeq {
			return nil, 0, ErrSnapshotStale
		}
	}
	return &env, entry.Revision(), nil
}

func (m *Model[T]) SaveSnapshot(ctx context.Context) error {
	b := m.snap
	if b == nil {
		return ErrSnapshotNotConfigured
	}
	candidate, err := m.captureSnapshot()
	if err != nil {
		return err
	}
	data, err := json.Marshal(candidate)
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	stream, err := b.store.js.Stream(ctx, b.store.stream)
	if err != nil {
		return fmt.Errorf("read stream info: %w", err)
	}
	info := stream.CachedInfo()
	if !b.created.Equal(info.Created) || candidate.Cursor > info.State.LastSeq {
		return ErrSnapshotStale
	}
	previous, revision, err := b.read(ctx, info)
	if err != nil {
		return err
	}
	if previous != nil && previous.Cursor >= candidate.Cursor {
		return nil
	}
	return b.write(ctx, info, data, candidate.Cursor, revision)
}

func (m *Model[T]) captureSnapshot() (snapshotEnvelope, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.evolveFailureLocked(); err != nil {
		return snapshotEnvelope{}, err
	}
	state, err := m.snap.config.Codec.Marshal(m.t)
	if err != nil {
		return snapshotEnvelope{}, fmt.Errorf("encode snapshot state: %w", err)
	}
	return snapshotEnvelope{
		Identity: m.snap.identity(), Created: m.snap.created,
		Cursor: m.snap.cursor, Sequences: maps.Clone(m.lseq), State: state,
	}, nil
}

func (b *snapshotBinding) write(ctx context.Context, info *jetstream.StreamInfo, data []byte, cursor, revision uint64) error {
	var err error
	if revision == 0 {
		_, err = b.kv.Create(ctx, b.key, data)
	} else {
		_, err = b.kv.Update(ctx, b.key, data, revision)
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		previous, _, readErr := b.read(ctx, info)
		if readErr != nil {
			return readErr
		}
		if previous != nil && previous.Cursor >= cursor {
			return nil
		}
		return fmt.Errorf("%w: %w", ErrSnapshotConflict, err)
	}
	return fmt.Errorf("save snapshot: %w", err)
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
			return fmt.Errorf("%w: snapshot uses %v, got %v", ErrSnapshotScopeChanged, b.filters, filters)
		}
	}
	o.filters = b.filters

	if o.afterSeq != nil && *o.afterSeq > cursor {
		return fmt.Errorf("%w: model applied events up to %d, start sequence is %d", ErrSnapshotSkipsEvents, cursor, *o.afterSeq)
	}
	if o.afterSeq == nil {
		o.afterSeq = &cursor
	}
	return nil
}

func (b *snapshotBinding) checkStore(s *EventStore) error {
	if s.stream != b.store.stream || s.tenant != b.store.tenant {
		return fmt.Errorf("%w: model uses stream %q tenant %q, got stream %q tenant %q",
			ErrSnapshotWrongStore, b.store.stream, b.store.tenant, s.stream, s.tenant)
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

func (p *orderedPath) checkHistory(info *jetstream.ConsumerInfo) error {
	if p.model != nil && info.Delivered.Stream > p.model.snapshotCursor() {
		return ErrSnapshotHistoryGap
	}
	return nil
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

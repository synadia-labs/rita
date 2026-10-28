package rita

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"github.com/synadia-labs/rita/codec"
)

var (
	ErrSnapshotConfigInvalid = errors.New("rita: snapshot config is not valid")
	ErrSnapshotWrongStore    = errors.New("rita: model uses a different event store")
	ErrSnapshotScopeChanged  = errors.New("rita: filters are different from the snapshot filters")
	ErrSnapshotSkipsEvents   = errors.New("rita: start sequence skips events that the model did not apply")
	ErrModelBusy             = errors.New("rita: another Evolve or Watch uses this model")
	ErrEvolvedOutsideStore   = errors.New("rita: snapshot model received events outside the event store; use EventStore.Evolve or Watch")
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
	return loadModel(ctx, store, initial, config,
		func() ([]byte, error) { return config.Codec.Marshal(initial) },
		func(data []byte) error { return config.Codec.Unmarshal(data, initial) },
	)
}

func loadModel[T any](ctx context.Context, store *EventStore, initial T, config SnapshotConfig, encode func() ([]byte, error), restore func([]byte) error) (*Model[T], error) {
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
		encode:  encode,
	}

	if err := m.snap.open(ctx); err != nil {
		return nil, err
	}
	env, info, err := m.snap.load(ctx, m.snap.fetch)
	if err != nil {
		return nil, err
	}
	m.snap.cursor = max(info.State.FirstSeq, 1) - 1
	if env != nil {
		if err := restore(env.State); err != nil {
			return nil, fmt.Errorf("%w: cannot decode state: %w", ErrSnapshotInvalid, err)
		}
		m.lseq, m.snap.cursor = env.Sequences, env.Cursor
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
	objects jetstream.ObjectStore
	key     string
	created time.Time
	config  SnapshotConfig
	filters []string
	cursor  uint64
	claim   modelClaim
	encode  func() ([]byte, error)
}

const snapshotFormat = 1

type snapshotIdentity struct {
	Format  int
	Stream  string
	Tenant  string
	Name    string
	Version string
	Codec   string
	Filters []string
}

type snapshotManifest struct {
	Identity snapshotIdentity
	Created  time.Time
	Cursor   uint64
	Current  string
	Previous string
}

type snapshotEnvelope struct {
	Identity  snapshotIdentity
	Created   time.Time
	Cursor    uint64
	Sequences map[string]uint64
	State     []byte `json:"-"`
}

const snapshotHeaderLenSize = 8

func (env *snapshotEnvelope) encodeHeader() ([]byte, error) {
	header, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	out := binary.BigEndian.AppendUint64(make([]byte, 0, snapshotHeaderLenSize+len(header)), uint64(len(header)))
	return append(out, header...), nil
}

func decodeSnapshotEnvelope(data []byte) (*snapshotEnvelope, error) {
	if len(data) < snapshotHeaderLenSize {
		return nil, errors.New("object is shorter than its header length")
	}
	size := binary.BigEndian.Uint64(data)
	rest := data[snapshotHeaderLenSize:]
	if size > uint64(len(rest)) {
		return nil, errors.New("header length exceeds object size")
	}
	var env snapshotEnvelope
	if err := json.Unmarshal(rest[:size], &env); err != nil {
		return nil, err
	}
	env.State = rest[size:]
	return &env, nil
}

func (b *snapshotBinding) identity() snapshotIdentity {
	return snapshotIdentity{snapshotFormat, b.store.stream, b.store.tenant, b.config.Name, b.config.Version, b.config.Codec.Name(), b.filters}
}

func snapshotBuckets(storeName string) (manifests, objects string, err error) {
	manifests = "ES_SNAP_META_" + storeName
	if !snapshotBucketRegex.MatchString(manifests) {
		return "", "", fmt.Errorf("%w: store name %q is not a valid KV bucket name", ErrSnapshotConfigInvalid, storeName)
	}
	return manifests, "ES_SNAP_DATA_" + storeName, nil
}

func (b *snapshotBinding) open(ctx context.Context) error {
	if err := b.store.requireTenant(); err != nil {
		return err
	}
	bucket, dataBucket, err := snapshotBuckets(b.store.name)
	if err != nil {
		return err
	}
	info, err := b.streamInfo(ctx)
	if err != nil {
		return err
	}
	b.created = info.Created
	b.kv, err = b.store.js.KeyValue(ctx, bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		b.kv, err = b.store.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket: bucket, Storage: info.Config.Storage, Replicas: info.Config.Replicas, History: 1,
		})
	}
	if err != nil {
		return fmt.Errorf("open snapshot bucket: %w", err)
	}
	b.objects, err = b.store.js.ObjectStore(ctx, dataBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		b.objects, err = b.store.js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
			Bucket: dataBucket, Storage: info.Config.Storage, Replicas: info.Config.Replicas,
		})
	}
	if err != nil {
		return fmt.Errorf("open snapshot data bucket: %w", err)
	}
	identity, err := json.Marshal(b.identity())
	if err != nil {
		return err
	}
	b.key = fmt.Sprintf("%x", sha256.Sum256(identity))
	return nil
}

func (b *snapshotBinding) streamInfo(ctx context.Context) (*jetstream.StreamInfo, error) {
	stream, err := b.store.js.Stream(ctx, b.store.stream)
	if err != nil {
		return nil, fmt.Errorf("read stream info: %w", err)
	}
	return stream.CachedInfo(), nil
}

func covers(previous *snapshotManifest, cursor uint64) bool {
	return previous != nil && previous.Cursor >= cursor
}

func (b *snapshotBinding) read(ctx context.Context) (*snapshotManifest, *jetstream.StreamInfo, uint64, error) {
	entry, err := b.kv.Get(ctx, b.key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, nil, 0, fmt.Errorf("read snapshot: %w", err)
	}
	info, infoErr := b.streamInfo(ctx)
	if infoErr != nil {
		return nil, nil, 0, infoErr
	}
	if err != nil {
		return nil, info, 0, nil
	}
	var manifest snapshotManifest
	if err := json.Unmarshal(entry.Value(), &manifest); err != nil {
		return nil, nil, 0, fmt.Errorf("%w: cannot decode stored value: %w", ErrSnapshotInvalid, err)
	}
	switch {
	case !reflect.DeepEqual(manifest.Identity, b.identity()):
		return nil, nil, 0, fmt.Errorf("%w: stored value is for a different snapshot config", ErrSnapshotInvalid)
	case manifest.Current == "":
		return nil, nil, 0, fmt.Errorf("%w: stored value names no snapshot object", ErrSnapshotInvalid)
	}
	if staleAgainst(info, manifest.Created, manifest.Cursor) {
		return nil, nil, 0, ErrSnapshotStale
	}
	return &manifest, info, entry.Revision(), nil
}

type snapshotFetch func(context.Context, *jetstream.StreamInfo, *snapshotManifest) (*snapshotEnvelope, error)

const (
	snapshotFetchAttempts = 3
	snapshotFetchTimeout  = 30 * time.Second
	snapshotFetchBackoff  = 50 * time.Millisecond

	snapshotDownloadPrealloc uint64 = 8 << 20
)

func snapshotFetchContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, snapshotFetchTimeout)
}

func (b *snapshotBinding) load(ctx context.Context, fetch snapshotFetch) (*snapshotEnvelope, *jetstream.StreamInfo, error) {
	var fetchErr error
	for attempt := range snapshotFetchAttempts {
		manifest, info, revision, err := b.read(ctx)
		if err != nil || manifest == nil {
			return nil, info, err
		}
		fetchCtx, cancel := snapshotFetchContext(ctx)
		env, err := fetch(fetchCtx, info, manifest)
		cancel()
		if err == nil {
			return env, info, nil
		}
		fetchErr = err
		latest, err := b.kv.Get(ctx, b.key)
		if err != nil {
			return nil, nil, fetchErr
		}
		if latest.Revision() != revision {
			continue
		}
		if !errors.Is(fetchErr, jetstream.ErrObjectNotFound) || attempt == snapshotFetchAttempts-1 {
			return nil, nil, fetchErr
		}
		select {
		case <-time.After(snapshotFetchBackoff * time.Duration(attempt+1)):
		case <-ctx.Done():
			return nil, nil, fetchErr
		}
	}
	return nil, nil, fetchErr
}

func (b *snapshotBinding) fetch(ctx context.Context, info *jetstream.StreamInfo, manifest *snapshotManifest) (*snapshotEnvelope, error) {
	data, err := b.download(ctx, manifest.Current)
	if errors.Is(err, jetstream.ErrObjectNotFound) {
		return nil, fmt.Errorf("%w: snapshot object %q: %w", ErrSnapshotInvalid, manifest.Current, err)
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot object %q: %w", manifest.Current, err)
	}
	env, err := decodeSnapshotEnvelope(data)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot decode snapshot object: %w", ErrSnapshotInvalid, err)
	}
	switch {
	case !reflect.DeepEqual(env.Identity, manifest.Identity) || !env.Created.Equal(manifest.Created) || env.Cursor != manifest.Cursor:
		return nil, fmt.Errorf("%w: snapshot object does not match its manifest", ErrSnapshotInvalid)
	case env.Sequences == nil:
		return nil, fmt.Errorf("%w: snapshot object has no entity sequences", ErrSnapshotInvalid)
	}
	for entity, seq := range env.Sequences {
		if !validEntity(entity) {
			return nil, fmt.Errorf("%w: entity %q is not valid", ErrSnapshotInvalid, entity)
		}
		if seq > info.State.LastSeq {
			return nil, ErrSnapshotStale
		}
	}
	return env, nil
}

func (b *snapshotBinding) download(ctx context.Context, object string) ([]byte, error) {
	result, err := b.objects.Get(ctx, object)
	if err != nil {
		return nil, err
	}
	defer func() { _ = result.Close() }()
	info, err := result.Info()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Grow(int(min(info.Size, snapshotDownloadPrealloc)))
	if _, err := buf.ReadFrom(result); err != nil {
		return nil, err
	}
	if uint64(buf.Len()) != info.Size {
		return nil, fmt.Errorf("%w: snapshot object size does not match metadata", ErrSnapshotInvalid)
	}
	return buf.Bytes(), nil
}

func (m *Model[T]) SaveSnapshot(ctx context.Context) error {
	b := m.snap
	if b == nil {
		return ErrSnapshotNotConfigured
	}
	cursor, err := m.savableCursor()
	if err != nil {
		return err
	}
	info, err := b.streamInfo(ctx)
	if err != nil {
		return err
	}
	if staleAgainst(info, b.created, cursor) {
		return ErrSnapshotStale
	}
	previous, _, revision, err := b.read(ctx)
	if err != nil {
		return err
	}
	if covers(previous, cursor) {
		return nil
	}
	candidate, err := m.captureSnapshot()
	if err != nil {
		return err
	}
	next, err := b.upload(ctx, candidate)
	if err != nil {
		return err
	}
	return b.publish(ctx, next, revision, previous)
}

func (m *Model[T]) savableCursor() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.evolveFailureLocked(); err != nil {
		return 0, err
	}
	return m.snap.cursor, nil
}

func staleAgainst(info *jetstream.StreamInfo, created time.Time, cursor uint64) bool {
	return !created.Equal(info.Created) || cursor > info.State.LastSeq
}

func (b *snapshotBinding) upload(ctx context.Context, candidate snapshotEnvelope) (snapshotManifest, error) {
	header, err := candidate.encodeHeader()
	if err != nil {
		return snapshotManifest{}, fmt.Errorf("encode snapshot: %w", err)
	}
	object := b.key + "." + nuid.Next()
	body := io.MultiReader(bytes.NewReader(header), bytes.NewReader(candidate.State))
	if _, err := b.objects.Put(ctx, jetstream.ObjectMeta{Name: object}, body); err != nil {
		return snapshotManifest{}, fmt.Errorf("upload snapshot: %w", err)
	}
	return snapshotManifest{
		Identity: candidate.Identity, Created: candidate.Created, Cursor: candidate.Cursor, Current: object,
	}, nil
}

func (b *snapshotBinding) retire(ctx context.Context, object string) {
	if object == "" {
		return
	}
	if err := b.objects.Delete(ctx, object); err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
		b.store.logger.Warn("snapshot object cleanup failed", "object", object, "error", err)
	}
}

func (m *Model[T]) captureSnapshot() (snapshotEnvelope, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.evolveFailureLocked(); err != nil {
		return snapshotEnvelope{}, err
	}
	state, err := m.snap.encode()
	if err != nil {
		return snapshotEnvelope{}, fmt.Errorf("encode snapshot state: %w", err)
	}
	return snapshotEnvelope{
		Identity: m.snap.identity(), Created: m.snap.created,
		Cursor: m.snap.cursor, Sequences: maps.Clone(m.lseq), State: state,
	}, nil
}

func publishRejected(err error) bool {
	return errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch)
}

func (b *snapshotBinding) publish(ctx context.Context, next snapshotManifest, revision uint64, previous *snapshotManifest) error {
	info, err := b.streamInfo(ctx)
	if err != nil {
		return err
	}
	if staleAgainst(info, next.Created, next.Cursor) {
		b.retire(ctx, next.Current)
		return ErrSnapshotStale
	}
	if previous != nil {
		next.Previous = previous.Current
	}
	data, err := json.Marshal(next)
	if err != nil {
		return fmt.Errorf("encode snapshot manifest: %w", err)
	}
	if revision == 0 {
		_, err = b.kv.Create(ctx, b.key, data)
	} else {
		_, err = b.kv.Update(ctx, b.key, data, revision)
	}
	switch {
	case err == nil:
		if previous != nil {
			b.retire(ctx, previous.Previous)
		}
		return nil
	case !publishRejected(err):
		return fmt.Errorf("save snapshot: %w", err)
	}
	b.retire(ctx, next.Current)
	current, _, _, readErr := b.read(ctx)
	if readErr != nil {
		return readErr
	}
	if covers(current, next.Cursor) {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrSnapshotConflict, err)
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
	if p.claim != nil {
		p.claim.stop(p.gen)
	}
}

func (p *orderedPath) release() {
	if p.claim != nil {
		p.claim.release(p.gen)
	}
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

func (s *EventStore) boundModel(model Evolver) (snapshotEvolver, *snapshotBinding, error) {
	sm, ok := model.(snapshotEvolver)
	if !ok || sm.snapshotBinding() == nil {
		return nil, nil, nil
	}
	b := sm.snapshotBinding()
	return sm, b, b.checkStore(s)
}

func (s *EventStore) orderedPath(ctx context.Context, model Evolver, o *options) (*orderedPath, error) {
	sm, b, err := s.boundModel(model)
	if err != nil {
		return nil, err
	}
	if sm == nil {
		return &orderedPath{evolve: model.Evolve}, nil
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
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held && l.gen == gen {
		l.stopped = true
	}
}

func (l *modelClaim) release(gen uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.held || l.gen != gen {
		return
	}
	l.held = false
	close(l.released)
}

func (s *EventStore) decisionEvolve(model Evolver) (func(context.Context, *Event) error, error) {
	sm, _, err := s.boundModel(model)
	if err != nil {
		return nil, err
	}
	if sm == nil {
		return model.Evolve, nil
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

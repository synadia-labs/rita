package rita

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-labs/rita/clock"
	"github.com/synadia-labs/rita/id"
	"github.com/synadia-labs/rita/types"
)

const (
	eventStoreNameTmpl    = "ES_%s"
	eventStoreSubjectTmpl = "$ES.%s."

	// tenantMetaKey marks a stream as a tenant store via its metadata. Its
	// presence is what GetEventStore keys on to set the store's mode; the value
	// is a minimal non-empty marker. The mode is fixed at creation and never toggled.
	tenantMetaKey = "rita.tenant"
	tenantMetaVal = "1"

	// natsMetaPrefix marks metadata keys the NATS server manages itself (stream
	// API level, server version). They appear on reads but must not be supplied
	// on writes — the server re-derives them — so a merge drops them rather than
	// echoing a stale snapshot back.
	natsMetaPrefix = "_nats."
)

type managerOption func(o *Manager) error

func (f managerOption) addOption(o *Manager) error {
	return f(o)
}

// ManagerOption models a option when creating a type registry.
type ManagerOption interface {
	addOption(o *Manager) error
}

// WithRegistry sets an explicit type registry.
func WithRegistry(types *types.Registry) ManagerOption {
	return managerOption(func(o *Manager) error {
		o.types = types
		return nil
	})
}

// WithClock sets a clock implementation. Default it clock.Time.
func WithClock(clock clock.Clock) ManagerOption {
	return managerOption(func(o *Manager) error {
		o.clock = clock
		return nil
	})
}

// WithIDer sets a unique ID generator implementation. Default is id.NUID.
func WithIDer(id id.ID) ManagerOption {
	return managerOption(func(o *Manager) error {
		o.id = id
		return nil
	})
}

func WithLogger(logger *slog.Logger) ManagerOption {
	return managerOption(func(o *Manager) error {
		o.logger = logger
		return nil
	})
}

// WithAPIPrefix sets a custom JetStream API prefix on the NATS connection.
func WithAPIPrefix(apiPrefix string) ManagerOption {
	return managerOption(func(o *Manager) error {
		o.apiPrefix = apiPrefix
		return nil
	})
}

// eventSubject builds the fully-qualified subject for an event, scoped to this
// store and (when the handle is tenant-scoped) its tenant. It funnels through
// subjectPrefix so the untenanted form stays byte-identical to
// "$ES.<name>.<entity>.<type>".
func (s *EventStore) eventSubject(event *Event) string {
	return s.subjectPrefix(event.Entity + "." + event.Type)
}

type EventStoreConfig struct {
	Name        string
	Description string
	Metadata    map[string]string
	Replicas    int
	Storage     jetstream.StorageType
	Placement   *jetstream.Placement
	RePublish   *jetstream.RePublish
	MaxMsgs     int64
	MaxAge      time.Duration
	MaxBytes    int64

	// Tenancy creates the store as a tenant store: every store operation must go
	// through a tenant-scoped handle (see (*EventStore).Tenant) and every event
	// subject carries a leading tenant token. The mode is fixed at creation and
	// recorded in stream metadata; it cannot be toggled by a later update.
	Tenancy bool
}

// streamMetadata augments the given metadata with the reserved tenancy marker,
// allocating the map if it is nil. Callers invoke it only for tenant stores, so
// untenanted streams keep their metadata untouched and stay byte-identical.
func streamMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		metadata = make(map[string]string, 1)
	}
	metadata[tenantMetaKey] = tenantMetaVal
	return metadata
}

// mergeStreamMetadata overlays the user-supplied metadata onto the metadata
// already stored on the stream. JetStream replaces a stream's metadata wholesale
// on update, so without this a caller that does not re-supply a custom key set by
// an earlier create or update would silently drop it. User-supplied keys win on
// conflict; server-managed natsMetaPrefix keys are dropped so the server can
// re-derive them. Returns nil when the result is empty so metadata-free streams
// stay byte-identical.
func mergeStreamMetadata(existing, supplied map[string]string) map[string]string {
	merged := make(map[string]string, len(existing)+len(supplied))
	for k, v := range existing {
		if strings.HasPrefix(k, natsMetaPrefix) {
			continue
		}
		merged[k] = v
	}
	maps.Copy(merged, supplied)
	if len(merged) == 0 {
		return nil
	}
	return merged
}

// toStreamConfig projects the store configuration onto the JetStream stream
// config. It is the single owner of this mapping so create and update cannot
// drift apart. Metadata is passed explicitly because the two callers derive it
// differently (tenancy marker vs merge with existing).
func (c EventStoreConfig) toStreamConfig(metadata map[string]string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:               fmt.Sprintf(eventStoreNameTmpl, c.Name),
		Description:        c.Description,
		Metadata:           metadata,
		Subjects:           []string{fmt.Sprintf(eventStoreSubjectTmpl, c.Name) + ">"},
		Replicas:           c.Replicas,
		Storage:            c.Storage,
		Placement:          c.Placement,
		RePublish:          c.RePublish,
		MaxMsgs:            c.MaxMsgs,
		MaxAge:             c.MaxAge,
		MaxBytes:           c.MaxBytes,
		AllowAtomicPublish: true,
		AllowDirect:        true,
	}
}

// Manager creates and manages EventStore instances. It provides shared
// dependencies (type registry, ID generator, clock) to all stores it creates.
type Manager struct {
	logger    *slog.Logger
	nc        *nats.Conn
	js        jetstream.JetStream
	apiPrefix string
	types     *types.Registry
	id        id.ID
	clock     clock.Clock
}

func (m *Manager) GetEventStore(ctx context.Context, name string) (*EventStore, error) {
	if name == "" {
		return nil, ErrEventStoreNameRequired
	}

	sname := fmt.Sprintf(eventStoreNameTmpl, name)

	// Verify the stream exists and discover whether it is a tenant store.
	str, err := m.js.Stream(ctx, sname)
	if err != nil {
		return nil, err
	}
	_, tenantMode := str.CachedInfo().Config.Metadata[tenantMetaKey]

	e := &EventStore{
		name:       name,
		tenantMode: tenantMode,
		nc:         m.nc,
		js:         m.js,
		id:         m.id,
		clock:      m.clock,
		types:      m.types,
		logger:     m.logger,
	}

	return e, nil
}

// Create creates the event store given the configuration. The stream
// name is the name of the store and the subjects default to "{name}.>".
func (m *Manager) CreateEventStore(ctx context.Context, config EventStoreConfig) (*EventStore, error) {
	if config.Name == "" {
		return nil, ErrEventStoreNameRequired
	}

	metadata := config.Metadata
	if config.Tenancy {
		metadata = streamMetadata(metadata)
	}

	_, err := m.js.CreateStream(ctx, config.toStreamConfig(metadata))
	if err != nil {
		return nil, err
	}

	es := EventStore{
		name:       config.Name,
		tenantMode: config.Tenancy,
		nc:         m.nc,
		js:         m.js,
		id:         m.id,
		clock:      m.clock,
		types:      m.types,
		logger:     m.logger,
	}

	return &es, nil
}

// Update updates the event store configuration. Tenancy is immutable: the
// existing stream's mode is preserved regardless of config.Tenancy, so an
// update that forgets to set it cannot silently demote a tenant store.
//
// Custom Metadata is merged with the stream's existing metadata rather than
// replacing it: keys present in config.Metadata are written (overriding any
// prior value) and keys omitted are preserved. Removing a key is therefore not
// expressible through Update today.
func (m *Manager) UpdateEventStore(ctx context.Context, config EventStoreConfig) error {
	if config.Name == "" {
		return ErrEventStoreNameRequired
	}

	sname := fmt.Sprintf(eventStoreNameTmpl, config.Name)
	str, err := m.js.Stream(ctx, sname)
	if err != nil {
		return err
	}
	existing := str.CachedInfo().Config.Metadata
	_, config.Tenancy = existing[tenantMetaKey]

	// Preserve custom metadata set by earlier creates/updates: JetStream replaces
	// metadata wholesale, so an update that omits a key would otherwise drop it.
	metadata := mergeStreamMetadata(existing, config.Metadata)
	if config.Tenancy {
		metadata = streamMetadata(metadata)
	}

	_, err = m.js.UpdateStream(ctx, config.toStreamConfig(metadata))
	return err
}

// Delete deletes the event store.
func (m *Manager) DeleteEventStore(ctx context.Context, name string) error {
	name = fmt.Sprintf(eventStoreNameTmpl, name)
	return m.js.DeleteStream(ctx, name)
}

// New initializes a new Manager instance with a NATS connection.
func New(nc *nats.Conn, opts ...ManagerOption) (*Manager, error) {
	m := &Manager{
		nc:     nc,
		logger: slog.Default(),
		id:     id.NUID,
		clock:  clock.Time,
	}

	for _, o := range opts {
		if err := o.addOption(m); err != nil {
			return nil, err
		}
	}

	var js jetstream.JetStream
	var err error
	if m.apiPrefix != "" {
		js, err = jetstream.NewWithAPIPrefix(nc, m.apiPrefix)
	} else {
		js, err = jetstream.New(nc)
	}
	if err != nil {
		return nil, err
	}
	m.js = js

	return m, nil
}

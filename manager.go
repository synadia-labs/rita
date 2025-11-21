package rita

import (
	"context"
	"fmt"
	"log/slog"
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

func eventSubject(name string, event *Event) string {
	return fmt.Sprintf(eventStoreSubjectTmpl+"%s.%s", name, event.Entity, event.Type)
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
}

// Manager creates and manages EventStore instances. It provides shared
// dependencies (type registry, ID generator, clock) to all stores it creates.
type Manager struct {
	logger *slog.Logger
	nc     *nats.Conn
	js     jetstream.JetStream
	types  *types.Registry
	id     id.ID
	clock  clock.Clock
}

func (m *Manager) GetEventStore(ctx context.Context, name string) (*EventStore, error) {
	if name == "" {
		return nil, ErrEventStoreNameRequired
	}

	e := &EventStore{
		name: name,
	}

	sname := fmt.Sprintf(eventStoreNameTmpl, name)

	// If the stream exists, extract the subject prefix, otherwise this
	// will be done on create.
	_, err := m.js.Stream(ctx, sname)
	if err != nil {
		return nil, err
	}

	return e, nil
}

// Create creates the event store given the configuration. The stream
// name is the name of the store and the subjects default to "{name}.>".
func (m *Manager) CreateEventStore(ctx context.Context, config EventStoreConfig) (*EventStore, error) {
	if config.Name == "" {
		return nil, ErrEventStoreNameRequired
	}

	jsc := &jetstream.StreamConfig{
		Name:               fmt.Sprintf(eventStoreNameTmpl, config.Name),
		Description:        config.Description,
		Metadata:           config.Metadata,
		Subjects:           []string{fmt.Sprintf(eventStoreSubjectTmpl, config.Name) + ">"},
		Replicas:           config.Replicas,
		Storage:            config.Storage,
		Placement:          config.Placement,
		RePublish:          config.RePublish,
		MaxMsgs:            config.MaxMsgs,
		MaxAge:             config.MaxAge,
		MaxBytes:           config.MaxBytes,
		AllowAtomicPublish: true,
		AllowDirect:        true,
	}

	_, err := m.js.CreateStream(ctx, *jsc)
	if err != nil {
		return nil, err
	}

	es := EventStore{
		name:   config.Name,
		nc:     m.nc,
		js:     m.js,
		id:     m.id,
		clock:  m.clock,
		types:  m.types,
		logger: m.logger,
	}

	return &es, nil
}

// Update updates the event store configuration.
func (m *Manager) UpdateEventStore(ctx context.Context, config EventStoreConfig) error {
	if config.Name == "" {
		return ErrEventStoreNameRequired
	}

	jsc := &jetstream.StreamConfig{
		Name:               fmt.Sprintf(eventStoreNameTmpl, config.Name),
		Description:        config.Description,
		Metadata:           config.Metadata,
		Subjects:           []string{fmt.Sprintf(eventStoreSubjectTmpl, config.Name) + ">"},
		Replicas:           config.Replicas,
		Storage:            config.Storage,
		Placement:          config.Placement,
		RePublish:          config.RePublish,
		MaxMsgs:            config.MaxMsgs,
		MaxAge:             config.MaxAge,
		MaxBytes:           config.MaxBytes,
		AllowAtomicPublish: true,
		AllowDirect:        true,
	}
	_, err := m.js.UpdateStream(ctx, *jsc)
	return err
}

// Delete deletes the event store.
func (m *Manager) DeleteEventStore(ctx context.Context, name string) error {
	name = fmt.Sprintf(eventStoreNameTmpl, name)
	return m.js.DeleteStream(ctx, name)
}

// New initializes a new Manager instance with a NATS connection.
func New(nc *nats.Conn, opts ...ManagerOption) (*Manager, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		nc:     nc,
		logger: slog.Default(),
		js:     js,
		id:     id.NUID,
		clock:  clock.Time,
	}

	for _, o := range opts {
		if err := o.addOption(m); err != nil {
			return nil, err
		}
	}

	return m, nil
}

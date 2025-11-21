package rita

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrEvolverNotImplemented = errors.New("evolver not implemented")
	ErrDeciderNotImplemented = errors.New("decider not implemented")
	ErrViewerNotImplemented  = errors.New("viewer not implemented")
)

type Expect struct {
	Sequence uint64
	Pattern  string
}

// ExpectSequence can be set to specify the expected sequence number
// for optimistic concurrency control. If the current last sequence
// number does not match the provided value, the append will fail.
// If nil, no sequence check will be performed. The subject defaults
// to the entity's pattern.
func ExpectSequence(seq uint64) *Expect {
	return &Expect{Sequence: seq}
}

// ExpectSubject can be set to specify an alternative pattern, such
// as the top-level type.
func ExpectSequenceSubject(seq uint64, pattern string) *Expect {
	return &Expect{Sequence: seq, Pattern: pattern}
}

// Event is a wrapper for application-defined events.
type Event struct {
	// ID of the event. This will be used as the NATS msg ID
	// for de-duplication.
	ID string

	// Identifier for specific entities. Can be used to determine if
	// an event is related to a specific entity/node/endpoint/agent/etc.
	// The format must be two tokens, e.g. "node.1234".
	Entity string

	// Time is the time of when the event occurred which may be different
	// from the time the event is appended to the store. If no time is provided,
	// the current local time will be used.
	Time time.Time

	// Type is a unique name for the event itself. This can be omitted
	// if a type registry is being used, otherwise it must be set explicitly
	// to identity the encoded data.
	Type string

	// Data is the event data. This must be a byte slice (pre-encoded) or a value
	// of a type registered in the type registry.
	Data any

	// Metadata is application-defined metadata about the event.
	Meta map[string]string

	// Expect can be set to specify optimistic concurrency control
	// expectations for an append operation.
	Expect *Expect

	// sequence is the sequence number of the event within the stream. Read-only.
	sequence uint64

	// subject is the the subject the event is associated with. Read-only.
	subject string
}

// Evolver is an interface that application-defined models can implement
// to evolve their state based on events.
type Evolver interface {
	Evolve(*Event) error
}

// Command is a wrapper for application-defined commands.
type Command struct {
	// ID is a unique identifier for the command.
	ID string

	// Time is the time of when the command was received.
	Time time.Time

	// Type is a unique name for the command. This can be omitted
	// if a type registry is being used, otherwise it must be set explicitly
	// to identity the encoded data.
	Type string

	// Data is the command data. This must be a byte slice (pre-encoded) or a value
	// of a type registered in the type registry.
	Data any

	// Meta is application-defined metadata about the command.
	Meta map[string]string
}

// Decider is an interface that application-defined models can implement
// to decide on state transitions. Zero or more events can be returned
// that represents the state transitions to be stored.
type Decider interface {
	Decide(*Command) ([]*Event, error)
}

// Viewer represents a read-only view of the state of an entity.
type Viewer[T any] interface {
	View(func(T) error) error
}

type DeciderEvolver interface {
	Decider
	Evolver
}

// Model combines an Evolver, Decider, and Viewer for a specific type T.
// It provides thread-safe access to the underlying interfaces and keeps track
// of the last sequence number of events applied to the model.
type Model[T any] struct {
	t T

	e Evolver
	d Decider

	// sseq is the start sequence of the model
	sseq uint64
	// lseq is the last sequence of the model
	lseq uint64

	mu sync.RWMutex
}

func (m *Model[T]) LastSequence() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lseq
}

func (m *Model[T]) Evolve(event *Event) error {
	if m.e == nil {
		return ErrEvolverNotImplemented
	}

	// Already applied
	if m.lseq >= event.sequence {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sseq == 0 {
		m.sseq = event.sequence
	}
	m.lseq = event.sequence

	return m.e.Evolve(event)
}

func (m *Model[T]) Decide(cmd *Command) ([]*Event, error) {
	if m.d == nil {
		return nil, ErrDeciderNotImplemented
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	events, err := m.d.Decide(cmd)
	if err != nil {
		return nil, err
	}
	if len(events) > 0 {
		events[0].Expect = ExpectSequence(m.lseq)
	}
	return events, nil
}

func (m *Model[T]) View(fn func(T) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return fn(m.t)
}

func NewModel[T any](t T) *Model[T] {
	m := &Model[T]{}
	m.t = t

	// Type may implement neither, one, or both interfaces.
	// Missing implementations will be caught at runtime when methods are called.
	m.e, _ = any(t).(Evolver)
	m.d, _ = any(t).(Decider)

	return m
}

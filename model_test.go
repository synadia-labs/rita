package rita

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/synadia-labs/rita/testutil"
)

type Profile struct{}

func (p *Profile) Evolve(ctx context.Context, event *Event) error             { return nil }
func (p *Profile) Decide(ctx context.Context, cmd *Command) ([]*Event, error) { return nil, nil }

// counter is a simple model for testing.
type counter struct {
	Count int
}

func (c *counter) Evolve(ctx context.Context, event *Event) error {
	c.Count++
	return nil
}

func (c *counter) Decide(ctx context.Context, cmd *Command) ([]*Event, error) {
	return []*Event{{Entity: "counter.1", Data: cmd.Data}}, nil
}

func TestModel_Evolve_DuplicateDetection(t *testing.T) {
	is := testutil.NewIs(t)

	ctx := context.Background()
	state := &counter{}
	m := NewModel(state)

	is.NoErr(m.Evolve(ctx, &Event{Entity: "counter.1", sequence: 1}))
	is.Equal(state.Count, 1)

	// Same sequence should be skipped.
	is.NoErr(m.Evolve(ctx, &Event{Entity: "counter.1", sequence: 1}))
	is.Equal(state.Count, 1)

	// Next sequence should apply.
	is.NoErr(m.Evolve(ctx, &Event{Entity: "counter.1", sequence: 2}))
	is.Equal(state.Count, 2)

	// Different entity with same sequence should apply.
	is.NoErr(m.Evolve(ctx, &Event{Entity: "counter.2", sequence: 2}))
	is.Equal(state.Count, 3)
}

func TestModel_Decide_AutoExpect(t *testing.T) {
	is := testutil.NewIs(t)

	ctx := context.Background()
	state := &counter{}
	m := NewModel(state)

	// Fresh model: Expect should be 0.
	events, err := m.Decide(ctx, &Command{Data: "test"})
	is.NoErr(err)
	is.True(events[0].Expect != nil)
	is.Equal(events[0].Expect.Sequence, uint64(0))

	// After evolving, Expect should reflect last sequence.
	is.NoErr(m.Evolve(ctx, &Event{Entity: "counter.1", sequence: 5}))
	events, err = m.Decide(ctx, &Command{Data: "test"})
	is.NoErr(err)
	is.Equal(events[0].Expect.Sequence, uint64(5))
}

type failingCounter struct {
	Count  int
	failOn uint64
}

func (c *failingCounter) Evolve(ctx context.Context, event *Event) error {
	if event.Sequence() == c.failOn {
		return errEvolverRejected
	}
	c.Count++
	return nil
}

func (c *failingCounter) Decide(ctx context.Context, cmd *Command) ([]*Event, error) {
	return []*Event{{Entity: "counter.1", Data: cmd.Data}}, nil
}

var errEvolverRejected = errors.New("evolve failed")

func TestModel_FailedEvolve_DecideExpectsLastAppliedSequence(t *testing.T) {
	is := testutil.NewIs(t)

	ctx := context.Background()
	m := NewModel(&failingCounter{failOn: 2})

	is.NoErr(m.Evolve(ctx, &Event{Entity: "counter.1", sequence: 1}))
	is.Err(m.Evolve(ctx, &Event{Entity: "counter.1", sequence: 2}), errEvolverRejected)

	events, err := m.Decide(ctx, &Command{Data: "test"})
	is.NoErr(err)
	is.Equal(events[0].Expect.Sequence, uint64(1))
}

func TestModel_FailedEvolve_RecordsFailure(t *testing.T) {
	is := testutil.NewIs(t)

	ctx := context.Background()
	m := NewModel(&failingCounter{failOn: 2})

	is.NoErr(m.Evolve(ctx, &Event{Entity: "counter.1", sequence: 1}))
	is.NoErr(m.evolveFailure())

	is.Err(m.Evolve(ctx, &Event{Entity: "counter.1", sequence: 2}), errEvolverRejected)
	is.Err(m.evolveFailure(), ErrEvolveFailed)
	is.Err(m.evolveFailure(), errEvolverRejected)
}

func TestModel_RepeatedFailedEvolve_KeepsFirstCause(t *testing.T) {
	is := testutil.NewIs(t)

	ctx := context.Background()
	m := NewModel(&failingCounter{failOn: 1})

	is.Err(m.Evolve(ctx, &Event{Entity: "counter.1", sequence: 1}), errEvolverRejected)
	m.e = evolverFunc(func(context.Context, *Event) error { return errLaterFailure })
	is.Err(m.Evolve(ctx, &Event{Entity: "counter.1", sequence: 2}), errLaterFailure)

	err := m.evolveFailure()
	is.Err(err, errEvolverRejected)
	is.True(!errors.Is(err, errLaterFailure))
}

var errLaterFailure = errors.New("later failure")

type evolverFunc func(context.Context, *Event) error

func (f evolverFunc) Evolve(ctx context.Context, e *Event) error { return f(ctx, e) }

type entityCounter struct {
	Counts map[string]int
	failOn uint64
}

func (c *entityCounter) Evolve(ctx context.Context, event *Event) error {
	if event.Sequence() == c.failOn {
		return errEvolverRejected
	}
	c.Counts[event.Entity]++
	return nil
}

func (c *entityCounter) Decide(ctx context.Context, cmd *Command) ([]*Event, error) {
	return []*Event{{Entity: cmd.Data.(string), Data: &OrderPlaced{}}}, nil
}

func TestModel_InterleavedEntities_TrackSequencesIndependently(t *testing.T) {
	is := testutil.NewIs(t)

	ctx := context.Background()
	state := &entityCounter{Counts: map[string]int{}, failOn: 6}
	m := NewModel(state)

	for _, ev := range []*Event{
		{Entity: "order.a", sequence: 1},
		{Entity: "order.b", sequence: 2},
		{Entity: "order.b", sequence: 3},
		{Entity: "order.a", sequence: 5},
		{Entity: "order.b", sequence: 3},
		{Entity: "order.a", sequence: 1},
	} {
		is.NoErr(m.Evolve(ctx, ev))
	}
	is.Err(m.Evolve(ctx, &Event{Entity: "order.b", sequence: 6}), errEvolverRejected)
	is.NoErr(m.Evolve(ctx, &Event{Entity: "order.a", sequence: 7}))

	is.Equal(state.Counts["order.a"], 3)
	is.Equal(state.Counts["order.b"], 2)

	expect := func(entity string) uint64 {
		events, err := m.Decide(ctx, &Command{Data: entity})
		is.NoErr(err)
		return events[0].Expect.Sequence
	}
	is.Equal(expect("order.a"), uint64(7))
	is.Equal(expect("order.b"), uint64(3))
	is.Equal(expect("order.c"), uint64(0))
}

type mutateThenFailCounter struct {
	Count    int
	attempts int
}

func (c *mutateThenFailCounter) Evolve(ctx context.Context, event *Event) error {
	c.Count++
	c.attempts++
	if c.attempts == 1 {
		return errEvolverRejected
	}
	return nil
}

func TestModel_MutateThenFail_RetryKeepsFailureRecorded(t *testing.T) {
	is := testutil.NewIs(t)

	ctx := context.Background()
	state := &mutateThenFailCounter{}
	m := NewModel(state)
	ev := &Event{Entity: "counter.1", sequence: 1}

	is.Err(m.Evolve(ctx, ev), errEvolverRejected)
	is.NoErr(m.Evolve(ctx, ev))

	is.Equal(state.Count, 2)
	is.Err(m.evolveFailure(), errEvolverRejected)
}

func TestModel_NotImplemented(t *testing.T) {
	is := testutil.NewIs(t)

	ctx := context.Background()

	// Type that only implements Evolver.
	type evolveOnly struct{}
	m := NewModel(&evolveOnly{})
	_, err := m.Decide(ctx, &Command{})
	is.Err(err, ErrDeciderNotImplemented)

	// Type that only implements Decider.
	type decideOnly struct{}
	m2 := NewModel(&decideOnly{})
	err = m2.Evolve(ctx, &Event{Entity: "x.1", sequence: 1})
	is.Err(err, ErrEvolverNotImplemented)
}

func TestModel_ConcurrentEvolveAndView(t *testing.T) {
	is := testutil.NewIs(t)

	ctx := context.Background()
	state := &counter{}
	m := NewModel(state)

	// Simulate a watcher evolving state while concurrent Views read it.
	var wg sync.WaitGroup

	// Writer goroutine: evolve events sequentially (like a Watch callback).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= 100; i++ {
			_ = m.Evolve(ctx, &Event{Entity: "counter.1", sequence: uint64(i)})
		}
	}()

	// Reader goroutines: view state concurrently.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = m.View(ctx, func(c *counter) error {
					_ = c.Count
					return nil
				})
			}
		}()
	}

	wg.Wait()
	is.Equal(state.Count, 100)
}

func BenchmarkModel_Decide(b *testing.B) {
	ctx := context.Background()
	p := &Profile{}
	m := NewModel(p)
	c := &Command{}

	b.ResetTimer()

	for b.Loop() {
		_, _ = m.Decide(ctx, c)
	}
}

func BenchmarkModel_Evolve(b *testing.B) {
	ctx := context.Background()
	p := &Profile{}
	m := NewModel(p)
	e := &Event{}

	b.ResetTimer()

	for b.Loop() {
		_ = m.Evolve(ctx, e)
	}
}

func BenchmarkModel_View(b *testing.B) {
	ctx := context.Background()
	p := &Profile{}
	m := NewModel(p)
	fn := func(p *Profile) error { return nil }

	b.ResetTimer()

	for b.Loop() {
		_ = m.View(ctx, fn)
	}
}

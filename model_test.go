package rita

import (
	"sync"
	"testing"

	"github.com/synadia-labs/rita/testutil"
)

type Profile struct{}

func (p *Profile) Evolve(event *Event) error             { return nil }
func (p *Profile) Decide(cmd *Command) ([]*Event, error) { return nil, nil }

// counter is a simple model for testing.
type counter struct {
	Count int
}

func (c *counter) Evolve(event *Event) error {
	c.Count++
	return nil
}

func (c *counter) Decide(cmd *Command) ([]*Event, error) {
	return []*Event{{Entity: "counter.1", Data: cmd.Data}}, nil
}

func TestModel_Evolve_DuplicateDetection(t *testing.T) {
	is := testutil.NewIs(t)

	state := &counter{}
	m := NewModel(state)

	is.NoErr(m.Evolve(&Event{Entity: "counter.1", sequence: 1}))
	is.Equal(state.Count, 1)

	// Same sequence should be skipped.
	is.NoErr(m.Evolve(&Event{Entity: "counter.1", sequence: 1}))
	is.Equal(state.Count, 1)

	// Next sequence should apply.
	is.NoErr(m.Evolve(&Event{Entity: "counter.1", sequence: 2}))
	is.Equal(state.Count, 2)

	// Different entity with same sequence should apply.
	is.NoErr(m.Evolve(&Event{Entity: "counter.2", sequence: 2}))
	is.Equal(state.Count, 3)
}

func TestModel_Decide_AutoExpect(t *testing.T) {
	is := testutil.NewIs(t)

	state := &counter{}
	m := NewModel(state)

	// Fresh model: Expect should be 0.
	events, err := m.Decide(&Command{Data: "test"})
	is.NoErr(err)
	is.True(events[0].Expect != nil)
	is.Equal(events[0].Expect.Sequence, uint64(0))

	// After evolving, Expect should reflect last sequence.
	is.NoErr(m.Evolve(&Event{Entity: "counter.1", sequence: 5}))
	events, err = m.Decide(&Command{Data: "test"})
	is.NoErr(err)
	is.Equal(events[0].Expect.Sequence, uint64(5))
}

func TestModel_NotImplemented(t *testing.T) {
	is := testutil.NewIs(t)

	// Type that only implements Evolver.
	type evolveOnly struct{}
	m := NewModel(&evolveOnly{})
	_, err := m.Decide(&Command{})
	is.Err(err, ErrDeciderNotImplemented)

	// Type that only implements Decider.
	type decideOnly struct{}
	m2 := NewModel(&decideOnly{})
	err = m2.Evolve(&Event{Entity: "x.1", sequence: 1})
	is.Err(err, ErrEvolverNotImplemented)
}

func TestModel_ConcurrentEvolveAndView(t *testing.T) {
	is := testutil.NewIs(t)

	state := &counter{}
	m := NewModel(state)

	// Simulate a watcher evolving state while concurrent Views read it.
	var wg sync.WaitGroup

	// Writer goroutine: evolve events sequentially (like a Watch callback).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= 100; i++ {
			_ = m.Evolve(&Event{Entity: "counter.1", sequence: uint64(i)})
		}
	}()

	// Reader goroutines: view state concurrently.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = m.View(func(c *counter) error {
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
	p := &Profile{}
	m := NewModel(p)
	c := &Command{}

	b.ResetTimer()

	for b.Loop() {
		_, _ = m.Decide(c)
	}
}

func BenchmarkModel_Evolve(b *testing.B) {
	p := &Profile{}
	m := NewModel(p)
	e := &Event{}

	b.ResetTimer()

	for b.Loop() {
		_ = m.Evolve(e)
	}
}

func BenchmarkModel_View(b *testing.B) {
	p := &Profile{}
	m := NewModel(p)
	fn := func(p *Profile) error { return nil }

	b.ResetTimer()

	for b.Loop() {
		_ = m.View(fn)
	}
}

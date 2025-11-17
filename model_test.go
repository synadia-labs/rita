package rita

import "testing"

type Profile struct{}

func (p *Profile) View(fn func(*Profile) error) error {
	return fn(p)
}

func (p *Profile) Evolve(event *Event) error {
	// Implement evolution logic here
	return nil
}

func (p *Profile) Decide(cmd *Command) ([]*Event, error) {
	// Implement decision logic here
	return nil, nil
}

func BenchmarkModel__Decide(b *testing.B) {
	p := &Profile{}
	m := NewModel(p)
	c := &Command{}

	b.ResetTimer()

	for b.Loop() {
		_, _ = m.Decide(c)
	}
}

func BenchmarkModel__Evolve(b *testing.B) {
	p := &Profile{}
	m := NewModel(p)
	e := &Event{}

	b.ResetTimer()

	for b.Loop() {
		_ = m.Evolve(e)
	}
}

func BenchmarkModel__View(b *testing.B) {
	p := &Profile{}
	m := NewModel(p)
	fn := func(p *Profile) error { return nil }

	b.ResetTimer()

	for b.Loop() {
		_ = m.View(fn)
	}
}

package rita

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/synadia-labs/rita/testutil"
	"github.com/synadia-labs/rita/types"
)

var (
	_ Decider = (*OrderStats)(nil)
	_ Evolver = (*OrderStats)(nil)
)

func TestModelDecideEvolve(t *testing.T) {
	is := testutil.NewIs(t)

	srv := testutil.NewNatsServer()
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	tr, err := types.NewRegistry(registry)
	is.NoErr(err)

	m, err := New(nc, WithRegistry(tr))
	is.NoErr(err)

	ctx := context.Background()
	es, err := m.CreateEventStore(ctx, EventStoreConfig{
		Name: "store",
	})
	is.NoErr(err)

	cmd := &Command{
		Data: &PlaceOrder{},
	}

	state := &OrderStats{}
	stateModel := NewModel(state)

	evt, err := stateModel.Decide(cmd)
	is.NoErr(err)
	is.Equal(len(evt), 1)

	seq, err := es.Append(ctx, evt)
	is.NoErr(err)
	is.Equal(seq, uint64(1))

	var events eventSlice
	_, err = es.Evolve(ctx, &events)
	is.NoErr(err)
	is.Equal(len(events), 1)
	is.Equal(events[0].Type, "order-placed")
}

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

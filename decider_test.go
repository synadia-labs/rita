package rita

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/synadia-labs/rita/testutil"
)

type DecideWithEvent struct{}

func (d *DecideWithEvent) Decide(command *Command) ([]*Event, error) {
	return []*Event{{
		Type: "TestEvent",
		Data: "TestData",
	}}, nil
}

type DecideWithoutEvent struct{}

func (d *DecideWithoutEvent) Decide(command *Command) ([]*Event, error) {
	return []*Event{}, nil
}

func TestDeciderWithoutEvents(t *testing.T) {
	is := testutil.NewIs(t)

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	_, err = New(t.Context(), nc)
	is.NoErr(err)

	d := &DecideWithoutEvent{}

	events, err := d.Decide(&Command{})
	is.NoErr(err)
	is.Equal(len(events), 0)
}

func TestDeciderWithEvents(t *testing.T) {
	is := testutil.NewIs(t)

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	_, err = New(t.Context(), nc)
	is.NoErr(err)

	d := &DecideWithEvent{}

	events, err := d.Decide(&Command{})
	is.NoErr(err)
	is.Equal(len(events), 1)
}

package rita

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/synadia-labs/rita/testutil"
	"github.com/synadia-labs/rita/types"
)

type ParcelOrdered struct{}

type ParcelRefunded struct{}

type ParcelAddressChanged struct {
	Address string
}

type ParcelShipped struct{}

type ShipParcel struct{}

var (
	errUnknownParcelEvent = errors.New("unknown parcel event")
	errParcelRefunded     = errors.New("parcel was refunded")
)

func releaseOneParcelTypes() map[string]*types.Type {
	return map[string]*types.Type{
		"parcel-ordered":         {Init: func() any { return &ParcelOrdered{} }},
		"parcel-address-changed": {Init: func() any { return &ParcelAddressChanged{} }},
		"parcel-shipped":         {Init: func() any { return &ParcelShipped{} }},
	}
}

func releaseTwoParcelTypes() map[string]*types.Type {
	reg := releaseOneParcelTypes()
	reg["parcel-refunded"] = &types.Type{Init: func() any { return &ParcelRefunded{} }}
	return reg
}

type ParcelReleaseOne struct {
	Address string
	Shipped bool
}

func (p *ParcelReleaseOne) Evolve(ctx context.Context, event *Event) error {
	switch e := event.Data.(type) {
	case *ParcelOrdered:
	case *ParcelAddressChanged:
		p.Address = e.Address
	case *ParcelShipped:
		p.Shipped = true
	default:
		return fmt.Errorf("%w: %T", errUnknownParcelEvent, event.Data)
	}
	return nil
}

func (p *ParcelReleaseOne) Decide(ctx context.Context, cmd *Command) ([]*Event, error) {
	return []*Event{{Entity: "parcel.1", Data: &ParcelShipped{}}}, nil
}

type ParcelReleaseTwo struct {
	Address  string
	Refunded bool
	Shipped  bool
}

func (p *ParcelReleaseTwo) Evolve(ctx context.Context, event *Event) error {
	switch e := event.Data.(type) {
	case *ParcelOrdered:
	case *ParcelRefunded:
		p.Refunded = true
	case *ParcelAddressChanged:
		p.Address = e.Address
	case *ParcelShipped:
		p.Shipped = true
	default:
		return fmt.Errorf("%w: %T", errUnknownParcelEvent, event.Data)
	}
	return nil
}

func (p *ParcelReleaseTwo) Decide(ctx context.Context, cmd *Command) ([]*Event, error) {
	if p.Refunded {
		return nil, errParcelRefunded
	}
	return []*Event{{Entity: "parcel.1", Data: &ParcelShipped{}}}, nil
}

type parcelDeploy struct {
	t  *testing.T
	nc *nats.Conn
}

func newParcelDeploy(t *testing.T) *parcelDeploy {
	t.Helper()
	srv := testutil.NewNatsServer(t)
	t.Cleanup(func() { testutil.ShutdownNatsServer(srv) })

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	d := &parcelDeploy{t: t, nc: nc}
	m := d.manager(releaseTwoParcelTypes())
	if _, err := m.CreateEventStore(context.Background(), EventStoreConfig{Name: "parcels"}); err != nil {
		t.Fatalf("create store: %v", err)
	}
	return d
}

func (d *parcelDeploy) manager(reg map[string]*types.Type) *Manager {
	d.t.Helper()
	tr, err := types.NewRegistry(reg)
	if err != nil {
		d.t.Fatalf("registry: %v", err)
	}
	m, err := New(d.nc, WithRegistry(tr))
	if err != nil {
		d.t.Fatalf("manager: %v", err)
	}
	return m
}

func (d *parcelDeploy) store(reg map[string]*types.Type) *EventStore {
	d.t.Helper()
	es, err := d.manager(reg).GetEventStore(context.Background(), "parcels")
	if err != nil {
		d.t.Fatalf("get store: %v", err)
	}
	return es
}

func (d *parcelDeploy) releaseTwoRefundsThenChangesAddress() {
	d.t.Helper()
	_, err := d.store(releaseTwoParcelTypes()).Append(context.Background(), []*Event{
		{Entity: "parcel.1", Data: &ParcelOrdered{}},
		{Entity: "parcel.1", Data: &ParcelRefunded{}},
		{Entity: "parcel.1", Data: &ParcelAddressChanged{Address: "2 New St"}},
	})
	if err != nil {
		d.t.Fatalf("append: %v", err)
	}
}

func (d *parcelDeploy) rebuiltOnReleaseTwo() (*EventStore, *Model[*ParcelReleaseTwo], *ParcelReleaseTwo) {
	d.t.Helper()
	es := d.store(releaseTwoParcelTypes())
	state := &ParcelReleaseTwo{}
	model := NewModel(state)
	if _, err := es.Evolve(context.Background(), model); err != nil {
		d.t.Fatalf("rebuild: %v", err)
	}
	return es, model, state
}

func watchUntilAddress[T any](t *testing.T, es *EventStore, model *Model[T], address func(T) string, want string) {
	t.Helper()
	w, err := es.Watch(context.Background(), model)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	t.Cleanup(w.Stop)

	waitFor(t, 2*time.Second, func() bool {
		var got string
		_ = model.View(context.Background(), func(s T) error {
			got = address(s)
			return nil
		})
		return got == want
	})
}

func releaseOneAddress(p *ParcelReleaseOne) string { return p.Address }

func TestFailedEvolve_WatcherCannotDecodeNewEventType_RefusesDecide(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	deploy := newParcelDeploy(t)
	deploy.releaseTwoRefundsThenChangesAddress()

	releaseOne := deploy.store(releaseOneParcelTypes())
	model := NewModel(&ParcelReleaseOne{})
	watchUntilAddress(t, releaseOne, model, releaseOneAddress, "2 New St")
	is.Err(model.EvolveErr(), types.ErrTypeNotRegistered)

	_, _, err := releaseOne.Decide(ctx, model, &Command{Data: &ShipParcel{}})
	is.Err(err, ErrEvolveFailed)
	is.Err(err, types.ErrTypeNotRegistered)

	_, _, parcel := deploy.rebuiltOnReleaseTwo()
	is.True(parcel.Refunded)
	is.True(!parcel.Shipped)
}

func TestFailedEvolve_WatcherEvolverRejectsNewEventType_RefusesDecide(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	deploy := newParcelDeploy(t)
	deploy.releaseTwoRefundsThenChangesAddress()

	typesUpgradedFirst := deploy.store(releaseTwoParcelTypes())
	model := NewModel(&ParcelReleaseOne{})
	watchUntilAddress(t, typesUpgradedFirst, model, releaseOneAddress, "2 New St")

	_, _, err := typesUpgradedFirst.Decide(ctx, model, &Command{Data: &ShipParcel{}})
	is.Err(err, ErrEvolveFailed)
	is.Err(err, errUnknownParcelEvent)

	_, _, parcel := deploy.rebuiltOnReleaseTwo()
	is.True(!parcel.Shipped)
}

func TestFailedEvolve_ReplayCannotDecodeNewEventType_RefusesDecide(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	deploy := newParcelDeploy(t)
	deploy.releaseTwoRefundsThenChangesAddress()

	releaseOne := deploy.store(releaseOneParcelTypes())
	model := NewModel(&ParcelReleaseOne{})
	_, err := releaseOne.Evolve(ctx, model)
	is.Err(err, types.ErrTypeNotRegistered)

	_, _, err = releaseOne.Decide(ctx, model, &Command{Data: &ShipParcel{}})
	is.Err(err, ErrEvolveFailed)
	is.Err(err, types.ErrTypeNotRegistered)
}

func TestFailedEvolve_ModelRebuiltOnNewRelease_DecidesOnFullHistory(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	deploy := newParcelDeploy(t)
	deploy.releaseTwoRefundsThenChangesAddress()

	releaseOne := deploy.store(releaseOneParcelTypes())
	stale := NewModel(&ParcelReleaseOne{})
	watchUntilAddress(t, releaseOne, stale, releaseOneAddress, "2 New St")
	_, _, err := releaseOne.Decide(ctx, stale, &Command{Data: &ShipParcel{}})
	is.Err(err, ErrEvolveFailed)

	releaseTwo, rebuilt, _ := deploy.rebuiltOnReleaseTwo()
	is.NoErr(rebuilt.EvolveErr())
	_, _, err = releaseTwo.Decide(ctx, rebuilt, &Command{Data: &ShipParcel{}})
	is.Err(err, errParcelRefunded)
}

func TestFailedEvolve_RebuildOnSameRelease_FailsAgain(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	deploy := newParcelDeploy(t)
	deploy.releaseTwoRefundsThenChangesAddress()

	releaseOne := deploy.store(releaseOneParcelTypes())
	stale := NewModel(&ParcelReleaseOne{})
	watchUntilAddress(t, releaseOne, stale, releaseOneAddress, "2 New St")
	_, _, err := releaseOne.Decide(ctx, stale, &Command{Data: &ShipParcel{}})
	is.Err(err, ErrEvolveFailed)

	rebuilt := NewModel(&ParcelReleaseOne{})
	watchUntilAddress(t, releaseOne, rebuilt, releaseOneAddress, "2 New St")
	is.Err(rebuilt.EvolveErr(), ErrEvolveFailed)
	is.Err(rebuilt.EvolveErr(), types.ErrTypeNotRegistered)
}

var errAddressServiceDown = errors.New("address service unavailable")

type ParcelWithFlakyEvolve struct {
	ParcelReleaseTwo
	failNext bool
}

func (p *ParcelWithFlakyEvolve) Evolve(ctx context.Context, event *Event) error {
	if p.failNext {
		p.failNext = false
		return errAddressServiceDown
	}
	return p.ParcelReleaseTwo.Evolve(ctx, event)
}

func TestFailedEvolve_TemporaryEvolverError_RebuildRecovers(t *testing.T) {
	is := testutil.NewIs(t)
	ctx := context.Background()
	deploy := newParcelDeploy(t)
	deploy.releaseTwoRefundsThenChangesAddress()
	releaseTwo := deploy.store(releaseTwoParcelTypes())

	flaky := NewModel(&ParcelWithFlakyEvolve{failNext: true})
	_, err := releaseTwo.Evolve(ctx, flaky)
	is.Err(err, errAddressServiceDown)
	is.Err(flaky.EvolveErr(), errAddressServiceDown)

	rebuilt := NewModel(&ParcelWithFlakyEvolve{})
	_, err = releaseTwo.Evolve(ctx, rebuilt)
	is.NoErr(err)
	is.NoErr(rebuilt.EvolveErr())

	_, _, err = releaseTwo.Decide(ctx, rebuilt, &Command{Data: &ShipParcel{}})
	is.Err(err, errParcelRefunded)
}

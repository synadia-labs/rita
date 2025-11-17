package rita

import (
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

type WatchOption func(*watcher)

type ErrHandlerFunc func(error, *Event, jetstream.Msg)

func WithWatchErrHandler(fn ErrHandlerFunc) WatchOption {
	return func(w *watcher) {
		w.eh = fn
	}
}

func WithWatchFilters(filters ...string) WatchOption {
	return func(w *watcher) {
		w.filters = filters
	}
}

func WithWatchNoWait() WatchOption {
	return func(w *watcher) {
		w.nowait = true
	}
}

type Watcher interface {
	Stop()
}

type watcher struct {
	e       Evolver
	m       jetstream.ConsumeContext
	c       jetstream.Consumer
	eh      ErrHandlerFunc
	filters []string

	nowait bool

	mux *sync.RWMutex
}

func (w *watcher) Stop() {
	w.m.Drain()
}

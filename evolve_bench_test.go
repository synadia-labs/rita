package rita

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/synadia-io/orbit.go/jetstreamext"
)

// countingEvolver is a minimal Evolver so the benchmarks measure replay
// transport and unpacking rather than model work.
type countingEvolver struct {
	n int
}

func (c *countingEvolver) Evolve(_ context.Context, _ *Event) error {
	c.n++
	return nil
}

// directGetReplay replays events with batched direct gets instead of
// Evolve's ephemeral ordered consumer. It was evaluated as a replacement —
// no server-side consumer create/delete — and rejected on the numbers: see
// BenchmarkEvolveReplay. It is kept as a test-only prototype so the
// comparison stays reproducible. It shares unpackEventFrom with the
// production path so the two sides differ only in transport.
func directGetReplay(ctx context.Context, es *EventStore, model Evolver, filters ...string) (uint64, error) {
	const batchSize = 500

	subjects, err := es.filtersToSubjects(filters)
	if err != nil {
		return 0, err
	}
	var subject string
	if len(subjects) == 1 {
		subject = subjects[0]
	}

	next := uint64(1)
	var lastSeq uint64
	for {
		gopts := []jetstreamext.GetBatchOpt{jetstreamext.GetBatchSeq(next)}
		if subject != "" {
			gopts = append(gopts, jetstreamext.GetBatchSubject(subject))
		}

		msgs, err := jetstreamext.GetBatch(ctx, es.js, es.stream, batchSize, gopts...)
		if err != nil {
			return lastSeq, err
		}

		var count int
		for raw, err := range msgs {
			if err != nil {
				if errors.Is(err, jetstreamext.ErrNoMessages) {
					return lastSeq, nil
				}
				return lastSeq, err
			}
			count++

			event, err := es.unpackEventFrom(raw.Subject, raw.Sequence, raw.Header, raw.Data)
			if err != nil {
				return lastSeq, err
			}
			if err := model.Evolve(ctx, event); err != nil {
				return lastSeq, err
			}
			lastSeq = event.sequence
		}

		// A short page means the matching messages are exhausted.
		if count < batchSize {
			return lastSeq, nil
		}
		next = lastSeq + 1
	}
}

// seedBenchStore returns a store seeded with n small registered events spread
// across entities order.0..order.99.
func seedBenchStore(tb testing.TB, n int) *EventStore {
	tb.Helper()
	es := newTestStore(tb)
	ctx := context.Background()

	const chunk = 500
	for off := 0; off < n; off += chunk {
		m := min(chunk, n-off)
		events := make([]*Event, m)
		for i := range events {
			events[i] = &Event{
				Entity: fmt.Sprintf("order.%d", (off+i)%100),
				Data:   &OrderPlaced{},
			}
		}
		if _, err := es.Append(ctx, events); err != nil {
			tb.Fatal(err)
		}
	}
	return es
}

// replayBench runs full replays through the given path and verifies the
// expected number of events arrived, so a silently broken path cannot post a
// fast time.
func replayBench(b *testing.B, want int, replay func(context.Context, Evolver) (uint64, error)) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		var c countingEvolver
		if _, err := replay(ctx, &c); err != nil {
			b.Fatal(err)
		}
		if c.n != want {
			b.Fatalf("replayed %d events, want %d", c.n, want)
		}
	}
}

// BenchmarkEvolveReplay compares Evolve's ordered-consumer replay against
// the direct-get prototype above. "full" replays the whole store; "entity"
// replays a single entity's events out of a 5000-event store — the
// Decide-reload shape where per-call consumer setup weighs heaviest.
//
// Measured on Apple M2 (2026-07): direct get wins ~70µs on tiny replays
// (n=10), is a wash on entity reloads, and consumes 2.5-3x slower at 1k-5k
// events with 3-5x the allocations — hence Evolve keeps the ordered
// consumer.
func BenchmarkEvolveReplay(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 5000} {
		es := seedBenchStore(b, n)
		b.Run(fmt.Sprintf("full/n=%d/ordered-consumer", n), func(b *testing.B) {
			replayBench(b, n, func(ctx context.Context, m Evolver) (uint64, error) {
				return es.Evolve(ctx, m)
			})
		})
		b.Run(fmt.Sprintf("full/n=%d/direct-get", n), func(b *testing.B) {
			replayBench(b, n, func(ctx context.Context, m Evolver) (uint64, error) {
				return directGetReplay(ctx, es, m)
			})
		})
	}

	es := seedBenchStore(b, 5000)
	b.Run("entity/50-of-5000/ordered-consumer", func(b *testing.B) {
		replayBench(b, 50, func(ctx context.Context, m Evolver) (uint64, error) {
			return es.Evolve(ctx, m, WithFilters("order.7"))
		})
	})
	b.Run("entity/50-of-5000/direct-get", func(b *testing.B) {
		replayBench(b, 50, func(ctx context.Context, m Evolver) (uint64, error) {
			return directGetReplay(ctx, es, m, "order.7")
		})
	})
}

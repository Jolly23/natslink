package natslink

// Mirrored-publish cost benchmark: since v1.8.0 this measures the caller's
// side only (one payload copy plus one non-blocking enqueue per endpoint); the
// sends happen on the endpoint goroutines. Needs two local NATS servers
// (4222/4223) and skips when either is unreachable.

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// BenchmarkHotMirrorPublish measures the caller's side of a mirrored publish
// only: one reservation per endpoint, one payload copy, one non-blocking
// enqueue per endpoint. The producer runs faster than the send goroutines, so
// queues fill and some calls return ErrQueueFull; that is the expected
// overload behaviour, reported as the "rejected/op" metric rather than failing
// the benchmark. Setup, Stop and draining are outside the timer.
func BenchmarkHotMirrorPublish(b *testing.B) {
	for _, u := range []string{benchURL(), dualURL2()} {
		if err := Probe(u); err != nil {
			b.Skipf("NATS not reachable: %v", err)
		}
	}
	mp, err := NewMirroredPublisherPool([]string{benchURL(), dualURL2()}, 4, PublisherOptions{
		Options: Options{Topic: "natslink.bench.mirror", ReportInterval: -1, Logger: benchLogger{}},
	})
	if err != nil {
		b.Fatal(err)
	}
	mp.MustStart()
	if err := mp.WaitConnected(5 * time.Second); err != nil {
		b.Fatal(err)
	}

	var rejected int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch err := mp.Publish(benchPayload); {
		case err == nil:
		case errors.Is(err, ErrQueueFull):
			rejected++
		default:
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(rejected)/float64(b.N), "rejected/op")
	mp.Stop()
}

// BenchmarkMirrorEndToEnd measures sustainable delivery through a mirrored
// publisher to a mirrored subscriber on two healthy servers: bounded batches
// of accepted messages with a completion barrier (every accepted message
// received on every endpoint) inside the timed interval, so the number is
// completed work, not enqueue attempts. Offered load is throttled by the
// barrier: a batch is published, then the benchmark waits for all of its
// copies before the next batch. Loss within a batch fails the benchmark, so
// the figure is labelled lossless.
func BenchmarkMirrorEndToEnd(b *testing.B) {
	urls := []string{benchURL(), dualURL2()}
	for _, u := range urls {
		if err := Probe(u); err != nil {
			b.Skipf("NATS not reachable: %v", err)
		}
	}
	for _, members := range []int{1, 4} {
		b.Run(fmt.Sprintf("members=%d", members), func(b *testing.B) {
			topic := fmt.Sprintf("natslink.bench.e2e.%d", time.Now().UnixNano())
			var received atomic.Int64
			sub, err := NewMirroredSubscriber(urls, SubscriberOptions{
				Options: Options{Topic: topic, ReportInterval: -1, Logger: benchLogger{}},
				Handler: func(*nats.Msg) { received.Add(1) },
				Workers: SyncMode,
			})
			if err != nil {
				b.Fatal(err)
			}
			sub.MustStart()
			defer sub.Stop()
			if err := sub.WaitConnected(5 * time.Second); err != nil {
				b.Fatal(err)
			}
			// Readiness barrier on every endpoint: the SUB must be processed by
			// each server before the first measured publish, otherwise a slow
			// endpoint's setup shows up as delivery loss.
			for i, l := range sub.links {
				c := l.Conn()
				if c == nil {
					b.Fatalf("endpoint %d: no connection after WaitConnected", i)
				}
				if err := c.FlushTimeout(5 * time.Second); err != nil {
					b.Fatalf("endpoint %d: flush subscription: %v", i, err)
				}
			}
			mp, err := NewMirroredPublisherPool(urls, members, PublisherOptions{
				Options: Options{Topic: topic, ReportInterval: -1, Logger: benchLogger{}},
			})
			if err != nil {
				b.Fatal(err)
			}
			mp.MustStart()
			defer mp.Stop()
			if err := mp.WaitConnected(5 * time.Second); err != nil {
				b.Fatal(err)
			}

			const batch = 1024
			var accepted, rejected int64
			wait := func(want int64) {
				deadline := time.Now().Add(10 * time.Second)
				for received.Load() < want {
					if time.Now().After(deadline) {
						b.Fatalf("lossless barrier: received %d copies, want %d", received.Load(), want)
					}
					time.Sleep(50 * time.Microsecond)
				}
			}
			b.SetBytes(int64(len(benchPayload)))
			b.ResetTimer()
			for sent := 0; sent < b.N; {
				n := batch
				if rest := b.N - sent; rest < n {
					n = rest
				}
				for i := 0; i < n; i++ {
					switch err := mp.Publish(benchPayload); {
					case err == nil:
						accepted++
					case errors.Is(err, ErrQueueFull):
						rejected++
					default:
						b.Fatal(err)
					}
				}
				wait(accepted * int64(len(urls)))
				sent += n
			}
			b.StopTimer()
			b.ReportMetric(float64(accepted)/float64(b.N), "accepted/op")
			b.ReportMetric(float64(rejected)/float64(b.N), "rejected/op")
			for i, st := range mp.Stats() {
				if st.QueueDropped != 0 {
					b.Fatalf("member %d queue-dropped %d copies in a lossless measurement", i, st.QueueDropped)
				}
			}
		})
	}
}

// BenchmarkMirrorPublishRejected measures a publish that every endpoint queue
// rejects, with the queues full either by bytes or by entries: no server, no
// consumer. The point is the allocation column: shedding load must not copy
// the payload in either case.
func BenchmarkMirrorPublishRejected(b *testing.B) {
	for _, full := range []string{"bytes", "entries"} {
		for _, size := range []int{64, 4 << 10, 2 << 20} {
			b.Run(fmt.Sprintf("%s/%dB", full, size), func(b *testing.B) {
				p, err := NewMirroredPublisher([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, PublisherOptions{
					Options: Options{Topic: "t", ReportInterval: -1, Logger: benchLogger{}},
				})
				if err != nil {
					b.Fatal(err)
				}
				p.running.Store(true) // never started: no connections, no send goroutines
				switch full {
				case "bytes":
					for _, g := range p.groups {
						g.queuedBytes.Store(g.maxBytes)
					}
				case "entries":
					for i := 0; i < mirrorQueueEntries; i++ {
						if err := p.Publish([]byte("small")); err != nil {
							b.Fatal(err)
						}
					}
				}
				payload := make([]byte, size)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := p.Publish(payload); !errors.Is(err, ErrQueueFull) {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

package natslink

// Hot-path benchmarks, written to answer "did the mirroring refactor cost anything
// on a single connection?". The file is self-contained so the same copy can be run
// against the pre-refactor package (the v1.2.x HEAD) and the post-refactor one and
// the numbers compared item by item. The publish benchmarks need a local NATS
// server (port 4222 by default, override with NATS_TEST_URL) and skip when it is
// unreachable; the dispatch benchmarks are pure in-memory and run anywhere.
//
//	go test -bench 'BenchmarkHot' -benchtime 1s -count 6 -run xxx .

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func benchURL() string {
	if u := os.Getenv("NATS_TEST_URL"); u != "" {
		return u
	}
	return "nats://natslink-itest-token@127.0.0.1:4222"
}

func benchSkipIfNoServer(b *testing.B) {
	b.Helper()
	if err := Probe(benchURL()); err != nil {
		b.Skipf("NATS not reachable: %v", err)
	}
}

var benchPayload = make([]byte, 128) // the size of a typical small event payload

// benchLogger silences logging: the connection-start INFO lines would pollute the
// benchmark output.
type benchLogger struct{}

func (benchLogger) Info(string, ...any)  {}
func (benchLogger) Warn(string, ...any)  {}
func (benchLogger) Error(string, ...any) {}

// Single publishing connection: the Publish hot path (identical code before and
// after the refactor, so it serves as the anchor).
func BenchmarkHotPublisherPublish(b *testing.B) {
	benchSkipIfNoServer(b)
	pub, err := NewPublisher(PublisherOptions{
		Options: Options{URL: benchURL(), Topic: "natslink.bench.pub", ReportInterval: -1, Logger: benchLogger{}},
	})
	if err != nil {
		b.Fatal(err)
	}
	pub.MustStart()
	defer pub.Stop()
	if err := pub.WaitConnected(nats.DefaultTimeout); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pub.Publish(benchPayload); err != nil {
			b.Fatal(err)
		}
	}
}

// Single-endpoint publisher pool (the common deployment shape): the refactor moved
// the two-pass member selection into pubGroup, adding one method call and a loop
// over a one-element groups slice. This measures the real difference.
func BenchmarkHotPoolPublish(b *testing.B) {
	benchSkipIfNoServer(b)
	pool, err := NewPublisherPool(4, PublisherOptions{
		Options: Options{URL: benchURL(), Topic: "natslink.bench.pool", ReportInterval: -1, Logger: benchLogger{}},
	})
	if err != nil {
		b.Fatal(err)
	}
	pool.MustStart()
	defer pool.Stop()
	if err := pool.WaitConnected(nats.DefaultTimeout); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pool.Publish(benchPayload); err != nil {
			b.Fatal(err)
		}
	}
}

// Subscriber dispatch hot path (worker-pool mode): dispatch enqueue + worker
// consumption. Pure in-memory, no server needed. ns/op is the admission cost
// per dispatch attempt (drops included); completed handler throughput is the
// separate "consumed/s" metric, and dropped/op says how much of the offered
// load the unthrottled producer pushed past the queue.
func BenchmarkHotSubscriberDispatch(b *testing.B) {
	var consumed atomic.Uint64
	s, err := NewSubscriber(SubscriberOptions{
		Options:   Options{URL: "nats://127.0.0.1:1", Topic: "t", ReportInterval: -1, Logger: benchLogger{}},
		Handler:   func(*nats.Msg) { consumed.Add(1) },
		Workers:   8,
		QueueSize: 4096,
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := s.Start(); err != nil { // starts even with a bogus URL (RetryOnFailedConnect); workers run as usual
		b.Fatal(err)
	}
	defer s.Stop()

	msg := &nats.Msg{Data: benchPayload}
	b.ReportAllocs()
	start := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.dispatch(msg)
	}
	// The drain is inside the timed interval so consumed/s is completed
	// handler work; drops (queue full under this unthrottled producer) are
	// reported separately rather than counted as consumption.
	for int(consumed.Load())+int(s.Stats().Dropped) < b.N {
		time.Sleep(10 * time.Microsecond)
	}
	b.StopTimer()
	elapsed := time.Since(start)
	b.ReportMetric(float64(s.Stats().Dropped)/float64(b.N), "dropped/op")
	b.ReportMetric(float64(consumed.Load())/elapsed.Seconds(), "consumed/s")
}

// Sync-mode dispatch: the lowest-latency path (no queue hand-off).
func BenchmarkHotSubscriberDispatchSync(b *testing.B) {
	var consumed atomic.Uint64
	s, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://127.0.0.1:1", Topic: "t", ReportInterval: -1, Logger: benchLogger{}},
		Handler: func(*nats.Msg) { consumed.Add(1) },
		Workers: SyncMode,
	})
	if err != nil {
		b.Fatal(err)
	}

	msg := &nats.Msg{Data: benchPayload}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.dispatch(msg)
	}
}

package natslink

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/nats-io/nats.go"
)

// A publish that no endpoint can admit (both byte budgets exhausted) is
// rejected before the payload is copied: zero allocations, every endpoint's
// QueueDropped and the pool-level Dropped each grow by one. A payload larger
// than the byte bound is rejected the same way with empty queues.
func TestMirroredPublishRejectionDoesNotCopy(t *testing.T) {
	p, _, _ := newTwoEndpointPool(t)
	p.running.Store(true) // never started: no connections, no send goroutines
	for _, g := range p.groups {
		g.queuedBytes.Store(g.maxBytes)
	}
	payload := make([]byte, 1<<20)
	allocs := testing.AllocsPerRun(50, func() {
		if err := p.Publish(payload); !errors.Is(err, ErrQueueFull) {
			t.Fatalf("publish = %v, want ErrQueueFull", err)
		}
	})
	if allocs != 0 {
		t.Errorf("rejected publish allocated %.1f objects per call, want 0 (payload must not be copied)", allocs)
	}
	st := p.Stats()
	if p.Dropped() != 51 || st[0].QueueDropped != 51 || st[1].QueueDropped != 51 {
		t.Errorf("dropped pool=%d queue e0=%d e1=%d, want 51 each", p.Dropped(), st[0].QueueDropped, st[1].QueueDropped)
	}
	for _, g := range p.groups {
		if g.queuedBytes.Load() != g.maxBytes {
			t.Errorf("rejection leaked a byte reservation: %d != %d", g.queuedBytes.Load(), g.maxBytes)
		}
	}

	// Empty queues but a payload over the byte bound: same path.
	for _, g := range p.groups {
		g.queuedBytes.Store(0)
	}
	huge := make([]byte, mirrorQueueBytes+1)
	if err := p.Publish(huge); !errors.Is(err, ErrQueueFull) {
		t.Errorf("oversized-for-queue publish = %v, want ErrQueueFull", err)
	}
	if got := p.groups[0].queuedBytes.Load(); got != 0 {
		t.Errorf("oversized rejection left %d reserved bytes", got)
	}
}

// Queues full by entries (not bytes) reject without copying as well, and the
// entry reservation never overcommits under concurrent producers.
func TestMirroredPublishEntryFullRejectionDoesNotCopy(t *testing.T) {
	p, _, _ := newTwoEndpointPool(t)
	p.running.Store(true) // never started: nothing drains, so the queues fill by entries
	for i := 0; i < mirrorQueueEntries; i++ {
		if err := p.Publish([]byte("small")); err != nil {
			t.Fatalf("fill publish %d: %v", i, err)
		}
	}
	for i, g := range p.groups {
		if n := g.queuedEntries.Load(); n != mirrorQueueEntries || len(g.queue) != mirrorQueueEntries {
			t.Fatalf("endpoint %d entries=%d len=%d, want %d", i, n, len(g.queue), mirrorQueueEntries)
		}
	}
	big := make([]byte, 2<<20)
	allocs := testing.AllocsPerRun(20, func() {
		if err := p.Publish(big); !errors.Is(err, ErrQueueFull) {
			t.Fatalf("publish into entry-full queues = %v, want ErrQueueFull", err)
		}
	})
	if allocs != 0 {
		t.Errorf("entry-full rejection allocated %.1f objects per call, want 0", allocs)
	}
	if got := p.groups[0].queuedBytes.Load(); got != int64(mirrorQueueEntries*len("small")) {
		t.Errorf("rejection leaked a byte reservation: queued bytes %d", got)
	}

	// Concurrent producers against drained-by-nobody queues: the number of
	// admitted messages never exceeds the entry cap, reservations balance.
	q, _, _ := newTwoEndpointPool(t)
	q.running.Store(true)
	var wg sync.WaitGroup
	var admitted atomic.Int64
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < mirrorQueueEntries; i++ {
				if err := q.Publish([]byte("x")); err == nil {
					admitted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	for i, g := range q.groups {
		if len(g.queue) != mirrorQueueEntries || g.queuedEntries.Load() != mirrorQueueEntries || g.queuedBytes.Load() != mirrorQueueEntries {
			t.Errorf("endpoint %d after concurrent fill: len=%d entries=%d bytes=%d, want %d each", i, len(g.queue), g.queuedEntries.Load(), g.queuedBytes.Load(), mirrorQueueEntries)
		}
	}
	if admitted.Load() != mirrorQueueEntries {
		t.Errorf("admitted %d publishes, want exactly %d (both endpoints fill in lockstep)", admitted.Load(), mirrorQueueEntries)
	}
}

// Partial admission keeps its semantics with reservation-first admission: an
// endpoint whose byte budget is exhausted drops its copy, the other accepts,
// and an endpoint whose entry queue is full after a successful byte
// reservation releases the reservation.
func TestMirroredPublishReservationRollback(t *testing.T) {
	p, a, _ := newTwoEndpointPool(t)
	a.block.Store(true)
	p.groups[0].queue = make(chan *mirrorMsg, 1)
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { a.unblock(); p.Stop() })

	if err := p.Publish([]byte("abcd")); err != nil {
		t.Fatalf("publish 0: %v", err)
	}
	waitFor(t, "first message in flight on endpoint 0", func() bool { return p.groups[0].queuedBytes.Load() == 0 })
	for i := 1; i < 3; i++ { // 1 queued, 1 rejected by entries
		if err := p.Publish([]byte("abcd")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	waitFor(t, "entry rejection", func() bool { return p.Stats()[0].QueueDropped == 1 })
	if got := p.groups[0].queuedBytes.Load(); got != 4 {
		t.Errorf("queued bytes on endpoint 0 = %d, want 4 (one queued message; the rejected one released its reservation)", got)
	}
	if p.Dropped() != 0 {
		t.Errorf("pool dropped = %d, want 0", p.Dropped())
	}
}

// Stats must not hold the subscriber lock while asking a connection whether
// it is closed: that call takes the nats.go connection lock, which a stalled
// endpoint can hold for seconds, and under subMu it would block another
// endpoint's setup (a rebuild finishing). The test locks endpoint 0's
// connection mutex to emulate the stall, starts Stats, then checks endpoint
// 1's setup completes while Stats is still pending.
func TestStatsDoesNotBlockOtherEndpointSetup(t *testing.T) {
	ms, err := NewMirroredSubscriber([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, SubscriberOptions{
		Options: Options{Topic: "t", ReportInterval: -1},
		Handler: func(*nats.Msg) {},
		Workers: SyncMode,
	})
	if err != nil {
		t.Fatalf("NewMirroredSubscriber: %v", err)
	}
	if err := ms.Pause(); err != nil { // paused: setup only records the connection, no SUB is sent
		t.Fatalf("Pause: %v", err)
	}
	conn0, err := nats.Connect("nats://127.0.0.1:1", nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1), nats.ReconnectWait(time.Hour))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(conn0.Close)
	conn1, err := nats.Connect("nats://127.0.0.1:2", nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1), nats.ReconnectWait(time.Hour))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(conn1.Close)
	ms.subMu.Lock()
	ms.conns[ms.links[0]] = conn0
	ms.subMu.Unlock()

	// Emulate a stalled write on endpoint 0: hold its connection mutex.
	mu := connMutex(t, conn0)
	mu.Lock()
	statsDone := make(chan struct{})
	go func() { ms.Stats(); close(statsDone) }()
	time.Sleep(50 * time.Millisecond) // Stats is now inside IsClosed on endpoint 0

	setupDone := make(chan struct{})
	go func() { _ = ms.subscribeConn(ms.links[1], conn1); close(setupDone) }()
	select {
	case <-setupDone:
	case <-time.After(500 * time.Millisecond):
		mu.Unlock()
		t.Fatal("endpoint 1 setup blocked behind a Stats call stalled on endpoint 0's connection mutex")
	}
	mu.Unlock()
	<-statsDone
}

// connMutex returns the nats.Conn's internal mutex. Test-only: it emulates
// the lock a stalled network operation holds, which no public API exposes.
func connMutex(t *testing.T, c *nats.Conn) *sync.RWMutex {
	t.Helper()
	f := reflect.ValueOf(c).Elem().FieldByName("mu")
	if !f.IsValid() {
		t.Skip("nats.Conn has no mu field in this nats.go version")
	}
	return (*sync.RWMutex)(unsafe.Pointer(f.UnsafeAddr()))
}

// Endpoint caps: the pool constructor refuses more endpoints than its
// admission mask can track.
func TestMirroredPoolEndpointCap(t *testing.T) {
	urls := make([]string, maxMirroredPoolEndpoints+1)
	for i := range urls {
		urls[i] = "nats://h" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + ":4222"
	}
	if _, err := NewMirroredPublisherPool(urls, 1, PublisherOptions{Options: Options{Topic: "t", ReportInterval: -1}}); err == nil {
		t.Fatal("65 endpoints should be rejected")
	}
	if _, err := NewMirroredPublisherPool(urls[:maxMirroredPoolEndpoints], 1, PublisherOptions{Options: Options{Topic: "t", ReportInterval: -1}}); err != nil {
		t.Fatalf("64 endpoints: %v", err)
	}
}

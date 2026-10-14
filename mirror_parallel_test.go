package natslink

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// blockingSend is a test send hook standing in for an endpoint's nats.go
// Publish: while blocked, every call waits until unblock; it records the
// payloads it got and returns the configured error. All fields are written by
// the test goroutine while the send goroutine reads them, hence the atomics.
type blockingSend struct {
	mu      sync.Mutex
	got     [][]byte
	release chan struct{}
	block   atomic.Bool
	err     atomic.Pointer[error]
}

func newBlockingSend(block bool) *blockingSend {
	b := &blockingSend{release: make(chan struct{})}
	b.block.Store(block)
	return b
}

func (b *blockingSend) send(data []byte) error {
	if b.block.Load() {
		b.mu.Lock()
		rel := b.release
		b.mu.Unlock()
		<-rel
	}
	b.mu.Lock()
	b.got = append(b.got, data)
	b.mu.Unlock()
	if e := b.err.Load(); e != nil {
		return *e
	}
	return nil
}

// unblock releases every send waiting now and lets later sends through.
func (b *blockingSend) unblock() {
	b.block.Store(false)
	b.mu.Lock()
	close(b.release)
	b.release = make(chan struct{})
	b.mu.Unlock()
}

func (b *blockingSend) fail(err error) { b.err.Store(&err) }

func (b *blockingSend) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.got)
}

func (b *blockingSend) payload(i int) byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.got[i][0]
}

func newTwoEndpointPool(t *testing.T) (*PublisherPool, *blockingSend, *blockingSend) {
	t.Helper()
	p, err := NewMirroredPublisher([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, PublisherOptions{
		Options: Options{Topic: "t", Name: "mp", ReportInterval: -1},
	})
	if err != nil {
		t.Fatalf("NewMirroredPublisher: %v", err)
	}
	a, b := newBlockingSend(false), newBlockingSend(false)
	p.groups[0].testSend = a.send
	p.groups[1].testSend = b.send
	return p, a, b
}

func waitFor(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", desc)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The core promise of the parallel mirror: a stalled endpoint delays neither
// the caller nor the other endpoint. Endpoint 0's send blocks; Publish still
// returns at once and endpoint 1 receives everything; endpoint 0's copies wait
// in its own queue (visible in its member's QueueLen) and flow once it
// unblocks. Nothing is lost at the pool level.
func TestMirroredPublishStalledEndpointDoesNotBlockOthers(t *testing.T) {
	p, a, b := newTwoEndpointPool(t)
	a.block.Store(true)
	if err := p.Start(); err != nil { // no server: members retry in the background, the hook never touches them
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { a.unblock(); p.Stop() })

	const n = 10
	start := time.Now()
	for i := 0; i < n; i++ {
		if err := p.Publish([]byte{byte(i)}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if el := time.Since(start); el > 100*time.Millisecond {
		t.Errorf("10 publishes took %v with one endpoint stalled; Publish must not block", el)
	}
	waitFor(t, "endpoint 1 to receive all", func() bool { return b.count() == n })
	if got := a.count(); got != 0 {
		t.Errorf("stalled endpoint delivered %d, want 0", got)
	}
	// One message is held inside the blocked send, the rest sit in the queue
	// (wait for the send goroutine to have taken the first one).
	waitFor(t, "stalled endpoint backlog", func() bool { return p.Stats()[0].QueueLen == n-1 })
	if st := p.Stats(); st[0].QueueCap != mirrorQueueEntries || st[1].QueueLen != 0 {
		t.Errorf("queue cap = %d, endpoint 1 queue = %d, want %d and 0", st[0].QueueCap, st[1].QueueLen, mirrorQueueEntries)
	}
	if p.Dropped() != 0 {
		t.Errorf("pool dropped = %d, want 0 (endpoint 1 delivered everything)", p.Dropped())
	}

	// Unblock endpoint 0: its backlog drains in order.
	a.unblock()
	waitFor(t, "stalled endpoint to drain", func() bool { return a.count() == n })
	for i := 0; i < n; i++ {
		if got := a.payload(i); got != byte(i) {
			t.Fatalf("endpoint 0 delivered out of order at %d: %d", i, got)
		}
	}
	if st := p.Stats(); st[0].QueueLen != 0 {
		t.Errorf("queue not drained: %d", st[0].QueueLen)
	}
}

// A full queue drops that endpoint's copy only (counted on the endpoint); the
// message is not lost while another endpoint accepts it. When every endpoint
// is full, Publish returns ErrQueueFull and the pool-level Dropped grows.
func TestMirroredPublishQueueFullAccounting(t *testing.T) {
	p, a, b := newTwoEndpointPool(t)
	a.block.Store(true)
	b.block.Store(true)
	p.groups[0].queue = make(chan *mirrorMsg, 2) // shrink endpoint 0's queue
	p.groups[1].queue = make(chan *mirrorMsg, 4)
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { a.unblock(); b.unblock(); p.Stop() })

	// Each endpoint holds one message inside its blocked send, then queues
	// fill: endpoint 0 takes 1+2, endpoint 1 takes 1+4.
	if err := p.Publish([]byte("m")); err != nil {
		t.Fatalf("publish 0: %v", err)
	}
	waitFor(t, "first message picked up by both send goroutines", func() bool {
		st := p.Stats()
		return st[0].QueueLen == 0 && st[1].QueueLen == 0
	})
	for i := 1; i < 5; i++ {
		if err := p.Publish([]byte("m")); err != nil {
			t.Fatalf("publish %d: %v (endpoint 1 still has room)", i, err)
		}
	}
	waitFor(t, "endpoint 0 queue to fill", func() bool { return p.Stats()[0].QueueDropped == 2 })
	if p.Dropped() != 0 {
		t.Errorf("pool dropped = %d, want 0 (every message reached endpoint 1's queue)", p.Dropped())
	}
	// Both endpoints are now full: the next message has nowhere to go.
	if err := p.Publish([]byte("m")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("publish with both queues full = %v, want ErrQueueFull", err)
	}
	// The endpoint drops are public, per member, and separate from send failures.
	st := p.Stats()
	if p.Dropped() != 1 || st[0].QueueDropped != 3 || st[1].QueueDropped != 1 || st[0].Dropped != 0 || st[1].Dropped != 0 {
		t.Errorf("dropped pool=%d queue e0=%d e1=%d member e0=%d e1=%d, want 1/3/1/0/0",
			p.Dropped(), st[0].QueueDropped, st[1].QueueDropped, st[0].Dropped, st[1].Dropped)
	}
}

// The byte bound: a stalled endpoint's queue refuses a message that would
// push its queued bytes over the limit, so a few large messages cannot pin an
// unbounded amount of memory.
func TestMirroredPublishQueueBytesBound(t *testing.T) {
	p, a, _ := newTwoEndpointPool(t)
	a.block.Store(true)
	p.groups[0].maxBytes = 10
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { a.unblock(); p.Stop() })

	if err := p.Publish([]byte("12345678")); err != nil { // 8 bytes: ends up in flight on endpoint 0
		t.Fatalf("first publish: %v", err)
	}
	// Wait until the first message left the queue into the blocked send.
	waitFor(t, "first message in flight", func() bool { return p.groups[0].queuedBytes.Load() == 0 })
	if err := p.Publish([]byte("12345678")); err != nil { // 8 <= 10: queued
		t.Fatalf("second publish: %v", err)
	}
	if err := p.Publish([]byte("123")); err != nil { // 8+3 > 10: endpoint 0 drops, endpoint 1 accepts
		t.Fatalf("third publish: %v", err)
	}
	if got := p.Stats()[0].QueueDropped; got != 1 {
		t.Errorf("endpoint 0 queue dropped = %d, want 1 (bytes bound)", got)
	}
	if got := p.groups[0].queuedBytes.Load(); got != 8 {
		t.Errorf("queued bytes = %d, want 8", got)
	}
}

// A message that every endpoint fails to send counts as lost; one endpoint
// failing while the other succeeds does not.
func TestMirroredPublishAllEndpointsFailCountsOnce(t *testing.T) {
	p, a, b := newTwoEndpointPool(t)
	a.fail(errors.New("boom"))
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(p.Stop)

	if err := p.Publish([]byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitFor(t, "both endpoints to settle", func() bool { return a.count() == 1 && b.count() == 1 })
	time.Sleep(20 * time.Millisecond) // settle runs right after the hook returns
	if p.Dropped() != 0 || p.Stats()[0].QueueDropped != 0 {
		t.Errorf("dropped pool=%d queue e0=%d, want 0/0 (a send failure is the member's Dropped, not a queue drop)", p.Dropped(), p.Stats()[0].QueueDropped)
	}
	b.fail(errors.New("boom too"))
	if err := p.Publish([]byte("y")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitFor(t, "pool-level drop", func() bool { return p.Dropped() == 1 })
}

// Stop with a stalled endpoint returns within the stop grace, discards and
// counts what was still queued, and Publish afterwards returns ErrStopped.
func TestMirroredPublishStopDrainsStalledQueue(t *testing.T) {
	p, a, b := newTwoEndpointPool(t)
	a.block.Store(true)
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(a.unblock)

	for i := 0; i < 5; i++ {
		if err := p.Publish([]byte("m")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	waitFor(t, "endpoint 0 backlog", func() bool { return p.Stats()[0].QueueLen == 4 })
	start := time.Now()
	p.Stop()
	if el := time.Since(start); el > stopGrace+2*time.Second {
		t.Errorf("Stop took %v with a stalled endpoint, want <= stopGrace + member stop", el)
	}
	st := p.Stats()
	if got := st[0].QueueDropped; got != 4 {
		t.Errorf("endpoint 0 queue dropped on stop = %d, want 4 (queued leftovers)", got)
	}
	// The healthy endpoint flushed its whole backlog before exiting: nothing
	// accepted by Publish was lost to the shutdown.
	if got := b.count(); got != 5 || st[1].QueueDropped != 0 {
		t.Errorf("healthy endpoint delivered %d (queue dropped %d) on stop, want 5 / 0", got, st[1].QueueDropped)
	}
	if err := p.Publish([]byte("late")); !errors.Is(err, ErrStopped) {
		t.Errorf("publish after stop = %v, want ErrStopped", err)
	}
	if p.Dropped() != 1 { // the "late" message: every endpoint failed
		t.Errorf("pool dropped = %d, want 1", p.Dropped())
	}
}

// Stop with every endpoint stalled is bounded by one stop grace, not one per
// endpoint: the send lines are waited for in parallel.
func TestMirroredPublishStopWaitsEndpointsInParallel(t *testing.T) {
	p, a, b := newTwoEndpointPool(t)
	a.block.Store(true)
	b.block.Store(true)
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { a.unblock(); b.unblock() })

	if err := p.Publish([]byte("m")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitFor(t, "both sends in flight", func() bool {
		st := p.Stats()
		return st[0].QueueLen == 0 && st[1].QueueLen == 0 && p.groups[0].queuedBytes.Load() == 0 && p.groups[1].queuedBytes.Load() == 0
	})
	start := time.Now()
	p.Stop()
	if el := time.Since(start); el >= 2*stopGrace {
		t.Errorf("Stop took %v with two stalled endpoints, want about one stopGrace (%v), not the sum", el, stopGrace)
	}
}

// Before Start a mirrored pool rejects synchronously, like the plain pool.
func TestMirroredPublishBeforeStart(t *testing.T) {
	p, _, _ := newTwoEndpointPool(t)
	if err := p.Publish([]byte("x")); !errors.Is(err, ErrNotRunning) || p.Dropped() != 1 {
		t.Errorf("publish before start = %v dropped=%d, want ErrNotRunning / 1", err, p.Dropped())
	}
}

// Single-URL mirrored constructors degrade to the plain constructors: same
// naming, no send queue, synchronous Publish; NewMirroredSubscriber keeps the
// plain name as well.
func TestMirrorSingleURLDegradesToPlain(t *testing.T) {
	pool, err := NewMirroredPublisherPool([]string{"nats://h1:4222"}, 3, PublisherOptions{
		Options: Options{Topic: "t", Name: "pp", ReportInterval: -1},
	})
	if err != nil {
		t.Fatalf("NewMirroredPublisherPool: %v", err)
	}
	if pool.async || pool.Endpoints() != 1 || pool.Size() != 3 {
		t.Errorf("async=%v endpoints=%d size=%d, want false/1/3", pool.async, pool.Endpoints(), pool.Size())
	}
	if got := pool.pubs[2].o.Name; got != "pp-2" {
		t.Errorf("member name = %q, want pp-2 (plain pool naming)", got)
	}

	var count atomic.Int32
	sub, err := NewMirroredSubscriber([]string{"nats://h1:4222"}, SubscriberOptions{
		Options: Options{Topic: "events.x", Name: "ss", ReportInterval: -1},
		Handler: func(*nats.Msg) { count.Add(1) },
		Workers: SyncMode,
	})
	if err != nil {
		t.Fatalf("NewMirroredSubscriber: %v", err)
	}
	if sub.Endpoints() != 1 || sub.links[0].o.Name != "ss" {
		t.Errorf("endpoints=%d name=%q, want 1 / \"ss\" (no -e0)", sub.Endpoints(), sub.links[0].o.Name)
	}
	anon, _ := NewMirroredSubscriber([]string{"nats://h1:4222"}, SubscriberOptions{
		Options: Options{Topic: "events.x", ReportInterval: -1},
		Handler: func(*nats.Msg) {},
	})
	if got := anon.links[0].o.Name; got != "natslink-sub-events.x" {
		t.Errorf("default name = %q, want natslink-sub-events.x", got)
	}
}

// Concurrent publishers against a mirrored pool during Stop: no panic, no
// race, and every message Publish accepted (nil) is accounted for on every
// endpoint, either sent or counted as a queue drop. Nothing may be left in a
// queue after Stop (the Publish-vs-Stop ordering), so
// 2 x accepted == sent(e0) + sent(e1) + queueDropped(e0) + queueDropped(e1).
func TestMirroredPublishConcurrentWithStop(t *testing.T) {
	for round := 0; round < 5; round++ {
		p, a, b := newTwoEndpointPool(t)
		slow := func(bs *blockingSend) func([]byte) error {
			return func(d []byte) error { time.Sleep(50 * time.Microsecond); return bs.send(d) } // a little backlog at Stop
		}
		p.groups[0].testSend, p.groups[1].testSend = slow(a), slow(b)
		if err := p.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		var (
			wg       sync.WaitGroup
			accepted atomic.Int64
		)
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 300; i++ {
					err := p.Publish([]byte("race"))
					switch {
					case err == nil:
						accepted.Add(1)
					case errors.Is(err, ErrStopped), errors.Is(err, ErrQueueFull):
					default:
						t.Errorf("unexpected publish error: %v", err)
					}
				}
			}()
		}
		time.Sleep(2 * time.Millisecond)
		p.Stop()
		wg.Wait()
		p.Stop() // idempotent
		st := p.Stats()
		if st[0].QueueLen != 0 || st[1].QueueLen != 0 {
			t.Fatalf("round %d: messages left in queues after Stop: %d / %d", round, st[0].QueueLen, st[1].QueueLen)
		}
		if got, want := int64(a.count()+b.count())+int64(st[0].QueueDropped+st[1].QueueDropped), 2*accepted.Load(); got != want {
			t.Fatalf("round %d: accepted %d publishes but endpoints account for %d copies (sent %d+%d, queue-dropped %d+%d)",
				round, accepted.Load(), got, a.count(), b.count(), st[0].QueueDropped, st[1].QueueDropped)
		}
	}
}

// Two concurrent Stops are one stop sequence: the second waits for the first
// instead of stopping the members while the send goroutines are still
// flushing. The send hook pauses in front of the real publish, so without the
// guard the members would already be stopped when the publish runs and the
// accepted message would be lost (Dropped=1); with it the publish reaches the
// members' reconnect buffers (no server) and nothing is lost.
func TestMirroredPublishConcurrentStopIsOneSequence(t *testing.T) {
	p, _, _ := newTwoEndpointPool(t)
	gate := make(chan struct{})
	for _, g := range p.groups {
		g := g
		g.testSend = func(d []byte) error { <-gate; return g.publish(d) }
	}
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := p.Publish([]byte("m")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitFor(t, "both sends in flight", func() bool {
		st := p.Stats()
		return st[0].QueueLen == 0 && st[1].QueueLen == 0 && p.groups[0].queuedBytes.Load() == 0 && p.groups[1].queuedBytes.Load() == 0
	})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); p.Stop() }()
	}
	time.Sleep(100 * time.Millisecond) // both Stops are now inside / waiting on the sequence
	close(gate)
	wg.Wait()
	if p.Dropped() != 0 {
		t.Errorf("pool dropped = %d, want 0 (the accepted message must reach the members before they stop)", p.Dropped())
	}
	for i, st := range p.Stats() {
		if st.Dropped != 0 || st.QueueDropped != 0 {
			t.Errorf("member %d dropped=%d queue_dropped=%d, want 0/0", i, st.Dropped, st.QueueDropped)
		}
	}
}

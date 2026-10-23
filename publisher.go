package natslink

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// PublisherOptions configures one publishing connection.
type PublisherOptions struct {
	Options

	// MaxPayload > 0 rejects messages larger than that many bytes outright
	// (returns ErrOversized and counts towards Dropped).
	// 0 means no application-level limit (the server still enforces its own max_payload).
	MaxPayload int
}

// Publisher is a self-supervising connection dedicated to publishing on a single topic.
// Usage: NewPublisher -> Start -> Publish(...) -> Stop.
//
// While disconnected, Publish writes into the reconnect buffer (ReconnectBufSize)
// and returns nil; the buffered messages are flushed automatically once
// reconnected. It returns an error only when the buffer overflows or the
// connection is inside a self-healing rebuild, leaving failover or discard to
// the caller. For round-robin over several connections with failover use PublisherPool.
type Publisher struct {
	link
	maxPayload int
}

// NewPublisher creates a publishing connection (not yet connected; call Start).
func NewPublisher(o PublisherOptions) (*Publisher, error) {
	opts, err := o.Options.normalize("pub")
	if err != nil {
		return nil, err
	}
	p := &Publisher{maxPayload: o.MaxPayload}
	p.link.init(opts, "pub")
	return p, nil
}

// Start opens the connection. A temporarily unreachable server is not an error
// (it retries in the background, and Publish writes into the reconnect buffer
// meanwhile); the returned error is essentially only a configuration error such
// as a bad URL and should be treated as fatal.
func (p *Publisher) Start() error {
	return p.link.start()
}

// MustStart is Start but panics on failure.
//
// Note: MustStart succeeding does not mean connected (it succeeds even when the
// server is unreachable; retries continue in the background). For fail-fast
// "die if it cannot connect" semantics at startup, call MustProbe first (the
// standard pattern is in the Probe documentation).
func (p *Publisher) MustStart() {
	if err := p.Start(); err != nil {
		panic(err)
	}
}

// Stop makes a best-effort drain of the pending send buffer (1 second timeout)
// and then closes the connection. While disconnected the drain necessarily
// fails and the unsent messages in the reconnect buffer are discarded (a Warn
// log carries the buffered byte count).
// Bounded promise: the whole call blocks at most stopGrace (5s); if the peer is
// wedged the wait is abandoned and the drain and close continue in the
// background (see link.stop). In that case Stop returning does not mean the
// connection is closed, and the Warn for a failed flush may come after the
// return (lost if the process exits right away).
// Stop is idempotent and safe to call repeatedly; the instance is not reusable afterwards.
func (p *Publisher) Stop() {
	p.link.stop(true)
}

// Publish sends one message on the bound topic. A nil return means the
// connection accepted the message: sent immediately when connected, or written
// into the reconnect buffer during RECONNECTING and flushed on reconnect (core
// NATS has no delivery guarantee, see README).
// A non-nil error means the message was not accepted. Possible errors (all but
// the first two pre-check rejections are counted in Stats.Dropped):
//
//	ErrStopped    the instance has been stopped (single use; create a new one)
//	ErrNotRunning Start has not been called
//	ErrOversized  larger than MaxPayload
//	ErrNoConn     the connection is inside a rebuild window (old connection permanently closed, new one not yet swapped in)
//	other         an underlying nats error (e.g. reconnect buffer overflow, nats.ErrReconnectBufExceeded)
//
// Safe for concurrent use. data is copied synchronously into the send/reconnect
// buffer before return, so the caller may reuse it immediately.
func (p *Publisher) Publish(data []byte) error {
	if p.stopped.Load() {
		return ErrStopped
	}
	if !p.running.Load() {
		return ErrNotRunning
	}
	if p.maxPayload > 0 && len(data) > p.maxPayload {
		p.dropped.Add(1)
		return fmt.Errorf("%w: %s > %s", ErrOversized, prettySize(uint64(len(data))), prettySize(uint64(p.maxPayload)))
	}

	conn := p.conn.Load()
	if conn == nil {
		p.dropped.Add(1)
		return ErrNoConn // the very narrow window where Start and Publish race
	}
	if err := conn.Publish(p.o.Topic, data); err != nil {
		p.dropped.Add(1)
		if errors.Is(err, nats.ErrConnectionClosed) {
			// Self-healing rebuild window: the old connection that triggered the
			// rebuild is CLOSED and the new one has not been swapped in yet.
			// Normalised to this package's error so callers can detect the rebuild
			// window reliably with errors.Is(err, ErrNoConn) and fail over, without
			// importing nats.go's error variables.
			return fmt.Errorf("%w (rebuilding): %v", ErrNoConn, redactError(p.o.URL, err))
		}
		return redactError(p.o.URL, err)
	}
	return nil
}

// PublisherPool is a pool of publishers. The plain constructor
// (NewPublisherPool) builds N connections to one endpoint: round-robin start
// position, failover one connection at a time, nil as soon as any one of them
// accepts the message. The mirrored constructors (NewMirroredPublisher,
// NewMirroredPublisherPool) build connections to each of M independent
// endpoints: every endpoint is an independent send line with its own bounded
// queue and goroutine, Publish hands one shared copy of the message to each
// queue without blocking and returns nil as soon as any queue accepts
// (multi-entry racing on the send side, deduplication on the consumer side).
// Both constructors return the same type and are used identically apart from construction.
// While one connection is in a self-healing rebuild, traffic automatically
// lands on the remaining connections of that endpoint, so 1/N of the messages
// are not silently lost.
type PublisherPool struct {
	pubs   []*Publisher // every member, flattened across endpoints (Start/Stop/Stats/Size iterate members)
	groups []*pubGroup  // grouped by endpoint (Publish routes group by group); always one group for the plain constructor

	maxPayload int
	dropped    atomic.Uint64 // messages for which every endpoint failed and the whole message was discarded (true loss at the mirror level)

	// async is set for mirrored pools (two or more endpoints): Publish enqueues
	// instead of sending, and each group's goroutine does the sending. A plain
	// pool (one endpoint) keeps the synchronous path, so a single stalled
	// endpoint cannot delay the others and a single-endpoint pool is unchanged.
	async   bool
	running atomic.Bool
	stopped atomic.Bool
	started atomic.Bool // Start is single-shot at the pool level (a second Start must not roll back the first one's members)

	testBeforeRegister func() // test hook between the member dials and the send-line registration
	// stopMu orders Publish against Stop in a mirrored pool: Publish checks
	// stopped and enqueues under the read lock, Stop flips stopped under the
	// write lock before telling the send goroutines to exit, so no message can
	// slip into a queue nobody will ever drain. stopOnce makes the whole stop
	// sequence run exactly once, with concurrent callers waiting for it.
	stopMu   sync.RWMutex
	stopOnce sync.Once
}

// Per-endpoint send queue bounds for mirrored pools. Entries bound the
// goroutine hand-off; bytes bound memory when one endpoint stalls while the
// others keep draining (the payload copy is shared, so a stalled queue is what
// keeps it alive). Matched to the reconnect buffer: beyond this much backlog
// the endpoint is dead for racing purposes and its copies are dropped and
// counted rather than queued.
const (
	mirrorQueueEntries = 4096
	mirrorQueueBytes   = reconnectBufSize
)

// pubGroup is one endpoint's members plus its round-robin cursor, and in a
// mirrored pool its send line: a bounded queue drained by one goroutine.
type pubGroup struct {
	pubs   []*Publisher
	cursor atomic.Uint64

	// Mirrored pools only (nil queue otherwise).
	queue         chan *mirrorMsg
	queuedEntries atomic.Int64 // reserved queue slots (<= cap(queue)); released when a message leaves the queue
	queuedBytes   atomic.Int64
	maxBytes      int64
	dropped       atomic.Uint64 // copies this endpoint's queue discarded: queue full, or still queued at Stop (send failures are the members' Stats.Dropped)
	done          chan struct{}
	wg            sync.WaitGroup
	testSend      func(data []byte) error // test hook replacing publish on the send goroutine
}

// mirrorMsg is one message shared read-only by every endpoint's send
// goroutine, with the bookkeeping that decides whether the message was lost
// everywhere (the pool-level Dropped).
type mirrorMsg struct {
	data      []byte
	remaining atomic.Int32
	failed    atomic.Int32
}

// settle records one endpoint's outcome; the last endpoint to settle counts
// the message as lost at the pool level if no endpoint accepted it.
func (m *mirrorMsg) settle(p *PublisherPool, failed bool) {
	if failed {
		m.failed.Add(1)
	}
	if m.remaining.Add(-1) == 0 && int(m.failed.Load()) == len(p.groups) {
		p.dropped.Add(1)
	}
}

// enableAsync turns a multi-endpoint pool into independent send lines. Called
// by the mirrored constructors before Start.
func (p *PublisherPool) enableAsync() {
	p.async = true
	for _, g := range p.groups {
		g.queue = make(chan *mirrorMsg, mirrorQueueEntries)
		g.maxBytes = mirrorQueueBytes
		g.done = make(chan struct{})
		for _, pub := range g.pubs {
			g := g
			pub.statsHook = func(st *Stats) {
				st.QueueLen = len(g.queue)
				st.QueueCap = cap(g.queue)
				st.QueueDropped = g.dropped.Load()
			}
		}
	}
}

// reserve claims one queue slot and n bytes of this endpoint's budget without
// blocking; false means either bound would be exceeded and nothing was
// claimed. Both are reserved before the payload is copied, so a publish that
// no endpoint can admit, whether full by entries or by bytes, costs no copy.
func (g *pubGroup) reserve(n int64) bool {
	if g.queuedEntries.Add(1) > int64(cap(g.queue)) {
		g.queuedEntries.Add(-1)
		return false
	}
	if g.queuedBytes.Add(n) > g.maxBytes {
		g.queuedBytes.Add(-n)
		g.queuedEntries.Add(-1)
		return false
	}
	return true
}

// release gives back a reservation that will not be consumed by a dequeue.
func (g *pubGroup) release(n int64) {
	g.queuedBytes.Add(-n)
	g.queuedEntries.Add(-1)
}

// enqueue hands m to this endpoint's send goroutine, with a slot and its
// bytes already reserved. The reservation guarantees room, so the channel send
// does not block; the default branch only guards the window between a
// consumer releasing a slot and the message actually leaving the channel.
func (g *pubGroup) enqueue(m *mirrorMsg) bool {
	select {
	case g.queue <- m:
		return true
	default:
		g.release(int64(len(m.data)))
		return false
	}
}

// run is the endpoint's send goroutine: it performs the two routing passes for
// each queued message, so a stall inside nats.go (a wedged write holding the
// connection lock) delays only this endpoint's copies. On Stop it first sends
// whatever is still queued (those messages were accepted by Publish; a healthy
// endpoint must not lose them to a shutdown race) and only then exits.
func (g *pubGroup) run(p *PublisherPool) {
	defer g.wg.Done()
	for {
		select {
		case <-g.done:
			for {
				select {
				case m := <-g.queue:
					g.send(p, m)
				default:
					return
				}
			}
		case m := <-g.queue:
			g.send(p, m)
		}
	}
}

func (g *pubGroup) send(p *PublisherPool, m *mirrorMsg) {
	g.release(int64(len(m.data)))
	var err error
	if g.testSend != nil {
		err = g.testSend(m.data)
	} else {
		err = g.publish(m.data)
	}
	m.settle(p, err != nil) // a failed send is already counted in the member's Stats.Dropped
}

// drain discards whatever is still queued when the send goroutine did not
// finish within the stop grace (the endpoint is stalled), keeping the loss
// accounting exact.
func (g *pubGroup) drain(p *PublisherPool) {
	for {
		select {
		case m := <-g.queue:
			g.release(int64(len(m.data)))
			g.dropped.Add(1)
			m.settle(p, true)
		default:
			return
		}
	}
}

// newPubGroup builds n publishing connections for one endpoint (members named <Name>-<i>).
func newPubGroup(n int, o PublisherOptions) (*pubGroup, error) {
	if n <= 0 {
		return nil, fmt.Errorf("natslink: pool size must be positive, got %d", n)
	}

	baseOpts, err := o.Options.normalize("pub")
	if err != nil {
		return nil, err
	}

	g := &pubGroup{}
	for i := 0; i < n; i++ {
		po := o
		po.Options = baseOpts
		po.Name = fmt.Sprintf("%s-%d", baseOpts.Name, i)
		pub, err := NewPublisher(po)
		if err != nil {
			return nil, err
		}
		g.pubs = append(g.pubs, pub)
	}
	return g, nil
}

// NewPublisherPool creates n publishing connections on the same topic (not yet connected; call Start).
// Each connection's client name is <Name>-<i>.
func NewPublisherPool(n int, o PublisherOptions) (*PublisherPool, error) {
	g, err := newPubGroup(n, o)
	if err != nil {
		return nil, err
	}
	return &PublisherPool{maxPayload: o.MaxPayload, pubs: g.pubs, groups: []*pubGroup{g}}, nil
}

// Start starts every connection in the pool; if any fails (configuration
// error) the ones already started are stopped and that error is returned.
//
// Members are dialled with bounded concurrency (maxConcurrentDials): a slow
// handshake on one endpoint's members no longer postpones the dial of an
// independent endpoint by their count times the dial timeout.
func (p *PublisherPool) Start() error {
	if p.stopped.Load() {
		return ErrStopped
	}
	if p.started.Swap(true) {
		return ErrAlreadyStarted
	}
	if err := startAll(p.dialOrder(), (*Publisher).Start, (*Publisher).Stop); err != nil {
		return err
	}
	if p.testBeforeRegister != nil {
		p.testBeforeRegister()
	}
	// Register the send lines and commit the running state in the same short
	// critical section Stop uses to flip stopped, so a Stop that raced the
	// dials either sees no send line registered yet (and this returns
	// ErrStopped: the members have discarded their connections, link.start
	// checks stopped) or waits for lines that are fully registered; a
	// WaitGroup Add can never race Stop's Wait.
	p.stopMu.Lock()
	defer p.stopMu.Unlock()
	if p.stopped.Load() {
		return ErrStopped
	}
	if p.async {
		for _, g := range p.groups {
			g.wg.Add(1)
			go g.run(p)
		}
	}
	p.running.Store(true)
	return nil
}

// dialOrder interleaves the members across endpoints (member 0 of every
// endpoint, then member 1, ...), so the bounded dial concurrency of startAll
// is spread over endpoints: a slow first endpoint with eight or more members
// would otherwise occupy every slot and postpone the first dial of an
// independent healthy endpoint.
func (p *PublisherPool) dialOrder() []*Publisher {
	order := make([]*Publisher, 0, len(p.pubs))
	for i := 0; ; i++ {
		added := false
		for _, g := range p.groups {
			if i < len(g.pubs) {
				order = append(order, g.pubs[i])
				added = true
			}
		}
		if !added {
			return order
		}
	}
}

// MustStart is Start but panics on failure. Suited to fail-fast startup.
func (p *PublisherPool) MustStart() {
	if err := p.Start(); err != nil {
		panic(err)
	}
}

// WaitConnected blocks until every connection in the pool is ready; on timeout
// it returns the error of the first connection that was not ready.
// Usage is described on Publisher.WaitConnected.
func (p *PublisherPool) WaitConnected(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for _, pub := range p.pubs {
		if err := pub.WaitConnected(max(time.Until(deadline), 0)); err != nil {
			return err
		}
	}
	return nil
}

// Stop stops every connection in the pool.
// Members are closed in parallel: each blocks at most stopGrace (see
// link.stop), and the parallelism makes the pool-level worst case max rather
// than the sum, so the shutdown budget (e.g. systemd TimeoutStopSec) does not
// shrink linearly with pool size. A mirrored pool first gives its send lines
// one stopGrace to drain, then gives member closes another stopGrace; its total
// budget is therefore about 2*stopGrace (10s), plus scheduling overhead.
func (p *PublisherPool) Stop() {
	p.stopOnce.Do(p.stop)
}

// stop is the single stop sequence; a concurrent second Stop waits in
// stopOnce until it has finished rather than stopping the members early
// underneath the send goroutines that are still flushing.
func (p *PublisherPool) stop() {
	p.stopMu.Lock()
	p.stopped.Store(true)
	p.stopMu.Unlock()
	if p.async {
		// Stop the send lines first so nothing is handed to members that are
		// closing: each goroutine flushes its queue and exits. One wedged inside
		// nats.go is waited for at most stopGrace (the member's Stop below
		// unblocks it anyway); only then is its queue drained with every
		// leftover counted as dropped.
		for _, g := range p.groups {
			close(g.done)
		}
		// Wait for the send lines in parallel, so this phase is bounded by
		// stopGrace (max), not endpoints x stopGrace (sum). The member Stop
		// phase below has its own, separate stopGrace budget.
		var lines sync.WaitGroup
		for _, g := range p.groups {
			lines.Add(1)
			go func(g *pubGroup) {
				defer lines.Done()
				if !waitGroupTimeout(&g.wg, stopGrace) {
					g.drain(p)
				}
			}(g)
		}
		lines.Wait()
	}
	var wg sync.WaitGroup
	for _, pub := range p.pubs {
		wg.Add(1)
		go func(pub *Publisher) {
			defer wg.Done()
			pub.Stop()
		}(pub)
	}
	wg.Wait()
}

// Publish sends one message. With the plain constructor it amounts to "two
// routing passes over the pool, nil as soon as one connection succeeds"; with
// the mirrored constructor the same routing runs once per endpoint (one send
// each) and it returns nil as soon as any endpoint accepts. Mirroring is racing
// redundancy: a single endpoint failing merely loses one path (the message was
// delivered via the others) and is not counted as loss. Only when every
// endpoint fails does it return an error (errors.Join) and count towards the pool-level Dropped.
//
// The two routing passes within an endpoint: the first pass tries only members
// that are currently connected (IsConnected), so the message leaves immediately
// on a healthy connection and never sits in a disconnected member's reconnect
// buffer until that member reconnects (latency first: a message delivered
// seconds late is about as good as undelivered, while a healthy connection was
// sitting idle right next to it). The second pass turns to the disconnected
// members: when no member of the endpoint is healthy it falls back to "written
// into the reconnect buffer counts as success", so the message is buffered rather than dropped.
//
// Mirrored pools (two or more endpoints) do not send on the caller's
// goroutine at all: one copy of data is handed to every endpoint's bounded
// send queue (non-blocking) and nil means at least one queue accepted it; the
// endpoint goroutines then run the routing above independently, so a stalled
// endpoint only backs up its own queue (its copies are dropped once the queue
// is full, counted on that endpoint) while the others keep sending. Send
// failures surface asynchronously in the members' Stats, and a message that
// every endpoint failed to send still counts towards Dropped. ErrQueueFull is
// returned when no endpoint could take the message.
//
// Safe for concurrent use. data is copied before Publish returns, so the
// caller may reuse it immediately.
func (p *PublisherPool) Publish(data []byte) error {
	// The size check is hoisted to the pool level so an oversized message is not counted as Dropped once per connection.
	if p.maxPayload > 0 && len(data) > p.maxPayload {
		p.dropped.Add(1)
		return fmt.Errorf("%w: %s > %s", ErrOversized, prettySize(uint64(len(data))), prettySize(uint64(p.maxPayload)))
	}
	if p.async {
		return p.publishAsync(data)
	}

	var (
		accepted bool
		errs     []error
	)
	for _, g := range p.groups {
		if err := g.publish(data); err != nil {
			errs = append(errs, err)
		} else {
			accepted = true
		}
	}
	if accepted {
		return nil
	}
	p.dropped.Add(1)
	return errors.Join(errs...)
}

// publishAsync hands one shared copy of data to every endpoint's send queue.
// It runs under stopMu's read lock so that a message accepted here is always
// seen by the send goroutines (Stop flips stopped under the write lock before
// it lets them exit); the lock is uncontended except during Stop.
func (p *PublisherPool) publishAsync(data []byte) error {
	p.stopMu.RLock()
	defer p.stopMu.RUnlock()
	if p.stopped.Load() {
		p.dropped.Add(1)
		return ErrStopped
	}
	if !p.running.Load() {
		p.dropped.Add(1)
		return ErrNotRunning
	}
	// Reserve a queue slot and bytes on every endpoint first and copy the
	// payload only if at least one endpoint can take it: under overload (full
	// by entries or by bytes, or a payload larger than the byte bound) a
	// rejection must cost nothing proportional to the payload, otherwise
	// shedding load is what fills the heap.
	n := int64(len(data))
	var reserved uint64 // bit i = endpoint i holds a reservation (endpoints are capped at 64)
	for i, g := range p.groups {
		if g.reserve(n) {
			reserved |= 1 << i
		}
	}
	if reserved == 0 {
		for _, g := range p.groups {
			g.dropped.Add(1)
		}
		p.dropped.Add(1)
		return ErrQueueFull
	}
	m := &mirrorMsg{data: bytes.Clone(data)}
	m.remaining.Store(int32(len(p.groups)))
	accepted := 0
	for i, g := range p.groups {
		if reserved&(1<<i) != 0 && g.enqueue(m) {
			accepted++
			continue
		}
		g.dropped.Add(1)
		m.settle(p, true)
	}
	if accepted == 0 {
		return ErrQueueFull
	}
	return nil
}

// waitGroupTimeout waits for wg at most d and reports whether it finished.
func waitGroupTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// publish runs the two routing passes within this endpoint starting from the
// round-robin position and returns nil as soon as one connection succeeds;
// each connection is tried at most once per call, and if all fail the last error is returned.
func (g *pubGroup) publish(data []byte) error {
	start := g.cursor.Add(1)
	lastErr := error(ErrNoConn)

	// First pass: healthy first. Disconnected members are skipped and kept as
	// fallback. The candidate list lives on the stack for common pool sizes
	// (a scratch size, not a limit: append spills to the heap beyond it), so a
	// degraded window does not add an allocation to every publish.
	var fallbackBuf [8]*Publisher
	fallback := fallbackBuf[:0]
	for i := uint64(0); i < uint64(len(g.pubs)); i++ {
		pub := g.pubs[(start+i)%uint64(len(g.pubs))]
		if !pub.IsConnected() {
			fallback = append(fallback, pub)
			continue
		}
		if err := pub.Publish(data); err != nil {
			lastErr = err
			continue // fail over to the next connection
		}
		return nil
	}
	// Second pass: every healthy member failed (or there was none), so hand the
	// message to a disconnected member's reconnect buffer as the fallback.
	for _, pub := range fallback {
		if err := pub.Publish(data); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// ConnectedCount returns the number of currently connected connections in the pool (for observing degradation).
func (p *PublisherPool) ConnectedCount() int {
	n := 0
	for _, pub := range p.pubs {
		if pub.IsConnected() {
			n++
		}
	}
	return n
}

// Size returns the total connection count of the pool (mirrored constructor: endpoints x n per endpoint).
func (p *PublisherPool) Size() int {
	return len(p.pubs)
}

// Endpoints returns the number of endpoints the pool covers (len(urls) for the
// mirrored constructor, always 1 for the plain one).
func (p *PublisherPool) Endpoints() int {
	return len(p.groups)
}

// Dropped returns the cumulative count of messages for which every endpoint
// failed and the whole message was discarded. In a mirrored pool this is
// settled asynchronously, once the last endpoint's send goroutine has handled
// the message (or dropped it at the queue).
// This is the pool's true message-loss count: a single connection's
// Stats.Dropped is the number of failed attempts on that connection, and the
// message may well have been failed over to another connection (or, under
// mirroring, another endpoint) and sent successfully, so summing the members
// grossly overstates loss (false alarms).
func (p *PublisherPool) Dropped() uint64 {
	return p.dropped.Load()
}

// Stats returns a statistics snapshot per connection in the pool, flattened by
// member (with the mirrored constructor, endpoint 0's members come first, in
// urls order; each entry's Name carries the -e<endpoint>-<member> suffix for attribution).
// Note that each entry's Dropped includes intermediate failover failures; to
// monitor message loss use the pool-level Dropped() (see its documentation).
// In a mirrored pool every member of an endpoint also reports that endpoint's
// send-queue level in QueueLen / QueueCap and the copies its queue discarded
// in QueueDropped, so a stalled endpoint shows as a filling queue and then as
// a growing QueueDropped.
func (p *PublisherPool) Stats() []Stats {
	out := make([]Stats, 0, len(p.pubs))
	for _, pub := range p.pubs {
		out = append(out, pub.Stats())
	}
	return out
}

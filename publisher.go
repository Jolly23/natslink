package natslink

import (
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
// accepts the message. The mirrored constructor (NewMirroredPublisherPool)
// builds N connections to each of M independent endpoints: Publish routes and
// sends once per endpoint (multi-entry racing on the send side, deduplication
// on the consumer side) and returns nil as soon as any endpoint accepts.
// Both constructors return the same type and are used identically apart from construction.
// While one connection is in a self-healing rebuild, traffic automatically
// lands on the remaining connections of that endpoint, so 1/N of the messages
// are not silently lost.
type PublisherPool struct {
	pubs   []*Publisher // every member, flattened across endpoints (Start/Stop/Stats/Size iterate members)
	groups []*pubGroup  // grouped by endpoint (Publish routes group by group); always one group for the plain constructor

	maxPayload int
	dropped    atomic.Uint64 // messages for which every endpoint failed and the whole message was discarded (true loss at the mirror level)
}

// pubGroup is one endpoint's members plus its round-robin cursor.
type pubGroup struct {
	pubs   []*Publisher
	cursor atomic.Uint64
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
func (p *PublisherPool) Start() error {
	for i, pub := range p.pubs {
		if err := pub.Start(); err != nil {
			for _, started := range p.pubs[:i] {
				started.Stop()
			}
			return err
		}
	}
	return nil
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
// shrink linearly with pool size.
func (p *PublisherPool) Stop() {
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
// Safe for concurrent use. data is copied synchronously before each underlying
// Publish returns, so the caller may reuse it immediately.
func (p *PublisherPool) Publish(data []byte) error {
	// The size check is hoisted to the pool level so an oversized message is not counted as Dropped once per connection.
	if p.maxPayload > 0 && len(data) > p.maxPayload {
		p.dropped.Add(1)
		return fmt.Errorf("%w: %s > %s", ErrOversized, prettySize(uint64(len(data))), prettySize(uint64(p.maxPayload)))
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

// publish runs the two routing passes within this endpoint starting from the
// round-robin position and returns nil as soon as one connection succeeds;
// each connection is tried at most once per call, and if all fail the last error is returned.
func (g *pubGroup) publish(data []byte) error {
	start := g.cursor.Add(1)
	lastErr := error(ErrNoConn)

	// First pass: healthy first. Disconnected members are skipped and kept as fallback.
	var fallback []*Publisher
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
// failed and the whole message was discarded.
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
func (p *PublisherPool) Stats() []Stats {
	out := make([]Stats, 0, len(p.pubs))
	for _, pub := range p.pubs {
		out = append(out, pub.Stats())
	}
	return out
}

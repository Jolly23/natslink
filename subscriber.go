package natslink

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// SyncMode, assigned to SubscriberOptions.Workers, selects synchronous delivery (see that field).
const SyncMode = -1

// GoPerMessage, assigned to SubscriberOptions.Workers, selects go-per-message delivery (see that field).
const GoPerMessage = -2

// Handler is the subscription message callback.
//
// Concurrency and ordering contract: in worker-pool mode (the default) the
// callback is invoked concurrently by up to Workers goroutines, and processing
// order is not guaranteed to match publish order (several workers pull from the
// same queue), so shared state inside the callback needs its own
// synchronisation. In go-per-message mode the callback runs on detached
// goroutines with no upper bound on concurrency, and again without ordering.
// For strict ordering use Workers: 1 (ordered, delivery never blocks) or
// Workers: SyncMode (ordered, but blocks the NATS delivery goroutine).
// Under a mirrored subscription (NewMirroredSubscriber) the callback receives
// every copy from every endpoint; the ordering guarantees above hold only
// within a single endpoint, and copies from different endpoints interleave
// freely.
// Mirrored x SyncMode warning: the "serial and ordered" property of
// synchronous mode does not survive mirroring. Each of the M endpoints has its
// own NATS delivery goroutine, so the callback is invoked concurrently by M
// goroutines. A callback that relies on SyncMode serialisation to skip locking
// is a data race under mirroring and must be made concurrency-safe.
//
// In worker-pool and go-per-message modes the callback may do slow work. In
// synchronous mode it runs on the NATS delivery goroutine, and blocking for too
// long triggers slow-consumer drops (counted in SlowConsumers).
//
// msg.Data is memory allocated for that message alone (nats.go does not reuse
// its read buffer), so the callback may retain it after returning (store it,
// hand it to another goroutine) without a defensive copy.
type Handler func(msg *nats.Msg)

// SubscriberOptions configures one subscription connection.
type SubscriberOptions struct {
	Options

	Handler Handler // Required. Message callback (concurrency contract on the Handler type).

	// Gate is a callback admission check. When non-nil it is called once per
	// message at the delivery entry point; the message continues to Handler only
	// if it returns true, otherwise it is dropped on the spot and counted
	// (Stats.Gated). nil means always admit (the previous behaviour). Typical
	// use: a host that is not yet ready to process messages (still warming up,
	// still catching up on state) gates everything off, and admission resumes
	// automatically once it is ready, without rebuilding the subscription.
	//
	// Contract: Gate runs on the NATS delivery goroutine (under mirroring, on
	// each endpoint's own delivery goroutine, concurrently), so it must be
	// concurrency-safe and extremely cheap (an atomic read; a slow Gate blocks
	// delivery and triggers slow-consumer drops). The decision is made before
	// enqueue/dispatch: in worker-pool mode a message that was admitted and
	// queued, but not yet picked up by a worker when the gate closes, is still
	// delivered to the callback (queue residency window). Callers that need
	// strict point-in-time semantics must re-check inside Handler.
	Gate func() bool

	// Workers is the worker-pool goroutine count (bounded concurrency), default 8.
	// The callback never blocks the NATS delivery goroutine; when the pool cannot
	// keep up, new messages are dropped and counted (Stats.Dropped, so backpressure
	// is visible), which avoids the goroutine explosion / OOM that go-per-message
	// suffers under backpressure.
	// Workers = SyncMode (-1) selects synchronous mode: no worker pool, the
	// callback runs directly on the NATS delivery goroutine. Lowest latency (one
	// queue hand-off saved) but only suitable for non-blocking, very light
	// processing (a callback with lock contention, IO or millisecond-scale compute
	// belongs in the worker pool).
	// Workers = GoPerMessage (-2) selects go-per-message mode: no worker pool, the
	// delivery goroutine spawns a new goroutine per message, with unbounded
	// concurrency. Suited to streams where most messages are light but a few do
	// heavy synchronous work and must not be dropped. A bounded pool on such mixed
	// traffic gets its workers filled by the heavy messages, head-of-line blocks
	// the light ones, and drops under bursts, while the throughput ceiling of such
	// callbacks is usually a downstream lock, so extra goroutines merely queue on
	// it. The cost (read before choosing it): this mode has no backpressure signal
	// at all (Dropped stays 0; the callback never blocks delivery, so
	// SlowConsumers hardly ever fires either), the goroutine count floats with
	// load, and the caller must know their upstream rate and downstream capacity.
	// Any other negative value is a configuration error (NewSubscriber returns an error).
	// A mirrored subscription shares one worker pool: Workers is the total
	// concurrency bound across endpoints, not multiplied per endpoint.
	Workers int
	// QueueSize is the worker-pool buffer length (backpressure bound), default 4096.
	// No negative-value semantics; <= 0 always means the default.
	// A mirrored subscription shares one queue.
	QueueSize int

	// QueueGroup, when non-empty, makes this a queue subscription: subscribers in
	// the same group load-balance and compete for messages (each message goes to
	// one member). Unset means broadcast: every Subscriber receives every message.
	// For replicated deployments leave it unset to process everything on every
	// replica, set it to share the load.
	// Under mirroring the queue-group semantics apply per endpoint: members of a
	// group compete only within one endpoint, and each endpoint still delivers
	// its own copy.
	QueueGroup string

	// NATS client subscription buffer limits (another layer of protection before
	// this package's callback). When exceeded the client drops messages and raises
	// ErrSlowConsumer (counted in Stats.SlowConsumers). Essentially only reached in
	// synchronous mode with a slow callback (or an extreme burst): in worker-pool
	// mode backpressure is governed by Workers/QueueSize, and these two normally
	// need no tuning. Under mirroring they apply per endpoint.
	PendingMsgLimit   int // 0 uses the nats default (512k messages); -1 means unlimited
	PendingBytesLimit int // 0 uses the nats default (64MB); -1 means unlimited
}

// Subscriber is a self-supervising subscription to a single topic. The plain
// constructor (NewSubscriber) holds one connection; the mirrored constructor
// (NewMirroredSubscriber) holds one connection per endpoint sharing a single
// worker pool and callback, and Handler receives every copy from every endpoint
// (deduplication is the consumer's job). Both constructors return the same type
// and are used identically apart from construction.
// Usage: NewSubscriber / NewMirroredSubscriber -> Start -> (Stats/IsConnected at
// any time; Pause/Resume to suspend and re-subscribe without disconnecting) -> Stop.
type Subscriber struct {
	links []*link // one self-supervising connection per endpoint; always 1 for the plain constructor
	topic string

	handler    Handler
	gate       func() bool   // callback admission check, nil = always admit (see SubscriberOptions.Gate)
	gated      atomic.Uint64 // messages dropped by the gate (Subscriber-wide, summed across endpoints under mirroring)
	queueGroup string

	workers  int
	msgCh    chan *nats.Msg // non-nil means worker-pool mode; when nil, goPerMsg distinguishes the other two modes
	goPerMsg bool           // true means go-per-message mode (Workers = GoPerMessage)
	workerWG sync.WaitGroup // Stop blocks until every worker exits, so callbacks never race the caller's resource teardown

	done     chan struct{} // closed by Stop to end the shared worker pool (each link has its own done for its reporter goroutine)
	stopOnce sync.Once
	dropped  atomic.Uint64 // worker-queue overflow drops (the queue is shared across endpoints, so is the counter)

	// Pause / Resume (see those methods; self-healing of partially failed
	// endpoints was added later). subMu serialises the three parties that read
	// and write the subscription handles: Pause / Resume and each link's setup
	// (initial connect and self-healing rebuild):
	//   - paused: whether currently paused; while paused, setup only records the
	//     connection and does not send SUB;
	//   - conns: each link's most recent successfully set-up connection (which may
	//     not yet have been swapped into link.conn by the rebuild; in the few
	//     microseconds before the swap, Resume can only obtain it from here, and
	//     reading link.Conn() would return the dead old connection and leave the
	//     new one unsubscribed);
	//   - subs: each link's currently live subscription handle (empty while paused);
	//   - healing: whether the reconcile goroutine is running (see reconcileLocked / heal).
	subMu   sync.Mutex
	paused  bool
	conns   map[*link]*nats.Conn
	subs    map[*link]*nats.Subscription
	healing bool
	pauses  atomic.Uint64 // number of successful Pause transitions (Stats.Pauses)

	// Test hooks: when non-nil they are called before the real SUB / UNSUB and a
	// non-nil return is treated as that call failing. Read only on the control
	// plane (under subMu), always nil in production; tests set them while holding subMu.
	testSubscribeErr   func() error
	testUnsubscribeErr func() error

	pendingMsgLimit   int
	pendingBytesLimit int
}

// NewSubscriber creates a subscription connection (not yet connected; call Start).
func NewSubscriber(o SubscriberOptions) (*Subscriber, error) {
	return newSubscriber([]string{o.Options.URL}, o, false)
}

// newSubscriber is the shared assembly line behind NewSubscriber and
// NewMirroredSubscriber: each element of urls becomes one link, and all links
// share the worker pool, callback and counters.
// When mirrored, every connection name gets an -e<i> suffix (symmetric with the
// mirrored publisher pool, even for a single element); the plain constructor
// keeps the original naming (backwards compatible).
func newSubscriber(urls []string, o SubscriberOptions, mirrored bool) (*Subscriber, error) {
	if o.Handler == nil {
		return nil, errors.New("natslink: SubscriberOptions.Handler is required")
	}

	s := &Subscriber{
		handler:           o.Handler,
		gate:              o.Gate,
		queueGroup:        o.QueueGroup,
		done:              make(chan struct{}),
		pendingMsgLimit:   o.PendingMsgLimit,
		pendingBytesLimit: o.PendingBytesLimit,
		conns:             make(map[*link]*nats.Conn),
		subs:              make(map[*link]*nats.Subscription),
	}
	switch {
	case o.Workers >= 0: // worker-pool mode (0 = default size)
		s.workers = o.Workers
		if s.workers == 0 {
			s.workers = defaultWorkers
		}
		queueSize := o.QueueSize
		if queueSize <= 0 {
			queueSize = defaultQueueSize
		}
		s.msgCh = make(chan *nats.Msg, queueSize)
	case o.Workers == SyncMode: // synchronous mode: no queue, no workers, dispatch calls the handler directly
	case o.Workers == GoPerMessage:
		s.goPerMsg = true
	default:
		return nil, fmt.Errorf("natslink: invalid Workers %d (valid: >=0, SyncMode, GoPerMessage)", o.Workers)
	}

	base := o.Options.Name
	for i, u := range urls {
		eo := o.Options
		eo.URL = u
		if mirrored {
			eo.Name = mirrorEndpointName(base, "sub", o.Options.Topic, i)
		}
		opts, err := eo.normalize("sub")
		if err != nil {
			return nil, err
		}
		l := &link{}
		l.init(opts, "sub")
		l.setup = func(conn *nats.Conn) error { return s.subscribeConn(l, conn) }
		// Let each link's built-in status report (and link-level snapshot) reflect
		// the real subscription-side backpressure: queue depth and overflow drops
		// are Subscriber-wide values, so under mirroring every endpoint reports the same ones.
		l.statsHook = s.injectSharedStats
		s.links = append(s.links, l)
	}
	s.topic = s.links[0].o.Topic
	return s, nil
}

// Start opens every endpoint connection and subscribes. A temporarily
// unreachable server is not an error (it retries in the background until
// connected, and the subscription takes effect with the connection). An error
// from any endpoint (essentially only configuration errors such as a bad URL or
// topic, which should be treated as fatal) rolls back the endpoints already started.
func (s *Subscriber) Start() error {
	for i, l := range s.links {
		if err := l.start(); err != nil {
			for _, started := range s.links[:i] {
				started.stop(false)
			}
			return err
		}
	}
	for i := 0; i < s.workers; i++ {
		s.workerWG.Add(1)
		go s.worker()
	}
	return nil
}

// MustStart is Start but panics on failure.
//
// Note: MustStart succeeding does not mean connected (it succeeds even when the
// server is unreachable; retries continue in the background). For fail-fast
// "die if it cannot connect" semantics at startup, call MustProbe first (the
// standard pattern is in the Probe documentation; for the mirrored constructor
// probe each endpoint URL separately).
func (s *Subscriber) MustStart() {
	if err := s.Start(); err != nil {
		panic(err)
	}
}

// Stop closes every endpoint connection, ends the worker goroutines, and blocks
// until all in-flight callbacks have returned. After Stop returns it is safe to
// release the resources the callback depends on (DB handles, channels, ...)
// without racing a straggling callback.
// Bounded promise: closing a single endpoint connection waits at most stopGrace
// (5s); if the peer is wedged the wait is abandoned and the close continues in
// the background (see link.stop), so in extreme cases the connection may not be
// fully closed when Stop returns. The GoPerMessage/SyncMode dispatch carries a
// done gate, so no new callbacks start after Stop (apart from the single-message
// race window with a callback already in flight).
// Callbacks must therefore terminate normally; a permanently blocked callback
// wedges Stop. Messages still queued and not yet picked up are discarded.
// The wait guarantee covers worker-pool mode only: in synchronous mode
// (SyncMode) the callback runs on the NATS delivery goroutine, and in
// go-per-message mode (GoPerMessage) on detached goroutines, and Stop waits for
// neither. A go-per-message user that wants to release callback resources after
// Stop must confirm for itself that in-flight callbacks have finished (or shut
// down in reverse dependency order so that the dependency outlives this subscription).
// Stop is idempotent and safe to call repeatedly; the instance is not reusable afterwards.
func (s *Subscriber) Stop() {
	s.stopOnce.Do(func() { close(s.done) })
	// Close the endpoint connections in parallel (each blocks at most stopGrace, see link.stop).
	var wg sync.WaitGroup
	for _, l := range s.links {
		wg.Add(1)
		go func(l *link) {
			defer wg.Done()
			l.stop(false)
		}(l)
	}
	wg.Wait()
	s.workerWG.Wait()
}

// Endpoints returns the endpoint count (len(urls) for the mirrored constructor, always 1 for the plain one).
func (s *Subscriber) Endpoints() int { return len(s.links) }

// Topic returns the topic this subscription is bound to.
func (s *Subscriber) Topic() string { return s.topic }

// IsConnected reports whether every endpoint is connected (false while
// RECONNECTING or inside a rebuild window).
// Under mirroring any endpoint being down yields false: endpoint degradation
// must be visible (the race has degraded to a single path).
func (s *Subscriber) IsConnected() bool {
	for _, l := range s.links {
		if !l.IsConnected() {
			return false
		}
	}
	return len(s.links) > 0
}

// WaitConnected blocks until every endpoint connection is ready; on timeout it
// returns the error of the first endpoint that was not ready.
// Usage and caveats are in the link.WaitConnected / Probe documentation.
func (s *Subscriber) WaitConnected(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for _, l := range s.links {
		if err := l.WaitConnected(max(time.Until(deadline), 0)); err != nil {
			return err
		}
	}
	return nil
}

// Conn returns the first endpoint's underlying connection, which may be nil
// during a rebuild window.
// Escape hatch only (e.g. Request-Reply); under mirroring it exposes only the
// endpoint for the first url.
func (s *Subscriber) Conn() *nats.Conn {
	return s.links[0].Conn()
}

// Stats returns a statistics snapshot merged across endpoints; safe to call
// concurrently at any time. Merge rules:
//   - Name is the first endpoint's connection name (plain constructor: the
//     connection name itself);
//   - Connected is true only when every endpoint is connected (endpoint
//     degradation shows up immediately as not connected, so a host's "degraded"
//     alert covers it for free); Status/Server/Cluster come from the first
//     connected endpoint, or from the first endpoint when none is connected;
//   - counters (Reconnects/Rebuilds/In*/Out*/SlowConsumers) are summed across endpoints;
//   - Dropped is the worker-queue overflow count (the queue is shared, so it is
//     a single value anyway). Note that under mirroring each message has M
//     copies, each taking one queue slot, so under overload Dropped counts
//     copies (roughly M times amplified); content is lost only when every copy
//     of a message is dropped, so divide by the multiplier before panicking
//     about Dropped > 0;
//   - for the plain (single-endpoint) constructor every field matches the previous behaviour.
func (s *Subscriber) Stats() Stats {
	paused, diverged := s.subState()
	out := Stats{Status: "no-conn", Dropped: s.dropped.Load(), Gated: s.gated.Load(), Paused: paused, Pauses: s.pauses.Load(), Diverged: diverged}
	if len(s.links) > 0 {
		out.Name = s.links[0].o.Name
	}
	if len(s.links) > 0 {
		out.Running = s.links[0].running.Load()
	}
	allConnected := len(s.links) > 0
	for _, l := range s.links {
		st := l.Stats()
		if st.Connected {
			if !out.Connected {
				out.Connected = true
				out.Status, out.Server, out.Cluster = st.Status, st.Server, st.Cluster
			}
		} else {
			allConnected = false
			if out.Status == "no-conn" {
				out.Status = st.Status
			}
		}
		out.Reconnects += st.Reconnects
		out.Rebuilds += st.Rebuilds
		out.InMsgs += st.InMsgs
		out.OutMsgs += st.OutMsgs
		out.InBytes += st.InBytes
		out.OutBytes += st.OutBytes
		out.SlowConsumers += st.SlowConsumers
	}
	out.Connected = out.Connected && allConnected
	if s.msgCh != nil {
		out.QueueLen = len(s.msgCh)
		out.QueueCap = cap(s.msgCh)
	}
	return out
}

// injectSharedStats copies the Subscriber-wide shared fields into a link-level
// snapshot. The built-in status report goes through link.Stats(); without this
// step the report would print dropped=0 and omit the queue fields while
// messages were being dropped under overload (a regression caught in review,
// now pinned by TestSubscriberLinkReportIncludesSharedStats).
func (s *Subscriber) injectSharedStats(st *Stats) {
	st.Dropped = s.dropped.Load()
	st.Gated = s.gated.Load()
	st.Paused, st.Diverged = s.subState()
	st.Pauses = s.pauses.Load()
	if s.msgCh != nil {
		st.QueueLen = len(s.msgCh)
		st.QueueCap = cap(s.msgCh)
	}
}

// subscribeConn completes the subscription on each new connection (initial
// connect or self-healing rebuild).
// Under mirroring each endpoint's connection goes through it separately, all
// subscribing to the same topic with the same dispatch.
// While paused (see Pause) it only records the connection and sends no SUB: the
// connection is still swapped in and kept alive, and Resume subscribes it later.
func (s *Subscriber) subscribeConn(l *link, conn *nats.Conn) error {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	s.conns[l] = conn
	// The old connection's subscription dies with the old connection (the rebuild
	// discards it after the swap), so drop the record first lest Pause call
	// Unsubscribe on a dead connection.
	delete(s.subs, l)
	if s.paused {
		return nil
	}
	sub, err := s.subscribe(conn)
	if err != nil {
		return err
	}
	s.subs[l] = sub
	return nil
}

// subscribe sends the SUB for this topic on conn and applies the client buffer limits. Caller holds subMu.
func (s *Subscriber) subscribe(conn *nats.Conn) (*nats.Subscription, error) {
	var (
		sub *nats.Subscription
		err error
	)
	if s.testSubscribeErr != nil {
		if err := s.testSubscribeErr(); err != nil {
			return nil, err
		}
	}
	if s.queueGroup != "" {
		sub, err = conn.QueueSubscribe(s.topic, s.queueGroup, s.dispatch)
	} else {
		sub, err = conn.Subscribe(s.topic, s.dispatch)
	}
	if err != nil {
		return nil, err
	}

	if s.pendingMsgLimit != 0 || s.pendingBytesLimit != 0 {
		msgLimit, bytesLimit := s.pendingMsgLimit, s.pendingBytesLimit
		if msgLimit == 0 {
			msgLimit = nats.DefaultSubPendingMsgsLimit
		}
		if bytesLimit == 0 {
			bytesLimit = nats.DefaultSubPendingBytesLimit
		}
		if err := sub.SetPendingLimits(msgLimit, bytesLimit); err != nil {
			_ = sub.Unsubscribe()
			return nil, err
		}
	}
	return sub, nil
}

// Pause suspends the subscription while keeping the connections open: it sends
// UNSUB on each endpoint's current connection, after which the server stops
// delivering this topic to the connection. Interest is withdrawn hop by hop
// along leaf and route links, so upstream NATS servers stop forwarding the
// stream to this node as well: the saving is real bandwidth, not a local
// discard (contrast Gate, which drops at the delivery entry point while the
// bytes still arrive on the NIC). Typical use: a host that knows it will not
// need this stream for a long stretch. The connection itself, heartbeats,
// automatic reconnect and self-healing rebuilds all continue, and
// Stats.Connected is unaffected; meanwhile Stats.Paused is true and Stats.Pauses
// is incremented once.
//
// Semantics and edges:
//   - Idempotent: calling it while already paused returns nil. After Stop it returns ErrStopped.
//   - A self-healing rebuild during the pause still swaps in the new connection,
//     it just does not subscribe on it; a nats.go automatic reconnect
//     (RECONNECTING -> CONNECTED) replays only the subscriptions still on the
//     books, so an UNSUBed one does not come back. Neither path needs caller intervention.
//   - Messages already in flight before the UNSUB still arrive and are delivered
//     (the server takes a moment to process the UNSUB), and in worker-pool mode
//     messages already queued but not yet consumed are still delivered to the
//     callback; callers that need strict point-in-time semantics must re-check
//     inside Handler (the same rule as Gate's queue residency window).
//   - An endpoint inside a rebuild window (connection permanently closed, new one
//     not yet swapped in) has nothing to do and is not an error: the new
//     connection is handled as paused when it is swapped in.
//   - The returned error is the join (errors.Join) of per-endpoint Unsubscribe
//     failures; "connection already closed" and "handle already invalid", both
//     meaning there was no subscription to begin with, are not counted as errors.
//   - When an endpoint's UNSUB fails, its handle stays on the books and the
//     paused state still takes effect; the package's reconcile goroutine retries
//     with backoff until the UNSUB succeeds (or the connection closes and the
//     rebuild takes over), logging
//     "[natslink] unsubscribe retried after failed pause"; until it converges
//     Stats.Diverged > 0. The caller does not need to retry.
//
// Unsubscribe takes the underlying connection lock and can block up to
// flusherTimeout (5s) if the peer is wedged; do not call it synchronously on a
// latency-sensitive path.
func (s *Subscriber) Pause() error {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	select {
	case <-s.done:
		return ErrStopped
	default:
	}
	if s.paused {
		return nil
	}
	s.paused = true
	s.pauses.Add(1)
	return s.reconcileAndHealLocked()
}

// Resume re-subscribes: it sends SUB again on each endpoint's most recently
// set-up connection, after which messages flow to the same Handler / worker
// pool. Idempotent: returns nil when not paused; returns ErrStopped after Stop.
//
// An endpoint whose connection is permanently closed (inside a rebuild window)
// is skipped: the setup of the rebuilt connection subscribes it as "not paused",
// so the caller need not retry. A connection in RECONNECTING has its SUB queued
// by nats.go and applied on reconnect, which is likewise not an error. The
// returned error is the join of per-endpoint subscribe failures; when one
// endpoint fails the others still resume and the paused state is already
// cleared (Stats.Paused is false). The failed endpoint is retried with backoff
// by the package's reconcile goroutine until its SUB succeeds (before that
// existed, an endpoint whose connection stayed healthy and never rebuilt would
// remain unsubscribed forever), logging
// "[natslink] subscribe retried after failed resume"; until it converges
// Stats.Diverged > 0. The caller does not need to retry.
func (s *Subscriber) Resume() error {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	select {
	case <-s.done:
		return ErrStopped
	default:
	}
	if !s.paused {
		return nil
	}
	s.paused = false
	return s.reconcileAndHealLocked()
}

// reconcileAndHealLocked runs one reconcile pass and starts the heal goroutine
// if any endpoint is left unaligned. Caller holds subMu.
func (s *Subscriber) reconcileAndHealLocked() error {
	errs, failed := s.reconcileLocked(false)
	if len(failed) > 0 && !s.healing {
		s.healing = true
		go s.heal()
	}
	return errors.Join(errs...)
}

// reconcileLocked aligns each endpoint's subscription handle with the current
// paused state. Caller holds subMu:
//   - not paused: a live connection (conns[l] non-nil and not closed) without a handle -> SUB;
//   - paused: a handle still present -> UNSUB; the record is removed only on
//     success or on "connection closed / handle invalid"; any other failure
//     (under nats.go 1.53.1 only ErrConnectionDraining) keeps the handle for the next retry.
//
// Endpoints whose connection is nil or closed belong to the rebuild (the new
// connection's setup applies the paused state of that moment and resets the
// endpoint's records), so they are skipped and not counted as unaligned. retry
// is true when called from the heal goroutine, in which case each endpoint that
// converges logs one line. Returns this pass's per-endpoint failures (errs[i]
// corresponds to failed[i], the endpoints still unaligned).
func (s *Subscriber) reconcileLocked(retry bool) (errs []error, failed []*link) {
	for _, l := range s.links {
		if s.paused {
			sub, ok := s.subs[l]
			if !ok {
				continue
			}
			err := s.unsubscribe(sub)
			if err != nil && !errors.Is(err, nats.ErrConnectionClosed) && !errors.Is(err, nats.ErrBadSubscription) {
				errs = append(errs, fmt.Errorf("natslink: %s pause unsubscribe: %w", l.o.Name, redactError(l.o.URL, err)))
				failed = append(failed, l)
				continue
			}
			delete(s.subs, l)
			if retry {
				l.o.Logger.Info("[natslink] unsubscribe retried after failed pause", "client", l.o.Name, "topic", l.o.Topic)
			}
			continue
		}
		conn := s.conns[l]
		if conn == nil || conn.IsClosed() {
			continue // rebuild window: the new connection's setup subscribes
		}
		if _, ok := s.subs[l]; ok {
			continue
		}
		sub, err := s.subscribe(conn)
		if err != nil {
			if errors.Is(err, nats.ErrConnectionClosed) {
				continue // connection closed mid-SUB: likewise the rebuild's job
			}
			errs = append(errs, fmt.Errorf("natslink: %s resume subscribe: %w", l.o.Name, redactError(l.o.URL, err)))
			failed = append(failed, l)
			continue
		}
		s.subs[l] = sub
		if retry {
			l.o.Logger.Info("[natslink] subscribe retried after failed resume", "client", l.o.Name, "topic", l.o.Topic)
		}
	}
	return errs, failed
}

// unsubscribe removes one subscription handle (with test hook). Caller holds subMu.
func (s *Subscriber) unsubscribe(sub *nats.Subscription) error {
	if s.testUnsubscribeErr != nil {
		if err := s.testUnsubscribeErr(); err != nil {
			return err
		}
	}
	return sub.Unsubscribe()
}

// heal is the reconcile goroutine, started only when Pause / Resume leaves an
// endpoint unaligned (single flight: the healing flag is maintained under
// subMu). It reruns reconcileLocked on the rebuild backoff schedule (starting at
// 2s, doubling, capped at 30s), each pass aligning to the paused state of that
// moment, so Pause / Resume flapping in the meantime only converges towards the
// latest state; it exits once everything is aligned or after Stop. Rate
// limiting is the backoff itself: each failed retry logs one Warn per failed endpoint.
//
// Locking: the same as Pause / Resume / setup, holding subMu across the SUB /
// UNSUB (both take the underlying connection lock and can block up to
// flusherTimeout 5s each if the peer is wedged; with N mirrored endpoints the
// worst case is N x 5s). While a heal pass runs, the host's Pause / Resume, the
// rebuild's setup and Stats() queue behind it. The "SUB outside the lock, then
// write back and re-verify under the lock" alternative was deliberately
// rejected: Pause / Resume / setup already hold the lock this way, heal only
// runs occasionally with backoff after a failure, and splitting the lock would
// buy nothing but one more path with different semantics plus compensation
// logic for "state flipped while unlocked -> tear down the subscription just
// created"; the risk outweighs the benefit.
func (s *Subscriber) heal() {
	backoff := rebuildBackoff
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	for {
		select {
		case <-s.done:
			s.subMu.Lock()
			s.healing = false
			s.subMu.Unlock()
			return
		case <-timer.C:
		}
		s.subMu.Lock()
		select {
		case <-s.done:
			s.healing = false
			s.subMu.Unlock()
			return
		default:
		}
		errs, failed := s.reconcileLocked(true)
		if len(failed) == 0 {
			s.healing = false
			s.subMu.Unlock()
			return
		}
		paused := s.paused
		s.subMu.Unlock()

		backoff = min(backoff*2, rebuildBackoffMax)
		// One line per failed endpoint: under mirroring only the client field identifies which endpoint.
		for i, l := range failed {
			l.o.Logger.Warn("[natslink] subscription reconcile failed, retrying",
				"client", l.o.Name, "topic", l.o.Topic, "paused", paused,
				"next_wait", backoff, "err", errs[i])
		}
		timer.Reset(backoff)
	}
}

// subState reads the paused state and the unaligned endpoint count in one subMu
// critical section (the Stats.Diverged definition, see reconcileLocked: only
// endpoints with a live connection count; endpoints inside a rebuild window do not).
func (s *Subscriber) subState() (paused bool, diverged int) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for _, l := range s.links {
		conn := s.conns[l]
		if conn == nil || conn.IsClosed() {
			continue
		}
		if _, ok := s.subs[l]; ok == s.paused {
			diverged++
		}
	}
	return s.paused, diverged
}

// IsPaused reports whether the subscription is currently paused (between Pause and Resume).
func (s *Subscriber) IsPaused() bool {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	return s.paused
}

// dispatch runs on the NATS delivery goroutine (under mirroring each endpoint
// has its own delivery goroutine sharing this method). In worker-pool mode it
// only does a non-blocking enqueue, never blocking the delivery goroutine, and
// drops with a count when the queue is full (visible backpressure); in
// go-per-message mode it only spawns one goroutine (microseconds, likewise
// non-blocking); in synchronous mode it calls the handler directly.
// The Gate is evaluated first, identically in all three modes: a gated-off
// message is not queued, spawns no goroutine and is not delivered, only counted (Stats.Gated).
func (s *Subscriber) dispatch(msg *nats.Msg) {
	if s.gate != nil && !s.gate() {
		s.gated.Add(1)
		return
	}
	switch {
	case s.msgCh != nil:
		select {
		case s.msgCh <- msg:
		default:
			s.dropped.Add(1)
		}
	case s.goPerMsg:
		// Late deliveries arriving after Stop must not start new callbacks: inside
		// the abandoned-close window (stopGrace exceeded) the connection may still
		// be delivering, or doReconnect may even have revived the subscription, and
		// without this gate a detached callback could run after the host has
		// released the resources it depends on. Worker-pool mode is covered by the
		// workers exiting and needs no gate (a late message is merely queued with
		// nobody consuming, or counted as dropped).
		select {
		case <-s.done:
		default:
			go s.handler(msg)
		}
	default:
		select {
		case <-s.done:
		default:
			s.handler(msg)
		}
	}
}

// worker consumes messages from the shared queue and calls the handler
// synchronously until Stop.
// The separate up-front done check matters: when several select cases are ready
// the choice is random, so after Stop a plain two-case select could keep
// consuming many backlogged messages before exiting; the up-front check bounds
// it to at most one more message after Stop.
func (s *Subscriber) worker() {
	defer s.workerWG.Done()
	for {
		select {
		case <-s.done:
			return
		default:
		}
		select {
		case <-s.done:
			return
		case msg := <-s.msgCh:
			s.handler(msg)
		}
	}
}

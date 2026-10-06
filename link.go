package natslink

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// link is the single-connection supervision core shared by Subscriber and
// Publisher: it dials, wires the event callbacks, rebuilds the connection
// after a permanent close, and accumulates statistics across rebuilds.
//
// Disconnects are handled on two layers:
//   - ordinary disconnects (server restart, network blip, a stall caught by
//     the heartbeat): the connection enters RECONNECTING and the nats.go client
//     reconnects on its own; after a successful reconnect subscriptions are
//     resent automatically, with no involvement from this package;
//   - permanent close (a fatal protocol error from the server, etc., leading to
//     CLOSED): the ClosedHandler starts the rebuild loop, which dials a whole
//     new connection (re-subscribing on subscriber connections) and swaps it in
//     atomically.
type link struct {
	o    Options
	role string // "sub" / "pub", for logging

	conn       atomic.Pointer[nats.Conn]
	rebuilding atomic.Bool // guarantees a single rebuild loop at a time
	running    atomic.Bool
	stopped    atomic.Bool

	done     chan struct{} // closed by Stop; ends the worker / status-report goroutines
	stopOnce sync.Once

	rebuilds      atomic.Uint64
	dropped       atomic.Uint64
	slowConsumers atomic.Uint64

	// Accumulated statistics of connections that have been swapped out, so
	// that Stats() is monotonic across rebuilds.
	prevReconnects atomic.Uint64
	prevInMsgs     atomic.Uint64
	prevOutMsgs    atomic.Uint64
	prevInBytes    atomic.Uint64
	prevOutBytes   atomic.Uint64

	// setup is called after every successful dial and before the connection is
	// swapped in (subscriber connections subscribe here); returning an error
	// discards the connection and retries.
	setup func(*nats.Conn) error
	// statsHook lets the owner inject shared fields into this connection's
	// Stats snapshot (the Subscriber's queue level and overflow drop count).
	// Without it the built-in status report (one line per connection when
	// ReportInterval>0) would be blind to subscriber back-pressure: logging
	// dropped=0 while messages are being dropped under overload is worse than
	// no report at all.
	statsHook func(*Stats)
}

func (l *link) init(o Options, role string) {
	l.o = o
	l.role = role
	l.done = make(chan struct{})
}

func (l *link) dial() (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(l.o.Name),

		// -- connect / reconnect policy --
		nats.MaxReconnects(-1),                      // unlimited reconnects, never give up
		nats.CustomReconnectDelay(l.reconnectDelay), // exponential backoff + jitter (see reconnectDelay), replacing the default fixed interval
		// Disable server-pool shuffling: a comma-separated URL list is tried in
		// the written order (ordered failover), so a reconnect always tries the
		// server you listed first. Note that nats.go stays on whichever server
		// it connected to and never fails back on its own, so this is "ordered
		// preference", not full primary/standby. It is a policy parameter and
		// may be overridden via ExtraNatsOptions.
		nats.DontRandomize(),
		// Ignore cluster gossip (connect_urls): the server pool contains only
		// the explicitly configured URLs, pinning the client to the intended
		// endpoint. Without this, as soon as the configured endpoint goes down
		// the client drifts to an arbitrary gossiped cluster member and never
		// comes home (observed in production as a client silently migrating to
		// the wrong cluster member; reproduced locally with a two-node cluster:
		// kill A and the client hops to B within a second and stays there after
		// A returns; with this option it keeps reconnecting to A and goes home
		// the moment A is back). It complements the server-side no_advertise
		// setting as defence in depth: the server does not advertise and the
		// client does not listen, so a configuration regression on either side
		// is still covered. The explicit comma-separated list is unaffected
		// (those are configured addresses, not discovered ones).
		nats.IgnoreDiscoveredServers(),
		nats.ReconnectBufSize(reconnectBufSize),
		nats.Timeout(dialTimeout),
		nats.RetryOnFailedConnect(true), // an initial connect failure is not an error either; keep retrying in the background
		nats.IgnoreAuthErrorAbort(),     // do not give up on auth errors during reconnect (common during a server's restart warm-up)

		// Stall detection tightened to ~9s (parameters are documented at the constants)
		nats.PingInterval(pingInterval),
		nats.MaxPingsOutstanding(maxPingsOut),
		// Per-write socket deadline cut from the default 60s to 5s: writes hold
		// the connection lock, and a 60s blocked write would leave both the ping
		// liveness check and Stop blind / queued (see the constant's comment)
		nats.FlusherTimeout(flusherTimeout),
	}

	// The caller's escape hatch is injected after the policy and before the
	// event callbacks: policy parameters may be overridden (at your own risk,
	// see the ExtraNatsOptions comment), the event callbacks may not. The
	// ClosedHandler drives the self-healing rebuild and the ErrorHandler
	// maintains the SlowConsumers observation; they are the core promise of
	// this package, and if they were silently replaced the three-layer failure
	// model would stop working with no symptom at all.
	opts = append(opts, l.o.ExtraNatsOptions...)

	opts = append(opts,
		// -- event callbacks (not overridable) --
		nats.ConnectHandler(func(c *nats.Conn) {
			l.o.Logger.Info("[natslink] connected", "client", l.o.Name, "topic", l.o.Topic, "server", c.ConnectedServerName())
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			l.o.Logger.Info("[natslink] 🔄 reconnected", "client", l.o.Name, "topic", l.o.Topic, "server", c.ConnectedServerName())
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			l.o.Logger.Warn("[natslink] ⚠️ disconnected", "client", l.o.Name, "topic", l.o.Topic, "err", redactError(l.o.URL, err))
		}),
		nats.ClosedHandler(func(c *nats.Conn) {
			// Reaching here means the connection is permanently closed: reconnecting was
			// abandoned, or the server sent a fatal protocol error (unknown -ERR).
			// IgnoreAuthErrorAbort does not cover the unknown -ERR path, so a rebuild is still needed.
			if !l.running.Load() {
				return // deliberate Stop, no rebuild
			}
			l.o.Logger.Error("[natslink] ⛔ connection permanently closed, rebuilding",
				"client", l.o.Name, "topic", l.o.Topic, "err", redactError(l.o.URL, c.LastError()))
			go l.rebuild()
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			subject := ""
			if sub != nil {
				subject = sub.Subject
			}
			// A slow consumer silently loses messages (the connection stays healthy and
			// looks fine while dropping data); count it separately so it can be observed.
			if errors.Is(err, nats.ErrSlowConsumer) {
				l.slowConsumers.Add(1)
			}
			l.o.Logger.Warn("[natslink] ⚠️ async error", "client", l.o.Name, "topic", subject, "err", redactError(l.o.URL, err))
		}),
	)

	conn, err := nats.Connect(l.o.URL, opts...)
	return conn, redactError(l.o.URL, err)
}

// reconnectDelay computes the wait before automatic reconnect attempt number
// attempts: exponential doubling from reconnectWait, capped at
// reconnectWaitMax, plus up to +50% random jitter (so many connections spread
// their reconnects instead of stampeding a server that has just restarted and
// is still warming up).
// In the typical case of a server restart lasting ten-odd seconds, the client
// is back at most one capped interval after the server recovers.
// nats.go resets attempts after a successful reconnect, so the next outage
// starts again from the base interval.
func (l *link) reconnectDelay(attempts int) time.Duration {
	d := reconnectWait
	for i := 0; i < attempts && d < reconnectWaitMax; i++ {
		d *= 2
	}
	d = min(d, reconnectWaitMax)
	d += rand.N(d/2 + 1)

	// Log every one of the first few attempts to ease diagnosis; after a long
	// outage, log less often to avoid flooding.
	if attempts <= 5 || attempts%10 == 0 {
		l.o.Logger.Warn("[natslink] 🔄 reconnecting", "client", l.o.Name, "topic", l.o.Topic, "attempt", attempts, "next_wait", d)
	}
	return d
}

// start establishes the initial connection. Because RetryOnFailedConnect is
// enabled, a temporarily unreachable server still returns success immediately
// (the connection keeps retrying in the background), so an error returned here
// is almost certainly a URL / parameter configuration error and should be
// treated as fatal.
func (l *link) start() error {
	if l.stopped.Load() {
		return ErrStopped
	}
	if l.running.Swap(true) {
		return ErrAlreadyStarted
	}
	l.o.Logger.Info("[natslink] starting", "version", Version, "role", l.role, "client", l.o.Name, "topic", l.o.Topic)

	conn, err := l.dial()
	if err != nil {
		l.running.Store(false)
		return redactError(l.o.URL, err)
	}
	if l.setup != nil {
		if err := l.setup(conn); err != nil {
			discard(conn)
			l.running.Store(false)
			return redactError(l.o.URL, err)
		}
	}
	l.conn.Store(conn)

	if l.o.ReportInterval > 0 {
		go l.reportLoop()
	}
	return nil
}

// stop closes the connection and ends all background goroutines. When flush is
// true (publishing connections) the pending-write buffer is drained first.
// Bounded promise: it blocks for at most stopGrace. The connection lock that
// FlushTimeout / Close need internally may be held by a blocked write or by a
// reconnect handshake (the root cause of a production shutdown hang, see the
// stopGrace comment), so drain + close run in a side goroutine, and if they
// have not finished within the grace period we stop waiting (the close
// continues in the background).
func (l *link) stop(flush bool) {
	// Clear the running flag first so that ClosedHandler / rebuild do not
	// rebuild the connection we are deliberately closing.
	l.running.Store(false)
	l.stopped.Store(true)
	l.stopOnce.Do(func() { close(l.done) })

	conn := l.conn.Load()
	if conn == nil {
		return
	}
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		if flush {
			// Best effort: flush necessarily fails while disconnected, and whatever is in
			// the reconnect buffer is lost when the process exits. Log how many bytes were
			// still unsent so the loss is visible.
			if err := conn.FlushTimeout(time.Second); err != nil {
				buffered, _ := conn.Buffered()
				l.o.Logger.Warn("[natslink] ⚠️ flush failed on stop, buffered messages lost",
					"client", l.o.Name, "buffered_bytes", buffered, "err", redactError(l.o.URL, err))
			}
		}
		discard(conn)
	}()
	select {
	case <-closed:
	case <-time.After(stopGrace):
		l.o.Logger.Warn("[natslink] ⚠️ stop grace exceeded, abandoning connection close",
			"client", l.o.Name, "topic", l.o.Topic, "grace", stopGrace)
	}
}

// rebuild performs the self-healing after a permanent close: dial again
// (+ setup, i.e. re-subscribe), then atomically replace the connection,
// retrying until it succeeds or Stop is called. The rebuilding flag
// guarantees there is never more than one rebuild loop per connection.
func (l *link) rebuild() {
	if !l.rebuilding.CompareAndSwap(false, true) {
		return // a rebuild loop is already running
	}
	// Note the LIFO order of the defers: release rebuilding first, then run the
	// "lost signal" compensation check. This prevents the following race: after
	// this rebuild successfully connects but before rebuilding is set back to
	// false, the new connection closes permanently again, and the rebuild
	// triggered by its ClosedHandler is dropped because the CAS fails, leaving
	// the connection dead forever with nobody to take over.
	defer func() {
		if c := l.conn.Load(); c != nil && c.IsClosed() && l.running.Load() {
			go l.rebuild()
		}
	}()
	defer l.rebuilding.Store(false)

	// Progressive backoff: start at rebuildBackoff, double on every failure,
	// cap at rebuildBackoffMax.
	backoff := rebuildBackoff
	for l.running.Load() {
		// Back off before dialing: this is the retry backoff, and it also
		// prevents a hot rebuild loop when the server keeps returning fatal errors.
		time.Sleep(backoff)
		backoff = min(backoff*2, rebuildBackoffMax)
		if !l.running.Load() {
			return // Stop was called during the backoff
		}

		conn, err := l.dial()
		if err != nil {
			l.o.Logger.Warn("[natslink] ❌ rebuild dial failed, retrying", "client", l.o.Name, "topic", l.o.Topic, "err", redactError(l.o.URL, err))
			continue
		}
		if l.setup != nil {
			// Crucial: a rebuilt subscriber connection must re-subscribe, otherwise
			// the connection is alive but receives nothing.
			if err := l.setup(conn); err != nil {
				l.o.Logger.Warn("[natslink] ❌ rebuild setup failed, retrying", "client", l.o.Name, "topic", l.o.Topic, "err", redactError(l.o.URL, err))
				// discard detaches the self-healing callbacks before closing, so this
				// deliberate discard does not trigger yet another rebuild.
				discard(conn)
				continue
			}
		}

		// Stop may have been called while dialing / subscribing: discard, to avoid
		// leaking the TCP connection and the background reconnect goroutines.
		if !l.running.Load() {
			discard(conn)
			return
		}
		// Atomically swap in the new connection, then absorb and close the old one.
		// The old connection is usually already CLOSED (that is what triggered this
		// rebuild), but if it is in a "stalled, auto-reconnecting" state and not
		// fully closed, leaving it open would duplicate deliveries / sends alongside
		// the new connection.
		if old := l.conn.Swap(conn); old != nil {
			l.absorb(old)
			discard(old)
		}
		l.rebuilds.Add(1)
		// Double check: if the Swap raced with Stop and Stop already passed over
		// this connection, close it here.
		if !l.running.Load() {
			conn.Close()
		}
		l.o.Logger.Info("[natslink] ♻️ connection rebuilt", "client", l.o.Name, "topic", l.o.Topic)
		return
	}
}

// absorb accumulates the statistics of a connection that is being swapped
// out, keeping Stats() monotonic across rebuilds.
func (l *link) absorb(old *nats.Conn) {
	st := old.Stats()
	l.prevReconnects.Add(st.Reconnects)
	l.prevInMsgs.Add(st.InMsgs)
	l.prevOutMsgs.Add(st.OutMsgs)
	l.prevInBytes.Add(st.InBytes)
	l.prevOutBytes.Add(st.OutBytes)
}

// discard silently drops a connection: the event callbacks are detached before
// closing, so neither a self-healing rebuild nor a spurious disconnect log is
// triggered.
func discard(conn *nats.Conn) {
	conn.SetClosedHandler(nil)
	conn.SetDisconnectErrHandler(nil)
	conn.Close()
}

// Stats returns a snapshot of the current status and data statistics. It is
// safe to call concurrently at any time.
func (l *link) Stats() Stats {
	s := Stats{
		Name:          l.o.Name,
		Running:       l.running.Load(),
		Status:        "no-conn",
		Rebuilds:      l.rebuilds.Load(),
		Dropped:       l.dropped.Load(),
		SlowConsumers: l.slowConsumers.Load(),
		Reconnects:    l.prevReconnects.Load(),
		InMsgs:        l.prevInMsgs.Load(),
		OutMsgs:       l.prevOutMsgs.Load(),
		InBytes:       l.prevInBytes.Load(),
		OutBytes:      l.prevOutBytes.Load(),
	}
	if conn := l.conn.Load(); conn != nil {
		s.Connected = conn.IsConnected()
		s.Status = conn.Status().String()
		s.Server = conn.ConnectedServerName()
		s.Cluster = conn.ConnectedClusterName()
		st := conn.Stats()
		s.Reconnects += st.Reconnects
		s.InMsgs += st.InMsgs
		s.OutMsgs += st.OutMsgs
		s.InBytes += st.InBytes
		s.OutBytes += st.OutBytes
	}
	if l.statsHook != nil {
		l.statsHook(&s)
	}
	return s
}

// IsConnected reports whether the connection is currently established
// (false while RECONNECTING or inside a rebuild window).
func (l *link) IsConnected() bool {
	conn := l.conn.Load()
	return conn != nil && conn.IsConnected()
}

// WaitConnected blocks until the connection is ready, or returns an error on
// timeout (including the current status and the underlying error, e.g. an
// authentication failure).
//
// This is a "wait until ready" utility (for tests, or when the application
// must be online before its first message). For startup fail-fast use
// MustProbe (see the Probe documentation): one probe connection exposes
// environment errors without every connection waiting out its own timeout.
// Also note that on a bad token this method waits the full timeout before
// returning (auth errors are deliberately treated as recoverable and retried,
// see the README's three lines of defence, item 2); the last_err in the
// returned error names the authentication failure.
func (l *link) WaitConnected(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if l.IsConnected() {
			return nil
		}
		if !l.running.Load() {
			if l.stopped.Load() {
				return ErrStopped
			}
			return ErrNotRunning
		}
		if time.Now().After(deadline) {
			status, lastErr := "no-conn", error(nil)
			if c := l.conn.Load(); c != nil {
				status = c.Status().String()
				lastErr = redactError(l.o.URL, c.LastError())
			}
			return fmt.Errorf("natslink: %s not connected within %v (status=%s, last_err=%v)",
				l.o.Name, timeout, status, lastErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Conn returns the current underlying connection; it may be nil inside a
// rebuild window. It is an escape hatch only (e.g. for Request-Reply);
// subscribing or publishing through it directly is outside this package's
// supervision.
func (l *link) Conn() *nats.Conn {
	return l.conn.Load()
}

// Topic returns the topic this connection is bound to.
func (l *link) Topic() string {
	return l.o.Topic
}

func (l *link) reportLoop() {
	l.report()
	ticker := time.NewTicker(l.o.ReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-l.done:
			return
		case <-ticker.C:
			l.report()
		}
	}
}

func (l *link) report() {
	s := l.Stats()
	args := []any{
		"role", l.role,
		"client", l.o.Name,
		"topic", l.o.Topic,
		"status", s.Status,
		"server", s.Server,
		"reconnects", s.Reconnects,
		"rebuilds", s.Rebuilds,
		"dropped", s.Dropped,
		"in_msgs", s.InMsgs,
		"in_bytes", prettySize(s.InBytes),
		"out_msgs", s.OutMsgs,
		"out_bytes", prettySize(s.OutBytes),
	}
	if s.QueueCap > 0 {
		args = append(args, "queue", fmt.Sprintf("%d/%d", s.QueueLen, s.QueueCap))
	}
	if s.Gated > 0 {
		args = append(args, "gated", s.Gated)
	}
	if s.Paused || s.Pauses > 0 {
		args = append(args, "paused", s.Paused, "pauses", s.Pauses)
	}
	if s.Diverged > 0 {
		args = append(args, "diverged", s.Diverged)
	}
	if s.SlowConsumers > 0 {
		args = append(args, "slow_consumers", s.SlowConsumers)
	}
	l.o.Logger.Info("[natslink] status", args...)

	// Not being connected is a degraded state; emit a separate Warn so it is
	// not buried among the Info lines.
	if !s.Connected {
		l.o.Logger.Warn("[natslink] ⚠️ degraded: not connected", "client", l.o.Name, "topic", l.o.Topic, "status", s.Status)
	}
}

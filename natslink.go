// Package natslink packages "one NATS connection bound to one topic" as a
// self-supervising unit that works out of the box.
//
// The goal is to let many programs share one battle-tested set of connection
// supervision logic:
//
//   - Probe / MustProbe: fail-fast probing at startup. Environment errors
//     (address / network / token) surface within about 5s, on the first line of
//     the process's life; a URL only needs to be probed once.
//   - Subscriber: one connection subscribing to one topic. Built-in connection
//     supervision, automatic reconnect on disconnect, self-healing rebuild after
//     a "permanent close" (with automatic re-subscription), and status / data
//     statistics accumulated across rebuilds. Callbacks are dispatched in one of
//     three modes: a bounded worker pool (default; back-pressure drops are
//     visible), synchronous (SyncMode), or go-per-message (GoPerMessage,
//     unbounded concurrency); see SubscriberOptions.Workers.
//   - Publisher: one connection publishing to one topic. The same supervision
//     and self-healing rebuild; a failed publish returns an error and the caller
//     decides whether to fail over or count a drop.
//   - PublisherPool: N Publishers with round-robin plus health-first failover
//     (latency first: disconnected members are demoted to last-resort
//     candidates, so a message always leaves on a healthy connection first).
//   - Mirrored constructors NewMirroredPublisherPool / NewMirroredSubscriber:
//     mirror the same topic onto M mutually independent NATS endpoints (publish
//     once per endpoint, subscribe once per endpoint; deduplication happens on
//     the consumer side). They return the ordinary types and are used exactly
//     the same way; intended for active-active topologies such as "two local
//     leaf nodes, each with its own upstream". See mirror.go and the README
//     section on mirroring.
//
// The standard startup sequence is MustProbe(url) (environment fail-fast) ->
// New* (configuration fail-fast) -> MustStart. After that, every runtime
// disconnect or permanent close is handled by the package's self-healing
// logic: it never gives up and never fails silently (see the README's
// mental-model and failure-handling sections).
//
// The package depends only on nats.go and the standard library; it carries no
// application code.
//
// Instances are single-use: after Stop they cannot be reused; create a new one.
package natslink

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Version is the package version. It mirrors the git tag (vX.Y.Z) and is
// logged at connection start, so operators can see which version each process
// runs.
const Version = "1.6.1"

var (
	ErrAlreadyStarted = errors.New("natslink: already started")
	ErrStopped        = errors.New("natslink: already stopped; create a new instance")
	ErrNotRunning     = errors.New("natslink: not running")
	ErrNoConn         = errors.New("natslink: no usable connection (rebuilding)")
	ErrOversized      = errors.New("natslink: payload exceeds MaxPayload")
)

// Logger is the minimal logging interface the package needs. The standard
// library's *slog.Logger and go-ethereum's log.Logger both satisfy it as-is:
// pass slog.Default() or log.Root() directly.
type Logger interface {
	Info(msg string, ctx ...any)
	Warn(msg string, ctx ...any)
	Error(msg string, ctx ...any)
}

// Probe opens a one-off connection to verify that natsURL is reachable and
// that authentication succeeds, then closes it immediately.
//
// Why it exists: Start / MustStart enable RetryOnFailedConnect, so an
// unreachable server or a bad token is not a Start error (the connection is
// created anyway and retries forever in the background). That is exactly the
// self-healing behaviour wanted at runtime, but it silently swallows
// environment errors (typo in the address, firewall, expired token, services
// deployed in the wrong order) until someone reads the logs. The right
// semantics at startup are fail-fast: configuring NATS is a promise, and not
// being able to connect at startup is an environment error that should kill
// the process so the deployer notices right away.
//
// Standard startup sequence (when several connections share one URL, probe it
// once; environment errors surface within about 5s instead of being stretched
// by N connections each waiting on their own):
//
//	natslink.MustProbe(url) // startup: environment error panics immediately, fail-fast
//	pool.MustStart()        // from here on an error can only be a configuration error (also fail-fast)
//	sub.MustStart()         // runtime disconnects are handled by the package's self-healing; it never gives up
//
// Credentials in the URL are redacted in the returned error, so it is safe to
// log.
func Probe(natsURL string) error {
	conn, err := nats.Connect(natsURL, nats.Name("natslink-probe"), nats.Timeout(dialTimeout), nats.DontRandomize())
	if err != nil {
		return fmt.Errorf("natslink: probe %s failed (check url / network / token): %w", redactURL(natsURL), redactError(natsURL, err))
	}
	conn.Close()
	return nil
}

// MustProbe is like Probe but panics on failure. Intended for process startup.
func MustProbe(natsURL string) {
	if err := Probe(natsURL); err != nil {
		panic(err)
	}
}

// MustProbeEach calls MustProbe on every URL. It is the standard startup
// fail-fast for the mirrored constructors (multiple endpoints): any endpoint
// with an environment error panics immediately. Configuring M endpoints means
// making M promises. When an element is a comma-separated list (ordered
// failover within the endpoint), the servers are tried in the written order
// and reaching any one of them counts as success (matching that endpoint's
// runtime semantics); note that the worst case is then 5s per server,
// accumulated serially, so the "surfaces within 5s" promise is per server.
func MustProbeEach(natsURLs ...string) {
	for _, u := range natsURLs {
		MustProbe(u)
	}
}

// redactURL replaces the credentials in a NATS URL (token or user:pass in the
// userinfo section) with ***.
// url.URL.Redacted() cannot be used here: it only masks the password, not the
// username, and NATS puts the token in the username position (and
// url.User("***") would be escaped to %2A), so this is plain string handling.
// Comma-separated multi-server URLs are supported; a malformed segment that
// might still contain credentials is masked as a whole. Showing less is always
// preferable to leaking.
func redactURL(raw string) string {
	parts := strings.Split(raw, ",")
	for i, p := range parts {
		p = strings.TrimSpace(p)
		at := strings.LastIndex(p, "@")
		if at < 0 {
			if len(parts) > 1 && strings.Contains(raw, "@") {
				parts[i] = "(redacted)" // a malformed comma list may have split credentials across segments; mask generously.
			} else {
				parts[i] = p
			}
			continue
		}
		if scheme := strings.Index(p, "://"); scheme >= 0 && scheme+3 <= at {
			parts[i] = p[:scheme+3] + "***" + p[at:]
		} else {
			parts[i] = "(redacted)"
		}
	}
	return strings.Join(parts, ",")
}

// Connection supervision parameters: fixed values tuned in production. Callers
// do not need to (and cannot) configure them.
const (
	dialTimeout      = 5 * time.Second        // per-dial timeout
	reconnectWait    = 500 * time.Millisecond // base automatic-reconnect interval (start of the exponential backoff, doubled each attempt)
	reconnectWaitMax = 5 * time.Second        // automatic-reconnect interval cap
	reconnectBufSize = 16 * 1024 * 1024       // pending-write buffer while reconnecting (publishing connections)

	// Stalled-connection detection: a heartbeat is sent every pingInterval and
	// the connection is declared dead after maxPingsOut consecutive unanswered
	// pings, so a stalled connection is detected within at most
	// pingInterval*(maxPingsOut+1) ~= 9s (the nats.go defaults take ~6 minutes).
	// A PING/PONG is a few bytes, so one every 3s is negligible; declaring death
	// requires two consecutive misses, so a single delayed or lost PONG (GC
	// pause, momentary congestion) does not kill a healthy connection.
	// Note: a clean disconnect (server restart / RST) is detected by a read
	// error within milliseconds and does not depend on the heartbeat; after a
	// stall is detected the connection enters RECONNECTING (recoverable),
	// subscriptions are restored automatically, and it never becomes
	// permanently CLOSED.
	pingInterval = 3 * time.Second
	maxPingsOut  = 2

	rebuildBackoff    = 2 * time.Second  // initial backoff of the self-healing rebuild after a permanent close (doubled each attempt)
	rebuildBackoffMax = 30 * time.Second // self-healing rebuild backoff cap

	// flusherTimeout is the deadline of a single socket write (overriding the
	// nats.go FlusherTimeout default of one minute). 60s is far too wide: the
	// synchronous writes done by the flusher and by publish hold the connection
	// mutex nc.mu for their whole duration, so when the peer stalls (zero
	// window / half-open / stopped reading, rather than a clean RST) a single
	// write pins the lock for a full minute. During that minute the ping-based
	// stall detection (processPingTimer also needs nc.mu) is completely blind
	// and the 9s promise above is void; Stop's FlushTimeout / Close queue on
	// the same lock. Publish targets are expected to be local or same-region
	// servers, and a link that cannot complete a write in 5s is already dead
	// for latency-sensitive traffic: declare it dead quickly, let the
	// reconnect handle it, and let in-flight traffic go out through the other
	// healthy connections in the pool (latency over bandwidth).
	flusherTimeout = 5 * time.Second

	// stopGrace is the shutdown grace period for a single connection's
	// "drain + close". The first thing FlushTimeout and Close do internally is
	// take nc.mu, and that lock may be held by a blocked write (up to
	// flusherTimeout) or by a reconnect handshake (doReconnect holds the lock
	// from dial to resend without releasing it). Against a stalled peer the
	// synchronous wait can reach minutes: a production shutdown hung for 36s+
	// and was SIGKILLed by the service manager's 40s budget, and in a local
	// reproduction with the server paused (docker pause) Stop took 117s. Once
	// the grace period is exceeded we stop waiting: the close continues in a
	// side goroutine and the caller moves on. The process is about to exit, so
	// leaking one connection that is still closing does not matter; a
	// deterministic shutdown matters more than a graceful goodbye.
	stopGrace = 5 * time.Second

	defaultReportInterval = time.Minute
	defaultWorkers        = 8
	defaultQueueSize      = 4096
)

// Options is the connection configuration shared by Subscriber and Publisher.
// Zero-valued fields use defaults.
type Options struct {
	URL   string // Required. Full NATS URL, e.g. nats://token@host:4222 (typically the nearest server or leaf node).
	Topic string // Required. The single topic this connection is bound to.
	Name  string // Client name (shown in server monitoring and logs); defaults to natslink-<role>-<topic>.

	Logger Logger // Log output; nil means slog.Default().

	// ReportInterval is the interval of the built-in status report: every
	// interval one Info line with statistics is logged, plus a Warn line when
	// not connected. Defaults to 1 minute; a negative value disables it, in
	// which case the caller aggregates Stats() itself.
	//
	// The supervision parameters (reconnect backoff, heartbeat, rebuild
	// backoff, ...) are package constants, see the top of natslink.go; they
	// need no configuration.
	ReportInterval time.Duration

	// ExtraNatsOptions are additional options appended to nats.Connect (e.g.
	// TLS configuration, a custom dialer). They are the escape hatch for
	// overriding the underlying policy parameters. Two caveats:
	//   - overriding MaxReconnects / RetryOnFailedConnect / CustomReconnectDelay /
	//     PingInterval / MaxPingsOutstanding removes the corresponding line of
	//     defence from the three-layer failure model; you are on your own;
	//   - the event callbacks (ClosedHandler / ErrorHandler / DisconnectErrHandler /
	//     ...) are injected after these options and cannot be overridden: the
	//     self-healing rebuild and the SlowConsumers observation are the core
	//     promise of this package and there is no way to remove them.
	ExtraNatsOptions []nats.Option
}

// normalize validates the required fields and fills in defaults. role is
// "sub" or "pub"; it is used for the default name and for topic validation.
func (o Options) normalize(role string) (Options, error) {
	if o.URL == "" {
		return o, errors.New("natslink: Options.URL is required")
	}
	if err := validateTopic(o.Topic, role); err != nil {
		return o, err
	}
	if o.Name == "" {
		o.Name = fmt.Sprintf("natslink-%s-%s", role, o.Topic)
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.ReportInterval == 0 {
		o.ReportInterval = defaultReportInterval
	}
	return o, nil
}

// validateTopic statically validates a subject at construction time (core NATS
// rules: tokens separated by ".", no empty token, no whitespace; the wildcards
// "*" and ">" are only legal on the subscribe side).
// It has to fail fast here because a Subscriber's invalid topic surfaces in
// the subscribe call made by Start, but a Publisher never subscribes: an
// invalid subject would pass all three startup checks (Probe / Start /
// WaitConnected) and only be rejected by the server with -ERR on the first
// Publish, possibly killing the connection and causing rebuild churn.
func validateTopic(topic, role string) error {
	if topic == "" {
		return errors.New("natslink: Options.Topic is required")
	}
	for _, tok := range strings.Split(topic, ".") {
		switch {
		case tok == "":
			return fmt.Errorf("natslink: invalid topic %q: empty token (leading/trailing/double dot)", topic)
		case strings.ContainsAny(tok, " \t\r\n"):
			return fmt.Errorf("natslink: invalid topic %q: whitespace in token", topic)
		case role == "pub" && (tok == "*" || tok == ">"):
			return fmt.Errorf("natslink: invalid topic %q: wildcard is subscribe-only, cannot publish to it", topic)
		}
	}
	return nil
}

// Stats is a point-in-time snapshot of status and data statistics. All
// counters accumulate across self-healing rebuilds. At the instant of a
// rebuild switch-over a counter may briefly appear to go backwards (the new
// connection has been swapped in but the old connection's statistics have not
// been absorbed yet); when computing deltas for monitoring, clamp negative
// increments to zero.
type Stats struct {
	Name      string // Connection name (Options.Name; pool members carry a -<i> suffix, mirror endpoints a -e<i> suffix). Used to attribute a degraded connection.
	Running   bool   // true between Start and Stop.
	Connected bool   // Whether currently connected.
	Status    string // Underlying connection status (CONNECTED / RECONNECTING / ...); "no-conn" when there is no connection.
	Server    string // Name of the currently connected server.
	Cluster   string // Name of the currently connected cluster.

	Reconnects uint64 // Successful client automatic reconnects (RECONNECTING -> CONNECTED).
	Rebuilds   uint64 // Whole-connection self-healing rebuilds after a "permanent close" (excluding the initial connect).
	InMsgs     uint64 // Messages received.
	OutMsgs    uint64 // Messages sent.
	InBytes    uint64 // Bytes received.
	OutBytes   uint64 // Bytes sent.

	// Dropped is, for a Publisher, the number of failed publishes (rebuild
	// window / buffer overflow); for a Subscriber, the number of messages
	// dropped by back-pressure because the worker queue was full. Only the
	// worker-pool mode increments it; the synchronous and go-per-message modes
	// keep it at 0 (in synchronous mode the data-loss signal is SlowConsumers;
	// go-per-message never drops and has no back-pressure signal either, see
	// the trade-off notes on SubscriberOptions.Workers).
	// Note: inside a PublisherPool, a single connection's Dropped counts
	// "attempts that failed on this connection"; the message may well have
	// been failed over to another connection and sent. To monitor the pool's
	// real losses use PublisherPool.Dropped(); summing the members' Dropped
	// grossly overestimates loss (false alarms).
	Dropped uint64
	// Paused reports whether the subscriber is paused (after Subscriber.Pause,
	// before Resume): the connection is up but the subscription is gone, so
	// the server delivers nothing to this connection. Always false for a
	// Publisher.
	Paused bool
	// Pauses is the cumulative number of successful Pause transitions on the
	// subscriber (idempotent repeated calls are not counted). Always 0 for a
	// Publisher.
	Pauses uint64
	// Diverged is the number of subscriber endpoints whose subscription state
	// disagrees with the paused state (v1.5.1): not paused but no subscription
	// on the connection (SUB failed during Resume), or paused but the
	// subscription is still there (UNSUB failed during Pause). Only endpoints
	// whose connection is alive are counted (an endpoint inside its rebuild
	// window is handled by rebuild; look at Connected instead). While it is
	// >0 the package's reconcile goroutine is retrying with backoff; if it
	// stays >0 the reconcile is not succeeding, see the "[natslink]
	// subscription reconcile failed" log line. Always 0 for a Publisher.
	Diverged int
	// Gated is the number of messages the subscriber dropped because the gate
	// (SubscriberOptions.Gate returned false) rejected them. The decision is
	// made at the delivery entry point, before the queue and the callback, so
	// all three consumption modes count it; always 0 when no Gate is set.
	// Under a mirrored subscriber it is, like Dropped, a single value shared
	// across endpoints and likewise counted per copy.
	Gated uint64
	// SlowConsumers is the number of times the subscriber was too slow and the
	// NATS client discarded messages (ErrSlowConsumer). The connection still
	// looks healthy at that point, so this counter is the key signal for
	// detecting "silent data loss" (in synchronous mode it is the only
	// signal). It counts events, not messages, and must not be added to
	// Dropped as a total loss figure.
	SlowConsumers uint64

	QueueLen int // Current length of the Subscriber worker queue (always 0 in synchronous / go-per-message mode).
	QueueCap int // Capacity of the Subscriber worker queue (always 0 in synchronous / go-per-message mode).
}

func prettySize(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

# natslink

[![CI](https://github.com/Jolly23/natslink/actions/workflows/ci.yml/badge.svg)](https://github.com/Jolly23/natslink/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/Jolly23/natslink.svg)](https://pkg.go.dev/github.com/Jolly23/natslink)
[![Go Report Card](https://goreportcard.com/badge/github.com/Jolly23/natslink)](https://goreportcard.com/report/github.com/Jolly23/natslink)

Self-supervising NATS connections for Go: **one connection, one subject, zero babysitting.**

`natslink` wraps [nats.go](https://github.com/nats-io/nats.go) in small units that
never give up. Each unit owns a single connection bound to a single subject and
takes care of dialing, exponential-backoff reconnects, rebuilding the connection
after a permanent close, re-subscribing, bounded back-pressure, and periodic
status reporting. You write the handler; the library keeps the pipe alive.

- **`Subscriber`** - one connection subscribing to one subject. Three dispatch
  modes (bounded worker pool, synchronous, goroutine-per-message), an optional
  `Gate` to drop messages at the door, and `Pause` / `Resume` to withdraw
  interest without dropping the connection.
- **`Publisher`** - one connection publishing to one subject. Same supervision;
  a failed publish returns an error for the caller to handle.
- **`PublisherPool`** - N publishers with round-robin and health-first failover.
  Traffic flows through the healthy members while one is reconnecting.
- **Mirroring** - `NewMirroredPublisherPool` / `NewMirroredSubscriber` fan the
  same subject out to M *independent* NATS endpoints. Publishes go to every
  endpoint; subscribers receive every copy. The return types are the plain
  `*PublisherPool` / `*Subscriber`, so the rest of your code does not change.

Only two dependencies: `nats.go` and the standard library.

## Install

```bash
go get github.com/Jolly23/natslink@latest
```

Requires Go 1.25 or newer.

## Mental model (read this first)

1. **One connection = one subject = one self-supervising unit.** Need more
   subjects? Create more units. Failures stay isolated.
2. **Failures fall into four classes, each with one owner:**

   | Phase   | Class                                                  | Surface                         | What you do                            |
   |---------|--------------------------------------------------------|---------------------------------|----------------------------------------|
   | Startup | **Environment** (bad address, no route, bad credentials) | `Probe` returns an error        | `MustProbe(url)` - panic, fail fast     |
   | Startup | **Configuration** (malformed URL or subject)           | `New*` / `Start` return an error | `MustStart()` - panic, fail fast        |
   | Runtime | **Ordinary disconnect** (server restart, flaky network, stalled peer) | automatic reconnect | nothing; it never gives up |
   | Runtime | **Permanent close** (fatal protocol error from the server) | automatic rebuild           | nothing; it never gives up             |

   Startup errors must crash you. Runtime failures must never bother you.
   *A successful start is a promise: everything is ready, and the unit will
   never stop trying.*
3. **Instances are single-use.** After `Stop()` create a new one.
4. **Supervision parameters are not configurable.** Dial timeout, reconnect and
   rebuild backoff, heartbeat probing and write deadlines are package constants
   tuned in production (see the top of `natslink.go`). Exposing them invites
   every project to tune its own broken set.
5. **Back-pressure is always visible.** When a handler cannot keep up, messages
   are dropped in a bounded way and counted (`Dropped`, `SlowConsumers`); nothing
   queues without limit. The one exception is the explicit `Workers: GoPerMessage`
   subscriber mode (unbounded concurrency, no back-pressure signal), which you
   choose only when you know the traffic and must not lose messages.

## Five-minute start

```go
import (
    "log/slog"

    "github.com/Jolly23/natslink"
    "github.com/nats-io/nats.go"
)

url := "nats://token@127.0.0.1:4222"

// 1. Fail fast on environment errors (<= 5s). One probe per URL per process.
natslink.MustProbe(url)

// 2. Subscribe: one connection, one subject.
sub, err := natslink.NewSubscriber(natslink.SubscriberOptions{
    Options: natslink.Options{
        URL:    url,
        Topic:  "events.tx",
        Name:   "mynode-sub-tx",   // default natslink-sub-<topic>; shows up in server monitoring and logs
        Logger: slog.Default(),    // anything with Info/Warn/Error(msg string, kv ...any)
    },
    Handler: func(m *nats.Msg) { /* called concurrently by default, see "Subscribing" */ },
})
if err != nil {
    panic(err) // configuration error (bad subject, ...), fail fast
}
sub.MustStart()
defer sub.Stop() // blocks until in-flight callbacks return

// 3. Publish through a pool of N connections with health-first failover.
pool, err := natslink.NewPublisherPool(4, natslink.PublisherOptions{
    Options:    natslink.Options{URL: url, Topic: "events.block", Name: "mynode-pub-block"},
    MaxPayload: 16 << 20, // application-level size cap, rejected with ErrOversized; 0 = unlimited
})
if err != nil {
    panic(err)
}
pool.MustStart()
defer pool.Stop()

if err := pool.Publish(payload); err != nil {
    // only when every member failed (already counted in pool.Dropped())
}
```

The three-line startup discipline: **`MustProbe`** (environment) → **`New*`**
(configuration) → **`MustStart`**. Note that `MustStart` succeeding does *not*
mean the connection is up (see the failure model); the fail-fast guarantee comes
from `MustProbe`.

## Failure model

**At startup, `MustProbe` is the only correct check.** `Start` / `MustStart`
enable `RetryOnFailedConnect`, so an unreachable server or a bad token is *not*
a `Start` error: the connection is created and retried in the background
forever. That is exactly what you want at runtime, but it means `MustStart`
cannot catch environment mistakes. So begin your process with:

```go
natslink.MustProbe(url) // one throw-away connection checks reachability and auth; panics within 5s (credentials redacted)
```

One probe per URL; afterwards start as many connections as you like. Do not use
"`WaitConnected` + panic on every connection" as a startup check: with many
connections each one waits out its own timeout, and a bad token also waits the
full timeout because auth errors are deliberately treated as recoverable (see
below). `WaitConnected(timeout)` stays available as a readiness helper for tests
or for "must be online before the first message" situations.

**At runtime there are two layers, both retrying forever:**

1. **Ordinary disconnect** (server restart, network blip, stalled peer caught by
   heartbeats) → state `RECONNECTING`, exponential backoff from 500 ms doubling
   to a 5 s cap with +50 % jitter. Subscriptions are restored automatically and
   the publisher's 16 MB reconnect buffer is flushed. A server restart of ten or
   twenty seconds is fully absorbed here.
2. **Permanent close** (a fatal protocol error puts nats.go into `CLOSED`, which
   layer 1 cannot recover from) → the whole connection is rebuilt: redial,
   re-subscribe, atomically swap in. Backoff from 2 s doubling to a 30 s cap.

**How fast a dead connection is noticed:** clean breaks (restart, kill, RST) are
seen by the read loop within milliseconds. A silently dead connection (NAT or
firewall dropped the mapping) is caught by a 3 s heartbeat with two consecutive
misses, so about **9 s worst case** (nats.go defaults take ~6 minutes). A single
late PONG never kills a healthy connection.

**Three defences against "the client silently gave up after a server restart":**

1. nats.go stops after `MaxReconnects=60` by default (roughly two minutes of
   outage) → `MaxReconnects(-1)`, retry forever.
2. During a server's warm-up it may answer with auth errors, and nats.go gives
   up permanently after the same auth error twice → `IgnoreAuthErrorAbort()`
   (covered by an integration test).
3. A fatal protocol error (an unknown `-ERR`) goes straight to `CLOSED` and
   neither of the above helps → the `ClosedHandler` triggers a full rebuild.

Even an unknown failure mode is never silent: a status line is logged every
minute (`ReportInterval`; negative disables it) and a `degraded` warning is added
while disconnected.

## Subscribing

Dispatch mode is chosen with `SubscriberOptions.Workers`:

| Setting                          | Behaviour                                                     | Use when |
|----------------------------------|---------------------------------------------------------------|----------|
| default (`0`)                    | 8 worker goroutines + bounded queue (`QueueSize`, default 4096) | The normal case. Concurrency is capped; when the queue is full new messages are dropped and counted (`Dropped`) instead of growing goroutines or memory. |
| `Workers: N`                     | N worker goroutines                                           | Tune per subject and handler cost. `Workers: 1` gives strict ordering without blocking delivery. |
| `Workers: natslink.SyncMode`     | Handler runs on the nats.go delivery goroutine                 | **Lowest latency** (no queue hand-off). The handler must not block: locks, I/O or millisecond work belong in a worker pool. Backlog is bounded by the client pending buffer (default 512k msgs / 64 MB); overflow is counted in `SlowConsumers`. |
| `Workers: natslink.GoPerMessage` | One new goroutine per message, **unbounded** concurrency       | Mixed traffic where most messages are light, a few carry heavy synchronous work, and *none may be dropped*. A bounded pool would let the heavy ones occupy every worker and head-of-line block the light ones, then drop during bursts; when the real throughput cap is a downstream lock, extra goroutines merely queue on that lock. **Cost: no back-pressure signal at all** (`Dropped` is always 0, `SlowConsumers` almost never fires); goroutine count floats with load. Only pick this when you understand both the producer rate and the consumer capacity. |

**Handler concurrency contract** (read before writing a handler):

- Worker-pool mode calls the handler from up to `Workers` goroutines
  **concurrently**, and processing order is **not** guaranteed to match publish
  order (workers compete for one queue). Guard shared state yourself; use
  `Workers: 1` or `SyncMode` when strict ordering matters.
- Sync mode is serial and ordered, but blocks the delivery goroutine.
- Goroutine-per-message mode calls the handler with **unbounded** concurrency
  and no ordering, and `Stop()` does **not** wait for those callbacks (unlike
  the worker-pool guarantee). Release handler dependencies only after you have
  confirmed the callbacks are done.

**`QueueGroup`**: unset means broadcast, every subscriber receives everything.
Set it and members of the same group **compete**: each message goes to exactly
one of them. Multiple replicas that each need the full stream must leave it
unset; replicas that share the load must set it. Getting this wrong in either
direction produces no error, only wrong behaviour.

**`Gate`** (v1.4.0): an optional `func() bool` evaluated at the delivery entry
point for every message. `true` delivers to the handler; `false` drops the
message on the spot and counts it in `Stats.Gated`. `nil` always passes. The
check happens *before* enqueueing or spawning in all three modes. The gate runs
on the nats.go delivery goroutine (M of them concurrently under mirroring), so
it must be concurrency-safe and very cheap (an atomic load); a slow gate blocks
delivery and triggers `SlowConsumers`. Typical use: hold all processing while
the host is not ready yet, then let it flow without rebuilding the subscription.
In worker-pool mode the gate is evaluated at enqueue time, so a message already
queued when the gate closes is still delivered; re-check inside the handler if
you need exact timing.

**`Pause` / `Resume`** (v1.5.0): withdraw and restore the subscription at
runtime **without dropping the connection**. `Pause()` sends UNSUB on every
endpoint's current connection: the server stops delivering, and interest is
withdrawn hop by hop across leaf nodes and routes, so upstream servers stop
forwarding that subject to you at all (real bandwidth saved; a `Gate` drops at
the door but the bytes still arrive). Connection, heartbeats, reconnect and
rebuild all keep running; `Stats.Connected` is unchanged, `Stats.Paused` is true
and `Stats.Pauses` increments. `Resume()` re-SUBs on each endpoint's most recent
successfully set-up connection; messages return to the same handler and worker
pool. Both are idempotent and return `ErrStopped` after `Stop`. A rebuild during
a pause swaps in a new connection without subscribing; nats.go's own reconnect
only replays subscriptions still on record, so an UNSUBed one never comes back
by accident. Neither path needs action from the caller. Edge: messages already
in flight before UNSUB, or already queued in the worker pool, are still
delivered (same window as `Gate`). `Unsubscribe` / `Subscribe` take the
underlying connection lock and can block up to the write deadline (5 s) against
a stalled peer, so do not call them synchronously on a latency-critical path.

**Partial-failure self-healing** (v1.5.1): if `Resume` fails to SUB on some
endpoint (or `Pause` fails to UNSUB), the paused state still flips, the errors
are returned joined, and a single in-package healer goroutine retries the
failed endpoints with the rebuild backoff (2 s doubling to 30 s), aligning each
round to the paused state *at that moment*, and exits once everything matches
or `Stop` is called. The caller never retries. Logs:
`[natslink] subscribe retried after failed resume` /
`unsubscribe retried after failed pause` on success and
`subscription reconcile failed, retrying` (Warn) per failed endpoint per round;
`Stats.Diverged > 0` while unconverged. The healer holds the internal
subscription lock across SUB / UNSUB like `Pause` / `Resume` and rebuild do, so
against a stalled peer each endpoint can block up to 5 s (N endpoints → N × 5 s)
during which the host's `Pause` / `Resume`, rebuild and `Stats()` queue behind
it. With nats.go 1.53 a SUB / UNSUB on a live connection only fails while the
connection is draining (after which it closes and rebuild takes over), so this
is a correctness backstop rather than a hot path.

Subjects pass straight through to NATS, so subscriptions may use wildcards
(`*` / `>`); one connection still binds exactly one subject expression. Client
buffer limits (`PendingMsgLimit` / `PendingBytesLimit`) are documented on
`SubscriberOptions` and rarely need changing in worker-pool mode.

## Publishing

```go
pub, err := natslink.NewPublisher(natslink.PublisherOptions{
    Options:    natslink.Options{URL: url, Topic: "events.tx", Name: "mynode-pub"},
    MaxPayload: 4 * 1024,
})
if err != nil {
    panic(err)
}
pub.MustStart()
defer pub.Stop()
```

`Publish` semantics (safe for concurrent use):

- **`nil` = the message was accepted by the connection**, not delivered. When
  connected it is written immediately; while `RECONNECTING` it goes into the
  16 MB reconnect buffer and is flushed after reconnect.
- **Replay is best effort** (core NATS has no delivery guarantee). If the
  connection goes permanently `CLOSED` while buffering, the old buffer is lost
  with it; a replay may also arrive before the peer has re-subscribed. Build
  acknowledgements at the application layer if you need delivery guarantees.
- **error = the message was not accepted** (counted in `Stats.Dropped`):
  `ErrStopped` (instance used up) / `ErrNotRunning` (not started) /
  `ErrOversized` (over `MaxPayload`) / `ErrNoConn` (rebuild window, match with
  `errors.Is` to fail over) / any underlying error such as
  `nats.ErrReconnectBufExceeded`.
- `data` is copied before `Publish` returns; reuse your buffer immediately.
- `Stop()` tries to drain the buffer (1 s timeout). While disconnected the
  drain necessarily fails and buffered messages are dropped (Warn log with the
  byte count).
- **Endpoint pinning** (v1.3.2): connections ignore cluster gossip
  (`IgnoreDiscoveredServers`). The server pool contains only the URLs you
  configured, so when your endpoint goes down the client keeps reconnecting to
  *that* endpoint and never drifts to another cluster member (a production
  client once wandered off to a remote node and never came back). This is a
  deliberate regional lock; for multiple endpoints use mirroring or an explicit
  comma-separated list.
- **Bounded shutdown** (v1.3.1): `Stop()` blocks at most `stopGrace` (5 s) per
  connection. Drain and close run on a side goroutine, so a stalled peer (zero
  window or half-open socket holding the write path's connection lock) cannot
  hang your shutdown; the close continues in the background.
  `PublisherPool.Stop` and mirrored `Subscriber.Stop` stop members in parallel,
  so pool shutdown costs max, not sum. The normal path stays in the millisecond
  range. Background: a production shutdown once hung until systemd sent SIGKILL
  because nats.go's `FlushTimeout(1s)` only bounds the wait for PONG, not the
  wait for the connection lock, and the lock was held by a write blocked on a
  60 s deadline. The same release cut the per-write socket deadline to 5 s
  (`flusherTimeout`, nats.go default 60 s), which brings the worst-case recovery
  from "a wedged write blinds the heartbeat" down from minutes to roughly
  10-30 s under write pressure (an idle connection stays at ~9 s). While wedged,
  the flusher discards one aggregated batch of **already accepted** messages
  every ~5 s (their `Publish` returned `nil`); this only shows up in async error
  logs, not in `Stats.Dropped`, so pool-level `Dropped` underestimates loss in
  that window.

**Large-message checklist** (go through it before sending anything over 1 MB):

1. The server's `max_payload` must be ≥ your `MaxPayload`. The NATS server
   default is **1 MB**; oversized messages are rejected or the connection is
   dropped. `Probe` checks reachability and auth only, **not** payload limits, so
   this mistake looks like a green startup followed by every publish failing.
2. The reconnect buffer is 16 MB: during an outage two 8 MB messages fit, the
   third gets `ErrReconnectBufExceeded`. Do not rely on the buffer for large
   messages over long outages. The 5 s write deadline also puts a floor on
   replay bandwidth: the pending backlog (up to 16 MB) is written as a single
   `Write` under a single 5 s deadline, so the link must sustain backlog/5 s
   (16 MB → ~3.2 MB/s ≈ 26 Mbit/s) or the whole batch is silently dropped and
   the client reconnects again. Fine for a local or same-region server;
   publishers on constrained cross-region links must either accept best-effort
   replay or size the backlog to "5 s × link throughput".

## Publisher pool (multiple connections + failover)

```go
pool, err := natslink.NewPublisherPool(6, natslink.PublisherOptions{
    Options:    natslink.Options{URL: url, Topic: "events.tx", Name: "ingest-tx"},
    MaxPayload: 4 * 1024,
})
if err != nil {
    panic(err)
}
pool.MustStart()
defer pool.Stop()

_ = pool.Publish(payload)                 // routing semantics below
_ = pool.WaitConnected(10 * time.Second)  // wait for all N members (optional; tests / readiness gates)
n := pool.ConnectedCount()                // healthy members right now
d := pool.Dropped()                       // real pool-level loss (see "Statistics")
```

**Routing (latency first):** starting at the round-robin cursor, `Publish`
makes two passes. The first pass tries only members that are **currently
connected**, so a message leaves on a healthy connection immediately instead of
sitting in a disconnected member's reconnect buffer. Only when no member is
healthy does the second pass fall back to "accepted into the reconnect buffer
counts as success". Any member accepting returns `nil`; all failing returns an
error and increments the pool-level `Dropped()`.

## Mirroring (active-active across independent NATS endpoints)

`NewMirroredPublisherPool` / `NewMirroredSubscriber` mirror one subject across
M **mutually independent** NATS endpoints: each publish goes to every endpoint,
each subscriber subscribes on every endpoint and the handler receives every
copy. **Mirroring is only a constructor.** The returned types are the plain
`*PublisherPool` / `*Subscriber`; every method behaves the same and the host
code changes on exactly one line.

Typical setup: one machine runs two local leaf nodes that connect upstream to
different regions (two trees that never meet). Publishing races both entry
points; subscribing takes the minimum of both paths. **Deduplication always
belongs to the consumer** (hash LRU, monotonic sequence numbers, a pool that
rejects duplicates); the mirror never deduplicates.

```go
urls := []string{"nats://tk@127.0.0.1:4222", "nats://tk@127.0.0.1:4223"}
natslink.MustProbeEach(urls...) // fail fast per endpoint: M endpoints are M promises

pool, err := natslink.NewMirroredPublisherPool(urls, 4, natslink.PublisherOptions{
    Options: natslink.Options{Topic: "events.tx", Name: "edge-pub-tx", Logger: slog.Default()},
})
if err != nil {
    panic(err)
}
pool.MustStart()
defer pool.Stop()
_ = pool.Publish(payload) // sent once per endpoint; nil if any endpoint accepted

sub, err := natslink.NewMirroredSubscriber(urls, natslink.SubscriberOptions{
    Options: natslink.Options{Topic: "events.block", Name: "edge-sub-block", Logger: slog.Default()},
    Handler: onBlock, // receives every copy from every endpoint; deduplicate here
})
```

Semantics at a glance:

- **Not the same as a comma-separated URL.** A comma list in `Options.URL` is
  **ordered failover within one endpoint**: only one server is connected at a
  time, in the order written (natslink disables nats.go's default server-pool
  shuffle), and the next one is tried after a disconnect. nats.go stays on
  whichever server it lands on and never fails back, so this is not full
  active/standby; the first entry is merely the preferred one for the initial
  dial and each reconnect round. The mirror `urls` are **active-active across
  endpoints**: all connected, all used. The two compose: each element of `urls`
  may itself be a comma list ("ordered failover inside an endpoint, mirroring
  across endpoints"). `MustProbeEach` on a comma-list endpoint can take up to
  5 s per listed server, serially.
- **Publish** runs the two-pass routing per endpoint and returns `nil` as soon
  as **any** endpoint accepts. Mirroring is redundancy: one endpoint failing
  just means one fewer path, not a loss. Only when every endpoint fails is an
  error returned and the pool-level `Dropped()` incremented.
- **Subscribers share one worker pool and one set of counters.** `Workers` is
  the **total** concurrency cap across endpoints and `QueueSize` is one shared
  queue. Ordering guarantees from `Workers: 1` / `SyncMode` hold only **within
  one endpoint**; copies from different endpoints interleave. `QueueGroup`
  applies per endpoint.
- **Observing endpoint degradation:** connection names carry an `-e<i>` suffix
  so server-side monitoring can attribute them. Pool `Stats()` flattens all
  members, so one endpoint being down shows as a block of members with
  `Connected=false`. A mirrored subscriber's merged `Stats().Connected` uses an
  **all-endpoints threshold**: any endpoint down → `false`, so your existing
  "degraded" alert covers it with no extra monitoring.
- **Mirroring × `SyncMode` warning:** sync mode's "serial and ordered" promise
  does not survive mirroring. M endpoints have M delivery goroutines, so the
  handler is called **concurrently**. A handler relying on sync mode to avoid
  locks is a data race under mirroring (there is a test proving it).
- **Mirrored `Dropped` counts copies.** Each message occupies M queue slots, so
  under overload `Dropped` is inflated by roughly M. Content is lost only when
  **all** copies of a message are dropped; divide by M before judging.
- **Each endpoint heals independently and knows nothing of the others.** One
  endpoint down for a long time = the race degrades to a single path (no loss,
  half the benefit).
- **Topology discipline (learned the hard way):** mirroring only means something
  when the endpoints really are two independent trees. Two local leaf nodes must
  have *different* `server_name`s; with the same name the upstream treats them
  as the same origin cluster and deduplicates or loop-protects, collapsing your
  two copies into one or black-holing both. Check `max_payload` hop by hop on
  each endpoint's path as well.

## Statistics

`Stats()` can be called concurrently at any time. Counters accumulate across
rebuilds (a brief dip may be observed at the instant of a rebuild swap; clamp
negative deltas to zero in rate monitors).

| Field | Meaning |
|-------|---------|
| `Name` | Connection name (pool members get `-<i>`, mirror endpoints `-e<i>`): the attribution key for a degraded connection |
| `Status` / `Connected` / `Server` / `Cluster` | Current connection state |
| `Reconnects` | Successful automatic reconnects |
| `Rebuilds` | Full connection rebuilds after a permanent close |
| `InMsgs` / `OutMsgs` / `InBytes` / `OutBytes` | Traffic counters |
| `Dropped` | pub: failed publishes; sub: worker-queue overflow drops (**worker-pool mode only**; always 0 in sync and goroutine-per-message modes) |
| `Gated` | sub: messages dropped by the `Gate` at the delivery entry point (all modes; 0 without a gate; counts copies under mirroring) |
| `Paused` / `Pauses` | sub: whether currently paused (connection up, subscription withdrawn) / number of successful pause transitions (idempotent repeats not counted). pub: always `false` / 0 |
| `Diverged` | sub: endpoints whose connection is alive but whose subscription state disagrees with the paused state (v1.5.1). `> 0` = the healer is retrying; persistently `> 0` → look for `subscription reconcile failed`. Endpoints inside a rebuild window are not counted. pub: always 0 |
| `SlowConsumers` | Number of slow-consumer **events** in which the client dropped messages (the only loss signal in sync mode; events, not messages, so **do not add to `Dropped`**) |
| `QueueLen` / `QueueCap` | Worker-queue level (0 in sync and goroutine-per-message modes) |

**Wiring monitoring:** subscriber loss = `Dropped` (worker pool) **plus**
`SlowConsumers` (sync); goroutine-per-message subscribers do not drop and have
**no back-pressure signal**, so watch process-level goroutine count and RSS
instead. Publisher-pool loss is **`pool.Dropped()` only**: a single member's
`Stats.Dropped` includes failover intermediate failures (the message may have
left on another member), so summing members produces false alarms.

## Logging

Every connection logs with the `[natslink]` prefix through the `Logger` you
pass in `Options` (any type with `Info` / `Warn` / `Error(msg string, kv ...any)`;
`*slog.Logger` and go-ethereum's `log.Logger` both qualify, the default is
`slog.Default()`). Events: `starting` (with `version`), `connected`,
`reconnected`, `disconnected`, `permanently closed, rebuilding`,
`connection rebuilt`, periodic `status`, and `degraded` while disconnected, plus
the Pause / Resume healer lines listed above. Errors never contain credentials:
the configured URL's userinfo is redacted everywhere it could leak, including
URLs re-rendered by `net/url` on parse failures and their `%q`-escaped and
percent-encoded forms. Errors keep `errors.Is` classification but do not expose
an `Unwrap` chain to the raw sensitive error; errors you obtain through the
`Conn()` escape hatch are your own responsibility.

## Notes

- Instances are single-use: after `Stop()` create a new one (further calls
  return `ErrStopped`). `Pause` / `Resume` are not `Stop`: they only withdraw or
  restore the subscription while the connection stays up.
- `Subscriber.Stop()` blocks until in-flight callbacks return, so handler
  dependencies may be released afterwards; a handler that blocks forever hangs
  `Stop`. Queued messages not yet started are discarded. This wait applies to
  worker-pool mode only (`SyncMode` / `GoPerMessage` callbacks are not awaited).
- `Publish`, `pool.Publish` and `Stats` are safe for concurrent use.
- `ExtraNatsOptions` is the escape hatch for policy options (TLS, custom
  dialer). Event handlers (rebuild, slow-consumer observation) are installed
  *after* your options and cannot be overridden. Overriding `MaxReconnects`,
  `RetryOnFailedConnect` and friends removes the corresponding defence; you own
  the consequences.
- `Conn()` returns the underlying `*nats.Conn` for advanced use such as
  Request-Reply. It is outside the supervision contract and may be `nil` during
  a rebuild window.
- Each `Subscriber` has its own worker pool; with many subjects, lower
  `Workers` accordingly.

## Testing

Unit tests run without any server:

```bash
go test -short -race ./...
```

The integration suite needs two local NATS servers and the `docker` CLI (the
restart and pause drills operate on the container that publishes the test
port). `make itest` starts them, runs everything and tears them down; see
[CONTRIBUTING.md](CONTRIBUTING.md) for the manual commands and the environment
variables (`NATS_TEST_URL`, `NATS_TEST_URL2`, `NATS_TEST_CONTAINER`).

## Versioning

Releases follow [semantic versioning](https://semver.org/) with git tags
(`v1.6.0`). The `natslink.Version` constant mirrors the tag and is logged by
every connection at start (`[natslink] starting version=...`), so operators can
see which version each process runs. See [CHANGELOG.md](CHANGELOG.md) for the
history and [CONTRIBUTING.md](CONTRIBUTING.md) for the release procedure.

## License

[MIT](LICENSE)

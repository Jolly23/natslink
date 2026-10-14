# Changelog

All notable changes to this module are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.8.0] - 2026-10-14

### Added
- `DedupSet` (`NewDedupSet(entries, window)`) and `SubscriberOptions.Dedup` /
  `DedupKey`: opt-in, allocation-free deduplication of byte-identical payloads
  at the delivery entry point (after `Gate`, before the queue). A general
  byte-level filter for any subscriber: one set per subscriber, one shared by
  several (identical payloads on different subjects then collapse too), one
  for a whole process, or `Seen` called directly on any path; the typical case
  is a mirrored subscription delivering each message once (first copy wins).
  FIFO set of 64-bit `hash/maphash` values with an optional age bound. New
  `Stats.Deduped` counter and `deduped=` field in the status line.
- `NewMirroredPublisher(urls, opts)`: one publishing connection per endpoint,
  at most 5 endpoints, returned as a `*PublisherPool`.
- `ErrQueueFull`, returned by a mirrored pool when no endpoint queue could
  take the message.

### Changed
- Mirrored publishing is now parallel: a `PublisherPool` with two or more
  endpoints gives each endpoint its own bounded send queue (4096 messages,
  16 MB) and goroutine. `Publish` copies the message once, hands it to every
  queue without blocking and returns `nil` as soon as any queue accepts; a
  stalled endpoint backs up and, when full, drops only its own copies. Send
  failures surface in the members' `Stats`, a backlog in their `QueueLen` /
  `QueueCap`, and the pool-level `Dropped` (every endpoint failed) is settled
  asynchronously. On `Stop` each send goroutine flushes its queue before
  exiting; the send lines are waited for in parallel, bounded by one stop
  grace in total. Single-endpoint pools keep the synchronous `Publish`.
- Every mirrored constructor given a single URL degrades to its plain
  counterpart: `NewMirroredPublisher` → `NewPublisherPool(1, opts)`,
  `NewMirroredPublisherPool` → `NewPublisherPool(n, opts)`,
  `NewMirroredSubscriber` → `NewSubscriber` (plain naming, no `-e0`).
- README: mirroring no longer says deduplication always belongs to the
  consumer; byte-identical copies can be collapsed in the library, content-level
  deduplication still cannot.

## [1.7.0] - 2026-10-14 [YANKED]

Tagged and withdrawn the same day; its content ships in 1.8.0 together with
the parallel mirrored publisher. Do not depend on this version.

## [1.6.1] - 2026-10-07

No library code change; CI and release-process maintenance.

### Changed
- CI runs with `GOTOOLCHAIN=local`, so the Go 1.25 matrix job fails for real
  when the `go` directive is raised instead of silently downloading a newer
  toolchain.
- GitHub Actions bumped: `actions/checkout` v7, `actions/setup-go` v7,
  `softprops/action-gh-release` v3.
- Dependabot ignores nats.go minor and major updates: nats.go 1.54 requires
  Go 1.26 and this module stays on Go 1.25 for now.
- `CONTRIBUTING.md`: the release procedure goes through a pull request.

## [1.6.0] - 2026-10-06

First public release as a standalone Go module at
`github.com/Jolly23/natslink`. No API or behaviour change over 1.5.2.

### Added
- GitHub Actions: formatting, vet, unit tests with the race detector, a full
  integration run against two NATS servers, `govulncheck`, and a tag-driven
  release workflow that checks the tag against `natslink.Version`.
- `CONTRIBUTING.md`, `Makefile` targets for local unit and integration runs.

## [1.5.2] - 2026-10-05

### Fixed
- Credential redaction could be bypassed when `net/url` failed to parse a URL
  whose userinfo contained double quotes, backslashes, control characters or
  non-ASCII: `*url.Error` re-renders the raw URL with `%q`, so the escaped form
  no longer matched the literal replacement and the credential survived in the
  wrapped error. Errors carrying a `*url.Error` are now re-rendered from a
  redacted copy (`Op` and the underlying classification preserved; `errors.As`
  yields the redacted copy), and the textual fallback also masks the quoted
  (`%q` / `%+q`), percent-encoded and split user / password forms, with the
  URL regex stopping only at whitespace. Fragments of `invalid URL escape`
  errors are replaced wholesale. Redaction material is cached per URL, which
  also makes the failed-publish error path cheaper.

## [1.5.1] - 2026-10-05

### Fixed
- `Resume` that failed to subscribe on some endpoints (or `Pause` that failed
  to unsubscribe) left those endpoints permanently out of sync when the
  connection stayed healthy and no rebuild happened. Both now flip the paused
  state, return the joined errors, and hand the failed endpoints to a single
  healer goroutine that retries with the rebuild backoff until every live
  endpoint matches the paused state or `Stop` is called.

### Added
- `Stats.Diverged`: number of live endpoints whose subscription state disagrees
  with the paused state.
- Logs `[natslink] subscribe retried after failed resume`,
  `unsubscribe retried after failed pause`,
  `subscription reconcile failed, retrying`.

## [1.5.0] - 2026-09-24

### Added
- `Subscriber.Pause()` / `Resume()` / `IsPaused()` and `Stats.Paused` /
  `Stats.Pauses`: withdraw and restore the subscription at runtime without
  dropping the connection, so interest is removed upstream and no bandwidth is
  spent on a stream the host does not need for a while. A rebuild during a
  pause does not subscribe; nats.go's reconnect never revives an UNSUBed
  subscription.

## [1.4.1] - 2026-09-04

### Fixed
- Error redaction now covers every surface: `Probe`, `Start`, connection
  events, `WaitConnected` and `Publish`, including URLs re-rendered by the
  underlying parser on invalid ports or percent sequences, and encoded /
  decoded tokens. Redacted errors keep `errors.Is` classification but no longer
  expose an `Unwrap` chain to the raw error.

### Changed
- Dependencies pinned to nats.go 1.53.1 (with compress 1.20.0, x/crypto
  0.55.0) while keeping the `go` directive at 1.25.

## [1.4.0] - 2026-08-25

### Added
- `SubscriberOptions.Gate func() bool`: a per-message admission check at the
  delivery entry point, applied before enqueueing in all dispatch modes, with
  drops counted in `Stats.Gated`.

## [1.3.3] - 2026-08

### Changed
- Minor hardening and documentation; no API change.

## [1.3.2] - 2026-07-10

### Fixed
- Connections now ignore cluster gossip (`IgnoreDiscoveredServers`). A client
  had drifted to a remote cluster member advertised via `connect_urls` and
  never returned to its configured endpoint. The server pool contains only the
  URLs you configure.

## [1.3.1] - 2026-07-11

### Fixed
- `Stop()` is bounded to `stopGrace` (5 s) per connection: drain and close run
  on a side goroutine so a stalled peer holding the connection lock cannot hang
  shutdown. Pools stop members in parallel.
- Per-write socket deadline cut from nats.go's 60 s default to 5 s
  (`flusherTimeout`), so a wedged write can no longer blind the heartbeat for a
  minute.

## [1.3.0] - 2026-07

### Added
- Mirroring: `NewMirroredPublisherPool` / `NewMirroredSubscriber` fan one
  subject out to several independent NATS endpoints while returning the plain
  pool / subscriber types.
- `MustProbeEach`.

## [1.2.x] and earlier - 2026-06/07

- `Subscriber`, `Publisher`, `PublisherPool` with health-first two-pass routing,
  exponential-backoff reconnect without a retry cap, `IgnoreAuthErrorAbort`,
  rebuild after a permanent close, 3 s × 2 heartbeat probing, bounded worker
  pool with visible drops, `SyncMode` and `GoPerMessage` dispatch, periodic
  status reporting, `Probe` / `MustProbe`.

[Unreleased]: https://github.com/Jolly23/natslink/compare/v1.8.0...HEAD
[1.8.0]: https://github.com/Jolly23/natslink/compare/v1.7.0...v1.8.0
[1.7.0]: https://github.com/Jolly23/natslink/compare/v1.6.1...v1.7.0
[1.6.1]: https://github.com/Jolly23/natslink/compare/v1.6.0...v1.6.1
[1.6.0]: https://github.com/Jolly23/natslink/releases/tag/v1.6.0

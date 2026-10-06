# Changelog

All notable changes to this module are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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

[Unreleased]: https://github.com/Jolly23/natslink/compare/v1.6.0...HEAD
[1.6.0]: https://github.com/Jolly23/natslink/releases/tag/v1.6.0

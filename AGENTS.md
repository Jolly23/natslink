# natslink: notes for contributors and coding agents

Read `README.md` first; it is the specification. `CONTRIBUTING.md` has the
test and release procedure. This file only lists what is easy to get wrong.

- **Scope.** One connection, one subject, self-supervising. No business logic,
  no dependency beyond `nats.go` and the standard library. Mirrored publishers
  own independent endpoint send queues; mirrored subscribers may share an
  opt-in byte-level `DedupSet`. Content-level deduplication, application routing
  and ordering across endpoints belong to the caller.
- **Two implementations, one contract.** The Go module and
  [Rust crate](https://github.com/Jolly23/natslink-rs) implement the same design.
  New features and shared behavior changes must ship in the same minor version
  in both. A change confined to one implementation (bug fix, CI, documentation)
  gets its own patch release; patch numbers need not match. Record justified
  client-library differences in the Rust README, and update the shared
  `testdata/contract-v1.json` fixture and both runners when the contract changes.
  Keep this module's Go 1.25 minimum unless explicitly changing that contract.
- **Constants are the product.** `dialTimeout`, `reconnectWait*`,
  `pingInterval` / `maxPingsOut`, `rebuildBackoff*`, `flusherTimeout`,
  `stopGrace`, `reconnectBufSize` are deliberately unexported and documented in
  `natslink.go`. Do not add options for them.
- **Log lines are grep targets.** Keep the `[natslink]` prefix and existing
  message texts and keys; add new lines rather than renaming.
- **Credentials never reach an error.** Every error that can carry a URL goes
  through `redactError` (`redaction.go`); add a case to
  `redaction_quoted_test.go` for any new escaping form.
- **Concurrency invariants** worth re-reading before touching `subscriber.go`:
  `subMu` serialises `Pause` / `Resume` / per-link `setup` / the healer;
  `conns[l]` is the most recently set-up connection (not `link.Conn()`, which
  is stale for a few microseconds during a rebuild swap); the healer holds
  `subMu` across SUB / UNSUB on purpose.
- **Tests.** `go test -short -race` must pass without a server. The full suite
  runs in CI against two docker NATS servers plus a `nats-server` binary and
  fails on any skip; keep new integration tests skipping cleanly in `-short`
  mode and when the server is unreachable.
- **Versioning.** Bump `Version` in `natslink.go` and `CHANGELOG.md` together;
  the release workflow refuses a tag that does not match.
- **Public documentation.** Keep examples, comments, change notes and review
  discussions useful to any user. Do not include private deployments, operator
  details, credentials, infrastructure inventories or identifying incident details.

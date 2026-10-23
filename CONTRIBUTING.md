# Contributing

## Ground rules

- The supervision constants (dial timeout, reconnect and rebuild backoff,
  heartbeat interval, write deadline, stop grace) are deliberately not
  configurable. Changing one is a behaviour change for every user; open an
  issue with the failure mode you observed before sending a PR.
- Log messages are an interface: operators grep for them. Do not rename an
  existing message or key; add new ones.
- Errors must never contain credentials. Any new error path that can carry a
  URL goes through `redactError` / `redactURL`, and gets a regression test in
  `redaction_test.go` or `redaction_quoted_test.go`.
- No business logic and no dependencies beyond `nats.go` and the standard
  library.
- Keep documentation, comments, examples and change notes independent of any
  private deployment. Use synthetic credentials and loopback test endpoints.
- Go 1.25 is the supported minimum; do not raise it as a side effect of a
  dependency or tooling update.

## Shared behavior with Rust

The [Rust crate](https://github.com/Jolly23/natslink-rs) implements the same
design. New features and shared behavior changes belong in the same minor
release of both implementations. A fix or maintenance change confined to one
implementation uses that implementation's next patch version; patch numbers
do not have to match. Each implementation should use its runtime efficiently.
Differences required by the underlying NATS clients must be explained in the
Rust README's comparison table and covered by tests.

`testdata/contract-v1.json` is the shared behavioral fixture. Keep its copy in
both repositories identical. `TestSharedContract` executes it against the real
deduplicator, subscriber dispatch and credential handshake paths. Contract
changes must update both runners and fixture copies, rather than merely
updating a version reference. Run the local cases with:

```bash
go test -short -run '^TestSharedContract$' -count=1 .
```

## Running the tests

Unit tests need no server:

```bash
make test            # gofmt check, go vet, go test -short -race
```

The integration suite needs two local NATS servers and the `docker` CLI. The
restart / pause drills find the container by the host port it publishes
(`docker ps --filter publish=<port>`), so the containers must publish fixed
ports:

```bash
make itest           # starts the two containers, runs everything, removes them
```

Manually:

```bash
docker run -d --name natslink-nats-1 -p 127.0.0.1:4222:4222 nats:latest --auth natslink-itest-token
docker run -d --name natslink-nats-2 -p 127.0.0.1:4223:4222 nats:latest --auth natslink-itest-token

NATS_TEST_URL='nats://natslink-itest-token@127.0.0.1:4222' \
NATS_TEST_URL2='nats://natslink-itest-token@127.0.0.1:4223' \
go test -race -count=1 ./...

docker rm -f natslink-nats-1 natslink-nats-2
```

Environment variables:

| Variable | Default | Used by |
|----------|---------|---------|
| `NATS_TEST_URL` | `nats://natslink-itest-token@127.0.0.1:4222` | single-server integration tests, pause tests, benchmarks |
| `NATS_TEST_URL2` | `nats://natslink-itest-token@127.0.0.1:4223` | dual-server matrix and mirrored pause tests |
| `NATS_TEST_CONTAINER` | resolved from the port of `NATS_TEST_URL` | overrides the container restarted by the restart drills |

Point these only at throw-away local servers: the drills `docker restart`,
`docker stop` / `start` and `docker pause` / `unpause` the container.

`mirror_integration_test.go` additionally spawns its own servers and needs a
`nats-server` binary on `PATH`; it skips itself otherwise:

```bash
go install github.com/nats-io/nats-server/v2@latest
```

A full run with both containers and `nats-server` installed takes about a
minute and should report no unexpected skips. Benchmarks:

```bash
go test -bench 'BenchmarkHot' -benchtime 1s -count 6 -run xxx .
```

## Releasing

Versions follow semantic versioning. The module is on major version 1: any
change that breaks the exported API requires a `/v2` module path, so do not
make one casually.

1. Bump `Version` in `natslink.go` and add the section to `CHANGELOG.md`
   (move items out of `Unreleased`).
2. Land that change on `main` through a pull request (`main` only accepts
   pull requests), then tag the merge commit and push the tag:

   ```bash
   git checkout main && git pull --ff-only
   git tag -a v1.6.0 -m "natslink v1.6.0"
   git push origin v1.6.0
   ```

3. The `release` workflow verifies that the tag matches `natslink.Version`,
   runs the unit tests, and creates the GitHub release whose body is the
   matching `## [X.Y.Z]` section of `CHANGELOG.md`
   (`.github/scripts/changelog_section.sh`). A version mismatch or a missing
   CHANGELOG section fails the workflow. If a tag was already pushed, correct
   the release in a new patch version; never replace a published tag.

Consumers then upgrade with `go get github.com/Jolly23/natslink@v1.6.0`.
The Go module proxy caches tags permanently, so never move or delete a tag
that has been pushed; publish a new patch version instead.

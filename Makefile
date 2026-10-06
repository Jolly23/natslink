GO ?= go
TOKEN ?= natslink-itest-token
NATS_IMAGE ?= nats:latest
C1 ?= natslink-nats-1
C2 ?= natslink-nats-2
P1 ?= 4222
P2 ?= 4223

.PHONY: fmt vet test itest itest-up itest-down bench vuln

fmt:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	$(GO) vet ./...

## Unit tests only (no server needed).
test: fmt vet
	$(GO) test -short -race -count=1 ./...

## Full suite against two throw-away docker NATS servers.
itest: itest-up
	@trap '$(MAKE) itest-down' EXIT; \
	NATS_TEST_URL='nats://$(TOKEN)@127.0.0.1:$(P1)' \
	NATS_TEST_URL2='nats://$(TOKEN)@127.0.0.1:$(P2)' \
	$(GO) test -race -count=1 -v ./... > itest.log 2>&1; rc=$$?; \
	grep -E '^(--- (FAIL|SKIP)|ok|FAIL)' itest.log; \
	echo "passed: $$(grep -c -- '--- PASS' itest.log)  skipped: $$(grep -c -- '--- SKIP' itest.log)"; \
	exit $$rc

itest-up:
	docker run -d --name $(C1) -p 127.0.0.1:$(P1):4222 $(NATS_IMAGE) --auth $(TOKEN) >/dev/null
	docker run -d --name $(C2) -p 127.0.0.1:$(P2):4222 $(NATS_IMAGE) --auth $(TOKEN) >/dev/null
	@sleep 1

itest-down:
	-docker rm -f $(C1) $(C2) >/dev/null 2>&1

bench:
	$(GO) test -bench 'BenchmarkHot' -benchtime 1s -count 6 -run xxx .

vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./...

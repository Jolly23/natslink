package natslink

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestOptionsNormalizeDefaults(t *testing.T) {
	o, err := Options{URL: "nats://token@localhost:4222", Topic: "events.tx"}.normalize("sub")
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if o.Name != "natslink-sub-events.tx" {
		t.Errorf("default name = %q", o.Name)
	}
	if o.Logger == nil {
		t.Error("default logger not set")
	}
	if o.ReportInterval != defaultReportInterval {
		t.Errorf("default ReportInterval = %v", o.ReportInterval)
	}

	// A negative value disables the periodic status report.
	o2, _ := Options{URL: "nats://localhost:4222", Topic: "t", ReportInterval: -1}.normalize("sub")
	if o2.ReportInterval >= 0 {
		t.Errorf("negative ReportInterval should stay disabled, got %v", o2.ReportInterval)
	}
}

func TestOptionsNormalizeRequired(t *testing.T) {
	if _, err := (Options{Topic: "t"}).normalize("pub"); err == nil {
		t.Error("missing URL should fail")
	}
	if _, err := (Options{URL: "nats://localhost:4222"}).normalize("pub"); err == nil {
		t.Error("missing Topic should fail")
	}
}

func TestReconnectDelayBackoff(t *testing.T) {
	l := &link{}
	l.o, _ = Options{URL: "nats://localhost:4222", Topic: "t", ReportInterval: -1}.normalize("sub")

	for attempts := 0; attempts <= 100; attempts++ {
		d := l.reconnectDelay(attempts)
		// Expected range: [base, cap*1.5] (jitter adds at most +50%).
		if d < reconnectWait || d > reconnectWaitMax+reconnectWaitMax/2 {
			t.Fatalf("attempt %d: delay %v out of range [%v, %v]", attempts, d, reconnectWait, reconnectWaitMax*3/2)
		}
	}
	// A huge attempt count must neither overflow nor loop forever.
	if d := l.reconnectDelay(1 << 30); d <= 0 {
		t.Errorf("huge attempts: delay = %v", d)
	}
}

func TestNewSubscriberValidation(t *testing.T) {
	if _, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://localhost:4222", Topic: "t"},
	}); err == nil {
		t.Error("missing Handler should fail")
	}

	// Default: worker-pool mode with 8 workers and a 4096-slot queue.
	s, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://localhost:4222", Topic: "t"},
		Handler: func(*nats.Msg) {},
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	if s.workers != defaultWorkers || cap(s.msgCh) != defaultQueueSize {
		t.Errorf("defaults: workers=%d queue=%d", s.workers, cap(s.msgCh))
	}

	// Custom worker count and queue size.
	s2, _ := NewSubscriber(SubscriberOptions{
		Options:   Options{URL: "nats://localhost:4222", Topic: "t"},
		Handler:   func(*nats.Msg) {},
		Workers:   4,
		QueueSize: 128,
	})
	if s2.workers != 4 || cap(s2.msgCh) != 128 {
		t.Errorf("custom: workers=%d queue=%d", s2.workers, cap(s2.msgCh))
	}

	// Workers = SyncMode selects synchronous mode: no queue, no workers.
	s3, _ := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://localhost:4222", Topic: "t"},
		Handler: func(*nats.Msg) {},
		Workers: SyncMode,
	})
	if s3.msgCh != nil || s3.workers != 0 || s3.goPerMsg {
		t.Error("sync mode should have no queue / workers / goPerMsg flag")
	}

	// Workers = GoPerMessage selects go-per-message mode: no queue, no workers, only the flag.
	s4, _ := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://localhost:4222", Topic: "t"},
		Handler: func(*nats.Msg) {},
		Workers: GoPerMessage,
	})
	if s4.msgCh != nil || s4.workers != 0 || !s4.goPerMsg {
		t.Error("go-per-message mode should have no queue / workers, flag set")
	}

	// Any other negative value is a configuration error.
	if _, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://localhost:4222", Topic: "t"},
		Handler: func(*nats.Msg) {},
		Workers: -3,
	}); err == nil {
		t.Error("invalid negative Workers should fail")
	}
}

// Go-per-message contract: dispatch never blocks the delivery goroutine (while a
// callback is blocked, further messages keep being dispatched with no concurrency
// bound), and Dropped / queue depth stay at 0.
func TestGoPerMessageDispatch(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	s, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://localhost:4222", Topic: "t", ReportInterval: -1},
		Handler: func(*nats.Msg) { entered <- struct{}{}; <-release },
		Workers: GoPerMessage,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	// The first callback blocks on release; the second must still enter the
	// callback (unbounded concurrency, no queueing).
	s.dispatch(&nats.Msg{})
	s.dispatch(&nats.Msg{})
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("handler %d not entered: go-per-message must not queue behind a blocked callback", i)
		}
	}
	close(release)

	if st := s.Stats(); st.Dropped != 0 || st.QueueLen != 0 || st.QueueCap != 0 {
		t.Errorf("go-per-message stats: dropped=%d queue=%d/%d, want all 0", st.Dropped, st.QueueLen, st.QueueCap)
	}
}

func TestDispatchQueueOverflow(t *testing.T) {
	handled := 0
	s, err := NewSubscriber(SubscriberOptions{
		Options:   Options{URL: "nats://localhost:4222", Topic: "t", ReportInterval: -1},
		Handler:   func(*nats.Msg) { handled++ },
		QueueSize: 2,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	// Not started (no workers draining): the first two messages are queued, the
	// third finds the queue full and is dropped and counted.
	s.dispatch(&nats.Msg{})
	s.dispatch(&nats.Msg{})
	s.dispatch(&nats.Msg{})
	if got := s.Stats(); got.Dropped != 1 || got.QueueLen != 2 || got.QueueCap != 2 {
		t.Errorf("stats after overflow: dropped=%d queue=%d/%d", got.Dropped, got.QueueLen, got.QueueCap)
	}
	if handled != 0 {
		t.Errorf("handler should not run without workers, ran %d times", handled)
	}

	// Sync mode: dispatch invokes the callback inline on the calling goroutine.
	sync, _ := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://localhost:4222", Topic: "t", ReportInterval: -1},
		Handler: func(*nats.Msg) { handled++ },
		Workers: -1,
	})
	sync.dispatch(&nats.Msg{})
	if handled != 1 {
		t.Errorf("sync dispatch should call handler inline, handled=%d", handled)
	}
}

// Gate contract: when Gate returns false the message is discarded at the delivery
// entry point (not queued, no goroutine spawned, no callback) and counted in
// Stats.Gated; when it returns true, or is unset (nil), behaviour is identical to
// the pre-Gate version. All three consumption modes share the single check at the
// top of dispatch.
func TestSubscriberGate(t *testing.T) {
	var pass atomic.Bool
	newSub := func(workers int, handled *atomic.Int32) *Subscriber {
		s, err := NewSubscriber(SubscriberOptions{
			Options: Options{URL: "nats://localhost:4222", Topic: "t", ReportInterval: -1},
			Handler: func(*nats.Msg) { handled.Add(1) },
			Workers: workers,
			Gate:    func() bool { return pass.Load() },
		})
		if err != nil {
			t.Fatalf("NewSubscriber: %v", err)
		}
		return s
	}

	// Sync mode: closed gate drops and counts, open gate passes straight to the callback.
	var syncHandled atomic.Int32
	s := newSub(SyncMode, &syncHandled)
	s.dispatch(&nats.Msg{})
	if got := s.Stats(); syncHandled.Load() != 0 || got.Gated != 1 {
		t.Errorf("closed gate (sync): handled=%d gated=%d, want 0/1", syncHandled.Load(), got.Gated)
	}
	pass.Store(true)
	s.dispatch(&nats.Msg{})
	if got := s.Stats(); syncHandled.Load() != 1 || got.Gated != 1 {
		t.Errorf("open gate (sync): handled=%d gated=%d, want 1/1", syncHandled.Load(), got.Gated)
	}

	// Worker-pool mode: a closed gate does not enqueue (queue depth 0, Dropped unchanged).
	pass.Store(false)
	var poolHandled atomic.Int32
	s2 := newSub(0, &poolHandled) // not started: no worker drains, so anything enqueued stays visible
	s2.dispatch(&nats.Msg{})
	if st := s2.Stats(); st.Gated != 1 || st.QueueLen != 0 || st.Dropped != 0 {
		t.Errorf("closed gate (worker pool): gated=%d queue=%d dropped=%d, want 1/0/0", st.Gated, st.QueueLen, st.Dropped)
	}

	// Go-per-message mode: a closed gate does not spawn a callback goroutine.
	var gpmHandled atomic.Int32
	s3 := newSub(GoPerMessage, &gpmHandled)
	s3.dispatch(&nats.Msg{})
	if st := s3.Stats(); st.Gated != 1 || gpmHandled.Load() != 0 {
		t.Errorf("closed gate (go-per-message): gated=%d handled=%d, want 1/0", st.Gated, gpmHandled.Load())
	}

	// nil gate = always pass (backwards compatible); Gated stays 0.
	var nilHandled atomic.Int32
	s4, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://localhost:4222", Topic: "t", ReportInterval: -1},
		Handler: func(*nats.Msg) { nilHandled.Add(1) },
		Workers: SyncMode,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	s4.dispatch(&nats.Msg{})
	if got := s4.Stats(); nilHandled.Load() != 1 || got.Gated != 0 {
		t.Errorf("nil gate: handled=%d gated=%d, want 1/0", nilHandled.Load(), got.Gated)
	}
}

func TestValidateTopic(t *testing.T) {
	for _, tc := range []struct{ topic, role string }{
		{"events.tx", "pub"}, {"events.tx", "sub"}, {"single", "pub"},
		{"a.*.c", "sub"}, {"a.>", "sub"}, // wildcard subscriptions are legal
	} {
		if err := validateTopic(tc.topic, tc.role); err != nil {
			t.Errorf("validateTopic(%q, %s) = %v, want nil", tc.topic, tc.role, err)
		}
	}
	for _, tc := range []struct{ topic, role string }{
		{"", "pub"},
		{".events.tx", "sub"}, {"events..tx", "pub"}, {"events.tx.", "sub"}, // empty token
		{"events. tx", "pub"}, {"events.tx\n", "sub"}, // whitespace
		{"a.*.c", "pub"}, {"a.>", "pub"}, // wildcard publishing is illegal
	} {
		if err := validateTopic(tc.topic, tc.role); err == nil {
			t.Errorf("validateTopic(%q, %s) = nil, want error", tc.topic, tc.role)
		}
	}
}

// Stop must block until in-flight callbacks return: after Stop the caller releases
// the resources the callback depends on, so returning early is a data race. Verifiable
// without a server (RetryOnFailedConnect lets Start succeed; messages are injected by
// calling dispatch directly).
func TestStopWaitsForInflightHandler(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var finished atomic.Bool
	s, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://127.0.0.1:1", Topic: "t", ReportInterval: -1},
		Handler: func(*nats.Msg) { close(entered); <-release; finished.Store(true) },
		Workers: 1,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	if err := s.Start(); err != nil { // no server: Start still succeeds under RetryOnFailedConnect
		t.Fatalf("Start: %v", err)
	}
	s.dispatch(&nats.Msg{})
	<-entered // the callback is now in flight

	stopped := make(chan struct{})
	go func() { s.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Stop must block while a handler is in flight")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after handler finished")
	}
	if !finished.Load() {
		t.Fatal("in-flight handler must run to completion")
	}
}

func TestRedactURL(t *testing.T) {
	for raw, want := range map[string]string{
		"nats://secret-token@1.2.3.4:4222":             "nats://***@1.2.3.4:4222",
		"nats://user:pass@1.2.3.4:4222":                "nats://***@1.2.3.4:4222",
		"nats://1.2.3.4:4222":                          "nats://1.2.3.4:4222", // no credentials: unchanged
		"nats://tokenA@h1:4222, nats://tokenB@h2:4222": "nats://***@h1:4222,nats://***@h2:4222",
		"not a url at all with token@inside":           "(redacted)", // unparsable and possibly carrying credentials: redact the whole string
	} {
		if got := redactURL(raw); got != want {
			t.Errorf("redactURL(%q) = %q, want %q", raw, got, want)
		}
	}
}

// Probe fails fast on an unreachable address, and the token is redacted from the error.
func TestProbeUnreachable(t *testing.T) {
	// Nothing listens on 127.0.0.1:1, so connection refused returns immediately
	// instead of waiting out the dial timeout.
	err := Probe("nats://my-secret-token@127.0.0.1:1")
	if err == nil {
		t.Fatal("probe to unreachable server must fail")
	}
	if s := err.Error(); strings.Contains(s, "my-secret-token") {
		t.Fatalf("probe error leaks token: %s", s)
	} else if !strings.Contains(s, "***") {
		t.Fatalf("probe error should contain redacted url: %s", s)
	}

	defer func() {
		if recover() == nil {
			t.Fatal("MustProbe must panic on unreachable server")
		}
	}()
	MustProbe("nats://127.0.0.1:1")
}

func TestPublisherNotRunning(t *testing.T) {
	p, err := NewPublisher(PublisherOptions{
		Options: Options{URL: "nats://localhost:4222", Topic: "t"},
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	if err := p.Publish([]byte("x")); !errors.Is(err, ErrNotRunning) {
		t.Errorf("Publish before Start = %v, want ErrNotRunning", err)
	}

	st := p.Stats()
	if st.Running || st.Connected || st.Status != "no-conn" {
		t.Errorf("stats before start: %+v", st)
	}
}

func TestPublisherPoolValidation(t *testing.T) {
	if _, err := NewPublisherPool(0, PublisherOptions{
		Options: Options{URL: "nats://localhost:4222", Topic: "t"},
	}); err == nil {
		t.Error("pool size 0 should fail")
	}

	pool, err := NewPublisherPool(3, PublisherOptions{
		Options: Options{URL: "nats://localhost:4222", Topic: "t", Name: "mypub"},
	})
	if err != nil {
		t.Fatalf("NewPublisherPool: %v", err)
	}
	if pool.Size() != 3 {
		t.Errorf("pool size = %d", pool.Size())
	}
	for i, pub := range pool.pubs {
		want := "mypub-" + string(rune('0'+i))
		if pub.o.Name != want {
			t.Errorf("pub[%d].Name = %q, want %q", i, pub.o.Name, want)
		}
	}

	// Publishing before Start: every connection returns ErrNotRunning.
	if err := pool.Publish([]byte("x")); !errors.Is(err, ErrNotRunning) {
		t.Errorf("pool Publish before Start = %v, want ErrNotRunning", err)
	}
	if pool.Dropped() != 1 {
		t.Errorf("pool dropped = %d, want 1", pool.Dropped())
	}
}

func TestOversized(t *testing.T) {
	p, err := NewPublisher(PublisherOptions{
		Options:    Options{URL: "nats://localhost:4222", Topic: "t"},
		MaxPayload: 4,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	// The size check runs after the running check, so flip the flag by hand to
	// simulate a started publisher.
	p.running.Store(true)
	if err := p.Publish([]byte("12345")); !errors.Is(err, ErrOversized) {
		t.Errorf("oversized publish = %v, want ErrOversized", err)
	}
	if p.Stats().Dropped != 1 {
		t.Errorf("dropped = %d, want 1", p.Stats().Dropped)
	}
}

// -- Mirroring --

func TestMirrorValidation(t *testing.T) {
	pubOpts := PublisherOptions{Options: Options{Topic: "t", ReportInterval: -1}}

	if _, err := NewMirroredPublisherPool(nil, 1, pubOpts); err == nil {
		t.Error("empty urls should fail")
	}
	if _, err := NewMirroredPublisherPool([]string{"nats://h1:4222", " nats://h1:4222 "}, 1, pubOpts); err == nil {
		t.Error("duplicate urls should fail")
	}
	if _, err := NewMirroredPublisherPool([]string{"nats://h1:4222", ""}, 1, pubOpts); err == nil {
		t.Error("empty url element should fail")
	}

	// Endpoint naming: explicit base name + "-e<i>" suffix, with members inside an
	// endpoint appending "-<j>"; members are flattened across endpoints.
	m, err := NewMirroredPublisherPool([]string{"nats://h1:4222", "nats://h2:4222"}, 2, PublisherOptions{
		Options: Options{Topic: "t", Name: "mypub", ReportInterval: -1},
	})
	if err != nil {
		t.Fatalf("NewMirroredPublisherPool: %v", err)
	}
	if m.Endpoints() != 2 || m.Size() != 4 {
		t.Errorf("endpoints=%d size=%d, want 2/4", m.Endpoints(), m.Size())
	}
	if got := m.pubs[0].o.Name; got != "mypub-e0-0" {
		t.Errorf("member0 name = %q", got)
	}
	if got := m.pubs[3].o.Name; got != "mypub-e1-1" {
		t.Errorf("member3 name = %q", got)
	}
	if got := m.pubs[2].o.URL; got != "nats://h2:4222" {
		t.Errorf("member2 url = %q", got)
	}

	// Without a base name the package default naming rule applies; a mirrored
	// subscriber is a plain *Subscriber.
	ms, err := NewMirroredSubscriber([]string{"nats://h1:4222", "nats://h2:4222"}, SubscriberOptions{
		Options: Options{Topic: "events.block", ReportInterval: -1},
		Handler: func(*nats.Msg) {},
	})
	if err != nil {
		t.Fatalf("NewMirroredSubscriber: %v", err)
	}
	if ms.Endpoints() != 2 {
		t.Errorf("sub endpoints = %d, want 2", ms.Endpoints())
	}
	if got := ms.links[0].o.Name; got != "natslink-sub-events.block-e0" {
		t.Errorf("sub endpoint0 name = %q", got)
	}

	// Regression guard: the plain (single-endpoint) constructor keeps the original
	// naming exactly, with no "-e" suffix.
	s, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://h1:4222", Topic: "events.block", ReportInterval: -1},
		Handler: func(*nats.Msg) {},
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	if got := s.links[0].o.Name; got != "natslink-sub-events.block" {
		t.Errorf("plain subscriber name = %q, want no -e suffix", got)
	}
	if s.Endpoints() != 1 {
		t.Errorf("plain subscriber endpoints = %d, want 1", s.Endpoints())
	}
}

// Mirrored publish semantics: success if any endpoint accepts; the pool-level
// Dropped counter only grows when every endpoint fails. Verifiable without a server:
// before Start every endpoint returns ErrNotRunning; after Start (RetryOnFailedConnect)
// writing into the reconnect buffer counts as "accepted".
func TestMirrorPublishSemantics(t *testing.T) {
	m, err := NewMirroredPublisherPool([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, 1, PublisherOptions{
		Options: Options{Topic: "t", ReportInterval: -1},
	})
	if err != nil {
		t.Fatalf("NewMirroredPublisherPool: %v", err)
	}

	// Before Start: all endpoints fail -> error (errors.Is sees through Join) + pool Dropped=1.
	if err := m.Publish([]byte("x")); !errors.Is(err, ErrNotRunning) {
		t.Errorf("publish before start = %v, want ErrNotRunning", err)
	}
	if m.Dropped() != 1 {
		t.Errorf("dropped = %d, want 1", m.Dropped())
	}

	// After Start (no server, connections retry in the background): both endpoints
	// write into the reconnect buffer = accepted -> nil.
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Stop()
	if err := m.Publish([]byte("y")); err != nil {
		t.Errorf("publish after start = %v, want nil (buffered)", err)
	}
	if m.Dropped() != 1 {
		t.Errorf("dropped grew unexpectedly: %d", m.Dropped())
	}

	// Single-endpoint failure (stop the only member of endpoint 0 -> ErrStopped):
	// endpoint 1 still accepts -> nil, no drop counted.
	m.pubs[0].Stop()
	if err := m.Publish([]byte("z")); err != nil {
		t.Errorf("publish with one endpoint down = %v, want nil", err)
	}
	if m.Dropped() != 1 {
		t.Errorf("dropped after single-endpoint failure = %d, want 1", m.Dropped())
	}

	// Stats flattens every member (2 endpoints x 1 connection).
	if st := m.Stats(); len(st) != 2 {
		t.Errorf("stats members = %d, want 2", len(st))
	}
}

// Mirrored subscriber: endpoints share one dispatch/worker pool and the Handler
// receives every copy from every endpoint; de-duplication is the caller's job.
func TestMirroredSubscriberDispatch(t *testing.T) {
	var count atomic.Int32
	ms, err := NewMirroredSubscriber([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, SubscriberOptions{
		Options: Options{Topic: "t", ReportInterval: -1},
		Handler: func(*nats.Msg) { count.Add(1) },
		Workers: SyncMode, // sync mode makes dispatch call back inline, so the test needs no workers
	})
	if err != nil {
		t.Fatalf("NewMirroredSubscriber: %v", err)
	}

	// Simulate two copies of the same message arriving from the two endpoints'
	// delivery goroutines (shared dispatch).
	ms.dispatch(&nats.Msg{})
	ms.dispatch(&nats.Msg{})
	if got := count.Load(); got != 2 {
		t.Errorf("handler calls = %d, want 2 (one per endpoint copy)", got)
	}

	// Merged stats: before Start, Connected=false and Running=false; counters are zero.
	st := ms.Stats()
	if st.Connected || st.Running {
		t.Errorf("stats before start: connected=%v running=%v", st.Connected, st.Running)
	}
}

// A mirrored Start that fails part-way must roll back the endpoints already
// started, leaving no leaked connections or goroutines.
func TestMirrorStartPartialFailureCleanup(t *testing.T) {
	ms, err := NewMirroredSubscriber([]string{"nats://127.0.0.1:1", "nats://bad url/"}, SubscriberOptions{
		Options: Options{Topic: "t", ReportInterval: -1},
		Handler: func(*nats.Msg) {},
	})
	if err != nil {
		t.Fatalf("New should succeed (url validity only surfaces at connect time): %v", err)
	}
	if err := ms.Start(); err == nil {
		ms.Stop()
		t.Fatal("Start must fail on unparsable endpoint url")
	}
	if !ms.links[0].stopped.Load() {
		t.Error("first endpoint must be stopped after partial start failure")
	}
}

// Stats.Name attribution: at the connection level, in the flattened pool view, and
// in the merged mirrored-subscriber view, a specific connection must be identifiable.
func TestStatsName(t *testing.T) {
	p, _ := NewPublisher(PublisherOptions{Options: Options{URL: "nats://h:4222", Topic: "t", Name: "solo", ReportInterval: -1}})
	if got := p.Stats().Name; got != "solo" {
		t.Errorf("publisher stats name = %q", got)
	}

	m, _ := NewMirroredPublisherPool([]string{"nats://h1:4222", "nats://h2:4222"}, 2, PublisherOptions{
		Options: Options{Topic: "t", Name: "mp", ReportInterval: -1},
	})
	st := m.Stats()
	want := []string{"mp-e0-0", "mp-e0-1", "mp-e1-0", "mp-e1-1"}
	for i, w := range want {
		if st[i].Name != w {
			t.Errorf("pool stats[%d].Name = %q, want %q (flattened order: endpoint 0 members first)", i, st[i].Name, w)
		}
	}

	ms, _ := NewMirroredSubscriber([]string{"nats://h1:4222", "nats://h2:4222"}, SubscriberOptions{
		Options: Options{Topic: "t", Name: "msub", ReportInterval: -1},
		Handler: func(*nats.Msg) {},
	})
	if got := ms.Stats().Name; got != "msub-e0" {
		t.Errorf("merged subscriber stats name = %q, want first endpoint name msub-e0", got)
	}
}

// Mirroring x SyncMode: the serial ordering guarantee of sync mode does not survive
// mirroring; the M endpoint delivery goroutines call the callback concurrently. This
// test demonstrates that behaviour (and is the basis for the documented warning):
// while one copy's callback is blocked, the copy from the other endpoint still
// enters the callback.
func TestMirrorSyncModeConcurrentAcrossEndpoints(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	ms, err := NewMirroredSubscriber([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, SubscriberOptions{
		Options: Options{Topic: "t", ReportInterval: -1},
		Handler: func(*nats.Msg) { entered <- struct{}{}; <-release },
		Workers: SyncMode,
	})
	if err != nil {
		t.Fatalf("NewMirroredSubscriber: %v", err)
	}

	// Simulate the two endpoints' delivery goroutines arriving at the same time
	// (in production they really are two independent goroutines).
	go ms.dispatch(&nats.Msg{})
	go ms.dispatch(&nats.Msg{})
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("SyncMode callbacks must be enterable concurrently under mirroring (the single-endpoint serial contract does not apply)")
		}
	}
	close(release)
}

// Concurrent publishing x endpoint shutdown x whole-pool shutdown: no data race or
// panic under -race; Publish keeps succeeding while a single endpoint is shut down
// (the other endpoint accepts) and fails consistently after the whole pool is stopped.
func TestMirrorPublishStopRace(t *testing.T) {
	m, err := NewMirroredPublisherPool([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, 2, PublisherOptions{
		Options: Options{Topic: "t", ReportInterval: -1},
	})
	if err != nil {
		t.Fatalf("NewMirroredPublisherPool: %v", err)
	}
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var wg sync.WaitGroup
	stopHalf := make(chan struct{})
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = m.Publish([]byte("race"))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-stopHalf
		// Stop every member of endpoint 0 (the first two in flattened order),
		// simulating one whole NATS tree going down and its instances being reclaimed.
		m.pubs[0].Stop()
		m.pubs[1].Stop()
	}()
	close(stopHalf)
	wg.Wait()

	// Endpoint 0 dead, endpoint 1 alive: must still succeed.
	if err := m.Publish([]byte("after-half-stop")); err != nil {
		t.Errorf("publish with endpoint0 stopped = %v, want nil", err)
	}
	m.Stop()
	if err := m.Publish([]byte("after-full-stop")); !errors.Is(err, ErrStopped) {
		t.Errorf("publish after full stop = %v, want ErrStopped", err)
	}
}

// Regression guard from an adversarial review: the built-in status report reads
// link.Stats(), so it must see the Subscriber-level queue-overflow drops and queue
// depth (injected via statsHook). Without that, the periodic status line during an
// overload would print dropped=0 with no queue field, which is worse than no report.
func TestSubscriberLinkReportIncludesSharedStats(t *testing.T) {
	s, err := NewSubscriber(SubscriberOptions{
		Options:   Options{URL: "nats://127.0.0.1:1", Topic: "t", ReportInterval: -1},
		Handler:   func(*nats.Msg) {},
		Workers:   1,
		QueueSize: 1,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	// Not started (no workers draining): message 1 is queued, messages 2 and 3 overflow and are dropped.
	s.dispatch(&nats.Msg{})
	s.dispatch(&nats.Msg{})
	s.dispatch(&nats.Msg{})

	st := s.links[0].Stats() // exactly the layer the built-in report reads
	if st.Dropped != 2 {
		t.Errorf("link-level dropped = %d, want 2 (the built-in report must reflect subscriber-side drops)", st.Dropped)
	}
	if st.QueueLen != 1 || st.QueueCap != 1 {
		t.Errorf("link-level queue = %d/%d, want 1/1", st.QueueLen, st.QueueCap)
	}
	// Under mirroring every endpoint reports the same shared values.
	ms, _ := NewMirroredSubscriber([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, SubscriberOptions{
		Options: Options{Topic: "t", ReportInterval: -1}, Handler: func(*nats.Msg) {}, QueueSize: 1, Workers: 1,
	})
	ms.dispatch(&nats.Msg{})
	ms.dispatch(&nats.Msg{})
	for i, l := range ms.links {
		if got := l.Stats().Dropped; got != 1 {
			t.Errorf("mirrored link[%d] dropped = %d, want 1 (shared value)", i, got)
		}
	}
}

// A mirrored subscriber shares one worker pool: Workers is the total concurrency
// bound across endpoints, not multiplied per endpoint. With Workers=1, even when both
// endpoints deliver a copy, only one callback is in flight.
func TestMirrorWorkersSharedBound(t *testing.T) {
	inFlight := make(chan struct{}, 4)
	release := make(chan struct{})
	ms, err := NewMirroredSubscriber([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, SubscriberOptions{
		Options:   Options{Topic: "t", ReportInterval: -1},
		Handler:   func(*nats.Msg) { inFlight <- struct{}{}; <-release },
		Workers:   1,
		QueueSize: 8,
	})
	if err != nil {
		t.Fatalf("NewMirroredSubscriber: %v", err)
	}
	if err := ms.Start(); err != nil { // starts without a server (RetryOnFailedConnect)
		t.Fatalf("Start: %v", err)
	}

	// Two "endpoint delivery goroutines" each push one message into the shared queue.
	ms.dispatch(&nats.Msg{})
	ms.dispatch(&nats.Msg{})

	select {
	case <-inFlight:
	case <-time.After(2 * time.Second):
		t.Fatal("first callback not entered")
	}
	select {
	case <-inFlight:
		t.Fatal("Workers=1 must allow exactly one in-flight callback globally (bound shared across endpoints), but a second one entered")
	case <-time.After(150 * time.Millisecond):
		// Expected: the second message can only wait in the queue.
	}
	close(release)
	select {
	case <-inFlight:
	case <-time.After(2 * time.Second):
		t.Fatal("second callback not processed after release")
	}
	go func() { // drain so Stop does not hang on an in-flight callback
		for range inFlight {
		}
	}()
	ms.Stop()
	close(inFlight)
}

// A mirrored publisher pool whose Start fails part-way must roll back the endpoints
// already started (previously only the subscriber side was covered).
func TestMirroredPoolStartPartialFailureCleanup(t *testing.T) {
	m, err := NewMirroredPublisherPool([]string{"nats://127.0.0.1:1", "nats://bad url/"}, 2, PublisherOptions{
		Options: Options{Topic: "t", ReportInterval: -1},
	})
	if err != nil {
		t.Fatalf("New should succeed (url validity only surfaces at connect time): %v", err)
	}
	if err := m.Start(); err == nil {
		m.Stop()
		t.Fatal("Start must fail on unparsable endpoint url")
	}
	for i := 0; i < 2; i++ { // both members of endpoint 0 must have been rolled back and stopped
		if !m.pubs[i].stopped.Load() {
			t.Errorf("endpoint0 member %d must be stopped after partial start failure", i)
		}
	}
}

// The Pause / Resume state machine must hold without a server too: an instance that
// was never started can be paused and resumed (no subscription handle to detach, no
// connection to re-subscribe on), and after Stop both return ErrStopped.
func TestPauseResumeStateWithoutServer(t *testing.T) {
	s, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://localhost:4222", Topic: "t", ReportInterval: -1},
		Handler: func(*nats.Msg) {},
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	if s.IsPaused() {
		t.Fatal("new subscriber should not be paused")
	}
	if err := s.Pause(); err != nil || !s.IsPaused() {
		t.Fatalf("Pause: err=%v paused=%v", err, s.IsPaused())
	}
	if err := s.Pause(); err != nil || s.Stats().Pauses != 1 {
		t.Fatalf("idempotent Pause: err=%v pauses=%d", err, s.Stats().Pauses)
	}
	if err := s.Resume(); err != nil || s.IsPaused() {
		t.Fatalf("Resume: err=%v paused=%v", err, s.IsPaused())
	}
	if st := s.Stats(); st.Paused || st.Pauses != 1 {
		t.Fatalf("stats after resume = %+v", st)
	}
	s.Stop()
	if err := s.Pause(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Pause after Stop = %v", err)
	}
	if err := s.Resume(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Resume after Stop = %v", err)
	}
}

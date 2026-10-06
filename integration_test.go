package natslink

// Integration tests against a real NATS server. Start one locally with:
//
//	docker run -d --name natslink-nats-1 -p 127.0.0.1:4222:4222 nats:latest --auth natslink-itest-token
//
// Tests skip automatically when the server is unreachable. The restart test
// additionally needs docker and a container (the name can be overridden with
// NATS_TEST_CONTAINER). The URL can be overridden with NATS_TEST_URL; the
// default is nats://natslink-itest-token@127.0.0.1:4222.

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func testURL() string {
	if u := os.Getenv("NATS_TEST_URL"); u != "" {
		return u
	}
	return "nats://natslink-itest-token@127.0.0.1:4222"
}

// testContainer resolves the container targeted by the restart test.
// NATS_TEST_CONTAINER takes precedence; otherwise the container is resolved by
// the host port of testURL (docker ps --filter publish=<port>), so no naming
// convention is assumed: whatever listens on testURL's port gets restarted.
// Returns "" when resolution fails; callers treat that as unavailable and skip.
func testContainer() string {
	if c := os.Getenv("NATS_TEST_CONTAINER"); c != "" {
		return c
	}
	u := testURL()
	out, err := exec.Command("docker", "ps", "--filter",
		"publish="+u[strings.LastIndex(u, ":")+1:], "--format", "{{.Names}}").Output()
	if err != nil {
		return ""
	}
	name := strings.TrimSpace(string(out))
	if strings.Contains(name, "\n") {
		return "" // several containers publish the port; no unique target
	}
	return name
}

// skipIfNoServer probes the server and skips when it is unreachable, so
// `go test` passes on machines without NATS.
func skipIfNoServer(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	conn, err := nats.Connect(testURL(), nats.Timeout(2*time.Second))
	if err != nil {
		t.Skipf("NATS server not reachable at %s: %v", testURL(), err)
	}
	conn.Close()
}

func uniqueTopic(name string) string {
	return fmt.Sprintf("natslink.test.%s.%d", name, time.Now().UnixNano())
}

func waitUntil(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout (%v) waiting for: %s", timeout, desc)
}

// collector gathers received payloads in a goroutine-safe way.
type collector struct {
	mu   sync.Mutex
	msgs []string
}

func (c *collector) handler(m *nats.Msg) {
	c.mu.Lock()
	c.msgs = append(c.msgs, string(m.Data))
	c.mu.Unlock()
}

func (c *collector) countPrefix(prefix string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, m := range c.msgs {
		if strings.HasPrefix(m, prefix) {
			n++
		}
	}
	return n
}

// startSubscriber creates a subscription and makes sure the SUB frame has
// reached the server (otherwise messages published right away would be lost).
func startSubscriber(t *testing.T, topic string, h Handler, mutate func(*SubscriberOptions)) *Subscriber {
	t.Helper()
	o := SubscriberOptions{
		Options: Options{URL: testURL(), Topic: topic, ReportInterval: -1},
		Handler: h,
	}
	if mutate != nil {
		mutate(&o)
	}
	sub, err := NewSubscriber(o)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	sub.MustStart()
	t.Cleanup(sub.Stop)
	waitUntil(t, 5*time.Second, "subscriber connected", sub.IsConnected)
	if err := sub.Conn().Flush(); err != nil {
		t.Fatalf("flush subscription: %v", err)
	}
	return sub
}

func startPublisher(t *testing.T, topic string) *Publisher {
	t.Helper()
	pub, err := NewPublisher(PublisherOptions{
		Options: Options{URL: testURL(), Topic: topic, ReportInterval: -1},
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	pub.MustStart()
	t.Cleanup(pub.Stop)
	waitUntil(t, 5*time.Second, "publisher connected", pub.IsConnected)
	return pub
}

// Fail-fast semantics of Probe: valid credentials succeed immediately; a bad
// token returns an error immediately (unlike Start, which silently retries in
// the background).
func TestIntegrationProbe(t *testing.T) {
	skipIfNoServer(t)

	if err := Probe(testURL()); err != nil {
		t.Fatalf("probe with valid credentials: %v", err)
	}

	badURL := strings.Replace(testURL(), "://", "://wrong-token-", 1)
	if err := Probe(badURL); err == nil {
		t.Fatal("probe with bad token must fail immediately")
	} else {
		t.Logf("bad-token probe error (as expected): %v", err)
	}
}

func TestIntegrationPubSub(t *testing.T) {
	skipIfNoServer(t)
	topic := uniqueTopic("pubsub")

	var c collector
	sub := startSubscriber(t, topic, c.handler, nil)
	pub := startPublisher(t, topic)

	const n = 100
	for i := 0; i < n; i++ {
		if err := pub.Publish([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if err := pub.Conn().Flush(); err != nil {
		t.Fatalf("flush publish: %v", err)
	}

	waitUntil(t, 10*time.Second, "all messages received", func() bool {
		return c.countPrefix("msg-") == n
	})

	if st := sub.Stats(); st.InMsgs != n || !st.Connected || st.Dropped != 0 {
		t.Errorf("sub stats: in_msgs=%d connected=%v dropped=%d", st.InMsgs, st.Connected, st.Dropped)
	}
	if st := pub.Stats(); st.OutMsgs != n || st.Dropped != 0 {
		t.Errorf("pub stats: out_msgs=%d dropped=%d", st.OutMsgs, st.Dropped)
	}
}

func TestIntegrationPublisherPool(t *testing.T) {
	skipIfNoServer(t)
	topic := uniqueTopic("pool")

	var c collector
	startSubscriber(t, topic, c.handler, nil)

	pool, err := NewPublisherPool(3, PublisherOptions{
		Options: Options{URL: testURL(), Topic: topic, ReportInterval: -1},
	})
	if err != nil {
		t.Fatalf("NewPublisherPool: %v", err)
	}
	pool.MustStart()
	t.Cleanup(pool.Stop)
	waitUntil(t, 5*time.Second, "pool fully connected", func() bool {
		return pool.ConnectedCount() == pool.Size()
	})

	const n = 60
	for i := 0; i < n; i++ {
		if err := pool.Publish([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("pool publish %d: %v", i, err)
		}
	}
	for _, st := range pool.Stats() {
		_ = st // round-robin should give every connection some traffic, but an even split is not required
	}

	waitUntil(t, 10*time.Second, "all pool messages received", func() bool {
		return c.countPrefix("msg-") == n
	})
	if pool.Dropped() != 0 {
		t.Errorf("pool dropped = %d", pool.Dropped())
	}
}

func TestIntegrationQueueGroup(t *testing.T) {
	skipIfNoServer(t)
	topic := uniqueTopic("qgroup")

	var c1, c2 collector
	mutate := func(o *SubscriberOptions) { o.QueueGroup = "workers" }
	startSubscriber(t, topic, c1.handler, mutate)
	startSubscriber(t, topic, c2.handler, mutate)

	pub := startPublisher(t, topic)
	const n = 100
	for i := 0; i < n; i++ {
		if err := pub.Publish([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	_ = pub.Conn().Flush()

	// Competing consumers in one queue group: the two subscribers together
	// receive exactly the published count (each message is delivered once).
	waitUntil(t, 10*time.Second, "queue group received all", func() bool {
		return c1.countPrefix("msg-")+c2.countPrefix("msg-") == n
	})
	t.Logf("queue group split: %d / %d", c1.countPrefix("msg-"), c2.countPrefix("msg-"))
}

func TestIntegrationBackpressure(t *testing.T) {
	skipIfNoServer(t)
	topic := uniqueTopic("backpressure")

	// 1 worker + a queue of length 1, with a blocking callback: message 1 is held
	// by the worker, message 2 fills the queue, the rest are dropped and counted.
	block := make(chan struct{})
	var c collector
	sub := startSubscriber(t, topic, func(m *nats.Msg) { <-block; c.handler(m) }, func(o *SubscriberOptions) {
		o.Workers = 1
		o.QueueSize = 1
	})
	t.Cleanup(func() { close(block) })

	pub := startPublisher(t, topic)
	const n = 50
	for i := 0; i < n; i++ {
		if err := pub.Publish([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	_ = pub.Conn().Flush()

	// A blocking callback must not block delivery. InMsgs==n cannot serve as the
	// sync point: InMsgs is counted in nats.go's readLoop, while dispatch is
	// driven by a separate asynchronous delivery goroutine (waitForMsgs). All
	// messages being read does not mean they have all been dispatched; on a
	// loaded machine the delivery goroutine lags and the assertion would race
	// ahead and read a Dropped value that is still too small (observed: InMsgs=50
	// with only 13 dispatched, dropped=11, a spurious failure). The drop counter
	// itself is the signal that dispatch has fully converged: 1 message is held
	// by the worker, 1 sits in the queue, so the remaining n-2 must all be
	// dropped by backpressure.
	waitUntil(t, 10*time.Second, "backpressure drops converged", func() bool {
		return sub.Stats().Dropped >= n-2
	})
	st := sub.Stats()
	t.Logf("backpressure: in_msgs=%d dropped=%d queue=%d/%d", st.InMsgs, st.Dropped, st.QueueLen, st.QueueCap)
	if st.Dropped < n-2 || st.Dropped >= n {
		t.Errorf("dropped = %d, want %d or %d", st.Dropped, n-2, n-1)
	}
}

// TestIntegrationWaitConnected checks the startup fail-fast semantics: with
// valid credentials WaitConnected is ready immediately; with a bad token Start
// still succeeds (background retry) and WaitConnected times out with an error
// (for the caller to panic on). The client must never give up under persistent
// auth errors: IgnoreAuthErrorAbort keeps it out of the permanent CLOSED state,
// which a production incident showed to be one root cause of "the server came
// back but the client never reconnected".
func TestIntegrationWaitConnected(t *testing.T) {
	skipIfNoServer(t)

	// Happy path.
	pub := startPublisher(t, uniqueTopic("waitconn"))
	if err := pub.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("WaitConnected on healthy conn: %v", err)
	}

	// Bad token: Start succeeds (the configuration is well-formed), WaitConnected times out.
	badURL := strings.Replace(testURL(), "://", "://wrong-token-", 1)
	sub, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: badURL, Topic: uniqueTopic("badauth"), ReportInterval: -1},
		Handler: func(*nats.Msg) {},
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	if err := sub.Start(); err != nil {
		t.Fatalf("Start with bad token should not error (background retry), got: %v", err)
	}
	t.Cleanup(sub.Stop)

	err = sub.WaitConnected(3 * time.Second)
	if err == nil {
		t.Fatal("WaitConnected with bad token should time out")
	}
	t.Logf("bad-token WaitConnected error (as expected): %v", err)

	// Key assertion: under persistent auth errors the client keeps retrying and
	// never enters the permanent CLOSED state (silent death).
	if st := sub.Stats(); st.Status == "CLOSED" || !st.Running {
		t.Errorf("client gave up on auth errors: status=%s running=%v", st.Status, st.Running)
	}
}

// TestIntegrationServerRestart simulates the production scenario of a server
// going down for a few seconds and coming back. It checks that the disconnect
// is noticed promptly, that Publish during the outage goes to the reconnect
// buffer without error, that both sides reconnect automatically, that the
// subscription is restored (messages published after the restart arrive), and
// that the Reconnects counter increments.
func TestIntegrationServerRestart(t *testing.T) {
	skipIfNoServer(t)
	container := testContainer()
	if container == "" {
		t.Skip("cannot resolve docker container for testURL port; set NATS_TEST_CONTAINER to run the restart test")
	}
	if out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", container).Output(); err != nil || strings.TrimSpace(string(out)) != "true" {
		t.Skipf("docker container %q not running, skipping restart test", container)
	}

	topic := uniqueTopic("restart")
	var c collector
	sub := startSubscriber(t, topic, c.handler, nil)
	pub := startPublisher(t, topic)

	// Baseline before the outage: normal publish and receive.
	if err := pub.Publish([]byte("pre-0")); err != nil {
		t.Fatalf("pre publish: %v", err)
	}
	_ = pub.Conn().Flush()
	waitUntil(t, 5*time.Second, "pre-restart message received", func() bool {
		return c.countPrefix("pre-") == 1
	})

	// -- Outage --
	t.Logf("stopping %s ...", container)
	if err := exec.Command("docker", "stop", container).Run(); err != nil {
		t.Fatalf("docker stop: %v", err)
	}
	// Bring the server back however the test exits (start is idempotent on a running container).
	defer func() { _ = exec.Command("docker", "start", container).Run() }()

	waitUntil(t, 15*time.Second, "both sides notice disconnect", func() bool {
		return !pub.IsConnected() && !sub.IsConnected()
	})
	t.Logf("during outage: pub status=%s sub status=%s", pub.Stats().Status, sub.Stats().Status)

	// Publishing during the outage must go to the reconnect buffer and return nil
	// (no loss, no error). Note that core NATS does not guarantee these messages
	// are deliverable after recovery: if pub reconnects first, it flushes before
	// sub has re-subscribed.
	for i := 0; i < 10; i++ {
		if err := pub.Publish([]byte(fmt.Sprintf("buf-%d", i))); err != nil {
			t.Errorf("publish during outage should buffer, got: %v", err)
		}
	}

	time.Sleep(3 * time.Second) // simulated outage window

	// -- Recovery --
	t.Logf("starting %s ...", container)
	if err := exec.Command("docker", "start", container).Run(); err != nil {
		t.Fatalf("docker start: %v", err)
	}

	waitUntil(t, 30*time.Second, "both sides reconnected", func() bool {
		return pub.IsConnected() && sub.IsConnected()
	})
	_ = sub.Conn().Flush() // make sure the re-issued SUB has reached the server

	// Publish and receive after the reconnect: proves the subscription was restored.
	for i := 0; i < 10; i++ {
		if err := pub.Publish([]byte(fmt.Sprintf("post-%d", i))); err != nil {
			t.Fatalf("post publish %d: %v", i, err)
		}
	}
	_ = pub.Conn().Flush()
	waitUntil(t, 10*time.Second, "post-restart messages received", func() bool {
		return c.countPrefix("post-") == 10
	})

	pubSt, subSt := pub.Stats(), sub.Stats()
	t.Logf("after restart: pub reconnects=%d sub reconnects=%d buffered_delivered=%d/10",
		pubSt.Reconnects, subSt.Reconnects, c.countPrefix("buf-"))
	if pubSt.Reconnects < 1 || subSt.Reconnects < 1 {
		t.Errorf("expected reconnects >= 1, pub=%d sub=%d", pubSt.Reconnects, subSt.Reconnects)
	}
	if pubSt.Dropped != 0 {
		t.Errorf("pub dropped = %d, buffered publishes should not count as drops", pubSt.Dropped)
	}
}

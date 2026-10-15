package natslink

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func permissionErr(subject, queue string) error {
	msg := fmt.Sprintf("Permissions Violation for Subscription to %q", subject)
	if queue != "" {
		msg += fmt.Sprintf(" using queue %q", queue)
	}
	return fmt.Errorf("%w: %s", nats.ErrPermissionViolation, msg)
}

// Which async errors count as a rejection of this subscription.
func TestRejectionMatching(t *testing.T) {
	plain := &Subscriber{topic: "events.block"}
	queued := &Subscriber{topic: "events.block", queueGroup: "workers"}
	escaped := &Subscriber{topic: `peer.back\slash`, queueGroup: `q"uote`}
	cases := []struct {
		name string
		s    *Subscriber
		err  error
		want bool
	}{
		{"escaped subject and queue (server formats with %q)", escaped, permissionErr(`peer.back\slash`, `q"uote`), true},
		{"escaped subject, other queue", escaped, permissionErr(`peer.back\slash`, "q"), false},
		{"own subject", plain, permissionErr("events.block", ""), true},
		{"other subject", plain, permissionErr("events.tx", ""), false},
		{"publish violation", plain, fmt.Errorf("%w: Permissions Violation for Publish to %q", nats.ErrPermissionViolation, "events.block"), false},
		{"queue group matches", queued, permissionErr("events.block", "workers"), true},
		{"queue group differs", queued, permissionErr("events.block", "other"), false},
		{"plain sub, queued error", plain, permissionErr("events.block", "workers"), false},
		{"subscription limit (not auto-recovered)", plain, nats.ErrMaxSubscriptionsExceeded, false},
		{"slow consumer", plain, nats.ErrSlowConsumer, false},
	}
	for _, c := range cases {
		if got := isSubscriptionRejection(c.err) && c.s.rejectionMatches(c.err); got != c.want {
			t.Errorf("%s: matches=%v, want %v", c.name, got, c.want)
		}
	}
	for n, want := range map[int]time.Duration{1: 2 * time.Second, 2: 4 * time.Second, 4: 16 * time.Second, 5: 30 * time.Second, 9: 30 * time.Second} {
		if got := rejectionBackoff(n); got != want {
			t.Errorf("rejectionBackoff(%d) = %v, want %v", n, got, want)
		}
	}
}

// Driving the rejection hook without a server: the hook triggers a probe
// (replaced here by a hook verdict). A rejected probe drops and unsubscribes
// the handle, Diverged shows the endpoint as unaligned, the healer runs and
// re-subscribes; an accepted probe (a stale error) and a foreign or
// stale-connection rejection change nothing; the healthy mirrored endpoint
// is untouched throughout.
func TestSubscriptionRejectionHandledWithoutServer(t *testing.T) {
	s, err := NewMirroredSubscriber([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, SubscriberOptions{
		Options: Options{Topic: "events.block", ReportInterval: -1},
		Handler: func(*nats.Msg) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	l0, l1 := s.links[0], s.links[1]
	conn0 := l0.Conn()
	if conn0 == nil {
		t.Fatal("no connection on endpoint 0")
	}
	var probes atomic.Int32
	verdict := probeAccepted
	s.testProbe = func(*nats.Conn) probeVerdict { probes.Add(1); return verdict }
	diverged := func() int { _, d := s.subState(); return d }
	state := func() (has0, has1, healing bool, rej int) {
		s.subMu.Lock()
		defer s.subMu.Unlock()
		_, has0 = s.subs[l0]
		_, has1 = s.subs[l1]
		return has0, has1, s.healing, s.rejections[l0]
	}

	// Foreign rejections never probe.
	s.onSubscriptionRejected(l0, conn0, permissionErr("events.tx", ""))
	s.onSubscriptionRejected(l0, conn0, fmt.Errorf("%w: Permissions Violation for Publish to %q", nats.ErrPermissionViolation, "events.block"))
	time.Sleep(20 * time.Millisecond)
	if probes.Load() != 0 || diverged() != 0 {
		t.Fatalf("foreign rejection probed %d times, diverged=%d", probes.Load(), diverged())
	}
	// A rejection from a connection that is not the link's current one is dropped before the probe.
	s.onSubscriptionRejected(l0, nil, permissionErr("events.block", ""))
	waitFor(t, "stale-connection probe finished", func() bool { return !l0.probing.Load() })
	if probes.Load() != 0 || diverged() != 0 {
		t.Fatalf("stale-connection rejection probed %d times, diverged=%d", probes.Load(), diverged())
	}
	// A stale error (the subject is permitted now): probe accepted, nothing changes.
	s.onSubscriptionRejected(l0, conn0, permissionErr("events.block", ""))
	waitFor(t, "accepted probe", func() bool { return probes.Load() == 1 && !l0.probing.Load() })
	if has0, has1, healing, rej := state(); !has0 || !has1 || healing || rej != 0 {
		t.Fatalf("accepted probe changed state: sub0=%v sub1=%v healing=%v rejections=%d", has0, has1, healing, rej)
	}

	// The real thing: the subject is denied on endpoint 0.
	verdict = probeRejected
	s.onSubscriptionRejected(l0, conn0, permissionErr("events.block", ""))
	waitFor(t, "endpoint 0 unaligned", func() bool { return diverged() == 1 })
	if has0, has1, healing, rej := state(); has0 || !has1 || !healing || rej != 1 {
		t.Fatalf("after rejected probe: sub0=%v sub1=%v healing=%v rejections=%d, want false/true/true/1", has0, has1, healing, rej)
	}
	// The healer re-subscribes after the rebuild backoff and converges.
	waitFor(t, "healer re-subscribed", func() bool { return diverged() == 0 })
	waitHealDone(t, s, 2*time.Second)

	// A second rejection on the same connection grows the backoff seed; a new
	// connection (rebuild) resets it.
	s.onSubscriptionRejected(l0, conn0, permissionErr("events.block", ""))
	waitFor(t, "second rejection recorded", func() bool { _, _, _, rej := state(); return rej == 2 })
	s.subMu.Lock()
	s.healing = false // stop tracking this healer for the assertion below (it exits on Stop)
	s.subMu.Unlock()
	if err := s.subscribeConn(l0, conn0); err != nil { // same conn object stands in for a rebuilt one
		t.Fatal(err)
	}
	if _, _, _, rej := state(); rej != 0 {
		t.Fatalf("rejections after a new connection setup = %d, want 0", rej)
	}
}

// A probe whose verdict is stale by the time it applies (the handle was
// replaced by Pause/Resume while the probe was in flight) must not touch the
// replacement: the generation check rejects it. The positive control, with
// nothing replaced, removes the handle.
func TestDelayedRejectionDoesNotTouchReplacementHandle(t *testing.T) {
	s, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://127.0.0.1:1", Topic: "events.block", ReportInterval: -1},
		Handler: func(*nats.Msg) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	l := s.links[0]
	conn := l.Conn()
	// The probe observes the generation (phase 1), then blocks inside the
	// network phase; its verdict is "rejected", which describes the old handle.
	release := make(chan struct{})
	entered := make(chan struct{})
	s.testProbe = func(*nats.Conn) probeVerdict { close(entered); <-release; return probeRejected }
	s.onSubscriptionRejected(l, conn, permissionErr("events.block", ""))
	<-entered

	// Meanwhile the subscription is replaced (ACL repaired, operator pauses and resumes).
	if err := s.Pause(); err != nil {
		t.Fatal(err)
	}
	if err := s.Resume(); err != nil {
		t.Fatal(err)
	}
	s.subMu.Lock()
	replacement := s.subs[l]
	s.subMu.Unlock()
	if replacement == nil {
		t.Fatal("Pause/Resume did not install a new handle")
	}
	close(release)
	waitFor(t, "probe finished", func() bool { return !l.probing.Load() })
	s.subMu.Lock()
	cur, has := s.subs[l]
	healing, rej := s.healing, s.rejections[l]
	s.subMu.Unlock()
	if !has || cur != replacement || healing || rej != 0 {
		t.Fatalf("stale probe touched the replacement: has=%v same=%v healing=%v rejections=%d", has, cur == replacement, healing, rej)
	}

	// Positive control: nothing replaced, the current handle is removed.
	s.testProbe = func(*nats.Conn) probeVerdict { return probeRejected }
	s.onSubscriptionRejected(l, conn, permissionErr("events.block", ""))
	waitFor(t, "current handle removed", func() bool { _, d := s.subState(); return d == 1 })
}

// launchNATSWithConfig starts nats-server on a config file and returns the
// process (for SIGHUP reloads) and the config path.
func launchNATSWithConfig(t *testing.T, port int, body string) (*exec.Cmd, string) {
	t.Helper()
	bin, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server binary not found in PATH")
	}
	cfg := filepath.Join(t.TempDir(), "nats.conf")
	body = fmt.Sprintf("listen: 127.0.0.1:%d\nserver_name: %s\n", port, natsServerName(port)) + body
	if err := os.WriteFile(cfg, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	// Everything in the config file: options given as command-line flags
	// cannot be reloaded (nats-server refuses the whole reload with
	// "config reload not supported for ServerName").
	cmd := exec.Command(bin, "-c", cfg)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	return cmd, cfg
}

const aclUser = "audit"

func aclConfig(subscribeAllow string) string {
	return fmt.Sprintf(`authorization {
  users: [{
    user: %s
    password: %s
    permissions: { publish: { allow: [">"] }, subscribe: { allow: [%q] } }
  }]
}
`, aclUser, aclUser, subscribeAllow)
}

// rewriteACL replaces the config with one allowing subscribe on allow,
// keeping the listen / server_name lines the reload requires unchanged.
func rewriteACL(cfg string, port int, allow string) error {
	body := fmt.Sprintf("listen: 127.0.0.1:%d\nserver_name: %s\n", port, natsServerName(port)) + aclConfig(allow)
	return os.WriteFile(cfg, []byte(body), 0o644)
}

func aclURL(port int) string { return fmt.Sprintf("nats://%s:%s@127.0.0.1:%d", aclUser, aclUser, port) }

func waitReady(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for Probe(url) != nil {
		if time.Now().After(deadline) {
			t.Fatalf("nats-server not ready on %s", redactURL(url))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A real server denies the subscription (ACL), the operator repairs the ACL
// and reloads the config: delivery resumes with no caller action. A publisher
// on the same credentials is unaffected throughout (publish permissions are
// not subscription failures).
func TestIntegrationRejectedSubscriptionRecoversAfterACLReload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	for name, topic := range map[string]string{"plain": "audit.acl", "escaped": `audit.back\slash`} {
		t.Run(name, func(t *testing.T) { testACLReloadRecovery(t, topic) })
	}
}

func testACLReloadRecovery(t *testing.T, topic string) {
	port := freeLocalPort(t)
	srv, cfg := launchNATSWithConfig(t, port, aclConfig("other.subject"))
	waitReady(t, aclURL(port))

	var got atomic.Int64
	s, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: aclURL(port), Topic: topic, ReportInterval: -1},
		Handler: func(*nats.Msg) { got.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	s.MustStart()
	defer s.Stop()
	if err := s.WaitConnected(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	// The server's -ERR arrives asynchronously; the subscriber must show the endpoint as unaligned.
	waitUntil(t, 5*time.Second, "rejection recorded as Diverged", func() bool { return s.Stats().Diverged == 1 })
	if !s.Stats().Connected {
		t.Fatal("connection should stay up while the subscription is denied")
	}

	// Repair the ACL and reload; the healer's first retry is due after 2s.
	if err := rewriteACL(cfg, port, topic); err != nil {
		t.Fatal(err)
	}
	if err := srv.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 15*time.Second, "subscription accepted after reload", func() bool { return s.Stats().Diverged == 0 })
	if err := s.Conn().FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	pub, err := nats.Connect(aclURL(port), nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	for i := 0; i < 5; i++ {
		if err := pub.Publish(topic, []byte("m")); err != nil {
			t.Fatal(err)
		}
	}
	if err := pub.Flush(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, "delivery resumed", func() bool { return got.Load() == 5 })
}

// Mirrored: one tree denies the subject, the other allows it. The healthy
// endpoint delivers from the start; after the denying server's ACL is
// repaired and reloaded, both copies arrive.
func TestIntegrationMirroredRejectionKeepsHealthyEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	topic := "audit.mirror"
	portA, portB := freeLocalPort(t), freeLocalPort(t)
	srvA, cfgA := launchNATSWithConfig(t, portA, aclConfig("other.subject"))
	launchNATSWithConfig(t, portB, aclConfig(topic))
	waitReady(t, aclURL(portA))
	waitReady(t, aclURL(portB))

	var got atomic.Int64
	s, err := NewMirroredSubscriber([]string{aclURL(portA), aclURL(portB)}, SubscriberOptions{
		Options: Options{Topic: topic, ReportInterval: -1},
		Handler: func(*nats.Msg) { got.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	s.MustStart()
	defer s.Stop()
	if err := s.WaitConnected(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, "denied endpoint unaligned", func() bool { return s.Stats().Diverged == 1 })
	if err := s.links[1].Conn().FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	publish := func(port int, n int) {
		pub, err := nats.Connect(aclURL(port), nats.Timeout(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		defer pub.Close()
		for i := 0; i < n; i++ {
			if err := pub.Publish(topic, []byte("m")); err != nil {
				t.Fatal(err)
			}
		}
		if err := pub.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	publish(portB, 3)
	waitUntil(t, 5*time.Second, "healthy endpoint delivers", func() bool { return got.Load() == 3 })

	if err := rewriteACL(cfgA, portA, topic); err != nil {
		t.Fatal(err)
	}
	if err := srvA.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 15*time.Second, "denied endpoint recovered", func() bool { return s.Stats().Diverged == 0 })
	if err := s.links[0].Conn().FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	publish(portA, 2)
	waitUntil(t, 5*time.Second, "recovered endpoint delivers", func() bool { return got.Load() == 5 })
}

// holdLogger forwards to the default logger but blocks any message containing
// hold until release is closed: used to stall nats.go's async-callback queue
// (our ConnectHandler logs through it) so an error callback queued behind it
// is delivered late, after the subscription has been replaced.
type holdLogger struct {
	hold    string
	release chan struct{}
	held    chan struct{}
	once    sync.Once
}

func (h *holdLogger) log(msg string, ctx ...any) {
	if strings.Contains(msg, h.hold) {
		h.once.Do(func() { close(h.held) })
		<-h.release
	}
	slog.Default().Info(msg, ctx...)
}
func (h *holdLogger) Info(msg string, ctx ...any)  { h.log(msg, ctx...) }
func (h *holdLogger) Warn(msg string, ctx ...any)  { h.log(msg, ctx...) }
func (h *holdLogger) Error(msg string, ctx ...any) { h.log(msg, ctx...) }

// A rejection already queued in nats.go's async callback queue before the
// subscription was replaced must not invalidate the replacement: the
// ConnectHandler's log line is held, so the initial SUB's rejection callback
// queues behind it; meanwhile the ACL is repaired and Pause/Resume installs an
// accepted handle; releasing the queue delivers the stale error, which is
// fenced by its identity against the install-time LastError.
func TestIntegrationQueuedStaleRejectionDoesNotTouchReplacement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	port := freeLocalPort(t)
	topic := "audit.stale"
	srv, cfg := launchNATSWithConfig(t, port, aclConfig("other.subject"))
	waitReady(t, aclURL(port))

	hl := &holdLogger{hold: "connected", release: make(chan struct{}), held: make(chan struct{})}
	var got atomic.Int64
	s, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: aclURL(port), Topic: topic, ReportInterval: -1, Logger: hl},
		Handler: func(*nats.Msg) { got.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	s.MustStart()
	defer s.Stop()
	select {
	case <-hl.held:
	case <-time.After(5 * time.Second):
		t.Fatal("connect callback never reached the logger")
	}
	// Detection is asynchronous (the rejection callback is what triggers the
	// probe), and that callback is now stuck behind the held log line; the
	// read loop has still parsed the server's -ERR.
	waitUntil(t, 3*time.Second, "rejection parsed by the read loop", func() bool {
		e := s.Conn().LastError()
		return e != nil && isSubscriptionRejection(e)
	})

	// Repair the ACL while the stale error callback is stuck in the queue,
	// then replace the subscription by hand.
	if err := rewriteACL(cfg, port, topic); err != nil {
		t.Fatal(err)
	}
	if err := srv.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // let the reload land
	if err := s.Pause(); err != nil {
		t.Fatal(err)
	}
	if err := s.Resume(); err != nil {
		t.Fatalf("Resume after ACL repair: %v", err)
	}
	if st := s.Stats(); st.Diverged != 0 {
		t.Fatalf("after Resume: Diverged=%d, want 0 (replacement accepted)", st.Diverged)
	}
	s.subMu.Lock()
	replacement := s.subs[s.links[0]]
	s.subMu.Unlock()
	pub, err := nats.Connect(aclURL(port), nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	publish := func(n int64) {
		for i := int64(0); i < n; i++ {
			if err := pub.Publish(topic, []byte("m")); err != nil {
				t.Fatal(err)
			}
		}
		if err := pub.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	publish(2)
	waitUntil(t, 5*time.Second, "replacement delivers", func() bool { return got.Load() == 2 })

	// Release the queue: the stale rejection is delivered now, its probe finds
	// the subject permitted, and nothing changes.
	close(hl.release)
	waitUntil(t, 3*time.Second, "stale rejection probed", func() bool { return !s.links[0].probing.Load() })
	time.Sleep(100 * time.Millisecond)
	s.subMu.Lock()
	cur := s.subs[s.links[0]]
	s.subMu.Unlock()
	if cur != replacement {
		t.Fatal("stale rejection replaced or removed the accepted handle")
	}
	if st := s.Stats(); st.Diverged != 0 {
		t.Fatalf("after the stale rejection: Diverged=%d, want 0", st.Diverged)
	}
	publish(3)
	waitUntil(t, 5*time.Second, "delivery continues", func() bool { return got.Load() == 5 })
}

// An ACL revoked while the subscription is live: on reload the server
// removes the subscription and reports it, the probe confirms the denial,
// the endpoint shows as unaligned, and a second repair recovers delivery.
func TestIntegrationRevokedSubscriptionRecovers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	port := freeLocalPort(t)
	topic := "audit.revoke"
	srv, cfg := launchNATSWithConfig(t, port, aclConfig(topic))
	waitReady(t, aclURL(port))

	var got atomic.Int64
	s, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: aclURL(port), Topic: topic, ReportInterval: -1},
		Handler: func(*nats.Msg) { got.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	s.MustStart()
	defer s.Stop()
	if err := s.WaitConnected(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := s.Conn().FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	pub, err := nats.Connect(aclURL(port), nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	publish := func(n int) {
		for i := 0; i < n; i++ {
			if err := pub.Publish(topic, []byte("m")); err != nil {
				t.Fatal(err)
			}
		}
		if err := pub.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	publish(2)
	waitUntil(t, 5*time.Second, "initial delivery", func() bool { return got.Load() == 2 })

	// Revoke: the server removes the live subscription and sends -ERR for it.
	if err := rewriteACL(cfg, port, "other.subject"); err != nil {
		t.Fatal(err)
	}
	if err := srv.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, "revocation detected", func() bool { return s.Stats().Diverged == 1 })

	// Repair again: the healer's retry is accepted.
	if err := rewriteACL(cfg, port, topic); err != nil {
		t.Fatal(err)
	}
	if err := srv.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 15*time.Second, "recovered", func() bool { return s.Stats().Diverged == 0 })
	if err := s.Conn().FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	publish(3)
	waitUntil(t, 5*time.Second, "delivery after repair", func() bool { return got.Load() == 5 })
}

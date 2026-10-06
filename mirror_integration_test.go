package natslink

// Two-server integration tests for the mirroring feature. They need a
// nats-server binary in PATH and skip automatically when it is missing (the
// same "no environment, no failure" policy as integration_test.go, but these
// tests launch their own processes and do not depend on docker).
//
// Each test starts two fully independent, unconnected nats-server instances:
// the deployment shape mirroring is built for, where a client talks to two
// local leaf nodes whose trees only meet at a remote origin cluster. In the
// tests they never meet at all, which makes the independence even stronger.
// The three core promises of mirroring are each pinned by one test:
//  1. a mirrored publish really reaches each tree (a single publish physically
//     cannot cross trees, so receipt on both is proof);
//  2. a mirrored subscription receives every copy from both trees;
//  3. when one whole tree goes down, publishing continues uninterrupted
//     (Publish stays nil, zero real drops) and recovers automatically when the
//     tree returns.

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const mirrorTestToken = "mirror-itest-token"

// freeLocalPort borrows a free port (listen, then release immediately; the
// test-level race is acceptable).
func freeLocalPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

// launchNATS starts a standalone nats-server on the given port, waits until
// it is ready, and returns a stop function.
func launchNATS(t *testing.T, port int) (stop func()) {
	t.Helper()
	bin, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server binary not found in PATH; skipping mirror integration test")
	}
	cmd := exec.Command(bin, "-a", "127.0.0.1", "-p", fmt.Sprint(port), "-n", natsServerName(port), "--auth", mirrorTestToken)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start nats-server: %v", err)
	}

	url := mirrorTestURL(port)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := Probe(url); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatalf("nats-server not ready on %s", url)
		}
		time.Sleep(50 * time.Millisecond)
	}

	var stopped atomic.Bool
	stop = func() {
		if stopped.CompareAndSwap(false, true) {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	}
	t.Cleanup(stop)
	return stop
}

func mirrorTestURL(port int) string {
	return fmt.Sprintf("nats://%s@127.0.0.1:%d", mirrorTestToken, port)
}

func natsServerName(port int) string {
	return fmt.Sprintf("itest-%d", port)
}

// flushSubscriber makes sure the subscription is active on the server before
// publishing starts. The SUB frame is written asynchronously, and WaitConnected
// only guarantees the connection is ready, not the subscription; one flush
// round trip is the synchronisation barrier. Without it, "subscribe then
// publish" relies on lucky timing and the test is flaky.
func flushSubscriber(t *testing.T, s *Subscriber) {
	t.Helper()
	for _, l := range s.links {
		conn := l.Conn()
		if conn == nil {
			t.Fatal("flushSubscriber: nil conn after WaitConnected")
		}
		if err := conn.FlushTimeout(2 * time.Second); err != nil {
			t.Fatalf("flush subscription: %v", err)
		}
	}
}

// startTwoTrees starts two independent trees and returns their URLs and stop functions.
func startTwoTrees(t *testing.T) (urlA, urlB string, stopA, stopB func()) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	portA, portB := freeLocalPort(t), freeLocalPort(t)
	stopA = launchNATS(t, portA)
	stopB = launchNATS(t, portB)
	return mirrorTestURL(portA), mirrorTestURL(portB), stopA, stopB
}

// Promise 1: a mirrored publish goes out once per tree. The two servers are not
// connected, so no single publish can reach the other tree; a plain subscriber
// on each tree receiving the full set is physical proof of the double publish.
func TestMirrorIntegrationPublishFanOut(t *testing.T) {
	urlA, urlB, _, _ := startTwoTrees(t)
	topic := uniqueTopic("mirror.fanout")

	var gotA, gotB atomic.Int32
	newSideSub := func(url string, counter *atomic.Int32) *Subscriber {
		sub, err := NewSubscriber(SubscriberOptions{
			Options: Options{URL: url, Topic: topic, ReportInterval: -1},
			Handler: func(*nats.Msg) { counter.Add(1) },
		})
		if err != nil {
			t.Fatalf("NewSubscriber: %v", err)
		}
		sub.MustStart()
		t.Cleanup(sub.Stop)
		if err := sub.WaitConnected(5 * time.Second); err != nil {
			t.Fatalf("subscriber connect: %v", err)
		}
		flushSubscriber(t, sub)
		return sub
	}
	newSideSub(urlA, &gotA)
	newSideSub(urlB, &gotB)

	pool, err := NewMirroredPublisherPool([]string{urlA, urlB}, 2, PublisherOptions{
		Options: Options{Topic: topic, Name: "itest-mirror-pub", ReportInterval: -1},
	})
	if err != nil {
		t.Fatalf("NewMirroredPublisherPool: %v", err)
	}
	pool.MustStart()
	t.Cleanup(pool.Stop)
	if err := pool.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("pool connect: %v", err)
	}

	const n = 200
	for i := 0; i < n; i++ {
		if err := pool.Publish([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	waitUntil(t, 5*time.Second, "both trees receive all messages", func() bool {
		return gotA.Load() == n && gotB.Load() == n
	})
	if pool.Dropped() != 0 {
		t.Errorf("dropped = %d, want 0", pool.Dropped())
	}
}

// Promise 2: a mirrored subscription receives every copy from both trees,
// including copies with identical content. Deduplication is the consumer's
// job; mirroring never does it on the consumer's behalf.
func TestMirrorIntegrationSubscriberBothCopies(t *testing.T) {
	urlA, urlB, _, _ := startTwoTrees(t)
	topic := uniqueTopic("mirror.subcopies")

	var got atomic.Int32
	ms, err := NewMirroredSubscriber([]string{urlA, urlB}, SubscriberOptions{
		Options: Options{Topic: topic, Name: "itest-mirror-sub", ReportInterval: -1},
		Handler: func(*nats.Msg) { got.Add(1) },
	})
	if err != nil {
		t.Fatalf("NewMirroredSubscriber: %v", err)
	}
	ms.MustStart()
	t.Cleanup(ms.Stop)
	if err := ms.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("mirrored subscriber connect: %v", err)
	}
	if st := ms.Stats(); !st.Connected {
		t.Fatalf("merged stats should be connected, got %+v", st)
	}
	flushSubscriber(t, ms)

	// Publish on each tree separately: A sends 60, B sends 40 (20 of which have
	// the same content as A's; identical copies must still be delivered one by one).
	newSidePub := func(url string) *Publisher {
		pub, err := NewPublisher(PublisherOptions{
			Options: Options{URL: url, Topic: topic, ReportInterval: -1},
		})
		if err != nil {
			t.Fatalf("NewPublisher: %v", err)
		}
		pub.MustStart()
		t.Cleanup(pub.Stop)
		return pub
	}
	pubA, pubB := newSidePub(urlA), newSidePub(urlB)
	for i := 0; i < 60; i++ {
		if err := pubA.Publish([]byte(fmt.Sprintf("same-%d", i))); err != nil {
			t.Fatalf("pubA %d: %v", i, err)
		}
	}
	for i := 0; i < 40; i++ {
		payload := fmt.Sprintf("b-only-%d", i)
		if i < 20 {
			payload = fmt.Sprintf("same-%d", i) // same content as the A side
		}
		if err := pubB.Publish([]byte(payload)); err != nil {
			t.Fatalf("pubB %d: %v", i, err)
		}
	}

	waitUntil(t, 5*time.Second, "handler receives every copy from both trees", func() bool {
		return got.Load() == 100
	})
}

// Promise 3 (the core value of the two-leaf deployment): one whole tree goes
// down and publishing continues uninterrupted (Publish stays nil, zero real
// drops at pool level, the surviving tree keeps receiving everything); when the
// tree returns, the connections reconnect and recover without any intervention.
func TestMirrorIntegrationEndpointOutageAndRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	portA, portB := freeLocalPort(t), freeLocalPort(t)
	launchNATS(t, portA)
	stopB := launchNATS(t, portB)
	urlA, urlB := mirrorTestURL(portA), mirrorTestURL(portB)
	topic := uniqueTopic("mirror.outage")

	var gotA atomic.Int32
	subA, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: urlA, Topic: topic, ReportInterval: -1},
		Handler: func(*nats.Msg) { gotA.Add(1) },
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	subA.MustStart()
	t.Cleanup(subA.Stop)
	if err := subA.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("subA connect: %v", err)
	}
	flushSubscriber(t, subA)

	pool, err := NewMirroredPublisherPool([]string{urlA, urlB}, 2, PublisherOptions{
		Options: Options{Topic: topic, Name: "itest-outage-pub", ReportInterval: -1},
	})
	if err != nil {
		t.Fatalf("NewMirroredPublisherPool: %v", err)
	}
	pool.MustStart()
	t.Cleanup(pool.Stop)
	if err := pool.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("pool connect: %v", err)
	}

	// Phase 1: both trees healthy.
	for i := 0; i < 50; i++ {
		if err := pool.Publish([]byte(fmt.Sprintf("p1-%d", i))); err != nil {
			t.Fatalf("phase1 publish %d: %v", i, err)
		}
	}
	waitUntil(t, 5*time.Second, "phase1 delivered via tree A", func() bool { return gotA.Load() == 50 })

	// Phase 2: tree B goes down entirely. Publishing must not pause (the B side
	// goes into the reconnect buffer, which counts as accepted), the surviving
	// tree A keeps receiving everything, zero real drops at pool level.
	stopB()
	waitUntil(t, 10*time.Second, "tree B members observed disconnected", func() bool {
		st := pool.Stats()
		// Endpoint 1's members are the second half of the flattened order (2 per endpoint).
		return !st[2].Connected && !st[3].Connected
	})
	for i := 0; i < 50; i++ {
		if err := pool.Publish([]byte(fmt.Sprintf("p2-%d", i))); err != nil {
			t.Fatalf("phase2 publish %d (B down): %v", i, err)
		}
	}
	waitUntil(t, 5*time.Second, "phase2 delivered via surviving tree A", func() bool { return gotA.Load() == 100 })
	if pool.Dropped() != 0 {
		t.Errorf("dropped during outage = %d, want 0 (B side goes to the reconnect buffer, not a drop)", pool.Dropped())
	}

	// Phase 3: tree B comes back on the same port, the connections recover
	// automatically (no intervention), and both trees are fully populated again.
	launchNATS(t, portB)
	if err := pool.WaitConnected(15 * time.Second); err != nil {
		t.Fatalf("pool did not recover after tree B restart: %v", err)
	}
	var gotB atomic.Int32
	subB, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: urlB, Topic: topic, ReportInterval: -1},
		Handler: func(*nats.Msg) { gotB.Add(1) },
	})
	if err != nil {
		t.Fatalf("NewSubscriber B: %v", err)
	}
	subB.MustStart()
	t.Cleanup(subB.Stop)
	if err := subB.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("subB connect: %v", err)
	}
	flushSubscriber(t, subB)
	for i := 0; i < 20; i++ {
		if err := pool.Publish([]byte(fmt.Sprintf("p3-%d", i))); err != nil {
			t.Fatalf("phase3 publish %d: %v", i, err)
		}
	}
	waitUntil(t, 5*time.Second, "phase3 delivered via both trees", func() bool {
		return gotA.Load() == 120 && gotB.Load() >= 20
	})
}

// Empirical check of the order preference for comma-separated URLs
// (DontRandomize): the initial connection must land on the first server in the
// list. Without DontRandomize, nats.go shuffles the list and connects to the
// second server about half the time, which would make the documented
// "first listed is preferred" promise false. Both directions are run 4 times
// each (8 in total) to rule out luck.
func TestMirrorIntegrationCommaOrderPreference(t *testing.T) {
	urlA, urlB, _, _ := startTwoTrees(t)
	topic := uniqueTopic("mirror.commaorder")

	check := func(commaURL, wantServerPrefix string) {
		for i := 0; i < 4; i++ {
			pub, err := NewPublisher(PublisherOptions{
				Options: Options{URL: commaURL, Topic: topic, ReportInterval: -1},
			})
			if err != nil {
				t.Fatalf("NewPublisher: %v", err)
			}
			pub.MustStart()
			if err := pub.WaitConnected(5 * time.Second); err != nil {
				pub.Stop()
				t.Fatalf("connect: %v", err)
			}
			if got := pub.Stats().Server; got != wantServerPrefix {
				pub.Stop()
				t.Fatalf("initial connection landed on %q, want %q (listed order preference broken)", got, wantServerPrefix)
			}
			pub.Stop()
		}
	}
	// Derive the server names from the ports in the URLs.
	var portA, portB int
	fmt.Sscanf(urlA[strings.LastIndex(urlA, ":")+1:], "%d", &portA)
	fmt.Sscanf(urlB[strings.LastIndex(urlB, ":")+1:], "%d", &portB)

	check(urlA+","+urlB, natsServerName(portA))
	check(urlB+","+urlA, natsServerName(portB))
}

// Subscriber side of a single-tree outage, the mixed state, and recovery
// (receiving is the other half of mirroring's value; the outage test above
// only covers publishing):
//   - tree B goes down: the merged Stats.Connected must flip to false right
//     away (the all-endpoints threshold makes degradation visible), while
//     receiving via the surviving tree A continues (degraded to a single
//     path, no data lost);
//   - tree B comes back: automatic reconnect, Connected flips back to true,
//     and copies via the B path are delivered again.
func TestMirrorIntegrationSubscriberOutageMixedStats(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	portA, portB := freeLocalPort(t), freeLocalPort(t)
	launchNATS(t, portA)
	stopB := launchNATS(t, portB)
	urlA, urlB := mirrorTestURL(portA), mirrorTestURL(portB)
	topic := uniqueTopic("mirror.suboutage")

	var got atomic.Int32
	ms, err := NewMirroredSubscriber([]string{urlA, urlB}, SubscriberOptions{
		Options: Options{Topic: topic, Name: "itest-suboutage", ReportInterval: -1},
		Handler: func(*nats.Msg) { got.Add(1) },
	})
	if err != nil {
		t.Fatalf("NewMirroredSubscriber: %v", err)
	}
	ms.MustStart()
	t.Cleanup(ms.Stop)
	if err := ms.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("connect: %v", err)
	}
	flushSubscriber(t, ms)

	pubA, err := NewPublisher(PublisherOptions{Options: Options{URL: urlA, Topic: topic, ReportInterval: -1}})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	pubA.MustStart()
	t.Cleanup(pubA.Stop)

	// Phase 1: both trees online.
	if !ms.IsConnected() || !ms.Stats().Connected {
		t.Fatal("phase1: merged Connected should be true")
	}

	// Phase 2: tree B goes down, leaving a mixed state. The merged Connected
	// flips to false (degradation is visible) while receiving via A continues.
	stopB()
	waitUntil(t, 10*time.Second, "merged Connected turns false when one tree dies", func() bool {
		return !ms.Stats().Connected
	})
	for i := 0; i < 30; i++ {
		if err := pubA.Publish([]byte(fmt.Sprintf("a-%d", i))); err != nil {
			t.Fatalf("pubA %d: %v", i, err)
		}
	}
	waitUntil(t, 5*time.Second, "degraded mirror still receives via surviving tree", func() bool {
		return got.Load() == 30
	})

	// Phase 3: tree B comes back on the same port; the mirror recovers to full
	// strength automatically and B-path copies are delivered again.
	launchNATS(t, portB)
	waitUntil(t, 15*time.Second, "merged Connected recovers after tree B restart", func() bool {
		return ms.Stats().Connected
	})
	flushSubscriber(t, ms) // barrier for the re-subscription on the B path
	pubB, err := NewPublisher(PublisherOptions{Options: Options{URL: urlB, Topic: topic, ReportInterval: -1}})
	if err != nil {
		t.Fatalf("NewPublisher B: %v", err)
	}
	pubB.MustStart()
	t.Cleanup(pubB.Stop)
	for i := 0; i < 10; i++ {
		if err := pubB.Publish([]byte(fmt.Sprintf("b-%d", i))); err != nil {
			t.Fatalf("pubB %d: %v", i, err)
		}
	}
	waitUntil(t, 5*time.Second, "B-path copies delivered after recovery", func() bool {
		return got.Load() == 40
	})
}

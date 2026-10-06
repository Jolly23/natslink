package natslink

// Full matrix test against two independent NATS servers running in docker:
//
//	docker run -d --name natslink-nats-1 -p 127.0.0.1:4222:4222 nats:latest --auth natslink-itest-token
//	docker run -d --name natslink-nats-2 -p 127.0.0.1:4223:4222 nats:latest --auth natslink-itest-token
//
// The URLs can be overridden with NATS_TEST_URL / NATS_TEST_URL2; the tests
// skip automatically when either server is unreachable. The restart and pause
// drills use the docker CLI and resolve the container by its published port
// (docker ps --filter publish=), so no container naming convention is assumed.
//
// Deployment reality: most deployments use a single connection (the plain
// constructors); mirroring is the exception. The claims in this file are
// therefore ordered by importance:
//  1. the single-connection path must be perfect: it is the baseline that
//     must never change (TestDualServerSingleConnBaseline);
//  2. when mirrored and single-connection clients share a subject, a
//     single-connection client must see exactly the same world as before:
//     exactly one copy per message, never a duplicate caused by a neighbour
//     using mirroring (TestDualServerMirrorInterop);
//  3. mirroring semantics are exact down to the copy: publish once per
//     connected tree, receive one copy per subscribed tree (every copy is
//     delivered; deduplication always belongs to the application layer);
//  4. while a neighbouring tree is restarted with docker restart: mirrored
//     publishing has zero errors and zero real drops, the surviving tree has
//     no gaps, and the restarted tree recovers automatically
//     (TestDualServerMirrorDockerRestartDrill).

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func dualURL1() string {
	if u := os.Getenv("NATS_TEST_URL"); u != "" {
		return u
	}
	return "nats://natslink-itest-token@127.0.0.1:4222"
}

func dualURL2() string {
	if u := os.Getenv("NATS_TEST_URL2"); u != "" {
		return u
	}
	return "nats://natslink-itest-token@127.0.0.1:4223"
}

func skipIfNoDualServers(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	for _, u := range []string{dualURL1(), dualURL2()} {
		if err := Probe(u); err != nil {
			t.Skipf("dual-server test needs both NATS up: %v", err)
		}
	}
}

// dockerContainerForPort resolves a container name by its published host port
// (no container naming convention is assumed).
func dockerContainerForPort(t *testing.T, hostPort int) string {
	t.Helper()
	out, err := exec.Command("docker", "ps", "--filter", fmt.Sprintf("publish=%d", hostPort), "--format", "{{.Names}}").Output()
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	name := strings.TrimSpace(string(out))
	if name == "" || strings.Contains(name, "\n") {
		t.Skipf("cannot resolve unique container for port %d (got %q)", hostPort, name)
	}
	return name
}

func hostPortOf(t *testing.T, url string) int {
	t.Helper()
	p, err := strconv.Atoi(url[strings.LastIndex(url, ":")+1:])
	if err != nil {
		t.Fatalf("parse port from %s: %v", url, err)
	}
	return p
}

// dualCollector records receipts exactly down to the copy: payload -> times received.
type dualCollector struct {
	mu     sync.Mutex
	counts map[string]int
	total  int
}

func newDualCollector() *dualCollector {
	return &dualCollector{counts: make(map[string]int)}
}

func (c *dualCollector) handler(m *nats.Msg) {
	c.mu.Lock()
	c.counts[string(m.Data)]++
	c.total++
	c.mu.Unlock()
}

func (c *dualCollector) totalCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}

// assertEachExactly asserts that every payload with the prefix was received
// exactly want times, and that there are exactly n distinct such payloads.
// "Exactly" is the soul of this file: one copy too many means mirroring
// created a duplicate inside a single tree, one too few means a lost message.
func (c *dualCollector) assertEachExactly(t *testing.T, prefix string, n, want int) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	matched := 0
	for payload, got := range c.counts {
		if !strings.HasPrefix(payload, prefix) {
			continue
		}
		matched++
		if got != want {
			t.Errorf("payload %q received %d times, want exactly %d", payload, got, want)
		}
	}
	if matched != n {
		t.Errorf("distinct payloads with prefix %q = %d, want %d", prefix, matched, n)
	}
}

// settle waits for the count to reach want, then keeps watching for a short
// while to confirm no extra "ghost copies" arrive late.
func settle(t *testing.T, desc string, want int, totalFn func() int) {
	t.Helper()
	waitUntil(t, 5*time.Second, desc, func() bool { return totalFn() == want })
	time.Sleep(300 * time.Millisecond)
	if got := totalFn(); got != want {
		t.Errorf("%s: count moved after settle: got %d, want %d (extra copies appeared)", desc, got, want)
	}
}

func newDualSub(t *testing.T, url, topic string, c *dualCollector, workers int) *Subscriber {
	t.Helper()
	sub, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: url, Topic: topic, ReportInterval: -1},
		Handler: c.handler,
		Workers: workers,
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

// -- 1. Single-connection baseline (what most deployments run on; must be perfect) --
func TestDualServerSingleConnBaseline(t *testing.T) {
	skipIfNoDualServers(t)

	t.Run("pool_delivery_exact_once", func(t *testing.T) {
		topic := uniqueTopic("dual.single.pool")
		c := newDualCollector()
		newDualSub(t, dualURL1(), topic, c, 0) // default worker pool

		pool, err := NewPublisherPool(3, PublisherOptions{
			Options: Options{URL: dualURL1(), Topic: topic, Name: "dual-single-pub", ReportInterval: -1},
		})
		if err != nil {
			t.Fatalf("NewPublisherPool: %v", err)
		}
		pool.MustStart()
		t.Cleanup(pool.Stop)
		if err := pool.WaitConnected(5 * time.Second); err != nil {
			t.Fatalf("pool connect: %v", err)
		}

		const n = 300
		for i := 0; i < n; i++ {
			if err := pool.Publish([]byte(fmt.Sprintf("sp-%06d", i))); err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
		}
		settle(t, "single-conn delivery", n, c.totalCount)
		c.assertEachExactly(t, "sp-", n, 1)
		if pool.Dropped() != 0 {
			t.Errorf("dropped = %d, want 0", pool.Dropped())
		}
		if st := pool.Stats(); len(st) != 3 || st[0].Name != "dual-single-pub-0" || !st[0].Connected {
			t.Errorf("pool stats unexpected: %+v", st[0])
		}
	})

	t.Run("workers1_strict_order", func(t *testing.T) {
		topic := uniqueTopic("dual.single.order")
		var (
			mu   sync.Mutex
			seqs []int
		)
		sub, err := NewSubscriber(SubscriberOptions{
			Options: Options{URL: dualURL1(), Topic: topic, ReportInterval: -1},
			Handler: func(m *nats.Msg) {
				v, _ := strconv.Atoi(string(m.Data))
				mu.Lock()
				seqs = append(seqs, v)
				mu.Unlock()
			},
			Workers: 1, // single worker = strict ordering (end-to-end order contract over one subscriber and one publisher connection)
		})
		if err != nil {
			t.Fatalf("NewSubscriber: %v", err)
		}
		sub.MustStart()
		t.Cleanup(sub.Stop)
		if err := sub.WaitConnected(5 * time.Second); err != nil {
			t.Fatalf("connect: %v", err)
		}
		flushSubscriber(t, sub)

		pub, err := NewPublisher(PublisherOptions{
			Options: Options{URL: dualURL1(), Topic: topic, ReportInterval: -1},
		})
		if err != nil {
			t.Fatalf("NewPublisher: %v", err)
		}
		pub.MustStart()
		t.Cleanup(pub.Stop)

		const n = 200
		for i := 0; i < n; i++ {
			if err := pub.Publish([]byte(strconv.Itoa(i))); err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
		}
		waitUntil(t, 5*time.Second, "ordered delivery", func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(seqs) == n
		})
		mu.Lock()
		defer mu.Unlock()
		for i := 1; i < len(seqs); i++ {
			if seqs[i] != seqs[i-1]+1 {
				t.Fatalf("order broken at %d: %d -> %d", i, seqs[i-1], seqs[i])
			}
		}
	})

	t.Run("sync_and_gopermessage_modes", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			workers int
		}{
			{"sync", SyncMode},
			{"go_per_message", GoPerMessage},
		} {
			t.Run(tc.name, func(t *testing.T) {
				topic := uniqueTopic("dual.single.mode." + tc.name)
				c := newDualCollector()
				newDualSub(t, dualURL1(), topic, c, tc.workers)

				pub, err := NewPublisher(PublisherOptions{
					Options: Options{URL: dualURL1(), Topic: topic, ReportInterval: -1},
				})
				if err != nil {
					t.Fatalf("NewPublisher: %v", err)
				}
				pub.MustStart()
				t.Cleanup(pub.Stop)
				const n = 100
				for i := 0; i < n; i++ {
					if err := pub.Publish([]byte(fmt.Sprintf("md-%s-%04d", tc.name, i))); err != nil {
						t.Fatalf("publish: %v", err)
					}
				}
				settle(t, tc.name+" delivery", n, c.totalCount)
				c.assertEachExactly(t, "md-"+tc.name+"-", n, 1)
			})
		}
	})
}

// -- 2/3. Interop between mirrored and single-connection clients: the world
// seen by single-connection clients must not change in the slightest --
func TestDualServerMirrorInterop(t *testing.T) {
	skipIfNoDualServers(t)
	topic := uniqueTopic("dual.interop")

	// One plain single-connection subscriber on each tree, plus one mirrored
	// subscriber spanning both.
	cA, cB, cM := newDualCollector(), newDualCollector(), newDualCollector()
	newDualSub(t, dualURL1(), topic, cA, 0)
	newDualSub(t, dualURL2(), topic, cB, 0)

	ms, err := NewMirroredSubscriber([]string{dualURL1(), dualURL2()}, SubscriberOptions{
		Options: Options{Topic: topic, Name: "dual-mirror-sub", ReportInterval: -1},
		Handler: cM.handler,
	})
	if err != nil {
		t.Fatalf("NewMirroredSubscriber: %v", err)
	}
	ms.MustStart()
	t.Cleanup(ms.Stop)
	if err := ms.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("mirrored sub connect: %v", err)
	}
	flushSubscriber(t, ms)

	// A mirrored publisher spanning both trees.
	mp, err := NewMirroredPublisherPool([]string{dualURL1(), dualURL2()}, 2, PublisherOptions{
		Options: Options{Topic: topic, Name: "dual-mirror-pub", ReportInterval: -1},
	})
	if err != nil {
		t.Fatalf("NewMirroredPublisherPool: %v", err)
	}
	mp.MustStart()
	t.Cleanup(mp.Stop)
	if err := mp.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("mirror pool connect: %v", err)
	}

	t.Run("mirror_publish", func(t *testing.T) {
		const n = 200
		for i := 0; i < n; i++ {
			if err := mp.Publish([]byte(fmt.Sprintf("mp-%06d", i))); err != nil {
				t.Fatalf("mirror publish %d: %v", i, err)
			}
		}
		// Single-connection subscribers: exactly one copy per message. A mirrored
		// publish never creates duplicates inside a single tree.
		settle(t, "tree-A single-conn node", n, cA.totalCount)
		cA.assertEachExactly(t, "mp-", n, 1)
		settle(t, "tree-B single-conn node", n, cB.totalCount)
		cB.assertEachExactly(t, "mp-", n, 1)
		// Mirrored subscriber: exactly two copies per message (one per subscribed
		// tree, every copy delivered; deduplication is the application's job).
		settle(t, "mirrored subscriber", 2*n, cM.totalCount)
		cM.assertEachExactly(t, "mp-", n, 2)
		if mp.Dropped() != 0 {
			t.Errorf("mirror pool dropped = %d, want 0", mp.Dropped())
		}
	})

	t.Run("reverse_direction", func(t *testing.T) {
		// Single-connection publishers on each tree publish independently (the
		// everyday case of one client per tree). The mirrored subscriber receives
		// the sum of both sides, each exactly once; the single-connection
		// subscribers only see their own tree.
		baseA, baseB, baseM := cA.totalCount(), cB.totalCount(), cM.totalCount()
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
		pubA, pubB := newSidePub(dualURL1()), newSidePub(dualURL2())
		for i := 0; i < 50; i++ {
			if err := pubA.Publish([]byte(fmt.Sprintf("ra-%04d", i))); err != nil {
				t.Fatalf("pubA: %v", err)
			}
		}
		for i := 0; i < 70; i++ {
			if err := pubB.Publish([]byte(fmt.Sprintf("rb-%04d", i))); err != nil {
				t.Fatalf("pubB: %v", err)
			}
		}
		settle(t, "tree-A sees only its own", baseA+50, cA.totalCount)
		cA.assertEachExactly(t, "ra-", 50, 1)
		cA.assertEachExactly(t, "rb-", 0, 0) // tree A cannot see tree B's messages
		settle(t, "tree-B sees only its own", baseB+70, cB.totalCount)
		cB.assertEachExactly(t, "rb-", 70, 1)
		settle(t, "mirrored sub sees both sums", baseM+120, cM.totalCount)
		cM.assertEachExactly(t, "ra-", 50, 1)
		cM.assertEachExactly(t, "rb-", 70, 1)
	})
}

// -- 4. docker restart drill: a neighbouring tree restarts; mirrored publishing
// has zero errors and zero real drops, the surviving tree has no gaps, and the
// restarted tree recovers automatically. This is exactly what happens in
// production when one of the two local leaf nodes is restarted. --
func TestDualServerMirrorDockerRestartDrill(t *testing.T) {
	skipIfNoDualServers(t)
	container := dockerContainerForPort(t, hostPortOf(t, dualURL2()))
	topic := uniqueTopic("dual.restart")

	cA := newDualCollector()
	newDualSub(t, dualURL1(), topic, cA, 0)

	mp, err := NewMirroredPublisherPool([]string{dualURL1(), dualURL2()}, 2, PublisherOptions{
		Options: Options{Topic: topic, Name: "dual-restart-pub", ReportInterval: -1},
	})
	if err != nil {
		t.Fatalf("NewMirroredPublisherPool: %v", err)
	}
	mp.MustStart()
	t.Cleanup(mp.Stop)
	if err := mp.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("mirror pool connect: %v", err)
	}

	// Background publisher: publishes continuously through the whole restart
	// and records every error.
	var (
		pubMu   sync.Mutex
		pubErrs []error
		seq     int
	)
	stopPub := make(chan struct{})
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPub:
				return
			case <-ticker.C:
				pubMu.Lock()
				payload := fmt.Sprintf("dr-%06d", seq)
				seq++
				pubMu.Unlock()
				if err := mp.Publish([]byte(payload)); err != nil {
					pubMu.Lock()
					pubErrs = append(pubErrs, fmt.Errorf("%s: %w", payload, err))
					pubMu.Unlock()
				}
			}
		}
	}()

	time.Sleep(300 * time.Millisecond) // steady-state warm-up
	t.Logf("restarting container %q (tree B) ...", container)
	if out, err := exec.Command("docker", "restart", "-t", "1", container).CombinedOutput(); err != nil {
		close(stopPub)
		<-pubDone
		t.Fatalf("docker restart: %v (%s)", err, out)
	}
	if err := mp.WaitConnected(30 * time.Second); err != nil {
		close(stopPub)
		<-pubDone
		t.Fatalf("mirror pool did not recover after restart: %v", err)
	}
	time.Sleep(300 * time.Millisecond) // keep running for a while after recovery
	close(stopPub)
	<-pubDone

	pubMu.Lock()
	total := seq
	errs := append([]error(nil), pubErrs...)
	pubMu.Unlock()

	// Claim 1: not a single publish failed during the whole restart window
	// (the B side goes to the reconnect buffer, which counts as accepted).
	if len(errs) != 0 {
		t.Errorf("publish errors during restart: %d, first: %v", len(errs), errs[0])
	}
	if mp.Dropped() != 0 {
		t.Errorf("mirror dropped = %d, want 0", mp.Dropped())
	}
	// Claim 2: the surviving tree (A) has no gaps at all. A neighbouring tree's
	// restart has zero effect on this tree and on its single-connection subscribers.
	settle(t, "tree-A received every message through the drill", total, cA.totalCount)
	cA.assertEachExactly(t, "dr-", total, 1)

	// Claim 3: the restarted tree (B) recovers automatically and new messages
	// arrive as usual. Only the post- prefix published after the restart counts,
	// strictly separated from older messages replayed from B's reconnect buffer.
	cB := newDualCollector()
	newDualSub(t, dualURL2(), topic, cB, 0)
	for i := 0; i < 10; i++ {
		if err := mp.Publish([]byte(fmt.Sprintf("post-%02d", i))); err != nil {
			t.Fatalf("post-restart publish: %v", err)
		}
	}
	waitUntil(t, 5*time.Second, "tree-B delivers after recovery", func() bool {
		cB.mu.Lock()
		defer cB.mu.Unlock()
		n := 0
		for p, c := range cB.counts {
			if strings.HasPrefix(p, "post-") {
				n += c
			}
		}
		return n == 10
	})
	cB.assertEachExactly(t, "post-", 10, 1)
}

// -- 5. docker pause drill (regression for a production shutdown hang) --
// The peer is a stalled process: frozen, the TCP connection stays open, no ACKs
// come back. The clean RST produced by docker restart does not cover this shape
// (the restart drill passed 30 of 30 runs while the pause shape hung Stop in
// 11 of 12 runs). Mechanism: nats.go's flusher/publish path writes to the
// socket synchronously while holding the connection lock; once the peer stops
// reading, the write blocks until the write deadline, and the first thing
// Stop's FlushTimeout/Close does is take that same lock. Before the fix the
// write deadline was the default 60s and Stop waited for the lock without
// bound; with 8+2 connections stopping serially, Stop measured 117s (in
// production that exceeded the service manager's stop budget and ended in
// SIGKILL). After the fix, flusherTimeout=5s shortens the time the lock is
// held, link.stop's stopGrace gives up via a side path, and pool members stop
// in parallel, so shutdown must be bounded.
func TestDualServerStallPeerBoundedStop(t *testing.T) {
	skipIfNoDualServers(t)
	container := dockerContainerForPort(t, hostPortOf(t, dualURL2()))
	topic := uniqueTopic("dual.stall")

	// Shape of the affected production service: two publisher pools (6+2) plus
	// one worker-pool subscriber, all pointed at tree B, which is about to be frozen.
	newPool := func(n int, name string) *PublisherPool {
		pool, err := NewPublisherPool(n, PublisherOptions{
			Options: Options{URL: dualURL2(), Topic: topic, Name: name, ReportInterval: -1},
		})
		if err != nil {
			t.Fatalf("NewPublisherPool(%s): %v", name, err)
		}
		pool.MustStart()
		if err := pool.WaitConnected(5 * time.Second); err != nil {
			t.Fatalf("%s connect: %v", name, err)
		}
		return pool
	}
	txPool := newPool(6, "stall-pub-tx")
	blockPool := newPool(2, "stall-pub-blk")

	sub, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: dualURL2(), Topic: topic, Name: "stall-sub", ReportInterval: -1},
		Handler: func(*nats.Msg) {},
		Workers: 8,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	sub.MustStart()
	if err := sub.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("sub connect: %v", err)
	}

	// Freeze tree B. It must be unpaused however the test ends, otherwise the
	// following tests and the other half of the matrix are affected.
	t.Logf("pausing container %q ...", container)
	if out, err := exec.Command("docker", "pause", container).CombinedOutput(); err != nil {
		t.Fatalf("docker pause: %v (%s)", err, out)
	}
	t.Cleanup(func() {
		if out, err := exec.Command("docker", "unpause", container).CombinedOutput(); err != nil {
			t.Errorf("docker unpause: %v (%s)", err, out)
		}
	})

	// After the freeze, pump 64KB payloads (above nats.go's 32KB inline flush
	// threshold, so the publishing goroutine itself writes to the socket while
	// holding the lock). This fills each connection's kernel send buffer and
	// produces the blocked write under lock that the incident showed. The pump
	// goroutines may block in that write (for at most flusherTimeout); still
	// being in flight while Stop runs is the realistic scenario. After Stop,
	// Publish returns ErrStopped immediately and the pumps exit on their own.
	big := make([]byte, 64*1024)
	stopPump := make(chan struct{})
	var pumpWG sync.WaitGroup
	for _, pool := range []*PublisherPool{txPool, blockPool} {
		pumpWG.Add(1)
		go func(pool *PublisherPool) {
			defer pumpWG.Done()
			for {
				select {
				case <-stopPump:
					return
				default:
					_ = pool.Publish(big)
				}
			}
		}(pool)
	}
	time.Sleep(1500 * time.Millisecond) // let the write path wedge on the frozen socket for real

	// -- Claim under test: Stop still returns within a bound while the peer is stalled --
	// Budget: three components stop serially, each internally parallel, with the
	// per-connection stopGrace=5s as the backstop, so the theoretical ceiling is
	// ~15s plus scheduling slack. Before the fix this was on the order of 60s
	// (the write deadline) multiplied by the serial chain.
	stopStart := time.Now()
	stopDone := make(chan struct{})
	go func() {
		txPool.Stop()
		blockPool.Stop()
		sub.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(25 * time.Second):
		t.Fatal("Stop exceeded 25s under stalled peer: unbounded shutdown regression")
	}
	elapsed := time.Since(stopStart)
	t.Logf("stop under stalled peer took %s", elapsed)
	if elapsed > 20*time.Second {
		t.Errorf("stop took %s, want < 20s (3 components × stopGrace + slack)", elapsed)
	}

	close(stopPump)
	pumpWG.Wait() // Publish on a stopped pool returns immediately, so the pumps exit at once; a hang here would itself be a defect
}

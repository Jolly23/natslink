package natslink

// Edge cases for Pause / Resume (added during the v1.5.0 review). They also run
// against the local docker NATS: single-server cases use NATS_TEST_URL; the
// mirrored case additionally needs NATS_TEST_URL2 (a second, independent server).
// A test skips itself when a server it needs is unreachable.

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Pause may precede Start: Start then connects without subscribing, and only
// Resume performs the subscription.
func TestPauseBeforeStartThenResume(t *testing.T) {
	skipIfNoServer(t)
	topic := fmt.Sprintf("natslink.test.pause.prestart.%d", time.Now().UnixNano())
	var got atomic.Int64
	s, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: testURL(), Topic: topic, ReportInterval: -1},
		Handler: func(*nats.Msg) { got.Add(1) },
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	if err := s.Pause(); err != nil {
		t.Fatalf("Pause before Start: %v", err)
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()
	if err := s.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("WaitConnected: %v", err)
	}
	pub, err := nats.Connect(testURL(), nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("connect publisher: %v", err)
	}
	defer pub.Close()

	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 3)
	time.Sleep(200 * time.Millisecond)
	if n := got.Load(); n != 0 {
		t.Fatalf("paused-before-start subscriber received %d messages", n)
	}
	if st := s.Stats(); !st.Paused || !st.Connected {
		t.Fatalf("stats = %+v, want Paused && Connected", st)
	}
	if err := s.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 2)
	waitCount(t, &got, 2, 3*time.Second)
}

// forceRebuild closes the underlying connection outright, which fires the
// ClosedHandler and triggers a rebuild (the self-heal path for a permanently closed
// connection), then waits for the rebuild to complete (Rebuilds counter +1 and the
// new connection ready).
func forceRebuild(t *testing.T, s *Subscriber) {
	t.Helper()
	before := s.Stats().Rebuilds
	conn := s.Conn()
	if conn == nil {
		t.Fatal("no conn to close")
	}
	conn.Close()
	deadline := time.Now().Add(20 * time.Second) // the rebuild starts with a 2s backoff
	for {
		if st := s.Stats(); st.Rebuilds == before+1 && st.Connected {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("rebuild did not complete: %+v", s.Stats())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Connection closed permanently while paused: the new connection swapped in by the
// rebuild must not subscribe, and Resume must be able to subscribe on it. A second
// Pause then confirms that what gets removed is the subscription on the new
// connection, i.e. the subscriber is still controllable after a rebuild.
func TestPauseSurvivesRebuildAndResumesOnNewConn(t *testing.T) {
	skipIfNoServer(t)
	topic := fmt.Sprintf("natslink.test.pause.rebuild.%d", time.Now().UnixNano())
	s, pub, got := pauseFixture(t, topic)

	publishAndFlush(t, pub, topic, 1)
	waitCount(t, got, 1, 3*time.Second)

	if err := s.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	forceRebuild(t, s)
	if !s.IsPaused() {
		t.Fatal("paused flag lost across rebuild")
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 3)
	time.Sleep(300 * time.Millisecond)
	if n := got.Load(); n != 1 {
		t.Fatalf("rebuilt connection delivered while paused: got %d, want 1", n)
	}

	if err := s.Resume(); err != nil {
		t.Fatalf("Resume after rebuild: %v", err)
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 2)
	waitCount(t, got, 3, 3*time.Second)

	// Pause again: what gets removed must be the subscription on the new connection
	if err := s.Pause(); err != nil {
		t.Fatalf("second Pause: %v", err)
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 4)
	time.Sleep(300 * time.Millisecond)
	if n := got.Load(); n != 3 {
		t.Fatalf("second pause ineffective on rebuilt conn: got %d, want 3", n)
	}
	if st := s.Stats(); st.Pauses != 2 || st.Rebuilds != 1 {
		t.Fatalf("stats = %+v, want Pauses=2 Rebuilds=1", st)
	}
}

// Rebuild while subscribed: the subscription is restored automatically (the existing
// promise), and a later Pause can still remove it (the subs table follows the new
// connection).
func TestRebuildWhileSubscribedThenPause(t *testing.T) {
	skipIfNoServer(t)
	topic := fmt.Sprintf("natslink.test.pause.rebuild2.%d", time.Now().UnixNano())
	s, pub, got := pauseFixture(t, topic)

	forceRebuild(t, s)
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 2)
	waitCount(t, got, 2, 3*time.Second)

	if err := s.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 3)
	time.Sleep(300 * time.Millisecond)
	if n := got.Load(); n != 2 {
		t.Fatalf("pause after rebuild ineffective: got %d, want 2", n)
	}
}

// Queue-group subscriptions can be paused / resumed just the same.
func TestPauseResumeQueueGroup(t *testing.T) {
	skipIfNoServer(t)
	topic := fmt.Sprintf("natslink.test.pause.queue.%d", time.Now().UnixNano())
	var got atomic.Int64
	s, err := NewSubscriber(SubscriberOptions{
		Options:    Options{URL: testURL(), Topic: topic, ReportInterval: -1},
		Handler:    func(*nats.Msg) { got.Add(1) },
		QueueGroup: "qg",
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()
	if err := s.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("WaitConnected: %v", err)
	}
	pub, err := nats.Connect(testURL(), nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("connect publisher: %v", err)
	}
	defer pub.Close()

	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 2)
	waitCount(t, &got, 2, 3*time.Second)
	if err := s.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 3)
	time.Sleep(200 * time.Millisecond)
	if n := got.Load(); n != 2 {
		t.Fatalf("queue-group pause ineffective: got %d", n)
	}
	if err := s.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 1)
	waitCount(t, &got, 3, 3*time.Second)
}

// Concurrent, interleaved Pause / Resume calls (under -race): no panic or deadlock,
// the final state matches the last call, and the subs table agrees with the paused
// flag (paused: no subscription handles; resumed: exactly one per endpoint).
func TestPauseResumeConcurrentHammer(t *testing.T) {
	skipIfNoServer(t)
	topic := fmt.Sprintf("natslink.test.pause.hammer.%d", time.Now().UnixNano())
	s, pub, got := pauseFixture(t, topic)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if (i+j)%2 == 0 {
					_ = s.Pause()
				} else {
					_ = s.Resume()
				}
			}
		}(i)
	}
	wg.Wait()
	if err := s.Resume(); err != nil {
		t.Fatalf("final Resume: %v", err)
	}
	s.subMu.Lock()
	paused, nsubs := s.paused, len(s.subs)
	s.subMu.Unlock()
	if paused || nsubs != len(s.links) {
		t.Fatalf("after hammer+Resume: paused=%v subs=%d links=%d", paused, nsubs, len(s.links))
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 2)
	waitCount(t, got, 2, 3*time.Second)

	if err := s.Pause(); err != nil {
		t.Fatalf("final Pause: %v", err)
	}
	s.subMu.Lock()
	paused, nsubs = s.paused, len(s.subs)
	s.subMu.Unlock()
	if !paused || nsubs != 0 {
		t.Fatalf("after final Pause: paused=%v subs=%d", paused, nsubs)
	}
}

// Mirrored subscription (two independent docker NATS servers): Pause unsubscribes
// from both endpoints at once, Resume restores both, and every copy arrives as
// usual (exactly one subscription handle per endpoint afterwards).
func TestMirroredPauseResumeDualServers(t *testing.T) {
	skipIfNoDualServers(t)
	topic := fmt.Sprintf("natslink.test.pause.mirror.%d", time.Now().UnixNano())
	var got atomic.Int64
	s, err := NewMirroredSubscriber([]string{dualURL1(), dualURL2()}, SubscriberOptions{
		Options: Options{URL: dualURL1(), Topic: topic, ReportInterval: -1},
		Handler: func(*nats.Msg) { got.Add(1) },
	})
	if err != nil {
		t.Fatalf("NewMirroredSubscriber: %v", err)
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()
	if err := s.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("WaitConnected: %v", err)
	}
	pubA, err := nats.Connect(dualURL1(), nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("connect A: %v", err)
	}
	defer pubA.Close()
	pubB, err := nats.Connect(dualURL2(), nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("connect B: %v", err)
	}
	defer pubB.Close()
	flushAll := func() {
		for _, l := range s.links {
			if c := l.Conn(); c != nil {
				_ = c.Flush()
			}
		}
	}

	flushAll()
	publishAndFlush(t, pubA, topic, 2)
	publishAndFlush(t, pubB, topic, 3)
	waitCount(t, &got, 5, 3*time.Second)

	if err := s.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	flushAll()
	publishAndFlush(t, pubA, topic, 2)
	publishAndFlush(t, pubB, topic, 2)
	time.Sleep(300 * time.Millisecond)
	if n := got.Load(); n != 5 {
		t.Fatalf("mirrored pause leaked: got %d, want 5", n)
	}
	s.subMu.Lock()
	nsubs := len(s.subs)
	s.subMu.Unlock()
	if nsubs != 0 {
		t.Fatalf("subs left after pause: %d", nsubs)
	}

	if err := s.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	flushAll()
	publishAndFlush(t, pubA, topic, 1)
	publishAndFlush(t, pubB, topic, 1)
	waitCount(t, &got, 7, 3*time.Second)
	s.subMu.Lock()
	nsubs = len(s.subs)
	s.subMu.Unlock()
	if nsubs != 2 {
		t.Fatalf("subs after resume: %d, want 2 (one per endpoint)", nsubs)
	}
}

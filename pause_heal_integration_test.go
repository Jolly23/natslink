package natslink

// Self-healing after Pause / Resume fails on some endpoints (v1.5.1). Runs against
// the local docker NATS (NATS_TEST_URL, default
// nats://natslink-itest-token@127.0.0.1:4222; skipped when unreachable). The test
// hooks testSubscribeErr / testUnsubscribeErr inject SUB / UNSUB failures while the
// connection stays healthy and no rebuild is triggered: in v1.5.0 a failed endpoint
// stayed unsubscribed (or kept a stale subscription) for good; since v1.5.1 a heal
// goroutine converges it.

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

var errInjected = errors.New("injected failure")

// failTimes returns a hook that fails with errInjected for the first n calls and
// returns nil afterwards.
func failTimes(n int64) func() error {
	var left atomic.Int64
	left.Store(n)
	return func() error {
		if left.Add(-1) >= 0 {
			return errInjected
		}
		return nil
	}
}

func waitDiverged(t *testing.T, s *Subscriber, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if st := s.Stats(); st.Diverged == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Diverged did not reach %d within %v: %+v", want, timeout, s.Stats())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitHealDone polls until the heal goroutine has exited (healing == false).
func waitHealDone(t *testing.T, s *Subscriber, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		s.subMu.Lock()
		healing := s.healing
		s.subMu.Unlock()
		if !healing {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("heal goroutine still running after %v: %+v", timeout, s.Stats())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// SUB fails on Resume (connection healthy): the paused state is still cleared, the
// error is returned and Stats.Diverged=1. The heal goroutine retries with backoff:
// the second attempt fails, the third succeeds, after which delivery resumes and
// Diverged drops back to zero.
func TestResumeSubscribeFailureHeals(t *testing.T) {
	skipIfNoServer(t)
	topic := fmt.Sprintf("natslink.test.pause.heal.resume.%d", time.Now().UnixNano())
	s, pub, got := pauseFixture(t, topic)

	if err := s.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	s.subMu.Lock()
	s.testSubscribeErr = failTimes(2) // one for Resume + one for the heal goroutine's first round
	s.subMu.Unlock()

	if err := s.Resume(); !errors.Is(err, errInjected) {
		t.Fatalf("Resume err = %v, want injected failure", err)
	}
	if st := s.Stats(); st.Paused || st.Diverged != 1 || !st.Connected {
		t.Fatalf("after failed Resume stats = %+v, want !Paused && Diverged=1 && Connected", st)
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 2)
	time.Sleep(200 * time.Millisecond)
	if n := got.Load(); n != 0 {
		t.Fatalf("delivered %d messages with no subscription", n)
	}

	// Heal: the first round fails at 2s, the second succeeds 4s later; leave plenty of slack.
	waitDiverged(t, s, 0, 15*time.Second)
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 3)
	waitCount(t, got, 3, 3*time.Second)

	// The state machine still works after convergence: one more Pause / Resume cycle.
	if err := s.Pause(); err != nil {
		t.Fatalf("Pause after heal: %v", err)
	}
	if err := s.Resume(); err != nil {
		t.Fatalf("Resume after heal: %v", err)
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 1)
	waitCount(t, got, 4, 3*time.Second)
}

// UNSUB fails on Pause: the handle stays on the books (v1.5.0 dropped the record
// before issuing the UNSUB, so a failure left a stale subscription nobody would ever
// remove), Diverged=1. Once the heal goroutine's retry removes it, nothing is
// delivered any more.
func TestPauseUnsubscribeFailureHeals(t *testing.T) {
	skipIfNoServer(t)
	topic := fmt.Sprintf("natslink.test.pause.heal.pause.%d", time.Now().UnixNano())
	s, pub, got := pauseFixture(t, topic)

	publishAndFlush(t, pub, topic, 1)
	waitCount(t, got, 1, 3*time.Second)

	s.subMu.Lock()
	s.testUnsubscribeErr = failTimes(1)
	s.subMu.Unlock()
	if err := s.Pause(); !errors.Is(err, errInjected) {
		t.Fatalf("Pause err = %v, want injected failure", err)
	}
	if st := s.Stats(); !st.Paused || st.Diverged != 1 {
		t.Fatalf("after failed Pause stats = %+v, want Paused && Diverged=1", st)
	}

	waitDiverged(t, s, 0, 10*time.Second)
	roundTrip(t, s)
	before := got.Load()
	publishAndFlush(t, pub, topic, 3)
	time.Sleep(300 * time.Millisecond)
	if n := got.Load(); n != before {
		t.Fatalf("delivered %d messages after heal unsubscribed, want 0", n-before)
	}
}

// State flips while the heal goroutine is running: Pause immediately after a failed
// Resume. The heal goroutine must converge on the latest state (paused, no
// subscription) and must not bring the subscription back.
func TestHealFollowsLatestState(t *testing.T) {
	skipIfNoServer(t)
	topic := fmt.Sprintf("natslink.test.pause.heal.flip.%d", time.Now().UnixNano())
	s, pub, got := pauseFixture(t, topic)

	if err := s.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	s.subMu.Lock()
	s.testSubscribeErr = failTimes(1)
	s.subMu.Unlock()
	if err := s.Resume(); !errors.Is(err, errInjected) {
		t.Fatalf("Resume err = %v, want injected failure", err)
	}
	if err := s.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if st := s.Stats(); st.Diverged != 0 {
		t.Fatalf("paused with no sub should be aligned: %+v", st)
	}
	waitHealDone(t, s, 2*rebuildBackoff+2*time.Second) // the heal goroutine's first wake-up sees the paused state, converges and exits
	waitDiverged(t, s, 0, time.Second)
	s.subMu.Lock()
	subs := len(s.subs)
	s.subMu.Unlock()
	if subs != 0 {
		t.Fatalf("heal after flip left %d subscriptions, want 0", subs)
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 2)
	time.Sleep(200 * time.Millisecond)
	if n := got.Load(); n != 0 {
		t.Fatalf("delivered %d messages while paused", n)
	}
}

// Stop while the heal goroutine is retrying: it exits as soon as done is closed
// (without waiting out the backoff), and Pause / Resume return ErrStopped afterwards.
func TestHealExitsOnStop(t *testing.T) {
	skipIfNoServer(t)
	topic := fmt.Sprintf("natslink.test.pause.heal.stop.%d", time.Now().UnixNano())
	s, _, _ := pauseFixture(t, topic)

	if err := s.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	s.subMu.Lock()
	s.testSubscribeErr = func() error { return errInjected } // always fails: keeps the heal goroutine retrying
	s.subMu.Unlock()
	if err := s.Resume(); !errors.Is(err, errInjected) {
		t.Fatalf("Resume err = %v, want injected failure", err)
	}
	s.subMu.Lock()
	healing := s.healing
	s.subMu.Unlock()
	if !healing {
		t.Fatal("heal goroutine not started after failed Resume")
	}
	time.Sleep(rebuildBackoff + 500*time.Millisecond) // at least one failed retry, now inside the 4s backoff
	s.Stop()
	waitHealDone(t, s, 2*time.Second)
	if err := s.Resume(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Resume after Stop = %v, want ErrStopped", err)
	}
}

// The connection is rebuilt while the heal goroutine is retrying: the new
// connection's setup subscribes because the subscriber is not paused, and the heal
// goroutine's next round finds everything aligned and exits. There must be no double
// subscription (each message is delivered exactly once).
func TestHealDuringRebuild(t *testing.T) {
	skipIfNoServer(t)
	topic := fmt.Sprintf("natslink.test.pause.heal.rebuild.%d", time.Now().UnixNano())
	s, pub, got := pauseFixture(t, topic)

	if err := s.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	s.subMu.Lock()
	s.testSubscribeErr = func() error { return errInjected }
	s.subMu.Unlock()
	if err := s.Resume(); !errors.Is(err, errInjected) {
		t.Fatalf("Resume err = %v, want injected failure", err)
	}
	// Lift the injection and close the connection right away: the rebuild's setup
	// subscribes on the new connection before the heal goroutine's first round (2s).
	s.subMu.Lock()
	s.testSubscribeErr = nil
	s.subMu.Unlock()
	forceRebuild(t, s)
	waitDiverged(t, s, 0, 2*time.Second)
	waitHealDone(t, s, 4*rebuildBackoff+2*time.Second)

	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 3)
	waitCount(t, got, 3, 3*time.Second)
	time.Sleep(300 * time.Millisecond)
	if n := got.Load(); n != 3 {
		t.Fatalf("received %d, want exactly 3 (double subscription?)", n)
	}
}

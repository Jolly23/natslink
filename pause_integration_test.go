package natslink

// Integration tests for Pause / Resume (v1.5.0). They reuse the server
// conventions of integration_test.go: the target is a local docker NATS reachable
// via NATS_TEST_URL (default nats://natslink-itest-token@127.0.0.1:4222) and the
// tests skip themselves when it is unreachable. The restart test additionally
// needs the docker container (NATS_TEST_CONTAINER), which it restarts.

import (
	"errors"
	"fmt"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// pauseFixture starts one Subscriber plus a bare publisher connection and returns
// both together with the received-message counter.
func pauseFixture(t *testing.T, topic string) (*Subscriber, *nats.Conn, *atomic.Int64) {
	t.Helper()
	var got atomic.Int64
	s, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: testURL(), Topic: topic, ReportInterval: -1},
		Handler: func(*nats.Msg) { got.Add(1) },
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := s.WaitConnected(5 * time.Second); err != nil {
		t.Fatalf("WaitConnected: %v", err)
	}
	pub, err := nats.Connect(testURL(), nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("connect publisher: %v", err)
	}
	t.Cleanup(func() { pub.Close(); s.Stop() })
	return s, pub, &got
}

// roundTrip makes the subscriber connection complete one PING/PONG exchange with
// the server, which guarantees that every SUB / UNSUB written before it has been
// processed server-side. Without it, "messages still arrive after Pause" could
// simply mean the UNSUB was still in flight.
func roundTrip(t *testing.T, s *Subscriber) {
	t.Helper()
	if c := s.Conn(); c != nil {
		if err := c.Flush(); err != nil {
			t.Fatalf("flush subscriber conn: %v", err)
		}
	}
}

func publishAndFlush(t *testing.T, pub *nats.Conn, topic string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := pub.Publish(topic, []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if err := pub.Flush(); err != nil {
		t.Fatalf("flush publisher: %v", err)
	}
}

func waitCount(t *testing.T, got *atomic.Int64, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for got.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("received %d, want %d", got.Load(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPauseResumeStopsDeliveryAndRestores(t *testing.T) {
	skipIfNoServer(t)
	topic := fmt.Sprintf("natslink.test.pause.%d", time.Now().UnixNano())
	s, pub, got := pauseFixture(t, topic)

	publishAndFlush(t, pub, topic, 3)
	waitCount(t, got, 3, 3*time.Second)
	if s.IsPaused() || s.Stats().Paused {
		t.Fatal("fresh subscriber reports paused")
	}

	// -- Pause: the server stops delivering, the connection stays up --
	if err := s.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if err := s.Pause(); err != nil { // idempotent
		t.Fatalf("second Pause: %v", err)
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 5)
	time.Sleep(300 * time.Millisecond)
	if n := got.Load(); n != 3 {
		t.Fatalf("paused subscriber still received messages: got %d, want 3", n)
	}
	st := s.Stats()
	if !st.Paused || st.Pauses != 1 || !st.Connected || !s.IsConnected() {
		t.Fatalf("paused stats = %+v (want Paused=true Pauses=1 Connected=true)", st)
	}

	// -- Resume: messages flow to the same Handler again --
	if err := s.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if err := s.Resume(); err != nil { // idempotent
		t.Fatalf("second Resume: %v", err)
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 2)
	waitCount(t, got, 5, 3*time.Second)
	if st := s.Stats(); st.Paused || st.Pauses != 1 {
		t.Fatalf("resumed stats = %+v", st)
	}

	// -- After Stop neither transition is allowed --
	s.Stop()
	if err := s.Pause(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Pause after Stop = %v, want ErrStopped", err)
	}
	if err := s.Resume(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Resume after Stop = %v, want ErrStopped", err)
	}
}

// The paused state must survive a nats.go auto-reconnect (server restart): the
// reconnect replays only the subscriptions still on the books, so one that was
// UNSUBed must not come back to life. Resume afterwards must be able to subscribe
// on the reconnected connection.
func TestPauseSurvivesServerRestart(t *testing.T) {
	skipIfNoServer(t)
	container := testContainer()
	if container == "" {
		t.Skip("no docker container resolved for the test server; set NATS_TEST_CONTAINER")
	}
	topic := fmt.Sprintf("natslink.test.pause.restart.%d", time.Now().UnixNano())
	s, pub, got := pauseFixture(t, topic)

	publishAndFlush(t, pub, topic, 1)
	waitCount(t, got, 1, 3*time.Second)
	if err := s.Pause(); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	roundTrip(t, s)

	if out, err := exec.Command("docker", "restart", container).CombinedOutput(); err != nil {
		t.Fatalf("docker restart %s: %v\n%s", container, err, out)
	}
	if err := s.WaitConnected(30 * time.Second); err != nil {
		t.Fatalf("subscriber did not reconnect: %v", err)
	}
	if !s.IsPaused() {
		t.Fatal("paused flag lost across reconnect")
	}
	// the publisher connection has to reconnect as well
	deadline := time.Now().Add(30 * time.Second)
	for !pub.IsConnected() {
		if time.Now().After(deadline) {
			t.Fatal("publisher did not reconnect")
		}
		time.Sleep(50 * time.Millisecond)
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 3)
	time.Sleep(300 * time.Millisecond)
	if n := got.Load(); n != 1 {
		t.Fatalf("paused subscription revived after reconnect: got %d, want 1", n)
	}

	if err := s.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	roundTrip(t, s)
	publishAndFlush(t, pub, topic, 2)
	waitCount(t, got, 3, 3*time.Second)
}

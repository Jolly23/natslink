package natslink

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// gatedDialer is a nats.CustomDialer whose Dial waits on a gate before
// dialing for real, and records every net.Conn it hands out so a test can
// check they were closed. It only controls scheduling; the socket is real.
type gatedDialer struct {
	entered chan struct{} // closed when the first Dial is waiting on the gate
	gate    chan struct{} // closed by the test to let the dial proceed
	once    sync.Once
	mu      sync.Mutex
	conns   []*trackedConn
}

func newGatedDialer() *gatedDialer {
	return &gatedDialer{entered: make(chan struct{}), gate: make(chan struct{})}
}

func (d *gatedDialer) Dial(network, address string) (net.Conn, error) {
	d.once.Do(func() { close(d.entered) })
	<-d.gate
	c, err := net.DialTimeout(network, address, 2*time.Second)
	if err != nil {
		return nil, err
	}
	tc := &trackedConn{Conn: c}
	d.mu.Lock()
	d.conns = append(d.conns, tc)
	d.mu.Unlock()
	return tc, nil
}

func (d *gatedDialer) release() { close(d.gate) }

// liveConns reports how many dialed sockets are still open.
func (d *gatedDialer) liveConns() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, c := range d.conns {
		if !c.closed.Load() {
			n++
		}
	}
	return n
}

type trackedConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *trackedConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func waitEntered(t *testing.T, d *gatedDialer) {
	t.Helper()
	select {
	case <-d.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("dialer never entered Dial")
	}
}

// Stop during the initial dial: Start returns ErrStopped, no connection is
// installed and WaitConnected reports the stop. No server is needed: the
// gated dial targets an unreachable port, and with RetryOnFailedConnect
// nats.Connect still returns a (retrying) connection, which start must
// discard.
func TestPublisherStopDuringInitialDial(t *testing.T) {
	d := newGatedDialer()
	p, err := NewPublisher(PublisherOptions{Options: Options{
		URL: "nats://127.0.0.1:1", Topic: "t", ReportInterval: -1,
		ExtraNatsOptions: []nats.Option{nats.SetCustomDialer(d)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- p.Start() }()
	waitEntered(t, d)
	p.Stop()
	d.release()
	select {
	case err := <-started:
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("Start interrupted by Stop = %v, want ErrStopped", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
	}
	if p.conn.Load() != nil {
		t.Fatal("a connection was installed after Stop returned")
	}
	if err := p.WaitConnected(100 * time.Millisecond); !errors.Is(err, ErrStopped) {
		t.Fatalf("WaitConnected after Stop = %v, want ErrStopped", err)
	}
	if err := p.Publish([]byte("x")); !errors.Is(err, ErrStopped) {
		t.Fatalf("Publish after Stop = %v, want ErrStopped", err)
	}
}

// The same race for a mirrored subscriber: both endpoint dials are gated,
// Stop runs in between, and no endpoint ends up with a connection or a
// subscription record.
func TestMirroredSubscriberStopDuringInitialDial(t *testing.T) {
	d := newGatedDialer()
	s, err := NewMirroredSubscriber([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, SubscriberOptions{
		Options: Options{Topic: "t", ReportInterval: -1, ExtraNatsOptions: []nats.Option{nats.SetCustomDialer(d)}},
		Handler: func(*nats.Msg) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- s.Start() }()
	waitEntered(t, d)
	s.Stop()
	d.release()
	select {
	case err := <-started:
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("Start interrupted by Stop = %v, want ErrStopped", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
	}
	for i, l := range s.links {
		if l.conn.Load() != nil {
			t.Errorf("endpoint %d: connection installed after Stop", i)
		}
	}
	if st := s.Stats(); st.Connected || st.Running {
		t.Errorf("stats after stop: %+v", st)
	}
}

// Stop between setup and the connection swap (the narrowest window): stop
// sees no connection and returns; start must notice and discard what it
// just dialed.
func TestLinkStopBetweenSetupAndInstall(t *testing.T) {
	p, err := NewPublisher(PublisherOptions{Options: Options{URL: "nats://127.0.0.1:1", Topic: "t", ReportInterval: -1}})
	if err != nil {
		t.Fatal(err)
	}
	p.testBeforeInstall = func() { p.Stop() }
	if err := p.Start(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Start = %v, want ErrStopped", err)
	}
	if p.conn.Load() != nil || p.running.Load() {
		t.Fatal("connection installed or running after a Stop that raced the install")
	}
}

// A mirrored pool whose members' dials are gated: Stop during Start, every
// member returns ErrStopped and no member keeps a connection.
func TestMirroredPoolStopDuringInitialDial(t *testing.T) {
	d := newGatedDialer()
	pool, err := NewMirroredPublisherPool([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, 2, PublisherOptions{
		Options: Options{Topic: "t", ReportInterval: -1, ExtraNatsOptions: []nats.Option{nats.SetCustomDialer(d)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- pool.Start() }()
	waitEntered(t, d)
	pool.Stop()
	d.release()
	select {
	case err := <-started:
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("Start interrupted by Stop = %v, want ErrStopped", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
	}
	for i, pub := range pool.pubs {
		if pub.conn.Load() != nil {
			t.Errorf("member %d: connection installed after Stop", i)
		}
	}
}

// Against a real server the leaked socket would be a live connection; the
// tracked dialer proves every socket handed out was closed after Stop won.
func TestIntegrationStopDuringInitialDialClosesSocket(t *testing.T) {
	skipIfNoServer(t)
	d := newGatedDialer()
	p, err := NewPublisher(PublisherOptions{Options: Options{
		URL: testURL(), Topic: uniqueTopic("lifecycle"), ReportInterval: -1,
		ExtraNatsOptions: []nats.Option{nats.SetCustomDialer(d)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- p.Start() }()
	waitEntered(t, d)
	p.Stop()
	d.release()
	if err := <-started; !errors.Is(err, ErrStopped) {
		t.Fatalf("Start = %v, want ErrStopped", err)
	}
	waitUntil(t, 3*time.Second, "dialed sockets closed", func() bool { return d.liveConns() == 0 })
	if p.conn.Load() != nil {
		t.Fatal("connection installed after Stop")
	}
}

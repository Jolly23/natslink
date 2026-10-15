package natslink

import (
	"errors"
	"fmt"
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

// addressGatedDialer gates dials to one address (the "slow" endpoint) and lets
// every other address through at once; it records when each address was
// first dialled.
type addressGatedDialer struct {
	slow    string
	gate    chan struct{}
	entered chan struct{}
	once    sync.Once
	mu      sync.Mutex
	first   map[string]time.Time
}

func newAddressGatedDialer(slow string) *addressGatedDialer {
	return &addressGatedDialer{slow: slow, gate: make(chan struct{}), entered: make(chan struct{}), first: make(map[string]time.Time)}
}

func (d *addressGatedDialer) Dial(network, address string) (net.Conn, error) {
	d.mu.Lock()
	if _, ok := d.first[address]; !ok {
		d.first[address] = time.Now()
	}
	d.mu.Unlock()
	if address == d.slow {
		d.once.Do(func() { close(d.entered) })
		<-d.gate
	}
	return net.DialTimeout(network, address, 2*time.Second)
}

func (d *addressGatedDialer) firstDial(address string) (time.Time, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	t, ok := d.first[address]
	return t, ok
}

// Workers are active before any endpoint connects: with the second endpoint's
// dial gated, messages arriving through the first endpoint are handled, not
// parked in a queue nobody drains (and dropped once it fills).
func TestIntegrationWorkersActiveBeforeAllEndpointsConnect(t *testing.T) {
	skipIfNoServer(t)
	topic := uniqueTopic("startup")
	const slow = "127.0.0.1:1" // unreachable, and gated: the second endpoint never gets past Dial during the test
	d := newAddressGatedDialer(slow)
	var handled atomic.Int64
	s, err := NewMirroredSubscriber([]string{testURL(), "nats://" + slow}, SubscriberOptions{
		Options: Options{Topic: topic, ReportInterval: -1, ExtraNatsOptions: []nats.Option{nats.SetCustomDialer(d)}},
		Handler: func(*nats.Msg) { handled.Add(1) },
		Workers: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- s.Start() }()
	t.Cleanup(func() { close(d.gate); <-started; s.Stop() })
	select {
	case <-d.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("second endpoint never reached Dial")
	}
	// Endpoint 0 is connected and subscribed while Start is still blocked on endpoint 1.
	waitUntil(t, 5*time.Second, "endpoint 0 connected", func() bool { return s.links[0].IsConnected() })
	if err := s.links[0].Conn().FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	pub, err := nats.Connect(testURL(), nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	const n = 200
	for i := 0; i < n; i++ {
		if err := pub.Publish(topic, []byte("m")); err != nil {
			t.Fatal(err)
		}
	}
	if err := pub.Flush(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, "messages handled while endpoint 1 still dialling", func() bool { return handled.Load() == n })
	select {
	case err := <-started:
		t.Fatalf("Start returned (%v) before the gated endpoint was released", err)
	default:
	}
	if st := s.Stats(); st.Dropped != 0 || st.QueueLen != 0 {
		t.Errorf("dropped=%d queue=%d, want 0/0", st.Dropped, st.QueueLen)
	}
}

// Members are dialled concurrently and interleaved across endpoints: with
// endpoint 0's members gated, the first dial of endpoint 1 happens right away
// instead of after the slow members' dial timeouts, also when endpoint 0 has
// as many members as the whole dial budget (8) or more. No server needed.
func TestMirroredPoolDialsEndpointsConcurrently(t *testing.T) {
	for _, members := range []int{4, maxConcurrentDials, 2 * maxConcurrentDials} {
		t.Run(fmt.Sprintf("members=%d", members), func(t *testing.T) {
			const slow, fast = "127.0.0.1:1", "127.0.0.1:2"
			d := newAddressGatedDialer(slow)
			pool, err := NewMirroredPublisherPool([]string{"nats://" + slow, "nats://" + fast}, members, PublisherOptions{
				Options: Options{Topic: "t", ReportInterval: -1, ExtraNatsOptions: []nats.Option{nats.SetCustomDialer(d)}},
			})
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan error, 1)
			start := time.Now()
			go func() { started <- pool.Start() }()
			t.Cleanup(func() { close(d.gate); <-started; pool.Stop() })
			waitUntil(t, 3*time.Second, "fast endpoint dialled", func() bool { _, ok := d.firstDial(fast); return ok })
			at, _ := d.firstDial(fast)
			if el := at.Sub(start); el > 500*time.Millisecond {
				t.Errorf("fast endpoint's first dial started %v after Start while slow members were gated; want immediate", el)
			}
		})
	}
}

// Stop between the member dials and the send-line registration: Start must
// not register any send line and returns ErrStopped; Publish afterwards is
// ErrStopped too. Plus a public-API race loop (Start and Stop concurrently)
// under the race detector, which is where a WaitGroup Add/Wait race shows.
func TestMirroredPoolStopDuringSenderRegistration(t *testing.T) {
	pool, err := NewMirroredPublisherPool([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, 1, PublisherOptions{
		Options: Options{Topic: "t", ReportInterval: -1},
	})
	if err != nil {
		t.Fatal(err)
	}
	pool.testBeforeRegister = pool.Stop
	if err := pool.Start(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Start with Stop before registration = %v, want ErrStopped", err)
	}
	if pool.running.Load() {
		t.Fatal("pool reports running after a Stop that raced registration")
	}
	if err := pool.Publish([]byte("x")); !errors.Is(err, ErrStopped) {
		t.Fatalf("Publish after Stop = %v, want ErrStopped", err)
	}

	for i := 0; i < 50; i++ {
		p, err := NewMirroredPublisherPool([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, 2, PublisherOptions{
			Options: Options{Topic: "t", ReportInterval: -1},
		})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = p.Start() }()
		go func() { defer wg.Done(); p.Stop() }()
		wg.Wait()
		p.Stop()
		if err := p.Publish([]byte("x")); !errors.Is(err, ErrStopped) {
			t.Fatalf("iteration %d: Publish after Stop = %v, want ErrStopped", i, err)
		}
	}
}

// A configuration failure rolls back every member that succeeded, including
// one whose dial finishes after the failure: the gated good member is still
// dialling when the bad URL fails, and must end up stopped.
func TestMirroredPoolStartFailureStopsLateFinisher(t *testing.T) {
	const slow = "127.0.0.1:1"
	d := newAddressGatedDialer(slow)
	pool, err := NewMirroredPublisherPool([]string{"nats://" + slow, "nats://bad url/"}, 1, PublisherOptions{
		Options: Options{Topic: "t", ReportInterval: -1, ExtraNatsOptions: []nats.Option{nats.SetCustomDialer(d)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- pool.Start() }()
	select {
	case <-d.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("good member never reached Dial")
	}
	time.Sleep(50 * time.Millisecond) // the bad member has failed by now; Start must still be waiting for the gated one
	select {
	case err := <-started:
		t.Fatalf("Start returned %v while a member was still dialling", err)
	default:
	}
	close(d.gate)
	if err := <-started; err == nil {
		t.Fatal("Start must fail on the bad URL")
	}
	for i, pub := range pool.pubs {
		switch c := pub.conn.Load(); {
		case pub.stopped.Load() && (c == nil || c.IsClosed()):
			// started, then rolled back (the late finisher)
		case !pub.stopped.Load() && c == nil && !pub.running.Load():
			// never started (its own Start failed)
		default:
			t.Errorf("member %d not rolled back: stopped=%v running=%v conn=%v", i, pub.stopped.Load(), pub.running.Load(), c != nil)
		}
	}
	if err := pool.Start(); !errors.Is(err, ErrAlreadyStarted) && !errors.Is(err, ErrStopped) {
		t.Errorf("second Start after a failed one = %v, want ErrAlreadyStarted or ErrStopped", err)
	}
}

// Start and Stop racing on a subscriber: no WaitGroup misuse, no panic,
// every outcome consistent (either Start wins and Stop tears it down, or Stop
// wins and Start reports it). Run many times under the race detector.
func TestSubscriberStartStopRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		s, err := NewSubscriber(SubscriberOptions{
			Options: Options{URL: "nats://127.0.0.1:1", Topic: "t", ReportInterval: -1},
			Handler: func(*nats.Msg) {},
		})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = s.Start() }()
		go func() { defer wg.Done(); s.Stop() }()
		wg.Wait()
		s.Stop()
		if err := s.Start(); !errors.Is(err, ErrStopped) {
			t.Fatalf("iteration %d: Start after Stop = %v, want ErrStopped", i, err)
		}
		if s.links[0].conn.Load() != nil && !s.links[0].stopped.Load() {
			t.Fatalf("iteration %d: live connection after Stop", i)
		}
	}
}

// A second Start on a running instance is rejected without touching it.
func TestSecondStartDoesNotRollBack(t *testing.T) {
	s, err := NewMirroredSubscriber([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, SubscriberOptions{
		Options: Options{Topic: "t", ReportInterval: -1},
		Handler: func(*nats.Msg) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	if err := s.Start(); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start = %v, want ErrAlreadyStarted", err)
	}
	for i, l := range s.links {
		if !l.running.Load() || l.stopped.Load() {
			t.Errorf("endpoint %d was disturbed by the second Start", i)
		}
	}
	pool, _ := NewMirroredPublisherPool([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, 1, PublisherOptions{Options: Options{Topic: "t", ReportInterval: -1}})
	if err := pool.Start(); err != nil {
		t.Fatal(err)
	}
	defer pool.Stop()
	if err := pool.Start(); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second pool Start = %v, want ErrAlreadyStarted", err)
	}
	if !pool.running.Load() || pool.pubs[0].stopped.Load() {
		t.Error("pool was disturbed by the second Start")
	}
}

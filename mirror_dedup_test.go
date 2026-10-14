package natslink

import (
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go"
)

// Two endpoint copies of one payload reach dispatch; with a DedupSet the
// handler runs once and the second copy is counted in Stats.Deduped. A
// different payload goes through, and an empty payload is never deduplicated.
func TestMirroredSubscriberDedupCollapsesCopies(t *testing.T) {
	var count atomic.Int32
	ms, err := NewMirroredSubscriber([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, SubscriberOptions{
		Options: Options{Topic: "t", ReportInterval: -1},
		Handler: func(*nats.Msg) { count.Add(1) },
		Workers: SyncMode,
		Dedup:   NewDedupSet(16, 0),
	})
	if err != nil {
		t.Fatalf("NewMirroredSubscriber: %v", err)
	}
	a := []byte("block-1")
	ms.dispatch(&nats.Msg{Data: a})
	ms.dispatch(&nats.Msg{Data: append([]byte(nil), a...)}) // same bytes, different backing array
	ms.dispatch(&nats.Msg{Data: []byte("block-2")})
	ms.dispatch(&nats.Msg{})
	ms.dispatch(&nats.Msg{})
	if got := count.Load(); got != 4 {
		t.Errorf("handler calls = %d, want 4 (block-1 once, block-2 once, two empty)", got)
	}
	st := ms.Stats()
	if st.Deduped != 1 || st.Gated != 0 || st.Dropped != 0 {
		t.Errorf("stats deduped=%d gated=%d dropped=%d, want 1/0/0", st.Deduped, st.Gated, st.Dropped)
	}
	// The per-link snapshot carries the shared counter too (status report).
	if got := ms.links[1].Stats().Deduped; got != 1 {
		t.Errorf("link-level deduped = %d, want 1", got)
	}
}

// Without a DedupSet nothing changes: every copy is delivered (the previous
// behaviour, pinned so the default stays backward compatible).
func TestMirroredSubscriberNoDedupByDefault(t *testing.T) {
	var count atomic.Int32
	ms, err := NewMirroredSubscriber([]string{"nats://127.0.0.1:1", "nats://127.0.0.1:2"}, SubscriberOptions{
		Options: Options{Topic: "t", ReportInterval: -1},
		Handler: func(*nats.Msg) { count.Add(1) },
		Workers: SyncMode,
	})
	if err != nil {
		t.Fatalf("NewMirroredSubscriber: %v", err)
	}
	ms.dispatch(&nats.Msg{Data: []byte("x")})
	ms.dispatch(&nats.Msg{Data: []byte("x")})
	if got := count.Load(); got != 2 || ms.Stats().Deduped != 0 {
		t.Errorf("handler calls = %d deduped = %d, want 2/0", got, ms.Stats().Deduped)
	}
}

// Gate runs before dedup: a gated-off copy is neither delivered nor recorded,
// so the next copy (gate open) is delivered. Dedup runs before the queue: in
// worker-pool mode a duplicate takes no queue slot.
func TestDedupOrderingAgainstGateAndQueue(t *testing.T) {
	var open atomic.Bool
	s, err := NewSubscriber(SubscriberOptions{
		Options:   Options{URL: "nats://127.0.0.1:1", Topic: "t", ReportInterval: -1},
		Handler:   func(*nats.Msg) {},
		Gate:      open.Load,
		Dedup:     NewDedupSet(16, 0),
		Workers:   1,
		QueueSize: 4,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	// Not started: messages stay in the queue, which makes the slot accounting observable.
	s.dispatch(&nats.Msg{Data: []byte("m")}) // gated off: not recorded
	open.Store(true)
	s.dispatch(&nats.Msg{Data: []byte("m")}) // first real sight: queued
	s.dispatch(&nats.Msg{Data: []byte("m")}) // duplicate: no slot
	st := s.Stats()
	if st.Gated != 1 || st.Deduped != 1 || st.QueueLen != 1 {
		t.Errorf("gated=%d deduped=%d queue=%d, want 1/1/1", st.Gated, st.Deduped, st.QueueLen)
	}
}

// DedupKey replaces the payload hash; ok=false bypasses the set entirely.
func TestDedupKeyOverridesHash(t *testing.T) {
	var count atomic.Int32
	s, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: "nats://127.0.0.1:1", Topic: "t", ReportInterval: -1},
		Handler: func(*nats.Msg) { count.Add(1) },
		Workers: SyncMode,
		Dedup:   NewDedupSet(16, 0),
		DedupKey: func(m *nats.Msg) (uint64, bool) {
			if len(m.Data) < 8 {
				return 0, false // too short to carry a key: always deliver
			}
			return uint64(m.Data[0]), true // key = first byte only
		},
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	s.dispatch(&nats.Msg{Data: []byte("A-payload-1")})
	s.dispatch(&nats.Msg{Data: []byte("A-payload-2")}) // different bytes, same key: duplicate
	s.dispatch(&nats.Msg{Data: []byte("B-payload-1")})
	s.dispatch(&nats.Msg{Data: []byte("x")}) // bypass
	s.dispatch(&nats.Msg{Data: []byte("x")}) // bypass again
	if got := count.Load(); got != 4 || s.Stats().Deduped != 1 {
		t.Errorf("handler calls = %d deduped = %d, want 4/1", got, s.Stats().Deduped)
	}
}

func TestDedupKeyRequiresDedupSet(t *testing.T) {
	_, err := NewSubscriber(SubscriberOptions{
		Options:  Options{URL: "nats://127.0.0.1:1", Topic: "t", ReportInterval: -1},
		Handler:  func(*nats.Msg) {},
		DedupKey: func(*nats.Msg) (uint64, bool) { return 0, true },
	})
	if err == nil {
		t.Fatal("DedupKey without Dedup must be a configuration error")
	}
}

// One DedupSet shared by two subscribers on different subjects collapses the
// same bytes across them; each subscriber counts its own drops.
func TestDedupSetSharedAcrossSubscribers(t *testing.T) {
	set := NewDedupSet(16, 0)
	var a, b atomic.Int32
	mk := func(topic string, c *atomic.Int32) *Subscriber {
		s, err := NewSubscriber(SubscriberOptions{
			Options: Options{URL: "nats://127.0.0.1:1", Topic: topic, ReportInterval: -1},
			Handler: func(*nats.Msg) { c.Add(1) },
			Workers: SyncMode,
			Dedup:   set,
		})
		if err != nil {
			t.Fatalf("NewSubscriber: %v", err)
		}
		return s
	}
	sa, sb := mk("feed.a", &a), mk("feed.b", &b)
	sa.dispatch(&nats.Msg{Data: []byte("same")})
	sb.dispatch(&nats.Msg{Data: []byte("same")})
	sb.dispatch(&nats.Msg{Data: []byte("other")})
	sa.dispatch(&nats.Msg{Data: []byte("other")})
	if a.Load() != 1 || b.Load() != 1 || sa.Stats().Deduped != 1 || sb.Stats().Deduped != 1 {
		t.Errorf("a=%d b=%d deduped a=%d b=%d, want 1/1/1/1", a.Load(), b.Load(), sa.Stats().Deduped, sb.Stats().Deduped)
	}
}

// NewMirroredPublisher: validation, the endpoint cap, naming, and the
// single-URL degradation to a plain publisher.
func TestNewMirroredPublisher(t *testing.T) {
	opts := PublisherOptions{Options: Options{Topic: "t", Name: "mp", ReportInterval: -1}}

	if _, err := NewMirroredPublisher(nil, opts); err == nil {
		t.Error("empty urls should fail")
	}
	six := make([]string, 6)
	for i := range six {
		six[i] = "nats://h" + string(rune('0'+i)) + ":4222"
	}
	if _, err := NewMirroredPublisher(six, opts); err == nil {
		t.Error("6 endpoints should fail (cap is 5)")
	}
	five, err := NewMirroredPublisher(six[:5], opts)
	if err != nil {
		t.Fatalf("5 endpoints: %v", err)
	}
	if five.Endpoints() != 5 || five.Size() != 5 {
		t.Errorf("endpoints=%d size=%d, want 5/5", five.Endpoints(), five.Size())
	}
	if got := five.pubs[4].o.Name; got != "mp-e4" {
		t.Errorf("member4 name = %q, want mp-e4 (one connection per endpoint, no member suffix)", got)
	}
	if got := five.pubs[2].o.URL; got != six[2] {
		t.Errorf("member2 url = %q", got)
	}

	// One URL: nothing to mirror, exactly NewPublisherPool(1, o): synchronous
	// Publish, member named <Name>-0, no send queue.
	one, err := NewMirroredPublisher(six[:1], opts)
	if err != nil {
		t.Fatalf("1 endpoint: %v", err)
	}
	if one.Endpoints() != 1 || one.Size() != 1 || one.async {
		t.Errorf("endpoints=%d size=%d async=%v, want 1/1/false", one.Endpoints(), one.Size(), one.async)
	}
	if got := one.Stats()[0].Name; got != "mp-0" {
		t.Errorf("single-endpoint name = %q, want \"mp-0\" (plain pool naming)", got)
	}
	if got := one.Stats()[0].QueueCap; got != 0 {
		t.Errorf("single-endpoint QueueCap = %d, want 0 (no send queue)", got)
	}
	if err := one.Publish([]byte("x")); err == nil || one.Dropped() != 1 {
		t.Errorf("publish before start: err=%v dropped=%d, want ErrNotRunning / 1", err, one.Dropped())
	}

	// Default naming without an explicit Name follows the package rule per endpoint.
	anon, err := NewMirroredPublisher(six[:2], PublisherOptions{Options: Options{Topic: "events.x", ReportInterval: -1}})
	if err != nil {
		t.Fatalf("anon: %v", err)
	}
	if got := anon.pubs[1].o.Name; got != "natslink-pub-events.x-e1" {
		t.Errorf("default name = %q", got)
	}
}

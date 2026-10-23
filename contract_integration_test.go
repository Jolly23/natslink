package natslink

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// A healthy bounded burst must drain after admission. It exercises the send
// queues and underlying clients together, without restarting either server.
func TestContractHealthyMirroredBurst(t *testing.T) {
	skipIfNoDualServers(t)
	for _, members := range []int{2, 6} {
		t.Run(fmt.Sprintf("members=%d", members), func(t *testing.T) {
			urls := []string{dualURL1(), dualURL2()}
			topic := uniqueTopic("contract.burst")
			var received [2]atomic.Uint64
			for endpoint, url := range urls {
				conn, err := nats.Connect(url)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(conn.Close)
				if _, err := conn.Subscribe(topic, func(*nats.Msg) { received[endpoint].Add(1) }); err != nil {
					t.Fatal(err)
				}
				if err := conn.FlushTimeout(5 * time.Second); err != nil {
					t.Fatal(err)
				}
			}
			pool, err := NewMirroredPublisherPool(urls, members, PublisherOptions{
				Options: Options{Topic: topic, Logger: benchLogger{}, ReportInterval: -1},
			})
			if err != nil {
				t.Fatal(err)
			}
			pool.MustStart()
			t.Cleanup(pool.Stop)
			if err := pool.WaitConnected(5 * time.Second); err != nil {
				t.Fatal(err)
			}
			const messages, payloadBytes = 2000, 512
			payload := make([]byte, payloadBytes)
			for i := 0; i < messages; i++ {
				if err := pool.Publish(payload); err != nil {
					t.Fatalf("publish %d: %v", i, err)
				}
			}
			waitUntil(t, 5*time.Second, "both endpoints to drain accepted burst", func() bool {
				return received[0].Load() >= messages && received[1].Load() >= messages
			})
			for endpoint := range received {
				if got := received[endpoint].Load(); got != messages {
					t.Errorf("endpoint %d received %d, want %d", endpoint, got, messages)
				}
			}
			var outMsgs, outBytes uint64
			for i, stats := range pool.Stats() {
				outMsgs += stats.OutMsgs
				outBytes += stats.OutBytes
				if stats.Dropped != 0 || stats.QueueDropped != 0 {
					t.Errorf("member %d dropped=%d queue_dropped=%d", i, stats.Dropped, stats.QueueDropped)
				}
			}
			if pool.Dropped() != 0 || outMsgs != 2*messages || outBytes != 2*messages*payloadBytes {
				t.Errorf("pool dropped=%d out_msgs=%d out_bytes=%d, want 0/%d/%d", pool.Dropped(), outMsgs, outBytes, 2*messages, 2*messages*payloadBytes)
			}
		})
	}
}

// Byte counters describe the message body: payload plus the encoded NATS
// header block. Subject/reply names and protocol framing are not counted.
func TestContractMessageByteStatistics(t *testing.T) {
	skipIfNoServer(t)
	topic := uniqueTopic("contract.bytes")
	var received atomic.Uint64
	sub, err := NewSubscriber(SubscriberOptions{
		Options: Options{URL: testURL(), Topic: topic, Logger: benchLogger{}, ReportInterval: -1},
		Workers: SyncMode,
		Handler: func(*nats.Msg) { received.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	sub.MustStart()
	t.Cleanup(sub.Stop)
	if err := sub.WaitConnected(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := sub.Conn().FlushTimeout(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	pub, err := NewPublisher(PublisherOptions{
		Options: Options{URL: testURL(), Topic: topic, Logger: benchLogger{}, ReportInterval: -1},
	})
	if err != nil {
		t.Fatal(err)
	}
	pub.MustStart()
	t.Cleanup(pub.Stop)
	if err := pub.WaitConnected(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	beforePub, beforeSub := pub.Stats(), sub.Stats()
	if err := pub.Publish([]byte("plain")); err != nil {
		t.Fatal(err)
	}
	// Headers use the documented Conn escape hatch; traffic still belongs to
	// this connection and its Stats. Keep one key to make encoding unambiguous.
	headers := nats.Header{"X-Contract": []string{"one", "two"}}
	for _, payload := range [][]byte{[]byte("body"), nil} {
		if err := pub.Conn().PublishMsg(&nats.Msg{
			Subject: topic, Reply: "contract.reply", Header: headers, Data: payload,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := pub.Conn().FlushTimeout(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, "three messages for byte statistics", func() bool { return received.Load() == 3 })
	const headerBytes = len("NATS/1.0\r\nX-Contract: one\r\nX-Contract: two\r\n\r\n")
	const wantBytes = uint64(len("plain") + len("body") + 2*headerBytes)
	afterPub, afterSub := pub.Stats(), sub.Stats()
	if got := afterPub.OutMsgs - beforePub.OutMsgs; got != 3 {
		t.Errorf("out_msgs=%d, want 3", got)
	}
	if got := afterSub.InMsgs - beforeSub.InMsgs; got != 3 {
		t.Errorf("in_msgs=%d, want 3", got)
	}
	if got := afterPub.OutBytes - beforePub.OutBytes; got != wantBytes {
		t.Errorf("out_bytes=%d, want %d", got, wantBytes)
	}
	if got := afterSub.InBytes - beforeSub.InBytes; got != wantBytes {
		t.Errorf("in_bytes=%d, want %d", got, wantBytes)
	}
}

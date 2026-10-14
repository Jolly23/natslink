package natslink

import (
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
)

// Failure-path cost of redactError on the common nats.go sentinels (no URL in
// the message: the regexp pass must be skipped) next to a credential-bearing
// URL error (the regexp pass must run).
func BenchmarkRedactError(b *testing.B) {
	const url = "nats://secret-token@127.0.0.1:4222"
	cases := []struct {
		name string
		err  error
	}{
		{"reconnect_buf_exceeded", nats.ErrReconnectBufExceeded},
		{"max_payload", nats.ErrMaxPayload},
		{"url_with_credentials", fmt.Errorf("dial %s: connection refused", url)},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := redactError(url, c.err); err == nil {
					b.Fatal("nil")
				}
			}
		})
	}
}

// Routing cost of pubGroup.publish with no usable member: every member is a
// fallback candidate and the call ends in ErrNotRunning, so this isolates
// candidate collection from network work.
func BenchmarkPoolRoutingAllDisconnected(b *testing.B) {
	for _, n := range []int{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("members=%d", n), func(b *testing.B) {
			pool, err := NewPublisherPool(n, PublisherOptions{Options: Options{URL: "nats://127.0.0.1:1", Topic: "t", ReportInterval: -1, Logger: benchLogger{}}})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := pool.groups[0].publish(benchPayload); !errors.Is(err, ErrNotRunning) {
					b.Fatal(err)
				}
			}
		})
	}
}

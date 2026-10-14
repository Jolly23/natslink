package natslink

// Mirrored-publish cost benchmark: since v1.8.0 this measures the caller's
// side only (one payload copy plus one non-blocking enqueue per endpoint); the
// sends happen on the endpoint goroutines. Needs two local NATS servers
// (4222/4223) and skips when either is unreachable.

import "testing"

func BenchmarkHotMirrorPublish(b *testing.B) {
	for _, u := range []string{benchURL(), dualURL2()} {
		if err := Probe(u); err != nil {
			b.Skipf("NATS not reachable: %v", err)
		}
	}
	mp, err := NewMirroredPublisherPool([]string{benchURL(), dualURL2()}, 4, PublisherOptions{
		Options: Options{Topic: "natslink.bench.mirror", ReportInterval: -1, Logger: benchLogger{}},
	})
	if err != nil {
		b.Fatal(err)
	}
	mp.MustStart()
	defer mp.Stop()
	if err := mp.WaitConnected(5000000000); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := mp.Publish(benchPayload); err != nil {
			b.Fatal(err)
		}
	}
}

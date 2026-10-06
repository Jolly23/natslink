package natslink

// Mirrored-publish cost benchmark. It only runs on the post-refactor package (it
// depends on the mirror constructors), which is why it is kept out of
// perf_bench_test.go, the file that can also be run against the old version. Needs
// two local NATS servers (4222/4223) and skips when either is unreachable.

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

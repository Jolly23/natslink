package natslink

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"
)

// dedupPayload makes distinct byte strings of a fixed shape.
func dedupPayload(i int) []byte {
	b := make([]byte, 16)
	copy(b, "payload-")
	binary.BigEndian.PutUint64(b[8:], uint64(i))
	return b
}

func TestDedupSetDefaults(t *testing.T) {
	d := NewDedupSet(0, -time.Second)
	if d.Entries() != defaultDedupEntries || d.Window() != 0 {
		t.Fatalf("entries=%d window=%v, want %d/0", d.Entries(), d.Window(), defaultDedupEntries)
	}
	if NewDedupSet(7, time.Second).Entries() != 7 {
		t.Fatal("explicit entries not honoured")
	}
}

// First sight is recorded, an identical payload is a duplicate, a different
// payload is not; order of arrival decides which copy "wins".
func TestDedupSetIdenticalBytes(t *testing.T) {
	d := NewDedupSet(16, 0)
	if d.Seen(dedupPayload(1)) {
		t.Fatal("first sight reported as seen")
	}
	if !d.Seen(dedupPayload(1)) {
		t.Fatal("identical copy not reported as seen")
	}
	if d.Seen(dedupPayload(2)) {
		t.Fatal("different payload reported as seen")
	}
	if !d.Seen(dedupPayload(1)) || !d.Seen(dedupPayload(2)) {
		t.Fatal("recorded payloads must stay seen")
	}
}

// After entries newer distinct payloads the oldest is forgotten (FIFO) and
// passes again; a hit does not refresh the entry, so flooding one message
// cannot keep it alive.
func TestDedupSetEvictsFIFOAndHitDoesNotRefresh(t *testing.T) {
	const n = 8
	d := NewDedupSet(n, 0)
	d.Seen(dedupPayload(0))
	for i := 1; i < n; i++ {
		d.Seen(dedupPayload(i))
		if !d.Seen(dedupPayload(0)) {
			t.Fatalf("payload 0 forgotten after %d newer entries (capacity %d)", i, n)
		}
	}
	// The set is full; the next distinct payload evicts payload 0 even though
	// it was just hit (hits do not refresh).
	if d.Seen(dedupPayload(n)) {
		t.Fatal("payload n reported as seen")
	}
	// Payload 1 is now the oldest live entry and must still be present (a hit
	// does not mutate the set, so check it before re-recording payload 0).
	if !d.Seen(dedupPayload(1)) {
		t.Fatal("payload 1 evicted too early")
	}
	if d.Seen(dedupPayload(0)) {
		t.Fatal("payload 0 should have been evicted by FIFO")
	}
	if len(d.seen) != n {
		t.Fatalf("map size %d, want %d (no leak, no shrink)", len(d.seen), n)
	}
}

// With a window, an entry older than the window no longer counts as seen and
// is recorded again; the stale ring slot must not corrupt the live mapping
// when it is later overwritten.
func TestDedupSetWindowExpiryAndReinsert(t *testing.T) {
	const n = 4
	d := NewDedupSet(n, 30*time.Millisecond)
	if d.Seen(dedupPayload(0)) {
		t.Fatal("first sight")
	}
	if !d.Seen(dedupPayload(0)) {
		t.Fatal("copy inside the window must be seen")
	}
	time.Sleep(60 * time.Millisecond)
	if d.Seen(dedupPayload(0)) {
		t.Fatal("copy after the window must be delivered again")
	}
	// Re-recorded in slot 1 while slot 0 still holds the same hash. Overwrite
	// slot 0 (fill the ring: slots 2, 3, then 0) and check payload 0 is still
	// known through slot 1.
	d.Seen(dedupPayload(1))
	d.Seen(dedupPayload(2))
	d.Seen(dedupPayload(3)) // overwrites slot 0, whose mapping must be left alone
	if !d.Seen(dedupPayload(0)) {
		t.Fatal("re-recorded payload lost when its stale slot was reclaimed")
	}
	if got := d.seen[d.ring[1].hash]; got != 1 {
		t.Fatalf("mapping for payload 0 points at slot %d, want 1", got)
	}
	if len(d.seen) != n {
		t.Fatalf("map size %d, want %d", len(d.seen), n)
	}
}

// Many goroutines hammering one set with overlapping payloads: exactly one
// goroutine sees each payload first, the map never exceeds capacity.
func TestDedupSetConcurrent(t *testing.T) {
	const (
		workers  = 8
		payloads = 2000
		capacity = 4096
	)
	d := NewDedupSet(capacity, 0)
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first = make(map[int]int)
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < payloads; i++ {
				if !d.Seen(dedupPayload(i)) {
					mu.Lock()
					first[i]++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	if len(first) != payloads {
		t.Fatalf("%d payloads seen first, want %d", len(first), payloads)
	}
	for i, c := range first {
		if c != 1 {
			t.Fatalf("payload %d was 'first' %d times", i, c)
		}
	}
	if len(d.seen) != payloads || d.n != payloads {
		t.Fatalf("set holds %d/%d entries, want %d", len(d.seen), d.n, payloads)
	}
}

func TestDedupSetSeenKey(t *testing.T) {
	d := NewDedupSet(4, 0)
	if d.SeenKey(42) || !d.SeenKey(42) || d.SeenKey(43) {
		t.Fatal("SeenKey semantics differ from Seen")
	}
}

func BenchmarkDedupSeen(b *testing.B) {
	for _, size := range []int{64, 4 << 10, 2 << 20} {
		b.Run(fmt.Sprintf("%dB_miss", size), func(b *testing.B) {
			d := NewDedupSet(4096, 0)
			buf := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				binary.LittleEndian.PutUint64(buf, uint64(i))
				d.Seen(buf)
			}
		})
		b.Run(fmt.Sprintf("%dB_hit", size), func(b *testing.B) {
			d := NewDedupSet(4096, 0)
			buf := make([]byte, size)
			d.Seen(buf)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d.Seen(buf)
			}
		})
	}
}

func BenchmarkDedupSeenParallel(b *testing.B) {
	d := NewDedupSet(4096, time.Second)
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, 256)
		i := 0
		for pb.Next() {
			i++
			binary.LittleEndian.PutUint64(buf, uint64(i%64)) // 64 hot payloads: mostly hits
			d.Seen(buf)
		}
	})
}

package natslink

import (
	"hash/maphash"
	"sync"
	"time"
)

// defaultDedupEntries is the number of distinct payloads a DedupSet remembers
// when the caller passes entries <= 0. Measured mirror copies arrive a few
// tens of milliseconds apart; at thousands of messages per second 4096
// entries still span seconds, at tens per second they span minutes.
const defaultDedupEntries = 4096

// DedupSet remembers the hashes of the last N distinct payloads and reports
// whether a payload is a byte-identical copy of one of them. It exists to
// collapse the M copies a mirrored subscription receives (one per endpoint)
// into a single callback without decoding anything, and can be shared by
// several subscribers so copies arriving on different subjects collapse too.
//
// It is a FIFO set of 64-bit maphash values: a ring buffer fixes the eviction
// order and a presized map does the lookup, so after warm-up a call allocates
// nothing. The hash is computed outside the lock; the critical section is one
// map lookup and, on a miss, one insert. A hit does not refresh the entry, so a
// flood of one message cannot pin it. With an optional time window an entry
// older than the window no longer counts as seen (useful on quiet subjects
// where a legitimately re-sent identical message would otherwise be dropped
// minutes later). A 64-bit collision inside the window would drop a distinct
// message; with 4096 entries that is about 2^-52 per message.
//
// This collapses byte-identical copies only. Messages that carry the same
// content in different bytes (a different sender field, a timestamp) are not
// duplicates to this set; that deduplication belongs to the application.
type DedupSet struct {
	seed   maphash.Seed
	window time.Duration // 0 = entries never expire by age, only by eviction
	base   time.Time     // monotonic reference for entry ages

	mu   sync.Mutex
	ring []dedupEntry
	next int            // slot the next insert overwrites
	n    int            // filled slots, up to len(ring)
	seen map[uint64]int // hash -> slot index in ring
}

type dedupEntry struct {
	hash uint64
	at   int64 // nanoseconds since base; only meaningful when window > 0
}

// NewDedupSet creates a set remembering the last entries distinct payloads
// (<= 0 selects 4096). A positive window additionally expires an entry once it
// is older than the window; 0 disables age expiry. Memory is fixed at about
// 40 bytes per entry.
func NewDedupSet(entries int, window time.Duration) *DedupSet {
	if entries <= 0 {
		entries = defaultDedupEntries
	}
	if window < 0 {
		window = 0
	}
	return &DedupSet{
		seed:   maphash.MakeSeed(),
		window: window,
		base:   time.Now(),
		ring:   make([]dedupEntry, entries),
		seen:   make(map[uint64]int, entries),
	}
}

// Entries returns the number of distinct payloads the set remembers.
func (d *DedupSet) Entries() int { return len(d.ring) }

// Window returns the age after which an entry stops counting as seen (0 = never).
func (d *DedupSet) Window() time.Duration { return d.window }

// Seen reports whether data is byte-identical to a payload recorded within the
// set's capacity (and window); if not, it records data and returns false.
// Safe for concurrent use.
func (d *DedupSet) Seen(data []byte) bool {
	return d.SeenKey(maphash.Bytes(d.seed, data))
}

// SeenKey is Seen for a caller-computed 64-bit key (see
// SubscriberOptions.DedupKey). Keys from different sources must not be mixed
// in one set unless they are drawn from the same space.
func (d *DedupSet) SeenKey(key uint64) bool {
	var now int64
	if d.window > 0 {
		now = int64(time.Since(d.base))
	}
	d.mu.Lock()
	if idx, ok := d.seen[key]; ok {
		if d.window == 0 || now-d.ring[idx].at <= int64(d.window) {
			d.mu.Unlock()
			return true
		}
		// Expired: fall through and record it again in a fresh slot. The stale
		// slot keeps its hash; the map now points at the new slot, and the
		// eviction below only deletes a map entry that still points at the slot
		// being overwritten, so the stale slot is reclaimed without touching the
		// live mapping.
	}
	slot := d.next
	if d.n == len(d.ring) {
		// Only drop the mapping if it still points at the slot being reclaimed:
		// after an age expiry the same hash lives in a newer slot and must stay
		// mapped. (A hash absent from the map yields index 0; the comparison
		// makes that a no-op except for slot 0, where deleting a missing key is
		// equally harmless.)
		if old := d.ring[slot].hash; d.seen[old] == slot {
			delete(d.seen, old)
		}
	} else {
		d.n++
	}
	d.ring[slot] = dedupEntry{hash: key, at: now}
	d.seen[key] = slot
	d.next++
	if d.next == len(d.ring) {
		d.next = 0
	}
	d.mu.Unlock()
	return false
}

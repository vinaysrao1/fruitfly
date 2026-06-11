package executor

import (
	"sync"
	"sync/atomic"
)

const counterBucketSeconds int64 = 60
const counterMaxWindowSeconds int64 = 3600 // enforced cap on counter() windows
const counterSlots = counterMaxWindowSeconds / counterBucketSeconds
const counterShards = 256

// counterKey identifies one sliding-window series.
type counterKey struct {
	EntityID  string
	EventType string
}

// shard maps a key to its home shard with FNV-1a over both fields.
func (k counterKey) shard() uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(k.EntityID); i++ {
		h = (h ^ uint32(k.EntityID[i])) * 16777619
	}
	h = (h ^ 0xff) * 16777619
	for i := 0; i < len(k.EventType); i++ {
		h = (h ^ uint32(k.EventType[i])) * 16777619
	}
	return h % counterShards
}

// counterSeries is a fixed ring of per-minute buckets covering
// counterMaxWindowSeconds. Each slot packs the bucket epoch
// (unix seconds / 60, high 32 bits) and the count (low 32 bits) into one
// atomic word, so increments are lock-free and exact under any number of
// concurrent writers: a CAS either bumps the count of the current epoch or
// installs the new epoch with count 1. (A bucket would need 2^32 increments
// in one minute to overflow into the epoch bits.)
type counterSeries struct {
	slots [counterSlots]atomic.Uint64
}

func slotEpoch(v uint64) uint32 { return uint32(v >> 32) }
func slotCount(v uint64) int64  { return int64(uint32(v)) }

func (s *counterSeries) increment(unixNow int64) {
	epoch := uint32(unixNow / counterBucketSeconds)
	slot := &s.slots[int64(epoch)%counterSlots]
	for {
		old := slot.Load()
		if slotEpoch(old) == epoch {
			if slot.CompareAndSwap(old, old+1) {
				return
			}
		} else {
			if slot.CompareAndSwap(old, uint64(epoch)<<32|1) {
				return
			}
		}
	}
}

// sum returns the total of all buckets whose start time is >= windowStart.
// The window is quantized to whole buckets, so results are approximate by
// up to one bucket width — the documented contract for counter().
func (s *counterSeries) sum(windowStart int64) int64 {
	var total int64
	for i := range s.slots {
		v := s.slots[i].Load()
		if int64(slotEpoch(v))*counterBucketSeconds >= windowStart {
			total += slotCount(v)
		}
	}
	return total
}

// counterStore holds all counter series, sharded by key hash. Every key has
// exactly one home shard, so any worker may increment or read any key and
// values stay exact — there is no per-worker copy to aggregate.
type counterStore struct {
	shards [counterShards]sync.Map // counterKey -> *counterSeries
}

func (cs *counterStore) increment(entityID, eventType string, unixNow int64) {
	key := counterKey{EntityID: entityID, EventType: eventType}
	m := &cs.shards[key.shard()]
	v, ok := m.Load(key)
	if !ok {
		v, _ = m.LoadOrStore(key, &counterSeries{})
	}
	v.(*counterSeries).increment(unixNow)
}

func (cs *counterStore) sum(entityID, eventType string, windowStart int64) int64 {
	key := counterKey{EntityID: entityID, EventType: eventType}
	v, ok := cs.shards[key.shard()].Load(key)
	if !ok {
		return 0
	}
	return v.(*counterSeries).sum(windowStart)
}

// gc evicts series whose buckets have all aged out, bounding memory under
// high entity-ID cardinality. An increment racing with the eviction of an
// hour-idle series can be lost; that is an accepted approximation, the same
// one a process restart imposes.
func (cs *counterStore) gc(now int64) {
	cutoffEpoch := uint32((now - counterMaxWindowSeconds) / counterBucketSeconds)
	for i := range cs.shards {
		cs.shards[i].Range(func(k, v any) bool {
			s := v.(*counterSeries)
			for j := range s.slots {
				if slotEpoch(s.slots[j].Load()) >= cutoffEpoch {
					return true
				}
			}
			cs.shards[i].Delete(k)
			return true
		})
	}
}

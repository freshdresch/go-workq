// Package diagnostics owns queue-independent diagnostic storage and reduction.
package diagnostics

import (
	"math/bits"
	"sync/atomic"
	"time"
	"unsafe"

	diagnosticvalues "github.com/freshdresch/go-data-structures/workq/diagnostics"
)

// AtomicSizeHistogram records sizes in [0, 2^30] with atomic lifetime aggregates.
type AtomicSizeHistogram struct {
	Buckets [diagnosticvalues.SizeBucketCount]atomic.Uint64
	Count   atomic.Uint64
	Sum     atomic.Uint64
	Min     atomic.Uint64
	Max     atomic.Uint64
}

// AtomicCapacityHistogram also counts observations at the queue capacity.
type AtomicCapacityHistogram struct {
	AtomicSizeHistogram
	AtCapacity atomic.Uint64
}

// AtomicDurationHistogram records nonnegative monotonic durations atomically.
type AtomicDurationHistogram struct {
	Buckets [diagnosticvalues.DurationBucketCount]atomic.Uint64
	Count   atomic.Uint64
	Sum     atomic.Uint64
	Min     atomic.Uint64
	Max     atomic.Uint64
}

// SideCounters contains concurrent counters for one side of a queue shard.
type SideCounters struct {
	Attempts                      [diagnosticvalues.AttemptPhaseCount]atomic.Uint64
	ZeroProgress                  [diagnosticvalues.AttemptPhaseCount]atomic.Uint64
	PartialProgress               [diagnosticvalues.AttemptPhaseCount]atomic.Uint64
	FullProgress                  [diagnosticvalues.AttemptPhaseCount]atomic.Uint64
	RequestedItems                [diagnosticvalues.AttemptPhaseCount]atomic.Uint64
	MovedItems                    [diagnosticvalues.AttemptPhaseCount]atomic.Uint64
	WaitStarts                    atomic.Uint64
	WaitEnds                      [diagnosticvalues.WaitOutcomeCount]atomic.Uint64
	WaitEndsWithCloseRace         atomic.Uint64
	SpinEntries                   atomic.Uint64
	YieldEntries                  atomic.Uint64
	NeverParkEntries              atomic.Uint64
	YieldCalls                    [diagnosticvalues.AttemptPhaseCount]atomic.Uint64
	Registrations                 atomic.Uint64
	ParkEnlistments               atomic.Uint64
	ParkEnlistClosed              atomic.Uint64
	ParkSelectStarts              atomic.Uint64
	ParkSelectEnds                [diagnosticvalues.ParkSelectCaseCount]atomic.Uint64
	WakeCalls                     [diagnosticvalues.WakeKindCount]atomic.Uint64
	WakeTransfers                 [diagnosticvalues.WakeKindCount]atomic.Uint64
	WakeRecipients                atomic.Uint64
	TokensSent                    atomic.Uint64
	TokensReceivedForRetry        atomic.Uint64
	TokensDrainedByCleanup        atomic.Uint64
	WakeRetryAttempts             atomic.Uint64
	WakeRetryZeroProgress         atomic.Uint64
	WakeRetryEnds                 [diagnosticvalues.WakeRetryOutcomeCount]atomic.Uint64
	ProducerOldestReenlistments   atomic.Uint64
	CapacityEligibleAttempts      atomic.Uint64
	CapacitySelectedProbes        atomic.Uint64
	CapacityCompletedProbes       atomic.Uint64
	AttemptsWithoutRemainingCount atomic.Uint64
	RequestedSize                 AtomicSizeHistogram
	MovedSize                     AtomicSizeHistogram
	Capacity                      [diagnosticvalues.CapacitySourceCount]AtomicCapacityHistogram
}

// ProducerCounters separates the producer payload from adjacent shard data.
type ProducerCounters struct {
	SideCounters
	_ [216]byte
}

// ConsumerCounters includes consumer group sizes and isolates adjacent shard data.
type ConsumerCounters struct {
	SideCounters
	GroupTarget     AtomicSizeHistogram
	GroupActualSize AtomicSizeHistogram
	_               [248]byte
}

// CounterShard isolates producer and consumer counter payloads.
type CounterShard struct {
	Producer ProducerCounters
	Consumer ConsumerCounters
}

// SideTiming stores sampled wait durations separately from ordinary counters.
type SideTiming struct {
	WaitStarts         atomic.Uint64
	WaitSamples        atomic.Uint64
	ParkSelectStarts   atomic.Uint64
	ParkSelectSamples  atomic.Uint64
	WaitDuration       [diagnosticvalues.WaitOutcomeCount]AtomicDurationHistogram
	ParkSelectDuration [diagnosticvalues.ParkSelectCaseCount]AtomicDurationHistogram
	_                  [128]byte
}

// TimingShard isolates producer and consumer timing payloads.
type TimingShard struct {
	Producer SideTiming
	Consumer SideTiming
}

// Storage owns shards and immutable configuration. Queue code owns ticket assignment.
type Storage struct {
	CreationTicket atomic.Uint64
	Counters       []CounterShard
	Timing         []TimingShard
	Config         diagnosticvalues.DiagnosticsConfig
}

// AllocationSize bounds both slices and the descriptor before any allocation.
func AllocationSize(cfg diagnosticvalues.DiagnosticsConfig) (uintptr, bool) {
	if cfg.Mode == diagnosticvalues.DiagnosticsDisabled {
		return 0, true
	}
	if cfg.Shards < 1 {
		return 0, false
	}
	stride := unsafe.Sizeof(CounterShard{})
	if cfg.Mode == diagnosticvalues.DiagnosticsTiming {
		stride += unsafe.Sizeof(TimingShard{})
	}
	descriptor := unsafe.Sizeof(Storage{})
	maxInt := uintptr(^uint(0) >> 1)
	if uintptr(cfg.Shards) > (maxInt-descriptor)/stride {
		return 0, false
	}
	return descriptor + uintptr(cfg.Shards)*stride, true
}

// New requires the resolved configuration checked before engine construction.
func New(cfg diagnosticvalues.DiagnosticsConfig) *Storage {
	if cfg.Mode == diagnosticvalues.DiagnosticsDisabled {
		return nil
	}
	result := &Storage{
		Counters: make([]CounterShard, cfg.Shards),
		Config:   cfg,
	}
	for i := range result.Counters {
		shard := &result.Counters[i]
		shard.Producer.initializeMinima()
		shard.Consumer.initializeMinima()
		shard.Consumer.GroupTarget.Min.Store(^uint64(0))
		shard.Consumer.GroupActualSize.Min.Store(^uint64(0))
	}
	if cfg.Mode != diagnosticvalues.DiagnosticsTiming {
		return result
	}
	result.Timing = make([]TimingShard, cfg.Shards)
	for i := range result.Timing {
		result.Timing[i].Producer.initializeMinima()
		result.Timing[i].Consumer.initializeMinima()
	}
	return result
}

func (c *SideCounters) initializeMinima() {
	c.RequestedSize.Min.Store(^uint64(0))
	c.MovedSize.Min.Store(^uint64(0))
	for i := range c.Capacity {
		c.Capacity[i].Min.Store(^uint64(0))
	}
}

func (t *SideTiming) initializeMinima() {
	for i := range t.WaitDuration {
		t.WaitDuration[i].Min.Store(^uint64(0))
	}
	for i := range t.ParkSelectDuration {
		t.ParkSelectDuration[i].Min.Store(^uint64(0))
	}
}

// Record requires a size in the supported queue range [0, 2^30].
func (h *AtomicSizeHistogram) Record(value uint64) {
	bucket := int(value)
	if value > 32 {
		bucket = bits.Len64(value-1) + 27
	}
	h.Buckets[bucket].Add(1)
	h.Count.Add(1)
	h.Sum.Add(value)
	updateAtomicMinimum(&h.Min, value)
	updateAtomicMaximum(&h.Max, value)
}

// Record requires a supported queue size and the queue capacity.
func (h *AtomicCapacityHistogram) Record(value uint64, capacity uint32) {
	h.AtomicSizeHistogram.Record(value)
	if value == uint64(capacity) {
		h.AtCapacity.Add(1)
	}
}

// Record requires a nonnegative duration measured with a monotonic clock.
func (h *AtomicDurationHistogram) Record(duration time.Duration) {
	value := uint64(duration)
	h.Buckets[bits.Len64(value)].Add(1)
	h.Count.Add(1)
	h.Sum.Add(value)
	updateAtomicMinimum(&h.Min, value)
	updateAtomicMaximum(&h.Max, value)
}

func updateAtomicMinimum(target *atomic.Uint64, value uint64) {
	for current := target.Load(); value < current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

func updateAtomicMaximum(target *atomic.Uint64, value uint64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

// RecordCapacity records a source observation when diagnostics are enabled.
func (c *SideCounters) RecordCapacity(
	source diagnosticvalues.CapacitySource,
	remaining int,
	capacity uint32,
) {
	if c != nil {
		c.Capacity[source].Record(uint64(remaining), capacity)
	}
}

// RecordRetryToken accounts for a received retry token when diagnostics are enabled.
func (c *SideCounters) RecordRetryToken() {
	if c != nil {
		c.TokensReceivedForRetry.Add(1)
	}
}

// RecordYield accounts for a yield call when diagnostics are enabled.
func (c *SideCounters) RecordYield(phase diagnosticvalues.AttemptPhase) {
	if c != nil {
		c.YieldCalls[phase].Add(1)
	}
}

package workq

import (
	"context"
	"math/bits"
	"math/rand/v2"
	"sync/atomic"
	"time"
	"unsafe"
)

type atomicSizeHistogram struct {
	buckets [SizeBucketCount]atomic.Uint64
	count   atomic.Uint64
	sum     atomic.Uint64
	min     atomic.Uint64
	max     atomic.Uint64
}

type atomicCapacityHistogram struct {
	atomicSizeHistogram
	atCapacity atomic.Uint64
}

type atomicDurationHistogram struct {
	buckets [DurationBucketCount]atomic.Uint64
	count   atomic.Uint64
	sum     atomic.Uint64
	min     atomic.Uint64
	max     atomic.Uint64
}

type diagnosticsSideCounters struct {
	attempts                      [AttemptPhaseCount]atomic.Uint64
	zeroProgress                  [AttemptPhaseCount]atomic.Uint64
	partialProgress               [AttemptPhaseCount]atomic.Uint64
	fullProgress                  [AttemptPhaseCount]atomic.Uint64
	requestedItems                [AttemptPhaseCount]atomic.Uint64
	movedItems                    [AttemptPhaseCount]atomic.Uint64
	waitStarts                    atomic.Uint64
	waitEnds                      [WaitOutcomeCount]atomic.Uint64
	waitEndsWithCloseRace         atomic.Uint64
	spinEntries                   atomic.Uint64
	yieldEntries                  atomic.Uint64
	neverParkEntries              atomic.Uint64
	yieldCalls                    [AttemptPhaseCount]atomic.Uint64
	registrations                 atomic.Uint64
	parkEnlistments               atomic.Uint64
	parkEnlistClosed              atomic.Uint64
	parkSelectStarts              atomic.Uint64
	parkSelectEnds                [ParkSelectCaseCount]atomic.Uint64
	wakeCalls                     [WakeKindCount]atomic.Uint64
	wakeTransfers                 [WakeKindCount]atomic.Uint64
	wakeRecipients                atomic.Uint64
	tokensSent                    atomic.Uint64
	tokensReceivedForRetry        atomic.Uint64
	tokensDrainedByCleanup        atomic.Uint64
	wakeRetryAttempts             atomic.Uint64
	wakeRetryZeroProgress         atomic.Uint64
	wakeRetryEnds                 [WakeRetryOutcomeCount]atomic.Uint64
	producerOldestReenlistments   atomic.Uint64
	capacityEligibleAttempts      atomic.Uint64
	capacitySelectedProbes        atomic.Uint64
	capacityCompletedProbes       atomic.Uint64
	attemptsWithoutRemainingCount atomic.Uint64
	requestedSize                 atomicSizeHistogram
	movedSize                     atomicSizeHistogram
	capacity                      [CapacitySourceCount]atomicCapacityHistogram
}

type diagnosticsProducerCounters struct {
	diagnosticsSideCounters
	_ [216]byte
}

type diagnosticsConsumerCounters struct {
	diagnosticsSideCounters
	groupTarget     atomicSizeHistogram
	groupActualSize atomicSizeHistogram
	_               [248]byte
}

type diagnosticsCounterShard struct {
	producer diagnosticsProducerCounters
	consumer diagnosticsConsumerCounters
}

type diagnosticsSideTiming struct {
	waitStarts         atomic.Uint64
	waitSamples        atomic.Uint64
	parkSelectStarts   atomic.Uint64
	parkSelectSamples  atomic.Uint64
	waitDuration       [WaitOutcomeCount]atomicDurationHistogram
	parkSelectDuration [ParkSelectCaseCount]atomicDurationHistogram
	_                  [128]byte
}

type diagnosticsTimingShard struct {
	producer diagnosticsSideTiming
	consumer diagnosticsSideTiming
}

type diagnostics struct {
	counters []diagnosticsCounterShard
	timing   []diagnosticsTimingShard
	config   DiagnosticsConfig
}

// diagnosticsAllocationSize bounds both slices and the descriptor before any allocation.
func diagnosticsAllocationSize(cfg DiagnosticsConfig) (uintptr, bool) {
	if cfg.Mode == DiagnosticsDisabled {
		return 0, true
	}
	if cfg.Shards < 1 {
		return 0, false
	}
	stride := unsafe.Sizeof(diagnosticsCounterShard{})
	if cfg.Mode == DiagnosticsTiming {
		stride += unsafe.Sizeof(diagnosticsTimingShard{})
	}
	descriptor := unsafe.Sizeof(diagnostics{})
	maxInt := uintptr(^uint(0) >> 1)
	if uintptr(cfg.Shards) > (maxInt-descriptor)/stride {
		return 0, false
	}
	return descriptor + uintptr(cfg.Shards)*stride, true
}

// newDiagnostics requires the resolved configuration checked before engine construction.
func newDiagnostics(cfg DiagnosticsConfig) *diagnostics {
	if cfg.Mode == DiagnosticsDisabled {
		return nil
	}
	result := &diagnostics{
		counters: make([]diagnosticsCounterShard, cfg.Shards),
		config:   cfg,
	}
	for i := range result.counters {
		shard := &result.counters[i]
		shard.producer.initializeMinima()
		shard.consumer.initializeMinima()
		shard.consumer.groupTarget.min.Store(^uint64(0))
		shard.consumer.groupActualSize.min.Store(^uint64(0))
	}
	if cfg.Mode != DiagnosticsTiming {
		return result
	}
	result.timing = make([]diagnosticsTimingShard, cfg.Shards)
	for i := range result.timing {
		result.timing[i].producer.initializeMinima()
		result.timing[i].consumer.initializeMinima()
	}
	return result
}

func (c *diagnosticsSideCounters) initializeMinima() {
	c.requestedSize.min.Store(^uint64(0))
	c.movedSize.min.Store(^uint64(0))
	for i := range c.capacity {
		c.capacity[i].min.Store(^uint64(0))
	}
}

func (t *diagnosticsSideTiming) initializeMinima() {
	for i := range t.waitDuration {
		t.waitDuration[i].min.Store(^uint64(0))
	}
	for i := range t.parkSelectDuration {
		t.parkSelectDuration[i].min.Store(^uint64(0))
	}
}

// record requires a size in the supported queue range [0, 2^30].
func (h *atomicSizeHistogram) record(value uint64) {
	bucket := int(value)
	if value > 32 {
		bucket = bits.Len64(value-1) + 27
	}
	h.buckets[bucket].Add(1)
	h.count.Add(1)
	h.sum.Add(value)
	updateAtomicMinimum(&h.min, value)
	updateAtomicMaximum(&h.max, value)
}

func (h *atomicCapacityHistogram) record(value uint64, capacity uint32) {
	h.atomicSizeHistogram.record(value)
	if value == uint64(capacity) {
		h.atCapacity.Add(1)
	}
}

// record requires a nonnegative duration measured with a monotonic clock.
func (h *atomicDurationHistogram) record(duration time.Duration) {
	value := uint64(duration)
	h.buckets[bits.Len64(value)].Add(1)
	h.count.Add(1)
	h.sum.Add(value)
	updateAtomicMinimum(&h.min, value)
	updateAtomicMaximum(&h.max, value)
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

// diagnosticsRecording belongs to one invocation. Wakes use the observed side of its shard.
// Waiters never retain it, so recipients account for their own token receipts separately.
type diagnosticsRecording struct {
	counters             *diagnosticsCounterShard
	timing               *diagnosticsTimingShard
	timingSampleEvery    uint32
	capacitySampleEvery  uint32
	pendingCapacityProbe bool
	wait                 diagnosticsWaitRecording
}

func (r *diagnosticsRecording) producerCounters() *diagnosticsSideCounters {
	if r == nil {
		return nil
	}
	return &r.counters.producer.diagnosticsSideCounters
}

func (r *diagnosticsRecording) consumerCounters() *diagnosticsSideCounters {
	if r == nil {
		return nil
	}
	return &r.counters.consumer.diagnosticsSideCounters
}

func (r *diagnosticsRecording) consumerGroupCounters() *diagnosticsConsumerCounters {
	if r == nil {
		return nil
	}
	return &r.counters.consumer
}

func (c *diagnosticsSideCounters) recordCapacity(
	source CapacitySource,
	remaining int,
	capacity uint32,
) {
	if c != nil {
		c.capacity[source].record(uint64(remaining), capacity)
	}
}

func (c *diagnosticsSideCounters) recordRetryToken() {
	if c != nil {
		c.tokensReceivedForRetry.Add(1)
	}
}

func (d *diagnostics) chooseRecording() diagnosticsRecording {
	shard := rand.IntN(len(d.counters))
	recording := diagnosticsRecording{
		counters:            &d.counters[shard],
		capacitySampleEvery: d.config.CapacitySampleEvery,
	}
	if d.config.Mode == DiagnosticsTiming {
		recording.timing = &d.timing[shard]
		recording.timingSampleEvery = d.config.TimingSampleEvery
	}
	return recording
}

// Each call uses a separate bounded draw so timing and capacity selection stay independent.
func chooseDiagnosticsSample(every uint32) bool {
	if every == 0 {
		return false
	}
	return diagnosticsSampleSelected(every, rand.Uint32N(every))
}

// draw is uniform in [0, every). Zero disables selection without requiring a draw.
func diagnosticsSampleSelected(every, draw uint32) bool {
	return every != 0 && draw == 0
}

func (r *diagnosticsRecording) recordProducerAttempt(
	phase AttemptPhase,
	requested, moved, remaining int,
	capacity uint32,
) {
	r.recordAttempt(r.producerCounters(), phase, requested, moved, remaining, capacity)
}

func (r *diagnosticsRecording) recordConsumerAttempt(
	phase AttemptPhase,
	requested, moved, remaining int,
	capacity uint32,
) {
	r.recordAttempt(r.consumerCounters(), phase, requested, moved, remaining, capacity)
}

func (r *diagnosticsRecording) recordAttempt(
	counters *diagnosticsSideCounters,
	phase AttemptPhase,
	requested, moved, remaining int,
	capacity uint32,
) {
	if r == nil {
		return
	}
	r.recordWaitAttempt(counters, phase, moved)
	counters.attempts[phase].Add(1)
	counters.requestedItems[phase].Add(uint64(requested))
	counters.movedItems[phase].Add(uint64(moved))
	counters.requestedSize.record(uint64(requested))
	counters.movedSize.record(uint64(moved))
	switch {
	case moved == 0:
		counters.zeroProgress[phase].Add(1)
	case moved < requested:
		counters.partialProgress[phase].Add(1)
	default:
		counters.fullProgress[phase].Add(1)
	}
	if remaining < 0 {
		counters.attemptsWithoutRemainingCount.Add(1)
	} else {
		source := TransferCachedAllowed
		if moved < requested {
			source = TransferFresh
		}
		counters.recordCapacity(source, remaining, capacity)
	}
	counters.capacityEligibleAttempts.Add(1)
	if !chooseDiagnosticsSample(r.capacitySampleEvery) {
		return
	}
	counters.capacitySelectedProbes.Add(1)
	r.pendingCapacityProbe = true
}

func (q *Queue[T]) flushProducerProbe(recording *diagnosticsRecording) {
	if recording == nil {
		return
	}
	if !recording.pendingCapacityProbe {
		return
	}
	recording.pendingCapacityProbe = false
	_, remaining := q.engine.EnqueueBurst(nil)
	counters := recording.producerCounters()
	counters.recordCapacity(DiagnosticProbe, remaining, q.capacity)
	counters.capacityCompletedProbes.Add(1)
}

func (q *Queue[T]) flushConsumerProbe(recording *diagnosticsRecording) {
	if recording == nil {
		return
	}
	if !recording.pendingCapacityProbe {
		return
	}
	recording.pendingCapacityProbe = false
	_, remaining := q.engine.DequeueBurst(nil)
	counters := recording.consumerCounters()
	counters.recordCapacity(DiagnosticProbe, remaining, q.capacity)
	counters.capacityCompletedProbes.Add(1)
}

// The invocation reuses this state, but each wait starts a fresh episode. Producer priority
// stays in wait.go because its lifetime extends past an individual retry's re-enlistment.
type diagnosticsWaitRecording struct {
	phaseEntered bool
	retryActive  bool
	timing       *diagnosticsSideTiming
	started      time.Time
	parkStarted  time.Time
}

func (r *diagnosticsRecording) startWait(counters *diagnosticsSideCounters) {
	if r == nil {
		return
	}
	r.wait = diagnosticsWaitRecording{}
	counters.waitStarts.Add(1)
	if r.timing == nil {
		return
	}
	if !chooseDiagnosticsSample(r.timingSampleEvery) {
		return
	}
	r.wait.timing = &r.timing.consumer
	if counters == r.producerCounters() {
		r.wait.timing = &r.timing.producer
	}
	r.wait.timing.waitStarts.Add(1)
	r.wait.started = time.Now()
}

func (r *diagnosticsRecording) finishWait(
	counters *diagnosticsSideCounters,
	moved int,
	err error,
) {
	if r == nil {
		return
	}
	var finished time.Time
	if r.wait.timing != nil {
		finished = time.Now()
	}
	outcome := WaitProgress
	retryOutcome := RetryProgress
	if moved == 0 {
		switch err {
		case ErrClosed, ErrClosedDuringPush:
			outcome, retryOutcome = WaitClosed, RetryClosed
		case context.DeadlineExceeded:
			outcome, retryOutcome = WaitDeadline, RetryDeadline
		default:
			outcome, retryOutcome = WaitCanceled, RetryCanceled
		}
	}
	r.finishRetry(counters, retryOutcome)
	counters.waitEnds[outcome].Add(1)
	if err == ErrClosedDuringPush {
		counters.waitEndsWithCloseRace.Add(1)
	}
	if r.wait.timing != nil {
		r.wait.timing.waitDuration[outcome].record(finished.Sub(r.wait.started))
		r.wait.timing.waitSamples.Add(1)
	}
}

func (r *diagnosticsRecording) resetPhaseEntry() {
	if r != nil {
		r.wait.phaseEntered = false
	}
}

func (r *diagnosticsRecording) recordWaitAttempt(
	counters *diagnosticsSideCounters,
	phase AttemptPhase,
	moved int,
) {
	if !r.wait.phaseEntered {
		switch phase {
		case PhaseSpin:
			counters.spinEntries.Add(1)
			r.wait.phaseEntered = true
		case PhaseYield:
			counters.yieldEntries.Add(1)
			r.wait.phaseEntered = true
		case PhaseNeverPark:
			counters.neverParkEntries.Add(1)
			r.wait.phaseEntered = true
		}
	}
	if !r.wait.retryActive {
		return
	}
	counters.wakeRetryAttempts.Add(1)
	if moved == 0 {
		counters.wakeRetryZeroProgress.Add(1)
	}
}

func (c *diagnosticsSideCounters) recordYield(phase AttemptPhase) {
	if c != nil {
		c.yieldCalls[phase].Add(1)
	}
}

func (r *diagnosticsRecording) startParkSelect(counters *diagnosticsSideCounters) {
	if r == nil {
		return
	}
	counters.parkSelectStarts.Add(1)
	if r.wait.timing != nil {
		r.wait.timing.parkSelectStarts.Add(1)
		r.wait.parkStarted = time.Now()
	}
}

func (r *diagnosticsRecording) finishParkSelect(
	counters *diagnosticsSideCounters,
	selected ParkSelectCase,
) {
	if r == nil {
		return
	}
	if r.wait.timing != nil {
		finished := time.Now()
		r.wait.timing.parkSelectDuration[selected].record(finished.Sub(r.wait.parkStarted))
		r.wait.timing.parkSelectSamples.Add(1)
	}
	counters.parkSelectEnds[selected].Add(1)
}

func (r *diagnosticsRecording) startRetry(counters *diagnosticsSideCounters) {
	if r == nil {
		return
	}
	counters.recordRetryToken()
	r.wait.retryActive = true
}

func (r *diagnosticsRecording) finishRetry(
	counters *diagnosticsSideCounters,
	outcome WakeRetryOutcome,
) {
	if r == nil {
		return
	}
	if !r.wait.retryActive {
		return
	}
	r.wait.retryActive = false
	counters.wakeRetryEnds[outcome].Add(1)
}

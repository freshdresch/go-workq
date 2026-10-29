package workq

import (
	"github.com/freshdresch/go-data-structures/ring/dpdk"
	"github.com/freshdresch/go-data-structures/ring/frontier"
)

// Diagnostics returns a detached cumulative snapshot without calling the engine.
// Disabled snapshots contain static metadata only, even during waits and after Close.
// Each collector must finish one call before starting the next call in its delta series.
func (q *Queue[T]) Diagnostics() DiagnosticsSnapshot {
	return q.collectDiagnostics(nil)
}

// A per-call barrier lets tests overlap reductions without changing queue storage or load order.
type diagnosticsReductionControl struct {
	counters *diagnosticsSideCounters
	loaded   chan<- struct{}
	resume   <-chan struct{}
}

func (q *Queue[T]) collectDiagnostics(control *diagnosticsReductionControl) DiagnosticsSnapshot {
	snapshot := DiagnosticsSnapshot{
		SchemaVersion: DiagnosticsSchemaVersion,
		Config: DiagnosticsStaticConfig{
			Engine:          q.diagnosticsEngineKind(),
			Capacity:        q.capacity,
			MaxPushBatch:    q.maxPushBatch,
			MaxReceiveBatch: q.maxReceiveBatch,
			ConsumerWait:    q.consumerWait,
			ProducerWait:    q.producerWait,
			ConsumerWakeCap: q.consumerWakeCap,
		},
	}
	if q.diagnostics == nil {
		return snapshot
	}
	snapshot.Config.DiagnosticsConfig = q.diagnostics.config
	snapshot.Producer.initializeHistogramMinima()
	snapshot.Consumer.initializeHistogramMinima()
	for i := range snapshot.Producer.ReusableSlots {
		snapshot.Producer.ReusableSlots[i].Min = ^uint64(0)
		snapshot.Consumer.AvailableItems[i].Min = ^uint64(0)
	}
	for i := range q.diagnostics.counters {
		shard := &q.diagnostics.counters[i]
		snapshot.Producer.addCounters(&shard.producer.diagnosticsSideCounters, control)
		snapshot.Consumer.addCounters(&shard.consumer.diagnosticsSideCounters, control)
		snapshot.Consumer.GroupTarget.addHistogram(&shard.consumer.groupTarget)
		snapshot.Consumer.GroupActualSize.addHistogram(&shard.consumer.groupActualSize)
		for source := range shard.producer.capacity {
			snapshot.Producer.ReusableSlots[source].addHistogram(&shard.producer.capacity[source])
			snapshot.Consumer.AvailableItems[source].addHistogram(&shard.consumer.capacity[source])
		}
	}
	for i := range q.diagnostics.timing {
		shard := &q.diagnostics.timing[i]
		snapshot.Producer.Timing.addTiming(&shard.producer)
		snapshot.Consumer.Timing.addTiming(&shard.consumer)
	}
	snapshot.Producer.normalizeEmptyHistograms()
	snapshot.Consumer.normalizeEmptyHistograms()
	for i := range snapshot.Producer.ReusableSlots {
		snapshot.Producer.ReusableSlots[i].normalizeEmptyMinimum()
		snapshot.Consumer.AvailableItems[i].normalizeEmptyMinimum()
	}
	snapshot.WaitState.Producer = diagnosticsWaitState(q.producers.load())
	snapshot.WaitState.Consumer = diagnosticsWaitState(q.consumers.load())
	return snapshot
}

func (q *Queue[T]) diagnosticsEngineKind() EngineKind {
	switch q.engine.(type) {
	case *dpdk.SPSCQueue[T]:
		return EngineSPSC
	case *frontier.MPSCQueue[T]:
		return EngineMPSC
	case *frontier.SPMCQueue[T]:
		return EngineSPMC
	case *frontier.MPMCQueue[T]:
		return EngineMPMC
	default:
		return EngineUnknown
	}
}

func diagnosticsWaitState(word sideWord) DiagnosticsSideWaitState {
	return DiagnosticsSideWaitState{
		Pollers: word.pollers(),
		Parked:  word.parked(),
		Closed:  word.closed(),
	}
}

// The sentinel distinguishes untouched histograms even after Count wraps to zero.
func (s *DiagnosticsSideSnapshot) initializeHistogramMinima() {
	s.RequestedSize.Min = ^uint64(0)
	s.MovedSize.Min = ^uint64(0)
	s.GroupTarget.Min = ^uint64(0)
	s.GroupActualSize.Min = ^uint64(0)
	for i := range s.Timing.WaitDuration {
		s.Timing.WaitDuration[i].Min = ^uint64(0)
	}
	for i := range s.Timing.ParkSelectDuration {
		s.Timing.ParkSelectDuration[i].Min = ^uint64(0)
	}
}

func (s *DiagnosticsSideSnapshot) normalizeEmptyHistograms() {
	s.RequestedSize.normalizeEmptyMinimum()
	s.MovedSize.normalizeEmptyMinimum()
	s.GroupTarget.normalizeEmptyMinimum()
	s.GroupActualSize.normalizeEmptyMinimum()
	for i := range s.Timing.WaitDuration {
		s.Timing.WaitDuration[i].normalizeEmptyMinimum()
	}
	for i := range s.Timing.ParkSelectDuration {
		s.Timing.ParkSelectDuration[i].normalizeEmptyMinimum()
	}
}

func (s *DiagnosticsSideSnapshot) addCounters(
	counters *diagnosticsSideCounters,
	control *diagnosticsReductionControl,
) {
	for i := range s.Attempts {
		s.Attempts[i] += counters.attempts[i].Load()
	}
	for i := range s.ZeroProgress {
		s.ZeroProgress[i] += counters.zeroProgress[i].Load()
	}
	for i := range s.PartialProgress {
		s.PartialProgress[i] += counters.partialProgress[i].Load()
	}
	for i := range s.FullProgress {
		s.FullProgress[i] += counters.fullProgress[i].Load()
	}
	for i := range s.RequestedItems {
		s.RequestedItems[i] += counters.requestedItems[i].Load()
	}
	for i := range s.MovedItems {
		s.MovedItems[i] += counters.movedItems[i].Load()
	}
	s.WaitStarts += counters.waitStarts.Load()
	if control != nil && control.counters == counters {
		control.loaded <- struct{}{}
		<-control.resume
	}
	for i := range s.WaitEnds {
		s.WaitEnds[i] += counters.waitEnds[i].Load()
	}
	s.WaitEndsWithCloseRace += counters.waitEndsWithCloseRace.Load()
	s.SpinEntries += counters.spinEntries.Load()
	s.YieldEntries += counters.yieldEntries.Load()
	s.NeverParkEntries += counters.neverParkEntries.Load()
	for i := range s.YieldCalls {
		s.YieldCalls[i] += counters.yieldCalls[i].Load()
	}
	s.Registrations += counters.registrations.Load()
	s.ParkEnlistments += counters.parkEnlistments.Load()
	s.ParkEnlistClosed += counters.parkEnlistClosed.Load()
	s.ParkSelectStarts += counters.parkSelectStarts.Load()
	for i := range s.ParkSelectEnds {
		s.ParkSelectEnds[i] += counters.parkSelectEnds[i].Load()
	}
	for i := range s.WakeCalls {
		s.WakeCalls[i] += counters.wakeCalls[i].Load()
	}
	for i := range s.WakeTransfers {
		s.WakeTransfers[i] += counters.wakeTransfers[i].Load()
	}
	s.WakeRecipients += counters.wakeRecipients.Load()
	s.TokensSent += counters.tokensSent.Load()
	s.TokensReceivedForRetry += counters.tokensReceivedForRetry.Load()
	s.TokensDrainedByCleanup += counters.tokensDrainedByCleanup.Load()
	s.WakeRetryAttempts += counters.wakeRetryAttempts.Load()
	s.WakeRetryZeroProgress += counters.wakeRetryZeroProgress.Load()
	for i := range s.WakeRetryEnds {
		s.WakeRetryEnds[i] += counters.wakeRetryEnds[i].Load()
	}
	s.ProducerOldestReenlistments += counters.producerOldestReenlistments.Load()
	s.CapacityEligibleAttempts += counters.capacityEligibleAttempts.Load()
	s.CapacitySelectedProbes += counters.capacitySelectedProbes.Load()
	s.CapacityCompletedProbes += counters.capacityCompletedProbes.Load()
	s.AttemptsWithoutRemainingCount += counters.attemptsWithoutRemainingCount.Load()

	s.RequestedSize.addHistogram(&counters.requestedSize)
	s.MovedSize.addHistogram(&counters.movedSize)
}

func (s *DiagnosticsTimingSnapshot) addTiming(timing *diagnosticsSideTiming) {
	s.WaitStarts += timing.waitStarts.Load()
	s.WaitSamples += timing.waitSamples.Load()
	s.ParkSelectStarts += timing.parkSelectStarts.Load()
	s.ParkSelectSamples += timing.parkSelectSamples.Load()
	for i := range s.WaitDuration {
		s.WaitDuration[i].addHistogram(&timing.waitDuration[i])
	}
	for i := range s.ParkSelectDuration {
		s.ParkSelectDuration[i].addHistogram(&timing.parkSelectDuration[i])
	}
}

func (h *SizeHistogram) addHistogram(source *atomicSizeHistogram) {
	for i := range h.Buckets {
		h.Buckets[i] += source.buckets[i].Load()
	}
	h.Count += source.count.Load()
	h.Sum += source.sum.Load()
	h.Min = min(h.Min, source.min.Load())
	h.Max = max(h.Max, source.max.Load())
}

func (h *SizeHistogram) normalizeEmptyMinimum() {
	if h.Min == ^uint64(0) {
		h.Min = 0
	}
}

func (h *DurationHistogram) addHistogram(source *atomicDurationHistogram) {
	for i := range h.Buckets {
		h.Buckets[i] += source.buckets[i].Load()
	}
	h.Count += source.count.Load()
	h.Sum += source.sum.Load()
	h.Min = min(h.Min, source.min.Load())
	h.Max = max(h.Max, source.max.Load())
}

func (h *DurationHistogram) normalizeEmptyMinimum() {
	if h.Min == ^uint64(0) {
		h.Min = 0
	}
}

func (h *CapacityHistogram) addHistogram(source *atomicCapacityHistogram) {
	h.SizeHistogram.addHistogram(&source.atomicSizeHistogram)
	h.ZeroCount = h.Buckets[0]
	h.AtCapacity += source.atCapacity.Load()
}

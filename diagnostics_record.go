package workq

import (
	"context"
	"math/rand/v2"
	"time"

	internaldiagnostics "github.com/freshdresch/go-data-structures/workq/internal/diagnostics"
)

type diagnostics internaldiagnostics.Storage

func diagnosticsAllocationSize(cfg DiagnosticsConfig) (uintptr, bool) {
	return internaldiagnostics.AllocationSize(cfg)
}

func newDiagnostics(cfg DiagnosticsConfig) *diagnostics {
	return (*diagnostics)(internaldiagnostics.New(cfg))
}

// diagnosticsRecording belongs to one invocation. Wakes use the observed side of its shard.
// Waiters never retain it, so recipients account for their own token receipts separately.
type diagnosticsRecording struct {
	counters             *internaldiagnostics.CounterShard
	timing               *internaldiagnostics.TimingShard
	timingSampleEvery    uint32
	capacitySampleEvery  uint32
	pendingCapacityProbe bool
	wait                 diagnosticsWaitRecording
}

func (r *diagnosticsRecording) producerCounters() *internaldiagnostics.SideCounters {
	if r == nil {
		return nil
	}
	return &r.counters.Producer.SideCounters
}

func (r *diagnosticsRecording) consumerCounters() *internaldiagnostics.SideCounters {
	if r == nil {
		return nil
	}
	return &r.counters.Consumer.SideCounters
}

func (r *diagnosticsRecording) consumerGroupCounters() *internaldiagnostics.ConsumerCounters {
	if r == nil {
		return nil
	}
	return &r.counters.Consumer
}

func (d *diagnostics) chooseRecording() diagnosticsRecording {
	return d.recordingAt(rand.IntN(len(d.Counters)))
}

func (d *diagnostics) recordingAt(shard int) diagnosticsRecording {
	recording := diagnosticsRecording{
		counters:            &d.Counters[shard],
		capacitySampleEvery: d.Config.CapacitySampleEvery,
	}
	if d.Config.Mode == DiagnosticsTiming {
		recording.timing = &d.Timing[shard]
		recording.timingSampleEvery = d.Config.TimingSampleEvery
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
	counters *internaldiagnostics.SideCounters,
	phase AttemptPhase,
	requested, moved, remaining int,
	capacity uint32,
) {
	if r == nil {
		return
	}
	r.recordWaitAttempt(counters, phase, moved)
	counters.Attempts[phase].Add(1)
	counters.RequestedItems[phase].Add(uint64(requested))
	counters.MovedItems[phase].Add(uint64(moved))
	counters.RequestedSize.Record(uint64(requested))
	counters.MovedSize.Record(uint64(moved))
	switch {
	case moved == 0:
		counters.ZeroProgress[phase].Add(1)
	case moved < requested:
		counters.PartialProgress[phase].Add(1)
	default:
		counters.FullProgress[phase].Add(1)
	}
	if remaining < 0 {
		counters.AttemptsWithoutRemainingCount.Add(1)
	} else {
		source := TransferCachedAllowed
		if moved < requested {
			source = TransferFresh
		}
		counters.RecordCapacity(source, remaining, capacity)
	}
	counters.CapacityEligibleAttempts.Add(1)
	if !chooseDiagnosticsSample(r.capacitySampleEvery) {
		return
	}
	counters.CapacitySelectedProbes.Add(1)
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
	counters.RecordCapacity(DiagnosticProbe, remaining, q.capacity)
	counters.CapacityCompletedProbes.Add(1)
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
	counters.RecordCapacity(DiagnosticProbe, remaining, q.capacity)
	counters.CapacityCompletedProbes.Add(1)
}

// The invocation reuses this state, but each wait starts a fresh episode. Producer priority
// stays in wait.go because its lifetime extends past an individual retry's re-enlistment.
type diagnosticsWaitRecording struct {
	phaseEntered bool
	retryActive  bool
	timing       *internaldiagnostics.SideTiming
	started      time.Time
	parkStarted  time.Time
}

func (r *diagnosticsRecording) startWait(counters *internaldiagnostics.SideCounters) {
	if r == nil {
		return
	}
	r.wait = diagnosticsWaitRecording{}
	counters.WaitStarts.Add(1)
	if r.timing == nil {
		return
	}
	if !chooseDiagnosticsSample(r.timingSampleEvery) {
		return
	}
	r.wait.timing = &r.timing.Consumer
	if counters == r.producerCounters() {
		r.wait.timing = &r.timing.Producer
	}
	r.wait.timing.WaitStarts.Add(1)
	r.wait.started = time.Now()
}

func (r *diagnosticsRecording) finishWait(
	counters *internaldiagnostics.SideCounters,
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
	counters.WaitEnds[outcome].Add(1)
	if err == ErrClosedDuringPush {
		counters.WaitEndsWithCloseRace.Add(1)
	}
	if r.wait.timing != nil {
		r.wait.timing.WaitDuration[outcome].Record(finished.Sub(r.wait.started))
		r.wait.timing.WaitSamples.Add(1)
	}
}

func (r *diagnosticsRecording) resetPhaseEntry() {
	if r != nil {
		r.wait.phaseEntered = false
	}
}

func (r *diagnosticsRecording) recordWaitAttempt(
	counters *internaldiagnostics.SideCounters,
	phase AttemptPhase,
	moved int,
) {
	if !r.wait.phaseEntered {
		switch phase {
		case PhaseSpin:
			counters.SpinEntries.Add(1)
			r.wait.phaseEntered = true
		case PhaseYield:
			counters.YieldEntries.Add(1)
			r.wait.phaseEntered = true
		case PhaseNeverPark:
			counters.NeverParkEntries.Add(1)
			r.wait.phaseEntered = true
		}
	}
	if !r.wait.retryActive {
		return
	}
	counters.WakeRetryAttempts.Add(1)
	if moved == 0 {
		counters.WakeRetryZeroProgress.Add(1)
	}
}

func (r *diagnosticsRecording) startParkSelect(counters *internaldiagnostics.SideCounters) {
	if r == nil {
		return
	}
	counters.ParkSelectStarts.Add(1)
	if r.wait.timing != nil {
		r.wait.timing.ParkSelectStarts.Add(1)
		r.wait.parkStarted = time.Now()
	}
}

func (r *diagnosticsRecording) finishParkSelect(
	counters *internaldiagnostics.SideCounters,
	selected ParkSelectCase,
) {
	if r == nil {
		return
	}
	if r.wait.timing != nil {
		finished := time.Now()
		r.wait.timing.ParkSelectDuration[selected].Record(finished.Sub(r.wait.parkStarted))
		r.wait.timing.ParkSelectSamples.Add(1)
	}
	counters.ParkSelectEnds[selected].Add(1)
}

func (r *diagnosticsRecording) startRetry(counters *internaldiagnostics.SideCounters) {
	if r == nil {
		return
	}
	counters.RecordRetryToken()
	r.wait.retryActive = true
}

func (r *diagnosticsRecording) finishRetry(
	counters *internaldiagnostics.SideCounters,
	outcome WakeRetryOutcome,
) {
	if r == nil {
		return
	}
	if !r.wait.retryActive {
		return
	}
	r.wait.retryActive = false
	counters.WakeRetryEnds[outcome].Add(1)
}

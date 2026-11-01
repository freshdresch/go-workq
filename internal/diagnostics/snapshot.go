package diagnostics

import diagnosticvalues "github.com/freshdresch/go-data-structures/workq/diagnostics"

// ReductionControl pauses only its collection call after the selected wait-start load.
// Production collectors pass nil, so no queue-owned synchronization is introduced.
type ReductionControl struct {
	Counters *SideCounters
	Loaded   chan<- struct{}
	Resume   <-chan struct{}
}

// Snapshot returns detached cumulative values in the original reduction load order.
func (d *Storage) Snapshot(control *ReductionControl) diagnosticvalues.DiagnosticsSnapshot {
	var snapshot diagnosticvalues.DiagnosticsSnapshot
	initializeHistogramMinima(&snapshot.Producer.DiagnosticsSideSnapshot)
	initializeHistogramMinima(&snapshot.Consumer.DiagnosticsSideSnapshot)
	for i := range snapshot.Producer.ReusableSlots {
		snapshot.Producer.ReusableSlots[i].Min = ^uint64(0)
		snapshot.Consumer.AvailableItems[i].Min = ^uint64(0)
	}
	for i := range d.Counters {
		shard := &d.Counters[i]
		addCounters(
			&snapshot.Producer.DiagnosticsSideSnapshot,
			&shard.Producer.SideCounters,
			control,
		)
		addCounters(
			&snapshot.Consumer.DiagnosticsSideSnapshot,
			&shard.Consumer.SideCounters,
			control,
		)
		addSizeHistogram(&snapshot.Consumer.GroupTarget, &shard.Consumer.GroupTarget)
		addSizeHistogram(&snapshot.Consumer.GroupActualSize, &shard.Consumer.GroupActualSize)
		for source := range shard.Producer.Capacity {
			addCapacityHistogram(&snapshot.Producer.ReusableSlots[source], &shard.Producer.Capacity[source])
			addCapacityHistogram(&snapshot.Consumer.AvailableItems[source], &shard.Consumer.Capacity[source])
		}
	}
	for i := range d.Timing {
		shard := &d.Timing[i]
		addTiming(&snapshot.Producer.Timing, &shard.Producer)
		addTiming(&snapshot.Consumer.Timing, &shard.Consumer)
	}
	normalizeEmptyHistograms(&snapshot.Producer.DiagnosticsSideSnapshot)
	normalizeEmptyHistograms(&snapshot.Consumer.DiagnosticsSideSnapshot)
	for i := range snapshot.Producer.ReusableSlots {
		normalizeEmptySizeMinimum(&snapshot.Producer.ReusableSlots[i].SizeHistogram)
		normalizeEmptySizeMinimum(&snapshot.Consumer.AvailableItems[i].SizeHistogram)
	}
	return snapshot
}

// initializeHistogramMinima mutates snapshot. The sentinel distinguishes untouched histograms
// even after Count wraps to zero.
func initializeHistogramMinima(snapshot *diagnosticvalues.DiagnosticsSideSnapshot) {
	snapshot.RequestedSize.Min = ^uint64(0)
	snapshot.MovedSize.Min = ^uint64(0)
	snapshot.GroupTarget.Min = ^uint64(0)
	snapshot.GroupActualSize.Min = ^uint64(0)
	for i := range snapshot.Timing.WaitDuration {
		snapshot.Timing.WaitDuration[i].Min = ^uint64(0)
	}
	for i := range snapshot.Timing.ParkSelectDuration {
		snapshot.Timing.ParkSelectDuration[i].Min = ^uint64(0)
	}
}

// normalizeEmptyHistograms mutates snapshot to clear untouched minima.
func normalizeEmptyHistograms(snapshot *diagnosticvalues.DiagnosticsSideSnapshot) {
	normalizeEmptySizeMinimum(&snapshot.RequestedSize)
	normalizeEmptySizeMinimum(&snapshot.MovedSize)
	normalizeEmptySizeMinimum(&snapshot.GroupTarget)
	normalizeEmptySizeMinimum(&snapshot.GroupActualSize)
	for i := range snapshot.Timing.WaitDuration {
		normalizeEmptyDurationMinimum(&snapshot.Timing.WaitDuration[i])
	}
	for i := range snapshot.Timing.ParkSelectDuration {
		normalizeEmptyDurationMinimum(&snapshot.Timing.ParkSelectDuration[i])
	}
}

// addCounters mutates snapshot by reducing counters in schema order.
func addCounters(
	snapshot *diagnosticvalues.DiagnosticsSideSnapshot,
	counters *SideCounters,
	control *ReductionControl,
) {
	for i := range snapshot.Attempts {
		snapshot.Attempts[i] += counters.Attempts[i].Load()
	}
	for i := range snapshot.ZeroProgress {
		snapshot.ZeroProgress[i] += counters.ZeroProgress[i].Load()
	}
	for i := range snapshot.PartialProgress {
		snapshot.PartialProgress[i] += counters.PartialProgress[i].Load()
	}
	for i := range snapshot.FullProgress {
		snapshot.FullProgress[i] += counters.FullProgress[i].Load()
	}
	for i := range snapshot.RequestedItems {
		snapshot.RequestedItems[i] += counters.RequestedItems[i].Load()
	}
	for i := range snapshot.MovedItems {
		snapshot.MovedItems[i] += counters.MovedItems[i].Load()
	}
	snapshot.WaitStarts += counters.WaitStarts.Load()
	if control != nil && control.Counters == counters {
		control.Loaded <- struct{}{}
		<-control.Resume
	}
	for i := range snapshot.WaitEnds {
		snapshot.WaitEnds[i] += counters.WaitEnds[i].Load()
	}
	snapshot.WaitEndsWithCloseRace += counters.WaitEndsWithCloseRace.Load()
	snapshot.SpinEntries += counters.SpinEntries.Load()
	snapshot.YieldEntries += counters.YieldEntries.Load()
	snapshot.NeverParkEntries += counters.NeverParkEntries.Load()
	for i := range snapshot.YieldCalls {
		snapshot.YieldCalls[i] += counters.YieldCalls[i].Load()
	}
	snapshot.Registrations += counters.Registrations.Load()
	snapshot.ParkEnlistments += counters.ParkEnlistments.Load()
	snapshot.ParkEnlistClosed += counters.ParkEnlistClosed.Load()
	snapshot.ParkSelectStarts += counters.ParkSelectStarts.Load()
	for i := range snapshot.ParkSelectEnds {
		snapshot.ParkSelectEnds[i] += counters.ParkSelectEnds[i].Load()
	}
	for i := range snapshot.WakeCalls {
		snapshot.WakeCalls[i] += counters.WakeCalls[i].Load()
	}
	for i := range snapshot.WakeTransfers {
		snapshot.WakeTransfers[i] += counters.WakeTransfers[i].Load()
	}
	snapshot.WakeRecipients += counters.WakeRecipients.Load()
	snapshot.TokensSent += counters.TokensSent.Load()
	snapshot.TokensReceivedForRetry += counters.TokensReceivedForRetry.Load()
	snapshot.TokensDrainedByCleanup += counters.TokensDrainedByCleanup.Load()
	snapshot.WakeRetryAttempts += counters.WakeRetryAttempts.Load()
	snapshot.WakeRetryZeroProgress += counters.WakeRetryZeroProgress.Load()
	for i := range snapshot.WakeRetryEnds {
		snapshot.WakeRetryEnds[i] += counters.WakeRetryEnds[i].Load()
	}
	snapshot.ProducerOldestReenlistments += counters.ProducerOldestReenlistments.Load()
	snapshot.CapacityEligibleAttempts += counters.CapacityEligibleAttempts.Load()
	snapshot.CapacitySelectedProbes += counters.CapacitySelectedProbes.Load()
	snapshot.CapacityCompletedProbes += counters.CapacityCompletedProbes.Load()
	snapshot.AttemptsWithoutRemainingCount += counters.AttemptsWithoutRemainingCount.Load()

	addSizeHistogram(&snapshot.RequestedSize, &counters.RequestedSize)
	addSizeHistogram(&snapshot.MovedSize, &counters.MovedSize)
}

// addTiming mutates snapshot by reducing timing counters and histograms.
func addTiming(snapshot *diagnosticvalues.DiagnosticsTimingSnapshot, timing *SideTiming) {
	snapshot.WaitStarts += timing.WaitStarts.Load()
	snapshot.WaitSamples += timing.WaitSamples.Load()
	snapshot.ParkSelectStarts += timing.ParkSelectStarts.Load()
	snapshot.ParkSelectSamples += timing.ParkSelectSamples.Load()
	for i := range snapshot.WaitDuration {
		addDurationHistogram(&snapshot.WaitDuration[i], &timing.WaitDuration[i])
	}
	for i := range snapshot.ParkSelectDuration {
		addDurationHistogram(&snapshot.ParkSelectDuration[i], &timing.ParkSelectDuration[i])
	}
}

// addSizeHistogram mutates histogram by reducing source in bucket order.
func addSizeHistogram(histogram *diagnosticvalues.SizeHistogram, source *AtomicSizeHistogram) {
	for i := range histogram.Buckets {
		histogram.Buckets[i] += source.Buckets[i].Load()
	}
	histogram.Count += source.Count.Load()
	histogram.Sum += source.Sum.Load()
	histogram.Min = min(histogram.Min, source.Min.Load())
	histogram.Max = max(histogram.Max, source.Max.Load())
}

// normalizeEmptySizeMinimum mutates histogram to clear an untouched minimum.
func normalizeEmptySizeMinimum(histogram *diagnosticvalues.SizeHistogram) {
	if histogram.Min == ^uint64(0) {
		histogram.Min = 0
	}
}

// addDurationHistogram mutates histogram by reducing source in bucket order.
func addDurationHistogram(
	histogram *diagnosticvalues.DurationHistogram,
	source *AtomicDurationHistogram,
) {
	for i := range histogram.Buckets {
		histogram.Buckets[i] += source.Buckets[i].Load()
	}
	histogram.Count += source.Count.Load()
	histogram.Sum += source.Sum.Load()
	histogram.Min = min(histogram.Min, source.Min.Load())
	histogram.Max = max(histogram.Max, source.Max.Load())
}

// normalizeEmptyDurationMinimum mutates histogram to clear an untouched minimum.
func normalizeEmptyDurationMinimum(histogram *diagnosticvalues.DurationHistogram) {
	if histogram.Min == ^uint64(0) {
		histogram.Min = 0
	}
}

// addCapacityHistogram mutates histogram by reducing source and its capacity count.
func addCapacityHistogram(
	histogram *diagnosticvalues.CapacityHistogram,
	source *AtomicCapacityHistogram,
) {
	addSizeHistogram(&histogram.SizeHistogram, &source.AtomicSizeHistogram)
	histogram.ZeroCount = histogram.Buckets[0]
	histogram.AtCapacity += source.AtCapacity.Load()
}

package workq

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticsHistogramSizeEdges(t *testing.T) {
	for bucket := 0; bucket < SizeBucketCount; bucket++ {
		lower, upper := uint64(bucket), uint64(bucket)
		if bucket == 33 {
			lower, upper = 33, 64
		} else if bucket > 33 {
			upper = uint64(1) << (bucket - 27)
			lower = upper/2 + 1
		}
		for _, value := range []uint64{lower, upper} {
			var histogram atomicSizeHistogram
			histogram.min.Store(math.MaxUint64)
			histogram.record(value)
			want := SizeHistogram{Count: 1, Sum: value, Min: value, Max: value}
			want.Buckets[bucket] = 1
			got := SizeHistogram{Min: math.MaxUint64}
			got.addHistogram(&histogram)
			assert.Equal(t, want, got, "size %d", value)
		}
	}
}

func TestDiagnosticsHistogramDurationEdges(t *testing.T) {
	for bucket := 0; bucket < DurationBucketCount; bucket++ {
		var lower, upper uint64
		if bucket > 0 {
			lower = uint64(1) << (bucket - 1)
			upper = uint64(1)<<bucket - 1
		}
		for _, value := range []uint64{lower, upper} {
			var histogram atomicDurationHistogram
			histogram.min.Store(math.MaxUint64)
			histogram.record(time.Duration(value))
			want := DurationHistogram{Count: 1, Sum: value, Min: value, Max: value}
			want.Buckets[bucket] = 1
			got := DurationHistogram{Min: math.MaxUint64}
			got.addHistogram(&histogram)
			assert.Equal(t, want, got, "duration %d", value)
		}
	}
}

func TestDiagnosticsHistogramCapacityAndEmpty(t *testing.T) {
	queue, err := New[int](64, WithDiagnostics(DiagnosticsConfig{
		Mode: DiagnosticsTiming, Shards: 2,
	}))
	require.NoError(t, err)
	empty := queue.Diagnostics()
	assert.Equal(t, DiagnosticsProducerSnapshot{}, empty.Producer)
	assert.Equal(t, DiagnosticsConsumerSnapshot{}, empty.Consumer)
	for source := CapacitySource(0); source < CapacitySourceCount; source++ {
		histogram := &queue.diagnostics.counters[1].producer.capacity[source]
		histogram.record(0, 64)
		histogram.record(64, 64)
		histogram.record(uint64(source)+1, 64)
	}
	snapshot := queue.Diagnostics()
	for source, histogram := range snapshot.Producer.ReusableSlots {
		want := CapacityHistogram{
			SizeHistogram: SizeHistogram{Count: 3, Sum: 65 + uint64(source), Max: 64},
			ZeroCount:     1, AtCapacity: 1,
		}
		want.Buckets[0], want.Buckets[33], want.Buckets[source+1] = 1, 1, 1
		assert.Equal(t, want, histogram, "source %d", source)
	}
	assert.Equal(t, DiagnosticsConsumerSnapshot{}, snapshot.Consumer)
}

func TestDiagnosticsHistogramWrap(t *testing.T) {
	queue, err := New[int](64, WithDiagnostics(DiagnosticsConfig{
		Mode: DiagnosticsTiming, Shards: 2,
	}))
	require.NoError(t, err)
	for i := range queue.diagnostics.counters {
		histogram := &queue.diagnostics.counters[i].producer.requestedSize
		histogram.count.Store(math.MaxUint64)
		histogram.sum.Store(math.MaxUint64)
		histogram.buckets[3].Store(math.MaxUint64)
		histogram.record(3)
		duration := &queue.diagnostics.timing[i].consumer.waitDuration[WaitProgress]
		duration.count.Store(math.MaxUint64)
		duration.sum.Store(math.MaxUint64)
		duration.buckets[2].Store(math.MaxUint64)
		duration.record(3)
	}
	snapshot := queue.Diagnostics()
	assert.Equal(t, SizeHistogram{Sum: 4, Min: 3, Max: 3}, snapshot.Producer.RequestedSize)
	assert.Equal(
		t,
		DurationHistogram{Sum: 4, Min: 3, Max: 3},
		snapshot.Consumer.Timing.WaitDuration[WaitProgress],
	)
	// Reduction itself must wrap, independently of recording into a shard.
	for i := range queue.diagnostics.counters {
		histogram := &queue.diagnostics.counters[i].consumer.capacity[DiagnosticProbe]
		histogram.record(64, 64)
		histogram.count.Store(uint64(1) << 63)
		histogram.sum.Store(uint64(1) << 63)
		histogram.buckets[33].Store(uint64(1) << 63)
		histogram.atCapacity.Store(uint64(1) << 63)
	}
	assert.Equal(
		t,
		CapacityHistogram{SizeHistogram: SizeHistogram{Min: 64, Max: 64}},
		queue.Diagnostics().Consumer.AvailableItems[DiagnosticProbe],
	)
}

func TestDiagnosticsSnapshotShardsAndDetachment(t *testing.T) {
	queue, err := New[int](64, WithDiagnostics(DiagnosticsConfig{
		Mode: DiagnosticsTiming, Shards: 4,
	}))
	require.NoError(t, err)
	for i := 1; i < len(queue.diagnostics.counters); i++ {
		shard := &queue.diagnostics.counters[i]
		shard.producer.requestedSize.record(uint64(i + 1))
		shard.consumer.movedSize.record(uint64(i))
		shard.consumer.groupTarget.record(uint64(i + 4))
		shard.consumer.groupActualSize.record(uint64(i - 1))
		for source := range shard.producer.capacity {
			shard.producer.capacity[source].record(uint64(i+source), 64)
			shard.consumer.capacity[source].record(uint64(i+source+1), 64)
		}
		for outcome := range queue.diagnostics.timing[i].producer.waitDuration {
			queue.diagnostics.timing[i].producer.waitDuration[outcome].record(
				time.Duration(i + outcome),
			)
		}
		for selected := range queue.diagnostics.timing[i].consumer.parkSelectDuration {
			queue.diagnostics.timing[i].consumer.parkSelectDuration[selected].record(
				time.Duration(i + selected),
			)
		}
	}
	snapshot := queue.Diagnostics()
	wantSize := SizeHistogram{Count: 3, Sum: 9, Min: 2, Max: 4}
	wantSize.Buckets[2], wantSize.Buckets[3], wantSize.Buckets[4] = 1, 1, 1
	assert.Equal(t, wantSize, snapshot.Producer.RequestedSize)
	assert.Equal(t, uint64(6), snapshot.Consumer.MovedSize.Sum)
	assert.Equal(t, uint64(18), snapshot.Consumer.GroupTarget.Sum)
	assert.Equal(t, uint64(3), snapshot.Consumer.GroupActualSize.Sum)
	assert.Equal(t, uint64(0), snapshot.Consumer.GroupActualSize.Min)
	assert.Equal(t, SizeHistogram{}, snapshot.Producer.GroupTarget)
	assert.Equal(t, SizeHistogram{}, snapshot.Producer.GroupActualSize)
	for source := range snapshot.Producer.ReusableSlots {
		producer := snapshot.Producer.ReusableSlots[source]
		consumer := snapshot.Consumer.AvailableItems[source]
		assert.Equal(t, uint64(6+3*source), producer.Sum)
		assert.Equal(t, uint64(9+3*source), consumer.Sum)
		assert.Equal(t, uint64(source+1), producer.Min)
		assert.Equal(t, uint64(source+4), consumer.Max)
	}
	for outcome, histogram := range snapshot.Producer.Timing.WaitDuration {
		assert.Equal(t, uint64(6+3*outcome), histogram.Sum)
		assert.Equal(t, uint64(outcome+1), histogram.Min)
		assert.Equal(t, uint64(outcome+3), histogram.Max)
	}
	for selected, histogram := range snapshot.Consumer.Timing.ParkSelectDuration {
		assert.Equal(t, uint64(6+3*selected), histogram.Sum)
	}
	original := snapshot
	snapshot.Producer.RequestedSize.Buckets[2] = 99
	snapshot.Consumer.AvailableItems[CleanupProbe].Sum = 999
	snapshot.Producer.Timing.WaitDuration[WaitProgress].Buckets[1] = 99
	snapshot.Config.Shards = 128
	assert.Equal(t, original, queue.Diagnostics())
	queue.diagnostics.counters[0].producer.requestedSize.record(1)
	assert.Equal(t, wantSize, original.Producer.RequestedSize)
}

func TestDiagnosticsHistogramConcurrentRecording(t *testing.T) {
	queue, err := New[int](64, WithDiagnostics(DiagnosticsConfig{
		Mode: DiagnosticsTiming, Shards: 1,
	}))
	require.NoError(t, err)
	var workers sync.WaitGroup
	for value := 0; value < 32; value++ {
		workers.Go(func() {
			for range 64 {
				queue.diagnostics.counters[0].producer.movedSize.record(uint64(value))
				queue.diagnostics.timing[0].producer.waitDuration[WaitProgress].record(
					time.Duration(value),
				)
			}
		})
	}
	for range 16 {
		queue.Diagnostics()
	}
	workers.Wait()
	snapshot := queue.Diagnostics()
	assert.Equal(t, uint64(2048), snapshot.Producer.MovedSize.Count)
	assert.Equal(t, uint64(31744), snapshot.Producer.MovedSize.Sum)
	assert.Equal(t, uint64(0), snapshot.Producer.MovedSize.Min)
	assert.Equal(t, uint64(31), snapshot.Producer.MovedSize.Max)
	for bucket := 0; bucket < 32; bucket++ {
		assert.Equal(t, uint64(64), snapshot.Producer.MovedSize.Buckets[bucket])
	}
	duration := snapshot.Producer.Timing.WaitDuration[WaitProgress]
	assert.Equal(t, uint64(2048), duration.Count)
	assert.Equal(t, uint64(31744), duration.Sum)
	assert.Equal(t, uint64(0), duration.Min)
	assert.Equal(t, uint64(31), duration.Max)
}

// Embedding an absent engine makes every unintended method call fail immediately.
type diagnosticsForbiddenEngine struct {
	engine[int]
	t *testing.T
}

func (e *diagnosticsForbiddenEngine) Capacity() int {
	e.t.Fatal("snapshot called engine Capacity")
	return 64
}

func TestDiagnosticsSnapshotWaitStateAndNoEngineCalls(t *testing.T) {
	for _, mode := range []DiagnosticsMode{
		DiagnosticsDisabled, DiagnosticsCounters, DiagnosticsTiming,
	} {
		queue, err := New[int](64, WithDiagnostics(DiagnosticsConfig{Mode: mode}))
		require.NoError(t, err)
		queue.engine = &diagnosticsForbiddenEngine{t: t}
		queue.producers.word.Store(uint64(3*onePoller + 7*oneParked + closedFlag))
		queue.consumers.word.Store(uint64(5*onePoller + 11*oneParked))
		snapshot := queue.Diagnostics()
		assert.Equal(t, uint32(1), snapshot.SchemaVersion)
		assert.Equal(t, uint32(64), snapshot.Config.Capacity)
		assert.Equal(t, EngineUnknown, snapshot.Config.Engine)
		assert.Equal(t, mode, snapshot.Config.Mode)
		want := DiagnosticsWaitState{}
		if mode != DiagnosticsDisabled {
			want.Producer = DiagnosticsSideWaitState{Pollers: 3, Parked: 7, Closed: true}
			want.Consumer = DiagnosticsSideWaitState{Pollers: 5, Parked: 11}
		}
		assert.Equal(t, want, snapshot.WaitState)
	}
}

func TestDiagnosticsSnapshotCounterReduction(t *testing.T) {
	queue, err := New[int](64, WithDiagnostics(DiagnosticsConfig{
		Mode: DiagnosticsTiming, Shards: 2,
	}))
	require.NoError(t, err)
	want := DiagnosticsSideSnapshot{}
	for shardIndex := range queue.diagnostics.counters {
		shard := &queue.diagnostics.counters[shardIndex]
		for _, counters := range []*diagnosticsSideCounters{
			&shard.producer.diagnosticsSideCounters,
			&shard.consumer.diagnosticsSideCounters,
		} {
			for i := range counters.attempts {
				counters.attempts[i].Store(uint64(1 + shardIndex + i))
			}
			for i := range counters.zeroProgress {
				counters.zeroProgress[i].Store(uint64(2 + shardIndex + i))
			}
			for i := range counters.partialProgress {
				counters.partialProgress[i].Store(uint64(3 + shardIndex + i))
			}
			for i := range counters.fullProgress {
				counters.fullProgress[i].Store(uint64(4 + shardIndex + i))
			}
			for i := range counters.requestedItems {
				counters.requestedItems[i].Store(uint64(5 + shardIndex + i))
			}
			for i := range counters.movedItems {
				counters.movedItems[i].Store(uint64(6 + shardIndex + i))
			}
			counters.waitStarts.Store(uint64(7 + shardIndex))
			for i := range counters.waitEnds {
				counters.waitEnds[i].Store(uint64(8 + shardIndex + i))
			}
			counters.waitEndsWithCloseRace.Store(uint64(9 + shardIndex))
			counters.spinEntries.Store(uint64(10 + shardIndex))
			counters.yieldEntries.Store(uint64(11 + shardIndex))
			counters.neverParkEntries.Store(uint64(12 + shardIndex))
			for i := range counters.yieldCalls {
				counters.yieldCalls[i].Store(uint64(13 + shardIndex + i))
			}
			counters.registrations.Store(uint64(14 + shardIndex))
			counters.parkEnlistments.Store(uint64(15 + shardIndex))
			counters.parkEnlistClosed.Store(uint64(16 + shardIndex))
			counters.parkSelectStarts.Store(uint64(17 + shardIndex))
			for i := range counters.parkSelectEnds {
				counters.parkSelectEnds[i].Store(uint64(18 + shardIndex + i))
			}
			for i := range counters.wakeCalls {
				counters.wakeCalls[i].Store(uint64(19 + shardIndex + i))
			}
			for i := range counters.wakeTransfers {
				counters.wakeTransfers[i].Store(uint64(20 + shardIndex + i))
			}
			counters.wakeRecipients.Store(uint64(21 + shardIndex))
			counters.tokensSent.Store(uint64(22 + shardIndex))
			counters.tokensReceivedForRetry.Store(uint64(23 + shardIndex))
			counters.tokensDrainedByCleanup.Store(uint64(24 + shardIndex))
			counters.wakeRetryAttempts.Store(uint64(25 + shardIndex))
			counters.wakeRetryZeroProgress.Store(uint64(26 + shardIndex))
			for i := range counters.wakeRetryEnds {
				counters.wakeRetryEnds[i].Store(uint64(27 + shardIndex + i))
			}
			counters.producerOldestReenlistments.Store(uint64(28 + shardIndex))
			counters.capacityEligibleAttempts.Store(uint64(29 + shardIndex))
			counters.capacitySelectedProbes.Store(uint64(30 + shardIndex))
			counters.capacityCompletedProbes.Store(uint64(31 + shardIndex))
			counters.attemptsWithoutRemainingCount.Store(uint64(32 + shardIndex))
		}
	}
	for i := range want.Attempts {
		want.Attempts[i] = uint64(3 + 2*i)
	}
	for i := range want.ZeroProgress {
		want.ZeroProgress[i] = uint64(5 + 2*i)
	}
	for i := range want.PartialProgress {
		want.PartialProgress[i] = uint64(7 + 2*i)
	}
	for i := range want.FullProgress {
		want.FullProgress[i] = uint64(9 + 2*i)
	}
	for i := range want.RequestedItems {
		want.RequestedItems[i] = uint64(11 + 2*i)
	}
	for i := range want.MovedItems {
		want.MovedItems[i] = uint64(13 + 2*i)
	}
	want.WaitStarts = 15
	for i := range want.WaitEnds {
		want.WaitEnds[i] = uint64(17 + 2*i)
	}
	want.WaitEndsWithCloseRace = 19
	want.SpinEntries = 21
	want.YieldEntries = 23
	want.NeverParkEntries = 25
	for i := range want.YieldCalls {
		want.YieldCalls[i] = uint64(27 + 2*i)
	}
	want.Registrations = 29
	want.ParkEnlistments = 31
	want.ParkEnlistClosed = 33
	want.ParkSelectStarts = 35
	for i := range want.ParkSelectEnds {
		want.ParkSelectEnds[i] = uint64(37 + 2*i)
	}
	for i := range want.WakeCalls {
		want.WakeCalls[i] = uint64(39 + 2*i)
	}
	for i := range want.WakeTransfers {
		want.WakeTransfers[i] = uint64(41 + 2*i)
	}
	want.WakeRecipients = 43
	want.TokensSent = 45
	want.TokensReceivedForRetry = 47
	want.TokensDrainedByCleanup = 49
	want.WakeRetryAttempts = 51
	want.WakeRetryZeroProgress = 53
	for i := range want.WakeRetryEnds {
		want.WakeRetryEnds[i] = uint64(55 + 2*i)
	}
	want.ProducerOldestReenlistments = 57
	want.CapacityEligibleAttempts = 59
	want.CapacitySelectedProbes = 61
	want.CapacityCompletedProbes = 63
	want.AttemptsWithoutRemainingCount = 65

	snapshot := queue.Diagnostics()
	assert.Equal(t, want, snapshot.Producer.DiagnosticsSideSnapshot)
	assert.Equal(t, want, snapshot.Consumer.DiagnosticsSideSnapshot)
	for i := range queue.diagnostics.timing {
		for _, timing := range []*diagnosticsSideTiming{
			&queue.diagnostics.timing[i].producer,
			&queue.diagnostics.timing[i].consumer,
		} {
			timing.waitStarts.Store(uint64(i + 1))
			timing.waitSamples.Store(uint64(i + 3))
			timing.parkSelectStarts.Store(uint64(i + 5))
			timing.parkSelectSamples.Store(uint64(i + 7))
		}
	}
	snapshot = queue.Diagnostics()
	want.Timing = DiagnosticsTimingSnapshot{
		WaitStarts: 3, WaitSamples: 7, ParkSelectStarts: 11, ParkSelectSamples: 15,
	}
	assert.Equal(t, want, snapshot.Producer.DiagnosticsSideSnapshot)
	assert.Equal(t, want, snapshot.Consumer.DiagnosticsSideSnapshot)
}

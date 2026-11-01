package diagnostics

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	diagnosticvalues "github.com/freshdresch/go-data-structures/workq/diagnostics"
)

func TestDiagnosticsHistogramSizeEdges(t *testing.T) {
	for bucket := 0; bucket < diagnosticvalues.SizeBucketCount; bucket++ {
		lower, upper := uint64(bucket), uint64(bucket)
		if bucket == 33 {
			lower, upper = 33, 64
		} else if bucket > 33 {
			upper = uint64(1) << (bucket - 27)
			lower = upper/2 + 1
		}
		for _, value := range []uint64{lower, upper} {
			var histogram AtomicSizeHistogram
			histogram.Min.Store(math.MaxUint64)
			histogram.Record(value)
			want := diagnosticvalues.SizeHistogram{Count: 1, Sum: value, Min: value, Max: value}
			want.Buckets[bucket] = 1
			got := diagnosticvalues.SizeHistogram{Min: math.MaxUint64}
			addSizeHistogram(&got, &histogram)
			assert.Equal(t, want, got, "size %d", value)
		}
	}
}

func TestDiagnosticsHistogramDurationEdges(t *testing.T) {
	for bucket := 0; bucket < diagnosticvalues.DurationBucketCount; bucket++ {
		var lower, upper uint64
		if bucket > 0 {
			lower = uint64(1) << (bucket - 1)
			upper = uint64(1)<<bucket - 1
		}
		for _, value := range []uint64{lower, upper} {
			var histogram AtomicDurationHistogram
			histogram.Min.Store(math.MaxUint64)
			histogram.Record(time.Duration(value))
			want := diagnosticvalues.DurationHistogram{Count: 1, Sum: value, Min: value, Max: value}
			want.Buckets[bucket] = 1
			got := diagnosticvalues.DurationHistogram{Min: math.MaxUint64}
			addDurationHistogram(&got, &histogram)
			assert.Equal(t, want, got, "duration %d", value)
		}
	}
}

func TestDiagnosticsHistogramCapacityAndEmpty(t *testing.T) {
	storage := New(diagnosticvalues.DiagnosticsConfig{
		Mode: diagnosticvalues.DiagnosticsTiming, Shards: 2,
	})
	empty := storage.Snapshot(nil)
	assert.Equal(t, diagnosticvalues.DiagnosticsProducerSnapshot{}, empty.Producer)
	assert.Equal(t, diagnosticvalues.DiagnosticsConsumerSnapshot{}, empty.Consumer)
	for source := range diagnosticvalues.CapacitySourceCount {
		histogram := &storage.Counters[1].Producer.Capacity[source]
		histogram.Record(0, 64)
		histogram.Record(64, 64)
		histogram.Record(uint64(source)+1, 64)
	}
	snapshot := storage.Snapshot(nil)
	for source, histogram := range snapshot.Producer.ReusableSlots {
		want := diagnosticvalues.CapacityHistogram{
			SizeHistogram: diagnosticvalues.SizeHistogram{Count: 3, Sum: 65 + uint64(source), Max: 64},
			ZeroCount:     1, AtCapacity: 1,
		}
		want.Buckets[0], want.Buckets[33], want.Buckets[source+1] = 1, 1, 1
		assert.Equal(t, want, histogram, "source %d", source)
	}
	assert.Equal(t, diagnosticvalues.DiagnosticsConsumerSnapshot{}, snapshot.Consumer)
}

func TestDiagnosticsHistogramWrap(t *testing.T) {
	storage := New(diagnosticvalues.DiagnosticsConfig{
		Mode: diagnosticvalues.DiagnosticsTiming, Shards: 2,
	})
	for i := range storage.Counters {
		histogram := &storage.Counters[i].Producer.RequestedSize
		histogram.Count.Store(math.MaxUint64)
		histogram.Sum.Store(math.MaxUint64)
		histogram.Buckets[3].Store(math.MaxUint64)
		histogram.Record(3)
		duration := &storage.Timing[i].Consumer.WaitDuration[diagnosticvalues.WaitProgress]
		duration.Count.Store(math.MaxUint64)
		duration.Sum.Store(math.MaxUint64)
		duration.Buckets[2].Store(math.MaxUint64)
		duration.Record(3)
	}
	snapshot := storage.Snapshot(nil)
	assert.Equal(
		t,
		diagnosticvalues.SizeHistogram{Sum: 4, Min: 3, Max: 3},
		snapshot.Producer.RequestedSize,
	)
	assert.Equal(
		t,
		diagnosticvalues.DurationHistogram{Sum: 4, Min: 3, Max: 3},
		snapshot.Consumer.Timing.WaitDuration[diagnosticvalues.WaitProgress],
	)
	// Reduction itself must wrap, independently of recording into a shard.
	for i := range storage.Counters {
		histogram := &storage.Counters[i].Consumer.Capacity[diagnosticvalues.DiagnosticProbe]
		histogram.Record(64, 64)
		histogram.Count.Store(uint64(1) << 63)
		histogram.Sum.Store(uint64(1) << 63)
		histogram.Buckets[33].Store(uint64(1) << 63)
		histogram.AtCapacity.Store(uint64(1) << 63)
	}
	assert.Equal(
		t,
		diagnosticvalues.CapacityHistogram{
			SizeHistogram: diagnosticvalues.SizeHistogram{Min: 64, Max: 64},
		},
		storage.Snapshot(nil).Consumer.AvailableItems[diagnosticvalues.DiagnosticProbe],
	)
}

func TestDiagnosticsSnapshotShardsAndDetachment(t *testing.T) {
	storage := New(diagnosticvalues.DiagnosticsConfig{
		Mode: diagnosticvalues.DiagnosticsTiming, Shards: 4,
	})
	for i := 1; i < len(storage.Counters); i++ {
		shard := &storage.Counters[i]
		shard.Producer.RequestedSize.Record(uint64(i + 1))
		shard.Consumer.MovedSize.Record(uint64(i))
		shard.Consumer.GroupTarget.Record(uint64(i + 4))
		shard.Consumer.GroupActualSize.Record(uint64(i - 1))
		for source := range shard.Producer.Capacity {
			shard.Producer.Capacity[source].Record(uint64(i+source), 64)
			shard.Consumer.Capacity[source].Record(uint64(i+source+1), 64)
		}
		for outcome := range storage.Timing[i].Producer.WaitDuration {
			storage.Timing[i].Producer.WaitDuration[outcome].Record(
				time.Duration(i + outcome),
			)
		}
		for selected := range storage.Timing[i].Consumer.ParkSelectDuration {
			storage.Timing[i].Consumer.ParkSelectDuration[selected].Record(
				time.Duration(i + selected),
			)
		}
	}
	snapshot := storage.Snapshot(nil)
	wantSize := diagnosticvalues.SizeHistogram{Count: 3, Sum: 9, Min: 2, Max: 4}
	wantSize.Buckets[2], wantSize.Buckets[3], wantSize.Buckets[4] = 1, 1, 1
	assert.Equal(t, wantSize, snapshot.Producer.RequestedSize)
	assert.Equal(t, uint64(6), snapshot.Consumer.MovedSize.Sum)
	assert.Equal(t, uint64(18), snapshot.Consumer.GroupTarget.Sum)
	assert.Equal(t, uint64(3), snapshot.Consumer.GroupActualSize.Sum)
	assert.Equal(t, uint64(0), snapshot.Consumer.GroupActualSize.Min)
	assert.Equal(t, diagnosticvalues.SizeHistogram{}, snapshot.Producer.GroupTarget)
	assert.Equal(t, diagnosticvalues.SizeHistogram{}, snapshot.Producer.GroupActualSize)
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
	snapshot.Consumer.AvailableItems[diagnosticvalues.CleanupProbe].Sum = 999
	snapshot.Producer.Timing.WaitDuration[diagnosticvalues.WaitProgress].Buckets[1] = 99
	snapshot.Config.Shards = 128
	assert.Equal(t, original, storage.Snapshot(nil))
	storage.Counters[0].Producer.RequestedSize.Record(1)
	assert.Equal(t, wantSize, original.Producer.RequestedSize)
}

func TestDiagnosticsHistogramConcurrentRecording(t *testing.T) {
	storage := New(diagnosticvalues.DiagnosticsConfig{
		Mode: diagnosticvalues.DiagnosticsTiming, Shards: 1,
	})
	var workers sync.WaitGroup
	for value := 0; value < 32; value++ {
		workers.Go(func() {
			for range 64 {
				storage.Counters[0].Producer.MovedSize.Record(uint64(value))
				storage.Timing[0].Producer.WaitDuration[diagnosticvalues.WaitProgress].Record(
					time.Duration(value),
				)
			}
		})
	}
	for range 16 {
		storage.Snapshot(nil)
	}
	workers.Wait()
	snapshot := storage.Snapshot(nil)
	assert.Equal(t, uint64(2048), snapshot.Producer.MovedSize.Count)
	assert.Equal(t, uint64(31744), snapshot.Producer.MovedSize.Sum)
	assert.Equal(t, uint64(0), snapshot.Producer.MovedSize.Min)
	assert.Equal(t, uint64(31), snapshot.Producer.MovedSize.Max)
	for bucket := 0; bucket < 32; bucket++ {
		assert.Equal(t, uint64(64), snapshot.Producer.MovedSize.Buckets[bucket])
	}
	duration := snapshot.Producer.Timing.WaitDuration[diagnosticvalues.WaitProgress]
	assert.Equal(t, uint64(2048), duration.Count)
	assert.Equal(t, uint64(31744), duration.Sum)
	assert.Equal(t, uint64(0), duration.Min)
	assert.Equal(t, uint64(31), duration.Max)
}

func TestDiagnosticsSnapshotCounterReduction(t *testing.T) {
	storage := New(diagnosticvalues.DiagnosticsConfig{
		Mode: diagnosticvalues.DiagnosticsTiming, Shards: 2,
	})
	want := diagnosticvalues.DiagnosticsSideSnapshot{}
	for shardIndex := range storage.Counters {
		shard := &storage.Counters[shardIndex]
		for _, counters := range []*SideCounters{
			&shard.Producer.SideCounters,
			&shard.Consumer.SideCounters,
		} {
			for i := range counters.Attempts {
				counters.Attempts[i].Store(uint64(1 + shardIndex + i))
			}
			for i := range counters.ZeroProgress {
				counters.ZeroProgress[i].Store(uint64(2 + shardIndex + i))
			}
			for i := range counters.PartialProgress {
				counters.PartialProgress[i].Store(uint64(3 + shardIndex + i))
			}
			for i := range counters.FullProgress {
				counters.FullProgress[i].Store(uint64(4 + shardIndex + i))
			}
			for i := range counters.RequestedItems {
				counters.RequestedItems[i].Store(uint64(5 + shardIndex + i))
			}
			for i := range counters.MovedItems {
				counters.MovedItems[i].Store(uint64(6 + shardIndex + i))
			}
			counters.WaitStarts.Store(uint64(7 + shardIndex))
			for i := range counters.WaitEnds {
				counters.WaitEnds[i].Store(uint64(8 + shardIndex + i))
			}
			counters.WaitEndsWithCloseRace.Store(uint64(9 + shardIndex))
			counters.SpinEntries.Store(uint64(10 + shardIndex))
			counters.YieldEntries.Store(uint64(11 + shardIndex))
			counters.NeverParkEntries.Store(uint64(12 + shardIndex))
			for i := range counters.YieldCalls {
				counters.YieldCalls[i].Store(uint64(13 + shardIndex + i))
			}
			counters.Registrations.Store(uint64(14 + shardIndex))
			counters.ParkEnlistments.Store(uint64(15 + shardIndex))
			counters.ParkEnlistClosed.Store(uint64(16 + shardIndex))
			counters.ParkSelectStarts.Store(uint64(17 + shardIndex))
			for i := range counters.ParkSelectEnds {
				counters.ParkSelectEnds[i].Store(uint64(18 + shardIndex + i))
			}
			for i := range counters.WakeCalls {
				counters.WakeCalls[i].Store(uint64(19 + shardIndex + i))
			}
			for i := range counters.WakeTransfers {
				counters.WakeTransfers[i].Store(uint64(20 + shardIndex + i))
			}
			counters.WakeRecipients.Store(uint64(21 + shardIndex))
			counters.TokensSent.Store(uint64(22 + shardIndex))
			counters.TokensReceivedForRetry.Store(uint64(23 + shardIndex))
			counters.TokensDrainedByCleanup.Store(uint64(24 + shardIndex))
			counters.WakeRetryAttempts.Store(uint64(25 + shardIndex))
			counters.WakeRetryZeroProgress.Store(uint64(26 + shardIndex))
			for i := range counters.WakeRetryEnds {
				counters.WakeRetryEnds[i].Store(uint64(27 + shardIndex + i))
			}
			counters.ProducerOldestReenlistments.Store(uint64(28 + shardIndex))
			counters.CapacityEligibleAttempts.Store(uint64(29 + shardIndex))
			counters.CapacitySelectedProbes.Store(uint64(30 + shardIndex))
			counters.CapacityCompletedProbes.Store(uint64(31 + shardIndex))
			counters.AttemptsWithoutRemainingCount.Store(uint64(32 + shardIndex))
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

	snapshot := storage.Snapshot(nil)
	assert.Equal(t, want, snapshot.Producer.DiagnosticsSideSnapshot)
	assert.Equal(t, want, snapshot.Consumer.DiagnosticsSideSnapshot)
	for i := range storage.Timing {
		for _, timing := range []*SideTiming{
			&storage.Timing[i].Producer,
			&storage.Timing[i].Consumer,
		} {
			timing.WaitStarts.Store(uint64(i + 1))
			timing.WaitSamples.Store(uint64(i + 3))
			timing.ParkSelectStarts.Store(uint64(i + 5))
			timing.ParkSelectSamples.Store(uint64(i + 7))
		}
	}
	snapshot = storage.Snapshot(nil)
	want.Timing = diagnosticvalues.DiagnosticsTimingSnapshot{
		WaitStarts: 3, WaitSamples: 7, ParkSelectStarts: 11, ParkSelectSamples: 15,
	}
	assert.Equal(t, want, snapshot.Producer.DiagnosticsSideSnapshot)
	assert.Equal(t, want, snapshot.Consumer.DiagnosticsSideSnapshot)
}

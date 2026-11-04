package benchmark

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
	"github.com/freshdresch/go-data-structures/ring/frontier"

	"github.com/freshdresch/go-workq/workq"
)

// workqTestModes are every mode the workq benchmarks run in.
var workqTestModes = []benchMode{
	spscMode,
	mpscMode,
	spmcMode,
	mpmcMode,
	wideConsumersMode,
	wideProducersMode,
}

// allWorkqRows returns the comparison rows followed by the tuning rows.
func allWorkqRows() []workRowSpec {
	return slices.Concat(workqCompareRows, workqTuningRows)
}

// workLoadNamed returns the load called name, failing the test if there is none.
func workLoadNamed(t *testing.T, name string) workLoad {
	t.Helper()
	index := slices.IndexFunc(workLoads, func(load workLoad) bool { return load.name == name })
	require.NotEqual(t, -1, index, "no load named %s", name)
	return workLoads[index]
}

// workRowNamed returns the row called name in rows, failing the test if there is none.
func workRowNamed(t *testing.T, rows []workRowSpec, name string) workRowSpec {
	t.Helper()
	index := slices.IndexFunc(rows, func(row workRowSpec) bool { return row.name == name })
	require.NotEqual(t, -1, index, "no row named %s", name)
	return rows[index]
}

// buildRow builds row for mode at batch, failing the test if it cannot be built.
func buildRow(t *testing.T, row workRowSpec, mode benchMode, batch int) builtRow {
	t.Helper()
	built, err := row.factory(mode, batch)
	require.NoError(t, err, "build %s in %s at batch %d", row.name, mode.name, batch)
	return built
}

// runLoadOnce runs built once with run's producer and with handlers, through runWorkq with
// noopTimer. It returns what the run measured, the run's wall time, and the run's error. The
// caller builds and releases the row.
func runLoadOnce(
	t *testing.T,
	built builtRow,
	mode benchMode,
	batch int,
	run *loadRun,
	handlers func(consumer int) func(batch []int64),
) (workResult, time.Duration, error) {
	t.Helper()
	start := time.Now()
	result, err := runWorkq(
		noopTimer{},
		built.state,
		built.row,
		mode,
		batch,
		run.total,
		run.produce,
		handlers,
		&run.base,
	)
	elapsed := time.Since(start)
	return result, elapsed, err
}

// TestWorkqRowsAccounting checks that every row moves each item exactly once, in order per
// producer within each consumer, in every mode and at every batch it runs at.
func TestWorkqRowsAccounting(t *testing.T) {
	const total = 10_007
	for _, row := range allWorkqRows() {
		for _, mode := range rowModes(row, workLoads) {
			for _, batch := range row.batches {
				name := fmt.Sprintf("impl=%s/mode=%s/batch=%d", row.name, mode.name, batch)
				t.Run(name, func(t *testing.T) {
					checkWorkqAccounting(t, row, mode, batch, total)
				})
			}
		}
	}
}

// checkWorkqAccounting runs total items through row with a recordSink per consumer, and checks
// what they recorded.
func checkWorkqAccounting(t *testing.T, row workRowSpec, mode benchMode, batch, total int) {
	t.Helper()
	sinks := make([]*recordSink[int64], mode.consumers)
	for c := range sinks {
		sinks[c] = &recordSink[int64]{}
	}
	handlers := func(consumer int) func(batch []int64) {
		return sinks[consumer].record
	}
	run := newSaturatedRun(total, mode, 0, 0)
	built := buildRow(t, row, mode, batch)
	defer built.release()
	stuck := fmt.Sprintf("impl=%s mode=%s batch=%d total=%d", row.name, mode.name, batch, total)
	result := runWithin(t, stuck, func() (workResult, error) {
		result, _, err := runLoadOnce(t, built, mode, batch, run, handlers)
		return result, err
	})
	require.Equal(t, total, result.handled)
	require.NoError(t, accountingError(sinks, mode.producers, total))
	require.GreaterOrEqual(t, result.largestShare, 1/float64(mode.consumers))
	require.LessOrEqual(t, result.largestShare, 1.0)
}

// TestWorkqRowsTotalOne checks that every row stops after one item in MPMC, where three of the
// four producers send nothing.
func TestWorkqRowsTotalOne(t *testing.T) {
	for _, row := range allWorkqRows() {
		for _, batch := range row.batches {
			t.Run(fmt.Sprintf("impl=%s/batch=%d", row.name, batch), func(t *testing.T) {
				run := newSaturatedRun(1, mpmcMode, 0, 0)
				built := buildRow(t, row, mpmcMode, batch)
				defer built.release()
				stuck := fmt.Sprintf("impl=%s batch=%d total=1", row.name, batch)
				result := runWithin(t, stuck, func() (workResult, error) {
					result, _, err := runLoadOnce(t, built, mpmcMode, batch, run, run.handlers)
					return result, err
				})
				require.Equal(t, 1, result.handled)
			})
		}
	}
}

// TestWorkqRowAbort checks that an abort in a handler ends the run with that error and frees
// every goroutine. For the channel rows, it checks that the consumers keep draining after the
// abort and that the unconditional stop frees them.
//
// Each handler runs 50µs of work per item, which keeps the queue full, so producers are blocked
// when the abort lands. Without the work, a channel consumer that wrongly returned on abort hung
// the run in 0 of 20 scratch runs under -race, and with it in 20 of 20.
func TestWorkqRowAbort(t *testing.T) {
	const abortAt = 100
	for _, row := range workqCompareRows {
		t.Run("impl="+row.name, func(t *testing.T) {
			built := buildRow(t, row, mpmcMode, 32)
			var handled atomic.Int64
			handler := func(batch []int64) {
				count := handled.Add(int64(len(batch)))
				old := count - int64(len(batch))
				if old < abortAt && abortAt <= count {
					built.state.fail(errStubAbort)
				}
				for range batch {
					spinWork(50 * time.Microsecond)
				}
			}
			handlers := func(_ int) func(batch []int64) {
				return handler
			}
			run := newSaturatedRun(abortTotal, mpmcMode, 0, 0)
			before := runtime.NumGoroutine()
			runErr := runWithin(
				t,
				row.name+" abort did not free every goroutine",
				func() (error, error) {
					_, err := runWorkq(
						noopTimer{},
						built.state,
						built.row,
						mpmcMode,
						32,
						run.total,
						run.produce,
						handlers,
						&run.base,
					)
					return err, nil
				},
			)
			built.release()
			checkRunError(t, runErr, "mode=MPMC: "+errStubAbort.Error())
			waitForGoroutines(t, before)
		})
	}
}

func TestReportWorkqError(t *testing.T) {
	otherErr := errors.New("other failure")
	tests := []struct {
		name    string
		abort   bool
		err     error
		wantErr error
	}{
		{name: "canceled after abort", abort: true, err: context.Canceled, wantErr: errStubAbort},
		{name: "canceled without abort", err: context.Canceled, wantErr: context.Canceled},
		{name: "other error", err: otherErr, wantErr: otherErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := newRunState(nil)
			if test.abort {
				state.fail(errStubAbort)
			}
			reportWorkqError(state, test.err)
			assert.Equal(t, test.wantErr, state.firstError())
			assert.Equal(t, test.wantErr != nil, state.aborted())
		})
	}
}

func TestWorkqChanBatchSizing(t *testing.T) {
	row := workRowNamed(t, workqCompareRows, "chan-batch")
	for _, test := range []struct {
		batch     int
		wantSlots int
	}{
		{batch: 32, wantSlots: 32},
		{batch: 128, wantSlots: 8},
	} {
		t.Run(fmt.Sprintf("batch=%d", test.batch), func(t *testing.T) {
			built := buildRow(t, row, mpmcMode, test.batch)
			defer built.release()
			target, ok := built.row.(*chanBatchRow)
			require.True(t, ok, "chan-batch row is %T", built.row)
			assert.Equal(t, test.wantSlots, cap(target.batches))
			wantBuffers := test.wantSlots + mpmcMode.producers + mpmcMode.consumers
			require.Equal(t, wantBuffers, len(target.free))
			for range wantBuffers {
				buffer := <-target.free
				assert.Equal(t, test.batch, cap(buffer))
			}
		})
	}
}

// pushGaps returns the gaps between consecutive push times of a paced run at pace, where a
// push time is the item's due time plus its lateness.
func pushGaps(pace time.Duration, lateness []time.Duration) []time.Duration {
	var gaps []time.Duration
	for k := 1; k < len(lateness); k++ {
		previous := pacedLead + time.Duration(k)*pace + lateness[k-1]
		current := pacedLead + time.Duration(k+1)*pace + lateness[k]
		gaps = append(gaps, current-previous)
	}
	return gaps
}

// medianDuration returns the median of durations, which must not be empty.
func medianDuration(durations []time.Duration) time.Duration {
	sorted := slices.Clone(durations)
	slices.Sort(sorted)
	return percentile(sorted, 50)
}

// pacedLatencies returns every latency the run's consumers recorded.
func pacedLatencies(run *loadRun) []time.Duration {
	var latencies []time.Duration
	for _, slot := range run.latencies {
		latencies = append(latencies, slot.values[:slot.next]...)
	}
	return latencies
}

// TestWorkqPacedLatency checks one paced run at pace=100us. The p50 bounds catch a consumer
// trapped in the producer's runnext slot, which measured about 89µs for both rows, and a
// latency that leaves out the item's push time, which is at least 1ms. Correct runs measured
// about 0.6µs for chan and about 1.2µs for workq. The workq bound is looser because its default
// wait strategy parks. At work=50us, a handler that recorded each latency after the item's work
// would put p50 at 50µs or more. The test assumes an idle machine.
func TestWorkqPacedLatency(t *testing.T) {
	const (
		items = 200
		pace  = 100 * time.Microsecond
	)
	tests := []struct {
		row    string
		work   time.Duration
		maxP50 time.Duration
	}{
		{row: "chan", work: 0, maxP50: 20 * time.Microsecond},
		{row: "workq", work: 0, maxP50: 50 * time.Microsecond},
		{row: "workq", work: 50 * time.Microsecond, maxP50: 50 * time.Microsecond},
	}
	load := workLoadNamed(t, loadPaced)
	for _, test := range tests {
		name := fmt.Sprintf("impl=%s/work=%s", test.row, durationLabel(test.work))
		t.Run(name, func(t *testing.T) {
			row := workRowNamed(t, workqCompareRows, test.row)
			run := load.newRun(items, spscMode, pace, test.work)
			built := buildRow(t, row, spscMode, 1)
			defer built.release()
			result, elapsed, err := runLoadOnce(t, built, spscMode, 1, run, run.handlers)
			require.NoError(t, err)
			require.Equal(t, items, result.handled)

			assert.GreaterOrEqual(t, elapsed, pacedLead+items*pace)
			latencies := pacedLatencies(run)
			require.Len(t, latencies, items)
			for _, latency := range latencies {
				assert.GreaterOrEqual(t, latency, time.Duration(0))
				assert.Less(t, latency, elapsed)
			}
			assert.GreaterOrEqual(
				t,
				medianDuration(pushGaps(pace, run.lateness)),
				50*time.Microsecond,
			)
			if raceEnabled {
				return
			}
			p50 := medianDuration(latencies)
			assert.Less(t, p50, test.maxP50, "p50 latency")
		})
	}
}

func TestProcessCPU(t *testing.T) {
	before, ok := processCPU()
	if !ok {
		t.Skip("processCPU is not available on this platform")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		spinWork(50 * time.Millisecond)
	}()
	<-done
	after, ok := processCPU()
	require.True(t, ok)
	assert.GreaterOrEqual(t, after-before, 30*time.Millisecond, "CPU time of a busy loop")

	before, ok = processCPU()
	require.True(t, ok)
	time.Sleep(50 * time.Millisecond)
	after, ok = processCPU()
	require.True(t, ok)
	assert.Less(t, after-before, 20*time.Millisecond, "CPU time of a sleep")
}

func TestPercentile(t *testing.T) {
	tests := []struct {
		length    int
		p         int
		wantIndex int
	}{
		{length: 1, p: 0, wantIndex: 0},
		{length: 1, p: 50, wantIndex: 0},
		{length: 1, p: 99, wantIndex: 0},
		{length: 1, p: 100, wantIndex: 0},
		{length: 2, p: 0, wantIndex: 0},
		{length: 2, p: 50, wantIndex: 0},
		{length: 2, p: 99, wantIndex: 0},
		{length: 2, p: 100, wantIndex: 1},
		{length: 100, p: 0, wantIndex: 0},
		{length: 100, p: 50, wantIndex: 49},
		{length: 100, p: 99, wantIndex: 98},
		{length: 100, p: 100, wantIndex: 99},
		{length: 101, p: 0, wantIndex: 0},
		{length: 101, p: 50, wantIndex: 50},
		{length: 101, p: 99, wantIndex: 99},
		{length: 101, p: 100, wantIndex: 100},
	}
	for _, test := range tests {
		sorted := make([]time.Duration, test.length)
		for i := range sorted {
			sorted[i] = time.Duration(i) * time.Microsecond
		}
		assert.Equal(
			t,
			time.Duration(test.wantIndex)*time.Microsecond,
			percentile(sorted, test.p),
			"length %d p %d",
			test.length,
			test.p,
		)
	}
}

func TestLargestShare(t *testing.T) {
	tests := []struct {
		counts []int
		want   float64
	}{
		{counts: []int{}, want: 0},
		{counts: []int{0, 0}, want: 0},
		{counts: []int{5}, want: 1},
		{counts: []int{1, 1, 1, 1}, want: 0.25},
		{counts: []int{1, 3}, want: 0.75},
	}
	for _, test := range tests {
		counts := make([]paddedCount, len(test.counts))
		for i, value := range test.counts {
			counts[i].value = value
		}
		assert.Equal(t, test.want, largestShare(counts), "counts %v", test.counts)
	}
}

func TestBurstDelays(t *testing.T) {
	tests := []struct {
		name             string
		starts           []time.Duration
		firstItems       [][]time.Duration
		wantFirstDelays  []time.Duration
		wantFourthDelays []time.Duration
		wantMissed       int
	}{
		{
			name:   "every consumer present",
			starts: []time.Duration{100, 200},
			firstItems: [][]time.Duration{
				{110, 210},
				{120, 230},
				{130, 220},
				{140, 240},
			},
			wantFirstDelays:  []time.Duration{10, 10},
			wantFourthDelays: []time.Duration{40, 40},
		},
		{
			name:   "one consumer missing in one burst",
			starts: []time.Duration{100, 200},
			firstItems: [][]time.Duration{
				{110, 205},
				{120, -1},
				{130, 220},
				{140, 240},
			},
			wantFirstDelays:  []time.Duration{10, 5},
			wantFourthDelays: []time.Duration{40},
			wantMissed:       1,
		},
		{
			name:   "consumer order differs per burst",
			starts: []time.Duration{100, 200, 300},
			firstItems: [][]time.Duration{
				{140, 201, 330},
				{110, 260, 302},
				{130, 220, 350},
				{125, 240, 310},
			},
			wantFirstDelays:  []time.Duration{10, 1, 2},
			wantFourthDelays: []time.Duration{40, 60, 50},
		},
		{
			name:   "every burst missed",
			starts: []time.Duration{100, 200},
			firstItems: [][]time.Duration{
				{110, -1},
				{-1, 230},
				{130, 220},
				{-1, -1},
			},
			wantFirstDelays: []time.Duration{10, 20},
			wantMissed:      2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			firstDelays, fourthDelays, missed := burstDelays(test.starts, test.firstItems)
			assert.Equal(t, test.wantFirstDelays, firstDelays)
			assert.Equal(t, test.wantFourthDelays, fourthDelays)
			assert.Equal(t, test.wantMissed, missed)
		})
	}

	t.Run("metrics when every burst missed", func(t *testing.T) {
		metrics := burstMetrics(
			[]time.Duration{100, 200},
			[][]time.Duration{{110, -1}, {-1, 230}},
		)
		assert.Equal(t, 1.0, metrics["missed-share"])
		assert.NotContains(t, metrics, "fourth-start-ns")
		assert.NotContains(t, metrics, "fourth-start-max-ns")
	})
}

// TestWorkqBurst checks ten bursts in SPMC at work=1us. The check that at least one burst was
// counted depends on timing, because a burst counts only when all four consumers handle items
// in it. With ten bursts at work=1us a failure is unlikely, and the scratch measurement during
// plan review missed no bursts at work=1us.
//
// It does not check an upper bound on first-item times, because a consumer descheduled between
// taking an item and reading the clock can record a time after the next burst starts.
//
// The pause check measures from the end of the previous burst's pushes, not from its start,
// because a burst's own handling time would otherwise hide a missing pause.
func TestWorkqBurst(t *testing.T) {
	const bursts = 10
	load := workLoadNamed(t, loadBurst)
	for _, name := range []string{"chan", "workq"} {
		t.Run("impl="+name, func(t *testing.T) {
			row := workRowNamed(t, workqCompareRows, name)
			run := load.newRun(bursts, spmcMode, 0, time.Microsecond)
			built := buildRow(t, row, spmcMode, 32)
			defer built.release()
			result := runWithin(t, name+" burst", func() (workResult, error) {
				result, _, err := runLoadOnce(t, built, spmcMode, 32, run, run.handlers)
				return result, err
			})
			require.Equal(t, bursts*burstItems, result.handled)
			for i := 1; i < len(run.burstStarts); i++ {
				gap := run.burstStarts[i] - run.burstEnds[i-1]
				require.GreaterOrEqual(t, gap, burstPause, "pause before burst %d", i)
			}

			for c, times := range run.firstItems {
				for i, at := range times {
					if at == -1 {
						continue
					}
					assert.GreaterOrEqual(t, at, run.burstStarts[i], "consumer %d burst %d", c, i)
				}
			}
			firstDelays, fourthDelays, _ := burstDelays(run.burstStarts, run.firstItems)
			for _, delay := range slices.Concat(firstDelays, fourthDelays) {
				assert.GreaterOrEqual(t, delay, time.Duration(0))
			}
			assert.NotEmpty(t, fourthDelays, "no burst reached all four consumers")
		})
	}
}

func TestWorkqHandlerWork(t *testing.T) {
	const work = time.Microsecond
	run := workLoadNamed(t, loadSaturated).newRun(1, spscMode, 0, work)
	handler := run.handlers(0)
	batch := make([]int64, 32)
	for range 100 {
		start := time.Now()
		handler(batch)
		assert.GreaterOrEqual(t, time.Since(start), 32*work)
	}
}

// TestWorkqRowsAllocations checks that no comparison row allocates per item or per batch. A run
// allocates a fixed amount for goroutines and each Consume call's buffer and waiter. The bound
// scales with the batch, because a larger batch makes fewer pushes: at batch 128 it is half the
// push count, so one allocation per push exceeds it, while the fixed amount stays below it.
func TestWorkqRowsAllocations(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops items at random under the race detector, so a producer park " +
			"can allocate a new waiter")
	}
	const items = 100_000
	for _, row := range workqCompareRows {
		for _, mode := range []benchMode{spscMode, mpmcMode} {
			for _, batch := range row.batches {
				name := fmt.Sprintf("impl=%s/mode=%s/batch=%d", row.name, mode.name, batch)
				t.Run(name, func(t *testing.T) {
					maxAllocs := min(1_000, items/batch/2)
					run := newSaturatedRun(items, mode, 0, 0)
					built := buildRow(t, row, mode, batch)
					defer built.release()
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					result, err := runWorkq(
						noopTimer{},
						built.state,
						built.row,
						mode,
						batch,
						run.total,
						run.produce,
						run.handlers,
						&run.base,
					)
					runtime.ReadMemStats(&after)
					require.NoError(t, err)
					require.Equal(t, items, result.handled)
					assert.Less(t, after.Mallocs-before.Mallocs, uint64(maxAllocs))
				})
			}
		}
	}
}

// TestWorkqRowAbortWakes checks that an abort frees workq goroutines waiting in PushBatch and
// Consume. Each subtest builds its own row: after the first, the run has aborted and the queue
// is full, so a shared row would check nothing in the second.
func TestWorkqRowAbortWakes(t *testing.T) {
	row := workRowNamed(t, workqCompareRows, "workq")
	tests := []struct {
		name string
		call func(target workRow)
	}{
		{
			name: "push",
			call: func(target workRow) {
				target.push(make([]int64, ringCapacity+1))
			},
		},
		{
			name: "consume",
			call: func(target workRow) {
				target.consume(0, func(_ []int64) {})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				built := buildRow(t, row, mpmcMode, 32)
				defer built.release()
				returned := make(chan struct{})
				go func() {
					defer close(returned)
					test.call(built.row)
				}()
				// No other worker can make progress, so this wait reaches the parked call.
				synctest.Wait()
				select {
				case <-returned:
					t.Fatal("call returned before the abort")
				default:
				}
				built.state.fail(errStubAbort)
				<-returned
				require.Equal(t, errStubAbort, built.state.firstError())
			})
		})
	}
}

func TestWorkqPushWaitMax(t *testing.T) {
	t.Run("run", func(t *testing.T) {
		row := workRowNamed(t, workqCompareRows, "workq")
		run := workLoadNamed(t, loadPushWait).newRun(2_000, mpscMode, 0, 50*time.Microsecond)
		built := buildRow(t, row, mpscMode, 32)
		defer built.release()
		result := runWithin(t, "push-wait run", func() (workResult, error) {
			result, _, err := runLoadOnce(t, built, mpscMode, 32, run, run.handlers)
			return result, err
		})
		require.Equal(t, 2_000, result.handled)
		require.Len(t, run.pushMax, mpscMode.producers)
		var longest time.Duration
		for p, slot := range run.pushMax {
			require.Greater(t, slot.value, time.Duration(0), "producer %d", p)
			longest = max(longest, slot.value)
		}
		require.Equal(t, longest, pushWaitMax(run.pushMax))
		require.GreaterOrEqual(t, pushWaitMax(run.pushMax), 50*time.Microsecond)
	})

	tests := []struct {
		name   string
		values []time.Duration
	}{
		{name: "largest first", values: []time.Duration{30, 10, 20}},
		{name: "largest in the middle", values: []time.Duration{10, 30, 20}},
		{name: "largest last", values: []time.Duration{10, 20, 30}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			slots := make([]paddedDuration, len(test.values))
			for i, value := range test.values {
				slots[i].value = value
			}
			assert.Equal(t, time.Duration(30), pushWaitMax(slots))
		})
	}
}

// TestWorkqPacedSleep checks one paced run at pace=2ms, where the producer sleeps. A producer
// that yields instead of sleeping uses about 1.3 CPUs, so the CPU check catches the wrong wait.
// The test assumes an idle machine.
func TestWorkqPacedSleep(t *testing.T) {
	const (
		items = 20
		pace  = 2 * time.Millisecond
	)
	row := workRowNamed(t, workqCompareRows, "workq")
	run := workLoadNamed(t, loadPaced).newRun(items, spscMode, pace, 0)
	built := buildRow(t, row, spscMode, 1)
	defer built.release()
	result, elapsed, err := runLoadOnce(t, built, spscMode, 1, run, run.handlers)
	require.NoError(t, err)
	require.Equal(t, items, result.handled)

	assert.GreaterOrEqual(t, elapsed, pacedLead+items*pace)
	latencies := pacedLatencies(run)
	require.Len(t, latencies, items)
	for _, latency := range latencies {
		assert.GreaterOrEqual(t, latency, time.Duration(0))
	}
	assert.GreaterOrEqual(t, medianDuration(pushGaps(pace, run.lateness)), time.Millisecond)
	if result.cpuOK {
		assert.Less(t, result.cpu, elapsed/2, "CPU time of a sleeping paced run")
	}
}

// TestWorkqTuningRows checks that the tuning rows apply their options. It reads the queue's
// unexported fields on purpose, to check the options end to end.
func TestWorkqTuningRows(t *testing.T) {
	park := &workq.WaitStrategy{}
	wantTunings := map[string]workqTuning{
		"default":    {},
		"recv16":     {receiveBatch: 16},
		"recv64":     {receiveBatch: 64},
		"recv128":    {receiveBatch: 128},
		"push16":     {pushBatch: 16},
		"push64":     {pushBatch: 64},
		"push128":    {pushBatch: 128},
		"cwait-park": {consumerWait: park},
		"cwait-long": {consumerWait: &workq.WaitStrategy{Spins: 256, Yields: 64}},
		"cwait-never": {
			consumerWait: &workq.WaitStrategy{Spins: 64, Yields: 16, NeverPark: true},
		},
		"pwait-park": {producerWait: park},
		"pwait-long": {producerWait: &workq.WaitStrategy{Spins: 64, Yields: 16}},
		"pwait-never": {
			producerWait: &workq.WaitStrategy{Spins: 16, Yields: 4, NeverPark: true},
		},
	}

	t.Run("table", func(t *testing.T) {
		require.Len(t, workqTuningRows, len(wantTunings))
		for _, row := range workqTuningRows {
			want, ok := wantTunings[row.name]
			require.True(t, ok, "unexpected tuning row %s", row.name)
			assert.Equal(t, want, row.tuning, row.name)
		}
	})

	t.Run("queue fields", func(t *testing.T) {
		for _, row := range workqTuningRows {
			t.Run(row.name, func(t *testing.T) {
				checkTunedQueue(t, row)
			})
		}
	})

	t.Run("receive limit", func(t *testing.T) {
		longest := longestReceivedBatch(t, "recv128")
		assert.Greater(t, longest, 32)
		assert.LessOrEqual(t, longest, 128)
		assert.LessOrEqual(t, longestReceivedBatch(t, "recv16"), 16)
	})
}

// checkTunedQueue builds row's workq queue in MPMC and checks its limits and wait strategies
// against the row's tuning, or the workq defaults where the row sets none.
func checkTunedQueue(t *testing.T, row workRowSpec) {
	t.Helper()
	built := buildRow(t, row, mpmcMode, 32)
	defer built.release()
	target, ok := built.row.(*workqRow)
	require.True(t, ok, "%s row is %T", row.name, built.row)
	queue := reflect.ValueOf(target.queue).Elem()

	wantReceive := cmp.Or(row.tuning.receiveBatch, workqDefaultBatch)
	wantPush := cmp.Or(row.tuning.pushBatch, workqDefaultBatch)
	assert.Equal(t, int64(wantReceive), queue.FieldByName("maxReceiveBatch").Int())
	assert.Equal(t, int64(wantPush), queue.FieldByName("maxPushBatch").Int())

	wantConsumerWait := workq.WaitStrategy{Spins: 64, Yields: 16}
	if row.tuning.consumerWait != nil {
		wantConsumerWait = *row.tuning.consumerWait
	}
	wantProducerWait := workq.WaitStrategy{Spins: 16, Yields: 4}
	if row.tuning.producerWait != nil {
		wantProducerWait = *row.tuning.producerWait
	}
	assert.Equal(t, wantConsumerWait, waitStrategyField(queue, "consumerWait"), "consumer wait")
	assert.Equal(t, wantProducerWait, waitStrategyField(queue, "producerWait"), "producer wait")
}

// waitStrategyField reads the WaitStrategy in queue's unexported field name. It reads each
// field with Int and Bool, because Interface panics on a value reached through an unexported
// field.
func waitStrategyField(queue reflect.Value, name string) workq.WaitStrategy {
	field := queue.FieldByName(name)
	return workq.WaitStrategy{
		Spins:     int(field.FieldByName("Spins").Int()),
		Yields:    int(field.FieldByName("Yields").Int()),
		NeverPark: field.FieldByName("NeverPark").Bool(),
	}
}

// longestReceivedBatch runs 10,000 items through the tuning row called name in SPMC at batch 32
// and work=1us, and returns the longest batch any consumer received. The consumers are slower
// than the producer, so the queue fills and a consumer takes as many items as its limit allows.
func longestReceivedBatch(t *testing.T, name string) int {
	t.Helper()
	const items = 10_000
	row := workRowNamed(t, workqTuningRows, name)
	run := newSaturatedRun(items, spmcMode, 0, time.Microsecond)
	longest := make([]paddedCount, spmcMode.consumers)
	handlers := func(consumer int) func(batch []int64) {
		handle := run.handlers(consumer)
		return func(batch []int64) {
			longest[consumer].value = max(longest[consumer].value, len(batch))
			handle(batch)
		}
	}
	built := buildRow(t, row, spmcMode, 32)
	defer built.release()
	result := runWithin(t, name+" receive limit", func() (workResult, error) {
		result, _, err := runLoadOnce(t, built, spmcMode, 32, run, handlers)
		return result, err
	})
	require.Equal(t, items, result.handled)
	overall := 0
	for _, slot := range longest {
		overall = max(overall, slot.value)
	}
	return overall
}

// TestWorkqEnginePushPieces checks that the engine row builds the engine workq picks in each
// mode, and that it splits a push into pieces of at most workqDefaultBatch items.
func TestWorkqEnginePushPieces(t *testing.T) {
	row := workRowNamed(t, workqCompareRows, "engine")
	wantTypes := map[string]string{
		spscMode.name:          "*benchmark.spscBurstQueue[int64]",
		mpscMode.name:          "*benchmark.frontierMPSCBurstQueue[int64]",
		wideProducersMode.name: "*benchmark.frontierMPSCBurstQueue[int64]",
		spmcMode.name:          "*benchmark.frontierSPMCBurstQueue[int64]",
		wideConsumersMode.name: "*benchmark.frontierSPMCBurstQueue[int64]",
		mpmcMode.name:          "*benchmark.frontierMPMCBurstQueue[int64]",
	}
	for _, mode := range workqTestModes {
		built := buildRow(t, row, mode, 128)
		target, ok := built.row.(*engineRow)
		require.True(t, ok, "engine row is %T", built.row)
		assert.Equal(t, wantTypes[mode.name], fmt.Sprintf("%T", target.target), mode.name)
		built.release()
	}

	built := buildRow(t, row, mpmcMode, 128)
	defer built.release()
	target, ok := built.row.(*engineRow)
	require.True(t, ok, "engine row is %T", built.row)
	for c, dst := range target.dsts {
		assert.Len(t, dst, workqDefaultBatch, "consumer %d buffer", c)
	}
	recorder := &enqueueRecorder{queue: target.target}
	target.target = recorder
	target.push(make([]int64, 128))
	assert.LessOrEqual(t, recorder.longest, workqDefaultBatch)
	assert.Equal(t, 128, recorder.enqueued)
}

// enqueueRecorder wraps a queue and records the longest items passed to enqueue and how many
// items it enqueued.
type enqueueRecorder struct {
	queue[int64]
	longest  int
	enqueued int
}

func (r *enqueueRecorder) enqueue(items []int64) int {
	r.longest = max(r.longest, len(items))
	moved := r.queue.enqueue(items)
	r.enqueued += moved
	return moved
}

// TestWorkqSelectsEngine checks that the workq row's single-producer and single-consumer
// options pick, in each mode, the engine the engine row builds. It reads the queue's
// unexported engine field on purpose, to check the harness's mapping from mode to
// WithSingleProducer and WithSingleConsumer.
func TestWorkqSelectsEngine(t *testing.T) {
	row := workRowNamed(t, workqCompareRows, "workq")
	wantTypes := map[string]reflect.Type{
		spscMode.name:          reflect.TypeFor[*dpdk.SPSCQueue[int64]](),
		mpscMode.name:          reflect.TypeFor[*frontier.MPSCQueue[int64]](),
		wideProducersMode.name: reflect.TypeFor[*frontier.MPSCQueue[int64]](),
		spmcMode.name:          reflect.TypeFor[*frontier.SPMCQueue[int64]](),
		wideConsumersMode.name: reflect.TypeFor[*frontier.SPMCQueue[int64]](),
		mpmcMode.name:          reflect.TypeFor[*frontier.MPMCQueue[int64]](),
	}
	for _, mode := range workqTestModes {
		built := buildRow(t, row, mode, 32)
		target, ok := built.row.(*workqRow)
		require.True(t, ok, "workq row is %T", built.row)
		engine := reflect.ValueOf(target.queue).Elem().FieldByName("engine").Elem().Type()
		assert.Equal(t, wantTypes[mode.name], engine, mode.name)
		built.release()
	}
}

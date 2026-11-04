package benchmark

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"time"
)

// The wide modes put 16 goroutines on the multi side.
var (
	wideConsumersMode = benchMode{name: "SPMC16", producers: 1, consumers: 16}
	wideProducersMode = benchMode{name: "MPSC16", producers: 16, consumers: 1}
)

// The load names, which the rows' scopes refer to.
const (
	loadSaturated     = "saturated"
	loadWideConsumers = "wide-consumers"
	loadWideProducers = "wide-producers"
	loadPushWait      = "push-wait"
	loadPaced         = "paced"
	loadBurst         = "burst"
)

// pacedLead is how long after the run's start the paced producer's schedule begins, so the
// consumers start before the first item and goroutine startup does not show up as latency.
const pacedLead = time.Millisecond

// sleepPaceMin is the smallest pace at which the paced producer sleeps between items. Sleeping
// is accurate enough only at millisecond paces.
const sleepPaceMin = time.Millisecond

// burstItems is how many items one burst pushes. One op of the burst load is one burst.
const burstItems = 10_000

// burstPause is how long the burst producer sleeps before each burst, so the consumers of every
// parking row reach their park. It is longer than any tuning row's spin and yield budget.
const burstPause = 2 * time.Millisecond

// workBatches are the producer batches of the throughput loads.
var workBatches = []int{1, 32, 128}

// throughputWorks are the handler work values of the saturated and wide loads.
var throughputWorks = []time.Duration{0, time.Microsecond, 50 * time.Microsecond}

// workLoads lists every load, in the order the benchmarks run them.
var workLoads = []workLoad{
	{
		name:    loadSaturated,
		modes:   benchModes,
		batches: workBatches,
		works:   throughputWorks,
		newRun:  newSaturatedRun,
	},
	{
		name:    loadWideConsumers,
		modes:   []benchMode{wideConsumersMode},
		batches: workBatches,
		works:   throughputWorks,
		newRun:  newSaturatedRun,
	},
	{
		name:    loadWideProducers,
		modes:   []benchMode{wideProducersMode},
		batches: workBatches,
		works:   throughputWorks,
		newRun:  newSaturatedRun,
	},
	{
		name:    loadPushWait,
		modes:   []benchMode{mpscMode, mpmcMode, wideProducersMode},
		batches: workBatches,
		works:   []time.Duration{time.Microsecond, 50 * time.Microsecond},
		newRun:  newPushWaitRun,
	},
	{
		name:    loadPaced,
		modes:   []benchMode{spscMode, spmcMode, wideConsumersMode},
		batches: []int{1},
		works:   throughputWorks,
		paces: []time.Duration{
			10 * time.Microsecond,
			100 * time.Microsecond,
			2 * time.Millisecond,
		},
		skip:   skipPaced,
		newRun: newPacedRun,
	},
	{
		name:    loadBurst,
		modes:   []benchMode{spmcMode},
		batches: []int{32},
		works:   []time.Duration{time.Microsecond},
		newRun:  newBurstRun,
	},
}

// workLoad is one load: the modes, batches, handler work, and paces it runs at, and how to set
// up one run of it.
type workLoad struct {
	name    string
	modes   []benchMode
	batches []int
	works   []time.Duration
	// paces is nil for every load except paced.
	paces []time.Duration
	// skip reports whether the load leaves out this combination, for the paced work and mode
	// limits in the loads table. It may be nil.
	skip func(mode benchMode, pace, work time.Duration) bool
	// newRun sets up one run of n ops.
	newRun func(n int, mode benchMode, pace, work time.Duration) *loadRun
}

// loadRun holds one run's producer, handlers, and per-run slots. It owns every slot the
// producers and consumers write, all allocated by newRun, outside the timer.
type loadRun struct {
	// base is the run's start time, which runWorkq sets.
	base     time.Time
	total    int
	produce  producerFunc
	handlers func(consumer int) func(batch []int64)
	// metrics reads the slots after the run and returns the load's own metrics by name. It is
	// nil for the loads that report none of their own.
	metrics func(res workResult) map[string]float64

	// The slots. Each load fills only its own, and the rest stay nil.
	// lateness holds the paced producer's lateness for each item.
	lateness []time.Duration
	// latencies holds each paced consumer's latencies.
	latencies []paddedLatencies
	// pushMax holds each push-wait producer's longest push.
	pushMax []paddedDuration
	// burstStarts holds each burst's start, and firstItems[c][i] consumer c's first-item time
	// in burst i, or -1.
	burstStarts []time.Duration
	// burstEnds holds the time the burst producer's last push of each burst returned.
	burstEnds  []time.Duration
	firstItems [][]time.Duration
}

// paddedDuration holds one goroutine's duration on its own cache line.
type paddedDuration struct {
	value time.Duration
	_     [cacheLine - 8]byte
}

// paddedLatencies holds one consumer's latency slice and its write index on their own cache
// line.
type paddedLatencies struct {
	values []time.Duration
	next   int
	_      [cacheLine - 32]byte
}

// paddedCount holds one goroutine's count on its own cache line.
type paddedCount struct {
	value int
	_     [cacheLine - 8]byte
}

// skipPaced leaves out the paced combinations the loads table excludes. 50us work at
// pace=10us is longer than the pace, which saturates the consumer, so the load stops being
// light. SPMC16 is there for the spare-workers case, which the shorter work already covers,
// so it skips 50us work to keep the grid small. It also skips SPMC16 at pace=2ms, where each
// item takes 2ms and SPSC and SPMC already measure the cold wake.
func skipPaced(mode benchMode, pace, work time.Duration) bool {
	const longWork = 50 * time.Microsecond
	if work == longWork && pace == 10*time.Microsecond {
		return true
	}
	if mode == wideConsumersMode && work == longWork {
		return true
	}
	return mode == wideConsumersMode && pace == 2*time.Millisecond
}

// rowRunsIn reports whether row runs in load, in mode, at work. Batches are checked separately.
func rowRunsIn(row workRowSpec, load workLoad, mode benchMode, work time.Duration) bool {
	if row.loads == nil {
		return true
	}
	scope, ok := row.loads[load.name]
	if !ok {
		return false
	}
	if scope.modes != nil && !slices.Contains(scope.modes, mode.name) {
		return false
	}
	return scope.works == nil || slices.Contains(scope.works, work)
}

// rowModes returns each distinct mode, in the order first seen, of every load in loads that row
// runs in at one or more of the load's works. It ignores batches, so it lists a mode even when
// the row runs in that load at no batch, as for chan-batch in paced.
func rowModes(row workRowSpec, loads []workLoad) []benchMode {
	var modes []benchMode
	for _, load := range loads {
		for _, mode := range load.modes {
			if slices.Contains(modes, mode) {
				continue
			}
			for _, work := range load.works {
				if rowRunsIn(row, load, mode, work) {
					modes = append(modes, mode)
					break
				}
			}
		}
	}
	return modes
}

// durationLabel formats d for a sub-benchmark name, as 0, 1us, or 2ms. time.Duration.String
// prints 0s and 1µs instead.
func durationLabel(d time.Duration) string {
	switch {
	case d == 0:
		return "0"
	case d%time.Millisecond == 0:
		return fmt.Sprintf("%dms", d/time.Millisecond)
	case d%time.Microsecond == 0:
		return fmt.Sprintf("%dus", d/time.Microsecond)
	default:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
}

// workResult is what one run of runWorkq measured.
type workResult struct {
	handled int
	// cpu is the process's CPU time across the timed region, valid only when cpuOK is true.
	cpu   time.Duration
	cpuOK bool
	// largestShare is the largest share of the handled items that one consumer handled. It is 0
	// when no item was handled.
	largestShare float64
}

// producerFunc sends share items through push for producer, using buf to build them. It checks
// state.aborted() before each push and returns when it is true.
type producerFunc func(
	state *runState,
	producer, share int,
	buf []int64,
	push func(items []int64),
)

// runWorkq moves total items from mode's producers to its consumers through row, filling each
// producer's buffer of batch items, and returns what the run measured.
//
// It stops row once every producer has returned, even after an abort, unlike runBlocking. Every
// producer has returned by then, so stop is safe in every row, and it frees the channel rows'
// consumers, which have no abort hook. The error is the run's first error, if any, and the
// caller decides how to fail. The caller releases the row after it returns.
func runWorkq(
	timer benchTimer,
	state *runState,
	row workRow,
	mode benchMode,
	batch, total int,
	produce producerFunc,
	handlers func(consumer int) func(batch []int64),
	base *time.Time,
) (workResult, error) {
	bufs := newBuffers[int64](mode.producers, batch)
	counts := make([]paddedCount, mode.consumers)
	wrapped := make([]func(batch []int64), mode.consumers)
	for c := range wrapped {
		handle := handlers(c)
		count := &counts[c]
		wrapped[c] = func(batch []int64) {
			count.value += len(batch)
			handle(batch)
		}
	}

	var cpuStart, cpuEnd time.Duration
	var startOK, endOK bool
	runTimed(timer, func() {
		cpuStart, startOK = processCPU()
		// The go statements below order this write before every read of base.
		*base = time.Now()
		var producerGroup, consumerGroup sync.WaitGroup
		for c := range mode.consumers {
			consumerGroup.Go(func() {
				row.consume(c, wrapped[c])
			})
		}
		for p := range mode.producers {
			producerGroup.Go(func() {
				produce(state, p, producerShare(total, mode.producers, p), bufs[p], row.push)
			})
		}
		producerGroup.Wait()
		row.stop()
		consumerGroup.Wait()
		cpuEnd, endOK = processCPU()
	})

	result := workResult{cpu: cpuEnd - cpuStart, cpuOK: startOK && endOK}
	for _, count := range counts {
		result.handled += count.value
	}
	result.largestShare = largestShare(counts)
	if state.firstError() != nil {
		return result, fmt.Errorf("mode=%s: %w", mode.name, state.firstError())
	}
	return result, nil
}

// largestShare returns the largest count in counts divided by the sum of the counts, or 0 when
// the sum is 0.
func largestShare(counts []paddedCount) float64 {
	sum, largest := 0, 0
	for _, count := range counts {
		sum += count.value
		largest = max(largest, count.value)
	}
	if sum == 0 {
		return 0
	}
	return float64(largest) / float64(sum)
}

// saturatedProducer sends share items as fast as it can, refilling buf before each push. The
// last batch is shorter when share does not divide evenly.
func saturatedProducer(
	state *runState,
	producer, share int,
	buf []int64,
	push func(items []int64),
) {
	for sent := 0; sent < share; {
		if state.aborted() {
			return
		}
		batch := buf[:min(len(buf), share-sent)]
		fillInt64(batch, producer, int64(sent))
		push(batch)
		sent += len(batch)
	}
}

// pushWaitProducer returns a saturated producer that times each push and writes its longest
// push to its own slot after its last push. Only this load reads the clock around pushes, so
// the clock reads never slow the throughput loads.
func pushWaitProducer(slots []paddedDuration) producerFunc {
	return func(
		state *runState,
		producer, share int,
		buf []int64,
		push func(items []int64),
	) {
		var longest time.Duration
		timedPush := func(items []int64) {
			start := time.Now()
			push(items)
			longest = max(longest, time.Since(start))
		}
		saturatedProducer(state, producer, share, buf, timedPush)
		slots[producer].value = longest
	}
}

// pacedProducer returns a producer that sends one item at a time on an absolute schedule: item
// k, counting from 0, is due at base + pacedLead + (k+1)*pace. It records each item's lateness
// in lateness, and the paced load has one producer, so lateness is indexed by k. Each item
// carries its push time since base, so latency runs from the push and the producer's lateness
// does not enter it.
//
// Below sleepPaceMin the producer yields in a loop until the due time, which keeps the
// scheduler warm. A 20µs time.Sleep takes about 1.08ms when the other goroutines are parked and
// about 34µs when one is yielding, so a sleeping producer would send bursts in some rows and
// paced items in others. A producer that spins without yielding keeps its P, and a send that
// wakes a parked consumer puts that consumer in that P's runnext slot, where it stays trapped:
// p50 latency with chan int64 in SPSC measured about 89µs at pace=100us and about 47µs at
// pace=10us, against about 0.6µs and 0.25µs with the yield. With the yield, the woken consumer
// may run on the producer's P. Locking the producer to its OS thread raised p50 to about 26µs,
// because each yield hands off the P.
//
// Each runtime.Gosched wakes an idle P, so the yield loop costs about 1.3 CPUs, including the
// runtime threads it wakes. A parked workq consumer measured p50 latency of about 1.3µs with
// this producer and about 1.85µs with a sleeping one, so latency at those paces is a lower
// bound on the cost of a park. At sleepPaceMin and above the producer sleeps, so the scheduler
// is cold when an item arrives, and pace=2ms measures that case.
func pacedProducer(pace time.Duration, lateness []time.Duration, base *time.Time) producerFunc {
	return func(
		state *runState,
		_, share int,
		buf []int64,
		push func(items []int64),
	) {
		item := buf[:1]
		for k := range share {
			if state.aborted() {
				return
			}
			due := base.Add(pacedLead + time.Duration(k+1)*pace)
			waitUntil(due, pace)
			lateness[k] = time.Since(due)
			item[0] = int64(time.Since(*base))
			push(item)
		}
	}
}

// waitUntil returns at due or later. It sleeps when pace is at least sleepPaceMin, and yields
// in a loop otherwise, for the reasons pacedProducer gives.
func waitUntil(due time.Time, pace time.Duration) {
	if pace >= sleepPaceMin {
		time.Sleep(time.Until(due))
		return
	}
	for time.Now().Before(due) {
		runtime.Gosched()
	}
}

// burstProducer returns a producer that sends share/burstItems bursts. Before each burst it
// sleeps burstPause (2 ms), then records the burst's start. The pause is intended to let the
// queue drain and parking consumers park, but does not prove either happened. It then pushes
// burstItems items whose value is the burst index, len(buf) at a time, and records when the
// burst's last push returned. The engine row and cwait-never never park.
//
// The producer does not yield while it pushes. A consumer that a push wakes goes into the
// producer's P's runnext slot and can stay there until another P steals it or the producer
// blocks on a full queue. So the start delays include how long each row's producer takes to
// fill the queue, which differs between rows for scheduler reasons and not wake-policy reasons.
// first-start-ns shows that delay on its own.
func burstProducer(starts, ends []time.Duration, base *time.Time) producerFunc {
	return func(
		state *runState,
		_, share int,
		buf []int64,
		push func(items []int64),
	) {
		for burst := range share / burstItems {
			time.Sleep(burstPause)
			starts[burst] = time.Since(*base)
			for sent := 0; sent < burstItems; {
				if state.aborted() {
					return
				}
				piece := buf[:min(len(buf), burstItems-sent)]
				for k := range piece {
					piece[k] = int64(burst)
				}
				push(piece)
				sent += len(piece)
			}
			ends[burst] = time.Since(*base)
		}
	}
}

// spinWork busy-waits for d without sleeping. It returns at once when d is 0.
func spinWork(d time.Duration) {
	if d == 0 {
		return
	}
	start := time.Now()
	for {
		if time.Since(start) >= d {
			return
		}
	}
}

// workHandler returns a handler that runs work once for each item in a batch.
func workHandler(work time.Duration) func(batch []int64) {
	return func(batch []int64) {
		for range batch {
			spinWork(work)
		}
	}
}

// newSaturatedRun sets up a run of n items from producers that push as fast as they can.
func newSaturatedRun(n int, _ benchMode, _, work time.Duration) *loadRun {
	handler := workHandler(work)
	return &loadRun{
		total:   n,
		produce: saturatedProducer,
		handlers: func(_ int) func(batch []int64) {
			return handler
		},
	}
}

// newPushWaitRun sets up a saturated run of n items that also times each producer's pushes.
func newPushWaitRun(n int, mode benchMode, _, work time.Duration) *loadRun {
	handler := workHandler(work)
	run := &loadRun{
		total:   n,
		pushMax: make([]paddedDuration, mode.producers),
		handlers: func(_ int) func(batch []int64) {
			return handler
		},
	}
	run.produce = pushWaitProducer(run.pushMax)
	run.metrics = func(_ workResult) map[string]float64 {
		return map[string]float64{"max-push-ns": float64(pushWaitMax(run.pushMax))}
	}
	return run
}

// newPacedRun sets up a run of n items, one per pace, from the one producer of a paced mode.
func newPacedRun(n int, mode benchMode, pace, work time.Duration) *loadRun {
	run := &loadRun{
		total:     n,
		lateness:  make([]time.Duration, n),
		latencies: make([]paddedLatencies, mode.consumers),
	}
	for c := range run.latencies {
		run.latencies[c].values = make([]time.Duration, n)
	}
	run.produce = pacedProducer(pace, run.lateness, &run.base)
	run.handlers = func(consumer int) func(batch []int64) {
		return pacedHandler(&run.latencies[consumer], work, &run.base)
	}
	run.metrics = func(_ workResult) map[string]float64 {
		return pacedMetrics(run.latencies, run.lateness)
	}
	return run
}

// pacedHandler returns a handler that records the latency of every item in the batch, in order,
// and then runs work for each item. Recording every latency first keeps earlier items' work out
// of later items' latency.
func pacedHandler(slot *paddedLatencies, work time.Duration, base *time.Time) func([]int64) {
	return func(batch []int64) {
		for _, item := range batch {
			slot.values[slot.next] = time.Since(*base) - time.Duration(item)
			slot.next++
		}
		for range batch {
			spinWork(work)
		}
	}
}

// pacedMetrics returns the latency percentiles across every consumer and the 99th percentile of
// the producer's lateness. It leaves latencies and lateness unchanged.
func pacedMetrics(latencies []paddedLatencies, lateness []time.Duration) map[string]float64 {
	var merged []time.Duration
	for _, slot := range latencies {
		merged = append(merged, slot.values[:slot.next]...)
	}
	slices.Sort(merged)
	sortedLateness := slices.Clone(lateness)
	slices.Sort(sortedLateness)
	return map[string]float64{
		"p50-ns":      float64(percentile(merged, 50)),
		"p99-ns":      float64(percentile(merged, 99)),
		"max-ns":      float64(percentile(merged, 100)),
		"late-p99-ns": float64(percentile(sortedLateness, 99)),
	}
}

// newBurstRun sets up a run of n bursts of burstItems items each.
func newBurstRun(n int, mode benchMode, _, work time.Duration) *loadRun {
	run := &loadRun{
		total:       burstItems * n,
		burstStarts: make([]time.Duration, n),
		burstEnds:   make([]time.Duration, n),
		firstItems:  make([][]time.Duration, mode.consumers),
	}
	for c := range run.firstItems {
		run.firstItems[c] = make([]time.Duration, n)
		for i := range run.firstItems[c] {
			run.firstItems[c][i] = -1
		}
	}
	run.produce = burstProducer(run.burstStarts, run.burstEnds, &run.base)
	run.handlers = func(consumer int) func(batch []int64) {
		return burstHandler(run.firstItems[consumer], work, &run.base)
	}
	run.metrics = func(_ workResult) map[string]float64 {
		return burstMetrics(run.burstStarts, run.firstItems)
	}
	return run
}

// burstHandler returns a handler that records, in firstItems, the time it first sees an item of
// each burst, and runs work for each item.
func burstHandler(firstItems []time.Duration, work time.Duration, base *time.Time) func([]int64) {
	lastBurst := int64(-1)
	return func(batch []int64) {
		for _, item := range batch {
			if item != lastBurst {
				lastBurst = item
				firstItems[item] = time.Since(*base)
			}
			spinWork(work)
		}
	}
}

// percentile returns the p-th percentile of sorted, whose length must be at least 1, for p in
// [0, 100].
func percentile(sorted []time.Duration, p int) time.Duration {
	return sorted[(len(sorted)-1)*p/100]
}

// pushWaitMax returns the longest duration in slots.
func pushWaitMax(slots []paddedDuration) time.Duration {
	var longest time.Duration
	for _, slot := range slots {
		longest = max(longest, slot.value)
	}
	return longest
}

// burstDelays returns, for each burst at least one consumer handled, the delay from the burst's
// start to its first consumer's first item, and for each burst every consumer handled, the
// delay to its last consumer's first item. missed counts the bursts some consumer did not
// handle. starts[i] is burst i's start, and firstItems[c][i] is consumer c's first-item time in
// burst i, or -1.
func burstDelays(
	starts []time.Duration,
	firstItems [][]time.Duration,
) (firstDelays, fourthDelays []time.Duration, missed int) {
	for i, start := range starts {
		present := 0
		var earliest, latest time.Duration
		for _, consumerTimes := range firstItems {
			at := consumerTimes[i]
			if at < 0 {
				continue
			}
			if present == 0 || at < earliest {
				earliest = at
			}
			latest = max(latest, at)
			present++
		}
		if present > 0 {
			firstDelays = append(firstDelays, earliest-start)
		}
		if present < len(firstItems) {
			missed++
			continue
		}
		fourthDelays = append(fourthDelays, latest-start)
	}
	return firstDelays, fourthDelays, missed
}

// burstMetrics returns the burst load's metrics by name. When no burst has a delay of a kind,
// it leaves out that kind's metrics, so a run that missed every burst reports only
// missed-share.
func burstMetrics(starts []time.Duration, firstItems [][]time.Duration) map[string]float64 {
	firstDelays, fourthDelays, missed := burstDelays(starts, firstItems)
	metrics := map[string]float64{
		"missed-share": float64(missed) / float64(len(starts)),
	}
	if len(firstDelays) > 0 {
		metrics["first-start-ns"] = float64(meanDuration(firstDelays))
	}
	if len(fourthDelays) > 0 {
		metrics["fourth-start-ns"] = float64(meanDuration(fourthDelays))
		metrics["fourth-start-max-ns"] = float64(slices.Max(fourthDelays))
	}
	return metrics
}

// meanDuration returns the mean of durations, whose length must be at least 1.
func meanDuration(durations []time.Duration) time.Duration {
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	return sum / time.Duration(len(durations))
}

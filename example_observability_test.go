package workq_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/freshdresch/go-data-structures/workq"
)

type submissionOutcome int

const (
	submissionSuccess submissionOutcome = iota
	submissionFull
	submissionCanceled
	submissionDeadline
	submissionClosed
	submissionCloseRace
	submissionOther
	submissionOutcomeCount
)

func classifySubmission(err error) submissionOutcome {
	switch {
	case err == nil:
		return submissionSuccess
	case errors.Is(err, workq.ErrClosedDuringPush):
		return submissionCloseRace
	case errors.Is(err, workq.ErrClosed):
		return submissionClosed
	case errors.Is(err, workq.ErrFull):
		return submissionFull
	case errors.Is(err, context.Canceled):
		return submissionCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return submissionDeadline
	default:
		return submissionOther
	}
}

// Each worker owns its counters until joined. Offered counts include retry attempts.
type submissionCounts struct {
	calls      int64
	offered    int64
	accepted   int64
	unaccepted int64
	outcomes   [submissionOutcomeCount]int64
}

func (counts *submissionCounts) recordBatch(offered, accepted int, err error) {
	counts.calls++
	counts.offered += int64(offered)
	counts.accepted += int64(accepted)
	counts.unaccepted += int64(offered - accepted)
	counts.outcomes[classifySubmission(err)]++
}

func (counts *submissionCounts) recordSingle(err error) {
	accepted := 0
	if err == nil || errors.Is(err, workq.ErrClosedDuringPush) {
		accepted = 1
	}
	counts.recordBatch(1, accepted, err)
}

func (counts *submissionCounts) merge(other submissionCounts) {
	counts.calls += other.calls
	counts.offered += other.offered
	counts.accepted += other.accepted
	counts.unaccepted += other.unaccepted
	for i, count := range other.outcomes {
		counts.outcomes[i] += count
	}
}

type processingCounts struct {
	acquired           int64
	completed          int64
	failed             int64
	unattempted        int64
	unresolved         int64
	failedHandlerCalls int64
}

func (counts *processingCounts) merge(other processingCounts) {
	counts.acquired += other.acquired
	counts.completed += other.completed
	counts.failed += other.failed
	counts.unattempted += other.unattempted
	counts.unresolved += other.unresolved
	counts.failedHandlerCalls += other.failedHandlerCalls
}

// processObservedItems uses a synchronous per-item success boundary and retains no payloads.
func processObservedItems(
	ctx context.Context,
	batch []int,
	process func(context.Context, int) error,
) (processingCounts, error) {
	counts := processingCounts{acquired: int64(len(batch))}
	for i, item := range batch {
		if err := ctx.Err(); err != nil {
			counts.unattempted = int64(len(batch) - i)
			counts.failedHandlerCalls = 1
			return counts, err
		}
		if err := process(ctx, item); err != nil {
			counts.failed = 1
			counts.unattempted = int64(len(batch) - i - 1)
			counts.failedHandlerCalls = 1
			return counts, err
		}
		counts.completed++
	}
	return counts, nil
}

// A nil result must mean that every item completed. Errors without per-item attribution
// leave the entire batch unresolved, even if the handler performed some work.
func processObservedBatch(
	ctx context.Context,
	batch []int,
	process func(context.Context, []int) error,
) (processingCounts, error) {
	counts := processingCounts{acquired: int64(len(batch))}
	if err := process(ctx, batch); err != nil {
		counts.unresolved = counts.acquired
		counts.failedHandlerCalls = 1
		return counts, err
	}
	counts.completed = counts.acquired
	return counts, nil
}

type finalProgress struct {
	deliveryValid bool
	unacquired    int64
	accounted     bool
	drained       bool
}

// Call only after producers and consumers join and accounting finishes. The application
// must also enforce producer join before Close. A detected close race invalidates the whole run.
func reconcileProgress(
	submissions submissionCounts,
	processing processingCounts,
	lifecycleErr error,
) finalProgress {
	report := finalProgress{
		deliveryValid: submissions.outcomes[submissionCloseRace] == 0,
		accounted: processing.acquired == processing.completed+processing.failed+
			processing.unattempted+processing.unresolved,
	}
	if !report.deliveryValid {
		return report
	}
	report.unacquired = submissions.accepted - processing.acquired
	report.accounted = report.accounted && report.unacquired >= 0
	report.drained = report.accounted && lifecycleErr == nil &&
		submissions.accepted == processing.completed && processing.failedHandlerCalls == 0
	return report
}

func Example_observabilityCounts() {
	queue, err := workq.New[int](4, workq.WithMaxReceiveBatch(2))
	if err != nil {
		fmt.Println(err)
		return
	}
	abortCtx, cancelAbort := context.WithCancel(context.Background())
	defer cancelAbort()
	var producerCounts [2]submissionCounts
	var consumerCounts [2]processingCounts
	var producerErrors, consumerErrors [2]error
	var producers, consumers sync.WaitGroup
	for worker := range consumerCounts {
		consumers.Go(func() {
			consumerErrors[worker] = queue.Consume(abortCtx, func(ctx context.Context, batch []int) error {
				progress, err := processObservedItems(ctx, batch, func(context.Context, int) error {
					// This application defines synchronous handler success as completion.
					return nil
				})
				consumerCounts[worker].merge(progress)
				return err
			})
			if consumerErrors[worker] != nil {
				cancelAbort()
			}
		})
	}
	for worker := range producerCounts {
		producers.Go(func() {
			// Each finite source owns one fixed batch, independent of queue capacity.
			batch := [4]int{worker * 4, worker*4 + 1, worker*4 + 2, worker*4 + 3}
			accepted, err := queue.PushBatch(abortCtx, batch[:])
			producerCounts[worker].recordBatch(len(batch), accepted, err)
			producerErrors[worker] = err
			if err != nil {
				cancelAbort()
			}
		})
	}
	producers.Wait()
	queue.Close()
	consumers.Wait()

	var submissions submissionCounts
	var processing processingCounts
	for _, counts := range producerCounts {
		submissions.merge(counts)
	}
	for _, counts := range consumerCounts {
		processing.merge(counts)
	}
	lifecycleErr := errors.Join(errors.Join(producerErrors[:]...), errors.Join(consumerErrors[:]...))
	report := reconcileProgress(submissions, processing, lifecycleErr)
	if lifecycleErr != nil {
		fmt.Println("lifecycle failure:", lifecycleErr)
	}
	fmt.Printf(
		"calls: %d, offered: %d, accepted: %d, unaccepted: %d\n",
		submissions.calls,
		submissions.offered,
		submissions.accepted,
		submissions.unaccepted,
	)
	fmt.Printf(
		"acquired: %d, completed: %d, failed: %d, unattempted: %d, unresolved: %d\n",
		processing.acquired,
		processing.completed,
		processing.failed,
		processing.unattempted,
		processing.unresolved,
	)
	fmt.Printf(
		"delivery valid: %t, unacquired: %d, drained: %t\n",
		report.deliveryValid,
		report.unacquired,
		report.drained,
	)
	// Output:
	// calls: 2, offered: 8, accepted: 8, unaccepted: 0
	// acquired: 8, completed: 8, failed: 0, unattempted: 0, unresolved: 0
	// delivery valid: true, unacquired: 0, drained: true
}

// Value fields are initialized before publication and never restamped on a retry.
type observedItem struct {
	payload  int
	sampled  bool
	admitted time.Time
}

// Buckets are disjoint: <=1µs, (1µs, 1ms], (1ms, 1s], and >1s.
// Each worker owns its histogram until joined. No raw samples are retained.
type durationHistogram struct {
	buckets [4]int64
	sum     time.Duration
}

func (histogram *durationHistogram) record(elapsed time.Duration) {
	bounds := [...]time.Duration{time.Microsecond, time.Millisecond, time.Second}
	bucket := len(bounds)
	for i, bound := range bounds {
		if elapsed <= bound {
			bucket = i
			break
		}
	}
	histogram.buckets[bucket]++
	histogram.sum += elapsed
}

func (histogram durationHistogram) samples() int64 {
	var total int64
	for _, count := range histogram.buckets {
		total += count
	}
	return total
}

func (histogram *durationHistogram) merge(other durationHistogram) {
	for i, count := range other.buckets {
		histogram.buckets[i] += count
	}
	histogram.sum += other.sum
}

// The shared observation is at handler entry, not each item's processing start.
func (histogram *durationHistogram) recordAcquisition(batch []observedItem, observed time.Time) {
	for _, item := range batch {
		if item.sampled {
			histogram.record(observed.Sub(item.admitted))
		}
	}
}

type submissionTiming [submissionOutcomeCount]durationHistogram

func (timing *submissionTiming) record(started, returned time.Time, err error) {
	timing[classifySubmission(err)].record(returned.Sub(started))
}

func Example_observabilitySampledTiming() {
	queue, err := workq.New[observedItem](4, workq.WithMaxReceiveBatch(2))
	if err != nil {
		fmt.Println(err)
		return
	}
	abortCtx, cancelAbort := context.WithCancel(context.Background())
	defer cancelAbort()
	var consumerCounts [2]processingCounts
	var admissionDelays [2]durationHistogram
	var consumerErrors [2]error
	var totals [2]int
	var consumers sync.WaitGroup
	for worker := range consumerCounts {
		consumers.Go(func() {
			consumerErrors[worker] = queue.Consume(
				abortCtx,
				func(_ context.Context, batch []observedItem) error {
					observed := time.Now()
					admissionDelays[worker].recordAcquisition(batch, observed)
					consumerCounts[worker].acquired += int64(len(batch))
					// Synchronous summation is this application's completion boundary.
					for _, item := range batch {
						totals[worker] += item.payload
					}
					consumerCounts[worker].completed += int64(len(batch))
					return nil
				},
			)
			if consumerErrors[worker] != nil {
				cancelAbort()
			}
		})
	}

	// Independent per-item draws precede latency observation. The fixed seed makes this
	// example reproducible. Set the probability to 1 for full timing with the same recipe.
	const sampleProbability = 0.5
	sampler := rand.New(rand.NewPCG(1, 2))
	var submissions submissionCounts
	var callLatency submissionTiming
	var producerErr error
	var batch [4]observedItem
	for first := 0; first < 8; first += len(batch) {
		for i := range batch {
			// Admission is this finite source's handoff of each item to the application.
			item := observedItem{payload: first + i, sampled: sampler.Float64() < sampleProbability}
			if item.sampled {
				item.admitted = time.Now()
			}
			batch[i] = item
		}
		started := time.Now()
		accepted, err := queue.PushBatch(abortCtx, batch[:])
		returned := time.Now()
		submissions.recordBatch(len(batch), accepted, err)
		callLatency.record(started, returned, err)
		if err != nil {
			producerErr = err
			cancelAbort()
			break
		}
		// The call returned, so its producer buffer can be reused. Published copies
		// keep their original timestamps, even while consumers still own those items.
	}
	// This caller is the only producer. All its calls finished before Close.
	queue.Close()
	consumers.Wait()

	var processing processingCounts
	var admissionDelay durationHistogram
	var producerCallLatency durationHistogram
	var total int
	for worker, counts := range consumerCounts {
		processing.merge(counts)
		admissionDelay.merge(admissionDelays[worker])
		total += totals[worker]
	}
	for _, histogram := range callLatency {
		producerCallLatency.merge(histogram)
	}
	lifecycleErr := errors.Join(producerErr, errors.Join(consumerErrors[:]...))
	if lifecycleErr != nil {
		fmt.Println("lifecycle failure:", lifecycleErr)
	}
	report := reconcileProgress(submissions, processing, lifecycleErr)
	fmt.Printf("item sampling probability: %.1f\n", sampleProbability)
	fmt.Printf(
		"offered: %d, accepted: %d, acquired: %d, completed: %d\n",
		submissions.offered, submissions.accepted, processing.acquired, processing.completed,
	)
	fmt.Println(
		"call outcomes (success, full, canceled, deadline, closed, close race, other):",
		submissions.outcomes,
	)
	fmt.Println("admission-to-observed-acquisition samples:", admissionDelay.samples())
	fmt.Println("producer call latency samples:", producerCallLatency.samples())
	fmt.Printf(
		"unacquired: %d, drained: %t, payload total: %d\n",
		report.unacquired, report.drained, total,
	)
	// Acquisition samples exclude rejected items and accepted items left queued after abort.
	// Output:
	// item sampling probability: 0.5
	// offered: 8, accepted: 8, acquired: 8, completed: 8
	// call outcomes (success, full, canceled, deadline, closed, close race, other): [2 0 0 0 0 0 0]
	// admission-to-observed-acquisition samples: 4
	// producer call latency samples: 2
	// unacquired: 0, drained: true, payload total: 28
}

// A fixed slot belongs to one worker. The collector reads only its atomic fields.
// Never copy a slot after first use, and do not reuse it before its worker joins.
type liveWorkerMetrics struct {
	accepted           atomic.Int64
	acquired           atomic.Int64
	completed          atomic.Int64
	failed             atomic.Int64
	unattempted        atomic.Int64
	failedHandlerCalls atomic.Int64
	activeHandlers     atomic.Int64
	handlerHeldItems   atomic.Int64
	handlerLatency     [4]atomic.Int64
	handlerLatencySum  atomic.Int64
}

func (metrics *liveWorkerMetrics) processItems(
	ctx context.Context,
	batch []int,
	process func(context.Context, int) error,
) error {
	metrics.acquired.Add(int64(len(batch)))
	metrics.activeHandlers.Add(1)
	metrics.handlerHeldItems.Add(int64(len(batch)))
	defer metrics.activeHandlers.Add(-1)
	defer metrics.handlerHeldItems.Add(-int64(len(batch)))

	started := time.Now()
	counts, err := processObservedItems(ctx, batch, process)
	elapsed := time.Since(started)
	metrics.completed.Add(counts.completed)
	metrics.failed.Add(counts.failed)
	metrics.unattempted.Add(counts.unattempted)
	metrics.failedHandlerCalls.Add(counts.failedHandlerCalls)
	var histogram durationHistogram
	histogram.record(elapsed)
	metrics.handlerLatencySum.Add(int64(elapsed))
	for bucket, count := range histogram.buckets {
		if count != 0 {
			metrics.handlerLatency[bucket].Add(count)
		}
	}
	return err
}

type liveSnapshot struct {
	accepted         int64
	processing       processingCounts
	activeHandlers   int64
	handlerHeldItems int64
	handlerLatency   durationHistogram
}

// The fields and slots can describe different instants while workers are active.
func scrapeLiveMetrics(workers []liveWorkerMetrics) liveSnapshot {
	var snapshot liveSnapshot
	for worker := range workers {
		metrics := &workers[worker]
		snapshot.accepted += metrics.accepted.Load()
		snapshot.processing.acquired += metrics.acquired.Load()
		snapshot.processing.completed += metrics.completed.Load()
		snapshot.processing.failed += metrics.failed.Load()
		snapshot.processing.unattempted += metrics.unattempted.Load()
		snapshot.processing.failedHandlerCalls += metrics.failedHandlerCalls.Load()
		snapshot.activeHandlers += metrics.activeHandlers.Load()
		snapshot.handlerHeldItems += metrics.handlerHeldItems.Load()
		snapshot.handlerLatency.sum += time.Duration(metrics.handlerLatencySum.Load())
		for bucket := range metrics.handlerLatency {
			snapshot.handlerLatency.buckets[bucket] += metrics.handlerLatency[bucket].Load()
		}
	}
	return snapshot
}

type completionsPerSecond float64

func completionRate(previous, current int64, elapsed time.Duration) completionsPerSecond {
	if elapsed <= 0 {
		return 0
	}
	return completionsPerSecond(float64(current-previous) / elapsed.Seconds())
}

// Only run writes these fields. Its owner reads them after joining the collector.
type liveCollector struct {
	latest liveSnapshot
	rate   completionsPerSecond
}

// The owner closes workersDone after joining all workers, waits for finalCollected,
// then cancels ctx and joins run. Collection continues through a work abort.
func (collector *liveCollector) run(
	ctx context.Context,
	workers []liveWorkerMetrics,
	interval time.Duration,
	workersDone <-chan struct{},
	finalCollected chan<- struct{},
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	previousTime := time.Now()
	collector.latest = scrapeLiveMetrics(workers)
	for {
		final := false
		select {
		case <-workersDone:
			final = true
		case <-ticker.C:
		}
		now := time.Now()
		snapshot := scrapeLiveMetrics(workers)
		collector.rate = completionRate(
			collector.latest.processing.completed,
			snapshot.processing.completed,
			now.Sub(previousTime),
		)
		collector.latest = snapshot
		previousTime = now
		// An application can export this approximate sample here, outside worker paths.
		if final {
			close(finalCollected)
			<-ctx.Done()
			return
		}
	}
}

func Example_observabilityLive() {
	queue, err := workq.New[int](4, workq.WithMaxReceiveBatch(2))
	if err != nil {
		fmt.Println(err)
		return
	}
	abortCtx, cancelAbort := context.WithCancel(context.Background())
	defer cancelAbort()
	collectorCtx, stopCollector := context.WithCancel(context.Background())
	defer stopCollector()
	// Slot zero belongs to this caller, the only producer. Slots 1 and 2 are consumers.
	var metrics [3]liveWorkerMetrics
	workersDone := make(chan struct{})
	finalCollected := make(chan struct{})
	var collector liveCollector
	var collectors, consumers sync.WaitGroup
	collectors.Go(func() {
		collector.run(collectorCtx, metrics[:], time.Second, workersDone, finalCollected)
	})
	var consumerErrors [2]error
	for worker := range consumerErrors {
		consumers.Go(func() {
			consumerErrors[worker] = queue.Consume(abortCtx, func(ctx context.Context, batch []int) error {
				return metrics[worker+1].processItems(ctx, batch, func(context.Context, int) error {
					// Synchronous success defines completion for this application.
					return nil
				})
			})
			if consumerErrors[worker] != nil {
				cancelAbort()
			}
		})
	}
	items := [8]int{0, 1, 2, 3, 4, 5, 6, 7}
	accepted, producerErr := queue.PushBatch(abortCtx, items[:])
	var submissions submissionCounts
	submissions.recordBatch(len(items), accepted, producerErr)
	metrics[0].accepted.Add(int64(accepted))
	if producerErr != nil {
		cancelAbort()
	}
	// The sole producer's call returned. Consumers finish accounting before final collection.
	queue.Close()
	consumers.Wait()
	close(workersDone)
	<-finalCollected
	stopCollector()
	collectors.Wait()

	lifecycleErr := errors.Join(producerErr, errors.Join(consumerErrors[:]...))
	if lifecycleErr != nil {
		fmt.Println("lifecycle failure:", lifecycleErr)
	}
	final := collector.latest
	report := reconcileProgress(submissions, final.processing, lifecycleErr)
	fmt.Println("live scrapes: approximate across fields")
	fmt.Printf(
		"final accepted: %d, acquired: %d, completed: %d\n",
		final.accepted, final.processing.acquired, final.processing.completed,
	)
	fmt.Printf(
		"active handler calls: %d, handler_held_items: %d, drained: %t\n",
		final.activeHandlers, final.handlerHeldItems, report.drained,
	)
	// Rates and histogram buckets depend on scheduling and are omitted from stable output.
	// Output:
	// live scrapes: approximate across fields
	// final accepted: 8, acquired: 8, completed: 8
	// active handler calls: 0, handler_held_items: 0, drained: true
}

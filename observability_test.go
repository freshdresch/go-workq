package workq_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq"
)

func newObservabilityQueue(t *testing.T, capacity, receiveLimit int) *workq.Queue[int] {
	t.Helper()
	queue, err := workq.New[int](
		capacity,
		workq.WithMaxPushBatch(1),
		workq.WithMaxReceiveBatch(receiveLimit),
		workq.WithConsumerWait(workq.WaitStrategy{}),
		workq.WithProducerWait(workq.WaitStrategy{}),
	)
	require.NoError(t, err)
	t.Cleanup(queue.Close)
	return queue
}

func TestObservabilityPartialAcceptance(t *testing.T) {
	for _, operation := range []string{"push batch", "try push batch"} {
		t.Run(operation, func(t *testing.T) {
			queue := newObservabilityQueue(t, 2, 2)
			items := []int{1, 2, 3}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var accepted int
			var err error
			var expectedAccepted int64
			var expectedOutcome submissionOutcome
			if operation == "push batch" {
				accepted, err = queue.PushBatch(ctx, items)
				require.ErrorIs(t, err, context.Canceled)
				expectedAccepted, expectedOutcome = 1, submissionCanceled
			} else {
				accepted, err = queue.TryPushBatch(items)
				require.ErrorIs(t, err, workq.ErrFull)
				expectedAccepted, expectedOutcome = 2, submissionFull
			}
			var counts submissionCounts
			counts.recordBatch(len(items), accepted, err)
			require.Equal(t, int64(1), counts.calls)
			require.Equal(t, int64(3), counts.offered)
			require.Equal(t, expectedAccepted, counts.accepted)
			require.Equal(t, int64(3)-expectedAccepted, counts.unaccepted)
			require.Equal(t, int64(1), counts.outcomes[expectedOutcome])
			buffer := make([]int, 3)
			received, err := queue.TryReceive(buffer)
			require.NoError(t, err)
			require.Equal(t, accepted, received)
			require.Equal(t, items[:accepted], buffer[:received])
		})
	}
}

func TestObservabilityCanceledFastPaths(t *testing.T) {
	queue := newObservabilityQueue(t, 2, 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var submissions submissionCounts
	err := queue.Push(ctx, 1)
	require.NoError(t, err)
	submissions.recordSingle(err)
	accepted, err := queue.PushBatch(ctx, []int{2})
	require.NoError(t, err)
	submissions.recordBatch(1, accepted, err)
	err = queue.Push(ctx, 3)
	require.ErrorIs(t, err, context.Canceled)
	submissions.recordSingle(err)
	buffer := make([]int, 2)
	received, err := queue.Receive(ctx, buffer)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, buffer[:received])
	processing, err := processObservedItems(ctx, buffer[:received], func(context.Context, int) error {
		t.Fatal("canceled processing attempted an item")
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(2), submissions.accepted)
	require.Equal(t, int64(1), submissions.unaccepted)
	require.Equal(t, int64(2), submissions.outcomes[submissionSuccess])
	require.Equal(t, int64(1), submissions.outcomes[submissionCanceled])
	require.Equal(t, processingCounts{
		acquired: 2, unattempted: 2, failedHandlerCalls: 1,
	}, processing)
}

func TestObservabilityEmptyBatches(t *testing.T) {
	queue := newObservabilityQueue(t, 2, 1)
	var counts submissionCounts
	for _, closed := range []bool{false, true} {
		if closed {
			queue.Close()
		}
		accepted, err := queue.PushBatch(context.Background(), nil)
		counts.recordBatch(0, accepted, err)
		accepted, err = queue.TryPushBatch(nil)
		counts.recordBatch(0, accepted, err)
	}
	require.Equal(t, int64(4), counts.calls)
	require.Zero(t, counts.offered)
	require.Zero(t, counts.accepted)
	require.Zero(t, counts.unaccepted)
	require.Equal(t, int64(2), counts.outcomes[submissionSuccess])
	require.Equal(t, int64(2), counts.outcomes[submissionClosed])
}

func TestObservabilitySingleOutcomes(t *testing.T) {
	queue := newObservabilityQueue(t, 2, 1)
	var counts submissionCounts
	err := queue.TryPush(1)
	require.NoError(t, err)
	counts.recordSingle(err)
	err = queue.TryPush(2)
	require.NoError(t, err)
	counts.recordSingle(err)
	err = queue.TryPush(3)
	require.ErrorIs(t, err, workq.ErrFull)
	counts.recordSingle(err)
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancel()
	err = queue.Push(ctx, 3)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	counts.recordSingle(err)
	queue.Close()
	err = queue.Push(context.Background(), 3)
	require.ErrorIs(t, err, workq.ErrClosed)
	counts.recordSingle(err)
	err = queue.TryPush(3)
	require.ErrorIs(t, err, workq.ErrClosed)
	counts.recordSingle(err)
	require.Equal(t, int64(6), counts.calls)
	require.Equal(t, int64(6), counts.offered)
	require.Equal(t, int64(2), counts.accepted)
	require.Equal(t, int64(4), counts.unaccepted)
	require.Equal(t, int64(2), counts.outcomes[submissionSuccess])
	require.Equal(t, int64(1), counts.outcomes[submissionFull])
	require.Equal(t, int64(1), counts.outcomes[submissionDeadline])
	require.Equal(t, int64(2), counts.outcomes[submissionClosed])
}

func TestObservabilityCloseRaceInvalidatesRun(t *testing.T) {
	// Returned-outcome accounting avoids injecting a race into the real queue lifecycle.
	var counts submissionCounts
	counts.recordSingle(nil)
	counts.recordSingle(workq.ErrClosedDuringPush)
	counts.recordBatch(3, 2, workq.ErrClosedDuringPush)
	require.Equal(t, int64(4), counts.accepted)
	require.Equal(t, int64(1), counts.unaccepted)
	require.Equal(t, int64(2), counts.outcomes[submissionCloseRace])
	require.Zero(t, counts.outcomes[submissionClosed])
	report := reconcileProgress(counts, processingCounts{acquired: 4, completed: 4}, nil)
	require.False(t, report.deliveryValid)
	require.False(t, report.drained)
}

func TestObservabilityProcessingAndReconciliation(t *testing.T) {
	failure := errors.New("item failed")
	for _, abort := range []bool{false, true} {
		name := "drain"
		if abort {
			name = "abort"
		}
		t.Run(name, func(t *testing.T) {
			queue := newObservabilityQueue(t, 8, 3)
			items := []int{1, 2, 3, 4, 5}
			accepted, err := queue.PushBatch(context.Background(), items)
			require.NoError(t, err)
			var submissions submissionCounts
			submissions.recordBatch(len(items), accepted, err)
			queue.Close()
			var processing processingCounts
			err = queue.Consume(context.Background(), func(ctx context.Context, batch []int) error {
				progress, err := processObservedItems(ctx, batch, func(_ context.Context, item int) error {
					if abort && item == 2 {
						return failure
					}
					return nil
				})
				processing.merge(progress)
				return err
			})
			report := reconcileProgress(submissions, processing, err)
			require.True(t, report.deliveryValid)
			require.True(t, report.accounted)
			if abort {
				require.ErrorIs(t, err, failure)
				require.Equal(t, processingCounts{
					acquired: 3, completed: 1, failed: 1, unattempted: 1, failedHandlerCalls: 1,
				}, processing)
				require.Equal(t, int64(2), report.unacquired)
				require.False(t, report.drained)
				return
			}
			require.NoError(t, err)
			require.Equal(t, processingCounts{acquired: 5, completed: 5}, processing)
			require.Zero(t, report.unacquired)
			require.True(t, report.drained)
			require.False(t, reconcileProgress(submissions, processing, failure).drained)
		})
	}
}

func TestObservabilityUnresolvedBatch(t *testing.T) {
	for _, handlerErr := range []error{nil, errors.New("unknown item outcomes")} {
		queue := newObservabilityQueue(t, 4, 3)
		accepted, err := queue.PushBatch(context.Background(), []int{1, 2, 3})
		require.NoError(t, err)
		var submissions submissionCounts
		submissions.recordBatch(3, accepted, err)
		queue.Close()
		var processing processingCounts
		err = queue.Consume(context.Background(), func(ctx context.Context, batch []int) error {
			progress, err := processObservedBatch(ctx, batch, func(context.Context, []int) error {
				return handlerErr
			})
			processing.merge(progress)
			return err
		})
		report := reconcileProgress(submissions, processing, err)
		require.True(t, report.accounted)
		if handlerErr != nil {
			require.ErrorIs(t, err, handlerErr)
			require.Equal(t, processingCounts{
				acquired: 3, unresolved: 3, failedHandlerCalls: 1,
			}, processing)
			require.False(t, report.drained)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, processingCounts{acquired: 3, completed: 3}, processing)
		require.True(t, report.drained)
	}
}

func TestObservabilityCompletionBeforeProducerReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue := newObservabilityQueue(t, 2, 1)
		ctx, cancel := context.WithCancel(context.Background())
		completed, release := make(chan struct{}), make(chan struct{})
		produced := make(chan struct{})
		releaseConsumer := sync.OnceFunc(func() { close(release) })
		var producers, consumers sync.WaitGroup
		defer func() {
			releaseConsumer()
			cancel()
			producers.Wait()
			queue.Close()
			consumers.Wait()
		}()
		var submissions submissionCounts
		var processing processingCounts
		var producerErr, consumerErr error
		consumers.Go(func() {
			consumerErr = queue.Consume(ctx, func(ctx context.Context, batch []int) error {
				progress, err := processObservedItems(ctx, batch, func(context.Context, int) error {
					return nil
				})
				processing.merge(progress)
				if batch[0] == 1 {
					close(completed)
					<-release
				}
				return err
			})
		})
		producers.Go(func() {
			defer close(produced)
			items := []int{1, 2, 3, 4}
			accepted, err := queue.PushBatch(ctx, items)
			submissions.recordBatch(len(items), accepted, err)
			producerErr = err
		})
		<-completed
		// Only one slot was freed. The remaining three items cannot fit in capacity two.
		// Wait confirms both workers reached their barriers before reading private counters.
		synctest.Wait()
		require.Equal(t, int64(1), processing.completed)
		require.Zero(t, submissions.accepted)
		select {
		case <-produced:
			t.Fatal("producer returned while its suffix could not fit")
		default:
		}
		releaseConsumer()
		producers.Wait()
		queue.Close()
		consumers.Wait()
		require.NoError(t, producerErr)
		require.NoError(t, consumerErr)
		require.Equal(t, int64(4), submissions.accepted)
		require.Equal(t, processingCounts{acquired: 4, completed: 4}, processing)
		require.True(t, reconcileProgress(submissions, processing, nil).drained)
	})
}

func TestObservabilityHistogramBoundaries(t *testing.T) {
	cases := []struct {
		elapsed time.Duration
		bucket  int
	}{
		{0, 0},
		{time.Microsecond, 0},
		{time.Microsecond + 1, 1},
		{time.Millisecond, 1},
		{time.Millisecond + 1, 2},
		{time.Second, 2},
		{time.Second + 1, 3},
	}
	var merged durationHistogram
	var sum time.Duration
	for _, test := range cases {
		var histogram durationHistogram
		histogram.record(test.elapsed)
		var expected [4]int64
		expected[test.bucket] = 1
		require.Equal(t, expected, histogram.buckets, "duration %v", test.elapsed)
		require.Equal(t, test.elapsed, histogram.sum)
		merged.merge(histogram)
		sum += test.elapsed
	}
	require.Equal(t, [4]int64{2, 2, 2, 1}, merged.buckets)
	require.Equal(t, int64(len(cases)), merged.samples())
	require.Equal(t, sum, merged.sum)
}

func TestObservabilitySampledDurationArithmetic(t *testing.T) {
	admitted := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	batch := []observedItem{
		{payload: 1, sampled: true, admitted: admitted},
		{payload: 2},
		{payload: 3, sampled: true, admitted: admitted.Add(time.Millisecond)},
	}
	var histogram durationHistogram
	histogram.recordAcquisition(batch, admitted.Add(2*time.Millisecond))
	require.Equal(t, [4]int64{0, 1, 1, 0}, histogram.buckets)
	require.Equal(t, 3*time.Millisecond, histogram.sum)
	require.Equal(t, int64(2), histogram.samples())
	var unsampled durationHistogram
	unsampled.recordAcquisition(batch[1:2], admitted.Add(time.Hour))
	require.Equal(t, durationHistogram{}, unsampled)
}

func TestObservabilityCallTimingOutcomes(t *testing.T) {
	started := time.Unix(100, 0)
	outcomes := []error{
		nil, workq.ErrFull, context.Canceled, context.DeadlineExceeded,
		workq.ErrClosed, workq.ErrClosedDuringPush, errors.New("other"),
	}
	var timing submissionTiming
	for i, err := range outcomes {
		elapsed := time.Duration(i+1) * time.Millisecond
		timing.record(started, started.Add(elapsed), err)
	}
	for i, histogram := range timing {
		require.Equal(t, int64(1), histogram.samples(), "outcome %d", i)
		require.Equal(t, time.Duration(i+1)*time.Millisecond, histogram.sum)
	}
}

func newObservedItemQueue(t *testing.T, capacity, receiveLimit int) *workq.Queue[observedItem] {
	t.Helper()
	queue, err := workq.New[observedItem](
		capacity,
		workq.WithMaxPushBatch(1),
		workq.WithMaxReceiveBatch(receiveLimit),
	)
	require.NoError(t, err)
	t.Cleanup(queue.Close)
	return queue
}

func TestObservabilityRetryPreservesAdmission(t *testing.T) {
	queue := newObservedItemQueue(t, 2, 2)
	admitted := time.Unix(100, 0)
	batch := []observedItem{
		{payload: 1, sampled: true, admitted: admitted},
		{payload: 2, sampled: true, admitted: admitted},
		{payload: 3, sampled: true, admitted: admitted},
	}
	var submissions submissionCounts
	var timing submissionTiming
	started := time.Now()
	accepted, err := queue.TryPushBatch(batch)
	returned := time.Now()
	submissions.recordBatch(len(batch), accepted, err)
	timing.record(started, returned, err)
	require.ErrorIs(t, err, workq.ErrFull)
	require.Equal(t, 2, accepted)
	var buffer [2]observedItem
	received, err := queue.TryReceive(buffer[:])
	require.NoError(t, err)
	require.Equal(t, 2, received)
	require.Equal(t, batch[:accepted], buffer[:received])
	var delays durationHistogram
	delays.recordAcquisition(buffer[:received], admitted.Add(time.Second))

	// The application retains responsibility for the suffix, so retry the same envelopes.
	suffix := batch[accepted:]
	started = time.Now()
	accepted, err = queue.PushBatch(context.Background(), suffix)
	returned = time.Now()
	submissions.recordBatch(len(suffix), accepted, err)
	timing.record(started, returned, err)
	require.NoError(t, err)
	require.Equal(t, 1, accepted)
	received, err = queue.TryReceive(buffer[:])
	require.NoError(t, err)
	require.Equal(t, 1, received)
	require.Equal(t, batch[2], buffer[0])
	delays.recordAcquisition(buffer[:received], admitted.Add(2*time.Second))
	require.Equal(t, 4*time.Second, delays.sum)
	require.Equal(t, int64(3), delays.samples())
	require.Equal(t, int64(4), submissions.offered)
	require.Equal(t, int64(3), submissions.accepted)
	require.Equal(t, int64(1), submissions.unaccepted)
	require.Equal(t, int64(1), timing[submissionFull].samples())
	require.Equal(t, int64(1), timing[submissionSuccess].samples())
}

func TestObservabilityMissingSamplesAfterAbort(t *testing.T) {
	queue := newObservedItemQueue(t, 2, 1)
	admitted := time.Unix(100, 0)
	batch := []observedItem{
		{payload: 1},
		{payload: 2, sampled: true, admitted: admitted},
		{payload: 3, sampled: true, admitted: admitted},
	}
	var submissions submissionCounts
	var timing submissionTiming
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := time.Now()
	accepted, err := queue.TryPushBatch(batch)
	returned := time.Now()
	submissions.recordBatch(len(batch), accepted, err)
	timing.record(started, returned, err)
	require.ErrorIs(t, err, workq.ErrFull)
	require.Equal(t, 2, accepted)
	cancel()
	started = time.Now()
	err = queue.Push(ctx, batch[2])
	returned = time.Now()
	submissions.recordSingle(err)
	timing.record(started, returned, err)
	require.ErrorIs(t, err, context.Canceled)
	queue.Close()
	var processing processingCounts
	var delays durationHistogram
	failure := errors.New("processing failed")
	err = queue.Consume(context.Background(), func(_ context.Context, batch []observedItem) error {
		observed := time.Now()
		delays.recordAcquisition(batch, observed)
		processing.acquired += int64(len(batch))
		processing.failed += int64(len(batch))
		processing.failedHandlerCalls++
		return failure
	})
	require.ErrorIs(t, err, failure)
	report := reconcileProgress(submissions, processing, err)
	require.True(t, report.accounted)
	require.Equal(t, int64(1), report.unacquired)
	require.Equal(t, int64(1), processing.acquired)
	require.Equal(t, int64(1), processing.failed)
	// The acquired item was unsampled. Neither the queued sample nor the rejected sample
	// gets a zero or timeout observation, even though all their accounting is retained.
	require.Equal(t, durationHistogram{}, delays)
	require.Equal(t, int64(4), submissions.offered)
	require.Equal(t, int64(2), submissions.accepted)
	require.Equal(t, int64(2), submissions.unaccepted)
	require.Equal(t, int64(1), timing[submissionFull].samples())
	require.Equal(t, int64(1), timing[submissionCanceled].samples())
}

func TestObservabilityEnvelopeBufferReuse(t *testing.T) {
	queue := newObservedItemQueue(t, 2, 1)
	ctx, cancel := context.WithCancel(context.Background())
	var consumers sync.WaitGroup
	defer func() {
		cancel()
		queue.Close()
		consumers.Wait()
	}()
	admitted := time.Unix(100, 0)
	var delays durationHistogram
	var processing processingCounts
	var consumerErr error
	consumers.Go(func() {
		consumerErr = queue.Consume(ctx, func(_ context.Context, batch []observedItem) error {
			// Synthetic observation times keep arithmetic independent of scheduling.
			delays.recordAcquisition(batch, admitted.Add(time.Second))
			processing.acquired += int64(len(batch))
			for _, item := range batch {
				expected := admitted.Add(time.Duration(item.payload) * time.Millisecond)
				if item.admitted != expected {
					return fmt.Errorf("item %d admission changed", item.payload)
				}
				if item.sampled != (item.payload%2 == 0) {
					return fmt.Errorf("item %d sampling changed", item.payload)
				}
				processing.completed++
			}
			return nil
		})
		if consumerErr != nil {
			cancel()
		}
	})
	var submissions submissionCounts
	var batch [2]observedItem
	for first := 0; first < 64; first += len(batch) {
		for i := range batch {
			payload := first + i
			batch[i] = observedItem{
				payload:  payload,
				sampled:  payload%2 == 0,
				admitted: admitted.Add(time.Duration(payload) * time.Millisecond),
			}
		}
		accepted, err := queue.PushBatch(ctx, batch[:])
		submissions.recordBatch(len(batch), accepted, err)
		require.NoError(t, err)
		// Reuse immediately after return, while the consumer can still be processing.
		clear(batch[:])
	}
	queue.Close()
	consumers.Wait()
	require.NoError(t, consumerErr)
	require.Equal(t, processingCounts{acquired: 64, completed: 64}, processing)
	require.Equal(t, int64(32), delays.samples())
	require.Equal(t, 32*time.Second-992*time.Millisecond, delays.sum)
	require.True(t, reconcileProgress(submissions, processing, nil).drained)
}

func TestObservabilityLiveHandlerGauges(t *testing.T) {
	failure := errors.New("processing failed")
	for _, handlerErr := range []error{nil, failure} {
		name := "success"
		if handlerErr != nil {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			var metrics [1]liveWorkerMetrics
			entered := make(chan struct{})
			release := make(chan struct{})
			var workers sync.WaitGroup
			var result error
			workers.Go(func() {
				result = metrics[0].processItems(
					context.Background(),
					[]int{1, 2, 3},
					func(_ context.Context, item int) error {
						if item == 2 {
							close(entered)
							<-release
							return handlerErr
						}
						return nil
					},
				)
			})
			<-entered
			active := scrapeLiveMetrics(metrics[:])
			// Capture before release, but assert after join so a failing check cannot leak work.
			close(release)
			workers.Wait()
			require.Equal(t, int64(3), active.processing.acquired)
			require.Equal(t, int64(1), active.activeHandlers)
			require.Equal(t, int64(3), active.handlerHeldItems)
			final := scrapeLiveMetrics(metrics[:])
			require.Zero(t, final.activeHandlers)
			require.Zero(t, final.handlerHeldItems)
			require.Equal(t, int64(1), final.handlerLatency.samples())
			if handlerErr != nil {
				require.ErrorIs(t, result, failure)
				require.Equal(t, processingCounts{
					acquired: 3, completed: 1, failed: 1, unattempted: 1, failedHandlerCalls: 1,
				}, final.processing)
				return
			}
			require.NoError(t, result)
			require.Equal(t, processingCounts{acquired: 3, completed: 3}, final.processing)
		})
	}
}

func TestObservabilityLiveConcurrentScrapes(t *testing.T) {
	var metrics [2]liveWorkerMetrics
	started := make(chan struct{}, len(metrics))
	stop := make(chan struct{})
	var workers sync.WaitGroup
	var workerErrors [2]error
	for worker := range metrics {
		workers.Go(func() {
			for iteration := 0; ; iteration++ {
				if iteration == 1 {
					started <- struct{}{}
				}
				select {
				case <-stop:
					return
				default:
				}
				metrics[worker].accepted.Add(1)
				err := metrics[worker].processItems(
					context.Background(),
					[]int{worker},
					func(context.Context, int) error { return nil },
				)
				if err != nil {
					workerErrors[worker] = err
					return
				}
			}
		})
	}
	for range metrics {
		<-started
	}
	// Workers keep updating throughout this collection loop. No barrier serializes
	// individual updates and loads. Atomic totals are monotonic, but fields can disagree.
	var previous liveSnapshot
	monotonic := true
	for range 1000 {
		current := scrapeLiveMetrics(metrics[:])
		if current.accepted < previous.accepted {
			monotonic = false
		}
		if current.processing.completed < previous.processing.completed {
			monotonic = false
		}
		if current.handlerLatency.samples() < previous.handlerLatency.samples() {
			monotonic = false
		}
		previous = current
	}
	close(stop)
	workers.Wait()
	require.True(t, monotonic)
	for _, err := range workerErrors {
		require.NoError(t, err)
	}
	final := scrapeLiveMetrics(metrics[:])
	require.Positive(t, final.accepted)
	require.Equal(t, final.accepted, final.processing.acquired)
	require.Equal(t, final.accepted, final.processing.completed)
	require.Equal(t, final.accepted, final.handlerLatency.samples())
	require.Zero(t, final.activeHandlers)
	require.Zero(t, final.handlerHeldItems)
}

func TestObservabilityLiveCollectorFinalAndJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var metrics [1]liveWorkerMetrics
		releaseWorker := make(chan struct{})
		workersDone := make(chan struct{})
		finalCollected := make(chan struct{})
		collectorExited := make(chan struct{})
		var workers, owners sync.WaitGroup
		workers.Go(func() {
			<-releaseWorker
			metrics[0].accepted.Add(7)
			metrics[0].acquired.Add(7)
			metrics[0].completed.Add(7)
		})
		owners.Go(func() {
			workers.Wait()
			close(workersDone)
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var collector liveCollector
		owners.Go(func() {
			collector.run(ctx, metrics[:], time.Second, workersDone, finalCollected)
			close(collectorExited)
		})
		// Fake time lets periodic collection run while the worker is still owned.
		time.Sleep(2 * time.Second)
		synctest.Wait()
		select {
		case <-finalCollected:
			t.Fatal("final collection preceded worker join")
		default:
		}
		close(releaseWorker)
		<-finalCollected
		synctest.Wait()
		select {
		case <-collectorExited:
			t.Fatal("collector exited before its owner stopped it")
		default:
		}
		cancel()
		owners.Wait()
		require.Equal(t, int64(7), collector.latest.accepted)
		require.Equal(t, int64(7), collector.latest.processing.completed)
		select {
		case <-collectorExited:
		default:
			t.Fatal("collector was not joined")
		}
	})
}

func TestObservabilityLiveCollectorRate(t *testing.T) {
	for _, periodic := range []bool{false, true} {
		name := "startup baseline"
		if periodic {
			name = "periodic baseline"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var metrics [1]liveWorkerMetrics
				metrics[0].completed.Store(11)
				workersDone := make(chan struct{})
				finalCollected := make(chan struct{})
				ctx, cancel := context.WithCancel(context.Background())
				var collector liveCollector
				var collectors sync.WaitGroup
				collectors.Go(func() {
					collector.run(ctx, metrics[:], time.Second, workersDone, finalCollected)
				})
				finish := sync.OnceFunc(func() {
					close(workersDone)
					<-finalCollected
					cancel()
					collectors.Wait()
				})
				defer finish()
				// Establish the startup baseline before publishing any new completions.
				synctest.Wait()
				expectedCompleted := int64(14)
				if periodic {
					metrics[0].completed.Add(8)
					time.Sleep(time.Second)
					synctest.Wait()
					expectedCompleted += 8
				}
				metrics[0].completed.Add(3)
				// Finish between ticks so the final rate uses the preceding collection.
				time.Sleep(500 * time.Millisecond)
				finish()
				require.Equal(t, expectedCompleted, collector.latest.processing.completed)
				require.Equal(t, completionsPerSecond(6), collector.rate)
			})
		})
	}
}

func TestObservabilityCompletionRate(t *testing.T) {
	require.Equal(t, completionsPerSecond(4), completionRate(3, 13, 2500*time.Millisecond))
	require.Zero(t, completionRate(3, 3, time.Second))
	require.Zero(t, completionRate(3, 13, 0))
}

package workq_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/freshdresch/go-data-structures/workq"
)

type recipeOutcome struct {
	mutex       sync.Mutex
	failures    []error
	abortReason error
	stopSources context.CancelFunc
	cancelAbort context.CancelFunc
}

func (outcome *recipeOutcome) failAndAbort(stage string, err error) {
	outcome.mutex.Lock()
	outcome.failures = append(outcome.failures, fmt.Errorf("%s: %w", stage, err))
	outcome.mutex.Unlock()
	outcome.stopSources()
	outcome.cancelAbort()
}

func (outcome *recipeOutcome) requestAbort(reason error) {
	outcome.mutex.Lock()
	if outcome.abortReason == nil {
		outcome.abortReason = reason
	}
	outcome.mutex.Unlock()
	outcome.stopSources()
	outcome.cancelAbort()
}

func (outcome *recipeOutcome) err() error {
	outcome.mutex.Lock()
	defer outcome.mutex.Unlock()
	if len(outcome.failures) > 0 {
		return errors.Join(outcome.failures...)
	}
	return outcome.abortReason
}

func queueFailure(ctx context.Context, err error) error {
	if errors.Is(err, workq.ErrClosedDuringPush) {
		return err
	}
	if errors.Is(err, workq.ErrClosed) {
		return err
	}
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return nil
	}
	return err
}

type peerAbort struct{ reason error }

func (abort peerAbort) Error() string {
	return abort.reason.Error()
}

func (abort peerAbort) Unwrap() error {
	return abort.reason
}

func consumeAndAbort(
	ctx context.Context,
	queue *workq.Queue[int],
	stage string,
	handler func(context.Context, []int) error,
	outcome *recipeOutcome,
) error {
	var handlerErr error
	err := queue.Consume(ctx, func(ctx context.Context, batch []int) error {
		handlerErr = handler(ctx, batch)
		if handlerErr != nil {
			var secondary peerAbort
			if !errors.As(handlerErr, &secondary) {
				outcome.failAndAbort(stage, handlerErr)
			}
		}
		return handlerErr
	})
	if handlerErr == nil {
		if failure := queueFailure(ctx, err); failure != nil {
			outcome.failAndAbort(stage, failure)
		}
	}
	return err
}

func drainQueue(
	graceCtx context.Context,
	queue *workq.Queue[int],
	producers *sync.WaitGroup,
	consumers *sync.WaitGroup,
	outcome *recipeOutcome,
	beforeWatch func(context.Context),
) {
	supervisionCtx, stopSupervision := context.WithCancel(graceCtx)
	var watcher sync.WaitGroup
	watcher.Go(func() {
		if beforeWatch != nil {
			beforeWatch(supervisionCtx)
		}
		<-supervisionCtx.Done()
		if errors.Is(supervisionCtx.Err(), context.DeadlineExceeded) {
			outcome.requestAbort(supervisionCtx.Err())
		}
	})
	producers.Wait()
	queue.Close()
	consumers.Wait()
	stopSupervision()
	watcher.Wait()
}

type batchProgress struct {
	completed   []int
	failed      []int
	unattempted []int
}

func processBatch(
	ctx context.Context,
	batch []int,
	process func(context.Context, int) error,
) (batchProgress, error) {
	var progress batchProgress
	for i, item := range batch {
		if err := ctx.Err(); err != nil {
			progress.unattempted = slices.Clone(batch[i:])
			return progress, peerAbort{reason: err}
		}
		if err := process(ctx, item); err != nil {
			progress.failed = []int{item}
			progress.unattempted = slices.Clone(batch[i+1:])
			return progress, err
		}
		progress.completed = append(progress.completed, item)
	}
	return progress, nil
}

type admissionGate struct {
	mutex   sync.Mutex
	stopped bool
	calls   sync.WaitGroup
}

func (gate *admissionGate) admit() bool {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	if gate.stopped {
		return false
	}
	gate.calls.Add(1)
	return true
}

func (gate *admissionGate) stop() {
	gate.mutex.Lock()
	gate.stopped = true
	gate.mutex.Unlock()
}

func (gate *admissionGate) done() { gate.calls.Done() }

// wait is called only after stop, so no new call can race with the join.
func (gate *admissionGate) wait() { gate.calls.Wait() }

func Example_gracefulShutdown() {
	queue, err := workq.New[int](2, workq.WithMaxPushBatch(1), workq.WithMaxReceiveBatch(1))
	if err != nil {
		fmt.Println(err)
		return
	}
	sourceCtx, stopSources := context.WithCancel(context.Background())
	defer stopSources()
	abortCtx, cancelAbort := context.WithCancel(context.Background())
	defer cancelAbort()
	outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}
	entered := make(chan struct{})
	admitted := make(chan struct{})
	release := make(chan struct{})
	var completed, accepted, unaccepted []int
	var producers, consumers sync.WaitGroup
	consumers.Go(func() {
		// The outcome owns failure handling.
		_ = consumeAndAbort(abortCtx, queue, "consumer", func(_ context.Context, batch []int) error {
			if batch[0] == 1 {
				close(entered)
				<-release
			}
			completed = append(completed, batch...)
			return nil
		}, outcome)
	})
	producers.Go(func() {
		first := []int{1}
		count, err := queue.PushBatch(abortCtx, first)
		accepted = append(accepted, first[:count]...)
		unaccepted = append(unaccepted, first[count:]...)
		if failure := queueFailure(abortCtx, err); failure != nil {
			outcome.failAndAbort("producer", failure)
			return
		}
		<-entered
		batch := []int{2, 3, 4}
		close(admitted)
		count, err = queue.PushBatch(abortCtx, batch)
		accepted = append(accepted, batch[:count]...)
		unaccepted = append(unaccepted, batch[count:]...)
		if failure := queueFailure(abortCtx, err); failure != nil {
			outcome.failAndAbort("producer", failure)
			return
		}
		<-sourceCtx.Done()
	})
	<-admitted
	graceCtx, cancelGrace := context.WithTimeout(context.Background(), time.Minute)
	defer cancelGrace()
	stopSources()
	close(release)
	drainQueue(graceCtx, queue, &producers, &consumers, outcome, nil)
	completed = slices.Clone(completed)
	slices.Sort(completed)
	fmt.Println("completed:", completed)
	fmt.Println("shutdown:", outcome.err())
	// Output:
	// completed: [1 2 3 4]
	// shutdown: <nil>
}

func Example_handlerFailure() {
	queue, err := workq.New[int](4, workq.WithMaxReceiveBatch(3))
	if err != nil {
		fmt.Println(err)
		return
	}
	sourceCtx, stopSources := context.WithCancel(context.Background())
	defer stopSources()
	abortCtx, cancelAbort := context.WithCancel(context.Background())
	defer cancelAbort()
	outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}
	items := []int{1, 2, 3}
	count, err := queue.PushBatch(abortCtx, items)
	accepted, unaccepted := slices.Clone(items[:count]), slices.Clone(items[count:])
	if failure := queueFailure(abortCtx, err); failure != nil {
		outcome.failAndAbort("preload", failure)
		fmt.Println("accepted:", accepted, "unaccepted:", unaccepted, "failure:", outcome.err())
		return
	}
	sentinel := errors.New("item 2 failed")
	entered := make(chan struct{})
	release := make(chan struct{})
	var progress batchProgress
	var producers, consumers sync.WaitGroup
	consumers.Go(func() {
		// The outcome owns failure handling.
		_ = consumeAndAbort(abortCtx, queue, "handler", func(ctx context.Context, batch []int) error {
			close(entered)
			<-release
			var err error
			progress, err = processBatch(ctx, batch, func(_ context.Context, item int) error {
				if item == 2 {
					return sentinel
				}
				return nil
			})
			return err
		}, outcome)
	})
	<-entered
	consumers.Go(func() {
		// The outcome owns failure handling.
		_ = consumeAndAbort(abortCtx, queue, "peer", func(context.Context, []int) error {
			return nil
		}, outcome)
	})
	producers.Go(func() { <-sourceCtx.Done() })
	close(release)
	drainQueue(context.Background(), queue, &producers, &consumers, outcome, nil)
	fmt.Println("completed:", progress.completed)
	fmt.Println("failed:", progress.failed)
	fmt.Println("unattempted:", progress.unattempted)
	fmt.Println("handler failure:", errors.Is(outcome.err(), sentinel))
	// Output:
	// completed: [1]
	// failed: [2]
	// unattempted: [3]
	// handler failure: true
}

func Example_partialPushBatch() {
	queue, err := workq.New[int](2, workq.WithMaxPushBatch(1))
	if err != nil {
		fmt.Println(err)
		return
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	items := []int{1, 2, 3}
	count, firstErr := queue.PushBatch(canceledCtx, items)
	firstCount := count
	accepted, unaccepted := slices.Clone(items[:count]), slices.Clone(items[count:])
	var completed []int
	var consumeErr error
	var consumers sync.WaitGroup
	consumers.Go(func() {
		consumeErr = queue.Consume(context.Background(), func(_ context.Context, batch []int) error {
			completed = append(completed, batch...)
			return nil
		})
	})
	count, retryErr := queue.PushBatch(context.Background(), unaccepted)
	accepted = append(accepted, unaccepted[:count]...)
	unaccepted = slices.Clone(unaccepted[count:])
	queue.Close()
	consumers.Wait()
	if err := errors.Join(retryErr, consumeErr); err != nil {
		fmt.Println("accepted:", accepted, "unaccepted:", unaccepted, "failure:", err)
		return
	}
	fmt.Printf("accepted: %d, canceled: %t\n", firstCount, errors.Is(firstErr, context.Canceled))
	slices.Sort(completed)
	fmt.Println("completed:", completed)
	// Output:
	// accepted: 1, canceled: true
	// completed: [1 2 3]
}

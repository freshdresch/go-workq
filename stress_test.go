package workq

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	stressCapacity   = 2
	stressWorkers    = 4
	stressMaxBatch   = 7
	stressItemsFull  = 5000
	stressItemsShort = 500
	stressTimeout    = 120 * time.Second
)

// stressShutdown is how a stress run ends.
type stressShutdown int

const (
	// closeAfterAllReceived calls Close only once every item has arrived and every producer has
	// returned, so a lost wakeup hangs the run instead of being rescued by done.
	closeAfterAllReceived stressShutdown = iota
	// closeAfterProducers calls Close as soon as the producers return, the normal shutdown.
	closeAfterProducers
	// cancelAtHalf cancels the shared ctx once half the items have arrived, and never closes.
	cancelAtHalf
)

// stressStrategy is one pair of wait strategies, the same for both sides.
type stressStrategy struct {
	name    string
	options []Option
	// singleConsumerReceives makes the one consumer of a single-consumer engine run a Receive
	// loop instead of Consume.
	singleConsumerReceives bool
}

func stressStrategies() []stressStrategy {
	return []stressStrategy{
		{name: "Zero", options: bothSidesWait(WaitStrategy{})},
		{name: "Default", options: nil, singleConsumerReceives: true},
		{name: "NeverPark", options: bothSidesWait(WaitStrategy{Yields: 4, NeverPark: true})},
	}
}

func bothSidesWait(strategy WaitStrategy) []Option {
	return []Option{WithConsumerWait(strategy), WithProducerWait(strategy)}
}

// stressRun holds the shared state of one stress subtest. Item producer*perProducer+sequence is
// the sequence-th item of producer.
type stressRun struct {
	queue         *Queue[int]
	perProducer   int
	itemCount     int
	timesReceived []atomic.Int32
	receivedTotal atomic.Int64
	allReceived   chan struct{}
	// cancel is called once half the items have arrived, and is nil unless the run cancels.
	cancel context.CancelFunc
	// On a single-consumer engine, the one consumer checks that each producer's items arrive in
	// order. Only that consumer touches nextSequence and outOfOrder.
	checkOrder   bool
	nextSequence []int
	outOfOrder   []int
}

// record counts each item of a received batch and, on a single-consumer engine, checks its
// producer's order. In cancel mode it cancels the shared ctx once half the items have arrived,
// and it closes allReceived once every item has arrived.
func (run *stressRun) record(batch []int) {
	for _, item := range batch {
		run.timesReceived[item].Add(1)
		if run.checkOrder {
			run.checkSequence(item)
		}
	}
	after := run.receivedTotal.Add(int64(len(batch)))
	before := after - int64(len(batch))
	half := int64(run.itemCount / 2)
	if run.cancel != nil && before < half && after >= half {
		run.cancel()
	}
	total := int64(run.itemCount)
	if before < total && after >= total {
		close(run.allReceived)
	}
}

func (run *stressRun) checkSequence(item int) {
	producer := item / run.perProducer
	if item%run.perProducer != run.nextSequence[producer] {
		run.outOfOrder = append(run.outOfOrder, item)
	}
	run.nextSequence[producer] = item%run.perProducer + 1
}

// produce pushes the producer's items in order, rotating through Push, PushBatch, TryPush, and
// TryPushBatch, and returns how many it pushed. The pushed items are always the first ones of
// its sequence.
func (run *stressRun) produce(ctx context.Context, producer int) (pushed int, err error) {
	for step := 0; pushed < run.perProducer; step++ {
		first := producer*run.perProducer + pushed
		batchSize := min(1+step%stressMaxBatch, run.perProducer-pushed)
		var moved int
		switch step % 4 {
		case 0:
			err = run.queue.Push(ctx, first)
			if err == nil {
				moved = 1
			}
		case 1:
			moved, err = run.queue.PushBatch(ctx, sequentialItems(first, batchSize))
		case 2:
			moved, err = run.tryPushOneUntilDone(ctx, first)
		default:
			moved, err = run.tryPushUntilDone(ctx, sequentialItems(first, batchSize))
		}
		pushed += moved
		if err != nil {
			return pushed, err
		}
	}
	return pushed, nil
}

// tryPushUntilDone retries TryPushBatch after runtime.Gosched until every item is pushed, ctx is
// done, or TryPushBatch fails with anything but ErrFull.
func (run *stressRun) tryPushUntilDone(ctx context.Context, items []int) (pushed int, err error) {
	for {
		moved, err := run.queue.TryPushBatch(items[pushed:])
		pushed += moved
		if !errors.Is(err, ErrFull) {
			return pushed, err
		}
		runtime.Gosched()
		if isDone(ctx) {
			return pushed, ctx.Err()
		}
	}
}

// tryPushOneUntilDone retries TryPush after runtime.Gosched until it pushes item, ctx is done, or
// TryPush fails with anything but ErrFull, and returns how many items it pushed.
func (run *stressRun) tryPushOneUntilDone(ctx context.Context, item int) (pushed int, err error) {
	for {
		err := run.queue.TryPush(item)
		if err == nil {
			return 1, nil
		}
		if !errors.Is(err, ErrFull) {
			return 0, err
		}
		runtime.Gosched()
		if isDone(ctx) {
			return 0, ctx.Err()
		}
	}
}

func sequentialItems(first int, count int) []int {
	items := make([]int, count)
	for i := range items {
		items[i] = first + i
	}
	return items
}

func (run *stressRun) consumeWithConsume(ctx context.Context) error {
	return run.queue.Consume(ctx, func(_ context.Context, batch []int) error {
		run.record(batch)
		return nil
	})
}

// consumeWithReceive calls Receive until it fails, and returns nil once the queue is closed and
// drained. Its dst alternates between one and two slots, so both the single-item and the burst
// dequeue paths run.
func (run *stressRun) consumeWithReceive(ctx context.Context) error {
	buffer := make([]int, 2)
	for call := 0; ; call++ {
		dst := buffer[:1+call%2]
		received, err := run.queue.Receive(ctx, dst)
		if errors.Is(err, ErrClosed) {
			return nil
		}
		if err != nil {
			return err
		}
		run.record(dst[:received])
	}
}

// TestStress moves items through every engine with every strategy pair, and closes the queue only
// after every item has arrived, so a lost wakeup hangs the run until the watchdog panics.
func TestStress(t *testing.T) {
	runStressMatrix(t, closeAfterAllReceived)
}

// TestStressCloseAfterProducers closes the queue as soon as the producers return, while
// consumers may still hold items in flight, and checks that every item still arrives once.
func TestStressCloseAfterProducers(t *testing.T) {
	runStressMatrix(t, closeAfterProducers)
}

// TestStressCancel cancels a shared ctx once half the items have arrived and never closes the
// queue. Every call must return. The test then drains the queue and requires each pushed item to
// have arrived exactly once.
func TestStressCancel(t *testing.T) {
	runStressMatrix(t, cancelAtHalf)
}

func runStressMatrix(t *testing.T, shutdown stressShutdown) {
	for _, engineCase := range engineCases() {
		t.Run(engineCase.name, func(t *testing.T) {
			for _, pair := range stressStrategies() {
				t.Run(pair.name, func(t *testing.T) {
					runStress(t, engineCase, pair, shutdown)
				})
			}
		})
	}
}

// runStress runs one stress subtest. If it does not finish within stressTimeout, it panics with
// every goroutine's stack, so the process ends and no worker outlives the test.
func runStress(
	t *testing.T,
	engineCase engineCase,
	pair stressStrategy,
	shutdown stressShutdown,
) {
	deadline := time.After(stressTimeout)
	cfg, err := resolveConfig(stressCapacity, engineCase.options)
	require.NoError(t, err)
	options := append(append([]Option{}, engineCase.options...), pair.options...)
	queue, err := New[int](stressCapacity, options...)
	require.NoError(t, err)

	producerCount := stressWorkers
	if cfg.singleProducer {
		producerCount = 1
	}
	consumerCount := stressWorkers
	if cfg.singleConsumer {
		consumerCount = 1
	}
	perProducer := stressItemsFull
	if testing.Short() || raceEnabled {
		perProducer = stressItemsShort
	}
	run := &stressRun{
		queue:         queue,
		perProducer:   perProducer,
		itemCount:     producerCount * perProducer,
		timesReceived: make([]atomic.Int32, producerCount*perProducer),
		allReceived:   make(chan struct{}),
		checkOrder:    cfg.singleConsumer,
		nextSequence:  make([]int, producerCount),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if shutdown == cancelAtHalf {
		run.cancel = cancel
	}

	pushedCounts := make([]int, producerCount)
	producerErrors := make([]error, producerCount)
	var producers sync.WaitGroup
	for producer := range producerCount {
		producers.Go(func() {
			pushedCounts[producer], producerErrors[producer] = run.produce(ctx, producer)
		})
	}
	consumerErrors := make([]error, consumerCount)
	var consumers sync.WaitGroup
	for consumer := range consumerCount {
		consumers.Go(func() {
			if usesReceive(consumer, consumerCount, pair) {
				consumerErrors[consumer] = run.consumeWithReceive(ctx)
				return
			}
			consumerErrors[consumer] = run.consumeWithConsume(ctx)
		})
	}

	switch shutdown {
	case closeAfterAllReceived:
		awaitStress(run.allReceived, deadline, "every item to arrive")
		awaitStress(waitDone(&producers), deadline, "the producers to return")
		queue.Close()
	case closeAfterProducers:
		awaitStress(waitDone(&producers), deadline, "the producers to return")
		queue.Close()
	case cancelAtHalf:
		awaitStress(waitDone(&producers), deadline, "the producers to return")
	}
	awaitStress(waitDone(&consumers), deadline, "the consumers to return")

	checkWorkerErrors(t, producerErrors, consumerErrors, shutdown)
	if shutdown == cancelAtHalf {
		drainOpen(t, run)
		checkPushedReceivedOnce(t, run, pushedCounts)
	} else {
		checkReceivedExactlyOnce(t, run)
	}
	assert.Empty(t, run.outOfOrder, "items that arrived out of their producer's order")
	requireIdleQueue(t, queue)
}

// usesReceive reports whether a consumer runs a Receive loop rather than Consume. Half of
// several consumers use each, and a lone consumer uses the one its strategy pair names.
func usesReceive(consumer int, consumerCount int, pair stressStrategy) bool {
	if consumerCount == 1 {
		return pair.singleConsumerReceives
	}
	return consumer%2 == 1
}

// waitDone returns a channel that closes once workers.Wait returns.
func waitDone(workers *sync.WaitGroup) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	return done
}

// awaitStress waits for done, and panics with every goroutine's stack if deadline passes first.
// A panic ends the process, so no stuck worker outlives the test.
func awaitStress(done <-chan struct{}, deadline <-chan time.Time, description string) {
	select {
	case <-done:
	case <-deadline:
		stacks := make([]byte, 1<<22)
		stacks = stacks[:runtime.Stack(stacks, true)]
		panic(fmt.Sprintf(
			"stress run timed out after %v waiting for %s\n%s",
			stressTimeout,
			description,
			stacks,
		))
	}
}

// checkWorkerErrors checks the errors the workers returned. Without cancellation every call
// succeeds. With it, a producer may return context.Canceled, and a consumer, which the
// never-closed queue can stop only through ctx, must return it.
func checkWorkerErrors(
	t *testing.T,
	producerErrors []error,
	consumerErrors []error,
	shutdown stressShutdown,
) {
	t.Helper()
	for producer, err := range producerErrors {
		if shutdown == cancelAtHalf && err != nil {
			assert.Same(t, context.Canceled, err, "producer %d", producer)
			continue
		}
		assert.NoError(t, err, "producer %d", producer)
	}
	for consumer, err := range consumerErrors {
		if shutdown == cancelAtHalf {
			assert.Same(t, context.Canceled, err, "consumer %d", consumer)
			continue
		}
		assert.NoError(t, err, "consumer %d", consumer)
	}
}

func checkReceivedExactlyOnce(t *testing.T, run *stressRun) {
	t.Helper()
	var notOnce []string
	for item := range run.timesReceived {
		if times := run.timesReceived[item].Load(); times != 1 {
			notOnce = append(notOnce, fmt.Sprintf("item %d received %d times", item, times))
		}
	}
	assert.Empty(t, notOnce)
}

// drainOpen receives what the workers left in the open queue, with a 2-slot TryReceive until it
// returns 0, and records it.
func drainOpen(t *testing.T, run *stressRun) {
	t.Helper()
	dst := make([]int, 2)
	for {
		received, err := run.queue.TryReceive(dst)
		require.NoError(t, err)
		if received == 0 {
			return
		}
		run.record(dst[:received])
	}
}

// checkPushedReceivedOnce checks that each producer's first pushedCounts items arrived exactly
// once and that no other item arrived.
func checkPushedReceivedOnce(t *testing.T, run *stressRun, pushedCounts []int) {
	t.Helper()
	var wrong []string
	for item := range run.timesReceived {
		times := run.timesReceived[item].Load()
		wantTimes := int32(0)
		if item%run.perProducer < pushedCounts[item/run.perProducer] {
			wantTimes = 1
		}
		if times != wantTimes {
			wrong = append(
				wrong,
				fmt.Sprintf("item %d received %d times, want %d", item, times, wantTimes),
			)
		}
	}
	assert.Empty(t, wrong)
}

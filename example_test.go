package workq_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/freshdresch/go-data-structures/workq"
)

// lastNode is the highest node number in the example tree. Node n has children 2n and 2n+1 up to
// lastNode, so the subtrees under nodes 2 and 3 hold every node from 2 to lastNode.
const lastNode = 31

func childrenOf(node int) []int {
	var children []int
	for _, child := range []int{2 * node, 2*node + 1} {
		if child <= lastNode {
			children = append(children, child)
		}
	}
	return children
}

// outstandingWork counts the work that may still lead to a push into its queue. Whoever brings
// the count to zero closes the queue, because at zero no push can be in flight.
type outstandingWork struct {
	count atomic.Int64
	queue *workq.Queue[int]
}

func (work *outstandingWork) add(n int) {
	work.count.Add(int64(n))
}

func (work *outstandingWork) finish() {
	if work.count.Add(-1) == 0 {
		work.queue.Close()
	}
}

// A queue that feeds itself. Its handlers push each node's children back into the queue they
// consume. A handler never blocks in Push on its own queue. It offers children with
// TryPushBatch and processes the ones that do not fit on a stack local to its call. Whoever
// brings the count of outstanding work to zero closes the queue.
func Example_selfFeeding() {
	// The small capacity makes handlers overflow onto their local stacks.
	queue, err := workq.New[int](4)
	if err != nil {
		fmt.Println(err)
		return
	}
	_, stopSources := context.WithCancel(context.Background())
	defer stopSources()
	abortCtx, cancelAbort := context.WithCancel(context.Background())
	defer cancelAbort()
	outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}

	// The owner's token prevents zero before every seeder has registered its lifetime.
	work := &outstandingWork{queue: queue}
	work.add(1)
	var visitedSum atomic.Int64
	handler := func(ctx context.Context, batch []int) error {
		return visitTree(ctx, queue, work, batch, func(_ context.Context, node int) error {
			visitedSum.Add(int64(node))
			return nil
		})
	}

	const consumerCount = 3
	var consumers sync.WaitGroup
	for range consumerCount {
		consumers.Go(func() {
			// The outcome owns failure recording and cancellation.
			_ = consumeAndAbort(abortCtx, queue, "tree consumer", handler, outcome)
		})
	}

	roots := []int{2, 3}
	accepted, unaccepted := make([][]int, len(roots)), make([][]int, len(roots))
	var seeders sync.WaitGroup
	for i, root := range roots {
		work.add(1)
		seeders.Go(func() {
			defer work.finish()
			items := []int{root}
			work.add(len(items))
			count, err := queue.PushBatch(abortCtx, items)
			accepted[i], unaccepted[i] = slices.Clone(items[:count]), slices.Clone(items[count:])
			// Only seeds proved unaccepted can give back their item counts.
			for range unaccepted[i] {
				work.finish()
			}
			if failure := queueFailure(abortCtx, err); failure != nil {
				outcome.failAndAbort("seeder", failure)
			}
		})
	}
	work.finish()

	joinSelfFeeding(queue, &seeders, &consumers)
	if err := outcome.err(); err != nil {
		fmt.Println("accepted:", accepted, "unaccepted:", unaccepted, "failure:", err)
		return
	}
	fmt.Println("sum of visited nodes:", visitedSum.Load())
	// Output: sum of visited nodes: 495
}

// A two-stage pipeline. Stage 1's handler doubles each item and pushes the result into stage 2
// with the ctx it received. Stage 1's handlers are stage 2's producers, so the example closes
// stage 2 only after every stage 1 Consume has returned.
func Example_pipeline() {
	stage1, err := workq.New[int](16)
	if err != nil {
		fmt.Println(err)
		return
	}
	stage2, err := workq.New[int](16)
	if err != nil {
		fmt.Println(err)
		return
	}
	_, stopSources := context.WithCancel(context.Background())
	defer stopSources()
	abortCtx, cancelAbort := context.WithCancel(context.Background())
	defer cancelAbort()
	outcome := &recipeOutcome{stopSources: stopSources, cancelAbort: cancelAbort}

	var total atomic.Int64
	sumHandler := func(_ context.Context, batch []int) error {
		for _, item := range batch {
			total.Add(int64(item))
		}
		return nil
	}

	const consumersPerStage = 2
	reports := make([][]forwardProgress, consumersPerStage)
	var stage1Consumers, stage2Consumers sync.WaitGroup
	for i := range consumersPerStage {
		stage1Consumers.Go(func() {
			handler := func(ctx context.Context, batch []int) error {
				progress, err := forwardDoubled(ctx, stage2, batch)
				reports[i] = append(reports[i], progress)
				return err
			}
			// The outcome owns failure recording and cancellation.
			_ = consumeAndAbort(abortCtx, stage1, "stage 1", handler, outcome)
		})
		stage2Consumers.Go(func() {
			// The outcome owns failure recording and cancellation.
			_ = consumeAndAbort(abortCtx, stage2, "stage 2", sumHandler, outcome)
		})
	}

	var accepted, unaccepted []int
	var producers sync.WaitGroup
	producers.Go(func() {
		items := make([]int, 100)
		for i := range items {
			items[i] = i + 1
		}
		count, err := stage1.PushBatch(abortCtx, items)
		accepted, unaccepted = slices.Clone(items[:count]), slices.Clone(items[count:])
		if failure := queueFailure(abortCtx, err); failure != nil {
			outcome.failAndAbort("producer", failure)
		}
	})
	closePipeline(stage1, stage2, &producers, &stage1Consumers, &stage2Consumers)
	if err := outcome.err(); err != nil {
		fmt.Println("accepted:", accepted, "unaccepted:", unaccepted, "failure:", err)
		fmt.Println("forwarding:", reports)
		return
	}
	fmt.Println("total:", total.Load())
	// Output: total: 10100
}

func visitTree(
	ctx context.Context,
	queue *workq.Queue[int],
	work *outstandingWork,
	batch []int,
	visit func(context.Context, int) error,
) error {
	stack := slices.Clone(batch)
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return peerAbort{reason: err}
		}
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if err := visit(ctx, node); err != nil {
			return err
		}
		children := childrenOf(node)
		work.add(len(children))
		pushed, err := queue.TryPushBatch(children)
		if err != nil && !errors.Is(err, workq.ErrFull) {
			return fmt.Errorf("push children of %d: %w", node, err)
		}
		stack = append(stack, children[pushed:]...)
		work.finish()
	}
	return nil
}

func joinSelfFeeding(
	queue *workq.Queue[int],
	seeders *sync.WaitGroup,
	consumers *sync.WaitGroup,
) {
	seeders.Wait()
	consumers.Wait()
	// After abort, unfinished items may still count, but no caller can publish again.
	queue.Close()
}

type forwardProgress struct {
	forwarded   []int
	unforwarded []int
}

func forwardDoubled(
	ctx context.Context,
	downstream *workq.Queue[int],
	batch []int,
) (forwardProgress, error) {
	doubled := make([]int, len(batch))
	for i, item := range batch {
		doubled[i] = 2 * item
	}
	count, err := downstream.PushBatch(ctx, doubled)
	progress := forwardProgress{
		forwarded:   slices.Clone(doubled[:count]),
		unforwarded: slices.Clone(doubled[count:]),
	}
	if err == nil {
		return progress, nil
	}
	if failure := queueFailure(ctx, err); failure != nil {
		return progress, fmt.Errorf("push to stage 2: %w", failure)
	}
	return progress, peerAbort{reason: err}
}

func closePipeline(
	stage1 *workq.Queue[int],
	stage2 *workq.Queue[int],
	producers *sync.WaitGroup,
	stage1Consumers *sync.WaitGroup,
	stage2Consumers *sync.WaitGroup,
) {
	producers.Wait()
	stage1.Close()
	stage1Consumers.Wait()
	// Stage 1 handlers are stage 2 producers, including while an abort is in progress.
	stage2.Close()
	stage2Consumers.Wait()
}

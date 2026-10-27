package workq_test

import (
	"context"
	"errors"
	"fmt"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The count covers every item from just before its push until a handler finishes it,
	// including items on a handler's local stack. The owner, this function, holds one count
	// from the start.
	work := &outstandingWork{queue: queue}
	work.add(1)

	var visitedSum atomic.Int64
	handler := func(_ context.Context, batch []int) error {
		stack := append([]int(nil), batch...)
		for len(stack) > 0 {
			node := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			visitedSum.Add(int64(node))

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

	const consumerCount = 3
	consumeErrs := make([]error, consumerCount)
	var consumers sync.WaitGroup
	for i := range consumerCount {
		consumers.Go(func() {
			consumeErrs[i] = queue.Consume(ctx, handler)
			// A failed handler leaves its counts unfinished, so canceling ctx is what stops
			// the other consumers and the seeders.
			if consumeErrs[i] != nil {
				cancel()
			}
		})
	}

	// Each seeder gets its count from the owner before it starts, so a handler that finishes
	// the first seeder's work cannot close the queue before the second seeder has counted
	// itself. A seeder gives its count up only after its last push returns.
	roots := []int{2, 3}
	seedErrs := make([]error, len(roots))
	var seeders sync.WaitGroup
	for i, root := range roots {
		work.add(1)
		seeders.Go(func() {
			defer work.finish()
			work.add(1)
			if err := queue.Push(ctx, root); err != nil {
				seedErrs[i] = fmt.Errorf("seed %d: %w", root, err)
				work.finish()
			}
		})
	}
	// The owner releases its count only after it has started every seeder.
	work.finish()

	seeders.Wait()
	consumers.Wait()
	if err := errors.Join(append(seedErrs, consumeErrs...)...); err != nil {
		fmt.Println(err)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var total atomic.Int64
	sumHandler := func(_ context.Context, batch []int) error {
		for _, item := range batch {
			total.Add(int64(item))
		}
		return nil
	}
	doubleHandler := func(ctx context.Context, batch []int) error {
		for i := range batch {
			batch[i] *= 2
		}
		if _, err := stage2.PushBatch(ctx, batch); err != nil {
			return fmt.Errorf("push to stage 2: %w", err)
		}
		return nil
	}

	const consumersPerStage = 2
	stage1Errs := make([]error, consumersPerStage)
	stage2Errs := make([]error, consumersPerStage)
	var stage1Consumers, stage2Consumers sync.WaitGroup
	for i := range consumersPerStage {
		stage1Consumers.Go(func() {
			stage1Errs[i] = stage1.Consume(ctx, doubleHandler)
			if stage1Errs[i] != nil {
				cancel()
			}
		})
		stage2Consumers.Go(func() {
			stage2Errs[i] = stage2.Consume(ctx, sumHandler)
			if stage2Errs[i] != nil {
				cancel()
			}
		})
	}

	items := make([]int, 100)
	for i := range items {
		items[i] = i + 1
	}
	_, produceErr := stage1.PushBatch(ctx, items)
	if produceErr != nil {
		cancel()
	}

	// The producer has returned, so stage 1 can close. Stage 2 closes only once every stage 1
	// Consume has returned, because until then a stage 1 handler may still push into it.
	stage1.Close()
	stage1Consumers.Wait()
	stage2.Close()
	stage2Consumers.Wait()

	errs := append([]error{produceErr}, stage1Errs...)
	if err := errors.Join(append(errs, stage2Errs...)...); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("total:", total.Load())
	// Output: total: 10100
}

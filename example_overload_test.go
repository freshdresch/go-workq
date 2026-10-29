package workq_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/freshdresch/go-data-structures/workq"
)

func Example_overloadReject() {
	queue, err := workq.New[int](2)
	if err != nil {
		fmt.Println(err)
		return
	}
	items := []int{1, 2, 3}
	count, pushErr := queue.TryPushBatch(items)
	accepted := slices.Clone(items[:count])
	unaccepted := slices.Clone(items[count:])
	queue.Close()
	if pushErr != nil && !errors.Is(pushErr, workq.ErrFull) {
		fmt.Println("accepted:", accepted, "unaccepted:", unaccepted, "failure:", pushErr)
		return
	}
	// The caller retains the suffix instead of discarding it or replaying the accepted prefix.
	rejected := unaccepted
	var completed []int
	err = queue.Consume(context.Background(), func(_ context.Context, batch []int) error {
		completed = append(completed, batch...)
		return nil
	})
	if err != nil {
		fmt.Println("consume:", err)
		return
	}
	fmt.Println("accepted:", accepted)
	fmt.Println("rejected:", rejected)
	fmt.Println("completed:", completed)
	// Output:
	// accepted: [1 2]
	// rejected: [3]
	// completed: [1 2]
}

func Example_overloadWaitBudget() {
	queue, err := workq.New[int](2)
	if err != nil {
		fmt.Println(err)
		return
	}
	abortCtx, cancelAbort := context.WithCancel(context.Background())
	defer cancelAbort()
	items := []int{1, 2}
	count, pushErr := queue.TryPushBatch(items)
	accepted := slices.Clone(items[:count])
	if pushErr != nil {
		queue.Close()
		fmt.Println("accepted:", accepted, "unaccepted:", items[count:], "failure:", pushErr)
		return
	}

	// Holding the queue full isolates expiry. Normal consumers keep running during a push.
	submitCtx, cancelSubmit := context.WithTimeout(abortCtx, time.Millisecond)
	defer cancelSubmit()
	pushErr = queue.Push(submitCtx, 3)
	queue.Close()
	if !errors.Is(pushErr, context.DeadlineExceeded) {
		fmt.Println("unexpected submission outcome:", pushErr)
		return
	}
	unaccepted := []int{3}
	var completed []int
	// The submission budget does not govern completion of previously accepted work.
	err = queue.Consume(abortCtx, func(_ context.Context, batch []int) error {
		completed = append(completed, batch...)
		return nil
	})
	if err != nil {
		fmt.Println("consume:", err)
		return
	}
	fmt.Println("accepted:", accepted)
	fmt.Println("unaccepted:", unaccepted)
	fmt.Println("submission:", pushErr)
	fmt.Println("work context:", abortCtx.Err())
	fmt.Println("completed:", completed)
	// Output:
	// accepted: [1 2]
	// unaccepted: [3]
	// submission: context deadline exceeded
	// work context: <nil>
	// completed: [1 2]
}

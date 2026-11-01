package workq_test

import (
	"context"
	"fmt"

	"github.com/freshdresch/go-data-structures/workq"
)

func ExampleQueue() {
	queue, err := workq.New[int](2)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer queue.Close()

	// The entire finite input fits, so this producer finishes before receiving starts.
	accepted, err := queue.PushBatch(context.Background(), []int{1, 2})
	if err != nil {
		fmt.Println("accepted:", accepted, "push error:", err)
		return
	}
	queue.Close()

	dst := make([]int, 2)
	count, err := queue.Receive(context.Background(), dst)
	if err != nil {
		fmt.Println("receive error:", err)
		return
	}
	fmt.Println("accepted:", accepted)
	fmt.Println("received:", dst[:count])
	// Output:
	// accepted: 2
	// received: [1 2]
}

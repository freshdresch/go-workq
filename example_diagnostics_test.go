package workq_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/freshdresch/go-data-structures/workq"
)

func ExampleWithDiagnostics() {
	queue, err := workq.New[int](
		8,
		workq.WithDiagnostics(workq.DiagnosticsConfig{
			Mode:                workq.DiagnosticsTiming,
			CapacitySampleEvery: 128,
		}),
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer queue.Close()

	settings := queue.Diagnostics().Config
	fmt.Println("shards:", settings.Shards)
	fmt.Println("timing sample denominator:", settings.TimingSampleEvery)
	fmt.Println("capacity sample denominator:", settings.CapacitySampleEvery)
	// Output:
	// shards: 16
	// timing sample denominator: 64
	// capacity sample denominator: 128
}

func ExampleQueue_Diagnostics() {
	queue, err := workq.New[int](
		4,
		workq.WithDiagnostics(workq.DiagnosticsConfig{
			Mode: workq.DiagnosticsCounters,
		}),
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	before := queue.Diagnostics()
	accepted, pushErr := queue.PushBatch(context.Background(), []int{1, 2, 3, 4})
	// The only producer has returned before closure. The entire finite input fits.
	queue.Close()
	if pushErr != nil {
		fmt.Println("accepted:", accepted, "push error:", pushErr)
		return
	}

	var processedTotal int
	var consumeErr error
	var consumers sync.WaitGroup
	consumers.Go(func() {
		consumeErr = queue.Consume(
			context.Background(),
			func(_ context.Context, batch []int) error {
				for _, item := range batch {
					// This application defines updating its total as successful processing.
					processedTotal += item
				}
				return nil
			},
		)
	})
	consumers.Wait()
	after := queue.Diagnostics()
	if consumeErr != nil {
		fmt.Println("consume error:", consumeErr)
		return
	}

	// Both snapshots belong to this queue, and their calls do not overlap.
	// Each delta must span fewer than 2^64 events or sum units. Extrema stay lifetime values.
	if before.SchemaVersion != after.SchemaVersion {
		fmt.Println("incompatible diagnostic schemas")
		return
	}
	var movedBuckets [workq.SizeBucketCount]uint64
	for i := range movedBuckets {
		movedBuckets[i] = after.Producer.MovedSize.Buckets[i] - before.Producer.MovedSize.Buckets[i]
	}
	movedItems := after.Producer.MovedSize.Sum - before.Producer.MovedSize.Sum
	acquiredItems := after.Consumer.MovedSize.Sum - before.Consumer.MovedSize.Sum
	fmt.Println("four-item enqueue attempts:", movedBuckets[4])
	fmt.Println("enqueued items:", movedItems)
	fmt.Println("acquired items:", acquiredItems)
	fmt.Println("application total:", processedTotal)
	// Output:
	// four-item enqueue attempts: 1
	// enqueued items: 4
	// acquired items: 4
	// application total: 10
}

package durable_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/freshdresch/go-data-structures/workq/durable"
)

func ExampleQueue() {
	if err := submitAndAcknowledge(); err != nil {
		fmt.Println(err)
	}
	// Output:
	// applied
	// acknowledged
}

func submitAndAcknowledge() (err error) {
	directory, err := os.MkdirTemp("", "durable-example-")
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, os.RemoveAll(directory))
	}()
	policy := durable.DurablePolicy{
		Profile: "sqlite-wal-v1", MaxPayload: 16 << 10, MaxCodec: 64,
		MaxPendingItems: 4096, MaxPendingPayload: 16 << 20,
		MaxReceipts: 16384, MaxProducers: 256, DatabaseLimit: 1 << 30,
	}
	runtime, err := durable.DefaultRuntime(policy)
	if err != nil {
		return err
	}
	ctx := context.Background()
	queue, err := durable.Open(ctx, directory, durable.Config{Policy: policy, Runtime: runtime})
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, queue.Close(ctx))
	}()
	entry := durable.Entry{
		ID:    durable.ID{Producer: [16]byte{'s', 'o', 'u', 'r', 'c', 'e'}, Sequence: 42},
		Codec: "text", Payload: []byte("hello"),
	}
	result, err := queue.SubmitBatch(ctx, []durable.Entry{entry})
	if err != nil {
		// Unknown requires reconciliation with the same ID, never a newly generated ID.
		return fmt.Errorf("submit (%s): %w", result.Decision, err)
	}
	fmt.Println(result.Decision)
	deliveries, err := queue.Receive(ctx, 1)
	if err != nil {
		return err
	}
	// This read-only validation is the example's entire effect. Real destinations need
	// stable-ID idempotency or transactional participation before success can be acknowledged.
	if string(deliveries[0].Payload) != "hello" {
		return fmt.Errorf("unexpected payload")
	}
	result, err = queue.AckBatch(ctx, []durable.Token{deliveries[0].Token})
	if err != nil {
		return fmt.Errorf("ack (%s): %w", result.Decision, err)
	}
	statuses, err := queue.Lookup(ctx, []durable.ID{entry.ID})
	if err != nil {
		return err
	}
	fmt.Println(statuses[0].Kind)
	return nil
}

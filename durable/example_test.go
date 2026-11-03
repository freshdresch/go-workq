package durable_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/freshdresch/go-workq/durable"
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

func ExampleResize() {
	if err := resizeAndReopen(); err != nil {
		fmt.Println(err)
	}
	// Output: sqlite-wal-v2 10000
}

func resizeAndReopen() (err error) {
	directory, err := os.MkdirTemp("", "durable-resize-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(directory)) }()
	ctx := context.Background()
	policy := durable.DurablePolicy{
		Profile: "sqlite-wal-v1", MaxPayload: 16 << 10, MaxCodec: 64,
		MaxPendingItems: 4096, MaxPendingPayload: 16 << 20,
		MaxReceipts: 16384, MaxProducers: 256, DatabaseLimit: 1 << 30,
	}
	runtime, err := durable.DefaultRuntime(policy)
	if err != nil {
		return err
	}
	queue, err := durable.Open(ctx, directory, durable.Config{Policy: policy, Runtime: runtime})
	if err != nil {
		return err
	}
	// Real applications stop and join their workers before closing and resizing.
	if err := queue.Close(ctx); err != nil {
		return err
	}
	inspection, err := durable.Inspect(ctx, directory, nil)
	if err != nil {
		return err
	}
	next := inspection.Store.Policy
	next.Profile = "sqlite-wal-v2"
	next.MaxPendingItems = 10000
	next.MaxPendingPayload = 64 << 20
	next.MaxReceipts = 20000
	next.MaxProducers = 512
	next.MaxPayload = 64 << 10
	next.MaxCodec = 256
	next.DatabaseLimit = 2 << 30
	request := durable.ResizeRequest{
		UUID: inspection.Store.UUID, Expected: inspection.Store.Policy, Next: next,
	}
	result, resizeErr := durable.Resize(ctx, directory, request)
	if result.Decision == durable.Unknown {
		// Keep the store quiescent. An Inspect error is never evidence of absence.
		observed, err := durable.Inspect(ctx, directory, nil)
		if err != nil {
			return errors.Join(resizeErr, err)
		}
		if observed.Store.UUID != request.UUID {
			return errors.Join(resizeErr, durable.ErrConfigMismatch)
		}
		switch observed.Store.Policy {
		case request.Next:
			// Inspect established the desired policy after synchronization.
			resizeErr = nil
		case request.Expected:
			// The identical request is safe to retry. This example retries only once.
			result, resizeErr = durable.Resize(ctx, directory, request)
			if result.Decision != durable.Applied {
				return fmt.Errorf("resize retry (%s): %w", result.Decision, resizeErr)
			}
		default:
			return errors.Join(resizeErr, durable.ErrConfigMismatch)
		}
	}
	if resizeErr != nil {
		return resizeErr
	}
	runtime, err = durable.DefaultRuntime(next)
	if err != nil {
		return err
	}
	// WALRefusal is per-session policy. Zero also selects this historical default.
	runtime.SQLite.WALRefusal = 64 << 20
	queue, err = durable.Open(ctx, directory, durable.Config{Policy: next, Runtime: runtime})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, queue.Close(ctx)) }()
	fmt.Println(queue.Snapshot().Store.Policy.Profile, queue.Snapshot().Store.Policy.MaxPendingItems)
	return nil
}

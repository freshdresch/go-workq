package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/freshdresch/go-data-structures/workq/durable"
)

const maxAttempts = 3

var errInterrupted = errors.New(
	"stopped after destination commit before Ack: restart with the same paths",
)

type observations struct {
	attempts         int
	postSuccesses    int
	lookupRecoveries int
	acknowledged     int
}

func submitSource(ctx context.Context, queue *durable.Queue) error {
	entries := sourceEntries()
	for range maxAttempts {
		result, err := queue.SubmitBatch(ctx, entries)
		if result.Decision == durable.Applied {
			return err
		}
		if result.Decision != durable.Unknown {
			return fmt.Errorf("submit source (%s), reconcile original IDs: %w", result.Decision, err)
		}
		if err := queueHealth(queue); err != nil {
			return err
		}
		statuses, err := queue.Lookup(ctx, sourceIDs())
		if err != nil {
			return fmt.Errorf("resolve submission, retain original IDs: %w", err)
		}
		entries = nil
		originals := sourceEntries()
		for index, status := range statuses {
			switch status.Kind {
			case durable.Pending, durable.Acknowledged:
			case durable.Absent:
				entries = append(entries, originals[index])
			default:
				return fmt.Errorf(
					"source sequence %d is %s: reconciliation required",
					status.ID.Sequence, status.Kind,
				)
			}
		}
		if len(entries) == 0 {
			return nil
		}
	}
	return errors.New("submission reconciliation exhausted: retain original IDs and restart")
}

func queueHealth(queue *durable.Queue) error {
	select {
	case <-queue.Stopping():
		return errors.New("queue stopped: close, inspect, and restart before reconciliation")
	default:
		return nil
	}
}

// work returns observations even on failure so diagnostics do not imply completion.
func work(
	ctx, ackParent context.Context,
	queue *durable.Queue,
	client *adjustmentClient,
	output io.Writer,
	stopAfterCommit bool,
) (observations, error) {
	var observed observations
	for {
		if err := ctx.Err(); err != nil {
			return observed, err
		}
		if err := queueHealth(queue); err != nil {
			return observed, err
		}
		deliveries, err := queue.Receive(ctx, 1)
		if errors.Is(err, durable.ErrDrained) {
			return observed, nil
		}
		if err != nil {
			return observed, err
		}
		delivery := deliveries[0]
		observed.attempts++
		result, err := client.apply(ctx, effectRequest(delivery))
		if err != nil {
			return observed, fmt.Errorf(
				"apply source sequence %d, leave pending: %w",
				delivery.ID.Sequence, err,
			)
		}
		if result.Duplicate {
			observed.lookupRecoveries++
		} else {
			observed.postSuccesses++
		}
		if stopAfterCommit {
			return observed, fmt.Errorf(
				"key=%s producer=%x sequence=%d destination=succeeded queue=pending: %w",
				result.AdjustmentID, delivery.ID.Producer, delivery.ID.Sequence, errInterrupted,
			)
		}
		ackCtx, cancel := context.WithTimeout(ackParent, operationTimeout)
		err = acknowledge(ackCtx, queue, delivery)
		cancel()
		if err != nil {
			return observed, fmt.Errorf(
				"destination key %s succeeded, queue unresolved: %w",
				result.AdjustmentID, err,
			)
		}
		observed.acknowledged++
		_, err = fmt.Fprintf(
			output,
			"key=%s producer=%x sequence=%d destination=succeeded lookup_recovery=%t queue=acknowledged\n",
			result.AdjustmentID, delivery.ID.Producer, delivery.ID.Sequence, result.Duplicate,
		)
		if err != nil {
			return observed, err
		}
	}
}

func acknowledge(ctx context.Context, queue *durable.Queue, delivery durable.Delivery) error {
	for range maxAttempts {
		result, err := queue.AckBatch(ctx, []durable.Token{delivery.Token})
		if result.Decision == durable.Applied {
			return err
		}
		if result.Decision != durable.Unknown {
			return fmt.Errorf(
				"acknowledge (%s), restart to receive a current token: %w",
				result.Decision, err,
			)
		}
		if err := queueHealth(queue); err != nil {
			return err
		}
		statuses, err := queue.Lookup(ctx, []durable.ID{delivery.ID})
		if err != nil {
			return fmt.Errorf("resolve Ack: %w", err)
		}
		switch statuses[0].Kind {
		case durable.Acknowledged:
			return nil
		case durable.Pending:
		default:
			return fmt.Errorf("resolve Ack: unexpected %s, reconciliation required", statuses[0].Kind)
		}
	}
	return errors.New("acknowledgment reconciliation exhausted: leave pending and restart")
}

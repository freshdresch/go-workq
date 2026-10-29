package workq_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq"
)

func TestOverloadRejectSuffix(t *testing.T) {
	queue, err := workq.New[int](2)
	require.NoError(t, err)
	defer queue.Close()
	items := []int{1, 2, 3}
	count, pushErr := queue.TryPushBatch(items)
	accepted := slices.Clone(items[:count])
	unaccepted := slices.Clone(items[count:])
	require.ErrorIs(t, pushErr, workq.ErrFull)
	require.Equal(t, []int{1, 2}, accepted)
	require.Equal(t, []int{3}, unaccepted)
	queue.Close()
	var completed []int
	err = queue.Consume(context.Background(), func(_ context.Context, batch []int) error {
		completed = append(completed, batch...)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, accepted, completed)
	require.Equal(t, items, append(completed, unaccepted...))
}

func TestOverloadClosureIsNotFull(t *testing.T) {
	queue, err := workq.New[int](2)
	require.NoError(t, err)
	queue.Close()
	items := []int{1, 2, 3}
	count, pushErr := queue.TryPushBatch(items)
	accepted := slices.Clone(items[:count])
	unaccepted := slices.Clone(items[count:])
	var rejected []int
	if errors.Is(pushErr, workq.ErrFull) {
		rejected = unaccepted
	}
	require.ErrorIs(t, pushErr, workq.ErrClosed)
	require.Empty(t, accepted)
	require.Equal(t, items, unaccepted)
	require.Empty(t, rejected)
}

func TestOverloadWaitBudget(t *testing.T) {
	for _, outcome := range []string{"deadline", "room", "parent canceled"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue, err := workq.New[int](
					2,
					workq.WithMaxReceiveBatch(1),
					workq.WithProducerWait(workq.WaitStrategy{}),
				)
				require.NoError(t, err)
				abortCtx, cancelAbort := context.WithCancel(context.Background())
				submitCtx, cancelSubmit := context.WithTimeout(abortCtx, time.Second)
				count, err := queue.TryPushBatch([]int{1, 2})
				require.NoError(t, err)
				require.Equal(t, 2, count)
				entered, done := make(chan struct{}), make(chan struct{})
				var pushErr error
				go func() {
					defer close(done)
					close(entered)
					pushErr = queue.Push(submitCtx, 3)
				}()
				defer func() {
					cancelSubmit()
					cancelAbort()
					<-done
					queue.Close()
				}()
				<-entered
				synctest.Wait()
				requirePending(t, done)
				require.NoError(t, submitCtx.Err())

				var completed []int
				switch outcome {
				case "deadline":
					<-submitCtx.Done()
				case "room":
					batch := make([]int, 1)
					count, err := queue.Receive(abortCtx, batch)
					require.NoError(t, err)
					require.Equal(t, 1, count)
					completed = append(completed, batch[:count]...)
					synctest.Wait()
					require.NoError(t, submitCtx.Err())
				case "parent canceled":
					cancelAbort()
				}
				<-done
				accepted, unaccepted := []int{1, 2}, []int{3}
				switch outcome {
				case "deadline":
					require.ErrorIs(t, pushErr, context.DeadlineExceeded)
					require.NoError(t, abortCtx.Err())
				case "room":
					require.NoError(t, pushErr)
					require.NoError(t, submitCtx.Err())
					accepted = append(accepted, 3)
					unaccepted = nil
				case "parent canceled":
					require.ErrorIs(t, pushErr, context.Canceled)
					require.ErrorIs(t, abortCtx.Err(), context.Canceled)
				}
				queue.Close()
				err = queue.Consume(context.Background(), func(_ context.Context, batch []int) error {
					completed = append(completed, batch...)
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, accepted, completed)
				require.Equal(t, []int{1, 2, 3}, append(completed, unaccepted...))
			})
		})
	}
}

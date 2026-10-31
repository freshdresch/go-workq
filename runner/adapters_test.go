package runner

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestItemsOrdinaryFailures(t *testing.T) {
	failure := errors.New("item failure")
	var calls []int
	handler := Items(func(_ context.Context, item int) error {
		calls = append(calls, item)
		if item == 2 {
			return failure
		}
		if item == 3 {
			return context.Canceled
		}
		return nil
	})
	results := handler(context.Background(), []int{1, 2, 3, 4})
	assert.Equal(t, []int{1, 2, 3, 4}, calls)
	require.NoError(t, validateResults(results, 4, false))
	assert.Equal(t, []Result{
		{}, {State: ResultFailed, Err: failure},
		{State: ResultFailed, Err: context.Canceled}, {},
	}, results)
}

func TestItemsCancellationBetweenItems(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls []int
	handler := Items(func(_ context.Context, item int) error {
		calls = append(calls, item)
		cancel()
		return nil
	})
	results := handler(ctx, []int{1, 2, 3})
	assert.Equal(t, []int{1}, calls)
	assert.Equal(t, []Result{
		{}, {State: ResultUnattempted}, {State: ResultUnattempted},
	}, results)
	require.NoError(t, validateResults(results, 3, true))
	calls = nil
	results = handler(ctx, []int{4, 5})
	assert.Empty(t, calls)
	assert.Equal(t, []Result{
		{State: ResultUnattempted}, {State: ResultUnattempted},
	}, results)
	assert.Empty(t, handler(ctx, nil))
}

func TestWholeBatch(t *testing.T) {
	failure := errors.New("uncertain batch")
	items := []int{1, 2, 3}
	for _, handlerErr := range []error{nil, failure, context.DeadlineExceeded} {
		calls := 0
		handler := WholeBatch(func(ctx context.Context, batch []int) error {
			calls++
			assert.Equal(t, context.Background(), ctx)
			assert.Equal(t, items, batch)
			return handlerErr
		})
		results := handler(context.Background(), items)
		assert.Equal(t, 1, calls)
		require.NoError(t, validateResults(results, len(items), false))
		if handlerErr == nil {
			assert.Nil(t, results)
			continue
		}
		for _, result := range results {
			assert.Equal(t, ResultFailed, result.State)
			assert.ErrorIs(t, result.Err, handlerErr)
		}
	}
}

func TestNilAdaptersPreserveConfigValidation(t *testing.T) {
	assert.Nil(t, Items[int](nil))
	assert.Nil(t, WholeBatch[int](nil))
}

func TestPeerCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	liveMarker := PeerCanceled(ctx)
	require.Error(t, validateResults([]Result{liveMarker}, 1, false))
	cancel()
	require.Error(t, validateResults([]Result{liveMarker}, 1, true))
	marker := PeerCanceled(ctx)
	require.NoError(t, validateResults([]Result{marker}, 1, true))
	assert.Equal(t, ResultInterrupted, marker.State)
	assert.ErrorIs(t, marker.Err, context.Canceled)
	require.Error(t, validateResults([]Result{
		{State: ResultInterrupted, Err: context.Canceled},
	}, 1, true))
}

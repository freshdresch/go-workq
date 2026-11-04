//go:build linux && amd64 && cgo

package inventory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type applyOutcome struct {
	result Result
	err    error
}

func TestStoreConcurrentConnections(t *testing.T) {
	for _, test := range []struct {
		name   string
		second Request
	}{
		{name: "same key and bytes", second: testRequest(1, 3)},
		{name: "same key different bytes", second: testRequest(1, 2)},
		{name: "different key same bytes", second: testRequest(2, 3)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := inventoryPath(t)
			first := openInventory(t, path, 2)
			second := openInventory(t, path, 2)
			require.NotSame(t, first.conn, second.conn)
			requests := []Request{testRequest(1, 3), test.second}
			outcomes := concurrentApply(t, []*Store{first, second}, requests)
			switch test.name {
			case "same key and bytes":
				for _, outcome := range outcomes {
					require.NoError(t, outcome.err)
					assert.Equal(t, int64(13), outcome.result.Quantity)
				}
				assert.NotEqual(t, outcomes[0].result.Duplicate, outcomes[1].result.Duplicate)
				outcomes[0].result.Duplicate = false
				outcomes[1].result.Duplicate = false
				assert.Equal(t, outcomes[0].result, outcomes[1].result)
				assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, requests[0])
			case "same key different bytes":
				winner := 0
				if outcomes[0].err != nil {
					winner = 1
				}
				require.NoError(t, outcomes[winner].err)
				assert.False(t, outcomes[winner].result.Duplicate)
				assert.ErrorIs(t, outcomes[1-winner].err, ErrConflict)
				assert.Zero(t, outcomes[1-winner].result)
				quantity := int64(13 - winner)
				assert.Equal(t, quantity, outcomes[winner].result.Quantity)
				assertDatabase(t, path,
					Inventory{Quantity: quantity, Adjustments: 1}, requests[winner])
			case "different key same bytes":
				var quantities []int64
				for _, outcome := range outcomes {
					require.NoError(t, outcome.err)
					assert.False(t, outcome.result.Duplicate)
					quantities = append(quantities, outcome.result.Quantity)
				}
				assert.ElementsMatch(t, []int64{13, 16}, quantities)
				assertDatabase(t, path, Inventory{Quantity: 16, Adjustments: 2}, requests...)
			}
			require.NoError(t, first.Close())
			require.NoError(t, second.Close())
			reopened := openInventory(t, path, 2)
			for i, request := range requests {
				result, err := reopened.Apply(t.Context(), request)
				if outcomes[i].err != nil {
					assert.ErrorIs(t, err, ErrConflict)
					continue
				}
				require.NoError(t, err)
				assert.True(t, result.Duplicate)
				outcomes[i].result.Duplicate = true
				assert.Equal(t, outcomes[i].result, result)
			}
		})
	}
}

func concurrentApply(t *testing.T, stores []*Store, requests []Request) []applyOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	ready := make(chan struct{})
	results := make([]chan applyOutcome, len(stores))
	for i, store := range stores {
		results[i] = make(chan applyOutcome, 1)
		go func() {
			select {
			case ready <- struct{}{}:
			case <-ctx.Done():
				results[i] <- applyOutcome{err: ctx.Err()}
				return
			}
			select {
			case <-start:
				result, err := store.Apply(ctx, requests[i])
				results[i] <- applyOutcome{result: result, err: err}
			case <-ctx.Done():
				results[i] <- applyOutcome{err: ctx.Err()}
			}
		}()
	}
	joined := make([]bool, len(stores))
	defer func() {
		cancel()
		for i, result := range results {
			if joined[i] {
				continue
			}
			select {
			case <-result:
			case <-time.After(5 * time.Second):
				t.Error("concurrent Apply did not join")
			}
		}
	}()
	for range stores {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("concurrent Apply did not reach start gate")
		}
	}
	close(start)
	outcomes := make([]applyOutcome, len(stores))
	for i, result := range results {
		select {
		case outcomes[i] = <-result:
			joined[i] = true
		case <-ctx.Done():
			t.Fatal("concurrent Apply did not finish")
		}
	}
	return outcomes
}

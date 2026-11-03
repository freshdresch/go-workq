package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq/durable"
)

func TestFixedQueuePolicy(t *testing.T) {
	t.Parallel()
	policy := durable.DurablePolicy{
		Profile: "sqlite-wal-v1", MaxPayload: 1024, MaxCodec: 64,
		MaxPendingItems: 16, MaxPendingPayload: 16384,
		MaxReceipts: 64, MaxProducers: 4, DatabaseLimit: 64 << 20,
	}
	runtime, err := durable.DefaultRuntime(policy)
	require.NoError(t, err)
	config := durable.Config{Policy: policy, Durability: durable.Synchronized, Runtime: runtime}
	queue, err := durable.Open(t.Context(), filepath.Join(t.TempDir(), "queue"), config)
	if errors.Is(err, durable.ErrUnsupported) {
		t.Skipf("queue storage profile unsupported: %v", err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, queue.Close(context.Background())) })
}

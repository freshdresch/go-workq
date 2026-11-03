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
	config, err := exampleConfig()
	require.NoError(t, err)
	queue, err := durable.Open(t.Context(), filepath.Join(t.TempDir(), "queue"), config)
	if errors.Is(err, durable.ErrUnsupported) {
		t.Skipf("queue storage profile unsupported: %v", err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, queue.Close(context.Background())) })
}

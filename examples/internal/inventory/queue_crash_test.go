//go:build linux && amd64 && cgo

package inventory

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq/durable"
)

func TestQueuePrecommitCrashRecovery(t *testing.T) {
	for _, stage := range []applyStage{provisionalReceiptInserted, inventoryUpdated} {
		t.Run(string(stage), func(t *testing.T) {
			t.Parallel()
			path := inventoryPath(t)
			directory := filepath.Join(filepath.Dir(path), "queue")
			store := openInventory(t, path, 2)
			require.NoError(t, store.Close())
			queue := openInventoryQueue(t, directory)
			request := testRequest(1, 3)
			entry := durable.Entry{
				ID:    durable.ID{Producer: request.Key.Producer, Sequence: request.Key.Sequence},
				Codec: request.Codec, Payload: request.Payload,
			}
			submitted, err := queue.SubmitBatch(t.Context(), []durable.Entry{entry})
			require.NoError(t, err)
			require.Equal(t, durable.Applied, submitted.Decision)
			old := receiveInventoryDelivery(t, queue)
			require.NoError(t, queue.Close(t.Context()))

			killQueueInventoryChild(t, directory, path, stage)

			assertDatabase(t, path, Inventory{Quantity: 10})
			inspection, err := durable.Inspect(t.Context(), directory, []durable.ID{entry.ID})
			require.NoError(t, err)
			require.Len(t, inspection.Statuses, 1)
			assert.Equal(t, durable.Pending, inspection.Statuses[0].Kind)
			assert.Equal(t, durable.Count(1), inspection.Store.Counts.PendingItems)
			queue = openInventoryQueue(t, directory)
			stale, err := queue.AckBatch(t.Context(), []durable.Token{old.Token})
			require.ErrorIs(t, err, durable.ErrStaleToken)
			assert.Equal(t, durable.NotApplied, stale.Decision)
			fresh := receiveInventoryDelivery(t, queue)
			assert.NotEqual(t, old.Token, fresh.Token)
			retry := inventoryDeliveryRequest(fresh)
			require.Equal(t, request, retry)
			reopened := openInventory(t, path, 2)
			result, err := reopened.Apply(t.Context(), retry)
			require.NoError(t, err)
			assert.False(t, result.Duplicate)
			assert.Equal(t, int64(13), result.Quantity)
			encoded, err := request.Key.Encode()
			require.NoError(t, err)
			assert.Equal(t, encoded, result.AdjustmentID)
			acknowledged, err := queue.AckBatch(t.Context(), []durable.Token{fresh.Token})
			require.NoError(t, err)
			assert.Equal(t, durable.Applied, acknowledged.Decision)
			statuses, err := queue.Lookup(t.Context(), []durable.ID{entry.ID})
			require.NoError(t, err)
			require.Len(t, statuses, 1)
			assert.Equal(t, durable.Acknowledged, statuses[0].Kind)
			assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, request)
		})
	}
}

func openInventoryQueue(t *testing.T, directory string) *durable.Queue {
	t.Helper()
	policy := durable.DurablePolicy{
		Profile: "sqlite-wal-v1", MaxPayload: 1024, MaxCodec: 64,
		MaxPendingItems: 16, MaxPendingPayload: 16384,
		MaxReceipts: 64, MaxProducers: 4, DatabaseLimit: 64 << 20,
	}
	runtime, err := durable.DefaultRuntime(policy)
	require.NoError(t, err)
	config := durable.Config{
		Policy: policy, Durability: durable.Synchronized, Runtime: runtime,
	}
	queue, err := durable.Open(t.Context(), directory, config)
	if errors.Is(err, durable.ErrUnsupported) {
		t.Skipf("queue storage profile unsupported: %v", err)
	}
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		assert.NoError(t, queue.Close(ctx))
	})
	return queue
}

func receiveInventoryDelivery(t *testing.T, queue *durable.Queue) durable.Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	deliveries, err := queue.Receive(ctx, 1)
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	return deliveries[0]
}

func inventoryDeliveryRequest(delivery durable.Delivery) Request {
	return Request{
		Key: Key{Namespace: "test", Producer: delivery.ID.Producer,
			Sequence: delivery.ID.Sequence, Effect: "inventory.adjust"},
		Codec: delivery.Codec, Payload: delivery.Payload,
	}
}

func TestQueueInventoryCrashChild(t *testing.T) {
	if os.Getenv("QUEUE_INVENTORY_CRASH_CHILD") != "1" {
		return
	}
	stage := applyStage(os.Getenv("QUEUE_INVENTORY_CRASH_STAGE"))
	require.Contains(t, []applyStage{provisionalReceiptInserted, inventoryUpdated}, stage)
	stagePipe := os.NewFile(3, "queue-inventory-stage")
	releasePipe := os.NewFile(4, "queue-inventory-release")
	require.NotNil(t, stagePipe)
	require.NotNil(t, releasePipe)
	t.Cleanup(func() {
		assert.NoError(t, stagePipe.Close())
		assert.NoError(t, releasePipe.Close())
	})
	queue := openInventoryQueue(t, os.Getenv("QUEUE_INVENTORY_CRASH_DIRECTORY"))
	current := receiveInventoryDelivery(t, queue)
	store := openInventory(t, os.Getenv("QUEUE_INVENTORY_CRASH_PATH"), 2)
	store.stageHook = func(reached applyStage) {
		if reached != stage {
			return
		}
		_, err := fmt.Fprintln(stagePipe, reached)
		require.NoError(t, err)
		var release [1]byte
		_, err = io.ReadFull(releasePipe, release[:])
		require.NoError(t, err)
		t.Fatal("parent released crash point instead of killing child")
	}
	_, err := store.Apply(t.Context(), inventoryDeliveryRequest(current))
	require.NoError(t, err)
	t.Fatal("Apply returned without stopping at the selected crash point")
}

func killQueueInventoryChild(t *testing.T, directory, path string, stage applyStage) {
	t.Helper()
	stageReader, stageWriter, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = stageReader.Close() }) // Cleanup also covers a failed Start.
	t.Cleanup(func() { _ = stageWriter.Close() })
	releaseReader, releaseWriter, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = releaseReader.Close() })
	t.Cleanup(func() { _ = releaseWriter.Close() })

	executable, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, executable, "-test.run=^TestQueueInventoryCrashChild$")
	command.Env = append(os.Environ(),
		"QUEUE_INVENTORY_CRASH_CHILD=1", "QUEUE_INVENTORY_CRASH_PATH="+path,
		"QUEUE_INVENTORY_CRASH_STAGE="+string(stage),
		"QUEUE_INVENTORY_CRASH_DIRECTORY="+directory,
	)
	command.ExtraFiles = []*os.File{stageWriter, releaseReader}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	require.NoError(t, command.Start())
	joined := false
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() {
		if joined {
			return
		}
		// Kill may race with timeout or an early child exit. Wait still owns reaping.
		_ = command.Process.Kill()
		select {
		case err := <-done:
			var exitErr *exec.ExitError
			if err != nil && !errors.As(err, &exitErr) {
				t.Errorf("wait for inventory child during cleanup: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("inventory child did not exit during cleanup")
		}
	})
	require.NoError(t, stageWriter.Close())
	require.NoError(t, releaseReader.Close())
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.NoError(t, stageReader.SetReadDeadline(deadline))
	record, err := bufio.NewReader(stageReader).ReadString('\n')
	require.NoError(t, err, "child did not report stage")
	require.Equal(t, string(stage)+"\n", record)
	require.NoError(t, command.Process.Kill())
	select {
	case err := <-done:
		joined = true
		var exitErr *exec.ExitError
		require.True(t, errors.As(err, &exitErr), "child exit: %v\n%s", err, output.String())
		assert.False(t, exitErr.Success())
	case <-ctx.Done():
		t.Fatal("inventory child did not exit after kill")
	}
}

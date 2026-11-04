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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreCrashRecovery(t *testing.T) {
	for _, stage := range []applyStage{
		provisionalReceiptInserted, inventoryUpdated, destinationCommitSucceeded,
	} {
		t.Run(string(stage), func(t *testing.T) {
			t.Parallel()
			path := inventoryPath(t)
			store := openInventory(t, path, 2)
			require.NoError(t, store.Close())
			killInventoryChild(t, path, stage)

			request := testRequest(1, 3)
			committed := stage == destinationCommitSucceeded
			if committed {
				assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, request)
			} else {
				assertDatabase(t, path, Inventory{Quantity: 10})
			}
			reopened := openInventory(t, path, 2)
			result, err := reopened.Apply(t.Context(), request)
			require.NoError(t, err)
			assert.Equal(t, committed, result.Duplicate)
			assert.Equal(t, int64(13), result.Quantity)
			encoded, err := request.Key.Encode()
			require.NoError(t, err)
			assert.Equal(t, encoded, result.AdjustmentID)
			assertDatabase(t, path, Inventory{Quantity: 13, Adjustments: 1}, request)
		})
	}
}

func TestInventoryCrashChild(t *testing.T) {
	if os.Getenv("INVENTORY_CRASH_CHILD") != "1" {
		return
	}
	stage := applyStage(os.Getenv("INVENTORY_CRASH_STAGE"))
	require.Contains(t, []applyStage{
		provisionalReceiptInserted, inventoryUpdated, destinationCommitSucceeded,
	}, stage)
	stagePipe := os.NewFile(3, "inventory-stage")
	releasePipe := os.NewFile(4, "inventory-release")
	require.NotNil(t, stagePipe)
	require.NotNil(t, releasePipe)
	t.Cleanup(func() {
		assert.NoError(t, stagePipe.Close())
		assert.NoError(t, releasePipe.Close())
	})
	store := openInventory(t, os.Getenv("INVENTORY_CRASH_PATH"), 2)
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
	_, err := store.Apply(t.Context(), testRequest(1, 3))
	require.NoError(t, err)
	t.Fatal("Apply returned without stopping at the selected crash point")
}

func killInventoryChild(t *testing.T, path string, stage applyStage) {
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
	command := exec.CommandContext(ctx, executable, "-test.run=^TestInventoryCrashChild$")
	command.Env = append(os.Environ(),
		"INVENTORY_CRASH_CHILD=1", "INVENTORY_CRASH_PATH="+path,
		"INVENTORY_CRASH_STAGE="+string(stage),
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

//go:build linux && amd64 && cgo

package main

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

	"github.com/freshdresch/go-workq/durable"
)

func TestWorkerCrashAfterDestinationCommit(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	directory := filepath.Join(root, "queue")
	destination := filepath.Join(root, "inventory.sqlite")
	queue := openRecoveryQueue(t, directory)
	entry := sourceEntries()[0]
	submitRecoveryEntry(t, queue, entry)
	old := receiveRecoveryEntry(t, queue)
	require.NoError(t, queue.Close(t.Context()))
	killTransactionalChild(t, directory, destination)
	request := effectRequest(old)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
	inspection, err := durable.Inspect(t.Context(), directory, []durable.ID{entry.ID})
	require.NoError(t, err)
	assert.Equal(t, durable.Pending, inspection.Statuses[0].Kind)
	queue = openRecoveryQueue(t, directory)
	assertStaleRecoveryToken(t, queue, old.Token)
	fresh := receiveRecoveryEntry(t, queue)
	assert.NotEqual(t, old.Token, fresh.Token)
	store := openRecoveryStore(t, destination)
	result, err := store.Apply(t.Context(), effectRequest(fresh))
	require.NoError(t, err)
	assert.True(t, result.Duplicate)
	assert.Equal(t, int64(13), result.Quantity)
	encoded, err := request.Key.Encode()
	require.NoError(t, err)
	assert.Equal(t, encoded, result.AdjustmentID)
	require.NoError(t, acknowledge(t.Context(), queue, fresh))
	assertQueueStatus(t, queue, entry.ID, durable.Acknowledged)
	assertRecoveryDestination(t, destination, 13, expectedReceipt{request, 13})
}

func TestTransactionalCrashChild(t *testing.T) {
	if os.Getenv("TRANSACTIONAL_CRASH_CHILD") != "1" {
		return
	}
	stagePipe := os.NewFile(3, "transactional-stage")
	releasePipe := os.NewFile(4, "transactional-release")
	require.NotNil(t, stagePipe)
	require.NotNil(t, releasePipe)
	t.Cleanup(func() {
		assert.NoError(t, stagePipe.Close())
		assert.NoError(t, releasePipe.Close())
	})
	queue := openRecoveryQueue(t, os.Getenv("TRANSACTIONAL_CRASH_QUEUE"))
	store := openRecoveryStore(t, os.Getenv("TRANSACTIONAL_CRASH_PATH"))
	afterApply := func() {
		_, err := fmt.Fprintln(stagePipe, "destination-committed")
		require.NoError(t, err)
		var release [1]byte
		_, err = io.ReadFull(releasePipe, release[:])
		require.NoError(t, err)
		t.Fatal("parent released crash point instead of killing child")
	}
	_, err := work(t.Context(), t.Context(), queue, store, io.Discard, false, afterApply)
	require.NoError(t, err)
	t.Fatal("worker returned without stopping before Ack")
}

func killTransactionalChild(t *testing.T, directory, path string) {
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
	command := exec.CommandContext(ctx, executable, "-test.run=^TestTransactionalCrashChild$")
	command.Env = append(os.Environ(),
		"TRANSACTIONAL_CRASH_CHILD=1", "TRANSACTIONAL_CRASH_PATH="+path,
		"TRANSACTIONAL_CRASH_QUEUE="+directory,
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
				t.Errorf("wait for transactional child during cleanup: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("transactional child did not exit during cleanup")
		}
	})
	require.NoError(t, stageWriter.Close())
	require.NoError(t, releaseReader.Close())
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.NoError(t, stageReader.SetReadDeadline(deadline))
	record, err := bufio.NewReader(stageReader).ReadString('\n')
	require.NoError(t, err, "child did not report stage")
	require.Equal(t, "destination-committed\n", record)
	require.NoError(t, command.Process.Kill())
	select {
	case err := <-done:
		joined = true
		var exitErr *exec.ExitError
		require.True(t, errors.As(err, &exitErr), "child exit: %v\n%s", err, output.String())
		assert.False(t, exitErr.Success())
	case <-ctx.Done():
		t.Fatal("transactional child did not exit after kill")
	}
}

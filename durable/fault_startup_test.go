//go:build durable_faulttest && linux && amd64 && cgo

package durable

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq/durable/internal/sqlitefault"
)

type startupImageBoundary struct {
	Manifest sqlitefault.Manifest `json:"manifest"`
	UUID     UUID                 `json:"uuid"`
	Epoch    uint64               `json:"epoch"`
}

// Refused startup completes cleanup. The successful boundary interrupts live storage.
func TestFaultStartupNamespace(t *testing.T) {
	if mode := os.Getenv("WORKQ_STARTUP_IMAGE"); mode != "" {
		if path := os.Getenv("WORKQ_STARTUP_RECOVER"); path != "" {
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			var boundary startupImageBoundary
			require.NoError(t, json.Unmarshal(data, &boundary))
			checkStartupImage(t, mode, boundary)
			return
		}
		startupImageChild(t, mode)
		return
	}
	for _, mode := range []string{"before-directory", "before-parent", "after-parent"} {
		t.Run(mode, func(t *testing.T) { runStartupImage(t, mode) })
	}
}

func runStartupImage(t *testing.T, mode string) {
	t.Helper()
	parent, err := os.MkdirTemp("", "startup-fixture-")
	require.NoError(t, err)
	scratch, err := os.MkdirTemp("", "startup-evidence-")
	require.NoError(t, err)
	root := filepath.Join(parent, "queue")
	require.NoError(t, os.Mkdir(root, 0700))
	t.Logf("fixture %s evidence %s", root, scratch)
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		assert.NoError(t, os.RemoveAll(parent))
		assert.NoError(t, os.RemoveAll(scratch))
	})
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()
	defer func() { _ = writer.Close() }() // Also close on failed Start.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultStartupNamespace$")
	command.Env = append(os.Environ(), "WORKQ_STARTUP_IMAGE="+mode,
		"WORKQ_STARTUP_ROOT="+root, "WORKQ_STARTUP_SCRATCH="+scratch)
	command.ExtraFiles = []*os.File{writer}
	log, err := os.Create(filepath.Join(scratch, "child.log"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, log.Close()) }()
	command.Stdout, command.Stderr = log, log
	require.NoError(t, command.Start())
	joined := false
	defer func() {
		if !joined {
			_ = command.Process.Kill() // Join before fixture cleanup after a failed assertion.
			_ = command.Wait()
		}
	}()
	require.NoError(t, writer.Close())
	require.NoError(t, reader.SetReadDeadline(time.Now().Add(25*time.Second)))
	var boundary startupImageBoundary
	require.NoError(t, json.NewDecoder(reader).Decode(&boundary), "child log: %s", log.Name())
	require.NoError(t, command.Process.Kill())
	err = command.Wait()
	joined = true
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	status, ok := exit.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.Equal(t, syscall.SIGKILL, status.Signal())
	data, err := json.Marshal(boundary)
	require.NoError(t, err)
	boundaryPath := filepath.Join(scratch, "boundary.json")
	require.NoError(t, os.WriteFile(boundaryPath, data, 0600))
	checkStartupManifest(t, mode, boundary.Manifest)
	_, err = sqlitefault.Restore(boundary.Manifest)
	require.NoError(t, err)
	recovery := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultStartupNamespace$")
	recovery.Env = append(os.Environ(), "WORKQ_STARTUP_IMAGE="+mode,
		"WORKQ_STARTUP_RECOVER="+boundaryPath)
	output, err := recovery.CombinedOutput()
	require.NoError(t, os.WriteFile(filepath.Join(scratch, "recovery.log"), output, 0600))
	require.NoError(t, err, "%s", output)
}

func startupImageChild(t *testing.T, mode string) {
	root := os.Getenv("WORKQ_STARTUP_ROOT")
	scratch := filepath.Join(os.Getenv("WORKQ_STARTUP_SCRATCH"), "images")
	require.NoError(t, os.Mkdir(scratch, 0700))
	_, controller, err := sqlitefault.Attach(root, sqlitefault.Limits{Events: 8192, Scratch: scratch})
	require.NoError(t, err)
	faultController.Store(controller)
	if mode == "before-directory" {
		parent, err := os.Open(filepath.Dir(root))
		require.NoError(t, err)
		require.NoError(t, syncOwnedDescriptor(parent, filepath.Dir(root)))
		require.NoError(t, parent.Close())
	}
	var boundary startupImageBoundary
	observer := observeStartup(func(stage startupStage) {
		if stage == startupAfterEpoch {
			// This bounded reader closes while startup still owns its live connection.
			snapshot := readCrashLogicalSnapshot(t, root)
			require.False(t, t.Failed(), "logical snapshot must complete before Freeze")
			require.Len(t, snapshot.metadata[metaUUID], len(boundary.UUID))
			copy(boundary.UUID[:], snapshot.metadata[metaUUID])
			require.Len(t, snapshot.metadata[metaEpoch], 8)
			boundary.Epoch = binary.BigEndian.Uint64(snapshot.metadata[metaEpoch])
			require.Equal(t, uint64(1), boundary.Epoch)
			require.Empty(t, snapshot.receipts)
			require.Empty(t, snapshot.payloads)
			require.Empty(t, snapshot.producers)
			if mode != "after-parent" {
				role := "directory"
				if mode == "before-parent" {
					role = "parent"
				}
				require.NoError(t, controller.Arm("owned-sync", role, 1, "before", sqlitefault.SyncError))
			}
		}
		if stage == startupAfterParentSync {
			require.Equal(t, "after-parent", mode)
			freezeStartupImage(t, controller, boundary)
		}
	})
	started, err := startStoreObserved(t.Context(), root, testConfig(t), observer)
	require.ErrorIs(t, err, ErrStorage)
	require.Nil(t, started)
	require.False(t, controller.Armed())
	require.NoError(t, controller.Err())
	freezeStartupImage(t, controller, boundary)
}

func freezeStartupImage(
	t *testing.T,
	controller *sqlitefault.Controller,
	boundary startupImageBoundary,
) {
	t.Helper()
	require.False(t, t.Failed(), "startup prerequisites must pass before Freeze")
	manifest, err := controller.Freeze()
	require.NoError(t, err)
	boundary.Manifest = manifest
	pipe := os.NewFile(3, "freeze")
	require.NoError(t, json.NewEncoder(pipe).Encode(boundary))
	for {
		_ = syscall.Pause()
	} // Parent owns SIGKILL and join.
}

func checkStartupManifest(t *testing.T, mode string, manifest sqlitefault.Manifest) {
	t.Helper()
	successful := make(map[string]int)
	failed := make(map[string]int)
	for _, event := range manifest.Events {
		if event.Operation != "owned-sync" || event.Phase != "after" {
			continue
		}
		require.Equal(t, event.Expected, event.Actual)
		require.NotZero(t, event.Actual)
		if event.Injected {
			require.False(t, event.Performed)
			require.NotZero(t, event.Result)
			failed[event.Role]++
			continue
		}
		require.True(t, event.Performed)
		require.Zero(t, event.ActualResult)
		require.Zero(t, event.Result)
		successful[event.Role]++
	}
	require.Equal(t, 1, successful["main"])
	switch mode {
	case "before-directory":
		require.Equal(t, map[string]int{"directory": 1}, failed)
		require.Zero(t, successful["directory"])
		require.Equal(t, 1, successful["parent"])
		require.True(t, manifest.DirectoryDurable)
		require.NotContains(t, manifest.Synchronized, "queue.sqlite3")
		require.NotContains(t, manifest.Synchronized, "queue.sqlite3-wal")
	case "before-parent":
		require.Equal(t, map[string]int{"parent": 1}, failed)
		require.Equal(t, 1, successful["directory"])
		require.Zero(t, successful["parent"])
		require.False(t, manifest.DirectoryDurable)
	case "after-parent":
		require.Empty(t, failed)
		require.Equal(t, 1, successful["directory"])
		require.Equal(t, 1, successful["parent"])
		require.True(t, manifest.DirectoryDurable)
	}
	if mode != "before-directory" {
		require.Contains(t, manifest.Synchronized, "queue.sqlite3")
		require.Contains(t, manifest.Synchronized, "queue.sqlite3-wal")
	}
}

func checkStartupImage(t *testing.T, mode string, boundary startupImageBoundary) {
	t.Helper()
	root := boundary.Manifest.Root
	_, statErr := os.Stat(root)
	if mode == "before-parent" {
		require.ErrorIs(t, statErr, os.ErrNotExist)
	} else {
		require.NoError(t, statErr)
	}
	if mode == "before-directory" {
		for _, name := range []string{"queue.sqlite3", "queue.sqlite3-wal"} {
			_, err := os.Stat(filepath.Join(root, name))
			require.ErrorIs(t, err, os.ErrNotExist)
		}
	}
	inspection, err := Inspect(t.Context(), root, nil)
	if mode != "after-parent" {
		require.Error(t, err)
		assert.Equal(t, Inspection{}, inspection)
		return
	}
	require.NoError(t, err)
	assert.NotZero(t, boundary.UUID)
	assert.Equal(t, boundary.UUID, inspection.Store.UUID)
	assert.Equal(t, boundary.Epoch, inspection.Store.Epoch)
	assert.Equal(t, testConfig(t).Policy, inspection.Store.Policy)
	assert.Equal(t, testConfig(t).Durability, inspection.Store.Durability)
	assert.Equal(t, StoreCounts{}, inspection.Store.Counts)
	assert.Empty(t, inspection.Statuses)
}

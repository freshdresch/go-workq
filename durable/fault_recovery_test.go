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

type recoveryImageBoundary struct {
	Manifest  sqlitefault.Manifest `json:"manifest"`
	Metadata  map[int][]byte       `json:"metadata"`
	Producers []string             `json:"producers"`
	Receipts  []string             `json:"receipts"`
	Payloads  []string             `json:"payloads"`
}

// The copied seed models valid visible bytes without modeled persistence. The loss
// case restores once before upgrading, so the upgrade cannot recover the lost batch.
func TestFaultRecoveryImages(t *testing.T) {
	if mode := os.Getenv("WORKQ_RECOVERY_IMAGE"); mode != "" {
		data, err := os.ReadFile(os.Getenv("WORKQ_RECOVERY_BOUNDARY"))
		require.NoError(t, err)
		var boundary recoveryImageBoundary
		require.NoError(t, json.Unmarshal(data, &boundary))
		if mode == "check" {
			checkRecoveryImage(t, boundary)
			return
		}
		recoveryImageChild(t, mode, boundary)
		return
	}
	for _, mode := range []string{"inspect-copy", "loss"} {
		t.Run(mode, func(t *testing.T) { runRecoveryImage(t, mode) })
	}
}

func runRecoveryImage(t *testing.T, mode string) {
	t.Helper()
	parent, err := os.MkdirTemp("", "recovery-fixture-")
	require.NoError(t, err)
	scratch, err := os.MkdirTemp("", "recovery-evidence-")
	require.NoError(t, err)
	t.Logf("fixture %s evidence %s", parent, scratch)
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		assert.NoError(t, os.RemoveAll(parent))
		assert.NoError(t, os.RemoveAll(scratch))
	})
	root := filepath.Join(parent, "queue")
	seed := root
	if mode == "inspect-copy" {
		seed = filepath.Join(parent, "seed")
		require.NoError(t, os.Mkdir(root, 0700))
	}
	require.NoError(t, os.Mkdir(seed, 0700))
	config := testConfig(t)
	config.Durability = ProcessCrash
	queue, err := Open(t.Context(), seed, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		queue.Abort(ErrStopped)
		// Abort supplies a terminal cause. Cleanup only needs to join the owner.
		_ = queue.Wait()
	})
	result, err := queue.SubmitBatch(t.Context(), recoverySurvivors())
	require.NoError(t, err)
	require.Equal(t, Applied, result.Decision)
	require.NoError(t, queue.Close(t.Context()))
	snapshot := readCrashLogicalSnapshot(t, seed)
	require.False(t, t.Failed(), "seed must be valid before starting recording")
	boundary := recoveryImageBoundary{
		Manifest: sqlitefault.Manifest{Root: root}, Metadata: snapshot.metadata,
		Producers: snapshot.producers, Receipts: snapshot.receipts, Payloads: snapshot.payloads,
	}
	boundary = recordRecoveryImage(t, mode, scratch, boundary)
	if mode == "loss" {
		boundary = recordRecoveryImage(t, "upgrade", scratch, boundary)
	}
	boundaryPath := filepath.Join(scratch, "final.json")
	writeRecoveryBoundary(t, boundaryPath, boundary)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultRecoveryImages$")
	command.Env = append(os.Environ(), "WORKQ_RECOVERY_IMAGE=check",
		"WORKQ_RECOVERY_BOUNDARY="+boundaryPath)
	output, err := command.CombinedOutput()
	require.NoError(t, os.WriteFile(filepath.Join(scratch, "recovery.log"), output, 0600))
	require.NoError(t, err, "%s", output)
}

func writeRecoveryBoundary(t *testing.T, path string, boundary recoveryImageBoundary) {
	t.Helper()
	data, err := json.Marshal(boundary)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
}

func recordRecoveryImage(
	t *testing.T,
	mode, scratch string,
	boundary recoveryImageBoundary,
) recoveryImageBoundary {
	t.Helper()
	phase := filepath.Join(scratch, mode)
	require.NoError(t, os.Mkdir(phase, 0700))
	boundaryPath := filepath.Join(phase, "input.json")
	writeRecoveryBoundary(t, boundaryPath, boundary)
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()
	defer func() { _ = writer.Close() }() // Also closes on failed Start.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultRecoveryImages$")
	command.Env = append(os.Environ(), "WORKQ_RECOVERY_IMAGE="+mode,
		"WORKQ_RECOVERY_BOUNDARY="+boundaryPath, "WORKQ_RECOVERY_SCRATCH="+phase)
	command.ExtraFiles = []*os.File{writer}
	log, err := os.Create(filepath.Join(phase, "child.log"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, log.Close()) }()
	command.Stdout, command.Stderr = log, log
	require.NoError(t, command.Start())
	joined := false
	defer func() {
		if !joined {
			_ = command.Process.Kill() // Join before parent-owned fixture cleanup.
			_ = command.Wait()
		}
	}()
	require.NoError(t, writer.Close())
	require.NoError(t, reader.SetReadDeadline(time.Now().Add(25*time.Second)))
	require.NoError(t, json.NewDecoder(reader).Decode(&boundary), "child log: %s", log.Name())
	require.NoError(t, command.Process.Kill())
	err = command.Wait()
	joined = true
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	status, ok := exit.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.Equal(t, syscall.SIGKILL, status.Signal())
	writeRecoveryBoundary(t, filepath.Join(phase, "boundary.json"), boundary)
	require.True(t, boundary.Manifest.DirectoryDurable)
	require.Contains(t, boundary.Manifest.Synchronized, "queue.sqlite3")
	if mode == "inspect-copy" {
		var mainSynced bool
		for _, event := range boundary.Manifest.Events {
			if event.Operation != "owned-sync" || event.Role != "main" || event.Phase != "after" {
				continue
			}
			require.True(t, event.Performed)
			require.Zero(t, event.ActualResult)
			require.Equal(t, event.Expected, event.Actual)
			mainSynced = true
		}
		require.True(t, mainSynced, "Inspect must perform the explicit main-file barrier")
	}
	if mode == "loss" {
		var write, sync uint64
		for _, event := range boundary.Manifest.Events {
			if event.Role != "wal" || event.Phase != "after" || !event.Performed {
				continue
			}
			require.Zero(t, event.ActualResult)
			if event.Operation == "native-write" {
				write = event.Sequence
			}
			if event.Operation == "native-sync" {
				sync = event.Sequence
			}
		}
		require.NotZero(t, sync)
		require.Greater(t, write, sync, "lost batch must follow the final WAL synchronization")
	}
	_, err = sqlitefault.Restore(boundary.Manifest)
	require.NoError(t, err)
	return boundary
}

func recoveryImageChild(t *testing.T, mode string, boundary recoveryImageBoundary) {
	t.Helper()
	root := boundary.Manifest.Root
	if mode == "upgrade" {
		// Loss has already happened. Inspect observes survivors without advancing epoch.
		checkRecoveryImage(t, boundary)
		require.False(t, t.Failed(), "recovered survivors must match before upgrade")
	}
	scratch := filepath.Join(os.Getenv("WORKQ_RECOVERY_SCRATCH"), "images")
	require.NoError(t, os.Mkdir(scratch, 0700))
	_, controller, err := sqlitefault.Attach(root, sqlitefault.Limits{
		Events: 8192, Scratch: scratch,
	})
	require.NoError(t, err)
	faultController.Store(controller)
	if mode == "inspect-copy" {
		seed := filepath.Join(filepath.Dir(root), "seed", "queue.sqlite3")
		data, err := os.ReadFile(seed)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(root, "queue.sqlite3"), data, 0600))
		require.NoError(t, os.WriteFile(filepath.Join(root, "LOCK"), nil, 0600))
	} else {
		syncRecoveryPath(t, filepath.Join(root, "queue.sqlite3"))
	}
	// File sync cannot establish either the child names or the root entry.
	syncRecoveryPath(t, root)
	syncRecoveryPath(t, filepath.Dir(root))
	switch mode {
	case "inspect-copy":
		for _, event := range controller.Events() {
			require.NotEqual(t, "main", event.Role, "copied bytes must not have a prior sync")
		}
		_, err := os.Stat(filepath.Join(root, "queue.sqlite3-wal"))
		require.ErrorIs(t, err, os.ErrNotExist)
		inspectRecoveryImage(t, boundary)
	case "loss":
		config := testConfig(t)
		config.Durability = ProcessCrash
		config.Runtime.Group.Commands = 1
		config.Runtime.Group.MaxDelay = 0
		queue, err := Open(t.Context(), root, config)
		require.NoError(t, err)
		prior := binary.BigEndian.Uint64(boundary.Metadata[metaEpoch])
		require.Equal(t, prior+1, queue.Snapshot().Store.Epoch)
		epoch := encodeUint64(prior + 1)
		boundary.Metadata[metaEpoch] = epoch[:]
		submitRecoveryLoss(t, queue)
	case "upgrade":
		result, err := ChangeDurability(t.Context(), root, ProcessCrash, Synchronized)
		require.NoError(t, err)
		require.Equal(t, Applied, result.Decision)
		boundary.Metadata[metaDurability] = []byte{byte(Synchronized)}
	}
	require.False(t, t.Failed(), "all prerequisites must pass before Freeze")
	boundary.Manifest, err = controller.Freeze()
	require.NoError(t, err)
	pipe := os.NewFile(3, "freeze")
	require.NoError(t, json.NewEncoder(pipe).Encode(boundary))
	for {
		_ = syscall.Pause()
	} // The parent kills and joins before Restore. Live queues never close here.
}

func syncRecoveryPath(t *testing.T, path string) {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	require.NoError(t, syncOwnedDescriptor(file, path))
	require.NoError(t, file.Close())
}

func recoverySurvivors() []Entry {
	entries := crashEntries(41)
	return []Entry{entries[1], entries[0]}
}

func inspectRecoveryImage(t *testing.T, expected recoveryImageBoundary) {
	t.Helper()
	root := expected.Manifest.Root
	entries := crashEntries(41)
	ids := make([]ID, len(entries))
	for index, entry := range entries {
		ids[index] = entry.ID
	}
	inspection, err := Inspect(t.Context(), root, ids)
	require.NoError(t, err)
	require.Equal(t, expected.Metadata[metaUUID], inspection.Store.UUID[:])
	require.Equal(t, testConfig(t).Policy, inspection.Store.Policy)
	require.Equal(t, expected.Metadata[metaDurability], []byte{byte(inspection.Store.Durability)})
	require.Equal(t, binary.BigEndian.Uint64(expected.Metadata[metaEpoch]), inspection.Store.Epoch)
	require.Equal(t, StoreCounts{
		PendingItems: 2, PendingPayload: 32768, Receipts: 2, Producers: 2,
	}, inspection.Store.Counts)
	require.Len(t, inspection.Statuses, len(ids))
	for index, id := range ids {
		kind := Pending
		if index >= 2 {
			kind = Absent
		}
		require.Equal(t, Status{ID: id, Kind: kind}, inspection.Statuses[index])
	}
}

func checkRecoveryImage(t *testing.T, expected recoveryImageBoundary) {
	t.Helper()
	inspectRecoveryImage(t, expected)
	root := expected.Manifest.Root
	checkImageAcceptanceOrder(t, root, recoverySurvivors())
	actual := readCrashLogicalSnapshot(t, root)
	require.Equal(t, expected.Metadata, actual.metadata)
	require.Equal(t, expected.Producers, actual.producers)
	require.Equal(t, expected.Receipts, actual.receipts)
	require.Equal(t, expected.Payloads, actual.payloads)
}

func submitRecoveryLoss(t *testing.T, queue *Queue) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	gateDone := make(chan error, 1)
	go func() {
		_, err := queue.submitBatchObserved(t.Context(), nil,
			executionGate{entered: entered, release: release})
		gateDone <- err
	}()
	<-entered
	queue.mu.Lock()
	target := queue.lastSequence
	queue.mu.Unlock()
	type outcome struct {
		result CommitResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := queue.SubmitBatch(t.Context(), crashEntries(41)[2:])
		done <- outcome{result: result, err: err}
	}()
	waitQueueSequence(t, queue, target+1)
	parked := make(chan struct{})
	go func() {
		_, _ = queue.submitBatchObserved(t.Context(), nil,
			executionGate{entered: parked, release: make(chan struct{})})
	}() // The recording child owns this parked goroutine until SIGKILL.
	waitQueueSequence(t, queue, target+2)
	close(release)
	require.NoError(t, <-gateDone)
	result := <-done
	require.NoError(t, result.err)
	require.Equal(t, Applied, result.result.Decision)
	<-parked
	require.Equal(t, Count(4), queue.Snapshot().Store.Counts.PendingItems)
}

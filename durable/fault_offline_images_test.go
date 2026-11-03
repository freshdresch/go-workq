//go:build durable_faulttest && linux && amd64 && cgo

package durable

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq/durable/internal/sqlitefault"
)

func TestFaultOfflineImages(t *testing.T) {
	if mode := os.Getenv("WORKQ_OFFLINE_IMAGE"); mode != "" {
		data, err := os.ReadFile(os.Getenv("WORKQ_OFFLINE_BOUNDARY"))
		require.NoError(t, err)
		var boundary recoveryImageBoundary
		require.NoError(t, json.Unmarshal(data, &boundary))
		if strings.HasPrefix(mode, "check-") {
			checkOfflineImage(t, strings.TrimPrefix(mode, "check-"), boundary)
			return
		}
		offlineImageChild(t, mode, boundary)
		return
	}
	for _, mode := range []string{
		"main", "directory", "identity", "cancel-before-commit", "cancel-after-commit", "commit-sync",
	} {
		t.Run(mode, func(t *testing.T) { runOfflineImage(t, mode) })
	}
}

func runOfflineImage(t *testing.T, mode string) {
	t.Helper()
	parent, err := os.MkdirTemp("", "offline-fixture-")
	require.NoError(t, err)
	scratch, err := os.MkdirTemp("", "offline-evidence-")
	require.NoError(t, err)
	t.Logf("fixture %s evidence %s", parent, scratch)
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		assert.NoError(t, os.RemoveAll(parent))
		assert.NoError(t, os.RemoveAll(scratch))
	})
	root, seed := filepath.Join(parent, "queue"), filepath.Join(parent, "seed")
	require.NoError(t, os.Mkdir(root, 0700))
	require.NoError(t, os.Mkdir(seed, 0700))
	config := testConfig(t)
	config.Durability = ProcessCrash
	queue, err := Open(t.Context(), seed, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		queue.Abort(ErrStopped)
		// A failed setup assertion already reports the error. Always join the seed.
		_ = queue.Wait()
	})
	result, err := queue.SubmitBatch(t.Context(), recoverySurvivors())
	require.NoError(t, err)
	require.Equal(t, Applied, result.Decision)
	require.NoError(t, queue.Close(t.Context()))
	snapshot := readCrashLogicalSnapshot(t, seed)
	require.False(t, t.Failed(), "seed must be valid before recording")
	boundary := recoveryImageBoundary{
		Manifest: sqlitefault.Manifest{Root: root}, Metadata: snapshot.metadata,
		Producers: snapshot.producers, Receipts: snapshot.receipts, Payloads: snapshot.payloads,
	}
	boundaryPath := filepath.Join(scratch, "boundary.json")
	writeRecoveryBoundary(t, boundaryPath, boundary)
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()
	defer func() { _ = writer.Close() }() // Also closes if Start fails.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultOfflineImages$")
	command.Env = append(os.Environ(), "WORKQ_OFFLINE_IMAGE="+mode,
		"WORKQ_OFFLINE_BOUNDARY="+boundaryPath, "WORKQ_OFFLINE_SCRATCH="+scratch)
	command.ExtraFiles = []*os.File{writer}
	log, err := os.Create(filepath.Join(scratch, "child.log"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, log.Close()) }()
	command.Stdout, command.Stderr = log, log
	require.NoError(t, command.Start())
	joined := false
	defer func() {
		if !joined {
			_ = command.Process.Kill() // Join before parent fixture cleanup.
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
	writeRecoveryBoundary(t, boundaryPath, boundary)
	checkOfflineManifest(t, mode, boundary.Manifest)
	_, err = sqlitefault.Restore(boundary.Manifest)
	require.NoError(t, err)
	recovery := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultOfflineImages$")
	recovery.Env = append(os.Environ(), "WORKQ_OFFLINE_IMAGE=check-"+mode,
		"WORKQ_OFFLINE_BOUNDARY="+boundaryPath)
	output, err := recovery.CombinedOutput()
	require.NoError(t, os.WriteFile(filepath.Join(scratch, "recovery.log"), output, 0600))
	require.NoError(t, err, "%s", output)
}

func offlineImageChild(t *testing.T, mode string, boundary recoveryImageBoundary) {
	t.Helper()
	root := boundary.Manifest.Root
	scratch := filepath.Join(os.Getenv("WORKQ_OFFLINE_SCRATCH"), "images")
	require.NoError(t, os.Mkdir(scratch, 0700))
	_, controller, err := sqlitefault.Attach(root, sqlitefault.Limits{
		Events: 8192, Scratch: scratch,
	})
	require.NoError(t, err)
	faultController.Store(controller)
	require.NoError(t, os.WriteFile(filepath.Join(root, "LOCK"), nil, 0600))
	// Preserve LOCK before introducing the unsynchronized main-file name.
	syncRecoveryPath(t, root)
	syncRecoveryPath(t, filepath.Dir(root))
	data, err := os.ReadFile(filepath.Join(filepath.Dir(root), "seed", "queue.sqlite3"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "queue.sqlite3"), data, 0600))
	if mode != "directory" {
		syncRecoveryPath(t, root)
	}
	if strings.HasPrefix(mode, "cancel-") || mode == "commit-sync" {
		syncRecoveryPath(t, filepath.Join(root, "queue.sqlite3"))
		changePolicyImage(t, mode, boundary, controller)
	} else {
		observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
			if stage != "before-barrier" {
				return nil
			}
			size, err := opened.offlineWALBytes()
			require.NoError(t, err)
			require.Zero(t, size)
			if mode != "identity" {
				return controller.Arm("owned-sync", mode, 1, "before", sqlitefault.SyncError)
			}
			replacement, err := os.Create(filepath.Join(filepath.Dir(root), "other"))
			require.NoError(t, err)
			require.NoError(t, opened.main.Close())
			opened.main = replacement
			return nil
		}}
		inspection, err := inspectObserved(t.Context(), root, []ID{{Sequence: 1}}, observer)
		require.ErrorIs(t, err, ErrStorage)
		require.Equal(t, Inspection{}, inspection)
	}
	// Offline calls have completed their own cleanup before this boundary.
	require.NoError(t, controller.Err())
	boundary.Manifest, err = controller.Freeze()
	require.NoError(t, err)
	pipe := os.NewFile(3, "freeze")
	require.NoError(t, json.NewEncoder(pipe).Encode(boundary))
	for {
		_ = syscall.Pause()
	} // Parent kills and joins before Restore.
}

func changePolicyImage(
	t *testing.T,
	mode string,
	boundary recoveryImageBoundary,
	controller *sqlitefault.Controller,
) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	cause := errors.New("policy commit cancellation")
	var observed, committed bool
	var commitStart int
	var commitEvents []sqlitefault.Event
	observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
		if stage == "before-commit" {
			size, err := opened.offlineWALBytes()
			require.NoError(t, err)
			require.Zero(t, size, "policy transaction starts with an empty WAL")
			commitStart = len(controller.Events())
		}
		if stage == "after-commit" {
			committed = true
			commitEvents = controller.Events()[commitStart:]
		}
		if "cancel-"+stage == mode {
			cancel(cause)
			observed = true
		}
		if mode == "commit-sync" && stage == "before-commit" {
			observed = true
			return controller.Arm("native-sync", "wal", 1, "before", sqlitefault.SyncError)
		}
		return nil
	}}
	result, err := changeDurabilityObserved(
		ctx, boundary.Manifest.Root, ProcessCrash, Synchronized, observer,
	)
	require.True(t, observed)
	if mode == "commit-sync" {
		require.ErrorIs(t, err, ErrStorage)
		require.Equal(t, Unknown, result.Decision)
		require.False(t, committed)
		return
	}
	require.ErrorIs(t, context.Cause(ctx), cause)
	require.NoError(t, err)
	require.Equal(t, Applied, result.Decision)
	require.True(t, committed)
	checkPolicyCommitSync(t, commitEvents)
	boundary.Metadata[metaDurability] = []byte{byte(Synchronized)}
}

// Only this transaction writes frames into the initially empty WAL. Capture its events
// before cleanup, whose checkpoint could otherwise conceal a NORMAL policy commit.
func checkPolicyCommitSync(t *testing.T, events []sqlitefault.Event) {
	t.Helper()
	var lastFrameWrite, lastSync uint64
	for _, event := range events {
		if event.Role != "wal" || event.Phase != "after" {
			continue
		}
		if event.Operation != "native-write" && event.Operation != "native-sync" {
			continue
		}
		require.True(t, event.Performed)
		require.Zero(t, event.ActualResult)
		require.Zero(t, event.Result)
		if event.Operation == "native-write" && event.Offset+event.Length > 32 {
			lastFrameWrite = event.Sequence
		}
		if event.Operation == "native-sync" {
			lastSync = event.Sequence
		}
	}
	require.Positive(t, lastFrameWrite, "policy frames must be written during COMMIT")
	require.Greater(t, lastSync, lastFrameWrite,
		"policy frames must be synchronized before after-commit, without cleanup")
}

func checkPolicyHeaderSyncFailure(t *testing.T, manifest sqlitefault.Manifest) {
	t.Helper()
	var headerWrite uint64
	var syncs int
	for _, event := range manifest.Events {
		if event.Role != "wal" || event.Phase != "after" {
			continue
		}
		switch event.Operation {
		case "native-write":
			require.Zero(t, headerWrite, "only the WAL header may be written")
			require.Zero(t, event.Offset)
			require.EqualValues(t, 32, event.Length)
			require.True(t, event.Performed)
			require.Zero(t, event.ActualResult)
			require.Zero(t, event.Result)
			headerWrite = event.Sequence
		case "native-sync":
			syncs++
			require.Positive(t, headerWrite)
			require.Greater(t, event.Sequence, headerWrite)
			require.True(t, event.Injected)
			require.False(t, event.Performed)
			require.Equal(t, 1034, event.Result) // SQLITE_IOERR_FSYNC.
		}
	}
	require.Equal(t, 1, syncs, "the first WAL sync must fail before header synchronization")
	var walImages int
	for _, image := range manifest.Images {
		if image.Role == "wal" {
			walImages++
			require.EqualValues(t, 32, image.VisibleLength)
			require.Zero(t, image.SynchronizedLength)
		}
	}
	require.Equal(t, 1, walImages)
}

func checkOfflineManifest(t *testing.T, mode string, manifest sqlitefault.Manifest) {
	t.Helper()
	require.True(t, manifest.DirectoryDurable)
	if mode == "commit-sync" {
		checkPolicyHeaderSyncFailure(t, manifest)
	}
	var refused, ownedMain, nativeMain bool
	for _, event := range manifest.Events {
		if event.Phase != "after" {
			continue
		}
		operation, role := "owned-sync", mode
		if mode == "commit-sync" {
			operation, role = "native-sync", "wal"
		}
		if event.Operation == operation && event.Role == role && event.Result != 0 {
			require.False(t, event.Performed)
			refused = true
		}
		if event.Role != "main" || !event.Performed || event.ActualResult != 0 {
			continue
		}
		if event.Operation == "owned-sync" {
			ownedMain = true
		}
		if event.Operation == "native-sync" {
			nativeMain = true
		}
	}
	switch mode {
	case "main", "directory", "commit-sync":
		require.True(t, refused, "armed sync failure must be consumed and completed")
	case "identity":
		for _, event := range manifest.Events {
			require.False(t, event.Operation == "owned-sync" && event.Role == "main")
		}
	}
	if mode == "directory" {
		require.True(t, ownedMain)
		require.NotContains(t, manifest.Synchronized, "queue.sqlite3")
		require.Contains(t, manifest.Visible, "queue.sqlite3")
	}
	if mode == "main" || mode == "identity" {
		require.False(t, ownedMain)
		var mainImages int
		for _, image := range manifest.Images {
			if image.Role != "main" {
				continue
			}
			mainImages++
			require.Positive(t, image.VisibleLength)
			if nativeMain {
				t.Log("native recovery/cleanup sync promoted main bytes before Freeze")
				require.Positive(t, image.SynchronizedLength)
			} else {
				require.Zero(t, image.SynchronizedLength)
				info, err := os.Stat(image.Synchronized)
				require.NoError(t, err)
				require.Zero(t, info.Size())
			}
		}
		require.Positive(t, mainImages)
	}
}

func checkOfflineImage(t *testing.T, mode string, boundary recoveryImageBoundary) {
	t.Helper()
	if mode == "directory" {
		_, err := os.Stat(filepath.Join(boundary.Manifest.Root, "queue.sqlite3"))
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	if mode == "main" || mode == "identity" || mode == "directory" {
		var synchronizedMain bool
		for _, image := range boundary.Manifest.Images {
			if image.Role == "main" && image.SynchronizedLength > 0 {
				synchronizedMain = true
			}
		}
		if !synchronizedMain || mode == "directory" {
			inspection, err := Inspect(t.Context(), boundary.Manifest.Root, nil)
			expected := ErrUnsupported
			if mode == "directory" {
				expected = ErrStorage
			}
			require.ErrorIs(t, err, expected)
			require.Equal(t, Inspection{}, inspection)
			return
		}
	}
	if mode == "commit-sync" {
		inspection, err := Inspect(t.Context(), boundary.Manifest.Root, nil)
		require.NoError(t, err)
		require.Equal(t, ProcessCrash, inspection.Store.Durability)
		require.Equal(t, []byte{byte(ProcessCrash)}, boundary.Metadata[metaDurability])
	}
	checkRecoveryImage(t, boundary)
}

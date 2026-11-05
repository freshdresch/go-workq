//go:build durable_faulttest && linux && amd64 && cgo

package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-workq/durable/internal/sqlitefault"
)

type resizeImageBoundary struct {
	Recovery   recoveryImageBoundary `json:"recovery"`
	Request    ResizeRequest         `json:"request"`
	Mode       string                `json:"mode"`
	Scratch    string                `json:"scratch"`
	Durability Durability            `json:"durability"`
}

func TestFaultResizeImages(t *testing.T) {
	if input := os.Getenv("WORKQ_RESIZE_IMAGE"); input != "" {
		data, err := os.ReadFile(input)
		require.NoError(t, err)
		var boundary resizeImageBoundary
		require.NoError(t, json.Unmarshal(data, &boundary))
		if os.Getenv("WORKQ_RESIZE_RECOVER") == "1" {
			checkResizeImage(t, boundary)
			return
		}
		resizeImageChild(t, boundary)
		return
	}
	for _, transition := range []string{"v1", "v2", "migration"} {
		for _, durability := range []Durability{Synchronized, ProcessCrash} {
			for _, mode := range []string{
				"before-commit", "after-commit", "commit-header-sync", "final-sync",
				"cancel-before-commit", "cancel-after-commit", "cancel-during-commit",
				"commit-frame-sync-before", "commit-frame-sync-after",
			} {
				t.Run(fmt.Sprintf("%s/%s/%s", transition, durability, mode), func(t *testing.T) {
					runResizeImage(t, transition, durability, mode)
				})
			}
		}
	}
}

func runResizeImage(t *testing.T, transition string, durability Durability, mode string) {
	t.Helper()
	scratch, err := os.MkdirTemp("", "resize-evidence-")
	require.NoError(t, err)
	t.Logf("resize evidence: %s", scratch)
	t.Cleanup(func() {
		if !t.Failed() {
			assert.NoError(t, os.RemoveAll(scratch))
		}
	})
	parent, err := os.MkdirTemp("", "resize-fixture-")
	require.NoError(t, err)
	t.Logf("resize fixture: %s", parent)
	t.Cleanup(func() {
		if !t.Failed() {
			assert.NoError(t, os.RemoveAll(parent))
		}
	})
	root := filepath.Join(parent, "queue")
	require.NoError(t, os.Mkdir(root, 0700))
	seed := migrationFixture(t, resizeFaultConfig(t, transition, durability), 3, 128)
	request := resizeFaultRequest(t, seed, transition)
	snapshot := readCrashLogicalSnapshot(t, seed)
	for _, name := range []string{"LOCK", "queue.sqlite3"} {
		data, err := os.ReadFile(filepath.Join(seed, name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(root, name), data, 0600))
	}
	boundary := resizeImageBoundary{
		Recovery: recoveryImageBoundary{
			Manifest: sqlitefault.Manifest{Root: root}, Metadata: snapshot.metadata,
			Producers: snapshot.producers, Receipts: snapshot.receipts, Payloads: snapshot.payloads,
		},
		Request: request, Mode: mode, Scratch: scratch, Durability: durability,
	}
	input := filepath.Join(scratch, "input.json")
	writeResizeImage(t, input, boundary)
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()
	writerClosed := false
	defer func() {
		if !writerClosed {
			assert.NoError(t, writer.Close())
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultResizeImages$")
	child.Env = append(os.Environ(), "WORKQ_RESIZE_IMAGE="+input)
	child.ExtraFiles = []*os.File{writer}
	log, err := os.Create(filepath.Join(scratch, "child.log"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, log.Close()) }()
	child.Stdout, child.Stderr = log, log
	require.NoError(t, child.Start())
	joined := false
	defer func() {
		if !joined {
			_ = child.Process.Kill() // Always join before fixture cleanup, including failed assertions.
			_ = child.Wait()
		}
	}()
	require.NoError(t, writer.Close())
	writerClosed = true
	require.NoError(t, reader.SetReadDeadline(time.Now().Add(25*time.Second)))
	require.NoError(t, json.NewDecoder(reader).Decode(&boundary), "child log: %s", log.Name())
	require.NoError(t, child.Process.Kill())
	err = child.Wait()
	joined = true
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	status, ok := exit.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.Equal(t, syscall.SIGKILL, status.Signal())
	outputPath := filepath.Join(scratch, "boundary.json")
	writeResizeImage(t, outputPath, boundary)
	require.True(t, boundary.Recovery.Manifest.DirectoryDurable)
	require.Contains(t, boundary.Recovery.Manifest.Synchronized, "queue.sqlite3")
	_, err = sqlitefault.Restore(boundary.Recovery.Manifest)
	require.NoError(t, err)
	recovery := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultResizeImages$")
	recovery.Env = append(os.Environ(), "WORKQ_RESIZE_IMAGE="+outputPath, "WORKQ_RESIZE_RECOVER=1")
	output, err := recovery.CombinedOutput()
	require.NoError(t, os.WriteFile(filepath.Join(scratch, "recovery.log"), output, 0600))
	require.NoError(t, err, "%s", output)
}

func writeResizeImage(t *testing.T, path string, boundary resizeImageBoundary) {
	t.Helper()
	data, err := json.MarshalIndent(boundary, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
}

func resizeImageChild(t *testing.T, boundary resizeImageBoundary) {
	t.Helper()
	root := boundary.Recovery.Manifest.Root
	images := filepath.Join(boundary.Scratch, "images")
	require.NoError(t, os.Mkdir(images, 0700))
	_, controller, err := sqlitefault.Attach(root, sqlitefault.Limits{Events: 8192, Scratch: images})
	require.NoError(t, err)
	faultController.Store(controller)
	// Establish the copied seed in the oracle before any transactional mutation.
	syncRecoveryPath(t, filepath.Join(root, "queue.sqlite3"))
	syncRecoveryPath(t, root)
	syncRecoveryPath(t, filepath.Dir(root))
	freeze := func() {
		require.NoError(t, controller.Err(), "evidence overflow must fail before publishing")
		require.False(t, controller.Armed())
		boundary.Recovery.Manifest, err = controller.Freeze()
		require.NoError(t, err)
		pipe := os.NewFile(3, "resize-freeze")
		require.NoError(t, json.NewEncoder(pipe).Encode(boundary))
		for {
			_ = syscall.Pause() // Parent kills and joins before restoring the captured image.
		}
	}
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	cause := errors.New("resize image commit cancellation")
	var commitCalls int
	commitStart := 0
	observed := false
	observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
		if stage == "before-commit" {
			checkResizeCommitStart(t, opened)
			commitStart = len(controller.Events())
		}
		if stage == "after-commit" {
			checkPolicyCommitSync(t, controller.Events()[commitStart:])
		}
		if stage == boundary.Mode {
			observed = true
			freeze()
		}
		if "cancel-"+stage == boundary.Mode {
			observed = true
			cancel(cause)
		}
		if stage == "before-commit" && boundary.Mode == "cancel-during-commit" {
			observed = true
			return installResizeCommitHook(opened, func() int {
				commitCalls++
				cancel(cause)
				return 0
			})
		}
		if stage == "before-commit" && resizeFrameSyncImage(boundary.Mode) {
			observed = true
			phase := "before"
			if boundary.Mode == "commit-frame-sync-after" {
				phase = "after"
			}
			return controller.Arm("native-sync", "wal", 2, phase, sqlitefault.SyncError)
		}
		if stage == "before-commit" && boundary.Mode == "commit-header-sync" {
			observed = true
			return controller.Arm("native-sync", "wal", 1, "before", sqlitefault.SyncError)
		}
		if stage == "before-final-barrier" && boundary.Mode == "final-sync" {
			observed = true
			return controller.Arm("owned-sync", "main", 1, "before", sqlitefault.SyncError)
		}
		return nil
	}}
	result, err := resizeObserved(ctx, root, boundary.Request, observer)
	require.True(t, observed)
	switch boundary.Mode {
	case "commit-header-sync", "final-sync", "commit-frame-sync-before", "commit-frame-sync-after":
		require.ErrorIs(t, err, ErrStorage)
		require.Equal(t, Unknown, result.Decision)
	case "cancel-before-commit":
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, NotApplied, result.Decision)
	case "cancel-after-commit", "cancel-during-commit":
		require.ErrorIs(t, context.Cause(ctx), cause)
		require.NoError(t, err)
		require.Equal(t, Applied, result.Decision)
	default:
		t.Fatal("selected crash boundary was not reached")
	}
	if boundary.Mode == "cancel-during-commit" {
		require.Equal(t, 1, commitCalls)
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	}
	if resizeFrameSyncImage(boundary.Mode) {
		phase := "before"
		if boundary.Mode == "commit-frame-sync-after" {
			phase = "after"
		}
		checkResizeFrameSyncFailure(t, controller.Events()[commitStart:], phase)
	}
	freeze()
}

func resizeFrameSyncImage(mode string) bool {
	return mode == "commit-frame-sync-before" || mode == "commit-frame-sync-after"
}

func checkResizeImage(t *testing.T, boundary resizeImageBoundary) {
	t.Helper()
	root := boundary.Recovery.Manifest.Root
	inspection, err := Inspect(t.Context(), root, nil)
	require.NoError(t, err, "a recovery error cannot establish an absent policy")
	expected := boundary.Request.Expected
	switch boundary.Mode {
	case "after-commit", "final-sync", "cancel-after-commit", "cancel-during-commit":
		expected = boundary.Request.Next
	}
	if resizeFrameSyncImage(boundary.Mode) {
		require.Contains(
			t,
			[]DurablePolicy{boundary.Request.Expected, boundary.Request.Next},
			inspection.Store.Policy,
		)
		expected = inspection.Store.Policy
	}
	require.Equal(t, expected, inspection.Store.Policy)
	require.Equal(t, boundary.Request.UUID, inspection.Store.UUID)
	require.Equal(t, boundary.Durability, inspection.Store.Durability)
	before := crashLogicalSnapshot{
		metadata: boundary.Recovery.Metadata, producers: boundary.Recovery.Producers,
		receipts: boundary.Recovery.Receipts, payloads: boundary.Recovery.Payloads,
	}
	checkResizeSnapshot(t, root, before, expected)
	result, err := Resize(t.Context(), root, boundary.Request)
	require.NoError(t, err)
	require.Equal(t, Applied, result.Decision)
	after, err := Inspect(t.Context(), root, nil)
	require.NoError(t, err)
	assert.Equal(t, boundary.Request.Next, after.Store.Policy)
	checkResizeSnapshot(t, root, before, boundary.Request.Next)
}

func checkResizeSnapshot(
	t *testing.T,
	root string,
	before crashLogicalSnapshot,
	expected DurablePolicy,
) {
	t.Helper()
	metadata := maps.Clone(before.metadata)
	policy := encodePolicy(expected)
	metadata[metaPolicy] = policy[:]
	version, err := schemaVersionForProfile(expected.Profile)
	require.NoError(t, err)
	schema := encodeUint64(version)
	metadata[metaSchema] = schema[:]
	metadata[metaProfile] = []byte(expected.Profile)
	actual := readCrashLogicalSnapshot(t, root)
	assert.Equal(t, metadata, actual.metadata)
	assert.Equal(t, before.producers, actual.producers)
	assert.Equal(t, before.receipts, actual.receipts)
	assert.Equal(t, before.payloads, actual.payloads)
	migrationSchema(t, root)
}

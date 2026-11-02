//go:build durable_faulttest && linux && amd64 && cgo

package durable

import (
	"context"
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

// The seed is closed before copying. The modeled target is never opened by SQLite before Freeze.
func TestFaultDirectImage(t *testing.T) {
	if mode := os.Getenv("WORKQ_DIRECT_IMAGE"); mode != "" {
		directImageChild(t, mode)
		return
	}
	for _, mode := range []string{"sync", "omit"} {
		t.Run(mode, func(t *testing.T) {
			parent, err := os.MkdirTemp("", "direct-fixture-")
			require.NoError(t, err)
			scratch, err := os.MkdirTemp("", "direct-evidence-")
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
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultDirectImage$")
			command.Env = append(os.Environ(), "WORKQ_DIRECT_IMAGE="+mode,
				"WORKQ_DIRECT_ROOT="+root, "WORKQ_DIRECT_SCRATCH="+scratch)
			command.ExtraFiles = []*os.File{writer}
			command.Stderr = os.Stderr
			command.Stdout = os.Stderr
			require.NoError(t, command.Start())
			require.NoError(t, writer.Close())
			joined := false
			t.Cleanup(func() {
				if joined {
					return
				}
				_ = command.Process.Kill() // Always join a child after a parent assertion fails.
				_ = command.Wait()
			})
			var manifest sqlitefault.Manifest
			require.NoError(t, json.NewDecoder(reader).Decode(&manifest))
			require.NoError(t, command.Process.Kill())
			err = command.Wait()
			joined = true
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit)
			require.Equal(t, syscall.SIGKILL, exit.Sys().(syscall.WaitStatus).Signal())
			_, err = sqlitefault.Restore(manifest)
			require.NoError(t, err)
			recovery := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFaultDirectRecovery$")
			recovery.Env = append(os.Environ(), "WORKQ_DIRECT_IMAGE="+mode, "WORKQ_DIRECT_ROOT="+root)
			output, err := recovery.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}

func directImageChild(t *testing.T, mode string) {
	root := os.Getenv("WORKQ_DIRECT_ROOT")
	seed := filepath.Join(filepath.Dir(root), "seed")
	require.NoError(t, os.Mkdir(seed, 0700))
	started, err := startStore(t.Context(), seed, testConfig(t))
	require.NoError(t, err)
	require.NoError(t, started.close())
	data, err := os.ReadFile(filepath.Join(seed, "queue.sqlite3"))
	require.NoError(t, err)
	_, controller, err := sqlitefault.Attach(root, sqlitefault.Limits{
		Events: 8192, Scratch: os.Getenv("WORKQ_DIRECT_SCRATCH"),
	})
	require.NoError(t, err)
	faultController.Store(controller)
	lock, err := os.OpenFile(filepath.Join(root, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, err)
	require.NoError(t, syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	defer func() { assert.NoError(t, lock.Close()) }() // Keep ownership until process death.
	path := filepath.Join(root, "queue.sqlite3")
	main, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, err)
	_, err = main.Write(data)
	require.NoError(t, err)
	if mode == "sync" {
		require.NoError(t, syncOwnedDescriptor(main, path))
	}
	for _, path := range []string{root, filepath.Dir(root)} {
		directory, err := os.Open(path)
		require.NoError(t, err)
		require.NoError(t, syncOwnedDescriptor(directory, path))
		// Retain all target descriptors through Freeze and process death.
	}
	manifest, err := controller.Freeze()
	require.NoError(t, err)
	pipe := os.NewFile(3, "freeze")
	require.NoError(t, json.NewEncoder(pipe).Encode(manifest))
	for {
		_ = syscall.Pause()
	} // Parent owns termination.
}

func TestFaultDirectRecovery(t *testing.T) {
	mode := os.Getenv("WORKQ_DIRECT_IMAGE")
	if mode == "" {
		t.Skip("recovery child entry")
	}
	inspection, err := Inspect(t.Context(), os.Getenv("WORKQ_DIRECT_ROOT"), nil)
	if mode == "omit" {
		require.Error(t, err)
		assert.Equal(t, Inspection{}, inspection)
		return
	}
	require.NoError(t, err)
	assert.NotZero(t, inspection.Store.UUID)
	assert.Zero(t, inspection.Store.Counts.PendingItems)
}

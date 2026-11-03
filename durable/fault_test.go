//go:build durable_faulttest && linux && amd64 && cgo

package durable

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq/durable/internal/sqlitefault"
)

func TestFaultProductionSync(t *testing.T) {
	if os.Getenv("WORKQ_DURABLE_FAULT_CHILD") != "1" {
		command := exec.CommandContext(
			t.Context(),
			os.Args[0],
			"-test.run=^TestFaultProductionSync$",
			"-test.v",
		)
		command.Env = append(os.Environ(), "WORKQ_DURABLE_FAULT_CHILD=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		t.Log(string(output))
		return
	}
	root := t.TempDir()
	_, controller, err := sqlitefault.Attach(root, sqlitefault.Limits{Events: 4096})
	require.NoError(t, err)
	faultController.Store(controller)
	t.Cleanup(func() {
		faultController.Store(nil)
		assert.NoError(t, controller.Close())
	})
	started, err := startStore(t.Context(), root, testConfig(t))
	require.NoError(t, err)
	require.NoError(t, started.close())
	var main, directory, parent bool
	for _, event := range controller.Events() {
		if event.Operation != "owned-sync" || event.Phase != "after" {
			continue
		}
		assert.Zero(t, event.Result)
		assert.Equal(t, event.Expected, event.Actual)
		switch event.Role {
		case "main":
			main = true
		case "directory":
			directory = true
		case "parent":
			parent = true
		}
	}
	assert.True(t, main)
	assert.True(t, directory)
	assert.True(t, parent)
	path := filepath.Join(root, "queue.sqlite3")
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { assert.NoError(t, file.Close()) }()
	require.NoError(t, controller.Arm("owned-sync", "main", 1, "before", sqlitefault.SyncError))
	require.ErrorIs(t, syncOwnedDescriptor(file, path), sqlitefault.ErrInjected)
	events := controller.Events()
	last := events[len(events)-1]
	assert.Equal(t, "after", last.Phase)
	assert.NotZero(t, last.Result)

	// O_PATH permits identity checks but fsync must fail with EBADF on Linux amd64.
	const openPath = 0x200000
	for _, tc := range []struct{ role, path string }{
		{"main", path}, {"directory", root}, {"parent", filepath.Dir(root)},
	} {
		t.Run(tc.role, func(t *testing.T) {
			descriptor, err := syscall.Open(tc.path, openPath|syscall.O_CLOEXEC, 0)
			require.NoError(t, err)
			file := os.NewFile(uintptr(descriptor), tc.path)
			t.Cleanup(func() { assert.NoError(t, file.Close()) })
			require.ErrorIs(t, syncOwnedDescriptor(file, tc.path), syscall.EBADF)
			events := controller.Events()
			last := events[len(events)-1]
			assert.Equal(t, "after", last.Phase)
			assert.Equal(t, tc.role, last.Role)
			assert.Equal(t, syscall.EBADF, last.Errno)
			assert.NotZero(t, last.Result)
			require.NoError(t, controller.Arm("owned-sync", tc.role, 1, "before", sqlitefault.SyncError))
			err = syncOwnedDescriptor(file, tc.path)
			assert.ErrorIs(t, err, sqlitefault.ErrInjected)
			assert.NotErrorIs(t, err, syscall.EBADF)
			events = controller.Events()
			last = events[len(events)-1]
			assert.Equal(t, "after", last.Phase)
			assert.Zero(t, last.Errno)
			assert.NotZero(t, last.Result)
			assert.NoError(t, controller.Err())
		})
	}
	events = controller.Events()

	other, err := os.Create(filepath.Join(root, "other"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, other.Close()) }()
	before := len(events)
	require.ErrorIs(t, syncOwnedDescriptor(other, path), ErrStorage)
	assert.Len(t, controller.Events(), before, "identity refusal precedes observer and Sync")
}

func TestFaultProductionDirectorySyncError(t *testing.T) {
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			_, controller, err := sqlitefault.Attach(root, sqlitefault.Limits{Events: 32})
			require.NoError(t, err)
			faultController.Store(controller)
			t.Cleanup(func() {
				faultController.Store(nil)
				assert.NoError(t, controller.Close())
			})
			file, err := os.Open(root)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, file.Close()) })
			require.NoError(t, controller.Arm("owned-sync", "directory", 1, phase, sqlitefault.SyncError))
			require.ErrorIs(t, syncOwnedDescriptor(file, root), sqlitefault.ErrInjected)
			events := controller.Events()
			require.Len(t, events, 2)
			assert.Equal(t, phase == "after", events[1].Performed)
			assert.Zero(t, events[1].ActualResult)
			assert.True(t, events[1].Injected)
			assert.Equal(t, events[1].Expected, events[1].Actual)
			require.NoError(t, syncOwnedDescriptor(file, root), "one-shot fault")
		})
	}
}

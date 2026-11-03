//go:build durable_faulttest && linux && amd64 && cgo

package durable

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/workq/durable/internal/sqlitefault"
)

func TestFaultEmptyWALInspect(t *testing.T) {
	for _, mode := range []string{"main", "directory", "identity", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			root := offlineFixture(t, testConfig(t))
			_, controller, err := sqlitefault.Attach(root, sqlitefault.Limits{Events: 8192})
			require.NoError(t, err)
			faultController.Store(controller)
			t.Cleanup(func() {
				faultController.Store(nil)
				assert.NoError(t, controller.Close())
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
				if stage == "before-close" && mode == "cancel" {
					cancel()
				}
				if stage != "before-barrier" {
					return nil
				}
				size, err := opened.offlineWALBytes()
				require.NoError(t, err)
				require.Zero(t, size)
				switch mode {
				case "main", "directory":
					require.NoError(t, controller.Arm("owned-sync", mode, 1, "before", sqlitefault.SyncError))
				case "identity":
					replacement, err := os.Create(filepath.Join(t.TempDir(), "other"))
					require.NoError(t, err)
					require.NoError(t, opened.main.Close())
					opened.main = replacement
				}
				return nil
			}}
			inspection, err := inspectObserved(ctx, root, []ID{{Sequence: 1}}, observer)
			require.Error(t, err)
			assert.Equal(t, Inspection{}, inspection)
			if mode == "cancel" {
				assert.ErrorIs(t, err, context.Canceled)
			} else {
				assert.ErrorIs(t, err, ErrStorage)
			}
			var refused, mainSynced bool
			for _, event := range controller.Events() {
				if event.Operation != "owned-sync" || event.Phase != "after" {
					continue
				}
				if event.Role == mode {
					assert.False(t, event.Performed)
					assert.NotZero(t, event.Result)
					refused = true
				}
				if event.Role == "main" && event.Performed && event.ActualResult == 0 {
					mainSynced = true
				}
			}
			if mode == "main" || mode == "directory" {
				assert.True(t, refused)
			}
			if mode == "main" || mode == "identity" {
				assert.False(t, mainSynced)
			}
			require.NoError(t, controller.Err())
		})
	}
}

func TestFaultChangeDurabilityBoundaries(t *testing.T) {
	for _, mode := range []string{"main", "directory", "cancel", "post-commit-close"} {
		t.Run(mode, func(t *testing.T) {
			config := testConfig(t)
			config.Durability = ProcessCrash
			root := offlineFixture(t, config)
			_, controller, err := sqlitefault.Attach(root, sqlitefault.Limits{Events: 8192})
			require.NoError(t, err)
			faultController.Store(controller)
			t.Cleanup(func() {
				faultController.Store(nil)
				assert.NoError(t, controller.Close())
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
				if stage == "before-barrier" && (mode == "main" || mode == "directory") {
					return controller.Arm("owned-sync", mode, 1, "before", sqlitefault.SyncError)
				}
				if stage == "after-barrier" && mode == "cancel" {
					cancel()
				}
				if stage == "before-close" && mode == "post-commit-close" {
					require.NoError(t, opened.main.Close())
				}
				return nil
			}}
			result, err := changeDurabilityObserved(ctx, root, ProcessCrash, Synchronized, observer)
			require.Error(t, err)
			wantDecision, wantPolicy := NotApplied, ProcessCrash
			if mode == "post-commit-close" {
				wantDecision, wantPolicy = Unknown, Synchronized
			}
			assert.Equal(t, wantDecision, result.Decision)
			if mode == "cancel" {
				assert.ErrorIs(t, err, context.Canceled)
			} else {
				assert.ErrorIs(t, err, ErrStorage)
			}
			inspection, err := Inspect(t.Context(), root, nil)
			require.NoError(t, err)
			assert.Equal(t, wantPolicy, inspection.Store.Durability)
			require.NoError(t, controller.Err())
		})
	}
}

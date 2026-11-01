package diagnostics

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"

	diagnosticvalues "github.com/freshdresch/go-data-structures/workq/diagnostics"
)

func TestDiagnosticsSnapshotOverlappingSeries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		storage := New(diagnosticvalues.DiagnosticsConfig{
			Mode: diagnosticvalues.DiagnosticsCounters, Shards: 1,
		})
		counters := &storage.Counters[0].Producer.SideCounters
		counters.WaitStarts.Store(10)
		loaded := make(chan struct{})
		resume := make(chan struct{})
		firstResults := make(chan diagnosticvalues.DiagnosticsSnapshot, 1)
		go func() {
			firstResults <- storage.Snapshot(&ReductionControl{
				Counters: counters,
				Loaded:   loaded,
				Resume:   resume,
			})
		}()
		<-loaded
		counters.WaitStarts.Add(1)
		second := storage.Snapshot(nil)
		assert.Equal(t, uint64(11), second.Producer.WaitStarts)
		select {
		case <-firstResults:
			t.Fatal("first collector returned before its barrier was released")
		default:
		}
		close(resume)
		first := <-firstResults
		assert.Equal(t, uint64(10), first.Producer.WaitStarts)
		assert.Greater(t, second.Producer.WaitStarts, first.Producer.WaitStarts)

		// Each collector retains its own baseline despite the reversed completion order.
		counters.WaitStarts.Add(2)
		firstNext := make(chan diagnosticvalues.DiagnosticsSnapshot, 1)
		secondNext := make(chan diagnosticvalues.DiagnosticsSnapshot, 1)
		go func() { firstNext <- storage.Snapshot(nil) }()
		go func() { secondNext <- storage.Snapshot(nil) }()
		firstDelta := (<-firstNext).Producer.WaitStarts - first.Producer.WaitStarts
		secondDelta := (<-secondNext).Producer.WaitStarts - second.Producer.WaitStarts
		assert.Equal(t, uint64(3), firstDelta)
		assert.Equal(t, uint64(2), secondDelta)
	})
}

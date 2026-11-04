package workq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Embedding an absent engine makes every unintended method call fail immediately.
type diagnosticsForbiddenEngine struct {
	engine[int]
	t *testing.T
}

func (e *diagnosticsForbiddenEngine) Capacity() int {
	e.t.Fatal("snapshot called engine Capacity")
	return 64
}

func TestDiagnosticsSnapshotWaitStateAndNoEngineCalls(t *testing.T) {
	for _, mode := range []DiagnosticsMode{
		DiagnosticsDisabled, DiagnosticsCounters, DiagnosticsTiming,
	} {
		queue, err := New[int](64, WithDiagnostics(DiagnosticsConfig{Mode: mode}))
		require.NoError(t, err)
		queue.engine = &diagnosticsForbiddenEngine{t: t}
		queue.producers.word.Store(uint64(3*onePoller + 7*oneParked + closedFlag))
		queue.consumers.word.Store(uint64(5*onePoller + 11*oneParked))
		snapshot := queue.Diagnostics()
		assert.Equal(t, uint32(1), snapshot.SchemaVersion)
		assert.Equal(t, uint32(64), snapshot.Config.Capacity)
		assert.Equal(t, EngineUnknown, snapshot.Config.Engine)
		assert.Equal(t, mode, snapshot.Config.Mode)
		want := DiagnosticsWaitState{}
		if mode != DiagnosticsDisabled {
			want.Producer = DiagnosticsSideWaitState{Pollers: 3, Parked: 7, Closed: true}
			want.Consumer = DiagnosticsSideWaitState{Pollers: 5, Parked: 11}
		}
		assert.Equal(t, want, snapshot.WaitState)
	}
}

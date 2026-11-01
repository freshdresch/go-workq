package workq

import (
	"github.com/freshdresch/go-data-structures/ring/dpdk"
	"github.com/freshdresch/go-data-structures/ring/frontier"
	internaldiagnostics "github.com/freshdresch/go-data-structures/workq/internal/diagnostics"
)

// Diagnostics returns a detached cumulative snapshot without calling the engine.
// Disabled snapshots contain static metadata only, even during waits and after Close.
// Each collector must finish one call before starting the next call in its delta series.
func (q *Queue[T]) Diagnostics() DiagnosticsSnapshot {
	return q.collectDiagnostics(nil)
}

func (q *Queue[T]) collectDiagnostics(
	control *internaldiagnostics.ReductionControl,
) DiagnosticsSnapshot {
	var snapshot DiagnosticsSnapshot
	if q.diagnostics != nil {
		snapshot = (*internaldiagnostics.Storage)(q.diagnostics).Snapshot(control)
		snapshot.Config.DiagnosticsConfig = q.diagnostics.Config
		snapshot.WaitState.Producer = diagnosticsWaitState(q.producers.load())
		snapshot.WaitState.Consumer = diagnosticsWaitState(q.consumers.load())
	}
	snapshot.SchemaVersion = DiagnosticsSchemaVersion
	snapshot.Config.Engine = q.diagnosticsEngineKind()
	snapshot.Config.Capacity = q.capacity
	snapshot.Config.MaxPushBatch = q.maxPushBatch
	snapshot.Config.MaxReceiveBatch = q.maxReceiveBatch
	snapshot.Config.ConsumerWait = q.consumerWait
	snapshot.Config.ProducerWait = q.producerWait
	snapshot.Config.ConsumerWakeCap = q.consumerWakeCap
	return snapshot
}

func (q *Queue[T]) diagnosticsEngineKind() EngineKind {
	switch q.engine.(type) {
	case *dpdk.SPSCQueue[T]:
		return EngineSPSC
	case *frontier.MPSCQueue[T]:
		return EngineMPSC
	case *frontier.SPMCQueue[T]:
		return EngineSPMC
	case *frontier.MPMCQueue[T]:
		return EngineMPMC
	default:
		return EngineUnknown
	}
}

func diagnosticsWaitState(word sideWord) DiagnosticsSideWaitState {
	return DiagnosticsSideWaitState{
		Pollers: word.pollers(),
		Parked:  word.parked(),
		Closed:  word.closed(),
	}
}

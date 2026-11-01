package workq_test

import (
	"testing"

	"github.com/freshdresch/go-data-structures/workq"
	"github.com/freshdresch/go-data-structures/workq/diagnostics"
	"github.com/freshdresch/go-data-structures/workq/wait"
)

// Pointer assignments require identical types in both directions, including nested fields.
var (
	_ *workq.DiagnosticsConfig         = (*diagnostics.DiagnosticsConfig)(nil)
	_ *diagnostics.DiagnosticsConfig   = (*workq.DiagnosticsConfig)(nil)
	_ *workq.DiagnosticsSnapshot       = (*diagnostics.DiagnosticsSnapshot)(nil)
	_ *diagnostics.DiagnosticsSnapshot = (*workq.DiagnosticsSnapshot)(nil)
	_ *workq.SizeHistogram             = (*diagnostics.SizeHistogram)(nil)
	_ *diagnostics.SizeHistogram       = (*workq.SizeHistogram)(nil)
	_ *workq.CapacityHistogram         = (*diagnostics.CapacityHistogram)(nil)
	_ *diagnostics.CapacityHistogram   = (*workq.CapacityHistogram)(nil)
	_ *workq.DurationHistogram         = (*diagnostics.DurationHistogram)(nil)
	_ *diagnostics.DurationHistogram   = (*workq.DurationHistogram)(nil)
	_ *workq.WaitStrategy              = (*wait.Strategy)(nil)
	_ *wait.Strategy                   = (*workq.WaitStrategy)(nil)
)

func TestDiagnosticsPackageCompatibility(t *testing.T) {
	settings := diagnostics.DiagnosticsConfig{
		Mode:   diagnostics.DiagnosticsCounters,
		Shards: 2,
	}
	strategy := wait.Strategy{Yields: 3, NeverPark: true}
	queue, err := workq.New[int](
		8,
		workq.WithDiagnostics(settings),
		workq.WithConsumerWait(strategy),
		workq.WithProducerWait(strategy),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	snapshot := queue.Diagnostics()
	if snapshot.SchemaVersion != diagnostics.DiagnosticsSchemaVersion {
		t.Fatalf("schema version = %d", snapshot.SchemaVersion)
	}
	if snapshot.Config.DiagnosticsConfig != settings {
		t.Fatalf("diagnostics config = %+v, want %+v", snapshot.Config.DiagnosticsConfig, settings)
	}
	if snapshot.Config.ConsumerWait != strategy || snapshot.Config.ProducerWait != strategy {
		t.Fatalf("wait strategies = %+v, %+v", snapshot.Config.ConsumerWait, snapshot.Config.ProducerWait)
	}
}

package workq

import (
	"fmt"

	diagnosticvalues "github.com/freshdresch/go-data-structures/workq/diagnostics"
)

// DiagnosticsMode selects construction-time diagnostics storage and recording.
type DiagnosticsMode = diagnosticvalues.DiagnosticsMode

const (
	// DiagnosticsDisabled allocates no diagnostics storage and reports only static metadata.
	DiagnosticsDisabled = diagnosticvalues.DiagnosticsDisabled
	// DiagnosticsCounters enables counters and optional capacity probes without clock reads.
	DiagnosticsCounters = diagnosticvalues.DiagnosticsCounters
	// DiagnosticsTiming also enables sampled wait and park-select durations.
	DiagnosticsTiming = diagnosticvalues.DiagnosticsTiming
)

// DiagnosticsConfig is immutable after construction. Options use their final values.
type DiagnosticsConfig = diagnosticvalues.DiagnosticsConfig

// DiagnosticsSchemaVersion fixes the enum order and histogram bucket interpretation.
const DiagnosticsSchemaVersion = diagnosticvalues.DiagnosticsSchemaVersion

// EngineKind identifies the concrete queue engine without querying its live state.
type EngineKind = diagnosticvalues.EngineKind

const (
	// EngineSPSC identifies dpdk.SPSCQueue.
	EngineSPSC = diagnosticvalues.EngineSPSC
	// EngineMPSC identifies frontier.MPSCQueue.
	EngineMPSC = diagnosticvalues.EngineMPSC
	// EngineSPMC identifies frontier.SPMCQueue.
	EngineSPMC = diagnosticvalues.EngineSPMC
	// EngineMPMC identifies frontier.MPMCQueue.
	EngineMPMC = diagnosticvalues.EngineMPMC
	// EngineUnknown identifies an internal engine outside the four supported implementations.
	EngineUnknown = diagnosticvalues.EngineUnknown
)

// AttemptPhase identifies the engine attempt location.
// Unrecognized values must not be treated as zero.
type AttemptPhase = diagnosticvalues.AttemptPhase

const (
	// PhaseInitial identifies ordinary transfer attempts.
	PhaseInitial = diagnosticvalues.PhaseInitial
	// PhaseSpin identifies unregistered spin attempts.
	PhaseSpin = diagnosticvalues.PhaseSpin
	// PhaseYield identifies bounded registered polling.
	PhaseYield = diagnosticvalues.PhaseYield
	// PhaseNeverPark identifies unbounded registered polling.
	PhaseNeverPark = diagnosticvalues.PhaseNeverPark
	// PhaseExtra identifies the attempt after enlistment.
	PhaseExtra = diagnosticvalues.PhaseExtra
	// PhaseDrain identifies closure drain attempts.
	PhaseDrain = diagnosticvalues.PhaseDrain
	// AttemptPhaseCount is the number of schema version 1 array entries.
	AttemptPhaseCount = diagnosticvalues.AttemptPhaseCount
)

// WaitOutcome identifies the returned wait result.
// Unrecognized values must not be treated as zero.
type WaitOutcome = diagnosticvalues.WaitOutcome

const (
	// WaitProgress identifies positive transferred counts.
	WaitProgress = diagnosticvalues.WaitProgress
	// WaitClosed identifies closure without progress.
	WaitClosed = diagnosticvalues.WaitClosed
	// WaitCanceled identifies cancellation without progress.
	WaitCanceled = diagnosticvalues.WaitCanceled
	// WaitDeadline identifies deadline expiry without progress.
	WaitDeadline = diagnosticvalues.WaitDeadline
	// WaitOutcomeCount is the number of schema version 1 array entries.
	WaitOutcomeCount = diagnosticvalues.WaitOutcomeCount
)

// ParkSelectCase identifies the selected park branch.
// Unrecognized values must not be treated as zero.
type ParkSelectCase = diagnosticvalues.ParkSelectCase

const (
	// ParkToken identifies a normal retry token.
	ParkToken = diagnosticvalues.ParkToken
	// ParkContext identifies context completion.
	ParkContext = diagnosticvalues.ParkContext
	// ParkDone identifies queue closure.
	ParkDone = diagnosticvalues.ParkDone
	// ParkSelectCaseCount is the number of schema version 1 array entries.
	ParkSelectCaseCount = diagnosticvalues.ParkSelectCaseCount
)

// WakeKind identifies the wake primitive.
// Unrecognized values must not be treated as zero.
type WakeKind = diagnosticvalues.WakeKind

const (
	// WakeOne identifies one-recipient wake calls.
	WakeOne = diagnosticvalues.WakeOne
	// WakeGroup identifies consumer group wake calls.
	WakeGroup = diagnosticvalues.WakeGroup
	// WakeKindCount is the number of schema version 1 array entries.
	WakeKindCount = diagnosticvalues.WakeKindCount
)

// WakeRetryOutcome identifies the endpoint of a normal token retry.
// Unrecognized values must not be treated as zero.
type WakeRetryOutcome = diagnosticvalues.WakeRetryOutcome

const (
	// RetryReparked identifies successful re-enlistment.
	RetryReparked = diagnosticvalues.RetryReparked
	// RetryProgress identifies positive transfer progress.
	RetryProgress = diagnosticvalues.RetryProgress
	// RetryClosed identifies closure without progress.
	RetryClosed = diagnosticvalues.RetryClosed
	// RetryCanceled identifies cancellation without progress.
	RetryCanceled = diagnosticvalues.RetryCanceled
	// RetryDeadline identifies deadline expiry without progress.
	RetryDeadline = diagnosticvalues.RetryDeadline
	// WakeRetryOutcomeCount is the number of schema version 1 array entries.
	WakeRetryOutcomeCount = diagnosticvalues.WakeRetryOutcomeCount
)

// CapacitySource identifies the origin of a remaining-capacity observation.
// Unrecognized values must not be treated as zero.
type CapacitySource = diagnosticvalues.CapacitySource

const (
	// TransferFresh identifies short burst observations, including zero progress.
	TransferFresh = diagnosticvalues.TransferFresh
	// TransferCachedAllowed identifies full burst observations that may use cached progress.
	TransferCachedAllowed = diagnosticvalues.TransferCachedAllowed
	// PublicationProbe identifies required consumer probes after publication.
	PublicationProbe = diagnosticvalues.PublicationProbe
	// CleanupProbe identifies required probes during wait cleanup.
	CleanupProbe = diagnosticvalues.CleanupProbe
	// DiagnosticProbe identifies optional sampled same-side probes.
	DiagnosticProbe = diagnosticvalues.DiagnosticProbe
	// CapacitySourceCount is the number of schema version 1 array entries.
	CapacitySourceCount = diagnosticvalues.CapacitySourceCount
)

// SizeBucketCount includes exact sizes 0..32, followed by upper bounds 2^6..2^30.
const SizeBucketCount = diagnosticvalues.SizeBucketCount

// DurationBucketCount includes zero, one, and ranges [2^k, 2^(k+1)-1] for k=1..62.
const DurationBucketCount = diagnosticvalues.DurationBucketCount

// SizeHistogram contains event counts and item-unit sums and lifetime extrema.
// Buckets 0..32 are exact sizes. Bucket i >= 33 ends at 1<<(i-27), inclusively.
// Empty histograms have zero extrema. Counters and sums use modulo-2^64 arithmetic.
type SizeHistogram = diagnosticvalues.SizeHistogram

// CapacityHistogram describes event-weighted remaining room or visible work, not queue depth.
// ZeroCount repeats bucket zero. AtCapacity counts observations equal to the static capacity.
type CapacityHistogram = diagnosticvalues.CapacityHistogram

// DurationHistogram contains event counts and nanosecond sums and lifetime extrema.
// Bucket zero is zero. Bucket i > 0 has inclusive upper bound (1<<i)-1.
// Empty histograms have zero extrema. Sums and counts use modulo-2^64 arithmetic.
type DurationHistogram = diagnosticvalues.DurationHistogram

// DiagnosticsStaticConfig records resolved construction settings and effective wait strategies.
type DiagnosticsStaticConfig = diagnosticvalues.DiagnosticsStaticConfig

// DiagnosticsTimingSnapshot contains selected starts and completed duration samples.
// An unfinished episode or select contributes only its start, never a zero duration.
type DiagnosticsTimingSnapshot = diagnosticvalues.DiagnosticsTimingSnapshot

// DiagnosticsSideSnapshot contains cumulative counters for one side of the queue.
// Phase arrays count completed nonempty engine attempts, not probes or logical calls.
// Group fields are consumer-only and remain zero on the producer side.
type DiagnosticsSideSnapshot = diagnosticvalues.DiagnosticsSideSnapshot

// DiagnosticsSideWaitState describes registrations, not scheduler or operating-system state.
type DiagnosticsSideWaitState = diagnosticvalues.DiagnosticsSideWaitState

// DiagnosticsWaitState contains independently observed side states in enabled modes only.
type DiagnosticsWaitState = diagnosticvalues.DiagnosticsWaitState

// DiagnosticsProducerSnapshot reports producer attempts and reusable-slot observations.
type DiagnosticsProducerSnapshot = diagnosticvalues.DiagnosticsProducerSnapshot

// DiagnosticsConsumerSnapshot reports consumer attempts and visible-item observations.
type DiagnosticsConsumerSnapshot = diagnosticvalues.DiagnosticsConsumerSnapshot

// DiagnosticsSnapshot is a detached cumulative value. Concurrent collection is safe, but fields
// need not describe one instant. Collect each delta series sequentially without overlapping calls.
// Lifetime extrema cannot be subtracted. Close does not freeze enabled diagnostics.
type DiagnosticsSnapshot = diagnosticvalues.DiagnosticsSnapshot

func resolveDiagnostics(cfg DiagnosticsConfig) (DiagnosticsConfig, error) {
	switch cfg.Mode {
	case DiagnosticsDisabled:
		if cfg.Shards != 0 || cfg.TimingSampleEvery != 0 || cfg.CapacitySampleEvery != 0 {
			return DiagnosticsConfig{}, fmt.Errorf(
				"disabled mode requires zero auxiliary fields: %w",
				ErrInvalidDiagnostics,
			)
		}
		return cfg, nil
	case DiagnosticsCounters:
		if cfg.TimingSampleEvery != 0 {
			return DiagnosticsConfig{}, fmt.Errorf(
				"counter mode cannot sample timing: %w",
				ErrInvalidDiagnostics,
			)
		}
	case DiagnosticsTiming:
		if cfg.TimingSampleEvery == 0 {
			cfg.TimingSampleEvery = 64
		}
	default:
		return DiagnosticsConfig{}, fmt.Errorf(
			"unknown diagnostics mode %d: %w",
			cfg.Mode, ErrInvalidDiagnostics,
		)
	}
	if cfg.Shards == 0 {
		cfg.Shards = 16
	}
	if cfg.Shards < 1 || cfg.Shards&(cfg.Shards-1) != 0 {
		return DiagnosticsConfig{}, fmt.Errorf(
			"shards %d must be a positive power of two: %w",
			cfg.Shards, ErrInvalidDiagnostics,
		)
	}
	if _, ok := diagnosticsAllocationSize(cfg); !ok {
		return DiagnosticsConfig{}, fmt.Errorf(
			"diagnostics allocation size overflows int: %w",
			ErrInvalidDiagnostics,
		)
	}
	return cfg, nil
}

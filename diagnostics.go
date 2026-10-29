package workq

import "fmt"

// DiagnosticsMode selects construction-time diagnostics storage and recording.
type DiagnosticsMode uint8

const (
	// DiagnosticsDisabled allocates no diagnostics storage and reports only static metadata.
	DiagnosticsDisabled DiagnosticsMode = iota
	// DiagnosticsCounters enables counters and optional capacity probes without clock reads.
	DiagnosticsCounters
	// DiagnosticsTiming also enables sampled wait and park-select durations.
	DiagnosticsTiming
)

// DiagnosticsConfig is immutable after construction. Options use their final values.
type DiagnosticsConfig struct {
	Mode DiagnosticsMode
	// Shards defaults to 16 when enabled and zero. Other enabled values must be powers of two
	// in [1, 256]. Disabled mode requires zero.
	Shards int
	// TimingSampleEvery selects episodes with probability 1/N. Timing mode defaults to 64.
	TimingSampleEvery uint32
	// CapacitySampleEvery selects attempts for fresh same-side probes. Zero disables probes.
	CapacitySampleEvery uint32
}

// DiagnosticsSchemaVersion fixes the enum order and histogram bucket interpretation.
const DiagnosticsSchemaVersion = 1

// EngineKind identifies the concrete queue engine without querying its live state.
type EngineKind uint8

const (
	// EngineSPSC identifies dpdk.SPSCQueue.
	EngineSPSC EngineKind = iota
	// EngineMPSC identifies frontier.MPSCQueue.
	EngineMPSC
	// EngineSPMC identifies frontier.SPMCQueue.
	EngineSPMC
	// EngineMPMC identifies frontier.MPMCQueue.
	EngineMPMC
	// EngineUnknown identifies an internal engine outside the four supported implementations.
	EngineUnknown EngineKind = 255
)

// AttemptPhase identifies the engine attempt location.
// Unrecognized values must not be treated as zero.
type AttemptPhase uint8

const (
	// PhaseInitial identifies ordinary transfer attempts.
	PhaseInitial AttemptPhase = iota
	// PhaseSpin identifies unregistered spin attempts.
	PhaseSpin
	// PhaseYield identifies bounded registered polling.
	PhaseYield
	// PhaseNeverPark identifies unbounded registered polling.
	PhaseNeverPark
	// PhaseExtra identifies the attempt after enlistment.
	PhaseExtra
	// PhaseDrain identifies closure drain attempts.
	PhaseDrain
	// AttemptPhaseCount is the number of schema version 1 array entries.
	AttemptPhaseCount
)

// WaitOutcome identifies the returned wait result.
// Unrecognized values must not be treated as zero.
type WaitOutcome uint8

const (
	// WaitProgress identifies positive transferred counts.
	WaitProgress WaitOutcome = iota
	// WaitClosed identifies closure without progress.
	WaitClosed
	// WaitCanceled identifies cancellation without progress.
	WaitCanceled
	// WaitDeadline identifies deadline expiry without progress.
	WaitDeadline
	// WaitOutcomeCount is the number of schema version 1 array entries.
	WaitOutcomeCount
)

// ParkSelectCase identifies the selected park branch.
// Unrecognized values must not be treated as zero.
type ParkSelectCase uint8

const (
	// ParkToken identifies a normal retry token.
	ParkToken ParkSelectCase = iota
	// ParkContext identifies context completion.
	ParkContext
	// ParkDone identifies queue closure.
	ParkDone
	// ParkSelectCaseCount is the number of schema version 1 array entries.
	ParkSelectCaseCount
)

// WakeKind identifies the wake primitive.
// Unrecognized values must not be treated as zero.
type WakeKind uint8

const (
	// WakeOne identifies one-recipient wake calls.
	WakeOne WakeKind = iota
	// WakeGroup identifies consumer group wake calls.
	WakeGroup
	// WakeKindCount is the number of schema version 1 array entries.
	WakeKindCount
)

// WakeRetryOutcome identifies the endpoint of a normal token retry.
// Unrecognized values must not be treated as zero.
type WakeRetryOutcome uint8

const (
	// RetryReparked identifies successful re-enlistment.
	RetryReparked WakeRetryOutcome = iota
	// RetryProgress identifies positive transfer progress.
	RetryProgress
	// RetryClosed identifies closure without progress.
	RetryClosed
	// RetryCanceled identifies cancellation without progress.
	RetryCanceled
	// RetryDeadline identifies deadline expiry without progress.
	RetryDeadline
	// WakeRetryOutcomeCount is the number of schema version 1 array entries.
	WakeRetryOutcomeCount
)

// CapacitySource identifies the origin of a remaining-capacity observation.
// Unrecognized values must not be treated as zero.
type CapacitySource uint8

const (
	// TransferFresh identifies short burst observations, including zero progress.
	TransferFresh CapacitySource = iota
	// TransferCachedAllowed identifies full burst observations that may use cached progress.
	TransferCachedAllowed
	// PublicationProbe identifies required consumer probes after publication.
	PublicationProbe
	// CleanupProbe identifies required probes during wait cleanup.
	CleanupProbe
	// DiagnosticProbe identifies optional sampled same-side probes.
	DiagnosticProbe
	// CapacitySourceCount is the number of schema version 1 array entries.
	CapacitySourceCount
)

// SizeBucketCount includes exact sizes 0..32, followed by upper bounds 2^6..2^30.
const SizeBucketCount = 58

// DurationBucketCount includes zero, one, and ranges [2^k, 2^(k+1)-1] for k=1..62.
const DurationBucketCount = 64

// SizeHistogram contains event counts and item-unit sums and lifetime extrema.
// Buckets 0..32 are exact sizes. Bucket i >= 33 ends at 1<<(i-27), inclusively.
// Empty histograms have zero extrema. Counters and sums use modulo-2^64 arithmetic.
type SizeHistogram struct {
	Buckets [SizeBucketCount]uint64
	Count   uint64
	Sum     uint64
	Min     uint64
	Max     uint64
}

// CapacityHistogram describes event-weighted remaining room or visible work, not queue depth.
// ZeroCount repeats bucket zero. AtCapacity counts observations equal to the static capacity.
type CapacityHistogram struct {
	SizeHistogram
	ZeroCount  uint64
	AtCapacity uint64
}

// DurationHistogram contains event counts and nanosecond sums and lifetime extrema.
// Bucket zero is zero. Bucket i > 0 has inclusive upper bound (1<<i)-1.
// Empty histograms have zero extrema. Sums and counts use modulo-2^64 arithmetic.
type DurationHistogram struct {
	Buckets [DurationBucketCount]uint64
	Count   uint64
	Sum     uint64
	Min     uint64
	Max     uint64
}

// DiagnosticsStaticConfig records resolved construction settings and effective wait strategies.
type DiagnosticsStaticConfig struct {
	DiagnosticsConfig
	Engine          EngineKind
	Capacity        uint32
	MaxPushBatch    int
	MaxReceiveBatch int
	ConsumerWait    WaitStrategy
	ProducerWait    WaitStrategy
	ConsumerWakeCap uint32
}

// DiagnosticsTimingSnapshot contains selected starts and completed duration samples.
// An unfinished episode or select contributes only its start, never a zero duration.
type DiagnosticsTimingSnapshot struct {
	WaitStarts         uint64
	WaitSamples        uint64
	ParkSelectStarts   uint64
	ParkSelectSamples  uint64
	WaitDuration       [WaitOutcomeCount]DurationHistogram
	ParkSelectDuration [ParkSelectCaseCount]DurationHistogram
}

// DiagnosticsSideSnapshot contains cumulative counters for one side of the queue.
// Phase arrays count completed nonempty engine attempts, not probes or logical calls.
// Group fields are consumer-only and remain zero on the producer side.
type DiagnosticsSideSnapshot struct {
	Attempts                      [AttemptPhaseCount]uint64
	ZeroProgress                  [AttemptPhaseCount]uint64
	PartialProgress               [AttemptPhaseCount]uint64
	FullProgress                  [AttemptPhaseCount]uint64
	RequestedItems                [AttemptPhaseCount]uint64
	MovedItems                    [AttemptPhaseCount]uint64
	WaitStarts                    uint64
	WaitEnds                      [WaitOutcomeCount]uint64
	WaitEndsWithCloseRace         uint64
	SpinEntries                   uint64
	YieldEntries                  uint64
	NeverParkEntries              uint64
	YieldCalls                    [AttemptPhaseCount]uint64
	Registrations                 uint64
	ParkEnlistments               uint64
	ParkEnlistClosed              uint64
	ParkSelectStarts              uint64
	ParkSelectEnds                [ParkSelectCaseCount]uint64
	WakeCalls                     [WakeKindCount]uint64
	WakeTransfers                 [WakeKindCount]uint64
	WakeRecipients                uint64
	TokensSent                    uint64
	TokensReceivedForRetry        uint64
	TokensDrainedByCleanup        uint64
	WakeRetryAttempts             uint64
	WakeRetryZeroProgress         uint64
	WakeRetryEnds                 [WakeRetryOutcomeCount]uint64
	ProducerOldestReenlistments   uint64
	CapacityEligibleAttempts      uint64
	CapacitySelectedProbes        uint64
	CapacityCompletedProbes       uint64
	AttemptsWithoutRemainingCount uint64
	RequestedSize                 SizeHistogram
	MovedSize                     SizeHistogram
	GroupTarget                   SizeHistogram
	GroupActualSize               SizeHistogram
	Timing                        DiagnosticsTimingSnapshot
}

// DiagnosticsSideWaitState describes registrations, not scheduler or operating-system state.
type DiagnosticsSideWaitState struct {
	Pollers uint32
	Parked  uint32
	Closed  bool
}

// DiagnosticsWaitState contains independently observed side states in enabled modes only.
type DiagnosticsWaitState struct {
	Producer DiagnosticsSideWaitState
	Consumer DiagnosticsSideWaitState
}

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
	if cfg.Shards < 1 || cfg.Shards > 256 || cfg.Shards&(cfg.Shards-1) != 0 {
		return DiagnosticsConfig{}, fmt.Errorf(
			"shards %d must be a power of two in [1, 256]: %w",
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

// DiagnosticsProducerSnapshot reports producer attempts and reusable-slot observations.
type DiagnosticsProducerSnapshot struct {
	DiagnosticsSideSnapshot
	ReusableSlots [CapacitySourceCount]CapacityHistogram
}

// DiagnosticsConsumerSnapshot reports consumer attempts and visible-item observations.
type DiagnosticsConsumerSnapshot struct {
	DiagnosticsSideSnapshot
	AvailableItems [CapacitySourceCount]CapacityHistogram
}

// DiagnosticsSnapshot is a detached cumulative value. Concurrent collection is safe, but fields
// need not describe one instant. Collect each delta series sequentially without overlapping calls.
// Lifetime extrema cannot be subtracted. Close does not freeze enabled diagnostics.
type DiagnosticsSnapshot struct {
	SchemaVersion uint32
	Config        DiagnosticsStaticConfig
	Producer      DiagnosticsProducerSnapshot
	Consumer      DiagnosticsConsumerSnapshot
	WaitState     DiagnosticsWaitState
}

package durable

import (
	"errors"
	"math"
	"strings"
	"time"
)

// StoreInfo describes the last observed committed store and physical sizes.
// Disk values in Snapshot are valid only when Snapshot.Disk.Valid is true.
// A successful Inspection always contains valid disk observations.
type StoreInfo struct {
	UUID          UUID
	Epoch         uint64
	Policy        DurablePolicy
	Durability    Durability
	Counts        StoreCounts
	DatabaseBytes Bytes
	WALBytes      Bytes
	PageCount     uint64
	FreePages     uint64
}

// OperationalState describes the session's current service and ownership phase.
type OperationalState uint8

const (
	// Active permits commands under the independent admission and delivery gates.
	Active OperationalState = iota
	// MaintenanceRequired permits healthy ordered Lookup and Release only.
	MaintenanceRequired
	// Closing settles admitted commands before releasing resources.
	Closing
	// Faulted refuses authoritative reconciliation after storage failure.
	Faulted
	// Aborting settles an entered storage boundary and rejects unstarted work.
	Aborting
	// Closed means every owned storage resource has joined.
	Closed
)

// String returns the operational state name.
func (s OperationalState) String() string {
	switch s {
	case Active:
		return "active"
	case MaintenanceRequired:
		return "maintenance-required"
	case Closing:
		return "closing"
	case Faulted:
		return "faulted"
	case Aborting:
		return "aborting"
	case Closed:
		return "closed"
	default:
		return "unknown"
	}
}

// CauseInfo is a bounded copy of the terminal cause, without an error history.
type CauseInfo struct {
	Operation   DiagnosticOperation
	Detail      string
	Truncated   bool
	Storage     bool
	Maintenance bool
	Exhausted   bool
}

// DiagnosticOperation names the boundary that published a terminal cause.
type DiagnosticOperation uint8

const (
	// NoOperation means no terminal boundary has been observed.
	NoOperation DiagnosticOperation = iota
	// AbortOperation identifies an application-requested shutdown.
	AbortOperation
	// StorageOperation identifies an observed storage failure.
	StorageOperation
	// DeliveryOperation identifies delivery identity exhaustion.
	DeliveryOperation
	// CheckpointOperation identifies a checkpoint or WAL pressure transition.
	CheckpointOperation
	// CloseOperation identifies an observable shutdown failure.
	CloseOperation
)

// String returns the diagnostic operation name.
func (o DiagnosticOperation) String() string {
	switch o {
	case NoOperation:
		return "none"
	case AbortOperation:
		return "abort"
	case StorageOperation:
		return "storage"
	case DeliveryOperation:
		return "delivery"
	case CheckpointOperation:
		return "checkpoint"
	case CloseOperation:
		return "close"
	default:
		return "unknown"
	}
}

// AdmissionStats counts queue-owned command reservations and scalar waiters.
type AdmissionStats struct {
	Commands Count
	Retained Bytes
	Waiters  uint64
}

// DeliveryStats describes current attempts, including unpublished reservations.
type DeliveryStats struct {
	Claims      Count
	Unpublished Count
	Retained    Bytes
}

// GroupStats describes bounded work collected for one outer transaction.
type GroupStats struct {
	Commands        Count
	Operations      Count
	Encoded         Bytes
	CollectionDelay time.Duration
}

// LatencyStats retains aggregate durations, never individual samples.
type LatencyStats struct {
	Count uint64
	Total time.Duration
	Max   time.Duration
	Last  time.Duration
}

// OutcomeStats contains saturating session counters.
type OutcomeStats struct {
	Commits            uint64
	Rollbacks          uint64
	UnknownResponses   uint64
	Acks               uint64
	CleanupTurns       uint64
	Checkpoints        uint64
	CheckpointFailures uint64
}

// DiskStats describes the last completed physical observation. Unknown allocated
// bytes and native memory are explicitly invalid, not measurements of zero.
type DiskStats struct {
	Valid          bool
	Sequence       uint64
	At             time.Time
	AllocatedValid bool
	AllocatedBytes Bytes
	NativeValid    bool
	NativeBytes    Bytes
	Cache          Bytes
	Mmap           Bytes
}

// CheckpointStats records the latest explicit checkpoint's actual result fields.
type CheckpointStats struct {
	Valid     bool
	Busy      int64
	Frames    int64
	Completed int64
	Duration  time.Duration
}

// MaintenanceDiagnostic describes the observation that stopped mutation service.
type MaintenanceDiagnostic struct {
	DatabaseBytes Bytes
	PageCount     uint64
	FreePages     uint64
	Reason        MaintenanceReason
	WALBytes      Bytes
	Checkpoint    CheckpointStats
}

// CleanupStats describes the last committed bounded cleanup turn.
type CleanupStats struct {
	Deleted       Count
	MarkerCleared bool
}

// MaintenanceReason identifies the pressure that stopped this session.
type MaintenanceReason uint8

const (
	// NoMaintenance means no pressure transition has occurred.
	NoMaintenance MaintenanceReason = iota
	// CheckpointBlocked means a checked checkpoint could not finish.
	CheckpointBlocked
	// WALEnvelopeExceeded means an observation exceeded the profile envelope.
	WALEnvelopeExceeded
)

// String returns the maintenance reason name.
func (r MaintenanceReason) String() string {
	switch r {
	case NoMaintenance:
		return "none"
	case CheckpointBlocked:
		return "checkpoint-blocked"
	case WALEnvelopeExceeded:
		return "wal-envelope-exceeded"
	default:
		return "unknown"
	}
}

// Snapshot is cached diagnostic state, not authoritative ID reconciliation.
// Disk/native values require their validity flags. After Wait it is stable.
// Final admission occupancy describes the resource-join instant, including settled
// responses and reservations not yet consumed by callers. Close does not wait for
// those callers. Their later reclamation does not change this final observation.
type Snapshot struct {
	Store            StoreInfo
	State            OperationalState
	Cause            CauseInfo
	AdmissionStopped bool
	DeliveryStopped  bool
	Ordinary         AdmissionStats
	Progress         AdmissionStats
	Delivery         DeliveryStats
	CurrentGroup     GroupStats
	LastGroup        GroupStats
	Outcomes         OutcomeStats
	CommitLatency    LatencyStats
	AckLatency       LatencyStats
	Disk             DiskStats
	Checkpoint       CheckpointStats
	Validation       ValidationStats
	Maintenance      MaintenanceDiagnostic
	Cleanup          CleanupStats
	Overflow         bool
}

// Snapshot returns bounded cached values without SQL, syscalls, or coordinator
// commands. It takes only the short in-memory state mutex.
func (q *Queue) Snapshot() Snapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.snapshot
}

func (q *Queue) cacheStateLocked() {
	if q.snapshot.State == Closed {
		return
	}
	state := Active
	if q.maintenance {
		state = MaintenanceRequired
	}
	if q.closing {
		state = Closing
	}
	if q.aborting {
		state = Aborting
	}
	if q.faulted {
		state = Faulted
	}
	q.snapshot.State = state
	q.snapshot.AdmissionStopped = q.admissionStopped
	q.snapshot.DeliveryStopped = q.deliveryStopped
	q.snapshot.Ordinary = poolStats(&q.ordinary)
	q.snapshot.Progress = poolStats(&q.progress)
	delivery := DeliveryStats{Retained: q.deliveryRetained}
	for _, slot := range q.claims {
		if slot.used {
			delivery.Claims++
		}
		if slot.used && !slot.acquired {
			delivery.Unpublished++
		}
	}
	q.snapshot.Delivery = delivery
}

func poolStats(pool *admissionPool) AdmissionStats {
	result := AdmissionStats{Retained: pool.retained, Waiters: pool.waiters}
	for index := range pool.slots {
		if pool.slots[index].used {
			result.Commands++
		}
	}
	return result
}

func describeCause(cause error) CauseInfo {
	if cause == nil {
		return CauseInfo{}
	}
	detail := cause.Error()
	return CauseInfo{Detail: strings.Clone(detail[:min(len(detail), 256)]),
		Truncated: len(detail) > 256,
		Storage:   errors.Is(cause, ErrStorage), Maintenance: errors.Is(cause, ErrMaintenanceRequired),
		Exhausted: errors.Is(cause, ErrExhausted)}
}

func (q *Queue) cacheCauseLocked() {
	q.snapshot.Cause.Storage = q.faulted
	q.snapshot.Cause.Maintenance = q.snapshot.Cause.Maintenance || q.maintenance
	q.snapshot.Cause.Exhausted = q.exhausted
}

func (q *Queue) addCounterLocked(value *uint64, amount uint64) {
	if math.MaxUint64-*value < amount {
		*value = math.MaxUint64
		q.snapshot.Overflow = true
		return
	}
	*value += amount
}

func (q *Queue) addLatencyLocked(stats *LatencyStats, duration time.Duration) {
	q.addCounterLocked(&stats.Count, 1)
	stats.Last = duration
	stats.Max = max(stats.Max, duration)
	if duration > time.Duration(math.MaxInt64)-stats.Total {
		stats.Total = time.Duration(math.MaxInt64)
		q.snapshot.Overflow = true
		return
	}
	stats.Total += duration
}

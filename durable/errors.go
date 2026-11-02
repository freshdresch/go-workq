package durable

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidInput identifies an invalid finite argument or configuration.
	ErrInvalidInput = errors.New("invalid input")
	// ErrConflict identifies a conflicting retry or retirement over pending work.
	ErrConflict = errors.New("conflict")
	// ErrStaleToken identifies a token that no longer owns its attempt.
	ErrStaleToken = errors.New("stale token")
	// ErrRetired identifies work at or below a committed retirement floor.
	ErrRetired = errors.New("retired")
	// ErrCapacity identifies a logical capacity refusal.
	ErrCapacity = errors.New("capacity")
	// ErrStopped identifies a session cutoff.
	ErrStopped = errors.New("stopped")
	// ErrDrained identifies a stopped admission stream with no pending work.
	ErrDrained = errors.New("drained")
	// ErrStorage identifies a storage failure and preserves the underlying cause.
	ErrStorage = errors.New("storage")
	// ErrInUse identifies an already owned queue directory.
	ErrInUse = errors.New("in use")
	// ErrConfigMismatch identifies a persisted policy mismatch.
	ErrConfigMismatch = errors.New("configuration mismatch")
	// ErrUnsupported identifies an unrecognized format, profile, or platform.
	ErrUnsupported = errors.New("unsupported")
	// ErrMaintenanceRequired identifies an irreversible session pressure stop.
	ErrMaintenanceRequired = errors.New("maintenance required")
	// ErrExhausted identifies an identity counter that cannot advance without wrapping.
	ErrExhausted = errors.New("identity exhausted")
)

// MaintenanceError preserves bounded observations from an incomplete offline checkpoint.
// It matches ErrMaintenanceRequired. WALBytes requires WALValid, and checkpoint
// progress fields require Checkpoint.Valid. Duration records the attempt even when
// progress is unavailable. It does not sample main-file size or page/free counts.
type MaintenanceError struct {
	Operation  DiagnosticOperation
	Reason     MaintenanceReason
	WALValid   bool
	WALBytes   Bytes
	Checkpoint CheckpointStats
}

// Error formats only bounded scalar observations, without retaining error history.
func (e *MaintenanceError) Error() string {
	return fmt.Sprintf(
		"maintenance required: %s: %s: WAL=%d WAL-valid=%t "+
			"checkpoint-valid=%t busy=%d frames=%d completed=%d duration=%s",
		e.Operation,
		e.Reason,
		e.WALBytes,
		e.WALValid,
		e.Checkpoint.Valid,
		e.Checkpoint.Busy,
		e.Checkpoint.Frames,
		e.Checkpoint.Completed,
		e.Checkpoint.Duration,
	)
}

// Unwrap identifies maintenance pressure without implying storage failure.
func (e *MaintenanceError) Unwrap() error { return ErrMaintenanceRequired }

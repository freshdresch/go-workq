package durable

import "errors"

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

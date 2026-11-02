package durable

import (
	"context"
	"errors"
	"strconv"
)

// MaintenanceResult reports completed physical work, including partial work on error.
// HasStore validates UUID. HasSizes validates the last completed size observations,
// which need not describe post-error storage. MoreWork is conservative on errors.
type MaintenanceResult struct {
	HasStore       bool
	UUID           UUID
	HasSizes       bool
	DatabaseBefore Bytes
	DatabaseAfter  Bytes
	WALAfter       Bytes
	Steps          uint32
	ReclaimedPages uint64
	MoreWork       bool
}

// Maintain validates and checkpoints an existing store, then reclaims free pages
// in steps requesting at most 256 pages and at most 4096 pages per call. It preserves
// logical records and file identity. Cancellation is checked between completed steps.
// Physical progress may accompany an error. It does not guarantee future capacity.
// Directory input, absolute form, and resolved form must each fit 4095 bytes.
// Longer paths return [ErrInvalidInput]. See the package path-resolution memory boundary.
func Maintain(ctx context.Context, directory string) (MaintenanceResult, error) {
	return maintainObserved(ctx, directory, nil)
}

func maintainObserved(
	ctx context.Context,
	directory string,
	observer offlineObserver,
) (result MaintenanceResult, err error) {
	if ctx == nil || directory == "" {
		return result, ErrInvalidInput
	}
	result.MoreWork = true
	opened, err := openOffline(ctx, directory)
	if err != nil {
		return result, err
	}
	result.HasStore = true
	result.UUID = opened.metadata.uuid
	defer func() {
		err = offlineError(ctx, errors.Join(err, closeOffline(opened, observer)))
		if err != nil {
			result.MoreWork = true
		}
	}()
	if err = opened.offlineBarrier(ctx); err != nil {
		return result, err
	}
	before, err := opened.offlineInfo(ctx)
	if err != nil {
		return result, err
	}
	result.HasSizes = true
	result.DatabaseBefore = before.DatabaseBytes
	result.DatabaseAfter = before.DatabaseBytes
	result.WALAfter = before.WALBytes
	current := before
	for requested := uint64(0); requested < 4096 && current.FreePages > 0; {
		if err = observeOffline(observer, "before-step", opened); err != nil {
			return result, err
		}
		if err = ctx.Err(); err != nil {
			return result, err
		}
		pages := min(uint64(256), 4096-requested, current.FreePages)
		boundary := context.WithoutCancel(ctx)
		if err = opened.storage.incrementalVacuum(boundary, pages); err != nil {
			return result, err
		}
		if err = opened.offlineCheckpoint(boundary); err != nil {
			return result, err
		}
		after, sampleErr := opened.offlineInfo(boundary)
		if sampleErr != nil {
			return result, sampleErr
		}
		result.Steps++
		if after.PageCount < current.PageCount {
			result.ReclaimedPages += current.PageCount - after.PageCount
		}
		result.DatabaseAfter = after.DatabaseBytes
		result.WALAfter = after.WALBytes
		current = after
		requested += pages
		if err = observeOffline(observer, "after-step", opened); err != nil {
			return result, err
		}
	}
	result.MoreWork = current.FreePages > 0
	return result, nil
}

func (s *store) incrementalVacuum(ctx context.Context, pages uint64) (err error) {
	// The caller supplies a bounded integer, and SQLite pragmas do not accept bindings.
	statement := "PRAGMA incremental_vacuum(" + strconv.FormatUint(pages, 10) + ")"
	rows, err := s.conn.QueryContext(ctx, statement)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		// Each row advances another page of SQLite's incremental vacuum program.
	}
	return rows.Err()
}

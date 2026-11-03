package durable

import (
	"context"
	"errors"
	"path/filepath"
)

// Resize atomically changes the complete stored policy under exclusive offline
// ownership. It preserves identity, epoch, acceptance order, records, and durability.
// Stop and join workers before calling it. Resize never creates a missing store.
// It uses FULL synchronization even for a store with ProcessCrash durability.
// Migration from v1 to v2 needs temporary space for both payload tables.
// Downgrading v2 to v1 is unsupported.
//
// A matching UUID and an already stored Next return Applied after validation,
// synchronization, and closure. Otherwise Expected must match the full policy.
// Mismatches return NotApplied with ErrConfigMismatch. Limits that cannot contain
// current usage or physical pages return NotApplied with ErrCapacity. Lower row
// limits also require that no acknowledged receipts remain, because their original
// lengths are unknown. Resize never deletes history or performs implicit maintenance.
//
// Cancellation before COMMIT rolls back without cancellation. Once COMMIT starts,
// commit, synchronization, and closure finish before return. Uncertain commit or
// rollback and postcommit failures return Unknown. Reconcile through Inspect while
// keeping the store quiescent, then retry the identical request if appropriate.
// NotApplied describes the logical policy change, not recovery or checkpoint work.
// Directory input, absolute form, and resolved form must each fit 4095 bytes.
func Resize(
	ctx context.Context,
	directory string,
	request ResizeRequest,
) (CommitResult, error) {
	return resizeObserved(ctx, directory, request, nil)
}

func resizeObserved(
	ctx context.Context,
	directory string,
	request ResizeRequest,
	observer offlineObserver,
) (result CommitResult, err error) {
	result.Decision = NotApplied
	if ctx == nil || directory == "" {
		return result, ErrInvalidInput
	}
	if err = validatePolicy(request.Expected); err != nil {
		return result, err
	}
	if err = validatePolicy(request.Next); err != nil {
		return result, err
	}
	if request.Expected.Profile == profileSQLiteWALV2 &&
		request.Next.Profile == profileSQLiteWALV1 {
		return result, ErrUnsupported
	}
	opened, err := openOffline(ctx, directory)
	if err != nil {
		return result, err
	}
	committed := false
	defer func() {
		err = errors.Join(err, closeOffline(opened, observer))
		// Capacity refusal is an expected policy outcome, not a storage failure.
		if !errors.Is(err, ErrCapacity) && !errors.Is(err, ErrMaintenanceRequired) {
			err = classifyStartupError(err)
		}
		if !committed && ctx.Err() != nil {
			err = errors.Join(err, ctx.Err(), context.Cause(ctx))
		}
		if err != nil && result.Decision == Applied {
			result.Decision = Unknown
		}
	}()
	if opened.metadata.uuid != request.UUID {
		return result, ErrConfigMismatch
	}
	if err = observeOffline(observer, "before-barrier", opened); err != nil {
		return result, err
	}
	if err = opened.offlineBarrier(ctx); err != nil {
		return result, err
	}
	if err = observeOffline(observer, "after-barrier", opened); err != nil {
		return result, err
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	if opened.metadata.policy == request.Next {
		committed = true
		result.Decision = Applied
		return result, nil
	}
	if opened.metadata.policy != request.Expected {
		return result, ErrConfigMismatch
	}
	if err = opened.checkResizeUsage(ctx, request.Next); err != nil {
		return result, err
	}
	if err = opened.checkResizeSize(ctx, request.Next.DatabaseLimit); err != nil {
		return result, err
	}
	path := filepath.Join(opened.owner.path, "queue.sqlite3")
	// max_page_count is connection-local and survives ROLLBACK. Every exit closes
	// this connection, and every reopen reinstalls the authoritative stored policy.
	if err = opened.storage.configurePolicy(ctx, path, request.Next); err != nil {
		return result, err
	}
	if err = observeOffline(observer, "before-mutation", opened); err != nil {
		return result, err
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	if _, err = opened.storage.conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return result, classifySQLError(err)
	}
	boundary := context.WithoutCancel(ctx)
	rollback := func(cause error) (CommitResult, error) {
		_, rollbackErr := opened.storage.conn.ExecContext(boundary, "ROLLBACK")
		if rollbackErr != nil {
			result.Decision = Unknown
			rollbackErr = errors.Join(ErrStorage, rollbackErr)
		}
		return result, errors.Join(cause, rollbackErr)
	}
	if request.Expected.Profile != request.Next.Profile {
		if err = opened.migratePayloads(ctx, observer); err != nil {
			return rollback(err)
		}
	}
	policy := encodePolicy(request.Next)
	_, err = opened.storage.conn.ExecContext(
		ctx,
		"UPDATE meta SET value=? WHERE key=?",
		policy[:],
		metaPolicy,
	)
	if err != nil {
		return rollback(classifySQLError(err))
	}
	if err = opened.checkResizeSize(ctx, request.Next.DatabaseLimit); err != nil {
		return rollback(err)
	}
	if err = opened.storage.validateSchemaForProfile(ctx, request.Next.Profile); err != nil {
		return rollback(err)
	}
	if err = observeOffline(observer, "before-commit", opened); err != nil {
		return rollback(err)
	}
	if err = ctx.Err(); err != nil {
		return rollback(err)
	}
	committed = true
	result.Decision = Unknown
	if _, err = opened.storage.conn.ExecContext(boundary, "COMMIT"); err != nil {
		return result, errors.Join(ErrStorage, err)
	}
	if err = observeOffline(observer, "after-commit", opened); err != nil {
		return result, err
	}
	metadata, err := opened.storage.readMetadata(boundary)
	if err != nil {
		return result, err
	}
	expected := opened.metadata
	expected.policy = request.Next
	if metadata != expected {
		return result, ErrStorage
	}
	if err = observeOffline(observer, "before-final-barrier", opened); err != nil {
		return result, err
	}
	if err = opened.offlineBarrier(boundary); err != nil {
		return result, err
	}
	result.Decision = Applied
	return result, nil
}

func (s *validatedStore) checkResizeUsage(ctx context.Context, next DurablePolicy) error {
	counts := s.metadata.counts
	fits := counts.PendingItems <= next.MaxPendingItems &&
		counts.PendingPayload <= next.MaxPendingPayload && counts.Receipts <= next.MaxReceipts &&
		counts.Producers <= next.MaxProducers
	if !fits {
		return ErrCapacity
	}
	old := s.metadata.policy
	smallerRows := next.MaxPayload < old.MaxPayload || next.MaxCodec < old.MaxCodec
	if smallerRows && counts.Receipts > counts.PendingItems {
		return ErrCapacity
	}
	var tooLarge bool
	err := s.storage.conn.QueryRowContext(
		ctx,
		`SELECT EXISTS(
 SELECT 1 FROM payloads WHERE payload_length>? OR length(codec)>?)`,
		uint64(next.MaxPayload),
		uint64(next.MaxCodec),
	).Scan(&tooLarge)
	if err != nil {
		return err
	}
	if tooLarge {
		return ErrCapacity
	}
	return nil
}

func (s *validatedStore) checkResizeSize(ctx context.Context, limit Bytes) error {
	var pages int64
	if err := s.storage.conn.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pages); err != nil {
		return err
	}
	if pages < 0 {
		return ErrStorage
	}
	info, err := s.main.Stat()
	if err != nil {
		return err
	}
	if info.Size() < 0 {
		return ErrStorage
	}
	if uint64(pages) > uint64(limit/pageSize) || uint64(info.Size()) > uint64(limit) {
		return ErrCapacity
	}
	return nil
}

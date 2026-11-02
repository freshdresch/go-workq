package durable

import (
	"context"
	"errors"
)

// ChangeDurability changes the stored policy while no session owns the directory.
// It synchronizes recovered state first. An already stored next policy is Applied
// after that barrier, even if expected differs. Cancellation before mutation is
// NotApplied. Once commit begins, it finishes safely and returns Applied or Unknown.
// Observable closure errors after commit return Unknown and require Inspect.
// Directory input, absolute form, and resolved form must each fit 4095 bytes.
// Longer paths return [ErrInvalidInput]. See the package path-resolution memory boundary.
func ChangeDurability(
	ctx context.Context,
	directory string,
	expected Durability,
	next Durability,
) (CommitResult, error) {
	return changeDurabilityObserved(ctx, directory, expected, next, nil)
}

func changeDurabilityObserved(
	ctx context.Context,
	directory string,
	expected Durability,
	next Durability,
	observer offlineObserver,
) (result CommitResult, err error) {
	result.Decision = NotApplied
	if ctx == nil || directory == "" || expected > ProcessCrash || next > ProcessCrash {
		return result, ErrInvalidInput
	}
	opened, err := openOffline(ctx, directory)
	if err != nil {
		return result, err
	}
	entered := false
	defer func() {
		err = errors.Join(err, closeOffline(opened, observer))
		if !entered {
			err = offlineError(ctx, err)
		} else {
			err = classifyStartupError(err)
		}
		if err != nil && result.Decision == Applied {
			result.Decision = Unknown
		}
	}()
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
	if opened.metadata.durability == next {
		result.Decision = Applied
		entered = true
		return result, nil
	}
	if opened.metadata.durability != expected {
		return result, ErrConfigMismatch
	}
	if err = observeOffline(observer, "before-mutation", opened); err != nil {
		return result, err
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	entered = true
	boundary := context.WithoutCancel(ctx)
	if _, err = opened.storage.conn.ExecContext(boundary, "BEGIN IMMEDIATE"); err != nil {
		return result, errors.Join(ErrStorage, err)
	}
	if _, err = opened.storage.conn.ExecContext(boundary,
		"UPDATE meta SET value=? WHERE key=?", []byte{byte(next)}, metaDurability); err != nil {
		_, rollbackErr := opened.storage.conn.ExecContext(boundary, "ROLLBACK")
		if rollbackErr != nil {
			result.Decision = Unknown
		}
		return result, errors.Join(ErrStorage, err, rollbackErr)
	}
	result.Decision = Unknown
	if err = observeOffline(observer, "before-commit", opened); err != nil {
		_, rollbackErr := opened.storage.conn.ExecContext(boundary, "ROLLBACK")
		if rollbackErr == nil {
			result.Decision = NotApplied
		}
		return result, errors.Join(err, rollbackErr)
	}
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
	if metadata.durability != next {
		return result, ErrStorage
	}
	if err = opened.storage.selectDurability(boundary, Synchronized); err != nil {
		return result, err
	}
	result.Decision = Applied
	return result, nil
}

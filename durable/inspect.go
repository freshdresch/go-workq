package durable

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Inspection is a synchronized, validated snapshot. Statuses preserve input order.
// Every field is zero when Inspect returns an error.
type Inspection struct {
	Store      StoreInfo
	Statuses   []Status
	Validation ValidationStats
}

// offlineObserver is a per-call test seam. It does not replace storage operations.
type offlineObserver interface {
	ObserveOffline(stage string, opened *validatedStore) error
}

func observeOffline(observer offlineObserver, stage string, opened *validatedStore) error {
	if observer == nil {
		return nil
	}
	return observer.ObserveOffline(stage, opened)
}

// Inspect validates and synchronizes an existing store without starting a session.
// It accepts at most 256 IDs, including duplicates and empty input. It needs writable
// storage for recovery and checkpointing. Cancellation before publication returns
// zero Inspection after all entered storage work and resource closure finish.
// Errors preserve causes returned by the driver and owned descriptors. The driver
// can replace a concurrent SQL failure with cancellation, so cancellation alone
// does not establish storage health. Checkpoint blockage returns [MaintenanceError].
func Inspect(ctx context.Context, directory string, ids []ID) (Inspection, error) {
	return inspectObserved(ctx, directory, ids, nil)
}

func inspectObserved(
	ctx context.Context,
	directory string,
	ids []ID,
	observer offlineObserver,
) (inspection Inspection, err error) {
	if ctx == nil || directory == "" || len(ids) > 256 {
		return Inspection{}, ErrInvalidInput
	}
	opened, err := openOffline(ctx, directory)
	if err != nil {
		return Inspection{}, err
	}
	defer func() {
		err = errors.Join(err, closeOffline(opened, observer))
		err = offlineError(ctx, err)
		if err != nil {
			inspection = Inspection{}
		}
	}()
	if err = observeOffline(observer, "before-barrier", opened); err != nil {
		return Inspection{}, err
	}
	if err = opened.offlineBarrier(ctx); err != nil {
		return Inspection{}, err
	}
	if err = observeOffline(observer, "after-barrier", opened); err != nil {
		return Inspection{}, err
	}
	statuses, err := opened.storage.lookup(ctx, ids)
	if err != nil {
		return Inspection{}, err
	}
	info, err := opened.offlineInfo(ctx)
	if err != nil {
		return Inspection{}, err
	}
	return Inspection{Store: info, Statuses: statuses, Validation: opened.validation}, nil
}

func offlineError(ctx context.Context, err error) error {
	if !errors.Is(err, ErrMaintenanceRequired) {
		err = classifyStartupError(err)
	}
	if ctx.Err() != nil {
		err = errors.Join(err, ctx.Err(), context.Cause(ctx))
	}
	return err
}

func closeOffline(opened *validatedStore, observer offlineObserver) error {
	observed := observeOffline(observer, "before-close", opened)
	closed := opened.close()
	if observed != nil || closed != nil {
		return errors.Join(ErrStorage, observed, closed)
	}
	return nil
}

func openOffline(ctx context.Context, directory string) (result *validatedStore, err error) {
	if err := ctx.Err(); err != nil {
		return nil, offlineError(ctx, err)
	}
	owner, err := acquireExistingDirectory(directory)
	if err != nil {
		return nil, offlineError(ctx, err)
	}
	opened := &validatedStore{owner: owner}
	defer func() {
		err = offlineError(ctx, err)
		if err != nil {
			err = errors.Join(err, closeOffline(opened, nil))
			result = nil
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	create, err := owner.checkContents()
	if err != nil {
		return nil, err
	}
	if create {
		return nil, ErrStorage
	}
	path := filepath.Join(owner.path, "queue.sqlite3")
	opened.main, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	// The recognized profile ceiling bounds discovery before stored limits are trusted.
	policy := DurablePolicy{
		Profile: profileSQLiteWALV1, MaxPayload: maxPayload, MaxCodec: maxCodec,
		MaxPendingItems: 4096, MaxPendingPayload: 16 << 20,
		MaxReceipts: 16384, MaxProducers: 256, DatabaseLimit: 1 << 30,
	}
	runtime, err := DefaultRuntime(policy)
	if err != nil {
		return nil, err
	}
	config := Config{Policy: policy, Runtime: runtime}
	opened.storage, err = openStoreConnection(ctx, path, false)
	if err != nil {
		return nil, err
	}
	if err := opened.storage.configure(ctx, path, config, false); err != nil {
		return nil, offlineConfigurationError(err)
	}
	if err := verifyOwnedDescriptor(opened.main, path); err != nil {
		return nil, err
	}
	started := time.Now()
	if err := opened.storage.validateSchema(ctx); err != nil {
		return nil, err
	}
	metadata, err := opened.storage.readMetadata(ctx)
	if err != nil {
		return nil, err
	}
	config.Policy = metadata.policy
	if err := opened.storage.configure(ctx, path, config, false); err != nil {
		return nil, offlineConfigurationError(err)
	}
	opened.validation.LogicalRecords, err = opened.storage.validateLogicalState(ctx, metadata)
	if err != nil {
		return nil, err
	}
	opened.metadata = metadata
	opened.validation.Duration = time.Since(started)
	if err := owner.verify(); err != nil {
		return nil, err
	}
	return opened, nil
}

func offlineConfigurationError(err error) error {
	if !errors.Is(err, ErrConfigMismatch) {
		return err
	}
	// configure uses this sentinel only for its own page/file limit checks.
	return fmt.Errorf("stored database limits: %w: %v", ErrStorage, err)
}

func (s *validatedStore) offlineBarrier(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	boundary := context.WithoutCancel(ctx)
	if err := s.storage.selectDurability(boundary, Synchronized); err != nil {
		return err
	}
	if err := s.offlineCheckpoint(boundary); err != nil {
		return err
	}
	// Only synchronize the retained main descriptor after SQLite has drained the WAL.
	if err := s.synchronizeCreation(nil); err != nil {
		return err
	}
	return s.owner.verify()
}

func (s *validatedStore) offlineCheckpoint(ctx context.Context) error {
	started := time.Now()
	var stats CheckpointStats
	checkpointErr := s.storage.conn.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").
		Scan(&stats.Busy, &stats.Frames, &stats.Completed)
	stats.Duration = time.Since(started)
	stats.Valid = checkpointErr == nil
	blocked := sqliteCheckpointBlocked(checkpointErr)
	if checkpointErr != nil && !blocked {
		return classifySQLError(checkpointErr)
	}
	wal, walErr := s.offlineWALBytes()
	if walErr != nil {
		walErr = errors.Join(ErrStorage, walErr)
	}
	if blocked || stats.Busy != 0 || stats.Frames != stats.Completed || wal != 0 {
		diagnostic := &MaintenanceError{
			Operation: CheckpointOperation, Reason: CheckpointBlocked,
			WALBytes: wal, WALValid: walErr == nil, Checkpoint: stats,
		}
		return errors.Join(diagnostic, checkpointErr, walErr)
	}
	if walErr != nil {
		return walErr
	}
	return nil
}

func (s *validatedStore) offlineWALBytes() (Bytes, error) {
	info, err := os.Stat(filepath.Join(s.owner.path, "queue.sqlite3-wal"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return Bytes(info.Size()), nil
}

func (s *validatedStore) offlineInfo(ctx context.Context) (StoreInfo, error) {
	metadata := s.metadata
	info := StoreInfo{UUID: metadata.uuid, Epoch: metadata.epoch, Policy: metadata.policy,
		Durability: metadata.durability, Counts: metadata.counts}
	if err := verifyOwnedDescriptor(s.main, filepath.Join(s.owner.path, "queue.sqlite3")); err != nil {
		return StoreInfo{}, err
	}
	file, err := s.main.Stat()
	if err != nil {
		return StoreInfo{}, err
	}
	info.DatabaseBytes = Bytes(file.Size())
	info.WALBytes, err = s.offlineWALBytes()
	if err != nil {
		return StoreInfo{}, err
	}
	row := s.storage.conn.QueryRowContext(ctx, "PRAGMA page_count")
	if err := row.Scan(&info.PageCount); err != nil {
		return StoreInfo{}, err
	}
	err = s.storage.conn.QueryRowContext(ctx, "PRAGMA freelist_count").Scan(&info.FreePages)
	if err != nil {
		return StoreInfo{}, err
	}
	return info, nil
}

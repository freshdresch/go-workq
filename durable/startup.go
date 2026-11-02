package durable

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type validatedStore struct {
	storage    *store
	owner      *directoryOwner
	metadata   storeMetadata
	validation ValidationStats
	main       *os.File
}

// ValidationStats records completed startup validation work.
type ValidationStats struct {
	LogicalRecords uint64
	Duration       time.Duration
}

type startupStage uint8

const (
	startupBeforeEpoch startupStage = iota
	startupAfterEpoch
	startupDirectoryReady
	startupAfterParentSync
	startupBeforeInitialize
	startupDuringInitialize
	startupAfterInitialize
	startupDuringEpoch
)

func (s startupStage) String() string {
	switch s {
	case startupBeforeInitialize:
		return "before-initialize"
	case startupDuringInitialize:
		return "during-initialize"
	case startupAfterInitialize:
		return "after-initialize"
	case startupDuringEpoch:
		return "during-epoch"
	case startupDirectoryReady:
		return "directory-ready"
	case startupAfterParentSync:
		return "after-parent-sync"
	case startupBeforeEpoch:
		return "before-epoch"
	case startupAfterEpoch:
		return "after-epoch"
	default:
		return fmt.Sprintf("startupStage(%d)", s)
	}
}

type startupObserver interface{ Observe(startupStage) }

func startStore(ctx context.Context, directory string, config Config) (*validatedStore, error) {
	return startStoreObserved(ctx, directory, config, nil)
}

func startStoreObserved(
	ctx context.Context,
	directory string,
	config Config,
	observer startupObserver,
) (_ *validatedStore, err error) {
	defer func() { err = classifyStartupError(err) }()
	if ctx == nil || directory == "" {
		return nil, ErrInvalidInput
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, context.Cause(ctx))
	}
	owner, err := acquireDirectoryObserved(directory, observer)
	if err != nil {
		return nil, err
	}
	opened := &validatedStore{owner: owner}
	defer func() {
		if err != nil {
			cleanupErr := opened.close()
			err = errors.Join(err, cleanupErr)
			if cleanupErr != nil {
				err = errors.Join(err, ErrStorage)
			}
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				err = errors.Join(err, context.Cause(ctx))
			}
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, context.Cause(ctx))
	}
	create, err := owner.checkContents()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(owner.path, "queue.sqlite3")
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE | os.O_EXCL
	}
	opened.main, err = os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, errors.Join(ErrStorage, err)
	}
	opened.storage, err = connectStore(ctx, path, config, create)
	if err != nil {
		return nil, err
	}
	if err := verifyOwnedDescriptor(opened.main, path); err != nil {
		return nil, err
	}
	if create {
		if err := opened.storage.initializeObserved(ctx, config, observer); err != nil {
			return nil, err
		}
	}
	started := time.Now()
	opened.metadata, opened.validation.LogicalRecords, err = opened.storage.validate(ctx)
	if err != nil {
		return nil, err
	}
	opened.validation.Duration = time.Since(started)
	if opened.metadata.policy != config.Policy || opened.metadata.durability != config.Durability {
		return nil, ErrConfigMismatch
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, context.Cause(ctx))
	}
	if observer != nil {
		observer.Observe(startupBeforeEpoch)
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, context.Cause(ctx))
	}
	epoch, err := nextIdentity(opened.metadata.epoch)
	if err != nil {
		return nil, err
	}
	// Cancellation cannot interrupt an entered fencing transaction or namespace barrier.
	boundary := context.WithoutCancel(ctx)
	if err := opened.storage.writeEpochObserved(boundary, epoch, observer); err != nil {
		return nil, err
	}
	opened.metadata.epoch = epoch
	if observer != nil {
		observer.Observe(startupAfterEpoch)
	}
	if err := opened.synchronizeCreation(observer); err != nil {
		return nil, err
	}
	if err := opened.storage.selectDurability(boundary, config.Durability); err != nil {
		return nil, err
	}
	if err := owner.verify(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, context.Cause(ctx))
	}
	return opened, nil
}

func (o *directoryOwner) checkContents() (bool, error) {
	entries, err := o.directory.ReadDir(5)
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	if len(entries) > 4 {
		return false, ErrStorage
	}
	hasMain := false
	for _, entry := range entries {
		switch entry.Name() {
		case "queue.sqlite3":
			hasMain = true
		case "LOCK", "queue.sqlite3-wal", "queue.sqlite3-shm":
		default:
			return false, fmt.Errorf("unexpected queue artifact %q: %w", entry.Name(), ErrStorage)
		}
		info, err := entry.Info()
		if err != nil {
			return false, err
		}
		if !info.Mode().IsRegular() {
			return false, ErrStorage
		}
	}
	if hasMain {
		return false, nil
	}
	if !o.lockCreated || len(entries) != 1 || entries[0].Name() != "LOCK" {
		return false, ErrStorage
	}
	return true, nil
}

func (s *store) initializeObserved(
	ctx context.Context,
	config Config,
	observer startupObserver,
) (err error) {
	if observer != nil {
		observer.Observe(startupBeforeInitialize)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	boundary := context.WithoutCancel(ctx)
	if _, err := s.conn.ExecContext(boundary, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, rollbackErr := s.conn.ExecContext(boundary, "ROLLBACK")
			err = errors.Join(err, rollbackErr)
		}
	}()
	if err := s.createSchema(boundary); err != nil {
		return err
	}
	var uuid UUID
	if _, err := rand.Read(uuid[:]); err != nil {
		return err
	}
	policy := encodePolicy(config.Policy)
	values := [metaCleanupMarkers][]byte{}
	for i := range values {
		encoded := encodeUint64(0)
		values[i] = encoded[:]
	}
	for _, key := range []int{metaSchema, metaProfileVersion, metaAccountingVersion} {
		encoded := encodeUint64(1)
		values[key-1] = encoded[:]
	}
	values[metaProfile-1] = []byte(config.Policy.Profile)
	values[metaUUID-1] = uuid[:]
	values[metaPolicy-1] = policy[:]
	values[metaDurability-1] = []byte{byte(config.Durability)}
	for i, value := range values {
		if _, err := s.conn.ExecContext(
			boundary,
			"INSERT INTO meta VALUES(?,?)",
			i+1,
			value,
		); err != nil {
			return err
		}
	}
	if observer != nil {
		observer.Observe(startupDuringInitialize)
	}
	_, err = s.conn.ExecContext(boundary, "COMMIT")
	if err == nil && observer != nil {
		observer.Observe(startupAfterInitialize)
	}
	return err
}

func (s *store) writeEpochObserved(
	ctx context.Context,
	epoch uint64,
	observer startupObserver,
) (err error) {
	if _, err := s.conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	encoded := encodeUint64(epoch)
	if _, err = s.conn.ExecContext(
		ctx,
		"UPDATE meta SET value=? WHERE key=?",
		encoded[:],
		metaEpoch,
	); err != nil {
		_, rollbackErr := s.conn.ExecContext(ctx, "ROLLBACK")
		return errors.Join(err, rollbackErr)
	}
	if observer != nil {
		observer.Observe(startupDuringEpoch)
	}
	_, err = s.conn.ExecContext(ctx, "COMMIT")
	return err
}

func (s *store) selectDurability(ctx context.Context, durability Durability) error {
	value := "2"
	if durability == ProcessCrash {
		value = "1"
	}
	if _, err := s.conn.ExecContext(ctx, "PRAGMA synchronous="+value); err != nil {
		return err
	}
	settings, err := s.readSettings(ctx)
	if err != nil {
		return err
	}
	expected := int64(2)
	if durability == ProcessCrash {
		expected = 1
	}
	if settings.Synchronous != expected {
		return ErrUnsupported
	}
	return nil
}

func (s *validatedStore) synchronizeCreation(observer startupObserver) error {
	path := filepath.Join(s.owner.path, "queue.sqlite3")
	if err := syncOwnedDescriptor(s.main, path); err != nil {
		return err
	}
	if err := syncOwnedDescriptor(s.owner.directory, s.owner.path); err != nil {
		return err
	}
	// A prior creator may have stopped before persisting the parent entry.
	parentPath := filepath.Dir(s.owner.path)
	if err := syncOwnedDescriptor(s.owner.parent, parentPath); err != nil {
		return err
	}
	if observer != nil {
		observer.Observe(startupAfterParentSync)
	}
	return nil
}

func (s *validatedStore) close() error {
	var err error
	if s.storage != nil {
		err = s.storage.close()
	}
	if s.main != nil {
		err = errors.Join(err, s.main.Close())
	}
	return errors.Join(err, s.owner.close())
}

func classifyStartupError(err error) error {
	if err == nil {
		return nil
	}
	for _, category := range []error{
		ErrInvalidInput, ErrConfigMismatch, ErrUnsupported, ErrInUse, ErrExhausted,
		context.Canceled, context.DeadlineExceeded, ErrStorage,
	} {
		if errors.Is(err, category) {
			return err
		}
	}
	return errors.Join(ErrStorage, err)
}

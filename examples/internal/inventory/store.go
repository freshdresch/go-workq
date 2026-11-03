package inventory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
)

// ErrConflict means an existing key is bound to different request bytes or codec.
var ErrConflict = errors.New("inventory identity conflict")

// ErrCapacity means the receipt limit refuses a new key without removing old receipts.
var ErrCapacity = errors.New("inventory receipt capacity reached")

// ErrUnresolved means no usable outcome was established. Retain the original request.
var ErrUnresolved = errors.New("inventory outcome unresolved")

// Result is the original committed result. Duplicate describes this observation only.
type Result struct {
	AdjustmentID string
	Quantity     int64
	Duplicate    bool
}

// Receipt binds a complete key and exact request bytes to their original committed result.
type Receipt struct {
	Key     Key
	Codec   string
	Payload []byte
	Result  Result
}

// Inventory is the persistent state of the fixed widget item.
type Inventory struct {
	Quantity    int64
	Adjustments int64
}

// Store owns one dedicated SQLite connection. Calls are serialized and safe for concurrent use.
// A commit or rollback error permanently fences the connection until owner Close and fresh Open.
// Cancellation keeps cleanup with the caller and can wait for native SQL to finish.
// A cancellation observed before commit rolls back. A successful commit remains success.
// The zero value is invalid. Owners must join callers before Close.
type Store struct {
	failed chan struct{}

	opMu         sync.Mutex
	db           *sql.DB
	conn         *sql.Conn
	receiptLimit int
	failure      error
	closed       bool
	closeErr     error

	// Tests set these before concurrent use. Hooks cannot replace SQL or clear failure.
	stageHook        func(applyStage)
	observeCommit    func(error) error
	observeAdmission func()
}

// Open opens an inventory database in a private operator-owned directory.
// It requires cgo and refuses unsupported builds before touching the filesystem.
// Only an exclusively created path is initialized. Existing files must contain the complete schema.
// With valid input, cancellation visible at entry returns before filesystem work, so a healthy
// retry can initialize a fresh path. Failure after creation starts can leave the database and WAL.
// Open never deletes or reinitializes an incomplete destination. Preserve it for investigation.
// Use a fresh path only with authoritative proof of failed first initialization and no prior
// effects. Otherwise recover receipts and business state together, preserving original source
// identities and bytes. An error, file size, or missing tables alone does not prove it was unused.
// The caller must keep path outside its queue directory and must Close the returned Store.
func Open(ctx context.Context, path string, receiptLimit int) (*Store, error) {
	if !sqliteAvailable {
		return nil, errors.New("inventory requires a cgo-enabled SQLite build")
	}
	if receiptLimit <= 0 {
		return nil, fmt.Errorf("receipt limit must be positive: %w", ErrRejected)
	}
	if path == "" {
		return nil, fmt.Errorf("database path required: %w", ErrRejected)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return nil, fmt.Errorf("resolve database parent: %w", err)
	}
	absolute = filepath.Join(parent, filepath.Base(absolute))
	created := false
	file, err := os.OpenFile(absolute, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err == nil {
		created = true
		if err := file.Close(); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	if !created {
		info, err := os.Lstat(absolute)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Size() == 0 {
			return nil, errors.New("existing inventory database must be a nonempty regular file")
		}
	}
	address := url.URL{Scheme: "file", Path: absolute}
	options := url.Values{
		"_journal_mode": {"WAL"}, "_synchronous": {"FULL"},
		"_busy_timeout": {"250"}, "_foreign_keys": {"on"}, "_txlock": {"immediate"},
	}
	address.RawQuery = options.Encode()
	db, err := sql.Open("sqlite3", address.String())
	if err != nil {
		return nil, fmt.Errorf("open inventory: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	store := &Store{
		db: db, conn: conn, receiptLimit: receiptLimit, failed: make(chan struct{}),
	}
	if err := store.configureAndInitialize(ctx, created); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return store, nil
}

// Failed returns a stable channel closed once when transaction outcome or cleanup is uncertain.
// Closure asks the owner to stop and join callers before Close.
// Normal Close does not signal failure.
func (s *Store) Failed() <-chan struct{} {
	return s.failed
}

func (s *Store) lockOperation() {
	if s.observeAdmission != nil {
		s.observeAdmission()
	}
	s.opMu.Lock()
}

func (s *Store) check(ctx context.Context) error {
	if s.failure != nil {
		return fmt.Errorf("store requires close and reopen: %w", s.failure)
	}
	if s.closed {
		return fmt.Errorf("store closed: %w", ErrUnresolved)
	}
	return ctx.Err()
}

func (s *Store) poison(err error) error {
	if s.failure == nil {
		s.failure = fmt.Errorf("%w: %w", ErrUnresolved, err)
		close(s.failed)
	}
	return s.failure
}

func (s *Store) rollback(tx *sql.Tx, cause error) error {
	if err := tx.Rollback(); err != nil {
		// Cleanup uncertainty replaces definitive rejection semantics. Keep the
		// earlier cause in diagnostics without exposing its sentinel to callers.
		return s.poison(fmt.Errorf("rollback after %v: %w", cause, err))
	}
	return cause
}

// begin requires exclusive operation ownership, or an unpublished Store during initialization.
// The caller must end the returned transaction before releasing that ownership.
func (s *Store) begin(ctx context.Context) (*sql.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Request cancellation must not transfer native cleanup to database/sql's awaitDone.
	tx, err := s.conn.BeginTx(context.WithoutCancel(ctx), nil)
	if err != nil {
		if sqliteBusy(err) {
			return nil, fmt.Errorf("begin transaction: %w: %w", ErrUnresolved, err)
		}
		return nil, s.poison(fmt.Errorf("begin transaction: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return nil, s.rollback(tx, err)
	}
	return tx, nil
}

func (s *Store) commit(ctx context.Context, tx *sql.Tx) error {
	if err := ctx.Err(); err != nil {
		return s.rollback(tx, err)
	}
	err := tx.Commit()
	if s.observeCommit != nil {
		err = errors.Join(err, s.observeCommit(err))
	}
	if err != nil {
		return s.poison(fmt.Errorf("commit: %w", err))
	}
	return nil
}

// Close releases the dedicated connection and database after all callers have joined.
// It waits for native cleanup and returns the retained closure result on repeated calls.
func (s *Store) Close() error {
	s.lockOperation()
	defer s.opMu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.closeErr = errors.Join(s.conn.Close(), s.db.Close())
	return s.closeErr
}

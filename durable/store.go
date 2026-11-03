package durable

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/mattn/go-sqlite3"
)

// store owns exactly one retained SQL connection. Its caller serializes all use.
// Directory ownership, metadata validation, and publication belong to startup.
type store struct {
	db   *sql.DB
	conn *sql.Conn
}

type sqliteSettings struct {
	JournalMode    string
	Synchronous    int64
	AutoVacuum     int64
	PageSize       int64
	CacheSize      int64
	MmapSize       int64
	TempStore      int64
	AutoCheckpoint int64
	BusyTimeout    int64
	MaxPageCount   int64
	AutomaticIndex int64
}

func openStoreConnection(ctx context.Context, path string, create bool) (*store, error) {
	mode := "rw"
	if create {
		mode = "rwc"
	}
	location := url.URL{Scheme: "file", Path: path}
	query := url.Values{"mode": {mode}, "_busy_timeout": {"0"}}
	if name := selectedFaultVFS(path); name != "" {
		query.Set("vfs", name)
	}
	location.RawQuery = query.Encode()
	db, err := sql.Open("sqlite3", location.String())
	if err != nil {
		return nil, fmt.Errorf("open SQLite: %w", classifySQLError(err))
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("retain connection: %w", classifySQLError(err)),
			classifySQLError(db.Close()),
		)
	}
	return &store{db: db, conn: conn}, nil
}

func (s *store) close() error {
	return errors.Join(s.conn.Close(), s.db.Close())
}

func (s *store) configureExisting(
	ctx context.Context,
	path string,
	config Config,
	metadata storeMetadata,
) error {
	if metadata.policy != config.Policy || metadata.durability != config.Durability {
		return ErrConfigMismatch
	}
	if err := s.configureTuning(ctx, config.Runtime.SQLite); err != nil {
		return err
	}
	return s.configurePolicy(ctx, path, metadata.policy)
}

func (s *store) bootstrap(ctx context.Context, create bool) error {
	if create {
		for _, statement := range []string{
			"PRAGMA page_size=4096", "PRAGMA auto_vacuum=INCREMENTAL",
		} {
			if _, err := s.conn.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("initialize SQLite: %w", classifySQLError(err))
			}
		}
	}
	var journal string
	if err := s.conn.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journal); err != nil {
		return fmt.Errorf("enable WAL: %w", classifySQLError(err))
	}
	if journal != "wal" {
		return fmt.Errorf("journal mode %q: %w", journal, ErrUnsupported)
	}
	return s.configureTuning(ctx, SQLiteConfig{Cache: 64 << 20, BusyTimeout: 5 * time.Second})
}

func (s *store) configureTuning(ctx context.Context, tuning SQLiteConfig) error {
	settings := []struct {
		name  string
		value int64
	}{
		{"synchronous", 2},
		{"cache_size", -int64(tuning.Cache / 1024)},
		{"mmap_size", int64(tuning.Mmap)},
		{"temp_store", 2},
		{"wal_autocheckpoint", 0},
		{"busy_timeout", int64(tuning.BusyTimeout / time.Millisecond)},
		{"automatic_index", 0},
	}
	for _, setting := range settings {
		// Names are fixed above and values passed bounded integer validation.
		statement := "PRAGMA " + setting.name + "=" + strconv.FormatInt(setting.value, 10)
		if _, err := s.conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("set %s: %w", setting.name, classifySQLError(err))
		}
		var actual int64
		if err := s.conn.QueryRowContext(ctx, "PRAGMA "+setting.name).Scan(&actual); err != nil {
			return fmt.Errorf("read %s: %w", setting.name, classifySQLError(err))
		}
		if actual != setting.value {
			return fmt.Errorf(
				"%s=%d, requested %d: %w",
				setting.name, actual, setting.value, ErrUnsupported,
			)
		}
	}
	actual, err := s.readSettings(ctx)
	if err != nil {
		return err
	}
	if actual.AutoVacuum != 2 || actual.PageSize != int64(pageSize) {
		return fmt.Errorf("database page format: %w", ErrUnsupported)
	}
	return nil
}

func (s *store) configurePolicy(ctx context.Context, path string, policy DurablePolicy) error {
	if err := s.validateBackendRowLimit(policy); err != nil {
		return err
	}
	requested := int64(policy.DatabaseLimit / pageSize)
	var actual int64
	statement := "PRAGMA max_page_count=" + strconv.FormatInt(requested, 10)
	if err := s.conn.QueryRowContext(ctx, statement).Scan(&actual); err != nil {
		return classifySQLError(err)
	}
	if actual > requested {
		return corrupt("database exceeds stored page limit")
	}
	if actual != requested {
		return ErrUnsupported
	}
	var pages int64
	if err := s.conn.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pages); err != nil {
		return fmt.Errorf("read page count: %w", classifySQLError(err))
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("database size: %w: %w", ErrStorage, err)
	}
	if pages < 0 || pages > actual {
		return corrupt("database exceeds stored page limit")
	}
	if info.Size() < 0 || uint64(info.Size()) > uint64(policy.DatabaseLimit) {
		return corrupt("database exceeds stored main limit")
	}
	return nil
}

func (s *store) readSettings(ctx context.Context) (sqliteSettings, error) {
	var settings sqliteSettings
	row := s.conn.QueryRowContext(ctx, "PRAGMA journal_mode")
	if err := row.Scan(&settings.JournalMode); err != nil {
		return sqliteSettings{}, fmt.Errorf("read journal mode: %w", classifySQLError(err))
	}
	fields := []struct {
		name  string
		value *int64
	}{
		{"synchronous", &settings.Synchronous}, {"auto_vacuum", &settings.AutoVacuum},
		{"page_size", &settings.PageSize}, {"cache_size", &settings.CacheSize},
		{"mmap_size", &settings.MmapSize}, {"temp_store", &settings.TempStore},
		{"wal_autocheckpoint", &settings.AutoCheckpoint}, {"busy_timeout", &settings.BusyTimeout},
		{"max_page_count", &settings.MaxPageCount}, {"automatic_index", &settings.AutomaticIndex},
	}
	for _, field := range fields {
		if err := s.conn.QueryRowContext(ctx, "PRAGMA "+field.name).Scan(field.value); err != nil {
			return sqliteSettings{}, fmt.Errorf("read %s: %w", field.name, classifySQLError(err))
		}
	}
	return settings, nil
}

func (s *store) autoCommit() (bool, error) {
	var autoCommit bool
	err := s.conn.Raw(func(raw any) error {
		connection, ok := raw.(*sqlite3.SQLiteConn)
		if !ok {
			return ErrUnsupported
		}
		autoCommit = connection.AutoCommit()
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("transaction state: %w: %w", ErrStorage, err)
	}
	return autoCommit, nil
}

// classifySQLError classifies one fresh SQL failure before independent errors are joined.
// The driver returns context sentinels for canceled calls. Never infer cancellation
// from ctx.Err(), which could hide an independent storage failure.
func classifySQLError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return errors.Join(ErrStorage, err)
}

func (s *store) validateBackendRowLimit(policy DurablePolicy) error {
	row, err := payloadRowLimit(policy)
	if err != nil {
		return err
	}
	return s.conn.Raw(func(raw any) error {
		connection, ok := raw.(*sqlite3.SQLiteConn)
		if !ok {
			return ErrUnsupported
		}
		limit := connection.GetLimit(sqlite3.SQLITE_LIMIT_LENGTH)
		if limit < 0 || row > Bytes(limit) {
			return fmt.Errorf("effective SQLite record length %d: %w", limit, ErrUnsupported)
		}
		return nil
	})
}

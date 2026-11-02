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

func connectStore(ctx context.Context, path string, config Config, create bool) (*store, error) {
	if ctx == nil || path == "" {
		return nil, ErrInvalidInput
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	mode := "rw"
	if create {
		mode = "rwc"
	}
	location := url.URL{Scheme: "file", Path: path}
	query := url.Values{"mode": {mode}, "_busy_timeout": {"0"}}
	location.RawQuery = query.Encode()
	db, err := sql.Open("sqlite3", location.String())
	if err != nil {
		return nil, fmt.Errorf("open SQLite: %w: %w", ErrStorage, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("retain connection: %w: %w", ErrStorage, err), db.Close())
	}
	storage := &store{db: db, conn: conn}
	if err := storage.configure(ctx, path, config, create); err != nil {
		return nil, errors.Join(err, storage.close())
	}
	return storage, nil
}

func (s *store) close() error {
	return errors.Join(s.conn.Close(), s.db.Close())
}

func (s *store) configure(ctx context.Context, path string, config Config, create bool) error {
	if create {
		for _, statement := range []string{
			"PRAGMA page_size=4096", "PRAGMA auto_vacuum=INCREMENTAL",
		} {
			if _, err := s.conn.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("initialize SQLite: %w: %w", ErrStorage, err)
			}
		}
	}
	var journal string
	if err := s.conn.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journal); err != nil {
		return fmt.Errorf("enable WAL: %w: %w", ErrStorage, err)
	}
	if journal != "wal" {
		return fmt.Errorf("journal mode %q: %w", journal, ErrUnsupported)
	}
	tuning := config.Runtime.SQLite
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
		{"max_page_count", int64(config.Policy.DatabaseLimit / pageSize)},
		{"automatic_index", 0},
	}
	for _, setting := range settings {
		// Names are fixed above and values passed bounded integer validation.
		statement := "PRAGMA " + setting.name + "=" + strconv.FormatInt(setting.value, 10)
		if _, err := s.conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("set %s: %w: %w", setting.name, ErrStorage, err)
		}
		var actual int64
		if err := s.conn.QueryRowContext(ctx, "PRAGMA "+setting.name).Scan(&actual); err != nil {
			return fmt.Errorf("read %s: %w: %w", setting.name, ErrStorage, err)
		}
		if actual != setting.value {
			return fmt.Errorf("%s=%d, requested %d: %w",
				setting.name, actual, setting.value, ErrUnsupported)
		}
	}
	actual, err := s.readSettings(ctx)
	if err != nil {
		return err
	}
	if actual.AutoVacuum != 2 || actual.PageSize != int64(pageSize) {
		return fmt.Errorf("database page format: %w", ErrUnsupported)
	}
	var pages int64
	if err := s.conn.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pages); err != nil {
		return fmt.Errorf("read page count: %w: %w", ErrStorage, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("database size: %w: %w", ErrStorage, err)
	}
	if pages > actual.MaxPageCount || uint64(info.Size()) > uint64(config.Policy.DatabaseLimit) {
		return fmt.Errorf("database exceeds main limit: %w", ErrConfigMismatch)
	}
	return nil
}

func (s *store) readSettings(ctx context.Context) (sqliteSettings, error) {
	var settings sqliteSettings
	row := s.conn.QueryRowContext(ctx, "PRAGMA journal_mode")
	if err := row.Scan(&settings.JournalMode); err != nil {
		return sqliteSettings{}, fmt.Errorf("read journal mode: %w: %w", ErrStorage, err)
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
			return sqliteSettings{}, fmt.Errorf("read %s: %w: %w", field.name, ErrStorage, err)
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

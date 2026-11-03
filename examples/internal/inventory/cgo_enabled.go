//go:build cgo

package inventory

import (
	"errors"

	"github.com/mattn/go-sqlite3"
)

const sqliteAvailable = true

func sqliteBusy(err error) bool {
	var sqliteErr sqlite3.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	return sqliteErr.Code == sqlite3.ErrBusy || sqliteErr.Code == sqlite3.ErrLocked
}

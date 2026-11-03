//go:build cgo

package inventory

import _ "github.com/mattn/go-sqlite3" // Register the pinned SQLite driver.

const sqliteAvailable = true

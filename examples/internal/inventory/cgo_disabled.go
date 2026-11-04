//go:build !cgo

package inventory

const sqliteAvailable = false

func sqliteBusy(error) bool {
	return false
}

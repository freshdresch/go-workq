//go:build durable_faulttest && linux && amd64 && cgo

package durable

import (
	"os"
	"sync/atomic"

	"github.com/freshdresch/go-data-structures/workq/durable/internal/sqlitefault"
)

// Tests install one fixture before starting storage and remove it after joining storage.
var faultController atomic.Pointer[sqlitefault.Controller]

func selectedFaultVFS(path string) string {
	controller := faultController.Load()
	if controller == nil {
		return ""
	}
	return controller.VFS(path)
}

func observeOwnedSync(file *os.File, path string) (func(error) error, error) {
	controller := faultController.Load()
	if controller == nil {
		return func(err error) error { return err }, nil
	}
	return controller.ObserveOwned(file, path)
}

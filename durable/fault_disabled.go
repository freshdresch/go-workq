//go:build !durable_faulttest

package durable

import "os"

func selectedFaultVFS(string) string { return "" }

func observeOwnedSync(*os.File, string) (func(error) error, error) {
	return unchangedSyncResult, nil
}

func unchangedSyncResult(err error) error { return err }

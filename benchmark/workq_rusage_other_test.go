//go:build !unix

package benchmark

import "time"

// processCPU reports false, because the process's CPU time is read only on Unix.
func processCPU() (time.Duration, bool) {
	return 0, false
}

//go:build unix

package benchmark

import (
	"syscall"
	"time"
)

// processCPU returns the process's user plus system CPU time so far, and false when it cannot
// be read.
func processCPU() (time.Duration, bool) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, false
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano()), true
}

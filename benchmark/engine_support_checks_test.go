// Adapter checks extracted from go-data-structures. See ORIGIN.md.
package benchmark

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
)

func TestModeRingTopology(t *testing.T) {
	for _, mode := range benchModes {
		for _, single := range []bool{false, true} {
			for _, relaxed := range []bool{false, true} {
				testMode := mode
				testMode.relaxed = relaxed
				target := newTestRing[int64](t, testMode, newRunState(nil), single)
				api := "Burst"
				if single {
					api = "Single"
				}
				want := fmt.Sprintf("*benchmark.%s%sQueue[int64]", strings.ToLower(mode.name), api)
				if got := fmt.Sprintf("%T", target); got != want {
					t.Errorf(
						"mode=%s single=%v relaxed=%v: got %s, want %s",
						mode.name, single, relaxed, got, want,
					)
				}
			}
		}
	}
}

func TestRingAdapterOutcomes(t *testing.T) {
	for _, expected := range []error{dpdk.ErrFull, dpdk.ErrEmpty} {
		opposite := dpdk.ErrFull
		if expected == opposite {
			opposite = dpdk.ErrEmpty
		}
		for _, original := range []error{nil, expected, opposite, errors.New("unexpected"),
			fmt.Errorf("wrapped: %w", expected)} {
			state := newRunState(nil)
			target := &ringSingleQueue{state: state}
			want := 0
			if original == nil {
				want = 1
			}
			if got := target.movedOrFail(original, expected); got != want {
				t.Errorf("moved = %d, want %d", got, want)
			}
			wantErr := original
			if original == expected {
				wantErr = nil
			}
			if state.firstError() != wantErr {
				t.Errorf("stored error = %v, want %v", state.firstError(), wantErr)
			}
			if state.aborted() != (wantErr != nil) {
				t.Errorf("abort = %v", state.aborted())
			}
		}
	}
}

func newTestRing[T any](
	t *testing.T,
	mode benchMode,
	state *runState,
	single bool,
	extra ...dpdk.Option,
) queue[T] {
	t.Helper()
	created, err := newModeRing[T](mode, state, single, extra...)
	if err != nil {
		t.Fatalf("build ring for %s: %v", mode.name, err)
	}
	return created
}

// abortTotal is how many elements each abort test sends, far more than any implementation holds,
// so goroutines are still blocked or retrying when the run aborts.
const abortTotal = 100_000

// goroutineDeadline bounds how long an abort test waits for every goroutine to return.
const goroutineDeadline = 5 * time.Second

// errStubAbort is the error abortingQueue fails its run with.
var errStubAbort = errors.New("stub queue failed")

// checkRunError fails the test unless err's whole message is want.
func checkRunError(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("run returned no error, want %q", want)
	}
	if err.Error() != want {
		t.Fatalf("run returned %q, want %q", err.Error(), want)
	}
}

// waitForGoroutines polls until no more than limit goroutines are running, and fails the test if
// that does not happen within goroutineDeadline. No test in this package runs in parallel, so the
// count reflects only the calling test.
func waitForGoroutines(t *testing.T, limit int) {
	t.Helper()
	deadline := time.Now().Add(goroutineDeadline)
	for runtime.NumGoroutine() > limit {
		if time.Now().After(deadline) {
			t.Fatalf(
				"%d goroutines still running after %v, want at most %d",
				runtime.NumGoroutine(),
				goroutineDeadline,
				limit,
			)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

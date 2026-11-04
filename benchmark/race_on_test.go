//go:build race

package benchmark

// raceEnabled reports whether the test binary was built with -race, so tests that the race
// detector distorts can skip.
const raceEnabled = true

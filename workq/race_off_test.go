//go:build !race

package workq

// raceEnabled reports whether the test binary was built with -race, so tests can scale their
// work down under it.
const raceEnabled = false

// Package wait defines the value used to configure queue waiting policies.
package wait

// Strategy selects polling counts and whether a waiting caller may park.
// The zero value skips bounded polling and allows parking. Counts must be nonnegative.
type Strategy struct {
	// Spins is the number of empty polls before the first yield.
	Spins int
	// Yields is the number of polls with runtime.Gosched between them, after the spins.
	Yields int
	// NeverPark makes the caller keep yielding after the yields instead of parking. It stays
	// registered as a poller for the whole wait, so the other side never takes a mutex to wake
	// it, at the cost of the CPU time it spends yielding.
	NeverPark bool
}

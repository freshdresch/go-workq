package workq

import (
	"sync"
	"sync/atomic"
)

// sideWord is one side's state word: pollers in bits 0-30, parked in bits 32-62, and closed in
// bit 63.
type sideWord uint64

const (
	onePoller  sideWord = 1
	oneParked  sideWord = 1 << 32
	closedFlag sideWord = 1 << 63
	countMask  sideWord = 1<<31 - 1
)

func (word sideWord) pollers() uint32 { return uint32(word & countMask) }
func (word sideWord) parked() uint32  { return uint32((word >> 32) & countMask) }
func (word sideWord) closed() bool    { return word&closedFlag != 0 }

// needsWake reports whether no registered poller is left to find new items while some waiter
// is parked. Consumer groups may additionally replenish while pollers remain.
func (word sideWord) needsWake() bool {
	return word.pollers() == 0 && word.parked() > 0
}

type waiter struct {
	token  chan struct{} // buffer of 1, holds a wake token as invariant I3 describes
	prev   *waiter
	next   *waiter
	listed bool
}

func newWaiter() *waiter {
	return &waiter{token: make(chan struct{}, 1)}
}

// waitSide tracks callers waiting for items or room. See "State and invariants" and
// "Transitions" in wakeup.md for invariants I1-I3 and their accounting boundaries.
type waitSide struct {
	// The padding keeps mu and the list off word's cache line, because producers load word on
	// every push. A struct that embeds a waitSide must pad before it, too.
	word   atomic.Uint64
	_      [128]byte
	mu     sync.Mutex
	oldest *waiter
	newest *waiter
	pool   sync.Pool
	// Tests force count races and early recipient exits without scheduler timing.
	beforeGroupCAS  func()
	afterGroupToken func()
}

// getWaiter returns a waiter that is unlisted, with nil links and an empty channel.
func (side *waitSide) getWaiter() *waiter {
	if pooled, ok := side.pool.Get().(*waiter); ok {
		return pooled
	}
	return newWaiter()
}

// putWaiter returns caller to the pool. Caller must be unlisted, with nil links and an empty
// channel, which invariant I3 guarantees once its owner has received any token it was sent.
func (side *waitSide) putWaiter(caller *waiter) {
	side.pool.Put(caller)
}

func (side *waitSide) load() sideWord {
	return sideWord(side.word.Load())
}

func (side *waitSide) compareAndSwap(current sideWord, updated sideWord) bool {
	return side.word.CompareAndSwap(uint64(current), uint64(updated))
}

// register adds the calling consumer to pollers at the start of the yield phase, keeping
// invariant I2. When closed is set, it registers nothing and returns true, and the caller goes
// to the drain check instead.
func (side *waitSide) register(counters *diagnosticsSideCounters) (closed bool) {
	for {
		word := side.load()
		if word.closed() {
			return true
		}
		if side.compareAndSwap(word, word+onePoller) {
			if counters != nil {
				counters.registrations.Add(1)
			}
			return false
		}
	}
}

// wakeCheck is the wake check a caller runs right after an engine call that moved items. It
// wakes a waiter whenever no poller is registered to see those items, so no wake is skipped on
// the belief that another caller took care of it. It reports whether closed was set, and wakes
// on the counts alone either way, because a waiter woken after closed is set is also woken by
// done and takes its token as it leaves.
func (side *waitSide) wakeCheck(counters *diagnosticsSideCounters) (closed bool) {
	word := side.load()
	if word.needsWake() {
		side.wakeOne(counters)
	}
	return word.closed()
}

// wakeOne serializes selection with list cleanup. See "Transitions" in wakeup.md.
// It reports whether it sent a token, after releasing mu.
func (side *waitSide) wakeOne(counters *diagnosticsSideCounters) bool {
	side.mu.Lock()
	woke := side.wakeOneLocked(counters)
	side.mu.Unlock()
	return woke
}

// wakeOneLocked requires mu held. See "Transitions" in wakeup.md. It moves one waiter
// from parked to pollers in a single compare-and-swap, so the woken consumer is registered
// before it runs and concurrent wake checks see a poller and skip (I2). It removes the waiter
// in the same critical section (I1) and only then sends its token (I3), so the send never
// blocks. It reports true only after the send. A retry that finds a poller reports false.
func (side *waitSide) wakeOneLocked(counters *diagnosticsSideCounters) bool {
	if counters != nil {
		counters.wakeCalls[WakeOne].Add(1)
	}
	for {
		word := side.load()
		if !word.needsWake() {
			return false
		}
		if side.compareAndSwap(word, word+onePoller-oneParked) {
			if counters != nil {
				counters.wakeTransfers[WakeOne].Add(1)
				counters.wakeRecipients.Add(1)
			}
			woken := side.removeForWake()
			select {
			case woken.token <- struct{}{}:
				if counters != nil {
					counters.tokensSent.Add(1)
				}
				return true
			default:
				panic("workq: waiter already holds a wake token")
			}
		}
	}
}

// removeForWake requires mu held and removes the waiter a wake goes to. The choice of waiter
// lives only here, because neither the invariants nor the proof depend on it, and benchmark 4
// compares oldest first with newest first.
func (side *waitSide) removeForWake() *waiter {
	woken := side.oldest
	side.unlink(woken)
	return woken
}

// unlink requires mu held and removes caller from the list. Every removal goes through it, so
// a removed waiter always ends unlisted with nil links, ready for reuse.
func (side *waitSide) unlink(caller *waiter) {
	if caller.prev == nil {
		side.oldest = caller.next
	} else {
		caller.prev.next = caller.next
	}
	if caller.next == nil {
		side.newest = caller.prev
	} else {
		caller.next.prev = caller.prev
	}
	caller.prev = nil
	caller.next = nil
	caller.listed = false
}

// leavePolling releases a registration as described in "Transitions" in wakeup.md. It runs on
// every exit from polling except parking (I2). Peek runs only after the decrement, because a
// count taken before it can miss an item made visible between the count and the decrement.
// It reports whether its cleanup sent a wake token.
func (side *waitSide) leavePolling(counters *diagnosticsSideCounters, peek func() bool) bool {
	return side.leavePollingWith(func(left sideWord) bool {
		if left.needsWake() && peek() {
			return side.wakeOne(counters)
		}
		return false
	})
}

// leavePollingWith drops the registration before cleanup samples engine visibility.
func (side *waitSide) leavePollingWith(cleanup func(sideWord) bool) bool {
	for {
		word := side.load()
		if word.pollers() == 0 {
			panic("workq: leavePolling without a registered poller")
		}
		left := word - onePoller
		if side.compareAndSwap(word, left) {
			return cleanup(left)
		}
	}
}

// enlistPosition chooses an end of the wait list without changing wake selection.
type enlistPosition bool

const (
	atNewest enlistPosition = false
	atOldest enlistPosition = true
)

// enlist begins parking as described in "Transitions" in wakeup.md. It moves the caller from
// pollers to parked and links it at position in one critical section (I1, I2). When closed
// is set, it returns true and changes nothing, so the caller is still registered and must run
// leavePolling.
func (side *waitSide) enlist(
	counters *diagnosticsSideCounters,
	caller *waiter,
	position enlistPosition,
) (closed bool) {
	side.mu.Lock()
	defer side.mu.Unlock()
	for {
		word := side.load()
		if word.closed() {
			if counters != nil {
				counters.parkEnlistClosed.Add(1)
			}
			return true
		}
		if word.pollers() == 0 {
			panic("workq: enlist without a registered poller")
		}
		if side.compareAndSwap(word, word-onePoller+oneParked) {
			if counters != nil {
				counters.parkEnlistments.Add(1)
				if position == atOldest {
					counters.producerOldestReenlistments.Add(1)
				}
			}
			break
		}
	}
	if position == atOldest {
		caller.next = side.oldest
		if side.oldest == nil {
			side.newest = caller
		} else {
			side.oldest.prev = caller
		}
		side.oldest = caller
	} else {
		caller.prev = side.newest
		if side.newest == nil {
			side.oldest = caller
		} else {
			side.newest.next = caller
		}
		side.newest = caller
	}
	caller.listed = true
	return false
}

// leaveList handles a parked caller's exit for a reason other than a token. See
// "Cancellation and close" in wakeup.md. MovedItems reports whether its extra dequeue attempt
// moved items. A caller still listed was chosen by no waker, so it owes a wake only when it
// took items and more remain. A caller already removed was registered as a poller by its waker,
// so it takes the token (I3) and leaves polling (I2). Both paths report whether they sent
// another caller a token after completing cleanup.
func (side *waitSide) leaveList(
	counters *diagnosticsSideCounters,
	caller *waiter,
	movedItems bool,
	peek func() bool,
) bool {
	return side.leaveListWith(counters, caller, movedItems, func(word sideWord) bool {
		if word.needsWake() && peek() {
			return side.wakeOne(counters)
		}
		return false
	})
}

// leaveListWith drains a selected caller's token before releasing its registration.
// A still-listed caller runs cleanup only after an extra attempt moved items.
func (side *waitSide) leaveListWith(
	counters *diagnosticsSideCounters,
	caller *waiter,
	movedItems bool,
	cleanup func(sideWord) bool,
) bool {
	side.mu.Lock()
	if !caller.listed {
		side.mu.Unlock()
		<-caller.token
		if counters != nil {
			counters.tokensDrainedByCleanup.Add(1)
		}
		return side.leavePollingWith(cleanup)
	}
	side.unlink(caller)
	for {
		word := side.load()
		if word.parked() == 0 {
			panic("workq: leaveList with no parked waiter")
		}
		if side.compareAndSwap(word, word-oneParked) {
			break
		}
	}
	side.mu.Unlock()
	if movedItems {
		return cleanup(side.load())
	}
	return false
}

// setClosedLocked requires mu held and sets closed, so that enlist under the same mutex either
// sees it or has already listed its caller. It reports whether this call was the one that set
// it.
func (side *waitSide) setClosedLocked() (first bool) {
	for {
		word := side.load()
		if word.closed() {
			return false
		}
		if side.compareAndSwap(word, word|closedFlag) {
			return true
		}
	}
}

// wakeGroup transfers one bounded group before any recipient can release its registration.
// The target stays fixed across CAS retries and recipients never refill this invocation.
// See "Consumer wake groups" in wakeup.md for the target and transfer boundaries.
func (side *waitSide) wakeGroup(counters *diagnosticsConsumerCounters, target uint32) bool {
	if counters != nil {
		counters.wakeCalls[WakeGroup].Add(1)
		counters.groupTarget.record(uint64(target))
	}
	side.mu.Lock()
	defer side.mu.Unlock()
	for {
		word := side.load()
		if word.parked() == 0 || word.pollers() >= target {
			if counters != nil {
				counters.groupActualSize.record(0)
			}
			return false
		}
		if sideWord(target) > countMask {
			panic("workq: consumer wake target exceeds count limit")
		}
		count := min(word.parked(), target-word.pollers())
		transfer := sideWord(count)
		if transfer > countMask-sideWord(word.pollers()) {
			panic("workq: consumer wake group exceeds count limit")
		}
		if side.beforeGroupCAS != nil {
			side.beforeGroupCAS()
		}
		if !side.compareAndSwap(word, word+transfer-transfer*oneParked) {
			continue
		}
		if counters != nil {
			counters.wakeTransfers[WakeGroup].Add(1)
			counters.wakeRecipients.Add(uint64(count))
			counters.groupActualSize.record(uint64(count))
		}
		for range count {
			woken := side.removeForWake()
			select {
			case woken.token <- struct{}{}:
				if counters != nil {
					counters.tokensSent.Add(1)
				}
				if side.afterGroupToken != nil {
					side.afterGroupToken()
				}
			default:
				panic("workq: waiter already holds a wake token")
			}
		}
		return true
	}
}

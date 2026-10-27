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
// is parked, which is the only state in which a wake is owed.
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

// waitSide is one side of the wakeup protocol in the design's "Waking without losing a wakeup"
// section. The consumer side holds callers waiting for items, and the producer side holds
// callers waiting for room. The methods follow the design's procedures and keep invariants I1,
// I2, and I3.
type waitSide struct {
	// The padding keeps mu and the list off word's cache line, because producers load word on
	// every push. A struct that embeds a waitSide must pad before it, too.
	word   atomic.Uint64
	_      [128]byte
	mu     sync.Mutex
	oldest *waiter
	newest *waiter
	pool   sync.Pool
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
func (side *waitSide) register() (closed bool) {
	for {
		word := side.load()
		if word.closed() {
			return true
		}
		if side.compareAndSwap(word, word+onePoller) {
			return false
		}
	}
}

// wakeCheck is the wake check a caller runs right after an engine call that moved items. It
// wakes a waiter whenever no poller is registered to see those items, so no wake is skipped on
// the belief that another caller took care of it. It reports whether closed was set, and wakes
// on the counts alone either way, because a waiter woken after closed is set is also woken by
// done and takes its token as it leaves.
func (side *waitSide) wakeCheck() (closed bool) {
	word := side.load()
	if word.needsWake() {
		side.wakeOne()
	}
	return word.closed()
}

// wakeOne is the design's Wake one procedure, taking mu around wakeOneLocked.
func (side *waitSide) wakeOne() {
	side.mu.Lock()
	side.wakeOneLocked()
	side.mu.Unlock()
}

// wakeOneLocked is the design's Wake one procedure and requires mu held. It moves one waiter
// from parked to pollers in a single compare-and-swap, so the woken consumer is registered
// before it runs and concurrent wake checks see a poller and skip (I2). It removes the waiter
// in the same critical section (I1) and only then sends its token (I3), so the send never
// blocks. A retry that finds a registered poller does nothing.
func (side *waitSide) wakeOneLocked() {
	for {
		word := side.load()
		if !word.needsWake() {
			return
		}
		if side.compareAndSwap(word, word+onePoller-oneParked) {
			woken := side.removeForWake()
			select {
			case woken.token <- struct{}{}:
			default:
				panic("workq: waiter already holds a wake token")
			}
			return
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

// leavePolling is the design's Leave polling procedure, which a registered consumer runs on
// every exit from polling except parking (I2). Peek runs only after the decrement, because a
// count taken before it can miss an item made visible between the count and the decrement.
func (side *waitSide) leavePolling(peek func() bool) {
	var left sideWord
	for {
		word := side.load()
		if word.pollers() == 0 {
			panic("workq: leavePolling without a registered poller")
		}
		left = word - onePoller
		if side.compareAndSwap(word, left) {
			break
		}
	}
	if left.needsWake() && peek() {
		side.wakeOne()
	}
}

// enlist is the list step of the design's Park procedure. It moves the registered caller from
// pollers to parked and appends it to the list in one critical section (I1, I2). When closed
// is set, it returns true and changes nothing, so the caller is still registered and must run
// leavePolling.
func (side *waitSide) enlist(caller *waiter) (closed bool) {
	side.mu.Lock()
	defer side.mu.Unlock()
	for {
		word := side.load()
		if word.closed() {
			return true
		}
		if word.pollers() == 0 {
			panic("workq: enlist without a registered poller")
		}
		if side.compareAndSwap(word, word-onePoller+oneParked) {
			break
		}
	}
	caller.prev = side.newest
	if side.newest == nil {
		side.oldest = caller
	} else {
		side.newest.next = caller
	}
	side.newest = caller
	caller.listed = true
	return false
}

// leaveList is the design's Leave the list procedure, which a parked caller runs when it stops
// waiting for a reason other than a token. MovedItems reports whether its extra dequeue attempt
// moved items. A caller still listed was chosen by no waker, so it owes a wake only when it
// took items and more remain. A caller already removed was registered as a poller by its waker,
// so it takes the token (I3) and leaves polling (I2).
func (side *waitSide) leaveList(caller *waiter, movedItems bool, peek func() bool) {
	side.mu.Lock()
	if !caller.listed {
		side.mu.Unlock()
		<-caller.token
		side.leavePolling(peek)
		return
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
	if movedItems && side.load().needsWake() && peek() {
		side.wakeOne()
	}
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

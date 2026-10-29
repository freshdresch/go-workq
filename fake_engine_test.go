package workq

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/ring"
)

// engineCallKind names one kind of engine call. A zero-length burst on either side is a peek.
type engineCallKind int

const (
	callEnqueue engineCallKind = iota
	callEnqueueBurst
	callDequeue
	callDequeueBurst
	callPeek
	engineCallKinds
)

// engineCall identifies one engine call to a hook: its kind and its count within that kind,
// from 1.
type engineCall struct {
	kind  engineCallKind
	count int
	// transferKind identifies the originating burst only for a probe.
	transferKind engineCallKind
}

// scriptedEngine is an engine over a slice that tests drive through hooks, so a test can place
// a Close or a new item at an exact point between two engine calls. Like the ring queues, each
// burst moves at most its side's batch limit. The limits start at the capacity, and
// newTestQueue sets them to the queue's resolved limits.
type scriptedEngine[T any] struct {
	capacity int

	maxEnqueueBatch int
	maxDequeueBatch int

	mu sync.Mutex
	// visible holds the items dequeues and peeks can see, oldest first. held holds the items
	// pushed while holding, which come after every visible item.
	visible    []T
	held       []T
	holding    bool
	maxMove    int
	forceEmpty bool
	calls      [engineCallKinds]int
	callLog    []engineCall
	before     func(call engineCall)
	after      func(call engineCall)
}

func newScriptedEngine[T any](capacity int) *scriptedEngine[T] {
	return &scriptedEngine[T]{
		capacity:        capacity,
		maxEnqueueBatch: capacity,
		maxDequeueBatch: capacity,
	}
}

// newTestQueue builds a Queue over fake with the given options, as New would over a ring queue,
// and gives fake the queue's batch limits, as New gives them to a ring queue.
func newTestQueue[T any](t *testing.T, fake *scriptedEngine[T], options ...Option) *Queue[T] {
	t.Helper()
	cfg, err := resolveConfig(fake.Capacity(), options)
	require.NoError(t, err)
	fake.maxEnqueueBatch = cfg.maxPushBatch
	fake.maxDequeueBatch = cfg.maxReceiveBatch
	return newQueue(fake, cfg)
}

// setHooks sets the hooks every later call runs. Before runs before the call reads or changes
// state, and after runs once the call has decided its result, before it returns. Either may be
// nil. Hooks run without mu held, so they may call any method of the fake, Close, or a wake
// check, but nothing a hook does changes the result of the call it runs in, except forceEmpty
// from a before hook.
func (fake *scriptedEngine[T]) setHooks(
	before func(call engineCall),
	after func(call engineCall),
) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.before = before
	fake.after = after
}

// setForceEmpty makes the next call to decide its result move nothing and report nothing
// available or free. A before hook calls it to force the call it runs in, which holds only
// while no other call runs between the hook and its call.
func (fake *scriptedEngine[T]) setForceEmpty() {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.forceEmpty = true
}

// holdVisibility makes later pushes invisible to dequeues and peeks until releaseVisibility, as
// a frontier producer stalled before an item holds back every item reserved after it.
func (fake *scriptedEngine[T]) holdVisibility() {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.holding = true
}

// releaseVisibility makes every held item visible, behind the items already visible.
func (fake *scriptedEngine[T]) releaseVisibility() {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.holding = false
	fake.visible = append(fake.visible, fake.held...)
	fake.held = nil
}

// setMaxMove limits how many items one EnqueueBurst moves. Zero removes the limit.
func (fake *scriptedEngine[T]) setMaxMove(maxMove int) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.maxMove = maxMove
}

// callCount returns how many calls of kind have started.
func (fake *scriptedEngine[T]) callCount(kind engineCallKind) int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.calls[kind]
}

// pushRaw adds items with no hooks and no wake checks. It panics if they do not fit, because
// that is a mistake in the test's script.
func (fake *scriptedEngine[T]) pushRaw(items ...T) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(items) > fake.freeLocked() {
		panic("scriptedEngine: pushRaw past capacity")
	}
	fake.appendLocked(items)
}

// takeRaw removes and returns the n oldest visible items with no hooks and no wake checks. It
// panics if fewer are visible, because that is a mistake in the test's script.
func (fake *scriptedEngine[T]) takeRaw(n int) []T {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if n > len(fake.visible) {
		panic("scriptedEngine: takeRaw past the visible items")
	}
	return fake.removeLocked(n)
}

func (fake *scriptedEngine[T]) Enqueue(item T) error {
	call := fake.begin(callEnqueue)
	fake.mu.Lock()
	full := fake.takeForceEmptyLocked() || fake.freeLocked() == 0
	if !full {
		fake.appendLocked([]T{item})
	}
	fake.mu.Unlock()
	fake.end(call)
	if full {
		return ring.ErrFull
	}
	return nil
}

func (fake *scriptedEngine[T]) EnqueueBurst(items []T) (enqueued, free int) {
	call := fake.beginCall(burstKind(callEnqueueBurst, len(items)), callEnqueueBurst)
	fake.mu.Lock()
	if !fake.takeForceEmptyLocked() {
		enqueued = min(len(items), fake.freeLocked(), fake.maxEnqueueBatch)
		if fake.maxMove > 0 {
			enqueued = min(enqueued, fake.maxMove)
		}
		fake.appendLocked(items[:enqueued])
		free = fake.freeLocked()
	}
	fake.mu.Unlock()
	fake.end(call)
	return enqueued, free
}

func (fake *scriptedEngine[T]) Dequeue() (T, error) {
	call := fake.begin(callDequeue)
	fake.mu.Lock()
	var item T
	empty := fake.takeForceEmptyLocked() || len(fake.visible) == 0
	if !empty {
		item = fake.removeLocked(1)[0]
	}
	fake.mu.Unlock()
	fake.end(call)
	if empty {
		return item, ring.ErrEmpty
	}
	return item, nil
}

func (fake *scriptedEngine[T]) DequeueBurst(dst []T) (dequeued, available int) {
	call := fake.beginCall(burstKind(callDequeueBurst, len(dst)), callDequeueBurst)
	fake.mu.Lock()
	if !fake.takeForceEmptyLocked() {
		dequeued = copy(dst[:min(len(dst), fake.maxDequeueBatch)], fake.visible)
		fake.removeLocked(dequeued)
		available = len(fake.visible)
	}
	fake.mu.Unlock()
	fake.end(call)
	return dequeued, available
}

func (fake *scriptedEngine[T]) Capacity() int {
	return fake.capacity
}

func burstKind(kind engineCallKind, length int) engineCallKind {
	if length == 0 {
		return callPeek
	}
	return kind
}

// begin counts a call of kind and runs the before hook for it.
func (fake *scriptedEngine[T]) begin(kind engineCallKind) engineCall {
	return fake.beginCall(kind, kind)
}

func (fake *scriptedEngine[T]) beginCall(kind, transferKind engineCallKind) engineCall {
	fake.mu.Lock()
	fake.calls[kind]++
	call := engineCall{kind: kind, count: fake.calls[kind]}
	if kind == callPeek {
		call.transferKind = transferKind
	}
	fake.callLog = append(fake.callLog, call)
	before := fake.before
	fake.mu.Unlock()
	if before != nil {
		before(call)
	}
	return call
}

// end runs the after hook for call.
func (fake *scriptedEngine[T]) end(call engineCall) {
	fake.mu.Lock()
	after := fake.after
	fake.mu.Unlock()
	if after != nil {
		after(call)
	}
}

func (fake *scriptedEngine[T]) takeForceEmptyLocked() bool {
	forced := fake.forceEmpty
	fake.forceEmpty = false
	return forced
}

func (fake *scriptedEngine[T]) freeLocked() int {
	return fake.capacity - len(fake.visible) - len(fake.held)
}

func (fake *scriptedEngine[T]) appendLocked(items []T) {
	if fake.holding {
		fake.held = append(fake.held, items...)
		return
	}
	fake.visible = append(fake.visible, items...)
}

// removeLocked removes and returns the n oldest visible items. It copies them out, so later
// appends cannot overwrite what it returned.
func (fake *scriptedEngine[T]) removeLocked(n int) []T {
	removed := append([]T(nil), fake.visible[:n]...)
	fake.visible = fake.visible[n:]
	return removed
}

// TestScriptedEngineControls checks that maxMove limits a burst, that held items stay invisible
// to dequeues and peeks until they are released, and that a zero-length burst counts as a peek.
func TestScriptedEngineControls(t *testing.T) {
	fake := newScriptedEngine[int](8)
	fake.setMaxMove(2)
	enqueued, free := fake.EnqueueBurst([]int{1, 2, 3})
	assert.Equal(t, 2, enqueued, "maxMove limits the burst")
	assert.Equal(t, 6, free)

	fake.holdVisibility()
	require.NoError(t, fake.Enqueue(3))
	dst := make([]int, 4)
	dequeued, available := fake.DequeueBurst(dst)
	assert.Equal(t, []int{1, 2}, dst[:dequeued], "the held item stays invisible")
	assert.Zero(t, available)

	fake.releaseVisibility()
	_, available = fake.DequeueBurst(nil)
	assert.Equal(t, 1, available, "the released item is visible")
	assert.Equal(t, 1, fake.callCount(callPeek))
}

func (fake *scriptedEngine[T]) recordedCalls() []engineCall {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]engineCall(nil), fake.callLog...)
}

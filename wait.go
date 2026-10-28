package workq

import (
	"context"
	"runtime"
)

// contextCheckInterval is how many spin-phase polls run between checks of ctx, because a check
// costs more than a poll.
const contextCheckInterval = 64

// receiveWait waits for items after a fast-path attempt found none, following the design's
// "Consumer path". It fills a prefix of dst, which must be non-empty and within the receive batch
// limit, and returns how many items it moved, or 0 with ErrClosed once the queue is closed and
// drained, or 0 with ctx.Err() once ctx is done. A closed queue wins over a done ctx.
//
// Own is the waiter the caller keeps across calls, as Consume does, or nil to take one from the
// pool for each park. ClearOnYield is cleared before the consumer registers, so a waiting
// Consume does not keep earlier batches reachable, and may be nil.
func (q *Queue[T]) receiveWait(
	ctx context.Context,
	dst []T,
	own *waiter,
	clearOnYield []T,
) (int, error) {
	if received, finished, err := q.spinForItems(ctx, dst); finished {
		return received, err
	}
	clear(clearOnYield)
	if q.consumers.register() {
		return q.drainCheck(dst)
	}
	woken := false
	for {
		received, finished, err := q.yieldForItems(ctx, dst, woken)
		if finished {
			return received, err
		}
		if q.consumerWait.NeverPark {
			return q.pollWithoutParking(ctx, dst)
		}
		received, finished, err = q.parkForItems(ctx, dst, own)
		if finished {
			return received, err
		}
		woken = true
	}
}

// spinForItems is the spin phase, in which the consumer polls without registering. Finished
// reports whether the wait is over, with received and err as its result.
func (q *Queue[T]) spinForItems(
	ctx context.Context,
	dst []T,
) (received int, finished bool, err error) {
	for poll := range q.consumerWait.Spins {
		if q.consumers.load().closed() {
			received, err = q.drainCheck(dst)
			return received, true, err
		}
		if poll%contextCheckInterval == 0 && isDone(ctx) {
			return 0, true, ctx.Err()
		}
		if received = q.dequeueAttempt(dst); received > 0 {
			q.producers.wakeCheck()
			return received, true, nil
		}
	}
	return 0, false, nil
}

// yieldForItems is the yield phase of a registered consumer. A consumer just woken polls at
// least once, because its waker registered it to find the items that caused the wake. If the
// wait is not finished, the consumer is still registered.
func (q *Queue[T]) yieldForItems(
	ctx context.Context,
	dst []T,
	woken bool,
) (received int, finished bool, err error) {
	polls := q.consumerWait.Yields
	if woken {
		polls = max(polls, 1)
	}
	for poll := range polls {
		if poll > 0 {
			runtime.Gosched()
		}
		if received, finished, err = q.pollRegistered(ctx, dst); finished {
			return received, true, err
		}
	}
	return 0, false, nil
}

// pollWithoutParking polls until the wait is finished, for a strategy with NeverPark. The
// consumer stays registered throughout, so producers never need to wake it.
func (q *Queue[T]) pollWithoutParking(ctx context.Context, dst []T) (int, error) {
	for {
		if received, finished, err := q.pollRegistered(ctx, dst); finished {
			return received, err
		}
		runtime.Gosched()
	}
}

// pollRegistered makes one poll as a registered consumer. Every return that finishes the wait
// has run Leave polling, and a return that does not leaves the consumer registered.
func (q *Queue[T]) pollRegistered(
	ctx context.Context,
	dst []T,
) (received int, finished bool, err error) {
	if q.consumers.load().closed() {
		received, err = q.leavePollingAndDrain(dst)
		return received, true, err
	}
	if received = q.dequeueAttempt(dst); received > 0 {
		q.producers.wakeCheck()
		if q.consumers.leavePolling(q.peekItems) {
			if q.hooks.beforeConsumerHandoffYield != nil {
				q.hooks.beforeConsumerHandoffYield()
			}
			runtime.Gosched()
		}
		return received, true, nil
	}
	// This load, not the one before the attempt, is the one that orders the drain check after
	// an empty attempt, and it comes before the ctx check so a closed queue wins over a done ctx.
	if q.consumers.load().closed() {
		received, err = q.leavePollingAndDrain(dst)
		return received, true, err
	}
	if isDone(ctx) {
		q.consumers.leavePolling(q.peekItems)
		return 0, true, ctx.Err()
	}
	return 0, false, nil
}

// parkForItems is the design's Park procedure for a registered consumer. If the wait is not
// finished, a token arrived, and the waker has registered the consumer again.
func (q *Queue[T]) parkForItems(
	ctx context.Context,
	dst []T,
	own *waiter,
) (received int, finished bool, err error) {
	caller := own
	if caller == nil {
		// Every return below comes after the waiter has left the list and its token, if any,
		// has been received, so it is ready for reuse.
		caller = q.consumers.getWaiter()
		defer q.consumers.putWaiter(caller)
	}
	if q.consumers.enlist(caller, atNewest) {
		received, err = q.leavePollingAndDrain(dst)
		return received, true, err
	}
	// The extra attempt must come after enlist. An item made visible before enlist but after
	// the last poll found no parked waiter to wake, so only this attempt can see it.
	if received = q.dequeueAttempt(dst); received > 0 {
		q.producers.wakeCheck()
		if q.consumers.leaveList(caller, true, q.peekItems) {
			if q.hooks.beforeConsumerHandoffYield != nil {
				q.hooks.beforeConsumerHandoffYield()
			}
			runtime.Gosched()
		}
		return received, true, nil
	}
	select {
	case <-caller.token:
		return 0, false, nil
	case <-ctx.Done():
		q.consumers.leaveList(caller, false, q.peekItems)
		if q.consumers.load().closed() {
			received, err = q.drainCheck(dst)
			return received, true, err
		}
		return 0, true, ctx.Err()
	case <-q.done:
		q.consumers.leaveList(caller, false, q.peekItems)
		received, err = q.drainCheck(dst)
		return received, true, err
	}
}

// leavePollingAndDrain is the exit of a registered consumer that saw closed.
func (q *Queue[T]) leavePollingAndDrain(dst []T) (int, error) {
	q.consumers.leavePolling(q.peekItems)
	return q.drainCheck(dst)
}

// drainCheck is the design's drain check, and it must run after the load that saw closed. That
// load comes after Close, so this attempt sees every item pushed before Close.
func (q *Queue[T]) drainCheck(dst []T) (int, error) {
	if received := q.dequeueAttempt(dst); received > 0 {
		q.producers.wakeCheck()
		return received, nil
	}
	return 0, ErrClosed
}

func isDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// pushWait waits for room after a fast-path attempt found none, following the design's
// "Producers waiting for room". Attempt pushes the current chunk, or the single item, and
// returns how many items it moved. PushedBefore is how many items the call pushed before it
// waited.
//
// Once an attempt moves items, pushWait returns their count, with ErrClosedDuringPush if the
// push raced Close and nil otherwise. If it sees the queue closed while it waits, it returns 0
// with ErrClosed when pushedBefore is 0 and ErrClosedDuringPush otherwise. If ctx is done
// first, it returns 0 with ctx.Err(). A closed queue wins over a done ctx. Whatever it returns,
// the producer has left the wait.
func (q *Queue[T]) pushWait(
	ctx context.Context,
	attempt func() int,
	pushedBefore int,
) (int, error) {
	if pushed, finished, err := q.spinForRoom(ctx, attempt, pushedBefore); finished {
		return pushed, err
	}
	if q.producers.register() {
		return 0, closedWhileWaiting(pushedBefore)
	}
	woken := false
	for {
		pushed, finished, err := q.yieldForRoom(ctx, attempt, pushedBefore, woken)
		if finished {
			return pushed, err
		}
		if q.producerWait.NeverPark {
			return q.pushWithoutParking(ctx, attempt, pushedBefore)
		}
		pushed, finished, err = q.parkForRoom(ctx, attempt, pushedBefore, woken)
		if finished {
			return pushed, err
		}
		woken = true
	}
}

// spinForRoom is the spin phase, in which the producer attempts without registering. Finished
// reports whether the wait is over, with pushed and err as its result.
func (q *Queue[T]) spinForRoom(
	ctx context.Context,
	attempt func() int,
	pushedBefore int,
) (pushed int, finished bool, err error) {
	for poll := range q.producerWait.Spins {
		if q.producers.load().closed() {
			return 0, true, closedWhileWaiting(pushedBefore)
		}
		if poll%contextCheckInterval == 0 && isDone(ctx) {
			return 0, true, q.producerContextError(ctx, pushedBefore)
		}
		if pushed = attempt(); pushed > 0 {
			return pushed, true, q.consumerWakeCheck()
		}
	}
	return 0, false, nil
}

// yieldForRoom is the yield phase of a registered producer. A producer just woken attempts at
// least once, because its waker registered it to find the room that caused the wake. If the
// wait is not finished, the producer is still registered.
func (q *Queue[T]) yieldForRoom(
	ctx context.Context,
	attempt func() int,
	pushedBefore int,
	woken bool,
) (pushed int, finished bool, err error) {
	polls := q.producerWait.Yields
	if woken {
		polls = max(polls, 1)
	}
	for poll := range polls {
		if poll > 0 {
			runtime.Gosched()
		}
		pushed, finished, err = q.attemptRegistered(ctx, attempt, pushedBefore)
		if finished {
			return pushed, true, err
		}
	}
	return 0, false, nil
}

// pushWithoutParking attempts until the wait is finished, for a strategy with NeverPark. The
// producer stays registered throughout, so consumers never need to wake it.
func (q *Queue[T]) pushWithoutParking(
	ctx context.Context,
	attempt func() int,
	pushedBefore int,
) (int, error) {
	for {
		pushed, finished, err := q.attemptRegistered(ctx, attempt, pushedBefore)
		if finished {
			return pushed, err
		}
		runtime.Gosched()
	}
}

// attemptRegistered makes one attempt as a registered producer. Every return that finishes the
// wait has run Leave polling, and a return that does not leaves the producer registered.
func (q *Queue[T]) attemptRegistered(
	ctx context.Context,
	attempt func() int,
	pushedBefore int,
) (pushed int, finished bool, err error) {
	if q.producers.load().closed() {
		return 0, true, q.leavePollingClosed(pushedBefore)
	}
	if pushed = attempt(); pushed > 0 {
		err = q.consumerWakeCheck()
		q.producers.leavePolling(q.peekRoom)
		return pushed, true, err
	}
	// A canceled producer observes consumer-side closure again after leaving polling.
	if q.producers.load().closed() {
		return 0, true, q.leavePollingClosed(pushedBefore)
	}
	if isDone(ctx) {
		q.producers.leavePolling(q.peekRoom)
		return 0, true, q.producerContextError(ctx, pushedBefore)
	}
	return 0, false, nil
}

// parkForRoom is the design's Park procedure for a registered producer. If the wait is not
// finished, a token arrived, and the waker has registered the producer again. Woken keeps
// unsuccessful retries ahead of callers that stayed parked until this wait makes progress.
func (q *Queue[T]) parkForRoom(
	ctx context.Context,
	attempt func() int,
	pushedBefore int,
	woken bool,
) (pushed int, finished bool, err error) {
	// Every return below comes after the waiter has left the list and its token, if any, has
	// been received, so it is ready for reuse.
	caller := q.producers.getWaiter()
	defer q.producers.putWaiter(caller)
	position := atNewest
	if woken {
		position = atOldest
	}
	if q.producers.enlist(caller, position) {
		return 0, true, q.leavePollingClosed(pushedBefore)
	}
	// The extra attempt must come after enlist. Room freed before enlist but after the last
	// attempt found no parked waiter to wake, so only this attempt can see it.
	if pushed = attempt(); pushed > 0 {
		err = q.consumerWakeCheck()
		q.producers.leaveList(caller, true, q.peekRoom)
		return pushed, true, err
	}
	select {
	case <-caller.token:
		return 0, false, nil
	case <-ctx.Done():
		q.producers.leaveList(caller, false, q.peekRoom)
		return 0, true, q.producerContextError(ctx, pushedBefore)
	case <-q.done:
		q.producers.leaveList(caller, false, q.peekRoom)
		return 0, true, closedWhileWaiting(pushedBefore)
	}
}

// consumerWakeCheck runs the consumer wake check after an attempt that moved items, and returns
// ErrClosedDuringPush if the push raced Close. It must run before the producer takes its own
// side's mutex to leave the wait, and the producer must leave before it returns the error.
func (q *Queue[T]) consumerWakeCheck() error {
	if q.consumers.wakeCheck() {
		return ErrClosedDuringPush
	}
	return nil
}

// leavePollingClosed is the exit of a registered producer that saw closed.
func (q *Queue[T]) leavePollingClosed(pushedBefore int) error {
	q.producers.leavePolling(q.peekRoom)
	return closedWhileWaiting(pushedBefore)
}

// closedWhileWaiting is the error of a producer that saw closed before an attempt moved items.
// An unwrapped ErrClosed always means the call pushed nothing.
func closedWhileWaiting(pushedBefore int) error {
	if pushedBefore == 0 {
		return ErrClosed
	}
	return ErrClosedDuringPush
}

// producerContextError selects an error after context completion and wait cleanup. The consumer
// bit is Close's first publication, so it also covers closure before done or the producer bit.
// Closure published after this load need not override the context error.
func (q *Queue[T]) producerContextError(ctx context.Context, pushedBefore int) error {
	if q.consumers.load().closed() {
		return closedWhileWaiting(pushedBefore)
	}
	return ctx.Err()
}

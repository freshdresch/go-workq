package workq

import (
	"context"
	"runtime"
)

// contextCheckInterval is how many spin-phase polls run between checks of ctx, because a check
// costs more than a poll.
const contextCheckInterval = 64

// receiveWait waits after a fast-path attempt found no items. See "How waiting works" in
// wakeup.md. It fills a prefix of dst, which must be non-empty and within the receive batch
// limit, and returns how many items it moved, or 0 with ErrClosed once the queue is closed and
// drained, or 0 with ctx.Err() once ctx is done. A closed queue wins over a done ctx.
//
// Own is the waiter the caller keeps across calls, as Consume does, or nil to take one from the
// pool for each park. ClearOnYield is cleared before the consumer registers, so a waiting
// Consume does not keep earlier batches reachable, and may be nil.
func (q *Queue[T]) receiveWait(
	ctx context.Context,
	recording *diagnosticsRecording,
	dst []T,
	own *waiter,
	clearOnYield []T,
) (int, error) {
	recording.startWait(recording.consumerCounters())
	moved, err := q.receiveWaitBody(ctx, recording, dst, own, clearOnYield)
	recording.finishWait(recording.consumerCounters(), moved, err)
	return moved, err
}

func (q *Queue[T]) receiveWaitBody(
	ctx context.Context,
	recording *diagnosticsRecording,
	dst []T,
	own *waiter,
	clearOnYield []T,
) (int, error) {
	if received, finished, err := q.spinForItems(ctx, recording, dst); finished {
		return received, err
	}
	clear(clearOnYield)
	if q.consumers.register(recording.consumerCounters()) {
		return q.drainCheck(recording, dst)
	}
	woken := false
	for {
		received, finished, err := q.yieldForItems(ctx, recording, dst, woken)
		if finished {
			return received, err
		}
		if q.consumerWait.NeverPark {
			return q.pollWithoutParking(ctx, recording, dst)
		}
		received, finished, err = q.parkForItems(ctx, recording, dst, own)
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
	recording *diagnosticsRecording,
	dst []T,
) (received int, finished bool, err error) {
	recording.resetPhaseEntry()
	for poll := range q.consumerWait.Spins {
		if q.consumers.load().closed() {
			received, err = q.drainCheck(recording, dst)
			return received, true, err
		}
		if poll%contextCheckInterval == 0 && isDone(ctx) {
			return 0, true, ctx.Err()
		}
		if received = q.dequeueAttempt(recording, PhaseSpin, dst); received > 0 {
			q.producers.wakeCheck(recording.producerCounters())
			q.flushConsumerProbe(recording)
			return received, true, nil
		}
		q.flushConsumerProbe(recording)
	}
	return 0, false, nil
}

// yieldForItems is the yield phase of a registered consumer. A consumer just woken polls at
// least once, because its waker registered it to find the items that caused the wake. If the
// wait is not finished, the consumer is still registered.
func (q *Queue[T]) yieldForItems(
	ctx context.Context,
	recording *diagnosticsRecording,
	dst []T,
	woken bool,
) (received int, finished bool, err error) {
	recording.resetPhaseEntry()
	polls := q.consumerWait.Yields
	if woken {
		polls = max(polls, 1)
	}
	for poll := range polls {
		if poll > 0 {
			recording.consumerCounters().recordYield(PhaseYield)
			runtime.Gosched()
		}
		if received, finished, err = q.pollRegistered(ctx, recording, PhaseYield, dst); finished {
			return received, true, err
		}
	}
	return 0, false, nil
}

// pollWithoutParking polls until the wait is finished, for a strategy with NeverPark. The
// consumer stays registered throughout, so producers never need to wake it.
func (q *Queue[T]) pollWithoutParking(
	ctx context.Context,
	recording *diagnosticsRecording,
	dst []T,
) (int, error) {
	recording.resetPhaseEntry()
	for {
		if received, finished, err := q.pollRegistered(ctx, recording, PhaseNeverPark, dst); finished {
			return received, err
		}
		recording.consumerCounters().recordYield(PhaseNeverPark)
		runtime.Gosched()
	}
}

// pollRegistered makes one poll as a registered consumer. Every return that finishes the wait
// has run Leave polling, and a return that does not leaves the consumer registered.
func (q *Queue[T]) pollRegistered(
	ctx context.Context,
	recording *diagnosticsRecording,
	phase AttemptPhase,
	dst []T,
) (received int, finished bool, err error) {
	if q.consumers.load().closed() {
		received, err = q.leavePollingAndDrain(recording, dst)
		return received, true, err
	}
	if received = q.dequeueAttempt(recording, phase, dst); received > 0 {
		q.producers.wakeCheck(recording.producerCounters())
		q.leaveConsumerPolling(recording)
		q.flushConsumerProbe(recording)
		return received, true, nil
	}
	// This load, not the one before the attempt, is the one that orders the drain check after
	// an empty attempt, and it comes before the ctx check so a closed queue wins over a done ctx.
	if q.consumers.load().closed() {
		received, err = q.leavePollingAndDrain(recording, dst)
		return received, true, err
	}
	if isDone(ctx) {
		q.leaveConsumerPolling(recording)
		err = ctx.Err()
		q.flushConsumerProbe(recording)
		return 0, true, err
	}
	q.flushConsumerProbe(recording)
	return 0, false, nil
}

// parkForItems parks a registered consumer. See "Transitions" in wakeup.md. If the wait is not
// finished, a token arrived, and the waker has registered the consumer again.
func (q *Queue[T]) parkForItems(
	ctx context.Context,
	recording *diagnosticsRecording,
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
	if q.consumers.enlist(recording.consumerCounters(), caller, atNewest) {
		received, err = q.leavePollingAndDrain(recording, dst)
		return received, true, err
	}
	recording.finishRetry(recording.consumerCounters(), RetryReparked)
	// The extra attempt must come after enlist. An item made visible before enlist but after
	// the last poll found no parked waiter to wake, so only this attempt can see it.
	if received = q.dequeueAttempt(recording, PhaseExtra, dst); received > 0 {
		q.producers.wakeCheck(recording.producerCounters())
		q.leaveConsumerList(recording, caller, true)
		q.flushConsumerProbe(recording)
		return received, true, nil
	}
	q.flushConsumerProbe(recording)
	recording.startParkSelect(recording.consumerCounters())
	select {
	case <-caller.token:
		recording.finishParkSelect(recording.consumerCounters(), ParkToken)
		recording.startRetry(recording.consumerCounters())
		return 0, false, nil
	case <-ctx.Done():
		recording.finishParkSelect(recording.consumerCounters(), ParkContext)
		if q.hooks.afterParkContext != nil {
			q.hooks.afterParkContext()
		}
		q.leaveConsumerList(recording, caller, false)
		if q.consumers.load().closed() {
			received, err = q.drainCheck(recording, dst)
			return received, true, err
		}
		return 0, true, ctx.Err()
	case <-q.done:
		recording.finishParkSelect(recording.consumerCounters(), ParkDone)
		q.leaveConsumerList(recording, caller, false)
		received, err = q.drainCheck(recording, dst)
		return received, true, err
	}
}

// leavePollingAndDrain is the exit of a registered consumer that saw closed.
func (q *Queue[T]) leavePollingAndDrain(recording *diagnosticsRecording, dst []T) (int, error) {
	q.leaveConsumerPolling(recording)
	q.flushConsumerProbe(recording)
	return q.drainCheck(recording, dst)
}

// drainCheck must run after the observation of closure. See "Cancellation and close" in
// wakeup.md for the ordering and the requirement that all producer calls finish before Close.
func (q *Queue[T]) drainCheck(recording *diagnosticsRecording, dst []T) (int, error) {
	if received := q.dequeueAttempt(recording, PhaseDrain, dst); received > 0 {
		q.producers.wakeCheck(recording.producerCounters())
		q.flushConsumerProbe(recording)
		return received, nil
	}
	q.flushConsumerProbe(recording)
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

// pushWait waits after a fast-path attempt found no room. See "Producer retry priority" in
// wakeup.md. Attempt pushes the current chunk, or the single item, and
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
	recording *diagnosticsRecording,
	attempt func(AttemptPhase) int,
	pushedBefore int,
) (int, error) {
	recording.startWait(recording.producerCounters())
	moved, err := q.pushWaitBody(ctx, recording, attempt, pushedBefore)
	recording.finishWait(recording.producerCounters(), moved, err)
	return moved, err
}

func (q *Queue[T]) pushWaitBody(
	ctx context.Context,
	recording *diagnosticsRecording,
	attempt func(AttemptPhase) int,
	pushedBefore int,
) (int, error) {
	if pushed, finished, err := q.spinForRoom(ctx, recording, attempt, pushedBefore); finished {
		return pushed, err
	}
	if q.producers.register(recording.producerCounters()) {
		return 0, closedWhileWaiting(pushedBefore)
	}
	woken := false
	for {
		pushed, finished, err := q.yieldForRoom(ctx, recording, attempt, pushedBefore, woken)
		if finished {
			return pushed, err
		}
		if q.producerWait.NeverPark {
			return q.pushWithoutParking(ctx, recording, attempt, pushedBefore)
		}
		pushed, finished, err = q.parkForRoom(ctx, recording, attempt, pushedBefore, woken)
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
	recording *diagnosticsRecording,
	attempt func(AttemptPhase) int,
	pushedBefore int,
) (pushed int, finished bool, err error) {
	recording.resetPhaseEntry()
	for poll := range q.producerWait.Spins {
		if q.producers.load().closed() {
			return 0, true, closedWhileWaiting(pushedBefore)
		}
		if poll%contextCheckInterval == 0 && isDone(ctx) {
			return 0, true, q.producerContextError(ctx, pushedBefore)
		}
		if pushed = attempt(PhaseSpin); pushed > 0 {
			err = q.consumerWakeCheck(recording)
			q.flushProducerProbe(recording)
			return pushed, true, err
		}
		q.flushProducerProbe(recording)
	}
	return 0, false, nil
}

// yieldForRoom is the yield phase of a registered producer. A producer just woken attempts at
// least once, because its waker registered it to find the room that caused the wake. If the
// wait is not finished, the producer is still registered.
func (q *Queue[T]) yieldForRoom(
	ctx context.Context,
	recording *diagnosticsRecording,
	attempt func(AttemptPhase) int,
	pushedBefore int,
	woken bool,
) (pushed int, finished bool, err error) {
	recording.resetPhaseEntry()
	polls := q.producerWait.Yields
	if woken {
		polls = max(polls, 1)
	}
	for poll := range polls {
		if poll > 0 {
			recording.producerCounters().recordYield(PhaseYield)
			runtime.Gosched()
		}
		pushed, finished, err = q.attemptRegistered(ctx, recording, attempt, pushedBefore, PhaseYield)
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
	recording *diagnosticsRecording,
	attempt func(AttemptPhase) int,
	pushedBefore int,
) (int, error) {
	recording.resetPhaseEntry()
	for {
		pushed, finished, err := q.attemptRegistered(
			ctx,
			recording,
			attempt,
			pushedBefore,
			PhaseNeverPark,
		)
		if finished {
			return pushed, err
		}
		recording.producerCounters().recordYield(PhaseNeverPark)
		runtime.Gosched()
	}
}

// attemptRegistered makes one attempt as a registered producer. Every return that finishes the
// wait has run Leave polling, and a return that does not leaves the producer registered.
func (q *Queue[T]) attemptRegistered(
	ctx context.Context,
	recording *diagnosticsRecording,
	attempt func(AttemptPhase) int,
	pushedBefore int,
	phase AttemptPhase,
) (pushed int, finished bool, err error) {
	if q.producers.load().closed() {
		err = q.leavePollingClosed(recording, pushedBefore)
		q.flushProducerProbe(recording)
		return 0, true, err
	}
	if pushed = attempt(phase); pushed > 0 {
		err = q.consumerWakeCheck(recording)
		q.producers.leavePolling(
			recording.producerCounters(),
			func() bool { return q.peekRoom(recording) },
		)
		q.flushProducerProbe(recording)
		return pushed, true, err
	}
	// A canceled producer observes consumer-side closure again after leaving polling.
	if q.producers.load().closed() {
		err = q.leavePollingClosed(recording, pushedBefore)
		q.flushProducerProbe(recording)
		return 0, true, err
	}
	if isDone(ctx) {
		q.producers.leavePolling(
			recording.producerCounters(),
			func() bool { return q.peekRoom(recording) },
		)
		err = q.producerContextError(ctx, pushedBefore)
		q.flushProducerProbe(recording)
		return 0, true, err
	}
	q.flushProducerProbe(recording)
	return 0, false, nil
}

// parkForRoom parks a registered producer. See "Transitions" in wakeup.md. If the wait is not
// finished, a token arrived, and the waker has registered the producer again. Woken keeps
// unsuccessful retries ahead of callers that stayed parked until this wait makes progress.
func (q *Queue[T]) parkForRoom(
	ctx context.Context,
	recording *diagnosticsRecording,
	attempt func(AttemptPhase) int,
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
	if q.producers.enlist(recording.producerCounters(), caller, position) {
		return 0, true, q.leavePollingClosed(recording, pushedBefore)
	}
	recording.finishRetry(recording.producerCounters(), RetryReparked)
	// The extra attempt must come after enlist. Room freed before enlist but after the last
	// attempt found no parked waiter to wake, so only this attempt can see it.
	if pushed = attempt(PhaseExtra); pushed > 0 {
		err = q.consumerWakeCheck(recording)
		q.producers.leaveList(
			recording.producerCounters(),
			caller,
			true,
			func() bool { return q.peekRoom(recording) },
		)
		q.flushProducerProbe(recording)
		return pushed, true, err
	}
	q.flushProducerProbe(recording)
	recording.startParkSelect(recording.producerCounters())
	select {
	case <-caller.token:
		recording.finishParkSelect(recording.producerCounters(), ParkToken)
		recording.startRetry(recording.producerCounters())
		return 0, false, nil
	case <-ctx.Done():
		recording.finishParkSelect(recording.producerCounters(), ParkContext)
		if q.hooks.afterParkContext != nil {
			q.hooks.afterParkContext()
		}
		q.producers.leaveList(
			recording.producerCounters(),
			caller,
			false,
			func() bool { return q.peekRoom(recording) },
		)
		return 0, true, q.producerContextError(ctx, pushedBefore)
	case <-q.done:
		recording.finishParkSelect(recording.producerCounters(), ParkDone)
		q.producers.leaveList(
			recording.producerCounters(),
			caller,
			false,
			func() bool { return q.peekRoom(recording) },
		)
		return 0, true, closedWhileWaiting(pushedBefore)
	}
}

// consumerWakeCheck runs the consumer wake check after an attempt that moved items, and returns
// ErrClosedDuringPush if the push raced Close. It must run before the producer takes its own
// side's mutex to leave the wait, and the producer must leave before it returns the error.
func (q *Queue[T]) consumerWakeCheck(recording *diagnosticsRecording) error {
	if q.wakeConsumersAfterPublication(recording) {
		return ErrClosedDuringPush
	}
	return nil
}

// leavePollingClosed is the exit of a registered producer that saw closed.
func (q *Queue[T]) leavePollingClosed(recording *diagnosticsRecording, pushedBefore int) error {
	q.producers.leavePolling(
		recording.producerCounters(),
		func() bool { return q.peekRoom(recording) },
	)
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

package benchmark

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/freshdresch/go-workq/workq"
)

// workqDefaultBatch mirrors workq's default receive and push batch limits at ringCapacity.
const workqDefaultBatch = 32

// workRow is one implementation the workq benchmarks drive. Elements are int64 only.
type workRow interface {
	// push blocks until every item in items is queued, or the run aborts.
	push(items []int64)
	// consume runs handle on each batch it takes, until stop has been called and the queue is
	// empty, and then returns. It may also return once the run aborts, but only in a row whose
	// producers cannot then block forever. A row whose push can block with no way to see the
	// abort keeps draining until stop, so its producers can return. consumer is the caller's
	// index, from 0, and selects the row's buffer for that consumer. handle must not keep
	// batch.
	consume(consumer int, handle func(batch []int64))
	// stop runs once, after every producer has returned, whether or not the run aborted.
	stop()
}

// builtRow is one row built for one run.
type builtRow struct {
	row workRow
	// state is the run state whose abort frees every goroutine blocked in the row.
	state *runState
	// release is called by the caller after the run.
	release func()
}

// rowFactory builds a fresh row for one run. batch is the producer batch.
type rowFactory func(mode benchMode, batch int) (builtRow, error)

// workRowSpec names one row, how to build it, the producer batches it runs at, and the loads
// it runs in.
type workRowSpec struct {
	name    string
	factory rowFactory
	batches []int
	// loads maps each load the row runs in to the row's limits there. A nil map means every
	// load, with no limits.
	loads map[string]rowScope
	// tuning is what a tuning row changes from the workq defaults. It is the zero value for the
	// comparison rows and for default.
	tuning workqTuning
}

// rowScope limits a row within one load. A nil field means no limit.
type rowScope struct {
	// modes names the modes the row runs in.
	modes []string
	// works lists the handler work values the row runs at.
	works []time.Duration
}

// workqTuning is what one tuning row changes from the workq defaults. A zero field keeps the
// default.
type workqTuning struct {
	receiveBatch int
	pushBatch    int
	consumerWait *workq.WaitStrategy
	producerWait *workq.WaitStrategy
}

// options returns one workq option for each field t sets.
func (t workqTuning) options() []workq.Option {
	var options []workq.Option
	if t.receiveBatch != 0 {
		options = append(options, workq.WithMaxReceiveBatch(t.receiveBatch))
	}
	if t.pushBatch != 0 {
		options = append(options, workq.WithMaxPushBatch(t.pushBatch))
	}
	if t.consumerWait != nil {
		options = append(options, workq.WithConsumerWait(*t.consumerWait))
	}
	if t.producerWait != nil {
		options = append(options, workq.WithProducerWait(*t.producerWait))
	}
	return options
}

// workqCompareRows lists the rows BenchmarkWorkq compares, in the order it runs them.
var workqCompareRows = []workRowSpec{
	{name: "chan", factory: newChanRow, batches: []int{1, 32, 128}},
	{name: "chan-batch", factory: newChanBatchRow, batches: []int{32, 128}},
	{name: "engine", factory: newEngineRow, batches: []int{1, 32, 128}},
	{name: "workq", factory: newWorkqFactory(workqTuning{}), batches: []int{1, 32, 128}},
}

// The loads each group of tuning rows runs in, and the row's limits there.
var (
	recvRowLoads = map[string]rowScope{
		loadSaturated:     {modes: []string{spmcMode.name, mpmcMode.name}},
		loadWideConsumers: {},
	}
	pushRowLoads = map[string]rowScope{
		loadSaturated:     {modes: []string{mpscMode.name, mpmcMode.name}},
		loadWideProducers: {},
		loadPushWait:      {},
	}
	// cwaitRowLoads runs saturated and wide-consumers at work=0 only, because with handler work
	// the consumers are the bottleneck and almost never wait.
	cwaitRowLoads = map[string]rowScope{
		loadSaturated:     {works: []time.Duration{0}},
		loadWideConsumers: {works: []time.Duration{0}},
		loadPaced:         {},
		loadBurst:         {},
	}
	pwaitRowLoads = map[string]rowScope{
		loadSaturated:     {},
		loadWideProducers: {},
		loadPushWait:      {},
	}
)

// workqTuningRows lists the rows BenchmarkWorkqTuning runs, in the order it runs them. Each
// varies one thing from the workq defaults. default also stands for a receive limit and a push
// limit of 32. The push rows run only at batch 128, because a push limit at or above the
// producer batch changes nothing.
var workqTuningRows = []workRowSpec{
	newTuningRow("default", []int{1, 32, 128}, nil, workqTuning{}),
	newTuningRow("recv16", []int{1, 32}, recvRowLoads, workqTuning{receiveBatch: 16}),
	newTuningRow("recv64", []int{1, 32}, recvRowLoads, workqTuning{receiveBatch: 64}),
	newTuningRow("recv128", []int{1, 32}, recvRowLoads, workqTuning{receiveBatch: 128}),
	newTuningRow("push16", []int{128}, pushRowLoads, workqTuning{pushBatch: 16}),
	newTuningRow("push64", []int{128}, pushRowLoads, workqTuning{pushBatch: 64}),
	newTuningRow("push128", []int{128}, pushRowLoads, workqTuning{pushBatch: 128}),
	newTuningRow("cwait-park", []int{1, 32}, cwaitRowLoads, workqTuning{
		consumerWait: &workq.WaitStrategy{},
	}),
	newTuningRow("cwait-long", []int{1, 32}, cwaitRowLoads, workqTuning{
		consumerWait: &workq.WaitStrategy{Spins: 256, Yields: 64},
	}),
	newTuningRow("cwait-never", []int{1, 32}, cwaitRowLoads, workqTuning{
		consumerWait: &workq.WaitStrategy{Spins: 64, Yields: 16, NeverPark: true},
	}),
	newTuningRow("pwait-park", []int{1, 32}, pwaitRowLoads, workqTuning{
		producerWait: &workq.WaitStrategy{},
	}),
	newTuningRow("pwait-long", []int{1, 32}, pwaitRowLoads, workqTuning{
		producerWait: &workq.WaitStrategy{Spins: 64, Yields: 16},
	}),
	newTuningRow("pwait-never", []int{1, 32}, pwaitRowLoads, workqTuning{
		producerWait: &workq.WaitStrategy{Spins: 16, Yields: 4, NeverPark: true},
	}),
}

// newTuningRow returns a workq row that applies tuning, built by newWorkqFactory.
func newTuningRow(
	name string,
	batches []int,
	loads map[string]rowScope,
	tuning workqTuning,
) workRowSpec {
	return workRowSpec{
		name:    name,
		factory: newWorkqFactory(tuning),
		batches: batches,
		loads:   loads,
		tuning:  tuning,
	}
}

// chanRow sends one item at a time on a channel of ringCapacity slots. It has no abort hook,
// because a channel operation cannot fail. Its consumers ignore the abort and keep draining
// until stop, as consumeBlocking does: a consumer that returned on abort would leave a producer
// blocked on a full channel forever, and stop would never run.
type chanRow struct {
	ch chan int64
	// dsts holds each consumer's one-element buffer.
	dsts [][]int64
}

func newChanRow(mode benchMode, _ int) (builtRow, error) {
	row := &chanRow{
		ch:   make(chan int64, ringCapacity),
		dsts: newBuffers[int64](mode.consumers, 1),
	}
	return builtRow{row: row, state: newRunState(nil), release: func() {}}, nil
}

func (r *chanRow) push(items []int64) {
	for _, item := range items {
		r.ch <- item
	}
}

func (r *chanRow) consume(consumer int, handle func(batch []int64)) {
	dst := r.dsts[consumer]
	for item := range r.ch {
		dst[0] = item
		handle(dst)
	}
}

func (r *chanRow) stop() {
	close(r.ch)
}

// chanBatchRow sends batches of items on a channel of slices, the usual way to batch with
// channels. Buffers cycle through the free list, so the row does not allocate per batch. It has
// no abort hook, and its consumers drain until stop, for the reason chanRow gives.
//
// batches holds ringCapacity items when full. A producer blocked on a full batches holds one
// filled buffer, so up to one batch per producer is in flight beyond the 1024, against one item
// for chanRow.
type chanBatchRow struct {
	batches chan []int64
	// free never runs dry: at most ringCapacity/batch buffers sit in batches, and each worker
	// holds at most one. Its capacity equals the buffer count, so returning a buffer never
	// blocks.
	free chan []int64
}

func newChanBatchRow(mode benchMode, batch int) (builtRow, error) {
	slots := ringCapacity / batch
	buffers := newBuffers[int64](slots+mode.producers+mode.consumers, batch)
	row := &chanBatchRow{
		batches: make(chan []int64, slots),
		free:    make(chan []int64, len(buffers)),
	}
	for _, buffer := range buffers {
		row.free <- buffer
	}
	return builtRow{row: row, state: newRunState(nil), release: func() {}}, nil
}

func (r *chanBatchRow) push(items []int64) {
	for unsent := items; len(unsent) > 0; {
		buf := <-r.free
		// A buffer sent short on a last batch comes back short.
		buf = buf[:cap(buf)]
		n := min(len(buf), len(unsent))
		copy(buf, unsent[:n])
		r.batches <- buf[:n]
		unsent = unsent[n:]
	}
}

func (r *chanBatchRow) consume(_ int, handle func(batch []int64)) {
	for buf := range r.batches {
		handle(buf)
		r.free <- buf
	}
}

func (r *chanBatchRow) stop() {
	close(r.batches)
}

// engineRow drives the ring engine workq would pick, with polling producers and consumers. It
// builds the SPSC queue with newModeRing and every other mode with newModeFrontier, which
// switches on the goroutine counts, so it also builds SPMC16 and MPSC16.
//
// workq configures its engine with batch limits of 32 and splits each push into chunks of at
// most 32. newModeRing and newModeFrontier use the engine defaults, which equal the capacity, so
// the row pushes 32-item pieces and dequeues into 32-item buffers to keep every engine call at
// 32 items or fewer, as workq does at every batch, including 128. The row does not follow the
// recv and push tuning rows.
//
// The row has no extra abort hook. Its consumers return on abort, because consume checks the
// abort, and that is safe because its producers poll and check the abort in enqueueAll.
type engineRow struct {
	target queue[int64]
	state  *runState
	done   atomic.Bool
	// dsts holds each consumer's workqDefaultBatch buffer.
	dsts [][]int64
}

func newEngineRow(mode benchMode, _ int) (builtRow, error) {
	var target queue[int64]
	var err error
	if mode.producers == 1 && mode.consumers == 1 {
		target, err = newModeRing[int64](mode, nil, false)
	} else {
		target, err = newModeFrontier[int64](mode, nil, false)
	}
	if err != nil {
		return builtRow{}, err
	}
	state := newRunState(nil)
	row := &engineRow{
		target: target,
		state:  state,
		dsts:   newBuffers[int64](mode.consumers, workqDefaultBatch),
	}
	return builtRow{row: row, state: state, release: func() {}}, nil
}

func (r *engineRow) push(items []int64) {
	for start := 0; start < len(items); start += workqDefaultBatch {
		enqueueAll(r.target, r.state, items[start:min(start+workqDefaultBatch, len(items))])
	}
}

func (r *engineRow) consume(consumer int, handle func(batch []int64)) {
	consume(r.target, r.state, &r.done, r.dsts[consumer], handlerSink{handle: handle})
}

func (r *engineRow) stop() {
	r.done.Store(true)
}

// handlerSink passes each batch to handle.
type handlerSink struct {
	handle func(batch []int64)
}

func (s handlerSink) record(batch []int64) {
	s.handle(batch)
}

// workqRow drives a workq.Queue through PushBatch and Consume. Consume allocates its own buffer
// of the receive limit, which newBuffers does not pad. The row uses Consume on purpose, because
// it is the library's main API, even though Receive with a padded destination would avoid that.
//
// At batch 1, push calls PushBatch with one item, a one-element burst, not Push. Enqueue against
// a one-element burst in workq's own path is deferred.
type workqRow struct {
	queue *workq.Queue[int64]
	// ctx is canceled when the run aborts, which frees every goroutine waiting in PushBatch or
	// Consume.
	ctx   context.Context
	state *runState
}

// newWorkqFactory returns a factory for workq rows that apply tuning. Each run gets its own
// context, canceled by the run's abort and by the release function.
func newWorkqFactory(tuning workqTuning) rowFactory {
	return func(mode benchMode, _ int) (builtRow, error) {
		var options []workq.Option
		if mode.producers == 1 {
			options = append(options, workq.WithSingleProducer())
		}
		if mode.consumers == 1 {
			options = append(options, workq.WithSingleConsumer())
		}
		options = append(options, tuning.options()...)
		created, err := workq.New[int64](ringCapacity, options...)
		if err != nil {
			return builtRow{}, err
		}
		ctx, cancel := context.WithCancel(context.Background())
		state := newRunState(cancel)
		row := &workqRow{queue: created, ctx: ctx, state: state}
		return builtRow{row: row, state: state, release: cancel}, nil
	}
}

func (r *workqRow) push(items []int64) {
	_, err := r.queue.PushBatch(r.ctx, items)
	reportWorkqError(r.state, err)
}

func (r *workqRow) consume(_ int, handle func(batch []int64)) {
	err := r.queue.Consume(r.ctx, func(_ context.Context, batch []int64) error {
		handle(batch)
		return nil
	})
	reportWorkqError(r.state, err)
}

func (r *workqRow) stop() {
	r.queue.Close()
}

// reportWorkqError passes a non-nil err to state.fail, unless err is context.Canceled after an
// abort. The abort cancels the row's context, so that error is the abort's own effect, not a new
// failure. Because fail keeps only the first error, passing it would do no harm, but skipping it
// keeps the run's error the one that caused the abort.
func reportWorkqError(state *runState, err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) && state.aborted() {
		return
	}
	state.fail(err)
}

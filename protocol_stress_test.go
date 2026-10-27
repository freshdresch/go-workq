package workq

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	frontierFakeCapacity = 8
	frontierFakeMaxTake  = 4

	stressProducers     = 4
	stressConsumers     = 4
	stressItemsPerRound = 48
	stressRoundTimeout  = 5 * time.Second

	// A producer yields between reserve and complete at every producerStallInterval-th
	// position, and a consumer yields between take and release when its first position is a
	// multiple of consumerStallInterval, so stalls hold back both frontiers.
	producerStallInterval = 3
	consumerStallInterval = 5
)

// frontierFake is a ring of frontierFakeCapacity slots that models frontier visibility on both
// sides. A completed position becomes visible only once every earlier position is complete, and
// a released position frees room only once every earlier position is released. So a producer
// stalled between reserve and complete holds back later items, and a consumer stalled between
// take and release holds back room for producers.
type frontierFake struct {
	reserved   atomic.Uint64
	taken      atomic.Uint64
	visible    atomic.Uint64
	releasedTo atomic.Uint64

	mu             sync.Mutex
	completed      uint64
	released       uint64
	completedFlags [frontierFakeCapacity]bool
	releasedFlags  [frontierFakeCapacity]bool

	// slots are written between reserve and complete and read between take and release,
	// outside mu, so the race detector checks that the frontiers order those accesses.
	slots [frontierFakeCapacity]int
}

// reserve claims the next position for a producer. It reports false when the fake is full.
func (fake *frontierFake) reserve() (position uint64, ok bool) {
	for {
		position = fake.reserved.Load()
		if position >= fake.releasedTo.Load()+frontierFakeCapacity {
			return 0, false
		}
		if fake.reserved.CompareAndSwap(position, position+1) {
			return position, true
		}
	}
}

// complete stores item at a reserved position and advances visible through every contiguous
// completed position.
func (fake *frontierFake) complete(position uint64, item int) {
	fake.slots[position%frontierFakeCapacity] = item

	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.completed++
	fake.completedFlags[position%frontierFakeCapacity] = true
	visible := fake.visible.Load()
	for fake.completedFlags[visible%frontierFakeCapacity] {
		fake.completedFlags[visible%frontierFakeCapacity] = false
		visible++
	}
	fake.visible.Store(visible)
}

// take claims up to frontierFakeMaxTake visible positions for a consumer, starting at first.
// It returns a count of 0 when nothing is visible.
func (fake *frontierFake) take() (first uint64, count uint64) {
	for {
		first = fake.taken.Load()
		visible := fake.visible.Load()
		if visible <= first {
			return 0, 0
		}
		count = min(visible-first, frontierFakeMaxTake)
		if fake.taken.CompareAndSwap(first, first+count) {
			return first, count
		}
	}
}

// item returns the item stored at a taken position that is not yet released.
func (fake *frontierFake) item(position uint64) int {
	return fake.slots[position%frontierFakeCapacity]
}

// release frees count taken positions starting at first and advances releasedTo through every
// contiguous released position.
func (fake *frontierFake) release(first uint64, count uint64) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.released += count
	for position := first; position < first+count; position++ {
		fake.releasedFlags[position%frontierFakeCapacity] = true
	}
	releasedTo := fake.releasedTo.Load()
	for fake.releasedFlags[releasedTo%frontierFakeCapacity] {
		fake.releasedFlags[releasedTo%frontierFakeCapacity] = false
		releasedTo++
	}
	fake.releasedTo.Store(releasedTo)
}

// itemsVisible is the consumer-side peek: whether a take would find an item.
func (fake *frontierFake) itemsVisible() bool {
	return fake.visible.Load() > fake.taken.Load()
}

// roomFree is the producer-side peek: whether a reserve would find room.
func (fake *frontierFake) roomFree() bool {
	return fake.reserved.Load() < fake.releasedTo.Load()+frontierFakeCapacity
}

// awaitProgress runs one side's waiting path with Spins 0 and Yields 0 until attempt moves
// items, and reports whether it did. It returns false when stop is closed, or after reporting
// an unexpected closed word. Attempt runs the other side's wake check itself, right after it
// moves items, so that check comes before this side's mutex is taken. With Yields 0 a new
// registrant parks without polling, and a woken caller gets one poll before it parks again.
func awaitProgress(
	t *testing.T,
	side *waitSide,
	attempt func() bool,
	peek func() bool,
	stop <-chan struct{},
) bool {
	if attempt() {
		return true
	}
	if side.register() {
		t.Error("register saw closed, which the stress test never sets")
		return false
	}
	caller := side.getWaiter()
	defer side.putWaiter(caller)
	for {
		if side.enlist(caller) {
			t.Error("enlist saw closed, which the stress test never sets")
			side.leavePolling(peek)
			return false
		}
		if attempt() {
			side.leaveList(caller, true, peek)
			return true
		}
		select {
		case <-caller.token:
			if attempt() {
				side.leavePolling(peek)
				return true
			}
		case <-stop:
			side.leaveList(caller, false, peek)
			return false
		}
	}
}

// protocolStress holds the shared state of one TestWaitSideProtocolStress round.
type protocolStress struct {
	fake         frontierFake
	consumerSide waitSide
	producerSide waitSide
	items        int
	timesTaken   []atomic.Int32
	takenTotal   atomic.Int64
	allTaken     chan struct{}
	stop         chan struct{}
}

// produce pushes items firstItem through firstItem+itemCount-1, one position each.
func (run *protocolStress) produce(t *testing.T, firstItem int, itemCount int) {
	for item := firstItem; item < firstItem+itemCount; item++ {
		push := func() bool {
			position, ok := run.fake.reserve()
			if !ok {
				return false
			}
			if position%producerStallInterval == 0 {
				runtime.Gosched()
			}
			run.fake.complete(position, item)
			run.consumerSide.wakeCheck()
			return true
		}
		if !awaitProgress(t, &run.producerSide, push, run.fake.roomFree, run.stop) {
			t.Errorf("producer stopped before pushing item %d", item)
			return
		}
	}
}

// consume takes items until it has taken quota of them, every item is taken, or stop is
// closed.
func (run *protocolStress) consume(t *testing.T, quota int) {
	takenHere := 0
	receive := func() bool {
		first, count := run.fake.take()
		if count == 0 {
			return false
		}
		if first%consumerStallInterval == 0 {
			runtime.Gosched()
		}
		for position := first; position < first+count; position++ {
			run.timesTaken[run.fake.item(position)].Add(1)
		}
		run.fake.release(first, count)
		run.producerSide.wakeCheck()
		takenHere += int(count)
		totalAfter := run.takenTotal.Add(int64(count))
		totalBefore := totalAfter - int64(count)
		if totalBefore < int64(run.items) && totalAfter >= int64(run.items) {
			close(run.allTaken)
		}
		return true
	}
	for takenHere < quota && run.takenTotal.Load() < int64(run.items) {
		if !awaitProgress(t, &run.consumerSide, receive, run.fake.itemsVisible, run.stop) {
			return
		}
	}
}

// TestWaitSideProtocolStress runs both sides of the protocol concurrently against a fake engine
// with frontier visibility, in many short rounds. Each round closes stop only after every item is
// taken, so a lost wake hangs the round until the watchdog panics. Because rounds end often,
// consumers often leave while the last items of a round are still in flight. The test catches
// lost wakes owed by the wake checks and by the handoff in Leave polling, plus races under
// -race. The deterministic model tests own the other procedures, such as the peek order, the
// wake after an extra attempt that moved items, and the handoff in Leave the list when a waker
// already removed the caller.
func TestWaitSideProtocolStress(t *testing.T) {
	rounds := 3000
	if testing.Short() || raceEnabled {
		rounds = 300
	}
	for round := range rounds {
		runProtocolStressRound(t, round)
		if t.Failed() {
			return
		}
	}
}

// runProtocolStressRound moves stressItemsPerRound items through a fresh fake and a fresh pair
// of wait sides. Three consumers leave after a sixth of the items each, and the fourth runs
// until every item is taken. If the round does not finish within stressRoundTimeout, it panics
// with every goroutine's stack, so the process ends and no worker outlives the test.
func runProtocolStressRound(t *testing.T, round int) {
	items := stressItemsPerRound
	run := &protocolStress{
		items:      items,
		timesTaken: make([]atomic.Int32, items),
		allTaken:   make(chan struct{}),
		stop:       make(chan struct{}),
	}

	var workers sync.WaitGroup
	perProducer := items / stressProducers
	for producer := range stressProducers {
		workers.Go(func() {
			run.produce(t, producer*perProducer, perProducer)
		})
	}
	quota := items / 6
	for range stressConsumers - 1 {
		workers.Go(func() {
			run.consume(t, quota)
		})
	}
	workers.Go(func() {
		run.consume(t, items)
	})

	done := make(chan struct{})
	go func() {
		<-run.allTaken
		close(run.stop)
		workers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(stressRoundTimeout):
		stacks := make([]byte, 1<<20)
		stacks = stacks[:runtime.Stack(stacks, true)]
		panic(fmt.Sprintf(
			"protocol stress round %d did not finish within %v\n%s",
			round,
			stressRoundTimeout,
			stacks,
		))
	}

	for item := range items {
		assert.Equal(
			t,
			int32(1),
			run.timesTaken[item].Load(),
			"round %d: times item %d was taken",
			round,
			item,
		)
	}
	assert.Equal(t, uint64(items), run.fake.completed, "round %d: completed positions", round)
	assert.Equal(t, uint64(items), run.fake.released, "round %d: released positions", round)
	assertIdleSide(t, &run.consumerSide)
	assertIdleSide(t, &run.producerSide)
}

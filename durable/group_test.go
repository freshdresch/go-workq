package durable

import (
	"math"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A coordinator-free fixture exercises the actual transaction engine on disk.
// Only this test goroutine owns SQL, metadata, and claim state.
func groupFixture(t *testing.T, config Config) *Queue {
	t.Helper()
	opened, err := startStore(t.Context(), filepath.Join(t.TempDir(), "queue"), config)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, opened.close()) })
	return &Queue{opened: opened, config: config, changed: make(chan struct{}),
		claims: make([]claim, config.Runtime.Delivery.Items)}
}

func runGroup(t *testing.T, queue *Queue, commands ...*command) commandGroup {
	t.Helper()
	group := commandGroup{commands: make([]groupedCommand, len(commands))}
	group.count = len(commands)
	for index, command := range commands {
		group.commands[index].slot = command
	}
	require.NoError(t, queue.executeGroup(t.Context(), &group))
	return group
}

func submitCommand(entries ...Entry) *command { return &command{submit: true, entries: entries} }

func TestGroupOrderedCapacityAndExactAck(t *testing.T) {
	for _, ackFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "ackFirst", false: "submitFirst"}[ackFirst], func(t *testing.T) {
			config := testConfig(t)
			config.Policy.MaxPendingItems = 1
			queue := groupFixture(t, config)
			old := Entry{ID: ID{Sequence: 1}, Payload: []byte("old")}
			runGroup(t, queue, submitCommand(old))
			token := encodeToken(queue.opened.metadata.uuid, queue.opened.metadata.epoch, old.ID, 1)
			queue.claims[0] = claim{used: true, acquired: true, delivery: Delivery{ID: old.ID, Token: token}}
			ack := &command{kind: ackCommand, tokens: []Token{token}}
			next := submitCommand(Entry{ID: ID{Sequence: 2}})
			commands := []*command{ack, next, ack}
			if !ackFirst {
				commands = []*command{next, ack, ack}
			}
			group := runGroup(t, queue, commands...)
			for index := range group.count {
				result := group.commands[index]
				if !ackFirst && index == 0 {
					assert.ErrorIs(t, result.err, ErrCapacity)
					assert.Equal(t, NotApplied, result.result.Decision)
				} else {
					assert.NoError(t, result.err)
					assert.Equal(t, Applied, result.result.Decision)
				}
			}
			assert.False(t, queue.claims[0].used)
			expected := Count(0)
			if ackFirst {
				expected = 1
			}
			assert.Equal(t, expected, queue.opened.metadata.counts.PendingItems)
		})
	}
}

func TestGroupRejectedCommandDoesNotMutate(t *testing.T) {
	queue := groupFixture(t, testConfig(t))
	group := runGroup(t, queue,
		submitCommand(Entry{ID: ID{Sequence: 1}, Payload: []byte("first")}),
		submitCommand(Entry{ID: ID{Sequence: 2}}, Entry{ID: ID{Sequence: 1}}),
		submitCommand(Entry{ID: ID{Sequence: 3}}),
	)
	assert.ErrorIs(t, group.commands[1].err, ErrConflict)
	statuses, err := queue.opened.storage.lookup(
		t.Context(),
		[]ID{{Sequence: 1}, {Sequence: 2}, {Sequence: 3}},
	)
	require.NoError(t, err)
	assert.Equal(t, Pending, statuses[0].Kind)
	assert.Equal(t, Absent, statuses[1].Kind)
	assert.Equal(t, Pending, statuses[2].Kind)
	assert.Equal(t, uint64(2), queue.opened.metadata.lastOrder)
}

func TestGroupSQLFailureRollsBackEveryCommand(t *testing.T) {
	queue := groupFixture(t, testConfig(t))
	// A real constraint violation after an earlier command's inserts exercises
	// storage failure rather than an expected pre-mutation validation refusal.
	_, err := queue.opened.storage.conn.ExecContext(t.Context(),
		"CREATE TRIGGER reject_second BEFORE INSERT ON receipts WHEN NEW.id=x'"+
			"000000000000000000000000000000000000000000000002' "+
			"BEGIN SELECT RAISE(ABORT,'injected'); END")
	require.NoError(t, err)
	group := commandGroup{commands: make([]groupedCommand, 3)}
	group.count = 3
	for index := range group.count {
		group.commands[index].slot = submitCommand(Entry{ID: ID{Sequence: uint64(index + 1)}})
	}
	err = queue.executeGroup(t.Context(), &group)
	require.ErrorIs(t, err, ErrStorage)
	for index := range group.count {
		assert.Equal(t, NotApplied, group.commands[index].result.Decision)
		assert.ErrorIs(t, group.commands[index].err, ErrStorage)
	}
	assert.Zero(t, queue.opened.metadata.counts.Receipts)
	var count int
	require.NoError(t, queue.opened.storage.conn.QueryRowContext(t.Context(),
		"SELECT count(*) FROM receipts").Scan(&count))
	assert.Zero(t, count)
}

func TestCollectionBoundsAndBarriers(t *testing.T) {
	tests := []struct {
		name   string
		limits GroupConfig
		middle command
		want   int
	}{
		{"commands", GroupConfig{Commands: 2, Operations: 8, Encoded: 1000},
			command{submit: true, entries: make([]Entry, 2)}, 2},
		{"operationsNoSplit", GroupConfig{Commands: 8, Operations: 2, Encoded: 1000},
			command{submit: true, entries: make([]Entry, 2)}, 1},
		{"encodedNoSplit", GroupConfig{Commands: 8, Operations: 8, Encoded: 200},
			command{submit: true, entries: make([]Entry, 2)}, 1},
		{"lookup", GroupConfig{Commands: 8, Operations: 8, Encoded: 1000}, command{}, 1},
		{"release", GroupConfig{Commands: 8, Operations: 8, Encoded: 1000},
			command{kind: releaseCommand}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			queue := &Queue{changed: make(chan struct{}),
				config: Config{Runtime: RuntimeConfig{Group: tc.limits}}}
			queue.ordinary.slots = []command{
				{used: true, handed: true, sequence: 1, submit: true, entries: []Entry{{}}},
				tc.middle,
				{used: true, handed: true, sequence: 3, submit: true, entries: []Entry{{}}},
			}
			middle := &queue.ordinary.slots[1]
			middle.used, middle.handed, middle.sequence = true, true, 2
			queue.mu.Lock()
			group := queue.collectGroupLocked(&queue.ordinary, &queue.ordinary.slots[0])
			queue.mu.Unlock()
			assert.Equal(t, tc.want, group.count)
			assert.LessOrEqual(t, group.operations, tc.limits.Operations)
			assert.LessOrEqual(t, group.encoded, tc.limits.Encoded)
		})
	}
}

func TestCollectionDelay(t *testing.T) {
	for _, delay := range []time.Duration{0, time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				queue := &Queue{changed: make(chan struct{}), config: Config{Runtime: RuntimeConfig{
					Group: GroupConfig{Commands: 8, Operations: 8, Encoded: 1000, MaxDelay: delay}}}}
				queue.ordinary.slots = []command{
					{used: true, handed: true, sequence: 1, submit: true},
					{used: true, sequence: 2, submit: true},
				}
				producerDone := make(chan struct{})
				go func() {
					defer close(producerDone)
					time.Sleep(time.Millisecond / 2)
					queue.mu.Lock()
					queue.ordinary.slots[1].handed = true
					queue.signalLocked()
					queue.mu.Unlock()
				}()
				started := time.Now()
				queue.mu.Lock()
				group := queue.collectGroupLocked(&queue.ordinary, &queue.ordinary.slots[0])
				queue.mu.Unlock()
				if delay == 0 {
					assert.Equal(t, 1, group.count)
				} else {
					assert.Equal(t, 2, group.count)
				}
				assert.Equal(t, delay, time.Since(started))
				<-producerDone
			})
		})
	}
}

func TestGroupLedgerAndOrderExhaustion(t *testing.T) {
	referenceCharge, err := allocationCharge(Bytes(unsafe.Sizeof(groupedCommand{})))
	require.NoError(t, err)
	assert.LessOrEqual(t, referenceCharge+Bytes(unsafe.Sizeof(commandGroup{})), Bytes(256))
	assert.LessOrEqual(t, 64*referenceCharge, Bytes(48<<10))
	queue := groupFixture(t, testConfig(t))
	queue.opened.metadata.lastOrder = math.MaxUint64 - 1
	group := runGroup(t, queue, submitCommand(Entry{ID: ID{Sequence: 1}}),
		submitCommand(Entry{ID: ID{Sequence: 2}}))
	assert.Equal(t, Applied, group.commands[0].result.Decision)
	assert.ErrorIs(t, group.commands[1].err, ErrExhausted)
	assert.Equal(t, uint64(math.MaxUint64), queue.opened.metadata.lastOrder)
}

func TestEpochExhaustion(t *testing.T) {
	config := testConfig(t)
	directory := filepath.Join(t.TempDir(), "queue")
	opened, err := startStore(t.Context(), directory, config)
	require.NoError(t, err)
	encoded := encodeUint64(math.MaxUint64)
	_, err = opened.storage.conn.ExecContext(t.Context(), "UPDATE meta SET value=? WHERE key=?",
		encoded[:], metaEpoch)
	require.NoError(t, err)
	require.NoError(t, opened.close())
	queue, err := Open(t.Context(), directory, config)
	assert.Nil(t, queue)
	assert.ErrorIs(t, err, ErrExhausted)
}

func TestGroupRollbackPreservesAcknowledgedClaim(t *testing.T) {
	queue := groupFixture(t, testConfig(t))
	id := ID{Sequence: 1}
	runGroup(t, queue, submitCommand(Entry{ID: id, Payload: []byte("pending")}))
	token := encodeToken(queue.opened.metadata.uuid, queue.opened.metadata.epoch, id, 1)
	queue.claims[0] = claim{used: true, acquired: true, delivery: Delivery{ID: id, Token: token}}
	_, err := queue.opened.storage.conn.ExecContext(t.Context(),
		"CREATE TRIGGER reject_insert BEFORE INSERT ON receipts "+
			"BEGIN SELECT RAISE(ABORT,'injected'); END")
	require.NoError(t, err)
	group := commandGroup{commands: make([]groupedCommand, 2)}
	group.count = 2
	group.commands[0].slot = &command{kind: ackCommand, tokens: []Token{token}}
	group.commands[1].slot = submitCommand(Entry{ID: ID{Sequence: 2}})
	require.ErrorIs(t, queue.executeGroup(t.Context(), &group), ErrStorage)
	assert.True(t, queue.claims[0].used)
	assert.True(t, queue.claims[0].acquired)
	assert.Equal(t, token, queue.claims[0].delivery.Token)
	assert.Equal(t, Count(1), queue.opened.metadata.counts.PendingItems)
	assert.Equal(t, Bytes(7), queue.opened.metadata.counts.PendingPayload)
	status, err := queue.opened.storage.lookupID(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, Pending, status.Kind)
	for index := range group.count {
		assert.Equal(t, NotApplied, group.commands[index].result.Decision)
	}
}

func TestGroupAutomaticRollbackLeavesUnstartedCommandsNotApplied(t *testing.T) {
	queue := groupFixture(t, testConfig(t))
	pendingID := ID{Sequence: 10}
	runGroup(t, queue, submitCommand(Entry{ID: pendingID, Payload: []byte("pending")}))
	token := encodeToken(queue.opened.metadata.uuid, queue.opened.metadata.epoch, pendingID, 1)
	originalClaim := claim{used: true, acquired: true,
		delivery: Delivery{ID: pendingID, Token: token}}
	queue.claims[0] = originalClaim
	originalMetadata := queue.opened.metadata
	_, err := queue.opened.storage.conn.ExecContext(t.Context(),
		"CREATE TRIGGER reject_second BEFORE INSERT ON receipts WHEN NEW.id=x'"+
			"000000000000000000000000000000000000000000000002' "+
			"BEGIN SELECT RAISE(ROLLBACK,'injected automatic rollback'); END")
	require.NoError(t, err)

	retirement := ID{Producer: [16]byte{1}, Sequence: 5}
	commands := []*command{
		submitCommand(Entry{ID: ID{Sequence: 1}, Payload: []byte("first")}),
		submitCommand(Entry{ID: ID{Sequence: 1}, Payload: []byte("conflict")}),
		submitCommand(Entry{ID: ID{Sequence: 2}}),
		submitCommand(Entry{ID: ID{Sequence: 3}}),
		{kind: ackCommand, tokens: []Token{token}},
		{kind: retireCommand, retirement: retirement},
	}
	group := commandGroup{commands: make([]groupedCommand, len(commands)), count: len(commands)}
	for index, command := range commands {
		group.commands[index].slot = command
	}
	require.ErrorIs(t, queue.executeGroup(t.Context(), &group), ErrStorage)
	for _, index := range []int{0, 2} {
		assert.Equal(t, Unknown, group.commands[index].result.Decision, "command %d", index)
		assert.ErrorIs(t, group.commands[index].err, ErrStorage)
	}
	assert.Equal(t, NotApplied, group.commands[1].result.Decision)
	assert.ErrorIs(t, group.commands[1].err, ErrConflict)
	assert.NotErrorIs(t, group.commands[1].err, ErrStorage)
	for index, name := range []string{"Submit", "Ack", "retirement"} {
		result := group.commands[index+3]
		assert.Equal(t, NotApplied, result.result.Decision, "unstarted %s", name)
		assert.ErrorIs(t, result.err, ErrStorage, "unstarted %s", name)
	}
	assert.Equal(t, originalClaim, queue.claims[0])
	assert.Equal(t, originalMetadata, queue.opened.metadata)
	storedMetadata, err := queue.opened.storage.readMetadata(t.Context())
	require.NoError(t, err)
	assert.Equal(t, originalMetadata, storedMetadata)
	statuses, err := queue.opened.storage.lookup(t.Context(), []ID{
		{Sequence: 1}, {Sequence: 2}, {Sequence: 3}, pendingID, retirement,
	})
	require.NoError(t, err)
	for _, index := range []int{0, 1, 2, 4} {
		assert.Equal(t, Absent, statuses[index].Kind, "lookup %d", index)
	}
	assert.Equal(t, Pending, statuses[3].Kind)
	assert.False(t, statuses[4].HasRetirement)
}

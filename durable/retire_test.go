package durable

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetirementFloorsCapacityAndCleanup(t *testing.T) {
	config := testConfig(t)
	config.Policy.MaxReceipts = 2
	config.Policy.MaxPendingItems = 2
	config.Policy.MaxProducers = 1
	config.Runtime.Maintenance.CleanupRows = 1
	config.Runtime.Maintenance.CleanupEncoded = 147
	queue := groupFixture(t, config)
	var producer [16]byte
	for index := range producer {
		producer[index] = 255
	}
	id := ID{Producer: producer, Sequence: 2}
	nextID := ID{Producer: producer, Sequence: 3}
	runGroup(t, queue, submitCommand(Entry{ID: id}, Entry{ID: nextID}))
	retired := &command{kind: retireCommand, retirement: ID{Producer: producer, Sequence: 0}}
	group := runGroup(t, queue, retired)
	require.NoError(t, group.commands[0].err)
	status, err := queue.opened.storage.lookupID(t.Context(), ID{Producer: producer})
	require.NoError(t, err)
	assert.Equal(t, Retired, status.Kind)
	assert.True(t, status.HasRetirement)
	assert.Zero(t, status.RetiredThrough)
	group = runGroup(t, queue, &command{kind: retireCommand, retirement: id})
	assert.ErrorIs(t, group.commands[0].err, ErrConflict)
	token := encodeToken(queue.opened.metadata.uuid, queue.opened.metadata.epoch, id, 1)
	queue.claims[0] = claim{used: true, acquired: true, delivery: Delivery{ID: id, Token: token}}
	nextToken := encodeToken(queue.opened.metadata.uuid, queue.opened.metadata.epoch, nextID, 2)
	queue.claims[1] = claim{used: true, acquired: true,
		delivery: Delivery{ID: nextID, Token: nextToken}}
	group = runGroup(t, queue, &command{kind: ackCommand, tokens: []Token{token, nextToken}},
		&command{kind: retireCommand, retirement: ID{Producer: producer, Sequence: math.MaxUint64}})
	require.NoError(t, group.commands[1].err)
	status, err = queue.opened.storage.lookupID(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, Retired, status.Kind, "floor overrides retained receipt")
	assert.Equal(t, Count(2), queue.opened.metadata.counts.Receipts)
	group = runGroup(t, queue, submitCommand(Entry{ID: id}),
		&command{kind: retireCommand, retirement: ID{Producer: [16]byte{1}}})
	assert.ErrorIs(t, group.commands[0].err, ErrRetired)
	assert.ErrorIs(t, group.commands[1].err, ErrCapacity)
	require.NoError(t, queue.cleanupTurn(t.Context()))
	assert.Equal(t, Count(1), queue.opened.metadata.counts.Receipts)
	assert.Equal(t, Count(1), queue.opened.metadata.counts.CleanupMarkers)
	require.NoError(t, queue.cleanupTurn(t.Context()))
	assert.Zero(t, queue.opened.metadata.counts.Receipts)
	assert.Zero(t, queue.opened.metadata.counts.CleanupMarkers)
	assert.Equal(t, Count(1), queue.opened.metadata.counts.Producers)
	// Repeated floors neither recreate cleanup markers nor consume producer capacity.
	group = runGroup(t, queue, retired)
	assert.NoError(t, group.commands[0].err)
	assert.Zero(t, queue.opened.metadata.counts.CleanupMarkers)
}

func TestReceiptCapacityReturnsOnlyAfterCleanupCommit(t *testing.T) {
	config := testConfig(t)
	config.Policy.MaxReceipts, config.Policy.MaxPendingItems = 1, 1
	queue := groupFixture(t, config)
	id := ID{}
	runGroup(t, queue, submitCommand(Entry{ID: id}))
	token := encodeToken(queue.opened.metadata.uuid, queue.opened.metadata.epoch, id, 1)
	queue.claims[0] = claim{used: true, acquired: true, delivery: Delivery{ID: id, Token: token}}
	group := runGroup(t, queue, &command{kind: ackCommand, tokens: []Token{token}},
		&command{kind: retireCommand, retirement: id}, submitCommand(Entry{ID: ID{Sequence: 1}}))
	assert.ErrorIs(t, group.commands[2].err, ErrCapacity)
	_, err := queue.opened.storage.conn.ExecContext(t.Context(),
		"CREATE TRIGGER reject_cleanup BEFORE DELETE ON receipts "+
			"BEGIN SELECT RAISE(ABORT,'injected'); END")
	require.NoError(t, err)
	assert.ErrorIs(t, queue.cleanupTurn(t.Context()), ErrStorage)
	assert.Equal(t, Count(1), queue.opened.metadata.counts.Receipts)
	var count int
	require.NoError(t, queue.opened.storage.conn.QueryRowContext(t.Context(),
		"SELECT count(*) FROM receipts").Scan(&count))
	assert.Equal(t, 1, count)
	_, err = queue.opened.storage.conn.ExecContext(t.Context(), "DROP TRIGGER reject_cleanup")
	require.NoError(t, err)
	require.NoError(t, queue.cleanupTurn(t.Context()))
	group = runGroup(t, queue, submitCommand(Entry{ID: ID{Sequence: 1}}))
	assert.NoError(t, group.commands[0].err)
}

type cleanupObservation struct {
	queue  *Queue
	counts chan<- StoreCounts
}

func (o cleanupObservation) BeforeExecute() { o.counts <- o.queue.opened.metadata.counts }

func TestContinuousBarriersReceiveBoundedCleanupService(t *testing.T) {
	config := testConfig(t)
	config.Runtime.Maintenance.MaxCommandTurns = 3
	queue, _ := openTestQueue(t, config)
	queue.StopDelivery()
	entered, release := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err := queue.submitBatchObserved(t.Context(), nil,
			executionGate{entered: entered, release: release})
		first <- err
	}()
	<-entered
	retire, err := queue.reserve(t.Context(), &queue.progress, 384, false)
	require.NoError(t, err)
	retire.kind = retireCommand
	require.NoError(t, queue.handoff(t.Context(), retire))
	const barriers = 10
	// One observation for every queued barrier. The coordinator cannot block on reporting.
	counts := make(chan StoreCounts, barriers)
	slots := make([]*command, barriers)
	for index := range slots {
		slots[index], err = queue.reserve(t.Context(), &queue.ordinary, 256, false)
		require.NoError(t, err)
		slots[index].observer = cleanupObservation{queue: queue, counts: counts}
		require.NoError(t, queue.handoff(t.Context(), slots[index]))
	}
	close(release)
	require.NoError(t, <-first)
	result, _, err := queue.await(t.Context(), &queue.progress, retire)
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
	for _, slot := range slots {
		_, _, err := queue.await(t.Context(), &queue.ordinary, slot)
		require.NoError(t, err)
	}
	for index := range barriers {
		observed := <-counts
		if index >= int(config.Runtime.Maintenance.MaxCommandTurns) {
			assert.Zero(t, observed.CleanupMarkers, "barrier %d", index)
		}
	}
}

func TestDeterministicReferenceHistoryAndReopen(t *testing.T) {
	for _, durability := range []Durability{Synchronized, ProcessCrash} {
		t.Run(durability.String(), func(t *testing.T) {
			config := testConfig(t)
			config.Durability = durability
			queue, directory := openTestQueue(t, config)
			expected := make(map[ID]StatusKind)
			for cycle := range 12 {
				producer := [16]byte{byte(cycle % 3)}
				id := ID{Producer: producer, Sequence: uint64(cycle*3 + 2)}
				entry := Entry{ID: id, Payload: []byte{byte(cycle)}}
				result, err := queue.SubmitBatch(t.Context(), []Entry{entry})
				require.NoError(t, err)
				assert.Equal(t, Applied, result.Decision)
				expected[id] = Pending
				deliveries, err := queue.Receive(t.Context(), 1)
				require.NoError(t, err)
				require.Len(t, deliveries, 1)
				assert.Equal(t, id, deliveries[0].ID)
				if cycle%2 == 0 {
					require.NoError(t, queue.Release(deliveries[0].Token))
					previous := deliveries[0].Token
					deliveries, err = queue.Receive(t.Context(), 1)
					require.NoError(t, err)
					assert.NotEqual(t, previous, deliveries[0].Token)
					_, err = queue.AckBatch(t.Context(), []Token{previous})
					assert.ErrorIs(t, err, ErrStaleToken)
				}
				_, err = queue.AckBatch(t.Context(), []Token{deliveries[0].Token})
				require.NoError(t, err)
				expected[id] = Acknowledged
				_, err = queue.SubmitBatch(t.Context(), []Entry{entry})
				require.NoError(t, err)
				_, err = queue.RetireThrough(t.Context(), producer, id.Sequence)
				require.NoError(t, err)
				for known := range expected {
					if known.Producer == producer && known.Sequence <= id.Sequence {
						expected[known] = Retired
					}
				}
				hole := ID{Producer: producer, Sequence: id.Sequence - 1}
				expected[hole] = Retired
				ids := []ID{id, hole, {Producer: producer, Sequence: id.Sequence + 1}}
				statuses, err := queue.Lookup(t.Context(), ids)
				require.NoError(t, err)
				assert.Equal(t, Retired, statuses[0].Kind)
				assert.Equal(t, Retired, statuses[1].Kind)
				assert.Equal(t, Absent, statuses[2].Kind)
			}
			require.NoError(t, queue.Close(t.Context()))
			reopened, err := Open(t.Context(), directory, config)
			require.NoError(t, err)
			defer func() { assert.NoError(t, reopened.Close(context.Background())) }()
			for id, kind := range expected {
				statuses, err := reopened.Lookup(t.Context(), []ID{id})
				require.NoError(t, err)
				assert.Equal(t, kind, statuses[0].Kind, "ID %v", id)
			}
		})
	}
}

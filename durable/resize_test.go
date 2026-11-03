package durable

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func resizeRequest(t *testing.T, directory string) ResizeRequest {
	t.Helper()
	inspection, err := Inspect(t.Context(), directory, nil)
	require.NoError(t, err)
	return ResizeRequest{UUID: inspection.Store.UUID, Expected: inspection.Store.Policy,
		Next: inspection.Store.Policy}
}

func resizeRecords(t *testing.T, directory string) []string {
	t.Helper()
	snapshot := logicalOfflineSnapshot(t, directory)
	prefix := fmt.Sprintf("meta[%d ", metaPolicy)
	var records []string
	for _, row := range snapshot {
		if !strings.HasPrefix(row, prefix) {
			records = append(records, row)
		}
	}
	return records
}

func TestResizeGrowthPreservesStateAndRetries(t *testing.T) {
	for _, profile := range []ProfileID{profileSQLiteWALV1, profileSQLiteWALV2} {
		for _, durability := range []Durability{Synchronized, ProcessCrash} {
			t.Run(fmt.Sprintf("%s/%s", profile, durability), func(t *testing.T) {
				t.Parallel()
				config := testConfig(t)
				config.Policy.Profile = profile
				config.Policy.MaxPendingItems = 32
				config.Policy.MaxReceipts = 64
				config.Policy.DatabaseLimit = 4 << 20
				config.Durability = durability
				directory := offlineFixture(t, config)
				before := resizeRecords(t, directory)
				request := resizeRequest(t, directory)
				request.Next.MaxPendingItems *= 2
				request.Next.MaxReceipts *= 2
				request.Next.DatabaseLimit *= 2
				result, err := Resize(t.Context(), directory, request)
				require.NoError(t, err)
				assert.Equal(t, Applied, result.Decision)
				assert.Equal(t, before, resizeRecords(t, directory))
				inspection, err := Inspect(t.Context(), directory, []ID{{Sequence: 1}})
				require.NoError(t, err)
				assert.Equal(t, request.Next, inspection.Store.Policy)
				assert.Equal(t, durability, inspection.Store.Durability)
				assert.Equal(t, Pending, inspection.Statuses[0].Kind)
				result, err = Resize(t.Context(), directory, request)
				require.NoError(t, err)
				assert.Equal(t, Applied, result.Decision)
				request.Expected.MaxProducers--
				result, err = Resize(t.Context(), directory, request)
				require.NoError(t, err)
				assert.Equal(t, Applied, result.Decision)
				queue, err := Open(t.Context(), directory, config)
				assert.Nil(t, queue)
				assert.ErrorIs(t, err, ErrConfigMismatch)
				config.Policy = request.Next
				queue, err = Open(t.Context(), directory, config)
				require.NoError(t, err)
				require.NoError(t, queue.Close(t.Context()))
			})
		}
	}
}

func TestResizeZeroUUIDAndMismatch(t *testing.T) {
	t.Parallel()
	config := testConfig(t)
	queue, directory := openTestQueue(t, config)
	require.NoError(t, queue.Close(t.Context()))
	opened, err := openOffline(t.Context(), directory)
	require.NoError(t, err)
	_, err = opened.storage.conn.ExecContext(
		t.Context(),
		"UPDATE meta SET value=zeroblob(16) WHERE key=?",
		metaUUID,
	)
	require.NoError(t, err)
	require.NoError(t, opened.close())
	request := resizeRequest(t, directory)
	require.Equal(t, UUID{}, request.UUID)
	request.Next.MaxPendingItems--
	result, err := Resize(t.Context(), directory, request)
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
	request.UUID[0] = 1
	result, err = Resize(t.Context(), directory, request)
	assert.ErrorIs(t, err, ErrConfigMismatch)
	assert.Equal(t, NotApplied, result.Decision)
	request.UUID = UUID{}
	request.Next.MaxPendingItems--
	result, err = Resize(t.Context(), directory, request)
	assert.ErrorIs(t, err, ErrConfigMismatch)
	assert.Equal(t, NotApplied, result.Decision)
}

func TestResizeValidationAndActiveOwner(t *testing.T) {
	t.Parallel()
	config := testConfig(t)
	queue, directory := openTestQueue(t, config)
	request := ResizeRequest{Expected: config.Policy, Next: config.Policy}
	result, err := Resize(t.Context(), directory, request)
	assert.ErrorIs(t, err, ErrInUse)
	assert.Equal(t, NotApplied, result.Decision)
	require.NoError(t, queue.Close(t.Context()))
	for _, mutation := range []string{"expected", "next", "transition"} {
		t.Run(mutation, func(t *testing.T) {
			request := ResizeRequest{Expected: config.Policy, Next: config.Policy}
			category := ErrInvalidInput
			switch mutation {
			case "expected":
				request.Expected.MaxPayload = 0
			case "next":
				request.Next.MaxReceipts = 0
			case "transition":
				request.Expected.Profile = profileSQLiteWALV2
				category = ErrUnsupported
			}
			path := filepath.Join(t.TempDir(), "absent")
			result, err := Resize(t.Context(), path, request)
			assert.ErrorIs(t, err, category)
			assert.Equal(t, NotApplied, result.Decision)
			_, err = os.Stat(path)
			assert.ErrorIs(t, err, os.ErrNotExist)
		})
	}
	//nolint:staticcheck // Exercise the public nil-context rejection contract.
	result, err = Resize(nil, directory, request)
	assert.ErrorIs(t, err, ErrInvalidInput)
	assert.Equal(t, NotApplied, result.Decision)
}

func resizeUsageFixture(t *testing.T, acknowledge bool) string {
	t.Helper()
	queue, directory := openTestQueue(t, testConfig(t))
	entries := []Entry{
		{ID: ID{Sequence: 1}, Codec: "12345678", Payload: []byte("12345678")},
		{ID: ID{Sequence: 2}, Codec: "12345678", Payload: []byte("12345678")},
	}
	_, err := queue.SubmitBatch(t.Context(), entries)
	require.NoError(t, err)
	_, err = queue.RetireThrough(t.Context(), [16]byte{1}, 0)
	require.NoError(t, err)
	if acknowledge {
		deliveries, err := queue.Receive(t.Context(), 1)
		require.NoError(t, err)
		require.Len(t, deliveries, 1)
		_, err = queue.AckBatch(t.Context(), []Token{deliveries[0].Token})
		require.NoError(t, err)
	}
	require.NoError(t, queue.Close(t.Context()))
	return directory
}

func TestResizeShrinkConstraints(t *testing.T) {
	for _, test := range []struct {
		name   string
		ack    bool
		change func(*DurablePolicy)
	}{
		{"pending-items", false, func(policy *DurablePolicy) { policy.MaxPendingItems = 1 }},
		{"pending-bytes", false, func(policy *DurablePolicy) {
			policy.MaxPayload = 8
			policy.MaxPendingPayload = 15
		}},
		{"receipts", true, func(policy *DurablePolicy) {
			policy.MaxPendingItems = 1
			policy.MaxReceipts = 1
		}},
		{"lifetime-producers", false, func(policy *DurablePolicy) { policy.MaxProducers = 1 }},
		{"payload", false, func(policy *DurablePolicy) { policy.MaxPayload = 7 }},
		{"codec", false, func(policy *DurablePolicy) { policy.MaxCodec = 7 }},
		{"ack-payload-history", true, func(policy *DurablePolicy) { policy.MaxPayload-- }},
		{"ack-codec-history", true, func(policy *DurablePolicy) { policy.MaxCodec-- }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			directory := resizeUsageFixture(t, test.ack)
			before := logicalOfflineSnapshot(t, directory)
			request := resizeRequest(t, directory)
			test.change(&request.Next)
			result, err := Resize(t.Context(), directory, request)
			assert.ErrorIs(t, err, ErrCapacity)
			assert.NotErrorIs(t, err, ErrStorage)
			assert.Equal(t, NotApplied, result.Decision)
			assert.Equal(t, before, logicalOfflineSnapshot(t, directory))
		})
	}
}

func TestResizeShrinkPendingRows(t *testing.T) {
	t.Parallel()
	directory := resizeUsageFixture(t, false)
	before := resizeRecords(t, directory)
	request := resizeRequest(t, directory)
	request.Next.MaxPayload = 8
	request.Next.MaxCodec = 8
	request.Next.MaxPendingItems = 2
	request.Next.MaxPendingPayload = 16
	request.Next.MaxReceipts = 2
	request.Next.MaxProducers = 2
	result, err := Resize(t.Context(), directory, request)
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
	assert.Equal(t, before, resizeRecords(t, directory))
}

func TestResizeCancellationAndFailures(t *testing.T) {
	for _, stage := range []string{
		"before-barrier", "after-barrier", "before-mutation", "before-commit",
		"after-commit", "before-final-barrier", "before-close",
	} {
		for _, fail := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fail=%t", stage, fail), func(t *testing.T) {
				t.Parallel()
				directory := offlineFixture(t, testConfig(t))
				before := logicalOfflineSnapshot(t, directory)
				request := resizeRequest(t, directory)
				request.Next.MaxPendingItems--
				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				cause := errors.New("resize canceled")
				injected := errors.New("resize observer failure")
				observer := offlineTestObserver{observe: func(actual string, _ *validatedStore) error {
					if actual != stage {
						return nil
					}
					cancel(cause)
					if fail {
						return injected
					}
					return nil
				}}
				result, err := resizeObserved(ctx, directory, request, observer)
				postcommit := stage == "after-commit" || stage == "before-final-barrier" ||
					stage == "before-close"
				if !postcommit {
					assert.Equal(t, NotApplied, result.Decision)
					assert.ErrorIs(t, err, context.Canceled)
					assert.ErrorIs(t, err, cause)
					if !fail {
						assert.NotErrorIs(t, err, ErrStorage)
					}
					assert.Equal(t, before, logicalOfflineSnapshot(t, directory))
				} else if fail {
					assert.Equal(t, Unknown, result.Decision)
				} else {
					assert.Equal(t, Applied, result.Decision)
					assert.NoError(t, err)
				}
				if fail {
					assert.ErrorIs(t, err, injected)
				}
				inspection, inspectErr := Inspect(t.Context(), directory, nil)
				require.NoError(t, inspectErr)
				expected := request.Expected
				if postcommit {
					expected = request.Next
				}
				assert.Equal(t, expected, inspection.Store.Policy)
				result, err = Resize(t.Context(), directory, request)
				require.NoError(t, err)
				assert.Equal(t, Applied, result.Decision)
			})
		}
	}
}

func TestResizeHoldsOwnershipThroughClosure(t *testing.T) {
	t.Parallel()
	directory := offlineFixture(t, testConfig(t))
	request := resizeRequest(t, directory)
	request.Next.MaxPendingItems--
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	observer := offlineTestObserver{observe: func(stage string, _ *validatedStore) error {
		if stage == "before-close" {
			close(entered)
			<-release
		}
		return nil
	}}
	go func() {
		_, err := resizeObserved(t.Context(), directory, request, observer)
		finished <- err
	}()
	<-entered
	_, err := Inspect(t.Context(), directory, nil)
	assert.ErrorIs(t, err, ErrInUse)
	close(release)
	require.NoError(t, <-finished)
	_, err = Inspect(t.Context(), directory, nil)
	require.NoError(t, err)
}

func TestResizePhysicalShrinkRequiresMaintenance(t *testing.T) {
	t.Parallel()
	config := testConfig(t)
	queue, directory := openTestQueue(t, config)
	entries := make([]Entry, 64)
	for i := range entries {
		entries[i] = Entry{ID: ID{Sequence: uint64(i)}, Payload: make([]byte, 16384)}
	}
	_, err := queue.SubmitBatch(t.Context(), entries)
	require.NoError(t, err)
	for remaining := len(entries); remaining > 0; {
		deliveries, err := queue.Receive(t.Context(), 32)
		require.NoError(t, err)
		tokens := make([]Token, len(deliveries))
		for i := range deliveries {
			tokens[i] = deliveries[i].Token
		}
		_, err = queue.AckBatch(t.Context(), tokens)
		require.NoError(t, err)
		remaining -= len(deliveries)
	}
	require.NoError(t, queue.Close(t.Context()))
	before := resizeRecords(t, directory)
	inspection, err := Inspect(t.Context(), directory, nil)
	require.NoError(t, err)
	require.Positive(t, inspection.Store.FreePages)
	request := resizeRequest(t, directory)
	request.Next.DatabaseLimit = Bytes(inspection.Store.PageCount-1) * pageSize
	result, err := Resize(t.Context(), directory, request)
	assert.ErrorIs(t, err, ErrCapacity)
	assert.Equal(t, NotApplied, result.Decision)
	after, err := Inspect(t.Context(), directory, nil)
	require.NoError(t, err)
	assert.Equal(t, inspection.Store.PageCount, after.Store.PageCount)
	assert.Equal(t, inspection.Store.FreePages, after.Store.FreePages)
	_, err = Maintain(t.Context(), directory)
	require.NoError(t, err)
	result, err = Resize(t.Context(), directory, request)
	require.NoError(t, err)
	assert.Equal(t, Applied, result.Decision)
	assert.Equal(t, before, resizeRecords(t, directory))
}

func TestResizeMainLengthMustFit(t *testing.T) {
	t.Parallel()
	directory := offlineFixture(t, testConfig(t))
	inspection, err := Inspect(t.Context(), directory, nil)
	require.NoError(t, err)
	request := resizeRequest(t, directory)
	request.Next.DatabaseLimit = Bytes(inspection.Store.PageCount) * pageSize
	path := filepath.Join(directory, "queue.sqlite3")
	observer := offlineTestObserver{observe: func(stage string, _ *validatedStore) error {
		if stage == "after-barrier" {
			return os.Truncate(path, int64(request.Next.DatabaseLimit+pageSize))
		}
		return nil
	}}
	result, err := resizeObserved(t.Context(), directory, request, observer)
	assert.ErrorIs(t, err, ErrCapacity)
	assert.Equal(t, NotApplied, result.Decision)
	require.NoError(t, os.Truncate(path, int64(inspection.Store.DatabaseBytes)))
	after, err := Inspect(t.Context(), directory, nil)
	require.NoError(t, err)
	assert.Equal(t, request.Expected, after.Store.Policy)
}

func TestResizePinnedReaderRefusal(t *testing.T) {
	t.Parallel()
	config := testConfig(t)
	directory := offlineFixture(t, config)
	request := resizeRequest(t, directory)
	request.Next.MaxPendingItems--
	var reader *store
	observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
		if stage != "before-barrier" {
			return nil
		}
		var err error
		reader, err = connectStore(t.Context(), filepath.Join(directory, "queue.sqlite3"), config, false)
		require.NoError(t, err)
		_, err = reader.conn.ExecContext(t.Context(), "BEGIN")
		require.NoError(t, err)
		var count int
		row := reader.conn.QueryRowContext(t.Context(), "SELECT count(*) FROM meta")
		require.NoError(t, row.Scan(&count))
		order := encodeUint64(2)
		_, err = opened.storage.conn.ExecContext(
			t.Context(),
			"UPDATE meta SET value=? WHERE key=?",
			order[:],
			metaLastOrder,
		)
		require.NoError(t, err)
		_, err = opened.storage.conn.ExecContext(t.Context(), "PRAGMA busy_timeout=0")
		require.NoError(t, err)
		return nil
	}}
	result, err := resizeObserved(t.Context(), directory, request, observer)
	assert.ErrorIs(t, err, ErrMaintenanceRequired)
	assert.NotErrorIs(t, err, ErrStorage)
	var diagnostic *MaintenanceError
	require.ErrorAs(t, err, &diagnostic)
	assert.True(t, diagnostic.WALValid)
	assert.Positive(t, diagnostic.WALBytes)
	assert.True(t, diagnostic.Checkpoint.Valid)
	assert.Equal(t, int64(1), diagnostic.Checkpoint.Busy)
	assert.Greater(t, diagnostic.Checkpoint.Frames, diagnostic.Checkpoint.Completed)
	assert.Equal(t, NotApplied, result.Decision)
	require.NotNil(t, reader)
	_, err = reader.conn.ExecContext(t.Context(), "ROLLBACK")
	require.NoError(t, err)
	require.NoError(t, reader.close())
	inspection, err := Inspect(t.Context(), directory, nil)
	require.NoError(t, err)
	assert.Equal(t, request.Expected, inspection.Store.Policy)
}

func TestResizeClosureAndRollbackFailures(t *testing.T) {
	for _, failure := range []string{"rollback", "descriptor", "already-next-close"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			directory := offlineFixture(t, testConfig(t))
			request := resizeRequest(t, directory)
			if failure != "already-next-close" {
				request.Next.MaxPendingItems--
			}
			injected := errors.New("precommit failure")
			observer := offlineTestObserver{observe: func(stage string, opened *validatedStore) error {
				if failure == "rollback" && stage == "before-commit" {
					_, err := opened.storage.conn.ExecContext(t.Context(), "ROLLBACK")
					require.NoError(t, err)
					return injected
				}
				if failure != "rollback" && stage == "before-close" {
					require.NoError(t, opened.main.Close())
				}
				return nil
			}}
			result, err := resizeObserved(t.Context(), directory, request, observer)
			assert.Equal(t, Unknown, result.Decision)
			assert.ErrorIs(t, err, ErrStorage)
			if failure == "rollback" {
				assert.ErrorIs(t, err, injected)
			} else {
				assert.ErrorIs(t, err, os.ErrClosed)
			}
			inspection, err := Inspect(t.Context(), directory, nil)
			require.NoError(t, err)
			expected := request.Next
			if failure == "rollback" {
				expected = request.Expected
			}
			assert.Equal(t, expected, inspection.Store.Policy)
		})
	}
}

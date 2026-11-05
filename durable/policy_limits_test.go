package durable

import (
	"math"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func v2Policy() DurablePolicy {
	return DurablePolicy{Profile: profileSQLiteWALV2, MaxPayload: 128 << 10, MaxCodec: 128 << 10,
		MaxPendingItems: 10000, MaxPendingPayload: 64 << 20, MaxReceipts: 20000,
		MaxProducers: 512, DatabaseLimit: 2 << 30}
}

func TestPolicyV2Boundaries(t *testing.T) {
	for _, test := range []struct {
		name    string
		change  func(*DurablePolicy)
		invalid bool
	}{
		{"expanded", func(*DurablePolicy) {}, false},
		{"counts", func(policy *DurablePolicy) {
			policy.MaxPendingItems = math.MaxUint32
			policy.MaxReceipts = math.MaxUint32
			policy.MaxProducers = math.MaxUint32
			policy.MaxPendingPayload = math.MaxUint64
		}, false},
		{"last page", func(policy *DurablePolicy) {
			policy.DatabaseLimit = sqliteMaxPages * pageSize
		}, false},
		{"extra page", func(policy *DurablePolicy) {
			policy.DatabaseLimit = (sqliteMaxPages + 1) * pageSize
		}, true},
		{"unaligned", func(policy *DurablePolicy) { policy.DatabaseLimit++ }, true},
		{"zero payload", func(policy *DurablePolicy) { policy.MaxPayload = 0 }, true},
		{"empty codec", func(policy *DurablePolicy) { policy.MaxCodec = 0 }, false},
		{"zero count", func(policy *DurablePolicy) { policy.MaxProducers = 0 }, true},
		{"receipt relation", func(policy *DurablePolicy) { policy.MaxReceipts = 1 }, true},
		{"payload relation", func(policy *DurablePolicy) { policy.MaxPendingPayload = 1 }, true},
		{"row boundary", func(policy *DurablePolicy) {
			policy.MaxPayload = sqliteMaxLength - payloadRowOverhead - policy.MaxCodec
			policy.MaxPendingPayload = policy.MaxPayload
		}, false},
		{"row over", func(policy *DurablePolicy) {
			policy.MaxPayload = sqliteMaxLength - payloadRowOverhead - policy.MaxCodec + 1
			policy.MaxPendingPayload = policy.MaxPayload
		}, true},
		{"row overflow", func(policy *DurablePolicy) {
			policy.MaxPayload = math.MaxUint64
			policy.MaxPendingPayload = math.MaxUint64
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := v2Policy()
			test.change(&policy)
			err := validatePolicy(policy)
			if test.invalid {
				assert.ErrorIs(t, err, ErrInvalidInput)
				runtime, err := DefaultRuntime(policy)
				assert.ErrorIs(t, err, ErrInvalidInput)
				assert.Zero(t, runtime)
				return
			}
			require.NoError(t, err)
			encoded := encodePolicy(policy)
			decoded, err := decodePolicy(policy.Profile, encoded[:])
			require.NoError(t, err)
			assert.Equal(t, policy, decoded)
			runtime, err := DefaultRuntime(policy)
			require.NoError(t, err)
			assert.NoError(t, validateConfig(Config{Policy: policy, Runtime: runtime}))
		})
	}
}

func TestPolicyV1KeepsDurableCeilings(t *testing.T) {
	for _, change := range []func(*DurablePolicy){
		func(policy *DurablePolicy) { policy.MaxPayload++ },
		func(policy *DurablePolicy) { policy.MaxCodec++ },
		func(policy *DurablePolicy) { policy.MaxPendingItems++ },
		func(policy *DurablePolicy) { policy.MaxPendingPayload++ },
		func(policy *DurablePolicy) { policy.MaxReceipts++ },
		func(policy *DurablePolicy) { policy.MaxProducers++ },
		func(policy *DurablePolicy) { policy.DatabaseLimit += pageSize },
	} {
		policy := testPolicy()
		change(&policy)
		assert.ErrorIs(t, validatePolicy(policy), ErrInvalidInput)
		policy.Profile = profileSQLiteWALV2
		assert.NoError(t, validatePolicy(policy))
	}
}

func TestPolicyEffectiveBackendLength(t *testing.T) {
	storage, _ := newTestStore(t)
	policy := v2Policy()
	policy.MaxCodec = 65
	policy.MaxPayload = 4096 - payloadRowOverhead - policy.MaxCodec
	require.NoError(t, storage.conn.Raw(func(raw any) error {
		connection, ok := raw.(*sqlite3.SQLiteConn)
		require.True(t, ok)
		connection.SetLimit(sqlite3.SQLITE_LIMIT_LENGTH, 4096)
		assert.Equal(t, 4096, connection.GetLimit(sqlite3.SQLITE_LIMIT_LENGTH))
		return nil
	}))
	require.NoError(t, storage.validateBackendRowLimit(policy))
	policy.MaxPayload++
	assert.ErrorIs(t, storage.validateBackendRowLimit(policy), ErrUnsupported)
}

func TestPolicyDefaultRuntimePreservesCountsAndTuning(t *testing.T) {
	original, err := DefaultRuntime(testPolicy())
	require.NoError(t, err)
	small := testPolicy()
	small.Profile = profileSQLiteWALV2
	same, err := DefaultRuntime(small)
	require.NoError(t, err)
	assert.Equal(t, original, same)
	policy := v2Policy()
	policy.MaxPayload = 32 << 20
	policy.MaxCodec = 8 << 20
	larger, err := DefaultRuntime(policy)
	require.NoError(t, err)
	assert.Equal(t, original.SQLite, larger.SQLite)
	assert.Equal(t, original.Progress, larger.Progress)
	assert.Equal(t, original.Maintenance, larger.Maintenance)
	assert.Equal(t, original.Batches.SubmitItems, larger.Batches.SubmitItems)
	assert.Equal(t, original.Delivery.Items, larger.Delivery.Items)
	assert.Equal(t, original.Ordinary.Commands, larger.Ordinary.Commands)
	assert.Greater(t, larger.Batches.SubmitEncoded, original.Batches.SubmitEncoded)
	assert.Greater(t, larger.Ordinary.Retained, original.Ordinary.Retained)
	assert.Greater(t, larger.Delivery.ReadScratch, original.Delivery.ReadScratch)
	assert.Greater(t, larger.Delivery.Retained, original.Delivery.Retained)
}

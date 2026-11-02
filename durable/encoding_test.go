package durable

import (
	"encoding/hex"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGoldenEncodings(t *testing.T) {
	t.Parallel()
	id := ID{Sequence: 0x0102030405060708}
	for i := range id.Producer {
		id.Producer[i] = byte(i)
	}
	encoded := encodeID(id)
	assert.Equal(t, "000102030405060708090a0b0c0d0e0f0102030405060708",
		hex.EncodeToString(encoded[:]))
	decoded, err := decodeID(encoded[:])
	require.NoError(t, err)
	assert.Equal(t, id, decoded)
	uuid := UUID{0xff}
	token := encodeToken(uuid, 1, id, math.MaxUint64)
	assert.Equal(t,
		"ff0000000000000000000000000000000000000000000001"+
			"000102030405060708090a0b0c0d0e0f0102030405060708ffffffffffffffff",
		hex.EncodeToString(token.bytes[:]),
	)
	decodedToken, err := decodeToken(token.bytes[:])
	require.NoError(t, err)
	assert.Equal(t, token, decodedToken)
	assert.NotEqual(t, Token{}, token)
	assert.Equal(t, "workq/durable/sqlite-wal-v1/entry\x00", digestDomain)
	digest := entryDigest("a", []byte("bc"))
	assert.Equal(t,
		"153f35e7c41a6e4aa3753b7d96b38e76ac64df97f6497830796766879bedd0df",
		hex.EncodeToString(digest[:]),
	)
	assert.NotEqual(t, digest, entryDigest("ab", []byte("c")))
	assert.Equal(t, entryDigest("", nil), entryDigest("", []byte{}))

	policy := testPolicy()
	policyBytes := encodePolicy(policy)
	assert.Equal(t,
		"000000000000400000000000000000400000000000001000"+
			"0000000001000000000000000000400000000000000001000000000040000000",
		hex.EncodeToString(policyBytes[:]),
	)
	decodedPolicy, err := decodePolicy(profileSQLiteWALV1, policyBytes[:])
	require.NoError(t, err)
	assert.Equal(t, policy, decodedPolicy)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, []int{
		metaSchema, metaProfile, metaProfileVersion, metaAccountingVersion, metaUUID, metaPolicy,
		metaDurability, metaEpoch, metaLastOrder, metaPendingItems, metaPendingPayload, metaReceipts,
		metaProducers, metaRetiredProducers, metaCleanupMarkers,
	})
	assert.Equal(t, [3]uint64{1, 1, 1}, [3]uint64{
		schemaVersion, profileVersion, accountingVersion,
	})
}

func TestUnsignedBoundaries(t *testing.T) {
	t.Parallel()
	for _, value := range []uint64{0, math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64} {
		encoded := encodeUint64(value)
		decoded, err := decodeUint64(encoded[:])
		require.NoError(t, err)
		assert.Equal(t, value, decoded)
	}
	next, err := nextIdentity(math.MaxUint64 - 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(math.MaxUint64), next)
	next, err = nextIdentity(math.MaxUint64)
	assert.ErrorIs(t, err, ErrExhausted)
	assert.Zero(t, next)
	for _, size := range []int{0, 7, 9} {
		_, err := decodeUint64(make([]byte, size))
		assert.ErrorIs(t, err, ErrStorage)
	}
	_, err = decodeID(make([]byte, 23))
	assert.ErrorIs(t, err, ErrStorage)
	_, err = decodeToken(make([]byte, 55))
	assert.ErrorIs(t, err, ErrStorage)
	_, err = decodePolicy(profileSQLiteWALV1, make([]byte, 55))
	assert.ErrorIs(t, err, ErrStorage)
	encodedPolicy := encodePolicy(testPolicy())
	encodedPolicy[16] = 0xff
	_, err = decodePolicy(profileSQLiteWALV1, encodedPolicy[:])
	assert.ErrorIs(t, err, ErrStorage)
}

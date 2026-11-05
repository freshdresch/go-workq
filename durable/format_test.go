package durable

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func v2Config(t *testing.T, durability Durability) Config {
	t.Helper()
	policy := v2Policy()
	runtime, err := DefaultRuntime(policy)
	require.NoError(t, err)
	return Config{Policy: policy, Runtime: runtime, Durability: durability}
}

func TestFormatCreationAndReopen(t *testing.T) {
	for _, profile := range []ProfileID{profileSQLiteWALV1, profileSQLiteWALV2} {
		t.Run(string(profile), func(t *testing.T) {
			config := testConfig(t)
			config.Policy.Profile = profile
			directory := offlineFixture(t, config)
			opened, err := openOffline(t.Context(), directory)
			require.NoError(t, err)
			var encoded []byte
			err = opened.storage.conn.QueryRowContext(
				t.Context(),
				"SELECT value FROM meta WHERE key=?",
				metaSchema,
			).Scan(&encoded)
			require.NoError(t, err)
			version, err := schemaVersionForProfile(profile)
			require.NoError(t, err)
			assert.Equal(t, version, binary.BigEndian.Uint64(encoded))
			require.NoError(t, opened.storage.validateSchemaForProfile(t.Context(), profile))
			if profile == profileSQLiteWALV2 {
				err = opened.storage.validateSchemaForProfile(t.Context(), profileSQLiteWALV1)
				assert.ErrorIs(t, err, ErrUnsupported)
			}
			require.NoError(t, opened.close())
			queue, err := Open(t.Context(), directory, config)
			require.NoError(t, err)
			require.NoError(t, queue.Close(t.Context()))
			config.Policy.MaxReceipts--
			queue, err = Open(t.Context(), directory, config)
			assert.Nil(t, queue)
			assert.ErrorIs(t, err, ErrConfigMismatch)
		})
	}
}

func TestFormatRejectsUnknownAndInconsistentTuples(t *testing.T) {
	for _, test := range []struct {
		name  string
		key   int
		value []byte
	}{
		{"unknown profile", metaProfile, []byte("sqlite-wal-v9")},
		{"v1 schema two", metaSchema, []byte{0, 0, 0, 0, 0, 0, 0, 2}},
		{"unknown schema", metaSchema, []byte{0, 0, 0, 0, 0, 0, 0, 9}},
		{"profile version", metaProfileVersion, []byte{0, 0, 0, 0, 0, 0, 0, 2}},
		{"accounting version", metaAccountingVersion, []byte{0, 0, 0, 0, 0, 0, 0, 2}},
		{"v2 schema one", metaProfile, []byte(profileSQLiteWALV2)},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := offlineFixture(t, testConfig(t))
			opened, err := openOffline(t.Context(), directory)
			require.NoError(t, err)
			_, err = opened.storage.conn.ExecContext(
				t.Context(),
				"UPDATE meta SET value=? WHERE key=?",
				test.value,
				test.key,
			)
			require.NoError(t, err)
			require.NoError(t, opened.close())
			_, err = Inspect(t.Context(), directory, nil)
			assert.ErrorIs(t, err, ErrUnsupported)
		})
	}
}

func TestFormatLargeCountMetadata(t *testing.T) {
	config := v2Config(t, Synchronized)
	config.Policy.MaxPendingItems = math.MaxUint32
	config.Policy.MaxReceipts = math.MaxUint32
	config.Policy.MaxProducers = math.MaxUint32
	config.Policy.MaxPendingPayload = math.MaxUint64
	directory := offlineFixture(t, config)
	opened, err := openOffline(t.Context(), directory)
	require.NoError(t, err)
	defer func() { assert.NoError(t, opened.close()) }()
	for _, key := range []int{metaPendingItems, metaReceipts, metaProducers} {
		encoded := encodeUint64(math.MaxUint32)
		_, err := opened.storage.conn.ExecContext(
			t.Context(),
			"UPDATE meta SET value=? WHERE key=?",
			encoded[:],
			key,
		)
		require.NoError(t, err)
	}
	metadata, err := opened.storage.readMetadata(t.Context())
	require.NoError(t, err)
	assert.Equal(t, Count(math.MaxUint32), metadata.counts.PendingItems)
	assert.Equal(t, Count(math.MaxUint32), metadata.counts.Receipts)
	assert.Equal(t, Count(math.MaxUint32), metadata.counts.Producers)
	encoded := encodeUint64(math.MaxUint32 + 1)
	_, err = opened.storage.conn.ExecContext(
		t.Context(),
		"UPDATE meta SET value=? WHERE key=?",
		encoded[:],
		metaPendingItems,
	)
	require.NoError(t, err)
	_, err = opened.storage.readMetadata(t.Context())
	assert.ErrorIs(t, err, ErrStorage)
}

type chunkObservation struct {
	query                                 string
	offset, transferred, scanDestinations int
}

type chunkTestObserver struct {
	observations []chunkObservation
	cancel       context.CancelFunc
	cancelQuery  string
}

func (o *chunkTestObserver) ObserveValidationChunk(
	query string,
	offset, transferred, scanDestinations int,
) {
	o.observations = append(o.observations, chunkObservation{
		query: query, offset: offset, transferred: transferred, scanDestinations: scanDestinations,
	})
	if o.cancel != nil && query == o.cancelQuery {
		o.cancel()
	}
}

func largeFormatFixture(t *testing.T, config Config, length, backlog int) (string, Entry) {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "queue")
	queue, err := Open(t.Context(), directory, config)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, queue.Close(context.Background())) })
	entry := Entry{Codec: strings.Repeat("c", length), Payload: bytes.Repeat([]byte{0x35}, length)}
	for i := range backlog {
		entry.ID.Sequence = uint64(i + 1)
		result, err := queue.SubmitBatch(t.Context(), []Entry{entry})
		require.NoError(t, err)
		require.Equal(t, Applied, result.Decision)
	}
	require.NoError(t, queue.Close(t.Context()))
	return directory, entry
}

func TestFormatV2StreamingAndOfflineOperations(t *testing.T) {
	for _, durability := range []Durability{Synchronized, ProcessCrash} {
		for _, length := range []int{validationChunkBytes, validationChunkBytes + 1,
			2 * validationChunkBytes, 2*validationChunkBytes + 7} {
			for _, backlog := range []int{1, 7} {
				t.Run(fmt.Sprintf("%d/%d/%d", durability, length, backlog), func(t *testing.T) {
					config := v2Config(t, durability)
					directory, entry := largeFormatFixture(t, config, length, backlog)
					inspection, err := Inspect(t.Context(), directory, []ID{entry.ID})
					require.NoError(t, err)
					assert.Equal(t, config.Policy, inspection.Store.Policy)
					assert.Equal(t, Count(backlog), inspection.Store.Counts.PendingItems)
					opened, err := openOffline(t.Context(), directory)
					require.NoError(t, err)
					observer := &chunkTestObserver{}
					_, err = opened.storage.validateLogicalStateObserved(t.Context(), opened.metadata, observer)
					require.NoError(t, err)
					chunks := (length + validationChunkBytes - 1) / validationChunkBytes
					assert.Len(t, observer.observations, backlog*2*chunks)
					maxScanDestinations := 0
					var maxCodecTransfer, maxPayloadTransfer int
					for _, observation := range observer.observations {
						assert.Equal(
							t,
							min(validationChunkBytes, length-observation.offset),
							observation.transferred,
						)
						assert.LessOrEqual(t, observation.transferred, validationChunkBytes)
						assert.LessOrEqual(t, observation.scanDestinations, validationChunkBytes+72)
						maxScanDestinations = max(maxScanDestinations, observation.scanDestinations)
						switch observation.query {
						case codecChunkQuery:
							maxCodecTransfer = max(maxCodecTransfer, observation.transferred)
						case payloadChunkQuery:
							maxPayloadTransfer = max(maxPayloadTransfer, observation.transferred)
						}
					}
					assert.Equal(t, validationChunkBytes, maxCodecTransfer)
					assert.Equal(t, validationChunkBytes, maxPayloadTransfer)
					// TestLayoutStartupValidationScratch accounts for driver copies and hash state.
					assert.Equal(t, validationChunkBytes+72, maxScanDestinations)
					t.Logf(
						"length=%d backlog=%d max Scan-destination BLOB capacity=%d",
						length, backlog, maxScanDestinations,
					)
					key := encodeID(entry.ID)
					digest := entryDigest(entry.Codec, entry.Payload)
					actual, err := opened.storage.validatePayload(t.Context(), key[:], digest[:], config.Policy)
					require.NoError(t, err)
					assert.Equal(t, length, actual)
					require.NoError(t, opened.close())
					_, err = Maintain(t.Context(), directory)
					require.NoError(t, err)
					next := Synchronized
					if durability == Synchronized {
						next = ProcessCrash
					}
					changed, err := ChangeDurability(t.Context(), directory, durability, next)
					require.NoError(t, err)
					assert.Equal(t, Applied, changed.Decision)
					config.Durability = next
					queue, err := Open(t.Context(), directory, config)
					require.NoError(t, err)
					deliveries, err := queue.Receive(t.Context(), 1)
					require.NoError(t, err)
					require.Len(t, deliveries, 1)
					assert.Equal(t, entry.Codec, deliveries[0].Codec)
					assert.Equal(t, entry.Payload, deliveries[0].Payload)
					require.NoError(t, queue.Close(t.Context()))
				})
			}
		}
	}
}

func TestFormatV2CancellationAndLaterCorruption(t *testing.T) {
	for _, durability := range []Durability{Synchronized, ProcessCrash} {
		for _, column := range []string{"codec", "payload"} {
			t.Run(fmt.Sprintf("%d/%s", durability, column), func(t *testing.T) {
				config := v2Config(t, durability)
				directory, entry := largeFormatFixture(t, config, 2*validationChunkBytes+7, 1)
				opened, err := openOffline(t.Context(), directory)
				require.NoError(t, err)
				key := encodeID(entry.ID)
				digest := entryDigest(entry.Codec, entry.Payload)
				query := codecChunkQuery
				if column == "payload" {
					query = payloadChunkQuery
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				observer := &chunkTestObserver{cancel: cancel, cancelQuery: query}
				_, err = opened.storage.validatePayloadObserved(ctx, key[:], digest[:], config.Policy, observer)
				assert.ErrorIs(t, err, context.Canceled)
				observed := 0
				for _, chunk := range observer.observations {
					if chunk.query == query {
						observed++
					}
				}
				assert.Equal(t, 1, observed)
				changed := bytes.Repeat([]byte{0x35}, len(entry.Payload))
				if column == "codec" {
					changed = []byte(entry.Codec)
				}
				changed[validationChunkBytes+1] ^= 1
				// column is chosen only from the two fixed names above.
				_, err = opened.storage.conn.ExecContext(
					t.Context(),
					"UPDATE payloads SET "+column+"=? WHERE id=?",
					changed,
					key[:],
				)
				require.NoError(t, err)
				_, err = opened.storage.validatePayload(t.Context(), key[:], digest[:], config.Policy)
				assert.ErrorIs(t, err, ErrStorage)
				require.NoError(t, opened.close())
				_, err = Inspect(t.Context(), directory, nil)
				assert.ErrorIs(t, err, ErrStorage)
			})
		}
	}
}

func TestFormatV2StreamingQueryPlans(t *testing.T) {
	directory, entry := largeFormatFixture(t, v2Config(t, Synchronized), 65543, 7)
	opened, err := openOffline(t.Context(), directory)
	require.NoError(t, err)
	defer func() { assert.NoError(t, opened.close()) }()
	key := encodeID(entry.ID)
	for _, query := range []string{codecChunkQuery, payloadChunkQuery} {
		assert.NotContains(t, strings.ToUpper(query), "OFFSET")
		rows, err := opened.storage.conn.QueryContext(
			t.Context(),
			"EXPLAIN QUERY PLAN "+query,
			1,
			validationChunkBytes,
			key[:],
		)
		require.NoError(t, err)
		plans := 0
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
			plans++
			t.Log(detail)
			assert.Contains(t, detail, "SEARCH payloads USING PRIMARY KEY")
			assert.NotContains(t, detail, "TEMP")
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		assert.Positive(t, plans)
	}
}

func TestFormatMetadataSchemaDiscovery(t *testing.T) {
	for _, statement := range []string{
		"",
		"CREATE TABLE meta(key INTEGER PRIMARY KEY, value BLOB)",
		"CREATE VIEW meta AS SELECT 1 AS key, zeroblob(8) AS value",
	} {
		t.Run(statement, func(t *testing.T) {
			storage, _ := newTestStore(t)
			_, err := storage.conn.ExecContext(t.Context(), "DROP TABLE meta")
			require.NoError(t, err)
			if statement != "" {
				_, err = storage.conn.ExecContext(t.Context(), statement)
				require.NoError(t, err)
			}
			_, err = storage.readMetadata(t.Context())
			assert.ErrorIs(t, err, ErrUnsupported)
		})
	}
}

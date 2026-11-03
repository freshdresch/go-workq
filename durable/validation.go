package durable

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"strings"
)

type storeMetadata struct {
	uuid       UUID
	policy     DurablePolicy
	durability Durability
	epoch      uint64
	lastOrder  uint64
	counts     StoreCounts
}

func (s *store) validateLogicalState(ctx context.Context, metadata storeMetadata) (uint64, error) {
	return s.validateLogicalStateObserved(ctx, metadata, nil)
}

func (s *store) validateLogicalStateObserved(
	ctx context.Context,
	metadata storeMetadata,
	observer validationObserver,
) (uint64, error) {
	counts, records, err := s.validateRecordsObserved(ctx, metadata, observer)
	if err != nil {
		return 0, err
	}
	if counts != metadata.counts {
		return 0, corrupt("stored counts")
	}
	return records + metaCleanupMarkers, nil
}

func corrupt(reason string) error { return fmt.Errorf("%s: %w", reason, ErrStorage) }

func (s *store) validateSchemaForProfile(ctx context.Context, profile ProfileID) (err error) {
	ddl, err := schemaForProfile(profile)
	if err != nil {
		return err
	}
	rows, err := s.conn.QueryContext(
		ctx,
		"SELECT integrity_check='ok' FROM pragma_integrity_check LIMIT 1",
	)
	if err != nil {
		return classifySQLError(err)
	}
	defer func(opened *sql.Rows) { err = errors.Join(err, classifySQLError(opened.Close())) }(rows)
	checked := false
	for rows.Next() {
		var result bool
		if err := rows.Scan(&result); err != nil {
			return err
		}
		if !result {
			return corrupt("SQLite integrity")
		}
		checked = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !checked {
		return corrupt("missing integrity result")
	}
	if err := rows.Close(); err != nil {
		return err
	}
	expected := strings.Split(strings.TrimSuffix(strings.TrimSpace(ddl), ";"), ";")
	for i := range expected {
		expected[i] = strings.Join(strings.Fields(expected[i]), " ")
	}
	// Bound Go transfers before Scan. SQLite's native schema loading is separate.
	rows, err = s.conn.QueryContext(ctx, `SELECT
 CASE WHEN length(CAST(sql AS BLOB))<=? THEN sql ELSE NULL END
 FROM sqlite_schema WHERE sql IS NOT NULL LIMIT ?`, len(ddl), len(expected)+1)
	if err != nil {
		return err
	}
	defer func(opened *sql.Rows) { err = errors.Join(err, classifySQLError(opened.Close())) }(rows)
	count := 0
	for rows.Next() {
		var statement sql.NullString
		if err := rows.Scan(&statement); err != nil {
			return err
		}
		if !statement.Valid || count == len(expected) {
			return ErrUnsupported
		}
		normalized := strings.Join(strings.Fields(statement.String), " ")
		found := false
		for _, candidate := range expected {
			if normalized == candidate {
				found = true
				break
			}
		}
		if !found {
			return ErrUnsupported
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != len(expected) {
		return ErrUnsupported
	}
	return nil
}

// Validate the common table before querying metadata, including interrupted creations.
// This bounds schema transfers without confusing an absent format with an I/O failure.
func (s *store) validateMetadataSchema(ctx context.Context) error {
	var isTable bool
	var statement sql.NullString
	err := s.conn.QueryRowContext(
		ctx,
		`SELECT type='table',
 CASE WHEN length(CAST(sql AS BLOB))<=? THEN sql ELSE NULL END
 FROM sqlite_schema WHERE name='meta' LIMIT 1`,
		len(metadataDDL),
	).Scan(&isTable, &statement)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrUnsupported
	}
	if err != nil {
		return classifySQLError(err)
	}
	if !isTable || !statement.Valid {
		return ErrUnsupported
	}
	expected := strings.TrimSuffix(strings.TrimSpace(metadataDDL), ";")
	if strings.Join(strings.Fields(statement.String), " ") !=
		strings.Join(strings.Fields(expected), " ") {
		return ErrUnsupported
	}
	return nil
}

func (s *store) readMetadata(ctx context.Context) (_ storeMetadata, err error) {
	if err := s.validateMetadataSchema(ctx); err != nil {
		return storeMetadata{}, err
	}
	rows, err := s.conn.QueryContext(ctx, `SELECT CASE WHEN typeof(key)='integer' THEN key ELSE 0 END,
 typeof(value), length(value),
 CASE WHEN typeof(value)='blob' AND length(value)<=56 THEN value ELSE NULL END
 FROM meta ORDER BY key LIMIT 16`)
	if err != nil {
		return storeMetadata{}, err
	}
	defer func(opened *sql.Rows) { err = errors.Join(err, classifySQLError(opened.Close())) }(rows)
	var values [metaCleanupMarkers][]byte
	count := 0
	for rows.Next() {
		var key, length int
		var kind string
		var value []byte
		if err := rows.Scan(&key, &kind, &length, &value); err != nil {
			return storeMetadata{}, err
		}
		count++
		if key != count || key > len(values) || kind != "blob" {
			return storeMetadata{}, corrupt("metadata key")
		}
		expected := 8
		switch key {
		case metaProfile:
			expected = 13
		case metaUUID:
			expected = 16
		case metaPolicy:
			expected = 56
		case metaDurability:
			expected = 1
		}
		if length != expected {
			return storeMetadata{}, corrupt("metadata length")
		}
		values[key-1] = value
	}
	if err := rows.Err(); err != nil {
		return storeMetadata{}, err
	}
	if count != len(values) {
		return storeMetadata{}, corrupt("missing metadata")
	}
	profile := ProfileID(values[metaProfile-1])
	version, err := schemaVersionForProfile(profile)
	if err != nil {
		return storeMetadata{}, err
	}
	if binary.BigEndian.Uint64(values[metaSchema-1]) != version {
		return storeMetadata{}, ErrUnsupported
	}
	for _, key := range []int{metaProfileVersion, metaAccountingVersion} {
		if binary.BigEndian.Uint64(values[key-1]) != 1 {
			return storeMetadata{}, ErrUnsupported
		}
	}
	policy, err := decodePolicy(ProfileID(values[metaProfile-1]), values[metaPolicy-1])
	if err != nil {
		return storeMetadata{}, err
	}
	durability := Durability(values[metaDurability-1][0])
	if durability > ProcessCrash {
		return storeMetadata{}, ErrUnsupported
	}
	metadata := storeMetadata{policy: policy, durability: durability}
	copy(metadata.uuid[:], values[metaUUID-1])
	metadata.epoch = binary.BigEndian.Uint64(values[metaEpoch-1])
	metadata.lastOrder = binary.BigEndian.Uint64(values[metaLastOrder-1])
	var counts [6]uint64
	for i := range counts {
		counts[i] = binary.BigEndian.Uint64(values[metaPendingItems+i-1])
	}
	if counts[0] > uint64(policy.MaxPendingItems) || counts[1] > uint64(policy.MaxPendingPayload) ||
		counts[2] > uint64(policy.MaxReceipts) || counts[3] > uint64(policy.MaxProducers) ||
		counts[4] > counts[3] || counts[5] > counts[4] {
		return storeMetadata{}, corrupt("count limits")
	}
	metadata.counts = StoreCounts{
		PendingItems: Count(counts[0]), PendingPayload: Bytes(counts[1]), Receipts: Count(counts[2]),
		Producers: Count(counts[3]), RetiredProducers: Count(counts[4]), CleanupMarkers: Count(counts[5]),
	}
	return metadata, nil
}

func (s *store) validateRecords(
	ctx context.Context,
	metadata storeMetadata,
) (StoreCounts, uint64, error) {
	return s.validateRecordsObserved(ctx, metadata, nil)
}

func (s *store) validateRecordsObserved(
	ctx context.Context,
	metadata storeMetadata,
	observer validationObserver,
) (_ StoreCounts, records uint64, err error) {
	var counts StoreCounts
	rows, err := s.conn.QueryContext(ctx, validationProducersQuery)
	if err != nil {
		return counts, records, err
	}
	defer func(opened *sql.Rows) { err = errors.Join(err, classifySQLError(opened.Close())) }(rows)
	for rows.Next() {
		var producer, floor []byte
		var retired, cleanup int
		if err := rows.Scan(&producer, &retired, &floor, &cleanup); err != nil {
			return counts, records, err
		}
		validFlags := retired >= 0 && retired <= 1 && cleanup >= 0 && cleanup <= 1
		if len(producer) != 16 || len(floor) != 8 || !validFlags {
			return counts, records, corrupt("producer shape")
		}
		if retired == 0 {
			if binary.BigEndian.Uint64(floor) != 0 || cleanup != 0 {
				return counts, records, corrupt("unretired producer")
			}
		}
		counts.Producers, err = validationCount(counts.Producers, 1, metadata.policy.MaxProducers)
		if err != nil {
			return counts, records, err
		}
		counts.RetiredProducers += Count(retired)
		counts.CleanupMarkers += Count(cleanup)
		records++
		if err := ctx.Err(); err != nil {
			return counts, records, err
		}
	}
	if err := rows.Err(); err != nil {
		return counts, records, err
	}
	if err := rows.Close(); err != nil {
		return counts, records, err
	}
	rows, err = s.conn.QueryContext(ctx, validationReceiptsQuery)
	if err != nil {
		return counts, records, err
	}
	defer func(opened *sql.Rows) { err = errors.Join(err, classifySQLError(opened.Close())) }(rows)
	for rows.Next() {
		var key, digest, order, token []byte
		var state int
		if err := rows.Scan(&key, &digest, &state, &order, &token); err != nil {
			return counts, records, err
		}
		if len(key) != 24 || len(digest) != 32 || len(order) != 8 || state < 0 || state > 1 {
			return counts, records, corrupt("receipt shape")
		}
		accepted := binary.BigEndian.Uint64(order)
		if accepted == 0 || accepted > metadata.lastOrder {
			return counts, records, corrupt("acceptance order")
		}
		var retired, cleanup int
		var floor []byte
		err := s.conn.QueryRowContext(
			ctx,
			`SELECT CASE WHEN typeof(has_retirement)='integer' THEN has_retirement ELSE -1 END,
 CASE WHEN typeof(retired_through)='blob' AND length(retired_through)=8
 THEN retired_through END,
 CASE WHEN typeof(cleanup_needed)='integer' THEN cleanup_needed ELSE -1 END
 FROM producers WHERE producer=?`,
			key[:16],
		).Scan(&retired, &floor, &cleanup)
		if errors.Is(err, sql.ErrNoRows) {
			return counts, records, errors.Join(corrupt("receipt producer"), err)
		}
		if err != nil {
			return counts, records, classifySQLError(err)
		}
		if len(floor) != 8 {
			return counts, records, corrupt("producer floor")
		}
		if retired == 1 && bytes.Compare(key[16:], floor) <= 0 {
			if state == 0 || cleanup == 0 {
				return counts, records, corrupt("retirement relationship")
			}
		}
		if state == 1 {
			if len(token) != 56 {
				return counts, records, corrupt("ack token length")
			}
			if !bytes.Equal(token[:16], metadata.uuid[:]) || !bytes.Equal(token[24:48], key) ||
				binary.BigEndian.Uint64(token[16:24]) == 0 ||
				binary.BigEndian.Uint64(token[16:24]) > metadata.epoch ||
				binary.BigEndian.Uint64(token[48:]) == 0 {
				return counts, records, corrupt("ack token identity")
			}
		} else {
			if len(token) != 0 {
				return counts, records, corrupt("pending token")
			}
			recordObserver := validationRecordObserver{
				observer:         observer,
				scanDestinations: cap(key) + cap(digest) + cap(order) + cap(token) + cap(floor),
			}
			length, err := s.validatePayloadObserved(
				ctx,
				key,
				digest,
				metadata.policy,
				recordObserver,
			)
			if err != nil {
				return counts, records, err
			}
			records++
			if err := ctx.Err(); err != nil {
				return counts, records, err
			}
			counts.PendingItems, err = validationCount(
				counts.PendingItems,
				1,
				metadata.policy.MaxPendingItems,
			)
			if err != nil {
				return counts, records, err
			}
			if Bytes(length) > metadata.policy.MaxPendingPayload-counts.PendingPayload {
				return counts, records, corrupt("pending payload limit")
			}
			counts.PendingPayload += Bytes(length)
		}
		counts.Receipts, err = validationCount(counts.Receipts, 1, metadata.policy.MaxReceipts)
		if err != nil {
			return counts, records, err
		}
		records++
		if err := ctx.Err(); err != nil {
			return counts, records, err
		}
	}
	if err := rows.Err(); err != nil {
		return counts, records, err
	}
	if err := rows.Close(); err != nil {
		return counts, records, err
	}
	var payloadCount uint64
	if err := s.conn.QueryRowContext(ctx, "SELECT count(*) FROM payloads").Scan(
		&payloadCount,
	); err != nil {
		return counts, records, err
	}
	if payloadCount != uint64(counts.PendingItems) {
		return counts, records, corrupt("payload relationship")
	}
	return counts, records, nil
}

func (s *store) validatePayload(
	ctx context.Context,
	key, digest []byte,
	policy DurablePolicy,
) (int, error) {
	return s.validatePayloadObserved(ctx, key, digest, policy, nil)
}

func (s *store) validatePayloadObserved(
	ctx context.Context,
	key, digest []byte,
	policy DurablePolicy,
	observer validationObserver,
) (int, error) {
	var codecLength, payloadLength, storedLength int
	var codecType, payloadType string
	err := s.conn.QueryRowContext(ctx, `SELECT typeof(codec), length(codec), typeof(payload),
 length(payload),
 CASE WHEN typeof(payload_length)='integer' THEN payload_length ELSE -1 END
 FROM payloads WHERE id=?`, key).
		Scan(&codecType, &codecLength, &payloadType, &payloadLength, &storedLength)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, corrupt("missing pending payload")
	}
	if err != nil {
		return 0, err
	}
	if codecType != "blob" || payloadType != "blob" || codecLength < 0 || payloadLength < 0 ||
		uint64(codecLength) > uint64(policy.MaxCodec) ||
		uint64(payloadLength) > uint64(policy.MaxPayload) ||
		storedLength != payloadLength {
		return 0, corrupt("payload shape")
	}
	digestHash := newEntryDigest(uint64(codecLength))
	if err := s.hashPayloadColumnObserved(
		ctx,
		key,
		codecChunkQuery,
		codecLength,
		digestHash,
		observer,
	); err != nil {
		return 0, err
	}
	writeDigestLength(digestHash, uint64(payloadLength))
	if err := s.hashPayloadColumnObserved(
		ctx,
		key,
		payloadChunkQuery,
		payloadLength,
		digestHash,
		observer,
	); err != nil {
		return 0, err
	}
	var actual [32]byte
	copy(actual[:], digestHash.Sum(nil))
	if !bytes.Equal(actual[:], digest) {
		return 0, corrupt("payload digest")
	}
	return payloadLength, nil
}

// validationObserver observes chunk lengths and Scan-destination BLOB capacities only.
// Driver source copies and fixed hash state are covered separately by the layout ledger.
// It never receives the buffer, so it cannot extend that buffer's lifetime.
type validationObserver interface {
	ObserveValidationChunk(query string, offset, transferred, scanDestinations int)
}

// The receipt and producer-floor projections stay live while its payload is hashed.
// Include their Scan destinations without claiming to measure driver-owned buffers.
type validationRecordObserver struct {
	observer         validationObserver
	scanDestinations int
}

func (o validationRecordObserver) ObserveValidationChunk(
	query string,
	offset, transferred, scanDestinations int,
) {
	if o.observer != nil {
		o.observer.ObserveValidationChunk(
			query,
			offset,
			transferred,
			o.scanDestinations+scanDestinations,
		)
	}
}

const validationChunkBytes = 32 << 10

const codecChunkQuery = "SELECT substr(codec,?,?) FROM payloads WHERE id=?"
const payloadChunkQuery = "SELECT substr(payload,?,?) FROM payloads WHERE id=?"

func validationCount(current Count, additional uint64, limit Count) (Count, error) {
	if current > limit || additional > uint64(limit-current) {
		return 0, corrupt("record count limit")
	}
	return Count(uint64(current) + additional), nil
}

func (s *store) hashPayloadColumn(
	ctx context.Context,
	key []byte,
	query string,
	length int,
	digest hash.Hash,
) error {
	return s.hashPayloadColumnObserved(ctx, key, query, length, digest, nil)
}

func (s *store) hashPayloadColumnObserved(
	ctx context.Context,
	key []byte,
	query string,
	length int,
	digest hash.Hash,
	observer validationObserver,
) error {
	for offset := 0; offset < length; {
		if err := ctx.Err(); err != nil {
			return err
		}
		size := min(validationChunkBytes, length-offset)
		var chunk []byte
		if err := s.conn.QueryRowContext(ctx, query, offset+1, size, key).Scan(&chunk); err != nil {
			return err
		}
		if len(chunk) != size {
			return corrupt("payload chunk length")
		}
		if observer != nil {
			observer.ObserveValidationChunk(query, offset, len(chunk), cap(chunk))
		}
		// hash.Hash writes consume their input and never return an error.
		_, _ = digest.Write(chunk)
		offset += size
	}
	return ctx.Err()
}

const validationProducersQuery = `SELECT
 CASE WHEN typeof(producer)='blob' AND length(producer)=16 THEN producer END,
 CASE WHEN typeof(has_retirement)='integer' THEN has_retirement ELSE -1 END,
 CASE WHEN typeof(retired_through)='blob' AND length(retired_through)=8
 THEN retired_through END,
 CASE WHEN typeof(cleanup_needed)='integer' THEN cleanup_needed ELSE -1 END
 FROM producers ORDER BY producer`

const validationReceiptsQuery = `SELECT
 CASE WHEN typeof(id)='blob' AND length(id)=24 THEN id END,
 CASE WHEN typeof(digest)='blob' AND length(digest)=32 THEN digest END,
 CASE WHEN typeof(state)='integer' THEN state ELSE -1 END,
 CASE WHEN typeof(accept_order)='blob' AND length(accept_order)=8 THEN accept_order END,
 CASE WHEN ack_token IS NULL THEN NULL
 WHEN typeof(ack_token)='blob' AND length(ack_token)=56 THEN ack_token ELSE x'00' END
 FROM receipts ORDER BY id`

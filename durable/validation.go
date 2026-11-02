package durable

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
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

func (s *store) validate(ctx context.Context) (storeMetadata, uint64, error) {
	if err := ctx.Err(); err != nil {
		return storeMetadata{}, 0, err
	}
	if err := s.validateSchema(ctx); err != nil {
		return storeMetadata{}, 0, err
	}
	metadata, err := s.readMetadata(ctx)
	if err != nil {
		return storeMetadata{}, 0, err
	}
	counts, records, err := s.validateRecords(ctx, metadata)
	if err != nil {
		return storeMetadata{}, 0, err
	}
	if counts != metadata.counts {
		return storeMetadata{}, 0, corrupt("stored counts")
	}
	return metadata, records + metaCleanupMarkers, nil
}

func corrupt(reason string) error { return fmt.Errorf("%s: %w", reason, ErrStorage) }

func (s *store) validateSchema(ctx context.Context) (err error) {
	rows, err := s.conn.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return errors.Join(ErrStorage, err)
	}
	defer func(opened *sql.Rows) { err = errors.Join(err, opened.Close()) }(rows)
	checked := false
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return err
		}
		if result != "ok" {
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
	expected := strings.Split(strings.TrimSpace(schemaDDL), ";")
	rows, err = s.conn.QueryContext(ctx,
		"SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY name")
	if err != nil {
		return err
	}
	defer func(opened *sql.Rows) { err = errors.Join(err, opened.Close()) }(rows)
	count := 0
	for rows.Next() {
		var statement string
		if err := rows.Scan(&statement); err != nil {
			return err
		}
		normalized := strings.Join(strings.Fields(statement), " ")
		found := false
		for _, candidate := range expected {
			if normalized == strings.Join(strings.Fields(candidate), " ") {
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
	if count != 7 {
		return ErrUnsupported
	}
	return nil
}

func (s *store) readMetadata(ctx context.Context) (_ storeMetadata, err error) {
	rows, err := s.conn.QueryContext(ctx, `SELECT key, typeof(value), length(value),
 CASE WHEN length(value)<=56 THEN value ELSE NULL END FROM meta ORDER BY key`)
	if err != nil {
		return storeMetadata{}, err
	}
	defer func(opened *sql.Rows) { err = errors.Join(err, opened.Close()) }(rows)
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
	for _, key := range []int{metaSchema, metaProfileVersion, metaAccountingVersion} {
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

func checkValidationContext(ctx context.Context, records uint64) error {
	if records%256 == 0 {
		return ctx.Err()
	}
	return nil
}

func (s *store) validateRecords(
	ctx context.Context,
	metadata storeMetadata,
) (_ StoreCounts, records uint64, err error) {
	var counts StoreCounts
	rows, err := s.conn.QueryContext(ctx, `SELECT producer, has_retirement, retired_through,
 cleanup_needed FROM producers ORDER BY producer`)
	if err != nil {
		return counts, records, err
	}
	defer func(opened *sql.Rows) { err = errors.Join(err, opened.Close()) }(rows)
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
		counts.Producers++
		counts.RetiredProducers += Count(retired)
		counts.CleanupMarkers += Count(cleanup)
		if counts.Producers > metadata.policy.MaxProducers {
			return counts, records, corrupt("producer limit")
		}
		records++
		if err := checkValidationContext(ctx, records); err != nil {
			return counts, records, err
		}
	}
	if err := rows.Err(); err != nil {
		return counts, records, err
	}
	if err := rows.Close(); err != nil {
		return counts, records, err
	}
	rows, err = s.conn.QueryContext(ctx, `SELECT id, digest, state, accept_order, ack_token
 FROM receipts ORDER BY id`)
	if err != nil {
		return counts, records, err
	}
	defer func(opened *sql.Rows) { err = errors.Join(err, opened.Close()) }(rows)
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
		err := s.conn.QueryRowContext(ctx, `SELECT has_retirement, retired_through, cleanup_needed
 FROM producers WHERE producer=?`, key[:16]).Scan(&retired, &floor, &cleanup)
		if err != nil {
			return counts, records, errors.Join(corrupt("receipt producer"), err)
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
			length, err := s.validatePayload(ctx, key, digest, metadata.policy)
			if err != nil {
				return counts, records, err
			}
			records++
			if err := checkValidationContext(ctx, records); err != nil {
				return counts, records, err
			}
			counts.PendingItems++
			counts.PendingPayload += Bytes(length)
		}
		counts.Receipts++
		if counts.Receipts > metadata.policy.MaxReceipts ||
			counts.PendingItems > metadata.policy.MaxPendingItems ||
			counts.PendingPayload > metadata.policy.MaxPendingPayload {
			return counts, records, corrupt("record limits")
		}
		records++
		if err := checkValidationContext(ctx, records); err != nil {
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
	var codecLength, payloadLength, storedLength int
	var codecType, payloadType string
	err := s.conn.QueryRowContext(ctx, `SELECT typeof(codec), length(codec), typeof(payload),
 length(payload), payload_length FROM payloads WHERE id=?`, key).
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
	var codec, payload []byte
	err = s.conn.QueryRowContext(ctx, "SELECT codec,payload FROM payloads WHERE id=?", key).
		Scan(&codec, &payload)
	if err != nil {
		return 0, err
	}
	actual := entryDigest(string(codec), payload)
	if !bytes.Equal(actual[:], digest) {
		return 0, corrupt("payload digest")
	}
	return payloadLength, nil
}

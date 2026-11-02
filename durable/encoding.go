package durable

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
)

// These values are storage format identifiers. Renumbering requires a schema migration.
const (
	metaSchema            = 1
	metaProfile           = 2
	metaProfileVersion    = 3
	metaAccountingVersion = 4
	metaUUID              = 5
	metaPolicy            = 6
	metaDurability        = 7
	metaEpoch             = 8
	metaLastOrder         = 9
	metaPendingItems      = 10
	metaPendingPayload    = 11
	metaReceipts          = 12
	metaProducers         = 13
	metaRetiredProducers  = 14
	metaCleanupMarkers    = 15

	schemaVersion     uint64 = 1
	profileVersion    uint64 = 1
	accountingVersion uint64 = 1
	digestDomain             = "workq/durable/sqlite-wal-v1/entry\x00"
)

func encodeUint64(value uint64) [8]byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return encoded
}

func decodeUint64(encoded []byte) (uint64, error) {
	if len(encoded) != 8 {
		return 0, fmt.Errorf("uint64 length %d: %w", len(encoded), ErrStorage)
	}
	return binary.BigEndian.Uint64(encoded), nil
}

func nextIdentity(last uint64) (uint64, error) {
	if last == math.MaxUint64 {
		return 0, ErrExhausted
	}
	return last + 1, nil
}

func encodeID(id ID) [24]byte {
	var encoded [24]byte
	copy(encoded[:16], id.Producer[:])
	binary.BigEndian.PutUint64(encoded[16:], id.Sequence)
	return encoded
}

func decodeID(encoded []byte) (ID, error) {
	if len(encoded) != 24 {
		return ID{}, fmt.Errorf("ID length %d: %w", len(encoded), ErrStorage)
	}
	var id ID
	copy(id.Producer[:], encoded[:16])
	id.Sequence = binary.BigEndian.Uint64(encoded[16:])
	return id, nil
}

func encodeToken(uuid UUID, epoch uint64, id ID, attempt uint64) Token {
	var token Token
	copy(token.bytes[:16], uuid[:])
	binary.BigEndian.PutUint64(token.bytes[16:24], epoch)
	encodedID := encodeID(id)
	copy(token.bytes[24:48], encodedID[:])
	binary.BigEndian.PutUint64(token.bytes[48:], attempt)
	return token
}

func decodeToken(encoded []byte) (Token, error) {
	if len(encoded) != 56 {
		return Token{}, fmt.Errorf("token length %d: %w", len(encoded), ErrStorage)
	}
	var token Token
	copy(token.bytes[:], encoded)
	return token, nil
}

func entryDigest(codec string, payload []byte) [32]byte {
	digest := sha256.New()
	// hash.Hash writes always consume their input without returning an error.
	_, _ = digest.Write([]byte(digestDomain))
	codecLength := encodeUint64(uint64(len(codec)))
	_, _ = digest.Write(codecLength[:])
	_, _ = digest.Write([]byte(codec))
	payloadLength := encodeUint64(uint64(len(payload)))
	_, _ = digest.Write(payloadLength[:])
	_, _ = digest.Write(payload)
	var result [32]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func encodePolicy(policy DurablePolicy) [56]byte {
	values := [...]uint64{
		uint64(policy.MaxPayload), uint64(policy.MaxCodec), uint64(policy.MaxPendingItems),
		uint64(policy.MaxPendingPayload), uint64(policy.MaxReceipts),
		uint64(policy.MaxProducers), uint64(policy.DatabaseLimit),
	}
	var encoded [56]byte
	for i, value := range values {
		binary.BigEndian.PutUint64(encoded[i*8:(i+1)*8], value)
	}
	return encoded
}

func decodePolicy(profile ProfileID, encoded []byte) (DurablePolicy, error) {
	if err := validateProfile(profile); err != nil {
		return DurablePolicy{}, err
	}
	if len(encoded) != 56 {
		return DurablePolicy{}, fmt.Errorf("policy length %d: %w", len(encoded), ErrStorage)
	}
	var values [7]uint64
	for i := range values {
		values[i] = binary.BigEndian.Uint64(encoded[i*8 : (i+1)*8])
	}
	for _, i := range []int{2, 4, 5} {
		if values[i] > math.MaxUint32 {
			return DurablePolicy{}, fmt.Errorf("policy count overflow: %w", ErrStorage)
		}
	}
	policy := DurablePolicy{
		Profile: profile, MaxPayload: Bytes(values[0]), MaxCodec: Bytes(values[1]),
		MaxPendingItems: Count(values[2]), MaxPendingPayload: Bytes(values[3]),
		MaxReceipts: Count(values[4]), MaxProducers: Count(values[5]),
		DatabaseLimit: Bytes(values[6]),
	}
	if err := validatePolicy(policy); err != nil {
		return DurablePolicy{}, fmt.Errorf("stored policy: %w: %v", ErrStorage, err)
	}
	return policy, nil
}

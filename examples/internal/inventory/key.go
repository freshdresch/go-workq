// Package inventory defines the destination contract shared by the external-effect examples.
package inventory

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

const (
	keyVersion      = 1
	maxKeyComponent = 64
	maxKeyBytes     = 1 + 2 + maxKeyComponent + 16 + 8 + 2 + maxKeyComponent
)

// Key identifies one logical effect across retries and queue recreation.
// Namespace and Effect must be nonempty UTF-8 strings of at most 64 bytes.
// Every Producer and Sequence value is valid, including zero.
type Key struct {
	Namespace string
	Producer  [16]byte
	Sequence  uint64
	Effect    string
}

func (k Key) validate() error {
	for _, component := range []string{k.Namespace, k.Effect} {
		if len(component) == 0 || len(component) > maxKeyComponent || !utf8.ValidString(component) {
			return fmt.Errorf("key component must be 1 to 64 UTF-8 bytes: %w", ErrRejected)
		}
	}
	return nil
}

// Encode returns the canonical version-1 unpadded base64url key.
// Invalid components return an error wrapping ErrRejected.
func (k Key) Encode() (string, error) {
	if err := k.validate(); err != nil {
		return "", err
	}
	encoded := []byte{keyVersion}
	encoded = binary.BigEndian.AppendUint16(encoded, uint16(len(k.Namespace)))
	encoded = append(encoded, k.Namespace...)
	encoded = append(encoded, k.Producer[:]...)
	encoded = binary.BigEndian.AppendUint64(encoded, k.Sequence)
	encoded = binary.BigEndian.AppendUint16(encoded, uint16(len(k.Effect)))
	encoded = append(encoded, k.Effect...)
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

// ParseKey accepts only canonical version-1 keys and rejects trailing bytes.
// Invalid encodings return an error wrapping ErrRejected.
func ParseKey(encoded string) (Key, error) {
	if len(encoded) > base64.RawURLEncoding.EncodedLen(maxKeyBytes) {
		return Key{}, fmt.Errorf("encoded key too long: %w", ErrRejected)
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return Key{}, fmt.Errorf("decode key: %w: %w", ErrRejected, err)
	}
	if len(raw) < 3 || raw[0] != keyVersion {
		return Key{}, fmt.Errorf("missing or unsupported key version: %w", ErrRejected)
	}
	namespaceLength := int(binary.BigEndian.Uint16(raw[1:3]))
	offset := 3 + namespaceLength
	if len(raw) < offset+26 {
		return Key{}, fmt.Errorf("truncated key: %w", ErrRejected)
	}
	key := Key{Namespace: string(raw[3:offset])}
	copy(key.Producer[:], raw[offset:offset+16])
	key.Sequence = binary.BigEndian.Uint64(raw[offset+16 : offset+24])
	effectLength := int(binary.BigEndian.Uint16(raw[offset+24 : offset+26]))
	offset += 26
	if len(raw) != offset+effectLength {
		return Key{}, fmt.Errorf("invalid key length: %w", ErrRejected)
	}
	key.Effect = string(raw[offset:])
	canonical, err := key.Encode()
	if err != nil {
		return Key{}, err
	}
	if canonical != encoded {
		return Key{}, fmt.Errorf("noncanonical key: %w", ErrRejected)
	}
	return key, nil
}

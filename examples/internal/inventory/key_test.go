package inventory

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testKey() Key {
	return Key{Namespace: "source", Producer: [16]byte{1, 2, 3}, Effect: "inventory.adjust"}
}

func TestKeyUnsignedSequences(t *testing.T) {
	t.Parallel()
	seen := make(map[string]struct{})
	for _, sequence := range []uint64{0, math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64} {
		key := testKey()
		key.Sequence = sequence
		encoded, err := key.Encode()
		require.NoError(t, err)
		assert.NotContains(t, seen, encoded)
		seen[encoded] = struct{}{}
		decoded, err := ParseKey(encoded)
		require.NoError(t, err)
		assert.Equal(t, key, decoded)
		raw, err := base64.RawURLEncoding.DecodeString(encoded)
		require.NoError(t, err)
		assert.Equal(t, sequence, binary.BigEndian.Uint64(raw[3+len(key.Namespace)+16:]))
	}
}

func TestKeyEncodingLayout(t *testing.T) {
	t.Parallel()
	key := Key{Namespace: "n", Sequence: 1, Effect: "e"}
	encoded, err := key.Encode()
	require.NoError(t, err)
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	require.NoError(t, err)
	expected := []byte{1, 0, 1, 'n'}
	expected = append(expected, make([]byte, 16)...)
	expected = append(expected, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 'e')
	assert.Equal(t, expected, raw)
}

func TestKeyComponents(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", strings.Repeat("a", 65), "\xff"} {
		for _, field := range []string{"namespace", "effect"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				key := testKey()
				if field == "namespace" {
					key.Namespace = value
				} else {
					key.Effect = value
				}
				encoded, err := key.Encode()
				assert.ErrorIs(t, err, ErrRejected)
				assert.Empty(t, encoded)
			})
		}
	}
	key := Key{Namespace: strings.Repeat("é", 32), Effect: strings.Repeat("x", 64)}
	encoded, err := key.Encode()
	require.NoError(t, err)
	decoded, err := ParseKey(encoded)
	require.NoError(t, err)
	assert.Equal(t, key, decoded)
}

func TestKeyScopeIsUnambiguous(t *testing.T) {
	t.Parallel()
	keys := []Key{
		{Namespace: "a:b", Effect: "c"},
		{Namespace: "a", Effect: "b:c"},
		{Namespace: "a\x00b", Effect: "c"},
		{Namespace: "a", Effect: "b\x00c"},
		{Namespace: "a:b", Effect: "c", Producer: [16]byte{1}},
		{Namespace: "a:b", Effect: "c", Sequence: 1},
	}
	seen := make(map[string]struct{})
	for _, key := range keys {
		encoded, err := key.Encode()
		require.NoError(t, err)
		assert.NotContains(t, seen, encoded, "key %+v", key)
		seen[encoded] = struct{}{}
		decoded, err := ParseKey(encoded)
		require.NoError(t, err)
		assert.Equal(t, key, decoded)
	}
}

func TestParseKeyRejectsMalformed(t *testing.T) {
	t.Parallel()
	encoded, err := testKey().Encode()
	require.NoError(t, err)
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	require.NoError(t, err)
	cases := []string{"", "!", encoded + "=", encoded + "\n", strings.Repeat("a", 211)}
	for size := range len(raw) {
		cases = append(cases, base64.RawURLEncoding.EncodeToString(raw[:size]))
	}
	changed := bytes.Clone(raw)
	changed[0] = 2
	cases = append(cases, base64.RawURLEncoding.EncodeToString(changed))
	changed = bytes.Clone(raw)
	changed[1], changed[2] = 255, 255
	cases = append(cases, base64.RawURLEncoding.EncodeToString(changed))
	cases = append(cases, base64.RawURLEncoding.EncodeToString(append(raw, 0)))
	for _, input := range cases {
		key, err := ParseKey(input)
		assert.ErrorIs(t, err, ErrRejected, "input %q", input)
		assert.Zero(t, key)
	}
}

func FuzzParseKey(f *testing.F) {
	encoded, err := testKey().Encode()
	require.NoError(f, err)
	f.Add(encoded)
	f.Add("")
	f.Add("AQ")
	f.Fuzz(func(t *testing.T, input string) {
		key, err := ParseKey(input)
		if err != nil {
			return
		}
		canonical, err := key.Encode()
		require.NoError(t, err)
		assert.Equal(t, input, canonical)
	})
}

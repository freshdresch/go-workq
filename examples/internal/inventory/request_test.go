package inventory

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestPreservesExactBytes(t *testing.T) {
	t.Parallel()
	for _, payload := range []string{
		`{"item":"widget","delta":3}`,
		" { \"delta\" : 3, \"item\" : \"widget\" }\n",
		`{"item":"w\u0069dget","delta":3}`,
	} {
		request := Request{Key: testKey(), Codec: Codec, Payload: []byte(payload)}
		original := bytes.Clone(request.Payload)
		value, err := request.adjustment()
		require.NoError(t, err)
		assert.Equal(t, adjustment{item: "widget", delta: 3}, value)
		assert.Equal(t, original, request.Payload)
	}
}

func TestRequestRejectsInvalidPayload(t *testing.T) {
	t.Parallel()
	payloads := []string{
		"", "null", "[]", "true", "3", "{}", "{", "\xff",
		`{"item":"widget"}`, `{"delta":1}`, `{"item":null,"delta":1}`,
		`{"item":"widget","delta":null}`, `{"item":"widget","delta":0}`,
		`{"item":"other","delta":1}`, `{"item":"widget","delta":1.0}`,
		`{"item":"widget","delta":1e0}`, `{"item":"widget","delta":"1"}`,
		`{"item":"widget","delta":9223372036854775808}`,
		`{"item":"widget","delta":-9223372036854775809}`,
		`{"item":"widget","delta":1,"extra":true}`,
		`{"Item":"widget","delta":1}`,
		`{"item":"widget","delta":1,"delta":2}`,
		`{"item":"widget","delta":1,"\u0064elta":2}`,
		`{"item":"widget","delta":1} {}`,
		`{"item":"widget","delta":1} garbage`,
	}
	for _, payload := range payloads {
		t.Run(payload, func(t *testing.T) {
			request := Request{Key: testKey(), Codec: Codec, Payload: []byte(payload)}
			value, err := request.adjustment()
			assert.ErrorIs(t, err, ErrRejected)
			assert.Zero(t, value)
		})
	}
}

func TestRequestBounds(t *testing.T) {
	t.Parallel()
	payload := `{"item":"widget","delta":1}`
	request := Request{
		Key: testKey(), Codec: Codec,
		Payload: []byte(payload + strings.Repeat(" ", MaxPayload-len(payload))),
	}
	_, err := request.adjustment()
	require.NoError(t, err)
	request.Payload = append(request.Payload, ' ')
	_, err = request.adjustment()
	assert.ErrorIs(t, err, ErrRejected)
	request.Payload = []byte(payload)
	for _, codec := range []string{"", "other", strings.Repeat("x", MaxCodec+1)} {
		request.Codec = codec
		_, err = request.adjustment()
		assert.ErrorIs(t, err, ErrRejected)
	}
	request.Codec = strings.Repeat("x", MaxCodec)
	assert.NoError(t, request.validateEnvelope())
	request.Key = Key{}
	assert.ErrorIs(t, request.validateEnvelope(), ErrRejected)
}

func TestAdjustmentQuantityBoundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                  string
		quantity, delta, want int64
		rejected              bool
	}{
		{name: "increase", quantity: 10, delta: 3, want: 13},
		{name: "empty", quantity: 10, delta: -10},
		{name: "maximum", quantity: 0, delta: math.MaxInt64, want: math.MaxInt64},
		{name: "maximum decrement", quantity: math.MaxInt64, delta: -math.MaxInt64},
		{name: "overflow", quantity: math.MaxInt64, delta: 1, rejected: true},
		{name: "negative result", quantity: 10, delta: -11, rejected: true},
		{name: "minimum delta", quantity: 0, delta: math.MinInt64, rejected: true},
		{name: "minimum against maximum", quantity: math.MaxInt64, delta: math.MinInt64, rejected: true},
		{name: "invalid starting quantity", quantity: -1, delta: 1, rejected: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := adjustedQuantity(tc.quantity, tc.delta)
			if tc.rejected {
				assert.ErrorIs(t, err, ErrRejected)
				assert.Zero(t, result)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, result)
		})
	}
	for _, payload := range []string{
		`{"item":"widget","delta":9223372036854775807}`,
		`{"item":"widget","delta":-9223372036854775808}`,
	} {
		request := Request{Key: testKey(), Codec: Codec, Payload: []byte(payload)}
		_, err := request.adjustment()
		require.NoError(t, err)
	}
}

func FuzzAdjustmentPayload(f *testing.F) {
	f.Add([]byte(`{"item":"widget","delta":3}`))
	f.Add([]byte("null"))
	f.Fuzz(func(t *testing.T, payload []byte) {
		original := bytes.Clone(payload)
		request := Request{Key: testKey(), Codec: Codec, Payload: payload}
		value, err := request.adjustment()
		assert.Equal(t, original, payload)
		if err != nil {
			return
		}
		assert.Equal(t, "widget", value.item)
		assert.NotZero(t, value.delta)
		assert.LessOrEqual(t, len(payload), MaxPayload)
	})
}

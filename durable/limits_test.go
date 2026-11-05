package durable

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaximumSubmitChargeEquivalence(t *testing.T) {
	t.Parallel()
	for payload := range Bytes(18) {
		for codec := range Bytes(18) {
			for count := range Count(25) {
				for _, encoded := range []Bytes{0, 1, 87, 88, 89, 175, 176, 177, 703, 704, 705, 4096} {
					checkMaximumSubmitCharge(t, payload, codec, count, encoded)
				}
			}
		}
	}
	random := rand.New(rand.NewPCG(20261103, 1))
	for range 3000 {
		checkMaximumSubmitCharge(
			t,
			Bytes(random.Uint64N(32769)),
			Bytes(random.Uint64N(257)),
			Count(random.Uint32N(513)),
			Bytes(random.Uint64N(4<<20)),
		)
	}
}

func checkMaximumSubmitCharge(t *testing.T, payload, codec Bytes, count Count, encoded Bytes) {
	t.Helper()
	policy := DurablePolicy{MaxPayload: payload, MaxCodec: codec}
	limits := BatchLimits{SubmitItems: count, SubmitEncoded: encoded}
	want, err := exhaustiveMaximumSubmitCharge(policy, limits)
	require.NoError(t, err)
	got, err := maximumSubmitCharge(policy, limits)
	require.NoError(t, err)
	assert.Equal(
		t,
		want,
		got,
		"payload=%d codec=%d count=%d encoded=%d",
		payload,
		codec,
		count,
		encoded,
	)
}

func exhaustiveMaximumSubmitCharge(policy DurablePolicy, limits BatchLimits) (Bytes, error) {
	codec, err := allocationCharge(policy.MaxCodec)
	if err != nil {
		return 0, err
	}
	maximum := commandBase
	for count := Bytes(1); count <= Bytes(limits.SubmitItems); count++ {
		base, err := checkedMultiply(count, 88)
		if err != nil {
			return 0, err
		}
		if base > limits.SubmitEncoded {
			break
		}
		payload, err := checkedMultiply(count, policy.MaxPayload)
		if err != nil {
			return 0, err
		}
		codecs, err := checkedMultiply(count, codec)
		if err != nil {
			return 0, err
		}
		// Payload and codec maxima compete for encoded space. Overestimating
		// both ensures every legal command fits an accepted configuration.
		charge, err := submitCharge(count, min(payload, limits.SubmitEncoded-base), codecs, codec)
		if err != nil {
			return 0, err
		}
		maximum = max(maximum, charge)
	}
	return maximum, nil
}

func TestMaximumSubmitChargeHugeCounts(t *testing.T) {
	t.Parallel()
	const count = Bytes(math.MaxUint32)
	for _, test := range []struct {
		name                          string
		payload, codec, encoded, want Bytes
	}{
		{"empty", 0, 0, math.MaxUint64, 162*count + 258},
		{"payload", 1, 0, math.MaxUint64, 164*count + 260},
		{"codec", 0, 1, math.MaxUint64, 178*count + 274},
		{"encodedBound", 0, 0, 88, 432},
		{"noRecord", 0, 0, 87, commandBase},
	} {
		t.Run(test.name, func(t *testing.T) {
			charge, err := maximumSubmitCharge(
				DurablePolicy{MaxPayload: test.payload, MaxCodec: test.codec},
				BatchLimits{SubmitItems: math.MaxUint32, SubmitEncoded: test.encoded},
			)
			require.NoError(t, err)
			assert.Equal(t, test.want, charge)
		})
	}
}

func TestCheckedArithmeticLimits(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name              string
		operation         func(Bytes, Bytes) (Bytes, error)
		left, right, want Bytes
		invalid           bool
	}{
		{"addMaximum", checkedAdd, math.MaxUint64 - 1, 1, math.MaxUint64, false},
		{"addOverflow", checkedAdd, math.MaxUint64, 1, 0, true},
		{"multiplyMaximum", checkedMultiply, math.MaxUint64, 1, math.MaxUint64, false},
		{"multiplyZero", checkedMultiply, math.MaxUint64, 0, 0, false},
		{"multiplyOverflow", checkedMultiply, math.MaxUint64/2 + 1, 2, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, err := test.operation(test.left, test.right)
			if test.invalid {
				assert.ErrorIs(t, err, ErrInvalidInput)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, test.want, value)
		})
	}
	length, err := allocationLength(Bytes(math.MaxInt))
	require.NoError(t, err)
	assert.Equal(t, math.MaxInt, length)
	length, err = allocationLength(Bytes(math.MaxInt) + 1)
	assert.ErrorIs(t, err, ErrInvalidInput)
	assert.Zero(t, length)
	count, err := checkedCountAdd(math.MaxUint32-1, 1, math.MaxUint32)
	require.NoError(t, err)
	assert.Equal(t, Count(math.MaxUint32), count)
	for _, values := range [][3]Count{{math.MaxUint32, 1, math.MaxUint32}, {8, 3, 10}, {11, 0, 10}} {
		count, err = checkedCountAdd(values[0], values[1], values[2])
		assert.ErrorIs(t, err, ErrInvalidInput)
		assert.Zero(t, count)
	}
	largest := min(Bytes(math.MaxInt), Bytes(math.MaxUint64/2)) &^ 7
	charge, err := allocationCharge(largest)
	require.NoError(t, err)
	assert.Equal(t, largest*2, charge)
	_, err = allocationCharge(Bytes(math.MaxInt) + 1)
	assert.ErrorIs(t, err, ErrInvalidInput)
	if Bytes(math.MaxInt) == math.MaxInt64 {
		_, err = allocationCharge(largest + 1)
		assert.ErrorIs(t, err, ErrInvalidInput)
	}
}

func TestDeliveryChargeLimits(t *testing.T) {
	t.Parallel()
	for _, size := range []Bytes{0, 1, 7, 8, 9, 8191, 8192, 8193} {
		charge, err := sourceBufferCharge(size)
		require.NoError(t, err)
		assert.Equal(t, min(2*((size+7)&^7), (size+8191)&^8191), charge)
	}
	for _, size := range []Bytes{Bytes(math.MaxInt) + 1, math.MaxUint64 - 8190, math.MaxUint64} {
		_, err := sourceBufferCharge(size)
		assert.ErrorIs(t, err, ErrInvalidInput)
		_, err = deliveryScratchCharge(size, 0)
		assert.ErrorIs(t, err, ErrInvalidInput)
		_, err = deliveryScratchCharge(0, size)
		assert.ErrorIs(t, err, ErrInvalidInput)
		_, err = deliveryCharge(size, 0)
		assert.ErrorIs(t, err, ErrInvalidInput)
		_, err = deliveryCharge(0, size)
		assert.ErrorIs(t, err, ErrInvalidInput)
	}
	if Bytes(math.MaxInt) == math.MaxInt64 {
		largest := Bytes(math.MaxInt64) &^ 7
		_, err := deliveryScratchCharge(largest, largest)
		assert.ErrorIs(t, err, ErrInvalidInput)
		_, err = deliveryCharge(largest, largest)
		assert.ErrorIs(t, err, ErrInvalidInput)
		_, err = deliveryCharge(largest, 0)
		assert.ErrorIs(t, err, ErrInvalidInput)
	}
}

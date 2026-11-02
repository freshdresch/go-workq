package durable

import "math"

const commandBase Bytes = 256

// allocationCharge bounds allocator rounding on the supported Go builds. Recheck
// this assumption when qualifying a toolchain. It is not an allocation length.
func allocationCharge(size Bytes) (Bytes, error) {
	if size > Bytes(math.MaxInt) || size > math.MaxUint64-7 {
		return 0, ErrInvalidInput
	}
	rounded := (size + 7) &^ 7
	if rounded > math.MaxUint64/2 {
		return 0, ErrInvalidInput
	}
	return rounded * 2, nil
}

func checkedAdd(left, right Bytes) (Bytes, error) {
	if left > math.MaxUint64-right {
		return 0, ErrInvalidInput
	}
	return left + right, nil
}

func checkedMultiply(left, right Bytes) (Bytes, error) {
	if right != 0 && left > math.MaxUint64/right {
		return 0, ErrInvalidInput
	}
	return left * right, nil
}

func submitCharge(count, payload, codecs, largestCodec Bytes) (Bytes, error) {
	charge := commandBase
	for _, element := range []Bytes{64, 1, 16} {
		size, err := checkedMultiply(count, element)
		if err != nil {
			return 0, err
		}
		allowance, err := allocationCharge(size)
		if err != nil {
			return 0, err
		}
		charge, err = checkedAdd(charge, allowance)
		if err != nil {
			return 0, err
		}
	}
	arena, err := allocationCharge(payload)
	if err != nil {
		return 0, err
	}
	for _, allowance := range []Bytes{arena, codecs, largestCodec} {
		charge, err = checkedAdd(charge, allowance)
		if err != nil {
			return 0, err
		}
	}
	return charge, nil
}

func lookupCharge(count Bytes) (Bytes, error) {
	charge := commandBase
	for _, element := range []Bytes{24, 40} {
		size, err := checkedMultiply(count, element)
		if err != nil {
			return 0, err
		}
		allowance, err := allocationCharge(size)
		if err != nil {
			return 0, err
		}
		charge, err = checkedAdd(charge, allowance)
		if err != nil {
			return 0, err
		}
	}
	return charge, nil
}

func maximumSubmitCharge(policy DurablePolicy, limits BatchLimits) (Bytes, error) {
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

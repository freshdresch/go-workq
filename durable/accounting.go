package durable

import "math"

const commandBase Bytes = 256

// allocationCharge bounds allocator rounding on the supported Go builds. Recheck
// this assumption when qualifying a toolchain. It is not an allocation length.
func allocationCharge(size Bytes) (Bytes, error) {
	if _, err := allocationLength(size); err != nil {
		return 0, ErrInvalidInput
	}
	rounded, err := checkedAdd(size, 7)
	if err != nil {
		return 0, err
	}
	rounded &^= 7
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
	record, err := checkedAdd(policy.MaxPayload, 88)
	if err != nil {
		return 0, err
	}
	countLimit := min(Bytes(limits.SubmitItems), limits.SubmitEncoded/88)
	crossover := min(countLimit, limits.SubmitEncoded/record)
	// Within each interval, the charge is affine for each count modulo eight.
	// Its maximum therefore occurs at one of that residue's two endpoints.
	second, err := checkedAdd(crossover, 1)
	if err != nil {
		return 0, err
	}
	maximum := commandBase
	for _, interval := range [][2]Bytes{{1, crossover}, {second, countLimit}} {
		first, last := interval[0], interval[1]
		if first > last {
			continue
		}
		for residue := range Bytes(8) {
			offset := (residue + 8 - first%8) % 8
			if offset > last-first {
				continue
			}
			start, err := checkedAdd(first, offset)
			if err != nil {
				return 0, err
			}
			end := last - (last-start)%8
			for _, count := range []Bytes{start, end} {
				base, err := checkedMultiply(count, 88)
				if err != nil {
					return 0, err
				}
				payload, err := checkedMultiply(count, policy.MaxPayload)
				if err != nil {
					return 0, err
				}
				codecs, err := checkedMultiply(count, codec)
				if err != nil {
					return 0, err
				}
				charge, err := submitCharge(
					count,
					min(payload, limits.SubmitEncoded-base),
					codecs,
					codec,
				)
				if err != nil {
					return 0, err
				}
				maximum = max(maximum, charge)
			}
		}
	}
	return maximum, nil
}

func allocationLength(size Bytes) (int, error) {
	if size > Bytes(math.MaxInt) {
		return 0, ErrInvalidInput
	}
	return int(size), nil
}

func checkedCountAdd(current, additional, limit Count) (Count, error) {
	if current > limit || additional > limit-current {
		return 0, ErrInvalidInput
	}
	return current + additional, nil
}

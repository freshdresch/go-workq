package inventory

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode/utf8"
)

const (
	// Codec identifies the immutable adjustment payload format.
	Codec = "inventory-adjustment-v1"
	// MaxPayload bounds the original request bytes retained in each receipt.
	MaxPayload = 1024
	// MaxCodec bounds the codec bytes retained in each receipt.
	MaxCodec = 64
)

// ErrRejected identifies an invalid key, request, or business adjustment.
var ErrRejected = errors.New("inventory request rejected")

// Request binds an effect key to its original codec and exact payload bytes.
// Callers must preserve these bytes across retries and not mutate them during a call.
type Request struct {
	Key     Key
	Codec   string
	Payload []byte
}

type adjustmentJSON struct {
	Item  *string `json:"item"`
	Delta *int64  `json:"delta"`
}

type adjustment struct {
	item  string
	delta int64
}

// validateEnvelope is separate from business decoding so Apply can resolve an
// existing key and detect changed bytes before validating a new business request.
func (r Request) validateEnvelope() error {
	if err := r.Key.validate(); err != nil {
		return err
	}
	if len(r.Codec) > MaxCodec {
		return fmt.Errorf("codec too long: %w", ErrRejected)
	}
	if len(r.Payload) > MaxPayload {
		return fmt.Errorf("payload too long: %w", ErrRejected)
	}
	return nil
}

func (r Request) adjustment() (adjustment, error) {
	if err := r.validateEnvelope(); err != nil {
		return adjustment{}, err
	}
	if r.Codec != Codec {
		return adjustment{}, fmt.Errorf("unsupported codec: %w", ErrRejected)
	}
	if !utf8.Valid(r.Payload) {
		return adjustment{}, fmt.Errorf("invalid UTF-8: %w", ErrRejected)
	}
	decoder := json.NewDecoder(bytes.NewReader(r.Payload))
	token, err := decoder.Token()
	if err != nil {
		return adjustment{}, fmt.Errorf("decode adjustment: %w: %w", ErrRejected, err)
	}
	if token != json.Delim('{') {
		return adjustment{}, fmt.Errorf("adjustment must be an object: %w", ErrRejected)
	}
	var payload adjustmentJSON
	seen := make(map[string]struct{}, 2)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return adjustment{}, fmt.Errorf("decode field: %w: %w", ErrRejected, err)
		}
		field, ok := token.(string)
		if !ok {
			return adjustment{}, fmt.Errorf("invalid field: %w", ErrRejected)
		}
		if _, duplicate := seen[field]; duplicate {
			return adjustment{}, fmt.Errorf("duplicate field: %w", ErrRejected)
		}
		seen[field] = struct{}{}
		switch field {
		case "item":
			err = decoder.Decode(&payload.Item)
		case "delta":
			err = decoder.Decode(&payload.Delta)
		default:
			return adjustment{}, fmt.Errorf("unknown field %q: %w", field, ErrRejected)
		}
		if err != nil {
			return adjustment{}, fmt.Errorf("decode %s: %w: %w", field, ErrRejected, err)
		}
	}
	if _, err := decoder.Token(); err != nil {
		return adjustment{}, fmt.Errorf("end adjustment: %w: %w", ErrRejected, err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return adjustment{}, fmt.Errorf("trailing adjustment data: %w", ErrRejected)
	}
	if payload.Item == nil || *payload.Item != "widget" {
		return adjustment{}, fmt.Errorf("item must be widget: %w", ErrRejected)
	}
	if payload.Delta == nil || *payload.Delta == 0 {
		return adjustment{}, fmt.Errorf("delta must be a nonzero signed integer: %w", ErrRejected)
	}
	return adjustment{item: *payload.Item, delta: *payload.Delta}, nil
}

func adjustedQuantity(quantity, delta int64) (int64, error) {
	if quantity < 0 {
		return 0, fmt.Errorf("negative inventory quantity: %w", ErrRejected)
	}
	if delta > 0 && quantity > math.MaxInt64-delta {
		return 0, fmt.Errorf("quantity overflow: %w", ErrRejected)
	}
	if delta < -quantity {
		return 0, fmt.Errorf("insufficient inventory: %w", ErrRejected)
	}
	return quantity + delta, nil
}

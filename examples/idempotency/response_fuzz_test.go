package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-workq/examples/internal/inventory"
)

func FuzzReceiptResponse(f *testing.F) {
	request := inventory.Request{
		Key:   inventory.Key{Namespace: "response-fuzz", Sequence: 1, Effect: "inventory.adjust"},
		Codec: inventory.Codec, Payload: []byte(`{"item":"widget","delta":3}`),
	}
	key, err := request.Key.Encode()
	require.NoError(f, err)
	envelope := successEnvelope{
		State: "succeeded", Key: key, Codec: request.Codec, Request: request.Payload,
		Result: resultEnvelope{AdjustmentID: key, Quantity: 0},
	}
	encoded, err := json.Marshal(envelope)
	require.NoError(f, err)
	valid := string(encoded)
	result, err := matchingResult(encoded, key, request)
	require.NoError(f, err)
	require.Equal(f, inventory.Result{AdjustmentID: key, Quantity: 0}, result)
	padded := valid + strings.Repeat(" ", maxResponseBytes-len(valid))
	for _, body := range []string{valid, valid + " \n\t", padded} {
		f.Add([]byte(body))
	}
	reordered := strings.TrimPrefix(valid, `{"state":"succeeded",`)
	f.Add([]byte(`{` + strings.TrimSuffix(reordered, "}") + `,"state":"succeeded"}`))
	for _, quantity := range []int64{3, -1} {
		changed := envelope
		changed.Result.Quantity = quantity
		body, err := json.Marshal(changed)
		require.NoError(f, err)
		_, err = decodeReceiptResponse(body)
		require.NoError(f, err)
		if quantity < 0 {
			result, err := matchingResult(body, key, request)
			require.ErrorIs(f, err, inventory.ErrUnresolved)
			require.Equal(f, inventory.Result{}, result)
		}
		f.Add(body)
	}
	wrongKey := envelope
	wrongKey.Key = "other"
	wrongPayload := envelope
	wrongPayload.Request = []byte("other")
	for _, changed := range []successEnvelope{wrongKey, wrongPayload} {
		body, err := json.Marshal(changed)
		require.NoError(f, err)
		_, err = decodeReceiptResponse(body)
		require.NoError(f, err)
		result, err := matchingResult(body, key, request)
		require.ErrorIs(f, err, inventory.ErrUnresolved)
		require.Equal(f, inventory.Result{}, result)
		f.Add(body)
	}
	resultJSON, err := json.Marshal(envelope.Result)
	require.NoError(f, err)
	requestField := `"request":"` + base64.StdEncoding.EncodeToString(request.Payload) + `"`
	resultField := `"result":` + string(resultJSON)
	malformed := []string{
		"", `null`, `[]`, `{`, valid + `{}`, valid + `x`, padded + " ",
		strings.Repeat("x", maxResponseBytes+1),
		strings.Replace(valid, `"quantity":0`, `"quantity":9223372036854775808`, 1),
		strings.Replace(valid, `,"quantity":0`, "", 1),
		strings.Replace(valid, `"quantity":0`, `"quantity":null`, 1),
		strings.Replace(valid, `,`+resultField, "", 1),
		strings.Replace(valid, resultField, `"result":null`, 1),
		strings.Replace(
			valid,
			resultField,
			`"result":{"adjustment_id":"id"},"result":{"quantity":0}`,
			1,
		),
		strings.TrimSuffix(valid, "}") + `,"state":"succeeded"}`,
		strings.Replace(valid, `"quantity":0`, `"quantity":0,"quantity":1`, 1),
		strings.Replace(valid, `"quantity":0`, `"quantity":0,"qu\u0061ntity":1`, 1),
		strings.TrimSuffix(valid, "}") + `,"st\u0061te":"succeeded"}`,
		strings.Replace(valid, `"state":`, `"State":`, 1),
		strings.Replace(valid, `"quantity":`, `"Quantity":`, 1),
		strings.Replace(valid, requestField, `"request":[123,125]`, 1),
		strings.Replace(valid, requestField, `"request":"!"`, 1),
		strings.Replace(valid, requestField, `"request":"Zh=="`, 1),
		strings.Replace(valid, requestField, `"request":"Zg==\n"`, 1),
	}
	for _, body := range malformed {
		receipt, err := decodeReceiptResponse([]byte(body))
		require.Error(f, err, "malformed seed %q", body)
		require.Equal(f, receiptResponse{}, receipt)
		f.Add([]byte(body))
	}

	f.Fuzz(func(t *testing.T, body []byte) {
		receipt, decodeErr := decodeReceiptResponse(body)
		if len(body) > maxResponseBytes {
			require.Error(t, decodeErr)
		}
		result, matchErr := matchingResult(body, key, request)
		if matchErr != nil {
			assert.ErrorIs(t, matchErr, inventory.ErrUnresolved)
			assert.Equal(t, inventory.Result{}, result)
		}
		if decodeErr != nil {
			assert.Equal(t, receiptResponse{}, receipt)
			assert.ErrorIs(t, matchErr, inventory.ErrUnresolved)
			return
		}
		var independent successEnvelope
		require.NoError(t, json.Unmarshal(body, &independent))
		assert.Equal(t, independent.State, receipt.State)
		assert.Equal(t, independent.Key, receipt.Key)
		assert.Equal(t, independent.Codec, receipt.Codec)
		assert.Equal(t, independent.Request, receipt.Request)
		assert.Equal(t, independent.Result, receipt.Result)
		if matchErr == nil {
			assert.Equal(t, "succeeded", independent.State)
			assert.Equal(t, key, independent.Key)
			assert.Equal(t, key, independent.Result.AdjustmentID)
			assert.Equal(t, request.Codec, independent.Codec)
			assert.Equal(t, request.Payload, independent.Request)
			assert.GreaterOrEqual(t, independent.Result.Quantity, int64(0))
			assert.Equal(t, independent.Result.Quantity, result.Quantity)
			assert.Equal(t, key, result.AdjustmentID)
			assert.False(t, result.Duplicate)
		}
		canonical, err := json.Marshal(successEnvelope(receipt))
		require.NoError(t, err)
		reparsed, err := decodeReceiptResponse(canonical)
		if len(canonical) > maxResponseBytes {
			assert.Error(t, err)
			assert.Equal(t, receiptResponse{}, reparsed)
			return
		}
		require.NoError(t, err)
		assert.Equal(t, receipt, reparsed)
		for _, malformed := range []string{
			strings.TrimSuffix(string(canonical), "}") + `,"state":"succeeded"}`,
			strings.Replace(string(canonical), `"quantity":`, `"quantity":0,"quantity":`, 1),
			strings.Replace(string(canonical), `"state":`, `"State":`, 1),
		} {
			if len(malformed) <= maxResponseBytes {
				assertInvalidReceipt(t, malformed)
			}
		}
	})
}

func FuzzStateResponse(f *testing.F) {
	statuses := []struct {
		status int
		state  string
		want   error
	}{
		{status: 400, state: "malformed", want: inventory.ErrRejected},
		{status: 409, state: "conflict", want: inventory.ErrConflict},
		{status: 413, state: "malformed", want: inventory.ErrRejected},
		{status: 422, state: "rejected", want: inventory.ErrRejected},
		{status: 507, state: "capacity", want: inventory.ErrCapacity},
	}
	for _, state := range []string{
		"malformed", "conflict", "rejected", "capacity", "unresolved", "absent",
	} {
		body, err := json.Marshal(stateEnvelope{State: state})
		require.NoError(f, err)
		f.Add(body)
	}
	valid, err := json.Marshal(stateEnvelope{State: "conflict"})
	require.NoError(f, err)
	padded := append(bytes.Clone(valid), bytes.Repeat([]byte(" "), maxResponseBytes-len(valid))...)
	f.Add(padded)
	f.Add(append(bytes.Clone(valid), []byte(" \n\t")...))
	for _, body := range []string{
		"", `null`, `[]`, `{`, string(valid) + `{}`, string(valid) + `x`, string(padded) + " ",
		strings.Repeat("x", maxResponseBytes+1),
		`{"state":"conflict","state":"conflict"}`,
		`{"state":"conflict","state":"unresolved"}`,
		`{"state":"conflict","st\u0061te":"conflict"}`,
		`{"State":"conflict"}`, `{}`, `{"state":null}`,
		`{"state":"conflict","unknown":0}`,
	} {
		envelope, err := decodeStateResponse([]byte(body))
		require.Error(f, err, "malformed seed %q", body)
		require.Equal(f, stateEnvelope{}, envelope)
		f.Add([]byte(body))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		envelope, decodeErr := decodeStateResponse(body)
		if len(body) > maxResponseBytes {
			require.Error(t, decodeErr)
		}
		for _, tc := range statuses {
			want := inventory.ErrUnresolved
			if decodeErr == nil && envelope.State == tc.state {
				want = tc.want
			}
			assert.ErrorIs(t, responseError(http.MethodPost, tc.status, body), want)
		}
		if decodeErr != nil {
			assert.Equal(t, stateEnvelope{}, envelope)
			return
		}
		var independent stateEnvelope
		require.NoError(t, json.Unmarshal(body, &independent))
		assert.Equal(t, independent, envelope)
		canonical, err := json.Marshal(envelope)
		require.NoError(t, err)
		reparsed, err := decodeStateResponse(canonical)
		if len(canonical) > maxResponseBytes {
			assert.Error(t, err)
			assert.Equal(t, stateEnvelope{}, reparsed)
			return
		}
		require.NoError(t, err)
		assert.Equal(t, envelope, reparsed)
		for _, malformed := range []string{
			strings.TrimSuffix(string(canonical), "}") + `,"state":"conflict"}`,
			strings.Replace(string(canonical), `"state":`, `"State":`, 1),
		} {
			if len(malformed) > maxResponseBytes {
				continue
			}
			rejected, err := decodeStateResponse([]byte(malformed))
			assert.Error(t, err)
			assert.Equal(t, stateEnvelope{}, rejected)
		}
	})
}

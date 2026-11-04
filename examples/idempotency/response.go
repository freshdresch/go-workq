package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type receiptResponse struct {
	State   string         `json:"state"`
	Key     string         `json:"key"`
	Codec   string         `json:"codec"`
	Request []byte         `json:"request"`
	Result  resultEnvelope `json:"result"`
}

func decodeReceiptResponse(body []byte) (receiptResponse, error) {
	if len(body) > maxResponseBytes {
		return receiptResponse{}, errors.New("receipt too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := readResponseDelimiter(decoder, '{'); err != nil {
		return receiptResponse{}, err
	}
	const (
		stateField = 1 << iota
		keyField
		codecField
		requestField
		resultField
		allFields = stateField | keyField | codecField | requestField | resultField
	)
	var receipt receiptResponse
	var seen uint8
	for decoder.More() {
		field, err := readResponseString(decoder)
		if err != nil {
			return receiptResponse{}, err
		}
		var mask uint8
		switch field {
		case "state":
			mask = stateField
		case "key":
			mask = keyField
		case "codec":
			mask = codecField
		case "request":
			mask = requestField
		case "result":
			mask = resultField
		default:
			return receiptResponse{}, fmt.Errorf("unknown receipt field %q", field)
		}
		if seen&mask != 0 {
			return receiptResponse{}, fmt.Errorf("duplicate receipt field %q", field)
		}
		seen |= mask
		switch field {
		case "state":
			receipt.State, err = readResponseString(decoder)
		case "key":
			receipt.Key, err = readResponseString(decoder)
		case "codec":
			receipt.Codec, err = readResponseString(decoder)
		case "request":
			receipt.Request, err = readResponseRequest(decoder)
		case "result":
			receipt.Result, err = readResponseResult(decoder)
		}
		if err != nil {
			return receiptResponse{}, fmt.Errorf("receipt %s: %w", field, err)
		}
	}
	if err := readResponseDelimiter(decoder, '}'); err != nil {
		return receiptResponse{}, err
	}
	if seen != allFields {
		return receiptResponse{}, errors.New("missing receipt field")
	}
	if err := responseEOF(decoder); err != nil {
		return receiptResponse{}, err
	}
	return receipt, nil
}

func readResponseResult(decoder *json.Decoder) (resultEnvelope, error) {
	if err := readResponseDelimiter(decoder, '{'); err != nil {
		return resultEnvelope{}, err
	}
	const (
		idField = 1 << iota
		quantityField
		allFields = idField | quantityField
	)
	var result resultEnvelope
	var seen uint8
	for decoder.More() {
		field, err := readResponseString(decoder)
		if err != nil {
			return resultEnvelope{}, err
		}
		var mask uint8
		switch field {
		case "adjustment_id":
			mask = idField
		case "quantity":
			mask = quantityField
		default:
			return resultEnvelope{}, fmt.Errorf("unknown result field %q", field)
		}
		if seen&mask != 0 {
			return resultEnvelope{}, fmt.Errorf("duplicate result field %q", field)
		}
		seen |= mask
		switch field {
		case "adjustment_id":
			result.AdjustmentID, err = readResponseString(decoder)
		case "quantity":
			var quantity *int64
			err = decoder.Decode(&quantity)
			if err == nil {
				if quantity == nil {
					return resultEnvelope{}, errors.New("null quantity")
				}
				result.Quantity = *quantity
			}
		}
		if err != nil {
			return resultEnvelope{}, fmt.Errorf("result %s: %w", field, err)
		}
	}
	if err := readResponseDelimiter(decoder, '}'); err != nil {
		return resultEnvelope{}, err
	}
	if seen != allFields {
		return resultEnvelope{}, errors.New("missing result field")
	}
	return result, nil
}

func decodeStateResponse(body []byte) (stateEnvelope, error) {
	if len(body) > maxResponseBytes {
		return stateEnvelope{}, errors.New("state response too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := readResponseDelimiter(decoder, '{'); err != nil {
		return stateEnvelope{}, err
	}
	const stateField = 1
	var seen uint8
	var envelope stateEnvelope
	for decoder.More() {
		field, err := readResponseString(decoder)
		if err != nil {
			return stateEnvelope{}, err
		}
		if field != "state" {
			return stateEnvelope{}, fmt.Errorf("unknown state field %q", field)
		}
		if seen&stateField != 0 {
			return stateEnvelope{}, errors.New("duplicate state field")
		}
		seen |= stateField
		envelope.State, err = readResponseString(decoder)
		if err != nil {
			return stateEnvelope{}, err
		}
	}
	if err := readResponseDelimiter(decoder, '}'); err != nil {
		return stateEnvelope{}, err
	}
	if seen != stateField {
		return stateEnvelope{}, errors.New("missing state field")
	}
	if err := responseEOF(decoder); err != nil {
		return stateEnvelope{}, err
	}
	return envelope, nil
}

func readResponseRequest(decoder *json.Decoder) ([]byte, error) {
	encoded, err := readResponseString(decoder)
	if err != nil {
		return nil, err
	}
	payload, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode request bytes: %w", err)
	}
	if base64.StdEncoding.EncodeToString(payload) != encoded {
		return nil, errors.New("noncanonical request encoding")
	}
	return payload, nil
}

func readResponseString(decoder *json.Decoder) (string, error) {
	token, err := decoder.Token()
	if err != nil {
		return "", err
	}
	value, ok := token.(string)
	if !ok {
		return "", errors.New("expected string")
	}
	return value, nil
}

func readResponseDelimiter(decoder *json.Decoder, want json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != want {
		return fmt.Errorf("expected %q", want)
	}
	return nil
}

func responseEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing response data")
	}
	return nil
}

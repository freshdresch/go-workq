package main

import (
	"encoding/json"
	"net/http"
)

const maxResponseBytes = 4 << 10

type resultEnvelope struct {
	AdjustmentID string `json:"adjustment_id"`
	Quantity     int64  `json:"quantity"`
}

type successEnvelope struct {
	State   string         `json:"state"`
	Key     string         `json:"key"`
	Codec   string         `json:"codec"`
	Request []byte         `json:"request"`
	Result  resultEnvelope `json:"result"`
}

type stateEnvelope struct {
	State string `json:"state"`
}

func writeState(w http.ResponseWriter, status int, state string) {
	writeJSON(w, status, stateEnvelope{State: state})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil || len(body) > maxResponseBytes {
		// No response has started. Never turn an unrepresentable receipt into success.
		writeState(w, http.StatusServiceUnavailable, "unresolved")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		// The peer cannot use a partial receipt. Abort without changing the stored outcome.
		panic(http.ErrAbortHandler)
	}
}

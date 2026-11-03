package main

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"sync"
	"time"

	"github.com/freshdresch/go-data-structures/workq/examples/internal/inventory"
)

const (
	maxActiveRequests = 4
	operationTimeout  = 5 * time.Second
)

func newServer(store *inventory.Store) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /adjustments", handleApply(store))
	mux.Handle("GET /adjustments/{key}", handleLookup(store))
	return mux
}

func handleApply(store *inventory.Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.Header.Values("Idempotency-Key")) != 1 {
			writeState(w, http.StatusBadRequest, "malformed")
			return
		}
		if len(r.Header.Values("X-Adjustment-Codec")) != 1 {
			writeState(w, http.StatusBadRequest, "malformed")
			return
		}
		key, err := inventory.ParseKey(r.Header.Get("Idempotency-Key"))
		if err != nil {
			writeState(w, http.StatusBadRequest, "malformed")
			return
		}
		codec := r.Header.Get("X-Adjustment-Codec")
		if len(codec) == 0 || len(codec) > inventory.MaxCodec {
			writeState(w, http.StatusBadRequest, "malformed")
			return
		}
		contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || contentType != "application/json" || len(r.Header.Values("Content-Type")) != 1 {
			writeState(w, http.StatusBadRequest, "malformed")
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, inventory.MaxPayload))
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeState(w, http.StatusRequestEntityTooLarge, "malformed")
				return
			}
			writeState(w, http.StatusBadRequest, "malformed")
			return
		}
		request := inventory.Request{Key: key, Codec: codec, Payload: body}
		ctx, cancel := context.WithTimeout(r.Context(), operationTimeout)
		defer cancel()
		result, err := store.Apply(ctx, request)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeSuccess(w, inventory.Receipt{Key: key, Codec: codec, Payload: body, Result: result})
	})
}

func handleLookup(store *inventory.Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, err := inventory.ParseKey(r.PathValue("key"))
		if err != nil {
			writeState(w, http.StatusBadRequest, "malformed")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), operationTimeout)
		defer cancel()
		receipt, found, err := store.Lookup(ctx, key)
		if err != nil {
			writeState(w, http.StatusServiceUnavailable, "unresolved")
			return
		}
		if !found {
			writeState(w, http.StatusNotFound, "absent")
			return
		}
		writeSuccess(w, receipt)
	})
}

func writeSuccess(w http.ResponseWriter, receipt inventory.Receipt) {
	key, err := receipt.Key.Encode()
	if err != nil {
		writeState(w, http.StatusServiceUnavailable, "unresolved")
		return
	}
	writeJSON(w, http.StatusOK, successEnvelope{
		State: "succeeded", Key: key, Codec: receipt.Codec, Request: receipt.Payload,
		Result: resultEnvelope{
			AdjustmentID: receipt.Result.AdjustmentID, Quantity: receipt.Result.Quantity,
		},
	})
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, inventory.ErrUnresolved):
		writeState(w, http.StatusServiceUnavailable, "unresolved")
	case errors.Is(err, inventory.ErrConflict):
		writeState(w, http.StatusConflict, "conflict")
	case errors.Is(err, inventory.ErrRejected):
		writeState(w, http.StatusUnprocessableEntity, "rejected")
	case errors.Is(err, inventory.ErrCapacity):
		writeState(w, http.StatusInsufficientStorage, "capacity")
	default:
		writeState(w, http.StatusServiceUnavailable, "unresolved")
	}
}

// requestAdmission owns handlers independently of client connection lifetime.
// The owner closes admission before waiting and before closing the Store.
type requestAdmission struct {
	handler  http.Handler
	failed   <-chan struct{}
	slots    chan struct{}
	mu       sync.Mutex
	closed   bool
	handlers sync.WaitGroup
}

func newRequestAdmission(store *inventory.Store, handler http.Handler) *requestAdmission {
	return &requestAdmission{
		handler: handler, failed: store.Failed(), slots: make(chan struct{}, maxActiveRequests),
	}
}

func (a *requestAdmission) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !a.admit() {
		writeState(w, http.StatusServiceUnavailable, "unresolved")
		return
	}
	defer a.handlers.Done()
	defer func() { <-a.slots }()
	a.handler.ServeHTTP(w, r)
}

func (a *requestAdmission) admit() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return false
	}
	select {
	case <-a.failed:
		return false
	default:
	}
	select {
	case a.slots <- struct{}{}:
		a.handlers.Add(1)
		return true
	default:
		return false
	}
}

func (a *requestAdmission) closeAdmission() {
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()
}

func (a *requestAdmission) waitHandlers() {
	a.handlers.Wait()
}

func newHTTPServer(handler http.Handler) *http.Server {
	return &http.Server{
		Handler: handler, ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout: operationTimeout, WriteTimeout: operationTimeout,
		IdleTimeout: operationTimeout, MaxHeaderBytes: 4 << 10,
	}
}

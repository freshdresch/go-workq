//go:build linux && amd64 && cgo

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-workq/durable"
	"github.com/freshdresch/go-workq/examples/internal/inventory"
)

func waitRecoveryStage(t *testing.T, stage <-chan struct{}) {
	t.Helper()
	select {
	case <-stage:
	case <-time.After(operationTimeout):
		t.Fatal("remote recovery stage did not finish")
	}
}

func TestRemoteDelayedPOSTAfterClientCancellation(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	release := make(chan struct{})
	joined := make(chan struct{})
	var oldBody []byte
	var posts atomic.Int64
	serverCtx, stopServer := context.WithTimeout(context.Background(), 4*operationTimeout)
	defer stopServer()
	service := startService(t, 4, func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || posts.Add(1) != 1 {
				handler.ServeHTTP(w, r)
				return
			}
			defer close(joined)
			// Read the original wire bytes before canceling their transport.
			body, err := io.ReadAll(r.Body)
			if !assert.NoError(t, err) {
				return
			}
			close(entered)
			select {
			case <-release:
			case <-serverCtx.Done():
				return
			}
			// Remote work has its own bounded owner. Client cancellation does not fence it.
			delayed := r.Clone(serverCtx)
			delayed.Body = io.NopCloser(bytes.NewReader(body))
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, delayed)
			assert.Equal(t, http.StatusOK, recorder.Code)
			oldBody = bytes.Clone(recorder.Body.Bytes())
		})
	})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(func() {
		unblock()
		stopServer()
		service.stop()
	})
	client := testAdjustmentClient(t, service.address)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request := adjustmentRequest(1, 3)
	clientJoined := make(chan struct{})
	var clientErr error
	go func() {
		defer close(clientJoined)
		_, clientErr = client.apply(ctx, request)
	}()
	t.Cleanup(func() {
		cancel()
		waitRecoveryStage(t, clientJoined)
		client.closeIdleConnections()
	})
	waitRecoveryStage(t, entered)
	cancel()
	waitRecoveryStage(t, clientJoined)
	assert.ErrorIs(t, clientErr, context.Canceled)
	assert.ErrorIs(t, clientErr, inventory.ErrUnresolved)
	select {
	case <-joined:
		t.Fatal("old server request stopped before its explicit release")
	default:
	}
	assertInventory(t, service.path, 10, 0)
	key, err := request.Key.Encode()
	require.NoError(t, err)
	_, err = client.exchange(t.Context(), http.MethodGet, key, request)
	require.ErrorIs(t, err, inventory.ErrUnresolved)
	result, err := client.apply(t.Context(), request)
	require.NoError(t, err)
	assertInventory(t, service.path, 13, 1, expectedReceipt{request: request, quantity: 13})
	unblock()
	waitRecoveryStage(t, joined)
	oldResult, err := matchingResult(oldBody, key, request)
	require.NoError(t, err)
	assert.Equal(t, result, oldResult)
	client.closeIdleConnections()
	service.stop()
	assertInventory(t, service.path, 13, 1, expectedReceipt{request: request, quantity: 13})
}

func openRecoveryQueue(t *testing.T, directory string) *durable.Queue {
	t.Helper()
	config, err := exampleConfig()
	require.NoError(t, err)
	queue, err := durable.Open(t.Context(), directory, config)
	if errors.Is(err, durable.ErrUnsupported) {
		t.Skipf("queue storage profile unsupported: %v", err)
	}
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cancel()
		assert.NoError(t, queue.Close(ctx))
	})
	return queue
}

func startRecoveryServer(
	t *testing.T,
	store *inventory.Store,
	handler http.Handler,
) (string, func()) {
	t.Helper()
	admission := newRequestAdmission(store, handler)
	listener, err := listenLoopback(t.Context())
	require.NoError(t, err)
	server := newHTTPServer(admission)
	joined := make(chan struct{})
	var serveErr error
	go func() {
		defer close(joined)
		serveErr = server.Serve(listener)
	}()
	stop := sync.OnceFunc(func() {
		admission.closeAdmission()
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cancel()
		shutdownErr := server.Shutdown(ctx)
		if shutdownErr != nil {
			assert.NoError(t, server.Close())
		}
		assert.NoError(t, shutdownErr)
		waitRecoveryStage(t, joined)
		assert.ErrorIs(t, serveErr, http.ErrServerClosed)
		// Shutdown does not wait for hijacked handlers.
		admission.waitHandlers()
	})
	t.Cleanup(stop)
	return "http://" + listener.Addr().String(), stop
}

func TestRemoteRecoveryAcrossIndependentRestarts(t *testing.T) {
	for _, restart := range []string{"server", "client"} {
		t.Run(restart, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			directory := filepath.Join(root, "queue")
			path := filepath.Join(root, "inventory.sqlite")
			queue := openRecoveryQueue(t, directory)
			require.NoError(t, submitSource(t.Context(), queue))
			deliveries, err := queue.Receive(t.Context(), 1)
			require.NoError(t, err)
			oldDelivery := deliveries[0]
			request := effectRequest(oldDelivery)
			store, err := inventory.Open(t.Context(), path, 64)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, store.Close()) })
			committed := make(chan struct{})
			dropped := make(chan struct{})
			var first atomic.Bool
			handler := newServer(store)
			address, stop := startRecoveryServer(t, store, http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodPost || !first.CompareAndSwap(false, true) {
						handler.ServeHTTP(w, r)
						return
					}
					defer close(dropped)
					recorder := httptest.NewRecorder()
					handler.ServeHTTP(recorder, r)
					if !assert.Equal(t, http.StatusOK, recorder.Code) {
						return
					}
					close(committed)
					connection, _, hijackErr := http.NewResponseController(w).Hijack()
					if !assert.NoError(t, hijackErr) {
						return
					}
					assert.NoError(t, connection.Close())
				},
			))
			client := testAdjustmentClient(t, address)
			key, err := request.Key.Encode()
			require.NoError(t, err)
			// One real POST leaves recovery to the restarted owner, before any Ack.
			_, err = client.exchange(t.Context(), http.MethodPost, key, request)
			require.ErrorIs(t, err, inventory.ErrUnresolved)
			waitRecoveryStage(t, committed)
			waitRecoveryStage(t, dropped)
			assertInventory(t, path, 13, 1, expectedReceipt{request: request, quantity: 13})
			statuses, err := queue.Lookup(t.Context(), sourceIDs())
			require.NoError(t, err)
			for _, status := range statuses {
				assert.Equal(t, durable.Pending, status.Kind)
			}
			client.closeIdleConnections()
			if restart == "server" {
				stop()
				require.NoError(t, store.Close())
				store, err = inventory.Open(t.Context(), path, 64)
				require.NoError(t, err)
				address, stop = startRecoveryServer(t, store, newServer(store))
				// Preserve this client while replacing only its remote endpoint.
				client.address = address
			} else {
				client = testAdjustmentClient(t, address)
			}
			current := oldDelivery
			if restart == "client" {
				require.NoError(t, queue.Close(t.Context()))
				queue = openRecoveryQueue(t, directory)
				require.NoError(t, submitSource(t.Context(), queue))
				commit, ackErr := queue.AckBatch(t.Context(), []durable.Token{oldDelivery.Token})
				require.ErrorIs(t, ackErr, durable.ErrStaleToken)
				assert.Equal(t, durable.NotApplied, commit.Decision)
				deliveries, err = queue.Receive(t.Context(), 1)
				require.NoError(t, err)
				current = deliveries[0]
			}
			queue.StopAdmission()
			require.Equal(t, request, effectRequest(current))
			result, err := client.apply(t.Context(), effectRequest(current))
			require.NoError(t, err)
			assert.Equal(t, key, result.AdjustmentID)
			assert.Equal(t, int64(13), result.Quantity)
			require.NoError(t, acknowledge(t.Context(), queue, current))
			observed, err := work(t.Context(), t.Context(), queue, client, io.Discard, false)
			require.NoError(t, err)
			assert.Equal(t, 2, observed.acknowledged)
			client.closeIdleConnections()
			stop()
			require.NoError(t, queue.Close(t.Context()))
			entries := sourceEntries()
			var receipts []expectedReceipt
			for index, quantity := range []int64{13, 12, 14} {
				entry := entries[index]
				receipts = append(receipts, expectedReceipt{
					request: effectRequest(durable.Delivery{
						ID: entry.ID, Codec: entry.Codec, Payload: entry.Payload,
					}),
					quantity: quantity,
				})
			}
			assertInventory(t, path, 14, 3, receipts...)
			assertCompleted(t, directory)
		})
	}
}

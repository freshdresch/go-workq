//go:build linux && amd64 && cgo

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-workq/durable"
	"github.com/freshdresch/go-workq/examples/internal/inventory"
)

func testAdjustmentClient(t *testing.T, address string) *adjustmentClient {
	t.Helper()
	client, err := newAdjustmentClient(address)
	require.NoError(t, err)
	t.Cleanup(client.closeIdleConnections)
	return client
}

func TestClientApplyAndDuplicate(t *testing.T) {
	t.Parallel()
	service := startService(t, 4, nil)
	client := testAdjustmentClient(t, service.address)
	request := adjustmentRequest(1, 3)
	first, err := client.apply(t.Context(), request)
	require.NoError(t, err)
	second, err := client.apply(t.Context(), adjustmentRequest(2, -1))
	require.NoError(t, err)
	assert.Equal(t, int64(12), second.Quantity)
	repeated, err := client.apply(t.Context(), request)
	require.NoError(t, err)
	assert.Equal(t, first, repeated)
	assert.Equal(t, int64(13), repeated.Quantity)
	assertInventory(t, service.path, 12, 2,
		expectedReceipt{request: request, quantity: 13},
		expectedReceipt{request: adjustmentRequest(2, -1), quantity: 12})
}

func TestClientResolvesLostReply(t *testing.T) {
	t.Parallel()
	var posts, lookups atomic.Int64
	service := startService(t, 4, func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				lookups.Add(1)
				handler.ServeHTTP(w, r)
				return
			}
			posts.Add(1)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, r)
			assert.Equal(t, http.StatusOK, recorder.Code)
			connection, _, err := http.NewResponseController(w).Hijack()
			if !assert.NoError(t, err) {
				return
			}
			assert.NoError(t, connection.Close())
		})
	})
	client := testAdjustmentClient(t, service.address)
	request := adjustmentRequest(1, 3)
	result, err := client.apply(t.Context(), request)
	require.NoError(t, err)
	assert.True(t, result.Duplicate)
	assert.Equal(t, int64(13), result.Quantity)
	assert.Equal(t, int64(1), posts.Load())
	assert.GreaterOrEqual(t, lookups.Load(), int64(1))
	assertInventory(t, service.path, 13, 1, expectedReceipt{request: request, quantity: 13})
}

func TestClientRetriesAbsentWithOriginalBytes(t *testing.T) {
	t.Parallel()
	request := adjustmentRequest(1, 3)
	request.Payload = []byte(" {\n\"delta\":3, \"item\":\"widget\"}\n")
	key, err := request.Key.Encode()
	require.NoError(t, err)
	var posts atomic.Int64
	service := startService(t, 4, func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				assert.Equal(t, "/adjustments/"+key, r.URL.Path)
				handler.ServeHTTP(w, r)
				return
			}
			assert.Equal(t, key, r.Header.Get("Idempotency-Key"))
			assert.Equal(t, request.Codec, r.Header.Get("X-Adjustment-Codec"))
			if posts.Add(1) == 1 {
				body, readErr := io.ReadAll(r.Body)
				assert.NoError(t, readErr)
				assert.Equal(t, request.Payload, body)
				writeState(w, http.StatusServiceUnavailable, "unresolved")
				return
			}
			handler.ServeHTTP(w, r)
		})
	})
	client := testAdjustmentClient(t, service.address)
	result, err := client.apply(t.Context(), request)
	require.NoError(t, err)
	assert.Equal(t, int64(13), result.Quantity)
	assert.Equal(t, int64(2), posts.Load())
	assertInventory(t, service.path, 13, 1, expectedReceipt{request: request, quantity: 13})
}

func TestClientRejectsUnusableSuccess(t *testing.T) {
	t.Parallel()
	request := adjustmentRequest(1, 3)
	key, err := request.Key.Encode()
	require.NoError(t, err)
	valid := successEnvelope{
		State: "succeeded", Key: key, Codec: request.Codec, Request: request.Payload,
		Result: resultEnvelope{AdjustmentID: key, Quantity: 13},
	}
	validBody, err := json.Marshal(valid)
	require.NoError(t, err)
	cases := []struct {
		name string
		body string
	}{
		{name: "wrong key", body: strings.Replace(string(validBody), `"key":"`, `"key":"x`, 1)},
		{name: "wrong codec", body: strings.Replace(string(validBody), request.Codec, "other", 1)},
		{name: "wrong bytes", body: strings.Replace(string(validBody),
			base64.StdEncoding.EncodeToString(request.Payload), "eA==", 1)},
		{name: "wrong result ID", body: strings.Replace(string(validBody),
			`"adjustment_id":"`, `"adjustment_id":"x`, 1)},
		{name: "negative quantity", body: strings.Replace(string(validBody), `:13`, `:-1`, 1)},
		{name: "overflow quantity", body: strings.Replace(string(validBody),
			`:13`, `:9223372036854775808`, 1)},
		{name: "missing quantity", body: strings.Replace(string(validBody), `,"quantity":13`, "", 1)},
		{name: "null quantity", body: strings.Replace(string(validBody), `:13`, `:null`, 1)},
		{name: "missing result", body: `{"state":"succeeded"}`},
		{name: "malformed", body: `{"state":`},
		{name: "null", body: `null`},
		{name: "processing", body: `{"state":"processing"}`},
		{name: "trailing JSON", body: string(validBody) + `{}`},
		{name: "unknown field", body: strings.TrimSuffix(string(validBody), "}") + `,"other":1}`},
		{name: "oversized", body: string(validBody) + strings.Repeat(" ", maxResponseBytes)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var posts atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts.Add(1)
				}
				w.Header().Set("Content-Type", "application/json")
				_, writeErr := io.WriteString(w, tc.body)
				assert.NoError(t, writeErr)
			}))
			t.Cleanup(server.Close)
			client := testAdjustmentClient(t, server.URL)
			result, applyErr := client.apply(t.Context(), request)
			assert.ErrorIs(t, applyErr, inventory.ErrUnresolved)
			assert.Equal(t, inventory.Result{}, result)
			assert.Equal(t, int64(maxApplyAttempts), posts.Load())
		})
	}
}

func TestClientDefinitiveRejections(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		status int
		state  string
		want   error
	}{
		{name: "conflict", status: 409, state: "conflict", want: inventory.ErrConflict},
		{name: "rejected", status: 422, state: "rejected", want: inventory.ErrRejected},
		{name: "capacity", status: 507, state: "capacity", want: inventory.ErrCapacity},
		{name: "malformed", status: 400, state: "malformed", want: inventory.ErrRejected},
		{name: "oversized", status: 413, state: "malformed", want: inventory.ErrRejected},
		{name: "mismatched state", status: 409, state: "unresolved", want: inventory.ErrUnresolved},
		{name: "unexpected 2xx", status: 202, state: "succeeded", want: inventory.ErrUnresolved},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var posts atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts.Add(1)
				}
				writeState(w, tc.status, tc.state)
			}))
			t.Cleanup(server.Close)
			client := testAdjustmentClient(t, server.URL)
			result, err := client.apply(t.Context(), adjustmentRequest(1, 3))
			assert.ErrorIs(t, err, tc.want)
			assert.Equal(t, inventory.Result{}, result)
			if tc.want != inventory.ErrUnresolved {
				assert.Equal(t, int64(1), posts.Load())
			}
		})
	}
}

func TestClientDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	var followed atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		followed.Add(1)
		writeState(w, http.StatusOK, "succeeded")
	}))
	t.Cleanup(target.Close)
	for _, status := range []int{301, 302, 303, 307, 308} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL, status)
		}))
		client := testAdjustmentClient(t, server.URL)
		result, err := client.apply(t.Context(), adjustmentRequest(1, 3))
		assert.ErrorIs(t, err, inventory.ErrUnresolved)
		assert.Equal(t, inventory.Result{}, result)
		client.closeIdleConnections()
		server.Close()
	}
	assert.Zero(t, followed.Load())
}

func TestClientCancellationAndTimeout(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"cancellation", "body timeout"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			entered := make(chan struct{})
			joined := make(chan struct{})
			var first sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					writeState(w, http.StatusNotFound, "absent")
					return
				}
				first.Do(func() {
					defer close(joined)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					assert.NoError(t, http.NewResponseController(w).Flush())
					close(entered)
					<-r.Context().Done()
				})
			}))
			t.Cleanup(server.Close)
			client := testAdjustmentClient(t, server.URL)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if name == "body timeout" {
				client.http.Timeout = 50 * time.Millisecond
			}
			done := make(chan error, 1)
			go func() {
				_, err := client.apply(ctx, adjustmentRequest(1, 3))
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(operationTimeout):
				t.Fatal("client did not reach handler")
			}
			if name == "cancellation" {
				cancel()
			}
			select {
			case err := <-done:
				assert.ErrorIs(t, err, inventory.ErrUnresolved)
				if name == "cancellation" {
					assert.ErrorIs(t, err, context.Canceled)
				}
			case <-time.After(operationTimeout):
				t.Fatal("client did not finish")
			}
			select {
			case <-joined:
			case <-time.After(operationTimeout):
				t.Fatal("canceled handler did not finish")
			}
		})
	}
}

func TestClientTransportBounds(t *testing.T) {
	t.Parallel()
	client := testAdjustmentClient(t, "http://127.0.0.1:1234")
	assert.Equal(t, 5*time.Second, client.http.Timeout)
	transport, ok := client.http.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Equal(t, 4, transport.MaxConnsPerHost)
	assert.Equal(t, 4, transport.MaxIdleConnsPerHost)
	assert.Equal(t, 5*time.Second, transport.IdleConnTimeout)
	assert.Equal(t, 2*time.Second, transport.ResponseHeaderTimeout)
	assert.NotNil(t, transport.DialContext)
}

func TestClientAcceptsZeroQuantity(t *testing.T) {
	t.Parallel()
	service := startService(t, 4, nil)
	client := testAdjustmentClient(t, service.address)
	request := adjustmentRequest(1, -10)
	result, err := client.apply(t.Context(), request)
	require.NoError(t, err)
	assert.Zero(t, result.Quantity)
	assertInventory(t, service.path, 0, 1, expectedReceipt{request: request, quantity: 0})
}

func splitResultReceipt(t *testing.T, request inventory.Request) (string, string) {
	t.Helper()
	key, err := request.Key.Encode()
	require.NoError(t, err)
	body := fmt.Sprintf(
		`{"state":"succeeded","key":%q,"codec":%q,"request":%q,`+
			`"result":{"adjustment_id":%q},"result":{"quantity":13}}`,
		key, request.Codec, base64.StdEncoding.EncodeToString(request.Payload), key,
	)
	return key, body
}

func TestClientRejectsSplitResultReceipt(t *testing.T) {
	t.Parallel()
	request := adjustmentRequest(1, 3)
	key, body := splitResultReceipt(t, request)
	result, err := matchingResult([]byte(body), key, request)
	assert.ErrorIs(t, err, inventory.ErrUnresolved)
	assert.Equal(t, inventory.Result{}, result)
}

func TestClientMalformedReceiptLeavesQueuePending(t *testing.T) {
	t.Parallel()
	request := adjustmentRequest(1, 3)
	_, body := splitResultReceipt(t, request)
	policy := durable.DurablePolicy{
		Profile: "sqlite-wal-v1", MaxPayload: 1024, MaxCodec: 64,
		MaxPendingItems: 16, MaxPendingPayload: 16384,
		MaxReceipts: 64, MaxProducers: 4, DatabaseLimit: 64 << 20,
	}
	runtime, err := durable.DefaultRuntime(policy)
	require.NoError(t, err)
	queue, err := durable.Open(
		t.Context(),
		filepath.Join(t.TempDir(), "queue"),
		durable.Config{Policy: policy, Durability: durable.Synchronized, Runtime: runtime},
	)
	if errors.Is(err, durable.ErrUnsupported) {
		t.Skipf("queue storage profile unsupported: %v", err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, queue.Close(context.Background())) })
	entry := durable.Entry{
		ID:    durable.ID{Producer: request.Key.Producer, Sequence: request.Key.Sequence},
		Codec: request.Codec, Payload: request.Payload,
	}
	commit, err := queue.SubmitBatch(t.Context(), []durable.Entry{entry})
	require.NoError(t, err)
	require.Equal(t, durable.Applied, commit.Decision)
	ctx, cancel := context.WithTimeout(t.Context(), operationTimeout)
	defer cancel()
	deliveries, err := queue.Receive(ctx, 1)
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	var posts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, writeErr := io.WriteString(w, body)
		assert.NoError(t, writeErr)
	}))
	t.Cleanup(server.Close)
	client := testAdjustmentClient(t, server.URL)
	result, applyErr := client.apply(ctx, request)
	// This is the success-only Ack boundary. A malformed receipt must never cross it.
	if applyErr == nil {
		commit, err = queue.AckBatch(ctx, []durable.Token{deliveries[0].Token})
		require.NoError(t, err)
		require.Equal(t, durable.Applied, commit.Decision)
	}
	assert.ErrorIs(t, applyErr, inventory.ErrUnresolved)
	assert.Equal(t, inventory.Result{}, result)
	assert.Equal(t, int64(maxApplyAttempts), posts.Load())
	statuses, err := queue.Lookup(ctx, []durable.ID{entry.ID})
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	assert.Equal(t, durable.Pending, statuses[0].Kind)
}

func TestClientRejectsWrongWireTypes(t *testing.T) {
	t.Parallel()
	request, _, fields := receiptFixture(t)
	array := make([]int, len(request.Payload))
	for index, value := range request.Payload {
		array[index] = int(value)
	}
	encoded, err := json.Marshal(array)
	require.NoError(t, err)
	fields[3] = `"request":` + string(encoded)
	arrayBody := receiptBody(fields)
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{name: "numeric array request", status: 200, body: arrayBody},
		{name: "duplicate conflict state", status: 409,
			body: `{"state":"unresolved","state":"conflict"}`},
		{name: "duplicate rejected state", status: 422,
			body: `{"state":"unresolved","state":"rejected"}`},
		{name: "duplicate capacity state", status: 507,
			body: `{"state":"unresolved","state":"capacity"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var posts atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					writeState(w, http.StatusNotFound, "absent")
					return
				}
				posts.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, writeErr := io.WriteString(w, tc.body)
				assert.NoError(t, writeErr)
			}))
			t.Cleanup(server.Close)
			client := testAdjustmentClient(t, server.URL)
			result, err := client.apply(t.Context(), request)
			assert.ErrorIs(t, err, inventory.ErrUnresolved)
			assert.Equal(t, inventory.Result{}, result)
			assert.Equal(t, int64(maxApplyAttempts), posts.Load())
		})
	}
}

func TestClientNormalizesOriginSuffix(t *testing.T) {
	t.Parallel()
	for _, suffix := range []string{"?", "#", "/?", "/#"} {
		t.Run(suffix, func(t *testing.T) {
			t.Parallel()
			request := adjustmentRequest(1, 3)
			key, err := request.Key.Encode()
			require.NoError(t, err)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/adjustments", r.URL.Path)
				assert.Empty(t, r.URL.RawQuery)
				assert.False(t, r.URL.ForceQuery)
				assert.Equal(t, key, r.Header.Get("Idempotency-Key"))
				writeSuccess(w, inventory.Receipt{
					Key: request.Key, Codec: request.Codec, Payload: request.Payload,
					Result: inventory.Result{AdjustmentID: key, Quantity: 13},
				})
			}))
			t.Cleanup(server.Close)
			client := testAdjustmentClient(t, server.URL+suffix)
			result, err := client.apply(t.Context(), request)
			require.NoError(t, err)
			assert.Equal(t, inventory.Result{AdjustmentID: key, Quantity: 13}, result)
		})
	}
}

func TestClientRejectsInvalidOrigin(t *testing.T) {
	t.Parallel()
	for _, address := range []string{
		"http://[", "https://127.0.0.1", "http:///", "http://user@127.0.0.1",
		"http://127.0.0.1?query=value", "http://127.0.0.1#fragment", "http://127.0.0.1/path",
	} {
		t.Run(address, func(t *testing.T) {
			t.Parallel()
			client, err := newAdjustmentClient(address)
			assert.Error(t, err)
			assert.Nil(t, client)
		})
	}
}

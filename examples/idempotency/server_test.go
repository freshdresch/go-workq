//go:build linux && amd64 && cgo

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-workq/examples/internal/inventory"
)

type testService struct {
	store     *inventory.Store
	admission *requestAdmission
	client    *http.Client
	path      string
	address   string
	stop      func()
}

func startService(t *testing.T, limit int, wrap func(http.Handler) http.Handler) *testService {
	t.Helper()
	directory := t.TempDir()
	var filesystem syscall.Statfs_t
	require.NoError(t, syscall.Statfs(directory, &filesystem))
	if filesystem.Type != 0xef53 {
		t.Skip("inventory storage tests require local ext4")
	}
	path := filepath.Join(directory, "inventory.db")
	store, err := inventory.Open(t.Context(), path, limit)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	handler := newServer(store)
	if wrap != nil {
		handler = wrap(handler)
	}
	admission := newRequestAdmission(store, handler)
	listener, err := listenLoopback(t.Context())
	require.NoError(t, err)
	server := newHTTPServer(admission)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	transport := &http.Transport{MaxConnsPerHost: 8, DisableKeepAlives: true}
	client := &http.Client{Transport: transport, Timeout: operationTimeout}
	stop := sync.OnceFunc(func() {
		admission.closeAdmission()
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cancel()
		shutdownErr := server.Shutdown(ctx)
		if shutdownErr != nil {
			assert.NoError(t, server.Close())
		}
		assert.NoError(t, shutdownErr)
		assert.ErrorIs(t, <-serveDone, http.ErrServerClosed)
		admission.waitHandlers()
		transport.CloseIdleConnections()
	})
	t.Cleanup(stop)
	return &testService{
		store: store, admission: admission, client: client,
		path: path, address: "http://" + listener.Addr().String(), stop: stop,
	}
}

func adjustmentRequest(sequence uint64, delta int64) inventory.Request {
	return inventory.Request{
		Key: inventory.Key{Namespace: "http-test", Producer: [16]byte{1},
			Sequence: sequence, Effect: "inventory.adjust"},
		Codec:   inventory.Codec,
		Payload: []byte(fmt.Sprintf(`{"item":"widget","delta":%d}`, delta)),
	}
}

func postRequest(
	ctx context.Context,
	address string,
	request inventory.Request,
) (*http.Request, error) {
	key, err := request.Key.Encode()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, address+"/adjustments", bytes.NewReader(request.Payload),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("X-Adjustment-Codec", request.Codec)
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func readResponse(t *testing.T, response *http.Response) []byte {
	t.Helper()
	defer func() { assert.NoError(t, response.Body.Close()) }()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	require.NoError(t, err)
	require.LessOrEqual(t, len(body), maxResponseBytes)
	assert.Equal(t, "application/json", response.Header.Get("Content-Type"))
	return body
}

func applyHTTP(t *testing.T, service *testService, request inventory.Request) successEnvelope {
	t.Helper()
	req, err := postRequest(t.Context(), service.address, request)
	require.NoError(t, err)
	response, err := service.client.Do(req)
	require.NoError(t, err)
	body := readResponse(t, response)
	require.Equal(t, http.StatusOK, response.StatusCode, string(body))
	var envelope successEnvelope
	require.NoError(t, json.Unmarshal(body, &envelope))
	key, err := request.Key.Encode()
	require.NoError(t, err)
	assert.Equal(t, "succeeded", envelope.State)
	assert.Equal(t, key, envelope.Key)
	assert.Equal(t, key, envelope.Result.AdjustmentID)
	assert.Equal(t, request.Codec, envelope.Codec)
	assert.Equal(t, request.Payload, envelope.Request)
	return envelope
}

type expectedReceipt struct {
	request  inventory.Request
	quantity int64
}

func assertInventory(
	t *testing.T,
	path string,
	quantity, adjustments int64,
	expected ...expectedReceipt,
) {
	t.Helper()
	address := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite3", address.String())
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()
	var gotQuantity, gotAdjustments, receipts, incomplete int64
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT quantity, adjustments FROM inventory WHERE item = 'widget'",
	).Scan(&gotQuantity, &gotAdjustments))
	require.NoError(t, db.QueryRowContext(
		t.Context(), "SELECT count(*) FROM receipts",
	).Scan(&receipts))
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT count(*) FROM receipts WHERE result_quantity IS NULL",
	).Scan(&incomplete))
	assert.Equal(t, quantity, gotQuantity)
	assert.Equal(t, adjustments, gotAdjustments)
	assert.Equal(t, adjustments, receipts)
	assert.Zero(t, incomplete)
	require.Equal(t, int64(len(expected)), receipts)
	for _, want := range expected {
		var version int
		var namespace, effect, codec string
		var producer, sequence, payload []byte
		var resultQuantity int64
		require.NoError(t, db.QueryRowContext(
			t.Context(),
			`SELECT version, namespace, producer, sequence, effect, codec, request, result_quantity
    FROM receipts WHERE version = 1 AND namespace = ? AND producer = ?
     AND sequence = ? AND effect = ?`,
			want.request.Key.Namespace,
			want.request.Key.Producer[:],
			binary.BigEndian.AppendUint64(nil, want.request.Key.Sequence),
			want.request.Key.Effect,
		).Scan(&version, &namespace, &producer, &sequence, &effect, &codec, &payload, &resultQuantity))
		assert.Equal(t, 1, version)
		assert.Equal(t, want.request.Key.Namespace, namespace)
		assert.Equal(t, want.request.Key.Producer[:], producer)
		assert.Equal(t, binary.BigEndian.AppendUint64(nil, want.request.Key.Sequence), sequence)
		assert.Equal(t, want.request.Key.Effect, effect)
		assert.Equal(t, want.request.Codec, codec)
		assert.Equal(t, want.request.Payload, payload)
		assert.Equal(t, want.quantity, resultQuantity)
	}
}

func TestServerPersistedBindingAndCapacity(t *testing.T) {
	t.Parallel()
	service := startService(t, 2, nil)
	first := adjustmentRequest(1, 3)
	original := applyHTTP(t, service, first)
	assert.Equal(t, int64(13), original.Result.Quantity)
	applyHTTP(t, service, adjustmentRequest(2, 3))
	assert.Equal(t, original, applyHTTP(t, service, first))
	key, err := first.Key.Encode()
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(
		t.Context(), http.MethodGet, service.address+"/adjustments/"+key, nil,
	)
	require.NoError(t, err)
	response, err := service.client.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var fetched successEnvelope
	require.NoError(t, json.Unmarshal(readResponse(t, response), &fetched))
	assert.Equal(t, original, fetched)
	for _, tc := range []struct {
		name    string
		request inventory.Request
		status  int
		state   string
	}{
		{"capacity", adjustmentRequest(3, 1), 507, "capacity"},
		{"conflict", adjustmentRequest(1, 2), 409, "conflict"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := postRequest(t.Context(), service.address, tc.request)
			require.NoError(t, err)
			response, err := service.client.Do(req)
			require.NoError(t, err)
			assert.Equal(t, tc.status, response.StatusCode)
			assert.JSONEq(t, `{"state":"`+tc.state+`"}`, string(readResponse(t, response)))
		})
	}
	assertInventory(
		t, service.path, 16, 2,
		expectedReceipt{request: first, quantity: 13},
		expectedReceipt{request: adjustmentRequest(2, 3), quantity: 16},
	)
	service.stop()
	require.NoError(t, service.store.Close())
	reopened, err := inventory.Open(t.Context(), service.path, 2)
	require.NoError(t, err)
	defer func() { assert.NoError(t, reopened.Close()) }()
	receipt, found, err := reopened.Lookup(t.Context(), first.Key)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, original.Result.Quantity, receipt.Result.Quantity)
	assert.Equal(t, first.Payload, receipt.Payload)
}

func TestServerConcurrentBinding(t *testing.T) {
	t.Parallel()
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprintf("conflict=%t", conflict), func(t *testing.T) {
			service := startService(t, 2, nil)
			start := make(chan struct{})
			var workers sync.WaitGroup
			var statuses [2]int
			var envelopes [2]successEnvelope
			var failures [2]error
			for i := range 2 {
				workers.Go(func() {
					<-start
					delta := int64(3)
					if conflict && i == 1 {
						delta = 4
					}
					req, err := postRequest(t.Context(), service.address, adjustmentRequest(1, delta))
					if err != nil {
						failures[i] = err
						return
					}
					response, err := service.client.Do(req)
					if err != nil {
						failures[i] = err
						return
					}
					defer func() { assert.NoError(t, response.Body.Close()) }()
					statuses[i] = response.StatusCode
					failures[i] = json.NewDecoder(response.Body).Decode(&envelopes[i])
				})
			}
			close(start)
			workers.Wait()
			for _, err := range failures {
				require.NoError(t, err)
			}
			if conflict {
				require.ElementsMatch(t, []int{200, 409}, statuses[:])
				quantity := int64(13)
				if statuses[1] == 200 {
					quantity = 14
				}
				assertInventory(
					t, service.path, quantity, 1,
					expectedReceipt{request: adjustmentRequest(1, quantity-10), quantity: quantity},
				)
				return
			}
			assert.Equal(t, [2]int{200, 200}, statuses)
			assert.Equal(t, envelopes[0], envelopes[1])
			assertInventory(
				t, service.path, 13, 1,
				expectedReceipt{request: adjustmentRequest(1, 3), quantity: 13},
			)
		})
	}
}

func TestServerInvalidRequests(t *testing.T) {
	t.Parallel()
	service := startService(t, 64, nil)
	for _, tc := range []struct {
		name   string
		modify func(*http.Request)
		status int
	}{
		{"missing key", func(req *http.Request) { req.Header.Del("Idempotency-Key") }, 400},
		{"bad key", func(req *http.Request) { req.Header.Set("Idempotency-Key", "bad") }, 400},
		{"duplicate key", func(req *http.Request) {
			req.Header.Add("Idempotency-Key", req.Header.Get("Idempotency-Key"))
		}, 400},
		{"duplicate codec", func(req *http.Request) {
			req.Header.Add("X-Adjustment-Codec", inventory.Codec)
		}, 400},
		{"missing codec", func(req *http.Request) { req.Header.Del("X-Adjustment-Codec") }, 400},
		{"long codec", func(req *http.Request) {
			req.Header.Set("X-Adjustment-Codec", strings.Repeat("a", inventory.MaxCodec+1))
		}, 400},
		{"content type", func(req *http.Request) { req.Header.Set("Content-Type", "text/plain") }, 400},
		{"unknown codec", func(req *http.Request) { req.Header.Set("X-Adjustment-Codec", "other") }, 422},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := postRequest(t.Context(), service.address, adjustmentRequest(1, 1))
			require.NoError(t, err)
			tc.modify(req)
			response, err := service.client.Do(req)
			require.NoError(t, err)
			readResponse(t, response)
			assert.Equal(t, tc.status, response.StatusCode)
		})
	}
	for _, payload := range []string{
		`{"item":"widget","delta":1} {}`, `{"item":"widget","delta":0}`,
		`{"item":"widget","delta":-11}`, `{"item":"widget","delta":1,"extra":0}`,
		`{"item":"widget","delta":1,"delta":2}`, `{`,
		strings.Repeat("x", inventory.MaxPayload+1),
	} {
		t.Run(payload[:min(30, len(payload))], func(t *testing.T) {
			request := adjustmentRequest(1, 1)
			request.Payload = []byte(payload)
			req, err := postRequest(t.Context(), service.address, request)
			require.NoError(t, err)
			response, err := service.client.Do(req)
			require.NoError(t, err)
			readResponse(t, response)
			expected := http.StatusUnprocessableEntity
			if len(payload) > inventory.MaxPayload {
				expected = http.StatusRequestEntityTooLarge
			}
			assert.Equal(t, expected, response.StatusCode)
		})
	}
	assertInventory(t, service.path, 10, 0)
}

func TestServerAdmissionSaturationAndJoin(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, maxActiveRequests)
	release := make(chan struct{})
	service := startService(t, 64, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
			}
			next.ServeHTTP(w, r)
		})
	})
	releaseAll := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseAll)
	var workers sync.WaitGroup
	var expected [maxActiveRequests]expectedReceipt
	for i := range maxActiveRequests {
		workers.Go(func() {
			req, err := postRequest(t.Context(), service.address, adjustmentRequest(uint64(i), 1))
			if !assert.NoError(t, err) {
				return
			}
			response, err := service.client.Do(req)
			if !assert.NoError(t, err) {
				return
			}
			defer func() { assert.NoError(t, response.Body.Close()) }()
			assert.Equal(t, http.StatusOK, response.StatusCode)
			var envelope successEnvelope
			if !assert.NoError(t, json.NewDecoder(response.Body).Decode(&envelope)) {
				return
			}
			expected[i] = expectedReceipt{
				request: adjustmentRequest(uint64(i), 1), quantity: envelope.Result.Quantity,
			}
		})
	}
	t.Cleanup(func() {
		releaseAll()
		workers.Wait()
	})
	for range maxActiveRequests {
		select {
		case <-entered:
		case <-time.After(operationTimeout):
			t.Fatal("handlers did not enter")
		}
	}
	req, err := postRequest(t.Context(), service.address, adjustmentRequest(10, 1))
	require.NoError(t, err)
	response, err := service.client.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	readResponse(t, response)
	service.admission.closeAdmission()
	req, err = postRequest(t.Context(), service.address, adjustmentRequest(11, 1))
	require.NoError(t, err)
	response, err = service.client.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	readResponse(t, response)
	assertInventory(t, service.path, 10, 0)
	releaseAll()
	workers.Wait()
	service.admission.waitHandlers()
	// The four independent adjustments may commit in any order, but each advances
	// quantity once. HTTP results identify that order without consulting the SQL oracle.
	quantities := make([]int64, 0, maxActiveRequests)
	for _, receipt := range expected {
		quantities = append(quantities, receipt.quantity)
	}
	assert.ElementsMatch(t, []int64{11, 12, 13, 14}, quantities)
	assertInventory(t, service.path, 14, 4, expected[:]...)
}

func TestListenerConnectionLimit(t *testing.T) {
	t.Parallel()
	listener, err := listenLoopback(t.Context())
	require.NoError(t, err)
	closeListener := sync.OnceFunc(func() { assert.NoError(t, listener.Close()) })
	defer closeListener()
	var accepted []net.Conn
	var clients []net.Conn
	defer func() {
		for _, conn := range clients {
			assert.NoError(t, conn.Close())
		}
		for _, conn := range accepted {
			assert.NoError(t, conn.Close())
		}
	}()
	for range maxLiveConnections {
		conn, err := net.DialTimeout("tcp", listener.Addr().String(), operationTimeout)
		require.NoError(t, err)
		clients = append(clients, conn)
		serverConn, err := listener.Accept()
		require.NoError(t, err)
		accepted = append(accepted, serverConn)
	}
	acceptDone := make(chan error, 1)
	var acceptWorker sync.WaitGroup
	defer func() {
		closeListener()
		acceptWorker.Wait()
	}()
	acceptWorker.Go(func() {
		conn, err := listener.Accept()
		if err == nil {
			err = conn.Close()
		}
		acceptDone <- err
	})
	excess, err := net.DialTimeout("tcp", listener.Addr().String(), operationTimeout)
	require.NoError(t, err)
	clients = append(clients, excess)
	require.NoError(t, excess.SetReadDeadline(time.Now().Add(operationTimeout)))
	var buffer [1]byte
	_, err = excess.Read(buffer[:])
	require.ErrorIs(t, err, io.EOF)
	assert.Len(t, listener.slots, maxLiveConnections)
	require.NoError(t, accepted[0].Close())
	require.NoError(t, accepted[0].Close())
	replacement, err := net.DialTimeout("tcp", listener.Addr().String(), operationTimeout)
	require.NoError(t, err)
	clients = append(clients, replacement)
	select {
	case err := <-acceptDone:
		assert.NoError(t, err)
	case <-time.After(operationTimeout):
		t.Fatal("replacement connection was not admitted")
	}
	assert.Len(t, listener.slots, maxLiveConnections-1)
}

func TestServerLookupAbsentAndMalformed(t *testing.T) {
	t.Parallel()
	service := startService(t, 1, nil)
	key, err := adjustmentRequest(1, 1).Key.Encode()
	require.NoError(t, err)
	for _, tc := range []struct {
		key    string
		status int
		state  string
	}{
		{key, http.StatusNotFound, "absent"},
		{"bad", http.StatusBadRequest, "malformed"},
	} {
		req, err := http.NewRequestWithContext(
			t.Context(), http.MethodGet, service.address+"/adjustments/"+tc.key, nil,
		)
		require.NoError(t, err)
		response, err := service.client.Do(req)
		require.NoError(t, err)
		assert.Equal(t, tc.status, response.StatusCode)
		assert.JSONEq(t, `{"state":"`+tc.state+`"}`, string(readResponse(t, response)))
	}
	assertInventory(t, service.path, 10, 0)
}

func TestServerStoreFailureClosesAdmission(t *testing.T) {
	t.Parallel()
	service := startService(t, 4, nil)
	address := url.URL{Scheme: "file", Path: service.path}
	db, err := sql.Open("sqlite3", address.String())
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()
	_, err = db.ExecContext(t.Context(), `
 CREATE TABLE guard_parent(id INTEGER PRIMARY KEY);
 CREATE TABLE guard_child(parent_id INTEGER REFERENCES guard_parent(id)
  DEFERRABLE INITIALLY DEFERRED);
 CREATE TRIGGER reject_commit AFTER UPDATE ON inventory BEGIN
  INSERT INTO guard_child VALUES(1);
 END;`)
	require.NoError(t, err)
	req, err := postRequest(t.Context(), service.address, adjustmentRequest(1, 1))
	require.NoError(t, err)
	response, err := service.client.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	assert.JSONEq(t, `{"state":"unresolved"}`, string(readResponse(t, response)))
	select {
	case <-service.store.Failed():
	default:
		t.Fatal("commit failure did not notify the owner")
	}
	req, err = postRequest(t.Context(), service.address, adjustmentRequest(2, 1))
	require.NoError(t, err)
	response, err = service.client.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	readResponse(t, response)
	assertInventory(t, service.path, 10, 0)
}

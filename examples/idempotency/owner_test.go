//go:build linux && amd64 && cgo

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
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

type ownerFixture struct {
	execution execution
	queuePath string
	storePath string
	address   string
}

func newOwnerFixture(t *testing.T) ownerFixture {
	t.Helper()
	directory := t.TempDir()
	queuePath := filepath.Join(directory, "queue")
	storePath := filepath.Join(directory, "inventory.db")
	config, err := exampleConfig()
	require.NoError(t, err)
	queue, err := durable.Open(t.Context(), queuePath, config)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, queue.Close(context.Background())) })
	store, err := inventory.Open(t.Context(), storePath, 64)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })
	listener, err := listenLoopback(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := listener.Close()
		if !errors.Is(err, net.ErrClosed) {
			assert.NoError(t, err)
		}
	})
	address := "http://" + listener.Addr().String()
	client, err := newAdjustmentClient(address)
	require.NoError(t, err)
	t.Cleanup(client.closeIdleConnections)
	return ownerFixture{
		execution: execution{
			queue: queue, store: store, listener: listener,
			admission: newRequestAdmission(store, newServer(store)), client: client, output: io.Discard,
		},
		queuePath: queuePath, storePath: storePath, address: address,
	}
}

func waitOwnerStage(t *testing.T, stage <-chan struct{}) {
	t.Helper()
	select {
	case <-stage:
	case <-time.After(2 * operationTimeout):
		t.Fatal("owner lifecycle stage did not finish")
	}
}

// The result channel joins the HTTP client independently of the server handler.
func ownerPOST(
	ctx context.Context,
	t *testing.T,
	address string,
	request inventory.Request,
) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		req, err := postRequest(ctx, address, request)
		if !assert.NoError(t, err) {
			return
		}
		transport := &http.Transport{DisableKeepAlives: true}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport, Timeout: 2 * operationTimeout}
		response, err := client.Do(req)
		if err != nil {
			return
		}
		defer func() { assert.NoError(t, response.Body.Close()) }()
		var envelope struct {
			State string `json:"state"`
		}
		assert.NoError(t, json.NewDecoder(response.Body).Decode(&envelope))
		assert.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
		assert.Equal(t, "unresolved", envelope.State)
	}()
	return done
}

func TestOwnerCommitFailureJoinsAndRestarts(t *testing.T) {
	t.Parallel()
	fixture := newOwnerFixture(t)
	owned := fixture.execution
	entries := sourceEntries()
	requests := make([]inventory.Request, len(entries))
	for index, entry := range entries {
		requests[index] = effectRequest(durable.Delivery{
			ID: entry.ID, Codec: entry.Codec, Payload: entry.Payload,
		})
	}
	_, err := owned.store.Apply(t.Context(), requests[0])
	require.NoError(t, err)
	assertInventory(
		t,
		fixture.storePath,
		13,
		1,
		expectedReceipt{request: requests[0], quantity: 13},
	)
	address := url.URL{Scheme: "file", Path: fixture.storePath}
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
	require.NoError(t, submitSource(t.Context(), owned.queue))
	owned.queue.StopAdmission()
	failingEntered := make(chan struct{})
	waiterEntered := make(chan struct{})
	releaseFailure := make(chan struct{})
	releaseWaiter := make(chan struct{})
	waiterJoined := make(chan struct{})
	cutoff := make(chan struct{})
	joined := make(chan struct{})
	unblockFailure := sync.OnceFunc(func() { close(releaseFailure) })
	unblockWaiter := sync.OnceFunc(func() { close(releaseWaiter) })
	remoteCtx, cancelRemote := context.WithTimeout(context.Background(), 4*operationTimeout)
	defer cancelRemote()
	failingKey, err := requests[1].Key.Encode()
	require.NoError(t, err)
	waiterKey, err := requests[2].Key.Encode()
	require.NoError(t, err)
	handler := owned.admission.handler
	owned.admission.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Idempotency-Key") {
		case failingKey:
			close(failingEntered)
			select {
			case <-releaseFailure:
			case <-remoteCtx.Done():
				return
			}
		case waiterKey:
			defer close(waiterJoined)
			close(waiterEntered)
			select {
			case <-releaseWaiter:
			case <-remoteCtx.Done():
				return
			}
			r = r.Clone(remoteCtx)
		}
		handler.ServeHTTP(w, r)
	})
	owned.admission.afterCutoff = func() { close(cutoff) }
	owned.afterJoin = func() {
		select {
		case <-waiterJoined:
		default:
			assert.Fail(t, "owner reached resource closure before waiter joined")
		}
		assert.Empty(t, owned.admission.slots)
		close(joined)
	}
	ctx, cancel := context.WithCancel(t.Context())
	ownerDone := make(chan struct{})
	var ownerErr error
	go func() {
		defer close(ownerDone)
		ownerErr = serveAndWork(ctx, owned)
	}()
	t.Cleanup(func() {
		unblockFailure()
		unblockWaiter()
		cancel()
		waitOwnerStage(t, ownerDone)
	})
	waitOwnerStage(t, failingEntered)
	waiterDone := ownerPOST(remoteCtx, t, fixture.address, requests[2])
	t.Cleanup(func() {
		cancelRemote()
		<-waiterDone
	})
	waitOwnerStage(t, waiterEntered)
	unblockFailure()
	waitOwnerStage(t, owned.store.Failed())
	waitOwnerStage(t, cutoff)
	select {
	case <-joined:
		t.Fatal("owner joined while admitted waiter remained paused")
	default:
	}
	unblockWaiter()
	waitOwnerStage(t, waiterJoined)
	waitOwnerStage(t, ownerDone)
	require.ErrorIs(t, ownerErr, inventory.ErrUnresolved)
	waitOwnerStage(t, joined)
	assert.False(t, owned.admission.admit())
	assertInventory(
		t,
		fixture.storePath,
		13,
		1,
		expectedReceipt{request: requests[0], quantity: 13},
	)
	_, err = db.ExecContext(
		t.Context(),
		"DROP TRIGGER reject_commit; DROP TABLE guard_child; DROP TABLE guard_parent;",
	)
	require.NoError(t, err)
	var output bytes.Buffer
	require.NoError(t, run(
		t.Context(),
		[]string{
			"idempotency", "-directory", fixture.queuePath, "-destination", fixture.storePath,
		},
		nil,
		nil,
		&output,
		&output,
	))
	assertInventory(
		t,
		fixture.storePath,
		14,
		3,
		expectedReceipt{request: requests[0], quantity: 13},
		expectedReceipt{request: requests[1], quantity: 12},
		expectedReceipt{request: requests[2], quantity: 14},
	)
	inspection, err := durable.Inspect(t.Context(), fixture.queuePath, sourceIDs())
	require.NoError(t, err)
	for _, status := range inspection.Statuses {
		assert.Equal(t, durable.Acknowledged, status.Kind)
	}
}

func TestOwnerCancellationCutsOffLateHandler(t *testing.T) {
	t.Parallel()
	fixture := newOwnerFixture(t)
	owned := fixture.execution
	entered := make(chan struct{})
	release := make(chan struct{})
	handlerJoined := make(chan struct{})
	admittedEntered := make(chan struct{})
	admittedRelease := make(chan struct{})
	admittedJoined := make(chan struct{})
	unblockAdmitted := sync.OnceFunc(func() { close(admittedRelease) })
	lateKey, err := adjustmentRequest(2, 1).Key.Encode()
	require.NoError(t, err)
	cutoff := make(chan struct{})
	joinsObserved := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var storeCalls atomic.Int64
	handler := owned.admission.handler
	owned.admission.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Idempotency-Key") == lateKey {
			storeCalls.Add(1)
		} else {
			defer close(admittedJoined)
			close(admittedEntered)
			<-admittedRelease
		}
		handler.ServeHTTP(w, r)
	})
	owned.admission.afterRequest = func(r *http.Request) {
		if r.Header.Get("Idempotency-Key") == lateKey {
			close(handlerJoined)
		}
	}
	owned.admission.beforeAdmission = func(r *http.Request) {
		if r.Header.Get("Idempotency-Key") != lateKey {
			return
		}
		close(entered)
		<-release
	}
	owned.admission.afterCutoff = func() { close(cutoff) }
	owned.afterJoin = func() {
		select {
		case <-admittedJoined:
		default:
			assert.Fail(t, "owner closed resources before admitted handler returned")
		}
		_, err := owned.store.Snapshot(context.Background())
		assert.NoError(t, err, "Store must remain open until all joins finish")
		assert.Empty(t, owned.admission.slots)
		close(joinsObserved)
	}
	ctx, cancel := context.WithCancel(t.Context())
	ownerDone := make(chan struct{})
	var ownerErr error
	go func() {
		defer close(ownerDone)
		ownerErr = serveAndWork(ctx, owned)
	}()
	t.Cleanup(func() {
		unblock()
		unblockAdmitted()
		cancel()
		waitOwnerStage(t, ownerDone)
	})
	admittedClient := ownerPOST(t.Context(), t, fixture.address, adjustmentRequest(1, 1))
	t.Cleanup(func() {
		unblockAdmitted()
		waitOwnerStage(t, admittedClient)
	})
	waitOwnerStage(t, admittedEntered)
	clientDone := ownerPOST(t.Context(), t, fixture.address, adjustmentRequest(2, 1))
	t.Cleanup(func() {
		unblock()
		waitOwnerStage(t, clientDone)
	})
	t.Cleanup(func() {
		unblock()
		waitOwnerStage(t, handlerJoined)
	})
	waitOwnerStage(t, entered)
	cancel()
	waitOwnerStage(t, cutoff)
	_, err = owned.store.Snapshot(t.Context())
	require.NoError(t, err, "owner must retain Store while admitted handler is paused")
	unblockAdmitted()
	waitOwnerStage(t, admittedJoined)
	owned.admission.waitHandlers()
	unblock()
	waitOwnerStage(t, handlerJoined)
	waitOwnerStage(t, ownerDone)
	waitOwnerStage(t, joinsObserved)
	require.ErrorIs(t, ownerErr, context.Canceled)
	assert.Zero(t, storeCalls.Load())
	assert.False(t, owned.admission.admit())
	_, err = owned.store.Snapshot(t.Context())
	assert.ErrorIs(t, err, inventory.ErrUnresolved)
	assertInventory(t, fixture.storePath, 10, 0)
	connection, err := net.DialTimeout("tcp", owned.listener.Addr().String(), operationTimeout)
	if assert.Error(t, err) {
		return
	}
	assert.NoError(t, connection.Close())
}

func TestOwnerForcedClosureJoinsAdmittedHandler(t *testing.T) {
	t.Parallel()
	fixture := newOwnerFixture(t)
	owned := fixture.execution
	entered := make(chan struct{})
	release := make(chan struct{})
	handlerReturned := make(chan struct{})
	handlerResult := make(chan error, 1)
	cutoff := make(chan struct{})
	waitEntered := make(chan struct{})
	joined := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	storeCtx, cancelStore := context.WithTimeout(context.Background(), 6*operationTimeout)
	defer cancelStore()
	owned.admission.handler = http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		defer close(handlerReturned)
		close(entered)
		// Admitted work may retain the Store after its client connection closes.
		<-release
		_, err := owned.store.Snapshot(storeCtx)
		handlerResult <- err
	})
	owned.admission.afterCutoff = func() { close(cutoff) }
	owned.admission.beforeWait = sync.OnceFunc(func() { close(waitEntered) })
	type joinObservation struct {
		handlerReturned bool
		activeSlots     int
		storeErr        error
	}
	joinResult := make(chan joinObservation, 1)
	owned.afterJoin = func() {
		observation := joinObservation{activeSlots: len(owned.admission.slots)}
		select {
		case <-handlerReturned:
			observation.handlerReturned = true
		default:
		}
		_, observation.storeErr = owned.store.Snapshot(storeCtx)
		joinResult <- observation
		close(joined)
	}
	ctx, cancel := context.WithCancel(t.Context())
	ownerDone := make(chan struct{})
	var ownerErr error
	go func() {
		defer close(ownerDone)
		ownerErr = serveAndWork(ctx, owned)
	}()
	t.Cleanup(func() {
		unblock()
		cancel()
		cancelStore()
		waitOwnerStage(t, ownerDone)
	})
	connection, err := net.DialTimeout("tcp", owned.listener.Addr().String(), operationTimeout)
	require.NoError(t, err)
	closeConnection := sync.OnceFunc(func() { assert.NoError(t, connection.Close()) })
	t.Cleanup(closeConnection)
	require.NoError(t, connection.SetDeadline(time.Now().Add(3*operationTimeout)))
	_, err = io.WriteString(
		connection,
		"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
	)
	require.NoError(t, err)
	type readResult struct {
		count int
		err   error
	}
	clientResult := make(chan readResult, 1)
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		var response [1]byte
		count, err := connection.Read(response[:])
		clientResult <- readResult{count: count, err: err}
	}()
	t.Cleanup(func() {
		unblock()
		cancel()
		cancelStore()
		closeConnection()
		waitOwnerStage(t, clientDone)
		waitOwnerStage(t, ownerDone)
		select {
		case <-entered:
			waitOwnerStage(t, handlerReturned)
		default:
		}
	})
	waitOwnerStage(t, entered)
	cancel()
	waitOwnerStage(t, cutoff)
	waitOwnerStage(t, clientDone)
	read := <-clientResult
	require.Zero(t, read.count, "paused handler must send no response before forced closure")
	require.ErrorIs(t, read.err, io.EOF, "server must close the connection remotely")
	waitOwnerStage(t, waitEntered)
	_, err = owned.store.Snapshot(storeCtx)
	require.NoError(t, err, "Store must remain open while its admitted handler is paused")
	select {
	case <-joined:
		t.Fatal("owner joined while admitted handler remained paused")
	default:
	}
	select {
	case <-ownerDone:
		t.Fatal("owner completed while admitted handler remained paused")
	default:
	}
	unblock()
	waitOwnerStage(t, handlerReturned)
	waitOwnerStage(t, ownerDone)
	waitOwnerStage(t, joined)
	assert.NoError(t, <-handlerResult)
	observation := <-joinResult
	assert.True(t, observation.handlerReturned, "handler must return before the owner joins")
	assert.Zero(t, observation.activeSlots)
	assert.NoError(t, observation.storeErr, "Store must remain open until all joins finish")
	assert.ErrorIs(t, ownerErr, context.DeadlineExceeded)
	assert.ErrorIs(t, ownerErr, context.Canceled)
	_, err = owned.store.Snapshot(t.Context())
	assert.ErrorIs(t, err, inventory.ErrUnresolved)
	assert.False(t, owned.admission.admit())
	assertInventory(t, fixture.storePath, 10, 0)
}

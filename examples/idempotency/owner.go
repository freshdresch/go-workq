package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/freshdresch/go-data-structures/workq/durable"
	"github.com/freshdresch/go-data-structures/workq/examples/internal/inventory"
)

type execution struct {
	queue           *durable.Queue
	store           *inventory.Store
	listener        *connectionListener
	admission       *requestAdmission
	client          *adjustmentClient
	output          io.Writer
	stopAfterCommit bool
}

type workerResult struct {
	observed observations
	err      error
}

// serveAndWork takes ownership of all resources in execution. Every exit cuts off
// admission before joining users, then closes storage after native calls return.
func serveAndWork(ctx context.Context, owned execution) error {
	ownerParent := context.WithoutCancel(ctx)
	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	serverCtx, cancelServer := context.WithCancel(ownerParent)
	defer cancelServer()
	server := newHTTPServer(owned.admission)
	server.BaseContext = func(net.Listener) context.Context { return serverCtx }
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(owned.listener) }()
	workerDone := make(chan workerResult, 1)
	go func() {
		observed, err := work(
			workerCtx,
			ownerParent,
			owned.queue,
			owned.client,
			owned.output,
			owned.stopAfterCommit,
		)
		workerDone <- workerResult{observed: observed, err: err}
	}()

	var result workerResult
	var serveErr, stopErr error
	workerJoined, serveJoined := false, false
	select {
	case result = <-workerDone:
		workerJoined = true
	case serveErr = <-serveDone:
		serveJoined = true
		stopErr = errors.New("HTTP server stopped before worker completion")
	case <-ctx.Done():
		stopErr = ctx.Err()
	case <-owned.store.Failed():
		stopErr = fmt.Errorf(
			"destination stopped, restart with the same paths: %w",
			inventory.ErrUnresolved,
		)
	}

	// Add and the cutoff share the admission mutex, so Wait cannot miss a late handler.
	owned.admission.closeAdmission()
	owned.queue.StopDelivery()
	cancelWorker()
	cancelServer()
	if !workerJoined {
		result = <-workerDone
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(ownerParent, operationTimeout)
	shutdownErr := server.Shutdown(shutdownCtx)
	cancelShutdown()
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, server.Close())
	}
	if !serveJoined {
		serveErr = <-serveDone
	}
	owned.admission.waitHandlers()
	owned.client.closeIdleConnections()
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	// Diagnostics are read only after every handler has relinquished the Store.
	outputErr := reportResults(ownerParent, owned.store, owned.output, result.observed)
	storeErr := owned.store.Close()
	queueErr := owned.queue.Close(ownerParent)
	return errors.Join(stopErr, result.err, shutdownErr, serveErr, outputErr, storeErr, queueErr)
}

func reportResults(
	ctx context.Context,
	store *inventory.Store,
	output io.Writer,
	observed observations,
) error {
	_, outputErr := fmt.Fprintf(
		output,
		"attempts=%d post_successes=%d lookup_recoveries=%d acknowledged=%d\n",
		observed.attempts, observed.postSuccesses, observed.lookupRecoveries, observed.acknowledged,
	)
	snapshotCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	snapshot, snapshotErr := store.Snapshot(snapshotCtx)
	if snapshotErr != nil {
		return errors.Join(outputErr, snapshotErr)
	}
	_, stateErr := fmt.Fprintf(
		output,
		"quantity=%d adjustments=%d\n",
		snapshot.Quantity, snapshot.Adjustments,
	)
	return errors.Join(outputErr, stateErr)
}

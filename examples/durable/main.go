// Durable demonstrates a bounded feeder from SQLite to caller-owned runner workers.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/freshdresch/go-data-structures/workq/durable"
	"github.com/freshdresch/go-data-structures/workq/runner"
)

func main() {
	if err := run(
		context.Background(),
		os.Args,
		os.Getenv,
		os.Stdin,
		os.Stdout,
		os.Stderr,
	); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(
	ctx context.Context,
	args []string,
	getenv func(string) string,
	_ io.Reader,
	stdout, stderr io.Writer,
) (err error) {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("directory", getenv("DURABLE_DIRECTORY"), "private queue directory")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *directory == "" || flags.NArg() != 0 {
		return errors.New("use -directory or DURABLE_DIRECTORY with no positional arguments")
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	config, err := exampleConfig()
	if err != nil {
		return err
	}
	queue, err := durable.Open(ctx, *directory, config)
	if err != nil {
		return err
	}
	// supervise joins the feeder and every callback before this closes Ack service.
	defer func() { err = errors.Join(err, queue.Close(context.Background())) }()
	entries := sourceEntries()
	result, err := queue.SubmitBatch(ctx, entries)
	if err != nil {
		return fmt.Errorf(
			"submit source records (%s, retain IDs for reconciliation): %w",
			result.Decision, err,
		)
	}
	queue.StopAdmission()
	if err := supervise(ctx, queue, printRecords(stdout)); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "all source records acknowledged")
	return err
}

func exampleConfig() (durable.Config, error) {
	policy := durable.DurablePolicy{
		Profile: "sqlite-wal-v1", MaxPayload: 16 << 10, MaxCodec: 64,
		MaxPendingItems: 4096, MaxPendingPayload: 16 << 20,
		MaxReceipts: 16384, MaxProducers: 256, DatabaseLimit: 1 << 30,
	}
	runtime, err := durable.DefaultRuntime(policy)
	if err != nil {
		return durable.Config{}, err
	}
	return durable.Config{Policy: policy, Durability: durable.Synchronized, Runtime: runtime}, nil
}

func sourceEntries() []durable.Entry {
	// The producer identifies this fixed source. Repeated runs keep IDs and bytes identical.
	producer := [16]byte{'d', 'e', 'm', 'o'}
	return []durable.Entry{
		{ID: durable.ID{Producer: producer, Sequence: 1}, Codec: "text", Payload: []byte("one")},
		{ID: durable.ID{Producer: producer, Sequence: 2}, Codec: "text", Payload: []byte("two")},
		{ID: durable.ID{Producer: producer, Sequence: 3}, Codec: "text", Payload: []byte("three")},
	}
}

func printRecords(output io.Writer) func(context.Context, []durable.Delivery) []runner.Result {
	return func(ctx context.Context, batch []durable.Delivery) []runner.Result {
		results := make([]runner.Result, len(batch))
		for index, delivery := range batch {
			if err := ctx.Err(); err != nil {
				results[index] = runner.PeerCanceled(ctx)
				continue
			}
			// Printing is a demonstration effect and can repeat after a crash before Ack.
			_, err := fmt.Fprintf(output, "%d: %s\n", delivery.ID.Sequence, delivery.Payload)
			if err != nil {
				results[index] = runner.Result{State: runner.ResultFailed, Err: err}
			}
		}
		return results
	}
}

func supervise(
	ctx context.Context,
	queue *durable.Queue,
	handler func(context.Context, []durable.Delivery) []runner.Result,
) error {
	// One worker makes the example's output writer single-owner. The caller owns its runner.
	failures := make(chan error, 1)
	workers, err := runner.New(runner.Config[durable.Delivery]{
		Capacity: 4, Workers: 1, Handler: handler,
		OnTerminal: terminalCallback(queue, failures),
	})
	if err != nil {
		return err
	}
	feedCtx, stopFeeder := context.WithCancel(ctx)
	defer stopFeeder()
	feederDone := make(chan error, 1)
	go func() { feederDone <- feed(feedCtx, queue, workers) }()
	var cause error
	feederJoined := false
	select {
	case cause = <-feederDone:
		feederJoined = true
	case cause = <-failures:
	case <-workers.Aborting():
		cause = workers.AbortCause()
	case <-queue.Stopping():
		cause = errors.New("durable queue stopped, reconcile source IDs")
	case <-ctx.Done():
		cause = ctx.Err()
	}
	queue.StopDelivery()
	stopFeeder()
	if cause != nil {
		workers.Abort(cause)
	}
	if !feederJoined {
		cause = errors.Join(cause, <-feederDone)
	}
	// Keep durable Ack service alive through every terminal callback, including abort cleanup.
	return errors.Join(cause, workers.Shutdown(context.Background()))
}

func terminalCallback(
	queue *durable.Queue,
	failures chan<- error,
) func(durable.Delivery, runner.ItemOutcome) error {
	return func(delivery durable.Delivery, outcome runner.ItemOutcome) error {
		if outcome.State != runner.TerminalSucceeded {
			// Keep failed/abandoned work pending for recovery after all old workers stop.
			return nil
		}
		ackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := queue.AckBatch(ackCtx, []durable.Token{delivery.Token})
		if err == nil {
			return nil
		}
		failure := fmt.Errorf(
			"ack source sequence %d (%s), reconcile before repeating effect: %w",
			delivery.ID.Sequence, result.Decision, err,
		)
		// Never join this callback's own runner. The supervisor owns shutdown.
		select {
		case failures <- failure:
		default:
		}
		return failure
	}
}

func feed(
	ctx context.Context,
	queue *durable.Queue,
	workers *runner.Runner[durable.Delivery],
) error {
	for {
		deliveries, err := queue.Receive(ctx, 4)
		if errors.Is(err, durable.ErrDrained) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := transfer(ctx, queue, workers, deliveries); err != nil {
			return err
		}
	}
}

func transfer(
	ctx context.Context,
	queue *durable.Queue,
	workers *runner.Runner[durable.Delivery],
	deliveries []durable.Delivery,
) (int, error) {
	accepted, err := workers.SubmitBatch(ctx, deliveries)
	// The accepted prefix belongs to the runner even on error. Only the suffix is ours.
	for _, delivery := range deliveries[accepted:] {
		err = errors.Join(err, queue.Release(delivery.Token))
	}
	// SubmitBatch returned and the suffix is no longer in use before Release permits redelivery.
	return accepted, err
}

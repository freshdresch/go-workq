package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/freshdresch/go-workq/durable"
	"github.com/freshdresch/go-workq/examples/internal/inventory"
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
	_ func(string) string,
	_ io.Reader,
	stdout, stderr io.Writer,
) (err error) {
	flags := flag.NewFlagSet("idempotency", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("directory", "", "private queue directory")
	destination := flags.String("destination", "", "inventory database outside the queue directory")
	stopAfterCommit := flags.Bool("stop-after-commit", false, "stop after success before Ack")
	if len(args) == 0 {
		return errors.New("missing command name")
	}
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *directory == "" || *destination == "" || flags.NArg() != 0 {
		return errors.New("use -directory and -destination with no positional arguments")
	}
	queuePath, destinationPath, err := separatePaths(*directory, *destination)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	config, err := exampleConfig()
	if err != nil {
		return err
	}
	if _, statErr := os.Stat(queuePath); statErr == nil {
		if _, err := durable.Inspect(ctx, queuePath, sourceIDs()); err != nil {
			return fmt.Errorf("inspect existing queue before restart: %w", err)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	queue, err := durable.Open(ctx, queuePath, config)
	if err != nil {
		return err
	}
	store, err := inventory.Open(ctx, destinationPath, 64)
	if err != nil {
		return errors.Join(err, queue.Close(context.WithoutCancel(ctx)))
	}
	// serveAndWork takes ownership only after startup succeeds.
	transferred := false
	defer func() {
		if !transferred {
			queue.StopDelivery()
			err = errors.Join(err, store.Close(), queue.Close(context.WithoutCancel(ctx)))
		}
	}()
	if err := submitSource(ctx, queue); err != nil {
		return err
	}
	queue.StopAdmission()
	listener, err := listenLoopback(ctx)
	if err != nil {
		return err
	}
	client, err := newAdjustmentClient("http://" + listener.Addr().String())
	if err != nil {
		return errors.Join(err, listener.Close())
	}
	admission := newRequestAdmission(store, newServer(store))
	transferred = true
	return serveAndWork(ctx, execution{
		queue: queue, store: store, listener: listener, admission: admission,
		client: client, output: stdout, stopAfterCommit: *stopAfterCommit,
	})
}

func separatePaths(directory, destination string) (string, string, error) {
	queuePath, err := resolvedPath(directory)
	if err != nil {
		return "", "", err
	}
	destinationPath, err := resolvedPath(destination)
	if err != nil {
		return "", "", err
	}
	for _, pair := range [][2]string{{queuePath, destinationPath}, {destinationPath, queuePath}} {
		relative, err := filepath.Rel(pair[0], pair[1])
		if err != nil {
			return "", "", err
		}
		outside := relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator))
		if !outside {
			return "", "", errors.New("queue and destination paths must be separate, not equal or nested")
		}
	}
	return queuePath, destinationPath, nil
}

func resolvedPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		return resolved, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", fmt.Errorf("path requires an existing private parent directory: %w", err)
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}

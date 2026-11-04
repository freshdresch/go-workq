//go:build linux

// Durable-resource records bounded production queue resource cases as JSON lines.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/freshdresch/go-workq/durable"
)

const sampleInterval = 20 * time.Millisecond

type event struct {
	WALBoundaryDelta *int64                     `json:"wal_boundary_delta_bytes,omitempty"`
	Kind             string                     `json:"kind"`
	At               time.Time                  `json:"at"`
	Phase            string                     `json:"phase,omitempty"`
	Duration         time.Duration              `json:"duration_ns"`
	Snapshot         *durable.Snapshot          `json:"snapshot,omitempty"`
	Inspection       *durable.Inspection        `json:"inspection,omitempty"`
	Maintenance      *durable.MaintenanceResult `json:"maintenance,omitempty"`
	Sample           *resourceSample            `json:"sample,omitempty"`
	Config           *durable.Config            `json:"config,omitempty"`
	Detail           string                     `json:"detail,omitempty"`
}

type recorder struct {
	mu      sync.Mutex
	encoder *json.Encoder
	err     error
}

func (r *recorder) write(value event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	value.At = time.Now().UTC()
	r.err = r.encoder.Encode(value)
	return r.err
}

func (r *recorder) sample(directory, phase string) error {
	sample, err := readResources(directory)
	if err != nil {
		return err
	}
	return r.write(event{Kind: "sample", Phase: phase, Sample: &sample})
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("durable-resource", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("directory", "", "new private fixture directory (retained after run)")
	name := flags.String("case", "", "capacity, maximum-group, lifecycle, or small-main")
	cache := flags.Int("cache-mib", 8, "SQLite cache: 8 or 64 MiB")
	mmap := flags.Int("mmap-mib", 0, "SQLite mmap: 0 or 256 MiB")
	policy := flags.String("durability", "synchronized", "synchronized or process-crash")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *directory == "" {
		return errors.New("directory and case are required")
	}
	if *cache != 8 && *cache != 64 {
		return errors.New("cache-mib must be 8 or 64")
	}
	if *mmap != 0 && *mmap != 256 {
		return errors.New("mmap-mib must be 0 or 256")
	}
	switch *name {
	case "capacity", "maximum-group", "lifecycle", "small-main":
	default:
		return fmt.Errorf("unknown case %q", *name)
	}
	durability := durable.Synchronized
	switch *policy {
	case "synchronized":
	case "process-crash":
		if *name != "maximum-group" {
			return errors.New("process-crash requires maximum-group")
		}
		durability = durable.ProcessCrash
	default:
		return fmt.Errorf("unknown durability %q", *policy)
	}
	if _, err := os.Lstat(*directory); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("fixture must not exist: %q (stat error %v)", *directory, err)
	}
	config, err := productionConfig(durability, *cache, *mmap)
	if err != nil {
		return err
	}
	if *name == "small-main" {
		config.Policy.DatabaseLimit = 256 << 10
	}
	directoryPath, err := filepath.Abs(*directory)
	if err != nil {
		return err
	}
	recorder := &recorder{encoder: json.NewEncoder(stdout)}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if err := recorder.write(event{
		Kind: "manifest", Phase: *name, Config: &config,
		Detail: runtime.Version() + "; samples every 20ms and boundaries, peaks may be missed; " +
			"native heap attribution unavailable; RSS includes harness; files retained for review; " +
			"cached group/WAL may be superseded by cleanup or idle checkpoints",
	}); err != nil {
		return err
	}
	return execute(ctx, directoryPath, *name, config, recorder)
}

func productionConfig(policy durable.Durability, cache, mmap int) (durable.Config, error) {
	config := durable.Config{
		Policy: durable.DurablePolicy{
			Profile: "sqlite-wal-v1", MaxPayload: 16 << 10, MaxCodec: 64,
			MaxPendingItems: 4096, MaxPendingPayload: 16 << 20,
			MaxReceipts: 16384, MaxProducers: 256, DatabaseLimit: 1 << 30,
		},
		Durability: policy,
	}
	tuning, err := durable.DefaultRuntime(config.Policy)
	if err != nil {
		return durable.Config{}, err
	}
	tuning.Group.MaxDelay = 0
	tuning.Delivery.ReceiveItems = 256
	tuning.SQLite.Cache = durable.Bytes(cache) << 20
	tuning.SQLite.Mmap = durable.Bytes(mmap) << 20
	config.Runtime = tuning
	return config, nil
}

func execute(
	ctx context.Context,
	directory, name string,
	config durable.Config,
	recorder *recorder,
) (err error) {
	ctx, stopExecution := context.WithCancel(ctx)
	defer stopExecution()
	samplingCtx, stopSampling := context.WithCancel(ctx)
	sampled := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(sampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-samplingCtx.Done():
				sampled <- nil
				return
			case <-ticker.C:
				if sampleErr := recorder.sample(directory, "periodic"); sampleErr != nil {
					sampled <- sampleErr
					stopExecution()
					return
				}
			}
		}
	}()
	defer func() {
		stopSampling()
		err = errors.Join(err, <-sampled)
		detail := "validated; fixture retained pending evidence review"
		if err != nil {
			detail = err.Error()
		}
		err = errors.Join(err, recorder.write(event{Kind: "result", Detail: detail}))
	}()
	harness := &harness{directory: directory, config: config, recorder: recorder}
	defer func() {
		if harness.queue != nil {
			harness.queue.Abort(errors.New("resource harness stopped"))
			err = errors.Join(err, harness.queue.Wait())
		}
	}()
	if err := harness.open(ctx); err != nil {
		return err
	}
	switch name {
	case "capacity":
		err = harness.capacity(ctx, 4, int(config.Policy.MaxPendingItems))
	case "maximum-group":
		err = harness.churn(ctx, 8)
	case "lifecycle":
		err = harness.churn(ctx, 1)
		if err == nil {
			err = harness.lifecycle(ctx)
		}
	case "small-main":
		return harness.smallMain(ctx)
	}
	if err != nil {
		return err
	}
	return harness.close(ctx)
}

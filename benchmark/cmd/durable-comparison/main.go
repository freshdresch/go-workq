// Durable-comparison measures the production queue with bounded correctness bookkeeping.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/freshdresch/go-workq/durable"
)

type options struct {
	Directory string        `json:"directory"`
	Policy    string        `json:"policy"`
	Payload   int           `json:"payload_bytes"`
	Batch     int           `json:"submit_batch_limit"`
	Warmup    time.Duration `json:"warmup_ns"`
	Measure   time.Duration `json:"admission_ns"`
	Revision  string        `json:"library_revision"`
}

type result struct {
	Options           options            `json:"options"`
	Config            durable.Config     `json:"resolved_config"`
	GoVersion         string             `json:"go_version"`
	Build             *debug.BuildInfo   `json:"build"`
	GOMAXPROCS        int                `json:"gomaxprocs"`
	CreditPerProducer int                `json:"credit_per_producer"`
	Warmup            phaseStats         `json:"warmup"`
	Measured          phaseStats         `json:"measured"`
	Before            durable.Snapshot   `json:"before"`
	Final             durable.Snapshot   `json:"final"`
	BeforeWarmup      sampleStats        `json:"resources_before_warmup"`
	Samples           sampleStats        `json:"sampled_resources"`
	Elapsed           time.Duration      `json:"elapsed_ns"`
	CPU               time.Duration      `json:"cpu_ns"`
	TotalAlloc        uint64             `json:"total_alloc_bytes"`
	Mallocs           uint64             `json:"mallocs"`
	RecordsPerSecond  float64            `json:"records_per_second"`
	CPUPerRecord      float64            `json:"cpu_ns_per_record"`
	BytesPerRecord    float64            `json:"allocated_bytes_per_record"`
	AllocsPerRecord   float64            `json:"allocations_per_record"`
	ReopenDuration    time.Duration      `json:"reopen_validation_ns"`
	Inspection        durable.Inspection `json:"inspection"`
	Valid             bool               `json:"valid"`
	Failure           string             `json:"failure,omitempty"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("durable-comparison", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var opts options
	flags.StringVar(&opts.Directory, "directory", "", "fresh private directory to create")
	flags.StringVar(&opts.Policy, "policy", "FULL", "FULL or NORMAL")
	flags.IntVar(&opts.Payload, "payload", 256, "256 or 4096 bytes")
	flags.IntVar(&opts.Batch, "batch", 1, "1 or 256 entries")
	flags.DurationVar(&opts.Warmup, "warmup", 100*time.Millisecond, "warmup admission window")
	flags.DurationVar(&opts.Measure, "measure", time.Second, "measured admission window")
	flags.StringVar(&opts.Revision, "revision", "", "source revision from provenance manifest")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := validateOptions(opts); err != nil {
		return err
	}
	report, err := measure(opts)
	if err != nil {
		report.Failure = err.Error()
	}
	return errors.Join(err, json.NewEncoder(stdout).Encode(report))
}

func validateOptions(opts options) error {
	if opts.Directory == "" || opts.Revision == "" {
		return errors.New("directory and revision required")
	}
	if opts.Policy != "FULL" && opts.Policy != "NORMAL" {
		return errors.New("policy must be FULL or NORMAL")
	}
	if opts.Payload != 256 && opts.Payload != 4096 {
		return errors.New("payload must be 256 or 4096")
	}
	if opts.Batch != 1 && opts.Batch != 256 {
		return errors.New("batch must be 1 or 256")
	}
	if opts.Warmup <= 0 || opts.Measure <= 0 {
		return errors.New("phase durations must be positive")
	}
	return nil
}

func queueConfig(opts options) (durable.Config, error) {
	policy := durable.DurablePolicy{
		Profile: "sqlite-wal-v1", MaxPayload: 4096, MaxCodec: 16,
		MaxPendingItems: 1024, MaxPendingPayload: 4 << 20,
		MaxReceipts: 8192, MaxProducers: 4, DatabaseLimit: 1 << 30,
	}
	tuning, err := durable.DefaultRuntime(policy)
	if err != nil {
		return durable.Config{}, err
	}
	tuning.Delivery.ReceiveItems = durable.Count(opts.Batch)
	config := durable.Config{Policy: policy, Runtime: tuning}
	if opts.Policy == "NORMAL" {
		config.Durability = durable.ProcessCrash
	}
	return config, nil
}

func measure(opts options) (report result, err error) {
	report.Options = opts
	report.GoVersion = runtime.Version()
	report.Build, _ = debug.ReadBuildInfo()
	report.GOMAXPROCS = runtime.GOMAXPROCS(0)
	report.CreditPerProducer = creditWindow
	report.Config, err = queueConfig(opts)
	if err != nil {
		return report, err
	}
	if err = os.Mkdir(opts.Directory, 0700); err != nil {
		return report, err
	}
	ctx := context.Background()
	queue, err := durable.Open(ctx, opts.Directory, report.Config)
	if err != nil {
		return report, err
	}
	closed := false
	defer func() {
		if !closed {
			queue.Abort(err)
			err = errors.Join(err, queue.Wait())
			report.Final = queue.Snapshot()
		}
	}()
	if err = report.BeforeWarmup.observe(opts.Directory); err != nil {
		return report, err
	}
	workload := newWorkload(opts)
	report.Warmup, err = workload.phase(queue, opts.Warmup)
	if err != nil {
		return report, err
	}
	report.Before = queue.Snapshot()
	if err = report.Samples.observe(opts.Directory); err != nil {
		return report, err
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpuBefore, err := processCPU()
	if err != nil {
		return report, err
	}
	started := time.Now()
	sampler := startSampler(opts.Directory)
	report.Measured, err = workload.phase(queue, opts.Measure)
	if err == nil {
		err = queue.Close(ctx)
	}
	if err != nil {
		queue.Abort(err)
		err = errors.Join(err, queue.Wait())
	}
	closed = true
	sampled, sampleErr := sampler.join()
	report.Samples.merge(sampled)
	report.Final = queue.Snapshot()
	err = errors.Join(err, sampleErr, report.Samples.observe(opts.Directory))
	cpuAfter, cpuErr := processCPU()
	runtime.ReadMemStats(&after)
	report.Elapsed = time.Since(started)
	report.CPU = cpuAfter - cpuBefore
	report.TotalAlloc = after.TotalAlloc - before.TotalAlloc
	report.Mallocs = after.Mallocs - before.Mallocs
	err = errors.Join(err, cpuErr)
	if err != nil {
		return report, err
	}
	if report.Measured.Acknowledged == 0 {
		return report, errors.New("no measured acknowledgments")
	}
	count := float64(report.Measured.Acknowledged)
	report.RecordsPerSecond = count / report.Elapsed.Seconds()
	report.CPUPerRecord = float64(report.CPU) / count
	report.BytesPerRecord = float64(report.TotalAlloc) / count
	report.AllocsPerRecord = float64(report.Mallocs) / count
	reopenedAt := time.Now()
	report.Inspection, err = validateReopen(ctx, report.Before.Store, opts, workload.frontiers)
	report.ReopenDuration = time.Since(reopenedAt)
	report.Valid = err == nil
	return report, err
}

func validateReopen(
	ctx context.Context,
	initial durable.StoreInfo,
	opts options,
	frontiers [producerCount]uint64,
) (durable.Inspection, error) {
	ids := make([]durable.ID, producerCount)
	for producer := range producerCount {
		ids[producer] = durable.ID{Producer: producerID(producer), Sequence: frontiers[producer]}
	}
	inspection, err := durable.Inspect(ctx, opts.Directory, ids)
	if err != nil {
		return inspection, err
	}
	if err := checkInspection(inspection, initial, ids); err != nil {
		return inspection, err
	}
	config, err := queueConfig(opts)
	if err != nil {
		return inspection, err
	}
	reopened, err := durable.Open(ctx, opts.Directory, config)
	if err != nil {
		return inspection, err
	}
	observed := reopened.Snapshot().Store
	if observed.Epoch != initial.Epoch+1 {
		err = errors.New("reopen epoch did not increment once")
	}
	err = errors.Join(err, reopened.Close(ctx))
	if err != nil {
		return inspection, err
	}
	after, err := durable.Inspect(ctx, opts.Directory, ids)
	if err != nil {
		return inspection, err
	}
	expected := initial
	expected.Epoch++
	return after, checkInspection(after, expected, ids)
}

func checkInspection(
	inspection durable.Inspection,
	expected durable.StoreInfo,
	ids []durable.ID,
) error {
	store := inspection.Store
	if store.UUID != expected.UUID || store.Policy != expected.Policy ||
		store.Durability != expected.Durability || store.Epoch != expected.Epoch {
		return errors.New("inspection identity, policy, durability, or epoch mismatch")
	}
	wantCounts := durable.StoreCounts{Producers: producerCount, RetiredProducers: producerCount}
	if store.Counts != wantCounts {
		return fmt.Errorf("inspection counts: %+v", store.Counts)
	}
	if len(inspection.Statuses) != len(ids) {
		return errors.New("inspection status count mismatch")
	}
	for index, status := range inspection.Statuses {
		if status.ID != ids[index] || status.Kind != durable.Retired || !status.HasRetirement ||
			status.RetiredThrough != ids[index].Sequence {
			return fmt.Errorf("retirement frontier mismatch for producer %d: %+v", index, status)
		}
	}
	return nil
}

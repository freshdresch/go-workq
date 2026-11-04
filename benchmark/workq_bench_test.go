package benchmark

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

// BenchmarkWorkq runs every load against the comparison rows: workq, a buffered channel, a
// channel of batches, and the engine workq picks, driven by polling. ns/op is nanoseconds per
// item handled, except in burst, where one op is one burst.
func BenchmarkWorkq(b *testing.B) {
	benchmarkWorkqRows(b, workqCompareRows)
}

// BenchmarkWorkqTuning runs the workq tuning rows, each in the loads where the setting it
// changes matters.
func BenchmarkWorkqTuning(b *testing.B) {
	benchmarkWorkqRows(b, workqTuningRows)
}

// workCase is one sub-benchmark: one row in one combination of a load.
type workCase struct {
	mode  benchMode
	pace  time.Duration
	batch int
	work  time.Duration
	row   workRowSpec
	// path is the case's sub-benchmark name below the load, one element per level.
	path []string
}

// benchmarkWorkqRows runs one sub-benchmark for each case of rows in each load.
func benchmarkWorkqRows(b *testing.B, rows []workRowSpec) {
	for _, load := range workLoads {
		cases := workqCases(rows, load)
		if len(cases) == 0 {
			continue
		}
		b.Run("load="+load.name, func(b *testing.B) {
			runWorkqCases(b, load, cases, 0)
		})
	}
}

// workqCases returns, in run order, every case of rows in load: each row at each mode, pace,
// batch, and work it runs at that the load does not skip.
func workqCases(rows []workRowSpec, load workLoad) []workCase {
	paces := load.paces
	if paces == nil {
		paces = []time.Duration{0}
	}
	var cases []workCase
	for _, mode := range load.modes {
		for _, pace := range paces {
			for _, batch := range load.batches {
				for _, work := range load.works {
					if load.skip != nil && load.skip(mode, pace, work) {
						continue
					}
					for _, row := range rows {
						if !slices.Contains(row.batches, batch) {
							continue
						}
						if !rowRunsIn(row, load, mode, work) {
							continue
						}
						cases = append(cases, workCase{
							mode:  mode,
							pace:  pace,
							batch: batch,
							work:  work,
							row:   row,
							path:  workCasePath(load, mode, pace, batch, work, row),
						})
					}
				}
			}
		}
	}
	return cases
}

// workCasePath returns the sub-benchmark name elements of one case below its load. Only the
// paced load has a pace element.
func workCasePath(
	load workLoad,
	mode benchMode,
	pace time.Duration,
	batch int,
	work time.Duration,
	row workRowSpec,
) []string {
	path := []string{"mode=" + mode.name}
	if load.paces != nil {
		path = append(path, "pace="+durationLabel(pace))
	}
	return append(
		path,
		fmt.Sprintf("batch=%d", batch),
		"work="+durationLabel(work),
		"impl="+row.name,
	)
}

// runWorkqCases runs cases as a tree of sub-benchmarks, one level per path element from depth
// on. Grouping only consecutive cases is enough, because workqCases lists them in path order.
func runWorkqCases(b *testing.B, load workLoad, cases []workCase, depth int) {
	for start := 0; start < len(cases); {
		name := cases[start].path[depth]
		end := start + 1
		for end < len(cases) && cases[end].path[depth] == name {
			end++
		}
		group := cases[start:end]
		b.Run(name, func(b *testing.B) {
			if depth == len(group[0].path)-1 {
				benchmarkWorkqCase(b, load, group[0])
				return
			}
			runWorkqCases(b, load, group, depth+1)
		})
		start = end
	}
}

// benchmarkWorkqCase runs one case with b.N ops and reports its metrics.
func benchmarkWorkqCase(b *testing.B, load workLoad, c workCase) {
	b.ReportAllocs()
	run := load.newRun(b.N, c.mode, c.pace, c.work)
	built, err := c.row.factory(c.mode, c.batch)
	if err != nil {
		b.Fatalf("build row: %v", err)
	}
	result, err := runWorkq(
		b,
		built.state,
		built.row,
		c.mode,
		c.batch,
		run.total,
		run.produce,
		run.handlers,
		&run.base,
	)
	built.release()
	if err != nil {
		b.Fatalf("run failed: %v", err)
	}
	if result.handled != run.total {
		b.Fatalf("handled %d items, want %d", result.handled, run.total)
	}
	if result.cpuOK {
		b.ReportMetric(float64(result.cpu)/float64(b.N), "cpu-ns/op")
	}
	if c.mode.consumers > 1 {
		b.ReportMetric(result.largestShare, "max-consumer-share")
	}
	if run.metrics == nil {
		return
	}
	for name, value := range run.metrics(result) {
		b.ReportMetric(value, name)
	}
}

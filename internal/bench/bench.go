// Package bench measures per-operation latency and summarizes it as percentiles.
//
// tpuf's whole design is about hiding object-storage latency, so the interesting
// question is not "how many ops/sec" on average but "what does the tail look
// like" — and especially how the tail of an unindexed WAL-tail scan compares to a
// query served from the indexed epoch with the DRAM cache warm. A Recorder
// collects raw samples for one operation, Summarize reduces them to a Stats, and
// WriteTable renders one row per operation.
//
// Percentiles use the nearest-rank method, so every reported figure (including
// p99.9) is an actually-observed latency rather than an interpolated one — which
// is what you want when reasoning about real tail behavior.
package bench

import (
	"fmt"
	"io"
	"math"
	"sort"
	"text/tabwriter"
	"time"
)

// Recorder accumulates latency samples for one named operation.
//
// Recording is not safe for concurrent use: the benchmark driver runs operations
// sequentially so it can attribute each latency to a single call without lock
// contention skewing the measurement. Call NewRecorder; the zero value has no
// name.
type Recorder struct {
	name    string
	samples []time.Duration
}

// NewRecorder returns a Recorder labelled name.
func NewRecorder(name string) *Recorder {
	return &Recorder{name: name}
}

// Combine merges the samples of several recorders into one labelled recorder.
// Use it to aggregate concurrent workers that each recorded independently (a
// single Recorder is not safe for concurrent Record calls, so each worker keeps
// its own and they are combined once all have finished).
func Combine(name string, recorders ...*Recorder) *Recorder {
	out := NewRecorder(name)
	for _, r := range recorders {
		out.samples = append(out.samples, r.samples...)
	}
	return out
}

// Record appends a single measured latency.
func (r *Recorder) Record(d time.Duration) {
	r.samples = append(r.samples, d)
}

// Time runs fn, records how long it took, and returns fn's error unchanged. The
// duration is recorded even when fn fails, so a partial run still reports the
// latencies it observed.
func (r *Recorder) Time(fn func() error) error {
	start := time.Now()
	err := fn()
	r.Record(time.Since(start))
	return err
}

// Count reports how many samples have been recorded.
func (r *Recorder) Count() int {
	return len(r.samples)
}

// Stats is the percentile summary of a Recorder's samples. The duration fields
// are raw observed latencies; OpsPerSec is the sequential throughput implied by
// the mean (one operation after another, doing nothing else).
type Stats struct {
	Name                     string
	Count                    int
	Min, Max, Mean           time.Duration
	P50, P90, P95, P99, P999 time.Duration
	OpsPerSec                float64
}

// Summarize sorts a copy of the samples and computes the percentile summary. An
// empty Recorder yields a zero-count Stats rather than panicking, so callers can
// summarize an operation that never ran (e.g. BM25 on a vector-only namespace).
func (r *Recorder) Summarize() Stats {
	n := len(r.samples)
	s := Stats{Name: r.name, Count: n}
	if n == 0 {
		return s
	}

	sorted := make([]time.Duration, n)
	copy(sorted, r.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	s.Min = sorted[0]
	s.Max = sorted[n-1]
	s.Mean = sum / time.Duration(n)
	s.P50 = percentile(sorted, 50)
	s.P90 = percentile(sorted, 90)
	s.P95 = percentile(sorted, 95)
	s.P99 = percentile(sorted, 99)
	s.P999 = percentile(sorted, 99.9)
	if s.Mean > 0 {
		s.OpsPerSec = float64(time.Second) / float64(s.Mean)
	}
	return s
}

// percentile returns the p-th percentile (0 < p <= 100) of an ascending slice
// using the nearest-rank method: the value at rank ceil(p/100 * n), 1-indexed.
func percentile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(n)))
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}

// WriteTable renders one aligned row per Stats. An operation with zero samples is
// shown with "—" placeholders so a skipped phase stays visible instead of
// silently vanishing from the report.
func WriteTable(w io.Writer, stats []Stats) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "OPERATION\tN\tMIN\tP50\tP90\tP95\tP99\tP99.9\tMAX\tMEAN\tOPS/S")
	for _, s := range stats {
		if s.Count == 0 {
			fmt.Fprintf(tw, "%s\t0\t—\t—\t—\t—\t—\t—\t—\t—\t—\n", s.Name)
			continue
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%.0f\n",
			s.Name, s.Count,
			FormatDuration(s.Min), FormatDuration(s.P50), FormatDuration(s.P90), FormatDuration(s.P95),
			FormatDuration(s.P99), FormatDuration(s.P999), FormatDuration(s.Max), FormatDuration(s.Mean),
			s.OpsPerSec)
	}
	return tw.Flush()
}

// FormatDuration formats a duration at a precision that suits its magnitude:
// nanosecond values (the memory backend) keep full detail, while millisecond
// values (the MinIO backend) are trimmed so tables stay readable.
func FormatDuration(x time.Duration) string {
	switch {
	case x == 0:
		return "0"
	case x < time.Microsecond:
		return x.String()
	case x < time.Millisecond:
		return x.Round(10 * time.Nanosecond).String()
	case x < time.Second:
		return x.Round(time.Microsecond).String()
	default:
		return x.Round(time.Millisecond).String()
	}
}

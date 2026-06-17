package bench

import (
	"strings"
	"testing"
	"time"
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

// seq returns ascending millisecond samples lo..hi inclusive.
func seq(lo, hi int) []time.Duration {
	out := make([]time.Duration, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		out = append(out, ms(i))
	}
	return out
}

func TestSummarize(t *testing.T) {
	tests := []struct {
		name    string
		samples []time.Duration
		want    Stats
	}{
		{
			name:    "empty yields zero-count, no panic",
			samples: nil,
			want:    Stats{Count: 0},
		},
		{
			name:    "single sample collapses every percentile to that value",
			samples: []time.Duration{ms(7)},
			want:    Stats{Count: 1, Min: ms(7), Max: ms(7), Mean: ms(7), P50: ms(7), P90: ms(7), P95: ms(7), P99: ms(7), P999: ms(7)},
		},
		{
			// 1..100ms: nearest-rank puts p50 at the 50th value, p99 at the 99th,
			// p99.9 at ceil(99.9)=100th (the max), and mean at 50.5ms.
			name:    "1..100ms ascending",
			samples: seq(1, 100),
			want:    Stats{Count: 100, Min: ms(1), Max: ms(100), Mean: ms(5050) / 100, P50: ms(50), P90: ms(90), P95: ms(95), P99: ms(99), P999: ms(100)},
		},
		{
			name:    "unsorted input is sorted before summarizing",
			samples: []time.Duration{ms(30), ms(10), ms(20)},
			want:    Stats{Count: 3, Min: ms(10), Max: ms(30), Mean: ms(20), P50: ms(20), P90: ms(30), P95: ms(30), P99: ms(30), P999: ms(30)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := NewRecorder(tt.name)
			for _, s := range tt.samples {
				r.Record(s)
			}
			got := r.Summarize()

			if got.Count != tt.want.Count {
				t.Fatalf("Count = %d, want %d", got.Count, tt.want.Count)
			}
			if tt.want.Count == 0 {
				return
			}
			for _, c := range []struct {
				label     string
				got, want time.Duration
			}{
				{"Min", got.Min, tt.want.Min},
				{"Max", got.Max, tt.want.Max},
				{"Mean", got.Mean, tt.want.Mean},
				{"P50", got.P50, tt.want.P50},
				{"P90", got.P90, tt.want.P90},
				{"P95", got.P95, tt.want.P95},
				{"P99", got.P99, tt.want.P99},
				{"P999", got.P999, tt.want.P999},
			} {
				if c.got != c.want {
					t.Errorf("%s = %v, want %v", c.label, c.got, c.want)
				}
			}
		})
	}
}

func TestTimeRecordsEvenOnError(t *testing.T) {
	r := NewRecorder("op")
	wantErr := errSentinel
	if err := r.Time(func() error { return wantErr }); err != wantErr {
		t.Errorf("Time returned %v, want %v", err, wantErr)
	}
	if r.Count() != 1 {
		t.Errorf("Count = %d, want 1 (a failed op is still timed)", r.Count())
	}
}

func TestWriteTableShowsZeroCountAsDash(t *testing.T) {
	var b strings.Builder
	stats := []Stats{
		{Name: "query-vec", Count: 2, Min: ms(1), P50: ms(2), P90: ms(2), P95: ms(2), P99: ms(2), P999: ms(2), Max: ms(2), Mean: ms(2), OpsPerSec: 500},
		{Name: "query-bm25", Count: 0},
	}
	if err := WriteTable(&b, stats); err != nil {
		t.Fatalf("WriteTable: %v", err)
	}
	out := b.String()
	for _, want := range []string{"OPERATION", "P99.9", "query-vec", "query-bm25", "—"} {
		if !strings.Contains(out, want) {
			t.Errorf("table missing %q:\n%s", want, out)
		}
	}
}

type benchError string

func (e benchError) Error() string { return string(e) }

const errSentinel benchError = "boom"

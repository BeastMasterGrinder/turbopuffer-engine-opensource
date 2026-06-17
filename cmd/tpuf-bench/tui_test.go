package main

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// TestDashboardView renders the model directly (no terminal needed) and checks it
// shows the phase, a progress figure, and the rolled-up footer fields.
func TestDashboardView(t *testing.T) {
	var vec, bm25 atomic.Int64
	vec.Store(60)
	bm25.Store(0)
	store := cache.New(storage.New())
	phases := []loadPhase{
		{name: "query-vec", total: 100, done: &vec},
		{name: "query-bm25", total: 100, done: &bm25},
	}
	m := newDashboard("multi-tenant load", phases, store, cache.CacheStats{}, time.Now().Add(-2*time.Second))

	out := m.View()
	for _, want := range []string{"tpuf-bench", "query-vec", "query-bm25", "60", "qps", "hot", "ETA"} {
		if !strings.Contains(out, want) {
			t.Errorf("dashboard view missing %q:\n%s", want, out)
		}
	}
}

func TestCompact(t *testing.T) {
	for _, tt := range []struct {
		in   int64
		want string
	}{
		{0, "0"}, {999, "999"}, {1000, "1.0k"}, {14400, "14.4k"},
	} {
		if got := compact(tt.in); got != tt.want {
			t.Errorf("compact(%d) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

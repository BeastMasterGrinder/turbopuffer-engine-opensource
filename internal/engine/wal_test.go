package engine

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// doc builds a live upsert op carrying only an id, which is all the WAL fold
// cares about (it keys on ID and last-writer-wins). Cases that need attributes
// use a Document literal directly.
func doc(id string) Document {
	return Document{ID: id}
}

// tombstone builds a delete op for id.
func tombstone(id string) Document {
	return Document{ID: id, Deleted: true}
}

// liveIDs returns the sorted ids of a materialized live map for stable
// comparison.
func liveIDs(live map[string]Document) []string {
	ids := make([]string, 0, len(live))
	for id := range live {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func TestAppendWALAndReadWAL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()

	ops := []Document{doc("a"), doc("b")}
	if err := AppendWAL(ctx, store, "ns", 0, ops); err != nil {
		t.Fatalf("AppendWAL(seq=0) error = %v, want nil", err)
	}

	got, err := ReadWAL(ctx, store, "ns", 0)
	if err != nil {
		t.Fatalf("ReadWAL(seq=0) error = %v, want nil", err)
	}
	want := WALSegment{Seq: 0, Ops: ops}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReadWAL(seq=0) = %+v, want %+v", got, want)
	}
}

func TestAppendWALWritesAt20DigitKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()

	if err := AppendWAL(ctx, store, "demo", 7, []Document{doc("x")}); err != nil {
		t.Fatalf("AppendWAL error = %v, want nil", err)
	}

	keys, err := store.List(ctx, "demo/wal/")
	if err != nil {
		t.Fatalf("List error = %v, want nil", err)
	}
	want := []string{"demo/wal/00000000000000000007.json"}
	if !reflect.DeepEqual(keys, want) {
		t.Errorf("List(demo/wal/) = %v, want %v", keys, want)
	}
}

func TestAppendWALPutIfAbsentRace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()

	// Two upserts both believe WALSeq == 3 and try to write the same segment.
	if err := AppendWAL(ctx, store, "ns", 3, []Document{doc("winner")}); err != nil {
		t.Fatalf("first AppendWAL(seq=3) error = %v, want nil", err)
	}

	err := AppendWAL(ctx, store, "ns", 3, []Document{doc("loser")})
	if !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Fatalf("second AppendWAL(seq=3) error = %v, want ErrPreconditionFailed", err)
	}

	// The loser reloads (now WALSeq == 4) and rewrites at the new seq; this must
	// succeed and must not have clobbered the winner's segment.
	if err := AppendWAL(ctx, store, "ns", 4, []Document{doc("loser")}); err != nil {
		t.Fatalf("retry AppendWAL(seq=4) error = %v, want nil", err)
	}
	winner, err := ReadWAL(ctx, store, "ns", 3)
	if err != nil {
		t.Fatalf("ReadWAL(seq=3) error = %v, want nil", err)
	}
	if len(winner.Ops) != 1 || winner.Ops[0].ID != "winner" {
		t.Errorf("seq=3 ops = %+v, want the winner's [{ID:winner}]", winner.Ops)
	}
}

func TestReadWALNotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()

	_, err := ReadWAL(ctx, store, "ns", 0)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("ReadWAL(missing) error = %v, want ErrNotFound", err)
	}
}

func TestMaterializeLive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		segments    [][]Document // segments[i] is the ops of WAL seq i
		from, to    int64
		wantLive    []string        // sorted surviving ids
		wantDeleted map[string]bool // expected tombstone set in the window
	}{
		{
			name:        "empty window yields empty non-nil maps",
			segments:    [][]Document{{doc("a")}},
			from:        0,
			to:          0,
			wantLive:    []string{},
			wantDeleted: map[string]bool{},
		},
		{
			name:        "single segment",
			segments:    [][]Document{{doc("a"), doc("b")}},
			from:        0,
			to:          1,
			wantLive:    []string{"a", "b"},
			wantDeleted: map[string]bool{},
		},
		{
			name: "last-writer-wins across segments",
			segments: [][]Document{
				{{ID: "a", Attributes: map[string]any{"v": float64(1)}}},
				{{ID: "a", Attributes: map[string]any{"v": float64(2)}}},
			},
			from:        0,
			to:          2,
			wantLive:    []string{"a"},
			wantDeleted: map[string]bool{},
		},
		{
			name: "tombstone removes a prior document",
			segments: [][]Document{
				{doc("a"), doc("b")},
				{tombstone("a")},
			},
			from:        0,
			to:          2,
			wantLive:    []string{"b"},
			wantDeleted: map[string]bool{"a": true},
		},
		{
			name: "re-upsert after delete revives the id",
			segments: [][]Document{
				{doc("a")},
				{tombstone("a")},
				{doc("a")},
			},
			from:        0,
			to:          3,
			wantLive:    []string{"a"},
			wantDeleted: map[string]bool{},
		},
		{
			name: "delete after upsert within same window",
			segments: [][]Document{
				{doc("a")},
				{doc("a")},
				{tombstone("a")},
			},
			from:        0,
			to:          3,
			wantLive:    []string{},
			wantDeleted: map[string]bool{"a": true},
		},
		{
			name: "from,to windowing excludes earlier segments",
			segments: [][]Document{
				{doc("a")}, // seq 0 — before the window
				{doc("b")}, // seq 1 — in the window
				{doc("c")}, // seq 2 — after the window (to is exclusive)
			},
			from:        1,
			to:          2,
			wantLive:    []string{"b"},
			wantDeleted: map[string]bool{},
		},
		{
			name: "tombstone before the window is not applied",
			segments: [][]Document{
				{doc("a")},       // seq 0 — before the window
				{tombstone("a")}, // seq 1 — in the window: deletes within window only
			},
			from:        1,
			to:          2,
			wantLive:    []string{},
			wantDeleted: map[string]bool{"a": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := newTestStore()
			for seq, ops := range tt.segments {
				if err := AppendWAL(ctx, store, "ns", int64(seq), ops); err != nil {
					t.Fatalf("AppendWAL(seq=%d) error = %v, want nil", seq, err)
				}
			}

			live, deleted, err := MaterializeLiveAndDeleted(ctx, store, "ns", tt.from, tt.to)
			if err != nil {
				t.Fatalf("MaterializeLiveAndDeleted(%d,%d) error = %v, want nil", tt.from, tt.to, err)
			}
			if live == nil || deleted == nil {
				t.Fatalf("MaterializeLiveAndDeleted returned nil map: live=%v deleted=%v", live, deleted)
			}
			if gotLive := liveIDs(live); !reflect.DeepEqual(gotLive, tt.wantLive) {
				t.Errorf("live ids = %v, want %v", gotLive, tt.wantLive)
			}
			if !reflect.DeepEqual(deleted, tt.wantDeleted) {
				t.Errorf("deleted set = %v, want %v", deleted, tt.wantDeleted)
			}

			// MaterializeLive must agree with the live half of the richer call.
			onlyLive, err := MaterializeLive(ctx, store, "ns", tt.from, tt.to)
			if err != nil {
				t.Fatalf("MaterializeLive(%d,%d) error = %v, want nil", tt.from, tt.to, err)
			}
			if !reflect.DeepEqual(onlyLive, live) {
				t.Errorf("MaterializeLive = %v, want %v (the live map)", onlyLive, live)
			}
		})
	}
}

func TestMaterializeLiveMissingSegmentErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()

	// seq 0 exists, seq 1 does not; a fold over [0,2) must surface the gap.
	if err := AppendWAL(ctx, store, "ns", 0, []Document{doc("a")}); err != nil {
		t.Fatalf("AppendWAL(seq=0) error = %v, want nil", err)
	}

	_, _, err := MaterializeLiveAndDeleted(ctx, store, "ns", 0, 2)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("MaterializeLiveAndDeleted over a gap error = %v, want ErrNotFound", err)
	}
}

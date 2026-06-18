package engine

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// refSet is a trivially-correct reference set used to cross-check the bitmap's
// and/or/contains against a plain map of the same ids.
type refSet map[uint32]bool

func (r refSet) sorted() []uint32 {
	out := make([]uint32, 0, len(r))
	for id := range r {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func TestBitmapAddContainsLen(t *testing.T) {
	t.Parallel()
	b := newBitmap()
	ids := []uint32{0, 1, 5, 63, 64, 65535, 65536, 131072, 200000}
	for _, id := range ids {
		b.add(id)
	}
	// Re-adding is a no-op: cardinality must not double-count.
	for _, id := range ids {
		b.add(id)
	}

	if got := b.len(); got != len(ids) {
		t.Errorf("len after dedup: got %d, want %d", got, len(ids))
	}
	for _, id := range ids {
		if !b.contains(id) {
			t.Errorf("contains(%d): got false, want true", id)
		}
	}
	for _, id := range []uint32{2, 66, 65537, 999999} {
		if b.contains(id) {
			t.Errorf("contains(%d): got true, want false", id)
		}
	}
	if got, want := b.toSorted(), ids; !reflect.DeepEqual(got, want) {
		t.Errorf("toSorted: got %v, want %v", got, want)
	}
}

func TestBitmapArrayToDensePromotion(t *testing.T) {
	t.Parallel()
	// Push a single chunk past arrayMax so its container promotes to the dense
	// bitmap form, then verify every id (and only those) is still present.
	b := newBitmap()
	n := uint32(arrayMax + 100)
	for i := uint32(0); i < n; i++ {
		b.add(i)
	}
	if b.containers[0].dense == nil {
		t.Fatalf("container holding %d ids must have promoted to dense", n)
	}
	if got := b.len(); got != int(n) {
		t.Errorf("dense len: got %d, want %d", got, n)
	}
	for i := uint32(0); i < n; i++ {
		if !b.contains(i) {
			t.Errorf("dense contains(%d): got false, want true", i)
		}
	}
	if b.contains(n) {
		t.Errorf("dense contains(%d): got true, want false (just past the run)", n)
	}
}

func TestBitmapAndOrAgainstReference(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(7))

	for trial := 0; trial < 50; trial++ {
		var aRef, bRef refSet = refSet{}, refSet{}
		a, b := newBitmap(), newBitmap()
		// Mix small ids, cross-chunk ids, and a dense run so both container types
		// and the cross-container set ops are exercised.
		for i := 0; i < 300; i++ {
			id := uint32(rng.Intn(3 * 65536))
			a.add(id)
			aRef[id] = true
		}
		for i := 0; i < 300; i++ {
			id := uint32(rng.Intn(3 * 65536))
			b.add(id)
			bRef[id] = true
		}
		// Force a dense chunk in a.
		for i := uint32(0); i < arrayMax+50; i++ {
			a.add(500000 + i)
			aRef[500000+i] = true
		}

		var wantAnd, wantOr refSet = refSet{}, refSet{}
		for id := range aRef {
			if bRef[id] {
				wantAnd[id] = true
			}
			wantOr[id] = true
		}
		for id := range bRef {
			wantOr[id] = true
		}

		if got, want := a.and(b).toSorted(), wantAnd.sorted(); !reflect.DeepEqual(got, want) {
			t.Fatalf("trial %d AND: got %d ids, want %d", trial, len(got), len(want))
		}
		if got, want := a.or(b).toSorted(), wantOr.sorted(); !reflect.DeepEqual(got, want) {
			t.Fatalf("trial %d OR: got %d ids, want %d", trial, len(got), len(want))
		}
	}
}

func TestBitmapAndEmptyIsEmpty(t *testing.T) {
	t.Parallel()
	a := bitmapFromSorted([]uint32{1, 2, 3})
	empty := newBitmap()
	if got := a.and(empty); got.len() != 0 {
		t.Errorf("AND with empty: got len %d, want 0 (the prune)", got.len())
	}
	// Disjoint sets intersect to empty too.
	b := bitmapFromSorted([]uint32{4, 5, 6})
	if got := a.and(b); got.len() != 0 {
		t.Errorf("AND of disjoint sets: got len %d, want 0", got.len())
	}
}

func TestBitmapFromSortedRoundTrip(t *testing.T) {
	t.Parallel()
	ids := []uint32{0, 7, 64, 65535, 70000, 1 << 20}
	got := bitmapFromSorted(ids).toSorted()
	if !reflect.DeepEqual(got, ids) {
		t.Errorf("round trip: got %v, want %v", got, ids)
	}
}

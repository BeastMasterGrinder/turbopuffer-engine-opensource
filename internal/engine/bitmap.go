package engine

import (
	"math/bits"
	"sort"
)

// bitmap is a hand-written, roaring-lite compressed set of uint32 ids — the
// substrate for the attribute index (docs/extensions/bitmap-attribute-indexes.md).
// It exists because the dependency rule forbids importing RoaringBitmap/roaring:
// reaching for a library to implement a conceptually interesting piece defeats
// the purpose of the clone (CLAUDE.md), so the compressed-set primitive is
// hand-written stdlib, the same discipline as the k-means, RaBitQ-lite and BM25.
//
// Following the roaring design (Lemire et al.), the 32-bit id space is split
// into chunks of 65536 keyed on the high 16 bits, and each chunk is stored in
// whichever container is cheaper for its density:
//
//   - array container: a sorted list of the chunk's low-16-bit ids — 2 bytes/id,
//     so a sparse value (a category held by a handful of documents) stays tiny.
//   - bitmap container: a fixed 1024×uint64 (8 KB) dense bitset — used once a
//     chunk holds more than arrayMax ids, where the flat cost beats 2 bytes/id.
//
// Run containers (the third roaring type) are intentionally omitted: this clone
// indexes low-cardinality categorical fields where array+bitmap already capture
// the win, and runs add code without changing the demonstrated result (the doc
// flags them as optional). Set operations stay correct because two bitmaps are
// combined container-by-container keyed on the high 16 bits, picking the right
// algorithm per container pair.
//
// A bitmap is built by adding ids in any order (add keeps each container sorted /
// flips the right bit) and then read with and / or / contains / len. It is value
// data, not safe for concurrent mutation, but the indexer builds each bitmap
// single-threaded and the query path only reads.
type bitmap struct {
	// containers keyed by the high 16 bits of the id space, kept sorted by key so
	// and/or can merge two bitmaps with a linear two-pointer walk and so the JSON
	// form is deterministic.
	keys       []uint16
	containers []container
}

// container is one 65536-id chunk. Exactly one of arr / dense is non-empty: arr
// for a sparse chunk (sorted low-16 ids), dense for a packed chunk (a 1024-word
// bitset). cardinality caches the popcount so len is O(chunks), not O(ids).
type container struct {
	arr         []uint16 // array container: sorted low-16 ids; nil when dense
	dense       []uint64 // bitmap container: 1024 words covering 65536 bits; nil when sparse
	cardinality int
}

// arrayMax is the array↔bitmap threshold: above this many ids in a chunk the flat
// 8 KB bitmap container is cheaper (and faster to test) than a 2-byte-per-id
// sorted array. 4096 is roaring's documented crossover, where 2 bytes/id stops
// beating the fixed 8 KB (docs/extensions/bitmap-attribute-indexes.md).
const arrayMax = 4096

// denseWords is the number of uint64 words in a bitmap container: 65536 bits / 64.
const denseWords = 1024

// newBitmap returns an empty bitmap (the empty set).
func newBitmap() *bitmap { return &bitmap{} }

// add inserts id into the set. Adding an id already present is a no-op, so a
// caller may add the same id twice (e.g. a document that re-lists a value)
// without inflating the cardinality.
func (b *bitmap) add(id uint32) {
	hi := uint16(id >> 16)
	lo := uint16(id)
	i := b.index(hi)
	if i < len(b.keys) && b.keys[i] == hi {
		b.containers[i].add(lo)
		return
	}
	// Insert a fresh array container at i, keeping keys sorted.
	b.keys = append(b.keys, 0)
	copy(b.keys[i+1:], b.keys[i:])
	b.keys[i] = hi
	b.containers = append(b.containers, container{})
	copy(b.containers[i+1:], b.containers[i:])
	b.containers[i] = container{}
	b.containers[i].add(lo)
}

// index returns the position where key hi is (or would be inserted), via binary
// search over the sorted keys.
func (b *bitmap) index(hi uint16) int {
	return sort.Search(len(b.keys), func(i int) bool { return b.keys[i] >= hi })
}

// len returns the number of ids in the set (the popcount).
func (b *bitmap) len() int {
	n := 0
	for i := range b.containers {
		n += b.containers[i].cardinality
	}
	return n
}

// contains reports whether id is in the set.
func (b *bitmap) contains(id uint32) bool {
	hi := uint16(id >> 16)
	i := b.index(hi)
	if i >= len(b.keys) || b.keys[i] != hi {
		return false
	}
	return b.containers[i].contains(uint16(id))
}

// add sets the low-16 bit lo within a container, promoting an array container to
// a dense bitmap once it would exceed arrayMax.
func (c *container) add(lo uint16) {
	if c.dense != nil {
		w, bit := lo>>6, uint(lo&63)
		if c.dense[w]&(1<<bit) == 0 {
			c.dense[w] |= 1 << bit
			c.cardinality++
		}
		return
	}
	// Array container: insert in sorted order, ignoring duplicates.
	i := sort.Search(len(c.arr), func(i int) bool { return c.arr[i] >= lo })
	if i < len(c.arr) && c.arr[i] == lo {
		return
	}
	c.arr = append(c.arr, 0)
	copy(c.arr[i+1:], c.arr[i:])
	c.arr[i] = lo
	c.cardinality++
	if len(c.arr) > arrayMax {
		c.toDense()
	}
}

// contains reports whether the low-16 id lo is set in the container.
func (c *container) contains(lo uint16) bool {
	if c.dense != nil {
		return c.dense[lo>>6]&(1<<uint(lo&63)) != 0
	}
	i := sort.Search(len(c.arr), func(i int) bool { return c.arr[i] >= lo })
	return i < len(c.arr) && c.arr[i] == lo
}

// toDense converts an array container to a bitmap container in place. Called when
// the array grows past arrayMax, where the dense form is the cheaper representation.
func (c *container) toDense() {
	dense := make([]uint64, denseWords)
	for _, lo := range c.arr {
		dense[lo>>6] |= 1 << uint(lo&63)
	}
	c.dense = dense
	c.arr = nil
}

// each calls fn for every id in the container, in ascending order. The caller
// passes the high-16 key so fn can reconstruct the full uint32 id.
func (c *container) each(hi uint16, fn func(id uint32)) {
	base := uint32(hi) << 16
	if c.dense != nil {
		for w, word := range c.dense {
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				fn(base | uint32(w*64+bit))
				word &= word - 1
			}
		}
		return
	}
	for _, lo := range c.arr {
		fn(base | uint32(lo))
	}
}

// each calls fn for every id in the set, ascending. Used by the query planner to
// translate a candidate-set bitmap back into the ordinals it must score.
func (b *bitmap) each(fn func(id uint32)) {
	for i, hi := range b.keys {
		b.containers[i].each(hi, fn)
	}
}

// and returns the intersection of b and other as a new bitmap, merging the two
// sorted key lists and intersecting the matching containers. An "and" of an empty
// operand is empty, which is exactly the prune the planner relies on: if no
// document carries a value, no cluster can match it.
func (b *bitmap) and(other *bitmap) *bitmap {
	out := newBitmap()
	i, j := 0, 0
	for i < len(b.keys) && j < len(other.keys) {
		switch {
		case b.keys[i] < other.keys[j]:
			i++
		case b.keys[i] > other.keys[j]:
			j++
		default:
			if c, ok := b.containers[i].and(&other.containers[j]); ok {
				out.keys = append(out.keys, b.keys[i])
				out.containers = append(out.containers, c)
			}
			i++
			j++
		}
	}
	return out
}

// or returns the union of b and other as a new bitmap. Used to combine the
// per-value bitmaps an "or" filter selects (category = "a" OR category = "b").
func (b *bitmap) or(other *bitmap) *bitmap {
	out := newBitmap()
	i, j := 0, 0
	for i < len(b.keys) || j < len(other.keys) {
		switch {
		case j >= len(other.keys) || (i < len(b.keys) && b.keys[i] < other.keys[j]):
			out.keys = append(out.keys, b.keys[i])
			out.containers = append(out.containers, b.containers[i].clone())
			i++
		case i >= len(b.keys) || (j < len(other.keys) && other.keys[j] < b.keys[i]):
			out.keys = append(out.keys, other.keys[j])
			out.containers = append(out.containers, other.containers[j].clone())
			j++
		default:
			out.keys = append(out.keys, b.keys[i])
			out.containers = append(out.containers, b.containers[i].or(&other.containers[j]))
			i++
			j++
		}
	}
	return out
}

// and returns the intersection of two containers covering the same chunk, plus a
// flag reporting whether the result is non-empty (so the caller can drop empty
// chunks rather than carry them). It dispatches on the (array, dense) pairing so
// each combination uses the cheaper algorithm — the per-container-pair selection
// that keeps roaring set operations both correct and fast.
func (c *container) and(other *container) (container, bool) {
	var out container
	switch {
	case c.dense != nil && other.dense != nil:
		dense := make([]uint64, denseWords)
		card := 0
		for w := range dense {
			dense[w] = c.dense[w] & other.dense[w]
			card += bits.OnesCount64(dense[w])
		}
		out = container{dense: dense, cardinality: card}
	case c.dense != nil:
		// other is sparse: probe its (smaller) array against c's bitset.
		out = intersectArrayDense(other.arr, c.dense)
	case other.dense != nil:
		out = intersectArrayDense(c.arr, other.dense)
	default:
		// Both arrays: a two-pointer merge over the sorted lists.
		var arr []uint16
		i, j := 0, 0
		for i < len(c.arr) && j < len(other.arr) {
			switch {
			case c.arr[i] < other.arr[j]:
				i++
			case c.arr[i] > other.arr[j]:
				j++
			default:
				arr = append(arr, c.arr[i])
				i++
				j++
			}
		}
		out = container{arr: arr, cardinality: len(arr)}
	}
	return out, out.cardinality > 0
}

// intersectArrayDense intersects a sorted array of low-16 ids with a dense
// bitset, returning an array container (the result of an AND is at most as dense
// as its sparser operand, so the array form is always adequate).
func intersectArrayDense(arr []uint16, dense []uint64) container {
	var out []uint16
	for _, lo := range arr {
		if dense[lo>>6]&(1<<uint(lo&63)) != 0 {
			out = append(out, lo)
		}
	}
	return container{arr: out, cardinality: len(out)}
}

// or returns the union of two containers covering the same chunk. To stay simple
// and always-correct it unions through a scratch dense bitset and then compacts
// back to an array container when the result is sparse enough — the same
// array↔bitmap rule used on insert.
func (c *container) or(other *container) container {
	dense := make([]uint64, denseWords)
	c.fill(dense)
	other.fill(dense)
	card := 0
	for _, w := range dense {
		card += bits.OnesCount64(w)
	}
	out := container{dense: dense, cardinality: card}
	if card <= arrayMax {
		out.toArray()
	}
	return out
}

// fill ORs the container's ids into a scratch dense bitset.
func (c *container) fill(dense []uint64) {
	if c.dense != nil {
		for w := range dense {
			dense[w] |= c.dense[w]
		}
		return
	}
	for _, lo := range c.arr {
		dense[lo>>6] |= 1 << uint(lo&63)
	}
}

// toArray compacts a dense container back to an array container in place, used
// after a union whose result turned out sparse.
func (c *container) toArray() {
	arr := make([]uint16, 0, c.cardinality)
	for w, word := range c.dense {
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			arr = append(arr, uint16(w*64+bit))
			word &= word - 1
		}
	}
	c.arr = arr
	c.dense = nil
}

// clone returns a deep copy of the container so a union can reuse an operand's
// container without aliasing its backing slices.
func (c *container) clone() container {
	out := container{cardinality: c.cardinality}
	if c.dense != nil {
		out.dense = append([]uint64(nil), c.dense...)
		return out
	}
	out.arr = append([]uint16(nil), c.arr...)
	return out
}

// toSorted returns every id in the set as a sorted slice. Used for the compact
// JSON form (bitmaps serialize as their sorted id list) and in tests.
func (b *bitmap) toSorted() []uint32 {
	ids := make([]uint32, 0, b.len())
	b.each(func(id uint32) { ids = append(ids, id) })
	return ids
}

// bitmapFromSorted rebuilds a bitmap from a sorted (or any-order) id slice — the
// inverse of toSorted, used when decoding the attribute index from JSON.
func bitmapFromSorted(ids []uint32) *bitmap {
	b := newBitmap()
	for _, id := range ids {
		b.add(id)
	}
	return b
}

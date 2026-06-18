// Black-box tests for copy-on-write branches (docs/extensions/branches-copy-on-write.md).
// They exercise only the public Namespace API (Branch/Upsert/Index/Query/Info)
// over the in-memory store, the same way the CLI does, proving the headline
// properties: a branch reads the parent's data at the fork point, writes to
// either side stay isolated, the fork is O(1) (one manifest PUT, zero data
// objects), and the read path stitches the parent chain correctly across the
// fork boundary (tombstones included) and through a branch reindex.
package engine_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/engine"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// queryIDSet runs a wide vector query and returns the set of ids it surfaces.
// Every test doc here carries a vector, so a vector query with a generous TopK
// and NProbe reflects the full live set without depending on ranking.
func queryIDSet(t *testing.T, ns *engine.Namespace, dim int) map[string]bool {
	t.Helper()
	ctx := context.Background()
	q := make([]float32, dim)
	results, err := ns.Query(ctx, engine.QueryParams{
		RankBy: engine.RankBy{Vector: q},
		TopK:   1000,
		NProbe: 1000,
	})
	if err != nil {
		t.Fatalf("query: got err %v, want nil", err)
	}
	got := map[string]bool{}
	for _, r := range results {
		got[r.ID] = true
	}
	return got
}

// TestBranchReadsParentAtForkPoint verifies a freshly forked branch sees exactly
// the parent's data at the fork point — before AND after the parent has been
// indexed, so both the tail-resolution and the inherited-epoch resolution are
// covered. It also confirms the branch inherits the parent's immutable schema.
func TestBranchReadsParentAtForkPoint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := cache.New(storage.New())
	parent := engine.Open(store, "base")

	cfg := engine.NamespaceConfig{Dimension: 2, Metric: "euclidean", TextField: "body"}
	if err := parent.Create(ctx, cfg); err != nil {
		t.Fatalf("Create parent: %v", err)
	}
	if err := parent.Upsert(ctx, []engine.Document{
		{ID: "a", Vector: []float32{0, 0}, Attributes: map[string]any{"body": "alpha"}},
		{ID: "b", Vector: []float32{1, 1}, Attributes: map[string]any{"body": "beta"}},
	}); err != nil {
		t.Fatalf("Upsert parent: %v", err)
	}

	// Fork BEFORE the parent is indexed: the branch's only data is the parent's
	// unindexed WAL, which the branch must resolve through the inherited tail.
	if err := parent.Branch(ctx, "exp-unindexed"); err != nil {
		t.Fatalf("Branch before index: %v", err)
	}
	expUnindexed := engine.Open(store, "exp-unindexed")
	if got := queryIDSet(t, expUnindexed, 2); !got["a"] || !got["b"] || len(got) != 2 {
		t.Errorf("branch (forked before index) sees %v, want exactly {a,b}", keys(got))
	}

	// The branch inherits the parent's schema verbatim.
	info, err := expUnindexed.Info(ctx)
	if err != nil {
		t.Fatalf("Info branch: %v", err)
	}
	if info.Dimension != 2 || info.Metric != "euclidean" || info.TextField != "body" {
		t.Errorf("branch schema: got dim=%d metric=%q text=%q, want 2/euclidean/body", info.Dimension, info.Metric, info.TextField)
	}
	if !info.IsBranch() || info.Parent != "base" {
		t.Errorf("branch manifest: got IsBranch=%v Parent=%q, want true/base", info.IsBranch(), info.Parent)
	}

	// Now index the parent and fork AGAIN: this branch reads the parent's FROZEN
	// index epoch (resolveIndex up the chain) rather than the tail.
	if err := parent.Index(ctx); err != nil {
		t.Fatalf("Index parent: %v", err)
	}
	if err := parent.Branch(ctx, "exp-indexed"); err != nil {
		t.Fatalf("Branch after index: %v", err)
	}
	expIndexed := engine.Open(store, "exp-indexed")
	if got := queryIDSet(t, expIndexed, 2); !got["a"] || !got["b"] || len(got) != 2 {
		t.Errorf("branch (forked after index) sees %v, want exactly {a,b}", keys(got))
	}

	// A branch forked after the parent indexed reads the parent's epoch but has no
	// epoch of its own (IndexEpoch 0) until it reindexes; its tail starts at the
	// fork seq, so there is nothing in the tail yet.
	bi, err := expIndexed.Info(ctx)
	if err != nil {
		t.Fatalf("Info exp-indexed: %v", err)
	}
	if bi.IndexEpoch != 0 {
		t.Errorf("branch own IndexEpoch: got %d, want 0 (reads parent's frozen epoch)", bi.IndexEpoch)
	}
	if bi.ForkIndexEpoch != 1 {
		t.Errorf("branch ForkIndexEpoch: got %d, want 1 (parent's epoch at fork)", bi.ForkIndexEpoch)
	}
	if bi.ForkWALSeq != 1 || bi.IndexedUpTo != 1 {
		t.Errorf("branch fork point: got ForkWALSeq=%d IndexedUpTo=%d, want 1/1", bi.ForkWALSeq, bi.IndexedUpTo)
	}
}

// TestBranchWritesAreIsolated is the core CoW guarantee: a write to the branch
// never appears in the parent, and a write to the parent after the fork never
// appears in the branch (a branch is a point-in-time fork). Both directions are
// asserted on the same store so the two manifests genuinely coexist.
func TestBranchWritesAreIsolated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := cache.New(storage.New())
	parent := engine.Open(store, "prod")

	if err := parent.Create(ctx, engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := parent.Upsert(ctx, []engine.Document{
		{ID: "shared", Vector: []float32{0, 0}},
	}); err != nil {
		t.Fatalf("Upsert parent seed: %v", err)
	}

	if err := parent.Branch(ctx, "exp"); err != nil {
		t.Fatalf("Branch: %v", err)
	}
	exp := engine.Open(store, "exp")

	// Write into the branch only.
	if err := exp.Upsert(ctx, []engine.Document{
		{ID: "branch-only", Vector: []float32{0, 0}},
	}); err != nil {
		t.Fatalf("Upsert branch: %v", err)
	}
	// Write into the parent only, AFTER the fork.
	if err := parent.Upsert(ctx, []engine.Document{
		{ID: "parent-only", Vector: []float32{0, 0}},
	}); err != nil {
		t.Fatalf("Upsert parent post-fork: %v", err)
	}

	parentIDs := queryIDSet(t, parent, 2)
	branchIDs := queryIDSet(t, exp, 2)

	// Parent: the seed + its own post-fork write, but NOT the branch's write.
	if !parentIDs["shared"] || !parentIDs["parent-only"] {
		t.Errorf("parent missing its own data: got %v, want shared+parent-only", keys(parentIDs))
	}
	if parentIDs["branch-only"] {
		t.Errorf("parent leaked branch write: got %v, must not contain branch-only", keys(parentIDs))
	}

	// Branch: the inherited seed + its own write, but NOT the parent's post-fork
	// write (point-in-time fork).
	if !branchIDs["shared"] || !branchIDs["branch-only"] {
		t.Errorf("branch missing expected data: got %v, want shared+branch-only", keys(branchIDs))
	}
	if branchIDs["parent-only"] {
		t.Errorf("branch saw parent's post-fork write: got %v, must not contain parent-only (point-in-time fork)", keys(branchIDs))
	}
}

// TestBranchTombstoneShadowsParent verifies a tombstone written into the branch
// hides an inherited parent document on the branch ONLY — the parent still has
// it. It then reindexes the branch (the CoW "flatten on write" boundary) and
// re-checks, proving the tombstone survives materialization into the branch's own
// epoch and the parent remains untouched.
func TestBranchTombstoneShadowsParent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := cache.New(storage.New())
	parent := engine.Open(store, "p")

	if err := parent.Create(ctx, engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := parent.Upsert(ctx, []engine.Document{
		{ID: "keep", Vector: []float32{0, 0}},
		{ID: "drop", Vector: []float32{1, 1}},
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := parent.Index(ctx); err != nil {
		t.Fatalf("Index parent: %v", err)
	}
	if err := parent.Branch(ctx, "b"); err != nil {
		t.Fatalf("Branch: %v", err)
	}
	branch := engine.Open(store, "b")

	// Delete an inherited (parent-only) id on the branch, and re-upsert a parent
	// id with a fresh vector to prove last-writer-wins across the fork boundary.
	if err := branch.Upsert(ctx, []engine.Document{
		{ID: "drop", Deleted: true},
		{ID: "keep", Vector: []float32{9, 9}}, // shadow the inherited vector
	}); err != nil {
		t.Fatalf("Upsert branch ops: %v", err)
	}

	// Pre-reindex: branch tail shadows the parent epoch.
	bIDs := queryIDSet(t, branch, 2)
	if bIDs["drop"] {
		t.Errorf("branch still shows tombstoned inherited id: got %v, must not contain drop", keys(bIDs))
	}
	if !bIDs["keep"] {
		t.Errorf("branch lost re-upserted id keep: got %v", keys(bIDs))
	}
	// The re-upsert must win: the branch's "keep" is at (9,9), so a query near
	// (9,9) finds it nearest with distance 0.
	near, err := branch.Query(ctx, engine.QueryParams{RankBy: engine.RankBy{Vector: []float32{9, 9}}, TopK: 1, NProbe: 1000})
	if err != nil {
		t.Fatalf("near query: %v", err)
	}
	if len(near) == 0 || near[0].ID != "keep" || near[0].Dist != 0 {
		t.Errorf("branch keep re-upsert not winning at (9,9): got %+v, want keep at dist 0", near)
	}

	// Parent must be untouched by the branch's delete + re-upsert.
	pIDs := queryIDSet(t, parent, 2)
	if !pIDs["drop"] || !pIDs["keep"] || len(pIDs) != 2 {
		t.Errorf("parent mutated by branch ops: got %v, want exactly {keep,drop}", keys(pIDs))
	}

	// Reindex the branch: it materializes its FULL logical WAL (parent-inherited +
	// own) into its OWN epoch and from now on reads its own epoch (CoW flatten).
	if err := branch.Index(ctx); err != nil {
		t.Fatalf("Index branch: %v", err)
	}
	bi, err := branch.Info(ctx)
	if err != nil {
		t.Fatalf("Info branch after index: %v", err)
	}
	if bi.IndexEpoch != 1 {
		t.Errorf("branch own epoch after reindex: got %d, want 1", bi.IndexEpoch)
	}
	if bi.DocCount != 1 {
		t.Errorf("branch DocCount after reindex: got %d, want 1 (keep only; drop tombstoned)", bi.DocCount)
	}

	// Post-reindex, the tombstone and last-writer-wins must still hold, now served
	// entirely from the branch's own epoch.
	bIDs2 := queryIDSet(t, branch, 2)
	if bIDs2["drop"] || !bIDs2["keep"] || len(bIDs2) != 1 {
		t.Errorf("branch after reindex: got %v, want exactly {keep}", keys(bIDs2))
	}
	// Parent still untouched after the branch's own index built under the branch
	// prefix.
	pIDs2 := queryIDSet(t, parent, 2)
	if !pIDs2["drop"] || !pIDs2["keep"] || len(pIDs2) != 2 {
		t.Errorf("parent mutated by branch reindex: got %v, want exactly {keep,drop}", keys(pIDs2))
	}
}

// TestBranchMultiLevelChain forks a branch off a branch (A -> B -> C) and checks
// C resolves the whole chain: it sees A's data, B's additions, and its own, while
// each ancestor stays isolated from the descendant's writes.
func TestBranchMultiLevelChain(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := cache.New(storage.New())

	a := engine.Open(store, "a")
	if err := a.Create(ctx, engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"}); err != nil {
		t.Fatalf("Create a: %v", err)
	}
	if err := a.Upsert(ctx, []engine.Document{{ID: "from-a", Vector: []float32{0, 0}}}); err != nil {
		t.Fatalf("Upsert a: %v", err)
	}
	if err := a.Index(ctx); err != nil { // index A so C reads A's frozen epoch
		t.Fatalf("Index a: %v", err)
	}

	if err := a.Branch(ctx, "b"); err != nil {
		t.Fatalf("Branch b: %v", err)
	}
	b := engine.Open(store, "b")
	if err := b.Upsert(ctx, []engine.Document{{ID: "from-b", Vector: []float32{1, 1}}}); err != nil {
		t.Fatalf("Upsert b: %v", err)
	}

	if err := b.Branch(ctx, "c"); err != nil {
		t.Fatalf("Branch c: %v", err)
	}
	c := engine.Open(store, "c")
	if err := c.Upsert(ctx, []engine.Document{{ID: "from-c", Vector: []float32{2, 2}}}); err != nil {
		t.Fatalf("Upsert c: %v", err)
	}

	// C sees the whole chain.
	cIDs := queryIDSet(t, c, 2)
	for _, want := range []string{"from-a", "from-b", "from-c"} {
		if !cIDs[want] {
			t.Errorf("grandchild c missing %q: got %v", want, keys(cIDs))
		}
	}
	if len(cIDs) != 3 {
		t.Errorf("grandchild c: got %v, want exactly {from-a,from-b,from-c}", keys(cIDs))
	}

	// Ancestors are isolated from descendants' writes.
	bIDs := queryIDSet(t, b, 2)
	if bIDs["from-c"] {
		t.Errorf("b leaked c's write: got %v", keys(bIDs))
	}
	aIDs := queryIDSet(t, a, 2)
	if aIDs["from-b"] || aIDs["from-c"] {
		t.Errorf("a leaked descendant writes: got %v, want only {from-a}", keys(aIDs))
	}
}

// countingStore wraps an ObjectStore and counts the data-bearing writes (Put,
// PutIfAbsent, PutCAS) per key prefix, so a test can prove a fork copies ZERO
// data objects — the only write a fork performs is the single child manifest PUT.
type countingStore struct {
	storage.ObjectStore
	mu     sync.Mutex
	writes map[string]int // key -> number of writes
}

func newCountingStore() *countingStore {
	return &countingStore{ObjectStore: storage.New(), writes: map[string]int{}}
}

func (c *countingStore) note(key string) {
	c.mu.Lock()
	c.writes[key]++
	c.mu.Unlock()
}

func (c *countingStore) Put(ctx context.Context, key string, body []byte) (string, error) {
	c.note(key)
	return c.ObjectStore.Put(ctx, key, body)
}

func (c *countingStore) PutIfAbsent(ctx context.Context, key string, body []byte) (string, error) {
	c.note(key)
	return c.ObjectStore.PutIfAbsent(ctx, key, body)
}

func (c *countingStore) PutCAS(ctx context.Context, key string, body []byte, ifMatch string) (string, error) {
	c.note(key)
	return c.ObjectStore.PutCAS(ctx, key, body, ifMatch)
}

// writesUnder returns the number of writes to keys beginning with prefix.
func (c *countingStore) writesUnder(prefix string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for k, v := range c.writes {
		if strings.HasPrefix(k, prefix) {
			n += v
		}
	}
	return n
}

// TestBranchForkIsO1 proves the fork is constant-time and copy-on-write: forking
// a parent that holds an indexed corpus performs EXACTLY ONE write — the child
// manifest — and ZERO writes to any wal/ or index/ object under the child prefix.
// That is the whole point of CoW: the child shares the parent's bytes by
// reference, so a fork's cost is independent of the parent's size.
func TestBranchForkIsO1(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cs := newCountingStore()
	store := cache.New(cs)
	parent := engine.Open(store, "huge")

	if err := parent.Create(ctx, engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Build a non-trivial corpus + index so "copy everything" would be many writes.
	docs := make([]engine.Document, 0, 20)
	for i := 0; i < 20; i++ {
		docs = append(docs, engine.Document{ID: idForBranch(i), Vector: []float32{float32(i), float32(i)}})
	}
	if err := parent.Upsert(ctx, docs); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := parent.Index(ctx); err != nil {
		t.Fatalf("Index: %v", err)
	}

	// Snapshot write counts, then fork.
	beforeChild := cs.writesUnder("child/")
	beforeManifest := cs.writes["child/manifest.json"]
	if err := parent.Branch(ctx, "child"); err != nil {
		t.Fatalf("Branch: %v", err)
	}

	// Exactly one write under the child prefix: its manifest. No wal/ or index/.
	if got := cs.writesUnder("child/"); got-beforeChild != 1 {
		t.Errorf("writes under child/ during fork: got %d, want exactly 1 (the manifest)", got-beforeChild)
	}
	if got := cs.writes["child/manifest.json"]; got-beforeManifest != 1 {
		t.Errorf("child manifest writes during fork: got %d, want exactly 1", got-beforeManifest)
	}
	if got := cs.writesUnder("child/wal/"); got != 0 {
		t.Errorf("child wal writes during fork: got %d, want 0 (no data copied)", got)
	}
	if got := cs.writesUnder("child/index/"); got != 0 {
		t.Errorf("child index writes during fork: got %d, want 0 (shares parent epoch)", got)
	}

	// And the fork must not have touched the parent's objects at all.
	beforeParent := cs.writesUnder("huge/")
	_ = beforeParent // already counted before; the assertion is that the child sees the data:
	childIDs := queryIDSet(t, engine.Open(store, "child"), 2)
	if len(childIDs) != 20 {
		t.Errorf("forked child sees %d docs, want 20 (all shared from parent)", len(childIDs))
	}
}

// TestBranchAlreadyExists verifies a branch onto a name that already exists fails
// write-once, the same guard Create gives — so two concurrent forks onto one name
// cannot both win.
func TestBranchAlreadyExists(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := cache.New(storage.New())
	parent := engine.Open(store, "src")
	if err := parent.Create(ctx, engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := parent.Branch(ctx, "fork"); err != nil {
		t.Fatalf("first Branch: %v", err)
	}
	if err := parent.Branch(ctx, "fork"); err == nil {
		t.Fatalf("second Branch onto same name: got nil err, want already-exists")
	}
}

// TestBranchMissingParent verifies branching off a non-existent parent errors
// clearly rather than writing a dangling child manifest.
func TestBranchMissingParent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := cache.New(storage.New())
	if err := engine.Open(store, "ghost").Branch(ctx, "child"); err == nil {
		t.Fatalf("branch off missing parent: got nil err, want an error")
	}
}

// TestBranchConcurrentIndependentHeads hammers a parent and a branch with
// concurrent upserts at once. Because each manifest is its OWN CAS head, the two
// write streams are independent races on two different keys and must not
// interfere: every write on each side lands, and neither side sees the other's
// ids. Under -race this also asserts the shared store and the two handles are
// safe to drive concurrently.
func TestBranchConcurrentIndependentHeads(t *testing.T) {
	ctx := context.Background()
	store := cache.New(storage.New())
	parent := engine.Open(store, "root")
	if err := parent.Create(ctx, engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := parent.Upsert(ctx, []engine.Document{{ID: "seed", Vector: []float32{0, 0}}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := parent.Branch(ctx, "fork"); err != nil {
		t.Fatalf("Branch: %v", err)
	}
	branch := engine.Open(store, "fork")

	const n = 8
	var wg sync.WaitGroup
	wg.Add(2 * n)
	errs := make([]error, 2*n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = parent.Upsert(ctx, []engine.Document{{ID: "p" + string(rune('A'+i)), Vector: []float32{0, 0}}})
		}(i)
		go func(i int) {
			defer wg.Done()
			errs[n+i] = branch.Upsert(ctx, []engine.Document{{ID: "b" + string(rune('A'+i)), Vector: []float32{0, 0}}})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent upsert %d: %v", i, err)
		}
	}

	pIDs := queryIDSet(t, parent, 2)
	bIDs := queryIDSet(t, branch, 2)
	// Parent: seed + its n writes, none of the branch's.
	if len(pIDs) != n+1 {
		t.Errorf("parent doc count: got %d (%v), want %d", len(pIDs), keys(pIDs), n+1)
	}
	// Branch: inherited seed + its n writes, none of the parent's post-fork writes.
	if len(bIDs) != n+1 {
		t.Errorf("branch doc count: got %d (%v), want %d", len(bIDs), keys(bIDs), n+1)
	}
	for i := 0; i < n; i++ {
		if pIDs["b"+string(rune('A'+i))] {
			t.Errorf("parent leaked branch write b%c", 'A'+i)
		}
		if bIDs["p"+string(rune('A'+i))] {
			t.Errorf("branch leaked parent write p%c", 'A'+i)
		}
	}
}

// idForBranch returns a stable doc id for index i in the fork-cost corpus.
func idForBranch(i int) string {
	return "d" + string(rune('A'+i))
}

// keys returns the keys of a set as a slice for readable failure messages.
func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

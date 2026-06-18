# Hybrid search: fusing vector and BM25 rankings

> **Implemented (2026-06-18).** Server-side Reciprocal Rank Fusion (k=60) is built in
> `internal/engine/query.go`; the CLI accepts `tpuf query <ns> --vector … --bm25 …` together (hits carry
> `$dist`, `$score`, and the fused `$rrf`), and `cmd/tpuf-bench --hybrid` proves the recall win. The text
> below is the design rationale; weighted/convex score fusion (`--alpha`) is left deliberately deferred.

A vector (ANN) query and a BM25 query answer different questions about the same corpus: the vector
ranking captures *semantic* similarity (a query about "canines" can surface a doc about "dogs"), while
BM25 captures *lexical* match (exact terms, rare keywords, product codes, names). **Hybrid search** runs
both and **fuses the two ranked lists into one**, so a result that is strong on either signal — or
moderately strong on both — rises to the top. This doc is about the fusion step: how to merge two
orderings that are produced by incomparable scoring functions, why the naive "just add the scores"
approach is a trap, and how this would hook into `tpuf`.

turbopuffer takes an explicit stance here: it serves vector and BM25 from the same `Query` endpoint
(`RankBy` chooses the mode) but **does not fuse server-side** — it documents fusion as a *client-side*
step. Its Hybrid Search guide ships a Python `reciprocal_rank_fusion()` helper and tells you to "Use
turbopuffer for initial retrieval to narrow millions of results to dozens for rank fusion and
re-ranking" — i.e. each query returns its own ranked list and the client combines them
([turbopuffer Hybrid Search docs](https://turbopuffer.com/docs/hybrid)). So this is a real extension to
*our* clone (we don't fuse today), and a faithful-to-turbopuffer one only if we keep it a thin layer
over two independent retrievals.

## The core problem: the two scores are not comparable

Our two retrieval modes return values on completely different scales (see
[`internal/engine/types.go`](../../internal/engine/types.go), `QueryResult`):

- **Vector mode** sets `Dist` — a distance, **lower is better**, range depends on the metric (cosine
  distance in `[0,2]`, Euclidean unbounded).
- **BM25 mode** sets `Score` — **higher is better**, unbounded above, and its magnitude depends on
  corpus statistics (`IDF`, `avgdl`) and query length (see [`04-bm25-fulltext.md`](../04-bm25-fulltext.md)).

You cannot add or compare these directly. A BM25 score of 8.3 and a cosine distance of 0.21 have no
common unit. Fusion methods exist precisely to bridge that gap. Two families dominate:

1. **Rank fusion** (RRF): throw the scores away, keep only the *position* in each list.
2. **Score fusion** (weighted/convex): normalize both scores onto a common scale, then take a weighted
   sum.

### Reciprocal Rank Fusion (RRF)

RRF (Cormack, Clarke & Büttcher, SIGIR 2009) combines several rankings by summing a reciprocal of each
document's rank. For a document `d`, a set of rankings `R`, and each ranking `r ∈ R` where `r(d)` is
`d`'s 1-based position in that ranking:

```
RRFscore(d) = Σ_{r ∈ R}  1 / (k + r(d))
```

The paper fixes **`k = 60`** (Section 1; "`k = 60` was fixed during a pilot investigation and not
altered during subsequent validation"). Its pilot (Table 1) sweeps `k` from 0 to 500 and finds MAP
barely moves (`.2072` at `k=0`, peaking `.2147` near `k=80`) — i.e. **the method is insensitive to `k`**;
`k` only "mitigates the impact of high rankings by outlier systems." Worked example with `k=60`:

| doc | vector rank | BM25 rank | RRF score                              |
|-----|-------------|-----------|----------------------------------------|
| A   | 1           | 4         | 1/61 + 1/64 ≈ 0.01639 + 0.01563 = **0.03202** |
| B   | 2           | 2         | 1/62 + 1/62 ≈ 0.01613 + 0.01613 = **0.03226** |
| C   | —           | 1         | 0      + 1/61 ≈ **0.01639**            |
| D   | 3           | —         | 1/63   + 0    ≈ **0.01587**            |

B wins despite never ranking #1 in either list, because it is *consistently* near the top of both — the
behavior hybrid search wants. A document missing from a list simply contributes nothing for that list
(no penalty term).

Why ranks and not scores? The paper's argument (Discussion): naive score combination (their CombMNZ
baseline, which weights each system's scores) has "higher variance," which the authors conjecture "is
due to the fact that, by happenstance, some scores are more amenable than others." RRF instead "combines
ranks without regard to the arbitrary scores returned by particular ranking methods." Empirically it beat Condorcet Fuse on
all TREC collections tested (Table 2) and, as a meta-learner, beat every individual rank-learning method
on LETOR 3 (Table 3: RRF MAP `.6051` vs. e.g. ListNet `.5846`, RankSVM `.5737`). RRF's strengths and the
matching weakness:

- **No tuning, no normalization** — it works even when the two relevance signals are unrelated. This is
  exactly why Elasticsearch adopted it for its `rrf` retriever (default `rank_constant: 60`,
  `rank_window_size` defaults to `size`) and calls out that "RRF requires no tuning, and the different
  relevance indicators do not have to be related to each other"
  ([Elasticsearch RRF reference](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/reciprocal-rank-fusion)).
- **It discards magnitude.** A document that BM25 considers a runaway #1 (score 40 vs. the runner-up's 3)
  is treated identically to a barely-#1. If the *gap* between adjacent results is meaningful, RRF can't
  see it.

### Weighted / convex score fusion

The score-fusion family keeps magnitudes but must first make them comparable, then takes a weighted sum
with a mixing weight `α ∈ [0,1]`:

```
norm(s)        = (s − min) / (max − min)          # min-max onto [0,1], per list
hybrid(d)      = α · norm(vec_sim(d)) + (1 − α) · norm(bm25(d))
```

Note vector mode must first be turned into a *similarity* (higher-is-better), e.g. `sim = −dist` or
`sim = 1 − dist` for cosine, before normalizing. Weaviate's hybrid search is the reference
implementation of this family. Its `alpha` is exactly this mixing weight — "An `alpha` of `1` is a pure
vector search. An `alpha` of `0` is a pure keyword search."
([Weaviate hybrid-search how-to](https://docs.weaviate.io/weaviate/search/hybrid)), with `alpha = 0.5`
the equal-weight default. Its `relativeScoreFusion` sets "the largest score … to 1 and the lowest to 0"
per list and became the default in v1.24 (replacing `rankedFusion`, computed "according to
`1/(RANK + 60)`"), per Weaviate's internal benchmarks "a ~6% improvement in recall"
([Weaviate hybrid-search concepts](https://docs.weaviate.io/weaviate/concepts/search/hybrid-search),
[Weaviate fusion-algorithms blog](https://weaviate.io/blog/hybrid-search-fusion-algorithms)).

**The score-normalization pitfalls** (why this is harder than it looks):

- **Min/max are set by the result window, not the corpus.** Normalize over the top-K and the
  bottom-ranked item is forced to `0` and the top to `1` *in every query*, even when the absolute
  spread is tiny — manufacturing contrast that isn't there. A different K changes the normalized values.
- **Outliers crush the scale.** One BM25 result with a huge score (a rare exact-match term) pins `max`,
  squashing everyone else toward 0; min-max is not robust to that. Standardization (`z`-score) trades
  one set of distribution assumptions for another.
- **The two distributions have different shapes.** BM25 is unbounded and long-tailed; cosine similarity
  is bounded and often clustered. Linearly normalizing both to `[0,1]` does **not** make a 0.8 mean the
  same thing in each.
- **`α` is a real tuning burden.** Unlike RRF's `k`, `α` materially changes ordering and the best value
  is corpus- and query-dependent — which is the cost you pay for keeping magnitude information.

Rule of thumb the sources support: **RRF when you want robustness and zero tuning** (the common default);
**weighted score fusion when the score gaps are trustworthy and you're willing to tune `α`** and validate
normalization on your data.

## What our clone does today, and the gap

Today `tpuf` has **no fusion at all**. The query planner in
[`internal/engine/query.go`](../../internal/engine/query.go) (`RunQuery`) requires *exactly one* mode and
rejects both at once:

```
vec, text := p.RankBy.IsVector(), p.RankBy.IsText()
// both set  -> error "exactly one is required"
// neither   -> error "no rank mode set"
```

`RankBy` (in [`types.go`](../../internal/engine/types.go)) holds either a `Vector` or a `Text`, never
both; the CLI surfaces this as mutually exclusive `--vector` / `--bm25` flags
([`cmd/tpuf/main.go`](../../cmd/tpuf/main.go), `runQuery`). `QueryResult` carries `Dist` *or* `Score`,
never a single fused number. This matches the honest ledger in
[`05-clone-mapping.md`](../05-clone-mapping.md), which lists "hybrid fusion" under
"documented, not built." So the gap is: a single call that runs **both** retrievals and returns **one**
fused ordering.

## How it would hook into tpuf

The clean, turbopuffer-faithful design is a thin layer **on top of** the two existing retrieval paths —
not a new index, not a manifest change. Concretely:

1. **Add a hybrid `RankBy`.** `RankBy` would need to carry both a `Vector` and a `Text` (and the planner
   in `RunQuery` would route the both-set case to a new `runHybridQuery` instead of erroring). The CLI
   would allow `--vector` *and* `--bm25` together, plus `--fusion rrf|weighted` and (for weighted)
   `--alpha`.

2. **Reuse the existing retrieval functions verbatim.** `runHybridQuery` calls the *current*
   `runVectorQuery` and `runBM25Query` (both in `query.go`) — each already overlays the unindexed WAL
   tail via `MaterializeLiveAndDeleted` (in [`wal.go`](../../internal/engine/wal.go)) and subtracts
   tombstones, so **freshness and last-writer-wins come for free on both legs**. Fetch a deeper window
   than the final `TopK` from each (e.g. `TopK * shortlistMultiplier`, matching the existing constant in
   `query.go`) so fusion has enough overlap to work with.

3. **Fuse the two `[]QueryResult` lists into one.** For RRF: assign each list a 1-based rank (the lists
   are already sorted — ascending `Dist`, descending `Score`), then sum `1/(k+rank)` per id, `k=60`. For
   weighted: convert `Dist` to a similarity, min-max normalize each list, then `α·sim + (1−α)·bm25`.
   Emit a new fused value in `QueryResult` (a `$rrf`/`$hybrid` field alongside the existing
   `$dist`/`$score`, per the JSON tags in `types.go`), sort, truncate to `TopK`.

4. **Filters and attributes need no new work.** Each leg already applies `Filter.Match` and returns
   `Attributes`; the fused id set is the union, deduped by id.

**Crucially, this touches no storage, manifest, WAL, or epoch code.** There is no new `index/v{epoch}/`
object and no manifest CAS — fusion is pure post-processing over two reads of the *same* live epoch +
WAL tail. That keeps the CAS-coordinated source-of-truth model
(see [`01-architecture.md`](../01-architecture.md)) completely untouched, which is the right call:
turbopuffer itself keeps fusion out of the engine, in client `search.py`/`search.ts`.

> One subtlety worth getting right: both legs must read the *same* manifest snapshot. Calling
> `LoadManifest` once and passing that `Manifest` into both retrievals (rather than letting each reload)
> avoids a torn read where a concurrent `index` swaps the epoch between the vector leg and the BM25 leg.

## What's genuinely hard / what to get right

- **Rank assignment with ties and gaps.** Our retrievals already break ties deterministically (on `id`),
  so ranks are stable — preserve that. Decide explicitly how a doc that appears in only one list is
  ranked: RRF naturally gives it one term and no penalty (correct); weighted fusion must treat the
  missing leg as the *normalized minimum* (often 0), or absent docs get an unfair free pass.
- **Window depth drives recall.** Fusing only the final top-10 of each list means a doc ranked #11 by
  vectors but #1 by BM25 may never enter the vector list and lose its vector contribution. Fetch a
  deeper candidate window per leg before fusing (the cost is a larger scan, not extra S3 round-trips,
  since both legs share the cached epoch).
- **RRF's `k` is safe; weighted's `α` is not.** Per the RRF paper's Table 1, `k` barely matters — pick
  60 and move on. `α` and the normalization scheme genuinely change results and must be validated on a
  labeled set; don't ship a default `α` as if it were neutral.
- **Don't normalize across the whole corpus by accident.** BM25 global stats (`N`, `avgdl`) already make
  raw scores corpus-relative; *then* min-max over the result window makes them window-relative. Be
  deliberate about which one you want — they are not the same, and mixing them silently is a common bug.
- **Honesty about turbopuffer.** turbopuffer publicly documents *client-side* RRF with `k=60` and does
  **not** publicly document any server-side fusion, weighting, or learned reranking inside its engine
  ([turbopuffer Hybrid Search docs](https://turbopuffer.com/docs/hybrid)). If we add server-side fusion
  to `tpuf`, we are going slightly *beyond* what turbopuffer exposes — fine for an educational clone, but
  it should be flagged as our choice, not presented as "what turbopuffer does."

## Sources

- **RRF paper (primary):** G. V. Cormack, C. L. A. Clarke, S. Büttcher, "Reciprocal Rank Fusion
  outperforms Condorcet and individual Rank Learning Methods," SIGIR 2009. Formula
  `RRFscore(d) = Σ 1/(k + r(d))` and `k = 60` (Section 1); `k`-insensitivity (Table 1, MAP `.2072`–`.2147`
  for `k`=0–500); TREC results (Table 2); LETOR 3 meta-learner results (Table 3, RRF MAP `.6051`).
  Read locally via WebFetch from <https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf> (redirects to
  `cormack.uwaterloo.ca`). Publisher record: <https://dl.acm.org/doi/10.1145/1571941.1572114>.
- **turbopuffer Hybrid Search:** <https://turbopuffer.com/docs/hybrid> — documents *client-side* RRF
  (`reciprocal_rank_fusion()` helper, `k = 60`), `multi_query` batching, and "Keep search logic in
  {search.py, search.ts}." No server-side fusion documented (flagged above as a gap, not an assertion of
  internals).
- **Elasticsearch RRF reference:** <https://www.elastic.co/docs/reference/elasticsearch/rest-apis/reciprocal-rank-fusion>
  — same `1/(k + rank)` formula, default `rank_constant: 60`, `rank_window_size` defaults to `size`,
  "RRF requires no tuning … relevance indicators do not have to be related."
- **Weaviate hybrid search (score fusion):** how-to page
  <https://docs.weaviate.io/weaviate/search/hybrid> ("An `alpha` of `1` is a pure vector search. An
  `alpha` of `0` is a pure keyword search."); concepts page
  <https://docs.weaviate.io/weaviate/concepts/search/hybrid-search> (`alpha` mixing weight, default
  `0.5`; `relativeScoreFusion` min-max to `[0,1]`, default since v1.24, vs. `rankedFusion` "according to
  `1/(RANK + 60)`"); and blog <https://weaviate.io/blog/hybrid-search-fusion-algorithms> (the "~6%
  improvement in recall" figure). The "~6%" figure is Weaviate's own internal-benchmark claim, not
  independently verified here.
- **This clone's code (read directly):** [`internal/engine/query.go`](../../internal/engine/query.go)
  (`RunQuery`, `runVectorQuery`, `runBM25Query`, `shortlistMultiplier`),
  [`internal/engine/types.go`](../../internal/engine/types.go) (`RankBy`, `QueryResult`, `Manifest`),
  [`internal/engine/wal.go`](../../internal/engine/wal.go) (`MaterializeLiveAndDeleted`),
  [`cmd/tpuf/main.go`](../../cmd/tpuf/main.go) (`runQuery`, `--vector`/`--bm25` flags).
- **In-repo context:** [`04-bm25-fulltext.md`](../04-bm25-fulltext.md) (BM25 scale/stats),
  [`01-architecture.md`](../01-architecture.md) (manifest/epoch/WAL-tail model),
  [`05-clone-mapping.md`](../05-clone-mapping.md) (hybrid fusion listed as "documented, not built").

# Extension — True RaBitQ: rotation and the unbiased estimator

> **Implemented (2026-06-18).** A seeded random orthogonal rotation + the unbiased distance estimator are
> built in `internal/engine/vector.go` (rotation stored per-epoch in `centroids.json`), used to order the
> prefilter shortlist before exact rerank; `cmd/tpuf-bench --rabitq` shows it reaches a recall target at a
> smaller shortlist than the lite codes. The text below is the design rationale; the bound-driven rerank
> cutoff is left deferred (the fixed shortlist multiplier + exact rerank is kept).

> An expansion of the `true RaBitQ rotation` non-goal in [`../05-clone-mapping.md`](../05-clone-mapping.md)
> ("documented, not built") and the "what to add to reach full RaBitQ fidelity" note at the end of
> [`../03-rabitq-quantization.md`](../03-rabitq-quantization.md).

Our clone ships **RaBitQ-lite**: it keeps the *shape* of RaBitQ (1-bit-per-dimension codes → cheap
popcount prefilter → exact rerank) but drops the two things that make real RaBitQ a *quantization
method with a provable error bound* rather than a heuristic: the **random orthogonal rotation** and the
**unbiased inner-product estimator**. This doc explains what the full method (Gao & Long, SIGMOD '24)
actually does — rotation, the unbiased estimator and its `O(1/√D)` error bound, and the bitwise scan —
citing the paper by section/equation, then describes exactly what upgrading `internal/engine/vector.go`
would entail and why turbopuffer cares. turbopuffer's published ANN v3 design uses RaBitQ for its binary
pass (16–32× compression; "less than 1% of data vectors in the narrowed search space need to be
reranked"), so this is the gap between our teaching code and what production does.

## Why a naive sign-bit code is "biased" (the gap RaBitQ closes)

Plain binary hashing — store `sign(o[i])`, treat the resulting ±1 vector as if it *were* the data
vector, dot it against the query — is exactly what our `Agreement` prefilter does, and it is **biased**:
the paper notes PQ and its variants "simply treat the quantized data vector as the data vector for
computing the distance ... it does not come with a theoretical error bound on the approximation"
(§1, Introduction). On adversarial datasets that bias can be large — the paper reports PQ exceeding
**50% average relative error** on the MSong dataset (§1). RaBitQ removes the bias by construction and
proves the residual error concentrates at `O(1/√D)`.

## How real RaBitQ works

The method has an **index phase** (Algorithm 1) and a **query phase** (Algorithm 2), summarized in §3.4.
Notation below follows the paper's Table 2: `oᵣ, qᵣ` are raw data/query vectors; `o, q` their unit-
normalized forms; `c` the IVF centroid; `P` the random orthogonal matrix; `x̄ ∈ C` a codebook vector;
`ō = P·x̄` the quantized (rotated) data vector; `x̄_b ∈ {0,1}^D` the stored bit string; `q' = P⁻¹q` the
inverse-rotated query.

### 1. Normalize onto the unit sphere (§3.1.1)

Distances are unbounded and data can sit anywhere, which defeats a fixed codebook, so RaBitQ first
normalizes every vector relative to its cluster centroid: `o = (oᵣ − c)/‖oᵣ − c‖`, and likewise
`q = (qᵣ − c)/‖qᵣ − c‖` at query time (§3.1.1). This reduces the squared-distance target to an inner
product of two unit vectors via the decomposition (Eq. 1–2):

```
‖oᵣ − qᵣ‖² = ‖oᵣ − c‖² + ‖qᵣ − c‖² − 2·‖oᵣ − c‖·‖qᵣ − c‖·⟨q, o⟩
             └ stored per-vector ┘   └ computed once per query ┘    └ the only term we estimate ┘
```

`‖oᵣ − c‖` is precomputed per data vector at index time; `‖qᵣ − c‖` is computed once per query per
cluster. Everything hard collapses to estimating one scalar, `⟨q, o⟩`.

### 2. The codebook and the random rotation (§3.1.2)

The base codebook is the `2^D` vertices of a scaled hypercube, `C := {+1/√D, −1/√D}^D` (Eq. 3) — unit
vectors that spread evenly over the sphere. A *deterministic* hypercube is excellent for some directions
and terrible for the axis-aligned ones (e.g. squared distance `2 − 2/√D` for `(1,0,…,0)`), so RaBitQ
injects randomness by applying a **random orthogonal matrix `P`** (a Johnson–Lindenstrauss transform):
the randomized codebook is `C_rand := {P·x̄ | x̄ ∈ C}` (Eq. 4). `P` is "uniformly sampled from all the
possible rotations of the space," which removes the codebook's preference for any specific direction
(§3.1.2). Critically, **the codebook is never materialized** — only `P` (or its action) is stored, and
the trick in step 3 avoids rotating `2^D` vectors.

### 3. Computing each data vector's code (§3.1.3)

For a unit vector `o`, its nearest codebook vector is the one maximizing `⟨o, P·x̄⟩`. Because inner
products are rotation-invariant, instead of rotating the whole codebook we **inverse-rotate the data
vector** and pick signs (Eq. 7–8):

```
⟨o, P·x̄⟩ = ⟨P⁻¹·o, x̄⟩
x̄_b[i] = 1 if (P⁻¹·o)[i] ≥ 0 else 0          # the stored D-bit string  (§3.1.3)
x̄[i]   = (2·x̄_b[i] − 1)/√D                    # reconstruct ±1/√D from the bit
ō = P·x̄                                        # the quantized data vector
```

So the data vector is rotated *into* the cube's frame, its signs are recorded as `D` bits, and `ō` is
the reconstructed quantized vector. **Index-time precompute per vector (Algorithm 1):** the bit string
`x̄_b`, the norm `‖oᵣ − c‖`, and the scalar `⟨ō, o⟩` (how well the code aligns with its own original —
the de-bias factor). Storage is `D` bits per vector plus those two floats, vs `32D` bits raw (§3.1.3) —
and RaBitQ's `D`-bit code is "only around a half of those of PQ and OPQ (i.e., `D` v.s. `2D`)" (§5.2.1).
This compactness is consistent with turbopuffer's stated **16–32× compression** for its binary pass
("binary quantization provides a 16-32x compression for data vectors", turbopuffer ANN v3 blog; the
blog names RaBitQ as the binary-quantization method but does not break down the per-vector layout).

### 4. The unbiased estimator (§3.2)

This is the heart of the method. Treating `ō` as `o` is biased; RaBitQ instead estimates `⟨q, o⟩` from
the quantities it *can* compute. By the geometric relationship (Lemma 3.1, Eq. 9–10) and the
concentration of `⟨ō, o⟩` (analyzed in §3.2.1), the estimator is (Eq. 11–13):

```
estimator = ⟨ō, q⟩ / ⟨ō, o⟩            with     E[ ⟨ō, q⟩ / ⟨ō, o⟩ ] = ⟨q, o⟩      (Eq. 13, Theorem 3.2)
            └ computed at query time ┘  └ precomputed at index time ┘
```

`⟨ō, o⟩` is the precomputed alignment scalar; `⟨ō, q⟩` is the only thing query-time computes, and step 5
reduces it to popcounts. Theorem 3.2 proves the estimator is **unbiased**, and the **error bound**
(Eq. 14–15) is:

```
| ⟨ō, q⟩/⟨ō, o⟩ − ⟨q, o⟩ |  =  O(1/√D)   with high probability        (Eq. 15)
```

The failure probability decays quadratic-exponentially in the parameter `ε₀` (Eq. 14); the paper fixes
`ε₀ = 1.9` in practice (§5.1, parameter setting). The bound is **sharp**: §3.2.2 cites the result that
for a `D`-bit code it is *impossible* in theory to beat `O(1/√D)`, so RaBitQ is asymptotically optimal,
and — unlike PQ/OPQ — needs **no per-dataset tuning**. Because the error is bounded, the rerank decision
can be principled: a candidate whose distance *lower bound* already exceeds the current K-th nearest can
be dropped without ever touching its raw vector (§4), rather than reranking a fixed, hand-tuned count.

### 5. The bitwise scan: why it is fast (§3.3)

`⟨ō, q⟩` reduces to `⟨x̄, q'⟩` where `q' = P⁻¹q` is the inverse-rotated query (Eq. 17). The query is
**scalar-quantized to `B_q`-bit unsigned integers** `q̄_u` (the paper uses **`B_q = 4`**, §5.1; randomized
rounding keeps this step unbiased, Eq. 18). Then the inner product decomposes over the query's bit-planes
(Eq. 21–22):

```
⟨x̄_b, q̄_u⟩  =  Σⱼ 2ʲ · ⟨x̄_b, q̄_u^(j)⟩  =  Σⱼ 2ʲ · popcount( x̄_b AND q̄_u^(j) )      (Eq. 22)
```

So one cluster scan is, per data vector, `B_q` rounds of **AND + popcount** over `D`-bit strings — the
operations "supported by virtually all platforms" and amenable to SIMD `VPOPCNT` (§3.3.2). The paper
reports this single-vector bitwise path runs **~3× faster than PQ/OPQ** (which need in-RAM lookup tables)
"while reaching the same accuracy" (§3.3.2, §5.2.1). Theorem 3.3 shows `B_q = Θ(log log D)` suffices to
keep the query-quantization error in the same `O(1/√D)` order as the estimator, so `B_q = 4` is enough.

> Note the asymmetry: **data vectors are 1 bit/dim, the query is `B_q` bits/dim.** Our RaBitQ-lite is
> symmetric (query is also reduced to sign bits), which is one reason it is only a heuristic.

## What our clone does today, and the gap

`internal/engine/vector.go` implements RaBitQ-lite end to end:

- `ResidualCode(v, centroid)` packs `sign(v[i] − centroid[i])` into `[]uint64`, one bit/dim
  (`vector.go:239`). **No rotation `P`**, **no normalization**, **no `‖oᵣ−c‖` or `⟨ō,o⟩` stored.**
- `Hamming`/`Agreement(a, b, dim)` return `dim − hamming(...)` (`vector.go:252`, `vector.go:266`) — the
  count of matching residual sign bits. This is the biased "treat the code as the vector" dot product the
  paper warns against (§1, §3.2), used purely as an ordering heuristic, not a distance.

The indexer wires this in `buildVectorIndex` (`internal/engine/indexer.go:112`): after `KMeans`, each
member is stored as a `ClusterEntry{ID, Vector, Code, Attrs}` (`types.go:190`) with
`Code = ResidualCode(points[i], centroids[c])` (`indexer.go:144`). The query path
`vectorCandidatesFromIndex` (`internal/engine/query.go:139`) computes `qCode := ResidualCode(query, clf.Centroid)`
per probed cluster, ranks members by `Agreement`, keeps the top `topK * shortlistMultiplier`
(`shortlistMultiplier = 4`, `query.go:27`), then reranks that shortlist with the exact metric
(`Distance`, `vector.go:54`).

**The gap, concretely:** RaBitQ-lite's prefilter has **no error bound**. A higher Hamming agreement only
loosely correlates with a smaller distance; there is no guarantee a true neighbor survives the shortlist,
which is why we paper over it with the `×4` headroom and an exact rerank of *everything* in the shortlist
rather than a provable lower-bound cutoff. [`../03-rabitq-quantization.md`](../03-rabitq-quantization.md)
already flags this and lists the four missing pieces (rotation `P`, `/⟨ō,o⟩` normalization, the `‖oᵣ−c‖`
decomposition, 4-bit query quantization).

## How a true-RaBitQ upgrade would hook into tpuf

The good news: the IVF/centroid plumbing, the two-stage "binary scan → rerank" control flow, and the
**epoch + manifest-CAS** publishing are already exactly where RaBitQ expects to sit (the paper applies
RaBitQ *on top of* IVF clusters, §4). The change is contained to the quantization math and the per-vector
stored fields. Sketch:

1. **Add a rotation per epoch.** Sample one orthogonal `P` at the start of `BuildIndex`
   (`indexer.go:62`). A readable stdlib construction: fill a `D×D` matrix with i.i.d. Gaussians and
   Gram–Schmidt / QR it to orthonormal columns (no new dependency — fits the "one external dep" rule in
   `CLAUDE.md`). `P` must be **stored in the epoch** so query-time uses the same rotation. Natural home:
   a new `Rotation [][]float32` field on `CentroidsFile` (`types.go:172`), written by
   `buildVectorIndex` (`indexer.go:112`) and read back in `vectorCandidatesFromIndex` (`query.go:139`).
   Because index objects are **write-once under `index/v{epoch}/`** (`indexer.go:21`, `putJSON` at
   `indexer.go:232`), a per-epoch `P` is immutable and cache-safe — no CAS implications beyond the single
   manifest swap that already makes the epoch live (`indexer.go:96`, rule 4).

2. **Enrich the stored code.** Replace the bit-only `ResidualCode` with a routine that, per member:
   normalizes the residual to `o`, computes `x̄_b = sign(P⁻¹o)`, and returns `x̄_b` **plus** the two
   precomputed scalars `‖oᵣ−c‖` and `⟨ō, o⟩`. That means extending `ClusterEntry` (`types.go:190`) with
   `ResidualNorm float64` and `OOAlign float64` (names illustrative) alongside the existing `Code`.

3. **Quantize the query, not just sign it.** In `vectorCandidatesFromIndex` (`query.go:139`), per probed
   cluster compute `q = normalize(query − centroid)`, `q' = P⁻¹q`, scalar-quantize `q'` to `B_q = 4`-bit
   `q̄_u` (Eq. 18), and store its bit-planes. Replace `Agreement` with the estimator:
   `⟨ō,q⟩ = Σⱼ 2ʲ·popcount(Code AND q̄_u^(j))` (Eq. 22), then `est⟨q,o⟩ = ⟨ō,q⟩/OOAlign`, then plug into
   the Eq. 2 decomposition using `ResidualNorm` and the per-query `‖qᵣ−c‖`. `bits.OnesCount64` (already
   imported in `vector.go`) is the popcount.

4. **Rerank by the error bound, not a fixed multiplier.** With a real distance estimate and the `O(1/√D)`
   bound, the shortlist cutoff in `query.go` can become "rerank a candidate only if its lower-bound
   distance ≤ current K-th best" (§4), letting `shortlistMultiplier` (`query.go:27`) go away. The exact
   rerank via `Distance` (`vector.go:54`) on `ClusterEntry.Vector` stays as-is.

Nothing in `storage/`, the WAL, or the manifest schema needs to change except adding fields to the
index-file structs — the rotation lives inside an epoch and ships atomically with it. The `Manifest`
(`types.go:29`) already carries `Metric`/`Dimension`; an optional `QuantVersion` field could let queries
refuse to read a lite epoch with full-RaBitQ code (defensive, not required since epochs are rebuilt
wholesale).

## What's genuinely hard / what to get right

- **Sampling a uniform orthogonal `P` correctly.** Gaussian-fill + QR is the standard recipe, but sign
  conventions in `R`'s diagonal can bias the result toward non-uniform rotations; the fix (flip column
  signs to make `R`'s diagonal positive) is easy to get subtly wrong. This is real linear algebra in
  stdlib `float32`/`float64` — the most error-prone new code.
- **`O(D²)` rotation cost.** Applying a dense `P⁻¹` to each query per cluster is `O(D²)`; at the small
  `D` and `N` this clone targets that is fine, but it is why production systems often use **structured**
  fast transforms (e.g. randomized Hadamard) instead of a dense matrix. Worth a comment, not worth
  building here. (turbopuffer does not publicly confirm which transform it uses; inferred from the RaBitQ
  paper's JLT/`P` construction, §3.1.2.)
- **Floating-point reproducibility.** `KMeans` is deliberately deterministic for reproducible tests
  (`vector.go:107`). A randomly sampled `P` breaks that unless the rotation is seeded from a fixed source
  the same way `kMeans` seeds `rand.NewSource(1)` (`vector.go:108`). Seed it, or tests over the vector
  index become flaky.
- **Storing `P` is `O(D²)` per epoch.** Cheap at small `D`, but it lands in `centroids.json` and the DRAM
  cache; keep it on `CentroidsFile`, not duplicated into every `cluster-{i}.json`.
- **Don't half-upgrade.** The paper stresses RaBitQ's components "are an integral whole"; ablating any
  one (the rotation, the de-bias `/⟨ō,o⟩`, the bound-driven rerank) "would cause the loss of the
  theoretical guarantee, the method becomes heuristic and the performance is no more theoretically
  predictable" (§5 intro, referencing §4). Either keep RaBitQ-lite honestly
  labeled as a heuristic, or implement all four pieces — a rotation without the unbiased estimator buys
  nothing.

## Sources

- **RaBitQ paper (read locally):** Jianyang Gao & Cheng Long, *RaBitQ: Quantizing High-Dimensional Vectors
  with a Theoretical Error Bound for Approximate Nearest Neighbor Search*, **SIGMOD '24** / Proc. ACM
  Manag. Data, Vol. 2, No. 3 (SIGMOD), Article 167. Local file
  [`../papers/rabitq-sigmod24.pdf`](../papers/rabitq-sigmod24.pdf). Specific claims:
  bias of PQ-style estimation and MSong >50% error — §1; normalization & distance decomposition — §3.1.1,
  Eq. 1–2; hypercube codebook — §3.1.2 Eq. 3; random orthogonal `P` / randomized codebook — §3.1.2 Eq. 4;
  inverse-rotate trick & bit code — §3.1.3 Eq. 5–8; unbiased estimator and Theorem 3.2 — §3.2 Eq. 11–13;
  `O(1/√D)` error bound and sharpness — §3.2.2 Eq. 14–15; query quantization `B_q` & randomized rounding —
  §3.3.1 Eq. 18; Theorem 3.3 (`B_q = Θ(log log D)`); AND+popcount decomposition — §3.3.2 Eq. 21–22;
  Algorithm 1/2 and rerank-by-bound — §3.4, §4; "integral whole" / ablation language — §5 intro
  (referencing §4); `ε₀ = 1.9`, `B_q = 4` — §5.1.
  DOI: <https://dl.acm.org/doi/10.1145/3654970>. Authors' code: <https://github.com/gaoj0017/RaBitQ>
  (moved to <https://github.com/VectorDB-NTU/RaBitQ-Library>). See also
  [`../papers/SOURCES.md`](../papers/SOURCES.md).
- **turbopuffer ANN v3 blog** (uses RaBitQ; "16–32× compression for data vectors"; "less than 1% of data
  vectors in the narrowed search space need to be reranked"): <https://turbopuffer.com/blog/ann-v3>
  (fetched). turbopuffer does **not** publicly confirm its rotation type, `B_q`, or internal storage
  layout — those specifics above are attributed to the RaBitQ paper, not to turbopuffer.
- **Clone code (read):** `internal/engine/vector.go` (`CosineDistance`/`EuclideanDistance`/`Distance`,
  `Normalize`, `KMeans`, `ResidualCode`, `Hamming`, `Agreement`); `internal/engine/indexer.go`
  (`BuildIndex`, `buildVectorIndex`, `indexPrefix`, `putJSON`); `internal/engine/query.go`
  (`vectorCandidatesFromIndex`, `nearestClusters`, `shortlistMultiplier`); `internal/engine/types.go`
  (`Manifest`, `CentroidsFile`, `ClusterFile`, `ClusterEntry`).
- **Clone docs:** [`../03-rabitq-quantization.md`](../03-rabitq-quantization.md) (RaBitQ-lite vs full),
  [`../05-clone-mapping.md`](../05-clone-mapping.md) (the non-goal),
  [`../01-architecture.md`](../01-architecture.md) (epoch + manifest-CAS swap).

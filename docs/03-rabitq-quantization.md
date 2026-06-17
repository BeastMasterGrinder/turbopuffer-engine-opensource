# 03 — RaBitQ: binary quantization for scoring inside a cluster

Once a query has fetched the candidate clusters (see [`02`](./02-spfresh-spann-index.md)), it still has
to **score** every candidate vector. Doing that in full `float32` precision is expensive and bandwidth-
heavy. turbopuffer uses **RaBitQ** (SIGMOD '24, `papers/rabitq-sigmod24.pdf`) to do a cheap **binary
first pass**, then rerank only a sliver at full precision.

> turbopuffer's published figures: binary quantization gives **16–32× compression**, and **"less than
> 1% of data vectors in the narrowed search space need to be reranked"** to keep recall. (The popular
> "~3000 → ~50" framing is *not* turbopuffer's wording.)

## The idea: 1 bit per dimension

A D-dimensional vector becomes a **D-bit code** (one bit per dimension). Distance estimates then reduce
to **bitwise AND + popcount** — operations a CPU does in a single instruction (and that AVX-512
`VPOPCNTDQ` does 512 dims at a time). That's why it's fast.

## How RaBitQ builds the code (the real method)

Per cluster with centroid `c`, for each member vector `oᵣ`:

1. **Center + normalize the residual** onto the unit sphere: `o = (oᵣ − c) / ‖oᵣ − c‖`.
   (`‖oᵣ − c‖` is stored per vector — it's needed to reconstruct the real distance.)
2. **Random rotation:** apply one shared random orthogonal matrix `P` (a Johnson–Lindenstrauss
   transform). Inner products are rotation-invariant, so instead of rotating a giant codebook you
   inverse-rotate the vector and take **sign bits**:
   ```
   code[i] = 1 if (P⁻¹·o)[i] ≥ 0 else 0      →  a D-bit string
   ```
3. **Store per vector:** the D-bit code, `‖oᵣ − c‖`, and `⟨ō, o⟩` (how well the code aligns with its own
   original — used to de-bias the estimate). Code size: D bits, padded to a multiple of 64.

## Why a naive sign-bit dot product isn't enough (and RaBitQ's fix)

Treating the quantized vector as the real one (plain binary hashing) gives a **biased** estimate. RaBitQ
uses an **unbiased** estimator:

```
estimate of ⟨o,q⟩  =  ⟨ō, q⟩ / ⟨ō, o⟩          (ō = the quantized/reconstructed unit vector)
```

`⟨ō, o⟩` is precomputed at index time. `⟨ō, q⟩` is computed at query time — and it reduces to popcounts:
inverse-rotate the query once per cluster (`q' = P⁻¹q`), optionally scalar-quantize `q'` to **4-bit**
integers, then for each bit-plane `j` of the query, `⟨code, q'⟩ = Σⱼ 2ʲ · popcount(code AND q'_plane_j)`.
Plug the estimate into the distance decomposition:

```
‖oᵣ − qᵣ‖²  =  ‖oᵣ − c‖²  +  ‖qᵣ − c‖²  −  2·‖oᵣ − c‖·‖qᵣ − c‖·⟨o,q⟩
                └ stored ┘     └ per-query ┘                  └ estimated from bits ┘
```

**Error bound:** `|estimate − ⟨o,q⟩| = O(1/√D)` with high probability — proven asymptotically optimal for
a D-bit code, and (unlike PQ/OPQ) it needs **no per-dataset tuning**.

## The two-stage search

1. **Binary scan (cheap):** estimate every candidate's distance via popcount. Scans the whole probed
   candidate set.
2. **Full-precision rerank (a sliver):** take the top shortlist and compute **exact** L2/cosine on the
   raw `float32` vectors. RaBitQ's error bound lets it rerank by a *provable lower bound* rather than a
   fixed count; in practice ~1,000 rerank candidates give near-perfect recall at K=100 (turbopuffer:
   **<1%** of the narrowed set).

## What our clone implements (RaBitQ-lite)

We keep the **structure** (binary prefilter → exact rerank) but drop the rotation and the unbiased-
estimator math, which is the right altitude for a teaching clone:

1. **Index time, per cluster:** centroid `c`; for each member, `residual = v − c`, store the **sign bit
   of each residual dimension**, packed into `[]uint64`. Also store the raw `float32` vector.
2. **Query time, per probed cluster:** compute the query's residual sign bits the same way.
3. **Prefilter (popcount agreement):** `agreement = D − hamming(query_bits, doc_bits)` — more matching
   sign bits ≈ more aligned direction. Keep the top **`M`** candidates by agreement.
4. **Rerank:** compute the **exact** metric (cosine/L2) on the raw vectors for those `M`, return top-K.

This demonstrates *why* binary codes make object-storage-scale vector search affordable, with code you
can read in one sitting. The doc notes exactly what to add (rotation `P`, `/⟨ō,o⟩` normalization, the
`‖oᵣ−c‖` decomposition, 4-bit query quantization) to reach full RaBitQ fidelity.

---

Next: [`04-bm25-fulltext.md`](./04-bm25-fulltext.md) — full-text search.

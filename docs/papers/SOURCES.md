# Source papers & references (stored locally)

The three primary papers are committed in this folder so the design is grounded in the actual
algorithms, not blog paraphrases. All three were read from these exact PDFs.

| File | Paper | Venue | What it gives us |
|------|-------|-------|------------------|
| [`spfresh-sosp23.pdf`](./spfresh-sosp23.pdf) | *SPFresh: Incremental In-Place Update for Billion-Scale Vector Search* — Xu et al. | SOSP '23 | The **LIRE** update protocol (split/merge/reassign + the NPA rule) and the centroid-index storage engine. turbopuffer's index is "derived from SPFresh." |
| [`spann-neurips21.pdf`](./spann-neurips21.pdf) | *SPANN: Highly-efficient Billion-scale ANN Search* — Chen et al. | NeurIPS '21 | The base **centroid + on-disk posting-list (IVF)** design: in-memory centroid index, balanced clustering, boundary replication, query-aware dynamic pruning. |
| [`rabitq-sigmod24.pdf`](./rabitq-sigmod24.pdf) | *RaBitQ: Quantizing High-Dimensional Vectors with a Theoretical Error Bound* — Gao & Long | SIGMOD '24 | **1-bit-per-dimension quantization** + the unbiased XOR/popcount inner-product estimator used to score candidates inside a cluster. |

## Canonical URLs

- SPFresh — arXiv: <https://arxiv.org/abs/2410.14452> · ACM DOI: 10.1145/3600006.3613166
- SPANN — arXiv: <https://arxiv.org/abs/2111.08566> · MSR PDF: <https://www.microsoft.com/en-us/research/wp-content/uploads/2021/11/SPANN_finalversion1.pdf> · code (SPTAG): <https://github.com/microsoft/SPTAG>
- RaBitQ — ACM DOI: 10.1145/3654970 · technical report: <https://github.com/gaoj0017/RaBitQ> (now <https://github.com/VectorDB-NTU/RaBitQ-Library>)

## turbopuffer's own pages (used for the architecture docs)

- Architecture — <https://turbopuffer.com/docs/architecture>
- Concepts — <https://turbopuffer.com/docs/concepts>
- ANN v3 blog (their current centroid index) — <https://turbopuffer.com/blog/ann-v3>
- Founding blog — <https://turbopuffer.com/blog/turbopuffer>
- Native filtering — <https://turbopuffer.com/blog/native-filtering>
- Guarantees — <https://turbopuffer.com/docs/guarantees>

## ⚠️ Three things commonly mis-stated about turbopuffer (we do NOT mirror these as fact)

1. **Filenames `centroids.bin` / `clusters-N.bin`** — turbopuffer does **not** publish these literal
   names. They describe *conceptual* components (centroid index + per-cluster blobs fetched by offset).
2. **"~3000 candidates narrowed to ~50"** inside a cluster — not in turbopuffer's own words. What they
   *do* publish: **"less than 1% of data vectors in the narrowed search space need to be reranked."**
3. **`128 MiB` as the unindexed-WAL reindex threshold** — wrong. `100³ × 128 bytes = 128 MiB` is the
   size of the **upper tree levels held in L3 cache**. The real WAL-reindex threshold is just
   "a configurable parameter."

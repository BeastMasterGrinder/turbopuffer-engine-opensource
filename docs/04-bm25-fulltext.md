# 04 — Full-text search (inverted index + BM25)

turbopuffer serves vector **and** full-text search from the **same `Query` endpoint** — the mode is
chosen by `RankBy`. A vector → ANN; a text query → **BM25** over an **inverted index**. This doc covers
how full-text works and how our clone implements it.

## The inverted index

An inverted index maps **each term → the list of documents that contain it** (the "postings"), plus the
term frequency in each. It's the data structure that makes "which docs contain *walrus*?" an O(1) lookup
instead of a full scan.

```
"quick"  → [ {doc: A, tf: 2}, {doc: C, tf: 1} ]
"walrus" → [ {doc: A, tf: 1}, {doc: B, tf: 3} ]
```

Building it: **tokenize** each document's text field (lowercase, split on non-alphanumerics, optionally
drop stopwords / stem), then for every token append a posting. Alongside it we store per-document
**length** (token count), the **average document length** (`avgdl`), and the **total document count**
`N` — BM25 needs all three.

## BM25 — the ranking formula

BM25 ("Best Matching 25") scores how well a document `D` matches a query `Q`:

```
score(D, Q) = Σ_{t ∈ Q}  IDF(t) ·  ( tf(t,D) · (k1 + 1) )
                                    ─────────────────────────────────────────
                                    tf(t,D) + k1 · (1 − b + b · |D| / avgdl)

IDF(t) = ln( 1 + (N − df(t) + 0.5) / (df(t) + 0.5) )
```

- `tf(t,D)` — how often term `t` appears in `D`.
- `df(t)` — how many documents contain `t`; `IDF` down-weights common terms (a word in every doc tells
  you nothing).
- `|D| / avgdl` — **length normalization** so long documents aren't unfairly favored.
- **`k1` (default 1.2)** — term-frequency **saturation**: the 10th occurrence of a word adds far less
  than the 1st.
- **`b` (default 0.75)** — how strongly to apply length normalization (`b=0` → none, `b=1` → full).

BM25 improves on plain TF-IDF in exactly those two ways (saturation + length normalization) and is what
Elasticsearch, Lucene, and Bleve use by default.

## Unindexed writes are still searchable

Just like the vector path, a brand-new document that hasn't been folded into the inverted index yet is
**not invisible** — the query **also exhaustively scans the unindexed WAL tail**, tokenizes those docs
on the fly, scores them against the same global stats, and unions the results. This is what makes
"writes appear in search results immediately" true for full-text too.

## Our clone

We **hand-write** the inverted index + BM25 (~80 lines) so the mechanics are visible:

- **Tokenizer:** lowercase, split on non-alphanumeric runs (a simple, dependency-free analyzer).
- **`bm25.json`** stores: `term → postings[{id, tf}]`, `docLen{id → len}`, `avgdl`, `N`.
- **Scoring:** the formula above with `k1 = 1.2`, `b = 0.75`.
- **WAL tail:** unindexed docs are tokenized and scored inline and merged in.
- **Filters** (metadata equality / AND / OR) apply to full-text results too.

> Production alternative: **[blevesearch/bleve](https://github.com/blevesearch/bleve)** is a pure-Go
> full-text engine with BM25, analyzers, fuzzy matching, and faceting built in — the natural drop-in if
> you wanted this to be real rather than educational. We hand-roll it precisely because the goal is to
> *see* how BM25 works.

---

Next: [`05-clone-mapping.md`](./05-clone-mapping.md) — how every concept maps to our Go code.

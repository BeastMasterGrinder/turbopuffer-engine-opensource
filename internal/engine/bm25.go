package engine

import (
	"math"
	"sort"
	"strings"
	"unicode"
)

// BM25 parameters. k1 controls term-frequency saturation (the 10th occurrence
// of a term adds far less than the 1st); b controls how strongly document
// length normalizes the score (0 = none, 1 = full). These are the standard
// Lucene/Elasticsearch defaults.
const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

// Tokenize splits text into lowercase terms, breaking on every run of
// non-alphanumeric characters. It is the engine's sole, dependency-free
// analyzer: no stemming or stopword removal. The same function tokenizes both
// indexed documents and the query, so the two always agree on term boundaries.
//
// Examples: "Quick, brown FOX!" -> ["quick", "brown", "fox"]; "" -> nil.
func Tokenize(text string) []string {
	terms := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !isAlphanumeric(r)
	})
	// FieldsFunc returns a non-nil empty slice when nothing matches; normalize
	// the no-term case to nil so callers (and tests) get a single, clear "no
	// terms" value rather than distinguishing nil from []string{}.
	if len(terms) == 0 {
		return nil
	}
	return terms
}

// isAlphanumeric reports whether r is a letter or digit. Any other rune
// (punctuation, whitespace, symbols) is treated as a token separator. Unicode
// letters and digits are kept so the analyzer is not silently English-only.
func isAlphanumeric(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

// BuildBM25 constructs the inverted index and global statistics over a set of
// documents, keyed by document id with the value being that document's text
// field. The returned BM25File holds the term -> postings index, per-document
// length, the average document length, and the document count N — everything
// Score and ScoreDoc need. An empty input yields an empty, usable index
// (N == 0, AvgDL == 0).
func BuildBM25(docs map[string]string) BM25File {
	idx := BM25File{
		N:      len(docs),
		DocLen: make(map[string]int, len(docs)),
		Index:  make(map[string][]Posting),
	}

	var totalLen int
	for id, text := range docs {
		terms := Tokenize(text)
		idx.DocLen[id] = len(terms)
		totalLen += len(terms)

		// Count term frequencies within this document before appending
		// postings, so each (term, doc) pair contributes exactly one posting.
		tf := make(map[string]int, len(terms))
		for _, term := range terms {
			tf[term]++
		}
		for term, freq := range tf {
			idx.Index[term] = append(idx.Index[term], Posting{ID: id, TF: freq})
		}
	}

	if idx.N > 0 {
		idx.AvgDL = float64(totalLen) / float64(idx.N)
	}

	// Sort each posting list by id for deterministic output (the index is
	// serialized to JSON and compared in tests).
	for term := range idx.Index {
		postings := idx.Index[term]
		sort.Slice(postings, func(i, j int) bool { return postings[i].ID < postings[j].ID })
	}

	return idx
}

// Score ranks every indexed document against the query terms, returning a map
// of document id -> BM25 score for the documents that match at least one query
// term. Documents that match no query term are absent (a zero score is not
// meaningful and would bloat the result). The caller sorts and truncates to
// TopK.
func Score(idx BM25File, queryTerms []string) map[string]float64 {
	scores := make(map[string]float64)
	for _, term := range queryTerms {
		postings, ok := idx.Index[term]
		if !ok {
			continue
		}
		idf := bm25IDF(idx.N, len(postings))
		for _, p := range postings {
			scores[p.ID] += bm25TermScore(idf, p.TF, idx.DocLen[p.ID], idx.AvgDL)
		}
	}
	return scores
}

// ScoreDoc scores a single, possibly unindexed document's text against the
// query using the index's global statistics (N, df, AvgDL). This is what the
// WAL-tail scan uses to rank freshly upserted documents that have not yet been
// folded into the index, so their scores are comparable to indexed hits. A
// document matching no query term scores 0.
func ScoreDoc(idx BM25File, docText string, queryTerms []string) float64 {
	tokens := Tokenize(docText)
	docLen := len(tokens)

	tf := make(map[string]int, len(tokens))
	for _, t := range tokens {
		tf[t]++
	}

	var score float64
	for _, term := range queryTerms {
		freq, ok := tf[term]
		if !ok {
			continue
		}
		idf := bm25IDF(idx.N, len(idx.Index[term]))
		score += bm25TermScore(idf, freq, docLen, idx.AvgDL)
	}
	return score
}

// bm25IDF is the BM25 inverse document frequency for a term appearing in df of
// N documents: ln(1 + (N - df + 0.5) / (df + 0.5)). The leading 1 keeps the IDF
// non-negative even for terms in most documents.
func bm25IDF(n, df int) float64 {
	return math.Log(1 + (float64(n)-float64(df)+0.5)/(float64(df)+0.5))
}

// bm25TermScore is the per-term contribution to a document's BM25 score given
// the term's IDF, its frequency tf in the document, the document length, and
// the corpus average document length. When avgDL is 0 (empty index) the
// length-normalization term degrades to k1*(1-b), which is still finite.
func bm25TermScore(idf float64, tf, docLen int, avgDL float64) float64 {
	var lenRatio float64
	if avgDL > 0 {
		lenRatio = float64(docLen) / avgDL
	}
	norm := bm25K1 * (1 - bm25B + bm25B*lenRatio)
	return idf * (float64(tf) * (bm25K1 + 1)) / (float64(tf) + norm)
}

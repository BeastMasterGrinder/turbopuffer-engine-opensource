package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/engine"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// newSeededNode builds a node whose shared object store already has one indexed
// namespace ("acme"). The node's own cache differs from the one used to seed —
// proving any node reading the same backend can serve the data.
func newSeededNode(t *testing.T) *nodeServer {
	t.Helper()
	ctx := context.Background()
	mem := storage.New()

	setup := engine.Open(cache.New(mem), "acme")
	if err := setup.Create(ctx, engine.NamespaceConfig{Dimension: 4, Metric: "cosine", TextField: "body"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	docs := []engine.Document{
		{ID: "d1", Vector: []float32{0.1, 0.2, 0.3, 0.4}, Attributes: map[string]any{"body": "quick brown walrus"}},
		{ID: "d2", Vector: []float32{0.2, 0.1, 0.4, 0.3}, Attributes: map[string]any{"body": "lazy dog sleeps"}},
		{ID: "d3", Vector: []float32{0.9, 0.1, 0.0, 0.0}, Attributes: map[string]any{"body": "object storage truth"}},
	}
	if err := setup.Upsert(ctx, docs); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := setup.Index(ctx); err != nil {
		t.Fatalf("index: %v", err)
	}
	return &nodeServer{id: "node-test", store: cache.New(mem)}
}

func TestNodeQueryVector(t *testing.T) {
	srv := newSeededNode(t)
	rec := httptest.NewRecorder()
	body := `{"vector":[0.1,0.2,0.3,0.4],"topK":2}`
	srv.routes().ServeHTTP(rec, httptest.NewRequest("POST", "/v1/namespaces/acme/query", strings.NewReader(body)))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Tpuf-Node"); got != "node-test" {
		t.Errorf("X-Tpuf-Node = %q, want node-test", got)
	}
	var resp queryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Node != "node-test" || resp.Namespace != "acme" {
		t.Errorf("resp node/ns = %q/%q, want node-test/acme", resp.Node, resp.Namespace)
	}
	if resp.Count == 0 {
		t.Errorf("got 0 results, want some")
	}
}

func TestNodeQueryBM25(t *testing.T) {
	srv := newSeededNode(t)
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, httptest.NewRequest("POST", "/v1/namespaces/acme/query", strings.NewReader(`{"bm25":"walrus","topK":3}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	var resp queryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count == 0 {
		t.Errorf("bm25 'walrus' returned 0 results, want a match")
	}
}

func TestNodeQueryRequiresRankMode(t *testing.T) {
	srv := newSeededNode(t)
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, httptest.NewRequest("POST", "/v1/namespaces/acme/query", strings.NewReader(`{"topK":3}`)))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for a query with neither vector nor bm25", rec.Code)
	}
}

func TestNodeInfoAndUnknownNamespace(t *testing.T) {
	srv := newSeededNode(t)

	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, httptest.NewRequest("GET", "/v1/namespaces/acme/info", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("info status = %d, want 200", rec.Code)
	}
	var m engine.Manifest
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if m.Dimension != 4 {
		t.Errorf("manifest dimension = %d, want 4", m.Dimension)
	}

	rec = httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, httptest.NewRequest("GET", "/v1/namespaces/ghost/info", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown-namespace info status = %d, want 404", rec.Code)
	}
}

func TestNodeStats(t *testing.T) {
	srv := newSeededNode(t)
	// A query warms the cache, so stats should show some activity afterward.
	srv.routes().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/namespaces/acme/query", strings.NewReader(`{"vector":[0.1,0.2,0.3,0.4],"topK":2}`)))

	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, httptest.NewRequest("GET", "/stats", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("stats status = %d, want 200", rec.Code)
	}
	var got struct {
		Node  string `json:"node"`
		Cache struct {
			Hits   uint64 `json:"hits"`
			Misses uint64 `json:"misses"`
		} `json:"cache"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if got.Node != "node-test" {
		t.Errorf("stats node = %q, want node-test", got.Node)
	}
	if got.Cache.Hits+got.Cache.Misses == 0 {
		t.Errorf("expected cache activity after a query, got none")
	}
}

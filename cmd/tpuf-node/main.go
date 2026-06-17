// Command tpuf-node is a stateless HTTP query server over the tpuf engine. It
// exists to demonstrate turbopuffer's routing model: many identical nodes share
// one object store (MinIO) as the source of truth, so ANY node can serve ANY
// namespace correctly. Each node keeps its OWN in-process DRAM cache, so routing
// a given namespace to the same node (which the nginx consistent-hash load
// balancer does) keeps that node's cache warm — a locality optimization, not a
// correctness requirement.
//
// Endpoints:
//
//	POST /v1/namespaces/{ns}/query   body: {"vector":[...]} or {"bm25":"..."}, topK, nProbe, filter
//	GET  /v1/namespaces/{ns}/info    the namespace manifest
//	GET  /stats                      this node's id + cache hit/miss/eviction counters
//	GET  /healthz                    liveness
//
// Every response carries an X-Tpuf-Node header naming the node that served it, so
// the load-balancer demo can show which node a namespace landed on.
//
// Config (env): NODE_ID, PORT (default 8080), TPUF_BACKEND (s3|memory),
// TPUF_S3_ENDPOINT/TPUF_S3_ACCESS_KEY/TPUF_S3_SECRET_KEY/TPUF_BUCKET,
// TPUF_CACHE_OBJECTS (0 = unbounded).
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/engine"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tpuf-node:", err)
		os.Exit(1)
	}
}

func run() error {
	backend, err := newBackend()
	if err != nil {
		return err
	}
	srv := &nodeServer{
		id:    envOr("NODE_ID", "node"),
		store: cache.NewWithCapacity(backend, envInt("TPUF_CACHE_OBJECTS", 0)),
	}
	addr := ":" + envOr("PORT", "8080")
	log.Printf("tpuf-node %s listening on %s", srv.id, addr)
	return http.ListenAndServe(addr, srv.routes())
}

// nodeServer holds the per-node DRAM cache over the shared object store.
type nodeServer struct {
	id    string
	store *cache.Store
}

func (s *nodeServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("POST /v1/namespaces/{ns}/query", s.handleQuery)
	mux.HandleFunc("GET /v1/namespaces/{ns}/info", s.handleInfo)
	return mux
}

// queryRequest is the JSON body of a query. Exactly one of Vector or BM25 is set.
type queryRequest struct {
	Vector []float32      `json:"vector,omitempty"`
	BM25   string         `json:"bm25,omitempty"`
	TopK   int            `json:"topK,omitempty"`
	NProbe int            `json:"nProbe,omitempty"`
	Filter *engine.Filter `json:"filter,omitempty"`
}

type queryResponse struct {
	Node      string               `json:"node"`
	Namespace string               `json:"namespace"`
	Count     int                  `json:"count"`
	Results   []engine.QueryResult `json:"results"`
}

func (s *nodeServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Tpuf-Node", s.id)
	ns := r.PathValue("ns")

	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decoding request: %v", err), http.StatusBadRequest)
		return
	}
	params := engine.QueryParams{TopK: req.TopK, NProbe: req.NProbe}
	switch {
	case len(req.Vector) > 0:
		params.RankBy = engine.RankBy{Vector: req.Vector}
	case req.BM25 != "":
		params.RankBy = engine.RankBy{Text: req.BM25}
	default:
		http.Error(w, "request must set either \"vector\" or \"bm25\"", http.StatusBadRequest)
		return
	}
	if req.Filter != nil {
		params.Filter = *req.Filter
	}

	results, err := engine.Open(s.store, ns).Query(r.Context(), params)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, queryResponse{Node: s.id, Namespace: ns, Count: len(results), Results: results})
}

func (s *nodeServer) handleInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Tpuf-Node", s.id)
	ns := r.PathValue("ns")
	m, err := engine.Open(s.store, ns).Info(r.Context())
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, m)
}

func (s *nodeServer) handleStats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("X-Tpuf-Node", s.id)
	c := s.store.Stats()
	writeJSON(w, map[string]any{
		"node": s.id,
		"cache": map[string]any{
			"hits": c.Hits, "misses": c.Misses, "evictions": c.Evictions, "hitRate": c.HitRate(),
		},
	})
}

// writeEngineError maps a missing namespace to 404 and everything else to 500.
func writeEngineError(w http.ResponseWriter, err error) {
	if errors.Is(err, storage.ErrNotFound) {
		http.Error(w, "namespace not found", http.StatusNotFound)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		log.Printf("encoding response: %v", err)
	}
}

// newBackend builds the object store from TPUF_BACKEND (default s3).
func newBackend() (storage.ObjectStore, error) {
	switch backend := envOr("TPUF_BACKEND", "s3"); backend {
	case "memory":
		return storage.New(), nil
	case "s3":
		store, err := storage.NewS3StoreFromEnv()
		if err != nil {
			return nil, fmt.Errorf("connecting to s3 backend: %w", err)
		}
		return store, nil
	default:
		return nil, fmt.Errorf("unknown TPUF_BACKEND %q (want \"s3\" or \"memory\")", backend)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

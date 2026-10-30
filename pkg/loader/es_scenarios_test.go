package loader

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
)

// This file holds the shared Elasticsearch lifecycle scenarios for loader
// tests. Each scenario registers only the routes it expects on an esstub.Stub,
// so a request outside the scenario fails the test instead of being absorbed
// by a permissive fake. Specific assertions stay in the tests themselves.

const (
	ackBody     = `{"acknowledged":true}`
	nodesBody   = `{"nodes":{"node1":{}}}`
	refreshBody = `{"_shards":{"total":1,"successful":1,"failed":0}}`
)

// requestLabels renders the stub's request trace as "METHOD /path" strings in
// arrival order.
func requestLabels(stub *esstub.Stub) []string {
	requests := stub.Requests()
	labels := make([]string, len(requests))
	for i, r := range requests {
		labels[i] = r.Method + " " + r.Path
	}
	return labels
}

// opRecorder records operation labels (such as "bulk" or "transform.put") in
// arrival order. It is the single recording implementation behind every
// lifecycle scenario; operations hands out copies, never the live slice.
type opRecorder struct {
	mu  sync.Mutex
	ops []string
}

func (r *opRecorder) record(op string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, op)
}

// operations returns a copy of the recorded operation labels.
func (r *opRecorder) operations() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.ops)
}

// recorded wraps handler so op is recorded before it runs. The recorder lock
// is released before handler is invoked.
func (r *opRecorder) recorded(op string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		r.record(op)
		handler(w, req)
	}
}

// existsReply answers an existence HEAD request with 200 or 404.
func existsReply(w http.ResponseWriter, exists bool) {
	if exists {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

// bulkOKBody is a successful single-document bulk response for index.
func bulkOKBody(index string) string {
	return fmt.Sprintf(`{"errors":false,"items":[{"index":{"_index":%q,"_id":"1","status":201}}]}`, index)
}

// registerTransformRoutes serves the get/stop/put/start requests of one
// transform id. The stop reply is the not-found a first-time transform gets.
// put may be nil for a plain acknowledgement.
func registerTransformRoutes(stub *esstub.Stub, rec *opRecorder, id string, put http.HandlerFunc) {
	if put == nil {
		put = esstub.JSON(http.StatusOK, ackBody)
	}
	stub.Handle("GET /_transform/"+id, rec.recorded("transform.get", esstub.JSON(http.StatusOK, `{"count":0,"transforms":[]}`)))
	stub.Handle("POST /_transform/"+id+"/_stop", rec.recorded("transform.stop", esstub.JSON(http.StatusNotFound, `{"error":{"type":"resource_not_found_exception"}}`)))
	stub.Handle("PUT /_transform/"+id, rec.recorded("transform.put", put))
	stub.Handle("POST /_transform/"+id+"/_start", rec.recorded("transform.start", esstub.JSON(http.StatusOK, ackBody)))
}

// lifecycleConfig configures newLifecycleStub. Only the index name is
// required; every other field selects an optional part of the cluster state.
type lifecycleConfig struct {
	index         string   // logical index/alias name; concrete generations are <index>-<suffix>
	indexExists   bool     // HEAD /<index> answers 200 before any create
	aliasTargets  []string // non-empty: GET /_alias/<index> resolves to these indices
	liveMappings  string   // response body for GET /<index>/_mapping
	mappingStatus int      // status for GET /<index>/_mapping; 0 means 200
	transforms    []string // transform ids whose get/stop/put/start routes are served
	policies      []string // enrich policies listed by GET /_enrich/policy and executable
}

// lifecycleStub is the shared operation-recording Elasticsearch stub for Run
// tests that create, flush, alias or load an index. operations lists the order
// of recorded calls; createdIndices and mappingFetchTargets name the concrete
// index of each create and mapping fetch, so alias tests can prove the
// timestamped index was checked.
type lifecycleStub struct {
	*esstub.Stub
	opRecorder

	url string

	mu             sync.Mutex
	present        map[string]bool
	aliasUpdated   bool
	created        []string
	mappingTargets []string
}

// createdIndices returns a copy of the index names created through PUT /<index>.
func (s *lifecycleStub) createdIndices() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.created)
}

// mappingFetchTargets returns a copy of the index names whose _mapping was fetched.
func (s *lifecycleStub) mappingFetchTargets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.mappingTargets)
}

// newLifecycleStub registers the lifecycle routes for cfg.index. Requests for
// any other index, method or path are unexpected routes and fail the test.
func newLifecycleStub(t *testing.T, cfg lifecycleConfig) *lifecycleStub {
	t.Helper()

	s := &lifecycleStub{
		Stub:    esstub.New(t),
		present: map[string]bool{cfg.index: cfg.indexExists},
	}
	s.url = s.URL()
	stub := s.Stub

	// ownsIndex reports whether name is the logical index or one of its
	// concrete generations. Anything else is a request the scenario does not
	// expect and is reported, not answered.
	ownsIndex := func(w http.ResponseWriter, r *http.Request, name string) bool {
		if name == cfg.index || strings.HasPrefix(name, cfg.index+"-") {
			return true
		}
		t.Errorf("unexpected request for index %q: %s %s", name, r.Method, r.URL.RequestURI())
		w.WriteHeader(http.StatusInternalServerError)
		return false
	}

	// /{name} is registered without a method: a method-qualified wildcard would
	// conflict with the fixed GET /_nodes style routes below.
	stub.Handle("/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if !ownsIndex(w, r, name) {
			return
		}
		switch r.Method {
		case http.MethodHead:
			s.record("index.head")
			s.mu.Lock()
			exists := s.present[name]
			s.mu.Unlock()
			existsReply(w, exists)
		case http.MethodPut:
			s.record("index.create")
			s.mu.Lock()
			s.present[name] = true
			s.created = append(s.created, name)
			s.mu.Unlock()
			_, _ = w.Write([]byte(ackBody))
		case http.MethodDelete:
			s.record("index.delete")
			s.mu.Lock()
			s.present[name] = false
			s.mu.Unlock()
			_, _ = w.Write([]byte(ackBody))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	stub.Handle("GET /_alias/"+cfg.index, func(w http.ResponseWriter, _ *http.Request) {
		s.record("alias.get")
		if len(cfg.aliasTargets) == 0 {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"type":"aliases_not_found_exception"},"status":404}`))
			return
		}
		targets := make(map[string]any, len(cfg.aliasTargets))
		for _, target := range cfg.aliasTargets {
			targets[target] = map[string]any{"aliases": map[string]any{cfg.index: map[string]any{}}}
		}
		body, err := json.Marshal(targets)
		if err != nil {
			t.Errorf("marshal alias targets: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(body)
	})
	stub.Handle("GET /{name}/_mapping", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if !ownsIndex(w, r, name) {
			return
		}
		s.record("mapping.get")
		s.mu.Lock()
		s.mappingTargets = append(s.mappingTargets, name)
		s.mu.Unlock()
		status := cfg.mappingStatus
		if status == 0 {
			status = http.StatusOK
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = fmt.Fprintf(w, `{"error":{"type":"stub_mapping_failure"},"status":%d}`, status)
			return
		}
		_, _ = w.Write([]byte(cfg.liveMappings))
	})
	stub.Handle("POST /{name}/_delete_by_query", func(w http.ResponseWriter, r *http.Request) {
		if !ownsIndex(w, r, r.PathValue("name")) {
			return
		}
		s.record("flush")
		_, _ = w.Write([]byte(`{"failures":[]}`))
	})
	stub.Handle("POST /{name}/_refresh", func(w http.ResponseWriter, r *http.Request) {
		if !ownsIndex(w, r, r.PathValue("name")) {
			return
		}
		s.record("index.refresh")
		_, _ = w.Write([]byte(refreshBody))
	})
	stub.Handle("GET /_nodes", s.recorded("nodes.get", esstub.JSON(http.StatusOK, nodesBody)))
	stub.Handle("POST /_bulk", s.recorded("bulk", esstub.JSON(http.StatusOK, bulkOKBody(cfg.index))))
	stub.Handle("POST /_aliases", s.recorded("alias.update", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		s.aliasUpdated = true
		s.mu.Unlock()
		_, _ = w.Write([]byte(ackBody))
	}))
	stub.Handle("PUT /_ingest/pipeline/{name}", s.recorded("pipeline.put", esstub.JSON(http.StatusOK, ackBody)))

	listPolicies := s.recorded("policy.get", func(w http.ResponseWriter, _ *http.Request) {
		configs := make([]string, 0, len(cfg.policies))
		for _, name := range cfg.policies {
			configs = append(configs, fmt.Sprintf(`{"config":{"match":{"name":%q}}}`, name))
		}
		_, _ = fmt.Fprintf(w, `{"policies":[%s]}`, strings.Join(configs, ","))
	})
	stub.Handle("GET /_enrich/policy", listPolicies)
	stub.Handle("GET /_enrich/policy/{$}", listPolicies)
	stub.Handle("PUT /_enrich/policy/{name}", s.recorded("policy.put", esstub.JSON(http.StatusOK, ackBody)))
	executePolicy := s.recorded("enrich.execute", esstub.JSON(http.StatusOK, `{"status":{"phase":"COMPLETED"}}`))
	stub.Handle("PUT /_enrich/policy/{name}/_execute", executePolicy)
	stub.Handle("POST /_enrich/policy/{name}/_execute", executePolicy)
	// Each execution first snapshots the policy's backing alias (absent here).
	// The routes are exact per policy: a generic GET /_alias/{name} would
	// conflict with GET /{name}/_mapping in ServeMux. They are reads, so they
	// are not recorded as lifecycle operations.
	for _, policy := range cfg.policies {
		stub.Handle("GET /_alias/.enrich-"+policy, esstub.JSON(http.StatusNotFound, `{"error":"alias [.enrich-`+policy+`] missing","status":404}`))
	}

	for _, id := range cfg.transforms {
		// Elasticsearch validates the transform source at PUT time, so the
		// stub does too: the logical name must be a concrete index or an alias
		// that has already been pointed at one. This keeps the alias
		// first-create ordering guard (transform.put only after alias.update)
		// observable as a real 400 rather than only as an index comparison.
		registerTransformRoutes(stub, &s.opRecorder, id, func(w http.ResponseWriter, _ *http.Request) {
			s.mu.Lock()
			valid := s.present[cfg.index] || s.aliasUpdated
			s.mu.Unlock()
			if !valid {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = fmt.Fprintf(w, `{"error":{"type":"validation_exception","reason":"Validation Failed: 1: no such index [%s];"},"status":400}`, cfg.index)
				return
			}
			_, _ = w.Write([]byte(ackBody))
		})
	}
	return s
}

// preflightStubConfig and preflightRunStub are the mapping-preflight tests'
// names for the shared lifecycle stub, which always serves the index "cards".
type (
	preflightStubConfig = lifecycleConfig
	preflightRunStub    = lifecycleStub
)

// newPreflightRunStub starts the shared lifecycle stub for the index "cards".
func newPreflightRunStub(t *testing.T, cfg preflightStubConfig) *preflightRunStub {
	t.Helper()
	cfg.index = "cards"
	return newLifecycleStub(t, cfg)
}

// newCardsBulkStub serves an -add Run against the existing index "cards": a
// HEAD existence check plus POST /_bulk, whose attempts are counted and
// answered by bulk. attempt is 1-based. Any other request is unexpected.
func newCardsBulkStub(t *testing.T, bulk func(w http.ResponseWriter, r *http.Request, attempt int)) (*esstub.Stub, *atomic.Int32) {
	t.Helper()

	var attempts atomic.Int32
	stub := esstub.New(t)
	stub.Handle("HEAD /cards", esstub.JSON(http.StatusOK, ""))
	stub.Handle("POST /_bulk", func(w http.ResponseWriter, r *http.Request) {
		bulk(w, r, int(attempts.Add(1)))
	})
	return stub, &attempts
}

// bulkAlways answers every bulk attempt with the same status and body.
func bulkAlways(status int, body string) func(http.ResponseWriter, *http.Request, int) {
	return func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

// bulkAfter runs first on attempt 1 and answers every later attempt with a
// successful bulk response.
func bulkAfter(first func(w http.ResponseWriter)) func(http.ResponseWriter, *http.Request, int) {
	return func(w http.ResponseWriter, _ *http.Request, attempt int) {
		if attempt == 1 {
			first(w)
			return
		}
		_, _ = w.Write([]byte(bulkOKBody("cards")))
	}
}

// replyTransient503 is a retryable first bulk failure.
func replyTransient503(w http.ResponseWriter) {
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(`{"error":true,"message":"transient"}`))
}

// dropConnection hijacks the response and closes the connection so the client
// sees a transport failure. The original ResponseWriter must keep its
// http.Hijacker; a failure is reported with Errorf (never Fatal off the test
// goroutine) and answered with a 500 so the request still completes.
func dropConnection(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		t.Errorf("response writer does not support hijacking")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	conn, _, err := hijacker.Hijack()
	if err != nil {
		t.Errorf("hijack returned error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_ = conn.Close()
}

// writeCollectionFixtures writes a data file named dataName with the given
// content plus a transforms file declaring one collection-to-binder transform,
// and returns both paths. The data extension selects JSON-array or NDJSON
// loading.
func writeCollectionFixtures(t *testing.T, dataName, data string) (dataPath, transformsPath string) {
	t.Helper()

	dir := t.TempDir()
	dataPath = filepath.Join(dir, dataName)
	if err := os.WriteFile(dataPath, []byte(data), 0o644); err != nil {
		t.Fatalf("write data fixture: %v", err)
	}
	transformsPath = filepath.Join(dir, "transforms.json")
	if err := os.WriteFile(transformsPath, []byte(`{
  "collection-to-binder": {
    "source_index": "collection",
    "body": {"source":{"index":"collection"},"dest":{"index":"binder"}}
  }
}`), 0o644); err != nil {
		t.Fatalf("write transforms fixture: %v", err)
	}
	return dataPath, transformsPath
}

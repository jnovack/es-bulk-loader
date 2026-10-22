package loader

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"
)

// enrichPolicyListServer serves GET /_enrich/policy with one policy per name.
func enrichPolicyListServer(t *testing.T, names ...string) *elasticsearch.Client {
	t.Helper()

	entries := make([]string, 0, len(names))
	for _, name := range names {
		entries = append(entries, fmt.Sprintf(`{"config":{"match":{"name":%q}}}`, name))
	}
	body := `{"policies":[` + strings.Join(entries, ",") + `]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	es, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{server.URL}})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	return es
}

var policyResolutionPipelines = namedDefinitions{
	"p": []byte(`{"processors":[{"enrich":{"policy_name":"enrich-cards"}}]}`),
}

// TestAmbiguousLogicalPolicyReferenceFailsRunInsteadOfGuessing guards the
// decision to fail rather than sort-and-take-last: a content hash has no
// recency, so choosing a generation silently binds a pipeline to a possibly
// stale policy. The policy is named "enrich-cards" on purpose: classification
// reads the message, so candidates interpolated into it would turn this into
// ErrEnrichExecution.
func TestAmbiguousLogicalPolicyReferenceFailsRunInsteadOfGuessing(t *testing.T) {
	t.Parallel()

	es := enrichPolicyListServer(t, "enrich-cards-bbbbbb", "enrich-cards-aaaaaa")
	mapping := map[string]string{}

	runErr := recoverRunError(t, func() {
		resolvePipelinePolicyFallbacks(t.Context(), es, policyResolutionPipelines, []string{"p"}, mapping)
	})
	if !errors.Is(runErr, ErrManagedResource) {
		t.Fatalf("err = %v, want ErrManagedResource", runErr)
	}
	msg := runErr.Error()
	first, second := strings.Index(msg, "enrich-cards-aaaaaa"), strings.Index(msg, "enrich-cards-bbbbbb")
	if first < 0 || second < 0 || first > second {
		t.Fatalf("error %q must name both candidates in sorted order", msg)
	}
	if len(mapping) != 0 {
		t.Fatalf("mapping assigned before failing: %v", mapping)
	}
}

// TestResolvePipelinePolicyFallbacksUnchangedPaths pins the paths the
// ambiguity fix must not touch.
func TestResolvePipelinePolicyFallbacksUnchangedPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		available []string
		premapped map[string]string
		want      map[string]string
	}{
		{"exact match", []string{"enrich-cards"}, nil, map[string]string{"enrich-cards": "enrich-cards"}},
		{"single managed match", []string{"enrich-cards-aaaaaa"}, nil, map[string]string{"enrich-cards": "enrich-cards-aaaaaa"}},
		{"zero match warns and skips", []string{"other-aaaaaa"}, nil, map[string]string{}},
		{"exact beats multiple generations", []string{"enrich-cards", "enrich-cards-aaaaaa", "enrich-cards-bbbbbb"}, nil, map[string]string{"enrich-cards": "enrich-cards"}},
		{"supplied mapping beats multiple generations", []string{"enrich-cards-aaaaaa", "enrich-cards-bbbbbb"}, map[string]string{"enrich-cards": "enrich-cards-cccccc"}, map[string]string{"enrich-cards": "enrich-cards-cccccc"}},
		{"supplied mapping beats exact and generations", []string{"enrich-cards", "enrich-cards-aaaaaa", "enrich-cards-bbbbbb"}, map[string]string{"enrich-cards": "enrich-cards-cccccc"}, map[string]string{"enrich-cards": "enrich-cards-cccccc"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			es := enrichPolicyListServer(t, tt.available...)
			mapping := map[string]string{}
			for k, v := range tt.premapped {
				mapping[k] = v
			}
			resolvePipelinePolicyFallbacks(t.Context(), es, policyResolutionPipelines, []string{"p"}, mapping)
			if len(mapping) != len(tt.want) {
				t.Fatalf("mapping = %v, want %v", mapping, tt.want)
			}
			for k, v := range tt.want {
				if mapping[k] != v {
					t.Fatalf("mapping = %v, want %v", mapping, tt.want)
				}
			}
		})
	}
}

// TestAmbiguousPolicyReferenceFailsRunBeforeMutation proves Run recovers the
// ambiguity as ErrManagedResource before pipeline PUT, flush, deletion or bulk.
// The candidates contain "enrich" and "bulk" to pin classification
// independently of resource spelling. Not parallel: Run swaps the global logger.
func TestAmbiguousPolicyReferenceFailsRunBeforeMutation(t *testing.T) {
	var (
		mu       sync.Mutex
		requests []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/_enrich/policy" {
			_, _ = w.Write([]byte(`{"policies":[{"config":{"match":{"name":"enrich-bulk-cards-bbbbbb"}}},{"config":{"match":{"name":"enrich-bulk-cards-aaaaaa"}}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(server.Close)

	dir := t.TempDir()
	_, err := Run(t.Context(), Options{
		URL:           server.URL,
		Index:         "products",
		FlushIndex:    true,
		PipelinesFile: writeTempJSON(t, dir, `{"p":{"processors":[{"enrich":{"policy_name":"enrich-bulk-cards"}}]}}`),
		DataFile:      writeTempJSON(t, dir, `{"id":"1"}`),
	})
	if !errors.Is(err, ErrManagedResource) {
		t.Fatalf("err = %v, want ErrManagedResource", err)
	}
	first, second := strings.Index(err.Error(), "enrich-bulk-cards-aaaaaa"), strings.Index(err.Error(), "enrich-bulk-cards-bbbbbb")
	if first < 0 || second < 0 || first > second {
		t.Fatalf("error %q must name both candidates in sorted order", err.Error())
	}

	mu.Lock()
	seen := slices.Clone(requests)
	mu.Unlock()
	for _, request := range seen {
		if !strings.HasPrefix(request, "GET ") && !strings.HasPrefix(request, "HEAD ") {
			t.Fatalf("mutating request reached the cluster: %v", seen)
		}
	}
}

package loader

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
)

// enrichPolicyListServer serves GET /_enrich/policy with one policy per name.
func enrichPolicyListServer(t *testing.T, names ...string) *elasticsearch.Client {
	t.Helper()

	entries := make([]string, 0, len(names))
	for _, name := range names {
		entries = append(entries, fmt.Sprintf(`{"config":{"match":{"name":%q}}}`, name))
	}
	stub := esstub.New(t)
	stub.Handle("GET /_enrich/policy", esstub.JSON(http.StatusOK, `{"policies":[`+strings.Join(entries, ",")+`]}`))
	return newStubClient(t, stub)
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
// independently of resource spelling.
func TestAmbiguousPolicyReferenceFailsRunBeforeMutation(t *testing.T) {
	t.Parallel()

	// Only the policy listing is registered. Any other request, mutating or
	// not, is an unexpected route and fails the test.
	stub := esstub.New(t)
	stub.Handle("GET /_enrich/policy", esstub.JSON(http.StatusOK, `{"policies":[{"config":{"match":{"name":"enrich-bulk-cards-bbbbbb"}}},{"config":{"match":{"name":"enrich-bulk-cards-aaaaaa"}}}]}`))

	dir := t.TempDir()
	_, err := Run(t.Context(), Options{
		URL:           stub.URL(),
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

	seen := requestLabels(stub)
	for _, request := range seen {
		if !strings.HasPrefix(request, "GET ") && !strings.HasPrefix(request, "HEAD ") {
			t.Fatalf("mutating request reached the cluster: %v", seen)
		}
	}
	if !slices.Contains(seen, "GET /_enrich/policy") {
		t.Fatalf("policy listing never requested; the run failed for another reason: %v", seen)
	}
}

package loader

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/fixture"
)

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
		PipelinesFile: fixture.WriteTempJSON(t, dir, `{"p":{"processors":[{"enrich":{"policy_name":"enrich-bulk-cards"}}]}}`),
		DataFile:      fixture.WriteTempJSON(t, dir, `{"id":"1"}`),
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

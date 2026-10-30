package managed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/enrich"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
)

// TestDeleteResourcesUsesNukePipelineRecovery verifies that declared
// pipelines receive nuke's default-pipeline recovery behavior.
func TestDeleteResourcesUsesNukePipelineRecovery(t *testing.T) {
	var (
		deleteAttempts atomic.Int32
		clearedDefault atomic.Bool
	)

	stub := esstub.New(t)
	stub.Handle("DELETE /_ingest/pipeline/declared-pipeline", func(w http.ResponseWriter, r *http.Request) {
		if deleteAttempts.Add(1) == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"reason":"pipeline [declared-pipeline] cannot be deleted because it is the default pipeline for 1 index(es) including [other-index]"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	})
	stub.Handle("GET /_settings/*", esstub.JSON(http.StatusOK, `{"other-index":{"settings":{"index":{"default_pipeline":"declared-pipeline"}}}}`))
	stub.Handle("PUT /other-index/_settings", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read settings request: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !strings.Contains(string(body), `"index.default_pipeline":null`) {
			t.Errorf("unexpected settings request body: %s", body)
		}
		clearedDefault.Store(true)
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	})

	es := stub.MustClient(t)

	if err := DeleteResources(context.Background(), es, []string{"declared-pipeline"}, nil, nil, true); err != nil {
		t.Fatalf("deleteManagedResources returned error: %v", err)
	}

	if got := deleteAttempts.Load(); got != 2 {
		t.Fatalf("delete attempts = %d, want 2", got)
	}
	if !clearedDefault.Load() {
		t.Fatal("expected nuke recovery to clear index.default_pipeline")
	}
}

// TestDeleteResourcesNonNukeUsesOrdinaryPipelineDeletion verifies that
// normal cleanup does not invoke nuke's cross-index recovery requests.
func TestDeleteResourcesNonNukeUsesOrdinaryPipelineDeletion(t *testing.T) {
	var deleteAttempts atomic.Int32
	stub := esstub.New(t)
	// Only the pipeline DELETE is registered: any recovery request is an
	// unexpected route and fails the test.
	stub.Handle("DELETE /_ingest/pipeline/declared-pipeline", func(w http.ResponseWriter, _ *http.Request) {
		deleteAttempts.Add(1)
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	})

	es := stub.MustClient(t)

	if err := DeleteResources(context.Background(), es, []string{"declared-pipeline"}, nil, nil, false); err != nil {
		t.Fatalf("deleteManagedResources returned error: %v", err)
	}

	if got := deleteAttempts.Load(); got != 1 {
		t.Fatalf("delete attempts = %d, want 1", got)
	}
}

// TestCreatePoliciesCancellationStopsRetry verifies cancellation after an
// index-not-found response prevents another policy PUT.
func TestCreatePoliciesCancellationStopsRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var attempts atomic.Int32
	stub := esstub.New(t)
	stub.Handle("PUT /_enrich/policy/policy", func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		cancel()
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"type":"index_not_found_exception"}}`))
	})

	es := stub.MustClient(t)

	err := CreatePolicies(ctx, es, config.Definitions{"policy": json.RawMessage(`{"match":{}}`)}, []string{"policy"}, sleepUntilDone)
	// The cancel can surface on the PUT response or on the retry sleep; both
	// ops classify the same way, and either way no second PUT is sent.
	requireOp(t, err, "creating enrich policy", "Context cancelled during enrich policy retry")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("createPolicies error = %v, want context.Canceled", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("policy PUT attempts = %d, want 1", got)
	}
}

// TestPutPolicyUsesCallerContext verifies cancellation reaches an in-flight
// enrich-policy request.
func TestPutPolicyUsesCallerContext(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	stub := esstub.New(t)
	stub.Handle("PUT /_enrich/policy/policy", func(_ http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		select {
		case <-r.Context().Done():
		case <-releaseHandler:
		}
	})
	// Registered after the stub's own cleanup, so it runs first and
	// unblocks the handler before the server waits for it.
	t.Cleanup(func() { close(releaseHandler) })

	es := stub.MustClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		res, err := putPolicy(ctx, es, "policy", json.RawMessage(`{"match":{}}`))
		if res != nil {
			res.Body.Close()
		}
		result <- err
	}()

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("policy request did not start")
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("putPolicy error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("putPolicy did not return promptly after cancellation")
	}
}

// TestDeletePipelinesUsesCallerContext verifies cancellation reaches an
// in-flight pipeline delete request, proving ctx now threads from the caller
// into this managed-resource helper instead of being hardcoded to
// context.Background().
func TestDeletePipelinesUsesCallerContext(t *testing.T) {
	requestStarted := make(chan struct{})
	stub := esstub.New(t)
	stub.Handle("DELETE /_ingest/pipeline/pipeline-a", func(_ http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-r.Context().Done()
	})

	es := stub.MustClient(t)
	ctx, cancel := context.WithCancel(context.Background())

	var deleteErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		deleteErr = deletePipelines(ctx, es, []string{"pipeline-a"})
	}()

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("delete pipeline request did not start")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("deletePipelines did not return promptly after cancellation")
	}

	err := deleteErr
	requireOp(t, err, "deleting pipeline")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("deletePipelines error = %v, want context.Canceled", err)
	}
}

// TestPipelineNamesReferencingPolicy guards finding every pipeline that references a given enrich policy.
func TestPipelineNamesReferencingPolicy(t *testing.T) {
	t.Parallel()

	definitions := config.Definitions{
		"direct-enrich": json.RawMessage(`{
			"processors": [
				{"enrich": {"policy_name": "slugs-by-name", "field": "name", "target_field": "slug"}}
			]
		}`),
		"nested-enrich": json.RawMessage(`{
			"processors": [
				{"foreach": {"field": "cards", "processor": {"enrich": {"policy_name": "slugs-by-name", "field": "_ingest._value.name", "target_field": "_ingest._value.slug"}}}}
			]
		}`),
		"other-policy": json.RawMessage(`{
			"processors": [
				{"enrich": {"policy_name": "different-policy", "field": "name", "target_field": "other"}}
			]
		}`),
	}

	got := pipelineNamesReferencingPolicy(definitions, "slugs-by-name")
	want := []string{"direct-enrich", "nested-enrich"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pipelineNamesReferencingPolicy mismatch: got %v want %v", got, want)
	}
}

// TestPolicyDeleteBlockedByPipelineReference guards that a policy-delete error naming a referencing pipeline is detected, and unrelated delete errors are ignored.
func TestPolicyDeleteBlockedByPipelineReference(t *testing.T) {
	t.Parallel()

	if !policyDeleteBlockedByPipelineReference([]byte(`{"error":{"reason":"Could not delete policy [slugs-by-name] because a pipeline is referencing it [enrich-from-slugs]"}}`)) {
		t.Fatal("expected pipeline reference conflict to be detected")
	}

	if policyDeleteBlockedByPipelineReference([]byte(`{"error":{"reason":"Could not delete policy [slugs-by-name]"}}`)) {
		t.Fatal("expected non-reference policy delete error to be ignored")
	}
}

// TestPipelineDeleteBlockedByDefaultIndex guards that a pipeline-delete error naming a default-pipeline conflict is detected, and unrelated delete errors are ignored.
func TestPipelineDeleteBlockedByDefaultIndex(t *testing.T) {
	t.Parallel()

	if !pipelineDeleteBlockedByDefaultIndex([]byte(`{"error":{"reason":"pipeline [enrich-from-slugs] cannot be deleted because it is the default pipeline for 1 index(es) including [cards]"}}`)) {
		t.Fatal("expected default-pipeline delete conflict to be detected")
	}

	if pipelineDeleteBlockedByDefaultIndex([]byte(`{"error":{"reason":"pipeline [enrich-from-slugs] cannot be deleted"}}`)) {
		t.Fatal("expected unrelated pipeline delete error to be ignored")
	}
}

// TestHashedPolicyNameIsStableAndShortHashed guards that a content-hashed policy name is stable for equal content and uses a six-character hash suffix.
func TestHashedPolicyNameIsStableAndShortHashed(t *testing.T) {
	t.Parallel()

	definition := json.RawMessage(`{"match":{"indices":"cards","match_field":"lookup_id","enrich_fields":["name"]}}`)
	got, err := hashedPolicyName(t.Context(), "cards-by-id", definition)
	if err != nil {
		t.Fatalf("managedPolicyName returned error: %v", err)
	}
	if !nameMatchesLogical("cards-by-id", got) {
		t.Fatalf("expected managed policy name format logical-<6hex>, got %q", got)
	}
	if len(got) != len("cards-by-id-")+6 {
		t.Fatalf("managed policy name length mismatch: got %d", len(got))
	}
}

// TestRewritePipelinePolicyReferences guards that pipeline enrich processors are rewritten to resolved managed policy names.
func TestRewritePipelinePolicyReferences(t *testing.T) {
	t.Parallel()

	definitions := config.Definitions{
		"pipeline-a": json.RawMessage(`{
			"processors": [
				{"enrich": {"policy_name": "cards-by-id", "field": "lookup_id", "target_field": "source"}}
			]
		}`),
	}
	rewritten, err := RewritePipelinePolicyReferences(t.Context(), definitions, []string{"pipeline-a"}, map[string]string{
		"cards-by-id": "cards-by-id-a1b2c3",
	})
	if err != nil {
		t.Fatalf("rewritePipelinePolicyReferences returned error: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(rewritten["pipeline-a"], &parsed); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	processors, ok := parsed["processors"].([]any)
	if !ok || len(processors) == 0 {
		t.Fatalf("expected processors array in rewritten pipeline")
	}
	firstProcessor, ok := processors[0].(map[string]any)
	if !ok {
		t.Fatalf("expected first processor object")
	}
	enrich, ok := firstProcessor["enrich"].(map[string]any)
	if !ok {
		t.Fatalf("expected enrich processor object")
	}
	if got := enrich["policy_name"]; got != "cards-by-id-a1b2c3" {
		t.Fatalf("policy_name mismatch: got %v want %v", got, "cards-by-id-a1b2c3")
	}
}

// TestRemapEnrichSelectionMapsLogicalNames guards that a logical enrich selection is remapped to managed policy names.
func TestRemapEnrichSelectionMapsLogicalNames(t *testing.T) {
	t.Parallel()

	selection := &enrich.Selection{Enabled: true, Raw: "cards-by-id,missing"}
	remapped := RemapEnrichSelection(t.Context(), selection, map[string]string{
		"cards-by-id": "cards-by-id-a1b2c3",
	})
	if remapped == nil {
		t.Fatal("expected remapped enrich selection")
	}
	if got := remapped.Raw; got != "cards-by-id-a1b2c3,missing" {
		t.Fatalf("remapped raw mismatch: got %q", got)
	}
}

// enrichPolicyListServer serves GET /_enrich/policy with one policy per name.
func enrichPolicyListServer(t *testing.T, names ...string) *elasticsearch.Client {
	t.Helper()

	entries := make([]string, 0, len(names))
	for _, name := range names {
		entries = append(entries, fmt.Sprintf(`{"config":{"match":{"name":%q}}}`, name))
	}
	stub := esstub.New(t)
	stub.Handle("GET /_enrich/policy", esstub.JSON(http.StatusOK, `{"policies":[`+strings.Join(entries, ",")+`]}`))
	return stub.MustClient(t)
}

var policyResolutionPipelines = config.Definitions{
	"p": []byte(`{"processors":[{"enrich":{"policy_name":"enrich-cards"}}]}`),
}

// TestAmbiguousLogicalPolicyReferenceFailsRunInsteadOfGuessing guards the
// decision to fail rather than sort-and-take-last: a content hash has no
// recency, so choosing a generation silently binds a pipeline to a possibly
// stale policy. The policy is named "enrich-cards" on purpose: the loader
// classifies by Op, so the candidates must stay in the cause, never the Op, or
// this would become ErrEnrichExecution.
func TestAmbiguousLogicalPolicyReferenceFailsRunInsteadOfGuessing(t *testing.T) {
	t.Parallel()

	es := enrichPolicyListServer(t, "enrich-cards-bbbbbb", "enrich-cards-aaaaaa")
	mapping := map[string]string{}

	runErr := ResolvePipelinePolicyFallbacks(t.Context(), es, policyResolutionPipelines, []string{"p"}, mapping)
	requireOp(t, runErr, "Ambiguous policy reference")
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
			if err := ResolvePipelinePolicyFallbacks(t.Context(), es, policyResolutionPipelines, []string{"p"}, mapping); err != nil {
				t.Fatalf("resolvePipelinePolicyFallbacks returned error: %v", err)
			}
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

// requireOp fails the test unless err is an operation error whose Op is one
// of ops. Kind for these ops is pinned by TestOperationKindsOfMovedHelpers in
// package loader, which owns classification.
func requireOp(t *testing.T, err error, ops ...string) {
	t.Helper()
	var opErr *failure.Error
	if !errors.As(err, &opErr) || !slices.Contains(ops, opErr.Op) || opErr.Err == nil {
		t.Fatalf("error = %#v, want an operation error with Op in %q and a cause", err, ops)
	}
}

// sleepUntilDone stands in for the Run's sleep seam: it honours ctx like the
// production seam but never waits.
func sleepUntilDone(ctx context.Context, _ time.Duration) error { return ctx.Err() }

// TestAmbiguousPolicyOpExcludesCandidates guards that the candidate names
// never enter the Op: the loader classifies by Op, and names containing
// "enrich" or "bulk" would reclassify the failure.
func TestAmbiguousPolicyOpExcludesCandidates(t *testing.T) {
	t.Parallel()

	es := enrichPolicyListServer(t, "enrich-cards-bbbbbb", "enrich-cards-aaaaaa")
	err := ResolvePipelinePolicyFallbacks(t.Context(), es, policyResolutionPipelines, []string{"p"}, map[string]string{})
	var opErr *failure.Error
	if !errors.As(err, &opErr) || opErr.Op != "Ambiguous policy reference" {
		t.Fatalf("error = %#v, want Op %q exactly", err, "Ambiguous policy reference")
	}
}

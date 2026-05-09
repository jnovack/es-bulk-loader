package loader

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

// TestReadNamedDefinitionsPreservesFileOrder verifies behavior for the related scenario.
func TestReadNamedDefinitionsPreservesFileOrder(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "pipelines.json")
	content := `{
  "first-pipeline": { "processors": [] },
  "second-pipeline": { "processors": [] }
}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write pipeline fixture: %v", err)
	}

	definitions, names := readNamedDefinitions(path, "pipeline", nil)

	if len(definitions) != 2 {
		t.Fatalf("expected 2 definitions, got %d", len(definitions))
	}

	expected := []string{"first-pipeline", "second-pipeline"}
	if len(names) != len(expected) {
		t.Fatalf("expected %d names, got %d", len(expected), len(names))
	}
	for i := range expected {
		if names[i] != expected[i] {
			t.Fatalf("expected names[%d] = %q, got %q", i, expected[i], names[i])
		}
	}
}

// TestNormalizeIndexSettingsUsesFirstPipelineWhenUnset verifies behavior for the related scenario.
func TestNormalizeIndexSettingsUsesFirstPipelineWhenUnset(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")
	if err := os.WriteFile(path, []byte(`{"settings":{"number_of_shards":1}}`), 0o644); err != nil {
		t.Fatalf("write settings fixture: %v", err)
	}

	normalized := normalizeIndexSettings(path, "first-pipeline", 0, nil)

	var parsed map[string]any
	if err := json.Unmarshal([]byte(normalized), &parsed); err != nil {
		t.Fatalf("parse normalized settings: %v", err)
	}

	if got := parsed["default_pipeline"]; got != "first-pipeline" {
		t.Fatalf("expected injected default pipeline, got %#v", got)
	}
	if got := parsed["number_of_shards"]; got != float64(1) {
		t.Fatalf("expected number_of_shards to be preserved, got %#v", got)
	}
}

// TestNormalizeIndexSettingsPreservesExplicitDefault verifies behavior for the related scenario.
func TestNormalizeIndexSettingsPreservesExplicitDefault(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")
	if err := os.WriteFile(path, []byte(`{"settings":{"index.default_pipeline":"explicit-pipeline"}}`), 0o644); err != nil {
		t.Fatalf("write settings fixture: %v", err)
	}

	normalized := normalizeIndexSettings(path, "first-pipeline", 0, nil)

	var parsed map[string]any
	if err := json.Unmarshal([]byte(normalized), &parsed); err != nil {
		t.Fatalf("parse normalized settings: %v", err)
	}

	if got := parsed["default_pipeline"]; got != "explicit-pipeline" {
		t.Fatalf("expected explicit default pipeline to win, got %#v", got)
	}
}

// TestNormalizeIndexSettingsFlattensNestedIndexObject verifies behavior for the related scenario.
func TestNormalizeIndexSettingsFlattensNestedIndexObject(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")
	if err := os.WriteFile(path, []byte(`{"settings":{"index":{"number_of_shards":1,"number_of_replicas":0}}}`), 0o644); err != nil {
		t.Fatalf("write settings fixture: %v", err)
	}

	// Pass replicas=1 so the override value differs from the fixture value of 0,
	// proving the topology value wins rather than the assertion passing by coincidence.
	normalized := normalizeIndexSettings(path, "first-pipeline", 1, nil)

	var parsed map[string]any
	if err := json.Unmarshal([]byte(normalized), &parsed); err != nil {
		t.Fatalf("parse normalized settings: %v", err)
	}

	if _, ok := parsed["index"]; ok {
		t.Fatalf("expected nested index wrapper to be flattened, got %#v", parsed["index"])
	}
	if _, ok := parsed["settings"]; ok {
		t.Fatalf("expected top-level settings wrapper to be removed, got %#v", parsed["settings"])
	}
	if got := parsed["number_of_shards"]; got != float64(1) {
		t.Fatalf("expected number_of_shards to be flattened, got %#v", got)
	}
	// Fixture specifies 0 but topology override is 1 — override must win.
	if got := parsed["number_of_replicas"]; got != float64(1) {
		t.Fatalf("expected number_of_replicas to reflect topology override, got %#v", got)
	}
	if got := parsed["default_pipeline"]; got != "first-pipeline" {
		t.Fatalf("expected default_pipeline to be injected, got %#v", got)
	}
}

// TestIsUnsupportedEnrichAPI verifies behavior for the related scenario.
func TestIsUnsupportedEnrichAPI(t *testing.T) {
	t.Parallel()

	if !isUnsupportedEnrichAPI(http.StatusNotFound, []byte(`{"error":true,"message":"Not Found"}`)) {
		t.Fatal("expected generic 404 response to be treated as unsupported enrich API")
	}

	if isUnsupportedEnrichAPI(http.StatusNotFound, []byte(`{"error":{"type":"resource_not_found_exception"}}`)) {
		t.Fatal("expected resource_not_found_exception to be treated as a missing policy, not an unsupported API")
	}

	if isUnsupportedEnrichAPI(http.StatusNotFound, []byte(`{"error":{"type":"index_not_found_exception"}}`)) {
		t.Fatal("expected index_not_found_exception to be treated as an Elasticsearch error, not an unsupported API")
	}
}

// TestResolveEnrichTargetsUsesDeclaredPoliciesWhenRequestedAll verifies behavior for the related scenario.
func TestResolveEnrichTargetsUsesDeclaredPoliciesWhenRequestedAll(t *testing.T) {
	t.Parallel()

	enrich := &enrichFlagValue{enabled: true, all: true}
	available := []string{"e2e-source-policy", "slugs-by-name"}
	declared := []string{"slugs-by-name"}

	targets, missing := resolveEnrichTargets(enrich, available, declared)

	if len(missing) != 0 {
		t.Fatalf("expected no missing policies, got %v", missing)
	}
	if len(targets) != 1 || targets[0] != "slugs-by-name" {
		t.Fatalf("expected declared policy only, got %v", targets)
	}
}

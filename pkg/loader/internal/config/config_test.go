package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/fixture"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/template"
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

	definitions, names, err := ReadNamedDefinitions(t.Context(), path, "pipeline", nil)
	if err != nil {
		t.Fatalf("readNamedDefinitions returned error: %v", err)
	}

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

	normalized, err := NormalizeIndexSettings(t.Context(), path, "first-pipeline", 0, "cluster topology", nil)
	if err != nil {
		t.Fatalf("normalizeIndexSettings returned error: %v", err)
	}

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

	normalized, err := NormalizeIndexSettings(t.Context(), path, "first-pipeline", 0, "cluster topology", nil)
	if err != nil {
		t.Fatalf("normalizeIndexSettings returned error: %v", err)
	}

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
	normalized, err := NormalizeIndexSettings(t.Context(), path, "first-pipeline", 1, "cluster topology", nil)
	if err != nil {
		t.Fatalf("normalizeIndexSettings returned error: %v", err)
	}

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

// TestReadNamedDefinitionsPreservesFileOrderWithReverseInput verifies behavior for the related scenario.
func TestReadNamedDefinitionsPreservesFileOrderWithReverseInput(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp(t.TempDir(), "definitions-*.json")
	if err != nil {
		t.Fatalf("CreateTemp returned error: %v", err)
	}

	if _, err := tmp.WriteString(`{"pipeline-b":{"description":"b"},"pipeline-a":{"description":"a"}}`); err != nil {
		t.Fatalf("WriteString returned error: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	definitions, names, err := ReadNamedDefinitions(t.Context(), tmp.Name(), "pipeline", nil)
	if err != nil {
		t.Fatalf("readNamedDefinitions returned error: %v", err)
	}

	if want := []string{"pipeline-b", "pipeline-a"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("names mismatch: got %v want %v", names, want)
	}
	if _, ok := definitions["pipeline-a"]; !ok {
		t.Fatal("expected pipeline-a definition to be present")
	}
	if _, ok := definitions["pipeline-b"]; !ok {
		t.Fatal("expected pipeline-b definition to be present")
	}
}

// TestBuildCreateIndexBodySupportsWrappedAndRawSections verifies behavior for the related scenario.
func TestBuildCreateIndexBodySupportsWrappedAndRawSections(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	wrappedSettings := fixture.WriteTempJSON(t, tempDir, `{"settings":{"index.default_pipeline":"wrapped-pipeline"}}`)
	rawMappings := fixture.WriteTempJSON(t, tempDir, `{"properties":{"lookup_id":{"type":"keyword"}}}`)

	body, err := BuildCreateIndexBody(t.Context(), 0, "cluster topology", wrappedSettings, rawMappings, "ignored-default", template.BuildVariables("runtime-index", nil))
	if err != nil {
		t.Fatalf("buildCreateIndexBody returned error: %v", err)
	}

	var parsed map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	var settings map[string]any
	if err := json.Unmarshal(parsed["settings"], &settings); err != nil {
		t.Fatalf("settings unmarshal returned error: %v", err)
	}
	if got := settings["default_pipeline"]; got != "wrapped-pipeline" {
		t.Fatalf("settings mismatch: got %q want %q", got, "wrapped-pipeline")
	}

	var mappings map[string]json.RawMessage
	if err := json.Unmarshal(parsed["mappings"], &mappings); err != nil {
		t.Fatalf("mappings unmarshal returned error: %v", err)
	}
	if _, ok := mappings["properties"]; !ok {
		t.Fatal("expected properties to be present in mappings")
	}
}

// TestReadNamedDefinitionsExpandsIndexVariable verifies behavior for the related scenario.
func TestReadNamedDefinitionsExpandsIndexVariable(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := fixture.WriteTempJSON(t, tempDir, `{"policy-a":{"match":{"indices":"${INDEX}"}}}`)

	definitions, names, err := ReadNamedDefinitions(t.Context(), path, "policy", template.BuildVariables("runtime-index", nil))
	if err != nil {
		t.Fatalf("readNamedDefinitions returned error: %v", err)
	}

	if want := []string{"policy-a"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("names mismatch: got %v want %v", names, want)
	}

	var parsed map[string]map[string]string
	if err := json.Unmarshal(definitions["policy-a"], &parsed); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if got := parsed["match"]["indices"]; got != "runtime-index" {
		t.Fatalf("indices mismatch: got %q want %q", got, "runtime-index")
	}
}

// TestBuildMappingPlan verifies behavior for the related scenario.
func TestBuildMappingPlan(t *testing.T) {
	t.Parallel()

	path := fixture.WriteTempJSON(t, t.TempDir(), `{
		"mappings": {
			"date_detection": false,
			"properties": {
				"notes": {"type":"keyword"},
				"quantity": {"type":"long"},
				"card": {"properties": {"name": {"type":"text"}}}
			}
		}
	}`)

	plan, err := BuildMappingPlan(t.Context(), path, nil)
	if err != nil {
		t.Fatalf("buildMappingPreflightPlan returned error: %v", err)
	}
	if !plan.HasExpectations() {
		t.Fatal("expected mapping preflight plan with expectations")
	}
	if plan.DateDetectionExpected == nil || *plan.DateDetectionExpected {
		t.Fatalf("expected date_detection=false in plan, got %v", plan.DateDetectionExpected)
	}
	if got := plan.RootFieldTypes["notes"]; got != "keyword" {
		t.Fatalf("notes field type mismatch: got %q want %q", got, "keyword")
	}
	if got := plan.RootFieldTypes["quantity"]; got != "long" {
		t.Fatalf("quantity field type mismatch: got %q want %q", got, "long")
	}
	if _, ok := plan.RootFieldTypes["card"]; ok {
		t.Fatal("expected object field without type to be excluded from root type checks")
	}
}

// TestNormalizeIndexSettingsReplicaOverridesFixture verifies that the replica count
// passed to normalizeIndexSettings always wins over the value in the settings file,
// regardless of what the fixture specifies.
// ADR: docs/decisions/0001-replica-count-from-node-count.md
func TestNormalizeIndexSettingsReplicaOverridesFixture(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		fixture  string
		replicas int
		expected float64
	}{
		{
			name:     "fixture 0 replicas overridden to 1 for multi-node",
			fixture:  `{"settings":{"index":{"number_of_replicas":0}}}`,
			replicas: 1,
			expected: 1,
		},
		{
			name:     "fixture 2 replicas overridden to 0 for single-node",
			fixture:  `{"settings":{"index":{"number_of_replicas":2}}}`,
			replicas: 0,
			expected: 0,
		},
		{
			name:     "missing replica key in fixture gets value from topology",
			fixture:  `{"settings":{"number_of_shards":1}}`,
			replicas: 1,
			expected: 1,
		},
		{
			name:     "fixture string 5 replicas overridden by explicit 2",
			fixture:  `{"settings":{"index":{"number_of_replicas":"5"}}}`,
			replicas: 2,
			expected: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := fixture.WriteTempJSON(t, dir, tc.fixture)

			normalized, err := NormalizeIndexSettings(t.Context(), path, "", tc.replicas, "explicit", nil)
			if err != nil {
				t.Fatalf("normalizeIndexSettings returned error: %v", err)
			}

			var parsed map[string]any
			if err := json.Unmarshal([]byte(normalized), &parsed); err != nil {
				t.Fatalf("parse normalized settings: %v", err)
			}

			if got := parsed["number_of_replicas"]; got != tc.expected {
				t.Fatalf("number_of_replicas = %v, want %v", got, tc.expected)
			}
		})
	}
}

// TestReadersReturnTheirOwnOperationErrors pins the failure Op of every
// config read site. The loader classifies a failure by its Op, so each read
// site keeps its fixed operation string and original cause.
func TestReadersReturnTheirOwnOperationErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.json")
	notObject := fixture.WriteTempJSON(t, dir, `["a"]`)
	truncated := fixture.WriteTempJSON(t, dir, `{"p":`)
	badSection := fixture.WriteTempJSON(t, dir, `{"mappings":`)
	nonObjectMappings := fixture.WriteTempJSON(t, dir, `{"mappings":[1]}`)
	unresolved := fixture.WriteTempJSON(t, dir, `{"p":{"v":"${CONFIG_TEST_UNSET}"}}`)

	tests := []struct {
		name      string
		call      func() error
		wantOp    string
		wantCause error
	}{
		{"missing definitions file", func() error {
			_, _, err := ReadNamedDefinitions(t.Context(), missing, "pipeline", nil)
			return err
		}, "Reading pipeline definitions file", os.ErrNotExist},
		{"unresolved template variable", func() error {
			_, _, err := ReadNamedDefinitions(t.Context(), unresolved, "policy", nil)
			return err
		}, "Reading policy definitions file", nil},
		{"definitions file not an object", func() error {
			_, _, err := ReadNamedDefinitions(t.Context(), notObject, "transform", nil)
			return err
		}, "transform definitions file must contain a JSON object", nil},
		{"truncated definition body", func() error {
			_, _, err := ReadNamedDefinitions(t.Context(), truncated, "pipeline", nil)
			return err
		}, "Parsing pipeline definition body", nil},
		{"missing settings file", func() error {
			_, err := NormalizeIndexSettings(t.Context(), missing, "", 0, "explicit", nil)
			return err
		}, "Reading settings file", os.ErrNotExist},
		{"malformed mappings section", func() error {
			_, err := NormalizeIndexSection(t.Context(), badSection, "mappings", nil)
			return err
		}, "Parsing mappings file", nil},
		{"mapping plan section not an object", func() error {
			_, err := BuildMappingPlan(t.Context(), nonObjectMappings, nil)
			return err
		}, "Failed to build mapping preflight plan", nil},
		{"create body propagates the settings read site", func() error {
			_, err := BuildCreateIndexBody(t.Context(), 0, "explicit", missing, "", "", nil)
			return err
		}, "Reading settings file", os.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call()
			var opErr *failure.Error
			if !errors.As(err, &opErr) {
				t.Fatalf("error = %T %v, want *failure.Error", err, err)
			}
			if opErr.Op != tt.wantOp || opErr.Err == nil {
				t.Fatalf("Op = %q (cause %v), want %q with a cause", opErr.Op, opErr.Err, tt.wantOp)
			}
			if tt.wantCause != nil && !errors.Is(err, tt.wantCause) {
				t.Fatalf("error = %v, want cause %v", err, tt.wantCause)
			}
		})
	}
}

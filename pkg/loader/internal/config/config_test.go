package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/fixture"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
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

func writeConfigFixture(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}

func assertOpError(t *testing.T, err error, wantOp string) *failure.Error {
	t.Helper()
	var opErr *failure.Error
	if !errors.As(err, &opErr) {
		t.Fatalf("error = %T %v, want *failure.Error", err, err)
	}
	if opErr.Op != wantOp {
		t.Fatalf("Op = %q, want %q", opErr.Op, wantOp)
	}
	return opErr
}

// TestParseSettingsFile pins the single settings parser shared by preflight
// and NormalizeIndexSettings: accepted shapes and the exact failure Ops.
func TestParseSettingsFile(t *testing.T) {
	// Not parallel: the unresolved-variable subtest mutates the environment.
	tests := []struct {
		name    string
		content string
		want    map[string]string
	}{
		{"raw", `{"number_of_shards":1}`, map[string]string{"number_of_shards": "1"}},
		{"wrapped", `{"settings":{"number_of_shards":2}}`, map[string]string{"number_of_shards": "2"}},
		{"nested index object", "{\"settings\":{\"index\":{\"index.refresh_interval\":\"1s\"}}}\r\n", map[string]string{"refresh_interval": `"1s"`}},
		{"dotted keys", `{"index.number_of_shards":3}`, map[string]string{"number_of_shards": "3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSettingsFile(t.Context(), writeConfigFixture(t, "s.json", tt.content), nil)
			if err != nil {
				t.Fatalf("ParseSettingsFile: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("settings = %v, want %v", got, tt.want)
			}
			for k, v := range tt.want {
				if string(got[k]) != v {
					t.Fatalf("settings[%q] = %s, want %s", k, got[k], v)
				}
			}
		})
	}

	t.Run("empty path", func(t *testing.T) {
		got, err := ParseSettingsFile(t.Context(), "", nil)
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("got %v, %v; want non-nil empty map and nil error", got, err)
		}
	})

	t.Run("missing file", func(t *testing.T) {
		_, err := ParseSettingsFile(t.Context(), filepath.Join(t.TempDir(), "nope.json"), nil)
		assertOpError(t, err, "Reading settings file")
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("error = %v, want os.ErrNotExist", err)
		}
	})
	t.Run("directory path", func(t *testing.T) {
		_, err := ParseSettingsFile(t.Context(), t.TempDir(), nil)
		if opErr := assertOpError(t, err, "Reading settings file"); opErr.Err == nil {
			t.Fatal("want non-nil cause")
		}
	})
	t.Run("malformed JSON", func(t *testing.T) {
		_, err := ParseSettingsFile(t.Context(), writeConfigFixture(t, "s.json", `{"settings":`), nil)
		assertOpError(t, err, "Parsing settings file")
		var syn *json.SyntaxError
		if !errors.As(err, &syn) {
			t.Fatalf("error = %v, want *json.SyntaxError", err)
		}
	})
	t.Run("nested settings wrong type", func(t *testing.T) {
		_, err := ParseSettingsFile(t.Context(), writeConfigFixture(t, "s.json", `{"settings":[]}`), nil)
		assertOpError(t, err, "Parsing nested settings file")
		var typ *json.UnmarshalTypeError
		if !errors.As(err, &typ) {
			t.Fatalf("error = %v, want *json.UnmarshalTypeError", err)
		}
	})
	t.Run("unresolved variable", func(t *testing.T) {
		t.Setenv("FIX_SETTINGSPRE_UNSET", "")
		if err := os.Unsetenv("FIX_SETTINGSPRE_UNSET"); err != nil {
			t.Fatalf("unsetenv: %v", err)
		}
		_, err := ParseSettingsFile(t.Context(), writeConfigFixture(t, "s.json", `{"number_of_shards":"${FIX_SETTINGSPRE_UNSET}"}`), template.Variables{})
		assertOpError(t, err, "Reading settings file")
		var unresolved *template.UnresolvedError
		if !errors.As(err, &unresolved) {
			t.Fatalf("error = %v, want *template.UnresolvedError", err)
		}
	})
}

// TestPreflightIndexInputs pins which file failures preflight reports and in
// which order, using the create step's own Ops.
func TestPreflightIndexInputs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		return path
	}
	goodSettings := write("good-settings.json", "{\"settings\":{\"number_of_shards\":1}}\r\n")
	goodMappings := write("good-mappings.json", "{\"properties\":{}}\r\n")
	badSettings := write("bad-settings.json", `{"settings":`)
	badMappings := write("bad-mappings.json", `{"mappings":`)
	missing := filepath.Join(dir, "missing.json")

	tests := []struct {
		name     string
		settings string
		mappings string
		wantOp   string
	}{
		{"valid", goodSettings, goodMappings, ""},
		{"both empty", "", "", ""},
		{"bad settings", badSettings, goodMappings, "Parsing settings file"},
		{"settings error first", missing, missing, "Reading settings file"},
		{"malformed mappings", goodSettings, badMappings, "Parsing mappings file"},
		{"missing mappings", goodSettings, missing, "Reading mappings file"},
		{"directory mappings", goodSettings, dir, "Reading mappings file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := PreflightIndexInputs(t.Context(), tt.settings, tt.mappings, nil)
			if tt.wantOp == "" {
				if err != nil {
					t.Fatalf("PreflightIndexInputs: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want %q failure, got nil", tt.wantOp)
			}
			assertOpError(t, err, tt.wantOp)
		})
	}
}

// TestPreflightIndexInputsPreservesCreateBodyAndLogs guards that preflight is
// silent and that the create body (and its two Info logs) are unchanged and
// still produced only at create time.
func TestPreflightIndexInputsPreservesCreateBodyAndLogs(t *testing.T) {
	t.Parallel()

	const mappings = `{"properties":{"id":{"type":"keyword"}}}`
	tests := []struct {
		name         string
		settings     string
		wantBody     string
		wantInjected bool
	}{
		{"wrapped nested", `{"settings":{"index":{"number_of_shards":1}}}`,
			`{"settings": {"default_pipeline":"p","number_of_replicas":1,"number_of_shards":1}, "mappings": ` + mappings + `}`, true},
		{"raw", `{"number_of_shards":1}`,
			`{"settings": {"default_pipeline":"p","number_of_replicas":1,"number_of_shards":1}, "mappings": ` + mappings + `}`, true},
		{"dotted", `{"index.number_of_shards":1}`,
			`{"settings": {"default_pipeline":"p","number_of_replicas":1,"number_of_shards":1}, "mappings": ` + mappings + `}`, true},
		{"explicit default wins", `{"default_pipeline":"q","number_of_shards":1}`,
			`{"settings": {"default_pipeline":"q","number_of_replicas":1,"number_of_shards":1}, "mappings": ` + mappings + `}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			settingsPath := writeConfigFixture(t, "s.json", tt.settings)
			mappingsPath := writeConfigFixture(t, "m.json", mappings)

			var buf bytes.Buffer
			logger := zerolog.New(&buf)
			ctx := runlog.WithLogger(t.Context(), &logger)

			if err := PreflightIndexInputs(ctx, settingsPath, mappingsPath, nil); err != nil {
				t.Fatalf("PreflightIndexInputs: %v", err)
			}
			if buf.Len() != 0 {
				t.Fatalf("preflight logged: %s", buf.String())
			}

			body, err := BuildCreateIndexBody(ctx, 1, "explicit", settingsPath, mappingsPath, "p", nil)
			if err != nil {
				t.Fatalf("BuildCreateIndexBody: %v", err)
			}
			if body != tt.wantBody {
				t.Fatalf("body = %s\nwant   %s", body, tt.wantBody)
			}

			counts := map[string]int{}
			for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
				var ev struct {
					Level   string `json:"level"`
					Message string `json:"message"`
				}
				if err := json.Unmarshal(line, &ev); err != nil {
					t.Fatalf("decode log %q: %v", line, err)
				}
				if ev.Level == "info" {
					counts[ev.Message]++
				}
			}
			if counts["Setting index replica count"] != 1 {
				t.Fatalf("replica logs = %d, want 1 (%v)", counts["Setting index replica count"], counts)
			}
			wantInjected := 0
			if tt.wantInjected {
				wantInjected = 1
			}
			if got := counts["Using first declared pipeline as index.default_pipeline"]; got != wantInjected {
				t.Fatalf("injection logs = %d, want %d", got, wantInjected)
			}
		})
	}
}

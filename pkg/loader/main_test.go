package loader

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// TestEnrichFlagValueBareFlagRunsAllPolicies verifies behavior for the related scenario.
func TestEnrichFlagValueBareFlagRunsAllPolicies(t *testing.T) {
	t.Parallel()

	var enrich enrichFlagValue

	if err := enrich.Set("true"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	if !enrich.enabled {
		t.Fatal("expected enrich flag to be enabled")
	}
	if !enrich.all {
		t.Fatal("expected bare enrich flag to target all policies")
	}
	if got := enrich.explicitPolicies(); got != nil {
		t.Fatalf("expected no explicit policies, got %v", got)
	}
}

// TestEnrichFlagValueExplicitPolicies verifies behavior for the related scenario.
func TestEnrichFlagValueExplicitPolicies(t *testing.T) {
	t.Parallel()

	var enrich enrichFlagValue

	if err := enrich.Set(" policy-a,policy-b, policy-a ,, policy-c "); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	want := []string{"policy-a", "policy-b", "policy-c"}
	if got := enrich.explicitPolicies(); !reflect.DeepEqual(got, want) {
		t.Fatalf("explicitPolicies mismatch: got %v want %v", got, want)
	}
}

// TestResolveEnrichTargetsAllPolicies verifies behavior for the related scenario.
func TestResolveEnrichTargetsAllPolicies(t *testing.T) {
	t.Parallel()

	enrich := &enrichFlagValue{enabled: true, all: true}
	available := []string{"policy-b", "policy-a"}

	targets, missing := resolveEnrichTargets(enrich, available, nil)

	if want := []string{"policy-a", "policy-b"}; !reflect.DeepEqual(targets, want) {
		t.Fatalf("targets mismatch: got %v want %v", targets, want)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing policies, got %v", missing)
	}
}

// TestResolveEnrichTargetsWarnsForMissingPolicies verifies behavior for the related scenario.
func TestResolveEnrichTargetsWarnsForMissingPolicies(t *testing.T) {
	t.Parallel()

	enrich := &enrichFlagValue{enabled: true, raw: "policy-b,policy-missing,policy-a"}
	available := []string{"policy-a", "policy-b"}

	targets, missing := resolveEnrichTargets(enrich, available, nil)

	if want := []string{"policy-b", "policy-a"}; !reflect.DeepEqual(targets, want) {
		t.Fatalf("targets mismatch: got %v want %v", targets, want)
	}
	if want := []string{"policy-missing"}; !reflect.DeepEqual(missing, want) {
		t.Fatalf("missing mismatch: got %v want %v", missing, want)
	}
}

// TestParseLogLevel verifies behavior for the related scenario.
func TestParseLogLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    zerolog.Level
		wantErr bool
	}{
		{name: "trace", input: "trace", want: zerolog.TraceLevel},
		{name: "debug uppercase", input: "DEBUG", want: zerolog.DebugLevel},
		{name: "info spaced", input: " info ", want: zerolog.InfoLevel},
		{name: "warn", input: "warn", want: zerolog.WarnLevel},
		{name: "error", input: "error", want: zerolog.ErrorLevel},
		{name: "invalid", input: "verbose", wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseLogLevel(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLogLevel returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseLogLevel mismatch: got %v want %v", got, tt.want)
			}
		})
	}
}

// TestRunWarningLogIncludesTimestamp verifies behavior for the related scenario.
func TestRunWarningLogIncludesTimestamp(t *testing.T) {
	previousLogger := log.Logger
	previousLevel := zerolog.GlobalLevel()
	t.Cleanup(func() {
		log.Logger = previousLogger
		zerolog.SetGlobalLevel(previousLevel)
	})

	var output bytes.Buffer
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: &output})
	zerolog.SetGlobalLevel(zerolog.TraceLevel)

	_, err := Run(context.Background(), Options{
		URL:      "http://127.0.0.1:1",
		Index:    "cards",
		KeepLast: 1,
		Nuke:     true,
	})
	if err == nil {
		t.Fatal("expected Run to fail for unreachable Elasticsearch")
	}

	logs := output.String()
	if !strings.Contains(logs, "Ignoring -keep-last because -alias is not enabled") {
		t.Fatalf("expected keep-last warning in logs, got: %s", logs)
	}
	if strings.Contains(logs, "<nil>") {
		t.Fatalf("expected timestamped log output without <nil>, got: %s", logs)
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

	definitions, names := readNamedDefinitions(tmp.Name(), "pipeline", nil)

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

	wrappedSettings := writeTempJSON(t, tempDir, `{"settings":{"index.default_pipeline":"wrapped-pipeline"}}`)
	rawMappings := writeTempJSON(t, tempDir, `{"properties":{"lookup_id":{"type":"keyword"}}}`)

	body := buildCreateIndexBody(nil, wrappedSettings, rawMappings, "ignored-default", buildTemplateVariables("runtime-index", nil))

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

// writeTempJSON centralizes this code path so package behavior stays consistent.
func writeTempJSON(t *testing.T, dir, content string) string {
	t.Helper()

	tmp, err := os.CreateTemp(dir, "*.json")
	if err != nil {
		t.Fatalf("CreateTemp returned error: %v", err)
	}
	if _, err := tmp.WriteString(content); err != nil {
		t.Fatalf("WriteString returned error: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	return tmp.Name()
}

// boolPointer centralizes this code path so package behavior stays consistent.
func boolPointer(value bool) *bool {
	return &value
}

// TestReadNamedDefinitionsExpandsIndexVariable verifies behavior for the related scenario.
func TestReadNamedDefinitionsExpandsIndexVariable(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := writeTempJSON(t, tempDir, `{"policy-a":{"match":{"indices":"${INDEX}"}}}`)

	definitions, names := readNamedDefinitions(path, "policy", buildTemplateVariables("runtime-index", nil))

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

// TestReadTemplatedFileLeavesUnknownVariablesUntouched verifies behavior for the related scenario.
func TestReadTemplatedFileLeavesUnknownVariablesUntouched(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := writeTempJSON(t, tempDir, `{"value":"${UNKNOWN_VAR}"}`)

	content, err := readTemplatedFile(path, buildTemplateVariables("runtime-index", nil))
	if err != nil {
		t.Fatalf("readTemplatedFile returned error: %v", err)
	}

	if got := string(content); got != `{"value":"${UNKNOWN_VAR}"}` {
		t.Fatalf("content mismatch: got %q want %q", got, `{"value":"${UNKNOWN_VAR}"}`)
	}
}

// TestBuildTemplateVariablesIncludesAdditionalValues verifies behavior for the related scenario.
func TestBuildTemplateVariablesIncludesAdditionalValues(t *testing.T) {
	t.Parallel()

	variables := buildTemplateVariables("collection-index", map[string]string{
		"BINDER_INDEX": "binder-index",
	})

	if got := variables["INDEX"]; got != "collection-index" {
		t.Fatalf("INDEX variable mismatch: got %q want %q", got, "collection-index")
	}
	if got := variables["BINDER_INDEX"]; got != "binder-index" {
		t.Fatalf("BINDER_INDEX variable mismatch: got %q want %q", got, "binder-index")
	}
}

// TestResolveTransformsForSourceFiltersBySourceIndex verifies behavior for the related scenario.
func TestResolveTransformsForSourceFiltersBySourceIndex(t *testing.T) {
	t.Parallel()

	definitions := namedDefinitions{
		"collection-to-binder": json.RawMessage(`{
			"source_index": "collection",
			"body": {"source":{"index":"collection"},"dest":{"index":"binder"}}
		}`),
		"cards-to-something": json.RawMessage(`{
			"source_index": "cards",
			"body": {"source":{"index":"cards"},"dest":{"index":"other"}}
		}`),
	}
	names := []string{"collection-to-binder", "cards-to-something"}

	selectedDefinitions, selectedNames, err := resolveTransformsForSource(definitions, names, "collection")
	if err != nil {
		t.Fatalf("resolveTransformsForSource returned error: %v", err)
	}

	if want := []string{"collection-to-binder"}; !reflect.DeepEqual(selectedNames, want) {
		t.Fatalf("selected names mismatch: got %v want %v", selectedNames, want)
	}
	if _, ok := selectedDefinitions["collection-to-binder"]; !ok {
		t.Fatal("expected selected transform definition to be present")
	}
	if _, ok := selectedDefinitions["cards-to-something"]; ok {
		t.Fatal("did not expect non-matching source transform definition")
	}
}

// TestResolveTransformsForSourceRejectsMissingSourceIndex verifies behavior for the related scenario.
func TestResolveTransformsForSourceRejectsMissingSourceIndex(t *testing.T) {
	t.Parallel()

	definitions := namedDefinitions{
		"bad-transform": json.RawMessage(`{
			"body": {"source":{"index":"collection"},"dest":{"index":"binder"}}
		}`),
	}
	_, _, err := resolveTransformsForSource(definitions, []string{"bad-transform"}, "collection")
	if err == nil {
		t.Fatal("expected missing source_index error")
	}
}

// TestResolveTransformsForSourceRejectsMissingBody verifies behavior for the related scenario.
func TestResolveTransformsForSourceRejectsMissingBody(t *testing.T) {
	t.Parallel()

	definitions := namedDefinitions{
		"bad-transform": json.RawMessage(`{
			"source_index": "collection"
		}`),
	}
	_, _, err := resolveTransformsForSource(definitions, []string{"bad-transform"}, "collection")
	if err == nil {
		t.Fatal("expected missing body error")
	}
}

// TestBuildMappingPreflightPlan verifies behavior for the related scenario.
func TestBuildMappingPreflightPlan(t *testing.T) {
	t.Parallel()

	path := writeTempJSON(t, t.TempDir(), `{
		"mappings": {
			"date_detection": false,
			"properties": {
				"notes": {"type":"keyword"},
				"quantity": {"type":"long"},
				"card": {"properties": {"name": {"type":"text"}}}
			}
		}
	}`)

	plan, err := buildMappingPreflightPlan(path, nil)
	if err != nil {
		t.Fatalf("buildMappingPreflightPlan returned error: %v", err)
	}
	if !plan.hasExpectations() {
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

// TestVerifyMappingPreflight verifies behavior for the related scenario.
func TestVerifyMappingPreflight(t *testing.T) {
	t.Parallel()

	plan := mappingPreflightPlan{
		DateDetectionExpected: boolPointer(false),
		RootFieldTypes: map[string]string{
			"notes": "keyword",
		},
	}

	t.Run("passes with expected date_detection and field types", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			if r.Method != http.MethodGet || r.URL.Path != "/target-index/_mapping" {
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"target-index":{"mappings":{"date_detection":false,"properties":{"notes":{"type":"keyword"}}}}}`))
		}))
		t.Cleanup(server.Close)

		es, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{server.URL}})
		if err != nil {
			t.Fatalf("new elasticsearch client: %v", err)
		}

		if err := verifyMappingPreflight(es, "target-index", plan); err != nil {
			t.Fatalf("verifyMappingPreflight returned error: %v", err)
		}
	})

	t.Run("fails when root field type mismatches", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			if r.Method != http.MethodGet || r.URL.Path != "/target-index/_mapping" {
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"target-index":{"mappings":{"date_detection":false,"properties":{"notes":{"type":"date"}}}}}`))
		}))
		t.Cleanup(server.Close)

		es, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{server.URL}})
		if err != nil {
			t.Fatalf("new elasticsearch client: %v", err)
		}

		err = verifyMappingPreflight(es, "target-index", plan)
		if err == nil {
			t.Fatal("expected mapping preflight error")
		}
		if !strings.Contains(err.Error(), `field "notes" has type "date", expected "keyword"`) {
			t.Fatalf("unexpected preflight error: %v", err)
		}
	})

	t.Run("fails when date_detection mismatches", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			if r.Method != http.MethodGet || r.URL.Path != "/target-index/_mapping" {
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"target-index":{"mappings":{"date_detection":true,"properties":{"notes":{"type":"keyword"}}}}}`))
		}))
		t.Cleanup(server.Close)

		es, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{server.URL}})
		if err != nil {
			t.Fatalf("new elasticsearch client: %v", err)
		}

		err = verifyMappingPreflight(es, "target-index", plan)
		if err == nil {
			t.Fatal("expected date_detection preflight error")
		}
		if !strings.Contains(err.Error(), "mappings.date_detection is true, expected false") {
			t.Fatalf("unexpected preflight error: %v", err)
		}
	})
}

// TestRunStartsTransformsAfterEnrichExecution verifies behavior for the related scenario.
func TestRunStartsTransformsAfterEnrichExecution(t *testing.T) {
	t.Parallel()

	var (
		mu         sync.Mutex
		operations []string
	)
	recordOp := func(op string) {
		mu.Lock()
		defer mu.Unlock()
		operations = append(operations, op)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		method := r.Method
		w.Header().Set("X-Elastic-Product", "Elasticsearch")

		switch {
		case method == http.MethodHead && path == "/collection":
			w.WriteHeader(http.StatusOK)
			return
		case method == http.MethodGet && path == "/_transform/collection-to-binder":
			recordOp("transform.get")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"count":0,"transforms":[]}`))
			return
		case method == http.MethodPost && path == "/_transform/collection-to-binder/_stop":
			recordOp("transform.stop")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"type":"resource_not_found_exception"}}`))
			return
		case method == http.MethodPut && path == "/_transform/collection-to-binder":
			recordOp("transform.put")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
			return
		case method == http.MethodPost && path == "/collection/_refresh":
			recordOp("index.refresh")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"_shards":{"total":1,"successful":1,"failed":0}}`))
			return
		case method == http.MethodGet && path == "/_enrich/policy":
			recordOp("enrich.list")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"policies":[{"config":{"match":{"name":"policy-a"}}}]}`))
			return
		case (method == http.MethodPut || method == http.MethodPost) && path == "/_enrich/policy/policy-a/_execute":
			recordOp("enrich.execute")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":{"phase":"COMPLETED"}}`))
			return
		case method == http.MethodPost && path == "/_transform/collection-to-binder/_start":
			recordOp("transform.start")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
			return
		default:
			t.Fatalf("unexpected request: %s %s", method, path)
		}
	}))
	t.Cleanup(server.Close)

	transformsPath := filepath.Join(t.TempDir(), "transforms.json")
	if err := os.WriteFile(transformsPath, []byte(`{
  "collection-to-binder": {
    "source_index": "collection",
    "body": {"source":{"index":"collection"},"dest":{"index":"binder"},"pivot":{"group_by":{"id":{"terms":{"field":"id"}}},"aggregations":{"quantity_total":{"sum":{"field":"quantity"}}}}}
  }
}`), 0o644); err != nil {
		t.Fatalf("write transforms fixture: %v", err)
	}

	_, err := Run(context.Background(), Options{
		URL:            server.URL,
		Index:          "collection",
		SyncManaged:    true,
		TransformsFile: transformsPath,
		Enrich: EnrichOptions{
			Enabled: true,
			All:     true,
		},
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(operations) == 0 {
		t.Fatal("expected transform and enrich operations to be recorded")
	}

	enrichIdx := -1
	transformPutIdx := -1
	startIdx := -1
	for i, op := range operations {
		if op == "enrich.execute" && enrichIdx == -1 {
			enrichIdx = i
		}
		if op == "transform.put" && transformPutIdx == -1 {
			transformPutIdx = i
		}
		if op == "transform.start" && startIdx == -1 {
			startIdx = i
		}
	}
	if enrichIdx == -1 {
		t.Fatalf("expected enrich.execute operation, got %v", operations)
	}
	if transformPutIdx == -1 {
		t.Fatalf("expected transform.put operation, got %v", operations)
	}
	if startIdx == -1 {
		t.Fatalf("expected transform.start operation, got %v", operations)
	}
	if transformPutIdx <= enrichIdx {
		t.Fatalf("expected transform.put after enrich.execute, got operations %v", operations)
	}
	if startIdx <= enrichIdx {
		t.Fatalf("expected transform.start after enrich.execute, got operations %v", operations)
	}
}

// TestRunAliasFirstCreateUpsertsTransformsAfterAliasUpdate verifies behavior for the related scenario.
func TestRunAliasFirstCreateUpsertsTransformsAfterAliasUpdate(t *testing.T) {
	t.Parallel()

	var (
		mu           sync.Mutex
		operations   []string
		aliasUpdated bool
		createdIndex string
	)
	recordOp := func(op string) {
		mu.Lock()
		defer mu.Unlock()
		operations = append(operations, op)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		method := r.Method
		path := r.URL.Path

		switch {
		case method == http.MethodGet && path == "/_alias/collection":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"type":"index_not_found_exception"}}`))
			return
		case method == http.MethodHead && path == "/collection":
			w.WriteHeader(http.StatusNotFound)
			return
		case method == http.MethodGet && path == "/_nodes":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"nodes":{"node1":{}}}`))
			return
		case method == http.MethodPut && strings.HasPrefix(path, "/collection-"):
			recordOp("index.create")
			createdIndex = strings.TrimPrefix(path, "/")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
			return
		case method == http.MethodHead && strings.HasPrefix(path, "/collection-"):
			if createdIndex != "" && path == "/"+createdIndex {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		case method == http.MethodPost && path == "/_bulk":
			recordOp("bulk")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"errors":false,"items":[{"index":{"_index":"collection","_id":"1","status":201}}]}`))
			return
		case method == http.MethodPost && path == "/_aliases":
			recordOp("alias.update")
			aliasUpdated = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
			return
		case method == http.MethodGet && path == "/_transform/collection-to-binder":
			recordOp("transform.get")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"count":0,"transforms":[]}`))
			return
		case method == http.MethodPost && path == "/_transform/collection-to-binder/_stop":
			recordOp("transform.stop")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"type":"resource_not_found_exception"}}`))
			return
		case method == http.MethodPut && path == "/_transform/collection-to-binder":
			recordOp("transform.put")
			if !aliasUpdated {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":{"type":"validation_exception","reason":"Validation Failed: 1: no such index [collection];"},"status":400}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
			return
		case method == http.MethodPost && path == "/_transform/collection-to-binder/_start":
			recordOp("transform.start")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
			return
		default:
			t.Fatalf("unexpected request: %s %s", method, path)
		}
	}))
	t.Cleanup(server.Close)

	tempDir := t.TempDir()
	dataPath := filepath.Join(tempDir, "data.json")
	if err := os.WriteFile(dataPath, []byte(`[{"id":"1","name":"card"}]`), 0o644); err != nil {
		t.Fatalf("write data fixture: %v", err)
	}
	transformsPath := filepath.Join(tempDir, "transforms.json")
	if err := os.WriteFile(transformsPath, []byte(`{
  "collection-to-binder": {
    "source_index": "collection",
    "body": {"source":{"index":"collection"},"dest":{"index":"binder"}}
  }
}`), 0o644); err != nil {
		t.Fatalf("write transforms fixture: %v", err)
	}

	_, err := Run(context.Background(), Options{
		URL:            server.URL,
		Index:          "collection",
		DataFile:       dataPath,
		DeleteIndex:    true,
		SyncManaged:    true,
		AliasMode:      true,
		TransformsFile: transformsPath,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	aliasIdx := firstOperationIndex(operations, "alias.update")
	transformPutIdx := firstOperationIndex(operations, "transform.put")
	if aliasIdx == -1 || transformPutIdx == -1 {
		t.Fatalf("expected alias.update and transform.put operations, got %v", operations)
	}
	if transformPutIdx <= aliasIdx {
		t.Fatalf("expected transform.put after alias.update, got operations %v", operations)
	}
}

// TestRunNonAliasCreateUpsertsTransformsAfterBulkLoad verifies behavior for the related scenario.
func TestRunNonAliasCreateUpsertsTransformsAfterBulkLoad(t *testing.T) {
	t.Parallel()

	var (
		mu         sync.Mutex
		operations []string
		indexReady bool
	)
	recordOp := func(op string) {
		mu.Lock()
		defer mu.Unlock()
		operations = append(operations, op)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		method := r.Method
		path := r.URL.Path

		switch {
		case method == http.MethodHead && path == "/collection":
			if indexReady {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		case method == http.MethodGet && path == "/_nodes":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"nodes":{"node1":{}}}`))
			return
		case method == http.MethodPut && path == "/collection":
			recordOp("index.create")
			indexReady = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
			return
		case method == http.MethodPost && path == "/_bulk":
			recordOp("bulk")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"errors":false,"items":[{"index":{"_index":"collection","_id":"1","status":201}}]}`))
			return
		case method == http.MethodGet && path == "/_transform/collection-to-binder":
			recordOp("transform.get")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"count":0,"transforms":[]}`))
			return
		case method == http.MethodPost && path == "/_transform/collection-to-binder/_stop":
			recordOp("transform.stop")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"type":"resource_not_found_exception"}}`))
			return
		case method == http.MethodPut && path == "/_transform/collection-to-binder":
			recordOp("transform.put")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
			return
		case method == http.MethodPost && path == "/_transform/collection-to-binder/_start":
			recordOp("transform.start")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
			return
		default:
			t.Fatalf("unexpected request: %s %s", method, path)
		}
	}))
	t.Cleanup(server.Close)

	tempDir := t.TempDir()
	dataPath := filepath.Join(tempDir, "data.json")
	if err := os.WriteFile(dataPath, []byte(`[{"id":"1","name":"card"}]`), 0o644); err != nil {
		t.Fatalf("write data fixture: %v", err)
	}
	transformsPath := filepath.Join(tempDir, "transforms.json")
	if err := os.WriteFile(transformsPath, []byte(`{
  "collection-to-binder": {
    "source_index": "collection",
    "body": {"source":{"index":"collection"},"dest":{"index":"binder"}}
  }
}`), 0o644); err != nil {
		t.Fatalf("write transforms fixture: %v", err)
	}

	_, err := Run(context.Background(), Options{
		URL:            server.URL,
		Index:          "collection",
		DataFile:       dataPath,
		AddToIndex:     true,
		SyncManaged:    true,
		TransformsFile: transformsPath,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	bulkIdx := firstOperationIndex(operations, "bulk")
	transformPutIdx := firstOperationIndex(operations, "transform.put")
	if bulkIdx == -1 || transformPutIdx == -1 {
		t.Fatalf("expected bulk and transform.put operations, got %v", operations)
	}
	if transformPutIdx <= bulkIdx {
		t.Fatalf("expected transform.put after bulk, got operations %v", operations)
	}
}

// TestRunCreatesPipelinesBeforeIndexWhenDefaultPipelineIsConfigured verifies behavior for the related scenario.
func TestRunCreatesPipelinesBeforeIndexWhenDefaultPipelineIsConfigured(t *testing.T) {
	t.Parallel()

	var (
		mu         sync.Mutex
		operations []string
		indexReady bool
	)
	recordOp := func(op string) {
		mu.Lock()
		defer mu.Unlock()
		operations = append(operations, op)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		method := r.Method
		path := r.URL.Path

		switch {
		case method == http.MethodHead && path == "/cards":
			if indexReady {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		case method == http.MethodPut && path == "/_ingest/pipeline/default-pipe":
			recordOp("pipeline.put")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
			return
		case method == http.MethodGet && path == "/_nodes":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"nodes":{"node1":{}}}`))
			return
		case method == http.MethodPut && path == "/cards":
			recordOp("index.create")
			indexReady = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
			return
		case method == http.MethodPost && path == "/_bulk":
			recordOp("bulk")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"errors":false,"items":[{"index":{"_index":"cards","_id":"1","status":201}}]}`))
			return
		default:
			t.Fatalf("unexpected request: %s %s", method, path)
		}
	}))
	t.Cleanup(server.Close)

	tempDir := t.TempDir()
	dataPath := filepath.Join(tempDir, "data.json")
	if err := os.WriteFile(dataPath, []byte(`[{"id":"1","name":"card"}]`), 0o644); err != nil {
		t.Fatalf("write data fixture: %v", err)
	}
	settingsPath := filepath.Join(tempDir, "settings.json")
	if err := os.WriteFile(settingsPath, []byte(`{"settings":{"index.default_pipeline":"default-pipe"}}`), 0o644); err != nil {
		t.Fatalf("write settings fixture: %v", err)
	}
	pipelinesPath := filepath.Join(tempDir, "pipelines.json")
	if err := os.WriteFile(pipelinesPath, []byte(`{
  "default-pipe": {
    "description": "default pipeline",
    "processors": []
  }
}`), 0o644); err != nil {
		t.Fatalf("write pipelines fixture: %v", err)
	}

	_, err := Run(context.Background(), Options{
		URL:           server.URL,
		Index:         "cards",
		DataFile:      dataPath,
		AddToIndex:    true,
		SyncManaged:   true,
		SettingsFile:  settingsPath,
		PipelinesFile: pipelinesPath,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	pipelineIdx := firstOperationIndex(operations, "pipeline.put")
	indexCreateIdx := firstOperationIndex(operations, "index.create")
	if pipelineIdx == -1 || indexCreateIdx == -1 {
		t.Fatalf("expected pipeline.put and index.create operations, got %v", operations)
	}
	if indexCreateIdx <= pipelineIdx {
		t.Fatalf("expected pipeline.put before index.create, got operations %v", operations)
	}
}

// TestRunRetriesBulkOnRetryableStatus verifies behavior for the related scenario.
func TestRunRetriesBulkOnRetryableStatus(t *testing.T) {
	t.Parallel()

	var bulkAttempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/cards":
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			bulkAttempts++
			if bulkAttempts == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":true,"message":"transient"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"errors":false,"items":[{"index":{"_index":"cards","_id":"1","status":201}}]}`))
			return
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	_, err := Run(context.Background(), Options{
		URL:        server.URL,
		Index:      "cards",
		DataFile:   writeBulkDataFixture(t),
		AddToIndex: true,
		BatchSize:  1,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if bulkAttempts != 2 {
		t.Fatalf("expected 2 bulk attempts, got %d", bulkAttempts)
	}
}

// TestRunRetriesBulkOnTransportFailure verifies behavior for the related scenario.
func TestRunRetriesBulkOnTransportFailure(t *testing.T) {
	t.Parallel()

	var bulkAttempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/cards":
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			bulkAttempts++
			if bulkAttempts == 1 {
				hijacker, ok := w.(http.Hijacker)
				if !ok {
					t.Fatal("response writer does not support hijacking")
				}
				conn, _, err := hijacker.Hijack()
				if err != nil {
					t.Fatalf("hijack returned error: %v", err)
				}
				_ = conn.Close()
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"errors":false,"items":[{"index":{"_index":"cards","_id":"1","status":201}}]}`))
			return
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	_, err := Run(context.Background(), Options{
		URL:        server.URL,
		Index:      "cards",
		DataFile:   writeBulkDataFixture(t),
		AddToIndex: true,
		BatchSize:  1,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if bulkAttempts != 2 {
		t.Fatalf("expected 2 bulk attempts, got %d", bulkAttempts)
	}
}

// TestRunDoesNotRetryBulkOnBadRequest verifies behavior for the related scenario.
func TestRunDoesNotRetryBulkOnBadRequest(t *testing.T) {
	t.Parallel()

	var bulkAttempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/cards":
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			bulkAttempts++
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":true,"message":"bad request"}`))
			return
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	_, err := Run(context.Background(), Options{
		URL:        server.URL,
		Index:      "cards",
		DataFile:   writeBulkDataFixture(t),
		AddToIndex: true,
		BatchSize:  1,
	})
	if err == nil {
		t.Fatal("expected Run to fail")
	}
	if bulkAttempts != 1 {
		t.Fatalf("expected 1 bulk attempt, got %d", bulkAttempts)
	}
}

// TestRunExhaustsRetriesOnRetryableStatus verifies behavior for the related scenario.
func TestRunExhaustsRetriesOnRetryableStatus(t *testing.T) {
	t.Parallel()

	var bulkAttempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/cards":
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			bulkAttempts++
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":true,"message":"too many requests"}`))
			return
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	_, err := Run(context.Background(), Options{
		URL:               server.URL,
		Index:             "cards",
		DataFile:          writeBulkDataFixture(t),
		AddToIndex:        true,
		BatchSize:         1,
		BulkRetryAttempts: 3,
	})
	if err == nil {
		t.Fatal("expected Run to fail")
	}
	if bulkAttempts != 3 {
		t.Fatalf("expected 3 bulk attempts, got %d", bulkAttempts)
	}
}

// TestRunRetryBackoffCapsAtConfiguredMax verifies behavior for the related scenario.
func TestRunRetryBackoffCapsAtConfiguredMax(t *testing.T) {
	previousSleep := sleepWithContext
	sleeps := make([]time.Duration, 0, 3)
	sleepWithContext = func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}
	t.Cleanup(func() {
		sleepWithContext = previousSleep
	})

	var bulkAttempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/cards":
			w.WriteHeader(http.StatusOK)
			return
		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			bulkAttempts++
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":true,"message":"transient"}`))
			return
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	_, err := Run(context.Background(), Options{
		URL:                  server.URL,
		Index:                "cards",
		DataFile:             writeBulkDataFixture(t),
		AddToIndex:           true,
		BatchSize:            1,
		BulkRetryAttempts:    4,
		BulkRetryBackoffBase: 200 * time.Millisecond,
		BulkRetryBackoffMax:  300 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected Run to fail")
	}
	if bulkAttempts != 4 {
		t.Fatalf("expected 4 bulk attempts, got %d", bulkAttempts)
	}
	wantSleeps := []time.Duration{200 * time.Millisecond, 300 * time.Millisecond, 300 * time.Millisecond}
	if !reflect.DeepEqual(sleeps, wantSleeps) {
		t.Fatalf("retry sleeps mismatch: got %v want %v", sleeps, wantSleeps)
	}
}

// writeBulkDataFixture centralizes this code path so package behavior stays consistent.
func writeBulkDataFixture(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "bulk.json")
	if err := os.WriteFile(path, []byte(`[{"id":"1","name":"card"}]`), 0o644); err != nil {
		t.Fatalf("write bulk fixture: %v", err)
	}
	return path
}

// firstOperationIndex centralizes this code path so package behavior stays consistent.
func firstOperationIndex(operations []string, operation string) int {
	for idx, current := range operations {
		if current == operation {
			return idx
		}
	}
	return -1
}

// TestSelectedDataAction verifies behavior for the related scenario.
func TestSelectedDataAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		addToIndex  bool
		flushIndex  bool
		deleteIndex bool
		want        dataAction
		wantErr     bool
	}{
		{name: "no action", want: dataActionNone},
		{name: "add", addToIndex: true, want: dataActionAdd},
		{name: "flush", flushIndex: true, want: dataActionFlush},
		{name: "delete", deleteIndex: true, want: dataActionDelete},
		{name: "add and flush conflict", addToIndex: true, flushIndex: true, wantErr: true},
		{name: "add and delete conflict", addToIndex: true, deleteIndex: true, wantErr: true},
		{name: "flush and delete conflict", flushIndex: true, deleteIndex: true, wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := selectedDataAction(tt.addToIndex, tt.flushIndex, tt.deleteIndex)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("selectedDataAction returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("selectedDataAction(%t, %t, %t) = %q, want %q", tt.addToIndex, tt.flushIndex, tt.deleteIndex, got, tt.want)
			}
		})
	}
}

// TestPipelineNamesReferencingPolicy verifies behavior for the related scenario.
func TestPipelineNamesReferencingPolicy(t *testing.T) {
	t.Parallel()

	definitions := namedDefinitions{
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

// TestPolicyDeleteBlockedByPipelineReference verifies behavior for the related scenario.
func TestPolicyDeleteBlockedByPipelineReference(t *testing.T) {
	t.Parallel()

	if !policyDeleteBlockedByPipelineReference([]byte(`{"error":{"reason":"Could not delete policy [slugs-by-name] because a pipeline is referencing it [enrich-from-slugs]"}}`)) {
		t.Fatal("expected pipeline reference conflict to be detected")
	}

	if policyDeleteBlockedByPipelineReference([]byte(`{"error":{"reason":"Could not delete policy [slugs-by-name]"}}`)) {
		t.Fatal("expected non-reference policy delete error to be ignored")
	}
}

// TestPipelineDeleteBlockedByDefaultIndex verifies behavior for the related scenario.
func TestPipelineDeleteBlockedByDefaultIndex(t *testing.T) {
	t.Parallel()

	if !pipelineDeleteBlockedByDefaultIndex([]byte(`{"error":{"reason":"pipeline [enrich-from-slugs] cannot be deleted because it is the default pipeline for 1 index(es) including [cards]"}}`)) {
		t.Fatal("expected default-pipeline delete conflict to be detected")
	}

	if pipelineDeleteBlockedByDefaultIndex([]byte(`{"error":{"reason":"pipeline [enrich-from-slugs] cannot be deleted"}}`)) {
		t.Fatal("expected unrelated pipeline delete error to be ignored")
	}
}

// TestBuildTimestampedIndexName verifies behavior for the related scenario.
func TestBuildTimestampedIndexName(t *testing.T) {
	t.Parallel()

	got := buildTimestampedIndexName("cards", time.Date(2026, time.March, 19, 13, 4, 59, 0, time.UTC))
	if got != "cards-20260319130459" {
		t.Fatalf("buildTimestampedIndexName mismatch: got %q want %q", got, "cards-20260319130459")
	}
}

// TestParseTimestampedIndexName verifies behavior for the related scenario.
func TestParseTimestampedIndexName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		alias  string
		index  string
		ok     bool
		expect string
	}{
		{
			name:   "valid",
			alias:  "cards",
			index:  "cards-20260319130459",
			ok:     true,
			expect: "2026-03-19T13:04:59Z",
		},
		{
			name:  "wrong alias",
			alias: "cards",
			index: "slugs-20260319130459",
			ok:    false,
		},
		{
			name:  "non numeric suffix",
			alias: "cards",
			index: "cards-20260319abc459",
			ok:    false,
		},
		{
			name:  "wrong width suffix",
			alias: "cards",
			index: "cards-2026031913045",
			ok:    false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := parseTimestampedIndexName(tt.alias, tt.index)
			if ok != tt.ok {
				t.Fatalf("parseTimestampedIndexName ok mismatch: got %t want %t", ok, tt.ok)
			}
			if !tt.ok {
				return
			}
			if got.UTC().Format(time.RFC3339) != tt.expect {
				t.Fatalf("parseTimestampedIndexName value mismatch: got %q want %q", got.UTC().Format(time.RFC3339), tt.expect)
			}
		})
	}
}

// TestNextAvailableTimestampedIndexNameWithCheck verifies behavior for the related scenario.
func TestNextAvailableTimestampedIndexNameWithCheck(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.March, 19, 13, 4, 59, 0, time.UTC)
	collisions := map[string]struct{}{
		"cards-20260319130459": {},
		"cards-20260319130500": {},
	}

	got, err := nextAvailableTimestampedIndexNameWithCheck("cards", base, func(candidate string) (bool, error) {
		_, exists := collisions[candidate]
		return exists, nil
	})
	if err != nil {
		t.Fatalf("nextAvailableTimestampedIndexNameWithCheck returned error: %v", err)
	}

	if got != "cards-20260319130501" {
		t.Fatalf("nextAvailableTimestampedIndexNameWithCheck mismatch: got %q want %q", got, "cards-20260319130501")
	}
}

// TestNextAvailableTimestampedIndexNameWithCheckPropagatesErrors verifies behavior for the related scenario.
func TestNextAvailableTimestampedIndexNameWithCheckPropagatesErrors(t *testing.T) {
	t.Parallel()

	_, err := nextAvailableTimestampedIndexNameWithCheck("cards", time.Date(2026, time.March, 19, 13, 4, 59, 0, time.UTC), func(candidate string) (bool, error) {
		return false, os.ErrPermission
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestManagedPolicyNameIsStableAndShortHashed verifies behavior for the related scenario.
func TestManagedPolicyNameIsStableAndShortHashed(t *testing.T) {
	t.Parallel()

	definition := json.RawMessage(`{"match":{"indices":"cards","match_field":"lookup_id","enrich_fields":["name"]}}`)
	got := managedPolicyName("cards-by-id", definition)
	if !managedPolicyNameMatchesLogical("cards-by-id", got) {
		t.Fatalf("expected managed policy name format logical-<6hex>, got %q", got)
	}
	if len(got) != len("cards-by-id-")+6 {
		t.Fatalf("managed policy name length mismatch: got %d", len(got))
	}
}

// TestRewritePipelinePolicyReferences verifies behavior for the related scenario.
func TestRewritePipelinePolicyReferences(t *testing.T) {
	t.Parallel()

	definitions := namedDefinitions{
		"pipeline-a": json.RawMessage(`{
			"processors": [
				{"enrich": {"policy_name": "cards-by-id", "field": "lookup_id", "target_field": "source"}}
			]
		}`),
	}
	rewritten := rewritePipelinePolicyReferences(definitions, []string{"pipeline-a"}, map[string]string{
		"cards-by-id": "cards-by-id-a1b2c3",
	})

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

// TestRemapEnrichSelectionMapsLogicalNames verifies behavior for the related scenario.
func TestRemapEnrichSelectionMapsLogicalNames(t *testing.T) {
	t.Parallel()

	enrich := &enrichFlagValue{enabled: true, raw: "cards-by-id,missing"}
	remapped := remapEnrichSelection(enrich, map[string]string{
		"cards-by-id": "cards-by-id-a1b2c3",
	})
	if remapped == nil {
		t.Fatal("expected remapped enrich selection")
	}
	if got := remapped.raw; got != "cards-by-id-a1b2c3,missing" {
		t.Fatalf("remapped raw mismatch: got %q", got)
	}
}

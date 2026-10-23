package loader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
)

// TestDeleteManagedResourcesUsesNukePipelineRecovery verifies that declared
// pipelines receive nuke's default-pipeline recovery behavior.
func TestDeleteManagedResourcesUsesNukePipelineRecovery(t *testing.T) {
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

	es := newStubClient(t, stub)

	deleteManagedResources(context.Background(), es, []string{"declared-pipeline"}, nil, nil, true)

	if got := deleteAttempts.Load(); got != 2 {
		t.Fatalf("delete attempts = %d, want 2", got)
	}
	if !clearedDefault.Load() {
		t.Fatal("expected nuke recovery to clear index.default_pipeline")
	}
}

// TestDeleteManagedResourcesNonNukeUsesOrdinaryPipelineDeletion verifies that
// normal cleanup does not invoke nuke's cross-index recovery requests.
func TestDeleteManagedResourcesNonNukeUsesOrdinaryPipelineDeletion(t *testing.T) {
	var deleteAttempts atomic.Int32
	stub := esstub.New(t)
	// Only the pipeline DELETE is registered: any recovery request is an
	// unexpected route and fails the test.
	stub.Handle("DELETE /_ingest/pipeline/declared-pipeline", func(w http.ResponseWriter, _ *http.Request) {
		deleteAttempts.Add(1)
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	})

	es := newStubClient(t, stub)

	deleteManagedResources(context.Background(), es, []string{"declared-pipeline"}, nil, nil, false)

	if got := deleteAttempts.Load(); got != 1 {
		t.Fatalf("delete attempts = %d, want 1", got)
	}
}

// TestIndexExistsUsesCallerContext verifies cancellation reaches an in-flight
// Elasticsearch existence request.
func TestIndexExistsUsesCallerContext(t *testing.T) {
	requestStarted := make(chan struct{})
	stub := esstub.New(t)
	stub.Handle("HEAD /cards", func(_ http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-r.Context().Done()
	})

	es := newStubClient(t, stub)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := indexExists(ctx, es, "cards")
		result <- err
	}()

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("existence request did not start")
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("indexExists error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("indexExists did not return promptly after cancellation")
	}
}

// TestWaitForIndexCancellationStopsPolling verifies cancellation during the
// polling delay prevents another existence request.
func TestWaitForIndexCancellationStopsPolling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var requests atomic.Int32
	stub := esstub.New(t)
	stub.Handle("HEAD /cards", func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		cancel()
		w.WriteHeader(http.StatusNotFound)
	})

	es := newStubClient(t, stub)

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		waitForIndex(ctx, es, "cards")
	}()

	recoveredErr, ok := recovered.(error)
	if !ok || !errors.Is(recoveredErr, context.Canceled) {
		t.Fatalf("waitForIndex panic = %#v, want context.Canceled", recovered)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("existence requests = %d, want 1", got)
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

	es := newStubClient(t, stub)

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		createPolicies(ctx, es, namedDefinitions{"policy": json.RawMessage(`{"match":{}}`)}, []string{"policy"})
	}()

	recoveredErr, ok := recovered.(error)
	if !ok || !errors.Is(recoveredErr, context.Canceled) {
		t.Fatalf("createPolicies panic = %#v, want context.Canceled", recovered)
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

	es := newStubClient(t, stub)
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

	es := newStubClient(t, stub)
	ctx, cancel := context.WithCancel(context.Background())

	var recovered any
	done := make(chan struct{})
	go func() {
		defer func() {
			recovered = recover()
			close(done)
		}()
		deletePipelines(ctx, es, []string{"pipeline-a"})
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

	recoveredErr, ok := recovered.(error)
	if !ok || !errors.Is(recoveredErr, context.Canceled) {
		t.Fatalf("deletePipelines panic = %#v, want context.Canceled", recovered)
	}
}

// TestExecuteEnrichPolicyCancellationStopsPolling verifies that cancelling the
// caller's context during the Tasks API poll wait stops the loop immediately
// instead of continuing to poll until the task completes.
func TestExecuteEnrichPolicyCancellationStopsPolling(t *testing.T) {
	// Not parallel: swaps sleepWithContext.
	ctx, cancel := context.WithCancel(context.Background())

	previousSleep := sleepWithContext
	sleepCalls := 0
	sleepWithContext = func(sleepCtx context.Context, _ time.Duration) error {
		sleepCalls++
		cancel()
		return sleepCtx.Err()
	}
	t.Cleanup(func() {
		sleepWithContext = previousSleep
	})

	var taskGetCalls atomic.Int32
	execute := esstub.JSON(http.StatusOK, `{"task":"node:123"}`)
	stub := esstub.New(t)
	stub.Handle("PUT /_enrich/policy/policy-a/_execute", execute)
	stub.Handle("POST /_enrich/policy/policy-a/_execute", execute)
	stub.Handle("GET /_tasks/node:123", func(w http.ResponseWriter, _ *http.Request) {
		taskGetCalls.Add(1)
		_, _ = w.Write([]byte(`{"completed":false,"task":{"status":{"phase":"RUNNING"}}}`))
	})

	es := newStubClient(t, stub)

	result := make(chan bool, 1)
	go func() {
		result <- executeEnrichPolicy(ctx, es, "policy-a", nil)
	}()

	select {
	case ok := <-result:
		if ok {
			t.Fatal("executeEnrichPolicy returned true, want false after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("executeEnrichPolicy did not return promptly after cancellation")
	}

	if sleepCalls != 1 {
		t.Fatalf("sleepWithContext calls = %d, want 1", sleepCalls)
	}
	if got := taskGetCalls.Load(); got != 0 {
		t.Fatalf("Tasks.Get calls = %d, want 0 (poll loop should stop before polling again)", got)
	}
}

// TestEnrichFromOptions verifies public option precedence and normalization.
// TestRunDoesNotMutateCallerOptions guards the documented Run contract that
// normalization happens on Run's private copy. Library callers reuse Options
// across runs; a pointer receiver or in-place map/slice edit would leak
// defaults (URL, BatchSize, INDEX template variable) back to them.
func TestRunDoesNotMutateCallerOptions(t *testing.T) {
	t.Parallel()
	replicas := 3
	tests := []struct {
		name string
		opts Options
	}{
		{
			name: "replicas pointer is read-only",
			opts: Options{
				URL:           "http://127.0.0.1:1",
				Index:         "products",
				SyncManaged:   true,
				PipelinesFile: filepath.Join(t.TempDir(), "missing-pipelines.json"),
				Replicas:      &replicas,
			},
		},
		{
			name: "invalid auth returns before client creation",
			opts: Options{
				Index:             "products",
				User:              "u",
				Pass:              "p",
				APIKey:            "k",
				TemplateVariables: map[string]string{"ENV": "test"},
				Enrich:            EnrichOptions{Policies: []string{"a", "b"}},
			},
		},
		{
			name: "fails after template variables are built",
			opts: Options{
				URL:               "http://127.0.0.1:1",
				Index:             "products",
				SyncManaged:       true,
				PipelinesFile:     filepath.Join(t.TempDir(), "missing-pipelines.json"),
				TemplateVariables: map[string]string{"ENV": "test", " SPACED ": "x"},
				Enrich:            EnrichOptions{Policies: []string{"a", "b"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			want := tt.opts
			want.TemplateVariables = maps.Clone(tt.opts.TemplateVariables)
			want.Enrich.Policies = slices.Clone(tt.opts.Enrich.Policies)
			// Copying Options copies the pointer, not the pointee: clone it so
			// DeepEqual compares the value Run could have written through.
			if tt.opts.Replicas != nil {
				cloned := *tt.opts.Replicas
				want.Replicas = &cloned
			}

			if _, err := Run(t.Context(), tt.opts); err == nil {
				t.Fatal("expected Run to fail for this fixture")
			}
			if !reflect.DeepEqual(tt.opts, want) {
				t.Fatalf("Run mutated caller options:\n got %+v\nwant %+v", tt.opts, want)
			}
		})
	}
}

func TestEnrichFromOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		options      EnrichOptions
		wantEnabled  bool
		wantAll      bool
		wantRaw      string
		wantPolicies []string
	}{
		{
			name:    "disabled raw is ignored",
			options: EnrichOptions{Raw: "policy-a"},
		},
		{
			name:        "enabled empty selection means all",
			options:     EnrichOptions{Enabled: true},
			wantEnabled: true,
			wantAll:     true,
		},
		{
			name:         "enabled raw selects explicit policies",
			options:      EnrichOptions{Enabled: true, Raw: " policy-a,policy-b, policy-a ,, policy-c "},
			wantEnabled:  true,
			wantRaw:      "policy-a,policy-b, policy-a ,, policy-c",
			wantPolicies: []string{"policy-a", "policy-b", "policy-c"},
		},
		{
			name:         "policies enable and override all and raw",
			options:      EnrichOptions{All: true, Raw: "ignored", Policies: []string{"policy-a", "policy-b"}},
			wantEnabled:  true,
			wantRaw:      "policy-a,policy-b",
			wantPolicies: []string{"policy-a", "policy-b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			enrich := enrichFromOptions(tt.options)
			if enrich.enabled != tt.wantEnabled {
				t.Fatalf("enabled = %t, want %t", enrich.enabled, tt.wantEnabled)
			}
			if enrich.all != tt.wantAll {
				t.Fatalf("all = %t, want %t", enrich.all, tt.wantAll)
			}
			if enrich.raw != tt.wantRaw {
				t.Fatalf("raw = %q, want %q", enrich.raw, tt.wantRaw)
			}
			if got := enrich.explicitPolicies(); !reflect.DeepEqual(got, tt.wantPolicies) {
				t.Fatalf("explicitPolicies() = %v, want %v", got, tt.wantPolicies)
			}
		})
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

// TestRunWarningLogIncludesTimestamp verifies behavior for the related scenario.
func TestRunWarningLogIncludesTimestamp(t *testing.T) {
	t.Parallel()

	// Run decorates injected loggers with a timestamp too; a missing one would
	// render as "<nil>" in console output.
	var output bytes.Buffer
	capture := zerolog.New(zerolog.ConsoleWriter{Out: &output}).Level(zerolog.TraceLevel)

	_, err := Run(context.Background(), Options{
		Logger:   &capture,
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

	definitions, names := readNamedDefinitions(t.Context(), tmp.Name(), "pipeline", nil)

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

	body := buildCreateIndexBody(t.Context(), nil, nil, wrappedSettings, rawMappings, "ignored-default", buildTemplateVariables("runtime-index", nil))

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

	definitions, names := readNamedDefinitions(t.Context(), path, "policy", buildTemplateVariables("runtime-index", nil))

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

// TestReadTemplatedFileRejectsUnresolvedVariables replaces the old passthrough
// contract: an unresolved ${NAME} must fail instead of reaching Elasticsearch
// as a literal that the cluster would accept as valid configuration.
func TestReadTemplatedFileRejectsUnresolvedVariables(t *testing.T) {
	t.Parallel()

	path := writeTempJSON(t, t.TempDir(), `{"value":"${UNKNOWN_VAR}"}`)

	content, err := readTemplatedFile(path, buildTemplateVariables("runtime-index", nil))
	if content != nil {
		t.Fatalf("content = %q, want nil on unresolved variable", content)
	}
	var unresolved *unresolvedTemplateError
	if !errors.As(err, &unresolved) {
		t.Fatalf("err = %v, want *unresolvedTemplateError", err)
	}
	if unresolved.Path != path || !slices.Equal(unresolved.Names, []string{"UNKNOWN_VAR"}) {
		t.Fatalf("error mismatch: path=%q names=%v", unresolved.Path, unresolved.Names)
	}
}

// TestExpandTemplate pins resolution order and one-pass expansion.
// Not parallel: t.Setenv forbids t.Parallel.
func TestExpandTemplate(t *testing.T) {
	t.Setenv("TMPL_ENV_ONLY", "from-env")
	t.Setenv("TMPL_BOTH", "env-value")
	t.Setenv("TMPL_ENV_EMPTY", "")
	t.Setenv("TMPL_MAP_EMPTY", "env-value")
	t.Setenv("HOME", "/should-not-appear")

	tests := []struct {
		name      string
		input     string
		variables templateVariables
		want      string
		wantNames []string
	}{
		{"map hit", `${A}`, templateVariables{"A": "1"}, "1", nil},
		{"env hit", `${TMPL_ENV_ONLY}`, nil, "from-env", nil},
		{"unresolved", `x${TMPL_NOPE_1}y`, nil, `x${TMPL_NOPE_1}y`, []string{"TMPL_NOPE_1"}},
		{"several unresolved sorted and deduplicated", `${TMPL_NOPE_B}${TMPL_NOPE_A}${TMPL_NOPE_B}`, nil,
			`${TMPL_NOPE_B}${TMPL_NOPE_A}${TMPL_NOPE_B}`, []string{"TMPL_NOPE_A", "TMPL_NOPE_B"}},
		{"no variables", `plain text`, nil, "plain text", nil},
		{"map beats env", `${TMPL_BOTH}`, templateVariables{"TMPL_BOTH": "map-value"}, "map-value", nil},
		{"empty map value beats env", `${TMPL_MAP_EMPTY}`, templateVariables{"TMPL_MAP_EMPTY": ""}, "", nil},
		{"empty env value resolves", `a${TMPL_ENV_EMPTY}b`, nil, "ab", nil},
		{"replacement is not rescanned", `${A}`, templateVariables{"A": "${B}", "B": "no"}, "${B}", nil},
		{"bare dollar and CRLF survive", "$HOME\r\n$5.00\r\n", nil, "$HOME\r\n$5.00\r\n", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, names := expandTemplate(tt.input, tt.variables)
			if got != tt.want {
				t.Fatalf("expanded = %q, want %q", got, tt.want)
			}
			if !slices.Equal(names, tt.wantNames) {
				t.Fatalf("unresolved = %v, want %v", names, tt.wantNames)
			}
		})
	}
}

// TestPreflightTemplateFilesSkipsUnreadablePaths guards that an empty or
// missing path neither errors here (the read sites own that classification)
// nor hides unresolved names in the paths after it.
func TestPreflightTemplateFilesSkipsUnreadablePaths(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.json")
	bad := writeTempJSON(t, dir, `{"v":"${PREFLIGHT_TYPO}"}`)
	good := writeTempJSON(t, dir, `{"v":"${INDEX}"}`)
	variables := buildTemplateVariables("idx", nil)

	if err := preflightTemplateFiles(variables, "", missing, good); err != nil {
		t.Fatalf("preflight with only missing/resolved paths returned %v", err)
	}

	err := preflightTemplateFiles(variables, missing, bad)
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("err = %v, want ErrInvalidOptions", err)
	}
	if !strings.Contains(err.Error(), "${PREFLIGHT_TYPO}") || !strings.Contains(err.Error(), bad) {
		t.Fatalf("error does not name variable and file: %v", err)
	}
}

// TestUnresolvedTemplateVariableFailsRunBeforeClusterChanges guards the
// fail-before-delete decision: settings and mappings are otherwise templated
// only at index-create time, after -delete, so without the preflight a typo
// would leave the cluster half-reset.
func TestUnresolvedTemplateVariableFailsRunBeforeClusterChanges(t *testing.T) {
	t.Parallel()
	const typoJSON = `{"v":"${TYPO_%s}"}`

	tests := []struct {
		name  string
		files map[string]string // Options field -> file content
		names []string
	}{
		{"settings", map[string]string{"settings": fmt.Sprintf(typoJSON, "S")}, []string{"${TYPO_S}"}},
		{"mappings", map[string]string{"mappings": fmt.Sprintf(typoJSON, "M")}, []string{"${TYPO_M}"}},
		{"pipelines", map[string]string{"pipelines": fmt.Sprintf(typoJSON, "P")}, []string{"${TYPO_P}"}},
		{"policies", map[string]string{"policies": fmt.Sprintf(typoJSON, "L")}, []string{"${TYPO_L}"}},
		{"transforms", map[string]string{"transforms": fmt.Sprintf(typoJSON, "T")}, []string{"${TYPO_T}"}},
		{"settings and pipelines together", map[string]string{
			"settings":  fmt.Sprintf(typoJSON, "A"),
			"pipelines": fmt.Sprintf(typoJSON, "B"),
		}, []string{"${TYPO_A}", "${TYPO_B}"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// No routes are registered: any request that reaches the
			// cluster is an unexpected route and fails the test.
			stub := esstub.New(t)
			dir := t.TempDir()
			opts := Options{
				URL:         stub.URL(),
				Index:       "products",
				DeleteIndex: true,
				SyncManaged: true,
				DataFile:    writeTempJSON(t, dir, `{"id":"1"}`),
			}
			wantPaths := []string{}
			for field, content := range tt.files {
				path := filepath.Join(dir, field+".json")
				if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
					t.Fatalf("WriteFile returned error: %v", err)
				}
				wantPaths = append(wantPaths, path)
				switch field {
				case "settings":
					opts.SettingsFile = path
				case "mappings":
					opts.MappingsFile = path
				case "pipelines":
					opts.PipelinesFile = path
				case "policies":
					opts.PoliciesFile = path
				case "transforms":
					opts.TransformsFile = path
				}
			}

			_, err := Run(t.Context(), opts)
			if !errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("err = %v, want ErrInvalidOptions", err)
			}
			for _, want := range append(tt.names, wantPaths...) {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not contain %q", err.Error(), want)
				}
			}
			// Files are reported in argument order (settings before pipelines),
			// not map or filesystem order.
			if len(tt.names) > 1 && strings.Index(err.Error(), tt.names[0]) > strings.Index(err.Error(), tt.names[1]) {
				t.Fatalf("error %q lists %q after %q, want argument order", err.Error(), tt.names[0], tt.names[1])
			}
			if got := requestLabels(stub); len(got) != 0 {
				t.Fatalf("requests reached the cluster before the error: %v", got)
			}
		})
	}
}

// TestSuppliedTemplateVariablesResolveThroughRun is the positive twin: a
// variable from Options.TemplateVariables passes validation and the resolved
// name reaches Elasticsearch.
func TestSuppliedTemplateVariablesResolveThroughRun(t *testing.T) {
	t.Parallel()

	stub := esstub.New(t)
	// Only the resolved pipeline PUT is expected; the run is allowed to fail
	// after it, so the assertion is on the resolved request path.
	stub.Handle("HEAD /products", esstub.JSON(http.StatusOK, ""))
	stub.Handle("PUT /_ingest/pipeline/p1", esstub.JSON(http.StatusOK, `{"acknowledged":true}`))
	requests := func() []string { return requestLabels(stub) }
	dir := t.TempDir()
	pipelines := writeTempJSON(t, dir, `{"${PIPE_NAME}":{"processors":[]}}`)

	_, err := Run(t.Context(), Options{
		URL:               stub.URL(),
		Index:             "products",
		SyncManaged:       true,
		PipelinesFile:     pipelines,
		TemplateVariables: map[string]string{"PIPE_NAME": "p1"},
	})
	if errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("supplied variable failed validation: %v", err)
	}
	if !slices.Contains(requests(), "PUT /_ingest/pipeline/p1") {
		t.Fatalf("PUT /_ingest/pipeline/p1 not seen; requests = %v (err = %v)", requests(), err)
	}
}

// TestMissingTemplateFileKeepsReadSiteClassification pins that the preflight
// leaves a missing file to the existing read site and its error kind.
func TestMissingTemplateFileKeepsReadSiteClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		set  func(opts *Options, missing string)
	}{
		{"settings", func(o *Options, p string) { o.SettingsFile = p }},
		{"mappings", func(o *Options, p string) { o.MappingsFile = p }},
		{"pipelines", func(o *Options, p string) { o.PipelinesFile = p }},
		{"policies", func(o *Options, p string) { o.PoliciesFile = p }},
		{"transforms", func(o *Options, p string) { o.TransformsFile = p }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// The run reaches the cluster before the read site fails for
			// settings and mappings, so the delete path is served.
			stub := esstub.New(t)
			stub.Handle("HEAD /products", esstub.JSON(http.StatusOK, ""))
			stub.Handle("DELETE /products", esstub.JSON(http.StatusOK, ackBody))
			stub.Handle("GET /_nodes", esstub.JSON(http.StatusOK, nodesBody))
			dir := t.TempDir()
			missing := filepath.Join(dir, "missing-"+tt.name+".json")

			opts := Options{
				URL:         stub.URL(),
				Index:       "products",
				DeleteIndex: true,
				SyncManaged: true,
				DataFile:    writeTempJSON(t, dir, `{"id":"1"}`),
			}
			tt.set(&opts, missing)

			_, err := Run(t.Context(), opts)
			if err == nil {
				t.Fatalf("expected an error for a missing %s file", tt.name)
			}
			if errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("err = %v: a missing file must not be reclassified as ErrInvalidOptions by the template preflight", err)
			}
			if !strings.Contains(err.Error(), missing) && tt.name != "settings" && tt.name != "mappings" {
				t.Fatalf("err = %v, want it to name %s", err, missing)
			}
		})
	}
}

// TestBracelessDollarNameIsNotSubstituted guards the decision that only the
// braced ${NAME} form is templated: a literal "$HOME" in a config asset must
// never be rewritten, even when HOME is in the variable map and environment.
// Not parallel: t.Setenv forbids t.Parallel.
func TestBracelessDollarNameIsNotSubstituted(t *testing.T) {
	t.Setenv("HOME", "/should-not-appear")

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"home", `{"v":"$HOME"}`, `{"v":"$HOME"}`},
		{"trailing dollar", `{"v":"foo$"}`, `{"v":"foo$"}`},
		{"currency", `{"v":"$5.00"}`, `{"v":"$5.00"}`},
		{"bare trailing dollar", `{"v":"$"}`, `{"v":"$"}`},
		{"braced still resolves", `{"v":"${INDEX}"}`, `{"v":"runtime-index"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variables := buildTemplateVariables("runtime-index", map[string]string{"HOME": "/also-should-not-appear"})
			path := writeTempJSON(t, t.TempDir(), tt.input)

			content, err := readTemplatedFile(path, variables)
			if err != nil {
				t.Fatalf("readTemplatedFile returned error: %v", err)
			}
			if got := string(content); got != tt.want {
				t.Fatalf("content mismatch: got %q want %q", got, tt.want)
			}
		})
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

	selectedDefinitions, selectedNames, err := resolveTransformsForSource(t.Context(), definitions, names, "collection")
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
	_, _, err := resolveTransformsForSource(t.Context(), definitions, []string{"bad-transform"}, "collection")
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
	_, _, err := resolveTransformsForSource(t.Context(), definitions, []string{"bad-transform"}, "collection")
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

	plan, err := buildMappingPreflightPlan(t.Context(), path, nil)
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

		es := newMappingResponseClient(t, "target-index", http.StatusOK, `{"target-index":{"mappings":{"date_detection":false,"properties":{"notes":{"type":"keyword"}}}}}`)

		if err := verifyMappingPreflight(t.Context(), es, "target-index", plan); err != nil {
			t.Fatalf("verifyMappingPreflight returned error: %v", err)
		}
	})

	t.Run("fails when root field type mismatches", func(t *testing.T) {
		t.Parallel()

		es := newMappingResponseClient(t, "target-index", http.StatusOK, `{"target-index":{"mappings":{"date_detection":false,"properties":{"notes":{"type":"date"}}}}}`)

		err := verifyMappingPreflight(t.Context(), es, "target-index", plan)
		if err == nil {
			t.Fatal("expected mapping preflight error")
		}
		if !strings.Contains(err.Error(), `field "notes" has type "date", expected "keyword"`) {
			t.Fatalf("unexpected preflight error: %v", err)
		}
	})

	t.Run("fails when date_detection mismatches", func(t *testing.T) {
		t.Parallel()

		es := newMappingResponseClient(t, "target-index", http.StatusOK, `{"target-index":{"mappings":{"date_detection":true,"properties":{"notes":{"type":"keyword"}}}}}`)

		err := verifyMappingPreflight(t.Context(), es, "target-index", plan)
		if err == nil {
			t.Fatal("expected date_detection preflight error")
		}
		if !strings.Contains(err.Error(), "mappings.date_detection is true, expected false") {
			t.Fatalf("unexpected preflight error: %v", err)
		}
	})
}

// TestVerifyMappingPreflightReportsEveryMismatchInFieldOrder guards the
// aggregated preflight error: every mismatch class (date_detection against the
// omitted-means-true default, missing fields, type mismatches) is reported in
// one error, date_detection first and then fields in sorted order. Repeating
// the call makes map-iteration nondeterminism fail the exact-string check.
func TestVerifyMappingPreflightReportsEveryMismatchInFieldOrder(t *testing.T) {
	t.Parallel()

	es := newMappingResponseClient(t, "target-index", http.StatusOK, `{"target-index":{"mappings":{"properties":{"beta":{"type":"text"}}}}}`)

	plan := mappingPreflightPlan{
		DateDetectionExpected: boolPointer(false),
		RootFieldTypes:        map[string]string{"gamma": "keyword", "alpha": "long", "beta": "keyword"},
	}
	want := `index "target-index" mappings do not match the mappings file: ` +
		`mappings.date_detection is true, expected false; ` +
		`required mapping for field "alpha" is missing; ` +
		`field "beta" has type "text", expected "keyword"; ` +
		`required mapping for field "gamma" is missing`

	for attempt := 0; attempt < 20; attempt++ {
		err := verifyMappingPreflight(t.Context(), es, "target-index", plan)
		if err == nil {
			t.Fatalf("attempt %d: expected mapping preflight error", attempt)
		}
		if got := err.Error(); got != want {
			t.Fatalf("attempt %d: preflight error mismatch\n got: %s\nwant: %s", attempt, got, want)
		}
	}
}

// TestVerifyMappingPreflightChecksEveryAliasTarget guards FIX-ALIASPRE-01:
// the preflight verifies every concrete index behind an alias and names each
// failing one in sorted order. It fails if the preflight goes back to checking
// one map-order entry, because the stale sibling would then pass or the
// reported index would vary between calls.
func TestVerifyMappingPreflightChecksEveryAliasTarget(t *testing.T) {
	t.Parallel()

	plan := mappingPreflightPlan{
		RootFieldTypes: map[string]string{"lookup_id": "keyword", "base_value": "integer"},
	}
	const good = `{"mappings":{"properties":{"lookup_id":{"type":"keyword"},"base_value":{"type":"integer"}}}}`
	const staleBase = `{"mappings":{"properties":{"lookup_id":{"type":"keyword"},"base_value":{"type":"keyword"}}}}`
	const missingLookup = `{"mappings":{"properties":{"base_value":{"type":"integer"}}}}`
	staleMsg := `index "cards-a" mappings do not match the mappings file: field "base_value" has type "keyword", expected "integer"`

	tests := []struct {
		name      string
		requested string
		body      string
		want      string
	}{
		{
			name:      "both targets match",
			requested: "cards",
			body:      `{"cards-a":` + good + `,"cards-b":` + good + `}`,
		},
		{
			name:      "one target stale",
			requested: "cards",
			body:      `{"cards-a":` + good + `,"cards-b":` + staleBase + `}`,
			want:      `alias "cards": index "cards-b" mappings do not match the mappings file: field "base_value" has type "keyword", expected "integer"`,
		},
		{
			name:      "both targets stale, sorted",
			requested: "cards",
			body:      `{"cards-b":` + missingLookup + `,"cards-a":` + staleBase + `}`,
			want: `alias "cards": ` + staleMsg +
				` | index "cards-b" mappings do not match the mappings file: required mapping for field "lookup_id" is missing`,
		},
		{
			name:      "single-target alias",
			requested: "cards",
			body:      `{"cards-a":` + staleBase + `}`,
			want:      `alias "cards": ` + staleMsg,
		},
		{
			name:      "concrete index",
			requested: "cards-a",
			body:      `{"cards-a":` + staleBase + `}`,
			want:      staleMsg,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			es := newMappingResponseClient(t, tt.requested, http.StatusOK, tt.body)
			for attempt := 0; attempt < 20; attempt++ {
				err := verifyMappingPreflight(t.Context(), es, tt.requested, plan)
				if tt.want == "" {
					if err != nil {
						t.Fatalf("attempt %d: unexpected error: %v", attempt, err)
					}
					continue
				}
				if err == nil {
					t.Fatalf("attempt %d: expected mapping preflight error", attempt)
				}
				if got := err.Error(); got != tt.want {
					t.Fatalf("attempt %d: preflight error mismatch\n got: %s\nwant: %s", attempt, got, tt.want)
				}
			}
		})
	}
}

// TestVerifyMappingPreflightChecksDateDetectionOnEveryAliasTarget guards that
// the date_detection rule is applied per target, not only the field types.
func TestVerifyMappingPreflightChecksDateDetectionOnEveryAliasTarget(t *testing.T) {
	t.Parallel()

	plan := mappingPreflightPlan{DateDetectionExpected: boolPointer(false)}
	body := `{"cards-a":{"mappings":{"date_detection":false}},"cards-b":{"mappings":{"date_detection":true}}}`
	es := newMappingResponseClient(t, "cards", http.StatusOK, body)

	err := verifyMappingPreflight(t.Context(), es, "cards", plan)
	want := `alias "cards": index "cards-b" mappings do not match the mappings file: mappings.date_detection is true, expected false`
	if err == nil || err.Error() != want {
		t.Fatalf("got %v, want %s", err, want)
	}
}

// TestFetchIndexMappingsRejectsMalformedResponses guards that every
// fetch-failure shape stays a fetch failure: never a nil error (a pass) and
// never a mismatch message. Two malformed entries must always name the same
// (first, sorted) one.
func TestFetchIndexMappingsRejectsMalformedResponses(t *testing.T) {
	t.Parallel()

	plan := mappingPreflightPlan{RootFieldTypes: map[string]string{"lookup_id": "keyword"}}
	tests := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"server error", http.StatusInternalServerError, `{"error":"boom"}`, `failed with status 500`},
		{"undecodable body", http.StatusOK, `not json`, `decoding mappings response`},
		{"empty response", http.StatusOK, `{}`, `was empty`},
		{"non-object entry", http.StatusOK, `{"cards-a":1}`, `"cards-a" did not include an object entry`},
		{"entry without mappings", http.StatusOK, `{"cards-a":{}}`, `"cards-a" did not include a mappings object`},
		{"two malformed entries name the first", http.StatusOK, `{"cards-z":1,"cards-b":{},"cards-a":2}`, `"cards-a" did not include an object entry`},
		{"concrete plus other index", http.StatusOK, `{"cards":{"mappings":{}},"cards-x":{"mappings":{}}}`, `included other indices: cards, cards-x`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			es := newMappingResponseClient(t, "cards", tt.status, tt.body)
			for attempt := 0; attempt < 20; attempt++ {
				_, err := fetchIndexMappings(es, "cards")
				if err == nil || !strings.Contains(err.Error(), tt.want) {
					t.Fatalf("attempt %d: fetchIndexMappings error = %v, want containing %q", attempt, err, tt.want)
				}
				err = verifyMappingPreflight(t.Context(), es, "cards", plan)
				if err == nil || !strings.Contains(err.Error(), tt.want) || strings.Contains(err.Error(), "do not match the mappings file") {
					t.Fatalf("attempt %d: verifyMappingPreflight error = %v, want fetch failure containing %q", attempt, err, tt.want)
				}
			}
		})
	}

	t.Run("transport error", func(t *testing.T) {
		t.Parallel()

		// Explicit early close: the connection must be refused so the
		// client reports a transport error, not an HTTP status.
		stub := esstub.New(t)
		es := newStubClient(t, stub)
		stub.Close()

		_, err := fetchIndexMappings(es, "cards")
		if err == nil || !strings.Contains(err.Error(), `fetching mappings for index "cards"`) {
			t.Fatalf("expected transport error, got %v", err)
		}
	})

	t.Run("response body read error", func(t *testing.T) {
		t.Parallel()

		stub := esstub.New(t)
		stub.Handle("GET /cards/_mapping", func(w http.ResponseWriter, _ *http.Request) {
			// Declare more bytes than are sent so the body read fails.
			w.Header().Set("Content-Length", "1000")
			_, _ = w.Write([]byte(`{"cards-a":`))
		})
		es := newStubClient(t, stub)

		err := verifyMappingPreflight(t.Context(), es, "cards", plan)
		if err == nil || !strings.Contains(err.Error(), `reading mappings response for index "cards"`) {
			t.Fatalf("expected body read error, got %v", err)
		}
	})
}

// TestVerifyMappingPreflightLogsAliasAndIndexFields guards the alias/index
// noun rule for the passing-target log: an alias request logs both fields, a
// concrete request logs only index.
func TestVerifyMappingPreflightLogsAliasAndIndexFields(t *testing.T) {
	t.Parallel()

	plan := mappingPreflightPlan{RootFieldTypes: map[string]string{"lookup_id": "keyword"}}
	const good = `{"mappings":{"properties":{"lookup_id":{"type":"keyword"}}}}`

	capture := func(requested, body string) []map[string]any {
		var output bytes.Buffer
		logger := zerolog.New(&output).Level(zerolog.TraceLevel)
		es := newMappingResponseClient(t, requested, http.StatusOK, body)
		if err := verifyMappingPreflight(withRunLogger(t.Context(), &logger), es, requested, plan); err != nil {
			t.Fatalf("verifyMappingPreflight returned error: %v", err)
		}
		var entries []map[string]any
		for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
			var entry map[string]any
			if err := json.Unmarshal([]byte(line), &entry); err != nil {
				t.Fatalf("decoding log line %q: %v", line, err)
			}
			entries = append(entries, entry)
		}
		return entries
	}

	aliasEntries := capture("cards", `{"cards-a":`+good+`,"cards-b":`+good+`}`)
	if len(aliasEntries) != 2 {
		t.Fatalf("expected one passed log per target, got %v", aliasEntries)
	}
	for i, wantIndex := range []string{"cards-a", "cards-b"} {
		if aliasEntries[i]["index"] != wantIndex || aliasEntries[i]["alias"] != "cards" {
			t.Fatalf("alias log %d = %v, want index %q and alias %q", i, aliasEntries[i], wantIndex, "cards")
		}
	}

	concreteEntries := capture("cards-a", `{"cards-a":`+good+`}`)
	if len(concreteEntries) != 1 || concreteEntries[0]["index"] != "cards-a" {
		t.Fatalf("unexpected concrete log: %v", concreteEntries)
	}
	if _, ok := concreteEntries[0]["alias"]; ok {
		t.Fatalf("concrete request must not log an alias field: %v", concreteEntries[0])
	}
}

// preflightRunFixtures holds the shared fixture paths for the mapping
// preflight Run tests.
type preflightRunFixtures struct {
	data              string
	divergentMappings string
	matchingMappings  string
	malformedMappings string
	pipelines         string
	policies          string
}

// writePreflightRunFixtures writes the shared fixtures. The divergent mappings
// declare two root fields (pipeline_version, new_mapping_only_field) that the
// stub's live mapping lacks; the matching mappings declare only lookup_id.
func writePreflightRunFixtures(t *testing.T) preflightRunFixtures {
	t.Helper()
	dir := t.TempDir()
	return preflightRunFixtures{
		data:              writeTempJSON(t, dir, `[{"id":"1"}]`),
		divergentMappings: writeTempJSON(t, dir, `{"mappings":{"properties":{"lookup_id":{"type":"keyword"},"pipeline_version":{"type":"integer"},"new_mapping_only_field":{"type":"keyword"}}}}`),
		matchingMappings:  writeTempJSON(t, dir, `{"mappings":{"properties":{"lookup_id":{"type":"keyword"}}}}`),
		malformedMappings: writeTempJSON(t, dir, `{"mappings":`),
		pipelines:         writeTempJSON(t, dir, `{"alt-pipe":{"processors":[]}}`),
		policies:          writeTempJSON(t, dir, `{"alt-policy":{"match":{"indices":"${INDEX}","match_field":"lookup_id","enrich_fields":["x"]}}}`),
	}
}

const (
	preflightLiveMappings      = `{"cards":{"mappings":{"properties":{"lookup_id":{"type":"keyword"}}}}}`
	preflightAliasLiveMappings = `{"cards-20260101000000":{"mappings":{"properties":{"lookup_id":{"type":"keyword"}}}}}`
)

// assertOperationOrder fails unless every operation is present and each one is
// first recorded before the next. firstOperationIndex returns -1 for a missing
// operation, and -1 < n is not ordering evidence, so presence is checked first.
func assertOperationOrder(t *testing.T, ops []string, ordered ...string) {
	t.Helper()
	previous := -1
	for i, operation := range ordered {
		idx := firstOperationIndex(ops, operation)
		if idx == -1 {
			t.Fatalf("expected operation %q to be recorded, got %v", operation, ops)
		}
		if i > 0 && idx <= previous {
			t.Fatalf("expected %q after %q, got operations %v", operation, ordered[i-1], ops)
		}
		previous = idx
	}
}

// TestRunExistingIndexMappingMismatchFailsBeforeMutation guards the decision
// that an existing index is verified against the mappings file before -flush
// deletes documents and before -sync-managed upserts pipelines or policies.
// It fails if the check moves back after flushAndCheck or after the
// existing-index managed upserts, which left an emptied index (and changed
// managed resources) behind a failed run. A mapping fetch failure must stop
// the run at the same point.
func TestRunExistingIndexMappingMismatchFailsBeforeMutation(t *testing.T) {
	t.Parallel()

	fixtures := writePreflightRunFixtures(t)
	divergentFields := []string{"pipeline_version", "new_mapping_only_field"}
	fetchFailure := []string{`fetching mappings for index "cards" failed with status 500`}

	tests := []struct {
		name         string
		opts         Options
		stub         preflightStubConfig
		wantContains []string
	}{
		{
			name:         "flush",
			opts:         Options{FlushIndex: true, MappingsFile: fixtures.divergentMappings},
			stub:         preflightStubConfig{indexExists: true, liveMappings: preflightLiveMappings},
			wantContains: divergentFields,
		},
		{
			name: "flush sync-managed",
			opts: Options{
				FlushIndex: true, SyncManaged: true, MappingsFile: fixtures.divergentMappings,
				PipelinesFile: fixtures.pipelines, PoliciesFile: fixtures.policies,
			},
			stub:         preflightStubConfig{indexExists: true, liveMappings: preflightLiveMappings},
			wantContains: divergentFields,
		},
		{
			name: "add sync-managed",
			opts: Options{
				AddToIndex: true, SyncManaged: true, MappingsFile: fixtures.divergentMappings,
				PipelinesFile: fixtures.pipelines, PoliciesFile: fixtures.policies,
			},
			stub:         preflightStubConfig{indexExists: true, liveMappings: preflightLiveMappings},
			wantContains: divergentFields,
		},
		{
			name: "alias flush sync-managed",
			opts: Options{
				AliasMode: true, FlushIndex: true, SyncManaged: true, MappingsFile: fixtures.divergentMappings,
				PipelinesFile: fixtures.pipelines, PoliciesFile: fixtures.policies,
			},
			stub: preflightStubConfig{
				aliasTargets: []string{"cards-20260101000000"},
				liveMappings: preflightAliasLiveMappings,
			},
			wantContains: divergentFields,
		},
		{
			name: "flush sync-managed mapping fetch failure",
			opts: Options{
				FlushIndex: true, SyncManaged: true, MappingsFile: fixtures.matchingMappings,
				PipelinesFile: fixtures.pipelines, PoliciesFile: fixtures.policies,
			},
			stub:         preflightStubConfig{indexExists: true, mappingStatus: http.StatusInternalServerError},
			wantContains: fetchFailure,
		},
		{
			name: "add sync-managed mapping fetch failure",
			opts: Options{
				AddToIndex: true, SyncManaged: true, MappingsFile: fixtures.matchingMappings,
				PipelinesFile: fixtures.pipelines, PoliciesFile: fixtures.policies,
			},
			stub:         preflightStubConfig{indexExists: true, mappingStatus: http.StatusInternalServerError},
			wantContains: fetchFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stub := newPreflightRunStub(t, tt.stub)
			opts := tt.opts
			opts.URL = stub.url
			opts.Index = "cards"
			opts.DataFile = fixtures.data

			_, err := Run(t.Context(), opts)
			if err == nil {
				t.Fatal("expected Run to fail mapping preflight")
			}
			// The kind is pinned deliberately (spec Constraint "Keep the
			// classification"): the fatal message contains "bulk", so
			// classifyRunErrorKind yields ErrBulkFailure. Changing it is a
			// separate decision, not a side effect.
			if !errors.Is(err, ErrBulkFailure) {
				t.Fatalf("expected ErrBulkFailure, got %v", err)
			}
			for _, want := range tt.wantContains {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("expected error to contain %q, got %v", want, err)
				}
			}

			ops := stub.operations()
			if got := operationCount(ops, "mapping.get"); got != 1 {
				t.Fatalf("expected exactly one mapping.get, got %d in %v", got, ops)
			}
			if got := stub.mappingFetchTargets(); !slices.Equal(got, []string{"cards"}) {
				t.Fatalf("expected the existing-index check to target %q, got %v", "cards", got)
			}
			for _, mutation := range []string{"flush", "pipeline.put", "policy.put", "bulk", "index.create", "index.delete"} {
				if got := operationCount(ops, mutation); got != 0 {
					t.Fatalf("expected no %s before the preflight failure, got %d in %v", mutation, got, ops)
				}
			}
		})
	}
}

// TestRunAliasFlushFailsWhenAnyTargetMappingMismatches guards that any stale
// target behind an alias blocks -flush before any mutation: alias-mode flush
// empties every target, so verifying only one would let a stale sibling be
// emptied (FIX-ALIASPRE-01, spec AC 7, 8 and 10).
func TestRunAliasFlushFailsWhenAnyTargetMappingMismatches(t *testing.T) {
	t.Parallel()

	fixtures := writePreflightRunFixtures(t)
	stub := newPreflightRunStub(t, preflightStubConfig{
		aliasTargets: []string{"cards-20250101000000", "cards-20260101000000"},
		liveMappings: `{"cards-20250101000000":{"mappings":{"properties":{}}},` +
			`"cards-20260101000000":{"mappings":{"properties":{"lookup_id":{"type":"keyword"}}}}}`,
	})

	_, err := Run(t.Context(), Options{
		URL: stub.url, Index: "cards", DataFile: fixtures.data,
		AliasMode: true, FlushIndex: true, MappingsFile: fixtures.matchingMappings,
	})
	if err == nil {
		t.Fatal("expected Run to fail mapping preflight")
	}
	if !errors.Is(err, ErrBulkFailure) {
		t.Fatalf("expected ErrBulkFailure, got %v", err)
	}
	if want := `alias "cards": index "cards-20250101000000" mappings do not match`; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error to contain %q, got %v", want, err)
	}
	if bad := `index "cards-20260101000000" mappings do not match`; strings.Contains(err.Error(), bad) {
		t.Fatalf("matching target must not be reported, got %v", err)
	}

	ops := stub.operations()
	if got := operationCount(ops, "mapping.get"); got != 1 {
		t.Fatalf("expected exactly one mapping.get, got %d in %v", got, ops)
	}
	for _, mutation := range []string{"flush", "pipeline.put", "policy.put", "bulk", "index.create", "index.delete"} {
		if got := operationCount(ops, mutation); got != 0 {
			t.Fatalf("expected no %s before the preflight failure, got %d in %v", mutation, got, ops)
		}
	}
}

// TestRunExistingIndexVerifiesMappingsBeforeFlush guards the happy path of the
// early existing-index check: the mapping fetch precedes _delete_by_query, it
// runs once (not again before bulk), and the run succeeds. It covers -flush,
// -add and alias mode on an existing target so a second (post-create) check on
// any of those paths fails the single-check count. A run with no -mappings file
// has no expectations and must not fetch mappings at all.
func TestRunExistingIndexVerifiesMappingsBeforeFlush(t *testing.T) {
	t.Parallel()

	fixtures := writePreflightRunFixtures(t)

	tests := []struct {
		name         string
		opts         Options
		stub         preflightStubConfig
		noMappings   bool
		wantMappings int
		wantFlush    bool
	}{
		{name: "flush", opts: Options{FlushIndex: true}, stub: preflightStubConfig{indexExists: true, liveMappings: preflightLiveMappings}, wantMappings: 1, wantFlush: true},
		{name: "add", opts: Options{AddToIndex: true}, stub: preflightStubConfig{indexExists: true, liveMappings: preflightLiveMappings}, wantMappings: 1},
		{name: "alias flush", opts: Options{AliasMode: true, FlushIndex: true}, stub: preflightStubConfig{indexExists: true, aliasTargets: []string{"cards-20260101000000"}, liveMappings: preflightAliasLiveMappings}, wantMappings: 1, wantFlush: true},
		{name: "alias add", opts: Options{AliasMode: true, AddToIndex: true}, stub: preflightStubConfig{indexExists: true, aliasTargets: []string{"cards-20260101000000"}, liveMappings: preflightAliasLiveMappings}, wantMappings: 1},
		{name: "flush without mappings file", opts: Options{FlushIndex: true}, stub: preflightStubConfig{indexExists: true, liveMappings: preflightLiveMappings}, noMappings: true, wantFlush: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stub := newPreflightRunStub(t, tt.stub)
			opts := tt.opts
			opts.URL = stub.url
			opts.Index = "cards"
			opts.DataFile = fixtures.data
			if !tt.noMappings {
				opts.MappingsFile = fixtures.matchingMappings
			}

			if _, err := Run(t.Context(), opts); err != nil {
				t.Fatalf("Run returned error: %v", err)
			}

			ops := stub.operations()
			if got := operationCount(ops, "mapping.get"); got != tt.wantMappings {
				t.Fatalf("expected %d mapping.get, got %d in %v", tt.wantMappings, got, ops)
			}
			if got := operationCount(ops, "index.create"); got != 0 {
				t.Fatalf("expected no index.create on an existing target, got %d in %v", got, ops)
			}
			switch {
			case tt.wantMappings > 0 && tt.wantFlush:
				assertOperationOrder(t, ops, "mapping.get", "flush", "bulk")
			case tt.wantMappings > 0:
				assertOperationOrder(t, ops, "mapping.get", "bulk")
			case tt.wantFlush:
				assertOperationOrder(t, ops, "flush", "bulk")
			}
		})
	}
}

// TestRunCreatedIndexVerifiesMappingsOnceAfterCreate guards the created-index
// path: when the run creates the index (-delete, or -add/-flush with no
// existing index or alias target), the mapping check runs exactly once, after
// the create and before bulk, against the concrete index that was created. In
// alias mode that is the new timestamped index, not the alias name.
func TestRunCreatedIndexVerifiesMappingsOnceAfterCreate(t *testing.T) {
	t.Parallel()

	fixtures := writePreflightRunFixtures(t)

	tests := []struct {
		name  string
		opts  Options
		stub  preflightStubConfig
		alias bool
	}{
		{name: "delete existing", opts: Options{DeleteIndex: true}, stub: preflightStubConfig{indexExists: true}},
		{name: "flush missing", opts: Options{FlushIndex: true}, stub: preflightStubConfig{indexExists: false}},
		{name: "add missing", opts: Options{AddToIndex: true}, stub: preflightStubConfig{indexExists: false}},
		{name: "alias add missing target", opts: Options{AliasMode: true, AddToIndex: true}, alias: true},
		{name: "alias flush missing target", opts: Options{AliasMode: true, FlushIndex: true}, alias: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := tt.stub
			cfg.liveMappings = preflightLiveMappings
			stub := newPreflightRunStub(t, cfg)
			opts := tt.opts
			opts.URL = stub.url
			opts.Index = "cards"
			opts.DataFile = fixtures.data
			opts.MappingsFile = fixtures.matchingMappings

			_, err := Run(t.Context(), opts)
			if err != nil {
				t.Fatalf("Run returned error: %v", err)
			}

			ops := stub.operations()
			if got := operationCount(ops, "mapping.get"); got != 1 {
				t.Fatalf("expected exactly one mapping.get, got %d in %v", got, ops)
			}
			assertOperationOrder(t, ops, "index.create", "mapping.get", "bulk")

			created := stub.createdIndices()
			if len(created) != 1 {
				t.Fatalf("expected exactly one created index, got %v", created)
			}
			if got := stub.mappingFetchTargets(); !slices.Equal(got, created) {
				t.Fatalf("expected the mapping check to target the created index %v, got %v", created, got)
			}
			if tt.alias {
				if _, ok := parseTimestampedIndexName("cards", created[0]); !ok {
					t.Fatalf("expected a timestamped alias generation, got %q", created[0])
				}
				assertOperationOrder(t, ops, "bulk", "alias.update")
			} else if created[0] != "cards" {
				t.Fatalf("expected concrete index %q to be created, got %q", "cards", created[0])
			}
		})
	}
}

// TestRunMalformedMappingsFailsBeforeDelete guards that the mappings file is
// parsed before -nuke and the data action, so a malformed file fails the run
// before any index is deleted.
func TestRunMalformedMappingsFailsBeforeDelete(t *testing.T) {
	t.Parallel()

	fixtures := writePreflightRunFixtures(t)

	tests := []struct {
		name string
		opts Options
	}{
		{name: "delete", opts: Options{DeleteIndex: true}},
		{name: "nuke delete", opts: Options{Nuke: true, DeleteIndex: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stub := newPreflightRunStub(t, preflightStubConfig{indexExists: true, liveMappings: preflightLiveMappings})
			opts := tt.opts
			opts.URL = stub.url
			opts.Index = "cards"
			opts.DataFile = fixtures.data
			opts.MappingsFile = fixtures.malformedMappings

			_, err := Run(t.Context(), opts)
			if err == nil {
				t.Fatal("expected Run to fail on a malformed mappings file")
			}
			if !errors.Is(err, ErrLoaderExecution) {
				t.Fatalf("expected ErrLoaderExecution, got %v", err)
			}
			if !strings.Contains(err.Error(), "Parsing mappings file") {
				t.Fatalf("expected a mappings parse failure, got %v", err)
			}
			ops := stub.operations()
			if got := operationCount(ops, "index.delete"); got != 0 {
				t.Fatalf("expected no index.delete before the parse failure, got %d in %v", got, ops)
			}
		})
	}
}

// TestRunStartsTransformsAfterEnrichExecution verifies behavior for the related scenario.
func TestRunStartsTransformsAfterEnrichExecution(t *testing.T) {
	t.Parallel()

	stub := newLifecycleStub(t, lifecycleConfig{
		index:       "collection",
		indexExists: true,
		transforms:  []string{"collection-to-binder"},
		policies:    []string{"policy-a"},
	})

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
		URL:            stub.URL(),
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

	operations := stub.operations()
	if len(operations) == 0 {
		t.Fatal("expected transform and enrich operations to be recorded")
	}
	assertOperationOrder(t, operations, "enrich.execute", "transform.put")
	assertOperationOrder(t, operations, "enrich.execute", "transform.start")
}

// TestRunAliasFirstCreateUpsertsTransformsAfterAliasUpdate verifies behavior for the related scenario.
func TestRunAliasFirstCreateUpsertsTransformsAfterAliasUpdate(t *testing.T) {
	t.Parallel()

	stub := newLifecycleStub(t, lifecycleConfig{index: "collection", transforms: []string{"collection-to-binder"}})
	dataPath, transformsPath := writeCollectionFixtures(t, "data.json", `[{"id":"1","name":"card"}]`)

	_, err := Run(context.Background(), Options{
		URL:            stub.URL(),
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

	assertOperationOrder(t, stub.operations(), "alias.update", "transform.put")
}

// TestRunNDJSONAliasFirstCreateUpsertsTransformsAfterAliasUpdate verifies behavior for the related scenario.
func TestRunNDJSONAliasFirstCreateUpsertsTransformsAfterAliasUpdate(t *testing.T) {
	t.Parallel()

	stub := newLifecycleStub(t, lifecycleConfig{index: "collection", transforms: []string{"collection-to-binder"}})
	dataPath, transformsPath := writeCollectionFixtures(t, "data.ndjson", "{\"id\":\"1\",\"name\":\"card\"}\n")

	_, err := Run(context.Background(), Options{
		URL:            stub.URL(),
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

	assertOperationOrder(t, stub.operations(), "alias.update", "transform.put")
}

// TestRunNonAliasCreateUpsertsTransformsAfterBulkLoad verifies behavior for the related scenario.
func TestRunNonAliasCreateUpsertsTransformsAfterBulkLoad(t *testing.T) {
	t.Parallel()

	stub := newLifecycleStub(t, lifecycleConfig{index: "collection", transforms: []string{"collection-to-binder"}})
	dataPath, transformsPath := writeCollectionFixtures(t, "data.json", `[{"id":"1","name":"card"}]`)

	_, err := Run(context.Background(), Options{
		URL:            stub.URL(),
		Index:          "collection",
		DataFile:       dataPath,
		AddToIndex:     true,
		SyncManaged:    true,
		TransformsFile: transformsPath,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	assertOperationOrder(t, stub.operations(), "bulk", "transform.put")
}

// TestRunNDJSONNonAliasCreateUpsertsTransformsAfterBulkLoad verifies behavior for the related scenario.
func TestRunNDJSONNonAliasCreateUpsertsTransformsAfterBulkLoad(t *testing.T) {
	t.Parallel()

	stub := newLifecycleStub(t, lifecycleConfig{index: "collection", transforms: []string{"collection-to-binder"}})
	dataPath, transformsPath := writeCollectionFixtures(t, "data.ndjson", "{\"id\":\"1\",\"name\":\"card\"}\n")

	_, err := Run(context.Background(), Options{
		URL:            stub.URL(),
		Index:          "collection",
		DataFile:       dataPath,
		AddToIndex:     true,
		SyncManaged:    true,
		TransformsFile: transformsPath,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	assertOperationOrder(t, stub.operations(), "bulk", "transform.put")
}

// TestRunRefreshesIndexBeforeStartingTransforms verifies that a _refresh is issued
// against the write index before any batch transform is started, preventing the
// ES refresh-window race where a transform reads 0 docs from an unrefreshed index.
func TestRunRefreshesIndexBeforeStartingTransforms(t *testing.T) {
	t.Parallel()

	stub := newLifecycleStub(t, lifecycleConfig{index: "collection", transforms: []string{"collection-to-binder"}})
	dataPath, transformsPath := writeCollectionFixtures(t, "data.json", `[{"id":"1","name":"card"}]`)

	_, err := Run(context.Background(), Options{
		URL:            stub.URL(),
		Index:          "collection",
		DataFile:       dataPath,
		AddToIndex:     true,
		SyncManaged:    true,
		TransformsFile: transformsPath,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	assertOperationOrder(t, stub.operations(), "index.refresh", "transform.start")
}

// TestRunCreatesPipelinesBeforeIndexWhenDefaultPipelineIsConfigured verifies behavior for the related scenario.
func TestRunCreatesPipelinesBeforeIndexWhenDefaultPipelineIsConfigured(t *testing.T) {
	t.Parallel()

	stub := newPreflightRunStub(t, preflightStubConfig{})

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
		URL:           stub.URL(),
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

	assertOperationOrder(t, stub.operations(), "pipeline.put", "index.create")
}

// TestFlushAndCheckValidatesDeleteByQueryResponse verifies that a flush fails
// on shard failures or malformed responses while tolerating version conflicts.
func TestFlushAndCheckValidatesDeleteByQueryResponse(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{
			name: "empty failures succeeds",
			body: `{"version_conflicts":0,"failures":[]}`,
		},
		{
			name: "version conflicts remain accepted",
			body: `{"version_conflicts":2,"failures":[]}`,
		},
		{
			name:    "shard failure fails",
			body:    `{"failures":[{"shard":0,"reason":{"type":"illegal_state_exception","reason":"failed"}}]}`,
			wantErr: true,
		},
		{
			name:    "malformed success body fails",
			body:    `{`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			stub.Handle("POST /cards/_delete_by_query", func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query().Get("conflicts"); got != "proceed" {
					t.Errorf("conflicts query mismatch: got %q want %q", got, "proceed")
				}
				if got := r.URL.Query().Get("refresh"); got != "true" {
					t.Errorf("refresh query mismatch: got %q want %q", got, "true")
				}
				_, _ = w.Write([]byte(tt.body))
			})
			es := newStubClient(t, stub)

			if !tt.wantErr {
				flushAndCheck(t.Context(), es, "cards")
				return
			}

			runErr := recoverRunError(t, func() {
				flushAndCheck(t.Context(), es, "cards")
			})
			if runErr.Kind != ErrIndexOperation {
				t.Fatalf("error kind mismatch: got %v want %v", runErr.Kind, ErrIndexOperation)
			}
		})
	}
}

// TestBulkInsertRejectsUnsupportedDocumentBeforeRequest verifies that payload
// serialization failures stop the batch before Elasticsearch receives it.
func TestBulkInsertRejectsUnsupportedDocumentBeforeRequest(t *testing.T) {
	// The bulk route is registered so a request would be answered; the
	// assertion below is that none arrives at all.
	stub := esstub.New(t)
	stub.Handle("POST /_bulk", esstub.JSON(http.StatusOK, `{"errors":false,"items":[]}`))
	es := newStubClient(t, stub)

	runErr := recoverRunError(t, func() {
		bulkInsert(
			context.Background(),
			es,
			"cards",
			[]map[string]interface{}{
				{"id": "1"},
				{"id": "2", "unsupported": func() {}},
			},
			2,
			2,
			1,
			time.Millisecond,
			time.Millisecond,
			"id",
		)
	})
	if runErr.Kind != ErrBulkFailure {
		t.Fatalf("error kind mismatch: got %v want %v", runErr.Kind, ErrBulkFailure)
	}
	if got := len(stub.Requests()); got != 0 {
		t.Fatalf("bulk request count mismatch: got %d want 0", got)
	}
}

// TestRunRetriesBulkOnRetryableStatus verifies behavior for the related scenario.
func TestRunRetriesBulkOnRetryableStatus(t *testing.T) {
	t.Parallel()

	stub, bulkAttempts := newCardsBulkStub(t, bulkAfter(replyTransient503))

	_, err := Run(context.Background(), Options{
		URL:        stub.URL(),
		Index:      "cards",
		DataFile:   writeBulkDataFixture(t),
		AddToIndex: true,
		BatchSize:  1,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := bulkAttempts.Load(); got != 2 {
		t.Fatalf("expected 2 bulk attempts, got %d", got)
	}
}

// TestRunNDJSONRetriesBulkOnRetryableStatus verifies behavior for the related scenario.
func TestRunNDJSONRetriesBulkOnRetryableStatus(t *testing.T) {
	t.Parallel()

	stub, bulkAttempts := newCardsBulkStub(t, bulkAfter(replyTransient503))

	_, err := Run(context.Background(), Options{
		URL:        stub.URL(),
		Index:      "cards",
		DataFile:   writeBulkNDJSONFixture(t),
		AddToIndex: true,
		BatchSize:  1,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := bulkAttempts.Load(); got != 2 {
		t.Fatalf("expected 2 bulk attempts, got %d", got)
	}
}

// TestRunRetriesBulkOnTransportFailure verifies behavior for the related scenario.
func TestRunRetriesBulkOnTransportFailure(t *testing.T) {
	t.Parallel()

	stub, bulkAttempts := newCardsBulkStub(t, bulkAfter(func(w http.ResponseWriter) { dropConnection(t, w) }))

	_, err := Run(context.Background(), Options{
		URL:        stub.URL(),
		Index:      "cards",
		DataFile:   writeBulkDataFixture(t),
		AddToIndex: true,
		BatchSize:  1,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := bulkAttempts.Load(); got != 2 {
		t.Fatalf("expected 2 bulk attempts, got %d", got)
	}
}

// TestRunNDJSONRetriesBulkOnTransportFailure verifies behavior for the related scenario.
func TestRunNDJSONRetriesBulkOnTransportFailure(t *testing.T) {
	t.Parallel()

	stub, bulkAttempts := newCardsBulkStub(t, bulkAfter(func(w http.ResponseWriter) { dropConnection(t, w) }))

	_, err := Run(context.Background(), Options{
		URL:        stub.URL(),
		Index:      "cards",
		DataFile:   writeBulkNDJSONFixture(t),
		AddToIndex: true,
		BatchSize:  1,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := bulkAttempts.Load(); got != 2 {
		t.Fatalf("expected 2 bulk attempts, got %d", got)
	}
}

// TestRunDoesNotRetryBulkOnBadRequest verifies behavior for the related scenario.
func TestRunDoesNotRetryBulkOnBadRequest(t *testing.T) {
	t.Parallel()

	stub, bulkAttempts := newCardsBulkStub(t, bulkAlways(http.StatusBadRequest, `{"error":true,"message":"bad request"}`))

	_, err := Run(context.Background(), Options{
		URL:        stub.URL(),
		Index:      "cards",
		DataFile:   writeBulkDataFixture(t),
		AddToIndex: true,
		BatchSize:  1,
	})
	if err == nil {
		t.Fatal("expected Run to fail")
	}
	if got := bulkAttempts.Load(); got != 1 {
		t.Fatalf("expected 1 bulk attempt, got %d", got)
	}
}

// TestRunNDJSONDoesNotRetryBulkOnBadRequest verifies behavior for the related scenario.
func TestRunNDJSONDoesNotRetryBulkOnBadRequest(t *testing.T) {
	t.Parallel()

	stub, bulkAttempts := newCardsBulkStub(t, bulkAlways(http.StatusBadRequest, `{"error":true,"message":"bad request"}`))

	_, err := Run(context.Background(), Options{
		URL:        stub.URL(),
		Index:      "cards",
		DataFile:   writeBulkNDJSONFixture(t),
		AddToIndex: true,
		BatchSize:  1,
	})
	if err == nil {
		t.Fatal("expected Run to fail")
	}
	if got := bulkAttempts.Load(); got != 1 {
		t.Fatalf("expected 1 bulk attempt, got %d", got)
	}
}

// TestRunExhaustsRetriesOnRetryableStatus verifies behavior for the related scenario.
func TestRunExhaustsRetriesOnRetryableStatus(t *testing.T) {
	t.Parallel()

	stub, bulkAttempts := newCardsBulkStub(t, bulkAlways(http.StatusTooManyRequests, `{"error":true,"message":"too many requests"}`))

	_, err := Run(context.Background(), Options{
		URL:               stub.URL(),
		Index:             "cards",
		DataFile:          writeBulkDataFixture(t),
		AddToIndex:        true,
		BatchSize:         1,
		BulkRetryAttempts: 3,
	})
	if err == nil {
		t.Fatal("expected Run to fail")
	}
	if got := bulkAttempts.Load(); got != 3 {
		t.Fatalf("expected 3 bulk attempts, got %d", got)
	}
}

// TestRunNDJSONExhaustsRetriesOnRetryableStatus verifies behavior for the related scenario.
func TestRunNDJSONExhaustsRetriesOnRetryableStatus(t *testing.T) {
	t.Parallel()

	stub, bulkAttempts := newCardsBulkStub(t, bulkAlways(http.StatusTooManyRequests, `{"error":true,"message":"too many requests"}`))

	_, err := Run(context.Background(), Options{
		URL:               stub.URL(),
		Index:             "cards",
		DataFile:          writeBulkNDJSONFixture(t),
		AddToIndex:        true,
		BatchSize:         1,
		BulkRetryAttempts: 3,
	})
	if err == nil {
		t.Fatal("expected Run to fail")
	}
	if got := bulkAttempts.Load(); got != 3 {
		t.Fatalf("expected 3 bulk attempts, got %d", got)
	}
}

// TestRunRetryBackoffCapsAtConfiguredMax verifies behavior for the related scenario.
func TestRunRetryBackoffCapsAtConfiguredMax(t *testing.T) {
	// Not parallel: swaps sleepWithContext.
	previousSleep := sleepWithContext
	sleeps := make([]time.Duration, 0, 3)
	sleepWithContext = func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}
	t.Cleanup(func() {
		sleepWithContext = previousSleep
	})

	stub, bulkAttempts := newCardsBulkStub(t, bulkAlways(http.StatusServiceUnavailable, `{"error":true,"message":"transient"}`))

	_, err := Run(context.Background(), Options{
		URL:                  stub.URL(),
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
	if got := bulkAttempts.Load(); got != 4 {
		t.Fatalf("expected 4 bulk attempts, got %d", got)
	}
	wantSleeps := []time.Duration{200 * time.Millisecond, 300 * time.Millisecond, 300 * time.Millisecond}
	if !reflect.DeepEqual(sleeps, wantSleeps) {
		t.Fatalf("retry sleeps mismatch: got %v want %v", sleeps, wantSleeps)
	}
}

// TestRunNDJSONRetryBackoffCapsAtConfiguredMax verifies behavior for the related scenario.
func TestRunNDJSONRetryBackoffCapsAtConfiguredMax(t *testing.T) {
	// Not parallel: swaps sleepWithContext.
	previousSleep := sleepWithContext
	sleeps := make([]time.Duration, 0, 3)
	sleepWithContext = func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}
	t.Cleanup(func() {
		sleepWithContext = previousSleep
	})

	stub, bulkAttempts := newCardsBulkStub(t, bulkAlways(http.StatusServiceUnavailable, `{"error":true,"message":"transient"}`))

	_, err := Run(context.Background(), Options{
		URL:                  stub.URL(),
		Index:                "cards",
		DataFile:             writeBulkNDJSONFixture(t),
		AddToIndex:           true,
		BatchSize:            1,
		BulkRetryAttempts:    4,
		BulkRetryBackoffBase: 200 * time.Millisecond,
		BulkRetryBackoffMax:  300 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected Run to fail")
	}
	if got := bulkAttempts.Load(); got != 4 {
		t.Fatalf("expected 4 bulk attempts, got %d", got)
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

// recoverRunError invokes fn and returns the RunError it must panic with.
func recoverRunError(t *testing.T, fn func()) (runErr *RunError) {
	t.Helper()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected function to panic with RunError")
		}

		var ok bool
		runErr, ok = recovered.(*RunError)
		if !ok {
			t.Fatalf("panic type mismatch: got %T want *RunError", recovered)
		}
	}()

	fn()
	return nil
}

// writeBulkNDJSONFixture writes a minimal NDJSON data file for tests.
// Mirrors writeBulkDataFixture but in NDJSON format.
func writeBulkNDJSONFixture(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "bulk.ndjson")
	if err := os.WriteFile(path, []byte("{\"id\":\"1\",\"name\":\"card\"}\n"), 0o644); err != nil {
		t.Fatalf("write NDJSON bulk fixture: %v", err)
	}
	return path
}

// TestPeekFirstByte verifies behavior for the related scenario.
func TestPeekFirstByte(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    byte
		wantErr bool
	}{
		{name: "json array", content: `[{"id":"1"}]`, want: '['},
		{name: "ndjson", content: "{\"id\":\"1\"}\n", want: '{'},
		{name: "leading whitespace before array", content: "  \n[{\"id\":\"1\"}]", want: '['},
		{name: "leading whitespace before object", content: "\t {\"id\":\"1\"}", want: '{'},
		{name: "empty file", content: "", wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "data.txt")
			if err := os.WriteFile(path, []byte(tt.content), 0o644); err != nil {
				t.Fatalf("write test file: %v", err)
			}

			got, err := peekFirstByte(path)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("peekFirstByte returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("peekFirstByte(%q) = %q, want %q", path, got, tt.want)
			}
		})
	}
}

// TestCountNDJSONLines verifies behavior for the related scenario.
func TestCountNDJSONLines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    int
	}{
		{name: "empty", content: "", want: 0},
		{name: "one line", content: "{\"a\":1}\n", want: 1},
		{name: "two lines", content: "{\"a\":1}\n{\"b\":2}\n", want: 2},
		{name: "blank line between docs", content: "{\"a\":1}\n\n{\"b\":2}\n", want: 2},
		{name: "no trailing newline", content: "{\"a\":1}", want: 1},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "data.ndjson")
			if err := os.WriteFile(path, []byte(tt.content), 0o644); err != nil {
				t.Fatalf("write test file: %v", err)
			}

			got, err := countNDJSONLines(path)
			if err != nil {
				t.Fatalf("countNDJSONLines returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("countNDJSONLines(%q) = %d, want %d", path, got, tt.want)
			}
		})
	}
}

// TestLoadNDJSONProducesCorrectDocuments verifies behavior for the related scenario.
func TestLoadNDJSONProducesCorrectDocuments(t *testing.T) {
	t.Parallel()

	type bulkLine struct {
		Index map[string]string `json:"index"`
	}

	wantDocs := []string{
		`{"id":"1","name":"alpha","mana":1}`,
		`{"id":"2","name":"beta","mana":2}`,
		`{"id":"3","name":"gamma","mana":3}`,
	}

	dataPath := filepath.Join(t.TempDir(), "bulk.ndjson")
	if err := os.WriteFile(dataPath, []byte(strings.Join(wantDocs, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write NDJSON fixture: %v", err)
	}

	stub, _ := newCardsBulkStub(t, bulkAlways(http.StatusOK, `{"errors":false,"items":[{"index":{"_index":"cards","_id":"1","status":201}},{"index":{"_index":"cards","_id":"2","status":201}},{"index":{"_index":"cards","_id":"3","status":201}}]}`))

	_, err := Run(context.Background(), Options{
		URL:        stub.URL(),
		Index:      "cards",
		DataFile:   dataPath,
		AddToIndex: true,
		BatchSize:  10,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	// The body is read from the stub's by-value trace, after Run returned.
	var bulkBody []byte
	for _, request := range stub.Requests() {
		if request.Method == http.MethodPost && request.Path == "/_bulk" {
			bulkBody = request.Body
		}
	}

	lines := strings.Split(strings.TrimSpace(string(bulkBody)), "\n")
	if len(lines) != 6 {
		t.Fatalf("expected 6 NDJSON lines in bulk body, got %d", len(lines))
	}

	for i := 0; i < len(lines); i += 2 {
		var meta bulkLine
		if err := json.Unmarshal([]byte(lines[i]), &meta); err != nil {
			t.Fatalf("unmarshal bulk action line %d: %v", i, err)
		}
		if meta.Index == nil {
			t.Fatalf("expected index action on line %d, got %s", i, lines[i])
		}

		var gotDoc map[string]interface{}
		if err := json.Unmarshal([]byte(lines[i+1]), &gotDoc); err != nil {
			t.Fatalf("unmarshal bulk document line %d: %v", i+1, err)
		}
		var wantDoc map[string]interface{}
		if err := json.Unmarshal([]byte(wantDocs[i/2]), &wantDoc); err != nil {
			t.Fatalf("unmarshal expected document %d: %v", i/2, err)
		}
		if !reflect.DeepEqual(gotDoc, wantDoc) {
			t.Fatalf("document %d mismatch: got %v want %v", i/2, gotDoc, wantDoc)
		}
	}
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

// operationCount returns how many times operation was recorded.
func operationCount(operations []string, operation string) int {
	count := 0
	for _, current := range operations {
		if current == operation {
			count++
		}
	}
	return count
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

	got, err := nextAvailableTimestampedIndexNameWithCheck(t.Context(), "cards", base, func(candidate string) (bool, error) {
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

	_, err := nextAvailableTimestampedIndexNameWithCheck(t.Context(), "cards", time.Date(2026, time.March, 19, 13, 4, 59, 0, time.UTC), func(candidate string) (bool, error) {
		return false, os.ErrPermission
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestNextAvailableTimestampedIndexNameWithCheckStopsAtCollisionLimit verifies
// that the collision search performs no more than the documented number of checks.
func TestNextAvailableTimestampedIndexNameWithCheckStopsAtCollisionLimit(t *testing.T) {
	t.Parallel()

	nop := zerolog.Nop()
	ctx := withRunLogger(t.Context(), &nop)

	checks := 0
	_, err := nextAvailableTimestampedIndexNameWithCheck(ctx, "cards", time.Date(2026, time.March, 19, 13, 4, 59, 0, time.UTC), func(candidate string) (bool, error) {
		checks++
		return true, nil
	})
	if err == nil {
		t.Fatal("expected collision-limit error, got nil")
	}
	if checks != timestampedIndexCollisionLimit {
		t.Fatalf("existence checks = %d, want %d", checks, timestampedIndexCollisionLimit)
	}
}

// TestManagedPolicyNameIsStableAndShortHashed verifies behavior for the related scenario.
func TestManagedPolicyNameIsStableAndShortHashed(t *testing.T) {
	t.Parallel()

	definition := json.RawMessage(`{"match":{"indices":"cards","match_field":"lookup_id","enrich_fields":["name"]}}`)
	got := managedPolicyName(t.Context(), "cards-by-id", definition)
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
	rewritten := rewritePipelinePolicyReferences(t.Context(), definitions, []string{"pipeline-a"}, map[string]string{
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
	remapped := remapEnrichSelection(t.Context(), enrich, map[string]string{
		"cards-by-id": "cards-by-id-a1b2c3",
	})
	if remapped == nil {
		t.Fatal("expected remapped enrich selection")
	}
	if got := remapped.raw; got != "cards-by-id-a1b2c3,missing" {
		t.Fatalf("remapped raw mismatch: got %q", got)
	}
}

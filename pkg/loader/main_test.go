package loader

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/fixture"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/aliasops"
)

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

			selection := enrichFromOptions(tt.options)
			if selection.Enabled != tt.wantEnabled {
				t.Fatalf("enabled = %t, want %t", selection.Enabled, tt.wantEnabled)
			}
			if selection.All != tt.wantAll {
				t.Fatalf("all = %t, want %t", selection.All, tt.wantAll)
			}
			if selection.Raw != tt.wantRaw {
				t.Fatalf("raw = %q, want %q", selection.Raw, tt.wantRaw)
			}
			if got := selection.ExplicitPolicies(); !reflect.DeepEqual(got, tt.wantPolicies) {
				t.Fatalf("explicitPolicies() = %v, want %v", got, tt.wantPolicies)
			}
		})
	}
}

// TestRunWarningLogIncludesTimestamp guards that warnings logged through an injected logger carry a timestamp, because Run decorates every injected logger.
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

// TestUnresolvedTemplateVariableFailsRunBeforeClusterChanges guards the
// fail-before-delete decision: the strict template check aggregates every
// unresolved name across all supplied files before any other parsing, so a
// typo cannot leave the cluster half-reset.
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
				DataFile:    fixture.WriteTempJSON(t, dir, `{"id":"1"}`),
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
	pipelines := fixture.WriteTempJSON(t, dir, `{"${PIPE_NAME}":{"processors":[]}}`)

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
			// Every missing file now fails before cluster access for this
			// delete/sync run, so no routes are registered.
			stub := esstub.New(t)
			dir := t.TempDir()
			missing := filepath.Join(dir, "missing-"+tt.name+".json")

			opts := Options{
				URL:         stub.URL(),
				Index:       "products",
				DeleteIndex: true,
				SyncManaged: true,
				DataFile:    fixture.WriteTempJSON(t, dir, `{"id":"1"}`),
			}
			tt.set(&opts, missing)

			_, err := Run(t.Context(), opts)
			if err == nil {
				t.Fatalf("expected an error for a missing %s file", tt.name)
			}
			if n := len(stub.Requests()); n != 0 {
				t.Fatalf("missing %s file reached the cluster with %d requests", tt.name, n)
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
		data:              fixture.WriteTempJSON(t, dir, `[{"id":"1"}]`),
		divergentMappings: fixture.WriteTempJSON(t, dir, `{"mappings":{"properties":{"lookup_id":{"type":"keyword"},"pipeline_version":{"type":"integer"},"new_mapping_only_field":{"type":"keyword"}}}}`),
		matchingMappings:  fixture.WriteTempJSON(t, dir, `{"mappings":{"properties":{"lookup_id":{"type":"keyword"}}}}`),
		malformedMappings: fixture.WriteTempJSON(t, dir, `{"mappings":`),
		pipelines:         fixture.WriteTempJSON(t, dir, `{"alt-pipe":{"processors":[]}}`),
		policies:          fixture.WriteTempJSON(t, dir, `{"alt-policy":{"match":{"indices":"${INDEX}","match_field":"lookup_id","enrich_fields":["x"]}}}`),
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
				if _, ok := aliasops.ParseTimestampedIndexName("cards", created[0]); !ok {
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

// TestRunStartsTransformsAfterEnrichExecution guards the stage order: enrich executes before managed transforms start.
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

// TestRunAliasFirstCreateUpsertsTransformsAfterAliasUpdate guards that on the first alias-mode create, transforms are upserted after the alias is repointed.
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

// TestRunNDJSONAliasFirstCreateUpsertsTransformsAfterAliasUpdate guards the same alias-first ordering with an NDJSON data file.
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

// TestRunNonAliasCreateUpsertsTransformsAfterBulkLoad guards that a non-alias create upserts transforms only after the bulk load.
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

// TestRunNDJSONNonAliasCreateUpsertsTransformsAfterBulkLoad guards the same bulk-load ordering with an NDJSON data file.
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

// TestRunCreatesPipelinesBeforeIndexWhenDefaultPipelineIsConfigured guards that pipelines exist before the index is created when settings name a default pipeline.
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

// TestRunRetriesBulkOnRetryableStatus guards that a retryable HTTP status on a bulk request is retried.
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

// TestRunNDJSONRetriesBulkOnRetryableStatus guards the same retry rule with an NDJSON data file.
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

// TestRunRetriesBulkOnTransportFailure guards that a transport failure on a bulk request is retried.
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

// TestRunNDJSONRetriesBulkOnTransportFailure guards the same retry rule with an NDJSON data file.
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

// TestRunDoesNotRetryBulkOnBadRequest guards that a non-retryable bulk response (400) is not retried.
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

// TestRunNDJSONDoesNotRetryBulkOnBadRequest guards the same no-retry rule with an NDJSON data file.
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

// TestRunExhaustsRetriesOnRetryableStatus guards that bulk retries stop at the attempt limit and then fail the run.
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

// TestRunNDJSONExhaustsRetriesOnRetryableStatus guards the same attempt limit with an NDJSON data file.
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

// TestRunRetryBackoffCapsAtConfiguredMax guards that bulk retry waits grow from the base but never exceed the configured maximum.
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

// TestRunNDJSONRetryBackoffCapsAtConfiguredMax guards the same backoff cap with an NDJSON data file.
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

// writeBulkDataFixture writes a one-document JSON-array data file in a test temp directory and returns its path.
func writeBulkDataFixture(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "bulk.json")
	if err := os.WriteFile(path, []byte(`[{"id":"1","name":"card"}]`), 0o644); err != nil {
		t.Fatalf("write bulk fixture: %v", err)
	}
	return path
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

// TestLoadNDJSONProducesCorrectDocuments guards that an NDJSON data file is loaded as the expected bulk documents.
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

// firstOperationIndex returns the index of the first recorded operation equal to operation, or -1 when none matches.
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

// TestSelectedDataAction guards that selectedDataAction rejects more than one of -add, -flush and -delete, and selects none when none is set.
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

// TestRunBasicAuthHeader pins what Run sends on the wire for each auth mode.
// Credentials must reach the client verbatim (no trimming or escaping), and
// an anonymous run must send no Authorization header at all. Stub URLs carry
// no userinfo, so these rows say nothing about URL-embedded credentials.
func TestRunBasicAuthHeader(t *testing.T) {
	t.Parallel()
	basic := func(creds string) string { return "Basic " + base64.StdEncoding.EncodeToString([]byte(creds)) }
	tests := []struct {
		name       string
		user, pass string
		apiKey     string
		want       string // expected Authorization header; empty means absent
	}{
		{name: "basic", user: "u", pass: "p", want: basic("u:p")},
		{name: "anonymous", want: ""},
		{name: "api key only", apiKey: "k", want: "APIKey k"},
		{name: "verbatim spaces and colon", user: "us er", pass: "p:a ss", want: basic("us er:p:a ss")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stub := esstub.New(t)
			stub.Handle("HEAD /cards", esstub.JSON(http.StatusOK, ""))
			_, err := Run(t.Context(), Options{
				URL: stub.URL(), Index: "cards", SyncManaged: true,
				User: tt.user, Pass: tt.pass, APIKey: tt.apiKey,
			})
			if err != nil {
				t.Fatalf("Run returned error: %v", err)
			}
			requests := stub.Requests()
			if len(requests) != 1 {
				t.Fatalf("requests = %v, want exactly one", requests)
			}
			values := requests[0].Header.Values("Authorization")
			if tt.want == "" {
				if len(values) != 0 {
					t.Fatalf("Authorization = %v, want absent", values)
				}
				return
			}
			if len(values) != 1 || values[0] != tt.want {
				t.Fatalf("Authorization = %v, want [%q]", values, tt.want)
			}
		})
	}
}

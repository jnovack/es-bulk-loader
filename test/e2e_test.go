//go:build e2e

package e2e

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ─── Harness Types ─────────────────────────────────────────────────────────────

// testEnv is the per-run harness state: the repo root, the built loader binary, the Elasticsearch container and its URL, and an HTTP client.
type testEnv struct {
	repoRoot      string
	binaryPath    string
	containerName string
	esURL         string
	httpClient    *http.Client
}

// scenarioContext holds one scenario's index, pipeline and policy names, all prefixed so scenarios do not collide, and its fixture directories.
type scenarioContext struct {
	env *testEnv

	prefix string

	sourceIndex string
	targetIndex string

	sourcePipelinePrimary   string
	sourcePipelineSecondary string
	sourcePolicy            string
	sourcePolicyManaged     string

	altSourcePipelinePrimary   string
	altSourcePipelineSecondary string
	altSourcePolicy            string

	targetPipelinePrimary   string
	targetPipelineSecondary string

	fixturesRoot       string
	sourceBaseDir      string
	sourceSyncDir      string
	sourceReplaceDir   string
	sourceAppendDir    string
	sourceDivergentDir string
	targetReferenceDir string
}

// commandResult is the combined output and exit error of one loader invocation.
type commandResult struct {
	output string
	err    error
}

// scenario is one row of the end-to-end table: its CLI flags, the outcome it asserts, and the function that runs it.
type scenario struct {
	name   string
	flags  string
	checks string
	run    func(t *testing.T, ctx *scenarioContext)
}

// sharedEnv is the environment set up by TestMain and shared by every scenario.
var sharedEnv *testEnv

// ─── Test Entry Points ─────────────────────────────────────────────────────────

// TestMain starts the shared Elasticsearch container, runs the scenarios, and removes the container afterward.
func TestMain(m *testing.M) {
	env, err := setupTestEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e setup failed: %v\n", err)
		os.Exit(1)
	}
	sharedEnv = env

	code := m.Run()

	if err := teardownTestEnv(env); err != nil {
		fmt.Fprintf(os.Stderr, "e2e teardown failed: %v\n", err)
		if code == 0 {
			code = 1
		}
	}

	os.Exit(code)
}

// TestEndToEndScenarios runs each scenario table row against the shared container and asserts the outcome named in its checks field.
func TestEndToEndScenarios(t *testing.T) {
	scenarios := []scenario{
		{
			name:   "delete_sync-managed_enrich",
			flags:  "-delete -sync-managed -enrich",
			checks: "index recreated, pipelines and policies recreated, enrich executed, computed fields present",
			run:    scenarioDeleteSyncManagedEnrich,
		},
		{
			name:   "flush",
			flags:  "-flush",
			checks: "documents replaced, settings and mappings preserved, managed resources untouched",
			run:    scenarioFlush,
		},
		{
			name:   "flush_sync-managed",
			flags:  "-flush -sync-managed",
			checks: "documents replaced, settings and mappings preserved, pipelines and policies ensured",
			run:    scenarioFlushSyncManaged,
		},
		{
			name:   "add",
			flags:  "-add",
			checks: "documents appended, managed resources untouched",
			run:    scenarioAdd,
		},
		{
			name:   "add_sync-managed",
			flags:  "-add -sync-managed",
			checks: "documents appended, pipelines and policies ensured",
			run:    scenarioAddSyncManaged,
		},
		{
			name:   "delete_referenced_policy_fails",
			flags:  "-delete",
			checks: "safe failure when another pipeline still references the enrich policy",
			run:    scenarioDeleteReferencedPolicyFails,
		},
		{
			name:   "nuke",
			flags:  "-nuke",
			checks: "source index and managed resources removed, dependent index survives with default pipeline cleared",
			run:    scenarioNuke,
		},
		{
			name:   "nuke_delete_sync-managed",
			flags:  "-nuke -delete -sync-managed",
			checks: "full teardown first, then clean rebuild, dependent index preserved without default pipeline",
			run:    scenarioNukeDeleteSyncManaged,
		},
		{
			name:   "enrich_missing_policies",
			flags:  "-delete -enrich",
			checks: "warns cleanly when no policies exist and skips enrich execution",
			run:    scenarioEnrichMissingPolicies,
		},
		{
			name:   "enrich_explicit_unknown_policy",
			flags:  "-add -enrich=<known>,<missing>",
			checks: "warns for unknown policy names and still executes the known policy",
			run:    scenarioEnrichExplicitUnknownPolicy,
		},
		{
			name:   "alias_delete_creates_timestamped_index",
			flags:  "-delete -alias",
			checks: "creates timestamped concrete index and points alias to it",
			run:    scenarioAliasDeleteCreatesTimestampedIndex,
		},
		{
			name:   "alias_keep_last_prunes_oldest_after_third_run",
			flags:  "run -delete -alias -keep-last=2 three times",
			checks: "third run prunes the oldest timestamped index and keeps newest two",
			run:    scenarioAliasKeepLastPrunesOldestAfterThirdRun,
		},
		{
			// Appended last: the slice index sets each scenario's e2e-NN prefix.
			name:   "flush_sync-managed_divergent_mappings_fails",
			flags:  "-flush -sync-managed",
			checks: "fails before flush and managed upserts; names every divergent field; documents and managed resources untouched",
			run:    scenarioFlushSyncManagedDivergentMappingsFails,
		},
		{
			// Appended last: the slice index sets each scenario's e2e-NN prefix.
			name:   "unresolved_template_variable_fails_safely",
			flags:  "-delete",
			checks: "unresolved ${VAR} in settings fails before -delete; names the variable; index and documents untouched",
			run:    scenarioUnresolvedTemplateVariableFailsSafely,
		},
		{
			// Appended last: the slice index sets each scenario's e2e-NN prefix.
			name:   "alias_flush_stale_target_fails",
			flags:  "-flush -alias (alias with two targets)",
			checks: "fails before flush when any alias target's mappings mismatch; names that index; no documents removed",
			run:    scenarioAliasFlushStaleTargetFailsBeforeFlush,
		},
		{
			// Appended last: the slice index sets each scenario's e2e-NN prefix.
			name:   "alias_replicas_override_creation_only",
			flags:  "-delete -alias -replicas 2, then -add -alias with REPLICAS=3",
			checks: "created generation has number_of_replicas 2; a later add run (env override) does not change the existing index",
			run:    scenarioAliasReplicasOverrideIsCreationOnly,
		},
		{
			// Appended last: the slice index sets each scenario's e2e-NN prefix.
			name:   "delete_sync-managed_wait_transforms",
			flags:  "-delete -sync-managed -transforms <file> -wait-transforms",
			checks: "loader returns only after the batch transform completes; destination index holds its documents at exit with no retry",
			run:    scenarioDeleteSyncManagedWaitTransforms,
		},
		{
			// Appended last: the slice index sets each scenario's e2e-NN prefix.
			name:   "settings_malformed_fails_before_delete",
			flags:  "-delete (malformed settings file)",
			checks: "fails with the settings parse error before -delete; the seeded index and its two documents remain",
			run:    scenarioSettingsMalformedFailsBeforeDelete,
		},
		{
			// Appended last: the slice index sets each scenario's e2e-NN prefix.
			name:   "data_format_unknown_fails_before_delete",
			flags:  "-delete (data file that is neither a JSON array nor NDJSON)",
			checks: "fails with the data format error before -delete; the seeded index and its two documents remain",
			run:    scenarioDataFormatUnknownFailsBeforeDelete,
		},
		{
			// Appended last: the slice index sets each scenario's e2e-NN prefix.
			name:   "sync-managed_missing_index_fails",
			flags:  "-sync-managed (absent index, no data action, settings or mappings)",
			checks: "fails naming the index, declared resources and the remedy; no index, pipelines or policy versions are created",
			run:    scenarioSyncManagedMissingIndexFails,
		},
	}

	for i, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			t.Logf("flags: %s", sc.flags)
			t.Logf("checks: %s", sc.checks)

			ctx := newScenarioContext(t, fmt.Sprintf("e2e-%02d", i+1))
			ctx.cleanup(t)
			t.Cleanup(func() {
				ctx.cleanup(t)
			})
			sc.run(t, ctx)
		})
	}
}

// ─── Scenario Implementations ──────────────────────────────────────────────────

// scenarioDeleteSyncManagedEnrich runs a full -delete -sync-managed -enrich
// load, then re-executes the same managed policy on its own. It also guards
// FIX-ENRICH404-01 on the pinned cluster: the exact .enrich-<managed> alias
// is absent before the first execution, present after it, and moves to a new
// target set on re-execution; and the re-execution logs the loader's task-404
// verification diagnostic, proving that branch, not an explicit task status,
// decided the outcome. Exit 0 alone proves nothing here: a failed enrich
// policy does not fail the Run.
func scenarioDeleteSyncManagedEnrich(t *testing.T, ctx *scenarioContext) {
	// The isolated container and this first scenario mean no execution of
	// the managed policy has happened yet.
	if before := enrichAliasTargets(t, ctx, ctx.sourcePolicyManaged); before != nil {
		t.Fatalf("expected enrich alias for %s to be absent before the first execution, got %v", ctx.sourcePolicyManaged, before)
	}

	result := runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, true, ""))
	mustSucceed(t, result)

	// The CLI runs at the default Info level: a long enrich execution must be
	// visible without -level debug, not silent until the end.
	mustContain(t, result.output, "Executing enrich policy")
	mustContain(t, result.output, "Enrich policy execution succeeded")

	assertIndexExists(t, ctx, ctx.sourceIndex)
	assertDocCount(t, ctx, ctx.sourceIndex, 2)
	assertDefaultPipeline(t, ctx, ctx.sourceIndex, ctx.sourcePipelinePrimary)
	assertMappingField(t, ctx, ctx.sourceIndex, "lookup_id")
	assertPipelineExists(t, ctx, ctx.sourcePipelinePrimary)
	assertPipelineExists(t, ctx, ctx.sourcePipelineSecondary)
	assertPolicyExists(t, ctx, ctx.sourcePolicy)
	assertEnrichBackingIndexExists(t, ctx, ctx.sourcePolicy)

	doc := getDocument(t, ctx, ctx.sourceIndex, "A1")
	assertEqual(t, nestedString(doc, "_source", "source_label"), "alpha-21")
	assertEqual(t, nestedFloat(doc, "_source", "calculated_value"), 21)

	if !containsString(listPolicyNames(t, ctx), ctx.sourcePolicyManaged) {
		t.Fatalf("expected managed policy %q to be listed, got %v", ctx.sourcePolicyManaged, listPolicyNames(t, ctx))
	}
	first := enrichAliasTargets(t, ctx, ctx.sourcePolicyManaged)
	if len(first) == 0 {
		t.Fatalf("expected enrich alias for %s to have targets after the first execution", ctx.sourcePolicyManaged)
	}

	// Re-execute the same policy only: no data action, no redefinition.
	rerun := runLoader(t, ctx, nil, []string{
		"-url", ctx.env.esURL,
		"-index", ctx.sourceIndex,
		"-enrich=" + ctx.sourcePolicyManaged,
		"-level", "debug",
	})
	mustSucceed(t, rerun)
	mustContain(t, rerun.output, "Enrich policy task started")
	mustContain(t, rerun.output, "Enrich policy outcome verified via backing alias")
	mustContain(t, rerun.output, "Enrich policy execution succeeded")

	second := enrichAliasTargets(t, ctx, ctx.sourcePolicyManaged)
	if len(second) == 0 || slices.Equal(first, second) {
		t.Fatalf("expected enrich alias for %s to move on re-execution: before=%v after=%v\n%s", ctx.sourcePolicyManaged, first, second, rerun.output)
	}
}

// scenarioFlush loads the base data, then replaces the documents with -flush and no managed-resource sync.
func scenarioFlush(t *testing.T, ctx *scenarioContext) {
	mustSucceed(t, runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, true, "")))

	assertDefaultPipeline(t, ctx, ctx.sourceIndex, ctx.sourcePipelinePrimary)
	assertPipelineExists(t, ctx, ctx.sourcePipelinePrimary)

	result := runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceReplaceDir, "-flush", false, false, ""))
	mustSucceed(t, result)

	assertDocCount(t, ctx, ctx.sourceIndex, 1)
	assertDefaultPipeline(t, ctx, ctx.sourceIndex, ctx.sourcePipelinePrimary)
	assertMappingField(t, ctx, ctx.sourceIndex, "lookup_id")
	assertPipelineExists(t, ctx, ctx.sourcePipelinePrimary)
	assertPipelineMissing(t, ctx, ctx.altSourcePipelinePrimary)
	assertPolicyExists(t, ctx, ctx.sourcePolicy)
	assertPolicyMissing(t, ctx, ctx.altSourcePolicy)

	doc := getDocument(t, ctx, ctx.sourceIndex, "C3")
	assertEqual(t, nestedString(doc, "_source", "source_label"), "gamma-20")
	assertEqual(t, nestedFloat(doc, "_source", "calculated_value"), 20)
}

// scenarioFlushSyncManaged loads the base data, then flushes with -sync-managed and asserts the managed pipelines are present afterward.
func scenarioFlushSyncManaged(t *testing.T, ctx *scenarioContext) {
	mustSucceed(t, runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, true, "")))

	result := runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceSyncDir, "-flush", true, false, ""))
	mustSucceed(t, result)

	assertDocCount(t, ctx, ctx.sourceIndex, 1)
	assertDefaultPipeline(t, ctx, ctx.sourceIndex, ctx.sourcePipelinePrimary)
	assertPipelineExists(t, ctx, ctx.sourcePipelinePrimary)
	assertPipelineExists(t, ctx, ctx.altSourcePipelinePrimary)
	assertPolicyExists(t, ctx, ctx.sourcePolicy)
	assertPolicyExists(t, ctx, ctx.altSourcePolicy)

	doc := getDocument(t, ctx, ctx.sourceIndex, "C3")
	assertEqual(t, nestedString(doc, "_source", "source_label"), "gamma-20")
	assertMissingField(t, doc, "_source", "pipeline_version")
}

// scenarioAdd loads the base data, then appends replacement documents with -add.
func scenarioAdd(t *testing.T, ctx *scenarioContext) {
	mustSucceed(t, runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, false, "")))

	result := runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceReplaceDir, "-add", false, false, ""))
	mustSucceed(t, result)

	assertDocCount(t, ctx, ctx.sourceIndex, 3)
	assertPipelineMissing(t, ctx, ctx.altSourcePipelinePrimary)
	assertPolicyMissing(t, ctx, ctx.altSourcePolicy)

	doc := getDocument(t, ctx, ctx.sourceIndex, "C3")
	assertEqual(t, nestedString(doc, "_source", "source_label"), "gamma-20")
}

// scenarioAddSyncManaged loads the base data, then appends a document with -add and -sync-managed and asserts the managed alt-source resources exist.
func scenarioAddSyncManaged(t *testing.T, ctx *scenarioContext) {
	mustSucceed(t, runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, false, "")))

	result := runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceSyncDir, "-add", true, false, ""))
	mustSucceed(t, result)

	assertDocCount(t, ctx, ctx.sourceIndex, 3)
	assertPipelineExists(t, ctx, ctx.altSourcePipelinePrimary)
	assertPolicyExists(t, ctx, ctx.altSourcePolicy)

	doc := getDocument(t, ctx, ctx.sourceIndex, "C3")
	assertEqual(t, nestedString(doc, "_source", "source_label"), "gamma-20")
}

// scenarioDeleteReferencedPolicyFails asserts that a -delete run fails when the enrich policy it would remove is still referenced by a pipeline.
func scenarioDeleteReferencedPolicyFails(t *testing.T, ctx *scenarioContext) {
	mustSucceed(t, runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, true, "")))
	mustSucceed(t, runLoader(t, ctx, nil, targetArgs(ctx, "-delete", true)))

	result := runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, false, ""))
	mustFailWith(t, result, "Failed to delete enrich policy")
	mustContain(t, result.output, "referencing it")

	assertIndexMissing(t, ctx, ctx.sourceIndex)
	assertPipelineMissing(t, ctx, ctx.sourcePipelinePrimary)
	assertPolicyExists(t, ctx, ctx.sourcePolicy)
	assertPipelineExists(t, ctx, ctx.targetPipelinePrimary)
}

// scenarioNuke loads the source and target indices, then runs -nuke against the source index.
func scenarioNuke(t *testing.T, ctx *scenarioContext) {
	mustSucceed(t, runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, true, "")))
	mustSucceed(t, runLoader(t, ctx, nil, targetArgs(ctx, "-delete", true)))

	result := runLoader(t, ctx, nil, nukeArgs(ctx))
	mustSucceed(t, result)

	assertIndexMissing(t, ctx, ctx.sourceIndex)
	assertPolicyMissing(t, ctx, ctx.sourcePolicy)
	assertPipelineMissing(t, ctx, ctx.sourcePipelinePrimary)
	assertPipelineMissing(t, ctx, ctx.targetPipelinePrimary)
	assertIndexExists(t, ctx, ctx.targetIndex)
	assertNoDefaultPipeline(t, ctx, ctx.targetIndex)
}

// scenarioNukeDeleteSyncManaged loads the source and target indices, then runs -nuke with -delete and -sync-managed against the source index.
func scenarioNukeDeleteSyncManaged(t *testing.T, ctx *scenarioContext) {
	mustSucceed(t, runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, true, "")))
	mustSucceed(t, runLoader(t, ctx, nil, targetArgs(ctx, "-delete", true)))

	result := runLoader(t, ctx, nil, sourceArgsWithNuke(ctx, ctx.sourceBaseDir, "-delete", true, true, ""))
	mustSucceed(t, result)

	assertIndexExists(t, ctx, ctx.sourceIndex)
	assertDocCount(t, ctx, ctx.sourceIndex, 2)
	assertPipelineExists(t, ctx, ctx.sourcePipelinePrimary)
	assertPolicyExists(t, ctx, ctx.sourcePolicy)
	assertEnrichBackingIndexExists(t, ctx, ctx.sourcePolicy)
	assertPipelineMissing(t, ctx, ctx.targetPipelinePrimary)
	assertIndexExists(t, ctx, ctx.targetIndex)
	assertNoDefaultPipeline(t, ctx, ctx.targetIndex)
}

// scenarioEnrichMissingPolicies asserts that -enrich against a cluster with no enrich policies warns ("No enrich policies found") and succeeds, leaving the index and its documents in place.
func scenarioEnrichMissingPolicies(t *testing.T, ctx *scenarioContext) {
	result := runLoader(t, ctx, nil, sourceArgsNoPolicies(ctx, "-delete", false, true, ""))
	mustSucceed(t, result)
	mustContain(t, result.output, "No enrich policies found; skipping enrich execution")

	assertIndexExists(t, ctx, ctx.sourceIndex)
	assertDocCount(t, ctx, ctx.sourceIndex, 2)
	assertPolicyMissing(t, ctx, ctx.sourcePolicy)
}

// scenarioEnrichExplicitUnknownPolicy asserts that an unknown policy named in -enrich is skipped with a warning and the run still succeeds.
func scenarioEnrichExplicitUnknownPolicy(t *testing.T, ctx *scenarioContext) {
	mustSucceed(t, runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, true, "")))

	result := runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceAppendDir, "-add", false, true, ctx.sourcePolicy+",missing-policy"))
	mustSucceed(t, result)
	mustContain(t, result.output, "Enrich policy not found; skipping")
	mustContain(t, result.output, ctx.sourcePolicy)

	assertDocCount(t, ctx, ctx.sourceIndex, 3)
	assertEnrichBackingIndexExists(t, ctx, ctx.sourcePolicy)
}

// scenarioAliasDeleteCreatesTimestampedIndex asserts that -delete -alias without -sync-managed warns that it is assuming -sync-managed, then rolls to a timestamped index.
func scenarioAliasDeleteCreatesTimestampedIndex(t *testing.T, ctx *scenarioContext) {
	args := append(sourceArgsNoPolicies(ctx, "-delete", false, false, ""), "-alias")
	result := runLoader(t, ctx, nil, args)
	mustSucceed(t, result)
	mustContain(t, result.output, "Alias delete detected without -sync-managed; assuming -sync-managed for this run")
	mustContain(t, result.output, "Alias delete detected without -keep-last; no old timestamped indices will be deleted and storage usage can grow over time.")

	targets := aliasTargets(t, ctx, ctx.sourceIndex)
	if len(targets) != 1 {
		t.Fatalf("expected alias %q to have exactly one target, got %v", ctx.sourceIndex, targets)
	}

	assertTimestampedIndexName(t, ctx.sourceIndex, targets[0])
	assertIndexExists(t, ctx, targets[0])
	assertDocCount(t, ctx, ctx.sourceIndex, 2)
}

// scenarioAliasReplicasOverrideIsCreationOnly proves -replicas reaches the
// created alias generation, and that an override on a later -add run never
// rewrites the existing index (the override applies at creation only).
func scenarioAliasReplicasOverrideIsCreationOnly(t *testing.T, ctx *scenarioContext) {
	args := append(sourceArgsNoPolicies(ctx, "-delete", false, false, ""), "-alias", "-replicas", "2")
	mustSucceed(t, runLoader(t, ctx, nil, args))

	generation := singleAliasTarget(t, ctx, ctx.sourceIndex)
	assertReplicaSetting(t, ctx, generation, "2")

	addArgs := append(sourceArgsNoPolicies(ctx, "-add", false, false, ""), "-alias")
	mustSucceed(t, runLoader(t, ctx, map[string]string{"REPLICAS": "3"}, addArgs))
	assertReplicaSetting(t, ctx, generation, "2")
}

// assertReplicaSetting fails unless the index's live number_of_replicas equals want.
func assertReplicaSetting(t *testing.T, ctx *scenarioContext, index, want string) {
	t.Helper()
	var response map[string]map[string]map[string]map[string]string
	readJSON(t, doRequest(t, ctx, http.MethodGet, "/"+index+"/_settings?filter_path=*.settings.index.number_of_replicas", nil), &response)
	got := response[index]["settings"]["index"]["number_of_replicas"]
	if got != want {
		t.Fatalf("number_of_replicas for %s = %q, want %q", index, got, want)
	}
}

// scenarioDeleteSyncManagedWaitTransforms proves -wait-transforms through the
// real binary: when the loader exits, the transform's destination index
// already holds the pivot output. There is deliberately no retry loop here;
// without the wait the destination is empty or missing at this point.
func scenarioDeleteSyncManagedWaitTransforms(t *testing.T, ctx *scenarioContext) {
	transformName := ctx.sourceIndex + "-transform"
	destIndex := transformName

	// Registered before anything is created so a failing loader run leaks
	// nothing; it runs before the scenario-wide cleanup (LIFO).
	cleanup := func() {
		deleteTransformIfExists(t, ctx, transformName)
		deleteIndexIfExists(t, ctx, destIndex)
	}
	cleanup()
	t.Cleanup(cleanup)

	dir := filepath.Join(ctx.fixturesRoot, "source-transforms")
	writeFixtureDir(t, dir, map[string]string{
		"transforms.json": fmt.Sprintf(sourceTransformsJSON, transformName, ctx.sourceIndex, destIndex),
	})

	args := append(sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, false, ""),
		"-transforms", filepath.Join(dir, "transforms.json"),
		"-wait-transforms",
	)
	result := runLoader(t, ctx, nil, args)
	mustSucceed(t, result)
	mustContain(t, result.output, "Waiting for transform")
	mustContain(t, result.output, "Transform completed")

	// Two source documents with distinct lookup_id values: two pivot rows.
	assertDocCount(t, ctx, destIndex, 2)
}

// scenarioAliasKeepLastPrunesOldestAfterThirdRun runs three alias rollovers with -keep-last 2 and asserts that the oldest generation is pruned.
func scenarioAliasKeepLastPrunesOldestAfterThirdRun(t *testing.T, ctx *scenarioContext) {
	baseArgs := []string{
		"-url", ctx.env.esURL,
		"-index", ctx.sourceIndex,
		"-settings", filepath.Join(ctx.sourceBaseDir, "settings.json"),
		"-mappings", filepath.Join(ctx.sourceBaseDir, "mappings.json"),
		"-pipelines", filepath.Join(ctx.sourceBaseDir, "pipelines.json"),
		"-data", filepath.Join(ctx.sourceBaseDir, "data.json"),
		"-id", "lookup_id",
		"-delete",
		"-sync-managed",
		"-alias",
		"-keep-last", "2",
	}

	mustSucceed(t, runLoader(t, ctx, nil, baseArgs))
	first := singleAliasTarget(t, ctx, ctx.sourceIndex)
	assertTimestampedIndexName(t, ctx.sourceIndex, first)
	assertPipelineExists(t, ctx, ctx.sourcePipelinePrimary)

	mustSucceed(t, runLoader(t, ctx, nil, baseArgs))
	second := singleAliasTarget(t, ctx, ctx.sourceIndex)
	assertTimestampedIndexName(t, ctx.sourceIndex, second)
	if second == first {
		t.Fatalf("expected second run to create a new timestamped index, got same index %q", second)
	}
	assertPipelineExists(t, ctx, ctx.sourcePipelinePrimary)

	mustSucceed(t, runLoader(t, ctx, nil, baseArgs))
	third := singleAliasTarget(t, ctx, ctx.sourceIndex)
	assertTimestampedIndexName(t, ctx.sourceIndex, third)
	if third == first || third == second {
		t.Fatalf("expected third run to create a new timestamped index, got %q", third)
	}
	assertPipelineExists(t, ctx, ctx.sourcePipelinePrimary)

	indices := timestampedIndices(t, ctx, ctx.sourceIndex)
	if len(indices) != 2 {
		t.Fatalf("expected exactly 2 timestamped indices after pruning, got %d (%v)", len(indices), indices)
	}
	if containsString(indices, first) {
		t.Fatalf("expected oldest index %q to be pruned, remaining=%v", first, indices)
	}
	if !containsString(indices, second) || !containsString(indices, third) {
		t.Fatalf("expected newest indices %q and %q to remain, got %v", second, third, indices)
	}

	assertEqual(t, singleAliasTarget(t, ctx, ctx.sourceIndex), third)
	assertDocCount(t, ctx, ctx.sourceIndex, 2)
}

// scenarioFlushSyncManagedDivergentMappingsFails proves the existing-index
// mapping preflight runs before -flush deletes documents and before
// -sync-managed upserts pipelines and policies: a mappings file declaring
// fields the index lacks fails the run, names every divergent field, and
// leaves documents, mappings and managed resources as they were.
func scenarioFlushSyncManagedDivergentMappingsFails(t *testing.T, ctx *scenarioContext) {
	mustSucceed(t, runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, false, "")))
	sourcePipelineBefore := pipelineDefinition(t, ctx, ctx.sourcePipelinePrimary)

	result := runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceDivergentDir, "-flush", true, false, ""))
	mustFailWith(t, result, "Mapping preflight failed")
	mustContain(t, result.output, "pipeline_version")
	mustContain(t, result.output, "new_mapping_only_field")

	assertDocCount(t, ctx, ctx.sourceIndex, 2)
	assertNoMappingField(t, ctx, ctx.sourceIndex, "new_mapping_only_field")
	assertPipelineMissing(t, ctx, ctx.altSourcePipelinePrimary)
	assertPipelineMissing(t, ctx, ctx.altSourcePipelineSecondary)
	assertPolicyMissing(t, ctx, ctx.altSourcePolicy)
	assertPipelineExists(t, ctx, ctx.sourcePipelinePrimary)
	assertEqual(t, pipelineDefinition(t, ctx, ctx.sourcePipelinePrimary), sourcePipelineBefore)
}

// scenarioUnresolvedTemplateVariableFailsSafely guards that a typo in a config
// file fails the run before -delete removes the index, rather than leaving the
// cluster half-reset.
func scenarioUnresolvedTemplateVariableFailsSafely(t *testing.T, ctx *scenarioContext) {
	// The negative case must not depend on the host shell: t.Setenv restores
	// the original value on cleanup, then the variable is removed for the child.
	t.Setenv("NOT_SET_ANYWHERE", "")
	if err := os.Unsetenv("NOT_SET_ANYWHERE"); err != nil {
		t.Fatalf("unset NOT_SET_ANYWHERE: %v", err)
	}

	mustSucceed(t, runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, false, "")))
	assertDocCount(t, ctx, ctx.sourceIndex, 2)

	unresolvedDir := writeSourceVariant(t, ctx, "source-unresolved", map[string]string{
		"settings.json": `{"settings":{"index":{"refresh_interval":"${NOT_SET_ANYWHERE}"}}}`,
	})

	result := runLoader(t, ctx, nil, sourceArgs(ctx, unresolvedDir, "-delete", true, false, ""))
	mustFailWith(t, result, "${NOT_SET_ANYWHERE}")

	assertIndexExists(t, ctx, ctx.sourceIndex)
	assertDocCount(t, ctx, ctx.sourceIndex, 2)
}

// scenarioSettingsMalformedFailsBeforeDelete guards that a malformed settings
// file fails the run before -delete removes the index, rather than leaving the
// cluster half-reset (FIX-SETTINGSPRE-01).
func scenarioSettingsMalformedFailsBeforeDelete(t *testing.T, ctx *scenarioContext) {
	mustSucceed(t, runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, false, "")))
	assertDocCount(t, ctx, ctx.sourceIndex, 2)

	badDir := writeSourceVariant(t, ctx, "source-bad-settings", map[string]string{
		"settings.json": `{"settings":`,
	})

	result := runLoader(t, ctx, nil, sourceArgs(ctx, badDir, "-delete", true, false, ""))
	mustFailWith(t, result, "Parsing settings file")

	assertIndexExists(t, ctx, ctx.sourceIndex)
	assertDocCount(t, ctx, ctx.sourceIndex, 2)
}

// scenarioDataFormatUnknownFailsBeforeDelete guards that a data file whose
// first byte is neither '[' nor '{' fails the run before -delete removes the
// index, rather than after it (FIX-DATAFMT-01).
func scenarioDataFormatUnknownFailsBeforeDelete(t *testing.T, ctx *scenarioContext) {
	mustSucceed(t, runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, false, "")))
	assertDocCount(t, ctx, ctx.sourceIndex, 2)

	badDir := writeSourceVariant(t, ctx, "source-bad-data-format", map[string]string{
		"data.json": "lookup_id,name\nA,Alice\nB,Bob",
	})
	result := runLoader(t, ctx, nil, sourceArgs(ctx, badDir, "-delete", true, false, ""))
	mustFailWith(t, result, "is neither '[' (JSON array) nor '{' (NDJSON)")

	assertIndexExists(t, ctx, ctx.sourceIndex)
	assertDocCount(t, ctx, ctx.sourceIndex, 2)
}

// scenarioSyncManagedMissingIndexFails guards FIX-SYNCNOOP-01: a sync-only run
// against an absent index with no settings or mappings must fail with a
// diagnostic instead of reporting a sync that upserted nothing, and must leave
// the cluster untouched.
func scenarioSyncManagedMissingIndexFails(t *testing.T, ctx *scenarioContext) {
	assertIndexMissing(t, ctx, ctx.sourceIndex)
	assertPipelineMissing(t, ctx, ctx.sourcePipelinePrimary)
	assertPipelineMissing(t, ctx, ctx.sourcePipelineSecondary)
	assertPolicyMissing(t, ctx, ctx.sourcePolicy)

	// Not sourceArgs: it always supplies settings, mappings and data.
	result := runLoader(t, ctx, nil, []string{
		"-url", ctx.env.esURL,
		"-index", ctx.sourceIndex,
		"-pipelines", filepath.Join(ctx.sourceBaseDir, "pipelines.json"),
		"-policies", filepath.Join(ctx.sourceBaseDir, "policies.json"),
		"-sync-managed",
	})
	mustFailWith(t, result, "-sync-managed without a data action cannot sync because index")
	mustContain(t, result.output, ctx.sourceIndex)
	mustContain(t, result.output, ctx.sourcePipelinePrimary)
	mustContain(t, result.output, ctx.sourcePipelineSecondary)
	mustContain(t, result.output, ctx.sourcePolicy)
	mustContain(t, result.output, "-settings or -mappings")

	assertIndexMissing(t, ctx, ctx.sourceIndex)
	assertPipelineMissing(t, ctx, ctx.sourcePipelinePrimary)
	assertPipelineMissing(t, ctx, ctx.sourcePipelineSecondary)
	assertPolicyMissing(t, ctx, ctx.sourcePolicy)
}

// scenarioAliasFlushStaleTargetFailsBeforeFlush guards that every target of a
// multi-target alias is verified before -flush: the stale second target must
// block the run, be named, and leave the seeded documents in place.
func scenarioAliasFlushStaleTargetFailsBeforeFlush(t *testing.T, ctx *scenarioContext) {
	mustSucceed(t, runLoader(t, ctx, nil, append(sourceArgsNoPolicies(ctx, "-delete", false, false, ""), "-alias")))
	seeded := singleAliasTarget(t, ctx, ctx.sourceIndex)

	// No is_write_index, so this does not conflict with the seed's write index.
	// The name matches YYYYMMDDHHMMSS, so ctx.cleanup removes it.
	stale := ctx.sourceIndex + "-20000101000000"
	body := fmt.Sprintf(`{"mappings":{"properties":{"base_value":{"type":"keyword"}}},"aliases":{%q:{}}}`, ctx.sourceIndex)
	resp := doRequest(t, ctx, http.MethodPut, "/"+stale, []byte(body))
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		t.Fatalf("create stale alias target %s failed: status=%d body=%s", stale, resp.StatusCode, string(respBody))
	}
	if got := len(aliasTargets(t, ctx, ctx.sourceIndex)); got != 2 {
		t.Fatalf("expected alias %s to have 2 targets, got %d", ctx.sourceIndex, got)
	}

	result := runLoader(t, ctx, nil, append(sourceArgsNoPolicies(ctx, "-flush", false, false, ""), "-alias"))
	mustFailWith(t, result, "Mapping preflight failed")
	mustContain(t, result.output, stale)
	assertDocCount(t, ctx, seeded, 2)
	assertDocCount(t, ctx, ctx.sourceIndex, 2)
}

// ─── Environment and Fixture Setup ─────────────────────────────────────────────

// setupTestEnv resolves the repo root, creates a temp directory, builds the loader binary, and starts the Elasticsearch container.
func setupTestEnv() (*testEnv, error) {
	repoRoot, err := repoRoot()
	if err != nil {
		return nil, err
	}

	tmpDir, err := os.MkdirTemp("", "esbl-e2e-*")
	if err != nil {
		return nil, err
	}

	binaryPath := filepath.Join(tmpDir, "es-bulk-loader")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/es-bulk-loader")
	buildCmd.Dir = repoRoot
	if output, err := buildCmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("build binary: %w\n%s", err, string(output))
	}

	containerName := fmt.Sprintf("esbl-e2e-%d", time.Now().UnixNano())
	port, err := startElasticsearchContainer(containerName)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return nil, err
	}

	esURL := fmt.Sprintf("http://127.0.0.1:%s", port)
	env := &testEnv{
		repoRoot:      repoRoot,
		binaryPath:    binaryPath,
		containerName: containerName,
		esURL:         esURL,
		httpClient:    &http.Client{Timeout: 10 * time.Second},
	}

	if err := waitForCluster(env); err != nil {
		_ = teardownTestEnv(env)
		return nil, err
	}

	return env, nil
}

// teardownTestEnv force-removes the Elasticsearch container and the temp directory holding the built binary. A container that is already gone is not an error.
func teardownTestEnv(env *testEnv) error {
	var errs []string
	if env.containerName != "" {
		cmd := exec.Command("docker", "rm", "-f", env.containerName)
		if output, err := cmd.CombinedOutput(); err != nil && !strings.Contains(string(output), "No such container") {
			errs = append(errs, fmt.Sprintf("remove container: %v (%s)", err, strings.TrimSpace(string(output))))
		}
	}
	if env.binaryPath != "" {
		if err := os.RemoveAll(filepath.Dir(env.binaryPath)); err != nil {
			errs = append(errs, fmt.Sprintf("remove temp dir: %v", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

// repoRoot returns the repository root, derived from this source file's location at runtime.
func repoRoot() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("unable to determine caller path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..")), nil
}

// startElasticsearchContainer starts a single-node Elasticsearch container with the given name and returns the host port published for 9200.
func startElasticsearchContainer(containerName string) (string, error) {
	cmd := exec.Command(
		"docker", "run", "-d", "-P", "--name", containerName,
		"-e", "discovery.type=single-node",
		"-e", "xpack.security.enabled=false",
		"-e", "xpack.license.self_generated.type=basic",
		"-e", "ES_JAVA_OPTS=-Xms512m -Xmx512m",
		"docker.elastic.co/elasticsearch/elasticsearch:9.3.1",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker run elasticsearch: %w\n%s", err, string(output))
	}

	portCmd := exec.Command("docker", "port", containerName, "9200/tcp")
	portOutput, err := portCmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker port: %w\n%s", err, string(portOutput))
	}

	host, port, err := net.SplitHostPort(strings.TrimSpace(string(portOutput)))
	if err != nil {
		return "", fmt.Errorf("parse docker port output %q: %w", strings.TrimSpace(string(portOutput)), err)
	}
	if host == "0.0.0.0" || host == "::" {
		return port, nil
	}
	return port, nil
}

// waitForCluster polls cluster health until it answers OK, or fails after 90 seconds.
func waitForCluster(env *testEnv) error {
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet, env.esURL+"/_cluster/health?wait_for_status=yellow&timeout=1s", nil)
		resp, err := env.httpClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}

	logsCmd := exec.Command("docker", "logs", env.containerName)
	logs, _ := logsCmd.CombinedOutput()
	return fmt.Errorf("elasticsearch did not become ready at %s\n%s", env.esURL, string(logs))
}

// newScenarioContext creates a scenario's names and fixture directories under t.TempDir(), prefixed so scenarios do not collide.
func newScenarioContext(t *testing.T, prefix string) *scenarioContext {
	t.Helper()

	root := t.TempDir()
	ctx := &scenarioContext{
		env: sharedEnv,

		prefix: prefix,

		sourceIndex: prefix + "-source",
		targetIndex: prefix + "-target",

		sourcePipelinePrimary:   prefix + "-source-default",
		sourcePipelineSecondary: prefix + "-source-secondary",
		sourcePolicy:            prefix + "-source-policy",

		altSourcePipelinePrimary:   prefix + "-source-sync-default",
		altSourcePipelineSecondary: prefix + "-source-sync-secondary",
		altSourcePolicy:            prefix + "-source-sync-policy",

		targetPipelinePrimary:   prefix + "-target-enrich",
		targetPipelineSecondary: prefix + "-target-secondary",

		fixturesRoot:       root,
		sourceBaseDir:      filepath.Join(root, "source-base"),
		sourceSyncDir:      filepath.Join(root, "source-sync"),
		sourceReplaceDir:   filepath.Join(root, "source-replace"),
		sourceAppendDir:    filepath.Join(root, "source-append"),
		sourceDivergentDir: filepath.Join(root, "source-divergent"),
		targetReferenceDir: filepath.Join(root, "target-reference"),
	}
	ctx.sourcePolicyManaged = managedPolicyNameForLogicalAndIndex(ctx.sourcePolicy, ctx.sourceIndex)

	ctx.writeFixtures(t)
	return ctx
}

// writeFixtures writes every fixture file a scenario reads, with pipeline and policy names templated per scenario.
func (ctx *scenarioContext) writeFixtures(t *testing.T) {
	t.Helper()

	writeFixtureDir(t, ctx.sourceBaseDir, map[string]string{
		"settings.json": sourceSettingsJSON,
		"mappings.json": sourceBaseMappingsJSON,
		"pipelines.json": strings.TrimSpace(fmt.Sprintf(sourcePipelinesJSON,
			"${SOURCE_PIPELINE_PRIMARY}", "${SOURCE_PIPELINE_SECONDARY}",
		)),
		"policies.json": strings.TrimSpace(fmt.Sprintf(sourcePoliciesJSON, "${SOURCE_POLICY}")),
		"data.json":     sourceInitialDataJSON,
	})

	writeFixtureDir(t, ctx.sourceSyncDir, map[string]string{
		"settings.json": sourceSettingsJSON,
		"mappings.json": sourceBaseMappingsJSON,
		"pipelines.json": strings.TrimSpace(fmt.Sprintf(sourceSyncPipelinesJSON,
			"${ALT_SOURCE_PIPELINE_PRIMARY}", "${ALT_SOURCE_PIPELINE_SECONDARY}",
		)),
		"policies.json": strings.TrimSpace(fmt.Sprintf(sourceSyncPoliciesJSON, "${ALT_SOURCE_POLICY}")),
		"data.json":     sourceReplacementDataJSON,
	})

	writeFixtureDir(t, ctx.sourceReplaceDir, map[string]string{
		"settings.json": sourceSettingsJSON,
		"mappings.json": sourceBaseMappingsJSON,
		"pipelines.json": strings.TrimSpace(fmt.Sprintf(sourceSyncPipelinesJSON,
			"${ALT_SOURCE_PIPELINE_PRIMARY}", "${ALT_SOURCE_PIPELINE_SECONDARY}",
		)),
		"policies.json": strings.TrimSpace(fmt.Sprintf(sourceSyncPoliciesJSON, "${ALT_SOURCE_POLICY}")),
		"data.json":     sourceReplacementDataJSON,
	})

	writeFixtureDir(t, ctx.sourceAppendDir, map[string]string{
		"settings.json": sourceSettingsJSON,
		"mappings.json": sourceBaseMappingsJSON,
		"pipelines.json": strings.TrimSpace(fmt.Sprintf(sourceSyncPipelinesJSON,
			"${ALT_SOURCE_PIPELINE_PRIMARY}", "${ALT_SOURCE_PIPELINE_SECONDARY}",
		)),
		"policies.json": strings.TrimSpace(fmt.Sprintf(sourceSyncPoliciesJSON, "${ALT_SOURCE_POLICY}")),
		"data.json":     sourceAppendDataJSON,
	})

	writeFixtureDir(t, ctx.sourceDivergentDir, map[string]string{
		"settings.json": sourceSettingsJSON,
		"mappings.json": sourceSyncMappingsJSON,
		"pipelines.json": strings.TrimSpace(fmt.Sprintf(sourceSyncPipelinesJSON,
			"${ALT_SOURCE_PIPELINE_PRIMARY}", "${ALT_SOURCE_PIPELINE_SECONDARY}",
		)),
		"policies.json": strings.TrimSpace(fmt.Sprintf(sourceSyncPoliciesJSON, "${ALT_SOURCE_POLICY}")),
		"data.json":     sourceReplacementDataJSON,
	})

	writeFixtureDir(t, ctx.targetReferenceDir, map[string]string{
		"settings.json": targetSettingsJSON,
		"mappings.json": targetMappingsJSON,
		"pipelines.json": strings.TrimSpace(fmt.Sprintf(targetPipelinesJSON,
			"${TARGET_PIPELINE_PRIMARY}", "${SOURCE_POLICY_NAME}", "${TARGET_PIPELINE_SECONDARY}",
		)),
		"data.json": targetDataJSON,
	})
}

// cleanup deletes the scenario's target index, source index and alias-generation indices, so a rerun starts clean.
func (ctx *scenarioContext) cleanup(t *testing.T) {
	t.Helper()

	deleteIndexIfExists(t, ctx, ctx.targetIndex)
	deleteIndexIfExists(t, ctx, ctx.sourceIndex)
	deleteTimestampedIndicesForAlias(t, ctx, ctx.sourceIndex)
	deleteTimestampedIndicesForAlias(t, ctx, ctx.targetIndex)

	deletePipelineIfExists(t, ctx, ctx.targetPipelinePrimary)
	deletePipelineIfExists(t, ctx, ctx.targetPipelineSecondary)
	deletePipelineIfExists(t, ctx, ctx.sourcePipelinePrimary)
	deletePipelineIfExists(t, ctx, ctx.sourcePipelineSecondary)
	deletePipelineIfExists(t, ctx, ctx.altSourcePipelinePrimary)
	deletePipelineIfExists(t, ctx, ctx.altSourcePipelineSecondary)

	deleteManagedPoliciesForLogical(t, ctx, ctx.altSourcePolicy)
	deleteManagedPoliciesForLogical(t, ctx, ctx.sourcePolicy)
	deletePolicyIfExists(t, ctx, ctx.altSourcePolicy)
	deletePolicyIfExists(t, ctx, ctx.sourcePolicy)
}

// writeSourceVariant writes a copy of the scenario's source fixtures into
// a sibling directory named dirName, with overrides replacing whole files,
// and returns that directory. Fail-safe scenarios use it to change one
// input while keeping the rest identical to the seeded run.
func writeSourceVariant(t *testing.T, ctx *scenarioContext, dirName string, overrides map[string]string) string {
	t.Helper()
	files := map[string]string{}
	for _, name := range []string{"settings.json", "mappings.json", "pipelines.json", "policies.json", "data.json"} {
		content, err := os.ReadFile(filepath.Join(ctx.sourceBaseDir, name))
		if err != nil {
			t.Fatalf("read base fixture %s: %v", name, err)
		}
		files[name] = strings.TrimSpace(string(content))
	}
	for name, content := range overrides {
		files[name] = content
	}
	dir := filepath.Join(filepath.Dir(ctx.sourceBaseDir), dirName)
	writeFixtureDir(t, dir, files)
	return dir
}

// writeFixtureDir writes each named file, newline-terminated, into dir, creating dir first.
func writeFixtureDir(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content+"\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", filepath.Join(dir, name), err)
		}
	}
}

// runLoader runs the built loader binary with args from the repo root and returns its combined output and error. extraEnv is added to the environment.
func runLoader(t *testing.T, ctx *scenarioContext, extraEnv map[string]string, args []string) commandResult {
	t.Helper()

	cmd := exec.Command(ctx.env.binaryPath, args...)
	cmd.Dir = ctx.env.repoRoot

	// REPLICAS is the -replicas env fallback. Drop any inherited value so a
	// developer's exported REPLICAS cannot change every scenario's replica
	// count; a scenario that wants it passes it through extraEnv.
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool {
		return strings.HasPrefix(kv, "REPLICAS=")
	})
	env = append(env,
		"SOURCE_PIPELINE_PRIMARY="+ctx.sourcePipelinePrimary,
		"SOURCE_PIPELINE_SECONDARY="+ctx.sourcePipelineSecondary,
		"SOURCE_POLICY="+ctx.sourcePolicy,
		"ALT_SOURCE_PIPELINE_PRIMARY="+ctx.altSourcePipelinePrimary,
		"ALT_SOURCE_PIPELINE_SECONDARY="+ctx.altSourcePipelineSecondary,
		"ALT_SOURCE_POLICY="+ctx.altSourcePolicy,
		"TARGET_PIPELINE_PRIMARY="+ctx.targetPipelinePrimary,
		"TARGET_PIPELINE_SECONDARY="+ctx.targetPipelineSecondary,
		"SOURCE_POLICY_NAME="+ctx.sourcePolicyManaged,
	)
	for key, value := range extraEnv {
		env = append(env, key+"="+value)
	}
	cmd.Env = env

	output, err := cmd.CombinedOutput()
	return commandResult{output: string(output), err: err}
}

// sourceArgs builds the argument list for the source index, selecting the fixture directory and the action flags.
func sourceArgs(ctx *scenarioContext, fixtureDir, action string, syncManaged, enrich bool, enrichValue string) []string {
	args := []string{
		"-url", ctx.env.esURL,
		"-index", ctx.sourceIndex,
		"-settings", filepath.Join(fixtureDir, "settings.json"),
		"-mappings", filepath.Join(fixtureDir, "mappings.json"),
		"-pipelines", filepath.Join(fixtureDir, "pipelines.json"),
		"-policies", filepath.Join(fixtureDir, "policies.json"),
		"-data", filepath.Join(fixtureDir, "data.json"),
		"-id", "lookup_id",
		action,
	}
	if syncManaged {
		args = append(args, "-sync-managed")
	}
	if enrich {
		if enrichValue == "" {
			args = append(args, "-enrich")
		} else {
			args = append(args, "-enrich="+enrichValue)
		}
	}
	return args
}

// sourceArgsNoPolicies is sourceArgs without a policies file, for runs that must see no declared policies.
func sourceArgsNoPolicies(ctx *scenarioContext, action string, syncManaged, enrich bool, enrichValue string) []string {
	args := []string{
		"-url", ctx.env.esURL,
		"-index", ctx.sourceIndex,
		"-settings", filepath.Join(ctx.sourceBaseDir, "settings.json"),
		"-mappings", filepath.Join(ctx.sourceBaseDir, "mappings.json"),
		"-data", filepath.Join(ctx.sourceBaseDir, "data.json"),
		"-id", "lookup_id",
		action,
	}
	if syncManaged {
		args = append(args, "-sync-managed")
	}
	if enrich {
		if enrichValue == "" {
			args = append(args, "-enrich")
		} else {
			args = append(args, "-enrich="+enrichValue)
		}
	}
	return args
}

// sourceArgsWithNuke is sourceArgs with -nuke appended.
func sourceArgsWithNuke(ctx *scenarioContext, fixtureDir, action string, syncManaged, enrich bool, enrichValue string) []string {
	args := sourceArgs(ctx, fixtureDir, action, syncManaged, enrich, enrichValue)
	return append(args, "-nuke")
}

// targetArgs builds the argument list for the target index fixtures.
func targetArgs(ctx *scenarioContext, action string, syncManaged bool) []string {
	args := []string{
		"-url", ctx.env.esURL,
		"-index", ctx.targetIndex,
		"-settings", filepath.Join(ctx.targetReferenceDir, "settings.json"),
		"-mappings", filepath.Join(ctx.targetReferenceDir, "mappings.json"),
		"-pipelines", filepath.Join(ctx.targetReferenceDir, "pipelines.json"),
		"-data", filepath.Join(ctx.targetReferenceDir, "data.json"),
		"-id", "lookup_id",
		action,
	}
	if syncManaged {
		args = append(args, "-sync-managed")
	}
	return args
}

// nukeArgs builds the argument list for a bare -nuke run against the source index.
func nukeArgs(ctx *scenarioContext) []string {
	return []string{
		"-url", ctx.env.esURL,
		"-index", ctx.sourceIndex,
		"-pipelines", filepath.Join(ctx.sourceBaseDir, "pipelines.json"),
		"-policies", filepath.Join(ctx.sourceBaseDir, "policies.json"),
		"-nuke",
	}
}

// aliasTargets returns the sorted concrete indices alias points to, or nil
// when the alias does not exist (404). Any other status, an unreadable body,
// or a target whose "aliases" object does not name alias fails the test with
// the response, so a wrong-alias or partial answer cannot pass as membership.
func aliasTargets(t *testing.T, ctx *scenarioContext, alias string) []string {
	t.Helper()

	resp := doRequest(t, ctx, http.MethodGet, "/_alias/"+alias, nil)
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read alias response for %s: %v", alias, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get alias %s failed: status=%d body=%s", alias, resp.StatusCode, string(body))
	}

	var parsed map[string]struct {
		Aliases map[string]json.RawMessage `json:"aliases"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil || parsed == nil {
		t.Fatalf("decode alias response for %s: %v\nbody=%s", alias, err, string(body))
	}

	targets := make([]string, 0, len(parsed))
	for index, entry := range parsed {
		if _, ok := entry.Aliases[alias]; !ok {
			t.Fatalf("alias response for %s lists index %s without that alias\nbody=%s", alias, index, string(body))
		}
		targets = append(targets, index)
	}
	slices.Sort(targets)
	return targets
}

// singleAliasTarget returns the one concrete index the alias points to, and fails the test when the alias has zero or several targets.
func singleAliasTarget(t *testing.T, ctx *scenarioContext, alias string) string {
	t.Helper()
	targets := aliasTargets(t, ctx, alias)
	if len(targets) != 1 {
		t.Fatalf("expected alias %q to have exactly one target, got %v", alias, targets)
	}
	return targets[0]
}

// timestampedIndices lists the indices that match the alias's timestamped generation pattern.
func timestampedIndices(t *testing.T, ctx *scenarioContext, alias string) []string {
	t.Helper()
	var response []map[string]any
	readJSON(t, doRequest(t, ctx, http.MethodGet, "/_cat/indices/"+alias+"-*?format=json&h=index", nil), &response)

	indices := make([]string, 0, len(response))
	for _, item := range response {
		indexName, _ := item["index"].(string)
		if isTimestampedIndexName(alias, indexName) {
			indices = append(indices, indexName)
		}
	}
	return indices
}

// deleteTimestampedIndicesForAlias deletes every timestamped generation of alias, for cleanup.
func deleteTimestampedIndicesForAlias(t *testing.T, ctx *scenarioContext, alias string) {
	t.Helper()
	for _, index := range timestampedIndices(t, ctx, alias) {
		deleteIndexIfExists(t, ctx, index)
	}
}

// assertTimestampedIndexName fails the test unless index matches <alias>-YYYYMMDDHHMMSS.
func assertTimestampedIndexName(t *testing.T, alias, index string) {
	t.Helper()
	if !isTimestampedIndexName(alias, index) {
		t.Fatalf("expected index %q to match alias timestamp pattern %s-YYYYMMDDHHMMSS", index, alias)
	}
}

// isTimestampedIndexName reports whether index is alias followed by a 14-digit numeric suffix.
func isTimestampedIndexName(alias, index string) bool {
	prefix := alias + "-"
	if !strings.HasPrefix(index, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(index, prefix)
	if len(suffix) != 14 {
		return false
	}
	if _, err := strconv.ParseInt(suffix, 10, 64); err != nil {
		return false
	}
	_, err := time.Parse("20060102150405", suffix)
	return err == nil
}

// containsString reports whether target is one of values.
func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// ─── Assertions and Utilities ──────────────────────────────────────────────────

// assertIndexExists fails the test unless the named index exists in Elasticsearch.
func assertIndexExists(t *testing.T, ctx *scenarioContext, index string) {
	t.Helper()
	if !indexExists(t, ctx, index) {
		t.Fatalf("expected index %q to exist", index)
	}
}

// assertIndexMissing fails the test if the named index exists.
func assertIndexMissing(t *testing.T, ctx *scenarioContext, index string) {
	t.Helper()
	if indexExists(t, ctx, index) {
		t.Fatalf("expected index %q to be missing", index)
	}
}

// indexExists queries Elasticsearch and reports whether the named index exists.
func indexExists(t *testing.T, ctx *scenarioContext, index string) bool {
	t.Helper()
	resp := doRequest(t, ctx, http.MethodHead, "/"+index, nil)
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// assertDocCount fails the test unless the index holds exactly want documents.
func assertDocCount(t *testing.T, ctx *scenarioContext, index string, want int) {
	t.Helper()
	refreshIndex(t, ctx, index)
	var response struct {
		Count int `json:"count"`
	}
	readJSON(t, doRequest(t, ctx, http.MethodGet, "/"+index+"/_count", nil), &response)
	if response.Count != want {
		t.Fatalf("doc count for %s = %d, want %d", index, response.Count, want)
	}
}

// assertDefaultPipeline fails the test unless the index's default pipeline equals pipeline.
func assertDefaultPipeline(t *testing.T, ctx *scenarioContext, index, want string) {
	t.Helper()
	var response map[string]map[string]map[string]map[string]string
	readJSON(t, doRequest(t, ctx, http.MethodGet, "/"+index+"/_settings?filter_path=*.settings.index.default_pipeline", nil), &response)
	got := response[index]["settings"]["index"]["default_pipeline"]
	if got != want {
		t.Fatalf("default pipeline for %s = %q, want %q", index, got, want)
	}
}

// assertNoDefaultPipeline fails the test if the index has a default pipeline set.
func assertNoDefaultPipeline(t *testing.T, ctx *scenarioContext, index string) {
	t.Helper()
	var response map[string]map[string]map[string]map[string]string
	readJSON(t, doRequest(t, ctx, http.MethodGet, "/"+index+"/_settings?filter_path=*.settings.index.default_pipeline", nil), &response)
	if settings, ok := response[index]; ok {
		if indexSettings, ok := settings["settings"]["index"]; ok {
			if got := indexSettings["default_pipeline"]; got != "" && got != "null" {
				t.Fatalf("expected no default pipeline for %s, got %q", index, got)
			}
		}
	}
}

// assertMappingField fails the test unless the index mappings declare the field at fieldPath.
func assertMappingField(t *testing.T, ctx *scenarioContext, index, field string) {
	t.Helper()
	if !mappingFieldExists(t, ctx, index, field) {
		t.Fatalf("expected mapping field %q to exist on %s", field, index)
	}
}

// assertNoMappingField fails the test if the index mappings declare the field at fieldPath.
func assertNoMappingField(t *testing.T, ctx *scenarioContext, index, field string) {
	t.Helper()
	if mappingFieldExists(t, ctx, index, field) {
		t.Fatalf("expected mapping field %q to be absent on %s", field, index)
	}
}

// mappingFieldExists reports whether the index mappings declare the dotted field path.
func mappingFieldExists(t *testing.T, ctx *scenarioContext, index, field string) bool {
	t.Helper()
	var response map[string]map[string]any
	readJSON(t, doRequest(t, ctx, http.MethodGet, "/"+index+"/_mapping", nil), &response)
	mappings, ok := response[index]["mappings"].(map[string]any)
	if !ok {
		return false
	}
	properties, ok := mappings["properties"].(map[string]any)
	if !ok {
		return false
	}
	_, ok = properties[field]
	return ok
}

// assertPipelineExists fails the test unless the named ingest pipeline exists.
func assertPipelineExists(t *testing.T, ctx *scenarioContext, name string) {
	t.Helper()
	resp := doRequest(t, ctx, http.MethodGet, "/_ingest/pipeline/"+name, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected pipeline %q to exist, status=%d body=%s", name, resp.StatusCode, string(body))
	}
}

// pipelineDefinition returns the pipeline's stored definition as canonical
// JSON (object keys sorted by encoding/json), so a before/after comparison
// catches a rewrite that a presence-only assertion would miss.
func pipelineDefinition(t *testing.T, ctx *scenarioContext, name string) string {
	t.Helper()
	var response map[string]any
	readJSON(t, doRequest(t, ctx, http.MethodGet, "/_ingest/pipeline/"+name, nil), &response)
	definition, ok := response[name]
	if !ok {
		t.Fatalf("pipeline %q missing from response: %v", name, response)
	}
	canonical, err := json.Marshal(definition)
	if err != nil {
		t.Fatalf("marshal pipeline %q definition: %v", name, err)
	}
	return string(canonical)
}

// assertPipelineMissing fails the test if the named ingest pipeline exists.
func assertPipelineMissing(t *testing.T, ctx *scenarioContext, name string) {
	t.Helper()
	resp := doRequest(t, ctx, http.MethodGet, "/_ingest/pipeline/"+name, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected pipeline %q to be missing, status=%d body=%s", name, resp.StatusCode, string(body))
	}
}

// assertPolicyExists fails the test unless the named enrich policy exists.
func assertPolicyExists(t *testing.T, ctx *scenarioContext, name string) {
	t.Helper()

	for _, policy := range listPolicyNames(t, ctx) {
		if policy == name || isManagedPolicyForLogical(name, policy) {
			return
		}
	}

	resp := doRequest(t, ctx, http.MethodGet, "/_enrich/policy/"+name, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected policy %q to exist, status=%d body=%s", name, resp.StatusCode, string(body))
	}
}

// assertPolicyMissing fails the test if the named enrich policy exists.
func assertPolicyMissing(t *testing.T, ctx *scenarioContext, name string) {
	t.Helper()
	for _, policy := range listPolicyNames(t, ctx) {
		if policy == name || isManagedPolicyForLogical(name, policy) {
			t.Fatalf("expected policy %q to be missing, found %q", name, policy)
		}
	}

	resp := doRequest(t, ctx, http.MethodGet, "/_enrich/policy/"+name, nil)
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read policy response: %v", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return
	}
	if resp.StatusCode == http.StatusOK && bytes.Contains(body, []byte(`"policies":[]`)) {
		return
	}
	t.Fatalf("expected policy %q to be missing, status=%d body=%s", name, resp.StatusCode, string(body))
}

// listPolicyNames returns the names of all enrich policies in the cluster.
func listPolicyNames(t *testing.T, ctx *scenarioContext) []string {
	t.Helper()
	resp := doRequest(t, ctx, http.MethodGet, "/_enrich/policy", nil)
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("list policies failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	var parsed struct {
		Policies []struct {
			Config map[string]struct {
				Name string `json:"name"`
			} `json:"config"`
		} `json:"policies"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode policies response: %v", err)
	}

	names := make([]string, 0)
	for _, policy := range parsed.Policies {
		for _, config := range policy.Config {
			if config.Name == "" {
				continue
			}
			names = append(names, config.Name)
		}
	}
	return names
}

// isManagedPolicyForLogical reports whether name is a content-hashed managed version of logical, in the form <logical>-<sha256[:6]>.
func isManagedPolicyForLogical(logical, candidate string) bool {
	prefix := logical + "-"
	if !strings.HasPrefix(candidate, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(candidate, prefix)
	if len(suffix) != 6 {
		return false
	}
	for _, ch := range suffix {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

// assertEnrichBackingIndexExists fails the test unless the policy's .enrich-<policy> backing index exists.
func assertEnrichBackingIndexExists(t *testing.T, ctx *scenarioContext, policy string) {
	t.Helper()
	var response []map[string]any
	readJSON(t, doRequest(t, ctx, http.MethodGet, "/_cat/indices/.enrich-*?format=json", nil), &response)
	for _, item := range response {
		if indexName, _ := item["index"].(string); strings.Contains(indexName, policy) {
			return
		}
	}
	t.Fatalf("expected enrich backing index for policy %q", policy)
}

// enrichAliasTargets returns the sorted backing indices of the exact
// .enrich-<policy> alias, or nil when it is absent. policy must be the managed
// (hash-suffixed) name the loader executes. Alias membership, unlike
// _cat/indices existence, is what the loader's task-404 verification reads.
func enrichAliasTargets(t *testing.T, ctx *scenarioContext, policy string) []string {
	t.Helper()
	return aliasTargets(t, ctx, ".enrich-"+policy)
}

// getDocument fetches one document by ID and returns its parsed response.
func getDocument(t *testing.T, ctx *scenarioContext, index, id string) map[string]any {
	t.Helper()
	refreshIndex(t, ctx, index)
	var response map[string]any
	readJSON(t, doRequest(t, ctx, http.MethodGet, "/"+index+"/_doc/"+id, nil), &response)
	return response
}

// refreshIndex refreshes the index so documents written by the previous step are searchable.
func refreshIndex(t *testing.T, ctx *scenarioContext, index string) {
	t.Helper()
	resp := doRequest(t, ctx, http.MethodPost, "/"+index+"/_refresh", nil)
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("refresh %s failed: status=%d body=%s", index, resp.StatusCode, string(body))
	}
}

// doRequest sends one request to Elasticsearch and returns the response.
func doRequest(t *testing.T, ctx *scenarioContext, method, path string, body []byte) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, ctx.env.esURL+path, reader)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := ctx.env.httpClient.Do(req)
	if err != nil {
		t.Fatalf("request %s %s: %v", method, path, err)
	}
	return resp
}

// deleteIndexIfExists deletes the index when present and ignores a 404.
func deleteIndexIfExists(t *testing.T, ctx *scenarioContext, index string) {
	t.Helper()
	resp := doRequest(t, ctx, http.MethodDelete, "/"+index, nil)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusBadRequest && bytes.Contains(body, []byte("matches an alias")) {
		return
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		t.Fatalf("delete index %s failed: status=%d body=%s", index, resp.StatusCode, string(body))
	}
}

// deleteTransformIfExists force-deletes a transform; a missing one is fine.
func deleteTransformIfExists(t *testing.T, ctx *scenarioContext, name string) {
	t.Helper()
	resp := doRequest(t, ctx, http.MethodDelete, "/_transform/"+name+"?force=true", nil)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		t.Fatalf("delete transform %s failed: status=%d body=%s", name, resp.StatusCode, string(body))
	}
}

// deletePipelineIfExists deletes the ingest pipeline when present and ignores a 404.
func deletePipelineIfExists(t *testing.T, ctx *scenarioContext, name string) {
	t.Helper()
	resp := doRequest(t, ctx, http.MethodDelete, "/_ingest/pipeline/"+name, nil)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		t.Fatalf("delete pipeline %s failed: status=%d body=%s", name, resp.StatusCode, string(body))
	}
}

// deletePolicyIfExists deletes the enrich policy when present and ignores a 404.
func deletePolicyIfExists(t *testing.T, ctx *scenarioContext, name string) {
	t.Helper()
	resp := doRequest(t, ctx, http.MethodDelete, "/_enrich/policy/"+name, nil)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		if resp.StatusCode == http.StatusOK && bytes.Contains(body, []byte(`"acknowledged":true`)) {
			return
		}
		t.Fatalf("delete policy %s failed: status=%d body=%s", name, resp.StatusCode, string(body))
	}
}

// deleteManagedPoliciesForLogical deletes every managed policy version of logical, for cleanup.
func deleteManagedPoliciesForLogical(t *testing.T, ctx *scenarioContext, logical string) {
	t.Helper()
	for _, policy := range listPolicyNames(t, ctx) {
		if policy == logical || isManagedPolicyForLogical(logical, policy) {
			deletePolicyIfExists(t, ctx, policy)
		}
	}
}

// readJSON decodes a JSON response body into out and fails the test on a parse error.
func readJSON(t *testing.T, resp *http.Response, target any) {
	t.Helper()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if resp.StatusCode >= 300 {
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, string(body))
	}
	if err := json.Unmarshal(body, target); err != nil {
		t.Fatalf("unmarshal response: %v\nbody=%s", err, string(body))
	}
}

// mustSucceed fails the test, printing the loader output, when the command exited with an error.
func mustSucceed(t *testing.T, result commandResult) {
	t.Helper()
	if result.err != nil {
		t.Fatalf("command failed: %v\n%s", result.err, result.output)
	}
}

// mustFailWith fails the test unless the command failed and its output contains want.
func mustFailWith(t *testing.T, result commandResult, substring string) {
	t.Helper()
	if result.err == nil {
		t.Fatalf("expected command failure containing %q\n%s", substring, result.output)
	}
	mustContain(t, result.output, substring)
}

// mustContain fails the test unless output contains want.
func mustContain(t *testing.T, output, substring string) {
	t.Helper()
	if !strings.Contains(output, substring) {
		t.Fatalf("expected output to contain %q\n%s", substring, output)
	}
}

// assertEqual fails the test when got and want differ, printing both.
func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// nestedString returns the string at the nested key path, or an empty string when a key is missing or the value is not a string.
func nestedString(value map[string]any, path ...string) string {
	current := nestedValue(value, path...)
	if s, ok := current.(string); ok {
		return s
	}
	return ""
}

// nestedFloat returns the number at the nested key path, or 0 when a key is missing or the value is not numeric.
func nestedFloat(value map[string]any, path ...string) float64 {
	current := nestedValue(value, path...)
	if f, ok := current.(float64); ok {
		return f
	}
	return 0
}

// assertMissingField fails the test if the nested key path exists in v.
func assertMissingField(t *testing.T, value map[string]any, path ...string) {
	t.Helper()
	if nestedValue(value, path...) != nil {
		t.Fatalf("expected field %v to be absent", path)
	}
}

// nestedValue returns the raw value at the nested key path, or nil when a key is missing.
func nestedValue(value map[string]any, path ...string) any {
	var current any = value
	for _, part := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = object[part]
		if current == nil {
			return nil
		}
	}
	return current
}

// managedPolicyNameForLogicalAndIndex computes the expected managed policy name, mirroring the loader's <logical>-<sha256[:6]> rule for the given index.
func managedPolicyNameForLogicalAndIndex(logical, index string) string {
	raw := map[string]any{
		"match": map[string]any{
			"indices":       index,
			"match_field":   "lookup_id",
			"enrich_fields": []string{"calculated_value", "source_name", "source_label"},
		},
	}
	canonical, err := json.Marshal(raw)
	if err != nil {
		panic(fmt.Sprintf("marshal managed policy canonical form: %v", err))
	}
	sum := sha256.Sum256(canonical)
	return logical + "-" + hex.EncodeToString(sum[:])[:6]
}

// ─── Embedded JSON Fixtures ────────────────────────────────────────────────────

// sourceTransformsJSON is a keyed transforms file: the pivot reads the source
// index and writes its own destination index, which keeps the source/target
// fixture split intact. Arguments: transform name, source index, destination
// index. It has no "$": transforms files go through template expansion.
const sourceTransformsJSON = `{
  %[1]q: {
    "source_index": %[2]q,
    "body": {
      "source": { "index": %[2]q },
      "dest": { "index": %[3]q },
      "pivot": {
        "group_by": { "lookup_id": { "terms": { "field": "lookup_id" } } },
        "aggregations": { "base_total": { "sum": { "field": "base_value" } } }
      }
    }
  }
}`

// sourceSettingsJSON is the source index settings fixture: an empty settings object.
const sourceSettingsJSON = `{"settings":{}}`

// sourceBaseMappingsJSON is the source index mappings fixture for the base load.
const sourceBaseMappingsJSON = `{
  "mappings": {
    "properties": {
      "lookup_id": { "type": "keyword" },
      "base_value": { "type": "integer" },
      "multiplier": { "type": "integer" },
      "source_name": { "type": "keyword" },
      "source_label": { "type": "keyword" },
      "calculated_value": { "type": "integer" }
    }
  }
}`

// sourceSyncMappingsJSON intentionally declares two root fields
// (pipeline_version, new_mapping_only_field) that the index created from
// sourceBaseMappingsJSON lacks. Only the divergent-mappings scenario uses it:
// -flush/-add never write mappings, so the loader must fail its preflight
// before mutating the index or managed resources.
const sourceSyncMappingsJSON = `{
  "mappings": {
    "properties": {
      "lookup_id": { "type": "keyword" },
      "base_value": { "type": "integer" },
      "multiplier": { "type": "integer" },
      "source_name": { "type": "keyword" },
      "source_label": { "type": "keyword" },
      "calculated_value": { "type": "integer" },
      "pipeline_version": { "type": "integer" },
      "new_mapping_only_field": { "type": "keyword" }
    }
  }
}`

// sourcePipelinesJSON is the keyed source pipeline fixture. Its two %s slots take the scenario's primary and secondary pipeline names.
const sourcePipelinesJSON = `{
  "%s": {
    "description": "Default source pipeline",
    "processors": [
      {
        "script": {
          "lang": "painless",
          "source": "ctx.calculated_value = ctx.base_value * ctx.multiplier; ctx.source_label = ctx.source_name + '-' + ctx.calculated_value;"
        }
      }
    ]
  },
  "%s": {
    "description": "Secondary source pipeline",
    "processors": [
      {
        "set": {
          "field": "secondary_pipeline_ran",
          "value": true
        }
      }
    ]
  }
}`

// sourceSyncPipelinesJSON is the source pipeline fixture for -sync-managed scenarios, with its pipeline and policy names templated per scenario.
const sourceSyncPipelinesJSON = `{
  "%s": {
    "description": "Synced source pipeline",
    "processors": [
      {
        "script": {
          "lang": "painless",
          "source": "ctx.calculated_value = ctx.base_value * ctx.multiplier; ctx.source_label = ctx.source_name + '-' + ctx.calculated_value; ctx.pipeline_version = 2;"
        }
      }
    ]
  },
  "%s": {
    "description": "Secondary synced source pipeline",
    "processors": [
      {
        "set": {
          "field": "secondary_pipeline_ran",
          "value": true
        }
      }
    ]
  }
}`

// sourcePoliciesJSON is the keyed source enrich policy fixture. Its %s slot takes the scenario's policy name.
const sourcePoliciesJSON = `{
  "%s": {
    "match": {
      "indices": "${INDEX}",
      "match_field": "lookup_id",
      "enrich_fields": ["calculated_value", "source_name", "source_label"]
    }
  }
}`

// sourceSyncPoliciesJSON is the source enrich policy fixture for -sync-managed scenarios.
const sourceSyncPoliciesJSON = `{
  "%s": {
    "match": {
      "indices": "${INDEX}",
      "match_field": "lookup_id",
      "enrich_fields": ["calculated_value", "source_name", "source_label"]
    }
  }
}`

// sourceInitialDataJSON is the first source data load: two documents.
const sourceInitialDataJSON = `[
  { "lookup_id": "A1", "base_value": 7, "multiplier": 3, "source_name": "alpha" },
  { "lookup_id": "B2", "base_value": 5, "multiplier": 4, "source_name": "beta" }
]`

// sourceReplacementDataJSON is the replacement load used by -flush scenarios: one document.
const sourceReplacementDataJSON = `[
  { "lookup_id": "C3", "base_value": 4, "multiplier": 5, "source_name": "gamma" }
]`

// sourceAppendDataJSON is the document appended by -add scenarios.
const sourceAppendDataJSON = `[
  { "lookup_id": "C3", "base_value": 4, "multiplier": 5, "source_name": "gamma" }
]`

// targetSettingsJSON is the target index settings fixture: an empty settings object.
const targetSettingsJSON = `{"settings":{}}`

// targetMappingsJSON is the target index mappings fixture, including the enrich target object.
const targetMappingsJSON = `{
  "mappings": {
    "properties": {
      "lookup_id": { "type": "keyword" },
      "target_name": { "type": "keyword" },
      "source_enrich": {
        "properties": {
          "calculated_value": { "type": "integer" },
          "source_name": { "type": "keyword" },
          "source_label": { "type": "keyword" }
        }
      }
    }
  }
}`

// targetPipelinesJSON is the keyed target pipeline fixture. Its enrich processor references the source policy.
const targetPipelinesJSON = `{
  "%s": {
    "description": "Target enrich pipeline",
    "processors": [
      {
        "enrich": {
          "policy_name": "%s",
          "field": "lookup_id",
          "target_field": "source_enrich",
          "max_matches": 1,
          "ignore_missing": false
        }
      }
    ]
  },
  "%s": {
    "description": "Secondary target pipeline",
    "processors": [
      {
        "set": {
          "field": "secondary_pipeline_ran",
          "value": true
        }
      }
    ]
  }
}`

// targetDataJSON is the target index's two documents, keyed to the source lookup_id values.
const targetDataJSON = `[
  { "lookup_id": "A1", "target_name": "first" },
  { "lookup_id": "B2", "target_name": "second" }
]`

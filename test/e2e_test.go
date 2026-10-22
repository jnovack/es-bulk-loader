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
	"strconv"
	"strings"
	"testing"
	"time"
)

// ─── Harness Types ─────────────────────────────────────────────────────────────

// testEnv groups state used to coordinate related package behavior.
type testEnv struct {
	repoRoot      string
	binaryPath    string
	containerName string
	esURL         string
	httpClient    *http.Client
}

// scenarioContext groups state used to coordinate related package behavior.
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

// commandResult groups state used to coordinate related package behavior.
type commandResult struct {
	output string
	err    error
}

// scenario groups state used to coordinate related package behavior.
type scenario struct {
	name   string
	flags  string
	checks string
	run    func(t *testing.T, ctx *scenarioContext)
}

// sharedEnv defines package-level state shared by related execution paths.
var sharedEnv *testEnv

// ─── Test Entry Points ─────────────────────────────────────────────────────────

// TestMain verifies behavior for the related scenario.
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

// TestEndToEndScenarios verifies behavior for the related scenario.
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

// scenarioDeleteSyncManagedEnrich centralizes this code path so package behavior stays consistent.
func scenarioDeleteSyncManagedEnrich(t *testing.T, ctx *scenarioContext) {
	result := runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, true, ""))
	mustSucceed(t, result)

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
}

// scenarioFlush centralizes this code path so package behavior stays consistent.
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

// scenarioFlushSyncManaged centralizes this code path so package behavior stays consistent.
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

// scenarioAdd centralizes this code path so package behavior stays consistent.
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

// scenarioAddSyncManaged centralizes this code path so package behavior stays consistent.
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

// scenarioDeleteReferencedPolicyFails centralizes this code path so package behavior stays consistent.
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

// scenarioNuke centralizes this code path so package behavior stays consistent.
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

// scenarioNukeDeleteSyncManaged centralizes this code path so package behavior stays consistent.
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

// scenarioEnrichMissingPolicies centralizes this code path so package behavior stays consistent.
func scenarioEnrichMissingPolicies(t *testing.T, ctx *scenarioContext) {
	result := runLoader(t, ctx, nil, sourceArgsNoPolicies(ctx, "-delete", false, true, ""))
	mustSucceed(t, result)
	mustContain(t, result.output, "No enrich policies found; skipping enrich execution")

	assertIndexExists(t, ctx, ctx.sourceIndex)
	assertDocCount(t, ctx, ctx.sourceIndex, 2)
	assertPolicyMissing(t, ctx, ctx.sourcePolicy)
}

// scenarioEnrichExplicitUnknownPolicy centralizes this code path so package behavior stays consistent.
func scenarioEnrichExplicitUnknownPolicy(t *testing.T, ctx *scenarioContext) {
	mustSucceed(t, runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceBaseDir, "-delete", true, true, "")))

	result := runLoader(t, ctx, nil, sourceArgs(ctx, ctx.sourceAppendDir, "-add", false, true, ctx.sourcePolicy+",missing-policy"))
	mustSucceed(t, result)
	mustContain(t, result.output, "Enrich policy not found; skipping")
	mustContain(t, result.output, ctx.sourcePolicy)

	assertDocCount(t, ctx, ctx.sourceIndex, 3)
	assertEnrichBackingIndexExists(t, ctx, ctx.sourcePolicy)
}

// scenarioAliasDeleteCreatesTimestampedIndex centralizes this code path so package behavior stays consistent.
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

// scenarioAliasKeepLastPrunesOldestAfterThirdRun centralizes this code path so package behavior stays consistent.
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

	unresolvedDir := filepath.Join(filepath.Dir(ctx.sourceBaseDir), "source-unresolved")
	files := map[string]string{}
	for _, name := range []string{"mappings.json", "pipelines.json", "policies.json", "data.json"} {
		content, err := os.ReadFile(filepath.Join(ctx.sourceBaseDir, name))
		if err != nil {
			t.Fatalf("read base fixture %s: %v", name, err)
		}
		files[name] = strings.TrimSpace(string(content))
	}
	files["settings.json"] = `{"settings":{"index":{"refresh_interval":"${NOT_SET_ANYWHERE}"}}}`
	writeFixtureDir(t, unresolvedDir, files)

	result := runLoader(t, ctx, nil, sourceArgs(ctx, unresolvedDir, "-delete", true, false, ""))
	mustFailWith(t, result, "${NOT_SET_ANYWHERE}")

	assertIndexExists(t, ctx, ctx.sourceIndex)
	assertDocCount(t, ctx, ctx.sourceIndex, 2)
}

// ─── Environment and Fixture Setup ─────────────────────────────────────────────

// setupTestEnv centralizes this code path so package behavior stays consistent.
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

// teardownTestEnv centralizes this code path so package behavior stays consistent.
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

// repoRoot centralizes this code path so package behavior stays consistent.
func repoRoot() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("unable to determine caller path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..")), nil
}

// startElasticsearchContainer centralizes this code path so package behavior stays consistent.
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

// waitForCluster centralizes this code path so package behavior stays consistent.
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

// newScenarioContext centralizes this code path so package behavior stays consistent.
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

// writeFixtures applies method-specific behavior to keep package workflows consistent.
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

// cleanup applies method-specific behavior to keep package workflows consistent.
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

// writeFixtureDir centralizes this code path so package behavior stays consistent.
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

// runLoader centralizes this code path so package behavior stays consistent.
func runLoader(t *testing.T, ctx *scenarioContext, extraEnv map[string]string, args []string) commandResult {
	t.Helper()

	cmd := exec.Command(ctx.env.binaryPath, args...)
	cmd.Dir = ctx.env.repoRoot

	env := append(os.Environ(),
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

// sourceArgs centralizes this code path so package behavior stays consistent.
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

// sourceArgsNoPolicies centralizes this code path so package behavior stays consistent.
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

// sourceArgsWithNuke centralizes this code path so package behavior stays consistent.
func sourceArgsWithNuke(ctx *scenarioContext, fixtureDir, action string, syncManaged, enrich bool, enrichValue string) []string {
	args := sourceArgs(ctx, fixtureDir, action, syncManaged, enrich, enrichValue)
	return append(args, "-nuke")
}

// targetArgs centralizes this code path so package behavior stays consistent.
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

// nukeArgs centralizes this code path so package behavior stays consistent.
func nukeArgs(ctx *scenarioContext) []string {
	return []string{
		"-url", ctx.env.esURL,
		"-index", ctx.sourceIndex,
		"-pipelines", filepath.Join(ctx.sourceBaseDir, "pipelines.json"),
		"-policies", filepath.Join(ctx.sourceBaseDir, "policies.json"),
		"-nuke",
	}
}

// aliasTargets centralizes this code path so package behavior stays consistent.
func aliasTargets(t *testing.T, ctx *scenarioContext, alias string) []string {
	t.Helper()

	resp := doRequest(t, ctx, http.MethodGet, "/_alias/"+alias, nil)
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("get alias %s failed: status=%d body=%s", alias, resp.StatusCode, string(body))
	}

	var parsed map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode alias response for %s: %v", alias, err)
	}

	targets := make([]string, 0, len(parsed))
	for index := range parsed {
		targets = append(targets, index)
	}
	return targets
}

// singleAliasTarget centralizes this code path so package behavior stays consistent.
func singleAliasTarget(t *testing.T, ctx *scenarioContext, alias string) string {
	t.Helper()
	targets := aliasTargets(t, ctx, alias)
	if len(targets) != 1 {
		t.Fatalf("expected alias %q to have exactly one target, got %v", alias, targets)
	}
	return targets[0]
}

// timestampedIndices centralizes this code path so package behavior stays consistent.
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

// deleteTimestampedIndicesForAlias centralizes this code path so package behavior stays consistent.
func deleteTimestampedIndicesForAlias(t *testing.T, ctx *scenarioContext, alias string) {
	t.Helper()
	for _, index := range timestampedIndices(t, ctx, alias) {
		deleteIndexIfExists(t, ctx, index)
	}
}

// assertTimestampedIndexName centralizes this code path so package behavior stays consistent.
func assertTimestampedIndexName(t *testing.T, alias, index string) {
	t.Helper()
	if !isTimestampedIndexName(alias, index) {
		t.Fatalf("expected index %q to match alias timestamp pattern %s-YYYYMMDDHHMMSS", index, alias)
	}
}

// isTimestampedIndexName centralizes this code path so package behavior stays consistent.
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

// containsString centralizes this code path so package behavior stays consistent.
func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// ─── Assertions and Utilities ──────────────────────────────────────────────────

// assertIndexExists centralizes this code path so package behavior stays consistent.
func assertIndexExists(t *testing.T, ctx *scenarioContext, index string) {
	t.Helper()
	if !indexExists(t, ctx, index) {
		t.Fatalf("expected index %q to exist", index)
	}
}

// assertIndexMissing centralizes this code path so package behavior stays consistent.
func assertIndexMissing(t *testing.T, ctx *scenarioContext, index string) {
	t.Helper()
	if indexExists(t, ctx, index) {
		t.Fatalf("expected index %q to be missing", index)
	}
}

// indexExists centralizes this code path so package behavior stays consistent.
func indexExists(t *testing.T, ctx *scenarioContext, index string) bool {
	t.Helper()
	resp := doRequest(t, ctx, http.MethodHead, "/"+index, nil)
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// assertDocCount centralizes this code path so package behavior stays consistent.
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

// assertDefaultPipeline centralizes this code path so package behavior stays consistent.
func assertDefaultPipeline(t *testing.T, ctx *scenarioContext, index, want string) {
	t.Helper()
	var response map[string]map[string]map[string]map[string]string
	readJSON(t, doRequest(t, ctx, http.MethodGet, "/"+index+"/_settings?filter_path=*.settings.index.default_pipeline", nil), &response)
	got := response[index]["settings"]["index"]["default_pipeline"]
	if got != want {
		t.Fatalf("default pipeline for %s = %q, want %q", index, got, want)
	}
}

// assertNoDefaultPipeline centralizes this code path so package behavior stays consistent.
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

// assertMappingField centralizes this code path so package behavior stays consistent.
func assertMappingField(t *testing.T, ctx *scenarioContext, index, field string) {
	t.Helper()
	if !mappingFieldExists(t, ctx, index, field) {
		t.Fatalf("expected mapping field %q to exist on %s", field, index)
	}
}

// assertNoMappingField centralizes this code path so package behavior stays consistent.
func assertNoMappingField(t *testing.T, ctx *scenarioContext, index, field string) {
	t.Helper()
	if mappingFieldExists(t, ctx, index, field) {
		t.Fatalf("expected mapping field %q to be absent on %s", field, index)
	}
}

// mappingFieldExists centralizes this code path so package behavior stays consistent.
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

// assertPipelineExists centralizes this code path so package behavior stays consistent.
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

// assertPipelineMissing centralizes this code path so package behavior stays consistent.
func assertPipelineMissing(t *testing.T, ctx *scenarioContext, name string) {
	t.Helper()
	resp := doRequest(t, ctx, http.MethodGet, "/_ingest/pipeline/"+name, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected pipeline %q to be missing, status=%d body=%s", name, resp.StatusCode, string(body))
	}
}

// assertPolicyExists centralizes this code path so package behavior stays consistent.
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

// assertPolicyMissing centralizes this code path so package behavior stays consistent.
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

// listPolicyNames centralizes this code path so package behavior stays consistent.
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

// isManagedPolicyForLogical centralizes this code path so package behavior stays consistent.
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

// assertEnrichBackingIndexExists centralizes this code path so package behavior stays consistent.
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

// getDocument centralizes this code path so package behavior stays consistent.
func getDocument(t *testing.T, ctx *scenarioContext, index, id string) map[string]any {
	t.Helper()
	refreshIndex(t, ctx, index)
	var response map[string]any
	readJSON(t, doRequest(t, ctx, http.MethodGet, "/"+index+"/_doc/"+id, nil), &response)
	return response
}

// refreshIndex centralizes this code path so package behavior stays consistent.
func refreshIndex(t *testing.T, ctx *scenarioContext, index string) {
	t.Helper()
	resp := doRequest(t, ctx, http.MethodPost, "/"+index+"/_refresh", nil)
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("refresh %s failed: status=%d body=%s", index, resp.StatusCode, string(body))
	}
}

// doRequest centralizes this code path so package behavior stays consistent.
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

// deleteIndexIfExists centralizes this code path so package behavior stays consistent.
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

// deletePipelineIfExists centralizes this code path so package behavior stays consistent.
func deletePipelineIfExists(t *testing.T, ctx *scenarioContext, name string) {
	t.Helper()
	resp := doRequest(t, ctx, http.MethodDelete, "/_ingest/pipeline/"+name, nil)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		t.Fatalf("delete pipeline %s failed: status=%d body=%s", name, resp.StatusCode, string(body))
	}
}

// deletePolicyIfExists centralizes this code path so package behavior stays consistent.
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

// deleteManagedPoliciesForLogical centralizes this code path so package behavior stays consistent.
func deleteManagedPoliciesForLogical(t *testing.T, ctx *scenarioContext, logical string) {
	t.Helper()
	for _, policy := range listPolicyNames(t, ctx) {
		if policy == logical || isManagedPolicyForLogical(logical, policy) {
			deletePolicyIfExists(t, ctx, policy)
		}
	}
}

// readJSON centralizes this code path so package behavior stays consistent.
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

// mustSucceed centralizes this code path so package behavior stays consistent.
func mustSucceed(t *testing.T, result commandResult) {
	t.Helper()
	if result.err != nil {
		t.Fatalf("command failed: %v\n%s", result.err, result.output)
	}
}

// mustFailWith centralizes this code path so package behavior stays consistent.
func mustFailWith(t *testing.T, result commandResult, substring string) {
	t.Helper()
	if result.err == nil {
		t.Fatalf("expected command failure containing %q\n%s", substring, result.output)
	}
	mustContain(t, result.output, substring)
}

// mustContain centralizes this code path so package behavior stays consistent.
func mustContain(t *testing.T, output, substring string) {
	t.Helper()
	if !strings.Contains(output, substring) {
		t.Fatalf("expected output to contain %q\n%s", substring, output)
	}
}

// assertEqual centralizes this code path so package behavior stays consistent.
func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// nestedString centralizes this code path so package behavior stays consistent.
func nestedString(value map[string]any, path ...string) string {
	current := nestedValue(value, path...)
	if s, ok := current.(string); ok {
		return s
	}
	return ""
}

// nestedFloat centralizes this code path so package behavior stays consistent.
func nestedFloat(value map[string]any, path ...string) float64 {
	current := nestedValue(value, path...)
	if f, ok := current.(float64); ok {
		return f
	}
	return 0
}

// assertMissingField centralizes this code path so package behavior stays consistent.
func assertMissingField(t *testing.T, value map[string]any, path ...string) {
	t.Helper()
	if nestedValue(value, path...) != nil {
		t.Fatalf("expected field %v to be absent", path)
	}
}

// nestedValue centralizes this code path so package behavior stays consistent.
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

// managedPolicyNameForLogicalAndIndex centralizes this code path so package behavior stays consistent.
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

// sourceSettingsJSON defines package-level values shared by related execution paths.
const sourceSettingsJSON = `{"settings":{}}`

// sourceBaseMappingsJSON defines package-level values shared by related execution paths.
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

// sourcePipelinesJSON defines package-level values shared by related execution paths.
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

// sourceSyncPipelinesJSON defines package-level values shared by related execution paths.
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

// sourcePoliciesJSON defines package-level values shared by related execution paths.
const sourcePoliciesJSON = `{
  "%s": {
    "match": {
      "indices": "${INDEX}",
      "match_field": "lookup_id",
      "enrich_fields": ["calculated_value", "source_name", "source_label"]
    }
  }
}`

// sourceSyncPoliciesJSON defines package-level values shared by related execution paths.
const sourceSyncPoliciesJSON = `{
  "%s": {
    "match": {
      "indices": "${INDEX}",
      "match_field": "lookup_id",
      "enrich_fields": ["calculated_value", "source_name", "source_label"]
    }
  }
}`

// sourceInitialDataJSON defines package-level values shared by related execution paths.
const sourceInitialDataJSON = `[
  { "lookup_id": "A1", "base_value": 7, "multiplier": 3, "source_name": "alpha" },
  { "lookup_id": "B2", "base_value": 5, "multiplier": 4, "source_name": "beta" }
]`

// sourceReplacementDataJSON defines package-level values shared by related execution paths.
const sourceReplacementDataJSON = `[
  { "lookup_id": "C3", "base_value": 4, "multiplier": 5, "source_name": "gamma" }
]`

// sourceAppendDataJSON defines package-level values shared by related execution paths.
const sourceAppendDataJSON = `[
  { "lookup_id": "C3", "base_value": 4, "multiplier": 5, "source_name": "gamma" }
]`

// targetSettingsJSON defines package-level values shared by related execution paths.
const targetSettingsJSON = `{"settings":{}}`

// targetMappingsJSON defines package-level values shared by related execution paths.
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

// targetPipelinesJSON defines package-level values shared by related execution paths.
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

// targetDataJSON defines package-level values shared by related execution paths.
const targetDataJSON = `[
  { "lookup_id": "A1", "target_name": "first" },
  { "lookup_id": "B2", "target_name": "second" }
]`

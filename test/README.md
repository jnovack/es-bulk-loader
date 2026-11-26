# Test Guide

This directory contains two different end-to-end testing paths:

- `test/run.sh` is the shell-driven smoke test.
- `test/e2e_test.go` is the Go-native tagged E2E suite.

The Go suite is the primary automated integration test for the loader itself.
It builds the real CLI, runs real commands, and verifies Elasticsearch state
directly.

## Test Layers

Use the normal Go test path for unit tests:

```bash
go test ./...
```

Use the tagged E2E suite when you want real Elasticsearch lifecycle coverage:

```bash
go test -count=1 -tags=e2e ./test -run TestEndToEndScenarios
```

Run the E2E suite in verbose mode to see each scenario and the checks it
performs:

```bash
go test -count=1 -tags=e2e ./test -run TestEndToEndScenarios -v
```

Canonical shortcuts from repo root:

```bash
make test-unit
make test-e2e
make test-e2e-verbose
```

If you see `build constraints exclude all Go files` for `./test`, the command
is missing `-tags=e2e`.
`test/e2e_test.go` is intentionally gated by the `e2e` build tag.

The shell smoke test remains useful for manual debugging:

```bash
./test/run.sh
```

## What The Go E2E Suite Does

The tagged E2E suite in [`e2e_test.go`](e2e_test.go) does the
following:

1. Builds `./cmd/es-bulk-loader` once into a temporary binary.
2. Starts a real Elasticsearch 9 container with Docker.
3. Creates per-scenario fixture directories under `t.TempDir()`.
4. Runs the compiled loader binary with real CLI flags. One scenario,
   [`e2e_inprocess_test.go`](e2e_inprocess_test.go), instead calls
   `loader.Run` in-process.
5. Verifies Elasticsearch with direct HTTP calls.
6. Tears down the Docker container when the package finishes.

This means the tests are not mocking Elasticsearch behavior. They are checking
the actual order of operations and the resulting cluster state.

## Isolation Model

The suite uses one shared Elasticsearch container for the whole `./test`
package, but each scenario is isolated in practice:

- Every scenario gets a unique prefix such as `e2e-01`, `e2e-02`, and so on.
- Indices, pipelines, and policies are named from that prefix.
- `ctx.cleanup(t)` runs before each scenario starts.
- The same cleanup runs again in `t.Cleanup(...)` after the scenario finishes.

That means one scenario should not taint another, because resources do not
share names and are deleted both before and after use.

This is not a brand-new Elasticsearch cluster per test case. It is a shared
cluster with strict per-scenario cleanup. That keeps the suite faster while
still giving strong isolation for this repository.

## Scenario Coverage

The E2E suite is table-driven around loader behavior, not helper functions. The
current scenarios cover:

- `-delete -sync-managed -enrich`
  Recreates the index, applies managed resources, loads data, refreshes, and
  executes enrich.
- `-flush`
  Replaces document data only and leaves managed resources alone.
- `-flush -sync-managed`
  Replaces document data and ensures pipelines and policies exist.
- `-add`
  Appends data only.
- `-add -sync-managed`
  Appends data and ensures pipelines and policies exist.
- `-delete`
  Verifies the safe failure path when another pipeline still references a
  policy being deleted.
- `-nuke`
  Removes the source index and declared managed resources, while preserving
  dependent indices and clearing their `index.default_pipeline` if needed.
- `-nuke -delete -sync-managed`
  Forces teardown first, then performs a clean rebuild.
- `-enrich` with no policy file
  Warns and skips enrich execution cleanly.
- `-enrich=<known>,<missing>`
  Warns for unknown policies and still executes the known policy.
- `-delete -alias`
  Creates a timestamped concrete index and points the alias at it.
- three runs of `-delete -sync-managed -alias -keep-last=2`
  Verifies rollover retention by pruning the oldest timestamped index on the
  third run while pipeline sync remains healthy.
- `-delete -sync-managed -transforms <file> -wait-transforms`
  Runs a batch pivot transform and checks that its destination index already
  holds the expected documents when the loader exits, with no retry loop. The
  scenario deletes its transform and destination index before and after it
  runs.
- `loader.Run` in-process (`in_process_observability`)
  Calls the library directly with a no-op logger and checks the typed
  observation contract. A mixed bulk load must report the rejected item in
  `Result.BulkFailures` with status 400 and the pinned Elasticsearch error
  type, and the failure counts must reconcile. Two
  `-delete -sync-managed -enrich -alias -keep-last 1` rollovers must produce
  the exact `Options.Lifecycle` sequence, with the prune `deleted` event only
  on the second run, one `succeeded` enrich outcome per run, and the alias,
  pruned generations and enrich backing index confirmed on the cluster. A
  sync-only run with an unknown ingest processor must return an error that
  `errors.As` reaches as an `*ElasticsearchError` (HTTP 400, resource
  `pipeline`), leave the pipeline unchanged and emit no successful-action
  event.

Verbose mode prints the flag set and the intended checks for each scenario so
the terminal output is easier to scan than a plain `ok`.

## What The Assertions Check

The suite checks Elasticsearch outcomes, not just process exit codes.

Depending on the scenario, assertions include:

- index exists or is missing
- document counts
- default pipeline settings
- mapping fields present or absent
- pipeline exists or is missing
- policy exists or is missing
- enrich backing index exists
- sample computed fields from ingest pipelines
- sample enriched fields from enrich execution
- expected failure output for safe conflict paths

This matters most for destructive flows. For example, the `-delete` conflict
test verifies that the source index and source pipeline are gone, while the
blocked policy still exists because another pipeline still references it.

## Fixtures

The Go E2E suite writes its own scenario-specific fixtures at runtime. That
keeps the scenarios independent and lets each test vary settings, mappings,
pipelines, policies, and data without colliding with another test.

The older shell fixture files under [`fixtures/`](fixtures) still exist for
`run.sh` and manual debugging:

- [`fixtures/index1-settings.json`](fixtures/index1-settings.json)
- [`fixtures/index1-mappings.json`](fixtures/index1-mappings.json)
- [`fixtures/index1-pipelines.json`](fixtures/index1-pipelines.json)
- [`fixtures/index1-policies.json`](fixtures/index1-policies.json)
- [`fixtures/index1-data.json`](fixtures/index1-data.json)
- [`fixtures/index2-settings.json`](fixtures/index2-settings.json)
- [`fixtures/index2-mappings.json`](fixtures/index2-mappings.json)
- [`fixtures/index2-pipelines.json`](fixtures/index2-pipelines.json)
- [`fixtures/index2-data.json`](fixtures/index2-data.json)

That split is intentional:

- the shell path is a concrete operator smoke test
- the Go path is the fuller scenario matrix for CLI behavior

[`fixtures/elasticsearch/`](fixtures/elasticsearch) is different: it holds
responses recorded from a real Elasticsearch 9.3.1 (enrich task steps, the
child reindex task list and transform `_stats`) that the `pkg/loader` unit
tests replay. See its [README](fixtures/elasticsearch/README.md) for
provenance.

## Shell Helpers

These files support the shell-driven smoke test and manual troubleshooting:

- [`run.sh`](run.sh)
- [`verify.sh`](verify.sh)
- [`setup.sh`](setup.sh)
- [`create-target.sh`](create-target.sh)
- [`compose.yaml`](compose.yaml)

They are still useful, but they are not the main behavioral test path anymore.
The Go tagged E2E suite is.

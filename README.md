# es-bulk-loader

A Go CLI for end-to-end loading of JSON documents into Elasticsearch-compatible
clusters.

`es-bulk-loader` can manage index creation, settings, mappings, ingest
pipelines, enrich policies, transforms, document loading, and optional enrich
execution as one repeatable CLI or CI workflow.

## Features

- Full index lifecycle support: create, replace data, rebuild, and destructive
  cleanup
- Managed Elasticsearch resources: settings, mappings, ingest pipelines, enrich
  policies, and transforms
- Keyed JSON definitions for multiple pipelines or policies in a single file
- Automatic default-pipeline selection from the first declared pipeline when
  settings leave it unset
- Bulk JSON array or NDJSON loading with configurable batch sizes and optional
  `_id` override
- Enrich execution after load, including source-index refresh before policy
  execution
- Transform upsert/start lifecycle with source-index filtering from config
- Mapping preflight guard that validates declared root field types and
  `date_detection` before bulk inserts
- Practical input normalization for wrapped settings, nested `index` settings,
  and `index.*` keys
- Safe-by-default policy deletion, with an explicit destructive override for
  dependent pipeline cleanup
- Graceful handling for clusters that do not expose enrich APIs

## Quick Start

### Build

```bash
go build -o es-bulk-loader ./cmd/es-bulk-loader
```

### Run Example

#### Docker

```bash
docker run --rm jnovack/es-bulk-loader:latest \
  -url https://localhost:9200 \
  -insecureSkipVerify=true \
  -index my-index \
  -settings settings.json \
  -mappings mappings.json \
  -pipelines pipelines.json \
  -policies policies.json \
  -data data.json \
  -batch 500 \
  -delete \
  -sync-managed \
  -enrich
```

#### Go

```bash
go run ./cmd/es-bulk-loader \
  -url https://localhost:9200 \
  -insecureSkipVerify=true \
  -index my-index \
  -settings settings.json \
  -mappings mappings.json \
  -pipelines pipelines.json \
  -policies policies.json \
  -data data.json \
  -batch 500 \
  -delete \
  -sync-managed \
  -enrich
```

Sample config file: [examples/es-bulk-loader.conf](examples/es-bulk-loader.conf)

## Library Usage

`es-bulk-loader` can now be called directly in-process from Go via [`pkg/loader`](pkg/loader).

**Compatibility.** The `pkg/loader` type surface and documented behavior
are stable. Fixes to undocumented or data-corrupting behavior ship in
minor releases with a release note. Changes to documented behavior get a
deprecation path.

**Error messages.** When Elasticsearch rejects a request in the enumerated
HTTP-failure paths (ingest pipelines, enrich policies, transforms, indices,
aliases, the bulk request and the list requests for index settings, ingest
pipelines and enrich policies), `Run` returns a `*RunError` that keeps its
existing Op and Kind and whose message names the resource, the HTTP status and
up to four Elasticsearch type/reason pairs. After the Op prefix the cause reads
`<resource> "<name>" rejected with HTTP <status>: <type>: <reason>`, with
further pairs separated by semicolons. A request with no single resource name
omits the quoted name: `bulk request rejected with HTTP 503: <type>: <reason>`.
Paths that never read a response body, such as `indexops.Delete`, are
unchanged and carry no richer cause.

Error bodies are read with a bound of 64 KiB plus one probe byte; a branch that
needs only the status may close the body without reading. Successful (2xx)
responses that the loader decodes are not subject to this bound. The log line
for a failure carries the same bounded text in its `error` field, and
warnings and debug lines that report a failed request carry it in `es_error`;
the raw response body is never logged or returned. The former `body` log field
is gone. A child-task response that could not be parsed logs only its length
(`body_bytes`).

A direct failure message, Op prefix included, is at most 2,048 bytes. Two
errors wrap the same bounded text inside longer context instead: the
mapping-fetch error (`fetching mappings for index ... failed with status`) and
the transform wait's `_stats returned status` error. Only the Rejection text
inside them is capped at 2,048 bytes; their outer context (index, transform,
timeout and Run Op) is preserved, so the complete message can be longer.

URL credentials, sensitive query parameters, Authorization values and common
secret keys (such as `password`, `api_key`, `token`, `secret`) are replaced
with `[REDACTED]`. An unreadable, oversized (over 64 KiB), empty or non-JSON
response yields a fixed explanation instead. An oversized body is treated as
no match by the loader's body classifiers, so a body-dependent retry or
recovery does not happen. An enrich-policy 404 whose body is oversized or could
not be read never classifies as an unsupported enrich API. Delete fails rather
than assuming the policy is absent, and list fails the run. Create fails unless
an under-bound partial body contains a recognized retry or already-exists
token; those token-based rules are unchanged. Execution counts the policy as
failed and logs the rejection. Status-only retries are
unchanged. Redaction recognizes structured credentials only: free-text reasons
can still contain sensitive prose that generic redaction cannot recognize.

Failure text that Elasticsearch reports inside a successful (2xx) response is
sanitized and bounded the same way, at four sites only:

- A flush that finishes with shard failures returns the same Op and Kind as
  before; its cause now reads
  `index "<index>" flush had <n> shard failures; first: <type>: <reason>`, at
  most 2,048 bytes for the whole message, Op prefix included. The
  `first_failure` log field is gone: the failure line's `error` field carries
  that cause, next to `shard_failures`.
- An enrich task that ends with an error logs it as the string `es_error`
  (sanitized, at most 2,048 bytes). The former `task_error` JSON object field
  is gone.
- A failed bulk item logs `error_type` and `error_reason` as before, now
  sanitized and each at most 2,048 bytes (still at most ten items per batch).
- A failed transform's `transform "<id>" failed: <reason>` error keeps its
  prefix; only the reason is sanitized and capped at 2,048 bytes, so the
  complete message with its outer context can be longer.

These limits apply to each field or reason, not to a whole log line. Successful
response bodies are still read and decoded in full, and the same free-text
limitation applies. Other fields decoded from successful responses are not
covered.

**Structured Elasticsearch cause.** The same rejection is also available as a
typed value, so a caller running with `zerolog.Nop()` does not have to parse
the message. `errors.As` reaches a `*loader.ElasticsearchError` from Run's
error:

```go
_, err := loader.Run(ctx, opts)
var esErr *loader.ElasticsearchError
if errors.As(err, &esErr) {
    log.Printf("%s %q: HTTP %d", esErr.Resource, esErr.Name, esErr.StatusCode)
    for _, cause := range esErr.Causes {
        log.Printf("  %s: %s", cause.Type, cause.Reason)
    }
}
```

| Field | Shape |
| --- | --- |
| `Resource string` | The request noun: `pipeline`, `enrich policy`, `enrich policy list request`, `bulk request`, `transform`, `index`, and so on. It names the request, not a resource kind, and is not a closed set. |
| `Name string` | The sanitized display name, at most 256 bytes (a longer name is cut and ends with the `[truncated]` marker). Empty when the request had no single resource, such as the bulk or a list request. |
| `StatusCode int` | The HTTP status. `0` means the failure was reported inside a successful (2xx) response. |
| `Causes []ElasticsearchCause` | Up to four `{Type, Reason}` pairs, in the message's order: `root_cause` entries, the top-level error, then each one's `caused_by` chain. Each field is at most 2,048 bytes of sanitized, single-line UTF-8. |

`esErr.Error()` is the inner rejection text, for example
`pipeline "p" rejected with HTTP 400: parse_exception: <reason>`. The
`*RunError` keeps the outer operation context: its message is
`<Op>: <that text>`, or `<Op>: <wrapper>: <that text>` for the two wrapped
errors. Op, Kind, message and `errors.Is` results are unchanged.
`esErr.Unwrap()` returns the body-read error when one ended the read, so
`errors.Is(err, context.Canceled)` holds through both values. An unreadable,
oversized, empty, non-JSON or unrecognized body keeps its resource, name and
status, has no `Causes`, and `Error()` holds the fixed explanation. Every
string is redacted as described above; each call returns a fresh value, so
changing its `Causes` affects nothing else.

It is present for every run-ending failure that builds a rejection cause:

- every direct HTTP rejection listed above;
- the mapping fetch before a bulk insert, which keeps Kind `ErrBulkFailure`
  and Op `Mapping preflight failed before bulk insert`;
- a non-retryable transform `_stats` status, which keeps Kind
  `ErrManagedResource` and Op `Transform wait failed`;
- the flush shard failure, with `StatusCode` 0 and the first failure's pairs.

It is absent, and `errors.As` returns false, in these cases:

- Status-only paths such as index deletion (`indexops.Delete`), which carry
  no richer cause.
- A transform wait timeout whose latest poll failed with a retryable `_stats`
  status (429, 502, 503 or 504). That rejection survives only as text in the
  timeout error (`; last poll failed: ...`).
- A response-body read failure on the mapping fetch or the transform `_stats`
  poll. These keep their existing read-error chain without a rejection.
- A transform that reports state `failed` inside a successful `_stats`
  response. Its sanitized reason survives only as text in the
  `transform "<id>" failed: <reason>` error.
- Transport, validation, file and other non-Elasticsearch failures.

Failures that do not end the run, such as a failed enrich policy or a bulk
item rejection, are not returned from `Run`. A failed enrich policy's cause is
in `Result.EnrichOutcomes` (see [Enrich outcomes](#enrich-outcomes)), and
`EnrichOutcome.Err` and the enrich Final `ProgressEvent.Err` also reach an
`*ElasticsearchError` with `errors.As` when that cause is an HTTP rejection or
a task `error`.
Bulk item rejections are sampled in `Result.BulkFailures` (see
[Bulk failure sample](#bulk-failure-sample)).

### Enrich outcomes

`Result.EnrichOutcomes []EnrichOutcome` has one entry per requested enrich
policy: the selected policies in execution order, then one `missing` entry per
name in `Result.EnrichMissing`, following its order (request order, or declared
order for an All selection). A failed policy never stops
the run, so check the outcomes, not just `Run`'s error. Recording does not
need `Options.Progress`.

```go
for _, o := range result.EnrichOutcomes {
    if o.Err != nil {
        var esErr *loaderpkg.ElasticsearchError
        if errors.As(o.Err, &esErr) {
            log.Printf("%s: HTTP %d %v", o.Policy, esErr.StatusCode, esErr.Causes)
        }
    }
}
```

| Field | Shape |
| --- | --- |
| `Policy string` | The managed policy name Run executed, or, for `missing`, the requested name (the declared name for an All selection). |
| `Status EnrichStatus` | `succeeded` (`EnrichStatusSucceeded`), `failed` (`EnrichStatusFailed`), `unsupported` (`EnrichStatusUnsupported`: the execute endpoint answered a generic 404), `cancelled` (`EnrichStatusCancelled`: the Run's context was cancelled or timed out) or `missing` (`EnrichStatusMissing`: requested but absent from a cluster that has policies; nothing was attempted). An Elasticsearch `CANCELLED` task phase is `failed`, not `cancelled`. An unreadable backing alias is `failed` even when the read failed because the context ended, so `errors.Is(Err, context.Canceled)` can hold for a `failed` policy: `Status`, not `errors.Is`, is the classification. |
| `Err error` | The real cause; nil for `succeeded` and `missing`. `errors.Is` and `errors.As` traverse it, for example `context.Canceled`. |

`Result.EnrichFailed` counts every selected outcome that is not `succeeded`
(`failed`, `unsupported` and `cancelled`); `Result.EnrichSucceeded` plus
`Result.EnrichFailed` equals the number of selected outcomes. The list is
empty when enrich is not requested, the policy-list API is unsupported, the
cluster has no enrich policies, or listing policies failed (then `Run` returns
the error). An unsupported execute endpoint produces an `unsupported` policy
outcome. The enrich Final `ProgressEvent.Err` text is
`enrich policy "<name>" failed: <cause>`; earlier versions ended at `failed`.

### Bulk failure sample

`Result.DocumentsFailed` counts every bulk item Elasticsearch rejected. A
bounded sample of them is in `Result.BulkFailures []BulkItemFailure`: the
first rejected items in response order across batches, at most
`MaxBulkFailureSample` (100). `Result.BulkFailuresOmitted` counts the rest, so
`len(BulkFailures) + BulkFailuresOmitted == DocumentsFailed`. The loader does
not retain every rejected document. An item is rejected when its response
status is 300 or higher or it carries an error.

```go
for _, f := range result.BulkFailures {
    log.Printf("%s %s: HTTP %d %s: %s", f.Index, f.ID, f.Status, f.Type, f.Reason)
}
if result.BulkFailuresOmitted > 0 {
    log.Printf("%d more rejected items not sampled", result.BulkFailuresOmitted)
}
```

| Field | Shape |
| --- | --- |
| `Index string` | The concrete index the response named for the item. |
| `ID string` | The document `_id` the response named. |
| `Action string` | The bulk action key of the response item, for example `index`. |
| `Status int` | The item's status from the bulk response. |
| `Type string`, `Reason string` | The Elasticsearch error type and reason; empty when the failed item had a null `error`. |

Every string field is sanitized single-line UTF-8 with recognizable
credentials redacted, at most 2,048 bytes (a longer value ends with the
`[truncated]` marker). The sample is filled when the bulk stage completes and
is kept if a later stage fails. It is nil, with `BulkFailuresOmitted` zero,
when no data action ran, when nothing was rejected, or when the bulk stage
itself failed (`Run` then returns the error). Build and compare
`BulkItemFailure` with keyed fields. The failed-item log lines are unchanged:
they still log the raw `_index` and `_id`.

Full lifecycle run:

```go
package main

import (
    "context"
    "log"

    loaderpkg "github.com/jnovack/es-bulk-loader/pkg/loader"
)

func main() {
    result, err := loaderpkg.Run(context.Background(), loaderpkg.Options{
        URL:           "http://localhost:9200",
        Index:         "slugs",
        SettingsFile:  "./examples/slugs/settings.json",
        MappingsFile:  "./examples/slugs/mappings.json",
        PipelinesFile: "./examples/slugs/pipelines.json",
        PoliciesFile:  "./examples/slugs/policies.json",
        DataFile:      "./examples/slugs/slugs.json",
        DeleteIndex:   true,
        SyncManaged:   true,
        Enrich: loaderpkg.EnrichOptions{
            Enabled: true,
            All:     true,
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("index=%s processed=%d succeeded=%d failed=%d warnings=%d",
        result.WriteIndex,
        result.DocumentsProcessed,
        result.DocumentsSucceeded,
        result.DocumentsFailed,
        len(result.Warnings),
    )
}
```

`Options.Replicas` is a `*int`: `nil` derives `number_of_replicas` from
cluster topology, any non-nil value (including `0`) overrides it for indices
the run creates, and a negative value fails with `ErrInvalidOptions`. Run
never modifies the pointee.

`Options.Logger` is a `*zerolog.Logger` that receives all of Run's log output:

```go
logger := zerolog.New(os.Stderr).With().Str("stage", "slugs").Logger()
result, err := loaderpkg.Run(ctx, loaderpkg.Options{
    URL:         "http://localhost:9200",
    Index:       "slugs",
    SyncManaged: true,
    Logger:      &logger,
})
```

`nil` is safe and keeps today's behavior: Run logs to the zerolog global
(`log.Logger`), read but never modified. Run adds a timestamp to a copy of
whichever logger it uses and never writes the global, so concurrent Runs with
distinct loggers are safe and need no mutex around Run. A Disabled logger such
as `zerolog.Nop()` silences Run. A shared writer must itself support
concurrent writes, and nil-logger callers must not reassign the global while
Run is in flight.

Targeted helpers are also available for orchestration:

- `loader.SyncManaged(ctx, opts)`
- `loader.LoadData(ctx, opts)`
- `loader.ExecuteEnrich(ctx, opts)`

Sentinel error classes are exposed for classification:

- `loader.ErrInvalidOptions`
- `loader.ErrIndexOperation`
- `loader.ErrManagedResource`
- `loader.ErrBulkFailure`
- `loader.ErrEnrichExecution`
- `loader.ErrLoaderExecution`

### Progress and waiting for transforms

`Options.Transforms` and `Options.Progress` report and bound long waits:

```go
result, err := loaderpkg.Run(ctx, loaderpkg.Options{
    URL:            "http://localhost:9200",
    Index:          "slugs",
    SyncManaged:    true,
    TransformsFile: "transforms.json", // definitions with source_index "slugs"
    Transforms: loaderpkg.TransformOptions{
        Wait:        true,             // block until each transform's first checkpoint
        WaitTimeout: 10 * time.Minute, // per transform; <= 0 means 30m
    },
    Progress: func(e loaderpkg.ProgressEvent) {
        log.Printf("%s %s phase=%q done=%d total=%d final=%v",
            e.Stage, e.Resource, e.Phase, e.Done, e.Total, e.Final)
    },
})
```

`Progress` is nil by default and then creates no events. Each event is a
`ProgressEvent`:

| Field | Meaning |
| --- | --- |
| `Stage` | `ProgressStageBulk`, `ProgressStageEnrich` or `ProgressStageTransform` |
| `Resource` | Write index (bulk), policy name (enrich), transform ID (transform) |
| `Phase` | Phase or state as Elasticsearch reports it; enrich uses the step label (`reindex`, `force-merge`, `refresh`, `promote`; other values pass through) |
| `Done`, `Total` | `bulk`: documents processed and in the file. `enrich`: documents done and total from the child reindex task during the `reindex` step, otherwise -1. `transform`: `Done` is cumulative `documents_processed`, `Total` is the current checkpoint's `total_docs`. -1 always means unknown, never an estimate |
| `Elapsed` | Time since the loader began tracking the resource |
| `Final` | Last event for this `(Stage, Resource)` in this Run |
| `Err` | Set only on a Final event for a resource that failed while the Run continued (an enrich policy). 9.3.1 stores no result for an async enrich task, so its task GET usually ends with HTTP 404; the loader then reads the policy's `.enrich-<policy>` alias, once before kickoff and once after the 404. It counts the policy as succeeded only if the alias moved to a new, non-empty target set. An unchanged or absent alias, or an alias read that fails (the credentials need alias metadata read access), sets `Err`. A concurrent execution of the same policy by another client also moves the alias and cannot be told apart. See [ADR 0004](docs/decisions/0004-progress-reporting-and-transform-wait.md). The text is `enrich policy "<name>" failed: <cause>`, the same cause as the policy's `EnrichOutcome.Err` ([Enrich outcomes](#enrich-outcomes)); `errors.Is` and `errors.As` reach it, including an `*ElasticsearchError` for an HTTP rejection or a task `error`. A succeeded policy's Final event has a nil `Err`, and a missing policy gets no event; every selected policy gets exactly one Final event and the Run continues after a failure |

Delivery contract:

- Events arrive synchronously on the goroutine that called `Run`, never
  concurrently. The callback must return promptly and must not panic; a panic
  with an `error` value fails `Run` through its existing recover.
- Build and compare events with keyed fields. Fields may be added in minor
  releases.
- A transform started without `Wait` gets one Final event with Phase
  `"started"`. Bulk item failures never set `Err`; see
  `Result.DocumentsFailed`.
- When `Run` returns an error, the resource that caused it gets no Final event
  and no events follow. Treat "`Run` returned" as the end of the stream and do
  not count Final events.
- Transient poll failures are retried with no new option, and a failed poll
  creates no event. A transform `_stats` poll retries HTTP 429, 502, 503 and
  504, network errors only when they are timeouts, and non-network transport
  errors such as a failed response read, all within the original
  `WaitTimeout`; a timeout error names the
  latest failed poll. An enrich task poll retries the same four statuses, not
  transport errors, and the third consecutive one fails the policy (`Err`
  set, the Run continues). A decoded task poll resets that count. A task 404
  after retries is still verified through the backing alias. Retries log a
  Warn and do not add to `Result.Warnings`. See
  [Waiting for transforms](#waiting-for-transforms).

### Logging levels

For every long wait the loader logs stage start and stage end at Info, each
poll or batch at Debug, and an Info heartbeat at most once every 30 seconds
while an enrich policy or transform wait is still running. At the default
`-level info` an enrich policy therefore logs its start and task ID, each step
change, document progress during `reindex`, and its completion. A retried
transient poll failure logs one Warn per retry.

## Testing

Unit tests stay in the default Go test path:

```bash
go test ./...
```

Deprecated-API check (staticcheck is pinned in `go.mod` and built with
the active toolchain):

```bash
go tool staticcheck -checks=SA1019 ./...
go tool staticcheck -tags=e2e -checks=SA1019 ./...
```

`make lint` runs both commands in that order and stops at the first failure.
It fails on any finding or tool error. It runs no tests and needs no Docker.

Canonical E2E command (also available as `make test-e2e`) uses the tagged
suite entrypoint:

```bash
go test -count=1 -tags=e2e ./test -run TestEndToEndScenarios
```

`-count=1` disables Go's test cache: the suite builds the CLI in a
subprocess, so a cached result cannot see loader changes.

Verbose E2E output:

```bash
go test -count=1 -tags=e2e ./test -run TestEndToEndScenarios -v
```

If you see `build constraints exclude all Go files` for `./test`, the run is
missing `-tags=e2e`.
The E2E package is intentionally build-tagged.

The `go-check` GitHub Actions workflow
(`.github/workflows/go-check.yml`) runs on every branch push and on pull
requests, as two independent jobs. `check` runs a non-mutating `gofmt -l .`
gate, `go vet ./...`, `go vet -tags=e2e ./...`, `go test -race ./...` and
`make lint`. `e2e` runs `make test-e2e` and then the focused port-parser
regression, which `make test-e2e` does not select:

```bash
go test -count=1 -tags=e2e ./test -run '^TestPublishedPort$' -v
```

Both E2E commands start the Docker-backed harness. The workflow does not run
on tags and does not gate image or release publication.

Makefile shortcuts:

```bash
make test-unit
make test-e2e
make test-e2e-verbose
make lint
```

## Loader Workflow

The loader now separates three concerns:

1. `-nuke` removes the current index and declared managed resources first.
2. One optional data action, exactly one of `-add`, `-flush`, or `-delete`,
   controls how documents are handled.
3. `-sync-managed` creates or updates declared pipelines, policies, and
   transforms. Against an existing index or alias it updates them in place
   without recreating the index. Without a data action or `-nuke`, it needs an
   existing index or alias, or `-settings`/`-mappings` that let it create one.
   After `-nuke`, it rebuilds only when the run creates an index from
   `-settings`/`-mappings` or a data action with `-data`; otherwise nothing is
   recreated.

When combined, the execution order is:

1. Parse the `-mappings` file, when a data action is selected, before any
   destructive work, so a malformed file fails the run before anything is
   deleted.
2. Run `-nuke`, if requested. With `-sync-managed` and no data action,
   `-settings` or `-mappings`, this is a reset that recreates nothing, and the
   loader warns before sending any request.
3. For `-add` or `-flush` into an existing index or alias, verify the live
   mappings against `-mappings` before any document or managed-resource
   change.
4. Run the selected data action, if any (one of `-add`, `-flush`, or
   `-delete`).
5. Create required ingest pipelines for index creation when
   `index.default_pipeline` is configured.
6. Create the index when needed, applying settings and mappings.
7. Bulk load data, if a data action was selected.
8. Update alias pointers, when alias mode creates a new concrete index.
9. Create or update enrich policies during `-sync-managed` once the source
   index/alias is ready.
10. Refresh the source index and execute enrich policies when `-enrich` is
    requested.
11. Create or update selected transforms during `-sync-managed`.
12. Start selected transforms after enrich execution (or immediately after
    transform upsert when enrich is disabled).

That ordering matters. The loader is intentionally opinionated so CI runs and
operator workflows stay predictable.

When `-mappings` is provided with a data action, the loader derives a
validation plan from the declared mapping and checks the live index mappings
against it:

- Every declared top-level field with a `type` must exist in the index with
  that type.
- A declared `mappings.date_detection` must match the index (an index that
  omits it has the Elasticsearch default, `true`).

When the plan runs depends on whether the index already exists:

- **Existing index** (`-add` or `-flush`): the check runs before the flush and
  before any `-sync-managed` pipeline or policy upsert. Any mismatch, such as a
  declared field the index lacks or a field with a different type, fails the
  run with the index untouched. The error lists every mismatched field.
  With `-alias`, every concrete index behind the alias is verified, because
  `-flush` empties each of them. The error names the alias and each failing
  index, in sorted order, separated by ` | `.
  The loader resolves the alias targets and reads their mappings in two
  separate requests, and the index names in the two answers must be the same
  set. If the mappings response omits a resolved target, or contains an index
  that is not a resolved target, the run fails before any flush, bulk load or
  managed upsert with `alias "<name>": mappings response does not match the
  alias targets: missing <names>; unexpected <names>` (names sorted, empty
  groups omitted). A concrete index that shares the alias name is compared the
  same way. This compares two snapshots; it does not stop another writer from
  changing the alias or mappings after they were read, so avoid concurrent
  lifecycle changes. Without mapping expectations (no `-mappings`, or a file
  declaring no fields or `date_detection`) no mappings are read and nothing
  is compared.
- **Created index** (`-delete`, or `-add`/`-flush` with no existing index or
  alias target): the check runs once, after the index is created and before
  the bulk load. Managed-resource setup for index creation can happen between
  those two points.

`-flush` and `-add` never add or change mappings on an existing index.
Changing mappings requires `-delete`, or `-delete -alias` rollover to a new
timestamped index.

## Command-Line Flags

Settings can be loaded from a configuration file (e.g.
`-config es-bulk-loader.conf`), the environment, or from the command-line.

| Flag | Description |
| --- | --- |
| `-config` | Path to configuration file with settings |
| `-url` | Endpoint URL (e.g., `http://localhost:9200`) |
| `-insecureSkipVerify` | Skip TLS verification for HTTPS |
| `-index` | Target index name (**required**) |
| `-alias` | Treat `-index` as an alias and create timestamped indices as `<alias>-YYYYMMDDHHMMSS` when creating a new index |
| `-keep-last` | With `-alias`, keep only the newest N timestamped indices matching `<alias>-YYYYMMDDHHMMSS` (default: 0, disabled) |
| `-replicas` | `number_of_replicas` for indices this run creates; env `REPLICAS` (default: -1, derive from cluster node count; values below -1 are rejected) |
| `-settings` | Optional path to JSON file with index settings |
| `-mappings` | Optional path to JSON file with index mappings |
| `-pipelines` | Optional path to JSON file with one or more ingest pipeline definitions |
| `-policies` | Optional path to JSON file with one or more enrich policy definitions |
| `-transforms` | Optional path to JSON file with one or more transform definitions |
| `-wait-transforms` | After starting managed transforms, wait for each to complete its first checkpoint; env `WAIT_TRANSFORMS` (requires `-sync-managed`) |
| `-wait-transforms-timeout` | Maximum time to wait for each transform when `-wait-transforms` is set; env `WAIT_TRANSFORMS_TIMEOUT` (default: 30m; 0 or less is rejected when `-wait-transforms` is set) |
| `-batch` | Number of documents per bulk insert (default: 1000) |
| `-bulk-retry-attempts` | Total bulk API request attempts for retryable failures, including the first attempt (default: 4) |
| `-bulk-retry-backoff-base` | Base backoff duration for retryable bulk failures (default: 500ms) |
| `-bulk-retry-backoff-max` | Maximum backoff duration for retryable bulk failures (default: 5s) |
| `-add` | Append data to an existing index or create the index first if it does not exist |
| `-flush` | Delete all documents from an existing index without deleting the index, then load replacement data |
| `-delete` | Recreate data target before loading data: deletes concrete index in normal mode; rolls alias to a new timestamped index in `-alias` mode |
| `-data` | Path to a JSON array or NDJSON document file to load (**required with** `-add`, `-flush`, or `-delete`) |
| `-sync-managed` | Create or update declared ingest pipelines, enrich policies, and transforms. After `-nuke`, this rebuilds only when the run creates an index (`-settings`/`-mappings`, or a data action with `-data`) |
| `-nuke` | Delete the current index and declared managed resources first, including dependent pipelines that reference declared enrich policies. With `-sync-managed` but no data action, `-settings` or `-mappings`, nothing is recreated and the loader warns |
| `-id` | Field to use in the document to override _id (default: not set) |
| `-enrich` | Run enrich policies after the bulk insert; omit value for all or pass a comma-separated list |
| `-user` / `-pass` | Username and password for Basic Auth; both are required together. `-user` is not read from the environment (`USER` is a shell variable); set it with `-user` or a `user` line in the `-config` file. `-pass` still reads `PASS` |
| `-apiKey` | Elasticsearch API key |
| `-level` | Log level filter: `trace`, `debug`, `info`, `warn`, or `error` (default: `info`) |
| `-version` | Print version and exit |

## Behavior Summary

`-add`, `-flush`, and `-delete` are mutually exclusive.

| Flags | Effect |
| --- | --- |
| `-add` | Append data to an existing index, or create the index and load data if it does not exist |
| `-flush` | Remove existing documents, keep the existing index structure, then load replacement data |
| `-delete` | Recreate data target and reload data: concrete index is deleted/recreated in normal mode; alias mode creates a new timestamped index and repoints alias |
| `-sync-managed` | Create or update declared pipelines, policies, and transforms without changing document data by itself. After `-nuke`, it recreates resources only when the run creates an index from `-settings`/`-mappings` or a data action with `-data`. With no data action and no `-nuke`, a missing index or alias and no `-settings`/`-mappings` fails with `ErrIndexOperation` before any write; supply `-settings` or `-mappings`, or choose `-add`, `-flush` or `-delete` (with `-data`) |
| `-nuke` | Remove the current index and declared managed resources first; if combined with another action, that action runs afterward. `-nuke -sync-managed` with no data action, `-settings` or `-mappings` is a reset that recreates nothing and warns |
| `-alias` | Treat `-index` as an alias and use timestamped concrete index names (`<alias>-YYYYMMDDHHMMSS`) when a new index is created |
| `-keep-last` | With `-alias`, prune older timestamped concrete indices after the run and keep only the newest N by parsed timestamp suffix |

Common combinations:

- `-add -sync-managed`: append documents and ensure declared pipelines,
  policies, and transforms exist
- `-flush -sync-managed`: replace documents, keep settings and mappings, and
  ensure declared pipelines, policies, and transforms exist
- `-delete -sync-managed`: rebuild the index from scratch and recreate declared
  pipelines, policies, and transforms
- `-nuke -delete -sync-managed`: force a full teardown first, then rebuild
  cleanly
- `-nuke`: remove the current index and declared managed resources without
  loading new data
- `-nuke -sync-managed` (no data action, `-settings` or `-mappings`): a
  managed-resource reset. It deletes the index and declared pipelines,
  policies, and transforms, recreates nothing, and logs a warning (also in
  `Result.Warnings`). To rebuild, add `-settings`/`-mappings` or a data action
  with `-data`, for example:

  ```bash
  es-bulk-loader -index cards -nuke -delete -sync-managed -data cards.ndjson
  ```

- `-delete -alias -keep-last 2`: roll to a new timestamped index, repoint
  alias, then keep only the newest two generations

### Data File Formats

Two formats are accepted. Format is detected automatically from the first
non-whitespace byte; no flag is required.

**JSON array** (original format):

```json
[
  { "id": "1", "name": "Alice" },
  { "id": "2", "name": "Bob" }
]
```

**NDJSON** (newline-delimited JSON, one object per line):

```text
{"id":"1","name":"Alice"}
{"id":"2","name":"Bob"}
```

NDJSON is preferred for large datasets. It enables streaming decode with
O(1) memory per document and is the canonical format for ES bulk-API
interchange.

## Alias Mode

When `-alias` is set, `-index` is interpreted as an Elasticsearch alias name.
The loader uses Elasticsearch terms in logs:

- `alias`: logical name provided by `-index` (for example `cards`)
- `index`: concrete index name (for example `cards-20260319130459`)

Concrete index names created by alias mode always use a numeric suffix:

- `<alias>-YYYYMMDDHHMMSS`

Data-action behavior in alias mode:

- `-delete`: create a new timestamped index, load data, then point the alias
  to the new index (older timestamped indices can be pruned with `-keep-last`)
- `-flush`: flush documents from current alias target indices, then load
  replacement data
- `-add`: append to current alias target index; if the alias has no index yet,
  create a first timestamped index and load data

`-keep-last` only applies when `-alias` is enabled. It parses and sorts
timestamped concrete index names, then deletes older ones so only the newest N
remain.
In alias mode, `-delete` keeps existing managed resources
(pipelines/policies/transforms) and relies on `-sync-managed` to upsert
definitions; use `-nuke` when you want destructive managed-resource cleanup.
If `-delete -alias` is run without `-sync-managed`, the loader logs a warning
and assumes `-sync-managed` for that run. Add `-sync-managed` explicitly in
automation for clarity.
If `-delete -alias` is run without `-keep-last`, the loader logs a warning that
no old generations were deleted and storage use can grow over time.

Example progression with `-keep-last 2`:

1. Run 1: keep `cards-20260319130000`
2. Run 2: keep `cards-20260319130000`, `cards-20260319130500`
3. Run 3: create `cards-20260319131000`, then prune oldest so remaining are
   `cards-20260319130500`, `cards-20260319131000`

## Enrich Policies

Use `-enrich` after a bulk load when enrich policy backing indices need to be
rebuilt.

When `-pipelines` and `-policies` are supplied, the loader imports those
definitions as part of the run:

- `-sync-managed` creates pipelines, attempts to create declared enrich
  policies, and upserts matching transforms.
- Declared enrich policies are managed by content hash during `-sync-managed`:
  each logical policy key is resolved to `<logical>-<sha256[:6]>`.
- If a policy definition is unchanged, the same managed policy name is reused.
- If a policy definition changes, a new managed policy name is created and
  pipelines are rewritten to reference it.
- Old managed policy versions are garbage-collected when they are unreferenced
  by any pipeline.
- A pipeline reference to a logical policy with no supplied definition fails
  the run, naming the candidates, when several managed generations match it.
- `-delete` fails loudly if a declared enrich policy is still referenced by
  another ingest pipeline. This is the safe default because those references
  may belong to another index workflow.
- `-nuke` keeps deleting through that conflict by finding and deleting ingest
  pipelines that reference the declared enrich policy. If one of those
  pipelines is still configured as `index.default_pipeline` on another index,
  the loader clears that setting on the dependent index before retrying the
  pipeline delete.
- `-add` and `-flush` affect documents only. They do not modify pipelines,
  policies, or transforms unless `-sync-managed` is also set.

> [!CAUTION]
> Use `-nuke` carefully. Unlike the normal one-index workflow, it can remove
> ingest pipelines outside the current index's declared pipeline file and clear
> `index.default_pipeline` on dependent indices if those pipelines reference a
> declared enrich policy you are deleting.

```bash
go run cmd/es-bulk-loader/main.go \
  -url https://localhost:9200 \
  -index my-index \
  -sync-managed \
  -data data.json \
  -enrich
```

When `-enrich` is provided without an explicit policy list and `-policies` is
also provided, the loader executes the resolved managed policy names declared
for that run. If no policy file is provided, it falls back to all enrich
policies currently available in the cluster.

Run only specific policies:

```bash
go run cmd/es-bulk-loader/main.go \
  -url https://localhost:9200 \
  -index my-index \
  -sync-managed \
  -data data.json \
  -enrich=policy-a,policy-b
```

Unknown policy names are logged as warnings and skipped. When a policy file is
provided, explicit logical policy names are mapped to the resolved managed
policy names automatically.
If the cluster does not expose the enrich APIs at all, the loader logs a
warning and skips enrich policy create/delete/execute operations instead of
aborting the whole load.

## Transforms

When `-transforms` is provided, definitions are read from a keyed JSON object:

- JSON key: transform ID.
- `source_index`: logical source index name that must match the current
  `-index`.
- `body`: native Elasticsearch transform definition passed directly to the
  transform APIs.

Only transforms whose `source_index` matches the current `-index` are selected
for the run.
During `-sync-managed`, selected transforms are stopped, upserted after data
load and alias updates, and then started after enrich execution (or immediately
after upsert when enrich is disabled).
During `-nuke`, selected transforms are force-deleted.

### Waiting for transforms

By default the loader starts transforms and returns at once. With
`-wait-transforms` it then polls `GET _transform/<id>/_stats` for each synced
transform, in order, until the first checkpoint completes:

- A batch transform (body has no top-level `sync`) is done when its state is
  `stopped` and `checkpointing.last.checkpoint` is 1 or higher.
- A continuous transform (body has a top-level `sync`) is done when
  `checkpointing.last.checkpoint` is 1 or higher and no later checkpoint is in
  progress.
- A transform in state `failed` fails the run. The error names the transform
  and quotes the reason Elasticsearch reports.
- `-wait-transforms-timeout` (default `30m`) is per transform and bounds both
  the HTTP calls and the sleeps. When it is reached, or the context is
  cancelled, the run fails with an error naming the transform, its last state
  and its last known progress.
- A transient `_stats` failure is retried, with no flag to set: HTTP 429, 502,
  503 or 504, a network timeout, or a non-network transport error such as a
  dropped connection's `EOF` or a failed response read. Each retry logs one
  Warn naming the
  transform, the poll number and the cause, and waits the normal poll
  interval. Retries never extend the wait: the original timeout covers them,
  and if it ends while polls are failing, the error appends
  `last poll failed: <cause>` with the latest cause. A valid poll clears that
  cause.
- Everything else fails at once: any other non-2xx status (400, 404, 500),
  a malformed `_stats` body, any network error that is not a timeout (such
  as a connection refused or reset, or a DNS lookup failure), and a `failed`
  state.

A failure or timeout returns a `loader.ErrManagedResource` error. If no
transforms are synced (no `-sync-managed`, or none match `-index`), the loader
warns and makes no `_stats` request.

```bash
go run cmd/es-bulk-loader/main.go \
  -url http://localhost:9200 \
  -index my-index \
  -delete -sync-managed \
  -data data.json \
  -transforms transforms.json \
  -wait-transforms -wait-transforms-timeout 10m
```

Definition file formats are documented in [docs/PIPELINES.md](docs/PIPELINES.md)
and [docs/POLICIES.md](docs/POLICIES.md).

When `-pipelines` is supplied during index creation, the loader preserves the
JSON key order from the pipeline file. If the settings file does not already
define a default pipeline, the first declared pipeline becomes the index
default pipeline automatically.

Definition files support variable expansion before they are parsed. `${INDEX}`
is populated from the current `-index` value (alias name when `-alias` is
enabled), and other `${NAME}` placeholders fall back to environment variables
when present. Only the braced `${NAME}` form is expanded; a bare `$NAME` (for
example `$HOME`) is left verbatim.

A `${NAME}` found in neither `Options.TemplateVariables` nor the environment is
invalid input: the run fails with `loader.ErrInvalidOptions` before any cluster
change, and the error lists every unresolved name per file. This applies to
every supplied template file (settings, mappings, pipelines, policies and
transforms), including one this run would not otherwise read, so a typo in an
unused `-settings` file still fails a `-sync-managed`-only run.

Read and parse failures are checked early too. When the run has a data action
(`-delete`, `-flush`, `-add`) or effective `-sync-managed` (including the
assumed one for `-delete -alias`), the `-settings` and `-mappings` files must
exist and parse as JSON before the first `-nuke`, `-delete` or `-flush`
request. This also applies to `-add`/`-flush` against an existing index and to
sync-managed-only runs, even when no index ends up being created. When there is
a data action, the `-data` file must open and its first non-whitespace byte
must be `[` (JSON array) or `{` (NDJSON). Enrich-only and nuke-only runs
without managed sync do not inspect these files.

This is not full validation. Invalid JSON later in the data file,
Elasticsearch rejecting a settings or mappings body, and a file or environment
value that changes after the check can still fail after the cluster has been
modified.

## JSON Formats

### `data.json`

```json
[
  { "id": 1, "name": "Alice" },
  { "id": 2, "name": "Bob" }
]
```

### `settings.json` (optional)

```json
{
  "number_of_shards": 1,
  "number_of_replicas": 1
}
```

Wrapped settings are also accepted:

```json
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1
  }
}
```

Nested `index` settings are also accepted and normalized:

```json
{
  "settings": {
    "index": {
      "number_of_shards": 2,
      "number_of_replicas": 1
    }
  }
}
```

Dotted `index.*` keys are also accepted:

```json
{
  "index.number_of_shards": 2,
  "index.number_of_replicas": 1,
  "index.default_pipeline": "my-default-pipeline"
}
```

All of these shapes are normalized into the create-index request body the
loader sends to Elasticsearch.

`number_of_replicas` is always chosen by the loader when it creates an index
(including each alias generation), in this order of precedence:

| Source | Wins? |
| --- | --- |
| `-replicas` / `REPLICAS` / `Options.Replicas` | Always, including an explicit `0` |
| Cluster topology (1 node: `0`, more: `1`) | Only when no override is supplied |
| `number_of_replicas` in `settings.json` | Never; it is ignored |

The override applies only at index creation. `-add` and `-flush` on an
existing index never change its replica count. See
[ADR 0001](docs/decisions/0001-replica-count-from-node-count.md).

### `mappings.json` (optional)

```json
{
  "properties": {
    "id":   { "type": "integer" },
    "name": { "type": "text" }
  }
}
```

Wrapped mappings are also accepted:

```json
{
  "mappings": {
    "properties": {
      "id": { "type": "integer" },
      "name": { "type": "text" }
    }
  }
}
```

## 🛡 Requirements

- Go 1.25+
- Elasticsearch 7.x, 8.x, or 9.x (tested with v9 client) or Opensearch (does
  not support enrich policies)

## 👥 License

MIT License © 2025-2026

## Disclaimer

This project is an independent, open-source tool and is not affiliated with,
endorsed by, or sponsored by Elastic. “Elasticsearch” and “Elastic” are
trademarks of Elasticsearch B.V.

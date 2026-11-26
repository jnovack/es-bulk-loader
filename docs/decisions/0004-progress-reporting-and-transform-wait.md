# ADR 0004: Progress reporting and opt-in transform wait

- Status: Accepted
- Date: 2026-10-22

## Context

Enrich execution and transforms are the longest waits in a run. At the default
log level enrich was silent from start to finish, `startTransforms` returned
before any transform did work, and in-process callers (moxfall,
moxfield-collection-downloader) could only see progress by parsing logs.
`pkg/loader` is imported as a library, so every exported name is a semver
commitment.

## Decision

- **Opt-in transform wait.** `Options.Transforms.Wait` (CLI
  `-wait-transforms`) polls `_stats` per synced transform until its first
  checkpoint. The zero value keeps today's behavior.
- **Public types in `loader`, mechanics under `internal/`.** `ProgressEvent`,
  `ProgressStage`, `TransformOptions` and `DefaultTransformWaitTimeout` live in
  `pkg/loader/progress.go`. Poll cadence, heartbeat, enrich step and child
  reindex parsing, and `_stats` parsing and done rules live in
  `pkg/loader/internal/{progress,enrich,transform}`. Those pure parsers and
  done rules do not log; the wait and lifecycle owners that call them
  (`transform.Wait`, `WaitForCheckpoint`, enrich's poll loop) do.
- **Function-field callback.** `Options.Progress func(ProgressEvent)`, called
  synchronously on Run's goroutine.
- **Grouped options.** `TransformOptions{Wait, WaitTimeout}` follows the
  `EnrichOptions` pattern.
- **Log-level rule.** Stage start and end at Info, each poll or batch at
  Debug, plus an Info heartbeat at most every 30s during enrich polling and
  transform waits. A retried transient poll failure logs one Warn.
- **Final/Err rule.** Each resource that completes, or fails without failing
  the Run, gets one Final event. A resource that causes Run to return an error
  gets none, and no events follow; Run's error is authoritative.

## Alternatives Considered

- Always wait for batch transforms: rejected, it changes today's behavior and
  can block runs for up to the timeout.
- Snapshot-only (report state once, never wait): rejected, callers that need
  the destination filled still cannot wait.
- An exported subpackage for the mechanics: rejected, it would make parsers
  and done rules public API.
- An interface or channel instead of a function field: rejected, an interface
  adds methods callers can break on, and a channel adds delivery and blocking
  semantics.
- Flat `Options` fields instead of `TransformOptions`: rejected in favor of
  the existing grouping pattern.
- A Final event with `Err` before every run-ending error: rejected, it needs a
  `recover` around each stage because those errors are `fatal()` panics, and
  the project prohibits clever control flow in lifecycle ordering.
  - Note, 2026-10-29: REFACTOR-INTERNAL-01 replaced those panics with returned
    operation errors, so the premise changed. The decision stands: Run returns
    at the first failure, which stays the only failure signal, and no Final
    event precedes a run-ending error.

## Consequences

- A slow callback delays the run. Callers must return promptly.
- Elasticsearch fields not reported are -1, never estimated.
- Enrich now issues best-effort child-task reads while reindexing; they never
  change a policy's outcome.
- Task polls that return a non-retryable non-2xx status, or a task `error`,
  now count as a failed policy instead of looping or being logged as success.
  Transient statuses are retried within a bound; see the poll-retry entry
  below.
- An enrich task whose record is gone (HTTP 404) is verified, not assumed.
  Elasticsearch 9.3.1 never stores the result of an async enrich task, so this
  is the normal ending, and a policy that failed between polls looks the same.
  - Amended 2026-10-30 (FIX-ENRICH404-01): before kickoff the loader reads the
    `.enrich-<policy>` alias, using the selected policy's actual managed name
    with its hash suffix. On a task 404 it reads the alias again. The policy
    succeeds only if both reads succeed and the alias now has a non-empty
    target set that differs from the earlier one. Comparing sets ignores the
    order Elasticsearch lists them in. An unchanged or absent alias fails the
    policy. So does either unreadable snapshot: transport error, non-404
    status (for example a 403 from missing alias metadata privileges),
    malformed JSON or a JSON `null` body. An unreadable snapshot is never
    treated as an absent alias.
  - A failed policy is counted in `Result.EnrichFailed` and gets one Final
    event with `Err`; later policies and stages still run and Run does not
    fail. Each policy makes one extra alias GET before kickoff and one more
    after a task 404. A synchronous or completed task status stays
    authoritative: it ignores a failed first read and makes no second read.
  - The `.enrich-<policy>` alias convention is Elasticsearch behavior that the
    Docker E2E proves on 9.3.1, not a documented API contract. A changed alias
    shows that the policy produced new output, not that this task produced
    it: another client running the same policy at the same time is
    indistinguishable.
  - Guards: `TestExecutePolicyVerifiesTask404`,
    `TestExecutePolicyExplicitOutcomesSkipAliasCheck`,
    `TestProgressEnrichAliasFailureContinues` and
    `TestEndToEndScenarios/delete_sync-managed_enrich`.
- Poll failures fail closed unless they are transient.
  - Amended 2026-10-30 (FEAT-WAITRETRY-01): before this, a transform wait failed
    on the first transport error or non-2xx `_stats` reply, and an enrich
    policy on the first non-2xx task poll. One 503 during a long wait, after
    the data was loaded, forced a full rerun. Both waits now retry HTTP 429,
    502, 503 and 504 (`esresponse.IsRetryableStatus`, the bulk set) with no
    new option. The status alone decides: the body of such a reply is
    diagnostic text, never parsed as stats or a task outcome.
  - Transform `_stats` polls also retry transport errors that
    `esresponse.IsRetryableTransportError` accepts, the bulk classifier: a
    `net.Error` only when `Timeout()` is true, never cancellation, and any
    other error (for example a dropped connection's `io.EOF` or a failed
    response read). A connection refused or reset fails at once. Retries are
    bounded only by the wait timeout: the one deadline set at entry covers
    every request and sleep, so a persistent outage ends the wait at the
    original deadline. The timeout error then names the latest retried
    failure since the last valid poll; a valid poll clears it.
  - Enrich task polls retry the transient statuses only, not transport
    errors. The third consecutive transient status fails the policy (two
    retries), logged at Error with its status and redacted ES error
    (`es_error`). A decoded task poll
    resets the streak, and each policy has its own. This caps a failure
    streak, not execution time: a healthy running task still waits for
    completion or cancellation. The kickoff, alias reads and child-task reads
    are never retried, and a task 404 after retries is still verified
    through the backing alias as above.
  - Both waits keep the shared poll cadence and log one Warn per retry,
    naming the resource, attempt and cause. These Warns do not add to
    `Result.Warnings`. Every other status, a malformed 2xx body and a failed
    state still fail at once, so no invalid reply can imply progress or
    completion.
  - Guards: `TestIsRetryableStatus`, `TestIsRetryableTransportError`,
    `TestWaitRetries`, `TestWaitRetryDeadline`,
    `TestWaitForCheckpointRetries`, `TestExecutePolicyPollRetries`,
    `TestExecutePollRetriesPreserveContinuation` and
    `TestProgressEnrichOutcomes`.

## Amended 2026-11-25 (LOG-OBSERVE-01)

A library caller running with `Options.Logger = zerolog.Nop()` could not learn
why a Run failed, why an enrich policy failed, which documents Elasticsearch
rejected, or what the Run changed. This amendment adds one additive v1
contract in five parts. Run's returned error stays the single authoritative
failure signal: an action that ends the Run gets no `Final` progress event
and no lifecycle event. The operator approved the exact public shapes on
2026-11-25; they are listed in the README "Library Usage" section and the
`pkg/loader` GoDoc, and changing one needs a new operator decision.

- **Separate `Options.Lifecycle func(LifecycleEvent)`.** `ProgressStage` was
  not extended, because the `Final` rule cannot mark a delete then recreate of
  the same name at emit time. Delivery mirrors `Progress`: synchronous on
  Run's goroutine, never concurrent, same panic rule (an error panic becomes
  Run's error, a panicked `*RunError` is returned unchanged, any other value
  propagates), nil creates no events. Events report completed actions
  (Elasticsearch acknowledged with a 2xx response), never intent; skipped and
  no-op actions get none. `resolved` is the one local event: it reports
  computed names, even with `SyncManaged` false. Events are delivered
  immediately per resource, are not de-duplicated, and are never rolled back
  when a later resource or action fails.
- **`Alias` only on `repointed`; every index delete reported once.** A
  `deleted` event never carries `Alias`. Every index delete path (direct,
  nuke, prune, `aliasops.DeleteIndices`) ends in `indexops.Delete`, the only
  index-delete emit. Event `Name`, `Alias` and `Logical` are sanitized and
  bounded (2,048 bytes) once, at the public adapter; emit sites pass raw names.
- **`Result.Notices` beside a byte-identical `Warnings`.** `Warnings` keeps its
  content and order. A `Notice{Code, Message, ResourceKind, Name}` is added for
  each actionable condition, including the ones that only logged before.
  Notices are de-duplicated once per run on the converted public tuple
  (Code, ResourceKind, Name) after `Name` is sanitized; the first Message and
  encounter order are kept. The unsupported-API Message differs between the
  list, create and delete sites and the execute site, but de-duplication
  excludes Message, so it stays one notice. Legacy warning-message
  compatibility is not a new redaction or size guarantee.
- **`ElasticsearchError` reachable through `RunError.As`.** The internal
  rejection stays unexported; `(*RunError).As` handles only
  `**ElasticsearchError` and returns a fresh projection per call, sharing no
  `Causes` slice. `ElasticsearchError` keeps its message and any read error
  unexported, so a caller-built value has an empty `Error()` and a nil
  `Unwrap()`. Structured 2xx
  failures use the additive `esresponse.NewDetailFrom`, not a changed
  `NewDetail` signature. Documented exclusions: status-only paths
  (`indexops.Delete`), the retryable `_stats` wait timeout, and mapping-fetch
  and transform-getter body-read failures. A type alias into `esresponse` was
  rejected, because public types stay defined in `loader`.
- **Enrich outcomes.** `Result.EnrichOutcomes` holds one outcome per selected
  policy (then per missing one) with the statuses `succeeded`, `failed`,
  `unsupported`, `cancelled` and `missing`. Cancellation follows the actual
  returned cause (`errors.Is` on `context.Canceled` or `DeadlineExceeded`,
  including through a rejection's body-read error); a sleep error is always
  `cancelled`; a truncated body and the Elasticsearch `CANCELLED` phase stay
  `failed`. Missing outcomes have nil `Err` and no progress event; they are
  populated on the no-targets return and absent on the unsupported,
  empty-cluster and list-failure returns. An outcome `Err` and the `Final`
  `Err` share cause reachability.
  `EnrichFailed` counts failed, unsupported and cancelled. The one deliberate
  visible change to existing text: the enrich `ProgressEvent.Err` gains
  `: <cause>`; it was never documented. New cause text is sanitized and
  bounded.
- **Bulk sample.** `Result.BulkFailures` is a bounded sample of the first
  `MaxBulkFailureSample` (100) rejected items in response order across all
  batches, independent of the ten-per-batch log cap. `BulkFailuresOmitted`
  makes `len(BulkFailures)+BulkFailuresOmitted == DocumentsFailed`. The sample
  is filled only when the bulk stage completes and is retained when a later
  stage fails; it is nil/zero after a bulk hard failure or with no data
  action. Index, ID, Action, Type and Reason all pass `SanitizeText` (the log
  keeps its raw fields). A null `error` item has an empty Type and
  Reason. The bound is defined once, in `loader`, and passed to `bulk` as
  `Config.SampleLimit`.
- **Ctx-carried `internal/observe` sink.** Internal owners reach the notice
  and lifecycle sink through a per-Run sink on the context (the `runlog`
  pattern), so concurrent Runs stay isolated and no package state exists. The
  rejected alternative was passing a sink explicitly through every owner's
  signature, which touches every call site and leaves a nil to forget. One
  sink carries both callbacks; a second `observe.WithSink` would replace it
  and drop notices. `WithSink(ctx, nil)` disables delivery with no parent
  fallback. Public string constants (`NoticeCode`, `LifecycleAction`,
  `ResourceKind`) are conversions of the internal untyped constants, which
  hold the value once. The public names derive from the internal ones:
  `Code<X>` becomes `Notice<X>`, `Kind<X>` becomes `ResourceKind<X>` and
  `Action<X>` becomes `LifecycleAction<X>`.
- **Deferred: per-Op machine code.** A machine code on `RunError` is out of
  scope (LOG-OPCODE-01).

Guards: `TestRunElasticsearchErrorReachable`, `TestRunErrorAsContract`,
`TestRunEnrichOutcomeCarriesCauseAndElasticsearchError`,
`TestRunEnrichCancelledOutcomeKeepsContextIdentity`,
`TestRunBulkFailuresSampleAcrossBatches`,
`TestRunBulkFailuresPartialResultRules`, `TestNoticeDeduplicationOncePerRun`,
`TestObserveConstantsHavePublicTwins`, `TestREADMEDocumentsEveryObserveConstant`,
`TestPublicTypeIdentity`, `TestRunLifecycleEventSequences`,
`TestRunLifecycleEventsSurviveLaterFailure`,
`TestRunLifecycleGCFailureKeepsLaterEvents`, `TestRunLifecycleResolvedOrder`,
`TestRunLifecycleNilCallbackAndSharedSink`,
`TestRunLifecycleCallbackPanicContract`, `TestRunLifecycleInlineAndIsolated`,
`TestEventAdapterSanitizesAndBounds` and
`TestDeleteIndicesAndPruneReportEachDeleteOnce`.

## References

- [pkg/loader/progress.go](../../pkg/loader/progress.go)
- [test/fixtures/elasticsearch/README.md](../../test/fixtures/elasticsearch/README.md)

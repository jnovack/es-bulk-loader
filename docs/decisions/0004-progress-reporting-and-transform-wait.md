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
  `pkg/loader/internal/{progress,enrich,transform}` and do not log.
- **Function-field callback.** `Options.Progress func(ProgressEvent)`, called
  synchronously on Run's goroutine.
- **Grouped options.** `TransformOptions{Wait, WaitTimeout}` follows the
  `EnrichOptions` pattern.
- **Log-level rule.** Stage start and end at Info, each poll or batch at
  Debug, plus an Info heartbeat at most every 30s during enrich polling and
  transform waits.
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
- Task polls that return a non-2xx status, or a task `error`, now count as a
  failed policy instead of looping or being logged as success.
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
- A transform wait fails on the first transport error or non-2xx `_stats`
  reply, and an enrich policy on the first non-2xx task poll. This is
  deliberate: it fails closed and never reports a false success. Retrying
  transient statuses (429, 502, 503, 504) within a bound is possible later
  without changing the public API.

## References

- [pkg/loader/progress.go](../../pkg/loader/progress.go)
- [test/fixtures/elasticsearch/README.md](../../test/fixtures/elasticsearch/README.md)

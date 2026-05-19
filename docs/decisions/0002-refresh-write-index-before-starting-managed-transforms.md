# ADR 0002: Refresh write index before starting managed transforms

- Status: Accepted
- Date: 2026-05-19

## Context

`es-bulk-loader` bulk loads documents and can then start managed Elasticsearch
transforms in the same invocation. Elasticsearch bulk writes are not immediately
visible to search; visibility depends on the index refresh cycle. Batch
transforms read the source index through search when `_start` is called, so they
can observe zero documents if the loader starts them before the source index is
refreshed.

This repository already handled the same visibility problem for enrich policy
execution by calling `refreshIndex` before `runEnrichPolicies`. The transform
path did not inherit that behavior, which made transform startup timing depend
on Elasticsearch's refresh window instead of the loader's workflow ordering.

## Findings

On 2026-05-19, the investigated failure mode was: bulk load completes, the
loader updates aliases or transform definitions, then `startTransforms` runs
before the source index becomes searchable. For batch transforms, that can
produce an empty destination index for the entire run.

The transform startup path already runs after data load and after managed
resource synchronization, so adding an explicit `_refresh` at that boundary fits
the loader's existing lifecycle model. `_refresh` is synchronous and idempotent,
which makes it a safer control point than relying on timing or sleeping.

## Alternatives Considered

The alternatives below were considered during the fix.

### Option 1: Refresh the write index immediately before managed transform start

Accepted. This keeps the refresh at the decision point that actually requires
search visibility. It matches the existing enrich behavior, keeps ordering local
to the transform workflow, and makes the loader own transform readiness instead
of depending on Elasticsearch's background refresh interval.

### Option 2: Do not refresh and rely on Elasticsearch auto-refresh timing

Rejected. That leaves transform correctness dependent on cluster timing and
refresh interval settings. This repo is meant to own the Elasticsearch loading
lifecycle end-to-end, so a race that can silently create empty transform output
is the wrong operational tradeoff.

### Option 3: Hoist one shared refresh above both enrich and transform execution

Rejected for now. A shared refresh would work in the current ordering, but it
would make correctness depend on the two blocks staying adjacent and ordered the
same way. This repo already treats enrich and transform execution as distinct
lifecycle stages, so keeping a refresh at each caller preserves local
correctness if those stages evolve independently.

## Decision

`es-bulk-loader` will explicitly refresh the write index immediately before
starting managed transforms. The existing `refreshIndex` helper remains the
single implementation for this behavior, and its log and error strings stay
generic because it now serves both enrich execution and transform startup.

## Consequences

### Positive

- Batch transforms see the documents written by the current loader run.
- Transform startup no longer depends on the cluster's background refresh
  interval.
- The fix reuses an existing loader primitive instead of introducing a new
  transform-only code path.

### Tradeoffs

- Runs that execute both enrich policies and transforms may issue two explicit
  refresh calls.
- Transform-related tests must model the `_refresh` request explicitly.
- Continuous transforms do not need this refresh for correctness, but they will
  still use the same startup path when managed by the loader.

## What Replaces It

The repo will no longer rely on Elasticsearch auto-refresh timing to make bulk
loaded documents visible before managed transform startup. It will use
`refreshIndex` at the transform boundary instead.

## Revisit Criteria

Revisit this ADR if the loader merges enrich and transform post-load execution
into a single coordinated stage, or if managed transforms stop using immediate
search visibility at startup.

## References

- [REFRESH-INDEX.spec.md](../../REFRESH-INDEX.spec.md)
- [REFRESH-INDEX.plan.md](../../REFRESH-INDEX.plan.md)
- [pkg/loader/loader.go](../../pkg/loader/loader.go)

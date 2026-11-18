// Package loader runs the Elasticsearch data-loading lifecycle for one index:
// it validates options, resolves managed resources, applies a data action,
// bulk-loads a data file, then executes enrich policies and managed transforms.
// The CLI and external callers import Run, LoadData, SyncManaged and
// ExecuteEnrich, so their signatures and the contracts below are public API.
//
// Stage order within one Run. Each stage finishes before the next begins. The
// first run-ending error stops the sequence. Stages 1 to 3 make no cluster
// writes.
//
//  1. Validate options and select the data action (-add, -flush, -delete or none).
//
//  2. Read and template-expand every input file, resolve managed policy names,
//     and preflight the settings, mappings and data file.
//
//  3. Look up the alias targets or the index. A sync-only run (no data action, no
//     -nuke) whose target is missing and that has no -settings or -mappings
//     fails here with ErrIndexOperation, naming the resources it would have synced.
//
//  4. -nuke: delete the alias targets and timestamped generations, or the index,
//     then the declared managed resources. With -sync-managed but no data action,
//     -settings or -mappings, this is a reset that recreates nothing; Run warns
//     before any request.
//
//  5. -delete on a concrete index: delete the index, then the managed resources.
//     In AliasMode, -delete keeps managed resources and rolls forward in steps 8 to 11.
//
//  6. For -add and -flush on an existing index, verify its live mappings.
//
//  7. -flush: flush the existing index or the alias targets.
//
//  8. Create the index when one is needed. Managed pipelines are upserted first
//     when syncing. In AliasMode the index name is a new timestamped index.
//
//  9. -sync-managed: upsert pipelines and policies, and garbage-collect unreferenced
//     policy versions, only when the index exists or step 8 created it. For an
//     alias create, policies wait for step 11.
//
//  10. Verify the mappings of a newly created index, then bulk-load the data file.
//
//  11. AliasMode create: repoint the alias, then upsert the deferred policies.
//
//  12. -keep-last: prune older generations.
//
//  13. -enrich: refresh the write index and execute the selected policies.
//
//  14. Transforms synced in step 9: refresh, stop, create or update, and start them,
//     then wait for the first checkpoint when Options.Transforms.Wait is set.
//
// Concurrency and callbacks. Options.Progress is a plain function field, called
// synchronously on Run's goroutine and never concurrently. A resource that causes
// a run-ending error gets no Final event, because Run's error is the only failure
// signal. See ProgressEvent and docs/decisions/0004.
//
// Logging. Run resolves its logger from Options.Logger and carries it on the
// context under a private key, not zerolog's WithContext, which silently drops a
// Disabled logger and would turn an injected zerolog.Nop() back into global
// output. Run never writes the global logger, so concurrent Runs with injected
// loggers need no consumer-side mutex.
//
// Transforms. Waiting on managed transforms is opt-in. The zero value starts
// transforms and returns. The wait polls _stats per transform under a timeout
// that bounds HTTP calls and sleeps.
//
// Failure modes:
//   - option validation and template preflight fail before any cluster write with ErrInvalidOptions,
//   - a bulk item rejection is counted in Result.DocumentsFailed and does not fail Run,
//   - a failed enrich policy is counted in Result.EnrichFailed and the run continues,
//   - every other run-ending error is returned as a *RunError whose Kind is chosen
//     from its Op name: bulk, then enrich, then pipeline/policy/transform/managed,
//     then index/indices/alias, otherwise ErrLoaderExecution (see RunError),
//   - an HTTP rejection keeps its Op and Kind; its cause names the resource, the HTTP
//     status and up to four bounded, redacted Elasticsearch type/reason pairs (at most
//     2,048 bytes for the whole message at direct failure sites), never the raw body.
//     The mapping-fetch and transform _stats errors cap only that inner text and keep
//     their longer outer context. Free-text reasons may still hold sensitive prose
//     redaction cannot recognize, and failure text parsed from successful responses is
//     not covered.
package loader

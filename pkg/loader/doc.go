// Package loader implements the Elasticsearch lifecycle used by es-bulk-loader.
//
// Responsibilities:
//   - validate run options and choose data action modes,
//   - create/update managed resources (settings, mappings, pipelines, policies, transforms),
//   - load/replace/append source data in bulk,
//   - execute enrich policies and managed transform lifecycle steps.
//
// Non-obvious decisions:
//   - managed enrich policy names are content-hashed and rewritten in dependent
//     pipelines to make updates deterministic and garbage-collectable.
//   - transform definitions are filtered by logical source index so a single
//     transforms file can serve multiple loader runs.
//   - the run logger travels on the context under a private key, not zerolog's
//     WithContext, which silently drops a Disabled logger and would turn an
//     injected zerolog.Nop() back into global output. Run never writes the
//     global logger, so concurrent Runs need no consumer-side mutex.
//   - Options.Progress is a plain function field, called synchronously on Run's
//     goroutine and never concurrently. A resource that causes a run-ending
//     error gets no Final event: Run's error is the only failure signal. Internal
//     helpers return operation errors that Run converts once into a *RunError
//     and returns at once, so no later stage runs (see ProgressEvent and
//     docs/decisions/0004).
//   - waiting on managed transforms is opt-in (Options.Transforms.Wait): the
//     zero value starts transforms and returns, as it always has. The wait
//     polls _stats per transform under a timeout that bounds HTTP and sleeps.
//
// Failure modes:
//   - option validation and managed resource failures return typed sentinel errors,
//   - bulk item failures include per-item diagnostics and fail the run,
//   - enrich/transform lifecycle failures are treated as fatal when requested.
package loader

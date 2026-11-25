package loader

import (
	"time"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
)

// ProgressStage names the lifecycle stage a ProgressEvent describes.
type ProgressStage string

const (
	// ProgressStageBulk is the bulk load of the data file.
	ProgressStageBulk ProgressStage = "bulk"
	// ProgressStageEnrich is the execution of an enrich policy.
	ProgressStageEnrich ProgressStage = "enrich"
	// ProgressStageTransform is the start of, or wait on, a managed transform.
	ProgressStageTransform ProgressStage = "transform"
)

// ProgressEvent reports progress for one resource within one stage. It is
// delivered to Options.Progress.
//
// Delivery: events arrive synchronously on the goroutine that called Run,
// never concurrently. The callback must return promptly and must not panic:
// it delays Run while it executes, a panic with an error value fails Run
// through Run's existing recover, and a panic with any other value propagates
// as it does today. No worker or queue isolates callbacks. New fields may be
// added in later minor releases, so build events and compare them with keyed
// fields, never unkeyed struct literals.
//
// Done and Total are -1 when unknown and are never estimated. Their unit
// depends on Stage:
//
//   - bulk: documents processed so far, and documents in the data file.
//   - enrich: documents done and total from the policy's child reindex task
//     while the step is "reindex". In every other step, or when the child task
//     cannot be read, both are -1. Phase carries the step label (for example
//     "force-merge"), or the Elasticsearch phase before any step is reported.
//   - transform: Done is the transform's cumulative documents_processed. Total
//     is the current checkpoint's total_docs, or -1. They have different
//     scopes, so do not compute a percentage from them.
//
// Final marks the last event for a (Stage, Resource) pair in one Run; the
// pair, not Resource alone, because an index and a policy can share a name.
//
//   - A transform that is started without Options.Transforms.Wait gets a single
//     Final event with Phase "started".
//   - Bulk item failures do not set Err. They are partial and are reported in
//     Result.DocumentsFailed.
//   - Err is set only when a resource fails and the Run continues. Today that
//     is an enrich policy that fails. Elasticsearch 9.3.1 stores no result for
//     an async enrich task, so its task GET usually ends with HTTP 404. The
//     outcome is then read from the policy's .enrich-<policy> alias. The
//     policy succeeds only if the alias moved to a new, non-empty target set
//     since kickoff. Otherwise, or if either alias read fails (for example,
//     the credentials cannot read alias metadata), the policy fails with Err
//     set, and the Run continues. A concurrent execution of the same policy
//     by another client also moves the alias; the check cannot tell them
//     apart. See ADR 0004.
//   - A failed enrich policy's Err reads `enrich policy "<name>" failed:
//     <cause>`, where <cause> is the same cause as the policy's
//     EnrichOutcome.Err in Result.EnrichOutcomes: an HTTP rejection, a task
//     error, a transport or decode error, an Elasticsearch FAILED or CANCELLED
//     phase, an unreadable or unmoved backing alias, or the context error when
//     the Run's context ended. The suffix after the colon was added in a minor
//     release; earlier versions ended at "failed". errors.Is and errors.As
//     reach the cause, and errors.As reaches an *ElasticsearchError when it is
//     an HTTP rejection or a task error. A succeeded policy and a policy
//     reported "missing" have a nil Err; a missing policy gets no event. Every
//     selected policy gets exactly one Final event, and the Run continues with
//     the next policy after a failure.
//   - When Run returns an error, the resource that caused it gets no Final
//     event, and no events follow. Run's error is the single authoritative
//     failure signal. This covers a hard bulk failure, a transform wait
//     failure or timeout, and any other run-ending error. Treat "Run returned"
//     as the end of the stream, and do not count Final events.
type ProgressEvent struct {
	// Stage is the lifecycle stage.
	Stage ProgressStage
	// Resource is the write index (bulk), policy name (enrich) or transform ID (transform).
	Resource string
	// Phase is the phase or state as reported by Elasticsearch; "" when none.
	Phase string
	// Done is the units completed; -1 when unknown.
	Done int64
	// Total is the units expected; -1 when unknown.
	Total int64
	// Elapsed is the time since the loader began tracking this resource.
	Elapsed time.Duration
	// Final is true on the last event for this (Stage, Resource) in this Run.
	Final bool
	// Err is non-nil only on a Final event for a resource that failed without
	// failing the Run. For an enrich policy it is `enrich policy "<name>"
	// failed: <cause>`; see the Final notes above and EnrichOutcome.
	Err error
}

// TransformOptions controls what Run does after starting managed transforms.
// The zero value starts transforms and returns without waiting.
type TransformOptions struct {
	// Wait blocks Run until each synced transform completes its first
	// checkpoint. A batch transform is done when it is stopped at checkpoint
	// 1 or later; a continuous transform (its body has a top-level "sync") is
	// done at checkpoint 1 or later with no later checkpoint in progress. A
	// failed transform or a timeout fails Run with an ErrManagedResource error.
	Wait bool
	// WaitTimeout bounds the wait for each transform; <= 0 means DefaultTransformWaitTimeout.
	WaitTimeout time.Duration
}

// DefaultTransformWaitTimeout is the per-transform wait bound used when
// TransformOptions.WaitTimeout is <= 0.
const DefaultTransformWaitTimeout = 30 * time.Minute

// progressEmitter wraps Options.Progress so call sites need no nil check.
type progressEmitter func(ProgressEvent)

// emit delivers e to the callback, or does nothing when there is none.
func (p progressEmitter) emit(e ProgressEvent) {
	if p != nil {
		p(e)
	}
}

// stage returns the internal snapshot callback for one public stage. It is
// the single mapping from progress.Snapshot to ProgressEvent, shared by the
// enrich, transform and bulk boundaries so the fields cannot drift between
// them; a nil Options.Progress still creates no events.
func (p progressEmitter) stage(stage ProgressStage) func(progress.Snapshot) {
	return func(s progress.Snapshot) {
		p.emit(ProgressEvent{
			Stage:    stage,
			Resource: s.Resource,
			Phase:    s.Phase,
			Done:     s.Done,
			Total:    s.Total,
			Elapsed:  s.Elapsed,
			Final:    s.Final,
			Err:      s.Err,
		})
	}
}

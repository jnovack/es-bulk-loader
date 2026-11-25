package loader

import (
	"errors"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/bulk"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/enrich"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
)

// ElasticsearchError is the structured Elasticsearch cause of a failure: a
// run-ending one (Run's error), or an enrich policy failure that does not end
// the run (EnrichOutcome.Err and the enrich Final ProgressEvent.Err; see
// EnrichOutcome). Reach it with errors.As:
//
//	var esErr *loader.ElasticsearchError
//	if errors.As(err, &esErr) {
//		// esErr.Resource, esErr.Name, esErr.StatusCode, esErr.Causes
//	}
//
// It is available wherever the failing request's response was turned into a
// rejection cause: every run-ending HTTP rejection whose body the loader read
// (including the mapping fetch before a bulk insert and a non-retryable
// transform _stats status, both wrapped in outer context) and the flush
// shard-failure cause reported inside a successful response (StatusCode 0).
// For an enrich policy it is present when the cause is an HTTP rejection of
// the execute request or a task poll, a task "error" reported inside a
// successful poll response (StatusCode 0, Resource "enrich task"), or a
// backing-alias read rejected during task-404 verification (Resource "alias").
//
// It is absent, and errors.As returns false, for:
//
//   - status-only paths, such as index deletion, whose errors carry no richer
//     cause;
//   - a transform wait timeout whose latest poll failed with a retryable
//     _stats status: that failure survives only as text in the timeout error;
//   - a body-read failure on the mapping fetch or the transform _stats getter:
//     those keep their existing read-error chain without a rejection;
//   - a transform that reports state "failed" inside a successful _stats
//     response: its sanitized reason survives only as text in the
//     `transform "<id>" failed: <reason>` error;
//   - transport, validation, file and every other non-Elasticsearch failure.
//
// Error returns the bounded text of the originating rejection, the same text
// RunError's message ends with; RunError keeps the outer operation context
// (Op and any wrapper prefix). Unwrap returns the response-body read error,
// when one ended the read, so errors.Is(err, context.Canceled) holds through
// both values. A nil or zero value returns "" from Error and nil from Unwrap.
// The text and the read error are unexported and set only by the loader, so a
// value a caller builds has empty Error text and a nil Unwrap.
//
// Every string is sanitized single-line UTF-8 with recognizable credentials
// redacted (the same redaction as RunError's message); free-text prose that
// redaction cannot recognize may remain. New fields may be added in later
// minor releases, so build and compare values with keyed fields, never unkeyed
// struct literals.
type ElasticsearchError struct {
	// Resource is the request noun the loader used, for example "pipeline",
	// "enrich policy", "enrich policy list request", "bulk request",
	// "transform" or "index". It describes the request, not a resource kind,
	// and is not one of a closed set.
	Resource string
	// Name is the sanitized display name of the resource, at most 256 bytes
	// (a longer name ends with " [truncated]"); "" when the request had no
	// single resource, such as a bulk or list request.
	Name string
	// StatusCode is the HTTP status of the rejected response, or 0 when the
	// failure was reported inside a successful (2xx) response.
	StatusCode int
	// Causes are up to four Elasticsearch type/reason pairs in the order the
	// message renders them: root_cause entries, then the top-level error,
	// then each one's caused_by chain. Empty when the body was unreadable,
	// oversized (over 65,536 bytes), empty, not a single JSON value or had no
	// recognizable error; Error then holds the fixed "ES error detail
	// unavailable (...)" explanation.
	Causes []ElasticsearchCause

	msg string
	err error
}

// ElasticsearchCause is one Elasticsearch error type/reason pair. Each field
// is at most 2,048 bytes of sanitized, single-line UTF-8 (a longer value ends
// with " [truncated]"); either may be empty, never both. Build and compare it
// with keyed fields.
type ElasticsearchCause struct {
	// Type is the Elasticsearch error type, for example
	// "mapper_parsing_exception".
	Type string
	// Reason is the Elasticsearch error reason.
	Reason string
}

// Error returns the bounded, sanitized text of the originating rejection, or
// "" for a nil or zero value. It is stored, never rebuilt from Causes.
func (e *ElasticsearchError) Error() string {
	if e == nil {
		return ""
	}
	return e.msg
}

// Unwrap returns the response-body read error, or nil. It never returns the
// internal rejection or RunError.Err.
func (e *ElasticsearchError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

// elasticsearchErrorFrom projects the first *esresponse.Rejection in err's
// chain into a new public ElasticsearchError. It is the single mapping from
// the internal rejection, shared by RunError.As and later wrappers. Values are
// copied as stored: they are already sanitized and bounded, so nothing is
// re-sanitized or re-cut. Causes is freshly allocated, so a caller mutating it
// cannot change the rejection or the next projection. A nil err, no
// rejection, or a typed nil rejection yields (nil, false).
func elasticsearchErrorFrom(err error) (*ElasticsearchError, bool) {
	if err == nil {
		return nil, false
	}
	var rejection *esresponse.Rejection
	if !errors.As(err, &rejection) || rejection == nil {
		return nil, false
	}
	pairs := rejection.Pairs()
	var causes []ElasticsearchCause
	if len(pairs) > 0 {
		causes = make([]ElasticsearchCause, len(pairs))
		for i, p := range pairs {
			causes[i] = ElasticsearchCause{Type: p.Type, Reason: p.Reason}
		}
	}
	return &ElasticsearchError{
		Resource:   rejection.Resource(),
		Name:       rejection.ResourceName(),
		StatusCode: rejection.Status(),
		Causes:     causes,
		msg:        rejection.Error(),
		err:        rejection.Unwrap(),
	}, true
}

// asElasticsearchError is the one target handler behind every public error
// path that offers an ElasticsearchError through errors.As: RunError.As and
// the enrich error wrapper. For a **ElasticsearchError target it sets the
// target to a new projection of the first rejection in err's chain and
// reports true. Any other target, a nil target pointer, a nil err or a chain
// without a rejection reports false and leaves the target unchanged. Callers
// pass the inner error, never the value whose As is running: errors.As on that
// value would call its As again.
func asElasticsearchError(err error, target any) bool {
	esTarget, ok := target.(**ElasticsearchError)
	if !ok || esTarget == nil {
		return false
	}
	esErr, ok := elasticsearchErrorFrom(err)
	if !ok {
		return false
	}
	*esTarget = esErr
	return true
}

// enrichErr makes an enrich failure reachable with errors.As as an
// *ElasticsearchError while leaving its text and errors.Is/Unwrap chain
// unchanged. It is unexported and applied in exactly two places: Run's enrich
// progress callback (the Final ProgressEvent.Err) and the EnrichOutcome
// adapter. It cannot live in internal/enrich (import cycle), and
// progressEmitter.stage must keep passing Err by identity.
type enrichErr struct{ err error }

// wrapEnrichErr wraps err, returning a nil error interface (not a typed nil
// wrapper) for a nil err so success keeps Err == nil.
func wrapEnrichErr(err error) error {
	if err == nil {
		return nil
	}
	return &enrichErr{err: err}
}

func (w *enrichErr) Error() string { return w.err.Error() }
func (w *enrichErr) Unwrap() error { return w.err }

// As offers the ElasticsearchError projection; every other target falls
// through to errors.As's normal Unwrap traversal.
func (w *enrichErr) As(target any) bool { return asElasticsearchError(w.err, target) }

// EnrichStatus is the outcome of one enrich policy in Result.EnrichOutcomes.
// The values are the strings "succeeded", "failed", "unsupported",
// "cancelled" and "missing". The prefix keeps them distinct from the int
// fields Result.EnrichSucceeded and Result.EnrichFailed.
type EnrichStatus string

// The EnrichStatus values.
const (
	// EnrichStatusSucceeded ("succeeded"): the policy executed and completed.
	EnrichStatusSucceeded = EnrichStatus(enrich.StatusSucceeded)
	// EnrichStatusFailed ("failed"): the policy was attempted and did not
	// complete: a transport or decode error, an HTTP rejection, a task error,
	// a FAILED or CANCELLED task phase reported by Elasticsearch (that
	// CANCELLED is deliberately "failed": "cancelled" below means this Run's
	// context ended), or an unverifiable or unmoved backing alias. An
	// unreadable backing alias is "failed" even when the read failed because
	// this Run's context ended, so errors.Is(Err, context.Canceled) can hold
	// for a "failed" policy: Status, not errors.Is, is the classification.
	EnrichStatusFailed = EnrichStatus(enrich.StatusFailed)
	// EnrichStatusUnsupported ("unsupported"): the execute endpoint answered
	// with a generic 404, so the enrich API is not served at this URL.
	EnrichStatusUnsupported = EnrichStatus(enrich.StatusUnsupported)
	// EnrichStatusCancelled ("cancelled"): this Run's context was cancelled or
	// its deadline passed while the policy was being executed or polled.
	EnrichStatusCancelled = EnrichStatus(enrich.StatusCancelled)
	// EnrichStatusMissing ("missing"): the policy was requested but absent
	// from a cluster that has policies. No execution was attempted.
	EnrichStatusMissing = EnrichStatus(enrich.StatusMissing)
)

// EnrichOutcome is the result of one requested enrich policy, in
// Result.EnrichOutcomes. Build and compare it with keyed fields.
//
// Err is nil for "succeeded" and "missing". For every other status it is the
// real cause (the cause text and chain, not the "enrich policy %q failed"
// prefix the enrich Final ProgressEvent.Err adds). errors.Is and errors.As
// reach the underlying error, for example context.Canceled for a "cancelled"
// policy, and errors.As reaches an *ElasticsearchError when the cause is an
// HTTP rejection or a task "error" (see ElasticsearchError). The failure does
// not end the run: Run returns no error for it.
type EnrichOutcome struct {
	// Policy is the managed policy name Run executed. For "missing" it is the
	// requested name, or the declared name for an All selection, in
	// Result.EnrichMissing order.
	Policy string
	// Status is how the policy ended.
	Status EnrichStatus
	// Err is the cause of a non-success status; nil for "succeeded" and
	// "missing".
	Err error
}

// enrichOutcomesFrom is the one adapter from the internal enrich outcomes to
// the public Result.EnrichOutcomes. It returns nil for no outcomes.
func enrichOutcomesFrom(outcomes []enrich.Outcome) []EnrichOutcome {
	if len(outcomes) == 0 {
		return nil
	}
	out := make([]EnrichOutcome, len(outcomes))
	for i, o := range outcomes {
		out[i] = EnrichOutcome{Policy: o.Policy, Status: EnrichStatus(o.Status), Err: wrapEnrichErr(o.Cause)}
	}
	return out
}

// wrapEnrichSnapshot is Run's pre-transform for the enrich progress callback.
func wrapEnrichSnapshot(next func(progress.Snapshot)) func(progress.Snapshot) {
	return func(s progress.Snapshot) {
		s.Err = wrapEnrichErr(s.Err)
		next(s)
	}
}

// MaxBulkFailureSample is the most rejected bulk items Result.BulkFailures
// holds: 100. The loader does not retain every rejected document; the rest are
// only counted in Result.BulkFailuresOmitted.
const MaxBulkFailureSample = 100

// BulkItemFailure is one bulk item Elasticsearch rejected, in
// Result.BulkFailures. An item is rejected when its response status is 300 or
// higher or it carries an error. Build and compare it with keyed fields.
//
// Every string field is sanitized single-line UTF-8 with recognizable
// credentials redacted and is at most 2,048 bytes (a
// longer value ends with " [truncated]"). A rejected item whose response had a
// null error has empty Type and Reason.
type BulkItemFailure struct {
	// Index is the concrete index the response named for the item.
	Index string
	// ID is the document _id the response named; the id Elasticsearch
	// generated when the loader sent none.
	ID string
	// Action is the bulk action key of the response item, for example "index".
	Action string
	// Status is the item's HTTP-style status from the bulk response.
	Status int
	// Type is the Elasticsearch error type, for example
	// "mapper_parsing_exception"; empty for a null error.
	Type string
	// Reason is the Elasticsearch error reason; empty for a null error.
	Reason string
}

// bulkFailuresFrom is the one adapter from the internal bulk sample to the
// public Result.BulkFailures. It returns nil for no sample and copies the
// values, so the Result never aliases loader-owned memory.
func bulkFailuresFrom(sample []bulk.ItemFailure) []BulkItemFailure {
	if len(sample) == 0 {
		return nil
	}
	out := make([]BulkItemFailure, len(sample))
	for i, f := range sample {
		out[i] = BulkItemFailure{Index: f.Index, ID: f.ID, Action: f.Action, Status: f.Status, Type: f.Type, Reason: f.Reason}
	}
	return out
}

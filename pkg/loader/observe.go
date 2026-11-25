package loader

import (
	"errors"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
)

// ElasticsearchError is the structured Elasticsearch cause of a run-ending
// failure. Reach it from Run's error with errors.As:
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

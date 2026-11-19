// Package esresponse holds pure predicates over Elasticsearch HTTP responses
// and transport errors that more than one owner package needs (bulk, enrich,
// managed, transform), plus bounded, redacted rejection diagnostics
// (NewRejection) and failure text decoded from successful responses
// (ErrorDetail, ShardFailureDetail, SanitizeText, NewDetail). Callers own
// requests, closing, retries, sleeps and deadlines; the package does not log.
// ReadBounded is its only read, of a caller-supplied body with a fixed bound.
package esresponse

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
)

// IsRetryableStatus reports whether an HTTP status is a transient
// Elasticsearch or proxy condition worth repeating an idempotent request for:
// exactly 429, 502, 503 and 504. Every other status, including 500, is a
// definitive answer that a retry would repeat. The caller decides whether the
// request is safe to repeat and how many times.
func IsRetryableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// IsRetryableTransportError classifies a failed transport call for retry.
// Among net.Error values (wrapped or not) only Timeout() errors retry, so a
// connection refused or reset fails at once. Temporary() is deprecated and
// ill-defined, so it is never consulted: a temporary-only error, such as a
// temporary DNS failure or, on Unix, EINTR/EMFILE/ENFILE, does not retry.
// Cancellation (including wrapped context.Canceled) never retries. Errors that
// are not net.Error, such as io.EOF from a dropped connection or a failed
// response read, retry. Classification is by error identity and type, never
// message text. Callers must still stop once their own context has ended,
// whatever this returns.
func IsRetryableTransportError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return true
}

// IsUnsupportedEnrichAPI reports a generic 404 that carries no Elasticsearch
// error object: a proxy or routing problem in front of the enrich API, not a
// missing policy (resource_not_found_exception) or another ES error.
//
// A body whose read failed or was cut is not classifiable: the predicate decides
// from the absence of ES error tokens, and missing bytes would read as a proxy
// 404, so it returns false.
func IsUnsupportedEnrichAPI(statusCode int, body []byte, readErr error) bool {
	if statusCode != http.StatusNotFound || readErr != nil || Oversized(body) {
		return false
	}

	if bytes.Contains(body, []byte(`"error":{`)) {
		return false
	}

	return !bytes.Contains(body, []byte("resource_not_found_exception"))
}

// HasErrorType reports whether body contains an Elasticsearch error of
// errorType (a literal "type":"<errorType>" match).
func HasErrorType(body []byte, errorType string) bool {
	if Oversized(body) {
		return false
	}
	return bytes.Contains(body, []byte(`"type":"`+errorType+`"`))
}

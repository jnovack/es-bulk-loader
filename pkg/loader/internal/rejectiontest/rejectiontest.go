// Package rejectiontest holds the shared assertions for the HTTP-rejection
// failure-site contract (LOG-RAWBODY-01). It is test support only and must
// not be imported by production packages. It lives under pkg/loader/internal/
// rather than internal/testutil/ because it imports failure and esresponse,
// which Go's internal rule hides from repo-root packages.
package rejectiontest

import (
	"errors"
	"strings"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/logbuf"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
)

// SecretMarker is the prefix of every synthetic credential in the fixtures.
// Any occurrence in a message or log is a leak.
const SecretMarker = "FAKE-SECRET"

// SecretBody puts its synthetic credential both in the selected reason and in
// ignored metadata, so one passing check proves both redaction and metadata
// exclusion. It is the rejection body every failure-site test serves.
const SecretBody = `{"error":{"type":"x","reason":` +
	`"rejected password=FAKE-SECRET-REASON now"},` +
	`"password":"FAKE-SECRET-META","status":500}`

// RequireRejection is the strictest union of six drifted copies; loosening
// any check re-opens a gap, such as a message over the cap going unnoticed.
// It asserts err is the operation error for op wrapping a Rejection, with the
// exact complete message, within the public message bound and free of
// secrets, and returns the operation error.
func RequireRejection(t testing.TB, err error, op, wantMsg string) *failure.Error {
	t.Helper()
	var opErr *failure.Error
	if !errors.As(err, &opErr) || opErr == nil || opErr.Op != op {
		t.Fatalf("error = %#v, want Op %q", err, op)
	}
	var rejection *esresponse.Rejection
	if !errors.As(err, &rejection) || rejection == nil {
		t.Fatalf("cause = %T, want *esresponse.Rejection", opErr.Err)
	}
	if err.Error() != wantMsg {
		t.Fatalf("message mismatch\n got: %s\nwant: %s", err.Error(), wantMsg)
	}
	if len(err.Error()) > esresponse.MaxPublicMessageBytes {
		t.Fatalf("message is %d bytes, want <= %d", len(err.Error()), esresponse.MaxPublicMessageBytes)
	}
	if strings.Contains(err.Error(), SecretMarker) {
		t.Fatalf("message leaks a secret: %s", err.Error())
	}
	return opErr
}

// RequireFailureLog is the strictest union of six drifted copies: a loosened
// version would let a failure be logged twice, at the wrong level, or with a
// raw body nested inside an object. It asserts the log carries exactly one
// line for err's Op, at fatal level, whose error field equals the returned
// cause, with no body key at any depth of any line and no secret anywhere.
// Op lines are counted before the level is checked, so one fatal plus an info
// line for the same Op fails. Other messages are allowed.
func RequireFailureLog(t testing.TB, buf *logbuf.Buffer, err error) {
	t.Helper()
	var opErr *failure.Error
	if !errors.As(err, &opErr) || opErr == nil {
		t.Fatalf("error = %T, want *failure.Error", err)
	}
	if opErr.Err == nil {
		t.Fatalf("operation error %q has a nil cause", opErr.Op)
	}
	var matches []map[string]any
	for _, line := range buf.Lines(t) {
		if hasBodyKey(line) {
			t.Fatalf("log line %v carries a body field", line)
		}
		if line["message"] == opErr.Op {
			matches = append(matches, line)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("found %d %q lines, want exactly 1:\n%s", len(matches), opErr.Op, buf.String())
	}
	line := matches[0]
	if line["level"] != "fatal" {
		t.Fatalf("%q line level = %v, want fatal", opErr.Op, line["level"])
	}
	if line["error"] != opErr.Err.Error() {
		t.Fatalf("logged error %v != returned cause %q", line["error"], opErr.Err.Error())
	}
	if strings.Contains(buf.String(), SecretMarker) {
		t.Fatalf("log leaks a secret:\n%s", buf.String())
	}
}

// hasBodyKey reports whether v holds a "body" key at any depth. A top-level
// lookup alone would pass a raw body logged inside a nested object (a zerolog
// Dict or Interface field), which the retired substring check still caught.
func hasBodyKey(v any) bool {
	switch v := v.(type) {
	case map[string]any:
		if _, has := v["body"]; has {
			return true
		}
		for _, child := range v {
			if hasBodyKey(child) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if hasBodyKey(child) {
				return true
			}
		}
	}
	return false
}

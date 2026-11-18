package runlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
)

// TestFromFallsBackToGlobal pins the documented fallback and that a Disabled
// logger carried on ctx is returned as-is, not replaced.
func TestFromFallsBackToGlobal(t *testing.T) {
	t.Parallel()

	var nilCtx context.Context
	if got := From(nilCtx); got != &log.Logger {
		t.Fatal("From(nil) must return the global logger")
	}
	if got := From(t.Context()); got != &log.Logger {
		t.Fatal("From(ctx without logger) must return the global logger")
	}

	nop := zerolog.Nop()
	if got := From(WithLogger(t.Context(), &nop)); got != &nop {
		t.Fatal("From must return the logger attached with WithLogger, even when Disabled")
	}
	var nilLogger *zerolog.Logger
	if got := From(WithLogger(t.Context(), nilLogger)); got != &log.Logger {
		t.Fatal("From must ignore a nil logger on ctx and fall back to the global logger")
	}
}

// TestResolveUsesInjectedLoggerWithTimestamp proves an injected logger is used
// (never the global) and that every event gains a timestamp.
func TestResolveUsesInjectedLoggerWithTimestamp(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	injected := zerolog.New(&buf)
	resolved := Resolve(&injected)
	resolved.Info().Msg("hello")

	line := decodeLine(t, buf.Bytes())
	if line["message"] != "hello" {
		t.Fatalf("message = %v, want hello (line %v)", line["message"], line)
	}
	if _, ok := line["time"]; !ok {
		t.Fatalf("expected a timestamp on the resolved logger, got %v", line)
	}
}

// TestFailLogsAndReturnsTheSameCause pins that the logged message and error
// field always match the returned Op and cause, and that a nil cause logs no
// error field but is returned as errors.New(op).
func TestFailLogsAndReturnsTheSameCause(t *testing.T) {
	t.Parallel()

	cause := errors.New("boom")
	tests := []struct {
		name      string
		cause     error
		wantField any // nil means the line must carry no "error" field
		wantMsg   string
	}{
		{name: "cause is logged and returned", cause: cause, wantField: "boom", wantMsg: "op failed: boom"},
		{name: "nil cause logs no error field", cause: nil, wantField: nil, wantMsg: "op failed: op failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			logger := zerolog.New(&buf)
			ctx := WithLogger(t.Context(), &logger)

			err := Fail(FailureEvent(ctx).Str("index", "cards"), "op failed", tt.cause)

			var opErr *failure.Error
			if !errors.As(err, &opErr) || opErr.Op != "op failed" || opErr.Err == nil {
				t.Fatalf("Fail returned %#v, want *failure.Error with Op and cause", err)
			}
			if tt.cause != nil && !errors.Is(err, tt.cause) {
				t.Fatalf("Fail returned %v, want cause %v", err, tt.cause)
			}
			if err.Error() != tt.wantMsg {
				t.Fatalf("message = %q, want %q", err.Error(), tt.wantMsg)
			}

			line := decodeLine(t, buf.Bytes())
			if line["level"] != "fatal" || line["message"] != "op failed" || line["index"] != "cards" {
				t.Fatalf("logged line = %v, want a fatal-level 'op failed' event with its fields", line)
			}
			if got, ok := line["error"]; tt.wantField == nil && ok || tt.wantField != nil && got != tt.wantField {
				t.Fatalf("error field = %v (present %t), want %v", got, ok, tt.wantField)
			}
		})
	}
}

// TestCheckWrapsOnlyFailures pins that Check traces op, returns nil for a nil
// error and the call-site operation error otherwise, without a fatal line.
func TestCheckWrapsOnlyFailures(t *testing.T) {
	t.Parallel()

	cause := errors.New("transport down")
	tests := []struct {
		name    string
		err     error
		wantErr bool
	}{
		{name: "nil error passes", err: nil},
		{name: "error becomes operation error", err: cause, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			logger := zerolog.New(&buf).Level(zerolog.TraceLevel)
			got := Check(WithLogger(t.Context(), &logger), "creating index", tt.err)

			if !tt.wantErr {
				if got != nil {
					t.Fatalf("Check = %v, want nil", got)
				}
			} else {
				var opErr *failure.Error
				if !errors.As(got, &opErr) || opErr.Op != "creating index" || !errors.Is(got, cause) {
					t.Fatalf("Check = %#v, want operation error for %q wrapping the cause", got, "creating index")
				}
			}
			line := decodeLine(t, buf.Bytes())
			if line["level"] != "trace" || line["message"] != "creating index" {
				t.Fatalf("logged line = %v, want only the trace line for the op", line)
			}
		})
	}
}

// decodeLine decodes the single JSON log line in raw.
func decodeLine(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(raw), []byte("\n"))
	if len(lines) != 1 {
		t.Fatalf("want exactly one log line, got %d: %s", len(lines), raw)
	}
	var line map[string]any
	if err := json.Unmarshal(lines[0], &line); err != nil {
		t.Fatalf("decode log line %s: %v", lines[0], err)
	}
	return line
}

// TestFailBoundsRejectionCauses pins LOG-RAWBODY-01's Op-relative bound: a
// *esresponse.Rejection cause is re-bounded so the whole "<op>: <cause>"
// message fits esresponse.MaxPublicMessageBytes, the logged error field equals
// the returned cause, and other causes and nil are untouched.
func TestFailBoundsRejectionCauses(t *testing.T) {
	t.Parallel()

	long := []byte(`{"error":{"type":"t","reason":"` + strings.Repeat("é", 3000) + `"}}`)
	boom := errors.New("read failed")
	tests := []struct {
		name string
		op   string
		mk   func() error
	}{
		{"long rejection is cut to fit with its op", "Failed to update alias", func() error {
			return esresponse.NewRejection("alias", "a", 500, long, nil, esresponse.MaxPublicMessageBytes)
		}},
		{"a long op still fits", "Failed to delete pipeline; nuke mode could not find indices using it as default pipeline", func() error {
			return esresponse.NewRejection("pipeline", "p", 400, long, nil, esresponse.MaxPublicMessageBytes)
		}},
		{"read error identity survives", "Failed to flush index", func() error {
			return esresponse.NewRejection("index", "i", 500, long, boom, esresponse.MaxPublicMessageBytes)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			logger := zerolog.New(&buf)
			cause := tt.mk()
			before := cause.Error()

			err := Fail(FailureEvent(WithLogger(t.Context(), &logger)), tt.op, cause)

			if len(err.Error()) > esresponse.MaxPublicMessageBytes {
				t.Fatalf("message is %d bytes, want <= %d", len(err.Error()), esresponse.MaxPublicMessageBytes)
			}
			if !strings.HasPrefix(err.Error(), tt.op+": ") {
				t.Fatalf("message %.60q does not start with %q", err.Error(), tt.op+": ")
			}
			var opErr *failure.Error
			if !errors.As(err, &opErr) {
				t.Fatalf("err = %T, want *failure.Error", err)
			}
			line := decodeLine(t, buf.Bytes())
			if line["error"] != opErr.Err.Error() {
				t.Fatalf("logged error field differs from the returned cause\n log: %v\n ret: %v", line["error"], opErr.Err)
			}
			if cause.Error() != before {
				t.Fatal("Fail must not mutate the caller's Rejection")
			}
			if tt.name == "read error identity survives" && !errors.Is(err, boom) {
				t.Fatal("errors.Is lost the read error after re-bounding")
			}
		})
	}

	t.Run("a Rejection that already fits is passed through", func(t *testing.T) {
		t.Parallel()
		r := esresponse.NewRejection("alias", "a", 500, []byte(`{"error":"x"}`), nil, esresponse.MaxPublicMessageBytes)
		err := Fail(nopEvent(), "op", r)
		if !errors.Is(err, r) {
			t.Fatal("a fitting Rejection must be returned as the cause unchanged")
		}
	})
	t.Run("a non-Rejection cause is untouched", func(t *testing.T) {
		t.Parallel()
		cause := errors.New(strings.Repeat("z", 5000))
		err := Fail(nopEvent(), "op", cause)
		if !errors.Is(err, cause) || err.Error() != "op: "+cause.Error() {
			t.Fatal("a non-Rejection cause must pass through unchanged and unbounded")
		}
	})
	t.Run("a nil cause still renders op: op", func(t *testing.T) {
		t.Parallel()
		if got := Fail(nopEvent(), "op", nil).Error(); got != "op: op" {
			t.Fatalf("message = %q, want %q", got, "op: op")
		}
	})
	t.Run("an op longer than the whole cap clamps the budget without panicking", func(t *testing.T) {
		t.Parallel()
		op := strings.Repeat("o", esresponse.MaxPublicMessageBytes+10)
		r := esresponse.NewRejection("alias", "a", 500, []byte(`{"error":"x"}`), nil, esresponse.MaxPublicMessageBytes)
		err := Fail(nopEvent(), op, r)
		if !strings.HasPrefix(err.Error(), op+": ") {
			t.Fatal("the op text must stay byte-identical")
		}
	})
}

// nopEvent returns a fatal-level event on a disabled logger.
func nopEvent() *zerolog.Event {
	nop := zerolog.Nop()
	return nop.WithLevel(zerolog.FatalLevel)
}

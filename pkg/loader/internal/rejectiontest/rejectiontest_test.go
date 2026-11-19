package rejectiontest

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/logbuf"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
)

const testOp = "Failed to do thing"

// recordingTB records failures instead of failing the real test. Fatal and
// Fatalf stop the calling goroutine like testing.T does, so a helper never
// runs on past a recorded failure.
type recordingTB struct {
	testing.TB
	failed bool
	msg    string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) record(msg string) {
	r.failed = true
	r.msg += msg + "\n"
}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.record(sprintf(format, args...))
	runtime.Goexit()
}

func (r *recordingTB) Fatal(args ...any) {
	r.record(sprint(args...))
	runtime.Goexit()
}

func (r *recordingTB) Errorf(format string, args ...any) { r.record(sprintf(format, args...)) }
func (r *recordingTB) Error(args ...any)                 { r.record(sprint(args...)) }

// runRecorded runs f against a fresh recorder in its own goroutine and waits
// for it, so Goexit from Fatal ends f only.
func runRecorded(f func(testing.TB)) *recordingTB {
	rec := &recordingTB{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		f(rec)
	}()
	wg.Wait()
	return rec
}

func TestRecordingTBFatalStopsTheCaller(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fail func(testing.TB)
	}{
		{"Fatalf", func(tb testing.TB) { tb.Fatalf("boom %d", 1) }},
		{"Fatal", func(tb testing.TB) { tb.Fatal("boom") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reached := false
			rec := runRecorded(func(tb testing.TB) {
				tt.fail(tb)
				reached = true
			})
			if !rec.failed || reached {
				t.Fatalf("failed=%v reached=%v, want failed and not reached", rec.failed, reached)
			}
		})
	}
}

// TestSecretBodyCarriesTheMarker guards the link between the two constants:
// every migrated site's "no secret" check searches for SecretMarker, so if
// SecretBody's credentials stopped carrying it, those checks would pass
// vacuously. Both the selected reason and the ignored metadata must carry it.
func TestSecretBodyCarriesTheMarker(t *testing.T) {
	t.Parallel()

	for _, want := range []string{
		"password=" + SecretMarker + "-REASON",
		`"password":"` + SecretMarker + "-META",
	} {
		if !strings.Contains(SecretBody, want) {
			t.Errorf("SecretBody lacks %q: %s", want, SecretBody)
		}
	}
}

func rejection(t *testing.T, name string, status int, body string, max int) *esresponse.Rejection {
	t.Helper()
	return esresponse.NewRejection("thing", name, status, []byte(body), nil, max)
}

func TestRequireRejection(t *testing.T) {
	t.Parallel()

	short := failure.New(testOp, rejection(t, "x", 500, SecretBody, esresponse.MaxPublicMessageBytes))
	long := failure.New(testOp, rejection(t, "x", 500,
		`{"error":{"type":"x","reason":"`+strings.Repeat("a", 5000)+`"}}`, 1<<20))
	if len(long.Error()) <= esresponse.MaxPublicMessageBytes {
		t.Fatalf("over-bound fixture is only %d bytes", len(long.Error()))
	}
	multi := failure.New(testOp, rejection(t, "x", 500,
		`{"error":{"type":"x","reason":"`+strings.Repeat("é", 1100)+`"}}`, 1<<20))
	if len(multi.Error()) <= esresponse.MaxPublicMessageBytes || len([]rune(multi.Error())) >= esresponse.MaxPublicMessageBytes {
		t.Fatalf("multibyte fixture: %d bytes, %d runes", len(multi.Error()), len([]rune(multi.Error())))
	}
	secret := failure.New(testOp, rejection(t, SecretMarker+"-NAME", 500, `{}`, esresponse.MaxPublicMessageBytes))
	if !strings.Contains(secret.Error(), SecretMarker) {
		t.Fatalf("secret fixture lacks the marker: %s", secret.Error())
	}
	budget := esresponse.MaxPublicMessageBytes - len(testOp) - len(": ")
	exact := failure.New(testOp, rejection(t, "x", 500,
		`{"error":{"type":"x","reason":"`+strings.Repeat("a", 5000)+`"}}`, budget))
	if len(exact.Error()) != esresponse.MaxPublicMessageBytes {
		t.Fatalf("exact fixture is %d bytes, want %d", len(exact.Error()), esresponse.MaxPublicMessageBytes)
	}
	var typedNil *failure.Error

	tests := []struct {
		name     string
		err      error
		op       string
		want     string
		wantFail string // empty means the case must pass
	}{
		{"valid", short, testOp, short.Error(), ""},
		{"exact boundary", exact, testOp, exact.Error(), ""},
		{"nil error", nil, testOp, "", "want Op"},
		{"plain error", errors.New("plain"), testOp, "plain", "want Op"},
		{"typed nil operation", typedNil, testOp, "", "want Op"},
		{"wrong Op", short, "Other op", short.Error(), "want Op"},
		{"non-Rejection cause", failure.New(testOp, errors.New("plain")), testOp, testOp + ": plain", "want *esresponse.Rejection"},
		{"message mismatch", short, testOp, "something else", "message mismatch"},
		{"message over the bound", long, testOp, long.Error(), "want <="},
		{"multibyte message over the bound", multi, testOp, multi.Error(), "want <="},
		{"secret in the message", secret, testOp, secret.Error(), "leaks a secret"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var got *failure.Error
			rec := runRecorded(func(tb testing.TB) { got = RequireRejection(tb, tt.err, tt.op, tt.want) })
			if tt.wantFail == "" {
				if rec.failed {
					t.Fatalf("unexpected failure: %s", rec.msg)
				}
				if got != tt.err {
					t.Fatalf("returned %p, want the original operation error", got)
				}
				return
			}
			if !rec.failed {
				t.Fatalf("unexpected pass, want a failure containing %q", tt.wantFail)
			}
			if !strings.Contains(rec.msg, tt.wantFail) {
				t.Fatalf("diagnostic %q lacks %q", rec.msg, tt.wantFail)
			}
		})
	}
}

// logCase builds a fresh logger, buffer and error for one case.
type logCase struct {
	name     string
	build    func() (*logbuf.Buffer, error)
	wantFail string
}

func validErr() *failure.Error {
	return failure.New(testOp, esresponse.NewRejection("thing", "x", 500, []byte(SecretBody), nil, 1024))
}

func fatalLine(logger *zerolog.Logger, err *failure.Error) {
	logger.WithLevel(zerolog.FatalLevel).Err(err.Err).Msg(err.Op)
}

func TestRequireFailureLog(t *testing.T) {
	t.Parallel()

	var typedNil *failure.Error
	tests := []logCase{
		{"valid single fatal line", func() (*logbuf.Buffer, error) {
			logger, buf := logbuf.New()
			e := validErr()
			fatalLine(logger, e)
			return buf, e
		}, ""},
		{"valid with unrelated lines", func() (*logbuf.Buffer, error) {
			logger, buf := logbuf.New()
			e := validErr()
			logger.Trace().Str("k", "v").Msg("setup")
			logger.Info().Msg("unrelated")
			fatalLine(logger, e)
			return buf, e
		}, ""},
		{"nil error", func() (*logbuf.Buffer, error) {
			_, buf := logbuf.New()
			return buf, nil
		}, "want *failure.Error"},
		{"plain error", func() (*logbuf.Buffer, error) {
			_, buf := logbuf.New()
			return buf, errors.New("plain")
		}, "want *failure.Error"},
		{"typed nil operation", func() (*logbuf.Buffer, error) {
			_, buf := logbuf.New()
			return buf, typedNil
		}, "want *failure.Error"},
		{"operation with nil cause", func() (*logbuf.Buffer, error) {
			_, buf := logbuf.New()
			return buf, &failure.Error{Op: testOp}
		}, "nil cause"},
		{"body field on the failure line", func() (*logbuf.Buffer, error) {
			logger, buf := logbuf.New()
			e := validErr()
			logger.WithLevel(zerolog.FatalLevel).Err(e.Err).Str("body", "x").Msg(e.Op)
			return buf, e
		}, "body field"},
		{"null body field on an unrelated line", func() (*logbuf.Buffer, error) {
			logger, buf := logbuf.New()
			e := validErr()
			logger.Trace().Interface("body", nil).Msg("setup")
			fatalLine(logger, e)
			return buf, e
		}, "body field"},
		{"body field nested in an object", func() (*logbuf.Buffer, error) {
			logger, buf := logbuf.New()
			e := validErr()
			logger.Info().Dict("resp", zerolog.Dict().Str("body", "x")).Msg("unrelated")
			fatalLine(logger, e)
			return buf, e
		}, "body field"},
		{"body field nested in an array", func() (*logbuf.Buffer, error) {
			logger, buf := logbuf.New()
			e := validErr()
			logger.Info().Interface("items", []any{map[string]any{"body": "x"}}).Msg("unrelated")
			fatalLine(logger, e)
			return buf, e
		}, "body field"},
		{"zero failure lines", func() (*logbuf.Buffer, error) {
			logger, buf := logbuf.New()
			logger.Info().Msg("unrelated")
			return buf, validErr()
		}, "found 0"},
		{"two fatal failure lines", func() (*logbuf.Buffer, error) {
			logger, buf := logbuf.New()
			e := validErr()
			fatalLine(logger, e)
			fatalLine(logger, e)
			return buf, e
		}, "found 2"},
		{"one fatal plus one info failure line", func() (*logbuf.Buffer, error) {
			logger, buf := logbuf.New()
			e := validErr()
			fatalLine(logger, e)
			logger.Info().Err(e.Err).Msg(e.Op)
			return buf, e
		}, "found 2"},
		{"failure line at info level", func() (*logbuf.Buffer, error) {
			logger, buf := logbuf.New()
			e := validErr()
			logger.Info().Err(e.Err).Msg(e.Op)
			return buf, e
		}, "want fatal"},
		{"logged error differs from the cause", func() (*logbuf.Buffer, error) {
			logger, buf := logbuf.New()
			e := validErr()
			logger.WithLevel(zerolog.FatalLevel).Err(errors.New("other")).Msg(e.Op)
			return buf, e
		}, "returned cause"},
		{"secret in the log", func() (*logbuf.Buffer, error) {
			logger, buf := logbuf.New()
			e := validErr()
			fatalLine(logger, e)
			logger.Info().Msg(SecretMarker + "-LOG")
			return buf, e
		}, "leaks a secret"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			buf, err := tt.build()
			rec := runRecorded(func(tb testing.TB) { RequireFailureLog(tb, buf, err) })
			if tt.wantFail == "" {
				if rec.failed {
					t.Fatalf("unexpected failure: %s", rec.msg)
				}
				return
			}
			if !rec.failed {
				t.Fatalf("unexpected pass, want a failure containing %q", tt.wantFail)
			}
			if !strings.Contains(rec.msg, tt.wantFail) {
				t.Fatalf("diagnostic %q lacks %q", rec.msg, tt.wantFail)
			}
		})
	}
}

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }
func sprint(args ...any) string                 { return fmt.Sprint(args...) }

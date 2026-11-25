package bulk

// LOG-RAWBODY-01 guards for the bulk request failure sites: the nameless
// Rejection form, es_error on the retry Warn, no raw body anywhere, and a
// bounded, closed-once error read. Bodies and secrets are synthetic.

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/bodytransport"
	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/logbuf"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/rejectiontest"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

func noSleep(ctx context.Context, _ time.Duration) error { return ctx.Err() }

func TestBulkRequestFailureCarriesNamelessRejection(t *testing.T) {
	t.Parallel()

	stub := esstub.New(t)
	stub.Handle("POST /_bulk", esstub.JSON(http.StatusBadRequest, rejectiontest.SecretBody))
	logger, buf := logbuf.New()
	ctx := runlog.WithLogger(t.Context(), logger)

	_, err := bulkInsert(ctx, stub.MustClient(t), "cards", []map[string]interface{}{{"id": "1"}}, 0, 1, 3, time.Millisecond, time.Millisecond, "", noSleep, 0)

	want := "Bulk API request failed: bulk request rejected with HTTP 400: x: rejected password=[REDACTED] now"
	rejectiontest.RequireRejection(t, err, "Bulk API request failed", want)
	rejectiontest.RequireFailureLog(t, buf, err)
}

// TestBulkRetryWarnLogsESError pins the log-only shape and that the retry stays
// status-only: a retryable status with an oversized body still retries.
func TestBulkRetryWarnLogsESError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		wantES string
	}{
		{"JSON reason is visible", `{"error":{"type":"x","reason":"busy password=FAKE-SECRET-REASON"}}`, "bulk request rejected with HTTP 503: x: busy password=[REDACTED]"},
		{"non-JSON body gets the fixed detail", "<html>boom</html>", "bulk request rejected with HTTP 503: ES error detail unavailable (response is not a single JSON value)"},
		{"oversized body still retries by status", strings.Repeat(" ", esresponse.MaxRejectionBodyBytes+10), "bulk request rejected with HTTP 503: ES error detail unavailable (response exceeded 65536 bytes)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			stub.Handle("POST /_bulk", esstub.JSON(http.StatusServiceUnavailable, tt.body))
			logger, buf := logbuf.New()
			ctx := runlog.WithLogger(t.Context(), logger)

			_, err := bulkInsert(ctx, stub.MustClient(t), "cards", []map[string]interface{}{{"id": "1"}}, 0, 1, 3, time.Millisecond, time.Millisecond, "", noSleep, 0)

			if err == nil {
				t.Fatal("expected the budget to be exhausted")
			}
			if got := len(stub.Requests()); got != 3 {
				t.Fatalf("requests = %d, want 3 (status-only retry unchanged)", got)
			}
			warns := 0
			for _, line := range buf.Lines(t) {
				if _, has := line["body"]; has {
					t.Fatalf("log line %v carries a body field", line)
				}
				if line["level"] == "warn" {
					warns++
					if line["es_error"] != tt.wantES {
						t.Fatalf("es_error = %v, want %q", line["es_error"], tt.wantES)
					}
				}
			}
			if warns != 2 {
				t.Fatalf("retry warns = %d, want 2", warns)
			}
			if strings.Contains(buf.String(), "FAKE-SECRET") {
				t.Fatal("log leaks a secret")
			}
		})
	}
}

// TestBulkErrorReadIsBoundedAndClosedOnce proves the error body read stops at
// the probe byte and the body is closed once per attempt.
func TestBulkErrorReadIsBoundedAndClosedOnce(t *testing.T) {
	t.Parallel()

	tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{func() bodytransport.Reply {
		return bodytransport.Reply{Status: http.StatusBadRequest, Body: &bodytransport.Body{R: bodytransport.Endless{}}}
	}}}
	nop := zerolog.Nop()
	ctx := runlog.WithLogger(t.Context(), &nop)

	_, err := bulkInsert(ctx, tr.Client(t), "cards", []map[string]interface{}{{"id": "1"}}, 0, 1, 3, time.Millisecond, time.Millisecond, "", noSleep, 0)

	if err == nil {
		t.Fatal("expected a failure")
	}
	bodies := tr.Bodies()
	if len(bodies) != 1 {
		t.Fatalf("bodies = %d, want 1 (400 is not retried)", len(bodies))
	}
	if bodies[0].Served.Load() != esresponse.MaxRejectionBodyBytes+1 || bodies[0].Closes.Load() != 1 {
		t.Fatalf("served %d, closed %d; want %d and 1", bodies[0].Served.Load(), bodies[0].Closes.Load(), esresponse.MaxRejectionBodyBytes+1)
	}
}

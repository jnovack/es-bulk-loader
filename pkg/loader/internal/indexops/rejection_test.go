package indexops

// LOG-RAWBODY-01 guards for the index failure sites and the mapping-fetch
// error tail: unchanged Op, bounded redacted Rejection cause, no raw body in
// the log, read-error precedence, bounded and closed-once error reads and
// unbounded 2xx decodes. Bodies and secrets are synthetic (FAKE-SECRET-*).

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/internal/testutil/bodytransport"
	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/logbuf"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

const secretBody = `{"error":{"type":"x","reason":"rejected password=FAKE-SECRET-REASON now"},` +
	`"password":"FAKE-SECRET-META","status":500}`

func TestIndexFailureSitesCarryRejectionCause(t *testing.T) {
	t.Parallel()

	const reason = "x: rejected password=[REDACTED] now"
	tests := []struct {
		name    string
		handle  func(*esstub.Stub)
		run     func(ctx context.Context, es *elasticsearch.Client) error
		wantOp  string
		wantMsg string
	}{
		{
			name:    "create index",
			handle:  func(s *esstub.Stub) { s.Handle("PUT /cards", esstub.JSON(500, secretBody)) },
			run:     func(ctx context.Context, es *elasticsearch.Client) error { return Create(ctx, es, "cards", "{}") },
			wantOp:  "Failed to create index",
			wantMsg: `Failed to create index: index "cards" rejected with HTTP 500: ` + reason,
		},
		{
			name:    "refresh index",
			handle:  func(s *esstub.Stub) { s.Handle("POST /cards/_refresh", esstub.JSON(500, secretBody)) },
			run:     func(ctx context.Context, es *elasticsearch.Client) error { return Refresh(ctx, es, "cards") },
			wantOp:  "Failed to refresh index",
			wantMsg: `Failed to refresh index: index "cards" rejected with HTTP 500: ` + reason,
		},
		{
			name:    "flush index",
			handle:  func(s *esstub.Stub) { s.Handle("POST /cards/_delete_by_query", esstub.JSON(500, secretBody)) },
			run:     func(ctx context.Context, es *elasticsearch.Client) error { return Flush(ctx, es, "cards") },
			wantOp:  "Failed to flush index",
			wantMsg: `Failed to flush index: index "cards" rejected with HTTP 500: ` + reason,
		},
		{
			name:   "clear default pipeline",
			handle: func(s *esstub.Stub) { s.Handle("PUT /cards/_settings", esstub.JSON(500, secretBody)) },
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				return ClearDefaultPipeline(ctx, es, []string{"cards"})
			},
			wantOp:  "Failed to clear index.default_pipeline",
			wantMsg: `Failed to clear index.default_pipeline: index "cards" rejected with HTTP 500: ` + reason,
		},
		{
			name:   "nameless get index settings",
			handle: func(s *esstub.Stub) { s.Handle("GET /_settings/*", esstub.JSON(500, secretBody)) },
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				_, err := FindIndicesUsingDefaultPipeline(ctx, es, "p")
				return err
			},
			wantOp:  "Failed to get index settings",
			wantMsg: `Failed to get index settings: index settings request rejected with HTTP 500: ` + reason,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			tt.handle(stub)
			logger, buf := logbuf.New()
			ctx := runlog.WithLogger(t.Context(), logger)

			err := tt.run(ctx, stub.MustClient(t))

			var opErr *failure.Error
			if !errors.As(err, &opErr) || opErr.Op != tt.wantOp {
				t.Fatalf("error = %#v, want Op %q", err, tt.wantOp)
			}
			var rejection *esresponse.Rejection
			if !errors.As(err, &rejection) {
				t.Fatalf("cause = %T, want *esresponse.Rejection", opErr.Err)
			}
			if err.Error() != tt.wantMsg {
				t.Fatalf("message mismatch\n got: %s\nwant: %s", err.Error(), tt.wantMsg)
			}
			var failureLine map[string]any
			for _, line := range buf.Lines(t) {
				if _, has := line["body"]; has {
					t.Fatalf("log line %v carries a body field", line)
				}
				if line["message"] == tt.wantOp {
					failureLine = line
				}
			}
			if failureLine == nil || failureLine["error"] != opErr.Err.Error() {
				t.Fatalf("failure line = %v, want error field equal to the returned cause", failureLine)
			}
			if strings.Contains(err.Error()+buf.String(), "FAKE-SECRET") {
				t.Fatalf("a secret leaked:\n%s\n%s", err, buf.String())
			}
		})
	}
}

// TestFlushReadErrorKeepsItsOp guards that a body read failure keeps the
// dedicated Op on both an error status and a 2xx, and that the Rejection cause
// on the error status hides the read error's text but keeps errors.Is.
func TestFlushReadErrorKeepsItsOp(t *testing.T) {
	t.Parallel()

	boom := errors.New("FAKE-SECRET-READERR injected read failure")
	for _, status := range []int{500, 200} {
		t.Run(statusName(status), func(t *testing.T) {
			t.Parallel()

			tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{func() bodytransport.Reply {
				return bodytransport.Reply{Status: status, Body: &bodytransport.Body{R: strings.NewReader(""), Err: boom}}
			}}}
			logger, buf := logbuf.New()
			ctx := runlog.WithLogger(t.Context(), logger)

			err := Flush(ctx, tr.Client(t), "cards")

			var opErr *failure.Error
			if !errors.As(err, &opErr) || opErr.Op != "Unable to read index flush response body" {
				t.Fatalf("error = %#v, want Op %q", err, "Unable to read index flush response body")
			}
			if !errors.Is(err, boom) {
				t.Fatalf("errors.Is lost the read error: %v", err)
			}
			if status == 500 {
				var rejection *esresponse.Rejection
				if !errors.As(err, &rejection) {
					t.Fatalf("cause = %T, want *esresponse.Rejection on an error status", opErr.Err)
				}
				if strings.Contains(err.Error(), "FAKE-SECRET") || strings.Contains(buf.String(), "FAKE-SECRET") {
					t.Fatalf("the read error's text leaked: %v\n%s", err, buf.String())
				}
			}
			if got := tr.Bodies()[0].Closes.Load(); got != 1 {
				t.Fatalf("body closed %d times, want 1", got)
			}
		})
	}
}

func statusName(s int) string {
	if s >= 400 {
		return "error status"
	}
	return "2xx"
}

// TestFlushErrorReadIsBoundedAndClosedOnce proves the bounded error read.
func TestFlushErrorReadIsBoundedAndClosedOnce(t *testing.T) {
	t.Parallel()

	tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{func() bodytransport.Reply {
		return bodytransport.Reply{Status: 500, Body: &bodytransport.Body{R: bodytransport.Endless{}}}
	}}}
	ctx := runlog.WithLogger(t.Context(), logbuf.Nop())

	err := Flush(ctx, tr.Client(t), "cards")

	var opErr *failure.Error
	if !errors.As(err, &opErr) || opErr.Op != "Failed to flush index" {
		t.Fatalf("error = %#v, want Op %q", err, "Failed to flush index")
	}
	body := tr.Bodies()[0]
	if body.Served.Load() != esresponse.MaxRejectionBodyBytes+1 || body.Closes.Load() != 1 {
		t.Fatalf("served %d bytes, closed %d times; want %d and 1", body.Served.Load(), body.Closes.Load(), esresponse.MaxRejectionBodyBytes+1)
	}
	if !strings.Contains(err.Error(), "response exceeded 65536 bytes") {
		t.Fatalf("message %q lacks the oversize detail", err.Error())
	}
}

// TestSuccessfulResponsesLargerThanTheBoundStillDecode guards that only error
// bodies are bounded: a legitimate large 2xx body is read in full.
func TestSuccessfulResponsesLargerThanTheBoundStillDecode(t *testing.T) {
	t.Parallel()

	pad := strings.Repeat("p", 2*esresponse.MaxRejectionBodyBytes)
	t.Run("flush failures past the bound are decoded", func(t *testing.T) {
		t.Parallel()
		// The failures array sits after the padding: a truncated read could
		// not parse, and would fail with a different Op.
		body := `{"pad":"` + pad + `","failures":[{"shard":1}]}`
		tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{bodytransport.Static(200, body)}}
		ctx := runlog.WithLogger(t.Context(), logbuf.Nop())

		err := Flush(ctx, tr.Client(t), "cards")

		var opErr *failure.Error
		if !errors.As(err, &opErr) || opErr.Op != "Index flush completed with shard-level failures" {
			t.Fatalf("error = %#v, want the shard-failure Op (the full body was read)", err)
		}
	})
	t.Run("mappings past the bound are decoded", func(t *testing.T) {
		t.Parallel()
		body := `{"i":{"mappings":{"pad":"` + pad + `"}}}`
		tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{bodytransport.Static(200, body)}}

		got, err := fetchIndexMappings(tr.Client(t), "i")

		if err != nil {
			t.Fatalf("fetchIndexMappings: %v", err)
		}
		if p, _ := got["i"]["pad"].(string); len(p) != len(pad) {
			t.Fatalf("decoded pad is %d bytes, want %d", len(p), len(pad))
		}
	})
}

// TestFetchIndexMappingsErrorTail pins the wrapped tail: the prefix is kept,
// the raw body is replaced by the Rejection text, which alone is capped at
// MaxPublicMessageBytes (so the complete message may be longer), and a read
// failure keeps its own prefix.
func TestFetchIndexMappingsErrorTail(t *testing.T) {
	t.Parallel()

	t.Run("JSON reason replaces the raw body", func(t *testing.T) {
		t.Parallel()
		tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{bodytransport.Static(500, secretBody)}}

		_, err := fetchIndexMappings(tr.Client(t), "cards")

		want := `fetching mappings for index "cards" failed with status 500: index "cards" rejected with HTTP 500: x: rejected password=[REDACTED] now`
		if err == nil || err.Error() != want {
			t.Fatalf("error = %v\nwant     %s", err, want)
		}
	})
	t.Run("the Rejection text is capped but the wrapper is not truncated", func(t *testing.T) {
		t.Parallel()
		body := `{"error":{"type":"t","reason":"` + strings.Repeat("é", 3000) + `"}}`
		tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{bodytransport.Static(500, body)}}

		_, err := fetchIndexMappings(tr.Client(t), "cards")

		var rejection *esresponse.Rejection
		if !errors.As(err, &rejection) {
			t.Fatalf("error %v does not wrap a Rejection", err)
		}
		if len(rejection.Error()) > esresponse.MaxPublicMessageBytes {
			t.Fatalf("inner Rejection is %d bytes, want <= %d", len(rejection.Error()), esresponse.MaxPublicMessageBytes)
		}
		prefix := `fetching mappings for index "cards" failed with status 500: `
		if !strings.HasPrefix(err.Error(), prefix) || len(err.Error()) != len(prefix)+len(rejection.Error()) {
			t.Fatalf("the outer wrapper must be preserved untruncated: %.120q", err.Error())
		}
	})
	t.Run("a read failure keeps the reading prefix on both statuses", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("injected read failure")
		for _, status := range []int{500, 200} {
			tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{func() bodytransport.Reply {
				return bodytransport.Reply{Status: status, Body: &bodytransport.Body{R: strings.NewReader(""), Err: boom}}
			}}}

			_, err := fetchIndexMappings(tr.Client(t), "cards")

			if err == nil || !strings.HasPrefix(err.Error(), `reading mappings response for index "cards": `) || !errors.Is(err, boom) {
				t.Fatalf("status %d: error = %v", status, err)
			}
		}
	})
}

package transform

// LOG-RAWBODY-01 guards for the transform failure sites and the _stats status
// tail: unchanged Op, bounded redacted Rejection cause, no raw body in the
// log, fail-closed body classifiers for a cut body, read-error precedence in
// statsGetter and unbounded 2xx decodes. Bodies and secrets are synthetic
// (FAKE-SECRET-*).

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/internal/testutil/bodytransport"
	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/logbuf"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/rejectiontest"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

func TestTransformFailureSitesCarryRejectionCause(t *testing.T) {
	t.Parallel()

	const reason = "x: rejected password=[REDACTED] now"
	defs := config.Definitions{"tx": json.RawMessage(`{}`)}
	tests := []struct {
		name    string
		handle  func(*esstub.Stub)
		run     func(ctx context.Context, es *elasticsearch.Client) error
		wantOp  string
		wantMsg string
	}{
		{
			name: "create transform",
			handle: func(s *esstub.Stub) {
				s.Handle("GET /_transform/tx", esstub.JSON(404, `{"error":{"type":"resource_not_found_exception"}}`))
				s.Handle("PUT /_transform/tx", esstub.JSON(500, rejectiontest.SecretBody))
			},
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				return CreateOrUpdate(ctx, es, defs, []string{"tx"})
			},
			wantOp:  "Failed to create transform",
			wantMsg: `Failed to create transform: transform "tx" rejected with HTTP 500: ` + reason,
		},
		{
			name:   "check transform existence",
			handle: func(s *esstub.Stub) { s.Handle("GET /_transform/tx", esstub.JSON(500, rejectiontest.SecretBody)) },
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				_, err := transformExists(ctx, es, "tx")
				return err
			},
			wantOp:  "Failed to check transform existence",
			wantMsg: `Failed to check transform existence: transform "tx" rejected with HTTP 500: ` + reason,
		},
		{
			name: "stop transform",
			handle: func(s *esstub.Stub) {
				s.Handle("POST /_transform/tx/_stop", esstub.JSON(500, rejectiontest.SecretBody))
			},
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				return StopBestEffort(ctx, es, []string{"tx"})
			},
			wantOp:  "Failed to stop transform",
			wantMsg: `Failed to stop transform: transform "tx" rejected with HTTP 500: ` + reason,
		},
		{
			name:   "delete transform",
			handle: func(s *esstub.Stub) { s.Handle("DELETE /_transform/tx", esstub.JSON(500, rejectiontest.SecretBody)) },
			run:    func(ctx context.Context, es *elasticsearch.Client) error { return Delete(ctx, es, []string{"tx"}) },
			wantOp: "Failed to delete transform",
			wantMsg: `Failed to delete transform: transform "tx" rejected with HTTP 500: ` +
				reason,
		},
		{
			name: "start transform",
			handle: func(s *esstub.Stub) {
				s.Handle("POST /_transform/tx/_start", esstub.JSON(500, rejectiontest.SecretBody))
			},
			run:    func(ctx context.Context, es *elasticsearch.Client) error { return Start(ctx, es, []string{"tx"}) },
			wantOp: "Failed to start transform",
			wantMsg: `Failed to start transform: transform "tx" rejected with HTTP 500: ` +
				reason,
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

			rejectiontest.RequireRejection(t, err, tt.wantOp, tt.wantMsg)
			rejectiontest.RequireFailureLog(t, buf, err)
		})
	}
}

// TestTransformBodyPredicatesRejectACutBody covers AC5 for the two transform
// classifiers: a match padded to exactly 64 KiB keeps its result, the same
// prefix at 64 KiB + 1 is no match.
func TestTransformBodyPredicatesRejectACutBody(t *testing.T) {
	t.Parallel()

	pad := func(prefix string, size int) []byte { return []byte(prefix + strings.Repeat(" ", size-len(prefix))) }
	tests := []struct {
		name  string
		match string
		pred  func(int, []byte) bool
	}{
		{"stopAlreadyStopped", `{"error":{"reason":"Cannot stop transform [t] as it is stopped"}}`, stopAlreadyStopped},
		{"startAlreadyStarted", `{"error":{"reason":"already started"}}`, startAlreadyStarted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if !tt.pred(409, pad(tt.match, esresponse.MaxRejectionBodyBytes)) {
				t.Fatal("a match at exactly the bound must keep its result")
			}
			if tt.pred(409, pad(tt.match, esresponse.MaxRejectionBodyBytes+1)) {
				t.Fatal("a match in a cut body must be no match")
			}
		})
	}
}

// TestStopAndStartFailClosedOnACutBody proves the request-level effect: an
// already-stopped/started token in the first bytes of an oversized body does
// not turn the failure into a skip.
func TestStopAndStartFailClosedOnACutBody(t *testing.T) {
	t.Parallel()

	cut := func(s string) string { return s + strings.Repeat(" ", esresponse.MaxRejectionBodyBytes+10-len(s)) }
	t.Run("stop", func(t *testing.T) {
		t.Parallel()
		tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{
			bodytransport.Static(409, cut(`{"error":{"reason":"Cannot stop transform [t] as it is stopped"}}`)),
		}}
		ctx := runlog.WithLogger(t.Context(), logbuf.Nop())

		err := StopBestEffort(ctx, tr.Client(t), []string{"tx", "ty"})

		var opErr *failure.Error
		if !errors.As(err, &opErr) || opErr.Op != "Failed to stop transform" {
			t.Fatalf("error = %#v, want Op %q", err, "Failed to stop transform")
		}
		if got := tr.Paths(); len(got) != 1 {
			t.Fatalf("requests = %q, want 1 (stop at the first failure)", got)
		}
		if b := tr.Bodies()[0]; b.Closes.Load() != 1 {
			t.Fatalf("body closed %d times", b.Closes.Load())
		}
	})
	t.Run("start", func(t *testing.T) {
		t.Parallel()
		tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{
			bodytransport.Static(409, cut(`{"error":{"reason":"already started"}}`)),
		}}
		ctx := runlog.WithLogger(t.Context(), logbuf.Nop())

		err := Start(ctx, tr.Client(t), []string{"tx", "ty"})

		var opErr *failure.Error
		if !errors.As(err, &opErr) || opErr.Op != "Failed to start transform" {
			t.Fatalf("error = %#v, want Op %q", err, "Failed to start transform")
		}
		if got := tr.Paths(); len(got) != 1 {
			t.Fatalf("requests = %q, want 1", got)
		}
	})
}

// TestTransformExistsDecodesALargeSuccess guards that the 2xx count decode is a
// full read: the count sits past the bound.
func TestTransformExistsDecodesALargeSuccess(t *testing.T) {
	t.Parallel()

	body := `{"pad":"` + strings.Repeat("p", 2*esresponse.MaxRejectionBodyBytes) + `","count":1}`
	tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{bodytransport.Static(200, body)}}
	// A truncated read would fail to unmarshal and fall back to true, so use a
	// count of 0 to tell a full read (false) from the fallback (true).
	body0 := strings.Replace(body, `"count":1`, `"count":0`, 1)
	tr0 := &bodytransport.Transport{Replies: []func() bodytransport.Reply{bodytransport.Static(200, body0)}}
	ctx := runlog.WithLogger(t.Context(), logbuf.Nop())

	if exists, err := transformExists(ctx, tr.Client(t), "tx"); err != nil || !exists {
		t.Fatalf("count 1: exists=%t err=%v, want true", exists, err)
	}
	if exists, err := transformExists(ctx, tr0.Client(t), "tx"); err != nil || exists {
		t.Fatalf("count 0: exists=%t err=%v, want false (the full body must be decoded)", exists, err)
	}
}

// TestStatsGetterBoundsErrorBodiesButReadsSuccessInFull covers statsGetter:
// an error status is read bounded, a 2xx in full, and a read failure keeps the
// existing wrapped (0, nil, err) transport/read return on both statuses.
func TestStatsGetterBoundsErrorBodiesButReadsSuccessInFull(t *testing.T) {
	t.Parallel()

	t.Run("error status is bounded and closed once", func(t *testing.T) {
		t.Parallel()
		tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{func() bodytransport.Reply {
			return bodytransport.Reply{Status: 503, Body: &bodytransport.Body{R: bodytransport.Endless{}}}
		}}}

		status, body, err := statsGetter(tr.Client(t))(t.Context(), "tx")

		if err != nil || status != 503 || len(body) != esresponse.MaxRejectionBodyBytes+1 {
			t.Fatalf("got status %d, %d bytes, err %v; want 503, %d bytes, nil", status, len(body), err, esresponse.MaxRejectionBodyBytes+1)
		}
		if c := tr.Bodies()[0].Closes.Load(); c != 1 {
			t.Fatalf("closed %d times", c)
		}
	})
	t.Run("2xx is read in full", func(t *testing.T) {
		t.Parallel()
		big := strings.Repeat("p", 3*esresponse.MaxRejectionBodyBytes)
		tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{bodytransport.Static(200, big)}}

		_, body, err := statsGetter(tr.Client(t))(t.Context(), "tx")

		if err != nil || len(body) != len(big) {
			t.Fatalf("got %d bytes, err %v; want %d bytes", len(body), err, len(big))
		}
	})
	t.Run("a read failure keeps its wrapper on both statuses", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("injected read failure")
		for _, status := range []int{503, 200} {
			tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{func() bodytransport.Reply {
				return bodytransport.Reply{Status: status, Body: &bodytransport.Body{R: strings.NewReader("{"), Err: boom}}
			}}}

			code, body, err := statsGetter(tr.Client(t))(t.Context(), "tx")

			if code != 0 || body != nil || !errors.Is(err, boom) || !strings.HasPrefix(err.Error(), "reading _stats response: ") {
				t.Fatalf("status %d: got (%d, %q, %v), want (0, nil, wrapped read error)", status, code, body, err)
			}
		}
	})
}

// TestWaitRetriesAreStatusOnlyEvenWithAnOversizedBody guards that the status
// alone decides a _stats retry: a complete body past the bound neither stops
// the retry nor changes its count.
func TestWaitRetriesAreStatusOnlyEvenWithAnOversizedBody(t *testing.T) {
	t.Parallel()

	huge := strings.Repeat("x", esresponse.MaxRejectionBodyBytes+10)
	done := `{"count":1,"transforms":[{"id":"t1","state":"stopped","stats":{"documents_processed":7},"checkpointing":{"last":{"checkpoint":1}}}]}`
	get, calls := replyGetter(pollReply{status: 503, body: huge}, pollReply{status: 503, body: huge}, pollReply{status: 200, body: done})
	var sleeps []time.Duration

	if _, err := Wait(quietContext(t), get, "t1", WaitConfig{Timeout: time.Hour, Sleep: recordingSleep(&sleeps)}); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if *calls != 3 || len(sleeps) != 2 {
		t.Fatalf("calls/sleeps = %d/%d, want 3/2 (status-only retry unchanged)", *calls, len(sleeps))
	}
}

// TestWaitStatsTailIsCappedInsideLongerContext pins the Q2 decision for the
// wrapped _stats tail: the Rejection text is at most 2,048 bytes, the
// surrounding transform/status context is preserved, so the complete message
// can be longer, and the timeout keeps its last-failure text and
// errors.Is identity.
func TestWaitStatsTailIsCappedInsideLongerContext(t *testing.T) {
	t.Parallel()

	body := `{"error":{"type":"t","reason":"` + strings.Repeat("é", 3000) + `"}}`
	t.Run("terminal status", func(t *testing.T) {
		t.Parallel()
		get, _ := replyGetter(pollReply{status: 400, body: body})

		_, err := Wait(quietContext(t), get, "t1", WaitConfig{Timeout: time.Hour, Sleep: recordingSleep(new([]time.Duration))})

		var rejection *esresponse.Rejection
		if !errors.As(err, &rejection) {
			t.Fatalf("error %v does not wrap a Rejection", err)
		}
		if len(rejection.Error()) > esresponse.MaxPublicMessageBytes {
			t.Fatalf("inner Rejection is %d bytes, want <= %d", len(rejection.Error()), esresponse.MaxPublicMessageBytes)
		}
		if !strings.HasPrefix(err.Error(), `transform "t1": _stats returned status 400: `) || len(err.Error()) <= len(rejection.Error()) {
			t.Fatalf("outer context must be preserved: %.100q", err.Error())
		}
	})
	t.Run("timeout keeps the last-failure text and its identity", func(t *testing.T) {
		t.Parallel()
		get, _ := replyGetter(pollReply{status: 503, body: body})

		_, err := Wait(quietContext(t), get, "t1", WaitConfig{Timeout: 50 * time.Millisecond, Sleep: ctxSleep})

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want DeadlineExceeded", err)
		}
		if !strings.Contains(err.Error(), "last poll failed: _stats returned status 503: transform \"t1\" rejected with HTTP 503: t: ééé") {
			t.Fatalf("last-failure context lost: %.200q", err.Error())
		}
	})
}

// ctxSleep waits for d or the end of ctx, whichever is first, like the
// production sleep seam.
func ctxSleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// quietContext carries a Nop logger so a Wait's retry warnings do not reach
// the zerolog global.
func quietContext(t *testing.T) context.Context {
	return runlog.WithLogger(t.Context(), logbuf.Nop())
}

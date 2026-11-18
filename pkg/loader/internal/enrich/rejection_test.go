package enrich

// LOG-RAWBODY-01 guards for the enrich HTTP-error diagnostics: the nameless
// list failure carries a bounded redacted Rejection, log-only lines carry
// es_error (or body_bytes for an unreadable 2xx) and never the raw body, a
// read failure keeps its precedence and error reads are bounded while a 2xx
// child-task body is still read in full. Bodies and secrets are synthetic.

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/bodytransport"
	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/logbuf"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

const secretBody = `{"error":{"type":"x","reason":"rejected password=FAKE-SECRET-REASON now"},` +
	`"password":"FAKE-SECRET-META","status":500}`

func TestListPoliciesFailureCarriesNamelessRejection(t *testing.T) {
	t.Parallel()

	stub := esstub.New(t)
	handler := esstub.JSON(http.StatusInternalServerError, secretBody)
	stub.Handle("GET /_enrich/policy", handler)
	stub.Handle("GET /_enrich/policy/{$}", handler)
	logger, buf := logbuf.New()
	ctx := runlog.WithLogger(t.Context(), logger)

	_, _, err := ListPolicies(ctx, stub.MustClient(t))

	want := "Failed to get enrich policies: enrich policy list request rejected with HTTP 500: x: rejected password=[REDACTED] now"
	var opErr *failure.Error
	if !errors.As(err, &opErr) || opErr.Op != "Failed to get enrich policies" {
		t.Fatalf("error = %#v, want Op %q", err, "Failed to get enrich policies")
	}
	var rejection *esresponse.Rejection
	if !errors.As(err, &rejection) {
		t.Fatalf("cause = %T, want *esresponse.Rejection", opErr.Err)
	}
	if err.Error() != want {
		t.Fatalf("message mismatch\n got: %s\nwant: %s", err.Error(), want)
	}
	var found bool
	for _, line := range buf.Lines(t) {
		if _, has := line["body"]; has {
			t.Fatalf("log line %v carries a body field", line)
		}
		if line["message"] == "Failed to get enrich policies" {
			found = true
			if line["error"] != opErr.Err.Error() {
				t.Fatalf("logged error %v != returned cause %q", line["error"], opErr.Err.Error())
			}
		}
	}
	// Without this, a site that stopped logging the failure would pass both
	// the cause comparison and the no-body sweep vacuously.
	if !found {
		t.Fatalf("no %q line in log:\n%s", "Failed to get enrich policies", buf.String())
	}
	if strings.Contains(err.Error()+buf.String(), "FAKE-SECRET") {
		t.Fatalf("a secret leaked:\n%s", buf.String())
	}
}

// TestEnrichLogOnlyLinesCarryESError pins the log-only shape for the generic
// 404 Warn and the execution Error: es_error is sanitized and bounded, the raw
// body and secrets are absent.
func TestEnrichLogOnlyLinesCarryESError(t *testing.T) {
	t.Parallel()

	t.Run("list generic 404", func(t *testing.T) {
		t.Parallel()
		stub := esstub.New(t)
		handler := esstub.JSON(http.StatusNotFound, `{"error":true,"note":"password=FAKE-SECRET-META"}`)
		stub.Handle("GET /_enrich/policy", handler)
		stub.Handle("GET /_enrich/policy/{$}", handler)
		logger, buf := logbuf.New()
		ctx := runlog.WithLogger(t.Context(), logger)

		_, supported, err := ListPolicies(ctx, stub.MustClient(t))

		if err != nil || supported {
			t.Fatalf("ListPolicies = (%t, %v), want an unsupported, non-error result", supported, err)
		}
		lines := enrichLogsWith(buf.Lines(t), "Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools")
		if len(lines) != 1 {
			t.Fatalf("generic-404 warns = %d, want 1\n%s", len(lines), buf.String())
		}
		want := "enrich API rejected with HTTP 404: ES error detail unavailable (no recognizable error field)"
		if _, has := lines[0]["body"]; has || lines[0]["es_error"] != want {
			t.Fatalf("line = %v, want es_error %q and no body", lines[0], want)
		}
		if strings.Contains(buf.String(), "FAKE-SECRET") {
			t.Fatal("log leaks a secret")
		}
	})
	t.Run("execute generic 404", func(t *testing.T) {
		t.Parallel()
		stub := esstub.New(t)
		stub.Handle("GET /_alias/.enrich-policy-a", esstub.JSON(http.StatusNotFound, aliasMissingBody))
		execute := esstub.JSON(http.StatusNotFound, `{"error":true,"note":"password=FAKE-SECRET-META"}`)
		stub.Handle("PUT /_enrich/policy/policy-a/_execute", execute)
		stub.Handle("POST /_enrich/policy/policy-a/_execute", execute)
		logger, buf := logbuf.New()
		ctx := runlog.WithLogger(t.Context(), logger)

		if executePolicy(ctx, stub.MustClient(t), "policy-a", fixedEnrichSeams()) {
			t.Fatal("executePolicy = true, want false")
		}

		lines := enrichLogsWith(buf.Lines(t), "Enrich execute endpoint returned a generic 404; check proxy or routing for /_enrich/policy/<name>/_execute and confirm this URL matches the backend used by Dev Tools")
		if len(lines) != 1 {
			t.Fatalf("generic-404 warns = %d, want 1\n%s", len(lines), buf.String())
		}
		// The generic-404 notice is about the endpoint, not the policy: the
		// nameless "enrich API" resource, not `enrich policy "policy-a"`.
		want := "enrich API rejected with HTTP 404: ES error detail unavailable (no recognizable error field)"
		if _, has := lines[0]["body"]; has || lines[0]["es_error"] != want {
			t.Fatalf("line = %v, want es_error %q and no body", lines[0], want)
		}
		if strings.Contains(buf.String(), "FAKE-SECRET") {
			t.Fatal("log leaks a secret")
		}
	})
	t.Run("execution failure", func(t *testing.T) {
		t.Parallel()
		stub := esstub.New(t)
		stub.Handle("GET /_alias/.enrich-policy-a", esstub.JSON(http.StatusNotFound, aliasMissingBody))
		execute := esstub.JSON(http.StatusInternalServerError, secretBody)
		stub.Handle("PUT /_enrich/policy/policy-a/_execute", execute)
		stub.Handle("POST /_enrich/policy/policy-a/_execute", execute)
		logger, buf := logbuf.New()
		ctx := runlog.WithLogger(t.Context(), logger)

		if executePolicy(ctx, stub.MustClient(t), "policy-a", fixedEnrichSeams()) {
			t.Fatal("executePolicy = true, want false")
		}

		lines := enrichLogsWith(buf.Lines(t), "Enrich policy execution failed")
		if len(lines) != 1 {
			t.Fatalf("execution-failed lines = %d, want 1\n%s", len(lines), buf.String())
		}
		want := `enrich policy "policy-a" rejected with HTTP 500: x: rejected password=[REDACTED] now`
		if _, has := lines[0]["body"]; has || lines[0]["es_error"] != want {
			t.Fatalf("line = %v, want es_error %q and no body", lines[0], want)
		}
		if strings.Contains(buf.String(), "FAKE-SECRET") {
			t.Fatal("log leaks a secret")
		}
	})
}

// childTaskBody is a valid child reindex task listing with a padded field so
// the counters sit past the 64 KiB error-read bound.
func childTaskBody(pad int) string {
	return `{"tasks":[{"action":"cluster:admin/xpack/enrich/reindex","description":"` + strings.Repeat("d", pad) +
		`","status":{"total":10,"created":3,"updated":2,"deleted":0,"noops":0}}]}`
}

// TestReindexProgressBodyHandling covers reindexProgress: an error status logs
// es_error from a bounded read, an unreadable 2xx logs only body_bytes, a read
// error returns (-1, -1) ahead of any status diagnostic on either branch, and
// a valid 2xx larger than the bound is still decoded in full.
func TestReindexProgressBodyHandling(t *testing.T) {
	t.Parallel()

	t.Run("error status logs es_error from a bounded read", func(t *testing.T) {
		t.Parallel()
		tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{func() bodytransport.Reply {
			return bodytransport.Reply{Status: 500, Body: &bodytransport.Body{R: bodytransport.Endless{}}}
		}}}
		logger, buf := logbuf.New()
		ctx := runlog.WithLogger(t.Context(), logger)

		done, total := reindexProgress(ctx, tr.Client(t), "node:1")

		if done != -1 || total != -1 {
			t.Fatalf("progress = (%d, %d), want (-1, -1)", done, total)
		}
		lines := enrichLogsWith(buf.Lines(t), "Enrich reindex progress unavailable: child task request returned an error")
		want := `enrich task "node:1" rejected with HTTP 500: ES error detail unavailable (response exceeded 65536 bytes)`
		if len(lines) != 1 || lines[0]["es_error"] != want {
			t.Fatalf("lines = %v, want one with es_error %q", lines, want)
		}
		if _, has := lines[0]["body"]; has {
			t.Fatal("the line carries a raw body field")
		}
		body := tr.Bodies()[0]
		if body.Served.Load() != esresponse.MaxRejectionBodyBytes+1 || body.Closes.Load() != 1 {
			t.Fatalf("served %d, closed %d; want %d and 1", body.Served.Load(), body.Closes.Load(), esresponse.MaxRejectionBodyBytes+1)
		}
	})
	t.Run("unparseable 2xx logs only body_bytes", func(t *testing.T) {
		t.Parallel()
		const raw = `{"tasks":"password=FAKE-SECRET-META"}`
		tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{bodytransport.Static(200, raw)}}
		logger, buf := logbuf.New()
		ctx := runlog.WithLogger(t.Context(), logger)

		done, total := reindexProgress(ctx, tr.Client(t), "node:1")

		if done != -1 || total != -1 {
			t.Fatalf("progress = (%d, %d), want (-1, -1)", done, total)
		}
		lines := enrichLogsWith(buf.Lines(t), "Enrich reindex progress unavailable: no single reindex child task with readable counters")
		if len(lines) != 1 || lines[0]["body_bytes"] != float64(len(raw)) {
			t.Fatalf("lines = %v, want one with body_bytes %d", lines, len(raw))
		}
		if _, has := lines[0]["body"]; has || strings.Contains(buf.String(), "FAKE-SECRET") {
			t.Fatalf("the raw body reached the log:\n%s", buf.String())
		}
	})
	t.Run("a read error wins over the status on both branches", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("injected read failure")
		for _, status := range []int{500, 200} {
			tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{func() bodytransport.Reply {
				return bodytransport.Reply{Status: status, Body: &bodytransport.Body{R: strings.NewReader("{"), Err: boom}}
			}}}
			logger, buf := logbuf.New()
			ctx := runlog.WithLogger(t.Context(), logger)

			done, total := reindexProgress(ctx, tr.Client(t), "node:1")

			if done != -1 || total != -1 {
				t.Fatalf("status %d: progress = (%d, %d), want (-1, -1)", status, done, total)
			}
			if len(enrichLogsWith(buf.Lines(t), "Enrich reindex progress unavailable: unable to read child task response")) != 1 {
				t.Fatalf("status %d: want the read-error Debug line\n%s", status, buf.String())
			}
		}
	})
	t.Run("a valid 2xx larger than the bound is decoded in full", func(t *testing.T) {
		t.Parallel()
		body := childTaskBody(2 * esresponse.MaxRejectionBodyBytes)
		tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{bodytransport.Static(200, body)}}
		ctx := runlog.WithLogger(t.Context(), logbuf.Nop())

		done, total := reindexProgress(ctx, tr.Client(t), "node:1")

		if done != 5 || total != 10 {
			t.Fatalf("progress = (%d, %d), want (5, 10): the counters sit past the bound", done, total)
		}
	})
}

// TestTaskPollOversizedErrorBodyKeepsStatusOnlyRetry covers the task-poll
// error branch with a body past the bound: every poll reads at most the probe
// byte and closes once, the retry count stays the status-only budget (three
// polls, two retries) and the retry Warn and terminal Error carry the oversize
// es_error instead of the body.
func TestTaskPollOversizedErrorBodyKeepsStatusOnlyRetry(t *testing.T) {
	t.Parallel()

	endless503 := func() bodytransport.Reply {
		return bodytransport.Reply{Status: 503, Body: &bodytransport.Body{R: bodytransport.Endless{}}}
	}
	tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{
		bodytransport.Static(http.StatusNotFound, aliasMissingBody), // pre-kickoff alias read
		bodytransport.Static(http.StatusOK, `{"task":"node:1"}`),    // kickoff
		endless503,
	}}
	logger, buf := logbuf.New()
	ctx := runlog.WithLogger(t.Context(), logger)

	if executePolicy(ctx, tr.Client(t), "policy-a", fixedEnrichSeams()) {
		t.Fatal("executePolicy = true, want false after the third consecutive 503")
	}

	if got := len(tr.Paths()); got != 5 {
		t.Fatalf("requests = %q, want alias read, kickoff and 3 polls", tr.Paths())
	}
	for i, b := range tr.Bodies()[2:] {
		if b.Served.Load() != esresponse.MaxRejectionBodyBytes+1 || b.Closes.Load() != 1 {
			t.Fatalf("poll %d served %d bytes, closed %d times; want %d and 1", i+1, b.Served.Load(), b.Closes.Load(), esresponse.MaxRejectionBodyBytes+1)
		}
	}
	const want = `enrich task "node:1" rejected with HTTP 503: ES error detail unavailable (response exceeded 65536 bytes)`
	lines := buf.Lines(t)
	retries := enrichLogsWith(lines, msgTaskPollRetry)
	finals := enrichLogsWith(lines, msgTaskPollError)
	if len(retries) != 2 || len(finals) != 1 {
		t.Fatalf("retry warns/terminal errors = %d/%d, want 2/1\n%s", len(retries), len(finals), buf.String())
	}
	for _, line := range append(retries, finals...) {
		if _, has := line["body"]; has || line["es_error"] != want {
			t.Fatalf("line = %v, want es_error %q and no body", line, want)
		}
	}
}

// TestListPoliciesOversizedGeneric404Fails pins the oversize guard on the
// generic-404 classifier at request level: a 404 whose body is past the bound
// and has no "error":{ object is no longer the unsupported-API skip, so the
// listing fails instead of reporting an unsupported, non-error result.
func TestListPoliciesOversizedGeneric404Fails(t *testing.T) {
	t.Parallel()

	stub := esstub.New(t)
	handler := esstub.JSON(http.StatusNotFound, strings.Repeat(" ", esresponse.MaxRejectionBodyBytes+10))
	stub.Handle("GET /_enrich/policy", handler)
	stub.Handle("GET /_enrich/policy/{$}", handler)
	ctx := runlog.WithLogger(t.Context(), logbuf.Nop())

	_, supported, err := ListPolicies(ctx, stub.MustClient(t))

	var opErr *failure.Error
	if !errors.As(err, &opErr) || opErr.Op != "Failed to get enrich policies" {
		t.Fatalf("ListPolicies = (supported %t, %v), want Op %q", supported, err, "Failed to get enrich policies")
	}
	if !strings.Contains(err.Error(), "response exceeded 65536 bytes") {
		t.Fatalf("message %q lacks the oversize detail", err.Error())
	}
}

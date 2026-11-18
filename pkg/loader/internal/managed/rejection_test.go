package managed

// LOG-RAWBODY-01 guards for the managed-resource HTTP failure sites: every
// failure returns its unchanged Op with a bounded, redacted Rejection cause,
// log-only lines carry es_error instead of the raw body, and a body past the
// read bound fails closed instead of being classified. All bodies and secrets
// are synthetic (FAKE-SECRET-*).

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/logbuf"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

// bufferContext attaches a JSON logger writing to the returned buffer.
func bufferContext(t *testing.T) (context.Context, *logbuf.Buffer) {
	t.Helper()
	logger, buf := logbuf.New()
	return runlog.WithLogger(t.Context(), logger), buf
}

// secretBody carries a synthetic credential both in the selected reason text
// and in ignored metadata, so a pass proves redaction and metadata exclusion.
const secretBody = `{"error":{"type":"x","reason":"rejected password=FAKE-SECRET-REASON now"},` +
	`"password":"FAKE-SECRET-META","status":500}`

// requireRejection asserts err is the operation error for op wrapping a
// Rejection, with the exact complete message, and returns it.
func requireRejection(t *testing.T, err error, op, wantMsg string) {
	t.Helper()
	var opErr *failure.Error
	if !errors.As(err, &opErr) || opErr.Op != op {
		t.Fatalf("error = %#v, want Op %q", err, op)
	}
	var rejection *esresponse.Rejection
	if !errors.As(err, &rejection) {
		t.Fatalf("cause = %T, want *esresponse.Rejection", opErr.Err)
	}
	if err.Error() != wantMsg {
		t.Fatalf("message mismatch\n got: %s\nwant: %s", err.Error(), wantMsg)
	}
	if len(err.Error()) > esresponse.MaxPublicMessageBytes {
		t.Fatalf("message is %d bytes, want <= %d", len(err.Error()), esresponse.MaxPublicMessageBytes)
	}
	for _, secret := range []string{"FAKE-SECRET-REASON", "FAKE-SECRET-META"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("message leaks %s: %s", secret, err.Error())
		}
	}
}

// requireLogCarriesCause asserts the failure line has no body field and its
// error field equals the returned cause, with no secret anywhere in the log.
func requireLogCarriesCause(t *testing.T, buf *logbuf.Buffer, err error) {
	t.Helper()
	var opErr *failure.Error
	if !errors.As(err, &opErr) {
		t.Fatalf("error = %T, want *failure.Error", err)
	}
	var found bool
	for _, line := range buf.Lines(t) {
		if _, has := line["body"]; has {
			t.Fatalf("log line %v carries a body field", line)
		}
		if line["message"] == opErr.Op && line["level"] == "fatal" {
			found = true
			if line["error"] != opErr.Err.Error() {
				t.Fatalf("logged error %v != returned cause %q", line["error"], opErr.Err.Error())
			}
		}
	}
	if !found {
		t.Fatalf("no fatal %q line in log:\n%s", opErr.Op, buf.String())
	}
	if strings.Contains(buf.String(), "FAKE-SECRET") {
		t.Fatalf("log leaks a secret:\n%s", buf.String())
	}
}

// TestManagedFailureSitesCarryRejectionCause drives one site per failing
// request shape: the Op is unchanged, the cause is a bounded redacted
// Rejection, a nameless request omits the quoted name and the log has no body.
func TestManagedFailureSitesCarryRejectionCause(t *testing.T) {
	t.Parallel()

	const reason = `x: rejected password=[REDACTED] now`
	tests := []struct {
		name    string
		handle  func(*esstub.Stub)
		run     func(ctx context.Context, es *elasticsearch.Client) error
		wantOp  string
		wantMsg string
	}{
		{
			name:   "delete pipeline",
			handle: func(s *esstub.Stub) { s.Handle("DELETE /_ingest/pipeline/p", esstub.JSON(500, secretBody)) },
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				return DeleteResources(ctx, es, []string{"p"}, nil, nil, false)
			},
			wantOp:  "Failed to delete pipeline",
			wantMsg: `Failed to delete pipeline: pipeline "p" rejected with HTTP 500: ` + reason,
		},
		{
			name:   "create enrich policy",
			handle: func(s *esstub.Stub) { s.Handle("PUT /_enrich/policy/pol", esstub.JSON(500, secretBody)) },
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				return CreatePolicies(ctx, es, config.Definitions{"pol": []byte(`{"match":{}}`)}, []string{"pol"}, sleepNow)
			},
			wantOp:  "Failed to create enrich policy",
			wantMsg: `Failed to create enrich policy: enrich policy "pol" rejected with HTTP 500: ` + reason,
		},
		{
			name:   "delete enrich policy keeps the referencing reason",
			handle: func(s *esstub.Stub) { s.Handle("DELETE /_enrich/policy/pol", esstub.JSON(409, referencingBody)) },
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				return DeleteResources(ctx, es, nil, []string{"pol"}, nil, false)
			},
			wantOp: "Failed to delete enrich policy",
			wantMsg: `Failed to delete enrich policy: enrich policy "pol" rejected with HTTP 409: ` +
				`illegal_argument_exception: Could not delete policy [pol] because a pipeline is referencing it [p]`,
		},
		{
			name: "nameless ingest pipeline list request",
			handle: func(s *esstub.Stub) {
				s.Handle("DELETE /_enrich/policy/pol", esstub.JSON(409, referencingBody))
				s.Handle("GET /_ingest/pipeline", esstub.JSON(500, secretBody))
			},
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				return DeleteResources(ctx, es, nil, []string{"pol"}, nil, true)
			},
			wantOp:  "Failed to get ingest pipelines",
			wantMsg: `Failed to get ingest pipelines: ingest pipeline list request rejected with HTTP 500: ` + reason,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			tt.handle(stub)
			ctx, buf := bufferContext(t)

			err := tt.run(ctx, stub.MustClient(t))

			requireRejection(t, err, tt.wantOp, tt.wantMsg)
			requireLogCarriesCause(t, buf, err)
		})
	}
}

const referencingBody = `{"error":{"type":"illegal_argument_exception",` +
	`"reason":"Could not delete policy [pol] because a pipeline is referencing it [p]"}}`

// TestCreatePoliciesRetryWarnLogsESError pins the log-only shape: the retry
// Warn carries es_error (sanitized, bounded) and never the raw body.
func TestCreatePoliciesRetryWarnLogsESError(t *testing.T) {
	t.Parallel()

	stub := esstub.New(t)
	stub.Handle("PUT /_enrich/policy/pol", esstub.JSON(404,
		`{"error":{"type":"index_not_found_exception","reason":"no such index password=FAKE-SECRET-REASON"},"password":"FAKE-SECRET-META"}`))
	ctx, buf := bufferContext(t)

	err := CreatePolicies(ctx, stub.MustClient(t), config.Definitions{"pol": []byte(`{"match":{}}`)}, []string{"pol"}, sleepNow)

	if err == nil {
		t.Fatal("expected the fifth attempt to fail")
	}
	want := `enrich policy "pol" rejected with HTTP 404: index_not_found_exception: no such index password=[REDACTED]`
	warns := 0
	for _, line := range buf.Lines(t) {
		if _, has := line["body"]; has {
			t.Fatalf("log line %v carries a body field", line)
		}
		if line["level"] == "warn" {
			warns++
			if line["es_error"] != want {
				t.Fatalf("es_error = %v, want %q", line["es_error"], want)
			}
		}
	}
	if warns != 4 {
		t.Fatalf("retry warns = %d, want 4", warns)
	}
	if strings.Contains(buf.String(), "FAKE-SECRET") {
		t.Fatal("log leaks a secret")
	}
}

// TestManagedLogOnlyLinesCarryESError pins the remaining managed log-only
// lines: both generic-404 notices name the nameless "enrich API" resource (the
// endpoint, not the policy), and the policy-GC delete failure names the policy.
// Each carries es_error (sanitized, bounded) and never the raw body or secret.
func TestManagedLogOnlyLinesCarryESError(t *testing.T) {
	t.Parallel()

	const generic404 = `{"error":true,"note":"password=FAKE-SECRET-META"}`
	const generic404Msg = "Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools"
	const generic404ESError = "enrich API rejected with HTTP 404: ES error detail unavailable (no recognizable error field)"
	tests := []struct {
		name    string
		handle  func(*esstub.Stub)
		run     func(ctx context.Context, es *elasticsearch.Client) error
		msg     string
		wantESE string
	}{
		{
			name:   "create policies generic 404",
			handle: func(s *esstub.Stub) { s.Handle("PUT /_enrich/policy/pol", esstub.JSON(404, generic404)) },
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				return CreatePolicies(ctx, es, config.Definitions{"pol": []byte(`{"match":{}}`)}, []string{"pol"}, sleepNow)
			},
			msg:     generic404Msg,
			wantESE: generic404ESError,
		},
		{
			name:   "delete policies generic 404",
			handle: func(s *esstub.Stub) { s.Handle("DELETE /_enrich/policy/pol", esstub.JSON(404, generic404)) },
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				return DeleteResources(ctx, es, nil, []string{"pol"}, nil, false)
			},
			msg:     generic404Msg,
			wantESE: generic404ESError,
		},
		{
			name:   "policy GC delete failure",
			handle: func(s *esstub.Stub) { s.Handle("DELETE /_enrich/policy/pol", esstub.JSON(500, secretBody)) },
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				deletePolicyBestEffort(ctx, es, "pol")
				return nil
			},
			msg:     "Managed policy GC could not delete policy",
			wantESE: `enrich policy "pol" rejected with HTTP 500: x: rejected password=[REDACTED] now`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			tt.handle(stub)
			ctx, buf := bufferContext(t)

			if err := tt.run(ctx, stub.MustClient(t)); err != nil {
				t.Fatalf("a log-only path must not fail: %v", err)
			}

			matched := 0
			for _, line := range buf.Lines(t) {
				if _, has := line["body"]; has {
					t.Fatalf("log line %v carries a body field", line)
				}
				if line["message"] == tt.msg {
					matched++
					if line["level"] != "warn" || line["es_error"] != tt.wantESE {
						t.Fatalf("line = %v, want a warn with es_error %q", line, tt.wantESE)
					}
				}
			}
			if matched != 1 {
				t.Fatalf("%q lines = %d, want 1\n%s", tt.msg, matched, buf.String())
			}
			if strings.Contains(buf.String(), "FAKE-SECRET") {
				t.Fatalf("log leaks a secret:\n%s", buf.String())
			}
		})
	}
}

// TestPolicyGCDeleteReadsBoundedAndClosesOnce covers deletePolicyBestEffort's
// restructured read: an error status reads at most the probe byte past the
// bound, a 404 (already deleted) needs no body and reads none, and both close
// the body exactly once.
func TestPolicyGCDeleteReadsBoundedAndClosesOnce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     int
		wantServed int64
	}{
		{"error status reads bounded", http.StatusInternalServerError, esresponse.MaxRejectionBodyBytes + 1},
		{"404 reads nothing", http.StatusNotFound, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodies := &bodyRecorder{make: func(ctx context.Context) *observedBody {
				return &observedBody{r: endlessReader{}, ctx: ctx}
			}}
			tr := &rejectionTransport{status: tt.status, body: bodies.factory}
			ctx, _ := bufferContext(t)

			deletePolicyBestEffort(ctx, newTransportClient(t, tr), "pol")

			requireOneClose(t, bodies.all(), 1)
			if got := bodies.all()[0].served.Load(); got != tt.wantServed {
				t.Fatalf("served %d bytes, want %d", got, tt.wantServed)
			}
		})
	}
}

// oversizedBody is a body one probe byte past the bound whose first bytes would
// match a body classifier: it must classify as no match.
func oversizedBody(match string, size int) string {
	return match + strings.Repeat(" ", size-len(match))
}

// TestCreatePoliciesOversizedBodyFailsInsteadOfRetrying pins AC5: a retryable
// token inside the first bytes of a body past the read bound is not trusted.
func TestCreatePoliciesOversizedBodyFailsInsteadOfRetrying(t *testing.T) {
	t.Parallel()

	stub := esstub.New(t)
	body := oversizedBody(`{"error":{"type":"index_not_found_exception"}}`, esresponse.MaxRejectionBodyBytes+100)
	stub.Handle("PUT /_enrich/policy/pol", esstub.JSON(404, body))
	ctx, _ := bufferContext(t)

	err := CreatePolicies(ctx, stub.MustClient(t), config.Definitions{"pol": []byte(`{"match":{}}`)}, []string{"pol"}, sleepNow)

	requireOp(t, err, "Failed to create enrich policy")
	if !strings.Contains(err.Error(), "response exceeded 65536 bytes") {
		t.Fatalf("message %q lacks the oversize detail", err.Error())
	}
	if got := len(stub.Requests()); got != 1 {
		t.Fatalf("requests = %d, want 1 (an oversized body must not retry)", got)
	}
}

// sleepNow is a retry sleep that never waits.
func sleepNow(ctx context.Context, _ time.Duration) error { return ctx.Err() }

// TestManagedBodyPredicatesRejectACutBody covers AC5 for the two managed
// classifiers: a match padded to exactly 64 KiB keeps its result, the same
// prefix at 64 KiB + 1 is no match.
func TestManagedBodyPredicatesRejectACutBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		match string
		pred  func([]byte) bool
	}{
		{"policy delete blocked by pipeline reference", `{"error":{"reason":"a pipeline is referencing it"}}`, policyDeleteBlockedByPipelineReference},
		{"pipeline delete blocked by default index", `{"error":{"reason":"cannot be deleted because it is the default pipeline"}}`, pipelineDeleteBlockedByDefaultIndex},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if !tt.pred([]byte(oversizedBody(tt.match, esresponse.MaxRejectionBodyBytes))) {
				t.Fatal("a match at exactly the bound must keep its result")
			}
			if tt.pred([]byte(oversizedBody(tt.match, esresponse.MaxRejectionBodyBytes+1))) {
				t.Fatal("a match in a cut body must be no match")
			}
		})
	}
}

// TestDeletePoliciesOversized404 guards the fail-closed branch: a 404 whose
// body is past the read bound cannot be classified, and predicate no-match
// alone would have reported an absent policy. It must fail with the existing
// literal Op, read and close the body once within the bound, and stop before
// the next policy or any recovery request. Ordinary 404s keep their handling.
func TestDeletePoliciesOversized404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		nuke     bool
		body     string
		wantFail bool
	}{
		{"generic 404 at exactly the bound is the generic-404 warning", false, oversizedBody(`no es error`, esresponse.MaxRejectionBodyBytes), false},
		{"missing-policy JSON at exactly the bound is absence", true, oversizedBody(`{"error":{"type":"resource_not_found_exception"}}`, esresponse.MaxRejectionBodyBytes), false},
		{"generic 404 one byte past the bound fails", false, oversizedBody(`no es error`, esresponse.MaxRejectionBodyBytes+1), true},
		{"missing-policy JSON one byte past the bound fails", false, oversizedBody(`{"error":{"type":"resource_not_found_exception"}}`, esresponse.MaxRejectionBodyBytes+1), true},
		{"nuke mode also fails when oversized", true, oversizedBody(`{"error":{"type":"resource_not_found_exception"}}`, esresponse.MaxRejectionBodyBytes+1), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodies := staticBodies(tt.body)
			tr := &rejectionTransport{status: http.StatusNotFound, body: bodies.factory}
			es := newTransportClient(t, tr)
			ctx, buf := bufferContext(t)

			err := DeleteResources(ctx, es, nil, []string{"first", "second"}, nil, tt.nuke)

			requireOneClose(t, bodies.all()[:1], 1)
			if !tt.wantFail {
				if err != nil {
					t.Fatalf("an ordinary 404 must not fail: %v", err)
				}
				return
			}
			var opErr *failure.Error
			if !errors.As(err, &opErr) || opErr.Op != "Failed to delete enrich policy" {
				t.Fatalf("error = %v, want Op %q", err, "Failed to delete enrich policy")
			}
			var rejection *esresponse.Rejection
			if !errors.As(err, &rejection) {
				t.Fatalf("cause = %T, want *esresponse.Rejection", opErr.Err)
			}
			want := `Failed to delete enrich policy: enrich policy "first" rejected with HTTP 404: ES error detail unavailable (response exceeded 65536 bytes)`
			if err.Error() != want {
				t.Fatalf("message mismatch\n got: %s\nwant: %s", err.Error(), want)
			}
			if got, want := tr.requested(), []string{"DELETE /_enrich/policy/first"}; len(got) != 1 || got[0] != want[0] {
				t.Fatalf("requests = %q, want exactly %q (no next policy, retry or recovery)", got, want)
			}
			if got := bodies.all()[0].served.Load(); got != esresponse.MaxRejectionBodyBytes+1 {
				t.Fatalf("served %d bytes, want %d (the bounded read)", got, esresponse.MaxRejectionBodyBytes+1)
			}
			for _, line := range buf.Lines(t) {
				if _, has := line["body"]; has {
					t.Fatalf("log carries a body field: %v", line)
				}
				if strings.Contains(line["message"].(string), "generic 404") || strings.Contains(line["message"].(string), "Nothing to delete") {
					t.Fatalf("an oversized 404 must not log the absence path: %v", line)
				}
			}
		})
	}

	t.Run("an oversized 404 with a read error keeps the error identity", func(t *testing.T) {
		t.Parallel()

		boom := errors.New("injected read failure")
		bodies := &bodyRecorder{make: func(ctx context.Context) *observedBody {
			return &observedBody{r: &overflowThenError{err: boom}, ctx: ctx}
		}}
		tr := &rejectionTransport{status: http.StatusNotFound, body: bodies.factory}
		es := newTransportClient(t, tr)
		ctx, _ := bufferContext(t)

		err := DeleteResources(ctx, es, nil, []string{"first", "second"}, nil, false)

		requireOp(t, err, "Failed to delete enrich policy")
		if !errors.Is(err, boom) {
			t.Fatalf("errors.Is lost the read error: %v", err)
		}
		if strings.Contains(err.Error(), boom.Error()) || !strings.Contains(err.Error(), "response body read failed") {
			t.Fatalf("message %q must show the fixed read-failed detail, not the error text", err.Error())
		}
		if got := tr.requested(); len(got) != 1 {
			t.Fatalf("requests = %q, want one", got)
		}
	})
}

// overflowThenError serves spaces and returns err together with the byte that
// brings the total to the read bound plus one probe byte, the (n > 0,
// err != nil) case io.Reader allows.
type overflowThenError struct {
	err    error
	served int
}

func (o *overflowThenError) Read(p []byte) (int, error) {
	n := min(len(p), esresponse.MaxRejectionBodyBytes+1-o.served)
	for i := range n {
		p[i] = ' '
	}
	o.served += n
	if o.served == esresponse.MaxRejectionBodyBytes+1 {
		return n, o.err
	}
	return n, nil
}

// TestManagedErrorReadsAreBoundedAndClosedOnce proves a failing delete reads at
// most the probe byte past the bound and closes the body once, for a site that
// previously did an unbounded io.ReadAll.
func TestManagedErrorReadsAreBoundedAndClosedOnce(t *testing.T) {
	t.Parallel()

	bodies := &bodyRecorder{make: func(ctx context.Context) *observedBody {
		return &observedBody{r: endlessReader{}, ctx: ctx}
	}}
	tr := &rejectionTransport{status: http.StatusInternalServerError, body: bodies.factory}
	es := newTransportClient(t, tr)
	ctx, _ := bufferContext(t)

	err := DeleteResources(ctx, es, []string{"p"}, nil, nil, false)

	requireOp(t, err, "Failed to delete pipeline")
	requireOneClose(t, bodies.all(), 1)
	if got := bodies.all()[0].served.Load(); got != esresponse.MaxRejectionBodyBytes+1 {
		t.Fatalf("served %d bytes, want %d", got, esresponse.MaxRejectionBodyBytes+1)
	}
}

package enrich

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/logbuf"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

// TestExecutePolicyCancellationStopsPolling verifies that cancelling the
// caller's context during the Tasks API poll wait stops the loop immediately
// instead of continuing to poll until the task completes.
func TestExecutePolicyCancellationStopsPolling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	sleepCalls := 0
	cfg := progress.Seams{
		Now: time.Now,
		Sleep: func(sleepCtx context.Context, _ time.Duration) error {
			sleepCalls++
			cancel()
			return sleepCtx.Err()
		},
	}

	var taskGetCalls atomic.Int32
	execute := esstub.JSON(http.StatusOK, `{"task":"node:123"}`)
	stub := esstub.New(t)
	// The pre-kickoff alias snapshot; 404 is the absent "before" state.
	stub.Handle("GET /_alias/.enrich-policy-a", esstub.JSON(http.StatusNotFound, aliasMissingBody))
	stub.Handle("PUT /_enrich/policy/policy-a/_execute", execute)
	stub.Handle("POST /_enrich/policy/policy-a/_execute", execute)
	stub.Handle("GET /_tasks/node:123", func(w http.ResponseWriter, _ *http.Request) {
		taskGetCalls.Add(1)
		_, _ = w.Write([]byte(`{"completed":false,"task":{"status":{"phase":"RUNNING"}}}`))
	})

	es := stub.MustClient(t)

	result := make(chan bool, 1)
	go func() {
		result <- executePolicy(ctx, es, "policy-a", cfg).succeeded()
	}()

	select {
	case ok := <-result:
		if ok {
			t.Fatal("executePolicy returned true, want false after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("executePolicy did not return promptly after cancellation")
	}

	if sleepCalls != 1 {
		t.Fatalf("Sleep calls = %d, want 1", sleepCalls)
	}
	if got := taskGetCalls.Load(); got != 0 {
		t.Fatalf("Tasks.Get calls = %d, want 0 (poll loop should stop before polling again)", got)
	}
}

// TestResolveTargetsAllPolicies guards that an "all" selection targets every available policy, sorted.
func TestResolveTargetsAllPolicies(t *testing.T) {
	t.Parallel()

	sel := &Selection{Enabled: true, All: true}
	available := []string{"policy-b", "policy-a"}

	targets, missing := resolveTargets(sel, available, nil)

	if want := []string{"policy-a", "policy-b"}; !reflect.DeepEqual(targets, want) {
		t.Fatalf("targets mismatch: got %v want %v", targets, want)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing policies, got %v", missing)
	}
}

// TestResolveTargetsWarnsForMissingPolicies guards that explicitly named policies absent from the cluster are reported as missing, not targeted.
func TestResolveTargetsWarnsForMissingPolicies(t *testing.T) {
	t.Parallel()

	sel := &Selection{Enabled: true, Raw: "policy-b,policy-missing,policy-a"}
	available := []string{"policy-a", "policy-b"}

	targets, missing := resolveTargets(sel, available, nil)

	if want := []string{"policy-b", "policy-a"}; !reflect.DeepEqual(targets, want) {
		t.Fatalf("targets mismatch: got %v want %v", targets, want)
	}
	if want := []string{"policy-missing"}; !reflect.DeepEqual(missing, want) {
		t.Fatalf("missing mismatch: got %v want %v", missing, want)
	}
}

// TestResolveTargetsUsesDeclaredPoliciesWhenRequestedAll guards that an "all" selection is limited to the declared policies when a declared list is supplied.
func TestResolveTargetsUsesDeclaredPoliciesWhenRequestedAll(t *testing.T) {
	t.Parallel()

	sel := &Selection{Enabled: true, All: true}
	available := []string{"e2e-source-policy", "slugs-by-name"}
	declared := []string{"slugs-by-name"}

	targets, missing := resolveTargets(sel, available, declared)

	if len(missing) != 0 {
		t.Fatalf("expected no missing policies, got %v", missing)
	}
	if len(targets) != 1 || targets[0] != "slugs-by-name" {
		t.Fatalf("expected declared policy only, got %v", targets)
	}
}

// TestSelectionExplicitPolicies pins the nil, disabled and All semantics and
// the trim/de-duplicate rules that the remap and target resolution rely on.
func TestSelectionExplicitPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sel  *Selection
		want []string
	}{
		{name: "nil selection", sel: nil, want: nil},
		{name: "disabled selection", sel: &Selection{Raw: "a"}, want: nil},
		{name: "all selection", sel: &Selection{Enabled: true, All: true, Raw: "a"}, want: nil},
		{name: "trimmed and de-duplicated in order", sel: &Selection{Enabled: true, Raw: " b,a, b ,,c "}, want: []string{"b", "a", "c"}},
		{name: "empty explicit list", sel: &Selection{Enabled: true, Raw: " , "}, want: []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.sel.ExplicitPolicies(); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ExplicitPolicies() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

// TestListPolicies pins sorting, the unsupported-API warn-and-skip result and
// the failure ops of an error status and an unparseable body.
func TestListPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		status        int
		body          string
		want          []string
		wantSupported bool
		wantOp        string
	}{
		{name: "sorted names", status: http.StatusOK, body: `{"policies":[{"config":{"match":{"name":"b"}}},{"config":{"range":{"name":"a"}}}]}`, want: []string{"a", "b"}, wantSupported: true},
		{name: "generic 404 is unsupported, not an error", status: http.StatusNotFound, body: `{"error":true}`},
		{name: "error status fails", status: http.StatusInternalServerError, body: `{"error":{"type":"stub"}}`, wantOp: "Failed to get enrich policies"},
		{name: "malformed body fails", status: http.StatusOK, body: `{`, wantOp: "Unable to parse enrich policy response"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			handler := esstub.JSON(tt.status, tt.body)
			stub.Handle("GET /_enrich/policy", handler)
			stub.Handle("GET /_enrich/policy/{$}", handler)
			got, supported, err := ListPolicies(t.Context(), stub.MustClient(t))
			if tt.wantOp != "" {
				var opErr *failure.Error
				if !errors.As(err, &opErr) || opErr.Op != tt.wantOp {
					t.Fatalf("error = %#v, want operation error %q", err, tt.wantOp)
				}
				return
			}
			if err != nil {
				t.Fatalf("ListPolicies returned error: %v", err)
			}
			if supported != tt.wantSupported || !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ListPolicies = (%v, %t), want (%v, %t)", got, supported, tt.want, tt.wantSupported)
			}
		})
	}
}

// aliasMissingBody is the 404 body Elasticsearch returns for an alias that
// does not exist.
const aliasMissingBody = `{"error":"alias [.enrich-x] missing","status":404}`

// enrichTaskNotFoundBody is the recorded 9.3.1 answer to GET _tasks/<id> once
// an async enrich task has ended without storing its result.
const enrichTaskNotFoundBody = `{"error":{"type":"resource_not_found_exception","reason":"task [node:1] isn't running and hasn't stored its results"},"status":404}`

// enrichAliasBody renders a GET /_alias/<alias> response listing targets in
// the given order, each carrying alias under its "aliases" object.
func enrichAliasBody(alias string, targets ...string) string {
	entries := make([]string, 0, len(targets))
	for _, target := range targets {
		entries = append(entries, fmt.Sprintf(`%q:{"aliases":{%q:{}}}`, target, alias))
	}
	return "{" + strings.Join(entries, ",") + "}"
}

// aliasStep is one alias read in a verification test: an HTTP reply, or a
// transport failure the client sees instead of a response.
type aliasStep struct {
	status    int
	body      string
	transport bool
}

// enrichRoundTripFunc adapts a function to http.RoundTripper so a test can
// fail one chosen request at the transport level.
type enrichRoundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip implements http.RoundTripper.
func (f enrichRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// errAliasTransport is the sentinel transport failure injected into an alias read.
var errAliasTransport = errors.New("injected alias transport failure")

// enrichLogLines decodes the captured JSON log records.
func enrichLogLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, text := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if text == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(text), &line); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", text, err)
		}
		lines = append(lines, line)
	}
	return lines
}

// enrichLogsWith returns the records with the given message.
func enrichLogsWith(lines []map[string]any, message string) []map[string]any {
	var found []map[string]any
	for _, line := range lines {
		if line["message"] == message {
			found = append(found, line)
		}
	}
	return found
}

// logStrs reads a string-array log field; a missing or null field is nil.
func logStrs(line map[string]any, key string) []string {
	raw, _ := line[key].([]any)
	if raw == nil {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		s, _ := v.(string)
		out = append(out, s)
	}
	return out
}

// fixedEnrichSeams returns seams with a fixed clock and a sleep that only
// reports cancellation, so polling needs no real time and no global seam.
func fixedEnrichSeams() progress.Seams {
	start := time.Unix(1_000_000, 0)
	return progress.Seams{
		Now:   func() time.Time { return start },
		Sleep: func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
	}
}

const (
	msgEnrichSucceeded  = "Enrich policy execution succeeded"
	msgAliasVerified    = "Enrich policy outcome verified via backing alias"
	msgAliasNotMoved    = "Enrich policy execution failed: the backing alias did not move"
	msgAliasUnverified  = "Enrich policy outcome could not be verified"
	msgAliasHTTPFailure = "Failed to resolve alias targets"
	msgAliasParseFail   = "Unable to parse alias response"
)

// TestExecutePolicyVerifiesTask404 guards FIX-ENRICH404-01 (ADR 0004): a task
// GET answering 404 says nothing about the outcome, so the policy succeeds only
// when the exact .enrich-<policy> alias, read before kickoff and again after
// the 404, now has a non-empty target set that differs. Unchanged or absent
// fails; either unreadable snapshot (HTTP, decode, JSON null or transport
// error) fails as unverified and is never mistaken for an absent alias.
// Reverting to "404 is success" makes the false rows pass as succeeded.
func TestExecutePolicyVerifiesTask404(t *testing.T) {
	t.Parallel()

	const hashed = "policy-a-abcdef"
	alias := func(policy string) string { return ".enrich-" + policy }
	present := func(policy string, targets ...string) aliasStep {
		return aliasStep{status: http.StatusOK, body: enrichAliasBody(alias(policy), targets...)}
	}
	absent := aliasStep{status: http.StatusNotFound, body: aliasMissingBody}
	fail := func(status int, body string) aliasStep { return aliasStep{status: status, body: body} }
	transport := aliasStep{transport: true}

	type verdict int
	const (
		verified verdict = iota
		notMoved
		unverified
	)

	tests := []struct {
		name          string
		policy        string
		before, after aliasStep
		want          verdict
		wantBefore    []string // not-moved rows: logged sets
		wantAfter     []string
		beforeFails   bool   // unverified rows: which reads are identified
		afterFails    bool   //
		wantDiag      string // original resolver diagnostic message
		wantDiagCode  int    // its status_code, when HTTP
		wantCause     string // substring of the contextual Error's error field
	}{
		{name: "first execution: absent then present", policy: hashed, before: absent, after: present(hashed, ".enrich-"+hashed+"-2"), want: verified},
		{name: "existing x then y", policy: "policy-a", before: present("policy-a", "x"), after: present("policy-a", "y"), want: verified},
		{name: "x,y then x is a changed non-empty set", policy: "policy-a", before: present("policy-a", "x", "y"), after: present("policy-a", "x"), want: verified},

		{name: "x then x is unchanged", policy: hashed, before: present(hashed, "x"), after: present(hashed, "x"), want: notMoved, wantBefore: []string{"x"}, wantAfter: []string{"x"}},
		{name: "x,y then y,x is unchanged regardless of order", policy: "policy-a", before: present("policy-a", "x", "y"), after: present("policy-a", "y", "x"), want: notMoved, wantBefore: []string{"x", "y"}, wantAfter: []string{"x", "y"}},
		{name: "absent then absent", policy: "policy-a", before: absent, after: absent, want: notMoved},
		{name: "x then absent", policy: "policy-a", before: present("policy-a", "x"), after: absent, want: notMoved, wantBefore: []string{"x"}},
		{name: "x then empty object", policy: "policy-a", before: present("policy-a", "x"), after: fail(http.StatusOK, `{}`), want: notMoved, wantBefore: []string{"x"}, wantAfter: []string{}},

		{name: "before 403", policy: hashed, before: fail(http.StatusForbidden, `{"error":{"type":"security_exception"},"status":403}`), after: present(hashed, "x"), want: unverified, beforeFails: true, wantDiag: msgAliasHTTPFailure, wantDiagCode: http.StatusForbidden, wantCause: msgAliasHTTPFailure},
		{name: "before 500", policy: "policy-a", before: fail(http.StatusInternalServerError, `{"error":{"type":"boom"}}`), after: present("policy-a", "x"), want: unverified, beforeFails: true, wantDiag: msgAliasHTTPFailure, wantDiagCode: http.StatusInternalServerError, wantCause: msgAliasHTTPFailure},
		{name: "before malformed JSON", policy: "policy-a", before: fail(http.StatusOK, `{`), after: present("policy-a", "x"), want: unverified, beforeFails: true, wantDiag: msgAliasParseFail, wantCause: "unexpected EOF"},
		{name: "before JSON null is not absent", policy: "policy-a", before: fail(http.StatusOK, `null`), after: present("policy-a", "x"), want: unverified, beforeFails: true, wantDiag: msgAliasParseFail, wantCause: "alias response was null"},
		{name: "before transport error", policy: "policy-a", before: transport, after: present("policy-a", "x"), want: unverified, beforeFails: true, wantCause: errAliasTransport.Error()},

		{name: "after 403", policy: "policy-a", before: absent, after: fail(http.StatusForbidden, `{"error":{"type":"security_exception"},"status":403}`), want: unverified, afterFails: true, wantDiag: msgAliasHTTPFailure, wantDiagCode: http.StatusForbidden, wantCause: msgAliasHTTPFailure},
		{name: "after 500", policy: "policy-a", before: present("policy-a", "x"), after: fail(http.StatusInternalServerError, `{"error":{"type":"boom"}}`), want: unverified, afterFails: true, wantDiag: msgAliasHTTPFailure, wantDiagCode: http.StatusInternalServerError, wantCause: msgAliasHTTPFailure},
		{name: "after malformed JSON", policy: "policy-a", before: absent, after: fail(http.StatusOK, `{"x":`), want: unverified, afterFails: true, wantDiag: msgAliasParseFail, wantCause: "unexpected EOF"},
		{name: "after JSON null", policy: "policy-a", before: absent, after: fail(http.StatusOK, `null`), want: unverified, afterFails: true, wantDiag: msgAliasParseFail, wantCause: "alias response was null"},
		{name: "after transport error", policy: hashed, before: absent, after: transport, want: unverified, afterFails: true, wantCause: errAliasTransport.Error()},

		{name: "both reads fail", policy: "policy-a", before: fail(http.StatusInternalServerError, `{"error":{"type":"boom"}}`), after: fail(http.StatusOK, `null`), want: unverified, beforeFails: true, afterFails: true, wantDiag: msgAliasParseFail, wantCause: msgAliasHTTPFailure},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			aliasPath := "/_alias/" + alias(tt.policy)
			steps := []aliasStep{tt.before, tt.after}
			var served []aliasStep // the steps that reach the stub, in order
			for _, step := range steps {
				if !step.transport {
					served = append(served, step)
				}
			}

			var servedCalls atomic.Int32
			stub := esstub.New(t)
			// Only the exact selected name is routed; a read of the logical
			// name (policy-a for the hashed rows) is an unexpected request.
			stub.Handle("GET "+aliasPath, func(w http.ResponseWriter, _ *http.Request) {
				i := int(servedCalls.Add(1)) - 1
				if i >= len(served) {
					t.Errorf("alias read %d, want at most %d", i+1, len(served))
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.WriteHeader(served[i].status)
				_, _ = w.Write([]byte(served[i].body))
			})
			execute := esstub.JSON(http.StatusOK, `{"task":"node:1"}`)
			stub.Handle("PUT /_enrich/policy/"+tt.policy+"/_execute", execute)
			stub.Handle("POST /_enrich/policy/"+tt.policy+"/_execute", execute)
			stub.Handle("GET /_tasks/node:1", esstub.JSON(http.StatusNotFound, enrichTaskNotFoundBody))

			base := &http.Transport{}
			t.Cleanup(base.CloseIdleConnections)
			var (
				mu         sync.Mutex
				attempts   []string
				aliasReads int
			)
			wrapper := enrichRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				mu.Lock()
				attempts = append(attempts, r.URL.Path)
				intercept := false
				if r.URL.Path == aliasPath {
					intercept = aliasReads < len(steps) && steps[aliasReads].transport
					aliasReads++
				}
				mu.Unlock()
				if intercept {
					return nil, errAliasTransport
				}
				return base.RoundTrip(r)
			})
			es, err := elasticsearch.NewClient(elasticsearch.Config{
				Addresses:    []string{stub.URL()},
				Transport:    wrapper,
				DisableRetry: true,
				MaxRetries:   0,
			})
			if err != nil {
				t.Fatalf("new elasticsearch client: %v", err)
			}

			var buf bytes.Buffer
			logger := zerolog.New(&buf).Level(zerolog.DebugLevel)
			ctx := runlog.WithLogger(t.Context(), &logger)

			got := executePolicy(ctx, es, tt.policy, fixedEnrichSeams()).succeeded()
			if want := tt.want == verified; got != want {
				t.Fatalf("executePolicy = %v, want %v\nlogs:\n%s", got, want, buf.String())
			}

			mu.Lock()
			gotOrder := slices.Clone(attempts)
			mu.Unlock()
			wantOrder := []string{aliasPath, "/_enrich/policy/" + tt.policy + "/_execute", "/_tasks/node:1", aliasPath}
			if !slices.Equal(gotOrder, wantOrder) {
				t.Fatalf("request order = %v, want %v (alias read before kickoff, again after the task 404)", gotOrder, wantOrder)
			}

			lines := enrichLogLines(t, &buf)
			succeeded := enrichLogsWith(lines, msgEnrichSucceeded)
			verifiedLines := enrichLogsWith(lines, msgAliasVerified)
			if tt.want == verified {
				if len(succeeded) != 1 || succeeded[0]["level"] != "info" {
					t.Fatalf("success lines = %v, want one at info", succeeded)
				}
				if len(verifiedLines) != 1 || verifiedLines[0]["level"] != "debug" ||
					verifiedLines[0]["policy"] != tt.policy || verifiedLines[0]["task"] != "node:1" || verifiedLines[0]["alias"] != alias(tt.policy) {
					t.Fatalf("verification lines = %v, want one debug line naming policy, task and alias", verifiedLines)
				}
				for _, line := range lines {
					if level := line["level"]; level == "error" || level == "fatal" {
						t.Fatalf("a verified outcome must not log %v: %v", level, line)
					}
				}
				return
			}

			if len(succeeded) != 0 || len(verifiedLines) != 0 {
				t.Fatalf("a failed outcome must log no success or verification line: %v %v", succeeded, verifiedLines)
			}
			message := msgAliasNotMoved
			if tt.want == unverified {
				message = msgAliasUnverified
			}
			failures := enrichLogsWith(lines, message)
			if len(failures) != 1 {
				t.Fatalf("%q lines = %d, want 1\nlogs:\n%s", message, len(failures), buf.String())
			}
			line := failures[0]
			if line["level"] != "error" || line["policy"] != tt.policy || line["task"] != "node:1" || line["alias"] != alias(tt.policy) {
				t.Fatalf("failure line = %v, want error level naming policy, task and alias", line)
			}
			if _, ok := line["before"]; !ok {
				t.Fatalf("failure line = %v, want a before field", line)
			}
			if _, ok := line["after"]; !ok {
				t.Fatalf("failure line = %v, want an after field", line)
			}

			if tt.want == notMoved {
				if gotBefore := logStrs(line, "before"); !slices.Equal(gotBefore, tt.wantBefore) {
					t.Fatalf("before = %v, want %v", gotBefore, tt.wantBefore)
				}
				if gotAfter := logStrs(line, "after"); !slices.Equal(gotAfter, tt.wantAfter) {
					t.Fatalf("after = %v, want %v", gotAfter, tt.wantAfter)
				}
				return
			}

			_, hasBeforeErr := line["before_error"]
			_, hasAfterErr := line["after_error"]
			if hasBeforeErr != tt.beforeFails || hasAfterErr != tt.afterFails {
				t.Fatalf("failure line = %v, want before_error=%v after_error=%v", line, tt.beforeFails, tt.afterFails)
			}
			if cause, _ := line["error"].(string); !strings.Contains(cause, tt.wantCause) {
				t.Fatalf("error = %q, want it to contain %q", cause, tt.wantCause)
			}
			if tt.wantDiag != "" {
				diags := enrichLogsWith(lines, tt.wantDiag)
				if len(diags) == 0 || diags[len(diags)-1]["alias"] != alias(tt.policy) {
					t.Fatalf("resolver diagnostic %q = %v, want it kept with the alias", tt.wantDiag, diags)
				}
				if tt.wantDiagCode != 0 {
					if code, _ := diags[0]["status_code"].(float64); int(code) != tt.wantDiagCode {
						t.Fatalf("resolver diagnostic status_code = %v, want %d", diags[0]["status_code"], tt.wantDiagCode)
					}
				}
			}
		})
	}
}

const (
	msgTaskPollRetry = "Enrich policy task poll failed; retrying"
	msgTaskPollError = "Enrich policy task poll returned an error"
	msgTaskPollFail  = "Failed to poll enrich policy task"
	msgTaskParseFail = "Unable to parse enrich policy task response"
	msgPollCancelled = "Context cancelled while polling enrich task"

	taskRunningBody   = `{"completed":false,"task":{"status":{"phase":"RUNNING"}}}`
	taskCompletedBody = `{"completed":true,"response":{"status":{"phase":"COMPLETE"}}}`
)

// taskStep is one GET _tasks/<id> attempt in a poll-retry test: an HTTP reply
// served by the stub, or a transport failure the client sees instead. readErr
// and closeErr are injected into the reply's body after its bytes are read.
type taskStep struct {
	status    int
	body      string
	transport error
	readErr   error
	closeErr  error
}

// trackedBody wraps a task response body: it delegates reads, replaces the
// final io.EOF with readErr when set (so a reader gets the text and then an
// error), and counts Close calls so a test can prove each body is closed
// exactly once before the next poll.
type trackedBody struct {
	io.ReadCloser
	readErr  error
	closeErr error
	closes   atomic.Int32
}

// Read implements io.Reader.
func (b *trackedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if errors.Is(err, io.EOF) && b.readErr != nil {
		return n, b.readErr
	}
	return n, err
}

// Close implements io.Closer.
func (b *trackedBody) Close() error {
	b.closes.Add(1)
	if err := b.ReadCloser.Close(); err != nil {
		return err
	}
	return b.closeErr
}

// TestExecutePolicyPollRetries guards FEAT-WAITRETRY-01's enrich task-poll
// budget: a 429/502/503/504 task read is retried on the existing loop-head
// sleep, the third consecutive one fails the policy with an Error and no
// further poll, a decoded poll resets the streak, a task 404 after retries is
// still verified through the backing alias, and every other failure (400,
// 500, malformed 2xx, a transport error, cancellation) ends on its first poll.
// The kickoff is never repeated and each task body is closed exactly once
// before the next sleep or return.
func TestExecutePolicyPollRetries(t *testing.T) {
	t.Parallel()

	const (
		aliasPath   = "/_alias/.enrich-policy-a"
		executePath = "/_enrich/policy/policy-a/_execute"
		taskPath    = "/_tasks/node:1"
	)
	absent := aliasStep{status: http.StatusNotFound, body: aliasMissingBody}
	present := aliasStep{status: http.StatusOK, body: enrichAliasBody(".enrich-policy-a", ".enrich-policy-a-2")}
	running := taskStep{status: http.StatusOK, body: taskRunningBody}
	completed := taskStep{status: http.StatusOK, body: taskCompletedBody}
	gone := taskStep{status: http.StatusNotFound, body: enrichTaskNotFoundBody}
	unavailable := taskStep{status: http.StatusServiceUnavailable, body: `{"error":"unavailable"}`}
	timeoutErr := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	readErr := errors.New("injected body read failure")
	closeErr := errors.New("injected body close failure")

	// The poll diagnostics log es_error, the sanitized Rejection text, never
	// the raw body: a fully read non-JSON body yields the fixed not-JSON
	// detail, an injected read error the fixed read-failed detail, and a JSON
	// ES error its type/reason.
	const (
		notJSON    = "ES error detail unavailable (response is not a single JSON value)"
		readFailed = "ES error detail unavailable (response body read failed)"
	)
	es := func(status int, detail string) string {
		return fmt.Sprintf("enrich task \"node:1\" rejected with HTTP %d: %s", status, detail)
	}
	busy := taskStep{status: http.StatusServiceUnavailable, body: `{"error":{"type":"x","reason":"busy"}}`}

	type warn struct {
		attempt int
		status  int
		es      string // exact expected es_error
		field   string // optional extra diagnostic field that must be present
	}
	type row struct {
		name          string
		tasks         []taskStep
		aliases       []aliasStep // nil means one absent pre-kickoff read
		cancelOnSleep int         // 1-based Sleep call that cancels; 0 never
		want          bool
		wantPolls     int // task attempts, transport failures included
		wantSleeps    int
		wantWarns     []warn
		wantFinalMsg  string // the terminal Error message, "" for none
		wantFinalCode int    // its status_code, 0 for none
		wantFinalES   string
		wantFinalTry  int    // its attempt field, 0 for absent
		wantFinalErr  string // substring of its error field
		wantVerdict   string // alias verification message for task-404 rows
	}
	var tests []row
	for _, status := range []int{http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		fail := taskStep{status: status, body: fmt.Sprintf("<html>%d</html>", status)}
		tests = append(tests, row{
			name:       fmt.Sprintf("%d twice then completed", status),
			tasks:      []taskStep{fail, fail, completed},
			want:       true,
			wantPolls:  3,
			wantSleeps: 3,
			wantWarns:  []warn{{1, status, es(status, notJSON), ""}, {2, status, es(status, notJSON), ""}},
		})
	}
	tests = append(tests,
		row{
			name:          "third consecutive 503 fails without a fourth poll",
			tasks:         []taskStep{unavailable, unavailable, unavailable},
			wantPolls:     3,
			wantSleeps:    3,
			wantWarns:     []warn{{1, 503, es(503, "unavailable"), ""}, {2, 503, es(503, "unavailable"), ""}},
			wantFinalMsg:  msgTaskPollError,
			wantFinalCode: 503,
			wantFinalES:   es(503, "unavailable"),
			wantFinalTry:  3,
		},
		row{
			name:          "mixed transient statuses report the last",
			tasks:         []taskStep{{status: 429, body: "a"}, {status: 502, body: "b"}, {status: 504, body: "c"}},
			wantPolls:     3,
			wantSleeps:    3,
			wantWarns:     []warn{{1, 429, es(429, notJSON), ""}, {2, 502, es(502, notJSON), ""}},
			wantFinalMsg:  msgTaskPollError,
			wantFinalCode: 504,
			wantFinalES:   es(504, notJSON),
			wantFinalTry:  3,
		},
		row{
			name:       "a decoded poll resets the streak",
			tasks:      []taskStep{unavailable, running, unavailable, unavailable, completed},
			want:       true,
			wantPolls:  5,
			wantSleeps: 5,
			wantWarns:  []warn{{1, 503, es(503, "unavailable"), ""}, {1, 503, es(503, "unavailable"), ""}, {2, 503, es(503, "unavailable"), ""}},
		},
		row{
			name:       "read error body still retries by status",
			tasks:      []taskStep{{status: 503, body: "partial-503", readErr: readErr}, completed},
			want:       true,
			wantPolls:  2,
			wantSleeps: 2,
			wantWarns:  []warn{{1, 503, es(503, readFailed), "body_error"}},
		},
		row{
			name:       "close error body still retries by status",
			tasks:      []taskStep{{status: 503, body: "closing-503", closeErr: closeErr}, completed},
			want:       true,
			wantPolls:  2,
			wantSleeps: 2,
			wantWarns:  []warn{{1, 503, es(503, notJSON), "body_close_error"}},
		},
		row{
			name:          "a JSON type and reason stay visible in the retry warn",
			tasks:         []taskStep{busy, busy, busy},
			wantPolls:     3,
			wantSleeps:    3,
			wantWarns:     []warn{{1, 503, es(503, "x: busy"), ""}, {2, 503, es(503, "x: busy"), ""}},
			wantFinalMsg:  msgTaskPollError,
			wantFinalCode: 503,
			wantFinalES:   es(503, "x: busy"),
			wantFinalTry:  3,
		},
		row{
			name:          "400 fails on the first poll",
			tasks:         []taskStep{{status: 400, body: "bad"}},
			wantPolls:     1,
			wantSleeps:    1,
			wantFinalMsg:  msgTaskPollError,
			wantFinalCode: 400,
			wantFinalES:   es(400, notJSON),
		},
		row{
			name:          "500 fails on the first poll",
			tasks:         []taskStep{{status: 500, body: "boom"}},
			wantPolls:     1,
			wantSleeps:    1,
			wantFinalMsg:  msgTaskPollError,
			wantFinalCode: 500,
			wantFinalES:   es(500, notJSON),
		},
		row{
			name:         "malformed 2xx fails on the first poll",
			tasks:        []taskStep{{status: 200, body: "{"}},
			wantPolls:    1,
			wantSleeps:   1,
			wantFinalMsg: msgTaskParseFail,
		},
		row{
			name:         "transport timeout fails on the first poll",
			tasks:        []taskStep{{transport: timeoutErr}},
			wantPolls:    1,
			wantSleeps:   1,
			wantFinalMsg: msgTaskPollFail,
			wantFinalErr: "i/o timeout",
		},
		row{
			name:          "cancel in the retry sleep stops polling",
			tasks:         []taskStep{unavailable},
			cancelOnSleep: 2,
			wantPolls:     1,
			wantSleeps:    2,
			wantWarns:     []warn{{1, 503, es(503, "unavailable"), ""}},
			wantFinalMsg:  msgPollCancelled,
		},
		row{
			name:        "task 404 after retries verifies a moved alias",
			tasks:       []taskStep{unavailable, unavailable, gone},
			aliases:     []aliasStep{absent, present},
			want:        true,
			wantPolls:   3,
			wantSleeps:  3,
			wantWarns:   []warn{{1, 503, es(503, "unavailable"), ""}, {2, 503, es(503, "unavailable"), ""}},
			wantVerdict: msgAliasVerified,
		},
		row{
			name:        "task 404 after retries fails an unmoved alias",
			tasks:       []taskStep{unavailable, unavailable, gone},
			aliases:     []aliasStep{absent, absent},
			wantPolls:   3,
			wantSleeps:  3,
			wantWarns:   []warn{{1, 503, es(503, "unavailable"), ""}, {2, 503, es(503, "unavailable"), ""}},
			wantVerdict: msgAliasNotMoved,
		},
		row{
			name:        "task 404 after retries fails an unreadable alias",
			tasks:       []taskStep{unavailable, unavailable, gone},
			aliases:     []aliasStep{absent, {status: http.StatusInternalServerError, body: `{"error":{"type":"boom"}}`}},
			wantPolls:   3,
			wantSleeps:  3,
			wantWarns:   []warn{{1, 503, es(503, "unavailable"), ""}, {2, 503, es(503, "unavailable"), ""}},
			wantVerdict: msgAliasUnverified,
		},
	)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			aliases := tt.aliases
			if aliases == nil {
				aliases = []aliasStep{absent}
			}
			var served []taskStep // task steps that reach the stub, in order
			for _, step := range tt.tasks {
				if step.transport == nil {
					served = append(served, step)
				}
			}

			stub := esstub.New(t)
			var aliasCalls, taskCalls atomic.Int32
			stub.Handle("GET "+aliasPath, func(w http.ResponseWriter, _ *http.Request) {
				i := int(aliasCalls.Add(1)) - 1
				if i >= len(aliases) {
					t.Errorf("alias read %d, want at most %d", i+1, len(aliases))
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.WriteHeader(aliases[i].status)
				_, _ = io.WriteString(w, aliases[i].body)
			})
			stub.Handle("PUT "+executePath, esstub.JSON(http.StatusOK, `{"task":"node:1"}`))
			stub.Handle("GET "+taskPath, func(w http.ResponseWriter, _ *http.Request) {
				i := int(taskCalls.Add(1)) - 1
				if i >= len(served) {
					t.Errorf("task poll %d reached the stub, want at most %d", i+1, len(served))
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.WriteHeader(served[i].status)
				_, _ = io.WriteString(w, served[i].body)
			})

			base := &http.Transport{}
			t.Cleanup(base.CloseIdleConnections)
			var (
				mu       sync.Mutex
				attempts int
				bodies   []*trackedBody
			)
			wrapper := enrichRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != taskPath {
					return base.RoundTrip(r)
				}
				mu.Lock()
				i := attempts
				attempts++
				mu.Unlock()
				if i >= len(tt.tasks) {
					return nil, fmt.Errorf("task attempt %d beyond the scripted %d", i+1, len(tt.tasks))
				}
				step := tt.tasks[i]
				if step.transport != nil {
					return nil, step.transport
				}
				res, err := base.RoundTrip(r)
				if err != nil {
					return nil, err
				}
				body := &trackedBody{ReadCloser: res.Body, readErr: step.readErr, closeErr: step.closeErr}
				mu.Lock()
				bodies = append(bodies, body)
				mu.Unlock()
				res.Body = body
				return res, nil
			})
			es, err := elasticsearch.NewClient(elasticsearch.Config{
				Addresses:    []string{stub.URL()},
				Transport:    wrapper,
				DisableRetry: true,
				MaxRetries:   0,
			})
			if err != nil {
				t.Fatalf("new elasticsearch client: %v", err)
			}

			// allClosedOnce reports whether every task body seen so far was
			// closed exactly once.
			allClosedOnce := func() bool {
				mu.Lock()
				defer mu.Unlock()
				for _, b := range bodies {
					if b.closes.Load() != 1 {
						return false
					}
				}
				return true
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var buf bytes.Buffer
			logger := zerolog.New(&buf).Level(zerolog.DebugLevel)
			ctx = runlog.WithLogger(ctx, &logger)

			sleeps := 0
			cfg := fixedEnrichSeams()
			cfg.Sleep = func(c context.Context, d time.Duration) error {
				sleeps++
				if !allClosedOnce() {
					t.Errorf("sleep %d: a task body was not closed exactly once before the next poll", sleeps)
				}
				if d != progress.PollInterval(sleeps) {
					t.Errorf("sleep %d = %v, want the shared cadence %v", sleeps, d, progress.PollInterval(sleeps))
				}
				if sleeps == tt.cancelOnSleep {
					cancel()
				}
				return c.Err()
			}

			got := executePolicy(ctx, es, "policy-a", cfg).succeeded()
			if got != tt.want {
				t.Fatalf("executePolicy = %v, want %v\nlogs:\n%s", got, tt.want, buf.String())
			}
			if !allClosedOnce() {
				t.Fatal("a task body was not closed exactly once before return")
			}
			mu.Lock()
			gotAttempts, tracked := attempts, len(bodies)
			mu.Unlock()
			if tracked != len(served) {
				t.Fatalf("tracked task bodies = %d, want one per served reply (%d)", tracked, len(served))
			}
			if gotAttempts != tt.wantPolls || sleeps != tt.wantSleeps {
				t.Fatalf("task polls/sleeps = %d/%d, want %d/%d\nlogs:\n%s", gotAttempts, sleeps, tt.wantPolls, tt.wantSleeps, buf.String())
			}
			kickoffs, aliasReads := 0, 0
			for _, r := range stub.Requests() {
				switch r.Path {
				case executePath:
					kickoffs++
				case aliasPath:
					aliasReads++
				}
			}
			wantAliasReads := 1
			if tt.wantVerdict != "" {
				wantAliasReads = 2
			}
			if kickoffs != 1 || aliasReads != wantAliasReads {
				t.Fatalf("kickoffs/alias reads = %d/%d, want 1/%d", kickoffs, aliasReads, wantAliasReads)
			}

			lines := enrichLogLines(t, &buf)
			warns := enrichLogsWith(lines, msgTaskPollRetry)
			if len(warns) != len(tt.wantWarns) {
				t.Fatalf("retry warns = %d, want %d\nlogs:\n%s", len(warns), len(tt.wantWarns), buf.String())
			}
			for i, w := range tt.wantWarns {
				line := warns[i]
				if line["level"] != "warn" || line["policy"] != "policy-a" || line["task"] != "node:1" ||
					line["status_code"] != float64(w.status) || line["attempt"] != float64(w.attempt) {
					t.Fatalf("retry warn %d = %v, want policy-a/node:1, status %d, attempt %d", i, line, w.status, w.attempt)
				}
				if _, has := line["body"]; has {
					t.Fatalf("retry warn %d = %v, must not log the raw body", i, line)
				}
				if got, _ := line["es_error"].(string); got != w.es {
					t.Fatalf("retry warn %d es_error = %q, want %q", i, got, w.es)
				}
				if w.field != "" {
					if text, _ := line[w.field].(string); text == "" {
						t.Fatalf("retry warn %d = %v, want a %s diagnostic", i, line, w.field)
					}
				}
			}

			pollErrors := enrichLogsWith(lines, msgTaskPollError)
			if tt.wantFinalMsg != msgTaskPollError && len(pollErrors) != 0 {
				t.Fatalf("unexpected %q lines: %v", msgTaskPollError, pollErrors)
			}
			if tt.wantFinalMsg != "" {
				finals := enrichLogsWith(lines, tt.wantFinalMsg)
				if len(finals) != 1 {
					t.Fatalf("%q lines = %d, want 1\nlogs:\n%s", tt.wantFinalMsg, len(finals), buf.String())
				}
				line := finals[0]
				if tt.wantFinalMsg != msgPollCancelled && line["level"] != "error" {
					t.Fatalf("terminal line = %v, want error level", line)
				}
				if tt.wantFinalCode != 0 && line["status_code"] != float64(tt.wantFinalCode) {
					t.Fatalf("terminal line = %v, want status_code %d", line, tt.wantFinalCode)
				}
				if _, has := line["body"]; has {
					t.Fatalf("terminal line = %v, must not log the raw body", line)
				}
				if got, _ := line["es_error"].(string); tt.wantFinalES != "" && got != tt.wantFinalES {
					t.Fatalf("terminal line es_error = %q, want %q", got, tt.wantFinalES)
				}
				attempt, hasAttempt := line["attempt"]
				if tt.wantFinalTry == 0 && hasAttempt {
					t.Fatalf("terminal line = %v, a non-retryable failure must not log an attempt", line)
				}
				if tt.wantFinalTry != 0 && attempt != float64(tt.wantFinalTry) {
					t.Fatalf("terminal line = %v, want attempt %d", line, tt.wantFinalTry)
				}
				if text, _ := line["error"].(string); !strings.Contains(text, tt.wantFinalErr) {
					t.Fatalf("terminal line error = %q, want it to contain %q", text, tt.wantFinalErr)
				}
			}
			if tt.wantVerdict != "" && len(enrichLogsWith(lines, tt.wantVerdict)) != 1 {
				t.Fatalf("want one %q line\nlogs:\n%s", tt.wantVerdict, buf.String())
			}
			if succeeded := len(enrichLogsWith(lines, msgEnrichSucceeded)); succeeded != map[bool]int{true: 1, false: 0}[tt.want] {
				t.Fatalf("success lines = %d, want them only for a successful policy\nlogs:\n%s", succeeded, buf.String())
			}
		})
	}
}

// TestExecutePollRetriesPreserveContinuation drives Execute over two policies:
// policy-a exhausts its three-failure budget and fails, then policy-b, with
// its own budget, recovers from two 503s. The run continues past the failure,
// counts one of each, emits one Final per policy and returns no error, and
// neither kickoff is repeated (FEAT-WAITRETRY-01).
func TestExecutePollRetriesPreserveContinuation(t *testing.T) {
	t.Parallel()

	stub := esstub.New(t)
	policies := esstub.JSON(http.StatusOK, `{"policies":[{"config":{"match":{"name":"policy-a"}}},{"config":{"match":{"name":"policy-b"}}}]}`)
	stub.Handle("GET /_enrich/policy", policies)
	stub.Handle("GET /_enrich/policy/{$}", policies)
	for _, name := range []string{"policy-a", "policy-b"} {
		stub.Handle("GET /_alias/.enrich-"+name, esstub.JSON(http.StatusNotFound, aliasMissingBody))
	}
	stub.Handle("PUT /_enrich/policy/policy-a/_execute", esstub.JSON(http.StatusOK, `{"task":"node:a"}`))
	stub.Handle("PUT /_enrich/policy/policy-b/_execute", esstub.JSON(http.StatusOK, `{"task":"node:b"}`))
	stub.Handle("GET /_tasks/node:a", esstub.JSON(http.StatusServiceUnavailable, `{"error":"a unavailable"}`))
	var bCalls atomic.Int32
	stub.Handle("GET /_tasks/node:b", func(w http.ResponseWriter, _ *http.Request) {
		if bCalls.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":"b unavailable"}`)
			return
		}
		_, _ = io.WriteString(w, taskCompletedBody)
	})

	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	ctx := runlog.WithLogger(t.Context(), &logger)
	cfg := fixedEnrichSeams()
	var finals []progress.Snapshot
	cfg.OnProgress = func(s progress.Snapshot) {
		if s.Final {
			finals = append(finals, s)
		}
	}

	selection := &Selection{Enabled: true, Raw: "policy-a,policy-b"}
	summary, err := Execute(ctx, stub.MustClient(t), selection, nil, cfg)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !slices.Equal(summary.Selected, []string{"policy-a", "policy-b"}) || summary.Failed != 1 || summary.Succeeded != 1 || len(summary.Missing) != 0 {
		t.Fatalf("summary = %+v, want policy-a failed and policy-b succeeded", summary)
	}

	counts := map[string]int{}
	for _, r := range stub.Requests() {
		counts[r.Method+" "+r.Path]++
	}
	for _, want := range []struct {
		route string
		n     int
	}{
		{"GET /_tasks/node:a", 3},
		{"GET /_tasks/node:b", 3},
		{"PUT /_enrich/policy/policy-a/_execute", 1},
		{"PUT /_enrich/policy/policy-b/_execute", 1},
	} {
		if counts[want.route] != want.n {
			t.Fatalf("%s requests = %d, want %d (all: %v)", want.route, counts[want.route], want.n, counts)
		}
	}

	if len(finals) != 2 || finals[0].Resource != "policy-a" || finals[0].Err == nil || finals[1].Resource != "policy-b" || finals[1].Err != nil {
		t.Fatalf("finals = %+v, want policy-a failed then policy-b succeeded", finals)
	}
	lines := enrichLogLines(t, &buf)
	if n := len(enrichLogsWith(lines, msgTaskPollRetry)); n != 4 {
		t.Fatalf("retry warns = %d, want two per policy\nlogs:\n%s", n, buf.String())
	}
	if n := len(enrichLogsWith(lines, msgTaskPollError)); n != 1 {
		t.Fatalf("poll error lines = %d, want one for policy-a\nlogs:\n%s", n, buf.String())
	}
}

// TestExecutePolicyExplicitOutcomesSkipAliasCheck guards the other half of
// FIX-ENRICH404-01: a synchronous status or a completed task status is
// authoritative, so it keeps its result even when the pre-kickoff alias read
// fails (403), and no after-read is made. Exactly one alias read happens, and
// it precedes the kickoff.
func TestExecutePolicyExplicitOutcomesSkipAliasCheck(t *testing.T) {
	t.Parallel()

	const aliasPath = "/_alias/.enrich-policy-a"
	outcomes := []struct {
		name        string
		executeBody string
		taskBody    string // "" for a synchronous completion
		want        bool
	}{
		{name: "synchronous COMPLETE", executeBody: `{"status":{"phase":"COMPLETE"}}`, want: true},
		{name: "synchronous FAILED", executeBody: `{"status":{"phase":"FAILED"}}`, want: false},
		{name: "completed task COMPLETE", executeBody: `{"task":"node:1"}`, taskBody: `{"completed":true,"task":{},"response":{"status":{"phase":"COMPLETE"}}}`, want: true},
		{name: "completed task FAILED", executeBody: `{"task":"node:1"}`, taskBody: `{"completed":true,"task":{},"response":{"status":{"phase":"FAILED"}}}`, want: false},
	}
	befores := []struct {
		name   string
		status int
		body   string
	}{
		{name: "readable before", status: http.StatusOK, body: enrichAliasBody(".enrich-policy-a", ".enrich-policy-a-1")},
		{name: "before 403", status: http.StatusForbidden, body: `{"error":{"type":"security_exception"},"status":403}`},
	}
	for _, outcome := range outcomes {
		for _, before := range befores {
			t.Run(outcome.name+"/"+before.name, func(t *testing.T) {
				t.Parallel()

				stub := esstub.New(t)
				stub.Handle("GET "+aliasPath, esstub.JSON(before.status, before.body))
				execute := esstub.JSON(http.StatusOK, outcome.executeBody)
				stub.Handle("PUT /_enrich/policy/policy-a/_execute", execute)
				stub.Handle("POST /_enrich/policy/policy-a/_execute", execute)
				if outcome.taskBody != "" {
					stub.Handle("GET /_tasks/node:1", esstub.JSON(http.StatusOK, outcome.taskBody))
				}

				var buf bytes.Buffer
				logger := zerolog.New(&buf).Level(zerolog.DebugLevel)
				ctx := runlog.WithLogger(t.Context(), &logger)

				if got := executePolicy(ctx, stub.MustClient(t), "policy-a", fixedEnrichSeams()).succeeded(); got != outcome.want {
					t.Fatalf("executePolicy = %v, want %v\nlogs:\n%s", got, outcome.want, buf.String())
				}

				var aliasReads []int
				requests := stub.Requests()
				for i, r := range requests {
					if r.Path == aliasPath {
						aliasReads = append(aliasReads, i)
					}
				}
				if len(aliasReads) != 1 || aliasReads[0] != 0 || len(requests) < 2 || !strings.HasSuffix(requests[1].Path, "/_execute") {
					t.Fatalf("requests = %+v, want exactly one alias read, before the kickoff", requests)
				}

				lines := enrichLogLines(t, &buf)
				if n := len(enrichLogsWith(lines, msgAliasUnverified)) + len(enrichLogsWith(lines, msgAliasNotMoved)) + len(enrichLogsWith(lines, msgAliasVerified)); n != 0 {
					t.Fatalf("an explicit status must not run alias verification, got %d verification lines:\n%s", n, buf.String())
				}
				if got := len(enrichLogsWith(lines, msgEnrichSucceeded)) == 1; got != outcome.want {
					t.Fatalf("success line logged = %v, want %v", got, outcome.want)
				}
			})
		}
	}
}

// TestExecutePolicyTaskErrorIsRedacted pins that a task error reported inside
// a 2xx task document is logged as a sanitized, bounded string es_error (it
// replaced the raw task_error JSON object), and that the policy still fails
// after a single poll. All secrets are synthetic.
func TestExecutePolicyTaskErrorIsRedacted(t *testing.T) {
	t.Parallel()

	const secret = `"password":"hunter2" at https://user:pw@host/x`
	long := strings.Repeat("é", 3000)
	errJSON := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	tests := []struct {
		name      string
		errField  string
		completed bool
		want      string // exact es_error; empty means check shape only
		wantCut   bool
	}{
		{"caused_by chain", errJSON(map[string]any{"type": "t", "reason": "r", "caused_by": map[string]string{"type": "u", "reason": "s"}}), true, "t: r; u: s", false},
		{"string error", `"plain failure"`, true, "plain failure", false},
		{"number error", `42`, true, "ES error detail unavailable (no recognizable error field)", false},
		{"array error", `["x"]`, true, "ES error detail unavailable (no recognizable error field)", false},
		{"secret reason", errJSON(map[string]any{"type": "t", "reason": secret}), true, "", false},
		{"error with completed false", errJSON(map[string]any{"type": "t", "reason": "r"}), false, "t: r", false},
		{"long multibyte reason and secret type with ignored metadata", errJSON(map[string]any{"type": "password=hunter2", "reason": long, "header": map[string]string{"password": "hunter2"}, "stack_trace": secret}), true, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var polls atomic.Int32
			stub := esstub.New(t)
			stub.Handle("GET /_alias/.enrich-policy-a", esstub.JSON(http.StatusNotFound, aliasMissingBody))
			execute := esstub.JSON(http.StatusOK, `{"task":"node:1"}`)
			stub.Handle("PUT /_enrich/policy/policy-a/_execute", execute)
			stub.Handle("POST /_enrich/policy/policy-a/_execute", execute)
			stub.Handle("GET /_tasks/node:1", func(w http.ResponseWriter, _ *http.Request) {
				polls.Add(1)
				_, _ = fmt.Fprintf(w, `{"completed":%t,"error":%s}`, tt.completed, tt.errField)
			})
			logger, buf := logbuf.New()
			ctx := runlog.WithLogger(t.Context(), logger)

			if executePolicy(ctx, stub.MustClient(t), "policy-a", fixedEnrichSeams()).succeeded() {
				t.Fatal("executePolicy = true, want false")
			}

			if polls.Load() != 1 {
				t.Fatalf("polls = %d, want 1", polls.Load())
			}
			lines := enrichLogsWith(buf.Lines(t), "Enrich policy execution failed")
			if len(lines) != 1 {
				t.Fatalf("execution-failed lines = %d, want 1\n%s", len(lines), buf.String())
			}
			if _, has := lines[0]["task_error"]; has {
				t.Fatalf("line still carries task_error: %v", lines[0])
			}
			got, ok := lines[0]["es_error"].(string)
			if !ok {
				t.Fatalf("es_error is %T, want string: %v", lines[0]["es_error"], lines[0])
			}
			if tt.want != "" && got != tt.want {
				t.Fatalf("es_error = %q, want %q", got, tt.want)
			}
			if len(got) > esresponse.MaxPublicMessageBytes || !utf8.ValidString(got) {
				t.Fatalf("es_error is %d bytes, want <= %d valid UTF-8", len(got), esresponse.MaxPublicMessageBytes)
			}
			if tt.wantCut && !strings.HasSuffix(got, " [truncated]") {
				t.Fatalf("es_error not marked truncated")
			}
			for _, bad := range []string{"hunter2", "user:pw"} {
				if strings.Contains(buf.String(), bad) {
					t.Fatalf("%q leaked in log: %s", bad, buf.String())
				}
			}
		})
	}

	// A null or absent task error is not a failure: no execution-failed line.
	for _, field := range []string{`"error":null,`, ``} {
		t.Run("control "+field, func(t *testing.T) {
			t.Parallel()
			stub := esstub.New(t)
			stub.Handle("GET /_alias/.enrich-policy-a", esstub.JSON(http.StatusNotFound, aliasMissingBody))
			execute := esstub.JSON(http.StatusOK, `{"task":"node:1"}`)
			stub.Handle("PUT /_enrich/policy/policy-a/_execute", execute)
			stub.Handle("POST /_enrich/policy/policy-a/_execute", execute)
			stub.Handle("GET /_tasks/node:1", esstub.JSON(http.StatusOK, `{`+field+`"completed":true,"response":{"status":"COMPLETED"}}`))
			logger, buf := logbuf.New()
			ctx := runlog.WithLogger(t.Context(), logger)

			executePolicy(ctx, stub.MustClient(t), "policy-a", fixedEnrichSeams())

			if n := len(enrichLogsWith(buf.Lines(t), "Enrich policy execution failed")); n != 0 {
				t.Fatalf("got %d execution-failed lines for a null/absent error: %s", n, buf.String())
			}
		})
	}
}

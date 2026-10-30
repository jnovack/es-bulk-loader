package enrich

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
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
		result <- executePolicy(ctx, es, "policy-a", cfg)
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

// TestResolveTargetsAllPolicies verifies behavior for the related scenario.
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

// TestResolveTargetsWarnsForMissingPolicies verifies behavior for the related scenario.
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

// TestResolveTargetsUsesDeclaredPoliciesWhenRequestedAll verifies behavior for the related scenario.
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

			got := executePolicy(ctx, es, tt.policy, fixedEnrichSeams())
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

				if got := executePolicy(ctx, stub.MustClient(t), "policy-a", fixedEnrichSeams()); got != outcome.want {
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

package loader

// Phase 2 (loader-observability) guards at the public boundary: Run exposes
// per-policy EnrichOutcomes whose Err (and the enrich Final ProgressEvent.Err)
// reach the real cause and an *ElasticsearchError, success and missing keep a
// nil Err, and the wrapper is invisible to text, Unwrap and non-matching
// targets. Serial: they use installFakeClock.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

const taskErrorReply = `{"completed":true,"error":{"type":"exception","reason":"boom","caused_by":{"type":"io_exception","reason":"disk"}}}`

func enrichOutcomeOptions(raw string) func(*Options) {
	return func(o *Options) { o.Enrich = EnrichOptions{Enabled: true, Raw: raw} }
}

func finalFor(t *testing.T, run progressRun, policy string) ProgressEvent {
	t.Helper()
	for _, e := range run.eventsFor(ProgressStageEnrich) {
		if e.Final && e.Resource == policy {
			return e
		}
	}
	t.Fatalf("no enrich Final event for %q in %+v", policy, run.events)
	return ProgressEvent{}
}

// TestRunEnrichOutcomeCarriesCauseAndElasticsearchError: a task error ends the
// policy as failed without failing Run; both the outcome Err and the Final
// ProgressEvent.Err reach the cause, its text and an ElasticsearchError.
func TestRunEnrichOutcomeCarriesCauseAndElasticsearchError(t *testing.T) {
	installFakeClock(t, time.Second)
	server, ops := newProgressStub(t, nil, withEnrichTask(stubReply{http.StatusOK, taskErrorReply}))

	nop := zerolog.Nop()
	run := runProgress(t, server, ops, zerolog.InfoLevel, withEnrich, func(o *Options) { o.Logger = &nop })

	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}
	if run.result.EnrichFailed != 1 || run.result.EnrichSucceeded != 0 || len(run.result.EnrichOutcomes) != 1 {
		t.Fatalf("result = %+v, want one failed outcome", run.result)
	}
	outcome := run.result.EnrichOutcomes[0]
	const wantCause = `enrich task "node1:1" failed: exception: boom; io_exception: disk`
	if outcome.Policy != "policy-a" || outcome.Status != EnrichStatusFailed || outcome.Err == nil || outcome.Err.Error() != wantCause {
		t.Fatalf("outcome = %+v, want policy-a failed with %q", outcome, wantCause)
	}
	final := finalFor(t, run, "policy-a")
	if want := `enrich policy "policy-a" failed: ` + wantCause; final.Err == nil || final.Err.Error() != want {
		t.Fatalf("Final Err = %v, want %q", final.Err, want)
	}
	for name, err := range map[string]error{"outcome": outcome.Err, "final": final.Err} {
		var esErr *ElasticsearchError
		if !errors.As(err, &esErr) {
			t.Fatalf("%s Err %v: errors.As found no *ElasticsearchError", name, err)
		}
		if esErr.Resource != "enrich task" || esErr.Name != "node1:1" || esErr.StatusCode != 0 ||
			len(esErr.Causes) != 2 || esErr.Causes[0] != (ElasticsearchCause{Type: "exception", Reason: "boom"}) ||
			esErr.Causes[1] != (ElasticsearchCause{Type: "io_exception", Reason: "disk"}) {
			t.Fatalf("%s ElasticsearchError = %+v", name, esErr)
		}
	}
	// Run's own error path is unchanged: a failed policy is not a RunError.
	var runErr *RunError
	if errors.As(run.err, &runErr) {
		t.Fatal("a failed enrich policy must not produce a RunError")
	}
}

// TestRunEnrichOutcomesSuccessMissingAndNilProgress: selected outcomes precede
// missing ones in request order, success and missing carry a nil Err, a
// missing policy gets no progress event, and a nil Progress still records
// every outcome and count.
func TestRunEnrichOutcomesSuccessMissingAndNilProgress(t *testing.T) {
	installFakeClock(t, time.Second)
	server, ops := newProgressStub(t, nil)

	nop := zerolog.Nop()
	run := runProgress(t, server, ops, zerolog.InfoLevel, enrichOutcomeOptions("ghost,policy-a"),
		func(o *Options) { o.Logger = &nop })

	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}
	want := []EnrichOutcome{
		{Policy: "policy-a", Status: EnrichStatusSucceeded},
		{Policy: "ghost", Status: EnrichStatusMissing},
	}
	if len(run.result.EnrichOutcomes) != len(want) {
		t.Fatalf("outcomes = %+v, want %+v", run.result.EnrichOutcomes, want)
	}
	for i, w := range want {
		if got := run.result.EnrichOutcomes[i]; got.Policy != w.Policy || got.Status != w.Status || got.Err != nil {
			t.Fatalf("outcome[%d] = %+v, want %+v with a nil Err", i, got, w)
		}
	}
	if got := run.result.EnrichMissing; len(got) != 1 || got[0] != "ghost" {
		t.Fatalf("EnrichMissing = %v, want [ghost]", got)
	}
	if run.result.EnrichSucceeded+run.result.EnrichFailed != 1 {
		t.Fatalf("succeeded+failed = %d+%d, want 1 selected outcome", run.result.EnrichSucceeded, run.result.EnrichFailed)
	}
	if e := finalFor(t, run, "policy-a"); e.Err != nil {
		t.Fatalf("succeeded Final Err = %v, want nil (not a typed-nil wrapper)", e.Err)
	}
	for _, e := range run.eventsFor(ProgressStageEnrich) {
		if e.Resource == "ghost" {
			t.Fatalf("missing policy got a progress event: %+v", e)
		}
	}

	// Same run with no Progress callback and a failing policy: outcomes and
	// counts must not depend on it.
	server2, ops2 := newProgressStub(t, nil, withEnrichTask(stubReply{http.StatusOK, taskErrorReply}))
	noProgress := runProgress(t, server2, ops2, zerolog.InfoLevel, withEnrich,
		func(o *Options) { o.Progress = nil; o.Logger = &nop })
	if noProgress.err != nil || noProgress.result.EnrichFailed != 1 || len(noProgress.result.EnrichOutcomes) != 1 ||
		noProgress.result.EnrichOutcomes[0].Status != EnrichStatusFailed || noProgress.result.EnrichOutcomes[0].Err == nil {
		t.Fatalf("nil Progress: err=%v result=%+v", noProgress.err, noProgress.result)
	}
}

// TestRunEnrichOutcomesEmptyWhenNotRequested: enrich off yields no outcomes.
func TestRunEnrichOutcomesEmptyWhenNotRequested(t *testing.T) {
	installFakeClock(t, time.Second)
	server, ops := newProgressStub(t, nil)

	nop := zerolog.Nop()
	run := runProgress(t, server, ops, zerolog.InfoLevel, func(o *Options) { o.Logger = &nop })

	if run.err != nil || run.result.EnrichOutcomes != nil {
		t.Fatalf("err=%v outcomes=%+v, want none", run.err, run.result.EnrichOutcomes)
	}
}

// TestRunEnrichCancelledOutcomeKeepsContextIdentity: a Run whose context ends
// while polling reports the policy as cancelled with errors.Is reaching
// context.Canceled through both the outcome and the Final event. Cancellation
// must not drop the remaining selected policies' outcomes or Final events.
func TestRunEnrichCancelledOutcomeKeepsContextIdentity(t *testing.T) {
	installFakeClock(t, time.Second)
	server, ops := newProgressStub(t, nil, withPolicies("policy-a", "policy-b"),
		withEnrichTask(stubReply{http.StatusOK, `{"completed":false,"task":{"status":{"phase":"RUNNING"}}}`}))
	ctx, cancel := context.WithCancel(t.Context())
	// Cancel at the first enrich poll wait (the only sleep in this Run), so
	// every earlier stage ran on a live context. installFakeClock restores
	// sleepWithContext in cleanup.
	sleepWithContext = func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}

	nop := zerolog.Nop()
	run := runProgressContext(t, ctx, server, ops, zerolog.InfoLevel, withEnrich, func(o *Options) { o.Logger = &nop })

	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}
	if len(run.result.EnrichOutcomes) != 2 || run.result.EnrichFailed != 2 || run.result.EnrichSucceeded != 0 {
		t.Fatalf("result = %+v, want both selected policies counted as failed", run.result)
	}
	finals := 0
	for _, event := range run.eventsFor(ProgressStageEnrich) {
		if event.Final {
			finals++
		}
	}
	if finals != 2 {
		t.Fatalf("got %d enrich Final events, want one per selected policy", finals)
	}
	for i, policy := range []string{"policy-a", "policy-b"} {
		outcome := run.result.EnrichOutcomes[i]
		if outcome.Policy != policy || outcome.Status != EnrichStatusCancelled ||
			!errors.Is(outcome.Err, context.Canceled) || !errors.Is(finalFor(t, run, policy).Err, context.Canceled) {
			t.Fatalf("%s: cancellation status or cause lost: %+v", policy, outcome)
		}
	}
}

// TestRunEnrichFailureClassesProjectStatusAndCause drives each failure class
// through Run and the Run-level stub, for two policies so continuation after a
// failure and one Final per selected policy are checked too. Every row asserts,
// on BOTH EnrichOutcome.Err and the Final ProgressEvent.Err, the presence (with
// StatusCode, Resource and Name) or absence of an *ElasticsearchError. Status,
// not errors.Is, is the classification, and "unsupported" counts in
// Result.EnrichFailed. Serial: installFakeClock replaces package clocks.
func TestRunEnrichFailureClassesProjectStatusAndCause(t *testing.T) {
	notFoundTask := stubReply{http.StatusNotFound, fixtureText(t, "enrich-task-not-found.json")}
	forbidden := stubReply{http.StatusForbidden, `{"error":{"type":"security_exception","reason":"no alias access"},"status":403}`}
	aliasBoth := func(replies func(policy string) []stubReply) []progressStubOption {
		var opts []progressStubOption
		for _, p := range []string{"policy-a", "policy-b"} {
			opts = append(opts, withEnrichAliasReplies(p, replies(p)...))
		}
		return opts
	}
	withTwo := func(opts ...progressStubOption) []progressStubOption {
		return append([]progressStubOption{withPolicies("policy-a", "policy-b")}, opts...)
	}
	type wantES struct {
		status         int
		resource, name string // name may hold {policy}
	}
	tests := []struct {
		name            string
		opts            []progressStubOption
		wantStatus      EnrichStatus
		wantES          *wantES // nil: errors.As must find no ElasticsearchError
		wantPolicyOps   string  // exact request order for each policy
		allowTaskReplay bool    // net/http may replay a GET after a reused connection drops
	}{
		{
			name:          "kickoff rejection 500",
			wantPolicyOps: "enrich.alias,enrich.execute",
			opts:          withTwo(withExecuteReply(http.StatusInternalServerError, `{"error":{"type":"exception","reason":"kick"},"status":500}`)),
			wantStatus:    EnrichStatusFailed,
			wantES:        &wantES{http.StatusInternalServerError, "enrich policy", "{policy}"},
		},
		{
			name:          "kickoff generic 404 is unsupported",
			wantPolicyOps: "enrich.alias,enrich.execute",
			opts:          withTwo(withExecuteReply(http.StatusNotFound, `{"status":404}`)),
			wantStatus:    EnrichStatusUnsupported,
			wantES:        &wantES{http.StatusNotFound, "enrich API", ""},
		},
		{
			name:          "kickoff transport failure",
			wantPolicyOps: "enrich.alias,enrich.execute",
			opts:          withTwo(withExecuteDrop()),
			wantStatus:    EnrichStatusFailed,
		},
		{
			name:          "kickoff decode error",
			wantPolicyOps: "enrich.alias,enrich.execute",
			opts:          withTwo(withExecuteBody(`{"task":`)),
			wantStatus:    EnrichStatusFailed,
		},
		{
			name:          "synchronous FAILED phase",
			wantPolicyOps: "enrich.alias,enrich.execute",
			opts:          withTwo(withExecuteBody(`{"status":{"phase":"FAILED"}}`)),
			wantStatus:    EnrichStatusFailed,
		},
		{
			// Elasticsearch's CANCELLED is deliberately failed: cancelled means
			// this Run's context ended.
			name:          "asynchronous CANCELLED phase stays failed",
			wantPolicyOps: "enrich.alias,enrich.execute,enrich.task",
			opts:          withTwo(withEnrichTask(stubReply{http.StatusOK, `{"completed":true,"response":{"status":{"phase":"CANCELLED"}}}`})),
			wantStatus:    EnrichStatusFailed,
		},
		{
			name:          "terminal poll rejection 403",
			wantPolicyOps: "enrich.alias,enrich.execute,enrich.task",
			opts:          withTwo(withEnrichTask(forbidden)),
			wantStatus:    EnrichStatusFailed,
			wantES:        &wantES{http.StatusForbidden, "enrich task", progressTaskID},
		},
		{
			name:          "poll retry exhaustion 503",
			wantPolicyOps: "enrich.alias,enrich.execute,enrich.task,enrich.task,enrich.task",
			opts:          withTwo(withEnrichTask(stubReply{http.StatusServiceUnavailable, `{"error":{"type":"unavailable","reason":"later"}}`})),
			wantStatus:    EnrichStatusFailed,
			wantES:        &wantES{http.StatusServiceUnavailable, "enrich task", progressTaskID},
		},
		{
			name:          "poll decode error",
			wantPolicyOps: "enrich.alias,enrich.execute,enrich.task",
			opts:          withTwo(withEnrichTask(stubReply{http.StatusOK, `{"completed":`})),
			wantStatus:    EnrichStatusFailed,
		},
		{
			name:            "poll transport failure",
			allowTaskReplay: true,
			wantPolicyOps:   "enrich.alias,enrich.execute,enrich.task",
			opts:            withTwo(withEnrichTask(stubReply{http.StatusOK, `{}`}), withTaskDrop()),
			wantStatus:      EnrichStatusFailed,
		},
		{
			name:          "task error inside a successful poll",
			wantPolicyOps: "enrich.alias,enrich.execute,enrich.task",
			opts:          withTwo(withEnrichTask(stubReply{http.StatusOK, taskErrorReply})),
			wantStatus:    EnrichStatusFailed,
			wantES:        &wantES{0, "enrich task", progressTaskID},
		},
		{
			name:          "task 404 then alias read rejected after",
			wantPolicyOps: "enrich.alias,enrich.execute,enrich.task,enrich.alias",
			opts: withTwo(append(aliasBoth(func(p string) []stubReply {
				return []stubReply{enrichAliasMissing(p), forbidden}
			}), withEnrichTask(notFoundTask))...),
			wantStatus: EnrichStatusFailed,
			wantES:     &wantES{http.StatusForbidden, "alias", ".enrich-{policy}"},
		},
		{
			name:          "task 404 then alias read rejected before",
			wantPolicyOps: "enrich.alias,enrich.execute,enrich.task,enrich.alias",
			opts: withTwo(append(aliasBoth(func(p string) []stubReply {
				return []stubReply{forbidden, enrichAliasAt(p, ".enrich-"+p+"-2")}
			}), withEnrichTask(notFoundTask))...),
			wantStatus: EnrichStatusFailed,
			wantES:     &wantES{http.StatusForbidden, "alias", ".enrich-{policy}"},
		},
		{
			name:          "task 404 and the alias did not move",
			wantPolicyOps: "enrich.alias,enrich.execute,enrich.task,enrich.alias",
			opts: withTwo(append(aliasBoth(func(p string) []stubReply {
				return []stubReply{enrichAliasAt(p, ".enrich-"+p+"-1")}
			}), withEnrichTask(notFoundTask))...),
			wantStatus: EnrichStatusFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installFakeClock(t, time.Second)
			server, ops := newProgressStub(t, nil, tt.opts...)

			// A Nop logger proves the contract needs no log parsing.
			nop := zerolog.Nop()
			run := runProgress(t, server, ops, zerolog.InfoLevel, withEnrich, func(o *Options) { o.Logger = &nop })
			if run.err != nil {
				t.Fatalf("Run: %v, want nil: a failed policy does not end the Run", run.err)
			}
			wantOps := "index.create,bulk,index.refresh,enrich.list," + tt.wantPolicyOps + "," + tt.wantPolicyOps
			gotOps := strings.Join(run.ops, ",")
			if tt.allowTaskReplay {
				// The HTTP transport can replay an idempotent GET once when a
				// pooled connection drops before headers. This is below enrich's
				// poll loop; normalize only that adjacent replay in this row.
				gotOps = strings.ReplaceAll(gotOps, "enrich.task,enrich.task", "enrich.task")
			}
			if got := gotOps; got != wantOps {
				t.Fatalf("request order = %s, want %s", got, wantOps)
			}

			selected := []string{"policy-a", "policy-b"}
			if got := len(run.result.EnrichOutcomes); got != len(selected) {
				t.Fatalf("outcomes = %+v, want %d", run.result.EnrichOutcomes, len(selected))
			}
			if run.result.EnrichSucceeded != 0 || run.result.EnrichFailed != len(selected) ||
				run.result.EnrichSucceeded+run.result.EnrichFailed != len(run.result.EnrichOutcomes) {
				t.Fatalf("succeeded+failed = %d+%d, want 0+%d (unsupported counts as failed)",
					run.result.EnrichSucceeded, run.result.EnrichFailed, len(selected))
			}
			finals := 0
			for _, e := range run.eventsFor(ProgressStageEnrich) {
				if e.Final {
					finals++
				}
			}
			if finals != len(selected) {
				t.Fatalf("enrich Final events = %d, want one per selected policy (%d)", finals, len(selected))
			}

			for i, policy := range selected {
				outcome := run.result.EnrichOutcomes[i]
				if outcome.Policy != policy || outcome.Status != tt.wantStatus || outcome.Err == nil {
					t.Fatalf("outcome[%d] = %+v, want %s %s with a non-nil Err", i, outcome, policy, tt.wantStatus)
				}
				final := finalFor(t, run, policy)
				if final.Err == nil {
					t.Fatalf("Final for %s has a nil Err", policy)
				}
				if want := fmt.Sprintf("enrich policy %q failed: %s", policy, outcome.Err.Error()); final.Err.Error() != want {
					t.Fatalf("Final Err = %q, want %q (the outcome cause under the unchanged prefix)", final.Err.Error(), want)
				}
				for where, err := range map[string]error{"outcome": outcome.Err, "final": final.Err} {
					var esErr *ElasticsearchError
					found := errors.As(err, &esErr)
					if tt.wantES == nil {
						if found {
							t.Fatalf("%s %s Err %v: unexpected %+v", policy, where, err, esErr)
						}
						continue
					}
					wantName := strings.ReplaceAll(tt.wantES.name, "{policy}", policy)
					if !found || esErr.StatusCode != tt.wantES.status || esErr.Resource != tt.wantES.resource || esErr.Name != wantName {
						t.Fatalf("%s %s Err %v: ElasticsearchError = %+v (found %v), want status %d resource %q name %q",
							policy, where, err, esErr, found, tt.wantES.status, tt.wantES.resource, wantName)
					}
				}
			}
		})
	}
}

// TestEnrichErrWrapper pins the wrapper contract: nil stays a nil interface,
// text and Unwrap are the inner error's, As offers only *ElasticsearchError and
// leaves every other target (and a failed match) untouched.
func TestEnrichErrWrapper(t *testing.T) {
	t.Parallel()

	if err := wrapEnrichErr(nil); err != nil { //nolint:errorlint // nil interface identity is the point
		t.Fatalf("wrapEnrichErr(nil) = %#v, want a nil interface", err)
	}
	inner := errors.New("plain cause")
	wrapped := wrapEnrichErr(inner)
	if wrapped.Error() != "plain cause" || errors.Unwrap(wrapped) != inner { //nolint:errorlint // identity
		t.Fatalf("wrapper changed text or Unwrap: %v", wrapped)
	}

	var esErr *ElasticsearchError
	if errors.As(wrapped, &esErr) || esErr != nil {
		t.Fatalf("a cause without a rejection matched: %+v", esErr)
	}
	sentinel := &ElasticsearchError{Resource: "keep"}
	esErr = sentinel
	if errors.As(wrapped, &esErr) || esErr != sentinel {
		t.Fatal("a failed match overwrote the target")
	}
	var other *RunError
	if errors.As(wrapped, &other) {
		t.Fatal("unrelated target matched")
	}
	if !errors.Is(wrapEnrichErr(context.Canceled), context.Canceled) {
		t.Fatal("errors.Is lost the inner identity")
	}
	if (&enrichErr{err: inner}).As(nil) {
		t.Fatal("As(nil) matched")
	}
}

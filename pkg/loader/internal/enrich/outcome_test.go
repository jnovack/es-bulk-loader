package enrich

// Phase 2 (loader-observability) guards: every failure class of executePolicy
// carries its real cause and the documented status, Execute collects ordered
// per-policy outcomes (selected, then missing) with or without a progress
// callback, and the early returns that never resolve a selection produce none.
// Replies are scripted per request in arrival order with bodytransport, so
// each case states its exact request sequence and needs no real time.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jnovack/es-bulk-loader/internal/testutil/bodytransport"
	"github.com/jnovack/es-bulk-loader/internal/testutil/logbuf"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

const (
	taskKickoffBody  = `{"task":"node:1"}`
	taskDoneBody     = `{"completed":true,"response":{"status":{"phase":"COMPLETE"}}}`
	taskCancelledDoc = `{"completed":true,"response":{"status":{"phase":"CANCELLED"}}}`
	taskErrorBody    = `{"completed":true,"error":{"type":"exception","reason":"boom password=hunter2","caused_by":{"type":"io_exception","reason":"disk"}}}`
)

func static(status int, body string) func() bodytransport.Reply {
	return bodytransport.Static(status, body)
}

func failWith(err error) func() bodytransport.Reply {
	return func() bodytransport.Reply { return bodytransport.Reply{Err: err} }
}

// bodyReadFailure answers status with a short body whose read ends in err.
func bodyReadFailure(status int, partial string, err error) func() bodytransport.Reply {
	return func() bodytransport.Reply {
		return bodytransport.Reply{Status: status, Body: &bodytransport.Body{R: strings.NewReader(partial), Err: err}}
	}
}

func scripted(replies ...func() bodytransport.Reply) *bodytransport.Transport {
	return &bodytransport.Transport{Replies: replies}
}

func outcomeCtx(t *testing.T) context.Context {
	t.Helper()
	return runlog.WithLogger(t.Context(), logbuf.Nop())
}

func requireRejection(t *testing.T, err error, resource, name string, status int) *esresponse.Rejection {
	t.Helper()
	var rejection *esresponse.Rejection
	if !errors.As(err, &rejection) {
		t.Fatalf("cause %v carries no *Rejection", err)
	}
	if rejection.Resource() != resource || rejection.ResourceName() != name || rejection.Status() != status {
		t.Fatalf("rejection = (%q, %q, %d), want (%q, %q, %d)",
			rejection.Resource(), rejection.ResourceName(), rejection.Status(), resource, name, status)
	}
	return rejection
}

// TestExecutePolicyOutcomeClasses drives every executePolicy failure class
// except the two alias-verification ones, which TestVerifyAliasOutcomeCauses
// covers, and pins each status and the identity of its cause.
func TestExecutePolicyOutcomeClasses(t *testing.T) {
	t.Parallel()

	boom := errors.New("injected transport failure")
	aliasBefore := static(http.StatusNotFound, aliasMissingBody)
	jsonErr500 := `{"error":{"type":"enrich_exception","reason":"policy broke"},"status":500}`

	tests := []struct {
		name       string
		replies    []func() bodytransport.Reply
		sleepErr   error
		want       Status
		wantPaths  int // 0 means unchecked
		checkCause func(t *testing.T, cause error)
	}{
		{
			name:    "kickoff transport error is failed with the error as cause",
			replies: []func() bodytransport.Reply{aliasBefore, failWith(boom)},
			want:    StatusFailed,
			checkCause: func(t *testing.T, cause error) {
				if !errors.Is(cause, boom) {
					t.Fatalf("cause %v does not wrap the transport error", cause)
				}
			},
		},
		{
			name:    "kickoff transport cancellation is cancelled",
			replies: []func() bodytransport.Reply{aliasBefore, failWith(context.Canceled)},
			want:    StatusCancelled,
			checkCause: func(t *testing.T, cause error) {
				if !errors.Is(cause, context.Canceled) {
					t.Fatalf("cause %v lost context.Canceled", cause)
				}
			},
		},
		{
			name:    "kickoff transport deadline is cancelled",
			replies: []func() bodytransport.Reply{aliasBefore, failWith(context.DeadlineExceeded)},
			want:    StatusCancelled,
		},
		{
			name:    "generic 404 is unsupported with the enrich API rejection",
			replies: []func() bodytransport.Reply{aliasBefore, static(http.StatusNotFound, `{"error":true}`)},
			want:    StatusUnsupported,
			checkCause: func(t *testing.T, cause error) {
				requireRejection(t, cause, "enrich API", "", http.StatusNotFound)
			},
		},
		{
			name:    "kickoff rejection is failed with the structured rejection",
			replies: []func() bodytransport.Reply{aliasBefore, static(http.StatusInternalServerError, jsonErr500)},
			want:    StatusFailed,
			checkCause: func(t *testing.T, cause error) {
				r := requireRejection(t, cause, "enrich policy", "policy-a", http.StatusInternalServerError)
				if pairs := r.Pairs(); len(pairs) != 1 || pairs[0].Type != "enrich_exception" || pairs[0].Reason != "policy broke" {
					t.Fatalf("pairs = %+v", pairs)
				}
			},
		},
		{
			name: "kickoff rejection whose body read was cancelled is cancelled and keeps both identities",
			replies: []func() bodytransport.Reply{
				aliasBefore, bodyReadFailure(http.StatusInternalServerError, `{"err`, context.Canceled),
			},
			want: StatusCancelled,
			checkCause: func(t *testing.T, cause error) {
				requireRejection(t, cause, "enrich policy", "policy-a", http.StatusInternalServerError)
				if !errors.Is(cause, context.Canceled) {
					t.Fatalf("cause %v lost the read cancellation", cause)
				}
			},
		},
		{
			name: "kickoff rejection with a truncated body stays failed",
			replies: []func() bodytransport.Reply{
				aliasBefore, bodyReadFailure(http.StatusInternalServerError, `{"err`, io.ErrUnexpectedEOF),
			},
			want: StatusFailed,
			checkCause: func(t *testing.T, cause error) {
				requireRejection(t, cause, "enrich policy", "policy-a", http.StatusInternalServerError)
				if !errors.Is(cause, io.ErrUnexpectedEOF) {
					t.Fatalf("cause %v lost the truncation error", cause)
				}
			},
		},
		{
			name:    "kickoff decode error is failed",
			replies: []func() bodytransport.Reply{aliasBefore, static(http.StatusOK, `{`)},
			want:    StatusFailed,
			checkCause: func(t *testing.T, cause error) {
				if cause == nil {
					t.Fatal("decode failure has no cause")
				}
			},
		},
		{
			name: "kickoff decode that hit a cancelled read is cancelled",
			replies: []func() bodytransport.Reply{
				aliasBefore, bodyReadFailure(http.StatusOK, `{"ta`, context.Canceled),
			},
			want: StatusCancelled,
			checkCause: func(t *testing.T, cause error) {
				if !errors.Is(cause, context.Canceled) {
					t.Fatalf("cause %v lost context.Canceled", cause)
				}
			},
		},
		{
			name:     "sleep error is cancelled with that error as cause",
			replies:  []func() bodytransport.Reply{aliasBefore, static(http.StatusOK, taskKickoffBody)},
			sleepErr: context.DeadlineExceeded,
			want:     StatusCancelled,
			checkCause: func(t *testing.T, cause error) {
				if cause != context.DeadlineExceeded { //nolint:errorlint // identity is the contract
					t.Fatalf("cause = %v, want the Sleep error itself", cause)
				}
			},
		},
		{
			name:    "task poll transport error is failed",
			replies: []func() bodytransport.Reply{aliasBefore, static(http.StatusOK, taskKickoffBody), failWith(boom)},
			want:    StatusFailed,
			checkCause: func(t *testing.T, cause error) {
				if !errors.Is(cause, boom) {
					t.Fatalf("cause %v does not wrap the poll error", cause)
				}
			},
		},
		{
			name:    "task poll transport cancellation is cancelled",
			replies: []func() bodytransport.Reply{aliasBefore, static(http.StatusOK, taskKickoffBody), failWith(context.Canceled)},
			want:    StatusCancelled,
		},
		{
			name: "poll rejection after three transient failures is failed with the rejection",
			replies: []func() bodytransport.Reply{
				aliasBefore, static(http.StatusOK, taskKickoffBody),
				static(http.StatusServiceUnavailable, jsonErr500),
			},
			want:      StatusFailed,
			wantPaths: 5, // alias, kickoff and exactly three polls: the retry budget is unchanged
			checkCause: func(t *testing.T, cause error) {
				requireRejection(t, cause, "enrich task", "node:1", http.StatusServiceUnavailable)
			},
		},
		{
			name: "non-retryable poll rejection fails at once",
			replies: []func() bodytransport.Reply{
				aliasBefore, static(http.StatusOK, taskKickoffBody),
				static(http.StatusForbidden, jsonErr500),
			},
			want:      StatusFailed,
			wantPaths: 3,
			checkCause: func(t *testing.T, cause error) {
				requireRejection(t, cause, "enrich task", "node:1", http.StatusForbidden)
			},
		},
		{
			name: "poll rejection whose body read was cancelled is cancelled and keeps both identities",
			replies: []func() bodytransport.Reply{
				aliasBefore, static(http.StatusOK, taskKickoffBody),
				bodyReadFailure(http.StatusForbidden, `{"err`, context.Canceled),
			},
			want: StatusCancelled,
			checkCause: func(t *testing.T, cause error) {
				requireRejection(t, cause, "enrich task", "node:1", http.StatusForbidden)
				if !errors.Is(cause, context.Canceled) {
					t.Fatalf("cause %v lost the read cancellation", cause)
				}
			},
		},
		{
			name: "poll rejection with a truncated body stays failed",
			replies: []func() bodytransport.Reply{
				aliasBefore, static(http.StatusOK, taskKickoffBody),
				bodyReadFailure(http.StatusForbidden, `{"err`, io.ErrUnexpectedEOF),
			},
			want: StatusFailed,
		},
		{
			name: "transient poll with an unreadable diagnostic body keeps the retry sequence",
			replies: []func() bodytransport.Reply{
				aliasBefore, static(http.StatusOK, taskKickoffBody),
				bodyReadFailure(http.StatusServiceUnavailable, `{"err`, context.Canceled), // retried, not classified
				static(http.StatusOK, taskDoneBody),
			},
			want:      StatusSucceeded,
			wantPaths: 4,
		},
		{
			name: "transient poll then a failing sleep makes the sleep error the cause",
			replies: []func() bodytransport.Reply{
				aliasBefore, static(http.StatusOK, taskKickoffBody),
				bodyReadFailure(http.StatusServiceUnavailable, `{"err`, io.ErrUnexpectedEOF),
			},
			sleepErr: errSleepSecond,
			want:     StatusCancelled,
			checkCause: func(t *testing.T, cause error) {
				if cause != errSleepSecond { //nolint:errorlint // identity is the contract
					t.Fatalf("cause = %v, want the Sleep error, not the unrelated read error", cause)
				}
			},
		},
		{
			name:    "task decode error is failed",
			replies: []func() bodytransport.Reply{aliasBefore, static(http.StatusOK, taskKickoffBody), static(http.StatusOK, `{`)},
			want:    StatusFailed,
		},
		{
			name: "task decode that hit a cancelled read is cancelled",
			replies: []func() bodytransport.Reply{
				aliasBefore, static(http.StatusOK, taskKickoffBody), bodyReadFailure(http.StatusOK, `{"com`, context.Canceled),
			},
			want: StatusCancelled,
		},
		{
			name:    "task error is failed with a structured status-0 cause",
			replies: []func() bodytransport.Reply{aliasBefore, static(http.StatusOK, taskKickoffBody), static(http.StatusOK, taskErrorBody)},
			want:    StatusFailed,
			checkCause: func(t *testing.T, cause error) {
				r := requireRejection(t, cause, "enrich task", "node:1", 0)
				pairs := r.Pairs()
				if len(pairs) != 2 || pairs[0].Type != "exception" || pairs[1].Type != "io_exception" {
					t.Fatalf("pairs = %+v", pairs)
				}
				want := `enrich task "node:1" failed: exception: boom password=[REDACTED]; io_exception: disk`
				if cause.Error() != want {
					t.Fatalf("cause text = %q, want %q", cause.Error(), want)
				}
			},
		},
		{
			name: "synchronous FAILED phase is failed",
			replies: []func() bodytransport.Reply{
				aliasBefore, static(http.StatusOK, `{"status":{"phase":"FAILED"}}`),
			},
			want: StatusFailed,
			checkCause: func(t *testing.T, cause error) {
				if want := `enrich policy ended in phase "FAILED"`; cause.Error() != want {
					t.Fatalf("cause text = %q, want %q", cause.Error(), want)
				}
			},
		},
		{
			name: "synchronous CANCELLED phase is failed",
			replies: []func() bodytransport.Reply{
				aliasBefore, static(http.StatusOK, `{"status":{"phase":"CANCELLED"}}`),
			},
			want: StatusFailed,
			checkCause: func(t *testing.T, cause error) {
				if want := `enrich policy ended in phase "CANCELLED"`; cause.Error() != want {
					t.Fatalf("cause text = %q, want %q", cause.Error(), want)
				}
			},
		},
		{
			name: "async FAILED phase is failed",
			replies: []func() bodytransport.Reply{
				aliasBefore, static(http.StatusOK, taskKickoffBody),
				static(http.StatusOK, `{"completed":true,"response":{"status":{"phase":"FAILED"}}}`),
			},
			want: StatusFailed,
			checkCause: func(t *testing.T, cause error) {
				if want := `enrich policy ended in phase "FAILED"`; cause.Error() != want {
					t.Fatalf("cause text = %q, want %q", cause.Error(), want)
				}
			},
		},
		{
			name: "completed task without a response reads task.status",
			replies: []func() bodytransport.Reply{
				aliasBefore, static(http.StatusOK, taskKickoffBody),
				static(http.StatusOK, `{"completed":true,"task":{"status":{"phase":"FAILED"}}}`),
			},
			want: StatusFailed,
			checkCause: func(t *testing.T, cause error) {
				if want := `enrich policy ended in phase "FAILED"`; cause.Error() != want {
					t.Fatalf("cause text = %q, want %q", cause.Error(), want)
				}
			},
		},
		{
			// An Elasticsearch CANCELLED phase is deliberately "failed":
			// "cancelled" means this Run's context ended. Do not "fix" this.
			name: "async CANCELLED phase stays failed, never cancelled",
			replies: []func() bodytransport.Reply{
				aliasBefore, static(http.StatusOK, taskKickoffBody), static(http.StatusOK, taskCancelledDoc),
			},
			want: StatusFailed,
			checkCause: func(t *testing.T, cause error) {
				if errors.Is(cause, context.Canceled) {
					t.Fatalf("an Elasticsearch CANCELLED phase must not look like context cancellation: %v", cause)
				}
				if want := `enrich policy ended in phase "CANCELLED"`; cause.Error() != want {
					t.Fatalf("cause text = %q, want %q", cause.Error(), want)
				}
			},
		},
		{
			name: "synchronous COMPLETE succeeds with a nil cause",
			replies: []func() bodytransport.Reply{
				aliasBefore, static(http.StatusOK, `{"status":{"phase":"COMPLETE"}}`),
			},
			want: StatusSucceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr := scripted(tt.replies...)
			cfg := fixedEnrichSeams()
			if tt.sleepErr != nil {
				calls := 0
				failOn := 1
				if tt.sleepErr == errSleepSecond { //nolint:errorlint // identity
					failOn = 2
				}
				cfg.Sleep = func(context.Context, time.Duration) error {
					calls++
					if calls == failOn {
						return tt.sleepErr
					}
					return nil
				}
			}

			got := executePolicy(outcomeCtx(t), tr.Client(t), "policy-a", cfg)

			if got.status != tt.want {
				t.Fatalf("status = %q (cause %v), want %q", got.status, got.cause, tt.want)
			}
			if (got.status == StatusSucceeded) != (got.cause == nil) {
				t.Fatalf("status %q with cause %v: only success may have a nil cause", got.status, got.cause)
			}
			if tt.wantPaths != 0 && len(tr.Paths()) != tt.wantPaths {
				t.Fatalf("paths = %q, want %d requests", tr.Paths(), tt.wantPaths)
			}
			if tt.checkCause != nil {
				tt.checkCause(t, got.cause)
			}
		})
	}
}

var errSleepSecond = errors.New("sleep failed on the second wait")

// TestVerifyAliasOutcomeCauses covers the two alias failure classes: an
// unreadable alias keeps the first failed read reachable (before, otherwise
// after), and an unchanged or absent target set names both sets.
func TestVerifyAliasOutcomeCauses(t *testing.T) {
	t.Parallel()

	const (
		aliasOneTarget = `{".enrich-policy-a-1":{"aliases":{".enrich-policy-a":{}}}}`
		aliasNewTarget = `{".enrich-policy-a-2":{"aliases":{".enrich-policy-a":{}}}}`
		err500         = `{"error":{"type":"before_failure","reason":"first"},"status":500}`
		err503         = `{"error":{"type":"after_failure","reason":"second"},"status":503}`
	)
	task404 := static(http.StatusNotFound, enrichTaskNotFoundBody)
	kickoff := static(http.StatusOK, taskKickoffBody)

	tests := []struct {
		name       string
		replies    []func() bodytransport.Reply
		wantStatus int // rejection status expected on the cause chain, 0 when none
		wantText   string
		wantIs     error // identity expected on the cause chain, nil when none
		secret     bool  // the cause must be free of the planted secret
	}{
		{
			// Documented: an unreadable alias is failed even when the read
			// failed because the context ended, not cancelled.
			name:    "after read fails with a cancelled context: failed, identity kept",
			replies: []func() bodytransport.Reply{static(404, aliasMissingBody), kickoff, task404, failWith(context.Canceled)},
			wantIs:  context.Canceled,
		},
		{
			name:    "before read fails with a cancelled context: failed, identity kept",
			replies: []func() bodytransport.Reply{failWith(context.Canceled), kickoff, task404, static(200, aliasNewTarget)},
			wantIs:  context.Canceled,
		},
		{
			name: "oversized alias rejection with a secret stays bounded and reachable",
			replies: []func() bodytransport.Reply{
				static(500, `{"error":{"type":"before_failure","reason":"password=hunter2 `+strings.Repeat("x", 4096)+`"},"status":500}`),
				kickoff, task404, static(200, aliasNewTarget),
			},
			wantStatus: 500,
			secret:     true,
		},
		{
			name:       "before read fails, after ok: before is the cause",
			replies:    []func() bodytransport.Reply{static(500, err500), kickoff, task404, static(200, aliasNewTarget)},
			wantStatus: 500,
		},
		{
			name:       "before ok, after read fails: after is the cause",
			replies:    []func() bodytransport.Reply{static(404, aliasMissingBody), kickoff, task404, static(503, err503)},
			wantStatus: 503,
		},
		{
			name:       "both reads fail: the before read stays the cause",
			replies:    []func() bodytransport.Reply{static(500, err500), kickoff, task404, static(503, err503)},
			wantStatus: 500,
		},
		{
			name:     "unchanged target set names before and after",
			replies:  []func() bodytransport.Reply{static(200, aliasOneTarget), kickoff, task404, static(200, aliasOneTarget)},
			wantText: `enrich backing alias ".enrich-policy-a" did not move (before [".enrich-policy-a-1"], after [".enrich-policy-a-1"])`,
		},
		{
			name:     "absent alias after names an empty after set",
			replies:  []func() bodytransport.Reply{static(200, aliasOneTarget), kickoff, task404, static(404, aliasMissingBody)},
			wantText: `enrich backing alias ".enrich-policy-a" did not move (before [".enrich-policy-a-1"], after [])`,
		},
		{
			name:     "absent both times",
			replies:  []func() bodytransport.Reply{static(404, aliasMissingBody), kickoff, task404, static(404, aliasMissingBody)},
			wantText: `enrich backing alias ".enrich-policy-a" did not move (before [], after [])`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr := scripted(tt.replies...)

			got := executePolicy(outcomeCtx(t), tr.Client(t), "policy-a", fixedEnrichSeams())

			if got.status != StatusFailed || got.cause == nil {
				t.Fatalf("outcome = %+v, want failed with a cause", got)
			}
			if len(got.cause.Error()) > esresponse.MaxPublicMessageBytes {
				t.Fatalf("cause is %d bytes, over the %d bound", len(got.cause.Error()), esresponse.MaxPublicMessageBytes)
			}
			if tt.wantStatus != 0 {
				var rejection *esresponse.Rejection
				if !errors.As(got.cause, &rejection) || rejection.Status() != tt.wantStatus {
					t.Fatalf("cause %v: want the %d alias read rejection reachable", got.cause, tt.wantStatus)
				}
			}
			if tt.wantIs != nil && !errors.Is(got.cause, tt.wantIs) {
				t.Fatalf("cause %v lost %v", got.cause, tt.wantIs)
			}
			if tt.secret && strings.Contains(got.cause.Error(), "hunter2") {
				t.Fatalf("cause leaks a secret: %q", got.cause.Error())
			}
			if tt.wantText != "" && got.cause.Error() != tt.wantText {
				t.Fatalf("cause text = %q, want %q", got.cause.Error(), tt.wantText)
			}
		})
	}
}

// TestNewBoundedCauseSanitizesAndBounds pins that the new phase and alias
// cause text is redacted, valid single-line UTF-8 and at most
// MaxPublicMessageBytes, and that the wrapped error stays reachable.
func TestNewBoundedCauseSanitizesAndBounds(t *testing.T) {
	t.Parallel()

	inner := errors.New("inner")
	hostile := "line1\nline2 password=hunter2 \xff\xfe " + strings.Repeat("é", 4000)

	got := newBoundedCause(hostile, inner)

	text := got.Error()
	if len(text) > esresponse.MaxPublicMessageBytes {
		t.Fatalf("text is %d bytes, want <= %d", len(text), esresponse.MaxPublicMessageBytes)
	}
	if !utf8.ValidString(text) || strings.ContainsAny(text, "\n\r") {
		t.Fatalf("text %q is not single-line valid UTF-8", text)
	}
	if strings.Contains(text, "hunter2") {
		t.Fatalf("text leaks a secret: %q", text)
	}
	if !errors.Is(got, inner) {
		t.Fatal("wrapped error is not reachable through Unwrap")
	}
}

func okPolicies(names ...string) func() bodytransport.Reply {
	configs := make([]string, 0, len(names))
	for _, n := range names {
		configs = append(configs, `{"config":{"match":{"name":"`+n+`"}}}`)
	}
	return static(http.StatusOK, `{"policies":[`+strings.Join(configs, ",")+`]}`)
}

// TestExecuteOutcomes covers Outcomes ordering, count derivation and the
// early returns, with a nil OnProgress (outcomes must not depend on it).
func TestExecuteOutcomes(t *testing.T) {
	t.Parallel()

	aliasBefore := static(http.StatusNotFound, aliasMissingBody)
	done := static(http.StatusOK, `{"status":{"phase":"COMPLETE"}}`)
	reject := static(http.StatusInternalServerError, `{"error":{"type":"enrich_exception","reason":"x"}}`)

	type want struct {
		policy string
		status Status
	}
	tests := []struct {
		name         string
		sel          *Selection
		replies      []func() bodytransport.Reply
		wantErr      bool
		want         []want
		wantFailed   int
		wantSuccess  int
		wantFinals   int
		wantMissing  []string
		wantSelected []string
	}{
		{
			name: "selected then missing in request order, continuing past a failure",
			sel:  &Selection{Enabled: true, Raw: "ghost-1,policy-b,policy-a,ghost-2"},
			replies: []func() bodytransport.Reply{
				okPolicies("policy-a", "policy-b"),
				aliasBefore, reject, // policy-b (request order) fails
				aliasBefore, done, // policy-a succeeds
			},
			want: []want{
				{"policy-b", StatusFailed}, {"policy-a", StatusSucceeded},
				{"ghost-1", StatusMissing}, {"ghost-2", StatusMissing},
			},
			wantFailed: 1, wantSuccess: 1, wantFinals: 2,
			wantMissing: []string{"ghost-1", "ghost-2"}, wantSelected: []string{"policy-b", "policy-a"},
		},
		{
			name:        "missing-only request against a non-empty cluster",
			sel:         &Selection{Enabled: true, Raw: "ghost-1,ghost-2"},
			replies:     []func() bodytransport.Reply{okPolicies("policy-a")},
			want:        []want{{"ghost-1", StatusMissing}, {"ghost-2", StatusMissing}},
			wantMissing: []string{"ghost-1", "ghost-2"},
		},
		{
			name:    "an unsupported policy counts as failed",
			sel:     &Selection{Enabled: true, All: true},
			replies: []func() bodytransport.Reply{okPolicies("policy-a"), aliasBefore, static(404, `{"error":true}`)},
			want:    []want{{"policy-a", StatusUnsupported}}, wantFailed: 1, wantFinals: 1,
			wantSelected: []string{"policy-a"},
		},
		{
			name:    "disabled enrich has no outcomes",
			sel:     &Selection{Enabled: false},
			replies: []func() bodytransport.Reply{okPolicies("policy-a")},
		},
		{
			name:    "empty cluster has no outcomes even for an explicit request",
			sel:     &Selection{Enabled: true, Raw: "ghost"},
			replies: []func() bodytransport.Reply{static(http.StatusOK, `{"policies":[]}`)},
		},
		{
			name:    "unsupported listing has no outcomes even for an explicit request",
			sel:     &Selection{Enabled: true, Raw: "ghost"},
			replies: []func() bodytransport.Reply{static(http.StatusNotFound, `{"error":true}`)},
		},
		{
			name:    "list failure is the only error and has no outcomes",
			sel:     &Selection{Enabled: true, All: true},
			replies: []func() bodytransport.Reply{static(http.StatusInternalServerError, `{"error":{"type":"x","reason":"y"}}`)},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr := scripted(tt.replies...)
			cfg := fixedEnrichSeams()
			cfg.OnProgress = nil // outcomes must be collected without a callback

			summary, err := Execute(outcomeCtx(t), tr.Client(t), tt.sel, nil, cfg)

			if (err != nil) != tt.wantErr {
				t.Fatalf("Execute error = %v, wantErr %v", err, tt.wantErr)
			}
			var got []want
			for _, o := range summary.Outcomes {
				got = append(got, want{o.Policy, o.Status})
				if (o.Status == StatusSucceeded || o.Status == StatusMissing) != (o.Cause == nil) {
					t.Fatalf("outcome %+v: only succeeded and missing may have a nil cause", o)
				}
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("outcomes = %+v, want %+v", got, tt.want)
			}
			if summary.Failed != tt.wantFailed || summary.Succeeded != tt.wantSuccess {
				t.Fatalf("succeeded/failed = %d/%d, want %d/%d", summary.Succeeded, summary.Failed, tt.wantSuccess, tt.wantFailed)
			}
			if !slices.Equal(summary.Missing, tt.wantMissing) || !slices.Equal(summary.Selected, tt.wantSelected) {
				t.Fatalf("selected/missing = %v/%v, want %v/%v", summary.Selected, summary.Missing, tt.wantSelected, tt.wantMissing)
			}
			selected := 0
			var missingNames []string
			for _, o := range summary.Outcomes {
				if o.Status == StatusMissing {
					missingNames = append(missingNames, o.Policy)
				} else {
					selected++
				}
			}
			if summary.Succeeded+summary.Failed != selected || !slices.Equal(missingNames, summary.Missing) {
				t.Fatalf("count invariants broken: %+v", summary)
			}
		})
	}
}

// TestExecuteFinalPerSelectedPolicyAndNoMissingEvents pins one Final event per
// selected policy (carrying the cause under the unchanged prefix), none for a
// missing policy, and continuation to the next policy after a failure.
func TestExecuteFinalPerSelectedPolicyAndNoMissingEvents(t *testing.T) {
	t.Parallel()

	tr := scripted(
		okPolicies("policy-a", "policy-b"),
		static(http.StatusNotFound, aliasMissingBody), static(http.StatusInternalServerError, `{"error":{"type":"enrich_exception","reason":"x"}}`),
		static(http.StatusNotFound, aliasMissingBody), static(http.StatusOK, `{"status":{"phase":"COMPLETE"}}`),
	)
	cfg := fixedEnrichSeams()
	var finals []progress.Snapshot
	cfg.OnProgress = func(s progress.Snapshot) {
		if s.Final {
			finals = append(finals, s)
		}
	}

	summary, err := Execute(outcomeCtx(t), tr.Client(t), &Selection{Enabled: true, Raw: "policy-a,policy-b,ghost"}, nil, cfg)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(finals) != 2 || finals[0].Resource != "policy-a" || finals[1].Resource != "policy-b" {
		t.Fatalf("finals = %+v, want one Final each for policy-a then policy-b, none for ghost", finals)
	}
	wantErr := `enrich policy "policy-a" failed: enrich policy "policy-a" rejected with HTTP 500: enrich_exception: x`
	if finals[0].Err == nil || finals[0].Err.Error() != wantErr {
		t.Fatalf("policy-a Final Err = %v, want %q", finals[0].Err, wantErr)
	}
	if !errors.Is(finals[0].Err, summary.Outcomes[0].Cause) {
		t.Fatal("Final Err does not wrap the outcome's cause")
	}
	if finals[1].Err != nil {
		t.Fatalf("policy-b succeeded but Final Err = %v", finals[1].Err)
	}
}

// TestPolicyOutcomeHelpers pins the status classification helpers.
func TestPolicyOutcomeHelpers(t *testing.T) {
	t.Parallel()

	if !okOutcome().succeeded() || failOutcome(errors.New("x")).succeeded() {
		t.Fatal("succeeded() must be true only for StatusSucceeded")
	}
	if causeOutcome(nil).status != StatusFailed {
		t.Fatal("a nil cause classifies as failed, never cancelled")
	}
	wrapped := causeOutcome(errors.Join(errors.New("a"), context.Canceled))
	if wrapped.status != StatusCancelled {
		t.Fatalf("wrapped cancellation status = %q, want cancelled", wrapped.status)
	}
}

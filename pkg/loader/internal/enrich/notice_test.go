package enrich

// Phase 4 (loader-observability) guards: each enrich notice site emits its
// observe notice beside the unchanged Warn log, with the fixed log message and
// never the response text, and the excluded or terminal paths emit none.
// De-duplication is the loader's job, so these tests count raw emits.

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jnovack/es-bulk-loader/internal/testutil/bodytransport"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/noticetest"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
)

// proxy404Body is a generic (unsupported-API) 404 carrying a secret, so a
// notice that copied response text would leak it.
const proxy404Body = `{"error":"proxy route missing password=hunter2"}`

var unsupportedListNotice = observe.Notice{Code: observe.CodeEnrichAPIUnsupported, Message: UnsupportedPolicyAPIMessage}

// TestListPoliciesUnsupportedNotice guards the list producer of
// enrich_api_unsupported and the list paths that must stay silent.
func TestListPoliciesUnsupportedNotice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		reply   func() bodytransport.Reply
		wantErr bool
		want    []observe.Notice
	}{
		{"generic 404 is unsupported", static(http.StatusNotFound, proxy404Body), false, []observe.Notice{unsupportedListNotice}},
		{"success emits nothing", okPolicies("policy-a"), false, nil},
		{"terminal rejection emits nothing", static(http.StatusInternalServerError, `{"error":{"type":"x","reason":"y"}}`), true, nil},
		{"policy-missing 404 emits nothing", static(http.StatusNotFound, `{"error":{"type":"resource_not_found_exception"}}`), true, nil},
		{"oversized 404 emits nothing", static(http.StatusNotFound, strings.Repeat(" ", esresponse.MaxRejectionBodyBytes+10)), true, nil},
		{"unreadable 404 emits nothing", bodytransport.ReadFail(http.StatusNotFound, `{`, errors.New("read failed")), true, nil},
		{"transport failure emits nothing", bodytransport.Fail(errors.New("dial failed")), true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, rec := noticetest.New(t)
			_, _, err := ListPolicies(ctx, scripted(tt.reply).Client(t))
			if (err != nil) != tt.wantErr {
				t.Fatalf("ListPolicies error = %v, wantErr %v", err, tt.wantErr)
			}
			noticetest.RequireNotices(t, rec.Notices, tt.want, "hunter2", "proxy route")
		})
	}
}

// TestExecuteNotices guards the Execute notice sites: no policies, each
// missing policy (kind policy, the requested name), no match, and the execute
// producer of enrich_api_unsupported, which carries no kind or name so the
// loader can report it once per Run; the per-policy fact stays the
// StatusUnsupported outcome.
func TestExecuteNotices(t *testing.T) {
	t.Parallel()

	aliasBefore := static(http.StatusNotFound, aliasMissingBody)
	done := static(http.StatusOK, `{"status":{"phase":"COMPLETE"}}`)
	missing := func(name string) observe.Notice {
		return observe.Notice{Code: observe.CodeEnrichPolicyMissing, Message: "Enrich policy not found; skipping", Kind: observe.KindPolicy, Name: name}
	}
	unsupportedExecute := observe.Notice{Code: observe.CodeEnrichAPIUnsupported, Message: unsupportedExecuteAPIMessage}

	tests := []struct {
		name         string
		sel          *Selection
		replies      []func() bodytransport.Reply
		want         []observe.Notice
		wantStatuses []Status
	}{
		{
			name:    "empty cluster",
			sel:     &Selection{Enabled: true, Raw: "ghost"},
			replies: []func() bodytransport.Reply{static(http.StatusOK, `{"policies":[]}`)},
			want:    []observe.Notice{{Code: observe.CodeEnrichNoPolicies, Message: "No enrich policies found; skipping enrich execution"}},
		},
		{
			name:    "unsupported listing",
			sel:     &Selection{Enabled: true, All: true},
			replies: []func() bodytransport.Reply{static(http.StatusNotFound, proxy404Body)},
			want:    []observe.Notice{unsupportedListNotice},
		},
		{
			name:         "missing policies beside a selected one",
			sel:          &Selection{Enabled: true, Raw: "ghost-1,policy-a,ghost-2"},
			replies:      []func() bodytransport.Reply{okPolicies("policy-a"), aliasBefore, done},
			want:         []observe.Notice{missing("ghost-1"), missing("ghost-2")},
			wantStatuses: []Status{StatusSucceeded, StatusMissing, StatusMissing},
		},
		{
			name:    "no request matched",
			sel:     &Selection{Enabled: true, Raw: "ghost"},
			replies: []func() bodytransport.Reply{okPolicies("policy-a")},
			want: []observe.Notice{
				missing("ghost"),
				{Code: observe.CodeEnrichNoMatch, Message: "No enrich policies matched the request"},
			},
			wantStatuses: []Status{StatusMissing},
		},
		{
			name: "every unsupported execute emits",
			sel:  &Selection{Enabled: true, All: true},
			replies: []func() bodytransport.Reply{
				okPolicies("policy-a", "policy-b"),
				aliasBefore, static(http.StatusNotFound, proxy404Body),
				aliasBefore, static(http.StatusNotFound, proxy404Body),
			},
			want:         []observe.Notice{unsupportedExecute, unsupportedExecute},
			wantStatuses: []Status{StatusUnsupported, StatusUnsupported},
		},
		{
			name:         "successful execution emits nothing",
			sel:          &Selection{Enabled: true, All: true},
			replies:      []func() bodytransport.Reply{okPolicies("policy-a"), aliasBefore, done},
			wantStatuses: []Status{StatusSucceeded},
		},
		{
			name:         "execute rejection emits nothing",
			sel:          &Selection{Enabled: true, All: true},
			replies:      []func() bodytransport.Reply{okPolicies("policy-a"), aliasBefore, static(http.StatusInternalServerError, `{"error":{"type":"enrich_exception","reason":"x"}}`)},
			wantStatuses: []Status{StatusFailed},
		},
		{
			name:         "unreadable execute 404 emits nothing",
			sel:          &Selection{Enabled: true, All: true},
			replies:      []func() bodytransport.Reply{okPolicies("policy-a"), aliasBefore, bodytransport.ReadFail(http.StatusNotFound, `{`, errors.New("read failed"))},
			wantStatuses: []Status{StatusFailed},
		},
		{
			name:         "oversized execute 404 emits nothing",
			sel:          &Selection{Enabled: true, All: true},
			replies:      []func() bodytransport.Reply{okPolicies("policy-a"), aliasBefore, static(http.StatusNotFound, strings.Repeat(" ", esresponse.MaxRejectionBodyBytes+10))},
			wantStatuses: []Status{StatusFailed},
		},
		{
			name: "a retried task poll emits nothing",
			sel:  &Selection{Enabled: true, All: true},
			replies: []func() bodytransport.Reply{
				okPolicies("policy-a"), aliasBefore, static(http.StatusOK, taskKickoffBody),
				static(http.StatusServiceUnavailable, `{"error":{"type":"busy","reason":"retry me"}}`),
				static(http.StatusOK, taskDoneBody),
			},
			wantStatuses: []Status{StatusSucceeded},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, rec := noticetest.New(t)
			summary, err := Execute(ctx, scripted(tt.replies...).Client(t), tt.sel, nil, fixedEnrichSeams())
			if err != nil {
				t.Fatalf("Execute error = %v", err)
			}
			noticetest.RequireNotices(t, rec.Notices, tt.want, "hunter2", "proxy route")
			var statuses []Status
			for _, o := range summary.Outcomes {
				statuses = append(statuses, o.Status)
			}
			if !slices.Equal(statuses, tt.wantStatuses) {
				t.Fatalf("outcome statuses = %v, want %v", statuses, tt.wantStatuses)
			}
		})
	}
}

// TestExecuteCancellationEmitsNoNotice guards the cancellation exclusion: the
// policy's StatusCancelled outcome reports it, so no notice is added.
func TestExecuteCancellationEmitsNoNotice(t *testing.T) {
	t.Parallel()

	ctx, rec := noticetest.New(t)
	cfg := progress.Seams{
		Now:   fixedEnrichSeams().Now,
		Sleep: func(context.Context, time.Duration) error { return context.Canceled },
	}
	tr := scripted(okPolicies("policy-a"), static(http.StatusNotFound, aliasMissingBody), static(http.StatusOK, taskKickoffBody))
	summary, err := Execute(ctx, tr.Client(t), &Selection{Enabled: true, All: true}, nil, cfg)
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}
	if len(summary.Outcomes) != 1 || summary.Outcomes[0].Status != StatusCancelled {
		t.Fatalf("outcomes = %+v, want one cancelled", summary.Outcomes)
	}
	noticetest.RequireNotices(t, rec.Notices, nil, "hunter2", "proxy route")
}

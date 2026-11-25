package loader

// Phase 4 (loader-observability) guards at the public boundary: every warn
// branch and every internal actionable condition reaches Result.Notices
// exactly once with its code, resource kind and name; a warn notice's Message
// equals its Warnings entry; notices are de-duplicated once per Run on the
// sanitized (Code, ResourceKind, Name) tuple; excluded conditions add none;
// and notices survive a run-ending failure. Every Run uses zerolog.Nop() and
// asserts on the error, Result and callbacks only. Tests that use
// installFakeClock are serial; the rest are parallel.

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/enrich"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"
)

// noticeBadURL fails client construction right after the option warnings, so
// a row reaches a known run-ending error with no cluster request.
const noticeBadURL = "http://%"

// noticeProxy404 is a generic (unsupported-API) 404 carrying a secret.
const noticeProxy404 = `{"error":"proxy route missing password=hunter2"}`

// runNop runs opts with the shared Nop logger (nopLogger).
func runNop(t *testing.T, ctx context.Context, opts Options) (Result, error) {
	t.Helper()
	nopLogger(&opts)
	return Run(ctx, opts)
}

// requireNoticeList fails unless got equals want exactly.
func requireNoticeList(t *testing.T, got, want []Notice) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("Notices = %+v\nwant      %+v", got, want)
	}
	for _, n := range got {
		if strings.Contains(n.Message, "hunter2") || strings.Contains(n.Message, "proxy route") {
			t.Fatalf("notice Message %q carries response text", n.Message)
		}
	}
}

// warnNotice builds the expected notice for a warn branch.
func warnNotice(code NoticeCode, kind ResourceKind, name, message string) Notice {
	return Notice{Code: code, Message: message, ResourceKind: kind, Name: name}
}

const (
	msgKeepLastIgnored  = "Ignoring -keep-last because -alias is not enabled"
	msgSyncAssumed      = "Alias delete detected without -sync-managed; assuming -sync-managed for this run. Add -sync-managed explicitly on the command line."
	msgKeepLastMissing  = "Alias delete detected without -keep-last; no old timestamped indices will be deleted and storage usage can grow over time."
	msgTransformsIgnore = "Ignoring -transforms because -sync-managed is not enabled"
	msgWaitIgnored      = "Ignoring transform wait because no transforms were synced for this run"
)

// TestRunWarnBranchesReportNotices covers all 13 warn call sites: each Run
// reports exactly the notices of the branches it took, in Warnings order,
// and each notice's Message equals its Warnings entry byte for byte.
func TestRunWarnBranchesReportNotices(t *testing.T) {
	t.Parallel()

	aliasMissing := esstub.JSON(http.StatusNotFound, `{"error":{"type":"aliases_not_found_exception"},"status":404}`)
	lifecycle := func(cfg lifecycleConfig) func(t *testing.T) string {
		return func(t *testing.T) string {
			cfg.index = "cards"
			return newLifecycleStub(t, cfg).URL()
		}
	}
	routes := func(r map[string]http.HandlerFunc) func(t *testing.T) string {
		return func(t *testing.T) string { return newRouteStub(t, r).URL() }
	}
	badURL := func(*testing.T) string { return noticeBadURL }

	tests := []struct {
		name    string
		server  func(t *testing.T) string
		opts    func(o *Options)
		want    []Notice
		wantErr bool
	}{
		{
			name:   "keep-last without alias",
			server: badURL, opts: func(o *Options) { o.AddToIndex, o.KeepLast = true, 1 },
			want:    []Notice{warnNotice(NoticeKeepLastIgnored, "", "", msgKeepLastIgnored)},
			wantErr: true,
		},
		{
			name:   "alias delete without sync-managed or keep-last",
			server: badURL, opts: func(o *Options) { o.AliasMode, o.DeleteIndex = true, true },
			want: []Notice{
				warnNotice(NoticeSyncManagedAssumed, "", "", msgSyncAssumed),
				warnNotice(NoticeKeepLastMissing, "", "", msgKeepLastMissing),
			},
			wantErr: true,
		},
		{
			name:   "transforms without sync-managed",
			server: badURL, opts: func(o *Options) { o.AddToIndex, o.TransformsFile = true, "transforms.json" },
			want:    []Notice{warnNotice(NoticeTransformsIgnored, "", "", msgTransformsIgnore)},
			wantErr: true,
		},
		{
			name:   "nuke reset of an index",
			server: badURL, opts: func(o *Options) { o.Nuke, o.SyncManaged, o.DataFile = true, true, "" },
			want:    []Notice{warnNotice(NoticeNukeResetOnly, ResourceKindIndex, "cards", nukeResetWantWarning(nukeResetConcrete))},
			wantErr: true,
		},
		{
			name:   "nuke reset of an alias",
			server: badURL, opts: func(o *Options) { o.Nuke, o.SyncManaged, o.AliasMode, o.DataFile = true, true, true, "" },
			want:    []Notice{warnNotice(NoticeNukeResetOnly, ResourceKindAlias, "cards", nukeResetWantWarning(nukeResetAlias))},
			wantErr: true,
		},
		{
			name: "concrete index with the alias name",
			server: routes(map[string]http.HandlerFunc{
				"GET /_alias/cards": aliasMissing,
				"HEAD /cards":       replyHeadOK,
				// Ends the Run after the warning; the notice must survive.
				"POST /cards/_refresh": replyFail,
			}),
			opts: func(o *Options) {
				o.AliasMode, o.DataFile, o.Enrich = true, "", EnrichOptions{Enabled: true, All: true}
			},
			want:    []Notice{warnNotice(NoticeAliasIsConcreteIndex, ResourceKindIndex, "cards", `Found concrete index with alias name "cards"; treating it as the current write target`)},
			wantErr: true,
		},
		{
			name: "nuke of an alias with no indices",
			server: routes(map[string]http.HandlerFunc{
				"GET /_alias/cards": aliasMissing,
				"HEAD /cards":       replyHeadMissing,
				"GET /cards-*":      esstub.JSON(http.StatusOK, `{}`),
			}),
			opts: func(o *Options) { o.AliasMode, o.Nuke, o.DataFile = true, true, "" },
			want: []Notice{warnNotice(NoticeNukeTargetMissing, ResourceKindAlias, "cards", `Alias "cards" does not currently resolve to an index. Nuke will still remove declared managed resources`)},
		},
		{
			name:   "nuke of a missing index",
			server: routes(map[string]http.HandlerFunc{"HEAD /cards": replyHeadMissing}),
			opts:   func(o *Options) { o.Nuke, o.DataFile = true, "" },
			want:   []Notice{warnNotice(NoticeNukeTargetMissing, ResourceKindIndex, "cards", `Index "cards" does not exist. Nuke will still remove declared managed resources`)},
		},
		{
			name:   "alias delete with no indices",
			server: lifecycle(lifecycleConfig{}),
			opts:   func(o *Options) { o.AliasMode, o.DeleteIndex, o.SyncManaged = true, true, true },
			want: []Notice{
				warnNotice(NoticeKeepLastMissing, "", "", msgKeepLastMissing),
				warnNotice(NoticeDeleteTargetMissing, ResourceKindAlias, "cards", `Alias "cards" has no indices. Nothing to delete.`),
			},
		},
		{
			name:   "delete of a missing index",
			server: lifecycle(lifecycleConfig{}),
			opts:   func(o *Options) { o.DeleteIndex = true },
			want:   []Notice{warnNotice(NoticeDeleteTargetMissing, ResourceKindIndex, "cards", `Index "cards" does not exist. Nothing to delete.`)},
		},
		{
			name:   "alias flush with no indices",
			server: lifecycle(lifecycleConfig{}),
			opts:   func(o *Options) { o.AliasMode, o.FlushIndex = true, true },
			want:   []Notice{warnNotice(NoticeFlushTargetMissing, ResourceKindAlias, "cards", `Alias "cards" has no indices. Nothing to flush.`)},
		},
		{
			name:   "flush of a missing index",
			server: lifecycle(lifecycleConfig{}),
			opts:   func(o *Options) { o.FlushIndex = true },
			want:   []Notice{warnNotice(NoticeFlushTargetMissing, ResourceKindIndex, "cards", `Index "cards" does not exist. Nothing to flush.`)},
		},
		{
			name:   "transform wait with nothing synced",
			server: lifecycle(lifecycleConfig{}),
			opts:   func(o *Options) { o.AddToIndex, o.Transforms.Wait = true, true },
			want:   []Notice{warnNotice(NoticeTransformWaitIgnored, "", "", msgWaitIgnored)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := Options{URL: tt.server(t), Index: "cards", DataFile: writeBulkDataFixture(t)}
			tt.opts(&opts)
			result, err := runNop(t, t.Context(), opts)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Run() error = %v, wantErr %v", err, tt.wantErr)
			}
			requireNoticeList(t, result.Notices, tt.want)
			if len(result.Warnings) != len(result.Notices) {
				t.Fatalf("Warnings = %q, want one per notice", result.Warnings)
			}
			for i, w := range result.Warnings {
				if result.Notices[i].Message != w {
					t.Fatalf("notice %d Message = %q, want the Warnings entry %q", i, result.Notices[i].Message, w)
				}
			}
		})
	}
}

// TestRunInternalNoticesReachResult proves the sink wiring for the internal
// sites that do not need the progress stub: each condition yields exactly one
// notice with its code, kind, name and fixed Message.
func TestRunInternalNoticesReachResult(t *testing.T) {
	t.Parallel()

	polName := managedPolicyName(t, "pol", stopPolicyBody)
	oldGeneration := "pol-aaaaaa"
	if polName == oldGeneration {
		oldGeneration = "pol-bbbbbb"
	}
	zero := 0
	tests := []struct {
		name    string
		routes  []map[string]http.HandlerFunc
		opts    func(t *testing.T, o *Options)
		want    []Notice
		wantErr bool
		check   func(t *testing.T, stub *esstub.Stub)
	}{
		{
			name: "replicas defaulted after a failed _nodes query",
			routes: []map[string]http.HandlerFunc{{
				"HEAD /cards": sequenced(replyHeadMissing, replyHeadOK),
				"GET /_nodes": replyFail,
				"PUT /cards":  replyAck,
				"POST /_bulk": esstub.JSON(http.StatusOK, bulkOKBody("cards")),
			}},
			opts: func(_ *testing.T, o *Options) { o.AddToIndex = true },
			want: []Notice{{Code: NoticeReplicasDefaulted, Message: "Cluster nodes query failed; defaulting to 0 replicas"}},
		},
		{
			// No /_nodes route: an explicit override must not query topology.
			name: "explicit replicas never default",
			routes: []map[string]http.HandlerFunc{{
				"HEAD /cards": sequenced(replyHeadMissing, replyHeadOK),
				"PUT /cards":  replyAck,
				"POST /_bulk": esstub.JSON(http.StatusOK, bulkOKBody("cards")),
			}},
			opts: func(_ *testing.T, o *Options) { o.AddToIndex, o.Replicas = true, &zero },
		},
		{
			name: "pipeline references an unresolvable policy",
			routes: []map[string]http.HandlerFunc{policyListRoutes(esstub.JSON(http.StatusOK, `{"policies":[]}`)), {
				// Ends the Run after resolution; the notice must survive.
				"HEAD /cards": replyFail,
			}},
			opts: func(t *testing.T, o *Options) {
				o.AddToIndex = true
				o.PipelinesFile = writeInputFile(t, "pipelines.json", `{"p":{"processors":[{"enrich":{"policy_name":"ghost","field":"id","target_field":"x"}}]}}`)
			},
			want: []Notice{{
				Code: NoticePipelinePolicyUnresolved, Message: "Pipeline references enrich policy that does not exist as exact or managed policy name",
				ResourceKind: ResourceKindPolicy, Name: "ghost",
			}},
			wantErr: true,
		},
		{
			name: "policy garbage collection rejected",
			routes: []map[string]http.HandlerFunc{policyListRoutes(esstub.JSON(http.StatusOK,
				`{"policies":[{"config":{"match":{"name":"`+polName+`"}}},{"config":{"match":{"name":"`+oldGeneration+`"}}}]}`)),
				{
					"HEAD /cards":                             replyHeadOK,
					"PUT /_enrich/policy/" + polName:          replyAck,
					"GET /_ingest/pipeline":                   esstub.JSON(http.StatusOK, `{}`),
					"DELETE /_enrich/policy/" + oldGeneration: esstub.JSON(http.StatusInternalServerError, `{"error":{"type":"x","reason":"password=hunter2"}}`),
				}},
			opts: func(t *testing.T, o *Options) {
				o.DataFile, o.SyncManaged = "", true
				o.PoliciesFile = writeInputFile(t, "policies.json", syncOnlyPolicies)
			},
			want: []Notice{{Code: NoticePolicyGCFailed, Message: "Managed policy GC could not delete policy", ResourceKind: ResourceKindPolicy, Name: oldGeneration}},
		},
		{
			// One unsupported enrich API, detected by four requests in one Run
			// (delete-name resolution, policy create, GC and enrich listing),
			// is one notice.
			name: "enrich API unsupported at several sites",
			routes: []map[string]http.HandlerFunc{policyListRoutes(esstub.JSON(http.StatusNotFound, noticeProxy404)), {
				"HEAD /cards":                    sequenced(replyHeadMissing, replyHeadOK),
				"GET /_nodes":                    esstub.JSON(http.StatusOK, nodesBody),
				"PUT /cards":                     replyAck,
				"PUT /_enrich/policy/" + polName: esstub.JSON(http.StatusNotFound, noticeProxy404),
				"POST /cards/_refresh":           esstub.JSON(http.StatusOK, refreshBody),
			}},
			opts: func(t *testing.T, o *Options) {
				o.DataFile, o.SyncManaged, o.Enrich = "", true, EnrichOptions{Enabled: true, All: true}
				o.SettingsFile = writeInputFile(t, "settings.json", `{}`)
				o.PoliciesFile = writeInputFile(t, "policies.json", syncOnlyPolicies)
			},
			want: []Notice{{Code: NoticeEnrichAPIUnsupported, Message: enrich.UnsupportedPolicyAPIMessage}},
			check: func(t *testing.T, stub *esstub.Stub) {
				labels := requestLabels(stub)
				if got := operationCount(labels, "GET /_enrich/policy"); got != 3 || !slices.Contains(labels, "PUT /_enrich/policy/"+polName) {
					t.Fatalf("requests = %q, want three listings and the policy create, each detecting the unsupported API", labels)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := newRouteStub(t, tt.routes...)
			opts := Options{URL: stub.URL(), Index: "cards", DataFile: writeBulkDataFixture(t)}
			tt.opts(t, &opts)
			result, err := runNop(t, t.Context(), opts)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Run() error = %v, wantErr %v", err, tt.wantErr)
			}
			requireNoticeList(t, result.Notices, tt.want)
			if len(result.Warnings) != 0 {
				t.Fatalf("Warnings = %q, want none: internal notices never reach Warnings", result.Warnings)
			}
			if tt.check != nil {
				tt.check(t, stub)
			}
		})
	}
}

// TestRunEnrichNoticesReachResult proves the enrich notice wiring through the
// Run-level progress stub. Serial: the progress stub's tests share clocks.
func TestRunEnrichNoticesReachResult(t *testing.T) {
	missing := func(name string) Notice {
		return Notice{Code: NoticeEnrichPolicyMissing, Message: "Enrich policy not found; skipping", ResourceKind: ResourceKindPolicy, Name: name}
	}
	unsupported := Notice{Code: NoticeEnrichAPIUnsupported, Message: "Enrich execute endpoint returned a generic 404; check proxy or routing for /_enrich/policy/<name>/_execute and confirm this URL matches the backend used by Dev Tools"}
	tests := []struct {
		name         string
		stubOpts     []progressStubOption
		enrich       EnrichOptions
		want         []Notice
		wantStatuses []EnrichStatus
	}{
		{
			name:         "requested policy missing",
			enrich:       EnrichOptions{Policies: []string{"policy-a", "ghost"}},
			want:         []Notice{missing("ghost")},
			wantStatuses: []EnrichStatus{EnrichStatusSucceeded, EnrichStatusMissing},
		},
		{
			name:         "no requested policy matched",
			enrich:       EnrichOptions{Policies: []string{"ghost"}},
			want:         []Notice{missing("ghost"), {Code: NoticeEnrichNoMatch, Message: "No enrich policies matched the request"}},
			wantStatuses: []EnrichStatus{EnrichStatusMissing},
		},
		{
			name:     "cluster has no policies",
			stubOpts: []progressStubOption{withPolicies()},
			enrich:   EnrichOptions{Enabled: true, All: true},
			want:     []Notice{{Code: NoticeEnrichNoPolicies, Message: "No enrich policies found; skipping enrich execution"}},
		},
		{
			// Two policies each detect the unsupported execute endpoint: one
			// notice, while each keeps its own "unsupported" outcome.
			name:         "execute endpoint unsupported for every policy",
			stubOpts:     []progressStubOption{withPolicies("policy-a", "policy-b"), withExecuteReply(http.StatusNotFound, noticeProxy404)},
			enrich:       EnrichOptions{Enabled: true, All: true},
			want:         []Notice{unsupported},
			wantStatuses: []EnrichStatus{EnrichStatusUnsupported, EnrichStatusUnsupported},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installFakeClock(t, time.Second)
			server, ops := newProgressStub(t, nil, tt.stubOpts...)
			run := runProgress(t, server, ops, zerolog.InfoLevel, nopLogger, func(o *Options) { o.Enrich = tt.enrich })
			if run.err != nil {
				t.Fatalf("Run: %v", run.err)
			}
			requireNoticeList(t, run.result.Notices, tt.want)
			var statuses []EnrichStatus
			for _, o := range run.result.EnrichOutcomes {
				statuses = append(statuses, o.Status)
			}
			if !slices.Equal(statuses, tt.wantStatuses) {
				t.Fatalf("outcome statuses = %v, want %v", statuses, tt.wantStatuses)
			}
		})
	}
}

// TestRunNoticeNameIsSanitizedAndBounded drives a hostile requested policy
// name through Run: the public Name is redacted, single-line valid UTF-8 and
// within 2,048 bytes, while the raw name never reaches Result.Notices.
func TestRunNoticeNameIsSanitizedAndBounded(t *testing.T) {
	installFakeClock(t, time.Second)
	hostile := "ghost\tpassword=hunter2\x01" + strings.Repeat("é", 1500) + "\xff"
	server, ops := newProgressStub(t, nil)
	run := runProgress(t, server, ops, zerolog.InfoLevel, nopLogger, func(o *Options) {
		o.Enrich = EnrichOptions{Policies: []string{"policy-a", hostile}}
	})
	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}
	if len(run.result.Notices) != 1 || run.result.Notices[0].Code != NoticeEnrichPolicyMissing {
		t.Fatalf("Notices = %+v, want one enrich_policy_missing", run.result.Notices)
	}
	requireSanitizedName(t, run.result.Notices[0].Name)
}

// requireSanitizedName checks the documented Name guarantees.
func requireSanitizedName(t *testing.T, name string) {
	t.Helper()
	if strings.Contains(name, "hunter2") || !strings.Contains(name, "[REDACTED]") {
		t.Fatalf("Name %q is not redacted", name)
	}
	if !utf8.ValidString(name) || strings.ContainsAny(name, "\t\n\x01") {
		t.Fatalf("Name %q is not single-line valid UTF-8", name)
	}
	if len(name) > 2048 || !strings.HasSuffix(name, " [truncated]") {
		t.Fatalf("Name is %d bytes (suffix %q), want at most 2048 ending with the truncation marker", len(name), name[max(0, len(name)-16):])
	}
}

// TestNoticeDeduplicationOncePerRun is the named de-duplication guard
// (operator decision 2026-11-25): one notice per sanitized
// (Code, ResourceKind, Name), the first Message and encounter order kept;
// different names, codes or kinds stay distinct; names that sanitize to the
// same display value collapse; and a second Run starts with an empty seen set.
func TestNoticeDeduplicationOncePerRun(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("é", 1500)
	emit := func(ctx context.Context, code, message, kind, name string) {
		observe.Notify(ctx, observe.Notice{Code: code, Message: message, Kind: kind, Name: name})
	}
	var result Result
	ctx := withRunSink(t.Context(), &result)
	emit(ctx, observe.CodeEnrichPolicyMissing, "first", observe.KindPolicy, "x")
	emit(ctx, observe.CodeEnrichPolicyMissing, "reworded", observe.KindPolicy, "x")
	emit(ctx, observe.CodeEnrichPolicyMissing, "other name", observe.KindPolicy, "y")
	emit(ctx, observe.CodeEnrichAPIUnsupported, "list", "", "")
	emit(ctx, observe.CodeEnrichAPIUnsupported, "execute", "", "")
	emit(ctx, observe.CodeNukeTargetMissing, "alias", observe.KindAlias, "cards")
	emit(ctx, observe.CodeNukeTargetMissing, "index", observe.KindIndex, "cards")
	emit(ctx, observe.CodeDeleteTargetMissing, "other code", observe.KindIndex, "cards")
	emit(ctx, observe.CodeEnrichPolicyMissing, "spaced", observe.KindPolicy, "a\tb")
	emit(ctx, observe.CodeEnrichPolicyMissing, "same display", observe.KindPolicy, " a \n b ")
	emit(ctx, observe.CodeEnrichPolicyMissing, "long", observe.KindPolicy, long+"1")
	emit(ctx, observe.CodeEnrichPolicyMissing, "same after the cut", observe.KindPolicy, long+"2")

	if len(result.Notices) != 8 {
		t.Fatalf("got %d notices, want 8: %+v", len(result.Notices), result.Notices)
	}
	cut := result.Notices[7].Name
	want := []Notice{
		{Code: NoticeEnrichPolicyMissing, Message: "first", ResourceKind: ResourceKindPolicy, Name: "x"},
		{Code: NoticeEnrichPolicyMissing, Message: "other name", ResourceKind: ResourceKindPolicy, Name: "y"},
		{Code: NoticeEnrichAPIUnsupported, Message: "list"},
		{Code: NoticeNukeTargetMissing, Message: "alias", ResourceKind: ResourceKindAlias, Name: "cards"},
		{Code: NoticeNukeTargetMissing, Message: "index", ResourceKind: ResourceKindIndex, Name: "cards"},
		{Code: NoticeDeleteTargetMissing, Message: "other code", ResourceKind: ResourceKindIndex, Name: "cards"},
		{Code: NoticeEnrichPolicyMissing, Message: "spaced", ResourceKind: ResourceKindPolicy, Name: "a b"},
		{Code: NoticeEnrichPolicyMissing, Message: "long", ResourceKind: ResourceKindPolicy, Name: cut},
	}
	requireNoticeList(t, result.Notices, want)
	if len(cut) > 2048 || !strings.HasSuffix(cut, " [truncated]") || !utf8.ValidString(cut) {
		t.Fatalf("long Name is %d bytes, want a valid cut of at most 2048 bytes", len(cut))
	}

	// A second Run's sink has its own seen set.
	var second Result
	emit(withRunSink(t.Context(), &second), observe.CodeEnrichPolicyMissing, "first", observe.KindPolicy, "x")
	if len(second.Notices) != 1 {
		t.Fatalf("second sink Notices = %+v, want its own first notice", second.Notices)
	}
	// Installing a sink without emitting leaves Notices nil.
	var quiet Result
	_ = withRunSink(t.Context(), &quiet)
	if quiet.Notices != nil {
		t.Fatalf("Notices = %+v, want nil", quiet.Notices)
	}
}

// TestRunNoticesAreIsolatedPerRun guards Run isolation: separate and
// concurrent Runs keep their own notices and seen sets (run with -race), and
// a sink on the caller's context is replaced, never fed.
func TestRunNoticesAreIsolatedPerRun(t *testing.T) {
	t.Parallel()

	resetRun := func(ctx context.Context, index string) (Result, error) {
		return runNop(t, ctx, Options{URL: noticeBadURL, Index: index, SyncManaged: true, Nuke: true})
	}

	// Sequential Runs with the same condition each report it.
	for i := 0; i < 2; i++ {
		result, _ := resetRun(t.Context(), "cards")
		if len(result.Notices) != 1 || result.Notices[0].Code != NoticeNukeResetOnly {
			t.Fatalf("run %d Notices = %+v, want its own nuke_reset_only", i, result.Notices)
		}
	}

	// Concurrent Runs keep independent slices.
	const runs = 8
	results := make([]Result, runs)
	var wg sync.WaitGroup
	for i := range runs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], _ = resetRun(t.Context(), fmt.Sprintf("idx-%d", i))
		}()
	}
	wg.Wait()
	for i, result := range results {
		if len(result.Notices) != 1 || result.Notices[0].Name != fmt.Sprintf("idx-%d", i) {
			t.Fatalf("concurrent run %d Notices = %+v, want only its own index", i, result.Notices)
		}
	}

	// An inherited sink on the caller's context is replaced.
	var inherited atomic.Int32
	callerCtx := observe.WithSink(t.Context(), &observe.Sink{OnNotice: func(observe.Notice) { inherited.Add(1) }})
	result, _ := resetRun(callerCtx, "cards")
	if len(result.Notices) != 1 || inherited.Load() != 0 {
		t.Fatalf("Notices = %+v, inherited sink calls = %d; want the Run's own notice and no inherited delivery", result.Notices, inherited.Load())
	}
}

// TestRunNoticesNilAndPartialResults pins nil Notices on an uneventful Run and
// the retention of notices reported before a run-ending failure, including a
// warning before the mapping preflight fails.
func TestRunNoticesNilAndPartialResults(t *testing.T) {
	t.Parallel()

	t.Run("uneventful run", func(t *testing.T) {
		t.Parallel()
		stub := newLifecycleStub(t, lifecycleConfig{index: "cards"})
		result, err := runNop(t, t.Context(), Options{URL: stub.URL(), Index: "cards", AddToIndex: true, DataFile: writeBulkDataFixture(t)})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if result.Notices != nil || result.Warnings != nil {
			t.Fatalf("Notices = %+v, Warnings = %q; want both nil", result.Notices, result.Warnings)
		}
	})

	t.Run("warning before a failed mapping preflight", func(t *testing.T) {
		t.Parallel()
		fixtures := writePreflightRunFixtures(t)
		stub := newPreflightRunStub(t, preflightStubConfig{indexExists: true, liveMappings: preflightLiveMappings})
		result, err := runNop(t, t.Context(), Options{
			URL: stub.URL(), Index: "cards", FlushIndex: true, KeepLast: 1,
			DataFile: fixtures.data, MappingsFile: fixtures.divergentMappings,
		})
		assertRunError(t, err, ErrBulkFailure, "Mapping preflight failed before bulk insert")
		requireNoticeList(t, result.Notices, []Notice{warnNotice(NoticeKeepLastIgnored, "", "", msgKeepLastIgnored)})
		if result.DocumentsProcessed != 0 || result.BulkFailures != nil {
			t.Fatalf("bulk fields = %+v, want zero", result)
		}
	})
}

// TestRunExcludedConditionsReportNoNotice guards the documented exclusions
// that run without fake clocks: nuke intent lines, the timestamped-name
// collision advance, a retried bulk request and the bulk failed-items summary
// (which still fills the sample).
func TestRunExcludedConditionsReportNoNotice(t *testing.T) {
	t.Parallel()

	t.Run("nuke of an existing index", func(t *testing.T) {
		t.Parallel()
		stub := newRouteStub(t, map[string]http.HandlerFunc{"HEAD /cards": replyHeadOK, "DELETE /cards": replyAck})
		result, err := runNop(t, t.Context(), Options{URL: stub.URL(), Index: "cards", Nuke: true})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		requireNoticeList(t, result.Notices, nil)
	})

	t.Run("nuke of alias targets and generations", func(t *testing.T) {
		t.Parallel()
		stub := newRouteStub(t, map[string]http.HandlerFunc{
			"GET /_alias/cards":            esstub.JSON(http.StatusOK, `{"cards-a":{"aliases":{"cards":{}}}}`),
			"DELETE /cards-a":              replyAck,
			"GET /cards-*":                 esstub.JSON(http.StatusOK, `{"cards-20260101000000":{}}`),
			"DELETE /cards-20260101000000": replyAck,
		})
		result, err := runNop(t, t.Context(), Options{URL: stub.URL(), Index: "cards", AliasMode: true, Nuke: true})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if got := operationCount(requestLabels(stub), "DELETE /cards-a") + operationCount(requestLabels(stub), "DELETE /cards-20260101000000"); got != 2 {
			t.Fatalf("requests = %q, want both nuke deletes", requestLabels(stub))
		}
		requireNoticeList(t, result.Notices, nil)
	})

	t.Run("timestamped name collision advance", func(t *testing.T) {
		t.Parallel()
		var (
			mu       sync.Mutex
			collided bool
			present  = map[string]bool{}
		)
		stub := esstub.New(t)
		stub.Handle("GET /_alias/cards", esstub.JSON(http.StatusNotFound, `{"error":{"type":"aliases_not_found_exception"},"status":404}`))
		stub.Handle("GET /_nodes", esstub.JSON(http.StatusOK, nodesBody))
		stub.Handle("POST /_bulk", esstub.JSON(http.StatusOK, bulkOKBody("cards")))
		stub.Handle("POST /_aliases", replyAck)
		// The first timestamped candidate is taken, so the name advances.
		stub.Handle("/{name}", func(w http.ResponseWriter, r *http.Request) {
			name := r.PathValue("name")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case r.Method == http.MethodPut:
				present[name] = true
				_, _ = w.Write([]byte(ackBody))
			case r.Method == http.MethodHead && strings.HasPrefix(name, "cards-") && !collided:
				collided = true
				w.WriteHeader(http.StatusOK)
			case r.Method == http.MethodHead:
				existsReply(w, present[name])
			default:
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusInternalServerError)
			}
		})
		result, err := runNop(t, t.Context(), Options{URL: stub.URL(), Index: "cards", AliasMode: true, AddToIndex: true, DataFile: writeBulkDataFixture(t)})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		mu.Lock()
		advanced := collided
		mu.Unlock()
		if !advanced || result.CreatedIndex == "" {
			t.Fatalf("collision = %v, created %q; want an advanced timestamped index", advanced, result.CreatedIndex)
		}
		requireNoticeList(t, result.Notices, nil)
	})

	t.Run("retried bulk transport error", func(t *testing.T) {
		t.Parallel()
		stub, attempts := newCardsBulkStub(t, bulkAfter(func(w http.ResponseWriter) { dropConnection(t, w) }))
		result, err := runNop(t, t.Context(), Options{
			URL: stub.URL(), Index: "cards", AddToIndex: true, DataFile: writeBulkDataFixture(t),
			BulkRetryBackoffBase: time.Millisecond, BulkRetryBackoffMax: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if attempts.Load() != 2 || result.DocumentsFailed != 0 {
			t.Fatalf("attempts = %d, result = %+v; want one transport retry that succeeds", attempts.Load(), result)
		}
		requireNoticeList(t, result.Notices, nil)
	})

	t.Run("retried bulk request with rejected items", func(t *testing.T) {
		t.Parallel()
		stub, attempts := newCardsBulkStub(t, func(w http.ResponseWriter, _ *http.Request, attempt int) {
			if attempt == 1 {
				replyTransient503(w)
				return
			}
			_, _ = w.Write([]byte(`{"errors":true,"items":[{"index":{"_index":"cards","_id":"1","status":400,"error":{"type":"mapper_parsing_exception","reason":"boom"}}}]}`))
		})
		result, err := runNop(t, t.Context(), Options{
			URL: stub.URL(), Index: "cards", AddToIndex: true, DataFile: writeBulkDataFixture(t),
			BulkRetryBackoffBase: time.Millisecond, BulkRetryBackoffMax: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if attempts.Load() != 2 || result.DocumentsFailed != 1 || len(result.BulkFailures) != 1 {
			t.Fatalf("attempts = %d, result = %+v; want a retry and one sampled rejection", attempts.Load(), result)
		}
		requireNoticeList(t, result.Notices, nil)
	})
}

// TestRunPollExclusionsReportNoNotice guards the excluded poll conditions
// through Run: a retried enrich task poll, an enrich cancellation (which keeps
// its "cancelled" outcome) and a retried transform _stats poll. Serial: they
// replace the package clocks.
func TestRunPollExclusionsReportNoNotice(t *testing.T) {
	t.Run("retried enrich task poll", func(t *testing.T) {
		installFakeClock(t, time.Second)
		server, ops := newProgressStub(t, nil, withEnrichTask(
			stubReply{http.StatusServiceUnavailable, `{"error":{"type":"busy","reason":"retry"}}`},
			stubReply{http.StatusOK, `{"completed":true,"response":{"status":{"phase":"COMPLETE"}}}`},
		))
		run := runProgress(t, server, ops, zerolog.InfoLevel, nopLogger, withEnrich)
		if run.err != nil || run.result.EnrichSucceeded != 1 || operationCount(run.ops, "enrich.task") != 2 {
			t.Fatalf("err = %v, result = %+v, ops = %v; want a retried poll that succeeds", run.err, run.result, run.ops)
		}
		requireNoticeList(t, run.result.Notices, nil)
	})

	t.Run("enrich cancellation", func(t *testing.T) {
		installFakeClock(t, time.Second)
		server, ops := newProgressStub(t, nil, withEnrichTask(stubReply{http.StatusOK, `{"completed":false,"task":{"status":{"phase":"RUNNING"}}}`}))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		sleepWithContext = func(ctx context.Context, _ time.Duration) error {
			cancel()
			return ctx.Err()
		}
		run := runProgressContext(t, ctx, server, ops, zerolog.InfoLevel, nopLogger, withEnrich)
		if run.err != nil || len(run.result.EnrichOutcomes) != 1 || run.result.EnrichOutcomes[0].Status != EnrichStatusCancelled {
			t.Fatalf("err = %v, outcomes = %+v; want one cancelled outcome", run.err, run.result.EnrichOutcomes)
		}
		requireNoticeList(t, run.result.Notices, nil)
	})

	t.Run("retried transform _stats poll", func(t *testing.T) {
		installFakeClock(t, time.Second)
		// The first poll fails with a retryable status and is retried; the
		// sleep waits on its context, so the wait timeout ends the Run.
		sleepWithContext = func(ctx context.Context, _ time.Duration) error {
			<-ctx.Done()
			return ctx.Err()
		}
		server, ops := newProgressStub(t, []string{`{"error":{"type":"busy"}}`}, withStatsStatus(http.StatusServiceUnavailable))
		run := runProgress(t, server, ops, zerolog.InfoLevel, nopLogger, withTransforms(t, false, true), func(o *Options) {
			o.Transforms.WaitTimeout = time.Millisecond
		})
		if run.err == nil || !strings.Contains(run.err.Error(), "last poll failed") {
			t.Fatalf("error = %v, want the wait timeout naming the retried poll", run.err)
		}
		requireNoticeList(t, run.result.Notices, nil)
	})
}

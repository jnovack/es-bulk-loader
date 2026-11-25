package managed

// Phase 4 (loader-observability) guards: each managed notice site emits its
// observe notice beside the unchanged Warn log, with the fixed log message and
// never response text, and the retry, nuke-intent, already-absent, success and
// terminal paths emit none. De-duplication is the loader's job, so these
// tests count raw emits.

import (
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/bodytransport"
	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/enrich"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/noticetest"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"
)

// proxy404 is a generic (unsupported-API) 404 carrying a secret, so a notice
// that copied response text would leak it.
const proxy404 = `{"error":"proxy route missing password=hunter2"}`

func replies(r ...func() bodytransport.Reply) *bodytransport.Transport {
	return &bodytransport.Transport{Replies: r}
}

var unsupportedNotice = observe.Notice{Code: observe.CodeEnrichAPIUnsupported, Message: enrich.UnsupportedPolicyAPIMessage}

// TestCreatePoliciesNotices guards the create producer of
// enrich_api_unsupported (which skips every policy and returns nil) and the
// create paths that must stay silent, including the index_not_found retry.
func TestCreatePoliciesNotices(t *testing.T) {
	t.Parallel()

	definitions := config.Definitions{"pol-aaaaaa": []byte(`{"match":{}}`), "pol-bbbbbb": []byte(`{"match":{}}`)}
	names := []string{"pol-aaaaaa", "pol-bbbbbb"}
	ack := bodytransport.Static(http.StatusOK, `{"acknowledged":true}`)
	tests := []struct {
		name      string
		replies   []func() bodytransport.Reply
		wantErr   bool
		wantPaths int
		want      []observe.Notice
	}{
		{"generic 404 skips every policy", []func() bodytransport.Reply{bodytransport.Static(http.StatusNotFound, proxy404)}, false, 1, []observe.Notice{unsupportedNotice}},
		{"success emits nothing", []func() bodytransport.Reply{ack}, false, 2, nil},
		{"index_not_found retry emits nothing", []func() bodytransport.Reply{
			bodytransport.Static(http.StatusBadRequest, `{"error":{"type":"index_not_found_exception","reason":"FAKE"}}`), ack,
		}, false, 3, nil},
		{"already exists emits nothing", []func() bodytransport.Reply{bodytransport.Static(http.StatusBadRequest, `{"error":{"type":"resource_already_exists_exception"}}`)}, false, 2, nil},
		{"terminal rejection emits nothing", []func() bodytransport.Reply{bodytransport.Static(http.StatusInternalServerError, `{"error":{"type":"x","reason":"FAKE"}}`)}, true, 1, nil},
		{"oversized 404 emits nothing", []func() bodytransport.Reply{bodytransport.Static(http.StatusNotFound, strings.Repeat(" ", esresponse.MaxRejectionBodyBytes+10))}, true, 1, nil},
		{"unreadable 404 emits nothing", []func() bodytransport.Reply{readFailed(http.StatusNotFound, `{`, errors.New("read failed"))}, true, 1, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, rec := noticetest.New(t)
			tr := replies(tt.replies...)
			err := CreatePolicies(ctx, tr.Client(t), definitions, names, sleepUntilDone)
			if (err != nil) != tt.wantErr {
				t.Fatalf("CreatePolicies error = %v, wantErr %v", err, tt.wantErr)
			}
			if got := len(tr.Paths()); got != tt.wantPaths {
				t.Fatalf("requests = %q, want %d", tr.Paths(), tt.wantPaths)
			}
			noticetest.RequireNotices(t, rec.Notices, tt.want, "hunter2", "proxy route", "FAKE")
		})
	}
}

// TestDeletePoliciesNotices guards the delete producer of
// enrich_api_unsupported (which skips the remaining policies and returns nil)
// and the delete paths that must stay silent.
func TestDeletePoliciesNotices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		reply   func() bodytransport.Reply
		wantErr bool
		want    []observe.Notice
	}{
		{"generic 404", bodytransport.Static(http.StatusNotFound, proxy404), false, []observe.Notice{unsupportedNotice}},
		{"absent policy emits nothing", bodytransport.Static(http.StatusNotFound, `{"error":{"type":"resource_not_found_exception"}}`), false, nil},
		{"success emits nothing", bodytransport.Static(http.StatusOK, `{"acknowledged":true}`), false, nil},
		{"terminal rejection emits nothing", bodytransport.Static(http.StatusInternalServerError, `{"error":{"type":"x","reason":"FAKE"}}`), true, nil},
		{"oversized 404 emits nothing", bodytransport.Static(http.StatusNotFound, strings.Repeat(" ", esresponse.MaxRejectionBodyBytes+10)), true, nil},
		{"unreadable 404 emits nothing", readFailed(http.StatusNotFound, `{`, errors.New("read failed")), true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, rec := noticetest.New(t)
			err := deletePolicies(ctx, replies(tt.reply).Client(t), []string{"pol"}, false)
			if (err != nil) != tt.wantErr {
				t.Fatalf("deletePolicies error = %v, wantErr %v", err, tt.wantErr)
			}
			noticetest.RequireNotices(t, rec.Notices, tt.want, "hunter2", "proxy route", "FAKE")
		})
	}
}

// TestPolicyGCNotices guards both policy_gc_failed producers (transport and
// HTTP rejection), each naming the policy with kind policy, and the GC paths
// that must stay silent: an already-absent policy (404) and a success.
func TestPolicyGCNotices(t *testing.T) {
	t.Parallel()

	gcFailed := func(message string) []observe.Notice {
		return []observe.Notice{{Code: observe.CodePolicyGCFailed, Message: message, Kind: observe.KindPolicy, Name: "pol-aaaaaa"}}
	}
	tests := []struct {
		name  string
		reply func() bodytransport.Reply
		want  []observe.Notice
	}{
		{"transport failure", func() bodytransport.Reply { return bodytransport.Reply{Err: errors.New("dial FAKE")} }, gcFailed("Managed policy GC failed while deleting policy")},
		{"HTTP rejection", bodytransport.Static(http.StatusConflict, `{"error":{"type":"x","reason":"FAKE password=hunter2"}}`), gcFailed("Managed policy GC could not delete policy")},
		{"already absent emits nothing", bodytransport.Static(http.StatusNotFound, `{"error":{"type":"resource_not_found_exception"}}`), nil},
		{"success emits nothing", bodytransport.Static(http.StatusOK, `{"acknowledged":true}`), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, rec := noticetest.New(t)
			deletePolicyBestEffort(ctx, replies(tt.reply).Client(t), "pol-aaaaaa")
			noticetest.RequireNotices(t, rec.Notices, tt.want, "hunter2", "proxy route", "FAKE")
		})
	}
}

// TestGarbageCollectPoliciesUnsupportedListNotice guards that GC's policy
// listing is one of the enrich_api_unsupported producers Run reaches.
func TestGarbageCollectPoliciesUnsupportedListNotice(t *testing.T) {
	t.Parallel()

	ctx, rec := noticetest.New(t)
	tr := replies(bodytransport.Static(http.StatusNotFound, proxy404))
	if err := GarbageCollectPolicies(ctx, tr.Client(t), []string{"pol"}, nil); err != nil {
		t.Fatalf("GarbageCollectPolicies error = %v", err)
	}
	noticetest.RequireNotices(t, rec.Notices, []observe.Notice{unsupportedNotice}, "hunter2", "proxy route", "FAKE")
}

// TestResolvePipelinePolicyFallbacksNotice guards pipeline_policy_unresolved:
// only a reference with no exact or managed match emits, naming the logical
// policy with kind policy.
func TestResolvePipelinePolicyFallbacksNotice(t *testing.T) {
	t.Parallel()

	unresolved := []observe.Notice{{
		Code:    observe.CodePipelinePolicyUnresolved,
		Message: "Pipeline references enrich policy that does not exist as exact or managed policy name",
		Kind:    observe.KindPolicy,
		Name:    "enrich-cards",
	}}
	tests := []struct {
		name      string
		available []string
		premapped map[string]string
		want      []observe.Notice
	}{
		{"no match", []string{"other-aaaaaa"}, nil, unresolved},
		{"empty cluster", nil, nil, unresolved},
		{"exact match emits nothing", []string{"enrich-cards"}, nil, nil},
		{"single managed match emits nothing", []string{"enrich-cards-aaaaaa"}, nil, nil},
		{"declared mapping emits nothing", nil, map[string]string{"enrich-cards": "enrich-cards-cccccc"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, rec := noticetest.New(t)
			mapping := map[string]string{}
			for k, v := range tt.premapped {
				mapping[k] = v
			}
			if err := ResolvePipelinePolicyFallbacks(ctx, enrichPolicyListServer(t, tt.available...), policyResolutionPipelines, []string{"p"}, mapping); err != nil {
				t.Fatalf("ResolvePipelinePolicyFallbacks error = %v", err)
			}
			noticetest.RequireNotices(t, rec.Notices, tt.want, "hunter2", "proxy route", "FAKE")
		})
	}
}

// TestNukeRecoveryEmitsNoNotice guards the nuke intent exclusions: deleting
// the pipelines that reference a policy, and clearing index.default_pipeline
// (an acknowledged settings change logged at Warn) before deleting a
// pipeline, are requested cluster changes, not notices.
func TestNukeRecoveryEmitsNoNotice(t *testing.T) {
	t.Parallel()

	var policyDeletes, pipelineDeletes atomic.Int32
	stub := esstub.New(t)
	stub.Handle("DELETE /_ingest/pipeline/declared-pipeline", func(w http.ResponseWriter, _ *http.Request) {
		if pipelineDeletes.Add(1) == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"reason":"pipeline [declared-pipeline] cannot be deleted because it is the default pipeline for 1 index(es) including [other-index]"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	})
	stub.Handle("GET /_settings/*", esstub.JSON(http.StatusOK, `{"other-index":{"settings":{"index":{"default_pipeline":"declared-pipeline"}}}}`))
	stub.Handle("PUT /other-index/_settings", esstub.JSON(http.StatusOK, `{"acknowledged":true}`))
	stub.Handle("DELETE /_enrich/policy/pol", func(w http.ResponseWriter, _ *http.Request) {
		if policyDeletes.Add(1) == 1 {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":{"reason":"Could not delete policy [pol] because a pipeline is referencing it [ref-pipeline]"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	})
	stub.Handle("GET /_ingest/pipeline", esstub.JSON(http.StatusOK, `{"ref-pipeline":{"processors":[{"enrich":{"policy_name":"pol"}}]}}`))
	stub.Handle("DELETE /_ingest/pipeline/ref-pipeline", esstub.JSON(http.StatusOK, `{"acknowledged":true}`))

	ctx, rec := noticetest.New(t)
	if err := DeleteResources(ctx, stub.MustClient(t), []string{"declared-pipeline"}, []string{"pol"}, nil, true); err != nil {
		t.Fatalf("DeleteResources error = %v", err)
	}
	if pipelineDeletes.Load() != 2 || policyDeletes.Load() != 2 {
		t.Fatalf("pipeline/policy delete attempts = %d/%d, want 2/2: the recovery paths did not run", pipelineDeletes.Load(), policyDeletes.Load())
	}
	noticetest.RequireNotices(t, rec.Notices, nil, "hunter2", "proxy route", "FAKE")
}

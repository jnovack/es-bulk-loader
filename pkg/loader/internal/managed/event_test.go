package managed

// Phase 5 (loader-observability) guards: each managed success branch emits
// exactly one lifecycle event, immediately after that resource's acknowledged
// response, and every skip, 404, unsupported-API and failure branch emits
// none. The recorder counts raw emits; Name sanitization is the loader's job.

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/bodytransport"
	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/noticetest"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"
)

func ev(action, kind, name string) observe.Event {
	return observe.Event{Action: action, Kind: kind, Name: name}
}

const ackJSON = `{"acknowledged":true}`

// TestCreatePipelinesEvents guards one upserted event per acknowledged PUT,
// delivered before the next pipeline is attempted: the first resource's event
// survives a later failure, and the failed resource has none.
func TestCreatePipelinesEvents(t *testing.T) {
	t.Parallel()

	definitions := config.Definitions{"p1": []byte(`{"processors":[]}`), "p2": []byte(`{"processors":[]}`)}
	names := []string{"p1", "p2"}
	tests := []struct {
		name    string
		replies []func() bodytransport.Reply
		wantErr bool
		want    []observe.Event
	}{
		{"both succeed", []func() bodytransport.Reply{bodytransport.Static(http.StatusOK, ackJSON)}, false,
			[]observe.Event{ev(observe.ActionUpserted, observe.KindPipeline, "p1"), ev(observe.ActionUpserted, observe.KindPipeline, "p2")}},
		{"second fails keeps the first", []func() bodytransport.Reply{
			bodytransport.Static(http.StatusOK, ackJSON), bodytransport.Static(http.StatusInternalServerError, `{"error":{"type":"x","reason":"FAKE"}}`),
		}, true, []observe.Event{ev(observe.ActionUpserted, observe.KindPipeline, "p1")}},
		{"first fails emits nothing", []func() bodytransport.Reply{bodytransport.Static(http.StatusBadRequest, `{"error":{"type":"x"}}`)}, true, nil},
		{"transport error emits nothing", []func() bodytransport.Reply{
			bodytransport.Fail(errors.New("dial FAKE")),
		}, true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, rec := noticetest.New(t)
			err := CreatePipelines(ctx, replies(tt.replies...).Client(t), definitions, names)
			if (err != nil) != tt.wantErr {
				t.Fatalf("CreatePipelines error = %v, wantErr %v", err, tt.wantErr)
			}
			noticetest.RequireEvents(t, rec.Events, tt.want)
		})
	}
}

// TestCreatePoliciesEvents guards the policy upsert event: not on the
// already-exists branch, the unsupported API, a terminal rejection or a retry
// that has not yet succeeded; a retry that eventually succeeds emits once.
func TestCreatePoliciesEvents(t *testing.T) {
	t.Parallel()

	definitions := config.Definitions{"pol-aaaaaa": []byte(`{"match":{}}`), "pol-bbbbbb": []byte(`{"match":{}}`)}
	names := []string{"pol-aaaaaa", "pol-bbbbbb"}
	ack := bodytransport.Static(http.StatusOK, ackJSON)
	both := []observe.Event{ev(observe.ActionUpserted, observe.KindPolicy, "pol-aaaaaa"), ev(observe.ActionUpserted, observe.KindPolicy, "pol-bbbbbb")}
	tests := []struct {
		name    string
		replies []func() bodytransport.Reply
		wantErr bool
		want    []observe.Event
	}{
		{"success", []func() bodytransport.Reply{ack}, false, both},
		{"retry that eventually succeeds emits once per policy", []func() bodytransport.Reply{
			bodytransport.Static(http.StatusBadRequest, `{"error":{"type":"index_not_found_exception","reason":"FAKE"}}`), ack, ack,
		}, false, both},
		{"already exists emits nothing", []func() bodytransport.Reply{bodytransport.Static(http.StatusBadRequest, `{"error":{"type":"resource_already_exists_exception"}}`)}, false, nil},
		{"unsupported API emits nothing", []func() bodytransport.Reply{bodytransport.Static(http.StatusNotFound, proxy404)}, false, nil},
		{"second rejected keeps the first", []func() bodytransport.Reply{
			ack, bodytransport.Static(http.StatusInternalServerError, `{"error":{"type":"x","reason":"FAKE"}}`),
		}, true, both[:1]},
		{"terminal rejection emits nothing", []func() bodytransport.Reply{bodytransport.Static(http.StatusInternalServerError, `{"error":{"type":"x"}}`)}, true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, rec := noticetest.New(t)
			err := CreatePolicies(ctx, replies(tt.replies...).Client(t), definitions, names, sleepUntilDone)
			if (err != nil) != tt.wantErr {
				t.Fatalf("CreatePolicies error = %v, wantErr %v", err, tt.wantErr)
			}
			noticetest.RequireEvents(t, rec.Events, tt.want)
		})
	}
}

// TestDeleteEvents guards the deleted event of the managed delete helpers: one
// per acknowledged DELETE, none for a 404, an unsupported API or a failure.
func TestDeleteEvents(t *testing.T) {
	t.Parallel()

	absent := `{"error":{"type":"resource_not_found_exception"}}`
	tests := []struct {
		name    string
		call    func(ctx context.Context, tr *bodytransport.Transport, t *testing.T) error
		replies []func() bodytransport.Reply
		wantErr bool
		want    []observe.Event
	}{
		{"pipeline deleted", callDeletePipelines, []func() bodytransport.Reply{bodytransport.Static(http.StatusOK, ackJSON)}, false,
			[]observe.Event{ev(observe.ActionDeleted, observe.KindPipeline, "x")}},
		{"pipeline 404", callDeletePipelines, []func() bodytransport.Reply{bodytransport.Static(http.StatusNotFound, absent)}, false, nil},
		{"pipeline failure", callDeletePipelines, []func() bodytransport.Reply{bodytransport.Static(http.StatusInternalServerError, `{"error":{"type":"x"}}`)}, true, nil},
		{"policy deleted", callDeletePolicies, []func() bodytransport.Reply{bodytransport.Static(http.StatusOK, ackJSON)}, false,
			[]observe.Event{ev(observe.ActionDeleted, observe.KindPolicy, "x")}},
		{"policy 404", callDeletePolicies, []func() bodytransport.Reply{bodytransport.Static(http.StatusNotFound, absent)}, false, nil},
		{"policy unsupported API", callDeletePolicies, []func() bodytransport.Reply{bodytransport.Static(http.StatusNotFound, proxy404)}, false, nil},
		{"policy failure", callDeletePolicies, []func() bodytransport.Reply{bodytransport.Static(http.StatusInternalServerError, `{"error":{"type":"x"}}`)}, true, nil},
		{"nuke pipeline deleted", callDeletePipelinesForNuke, []func() bodytransport.Reply{bodytransport.Static(http.StatusOK, ackJSON)}, false,
			[]observe.Event{ev(observe.ActionDeleted, observe.KindPipeline, "x")}},
		{"nuke pipeline 404", callDeletePipelinesForNuke, []func() bodytransport.Reply{bodytransport.Static(http.StatusNotFound, absent)}, false, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, rec := noticetest.New(t)
			err := tt.call(ctx, replies(tt.replies...), t)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			noticetest.RequireEvents(t, rec.Events, tt.want)
		})
	}
}

func callDeletePipelines(ctx context.Context, tr *bodytransport.Transport, t *testing.T) error {
	return deletePipelines(ctx, tr.Client(t), []string{"x"})
}

func callDeletePolicies(ctx context.Context, tr *bodytransport.Transport, t *testing.T) error {
	return deletePolicies(ctx, tr.Client(t), []string{"x"}, false)
}

func callDeletePipelinesForNuke(ctx context.Context, tr *bodytransport.Transport, t *testing.T) error {
	return deletePipelinesForNuke(ctx, tr.Client(t), []string{"x"})
}

// TestPolicyGCEvents guards the GC deleted event and that a best-effort
// failure (a notice) neither emits a deleted event nor suppresses the later
// policy's event.
func TestPolicyGCEvents(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	stub := esstub.New(t)
	stub.Handle("GET /_enrich/policy", esstub.JSON(http.StatusOK,
		`{"policies":[{"config":{"match":{"name":"pol-aaaaaa"}}},{"config":{"match":{"name":"pol-bbbbbb"}}},{"config":{"match":{"name":"pol-cccccc"}}}]}`))
	stub.Handle("GET /_ingest/pipeline", esstub.JSON(http.StatusOK, `{}`))
	stub.Handle("DELETE /_enrich/policy/{name}", func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		switch r.PathValue("name") {
		case "pol-aaaaaa":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"type":"x","reason":"password=hunter2"}}`))
		case "pol-bbbbbb":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"type":"resource_not_found_exception"}}`))
		default:
			_, _ = w.Write([]byte(ackJSON))
		}
	})

	ctx, rec := noticetest.New(t)
	if err := GarbageCollectPolicies(ctx, stub.MustClient(t), []string{"pol"}, map[string]struct{}{}); err != nil {
		t.Fatalf("GarbageCollectPolicies error = %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("delete attempts = %d, want 3", attempts.Load())
	}
	noticetest.RequireEvents(t, rec.Events, []observe.Event{ev(observe.ActionDeleted, observe.KindPolicy, "pol-cccccc")})
	if len(rec.Notices) != 1 || rec.Notices[0].Code != observe.CodePolicyGCFailed || rec.Notices[0].Name != "pol-aaaaaa" {
		t.Fatalf("notices = %+v, want one policy_gc_failed for pol-aaaaaa", rec.Notices)
	}
	for _, e := range rec.Events {
		if strings.Contains(e.Name, "hunter2") {
			t.Fatalf("event leaked response text: %+v", e)
		}
	}
}

// TestNukeRecoveryEvents guards the nuke recovery delete paths: the blocked
// first attempt emits nothing, and the retry that succeeds emits one event,
// after the referencing pipeline's own delete and any default_pipeline
// settings change that made it possible.
func TestNukeRecoveryEvents(t *testing.T) {
	t.Parallel()

	var policyDeletes, pipelineDeletes atomic.Int32
	stub := esstub.New(t)
	stub.Handle("DELETE /_ingest/pipeline/declared-pipeline", func(w http.ResponseWriter, _ *http.Request) {
		if pipelineDeletes.Add(1) == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"reason":"pipeline [declared-pipeline] cannot be deleted because it is the default pipeline for 1 index(es) including [other-index]"}}`))
			return
		}
		_, _ = w.Write([]byte(ackJSON))
	})
	stub.Handle("GET /_settings/*", esstub.JSON(http.StatusOK, `{"other-index":{"settings":{"index":{"default_pipeline":"declared-pipeline"}}}}`))
	stub.Handle("PUT /other-index/_settings", esstub.JSON(http.StatusOK, ackJSON))
	stub.Handle("DELETE /_enrich/policy/pol", func(w http.ResponseWriter, _ *http.Request) {
		if policyDeletes.Add(1) == 1 {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":{"reason":"Could not delete policy [pol] because a pipeline is referencing it [ref-pipeline]"}}`))
			return
		}
		_, _ = w.Write([]byte(ackJSON))
	})
	stub.Handle("GET /_ingest/pipeline", esstub.JSON(http.StatusOK, `{"ref-pipeline":{"processors":[{"enrich":{"policy_name":"pol"}}]}}`))
	stub.Handle("DELETE /_ingest/pipeline/ref-pipeline", esstub.JSON(http.StatusOK, ackJSON))

	ctx, rec := noticetest.New(t)
	if err := DeleteResources(ctx, stub.MustClient(t), []string{"declared-pipeline"}, []string{"pol"}, nil, true); err != nil {
		t.Fatalf("DeleteResources error = %v", err)
	}
	noticetest.RequireEvents(t, rec.Events, []observe.Event{
		ev(observe.ActionSettingsUpdated, observe.KindIndex, "other-index"),
		ev(observe.ActionDeleted, observe.KindPipeline, "declared-pipeline"),
		ev(observe.ActionDeleted, observe.KindPipeline, "ref-pipeline"),
		ev(observe.ActionDeleted, observe.KindPolicy, "pol"),
	})
}

package transform

// Phase 5 (loader-observability) guards: each transform success emits one
// lifecycle event, an existing transform is replaced as deleted then upserted
// (not suppressed), and a missing, already-stopped or already-running
// transform and every failure emit none.

import (
	"context"
	"net/http"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/noticetest"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"
)

func tev(action, name string) observe.Event {
	return observe.Event{Action: action, Kind: observe.KindTransform, Name: name}
}

func TestStopStartDeleteEvents(t *testing.T) {
	t.Parallel()

	ack := esstub.JSON(http.StatusOK, `{"acknowledged":true}`)
	reject := esstub.JSON(http.StatusInternalServerError, `{"error":{"type":"x","reason":"FAKE"}}`)
	missing := esstub.JSON(http.StatusNotFound, `{"error":{"type":"resource_not_found_exception"}}`)
	stopped := esstub.JSON(http.StatusConflict, `{"error":{"reason":"Cannot stop transform [tx] as it is already stopped"}}`)
	running := esstub.JSON(http.StatusConflict, `{"error":{"type":"resource_already_exists_exception","reason":"already started"}}`)

	stop := func(ctx context.Context, es *elasticsearch.Client) error {
		return StopBestEffort(ctx, es, []string{"tx"})
	}
	del := func(ctx context.Context, es *elasticsearch.Client) error { return Delete(ctx, es, []string{"tx"}) }
	start := func(ctx context.Context, es *elasticsearch.Client) error { return Start(ctx, es, []string{"tx"}) }
	tests := []struct {
		name    string
		route   string
		reply   http.HandlerFunc
		call    func(context.Context, *elasticsearch.Client) error
		wantErr bool
		want    []observe.Event
	}{
		{"stop ok", "POST /_transform/tx/_stop", ack, stop, false, []observe.Event{tev(observe.ActionStopped, "tx")}},
		{"stop not running (404)", "POST /_transform/tx/_stop", missing, stop, false, nil},
		{"stop already stopped", "POST /_transform/tx/_stop", stopped, stop, false, nil},
		{"stop rejected", "POST /_transform/tx/_stop", reject, stop, true, nil},
		{"delete ok", "DELETE /_transform/tx", ack, del, false, []observe.Event{tev(observe.ActionDeleted, "tx")}},
		{"delete 404", "DELETE /_transform/tx", missing, del, false, nil},
		{"delete rejected", "DELETE /_transform/tx", reject, del, true, nil},
		{"start ok", "POST /_transform/tx/_start", ack, start, false, []observe.Event{tev(observe.ActionStarted, "tx")}},
		{"start already running", "POST /_transform/tx/_start", running, start, false, nil},
		{"start rejected", "POST /_transform/tx/_start", reject, start, true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stub := esstub.New(t)
			stub.Handle(tt.route, tt.reply)
			ctx, rec := noticetest.New(t)
			err := tt.call(ctx, stub.MustClient(t))
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			noticetest.RequireEvents(t, rec.Events, tt.want)
		})
	}
}

// TestCreateOrUpdateEvents guards the upsert event and the real sequence for an
// existing transform: deleted then upserted. A failed PUT after the delete
// keeps the delete event and has no upsert; a second transform whose PUT fails
// keeps the first transform's event.
func TestCreateOrUpdateEvents(t *testing.T) {
	t.Parallel()

	definitions := config.Definitions{"a": []byte(`{}`), "b": []byte(`{}`)}
	tests := []struct {
		name     string
		existing map[string]bool
		failPut  string
		wantErr  bool
		want     []observe.Event
	}{
		{"new transforms", nil, "", false, []observe.Event{tev(observe.ActionUpserted, "a"), tev(observe.ActionUpserted, "b")}},
		{"existing transform is deleted then upserted", map[string]bool{"a": true}, "", false,
			[]observe.Event{tev(observe.ActionDeleted, "a"), tev(observe.ActionUpserted, "a"), tev(observe.ActionUpserted, "b")}},
		{"put fails after the delete", map[string]bool{"a": true}, "a", true, []observe.Event{tev(observe.ActionDeleted, "a")}},
		{"second put fails keeps the first", nil, "b", true, []observe.Event{tev(observe.ActionUpserted, "a")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stub := esstub.New(t)
			stub.Handle("GET /_transform/{id}", func(w http.ResponseWriter, r *http.Request) {
				if tt.existing[r.PathValue("id")] {
					_, _ = w.Write([]byte(`{"count":1,"transforms":[{}]}`))
					return
				}
				_, _ = w.Write([]byte(`{"count":0,"transforms":[]}`))
			})
			stub.Handle("DELETE /_transform/{id}", esstub.JSON(http.StatusOK, `{"acknowledged":true}`))
			stub.Handle("PUT /_transform/{id}", func(w http.ResponseWriter, r *http.Request) {
				if r.PathValue("id") == tt.failPut {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(`{"error":{"type":"x"}}`))
					return
				}
				_, _ = w.Write([]byte(`{"acknowledged":true}`))
			})
			ctx, rec := noticetest.New(t)
			err := CreateOrUpdate(ctx, stub.MustClient(t), definitions, []string{"a", "b"})
			if (err != nil) != tt.wantErr {
				t.Fatalf("CreateOrUpdate error = %v, wantErr %v", err, tt.wantErr)
			}
			noticetest.RequireEvents(t, rec.Events, tt.want)
		})
	}
}

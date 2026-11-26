package indexops

// Phase 5 (loader-observability) guards: each indexops success emits exactly
// one lifecycle event, only after the response is fully acknowledged (Flush:
// after the body decodes with no shard failures), and every rejection, transport
// error and malformed 2xx body emits none.

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/bodytransport"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/noticetest"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"
)

func event(action, name string) []observe.Event {
	return []observe.Event{{Action: action, Kind: observe.KindIndex, Name: name}}
}

func TestIndexEvents(t *testing.T) {
	t.Parallel()

	rejected := bodytransport.Static(http.StatusInternalServerError, `{"error":{"type":"x","reason":"FAKE"}}`)
	notFound := bodytransport.Static(http.StatusNotFound, `{"error":{"type":"index_not_found_exception"}}`)
	transportErr := func() bodytransport.Reply { return bodytransport.Reply{Err: errors.New("dial FAKE")} }
	readErr := func() bodytransport.Reply {
		return bodytransport.Reply{Status: http.StatusOK, Body: &bodytransport.Body{R: strings.NewReader(`{"fail`), Err: errors.New("read FAKE")}}
	}
	tests := []struct {
		name    string
		call    func(ctx context.Context, tr *bodytransport.Transport, t *testing.T) error
		reply   func() bodytransport.Reply
		wantErr bool
		want    []observe.Event
	}{
		{"create ok", callCreate, bodytransport.Static(http.StatusOK, `{"acknowledged":true}`), false, event(observe.ActionCreated, "i")},
		{"create rejected", callCreate, rejected, true, nil},
		{"create transport error", callCreate, transportErr, true, nil},
		{"delete ok", callDelete, bodytransport.Static(http.StatusOK, `{"acknowledged":true}`), false, event(observe.ActionDeleted, "i")},
		{"delete 404 keeps its error and emits nothing", callDelete, notFound, true, nil},
		{"delete rejected", callDelete, rejected, true, nil},
		{"delete transport error", callDelete, transportErr, true, nil},
		{"flush ok", callFlush, bodytransport.Static(http.StatusOK, `{"failures":[]}`), false, event(observe.ActionFlushed, "i")},
		{"flush rejected", callFlush, rejected, true, nil},
		{"flush transport error", callFlush, transportErr, true, nil},
		{"flush unreadable 2xx body", callFlush, readErr, true, nil},
		{"flush malformed 2xx body", callFlush, bodytransport.Static(http.StatusOK, `{"failures":`), true, nil},
		{"flush shard failures", callFlush, bodytransport.Static(http.StatusOK, `{"failures":[{"index":"i","reason":{"type":"x","reason":"FAKE"}}]}`), true, nil},
		{"refresh ok", callRefresh, bodytransport.Static(http.StatusOK, `{"_shards":{"total":1,"successful":1,"failed":0}}`), false, event(observe.ActionRefreshed, "i")},
		{"refresh rejected", callRefresh, rejected, true, nil},
		{"refresh transport error", callRefresh, transportErr, true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, rec := noticetest.New(t)
			err := tt.call(ctx, &bodytransport.Transport{Replies: []func() bodytransport.Reply{tt.reply}}, t)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			noticetest.RequireEvents(t, rec.Events, tt.want)
		})
	}
}

func callCreate(ctx context.Context, tr *bodytransport.Transport, t *testing.T) error {
	return Create(ctx, tr.Client(t), "i", `{}`)
}

func callDelete(ctx context.Context, tr *bodytransport.Transport, t *testing.T) error {
	return Delete(ctx, tr.Client(t), "i")
}

func callFlush(ctx context.Context, tr *bodytransport.Transport, t *testing.T) error {
	return Flush(ctx, tr.Client(t), "i")
}

func callRefresh(ctx context.Context, tr *bodytransport.Transport, t *testing.T) error {
	return Refresh(ctx, tr.Client(t), "i")
}

// TestClearDefaultPipelineEvents guards one settings_updated event per
// acknowledged settings PUT, inside the loop: an earlier success stays
// observable when a later index fails, and the failed one has none.
func TestClearDefaultPipelineEvents(t *testing.T) {
	t.Parallel()

	ack := bodytransport.Static(http.StatusOK, `{"acknowledged":true}`)
	fail := bodytransport.Static(http.StatusInternalServerError, `{"error":{"type":"x"}}`)
	tests := []struct {
		name    string
		replies []func() bodytransport.Reply
		wantErr bool
		want    []observe.Event
	}{
		{"all acknowledged", []func() bodytransport.Reply{ack}, false,
			[]observe.Event{{Action: observe.ActionSettingsUpdated, Kind: observe.KindIndex, Name: "a"}, {Action: observe.ActionSettingsUpdated, Kind: observe.KindIndex, Name: "b"}}},
		{"second rejected keeps the first", []func() bodytransport.Reply{ack, fail}, true, event(observe.ActionSettingsUpdated, "a")},
		{"first rejected emits nothing", []func() bodytransport.Reply{fail}, true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, rec := noticetest.New(t)
			tr := &bodytransport.Transport{Replies: tt.replies}
			err := ClearDefaultPipeline(ctx, tr.Client(t), []string{"a", "b"})
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			noticetest.RequireEvents(t, rec.Events, tt.want)
		})
	}
}

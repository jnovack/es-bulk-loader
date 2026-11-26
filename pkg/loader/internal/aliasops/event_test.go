package aliasops

// Phase 5 (loader-observability) guards: Update emits the one repointed event
// (the only event that carries Alias), and every index delete reachable through
// DeleteIndices and Prune is reported exactly once, by indexops.Delete.

import (
	"net/http"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/noticetest"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"
)

func TestUpdateEmitsRepointedWithAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		update  http.HandlerFunc
		wantErr bool
		want    []observe.Event
	}{
		{"acknowledged", esstub.JSON(http.StatusOK, `{"acknowledged":true}`), false,
			[]observe.Event{{Action: observe.ActionRepointed, Kind: observe.KindAlias, Name: "cards-2", Alias: "cards"}}},
		{"rejected emits nothing", esstub.JSON(http.StatusInternalServerError, `{"error":{"type":"x"}}`), true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stub := esstub.New(t)
			stub.Handle("GET /_alias/cards", esstub.JSON(http.StatusOK, `{"cards-1":{"aliases":{"cards":{}}}}`))
			stub.Handle("POST /_aliases", tt.update)
			ctx, rec := noticetest.New(t)
			err := Update(ctx, stub.MustClient(t), "cards", "cards-2")
			if (err != nil) != tt.wantErr {
				t.Fatalf("Update error = %v, wantErr %v", err, tt.wantErr)
			}
			noticetest.RequireEvents(t, rec.Events, tt.want)
		})
	}
}

// TestDeleteIndicesAndPruneReportEachDeleteOnce guards the single-source rule:
// indexops.Delete is the only index-delete emit, so DeleteIndices and Prune add
// none of their own, never set Alias, and stop at the first failure.
func TestDeleteIndicesAndPruneReportEachDeleteOnce(t *testing.T) {
	t.Parallel()

	newStub := func(t *testing.T, failing string) *esstub.Stub {
		stub := esstub.New(t)
		stub.Handle("GET /cards-*", esstub.JSON(http.StatusOK, `{"cards-20260101000000":{},"cards-20260102000000":{},"cards-20260103000000":{}}`))
		stub.Handle("DELETE /{name}", func(w http.ResponseWriter, r *http.Request) {
			if r.PathValue("name") == failing {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":{"type":"x"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
		})
		return stub
	}
	idx := func(name string) observe.Event {
		return observe.Event{Action: observe.ActionDeleted, Kind: observe.KindIndex, Name: name}
	}

	t.Run("prune deletes each older generation once, newest first", func(t *testing.T) {
		t.Parallel()
		ctx, rec := noticetest.New(t)
		if err := Prune(ctx, newStub(t, "").MustClient(t), "cards", 1); err != nil {
			t.Fatalf("Prune error = %v", err)
		}
		noticetest.RequireEvents(t, rec.Events, []observe.Event{idx("cards-20260102000000"), idx("cards-20260101000000")})
	})
	t.Run("prune that fails midway keeps the earlier delete", func(t *testing.T) {
		t.Parallel()
		ctx, rec := noticetest.New(t)
		if err := Prune(ctx, newStub(t, "cards-20260101000000").MustClient(t), "cards", 1); err == nil {
			t.Fatal("Prune error = nil, want the delete failure")
		}
		noticetest.RequireEvents(t, rec.Events, []observe.Event{idx("cards-20260102000000")})
	})
	t.Run("DeleteIndices reports once per index", func(t *testing.T) {
		t.Parallel()
		ctx, rec := noticetest.New(t)
		if err := DeleteIndices(ctx, newStub(t, "").MustClient(t), []string{"a", "b"}); err != nil {
			t.Fatalf("DeleteIndices error = %v", err)
		}
		noticetest.RequireEvents(t, rec.Events, []observe.Event{idx("a"), idx("b")})
	})
}

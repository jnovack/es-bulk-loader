package transform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
)

// TestResolveForSourceFiltersBySourceIndex guards that only transform definitions for the run's source index are kept.
func TestResolveForSourceFiltersBySourceIndex(t *testing.T) {
	t.Parallel()

	definitions := config.Definitions{
		"collection-to-binder": json.RawMessage(`{
			"source_index": "collection",
			"body": {"source":{"index":"collection"},"dest":{"index":"binder"}}
		}`),
		"cards-to-something": json.RawMessage(`{
			"source_index": "cards",
			"body": {"source":{"index":"cards"},"dest":{"index":"other"}}
		}`),
	}
	names := []string{"collection-to-binder", "cards-to-something"}

	selectedDefinitions, selectedNames, err := ResolveForSource(t.Context(), definitions, names, "collection")
	if err != nil {
		t.Fatalf("resolveTransformsForSource returned error: %v", err)
	}

	if want := []string{"collection-to-binder"}; !reflect.DeepEqual(selectedNames, want) {
		t.Fatalf("selected names mismatch: got %v want %v", selectedNames, want)
	}
	if _, ok := selectedDefinitions["collection-to-binder"]; !ok {
		t.Fatal("expected selected transform definition to be present")
	}
	if _, ok := selectedDefinitions["cards-to-something"]; ok {
		t.Fatal("did not expect non-matching source transform definition")
	}
}

// TestResolveForSourceRejectsMissingSourceIndex guards that a transform definition without a source index is rejected.
func TestResolveForSourceRejectsMissingSourceIndex(t *testing.T) {
	t.Parallel()

	definitions := config.Definitions{
		"bad-transform": json.RawMessage(`{
			"body": {"source":{"index":"collection"},"dest":{"index":"binder"}}
		}`),
	}
	_, _, err := ResolveForSource(t.Context(), definitions, []string{"bad-transform"}, "collection")
	if err == nil {
		t.Fatal("expected missing source_index error")
	}
}

// TestResolveForSourceRejectsMissingBody guards that a transform definition without a body is rejected.
func TestResolveForSourceRejectsMissingBody(t *testing.T) {
	t.Parallel()

	definitions := config.Definitions{
		"bad-transform": json.RawMessage(`{
			"source_index": "collection"
		}`),
	}
	_, _, err := ResolveForSource(t.Context(), definitions, []string{"bad-transform"}, "collection")
	if err == nil {
		t.Fatal("expected missing body error")
	}
}

// TestStopAndStartAlreadyInStatePredicates pins which stop/start rejections
// count as "already in that state" (not a failure) and which still fail.
func TestStopAndStartAlreadyInStatePredicates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		check  func(int, []byte) bool
		status int
		body   string
		want   bool
	}{
		{"stop conflict on stopped transform", stopAlreadyStopped, http.StatusConflict, `{"reason":"Cannot stop transform [t] as it is already STOPPED"}`, true},
		{"stop bad request on stopped transform", stopAlreadyStopped, http.StatusBadRequest, `cannot stop transform [t]: stopped`, true},
		{"stop other conflict fails", stopAlreadyStopped, http.StatusConflict, `{"reason":"task failed"}`, false},
		{"stop server error never matches", stopAlreadyStopped, http.StatusInternalServerError, `cannot stop transform stopped`, false},
		{"start resource already exists", startAlreadyStarted, http.StatusConflict, `{"type":"resource_already_exists_exception"}`, true},
		{"start already started", startAlreadyStarted, http.StatusBadRequest, `transform [t] already started`, true},
		{"start cannot start started", startAlreadyStarted, http.StatusConflict, `Cannot start transform [t] as it is in state [started]`, true},
		{"start other bad request fails", startAlreadyStarted, http.StatusBadRequest, `{"reason":"validation failed"}`, false},
		{"start server error never matches", startAlreadyStarted, http.StatusInternalServerError, `already started`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.check(tt.status, []byte(tt.body)); got != tt.want {
				t.Fatalf("predicate(%d, %s) = %t, want %t", tt.status, tt.body, got, tt.want)
			}
		})
	}
}

// TestWaitForCheckpointRetries drives the real client and statsGetter against
// a strict stub: transient _stats replies are retried without a progress
// snapshot and recover to one valid snapshot plus one successful Final, while
// a permanent 500 ends the wait at once with no snapshot or Final
// (FEAT-WAITRETRY-01). Only GET _stats is routed: the wait never writes.
func TestWaitForCheckpointRetries(t *testing.T) {
	t.Parallel()

	done := `{"count":1,"transforms":[{"id":"t1","state":"stopped","stats":{"documents_processed":3},"checkpointing":{"last":{"checkpoint":1}}}]}`
	tests := []struct {
		name       string
		replies    []pollReply
		wantGets   int
		wantSleeps int
		wantWarns  []wantRetryWarn
		wantErr    bool
	}{
		{
			name: "two transient replies then done",
			replies: []pollReply{
				{status: http.StatusServiceUnavailable, body: `{"error":"unavailable"}`},
				{status: http.StatusBadGateway, body: "<html>bad gateway</html>"},
				{status: http.StatusOK, body: done},
			},
			wantGets:   3,
			wantSleeps: 2,
			wantWarns: []wantRetryWarn{
				{1, http.StatusServiceUnavailable, `_stats returned status 503: {"error":"unavailable"}`},
				{2, http.StatusBadGateway, "_stats returned status 502: <html>bad gateway</html>"},
			},
		},
		{
			name:     "permanent 500 fails at once",
			replies:  []pollReply{{status: http.StatusInternalServerError, body: `{"error":"boom"}`}},
			wantGets: 1,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			var mu sync.Mutex
			gets := 0
			stub.Handle("GET /_transform/t1/_stats", func(w http.ResponseWriter, _ *http.Request) {
				mu.Lock()
				r := tt.replies[min(gets, len(tt.replies)-1)]
				gets++
				mu.Unlock()
				esstub.JSON(r.status, r.body)(w, nil)
			})
			es := stub.MustClient(t)

			ctx, buf := bufferLogger(t.Context())
			now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			sleeps := 0
			var snapshots []progress.Snapshot
			seams := progress.Seams{
				Now: func() time.Time { return now },
				Sleep: func(c context.Context, _ time.Duration) error {
					sleeps++
					return c.Err()
				},
				OnProgress: func(s progress.Snapshot) { snapshots = append(snapshots, s) },
			}

			err := WaitForCheckpoint(ctx, es, config.Definitions{"t1": json.RawMessage("{}")}, []string{"t1"}, time.Hour, seams)

			mu.Lock()
			gotGets := gets
			mu.Unlock()
			if gotGets != tt.wantGets || sleeps != tt.wantSleeps {
				t.Fatalf("GETs/sleeps = %d/%d, want %d/%d", gotGets, sleeps, tt.wantGets, tt.wantSleeps)
			}
			for _, r := range stub.Requests() {
				if r.Method != http.MethodGet || r.Path != "/_transform/t1/_stats" {
					t.Fatalf("unexpected request %s %s: the wait must only read _stats", r.Method, r.Path)
				}
			}
			checkRetryWarns(t, buf, "t1", tt.wantWarns)

			if tt.wantErr {
				var opErr *failure.Error
				if !errors.As(err, &opErr) || opErr.Op != "Transform wait failed" || !strings.Contains(err.Error(), "status 500") {
					t.Fatalf("err = %v, want operation error %q naming status 500", err, "Transform wait failed")
				}
				if len(snapshots) != 0 {
					t.Fatalf("snapshots = %+v, want none for a failed wait", snapshots)
				}
				return
			}
			if err != nil {
				t.Fatalf("WaitForCheckpoint: %v", err)
			}
			want := []progress.Snapshot{
				{Resource: "t1", Phase: "stopped", Done: 3, Total: -1},
				{Resource: "t1", Phase: "stopped", Done: 3, Total: -1, Final: true},
			}
			if !reflect.DeepEqual(snapshots, want) {
				t.Fatalf("snapshots = %+v, want one valid snapshot then one Final %+v", snapshots, want)
			}
		})
	}
}

// TestReportStartedEmitsOneFinalPerTransform pins the no-wait contract: one
// Final snapshot with Phase "started" and unknown counts per started transform.
func TestReportStartedEmitsOneFinalPerTransform(t *testing.T) {
	t.Parallel()

	var got []progress.Snapshot
	ReportStarted(progress.Seams{OnProgress: func(s progress.Snapshot) { got = append(got, s) }}, []string{"a", "b"})
	want := []progress.Snapshot{
		{Resource: "a", Phase: "started", Done: -1, Total: -1, Final: true},
		{Resource: "b", Phase: "started", Done: -1, Total: -1, Final: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshots = %+v, want %+v", got, want)
	}
}

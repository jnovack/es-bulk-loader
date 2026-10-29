package transform

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
)

// TestResolveForSourceFiltersBySourceIndex verifies behavior for the related scenario.
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

// TestResolveForSourceRejectsMissingSourceIndex verifies behavior for the related scenario.
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

// TestResolveForSourceRejectsMissingBody verifies behavior for the related scenario.
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

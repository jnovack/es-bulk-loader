package aliasops

import (
	"errors"
	"net/http"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

// TestBuildTimestampedIndexName guards the <alias>-YYYYMMDDHHMMSS name format for a fixed UTC time.
func TestBuildTimestampedIndexName(t *testing.T) {
	t.Parallel()

	got := buildTimestampedIndexName("cards", time.Date(2026, time.March, 19, 13, 4, 59, 0, time.UTC))
	if got != "cards-20260319130459" {
		t.Fatalf("buildTimestampedIndexName mismatch: got %q want %q", got, "cards-20260319130459")
	}
}

// TestParseTimestampedIndexName guards parsing a generation name's numeric suffix back into its timestamp.
func TestParseTimestampedIndexName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		alias  string
		index  string
		ok     bool
		expect string
	}{
		{
			name:   "valid",
			alias:  "cards",
			index:  "cards-20260319130459",
			ok:     true,
			expect: "2026-03-19T13:04:59Z",
		},
		{
			name:  "wrong alias",
			alias: "cards",
			index: "slugs-20260319130459",
			ok:    false,
		},
		{
			name:  "non numeric suffix",
			alias: "cards",
			index: "cards-20260319abc459",
			ok:    false,
		},
		{
			name:  "wrong width suffix",
			alias: "cards",
			index: "cards-2026031913045",
			ok:    false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := ParseTimestampedIndexName(tt.alias, tt.index)
			if ok != tt.ok {
				t.Fatalf("parseTimestampedIndexName ok mismatch: got %t want %t", ok, tt.ok)
			}
			if !tt.ok {
				return
			}
			if got.UTC().Format(time.RFC3339) != tt.expect {
				t.Fatalf("parseTimestampedIndexName value mismatch: got %q want %q", got.UTC().Format(time.RFC3339), tt.expect)
			}
		})
	}
}

// TestNextAvailableTimestampedIndexNameWithCheck guards that name selection advances one second at a time past existing candidates.
func TestNextAvailableTimestampedIndexNameWithCheck(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, time.March, 19, 13, 4, 59, 0, time.UTC)
	collisions := map[string]struct{}{
		"cards-20260319130459": {},
		"cards-20260319130500": {},
	}

	got, err := nextAvailableTimestampedIndexNameWithCheck(t.Context(), "cards", base, func(candidate string) (bool, error) {
		_, exists := collisions[candidate]
		return exists, nil
	})
	if err != nil {
		t.Fatalf("nextAvailableTimestampedIndexNameWithCheck returned error: %v", err)
	}

	if got != "cards-20260319130501" {
		t.Fatalf("nextAvailableTimestampedIndexNameWithCheck mismatch: got %q want %q", got, "cards-20260319130501")
	}
}

// TestNextAvailableTimestampedIndexNameWithCheckPropagatesErrors guards that an error from the existence check is returned, not treated as a free name.
func TestNextAvailableTimestampedIndexNameWithCheckPropagatesErrors(t *testing.T) {
	t.Parallel()

	_, err := nextAvailableTimestampedIndexNameWithCheck(t.Context(), "cards", time.Date(2026, time.March, 19, 13, 4, 59, 0, time.UTC), func(candidate string) (bool, error) {
		return false, os.ErrPermission
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestNextAvailableTimestampedIndexNameWithCheckStopsAtCollisionLimit verifies
// that the collision search performs no more than the documented number of checks.
func TestNextAvailableTimestampedIndexNameWithCheckStopsAtCollisionLimit(t *testing.T) {
	t.Parallel()

	nop := zerolog.Nop()
	ctx := runlog.WithLogger(t.Context(), &nop)

	checks := 0
	_, err := nextAvailableTimestampedIndexNameWithCheck(ctx, "cards", time.Date(2026, time.March, 19, 13, 4, 59, 0, time.UTC), func(candidate string) (bool, error) {
		checks++
		return true, nil
	})
	if err == nil {
		t.Fatal("expected collision-limit error, got nil")
	}
	if checks != timestampedIndexCollisionLimit {
		t.Fatalf("existence checks = %d, want %d", checks, timestampedIndexCollisionLimit)
	}
}

// TestResolveTargets pins target sorting, the missing-alias nil result, the
// empty-object empty result and the failure ops of an error status, an
// unparseable body and a JSON null body. The null row guards against null
// decoding to a nil map and reading as a missing alias.
func TestResolveTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		status    int
		body      string
		want      []string
		wantOp    string
		wantCause string
	}{
		{name: "targets sorted", status: http.StatusOK, body: `{"cards-b":{},"cards-a":{}}`, want: []string{"cards-a", "cards-b"}},
		{name: "missing alias is nil", status: http.StatusNotFound, body: `{"error":"alias [cards] missing"}`},
		{name: "empty object is no targets", status: http.StatusOK, body: `{}`, want: []string{}},
		{name: "error status fails", status: http.StatusInternalServerError, body: `{"error":{}}`, wantOp: "Failed to resolve alias targets"},
		{name: "malformed body fails", status: http.StatusOK, body: `{`, wantOp: "Unable to parse alias response"},
		{name: "null body fails, not absent", status: http.StatusOK, body: `null`, wantOp: "Unable to parse alias response", wantCause: "alias response was null"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			stub.Handle("GET /_alias/cards", esstub.JSON(tt.status, tt.body))
			got, err := ResolveTargets(t.Context(), stub.MustClient(t), "cards")
			if tt.wantOp != "" {
				var opErr *failure.Error
				if !errors.As(err, &opErr) || opErr.Op != tt.wantOp {
					t.Fatalf("error = %#v, want operation error %q", err, tt.wantOp)
				}
				if tt.wantCause != "" && (opErr.Err == nil || opErr.Err.Error() != tt.wantCause) {
					t.Fatalf("cause = %v, want %q", opErr.Err, tt.wantCause)
				}
				if got != nil {
					t.Fatalf("targets = %v on error, want nil", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveTargets returned error: %v", err)
			}
			if !slices.Equal(got, tt.want) || (got == nil) != (tt.want == nil) {
				t.Fatalf("targets = %#v, want %#v", got, tt.want)
			}
		})
	}
}

// TestPruneKeepsNewestGenerations guards retention safety: only valid
// timestamped generations are candidates, the newest keepLast survive, and
// nothing is deleted when keepLast covers every generation.
func TestPruneKeepsNewestGenerations(t *testing.T) {
	t.Parallel()

	listing := `{"cards-20260101000000":{},"cards-20260103000000":{},"cards-20260102000000":{},"cards-manual":{},"cards-2026010100000x":{}}`
	tests := []struct {
		name        string
		keepLast    int
		wantDeleted []string
	}{
		{name: "keep one deletes the two older", keepLast: 1, wantDeleted: []string{"/cards-20260102000000", "/cards-20260101000000"}},
		{name: "keep all deletes nothing", keepLast: 3},
		{name: "keep zero is a no-op", keepLast: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			stub.Handle("GET /cards-*", esstub.JSON(http.StatusOK, listing))
			stub.Handle("DELETE /{name}", esstub.JSON(http.StatusOK, `{"acknowledged":true}`))
			if err := Prune(t.Context(), stub.MustClient(t), "cards", tt.keepLast); err != nil {
				t.Fatalf("Prune returned error: %v", err)
			}
			var deleted []string
			for _, r := range stub.Requests() {
				if r.Method == http.MethodDelete {
					deleted = append(deleted, r.Path)
				}
			}
			if !slices.Equal(deleted, tt.wantDeleted) {
				t.Fatalf("deleted = %v, want %v", deleted, tt.wantDeleted)
			}
		})
	}
}

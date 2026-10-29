package enrich

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
)

// TestExecutePolicyCancellationStopsPolling verifies that cancelling the
// caller's context during the Tasks API poll wait stops the loop immediately
// instead of continuing to poll until the task completes.
func TestExecutePolicyCancellationStopsPolling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	sleepCalls := 0
	cfg := progress.Seams{
		Now: time.Now,
		Sleep: func(sleepCtx context.Context, _ time.Duration) error {
			sleepCalls++
			cancel()
			return sleepCtx.Err()
		},
	}

	var taskGetCalls atomic.Int32
	execute := esstub.JSON(http.StatusOK, `{"task":"node:123"}`)
	stub := esstub.New(t)
	stub.Handle("PUT /_enrich/policy/policy-a/_execute", execute)
	stub.Handle("POST /_enrich/policy/policy-a/_execute", execute)
	stub.Handle("GET /_tasks/node:123", func(w http.ResponseWriter, _ *http.Request) {
		taskGetCalls.Add(1)
		_, _ = w.Write([]byte(`{"completed":false,"task":{"status":{"phase":"RUNNING"}}}`))
	})

	es := stub.MustClient(t)

	result := make(chan bool, 1)
	go func() {
		result <- executePolicy(ctx, es, "policy-a", cfg)
	}()

	select {
	case ok := <-result:
		if ok {
			t.Fatal("executePolicy returned true, want false after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("executePolicy did not return promptly after cancellation")
	}

	if sleepCalls != 1 {
		t.Fatalf("Sleep calls = %d, want 1", sleepCalls)
	}
	if got := taskGetCalls.Load(); got != 0 {
		t.Fatalf("Tasks.Get calls = %d, want 0 (poll loop should stop before polling again)", got)
	}
}

// TestResolveTargetsAllPolicies verifies behavior for the related scenario.
func TestResolveTargetsAllPolicies(t *testing.T) {
	t.Parallel()

	sel := &Selection{Enabled: true, All: true}
	available := []string{"policy-b", "policy-a"}

	targets, missing := resolveTargets(sel, available, nil)

	if want := []string{"policy-a", "policy-b"}; !reflect.DeepEqual(targets, want) {
		t.Fatalf("targets mismatch: got %v want %v", targets, want)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing policies, got %v", missing)
	}
}

// TestResolveTargetsWarnsForMissingPolicies verifies behavior for the related scenario.
func TestResolveTargetsWarnsForMissingPolicies(t *testing.T) {
	t.Parallel()

	sel := &Selection{Enabled: true, Raw: "policy-b,policy-missing,policy-a"}
	available := []string{"policy-a", "policy-b"}

	targets, missing := resolveTargets(sel, available, nil)

	if want := []string{"policy-b", "policy-a"}; !reflect.DeepEqual(targets, want) {
		t.Fatalf("targets mismatch: got %v want %v", targets, want)
	}
	if want := []string{"policy-missing"}; !reflect.DeepEqual(missing, want) {
		t.Fatalf("missing mismatch: got %v want %v", missing, want)
	}
}

// TestResolveTargetsUsesDeclaredPoliciesWhenRequestedAll verifies behavior for the related scenario.
func TestResolveTargetsUsesDeclaredPoliciesWhenRequestedAll(t *testing.T) {
	t.Parallel()

	sel := &Selection{Enabled: true, All: true}
	available := []string{"e2e-source-policy", "slugs-by-name"}
	declared := []string{"slugs-by-name"}

	targets, missing := resolveTargets(sel, available, declared)

	if len(missing) != 0 {
		t.Fatalf("expected no missing policies, got %v", missing)
	}
	if len(targets) != 1 || targets[0] != "slugs-by-name" {
		t.Fatalf("expected declared policy only, got %v", targets)
	}
}

// TestSelectionExplicitPolicies pins the nil, disabled and All semantics and
// the trim/de-duplicate rules that the remap and target resolution rely on.
func TestSelectionExplicitPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sel  *Selection
		want []string
	}{
		{name: "nil selection", sel: nil, want: nil},
		{name: "disabled selection", sel: &Selection{Raw: "a"}, want: nil},
		{name: "all selection", sel: &Selection{Enabled: true, All: true, Raw: "a"}, want: nil},
		{name: "trimmed and de-duplicated in order", sel: &Selection{Enabled: true, Raw: " b,a, b ,,c "}, want: []string{"b", "a", "c"}},
		{name: "empty explicit list", sel: &Selection{Enabled: true, Raw: " , "}, want: []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.sel.ExplicitPolicies(); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ExplicitPolicies() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

// TestListPolicies pins sorting, the unsupported-API warn-and-skip result and
// the failure ops of an error status and an unparseable body.
func TestListPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		status        int
		body          string
		want          []string
		wantSupported bool
		wantOp        string
	}{
		{name: "sorted names", status: http.StatusOK, body: `{"policies":[{"config":{"match":{"name":"b"}}},{"config":{"range":{"name":"a"}}}]}`, want: []string{"a", "b"}, wantSupported: true},
		{name: "generic 404 is unsupported, not an error", status: http.StatusNotFound, body: `{"error":true}`},
		{name: "error status fails", status: http.StatusInternalServerError, body: `{"error":{"type":"stub"}}`, wantOp: "Failed to get enrich policies"},
		{name: "malformed body fails", status: http.StatusOK, body: `{`, wantOp: "Unable to parse enrich policy response"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			handler := esstub.JSON(tt.status, tt.body)
			stub.Handle("GET /_enrich/policy", handler)
			stub.Handle("GET /_enrich/policy/{$}", handler)
			got, supported, err := ListPolicies(t.Context(), stub.MustClient(t))
			if tt.wantOp != "" {
				var opErr *failure.Error
				if !errors.As(err, &opErr) || opErr.Op != tt.wantOp {
					t.Fatalf("error = %#v, want operation error %q", err, tt.wantOp)
				}
				return
			}
			if err != nil {
				t.Fatalf("ListPolicies returned error: %v", err)
			}
			if supported != tt.wantSupported || !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ListPolicies = (%v, %t), want (%v, %t)", got, supported, tt.want, tt.wantSupported)
			}
		})
	}
}

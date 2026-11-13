package main

import (
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
)

// Untagged for the same reason as replicas_cli_test.go: the flag/env wiring
// from main() into loader.Options is only provable through the real binary.

const (
	statsDone    = `{"count":1,"transforms":[{"id":"tx1","state":"stopped","stats":{"documents_processed":5,"documents_indexed":5},"checkpointing":{"last":{"checkpoint":1}}}]}`
	statsRunning = `{"count":1,"transforms":[{"id":"tx1","state":"indexing","checkpointing":{"last":{"checkpoint":0},"next":{"checkpoint":1}}}]}`
)

// TestCLIWaitTransformsReachesLoader proves -wait-transforms, WAIT_TRANSFORMS
// and -wait-transforms-timeout travel from main() into loader.Options: with
// the wait off no _stats request is made, with it on one is, and a timeout
// shorter than the time the transform needs (the first stats poll is
// immediate, then the deadline interrupts the wait) fails the run.
func TestCLIWaitTransformsReachesLoader(t *testing.T) {
	// Not parallel: builds one binary shared by every row.
	bin := buildCLI(t)
	dir := t.TempDir()
	dataFile := filepath.Join(dir, "data.json")
	if err := os.WriteFile(dataFile, []byte(`[{"id":1}]`), 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}
	transformsFile := filepath.Join(dir, "transforms.json")
	transforms := `{"tx1":{"source_index":"idx","body":{"source":{"index":"idx"},"dest":{"index":"idx-out"},"pivot":{"group_by":{"id":{"terms":{"field":"id"}}},"aggregations":{"n":{"value_count":{"field":"id"}}}}}}}`
	if err := os.WriteFile(transformsFile, []byte(transforms), 0o600); err != nil {
		t.Fatalf("write transforms: %v", err)
	}

	tests := []struct {
		name      string
		args      []string
		env       []string
		stats     string
		wantStats bool
		wantFail  string // substring of the output when the run must fail
	}{
		{name: "default does not wait", stats: statsDone, wantStats: false},
		{name: "flag waits", args: []string{"-wait-transforms"}, stats: statsDone, wantStats: true},
		{name: "env waits", env: []string{"WAIT_TRANSFORMS=true"}, stats: statsDone, wantStats: true},
		{
			name: "timeout flag bounds the wait", args: []string{"-wait-transforms", "-wait-transforms-timeout", "100ms"},
			stats: statsRunning, wantStats: true, wantFail: "timed out after 100ms",
		},
		{
			name: "timeout env bounds the wait", args: []string{"-wait-transforms"}, env: []string{"WAIT_TRANSFORMS_TIMEOUT=100ms"},
			stats: statsRunning, wantStats: true, wantFail: "timed out after 100ms",
		},
		// A timeout of 0 or less must fail before any request: Run would
		// otherwise silently rewrite it to the 30m default.
		{
			name: "zero timeout flag is rejected", args: []string{"-wait-transforms", "-wait-transforms-timeout", "0s"},
			stats: statsDone, wantStats: false, wantFail: "invalid -wait-transforms-timeout",
		},
		{
			name: "negative timeout env is rejected", args: []string{"-wait-transforms"}, env: []string{"WAIT_TRANSFORMS_TIMEOUT=-1s"},
			stats: statsDone, wantStats: false, wantFail: "invalid -wait-transforms-timeout",
		},
		{
			name: "zero timeout is ignored when the wait is off", args: []string{"-wait-transforms-timeout", "0s"},
			stats: statsDone, wantStats: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newCLIStub(t)
			// Plain -delete removes the declared transform before the index
			// exists; the later existence check reports it absent so
			// CreateOrUpdate does not send a second DELETE.
			stub.Handle("DELETE /_transform/tx1", esstub.JSON(http.StatusOK, `{"acknowledged":true}`))
			stub.Handle("POST /idx/_refresh", esstub.JSON(http.StatusOK, `{"acknowledged":true}`))
			stub.Handle("POST /_transform/tx1/_stop", esstub.JSON(http.StatusOK, `{"acknowledged":true}`))
			stub.Handle("GET /_transform/tx1", esstub.JSON(http.StatusOK, `{"count":0,"transforms":[]}`))
			stub.Handle("PUT /_transform/tx1", esstub.JSON(http.StatusOK, `{"acknowledged":true}`))
			stub.Handle("POST /_transform/tx1/_start", esstub.JSON(http.StatusOK, `{"acknowledged":true}`))
			// Registered even when waiting is off so an unwanted poll is
			// caught by the sawStats assertion, not by an unrouted request.
			stub.Handle("GET /_transform/tx1/_stats", esstub.JSON(http.StatusOK, tt.stats))

			args := append([]string{"-url", stub.URL(), "-index", "idx", "-delete", "-sync-managed", "-data", dataFile, "-transforms", transformsFile}, tt.args...)
			cmd := exec.CommandContext(t.Context(), bin, args...)
			cmd.Env = append([]string{"HOME=" + t.TempDir()}, tt.env...)
			out, err := cmd.CombinedOutput()
			requests := stub.Requests()

			if tt.wantFail == "" && err != nil {
				t.Fatalf("run failed: %v\n%s", err, out)
			}
			if tt.wantFail != "" {
				if err == nil || !strings.Contains(string(out), tt.wantFail) {
					t.Fatalf("want failure containing %q, got err=%v\n%s", tt.wantFail, err, out)
				}
			}

			if tt.wantFail == "invalid -wait-transforms-timeout" && len(requests) != 0 {
				t.Fatalf("invalid timeout reached Elasticsearch: %v", requests)
			}
			sawStats := false
			for _, req := range requests {
				if req.Method == http.MethodGet && req.Path == "/_transform/tx1/_stats" {
					sawStats = true
				}
			}
			if sawStats != tt.wantStats {
				t.Fatalf("_stats requested = %t, want %t (%v)", sawStats, tt.wantStats, requests)
			}
		})
	}
}

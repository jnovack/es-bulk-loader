package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Untagged for the same reason as replicas_cli_test.go: the flag/env wiring
// from main() into loader.Options is only provable through the real binary.

const (
	statsDone    = `{"count":1,"transforms":[{"id":"tx1","state":"stopped","stats":{"documents_processed":5,"documents_indexed":5},"checkpointing":{"last":{"checkpoint":1}}}]}`
	statsRunning = `{"count":1,"transforms":[{"id":"tx1","state":"indexing","checkpointing":{"last":{"checkpoint":0},"next":{"checkpoint":1}}}]}`
)

// transformStatsHandler serves _stats with body and delegates everything else
// to cliStub, which records it.
func transformStatsHandler(stub *cliStub, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/_transform/tx1/_stats" {
			stub.mu.Lock()
			stub.requests = append(stub.requests, r.Method+" "+r.URL.Path)
			stub.mu.Unlock()
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
			return
		}
		stub.handler(w, r)
	}
}

// TestCLIWaitTransformsReachesLoader proves -wait-transforms, WAIT_TRANSFORMS
// and -wait-transforms-timeout travel from main() into loader.Options: with
// the wait off no _stats request is made, with it on one is, and a timeout
// shorter than the first poll interval fails the run.
func TestCLIWaitTransformsReachesLoader(t *testing.T) {
	// Not parallel: builds one binary shared by every row.
	bin := filepath.Join(t.TempDir(), "es-bulk-loader")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
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
			stub := &cliStub{}
			server := httptest.NewServer(transformStatsHandler(stub, tt.stats))
			t.Cleanup(server.Close)

			args := append([]string{"-url", server.URL, "-index", "idx", "-delete", "-sync-managed", "-data", dataFile, "-transforms", transformsFile}, tt.args...)
			cmd := exec.CommandContext(t.Context(), bin, args...)
			cmd.Env = append([]string{"HOME=" + t.TempDir()}, tt.env...)
			out, err := cmd.CombinedOutput()

			if tt.wantFail == "" && err != nil {
				t.Fatalf("run failed: %v\n%s", err, out)
			}
			if tt.wantFail != "" {
				if err == nil || !strings.Contains(string(out), tt.wantFail) {
					t.Fatalf("want failure containing %q, got err=%v\n%s", tt.wantFail, err, out)
				}
			}

			stub.mu.Lock()
			defer stub.mu.Unlock()
			sawStats := false
			for _, req := range stub.requests {
				if req == "GET /_transform/tx1/_stats" {
					sawStats = true
				}
			}
			if sawStats != tt.wantStats {
				t.Fatalf("_stats requested = %t, want %t (%v)", sawStats, tt.wantStats, stub.requests)
			}
		})
	}
}

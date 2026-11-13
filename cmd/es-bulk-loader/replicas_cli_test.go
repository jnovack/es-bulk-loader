package main

import (
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
)

// This file is deliberately untagged: it runs under plain `go test ./...`.
// It builds the real binary and talks to a loopback esstub listener, which the
// template's Go testing rules would put behind a build tag, but this repo's
// project override defers to its own E2E layout, and pkg/loader already runs
// httptest-backed stubs untagged. Tagging it would drop the -replicas flag/env
// wiring (including "invalid value exits before any ES request") from the
// default gate, leaving only the Docker E2E to catch a broken flag.

// newCLIStub returns a strict esstub with exactly the routes a
// `-delete -data` run sends. Any other request fails the test; do not add a
// catch-all. HEAD /idx must answer 404 until the create PUT and 200 after,
// because the loader verifies the index is visible once created; a fixed reply
// would either skip creation or fail that verification. That is why the PUT
// and HEAD handlers share the created flag.
func newCLIStub(t *testing.T) *esstub.Stub {
	t.Helper()
	stub := esstub.New(t)
	var created atomic.Bool
	ack := esstub.JSON(http.StatusOK, `{"acknowledged":true}`)

	stub.Handle("HEAD /idx", func(w http.ResponseWriter, _ *http.Request) {
		if created.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	stub.Handle("GET /_nodes", esstub.JSON(http.StatusOK, `{"nodes":{"n1":{},"n2":{},"n3":{}}}`))
	stub.Handle("PUT /idx", func(w http.ResponseWriter, r *http.Request) {
		created.Store(true)
		ack(w, r)
	})
	stub.Handle("POST /_bulk", esstub.JSON(http.StatusOK, `{"errors":false,"items":[{"index":{"_index":"idx","_id":"1","status":201}}]}`))
	return stub
}

// TestCLIReplicasReachCreateIndexBody builds the real binary and proves that
// -replicas and REPLICAS travel from main() to the create-index body, that the
// flag wins over the env var, that the default derives from topology (3 nodes
// -> 1), and that values below -1 fail before any Elasticsearch request.
// Building the binary alone would not prove this wiring.
func TestCLIReplicasReachCreateIndexBody(t *testing.T) {
	// Not parallel: builds one binary shared by every row.
	bin := buildCLI(t)
	dataFile := filepath.Join(t.TempDir(), "data.json")
	if err := os.WriteFile(dataFile, []byte(`[{"id":1}]`), 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	tests := []struct {
		name         string
		args         []string
		env          []string
		wantReplicas float64 // ignored when wantFail
		wantNodes    bool
		wantFail     bool
	}{
		{name: "no override derives from topology", wantReplicas: 1, wantNodes: true},
		{name: "flag 0", args: []string{"-replicas", "0"}, wantReplicas: 0},
		{name: "flag 3", args: []string{"-replicas", "3"}, wantReplicas: 3},
		{name: "env REPLICAS=2", env: []string{"REPLICAS=2"}, wantReplicas: 2},
		{name: "flag wins over env", args: []string{"-replicas", "4"}, env: []string{"REPLICAS=2"}, wantReplicas: 4},
		{name: "flag below -1 fails", args: []string{"-replicas", "-2"}, wantFail: true},
		{name: "env below -1 fails", env: []string{"REPLICAS=-5"}, wantFail: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newCLIStub(t)

			args := append([]string{"-url", stub.URL(), "-index", "idx", "-delete", "-data", dataFile}, tt.args...)
			cmd := exec.CommandContext(t.Context(), bin, args...)
			// Explicit minimal env: no inherited REPLICAS or other loader
			// variables, and a private HOME so no user config is picked up.
			cmd.Env = append([]string{"HOME=" + t.TempDir()}, tt.env...)
			out, err := cmd.CombinedOutput()
			requests := stub.Requests()

			if tt.wantFail {
				if err == nil {
					t.Fatalf("expected nonzero exit, got success:\n%s", out)
				}
				if len(requests) != 0 {
					t.Fatalf("invalid replicas reached Elasticsearch: %v", requests)
				}
				return
			}
			if err != nil {
				t.Fatalf("run failed: %v\n%s", err, out)
			}

			var creates []esstub.Request
			sawNodes := false
			for _, req := range requests {
				if req.Method == http.MethodPut && req.Path == "/idx" {
					creates = append(creates, req)
				}
				if req.Method == http.MethodGet && req.Path == "/_nodes" {
					sawNodes = true
				}
			}
			if len(creates) != 1 {
				t.Fatalf("create bodies = %d, want 1; requests %v\n%s", len(creates), requests, out)
			}
			var body struct {
				Settings map[string]any `json:"settings"`
			}
			if err := json.Unmarshal(creates[0].Body, &body); err != nil {
				t.Fatalf("parse create body: %v", err)
			}
			if got := body.Settings["number_of_replicas"]; got != tt.wantReplicas {
				t.Fatalf("number_of_replicas = %v, want %v", got, tt.wantReplicas)
			}
			if sawNodes != tt.wantNodes {
				t.Fatalf("/_nodes queried = %t, want %t (%v)", sawNodes, tt.wantNodes, requests)
			}
		})
	}
}

package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

// This file is deliberately untagged: it runs under plain `go test ./...`.
// It builds the real binary and starts a loopback httptest listener, which the
// template's Go testing rules would put behind a build tag, but this repo's
// project override defers to its own E2E layout, and pkg/loader already runs
// httptest listeners untagged. Tagging it would drop the -replicas flag/env
// wiring (including "invalid value exits before any ES request") from the
// default gate, leaving only the Docker E2E to catch a broken flag.

// cliStub is a minimal Elasticsearch that records every request path and the
// body of each create-index PUT. Every unknown request is acknowledged so the
// loader can run a -delete workflow to completion.
type cliStub struct {
	mu          sync.Mutex
	requests    []string
	createBodys []string
}

func (s *cliStub) handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.Header().Set("Content-Type", "application/json")
	s.mu.Lock()
	s.requests = append(s.requests, r.Method+" "+r.URL.Path)
	s.mu.Unlock()

	switch {
	case r.Method == http.MethodHead:
		// Absent until the create PUT, then visible, as the loader verifies.
		s.mu.Lock()
		created := len(s.createBodys) > 0
		s.mu.Unlock()
		if created {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	case r.Method == http.MethodGet && r.URL.Path == "/_nodes":
		_, _ = w.Write([]byte(`{"nodes":{"n1":{},"n2":{},"n3":{}}}`))
	case r.Method == http.MethodPut && r.URL.Path == "/idx":
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.createBodys = append(s.createBodys, string(body))
		s.mu.Unlock()
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
		_, _ = w.Write([]byte(`{"errors":false,"items":[{"index":{"_index":"idx","_id":"1","status":201}}]}`))
	default:
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	}
}

// TestCLIReplicasReachCreateIndexBody builds the real binary and proves that
// -replicas and REPLICAS travel from main() to the create-index body, that the
// flag wins over the env var, that the default derives from topology (3 nodes
// -> 1), and that values below -1 fail before any Elasticsearch request.
// Building the binary alone would not prove this wiring.
func TestCLIReplicasReachCreateIndexBody(t *testing.T) {
	// Not parallel: builds one binary shared by every row.
	bin := filepath.Join(t.TempDir(), "es-bulk-loader")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
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
			stub := &cliStub{}
			server := httptest.NewServer(http.HandlerFunc(stub.handler))
			t.Cleanup(server.Close)

			args := append([]string{"-url", server.URL, "-index", "idx", "-delete", "-data", dataFile}, tt.args...)
			cmd := exec.CommandContext(t.Context(), bin, args...)
			// Explicit minimal env: no inherited REPLICAS or other loader
			// variables, and a private HOME so no user config is picked up.
			cmd.Env = append([]string{"HOME=" + t.TempDir()}, tt.env...)
			out, err := cmd.CombinedOutput()

			if tt.wantFail {
				if err == nil {
					t.Fatalf("expected nonzero exit, got success:\n%s", out)
				}
				if len(stub.requests) != 0 {
					t.Fatalf("invalid replicas reached Elasticsearch: %v", stub.requests)
				}
				return
			}
			if err != nil {
				t.Fatalf("run failed: %v\n%s", err, out)
			}

			stub.mu.Lock()
			defer stub.mu.Unlock()
			if len(stub.createBodys) != 1 {
				t.Fatalf("create bodies = %d, want 1; requests %v\n%s", len(stub.createBodys), stub.requests, out)
			}
			var body struct {
				Settings map[string]any `json:"settings"`
			}
			if err := json.Unmarshal([]byte(stub.createBodys[0]), &body); err != nil {
				t.Fatalf("parse create body: %v", err)
			}
			if got := body.Settings["number_of_replicas"]; got != tt.wantReplicas {
				t.Fatalf("number_of_replicas = %v, want %v", got, tt.wantReplicas)
			}
			sawNodes := false
			for _, req := range stub.requests {
				if req == "GET /_nodes" {
					sawNodes = true
				}
			}
			if sawNodes != tt.wantNodes {
				t.Fatalf("/_nodes queried = %t, want %t (%v)", sawNodes, tt.wantNodes, stub.requests)
			}
		})
	}
}

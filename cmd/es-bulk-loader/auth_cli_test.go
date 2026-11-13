package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
)

// Untagged for the same reason as replicas_cli_test.go: basic-auth sourcing is
// decided by flag/env/config wiring in main(), provable only via the binary.

// TestCLIBasicAuthIgnoresUSEREnv guards that the CLI never takes its basic-auth
// username from the shell's USER variable (jnovack/flag would otherwise bind
// -user to it), while the username still comes from -user or the -config file,
// and USER is restored before loader templates read it.
func TestCLIBasicAuthIgnoresUSEREnv(t *testing.T) {
	// Not parallel: builds one binary shared by every row.
	bin := buildCLI(t)
	dir := t.TempDir()

	writeCfg := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "es.conf")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		return path
	}
	basic := func(creds string) string { return "Basic " + base64.StdEncoding.EncodeToString([]byte(creds)) }

	const (
		missingUser = "basic auth requires both -user and -pass: -user is empty"
		missingPass = "basic auth requires both -user and -pass: -pass is empty"
		conflict    = "cannot use both basic auth and API key"
	)

	tests := []struct {
		name     string
		cfg      string   // config file content; empty means no -config
		cfgEnv   bool     // pass the config through CONFIG instead of -config
		args     []string // extra argv
		env      []string
		wantAuth string // expected Authorization header on HEAD; empty means absent
		wantFail string // substring of the output when the run must fail
	}{
		{name: "shell username only", env: []string{"USER=someone"}},
		{name: "api key argument", args: []string{"-apiKey", "k"}, env: []string{"USER=someone"}, wantAuth: "APIKey k"},
		{name: "api key environment", env: []string{"APIKEY=k", "USER=someone"}, wantAuth: "APIKey k"},
		{
			name: "api key argument beats env and config", cfg: "apiKey=cfgkey\n", args: []string{"-apiKey", "k"},
			env: []string{"APIKEY=envkey", "USER=someone"}, wantAuth: "APIKey k",
		},
		{name: "lone username", args: []string{"-user", "u"}, wantFail: missingPass},
		{name: "lone password argument", args: []string{"-pass", "p"}, wantFail: missingUser},
		{name: "shell username cannot satisfy password", env: []string{"USER=someone", "PASS=p"}, wantFail: missingUser},
		{name: "explicit basic auth", args: []string{"-user", "u", "-pass", "p"}, env: []string{"USER=other"}, wantAuth: basic("u:p")},
		{name: "config username, USER other", cfg: "user=cfguser\n", env: []string{"PASS=p", "USER=other"}, wantAuth: basic("cfguser:p")},
		{name: "config username, USER empty", cfg: "user=cfguser\n", env: []string{"PASS=p", "USER="}, wantAuth: basic("cfguser:p")},
		{name: "config username, USER absent", cfg: "user=cfguser\n", env: []string{"PASS=p"}, wantAuth: basic("cfguser:p")},
		{name: "config username, CRLF", cfg: "user=cfguser\r\n", env: []string{"PASS=p", "USER=other"}, wantAuth: basic("cfguser:p")},
		{name: "username argument beats config", cfg: "user=cfguser\n", args: []string{"-user", "u"}, env: []string{"PASS=p", "USER=other"}, wantAuth: basic("u:p")},
		{name: "explicit empty username suppresses config", cfg: "user=cfguser\n", args: []string{"-user="}, env: []string{"PASS=p", "USER=other"}, wantFail: missingUser},
		{
			name: "password argument beats env and config", cfg: "user=cfguser\npass=cfgpass\n", args: []string{"-pass", "p"},
			env: []string{"PASS=envpass", "USER=other"}, wantAuth: basic("cfguser:p"),
		},
		{name: "config via CONFIG env", cfg: "user=cfguser\n", cfgEnv: true, env: []string{"PASS=p", "USER=other"}, wantAuth: basic("cfguser:p")},
		{name: "existing conflict wins", args: []string{"-user", "u"}, env: []string{"APIKEY=k", "USER=other"}, wantFail: conflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := esstub.New(t)
			stub.Handle("HEAD /idx", esstub.JSON(http.StatusOK, ""))

			args := []string{"-url", stub.URL(), "-index", "idx", "-sync-managed"}
			env := []string{"HOME=" + t.TempDir()}
			if tt.cfg != "" {
				cfg := writeCfg(t, tt.cfg)
				if tt.cfgEnv {
					env = append(env, "CONFIG="+cfg)
				} else {
					args = append(args, "-config", cfg)
				}
			}
			args = append(args, tt.args...)
			cmd := exec.CommandContext(t.Context(), bin, args...)
			cmd.Env = append(env, tt.env...)
			out, err := cmd.CombinedOutput()
			requests := stub.Requests()

			if tt.wantFail != "" {
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) {
					t.Fatalf("want non-zero exit, got err=%v\n%s", err, out)
				}
				if !strings.Contains(string(out), tt.wantFail) {
					t.Fatalf("output missing %q:\n%s", tt.wantFail, out)
				}
				if len(requests) != 0 {
					t.Fatalf("failed run reached Elasticsearch: %v", requests)
				}
				return
			}
			if err != nil {
				t.Fatalf("run failed: %v\n%s", err, out)
			}
			if len(requests) != 1 || requests[0].Method != http.MethodHead || requests[0].Path != "/idx" {
				t.Fatalf("requests = %v, want one HEAD /idx\n%s", requests, out)
			}
			assertAuthHeader(t, requests[0], tt.wantAuth)
		})
	}

	// USER must be restored after flag parsing: ${USER} in a template falls
	// back to the process environment.
	t.Run("environment restored for templates", func(t *testing.T) {
		pipelines := filepath.Join(dir, "pipelines.json")
		if err := os.WriteFile(pipelines, []byte(`{"p":{"description":"${USER}","processors":[]}}`), 0o600); err != nil {
			t.Fatalf("write pipelines: %v", err)
		}
		stub := esstub.New(t)
		stub.Handle("HEAD /idx", esstub.JSON(http.StatusOK, ""))
		stub.Handle("PUT /_ingest/pipeline/p", esstub.JSON(http.StatusOK, `{"acknowledged":true}`))

		cmd := exec.CommandContext(t.Context(), bin, "-url", stub.URL(), "-index", "idx", "-sync-managed", "-pipelines", pipelines)
		cmd.Env = []string{"HOME=" + t.TempDir(), "USER=someone"}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("run failed: %v\n%s", err, out)
		}
		requests := stub.Requests()
		if len(requests) != 2 || requests[0].Method != http.MethodHead || requests[1].Method != http.MethodPut || requests[1].Path != "/_ingest/pipeline/p" {
			t.Fatalf("requests = %v, want HEAD /idx then PUT pipeline\n%s", requests, out)
		}
		assertAuthHeader(t, requests[0], "")
		assertAuthHeader(t, requests[1], "")
		var body struct {
			Description string `json:"description"`
		}
		if err := json.Unmarshal(requests[1].Body, &body); err != nil {
			t.Fatalf("parse pipeline body: %v\n%s", err, requests[1].Body)
		}
		if body.Description != "someone" {
			t.Fatalf("description = %q, want %q (USER must be restored after parsing)", body.Description, "someone")
		}
	})
}

// assertAuthHeader fails unless req carries exactly want as its Authorization
// header; an empty want means the header must be absent.
func assertAuthHeader(t *testing.T, req esstub.Request, want string) {
	t.Helper()
	values := req.Header.Values("Authorization")
	if want == "" {
		if len(values) != 0 {
			t.Fatalf("Authorization = %v, want absent", values)
		}
		return
	}
	if len(values) != 1 || values[0] != want {
		t.Fatalf("Authorization = %v, want [%q]", values, want)
	}
}

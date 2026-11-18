package repocheck

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// recipeLines returns the executable recipe lines of target in makefile:
// the tab-indented lines directly after "target:", minus comment lines.
// It returns nil when the target is absent.
func recipeLines(makefile, target string) []string {
	lines := strings.Split(makefile, "\n")
	for i, l := range lines {
		lines[i] = strings.TrimSuffix(l, "\r")
	}
	start := slices.IndexFunc(lines, func(l string) bool {
		return strings.HasPrefix(l, target+":")
	})
	if start < 0 {
		return nil
	}
	var out []string
	for _, l := range lines[start+1:] {
		body, ok := strings.CutPrefix(l, "\t")
		if !ok {
			break
		}
		body = strings.TrimSpace(body)
		// A recipe comment (also the silent "@#" form) is not executable, so
		// a "-count=1" mentioned there must not satisfy the guard.
		if strings.HasPrefix(body, "#") || strings.HasPrefix(body, "@#") {
			continue
		}
		out = append(out, l)
	}
	return out
}

// hasCountOne reports whether lines is exactly one direct, single-line
// `go test` command (optionally "@"-prefixed) carrying exactly one count
// option, spelled -count=1. Anything else, including shell compound syntax
// or several recipe lines, is rejected so a recipe change forces an explicit
// update of this guard.
func hasCountOne(lines []string) bool {
	if len(lines) != 1 {
		return false
	}
	text := strings.TrimPrefix(strings.TrimSpace(lines[0]), "@")
	var fields []string
	for _, f := range strings.Fields(text) {
		if strings.HasPrefix(f, "#") {
			break // inline comment
		}
		fields = append(fields, f)
	}
	if len(fields) < 2 || fields[0] != "go" || fields[1] != "test" {
		return false
	}
	counts := 0
	ok := false
	afterArgs := false
	for _, f := range fields[2:] {
		if strings.ContainsAny(f, ";&|`<>$\\()") {
			return false
		}
		// Everything after -args goes to the test binary, not to go test, so a
		// -count=1 there does not disable the cache. Shell syntax after it is
		// still rejected above.
		if f == "-args" || f == "--args" {
			afterArgs = true
			continue
		}
		if afterArgs {
			continue
		}
		if strings.HasPrefix(f, "-count") || strings.HasPrefix(f, "--count") {
			counts++
			ok = f == "-count=1"
		}
	}
	return counts == 1 && ok
}

func TestRecipeLinesCountOne(t *testing.T) {
	tests := []struct {
		name     string
		makefile string
		target   string
		want     bool
	}{
		{"flag present", "t:\n\tgo test -count=1 ./x\n", "t", true},
		{"flag absent", "t:\n\tgo test ./x\n", "t", false},
		{"flag only in recipe comment", "t:\n\t# -count=1\n\tgo test ./x\n", "t", false},
		{"flag only in silent recipe comment", "t:\n\t@# -count=1\n\tgo test ./x\n", "t", false},
		{"flag only in column-0 comment above target", "# -count=1\nt:\n\tgo test ./x\n", "t", false},
		{"flag only in inline comment", "t:\n\tgo test ./x # -count=1\n", "t", false},
		{"echo of command", "t:\n\techo go test -count=1 ./x\n", "t", false},
		{"unflagged go test then flagged separate command", "t:\n\tgo test ./x\n\tgo vet -count=1 ./x\n", "t", false},
		{"unflagged go test chained with flagged command", "t:\n\tgo test ./x && go test -count=1 ./y\n", "t", false},
		{"conflicting count options", "t:\n\tgo test -count=1 -count=2 ./x\n", "t", false},
		{"duplicate count options", "t:\n\tgo test -count=1 -count=1 ./x\n", "t", false},
		{"count other than one", "t:\n\tgo test -count=2 ./x\n", "t", false},
		{"flag after -args goes to the test binary", "t:\n\tgo test ./x -args -count=1\n", "t", false},
		{"shell syntax after -args", "t:\n\tgo test -count=1 ./x -args ; go test ./y\n", "t", false},
		{"spaced count form is unsupported", "t:\n\tgo test -count 1 ./x\n", "t", false},
		{"line continuation is unsupported", "t:\n\tgo test \\\n\t-count=1 ./x\n", "t", false},
		{"silent prefix", "t:\n\t@go test -count=1 ./x\n", "t", true},
		{"CRLF line endings", "t:\r\n\tgo test -count=1 ./x\r\n", "t", true},
		{"target missing", "other:\n\tgo test -count=1 ./x\n", "t", false},
		{"flag only on a different target", "t:\n\tgo test ./x\n\nu:\n\tgo test -count=1 ./x\n", "t", false},
		{"target is not a prefix match", "t-verbose:\n\tgo test -count=1 ./x\n", "t", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasCountOne(recipeLines(tt.makefile, tt.target)); got != tt.want {
				t.Errorf("hasCountOne(recipeLines(%q, %q)) = %v, want %v", tt.makefile, tt.target, got, tt.want)
			}
		})
	}
}

// TestMakefileE2ETargetsDisableTestCache guards the E2E make targets against
// losing -count=1. The E2E suite builds the CLI in a subprocess and talks to a
// live Elasticsearch container, neither of which Go's test cache can see, so a
// cached "ok" would be a false pass in the gate for destructive-path changes.
func TestMakefileE2ETargetsDisableTestCache(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	for _, target := range []string{"test-e2e", "test-e2e-verbose"} {
		lines := recipeLines(string(raw), target)
		if lines == nil {
			t.Errorf("%s target has no recipe lines in Makefile", target)
			continue
		}
		if !hasCountOne(lines) {
			t.Errorf("%s recipe lacks -count=1 on a single direct `go test` line: %q", target, lines)
		}
	}
}

// lintGoStandIn records each argument list it receives and fails the
// untagged or -tags=e2e invocation on request. It never runs Go itself.
const lintGoStandIn = `#!/bin/sh
echo "$*" >> "$LINT_RECORD"
case "$*" in
*-tags=e2e*) [ "$LINT_FAIL_TAGGED" = 1 ] && exit 1 ;;
*) [ "$LINT_FAIL_UNTAGGED" = 1 ] && exit 1 ;;
esac
exit 0
`

// TestMakefileLintTarget runs the real lint recipe, copied into a fresh
// directory, with the recording stand-in for go first on PATH. It guards the
// order of the two SA1019 invocations, that the tagged pass never runs after
// an untagged failure, that either failure fails make, and that a file named
// "lint" cannot shadow the phony target. No analyzer, network or Docker runs.
func TestMakefileLintTarget(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the go stand-in is a POSIX shell script")
	}
	makePath, err := exec.LookPath("make")
	if err != nil {
		t.Skipf("make not available: %v", err)
	}
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skipf("POSIX sh not available: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	const untagged = "tool staticcheck -checks=SA1019 ./..."
	const tagged = "tool staticcheck -tags=e2e -checks=SA1019 ./..."
	tests := []struct {
		name         string
		failUntagged bool
		failTagged   bool
		lintFile     bool
		wantRecord   []string
		wantFailure  bool
	}{
		{"both passes succeed in order", false, false, false, []string{untagged, tagged}, false},
		{"untagged failure stops before tagged pass", true, false, false, []string{untagged}, true},
		{"tagged failure after clean untagged pass fails", false, true, false, []string{untagged, tagged}, true},
		{"file named lint does not shadow target", false, false, true, []string{untagged, tagged}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			binDir := filepath.Join(dir, "bin")
			if err := os.Mkdir(binDir, 0o755); err != nil {
				t.Fatalf("create stand-in dir: %v", err)
			}
			if err := os.WriteFile(filepath.Join(binDir, "go"), []byte(lintGoStandIn), 0o755); err != nil {
				t.Fatalf("write go stand-in: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "Makefile"), raw, 0o644); err != nil {
				t.Fatalf("copy Makefile: %v", err)
			}
			if tt.lintFile {
				if err := os.WriteFile(filepath.Join(dir, "lint"), nil, 0o644); err != nil {
					t.Fatalf("create lint file: %v", err)
				}
			}
			recordPath := filepath.Join(dir, "record.txt")

			// Drop caller make overrides so they cannot mask a failing recipe.
			env := []string{
				"PATH=" + binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
				"LINT_RECORD=" + recordPath,
			}
			if tt.failUntagged {
				env = append(env, "LINT_FAIL_UNTAGGED=1")
			}
			if tt.failTagged {
				env = append(env, "LINT_FAIL_TAGGED=1")
			}
			for _, kv := range os.Environ() {
				if strings.HasPrefix(kv, "MAKEFLAGS=") || strings.HasPrefix(kv, "MFLAGS=") ||
					strings.HasPrefix(kv, "GNUMAKEFLAGS=") || strings.HasPrefix(kv, "PATH=") ||
					strings.HasPrefix(kv, "LINT_") {
					continue
				}
				env = append(env, kv)
			}

			cmd := exec.CommandContext(t.Context(), makePath, "lint")
			cmd.Dir = dir
			cmd.Env = env
			out, err := cmd.CombinedOutput()
			if tt.wantFailure && err == nil {
				t.Errorf("make lint succeeded, want failure; output:\n%s", out)
			}
			if !tt.wantFailure && err != nil {
				t.Errorf("make lint failed, want success: %v; output:\n%s", err, out)
			}

			var got []string
			data, readErr := os.ReadFile(recordPath)
			switch {
			case errors.Is(readErr, fs.ErrNotExist):
			case readErr != nil:
				t.Fatalf("read invocation record: %v", readErr)
			default:
				got = strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
			}
			if !slices.Equal(got, tt.wantRecord) {
				t.Errorf("go invocations = %q, want %q; output:\n%s", got, tt.wantRecord, out)
			}
		})
	}
}

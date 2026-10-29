package template

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/fixture"
)

// TestReadFileRejectsUnresolvedVariables replaces the old passthrough
// contract: an unresolved ${NAME} must fail instead of reaching Elasticsearch
// as a literal that the cluster would accept as valid configuration.
func TestReadFileRejectsUnresolvedVariables(t *testing.T) {
	t.Parallel()

	path := fixture.WriteTempJSON(t, t.TempDir(), `{"value":"${UNKNOWN_VAR}"}`)

	content, err := ReadFile(path, BuildVariables("runtime-index", nil))
	if content != nil {
		t.Fatalf("content = %q, want nil on unresolved variable", content)
	}
	var unresolved *UnresolvedError
	if !errors.As(err, &unresolved) {
		t.Fatalf("err = %v, want *UnresolvedError", err)
	}
	if unresolved.Path != path || !slices.Equal(unresolved.Names, []string{"UNKNOWN_VAR"}) {
		t.Fatalf("error mismatch: path=%q names=%v", unresolved.Path, unresolved.Names)
	}
}

// TestExpand pins resolution order and one-pass expansion.
// Not parallel: t.Setenv forbids t.Parallel.
func TestExpand(t *testing.T) {
	t.Setenv("TMPL_ENV_ONLY", "from-env")
	t.Setenv("TMPL_BOTH", "env-value")
	t.Setenv("TMPL_ENV_EMPTY", "")
	t.Setenv("TMPL_MAP_EMPTY", "env-value")
	t.Setenv("HOME", "/should-not-appear")

	tests := []struct {
		name      string
		input     string
		variables Variables
		want      string
		wantNames []string
	}{
		{"map hit", `${A}`, Variables{"A": "1"}, "1", nil},
		{"env hit", `${TMPL_ENV_ONLY}`, nil, "from-env", nil},
		{"unresolved", `x${TMPL_NOPE_1}y`, nil, `x${TMPL_NOPE_1}y`, []string{"TMPL_NOPE_1"}},
		{"several unresolved sorted and deduplicated", `${TMPL_NOPE_B}${TMPL_NOPE_A}${TMPL_NOPE_B}`, nil,
			`${TMPL_NOPE_B}${TMPL_NOPE_A}${TMPL_NOPE_B}`, []string{"TMPL_NOPE_A", "TMPL_NOPE_B"}},
		{"no variables", `plain text`, nil, "plain text", nil},
		{"map beats env", `${TMPL_BOTH}`, Variables{"TMPL_BOTH": "map-value"}, "map-value", nil},
		{"empty map value beats env", `${TMPL_MAP_EMPTY}`, Variables{"TMPL_MAP_EMPTY": ""}, "", nil},
		{"empty env value resolves", `a${TMPL_ENV_EMPTY}b`, nil, "ab", nil},
		{"replacement is not rescanned", `${A}`, Variables{"A": "${B}", "B": "no"}, "${B}", nil},
		{"bare dollar and CRLF survive", "$HOME\r\n$5.00\r\n", nil, "$HOME\r\n$5.00\r\n", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, names := Expand(tt.input, tt.variables)
			if got != tt.want {
				t.Fatalf("expanded = %q, want %q", got, tt.want)
			}
			if !slices.Equal(names, tt.wantNames) {
				t.Fatalf("unresolved = %v, want %v", names, tt.wantNames)
			}
		})
	}
}

// TestPreflightSkipsUnreadablePaths guards that an empty or
// missing path neither errors here (the read sites own that classification)
// nor hides unresolved names in the paths after it.
func TestPreflightSkipsUnreadablePaths(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.json")
	bad := fixture.WriteTempJSON(t, dir, `{"v":"${PREFLIGHT_TYPO}"}`)
	good := fixture.WriteTempJSON(t, dir, `{"v":"${INDEX}"}`)
	variables := BuildVariables("idx", nil)

	if err := Preflight(variables, "", missing, good); err != nil {
		t.Fatalf("preflight with only missing/resolved paths returned %v", err)
	}

	// The message is the public RunError cause text that package loader wraps
	// as ErrInvalidOptions, so it is pinned exactly; every failing file is
	// reported, joined by "; ".
	other := fixture.WriteTempJSON(t, dir, `{"v":"${PREFLIGHT_B}${PREFLIGHT_A}"}`)
	err := Preflight(variables, missing, bad, good, other)
	want := "unresolved template variables in " + bad + ": ${PREFLIGHT_TYPO}; " +
		"unresolved template variables in " + other + ": ${PREFLIGHT_A}, ${PREFLIGHT_B}"
	if err == nil || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
	var unresolved *UnresolvedError
	if errors.As(err, &unresolved) {
		t.Fatal("Preflight must return the joined diagnostic, not a single file's typed error")
	}
}

// TestBracelessDollarNameIsNotSubstituted guards the decision that only the
// braced ${NAME} form is templated: a literal "$HOME" in a config asset must
// never be rewritten, even when HOME is in the variable map and environment.
// Not parallel: t.Setenv forbids t.Parallel.
func TestBracelessDollarNameIsNotSubstituted(t *testing.T) {
	t.Setenv("HOME", "/should-not-appear")

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"home", `{"v":"$HOME"}`, `{"v":"$HOME"}`},
		{"trailing dollar", `{"v":"foo$"}`, `{"v":"foo$"}`},
		{"currency", `{"v":"$5.00"}`, `{"v":"$5.00"}`},
		{"bare trailing dollar", `{"v":"$"}`, `{"v":"$"}`},
		{"braced still resolves", `{"v":"${INDEX}"}`, `{"v":"runtime-index"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variables := BuildVariables("runtime-index", map[string]string{"HOME": "/also-should-not-appear"})
			path := fixture.WriteTempJSON(t, t.TempDir(), tt.input)

			content, err := ReadFile(path, variables)
			if err != nil {
				t.Fatalf("ReadFile returned error: %v", err)
			}
			if got := string(content); got != tt.want {
				t.Fatalf("content mismatch: got %q want %q", got, tt.want)
			}
		})
	}
}

// TestBuildVariablesIncludesAdditionalValues verifies behavior for the related scenario.
func TestBuildVariablesIncludesAdditionalValues(t *testing.T) {
	t.Parallel()

	variables := BuildVariables("collection-index", map[string]string{
		"BINDER_INDEX": "binder-index",
	})

	if got := variables["INDEX"]; got != "collection-index" {
		t.Fatalf("INDEX variable mismatch: got %q want %q", got, "collection-index")
	}
	if got := variables["BINDER_INDEX"]; got != "binder-index" {
		t.Fatalf("BINDER_INDEX variable mismatch: got %q want %q", got, "binder-index")
	}
}

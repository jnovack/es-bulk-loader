package repocheck

import (
	"cmp"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// actionPin is one remote `uses:` reference in a workflow file. file is the
// base name of the workflow, set by readActionPins; actionPins leaves it empty
// because it sees only one file's text.
type actionPin struct {
	file   string
	action string // owner/repo; a subpath such as owner/repo/sub keys on owner/repo
	ref    string // text after the last @
	line   int    // 1-based
}

// actionPins returns the remote action pins in one workflow file. It supports
// only the single-line `uses:` form this repository writes, optionally as a
// `- uses:` list item. Comment lines, local `./` paths, `docker://` images and
// malformed remote values are not pins. The scanner does not interpret YAML,
// so flow mappings, aliases, multiline values and `uses:` text inside block
// scalars are outside its guarantees.
func actionPins(workflow string) []actionPin {
	var out []actionPin
	for i, raw := range strings.Split(workflow, "\n") {
		text := strings.TrimSpace(strings.TrimSuffix(raw, "\r"))
		if strings.HasPrefix(text, "#") {
			continue
		}
		text = strings.TrimPrefix(text, "- ")
		value, ok := strings.CutPrefix(strings.TrimSpace(text), "uses:")
		if !ok {
			continue
		}
		action, ref, ok := remoteActionRef(value)
		if !ok {
			continue
		}
		out = append(out, actionPin{action: action, ref: ref, line: i + 1})
	}
	return out
}

// remoteActionRef parses the value of a uses: key into its owner/repo action
// and ref. A trailing comment is cut at a `#` preceded by whitespace, and one
// matching pair of quotes is removed. An unquoted ref ends at whitespace. It
// reports false for local paths, docker:// images and malformed remote values.
func remoteActionRef(value string) (action, ref string, ok bool) {
	value = strings.TrimSpace(value)
	if i := inlineCommentStart(value); i >= 0 {
		value = strings.TrimSpace(value[:i])
	}
	if n := len(value); n >= 2 && (value[0] == '"' || value[0] == '\'') && value[n-1] == value[0] {
		value = value[1 : n-1]
	} else if fields := strings.Fields(value); len(fields) > 0 {
		value = fields[0]
	} else {
		return "", "", false
	}
	if strings.HasPrefix(value, "./") || strings.HasPrefix(value, "docker://") {
		return "", "", false
	}
	at := strings.LastIndex(value, "@")
	if at < 0 || at == len(value)-1 {
		return "", "", false
	}
	segments := strings.Split(value[:at], "/")
	if len(segments) < 2 || segments[0] == "" || segments[1] == "" {
		return "", "", false
	}
	return segments[0] + "/" + segments[1], value[at+1:], true
}

// inlineCommentStart returns the index of a `#` that begins a trailing comment,
// meaning one preceded by whitespace, or -1 when there is none. A `#` inside a
// ref such as x/y@v1#a is part of the ref, not a comment.
func inlineCommentStart(v string) int {
	for i := 1; i < len(v); i++ {
		if v[i] == '#' && (v[i-1] == ' ' || v[i-1] == '\t') {
			return i
		}
	}
	return -1
}

// actionPinConflicts reports each action pinned at more than one exact ref,
// one diagnostic per action. Refs are compared as text, so @v7 and @v7.0.1 are
// a conflict even though both name the same major. That is deliberate: the
// workflows must agree on identical refs, not on equal majors. The guard does
// not judge whether a ref is current or secure. Actions, refs and locations
// are sorted so the output is deterministic whatever the input order.
func actionPinConflicts(pins map[string][]actionPin) []string {
	var out []string
	for _, action := range slices.Sorted(maps.Keys(pins)) {
		byRef := map[string][]actionPin{}
		for _, p := range pins[action] {
			byRef[p.ref] = append(byRef[p.ref], p)
		}
		if len(byRef) < 2 {
			continue
		}
		var refs []string
		for _, ref := range slices.Sorted(maps.Keys(byRef)) {
			locs := byRef[ref]
			slices.SortFunc(locs, comparePinLocation)
			where := make([]string, 0, len(locs))
			for _, p := range locs {
				where = append(where, fmt.Sprintf("%s:%d", p.file, p.line))
			}
			refs = append(refs, fmt.Sprintf("%q at %s", ref, strings.Join(where, ", ")))
		}
		out = append(out, fmt.Sprintf("action %s pinned at multiple refs: %s", action, strings.Join(refs, "; ")))
	}
	return out
}

// comparePinLocation orders pins by file name, then by line number. Lines
// compare numerically, so line 100 sorts after line 39.
func comparePinLocation(a, b actionPin) int {
	if c := strings.Compare(a.file, b.file); c != 0 {
		return c
	}
	return cmp.Compare(a.line, b.line)
}

// readActionPins reads every immediate *.yml and *.yaml file in dir, in
// filename order, and groups their remote pins by action. Other extensions
// are ignored. A directory named like a workflow fails the read instead of
// being skipped, and a missing or empty workflow set fails explicitly, so the
// guard cannot pass by finding nothing to check.
func readActionPins(dir string) (map[string][]actionPin, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read workflow directory %s: %w", dir, err)
	}
	var names []string
	for _, e := range entries {
		if n := e.Name(); strings.HasSuffix(n, ".yml") || strings.HasSuffix(n, ".yaml") {
			names = append(names, n)
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no workflow files (*.yml, *.yaml) in %s", dir)
	}
	pins := map[string][]actionPin{}
	total := 0
	for _, name := range names {
		path := filepath.Join(dir, name)
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read workflow %s: %w", path, err)
		}
		for _, p := range actionPins(string(raw)) {
			p.file = name
			pins[p.action] = append(pins[p.action], p)
			total++
		}
	}
	if total == 0 {
		return nil, fmt.Errorf("no remote action pins in workflows in %s", dir)
	}
	return pins, nil
}

func TestActionPins(t *testing.T) {
	tests := []struct {
		name     string
		workflow string
		want     []actionPin
	}{
		{
			name:     "uses under name",
			workflow: "steps:\n  - name: Check out\n    uses: actions/checkout@v7\n",
			want:     []actionPin{{action: "actions/checkout", ref: "v7", line: 3}},
		},
		{
			name:     "list item form",
			workflow: "- uses: actions/setup-go@v7\n",
			want:     []actionPin{{action: "actions/setup-go", ref: "v7", line: 1}},
		},
		{
			name:     "trailing inline comment ends the ref",
			workflow: "    uses: actions/checkout@v7 # pinned\n",
			want:     []actionPin{{action: "actions/checkout", ref: "v7", line: 1}},
		},
		{
			name:     "single-quoted ref with trailing comment",
			workflow: "uses: 'actions/checkout@v7' # note\n",
			want:     []actionPin{{action: "actions/checkout", ref: "v7", line: 1}},
		},
		{
			name:     "double-quoted ref",
			workflow: "uses: \"docker/login-action@v4\"\n",
			want:     []actionPin{{action: "docker/login-action", ref: "v4", line: 1}},
		},
		{
			name:     "double-quoted ref with trailing comment",
			workflow: "uses: \"docker/login-action@v4\" # note\n",
			want:     []actionPin{{action: "docker/login-action", ref: "v4", line: 1}},
		},
		{
			name:     "hash inside a ref is not a comment",
			workflow: "uses: x/y@v1#frag\n",
			want:     []actionPin{{action: "x/y", ref: "v1#frag", line: 1}},
		},
		{
			name:     "subpath keys on owner/repo",
			workflow: "uses: owner/repo/sub@v1\n",
			want:     []actionPin{{action: "owner/repo", ref: "v1", line: 1}},
		},
		{
			name:     "local path is ignored",
			workflow: "uses: ./local-action\n",
			want:     nil,
		},
		{
			name:     "docker image reference is ignored",
			workflow: "uses: docker://alpine:3\n",
			want:     nil,
		},
		{
			name:     "commented uses line is ignored",
			workflow: "# uses: x/y@v1\n",
			want:     nil,
		},
		{
			name:     "CRLF line endings",
			workflow: "steps:\r\n  - uses: actions/checkout@v7\r\n",
			want:     []actionPin{{action: "actions/checkout", ref: "v7", line: 2}},
		},
		{
			name:     "multiple actions in one file",
			workflow: "- uses: actions/checkout@v7\n- run: go test ./...\n- uses: docker/setup-buildx-action@v4\n",
			want: []actionPin{
				{action: "actions/checkout", ref: "v7", line: 1},
				{action: "docker/setup-buildx-action", ref: "v4", line: 3},
			},
		},
		{
			name:     "empty input",
			workflow: "",
			want:     nil,
		},
		{
			name:     "malformed remote values are ignored",
			workflow: "uses: owner@v1\nuses: x/y\nuses: x/y@\nuses: /repo/sub@v1\nuses: owner//sub@v1\nuses:\n",
			want:     nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := actionPins(tt.workflow)
			if !slices.Equal(got, tt.want) {
				t.Errorf("actionPins(%q) = %+v, want %+v", tt.workflow, got, tt.want)
			}
		})
	}
}

func TestActionPinConflicts(t *testing.T) {
	tests := []struct {
		name string
		pins map[string][]actionPin
		want []string
	}{
		{
			name: "agreement across files, unsorted input",
			pins: map[string][]actionPin{
				"actions/checkout": {
					{file: "b.yml", ref: "v7", line: 5},
					{file: "a.yml", ref: "v7", line: 3},
				},
			},
			want: nil,
		},
		{
			name: "cross-major disagreement across files",
			pins: map[string][]actionPin{
				"actions/checkout": {
					{file: "b.yml", ref: "v7", line: 4},
					{file: "a.yml", ref: "v6", line: 3},
				},
			},
			want: []string{`action actions/checkout pinned at multiple refs: "v6" at a.yml:3; "v7" at b.yml:4`},
		},
		{
			name: "same major, different exact ref",
			pins: map[string][]actionPin{
				"actions/checkout": {
					{file: "a.yml", ref: "v7", line: 3},
					{file: "b.yml", ref: "v7.0.1", line: 4},
				},
			},
			want: []string{`action actions/checkout pinned at multiple refs: "v7" at a.yml:3; "v7.0.1" at b.yml:4`},
		},
		{
			name: "disagreement within one file",
			pins: map[string][]actionPin{
				"actions/checkout": {
					{file: "a.yml", ref: "v7", line: 9},
					{file: "a.yml", ref: "v6", line: 3},
				},
			},
			want: []string{`action actions/checkout pinned at multiple refs: "v6" at a.yml:3; "v7" at a.yml:9`},
		},
		{
			name: "multiple independent actions, one conflicting",
			pins: map[string][]actionPin{
				"docker/login-action": {
					{file: "b.yml", ref: "v4", line: 2},
					{file: "b.yml", ref: "v3", line: 8},
				},
				"actions/setup-go": {
					{file: "a.yml", ref: "v7", line: 1},
					{file: "b.yml", ref: "v7", line: 6},
				},
				"actions/checkout": {
					{file: "a.yml", ref: "v6", line: 4},
					{file: "b.yml", ref: "v7", line: 7},
				},
			},
			want: []string{
				`action actions/checkout pinned at multiple refs: "v6" at a.yml:4; "v7" at b.yml:7`,
				`action docker/login-action pinned at multiple refs: "v3" at b.yml:8; "v4" at b.yml:2`,
			},
		},
		{
			name: "repeated occurrences at one ref are all reported, lines sorted numerically",
			pins: map[string][]actionPin{
				"actions/checkout": {
					{file: "a.yml", ref: "v7", line: 100},
					{file: "b.yml", ref: "v7", line: 2},
					{file: "a.yml", ref: "v7", line: 39},
					{file: "c.yml", ref: "v6", line: 1},
				},
			},
			want: []string{`action actions/checkout pinned at multiple refs: "v6" at c.yml:1; "v7" at a.yml:39, a.yml:100, b.yml:2`},
		},
		{
			name: "empty input",
			pins: map[string][]actionPin{},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := actionPinConflicts(tt.pins)
			if !slices.Equal(got, tt.want) {
				t.Errorf("actionPinConflicts() =\n%q\nwant\n%q", got, tt.want)
			}
		})
	}
}

func TestReadActionPins(t *testing.T) {
	tests := []struct {
		name     string
		files    map[string]string
		dirs     []string // directories created inside the workflow directory
		missing  bool     // point readActionPins at a directory that does not exist
		wantPins map[string][]actionPin
		wantErr  string // substring of the error; empty when a read must succeed
	}{
		{
			name: ".yml and .yaml both contribute, disagreeing refs are kept",
			files: map[string]string{
				"a.yml":  "steps:\n  - uses: actions/checkout@v7\n",
				"b.yaml": "steps:\n  - uses: actions/checkout@v6\n",
			},
			wantPins: map[string][]actionPin{
				"actions/checkout": {
					{file: "a.yml", action: "actions/checkout", ref: "v7", line: 2},
					{file: "b.yaml", action: "actions/checkout", ref: "v6", line: 2},
				},
			},
		},
		{
			name: "subpath pins group under owner/repo",
			files: map[string]string{
				"a.yml": "uses: owner/repo/sub@v1\nuses: owner/repo@v1\n",
			},
			wantPins: map[string][]actionPin{
				"owner/repo": {
					{file: "a.yml", action: "owner/repo", ref: "v1", line: 1},
					{file: "a.yml", action: "owner/repo", ref: "v1", line: 2},
				},
			},
		},
		{
			name: "unrelated extensions are ignored",
			files: map[string]string{
				"a.yml":    "uses: actions/checkout@v7\n",
				"notes.md": "uses: actions/checkout@v6\n",
			},
			wantPins: map[string][]actionPin{
				"actions/checkout": {
					{file: "a.yml", action: "actions/checkout", ref: "v7", line: 1},
				},
			},
		},
		{
			name:    "missing directory fails",
			missing: true,
			wantErr: "read workflow directory",
		},
		{
			name:    "empty directory fails",
			wantErr: "no workflow files",
		},
		{
			name: "only non-workflow files fail",
			files: map[string]string{
				"notes.txt": "uses: actions/checkout@v7\n",
			},
			wantErr: "no workflow files",
		},
		{
			name: "only comments, local and docker references fail",
			files: map[string]string{
				"a.yml": "# uses: actions/checkout@v7\nuses: ./local\nuses: docker://alpine:3\n",
			},
			wantErr: "no remote action pins",
		},
		{
			name: "directory named like a workflow fails the read",
			files: map[string]string{
				"good.yml": "uses: actions/checkout@v7\n",
			},
			dirs:    []string{"bad.yml"},
			wantErr: "bad.yml",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "workflows")
			if !tt.missing {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				writeTree(t, dir, tt.files)
				for _, d := range tt.dirs {
					if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
						t.Fatal(err)
					}
				}
			}
			got, err := readActionPins(dir)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("readActionPins error = %v, want one containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("readActionPins: %v", err)
			}
			if !reflect.DeepEqual(got, tt.wantPins) {
				t.Errorf("readActionPins() = %+v, want %+v", got, tt.wantPins)
			}
		})
	}
}

// TestWorkflowActionPinsConsistent guards identical refs across the workflow
// files in .github/workflows, including go-check.yml from CI-GOCHECK-01. It
// fails when one action is pinned at two different refs, even two refs within
// the same major, because a drifting pin is the failure it exists to catch.
// It does not hardcode any action or ref, so a new workflow is checked
// automatically. It is not an Actions linter and does not judge whether a ref
// is current or secure.
func TestWorkflowActionPinsConsistent(t *testing.T) {
	pins, err := readActionPins(filepath.Join("..", "..", ".github", "workflows"))
	if err != nil {
		t.Fatalf("read workflow action pins: %v", err)
	}
	for _, msg := range actionPinConflicts(pins) {
		t.Error(msg)
	}
}

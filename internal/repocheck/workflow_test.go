package repocheck

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// goBuildCommands returns the `go build` shell commands in a workflow file,
// with each backslash-continued group joined into one line. YAML comments and
// other text are ignored because only a line that starts with `go build ` is
// returned. It returns nil when there is none.
//
// A `#` comment line is never joined to the next line, even when it ends in
// `\`. In shell (and YAML) a comment ends at the newline. Joining it would
// swallow a real `go build` on the following line into the comment, and the
// guard would silently miss that build.
func goBuildCommands(workflow string) []string {
	lines := strings.Split(workflow, "\n")
	var out []string
	for i := 0; i < len(lines); i++ {
		text := strings.TrimSpace(strings.TrimSuffix(lines[i], "\r"))
		if strings.HasPrefix(text, "#") {
			continue
		}
		for strings.HasSuffix(text, `\`) {
			text = strings.TrimSpace(strings.TrimSuffix(text, `\`))
			if i+1 >= len(lines) {
				break
			}
			i++
			text += " " + strings.TrimSpace(strings.TrimSuffix(lines[i], "\r"))
		}
		text = strings.TrimSpace(text)
		if strings.HasPrefix(text, "go build ") {
			out = append(out, text)
		}
	}
	return out
}

// isFileListBuild reports whether a go build command names .go files, one or
// several, instead of a package. The package target is the final argument,
// so a command that ends in .go is a file-list build. A file list compiles
// only the named files, not their package, so the build breaks as
// soon as main uses a helper from a sibling file. That happened with
// withEnvHidden in cmd/es-bulk-loader/flagenv.go (CI-RELBUILD-01). The check
// looks at the end of the command rather than splitting on whitespace,
// because the target `${{ github.event.repository.name }}` contains spaces.
func isFileListBuild(cmd string) bool {
	return strings.HasSuffix(strings.TrimSpace(cmd), ".go")
}

func TestGoBuildCommandsFileListTarget(t *testing.T) {
	// realStep is the shape of the release workflow's Build binary step, with
	// the target appended. It has backslash continuations and a quoted
	// -ldflags that holds a $(date ...) substitution.
	realStep := func(target string) string {
		return "run: |\n" +
			"  go build \\\n" +
			"    -ldflags \"-s -w \\\n" +
			"      -X main.version=v \\\n" +
			"      -X main.buildRFC3339=$(date -u +\"%Y\")\" \\\n" +
			"    -o out \\\n" +
			"    " + target + "\n"
	}
	const pkgTarget = "./cmd/${{ github.event.repository.name }}"

	tests := []struct {
		name         string
		workflow     string
		wantCount    int
		wantFileList []bool
	}{
		{
			name:         "package target",
			workflow:     "run: |\n  go build -o x " + pkgTarget + "\n",
			wantCount:    1,
			wantFileList: []bool{false},
		},
		{
			name:         "single file target",
			workflow:     "run: |\n  go build -o x " + pkgTarget + "/main.go\n",
			wantCount:    1,
			wantFileList: []bool{true},
		},
		{
			name:         "continued lines with quoted ldflags, package target",
			workflow:     realStep(pkgTarget),
			wantCount:    1,
			wantFileList: []bool{false},
		},
		{
			name:         "continued lines, single file target",
			workflow:     realStep(pkgTarget + "/main.go"),
			wantCount:    1,
			wantFileList: []bool{true},
		},
		{
			name:         "no go build",
			workflow:     "run: |\n  go test ./...\n",
			wantCount:    0,
			wantFileList: nil,
		},
		{
			name:         "yaml comment mentioning go build",
			workflow:     "# go build ./cmd/x/main.go\nrun: echo hi\n",
			wantCount:    0,
			wantFileList: nil,
		},
		{
			name:         "CRLF line endings",
			workflow:     strings.ReplaceAll(realStep(pkgTarget+"/main.go"), "\n", "\r\n"),
			wantCount:    1,
			wantFileList: []bool{true},
		},
		{
			name:         "continuation on last line",
			workflow:     "go build -o x ./cmd/x/main.go \\",
			wantCount:    1,
			wantFileList: []bool{true},
		},
		{
			name:         "trailing continuation followed by final newline",
			workflow:     "go build -o x ./cmd/x/main.go \\\n",
			wantCount:    1,
			wantFileList: []bool{true},
		},
		{
			name:         "shell comment line inside run block",
			workflow:     "run: |\n  # build the loader\n  go build -o x ./cmd/x/main.go\n",
			wantCount:    1,
			wantFileList: []bool{true},
		},
		{
			name:         "package target followed by file target",
			workflow:     "run: |\n  go build -o x ./cmd/x\n  go build -o y ./cmd/x/main.go\n",
			wantCount:    2,
			wantFileList: []bool{false, true},
		},
		{
			name:         "comment line ending in backslash does not swallow the next build",
			workflow:     "run: |\n  # note \\\n  go build -o x ./cmd/x/main.go\n",
			wantCount:    1,
			wantFileList: []bool{true},
		},
		{
			name:         "multi-file list target",
			workflow:     "run: |\n  go build -o x ./cmd/x/main.go ./cmd/x/flagenv.go\n",
			wantCount:    1,
			wantFileList: []bool{true},
		},
		{
			name:         "empty input",
			workflow:     "",
			wantCount:    0,
			wantFileList: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmds := goBuildCommands(tt.workflow)
			if len(cmds) != tt.wantCount {
				t.Fatalf("goBuildCommands returned %d commands, want %d: %q", len(cmds), tt.wantCount, cmds)
			}
			if len(tt.wantFileList) != tt.wantCount {
				t.Fatalf("test case bug: %d wantFileList entries for %d commands", len(tt.wantFileList), tt.wantCount)
			}
			for i, cmd := range cmds {
				if got := isFileListBuild(cmd); got != tt.wantFileList[i] {
					t.Errorf("isFileListBuild(%q) = %v, want %v", cmd, got, tt.wantFileList[i])
				}
			}
		})
	}
}

// TestReleaseWorkflowBuildsPackageDirectory guards CI-RELBUILD-01. A release
// tag with a file-list build target publishes a GitHub release with no
// binaries, because create_release runs before the build matrix does. The
// test fails when the workflow has no go build command at all, so renaming or
// restructuring the workflow cannot pass silently.
func TestReleaseWorkflowBuildsPackageDirectory(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "go-package.yml"))
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}
	cmds := goBuildCommands(string(raw))
	if len(cmds) == 0 {
		t.Fatalf("no go build command found in go-package.yml; the release build guard cannot verify it")
	}
	for _, cmd := range cmds {
		if isFileListBuild(cmd) {
			t.Errorf("release workflow file-list build: %q compiles only the named file; build the package directory instead", cmd)
		}
	}
}

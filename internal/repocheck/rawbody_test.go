package repocheck

// LOG-RAWBODY-01 guard: production code under pkg/ and cmd/ must not put a raw
// Elasticsearch response body in a log field or an error. The scan is a bounded
// syntactic check, not a data-flow privacy proof. It flags two shapes only:
//
//   - a call to a method named Str whose first argument is the string literal
//     "body" (the former Str("body", string(body)) log field);
//   - a fmt.Errorf argument that is, or directly wraps through one call such as
//     strings.TrimSpace, a string(x) conversion where x is an identifier named
//     like a response body (body, responseBody, data, ...).
//
// It does not track variable aliases, arbitrary nested formatting, RawJSON or
// parsed fields: application-level failure text inside 2xx bodies is out of
// scope and tracked as LOG-2XXFAIL-01. The reasonable fix for a finding is to
// build an esresponse.Rejection from the bounded read and log or return that.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// bodyIdent matches the identifiers response bodies are held in: body,
// responseBody, errBody, and data (the name the migrated sites use for
// esresponse.ReadBounded's result).
var bodyIdent = regexp.MustCompile(`(?i)body|^data$`)

// rawBodyFindings returns one "file:line: reason" per leak in file, and a
// diagnostic for a construct it cannot scan (a dot import of fmt).
func rawBodyFindings(fset *token.FileSet, file *ast.File) []string {
	var out []string
	at := func(p token.Pos) string {
		pos := fset.Position(p)
		return fmt.Sprintf("%s:%d", pos.Filename, pos.Line)
	}

	// Resolve fmt by import path, so an ordinary alias cannot hide Errorf.
	fmtNames := map[string]bool{}
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			out = append(out, fmt.Sprintf("%s: unquote import path %s: %v", at(imp.Pos()), imp.Path.Value, err))
			continue
		}
		if path != "fmt" {
			continue
		}
		name := "fmt"
		if imp.Name != nil {
			name = imp.Name.Name
		}
		switch name {
		case ".":
			out = append(out, fmt.Sprintf("%s: dot import of fmt is unsupported: unqualified Errorf calls cannot be scanned", at(imp.Pos())))
		case "_":
		default:
			fmtNames[name] = true
		}
	}

	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch {
		case sel.Sel.Name == "Str" && len(call.Args) > 0 && isStringLit(call.Args[0], "body"):
			out = append(out, fmt.Sprintf("%s: Str(\"body\", ...) logs a raw response body; log an esresponse.Rejection (es_error) instead", at(call.Pos())))
		case sel.Sel.Name == "Errorf" && isFmtIdent(sel.X, fmtNames):
			for _, arg := range call.Args {
				if name, ok := bodyConversion(arg); ok {
					out = append(out, fmt.Sprintf("%s: fmt.Errorf formats string(%s), a raw response body; wrap an esresponse.Rejection instead", at(arg.Pos()), name))
				}
			}
		}
		return true
	})
	return out
}

// isStringLit reports whether e, parentheses peeled, is the string literal
// want in any quoting form (interpreted or raw).
func isStringLit(e ast.Expr, want string) bool {
	lit, ok := unparen(e).(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	got, err := strconv.Unquote(lit.Value)
	return err == nil && got == want
}

// isFmtIdent reports whether e is an unshadowed identifier bound to the fmt
// import. A resolved Obj means a local declaration shadows the import.
func isFmtIdent(e ast.Expr, names map[string]bool) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Obj == nil && names[id.Name]
}

func unparen(e ast.Expr) ast.Expr {
	for {
		p, ok := e.(*ast.ParenExpr)
		if !ok {
			return e
		}
		e = p.X
	}
}

// bodyConversion reports whether arg is string(x), or one call directly
// wrapping string(x), with x an identifier named like a response body.
func bodyConversion(arg ast.Expr) (string, bool) {
	call, ok := unparen(arg).(*ast.CallExpr)
	if !ok {
		return "", false
	}
	if name, ok := stringOfBody(call); ok {
		return name, true
	}
	for _, inner := range call.Args {
		if innerCall, ok := unparen(inner).(*ast.CallExpr); ok {
			if name, ok := stringOfBody(innerCall); ok {
				return name, true
			}
		}
	}
	return "", false
}

// stringOfBody matches the conversion string(x) with a body-like identifier x.
func stringOfBody(call *ast.CallExpr) (string, bool) {
	fn, ok := call.Fun.(*ast.Ident)
	if !ok || fn.Name != "string" || len(call.Args) != 1 {
		return "", false
	}
	x, ok := unparen(call.Args[0]).(*ast.Ident)
	if !ok || !bodyIdent.MatchString(x.Name) {
		return "", false
	}
	return x.Name, true
}

// scanRawBody parses every non-test .go file under each root (skipping
// testdata directories) and returns the findings and the number of files
// parsed. A missing or empty root, a walk, read or parse error is an error, so
// a moved tree cannot pass silently.
func scanRawBody(roots ...string) (findings []string, parsed int, err error) {
	for _, root := range roots {
		rootParsed := 0
		walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, src, 0)
			if err != nil {
				return fmt.Errorf("parse %s: %w", path, err)
			}
			findings = append(findings, rawBodyFindings(fset, file)...)
			rootParsed++
			return nil
		})
		if walkErr != nil {
			return nil, 0, fmt.Errorf("scan %s: %w", root, walkErr)
		}
		if rootParsed == 0 {
			return nil, 0, fmt.Errorf("scan %s: no production Go files found", root)
		}
		parsed += rootParsed
	}
	return findings, parsed, nil
}

// TestRawBodyNotInLogsOrErrors fails when production code reintroduces a raw
// response body in a log field or error (see the file comment for the exact,
// bounded scope).
func TestRawBodyNotInLogsOrErrors(t *testing.T) {
	findings, parsed, err := scanRawBody(filepath.Join("..", "..", "pkg"), filepath.Join("..", "..", "cmd"))
	if err != nil {
		t.Fatal(err)
	}
	if parsed == 0 {
		t.Fatal("the scan parsed no files")
	}
	for _, f := range findings {
		t.Error(f)
	}
}

func parseForTest(t *testing.T, name, src string) (*token.FileSet, *ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return fset, file
}

func TestRawBodyFindings(t *testing.T) {
	t.Parallel()

	const head = "package p\nimport (\n\t\"fmt\"\n\t\"strings\"\n)\nvar _ = strings.TrimSpace\nvar _ = fmt.Sprint\nfunc f(ev ev, body, responseBody, data, name []byte, err error) {\n"
	tests := []struct {
		name string
		body string
		want []string // substrings of the findings, in order; nil means clean
	}{
		{"Str body of string(body)", `ev.Str("body", string(body))`, []string{"x.go:9: Str(\"body\""}},
		{"raw string literal key", "ev.Str(`body`, string(body))", []string{"Str(\"body\""}},
		{"parenthesized key", `ev.Str(("body"), "x")`, []string{"Str(\"body\""}},
		{"es_error passes", `ev.Str("es_error", "x")`, nil},
		{"other key passes", `ev.Str("policy", string(name))`, nil},
		{"Errorf of string(body)", `_ = fmt.Errorf("%s", string(body))`, []string{"x.go:9: fmt.Errorf formats string(body)"}},
		{"Errorf of TrimSpace(string(responseBody))", `_ = fmt.Errorf("%s", strings.TrimSpace(string(responseBody)))`, []string{"string(responseBody)"}},
		{"Errorf of string(data)", `_ = fmt.Errorf("%s", string(data))`, []string{"string(data)"}},
		{"parenthesized conversion", `_ = fmt.Errorf("%s", (string(body)))`, []string{"string(body)"}},
		{"Errorf wrapping an error passes", `_ = fmt.Errorf("%w", err)`, nil},
		{"string of a non-body name passes", `_ = fmt.Errorf("%s", string(name))`, nil},
		{"two findings on two lines", "ev.Str(\"body\", \"x\")\n_ = fmt.Errorf(\"%s\", string(body))", []string{"x.go:9:", "x.go:10:"}},
		{"CRLF source still reports its line", "ev.Str(\"body\", \"x\")\r\n_ = fmt.Errorf(\"%s\", string(body))", []string{"x.go:9:", "x.go:10:"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fset, file := parseForTest(t, "x.go", head+tt.body+"\n}\ntype ev interface{ Str(k, v string) ev }\n")
			got := rawBodyFindings(fset, file)
			if len(got) != len(tt.want) {
				t.Fatalf("findings = %q, want %d matching %q", got, len(tt.want), tt.want)
			}
			for i, want := range tt.want {
				if !strings.Contains(got[i], want) {
					t.Fatalf("finding %d = %q, want it to contain %q", i, got[i], want)
				}
			}
		})
	}

	t.Run("aliased fmt is still resolved", func(t *testing.T) {
		t.Parallel()
		fset, file := parseForTest(t, "x.go", "package p\nimport f \"fmt\"\nfunc g(body []byte) { _ = f.Errorf(\"%s\", string(body)) }\n")
		if got := rawBodyFindings(fset, file); len(got) != 1 {
			t.Fatalf("findings = %q, want 1", got)
		}
	})
	t.Run("an Errorf on another package passes", func(t *testing.T) {
		t.Parallel()
		fset, file := parseForTest(t, "x.go", "package p\nimport errs \"example.com/errs\"\nfunc g(body []byte) { _ = errs.Errorf(\"%s\", string(body)) }\n")
		if got := rawBodyFindings(fset, file); len(got) != 0 {
			t.Fatalf("findings = %q, want none", got)
		}
	})
	t.Run("a dot import of fmt fails clearly", func(t *testing.T) {
		t.Parallel()
		fset, file := parseForTest(t, "x.go", "package p\nimport . \"fmt\"\nfunc g(body []byte) { _ = Errorf(\"%s\", string(body)) }\n")
		got := rawBodyFindings(fset, file)
		if len(got) != 1 || !strings.Contains(got[0], "dot import of fmt is unsupported") {
			t.Fatalf("findings = %q, want the dot-import diagnostic", got)
		}
	})
}

// writeTree writes files (path relative to dir) under dir.
func writeTree(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		path := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

const leakySource = "package p\nfunc f(ev interface{ Str(k, v string) any }, body []byte) { ev.Str(\"body\", string(body)) }\n"

// TestRawBodyScanDiscovery covers the file walk: nested production files are
// scanned, _test.go and testdata are skipped, and an empty or missing root or
// a malformed file is an error rather than a silent pass.
func TestRawBodyScanDiscovery(t *testing.T) {
	t.Parallel()

	t.Run("nested production file is found, test and testdata are skipped", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeTree(t, dir, map[string]string{
			"a/b/leak.go":           leakySource,
			"a/b/leak_test.go":      leakySource,
			"a/testdata/ignore.go":  leakySource,
			"a/clean.go":            "package p\n",
			"a/b/c/also_clean.go":   "package c\n",
			"a/b/ignored_notgo.txt": leakySource,
		})
		findings, parsed, err := scanRawBody(dir)
		if err != nil {
			t.Fatal(err)
		}
		if parsed != 3 {
			t.Fatalf("parsed = %d, want 3 (leak.go, clean.go, also_clean.go)", parsed)
		}
		if len(findings) != 1 || !strings.Contains(findings[0], filepath.Join("a", "b", "leak.go")+":2") {
			t.Fatalf("findings = %q, want one at a/b/leak.go:2", findings)
		}
	})
	t.Run("a missing root is an error", func(t *testing.T) {
		t.Parallel()
		if _, _, err := scanRawBody(filepath.Join(t.TempDir(), "absent")); err == nil {
			t.Fatal("want an error for a missing root")
		}
	})
	t.Run("an empty root is an error", func(t *testing.T) {
		t.Parallel()
		if _, _, err := scanRawBody(t.TempDir()); err == nil || !strings.Contains(err.Error(), "no production Go files") {
			t.Fatalf("err = %v, want the empty-root error", err)
		}
	})
	t.Run("only test files in a root is an error", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeTree(t, dir, map[string]string{"only_test.go": "package p\n"})
		if _, _, err := scanRawBody(dir); err == nil {
			t.Fatal("want an error when a root has no production file")
		}
	})
	t.Run("malformed source is an error", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeTree(t, dir, map[string]string{"bad.go": "package p\nfunc ("})
		if _, _, err := scanRawBody(dir); err == nil {
			t.Fatal("want a parse error")
		}
	})
	t.Run("a filesystem error is an error", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeTree(t, dir, map[string]string{"ok.go": "package p\n", "locked/x.go": "package p\n"})
		locked := filepath.Join(dir, "locked")
		if err := os.Chmod(locked, 0o000); err != nil {
			t.Skipf("cannot remove directory permissions: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
		if _, err := os.ReadDir(locked); err == nil {
			t.Skip("permissions are not enforced here (running as root?)")
		}
		if _, _, err := scanRawBody(dir); err == nil {
			t.Fatal("want a walk error for an unreadable directory")
		}
	})
}

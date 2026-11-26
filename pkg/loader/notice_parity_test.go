package loader

// Parity guard for the notice and resource-kind constants. Their string values
// live once, in internal/observe; package loader exposes each as a typed
// conversion (NoticeX = NoticeCode(observe.CodeX)). A new internal constant
// without a public twin would let an internal emit carry a code callers cannot
// name, so this test discovers the internal constants from their declarations
// (never from a second hand-written list) and requires each to have a twin of
// the right public type converting exactly that constant. A same-name public
// constant with its own literal is not a twin: the value could drift.
//
// The guard is bidirectional. Every public constant of a group's public type
// must be exactly T(observe.<internalPrefix>X) under its own name (a public
// literal such as NoticeCode("x") has no internal source and could drift), and
// no internal constant may be converted by two public names. The README must
// also document every discovered constant (see checkReadme).
//
// Scope: production (non-test) files of internal/observe and package loader.
// Unsupported declaration shapes are REPORTED with file:line, never skipped:
// an internal exported constant that is not `Name = "literal"`, one that fits
// no parity group, a duplicate value within a group, a public constant that
// references the observe import in any shape other than T(observe.Name), and
// a dot or blank import of observe.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const observeImportPath = "github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"

// parityGroup maps internal constants named <internalPrefix><X> to public
// twins named <publicPrefix><X> of type publicType. The Action group covers
// the lifecycle actions.
//
// tableRow selects how the README must document each constant: false requires
// both `<value>` and `<PublicName>` somewhere in the README; true requires a
// table row whose first two cells are `<value>` and `<PublicName>`.
type parityGroup struct {
	internalPrefix, publicPrefix, publicType string
	tableRow                                 bool
}

var parityGroups = []parityGroup{
	{internalPrefix: "Code", publicPrefix: "Notice", publicType: "NoticeCode", tableRow: true},
	{internalPrefix: "Kind", publicPrefix: "ResourceKind", publicType: "ResourceKind"},
	{internalPrefix: "Action", publicPrefix: "LifecycleAction", publicType: "LifecycleAction", tableRow: true},
}

// internalConst is one exported constant of internal/observe.
type internalConst struct {
	name, value, pos string
	group            int
}

// publicConst is one constant of package loader. typ is its declared or
// converted type, as far as a top-level declaration shows it (T("lit"),
// X T = "lit" or T(observe.ref)); ref is set only for T(observe.ref).
type publicConst struct {
	typ, ref, pos string
}

// sortedNames returns the keys of files in a stable order.
func sortedNames(files map[string][]byte) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// groupOf returns the parity group index of an internal constant name, or -1.
func groupOf(name string) int {
	for i, g := range parityGroups {
		if strings.HasPrefix(name, g.internalPrefix) && len(name) > len(g.internalPrefix) {
			return i
		}
	}
	return -1
}

// scanInternalConsts returns the exported constants of the given internal
// observe sources, with diagnostics for unsupported shapes.
func scanInternalConsts(files map[string][]byte) (consts []internalConst, diags []string, err error) {
	fset := token.NewFileSet()
	for _, name := range sortedNames(files) {
		file, parseErr := parser.ParseFile(fset, name, files[name], 0)
		if parseErr != nil {
			return nil, nil, fmt.Errorf("parse %s: %w", name, parseErr)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, ident := range vs.Names {
					if !ident.IsExported() {
						continue
					}
					pos := fset.Position(ident.Pos()).String()
					if vs.Type != nil || len(vs.Values) != len(vs.Names) {
						diags = append(diags, fmt.Sprintf("%s: unsupported constant shape for %s: want Name = \"literal\"", pos, ident.Name))
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						diags = append(diags, fmt.Sprintf("%s: unsupported constant shape for %s: want a string literal value", pos, ident.Name))
						continue
					}
					value, unquoteErr := strconv.Unquote(lit.Value)
					if unquoteErr != nil {
						return nil, nil, fmt.Errorf("%s: unquote %s: %w", pos, lit.Value, unquoteErr)
					}
					group := groupOf(ident.Name)
					if group < 0 {
						diags = append(diags, fmt.Sprintf("%s: exported constant %s matches no parity group", pos, ident.Name))
						continue
					}
					consts = append(consts, internalConst{name: ident.Name, value: value, pos: pos, group: group})
				}
			}
		}
	}
	return consts, diags, nil
}

// observeImportName returns the local name file uses for the observe import,
// "" when it does not import it, with a diagnostic for a dot or blank import.
func observeImportName(fset *token.FileSet, file *ast.File) (string, []string) {
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil || path != observeImportPath {
			continue
		}
		if spec.Name == nil {
			return "observe", nil
		}
		if spec.Name.Name == "." || spec.Name.Name == "_" {
			return "", []string{fmt.Sprintf("%s: unsupported %s import of internal/observe", fset.Position(spec.Pos()), spec.Name.Name)}
		}
		return spec.Name.Name, nil
	}
	return "", nil
}

// referencesImport reports whether expr mentions local.<anything>.
func referencesImport(expr ast.Expr, local string) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok {
			if x, ok := sel.X.(*ast.Ident); ok && x.Name == local {
				found = true
			}
		}
		return !found
	})
	return found
}

// declaredType returns the type name a top-level constant carries through its
// declared type (X T = ...) or a conversion value (T(...)), "" otherwise.
func declaredType(vs *ast.ValueSpec, value ast.Expr) string {
	if id, ok := vs.Type.(*ast.Ident); ok {
		return id.Name
	}
	if call, ok := value.(*ast.CallExpr); ok && len(call.Args) == 1 {
		if id, ok := call.Fun.(*ast.Ident); ok {
			return id.Name
		}
	}
	return ""
}

// scanPublicConsts returns every constant of the given package loader sources
// by name, recording typ and ref for the twin shape T(observe.ref).
func scanPublicConsts(files map[string][]byte) (consts map[string]publicConst, diags []string, err error) {
	consts = map[string]publicConst{}
	fset := token.NewFileSet()
	for _, name := range sortedNames(files) {
		file, parseErr := parser.ParseFile(fset, name, files[name], 0)
		if parseErr != nil {
			return nil, nil, fmt.Errorf("parse %s: %w", name, parseErr)
		}
		local, importDiags := observeImportName(fset, file)
		diags = append(diags, importDiags...)
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, ident := range vs.Names {
					pos := fset.Position(ident.Pos()).String()
					c := publicConst{pos: pos}
					var value ast.Expr
					if i < len(vs.Values) {
						value = vs.Values[i]
					}
					if local != "" && value != nil && referencesImport(value, local) {
						call, isCall := value.(*ast.CallExpr)
						var typ *ast.Ident
						var sel *ast.SelectorExpr
						if isCall && len(call.Args) == 1 {
							typ, _ = call.Fun.(*ast.Ident)
							sel, _ = call.Args[0].(*ast.SelectorExpr)
						}
						if vs.Type != nil || typ == nil || sel == nil {
							diags = append(diags, fmt.Sprintf("%s: unsupported constant shape for %s: want Name = T(%s.Name)", pos, ident.Name, local))
						} else if x, ok := sel.X.(*ast.Ident); ok && x.Name == local {
							c.typ, c.ref = typ.Name, sel.Sel.Name
						} else {
							diags = append(diags, fmt.Sprintf("%s: unsupported constant shape for %s: want Name = T(%s.Name)", pos, ident.Name, local))
						}
					}
					if c.typ == "" {
						c.typ = declaredType(vs, value)
					}
					consts[ident.Name] = c
				}
			}
		}
	}
	return consts, diags, nil
}

// checkParity reports every internal constant without a correct public twin,
// an empty group and a duplicate value within a group.
func checkParity(internals []internalConst, publics map[string]publicConst) []string {
	var problems []string
	counts := make([]int, len(parityGroups))
	values := make([]map[string]string, len(parityGroups))
	for i := range values {
		values[i] = map[string]string{}
	}
	for _, c := range internals {
		g := parityGroups[c.group]
		counts[c.group]++
		if first, dup := values[c.group][c.value]; dup {
			problems = append(problems, fmt.Sprintf("%s: %s duplicates the value %q of %s", c.pos, c.name, c.value, first))
		} else {
			values[c.group][c.value] = c.name
		}
		want := g.publicPrefix + strings.TrimPrefix(c.name, g.internalPrefix)
		twin, ok := publics[want]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("%s: internal constant %s (%q) has no public twin %s", c.pos, c.name, c.value, want))
		case twin.typ != g.publicType || twin.ref != c.name:
			problems = append(problems, fmt.Sprintf("%s: public %s is not %s(observe.%s)", twin.pos, want, g.publicType, c.name))
		}
	}
	problems = append(problems, checkPublicSources(internals, publics)...)
	for i, n := range counts {
		if n == 0 {
			problems = append(problems, fmt.Sprintf("parity group %s* has no internal constants", parityGroups[i].internalPrefix))
		}
	}
	return problems
}

// checkPublicSources is the public-to-internal direction: a constant of a
// group's public type must be the expected twin of an internal constant (so a
// public literal or a differently named conversion is reported), and no
// internal constant may be converted by two public names.
func checkPublicSources(internals []internalConst, publics map[string]publicConst) []string {
	var problems []string
	expected := map[string]bool{}
	for _, c := range internals {
		g := parityGroups[c.group]
		expected[g.publicPrefix+strings.TrimPrefix(c.name, g.internalPrefix)] = true
	}
	names := make([]string, 0, len(publics))
	for name := range publics {
		names = append(names, name)
	}
	sort.Strings(names)
	byRef := map[string][]string{}
	for _, name := range names {
		pc := publics[name]
		for _, g := range parityGroups {
			if pc.typ != g.publicType {
				continue
			}
			if !expected[name] {
				problems = append(problems, fmt.Sprintf("%s: public %s of type %s has no internal source: want %s(observe.%sX) under the name %sX", pc.pos, name, g.publicType, g.publicType, g.internalPrefix, g.publicPrefix))
			}
			if pc.ref != "" {
				byRef[pc.ref] = append(byRef[pc.ref], name)
			}
		}
	}
	refs := make([]string, 0, len(byRef))
	for ref := range byRef {
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	for _, ref := range refs {
		if users := byRef[ref]; len(users) > 1 {
			problems = append(problems, fmt.Sprintf("%s: internal constant %s is converted by more than one public name: %s", publics[users[0]].pos, ref, strings.Join(users, ", ")))
		}
	}
	return problems
}

// checkReadme requires the README to document every discovered internal
// constant per its group's tableRow mode. It reuses the scan output, so a new
// constant (or a Phase 5 action group) is covered with no third list.
func checkReadme(readme string, internals []internalConst) []string {
	var problems []string
	for _, c := range internals {
		g := parityGroups[c.group]
		public := g.publicPrefix + strings.TrimPrefix(c.name, g.internalPrefix)
		value, name := "`"+c.value+"`", "`"+public+"`"
		if !g.tableRow {
			if !strings.Contains(readme, value) || !strings.Contains(readme, name) {
				problems = append(problems, fmt.Sprintf("%s: README lacks %s and %s", c.pos, value, name))
			}
			continue
		}
		row := "| " + value + " | " + name + " |"
		if !strings.Contains(readme, "\n"+row) && !strings.HasPrefix(readme, row) {
			problems = append(problems, fmt.Sprintf("%s: README has no table row starting %s", c.pos, row))
		}
	}
	return problems
}

// readProductionSources reads every non-test .go file directly in dir.
func readProductionSources(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	files := map[string][]byte{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		files[filepath.ToSlash(path)] = src
	}
	return files
}

// runParity scans both source sets and returns every diagnostic and problem.
func runParity(t *testing.T, internal, public map[string][]byte) []string {
	t.Helper()
	internals, diags, err := scanInternalConsts(internal)
	if err != nil {
		t.Fatal(err)
	}
	publics, publicDiags, err := scanPublicConsts(public)
	if err != nil {
		t.Fatal(err)
	}
	return append(append(diags, publicDiags...), checkParity(internals, publics)...)
}

// TestObserveConstantsHavePublicTwins is the guard over the real tree.
func TestObserveConstantsHavePublicTwins(t *testing.T) {
	t.Parallel()

	internal := readProductionSources(t, filepath.Join("internal", "observe"))
	if len(internal) == 0 {
		t.Fatal("no internal/observe production sources found")
	}
	public := readProductionSources(t, ".")
	if problems := runParity(t, internal, public); len(problems) != 0 {
		t.Fatalf("constant parity problems:\n%s", strings.Join(problems, "\n"))
	}
	// The real tree must actually exercise both groups and the public types.
	internals, _, err := scanInternalConsts(internal)
	if err != nil {
		t.Fatal(err)
	}
	perGroup := make([]int, len(parityGroups))
	for _, c := range internals {
		perGroup[c.group]++
	}
	for i, n := range perGroup {
		if n == 0 {
			t.Fatalf("group %s* is empty in the real tree", parityGroups[i].internalPrefix)
		}
	}
}

// TestREADMEDocumentsEveryObserveConstant guards the README tables against the
// discovered constants, so a new code, kind or renamed value cannot stay
// undocumented or leave a stale row.
func TestREADMEDocumentsEveryObserveConstant(t *testing.T) {
	t.Parallel()

	internals, diags, err := scanInternalConsts(readProductionSources(t, filepath.Join("internal", "observe")))
	if err != nil {
		t.Fatal(err)
	}
	if len(diags) != 0 {
		t.Fatalf("unsupported internal constants:\n%s", strings.Join(diags, "\n"))
	}
	readme, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if problems := checkReadme(string(readme), internals); len(problems) != 0 {
		t.Fatalf("README does not document the observe constants:\n%s", strings.Join(problems, "\n"))
	}
}

// TestCheckREADMEReportsMissingRows proves checkReadme can fail: a missing
// value, a missing public name, a code that is only mentioned in prose rather
// than a table row, and a kind that is absent are each reported.
func TestCheckREADMEReportsMissingRows(t *testing.T) {
	t.Parallel()

	internals := []internalConst{
		{name: "CodeKept", value: "kept", pos: "a:1", group: 0},
		{name: "KindIndex", value: "index", pos: "a:2", group: 1},
		{name: "ActionMade", value: "made", pos: "a:3", group: 2},
	}
	const actionRow = "\n| `made` | `LifecycleActionMade` | none |"
	tests := []struct {
		name, readme string
		want         string
	}{
		{"complete", "x\n| `kept` | `NoticeKept` | none |\n`index` (`ResourceKindIndex`)" + actionRow, ""},
		{"complete at start", "| `kept` | `NoticeKept` | none |\n`index` (`ResourceKindIndex`)" + actionRow, ""},
		{"missing code row", "`index` (`ResourceKindIndex`)" + actionRow, "a:1: README has no table row starting | `kept` | `NoticeKept` |"},
		{"code only in prose", "see `kept` and `NoticeKept`\n`index` (`ResourceKindIndex`)" + actionRow, "a:1: README has no table row"},
		{"swapped cells", "| `NoticeKept` | `kept` |\n`index` (`ResourceKindIndex`)" + actionRow, "a:1: README has no table row"},
		{"missing kind name", "| `kept` | `NoticeKept` |\n`index`" + actionRow, "a:2: README lacks `index` and `ResourceKindIndex`"},
		{"missing kind value", "| `kept` | `NoticeKept` |\n`ResourceKindIndex`" + actionRow, "a:2: README lacks"},
		{"missing action row", "| `kept` | `NoticeKept` |\n`index` (`ResourceKindIndex`)", "a:3: README has no table row starting | `made` | `LifecycleActionMade` |"},
		{"action only in prose", "| `kept` | `NoticeKept` |\n`index` (`ResourceKindIndex`)\nsee `made` and `LifecycleActionMade`", "a:3: README has no table row"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := strings.Join(checkReadme(tt.readme, internals), "\n")
			if tt.want == "" && got != "" {
				t.Fatalf("unexpected problems:\n%s", got)
			}
			if tt.want != "" && !strings.Contains(got, tt.want) {
				t.Fatalf("problems lack %q; got:\n%s", tt.want, got)
			}
		})
	}
}

// TestParityScannerReportsMissingAndWrongTwins proves the guard can fail: a
// missing twin, a twin of the wrong public type, a same-name constant with its
// own literal, unsupported shapes on both sides and an empty group are each
// reported with file:line.
func TestParityScannerReportsMissingAndWrongTwins(t *testing.T) {
	t.Parallel()

	internal := map[string][]byte{"internal/observe/observe.go": []byte(`package observe

const (
	CodeKept    = "kept"
	CodeMissing = "missing"
	CodeLiteral = "literal"
	CodeTyped string = "typed"
	CodeIota = iota
	CodeDup = "kept"
	Stray = "stray"
	unexported = "ignored"
	CodeSwapped = "swapped"
	ActionKept = "kept"
	ActionMissing = "missing"
	ActionWrong = "wrong"
)
`)}
	public := map[string][]byte{"observe.go": []byte(`package loader

import "github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"

const (
	NoticeKept    = NoticeCode(observe.CodeKept)
	NoticeLiteral = NoticeCode("literal")
	NoticeDup     = ResourceKind(observe.CodeDup)
	NoticeOdd     = NoticeCode(observe.CodeKept + "x")
	// Right public type, wrong internal reference: only the ref clause of
	// checkParity can catch it.
	NoticeSwapped = NoticeCode(observe.CodeKept)
	// Public-to-internal direction: literal values with no internal source,
	// in both declaration shapes, and a second name for one internal constant.
	NoticeProbe             = NoticeCode("probe")
	ResourceKindBar ResourceKind = "bar"
	NoticeAgain             = NoticeCode(observe.CodeKept)
	// The Action group: a correct twin is silent, a missing one and one of
	// the wrong public type are reported.
	LifecycleActionKept  = LifecycleAction(observe.ActionKept)
	LifecycleActionWrong = NoticeCode(observe.ActionWrong)
)

// Unrelated public types and non-constant expressions are never classified.
const StatusX EnrichStatus = "x"

var _ = NoticeCode("not-a-const")

func f(n int) NoticeCode { return NoticeCode("inside") }
`)}

	got := strings.Join(runParity(t, internal, public), "\n")
	for _, want := range []string{
		`internal/observe/observe.go:5:2: internal constant CodeMissing ("missing") has no public twin NoticeMissing`,
		`observe.go:7:2: public NoticeLiteral is not NoticeCode(observe.CodeLiteral)`,
		`observe.go:8:2: public NoticeDup is not NoticeCode(observe.CodeDup)`,
		`internal/observe/observe.go:9:2: CodeDup duplicates the value "kept" of CodeKept`,
		`internal/observe/observe.go:7:2: unsupported constant shape for CodeTyped`,
		`internal/observe/observe.go:8:2: unsupported constant shape for CodeIota`,
		`internal/observe/observe.go:10:2: exported constant Stray matches no parity group`,
		`observe.go:9:2: unsupported constant shape for NoticeOdd`,
		`parity group Kind* has no internal constants`,
		`internal constant ActionMissing ("missing") has no public twin LifecycleActionMissing`,
		`public LifecycleActionWrong is not LifecycleAction(observe.ActionWrong)`,
		`observe.go:12:2: public NoticeSwapped is not NoticeCode(observe.CodeSwapped)`,
		`public NoticeProbe of type NoticeCode has no internal source`,
		`public ResourceKindBar of type ResourceKind has no internal source`,
		`public NoticeAgain of type NoticeCode has no internal source`,
		`internal constant CodeKept is converted by more than one public name: NoticeAgain, NoticeKept, NoticeSwapped`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("diagnostics lack %q; got:\n%s", want, got)
		}
	}
	if strings.Contains(got, "ActionKept") || strings.Contains(got, "LifecycleActionKept") || strings.Contains(got, "CodeKept (") || strings.Contains(got, "unexported") {
		t.Errorf("a correct twin or an unexported constant was reported:\n%s", got)
	}

	// The unrelated type and the non-constant expressions are never classified.
	for _, unwanted := range []string{"StatusX", "inside", "not-a-const"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("%q was classified; got:\n%s", unwanted, got)
		}
	}

	// An aliased import is resolved: a correct twin through the alias is
	// silent, a wrong one is reported.
	aliasInternal := map[string][]byte{"internal/observe/observe.go": []byte(`package observe

const (
	CodeKept    = "kept"
	CodeSwapped = "swapped"
	KindIndex   = "index"
)
`)}
	aliased := map[string][]byte{"alias.go": []byte(`package loader

import obs "github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"

const (
	NoticeKept        = NoticeCode(obs.CodeKept)
	NoticeSwapped     = NoticeCode(obs.CodeKept)
	ResourceKindIndex = ResourceKind(obs.KindIndex)
)
`)}
	aliasGot := runParity(t, aliasInternal, aliased)
	joined := strings.Join(aliasGot, "\n")
	if !strings.Contains(joined, "alias.go:7:2: public NoticeSwapped is not NoticeCode(observe.CodeSwapped)") {
		t.Errorf("wrong twin through an aliased import not reported; got:\n%s", joined)
	}
	for _, line := range aliasGot {
		if strings.Contains(line, "public NoticeKept") || strings.Contains(line, "ResourceKindIndex") {
			t.Errorf("correct twin through an aliased import was reported: %s", line)
		}
	}

	// A dot import is reported rather than silently scanning nothing.
	dot := map[string][]byte{"dot.go": []byte(`package loader

import . "github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"

const NoticeKept = NoticeCode(CodeKept)
`)}
	if got := strings.Join(runParity(t, internal, dot), "\n"); !strings.Contains(got, "dot.go:3:8: unsupported . import of internal/observe") {
		t.Errorf("dot import not reported; got:\n%s", got)
	}
}

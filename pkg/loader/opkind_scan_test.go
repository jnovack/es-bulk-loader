package loader

// Compatibility guard for the public RunError.Kind. classifyRunErrorKind picks
// a Kind by case-insensitive substring of the failure Op, so rewording an Op
// (or adding one) can silently change how library consumers who branch on Kind
// see it. A hand-written list of Ops cannot discover a new Op, so this test
// parses the production tree and requires every Op to have an explicit
// expected-Kind row. It pins the CURRENT classifier; it does not endorse every
// current classification.
//
// Scope: direct calls to runlog.Check, runlog.Fail (Op is argument 1) and
// failure.New (Op is argument 0) under this package directory. Explicit-Kind
// RunError construction, indirect calls and packages outside pkg/loader are
// out of scope.

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
)

const (
	opKindPkgRunlog  = "github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
	opKindPkgFailure = "github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	opKindPkgFmt     = "fmt"
	// opKindForwardFile is the only file whose failure.New(op, ...) calls are
	// forwarding wrappers rather than Op sites.
	opKindForwardFile = "internal/runlog/runlog.go"
)

type opKindShape int

const (
	opShapeLiteral opKindShape = iota
	opShapeFormat
)

func (s opKindShape) String() string {
	if s == opShapeFormat {
		return "format"
	}
	return "literal"
}

// opKindSite is one production Op call site.
type opKindSite struct {
	Pos   string // file:line relative to the scan root
	Key   string // unquoted literal, or unquoted Sprintf format
	Shape opKindShape
}

// opKindRow is the expected Kind of one unique Op literal or format.
type opKindRow struct {
	Key    string
	Shape  opKindShape
	Kind   error
	Values []any // formatted rows only: caller-derived substitution values
	Note   string
}

type opKindCallee struct{ pkg, fn string }

// opKindTargets maps each recognized call to its argument count and Op index.
var opKindTargets = map[opKindCallee]struct{ argc, opIdx int }{
	{opKindPkgRunlog, "Check"}: {3, 1},
	{opKindPkgRunlog, "Fail"}:  {3, 1},
	{opKindPkgFailure, "New"}:  {2, 0},
}

// scanOpSites parses every non-test .go file under root (ignoring build
// constraints and testdata directories) and returns the Op sites in path and
// source order. diags holds unsupported shapes with file:line; err is a walk,
// read or parse failure.
func scanOpSites(root string) (sites []opKindSite, diags []string, err error) {
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if d.Name() == "testdata" && path != root {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return fmt.Errorf("relative path of %s: %w", path, relErr)
		}
		rel = filepath.ToSlash(rel)
		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("read %s: %w", rel, readErr)
		}
		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, rel, src, 0)
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", rel, parseErr)
		}
		fileSites, fileDiags := scanOpFile(fset, file, rel)
		sites = append(sites, fileSites...)
		diags = append(diags, fileDiags...)
		return nil
	})
	return sites, diags, err
}

func scanOpFile(fset *token.FileSet, file *ast.File, rel string) (sites []opKindSite, diags []string) {
	posOf := func(p token.Pos) string { return fmt.Sprintf("%s:%d", rel, fset.Position(p).Line) }

	// Resolve imports by path so an aliased import cannot hide a call.
	imports := map[string]string{}
	for _, imp := range file.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			diags = append(diags, fmt.Sprintf("%s: unquote import path %s: %v", posOf(imp.Pos()), imp.Path.Value, err))
			continue
		}
		if p != opKindPkgFmt && p != opKindPkgRunlog && p != opKindPkgFailure {
			continue
		}
		name := p[strings.LastIndex(p, "/")+1:]
		if imp.Name != nil {
			name = imp.Name.Name
		}
		switch name {
		case ".":
			diags = append(diags, fmt.Sprintf("%s: dot import of %q is unsupported: unqualified calls cannot be scanned", posOf(imp.Pos()), p))
		case "_":
		default:
			imports[name] = p
		}
	}

	forwards := map[string]int{}
	for _, decl := range file.Decls {
		funcName := ""
		if fd, ok := decl.(*ast.FuncDecl); ok && fd.Recv == nil {
			funcName = fd.Name.Name
		}
		ast.Inspect(decl, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			callee, ok := opKindResolve(call, imports)
			if !ok {
				return true
			}
			target, ok := opKindTargets[callee]
			if !ok {
				return true
			}
			pos := posOf(call.Pos())
			if len(call.Args) != target.argc || call.Ellipsis.IsValid() {
				diags = append(diags, fmt.Sprintf("%s: %s.%s call has %d arguments (ellipsis=%t), want exactly %d", pos, callee.pkg[strings.LastIndex(callee.pkg, "/")+1:], callee.fn, len(call.Args), call.Ellipsis.IsValid(), target.argc))
				return true
			}
			opArg := call.Args[target.opIdx]
			if rel == opKindForwardFile && callee.pkg == opKindPkgFailure && (funcName == "Fail" || funcName == "Check") {
				if id, isIdent := opArg.(*ast.Ident); isIdent && id.Name == "op" {
					forwards[funcName]++
					return true
				}
			}
			key, shape, problem := opKindExtract(opArg, imports)
			if problem != "" {
				diags = append(diags, fmt.Sprintf("%s: unsupported Op expression: %s", pos, problem))
				return true
			}
			sites = append(sites, opKindSite{Pos: pos, Key: key, Shape: shape})
			return true
		})
	}
	if rel == opKindForwardFile {
		for _, fn := range []string{"Fail", "Check"} {
			if forwards[fn] != 1 {
				diags = append(diags, fmt.Sprintf("%s: expected exactly one failure.New(op, ...) forward in %s, found %d", rel, fn, forwards[fn]))
			}
		}
	}
	return sites, diags
}

// opKindResolve returns the imported package path and function of a
// package-qualified call. A local variable shadowing the package name has a
// non-nil Obj and is not a call into the package.
func opKindResolve(call *ast.CallExpr, imports map[string]string) (opKindCallee, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return opKindCallee{}, false
	}
	id, ok := sel.X.(*ast.Ident)
	if !ok || id.Obj != nil {
		return opKindCallee{}, false
	}
	p, ok := imports[id.Name]
	if !ok {
		return opKindCallee{}, false
	}
	return opKindCallee{pkg: p, fn: sel.Sel.Name}, true
}

// opKindExtract accepts a string literal or fmt.Sprintf(literal, oneArg). It
// does not evaluate constants or other expressions.
func opKindExtract(expr ast.Expr, imports map[string]string) (key string, shape opKindShape, problem string) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return "", 0, fmt.Sprintf("non-string literal %s", e.Value)
		}
		s, err := strconv.Unquote(e.Value)
		if err != nil {
			return "", 0, fmt.Sprintf("unquote %s: %v", e.Value, err)
		}
		return s, opShapeLiteral, ""
	case *ast.CallExpr:
		callee, ok := opKindResolve(e, imports)
		if !ok || callee.pkg != opKindPkgFmt || callee.fn != "Sprintf" {
			return "", 0, fmt.Sprintf("call %T is not fmt.Sprintf", e.Fun)
		}
		if len(e.Args) != 2 || e.Ellipsis.IsValid() {
			return "", 0, fmt.Sprintf("fmt.Sprintf has %d arguments (ellipsis=%t), want a literal format and exactly one substitution", len(e.Args), e.Ellipsis.IsValid())
		}
		lit, ok := e.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return "", 0, "fmt.Sprintf format is not a string literal"
		}
		s, err := strconv.Unquote(lit.Value)
		if err != nil {
			return "", 0, fmt.Sprintf("unquote format %s: %v", lit.Value, err)
		}
		return s, opShapeFormat, ""
	default:
		return "", 0, fmt.Sprintf("expression %T is not a string literal or fmt.Sprintf", expr)
	}
}

// checkOpKinds returns every table, parity and classification problem, so a
// rewording reports its missing and stale rows together.
func checkOpKinds(sites []opKindSite, rows []opKindRow) []string {
	var diags []string

	// Row structure.
	rowByKey := map[string]opKindRow{}
	valid := map[string]bool{}
	for _, row := range rows {
		ok := true
		if _, dup := rowByKey[row.Key]; dup {
			diags = append(diags, fmt.Sprintf("duplicate row for key %q", row.Key))
			continue
		}
		rowByKey[row.Key] = row
		if row.Kind == nil {
			diags = append(diags, fmt.Sprintf("row %q has a nil expected Kind", row.Key))
			ok = false
		}
		switch row.Shape {
		case opShapeLiteral:
			if len(row.Values) != 0 {
				diags = append(diags, fmt.Sprintf("literal row %q must not carry substitution values", row.Key))
				ok = false
			}
		case opShapeFormat:
			if len(row.Values) == 0 {
				diags = append(diags, fmt.Sprintf("format row %q has no substitution values", row.Key))
				ok = false
			}
		}
		if row.Kind == ErrLoaderExecution && strings.TrimSpace(row.Note) == "" {
			diags = append(diags, fmt.Sprintf("row %q pins the ErrLoaderExecution fallback and needs a note saying why", row.Key))
		}
		valid[row.Key] = ok
	}

	// Sites grouped by key.
	sitesByKey := map[string][]opKindSite{}
	var keys []string
	for _, s := range sites {
		if _, seen := sitesByKey[s.Key]; !seen {
			keys = append(keys, s.Key)
		}
		sitesByKey[s.Key] = append(sitesByKey[s.Key], s)
	}
	sort.Strings(keys)
	positions := func(ss []opKindSite) string {
		ps := make([]string, len(ss))
		for i, s := range ss {
			ps[i] = s.Pos
		}
		return strings.Join(ps, ", ")
	}

	for _, key := range keys {
		ss := sitesByKey[key]
		for _, s := range ss[1:] {
			if s.Shape != ss[0].Shape {
				diags = append(diags, fmt.Sprintf("key %q is both a literal and a format at %s", key, positions(ss)))
				valid[key] = false
				break
			}
		}
		row, ok := rowByKey[key]
		if !ok {
			diags = append(diags, fmt.Sprintf("missing row for %s Op %q at %s", ss[0].Shape, key, positions(ss)))
			continue
		}
		if row.Shape != ss[0].Shape {
			diags = append(diags, fmt.Sprintf("row %q is a %s but production uses a %s at %s", key, row.Shape, ss[0].Shape, positions(ss)))
			valid[key] = false
		}
	}
	var staleKeys []string
	for key := range rowByKey {
		if _, ok := sitesByKey[key]; !ok {
			staleKeys = append(staleKeys, key)
		}
	}
	sort.Strings(staleKeys)
	for _, key := range staleKeys {
		diags = append(diags, fmt.Sprintf("stale row %q: no production position uses it", key))
	}

	// Classification through the public error boundary.
	for _, key := range keys {
		row, ok := rowByKey[key]
		if !ok || !valid[key] || row.Kind == nil {
			continue
		}
		ops := []string{key}
		if row.Shape == opShapeFormat {
			ops = ops[:0]
			for _, v := range row.Values {
				ops = append(ops, fmt.Sprintf(key, v))
			}
		}
		for _, op := range ops {
			err := runFailure(failure.New(op, errors.New("cause")))
			var runErr *RunError
			if !errors.As(err, &runErr) {
				diags = append(diags, fmt.Sprintf("key %q expansion %q: runFailure returned %T, want *RunError (sites %s)", key, op, err, positions(sitesByKey[key])))
				continue
			}
			if runErr.Kind != row.Kind {
				diags = append(diags, fmt.Sprintf("Kind mismatch for key %q expansion %q at %s: got %v, want %v", key, op, positions(sitesByKey[key]), runErr.Kind, row.Kind))
			}
		}
	}
	return diags
}

func opKindLit(key string, kind error, note string) opKindRow {
	return opKindRow{Key: key, Shape: opShapeLiteral, Kind: kind, Note: note}
}

func opKindFmtRow(key string, kind error, note string, values ...any) opKindRow {
	return opKindRow{Key: key, Shape: opShapeFormat, Kind: kind, Values: values, Note: note}
}

// expectedOpKinds is the explicit table. Kinds are hardcoded sentinels, never
// computed by the classifier.
//
// Maintenance limit: the Values of a formatted row are the substitutions the
// production callers pass today (see each row). The scanner does not follow
// data flow, so a new caller value for an unchanged format needs a manual row
// review. A format with several arguments or values needing different Kinds is
// unsupported and needs a scanner/table design update, not a guessed expansion.
func expectedOpKinds() []opKindRow {
	return []opKindRow{
		opKindLit("finding available timestamped index name", ErrIndexOperation, ""),
		opKindLit("resolving alias targets", ErrIndexOperation, ""),
		opKindLit("Failed to resolve alias targets", ErrIndexOperation, ""),
		opKindLit("Unable to parse alias response", ErrIndexOperation, ""),
		opKindLit("serializing alias actions", ErrIndexOperation, ""),
		opKindLit("updating alias", ErrIndexOperation, ""),
		opKindLit("Failed to update alias", ErrIndexOperation, ""),
		opKindLit("listing timestamped indices", ErrIndexOperation, ""),
		opKindLit("Failed to list timestamped indices", ErrIndexOperation, ""),
		opKindLit("Unable to parse timestamped index response", ErrIndexOperation, ""),
		opKindLit("opening data file", ErrLoaderExecution, "data-file reading/decoding has no classifier keyword; generic loader failure by design"),
		opKindLit("Data file must be a JSON array", ErrLoaderExecution, "data-file format error names no classifier keyword; generic loader failure by design"),
		opKindLit("Error counting objects in data file", ErrLoaderExecution, "data-file reading/decoding has no classifier keyword; generic loader failure by design"),
		opKindLit("Error rewinding data file", ErrLoaderExecution, "data-file reading/decoding has no classifier keyword; generic loader failure by design"),
		opKindLit("Error re-reading data file", ErrLoaderExecution, "data-file reading/decoding has no classifier keyword; generic loader failure by design"),
		opKindLit("Error decoding object in data file", ErrLoaderExecution, "data-file reading/decoding has no classifier keyword; generic loader failure by design"),
		opKindLit("Error counting NDJSON lines", ErrLoaderExecution, "data-file reading/decoding has no classifier keyword; generic loader failure by design"),
		opKindLit("Error opening NDJSON data file", ErrLoaderExecution, "data-file reading/decoding has no classifier keyword; generic loader failure by design"),
		opKindLit("Error decoding NDJSON document", ErrLoaderExecution, "data-file reading/decoding has no classifier keyword; generic loader failure by design"),
		opKindLit("Unable to serialize bulk action metadata", ErrBulkFailure, ""),
		opKindLit("Unable to serialize document for bulk insert", ErrBulkFailure, ""),
		opKindLit("Bulk API request failed", ErrBulkFailure, ""),
		opKindLit("Unable to parse bulk response body", ErrBulkFailure, ""),
		opKindLit("Cannot read data file", ErrLoaderExecution, "data-file reading/decoding has no classifier keyword; generic loader failure by design"),
		opKindLit("Failed to build mapping preflight plan", ErrLoaderExecution, "mapping preflight plan construction names no index noun; pinned as the fallback (suspected ErrIndexOperation, see spec follow-up)"),
		opKindLit("Reading settings file", ErrLoaderExecution, "settings file handling names no index/managed noun; pinned as the fallback (suspected ErrIndexOperation, see spec follow-up)"),
		opKindLit("Parsing settings file", ErrLoaderExecution, "settings file handling names no index/managed noun; pinned as the fallback (suspected ErrIndexOperation, see spec follow-up)"),
		opKindLit("Parsing nested settings file", ErrLoaderExecution, "settings file handling names no index/managed noun; pinned as the fallback (suspected ErrIndexOperation, see spec follow-up)"),
		opKindLit("Serializing settings", ErrLoaderExecution, "settings file handling names no index/managed noun; pinned as the fallback (suspected ErrIndexOperation, see spec follow-up)"),
		opKindLit("getting enrich policies", ErrEnrichExecution, ""),
		opKindLit("Failed to get enrich policies", ErrEnrichExecution, ""),
		opKindLit("Unable to parse enrich policy response", ErrEnrichExecution, ""),
		opKindLit("waiting for index creation", ErrIndexOperation, ""),
		opKindLit("Context cancelled while waiting for index to become visible", ErrIndexOperation, ""),
		opKindLit("Index create was acknowledged but the index did not become visible", ErrIndexOperation, ""),
		opKindLit("deleting index", ErrIndexOperation, ""),
		opKindLit("Failed to delete index", ErrIndexOperation, ""),
		opKindLit("flushing index", ErrIndexOperation, ""),
		opKindLit("Unable to read index flush response body", ErrIndexOperation, ""),
		opKindLit("Failed to flush index", ErrIndexOperation, ""),
		opKindLit("Unable to parse index flush response body", ErrIndexOperation, ""),
		opKindLit("Index flush completed with shard-level failures", ErrIndexOperation, ""),
		opKindLit("getting index settings", ErrIndexOperation, ""),
		opKindLit("Failed to get index settings", ErrIndexOperation, ""),
		opKindLit("Unable to parse index settings response", ErrIndexOperation, ""),
		opKindLit("clearing index.default_pipeline", ErrManagedResource, ""),
		opKindLit("Failed to clear index.default_pipeline", ErrManagedResource, ""),
		opKindLit("refreshing index", ErrIndexOperation, ""),
		opKindLit("Failed to refresh index", ErrIndexOperation, ""),
		opKindLit("creating index", ErrIndexOperation, ""),
		opKindLit("Failed to create index", ErrIndexOperation, ""),
		opKindLit("Policy definition missing from parsed policy file", ErrManagedResource, ""),
		opKindLit("Failed to canonicalize policy definition for hashing", ErrManagedResource, ""),
		opKindLit("Unable to parse pipeline definition for policy rewrite", ErrManagedResource, ""),
		opKindLit("Unable to serialize rewritten pipeline definition", ErrManagedResource, ""),
		opKindLit("Ambiguous policy reference", ErrManagedResource, ""),
		opKindLit("Unable to parse pipeline definition while collecting policy references", ErrManagedResource, ""),
		opKindLit("creating pipeline", ErrManagedResource, ""),
		opKindLit("Failed to create pipeline", ErrManagedResource, ""),
		opKindLit("deleting pipeline", ErrManagedResource, ""),
		opKindLit("Failed to delete pipeline", ErrManagedResource, ""),
		opKindLit("creating enrich policy", ErrEnrichExecution, ""),
		opKindLit("Context cancelled during enrich policy retry", ErrEnrichExecution, ""),
		opKindLit("Failed to create enrich policy", ErrEnrichExecution, ""),
		opKindLit("deleting enrich policy", ErrEnrichExecution, ""),
		opKindLit("Failed to delete enrich policy; nuke mode could not find referencing pipelines", ErrEnrichExecution, ""),
		opKindLit("Failed to delete enrich policy", ErrEnrichExecution, ""),
		opKindLit("Failed to delete pipeline; nuke mode could not find indices using it as default pipeline", ErrManagedResource, ""),
		opKindLit("getting ingest pipelines", ErrManagedResource, ""),
		opKindLit("Failed to get ingest pipelines", ErrManagedResource, ""),
		opKindLit("Unable to parse ingest pipeline response", ErrManagedResource, ""),
		opKindLit("Transform definition missing from parsed transform file", ErrManagedResource, ""),
		opKindLit("creating transform", ErrManagedResource, ""),
		opKindLit("Failed to create transform", ErrManagedResource, ""),
		opKindLit("checking if transform exists", ErrManagedResource, ""),
		opKindLit("Failed to check transform existence", ErrManagedResource, ""),
		opKindLit("stopping transform", ErrManagedResource, ""),
		opKindLit("Failed to stop transform", ErrManagedResource, ""),
		opKindLit("deleting transform", ErrManagedResource, ""),
		opKindLit("Failed to delete transform", ErrManagedResource, ""),
		opKindLit("starting transform", ErrManagedResource, ""),
		opKindLit("Failed to start transform", ErrManagedResource, ""),
		opKindLit("Transform wait failed", ErrManagedResource, ""),
		opKindLit("creating Elasticsearch client", ErrLoaderExecution, "client construction is not tied to a resource; generic loader failure by design"),
		opKindLit("Failed to resolve transform definitions", ErrManagedResource, ""),
		opKindLit("checking if index exists", ErrIndexOperation, ""),
		opKindLit("Mapping preflight failed before bulk insert", ErrBulkFailure, ""),
		// Formatted rows. Values come from the production callers:
		// NormalizeIndexSection is only called with "mappings" (config.go) and
		// ReadNamedDefinitions only with "pipeline", "policy" or "transform"
		// (loader.go). The two section formats carry no classifier keyword, so
		// they fall back by design; the definition formats name the resource
		// type, which the classifier maps to ErrManagedResource.
		opKindFmtRow("Reading %s file", ErrLoaderExecution, "section is only \"mappings\"; names no index noun, pinned as the fallback (suspected ErrIndexOperation, see spec follow-up)", "mappings"),
		opKindFmtRow("Parsing %s file", ErrLoaderExecution, "section is only \"mappings\"; names no index noun, pinned as the fallback (suspected ErrIndexOperation, see spec follow-up)", "mappings"),
		opKindFmtRow("Reading %s definitions file", ErrManagedResource, "", "pipeline", "policy", "transform"),
		opKindFmtRow("Parsing %s definitions file", ErrManagedResource, "", "pipeline", "policy", "transform"),
		opKindFmtRow("%s definitions file must contain a JSON object", ErrManagedResource, "", "pipeline", "policy", "transform"),
		opKindFmtRow("Reading %s definition name", ErrManagedResource, "", "pipeline", "policy", "transform"),
		opKindFmtRow("Invalid %s definition name", ErrManagedResource, "", "pipeline", "policy", "transform"),
		opKindFmtRow("Parsing %s definition body", ErrManagedResource, "", "pipeline", "policy", "transform"),
		// The substitution is one byte (256 values); byte('x') is representative
		// because quoting a single byte cannot create a classifier keyword and
		// the fixed text contains none.
		opKindFmtRow("Data file first byte %q is neither '[' (JSON array) nor '{' (NDJSON)", ErrLoaderExecution, "data-file format error names no classifier keyword; generic loader failure by design", byte('x')),
	}
}

func TestEveryProductionOpHasExpectedKind(t *testing.T) {
	t.Parallel()
	sites, diags, err := scanOpSites(".")
	if err != nil {
		t.Fatalf("scan production tree: %v", err)
	}
	if len(sites) == 0 {
		t.Fatal("scan found no production Op sites")
	}
	diags = append(diags, checkOpKinds(sites, expectedOpKinds())...)
	for _, d := range diags {
		t.Error(d)
	}
}

// ─── Scanner and table regression tests ────────────────────────────────────────

const opKindFixtureHeader = "package p\n\nimport (\n\t\"fmt\"\n\n\t\"" + opKindPkgRunlog + "\"\n\t\"" + opKindPkgFailure + "\"\n)\n\n"

// writeOpKindTree writes files (slash-separated relative path to content) under
// a fresh temp dir. Fixtures are parsed, never compiled.
func writeOpKindTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, content := range files {
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestOpKindScannerDiscovery(t *testing.T) {
	t.Parallel()
	aliased := "package p\n\nimport (\n\tf \"fmt\"\n\trl \"" + opKindPkgRunlog + "\"\n\tfl \"" + opKindPkgFailure + "\"\n)\n\n" +
		"func a() error { return rl.Check(nil, \"aliased op\", nil) }\n" +
		"func b() error { return fl.New(f.Sprintf(\"fmt %s\", \"x\"), nil) }\n"
	root := writeOpKindTree(t, map[string]string{
		"top.go": opKindFixtureHeader +
			"func a() error { return runlog.Check(nil, \"first\", nil) }\n" +
			"func b() error { return failure.New(\"second\", nil) }\n" +
			"func c() error { return runlog.Fail(nil, `raw \\n op`, nil) }\n" +
			"func d() error { return runlog.Check(nil, \"esc\\tcaped \\\"q\\\"\", nil) }\n" +
			"func e() error { return runlog.Check(nil, fmt.Sprintf(\"Reading %s file\", \"mappings\"), nil) }\n",
		"nested/deep/x.go":           aliased,
		"nested/crlf.go":             strings.ReplaceAll(opKindFixtureHeader+"func a() error {\n\treturn runlog.Check(nil, \"crlf op\", nil)\n}\n", "\n", "\r\n"),
		"nested/skipped_test.go":     opKindFixtureHeader + "func a() error { return runlog.Check(nil, \"from test\", nil) }\n",
		"nested/testdata/ignored.go": opKindFixtureHeader + "func a() error { return runlog.Check(nil, \"from testdata\", nil) }\n",
		"nested/tagged.go":           "//go:build ignore_everywhere\n\n" + opKindFixtureHeader + "func a() error { return runlog.Check(nil, \"tagged op\", nil) }\n",
		"nested/unrelated.go":        "package p\n\ntype t struct{}\n\nfunc (t) Check(a, b string) {}\n\nfunc a(x t) { x.Check(\"a\", \"not an op\") }\n",
		"nested/shadow.go":           opKindFixtureHeader + "func a() error { runlog := local{}; return runlog.Check(nil, \"shadowed\", nil) }\n",
		"nested/readme.txt":          "runlog.Check(nil, \"not go\", nil)",
		"nested/other/unimported.go": "package p\n\nfunc a() error { return runlog.Check(nil, \"unimported\", nil) }\n",
	})
	sites, diags, err := scanOpSites(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(diags) != 0 {
		t.Fatalf("unexpected diagnostics: %v", diags)
	}
	got := map[string]opKindShape{}
	for _, s := range sites {
		got[s.Key] = s.Shape
	}
	want := map[string]opKindShape{
		"first":            opShapeLiteral,
		"second":           opShapeLiteral,
		"raw \\n op":       opShapeLiteral,
		"esc\tcaped \"q\"": opShapeLiteral,
		"Reading %s file":  opShapeFormat,
		"aliased op":       opShapeLiteral,
		"fmt %s":           opShapeFormat,
		"crlf op":          opShapeLiteral,
		"tagged op":        opShapeLiteral,
	}
	if len(got) != len(want) {
		t.Fatalf("sites = %v, want %v", got, want)
	}
	for k, shape := range want {
		if g, ok := got[k]; !ok || g != shape {
			t.Errorf("key %q: got %v (present=%t), want %v", k, g, ok, shape)
		}
	}
	for _, s := range sites {
		if filepath.IsAbs(s.Pos) || !strings.Contains(s.Pos, ":") {
			t.Errorf("position %q is not relative file:line", s.Pos)
		}
	}
}

func TestOpKindScannerRepeatedSitesKeepEveryPosition(t *testing.T) {
	t.Parallel()
	root := writeOpKindTree(t, map[string]string{
		"a.go": opKindFixtureHeader + "func a() error { return runlog.Check(nil, \"same\", nil) }\n",
		"b.go": opKindFixtureHeader + "func a() error { return failure.New(\"same\", nil) }\n",
	})
	sites, diags, err := scanOpSites(root)
	if err != nil || len(diags) != 0 {
		t.Fatalf("err=%v diags=%v", err, diags)
	}
	if len(sites) != 2 || sites[0].Pos != "a.go:"+strings.Split(sites[0].Pos, ":")[1] || sites[1].Pos[:4] != "b.go" {
		t.Fatalf("sites = %+v, want one per file in path order", sites)
	}
}

func TestOpKindScannerAliasedCallWithKnownKeyIsStillFound(t *testing.T) {
	t.Parallel()
	// Identifier-only matching would skip the aliased call, and a stale-row
	// check cannot notice because the key is also used by the plain call.
	root := writeOpKindTree(t, map[string]string{
		"a.go": opKindFixtureHeader + "func a() error { return runlog.Check(nil, \"known\", nil) }\n",
		"b.go": "package p\n\nimport rl \"" + opKindPkgRunlog + "\"\n\nfunc a() error { return rl.Check(nil, \"known\", nil) }\n",
	})
	sites, _, err := scanOpSites(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(sites) != 2 {
		t.Fatalf("found %d sites, want 2: %+v", len(sites), sites)
	}
}

func TestOpKindScannerForwardingExemption(t *testing.T) {
	t.Parallel()
	genuine := opKindFixtureHeader +
		"func Fail(event int, op string, cause error) error {\n\treturn failure.New(op, cause)\n}\n\n" +
		"func Check(ctx int, op string, err error) error {\n\treturn failure.New(op, err)\n}\n"

	tests := []struct {
		name      string
		files     map[string]string
		wantSites int
		wantDiag  string // empty: no diagnostics expected
	}{
		{name: "genuine forwards are exempt and not sites", files: map[string]string{"internal/runlog/runlog.go": genuine}},
		{
			name:     "same package name elsewhere gets no exemption",
			files:    map[string]string{"other/runlog/runlog.go": genuine},
			wantDiag: "other/runlog/runlog.go:",
		},
		{
			name: "another function in the forwarding file gets no exemption",
			files: map[string]string{"internal/runlog/runlog.go": genuine +
				"\nfunc Extra(op string) error { return failure.New(op, nil) }\n"},
			wantDiag: "unsupported Op expression",
		},
		{
			name: "a second forward inside Check is reported",
			files: map[string]string{"internal/runlog/runlog.go": genuine +
				"\nfunc Fail2() {}\n" + strings.Replace(genuine, "func Fail(", "func Unused(", 1)[len(opKindFixtureHeader):]},
			// Pin the over-count branch: the Unused forward's "unsupported Op
			// expression" diagnostic would keep this case green even if the
			// exactly-one check were weakened to "at least one".
			wantDiag: "forward in Check, found 2",
		},
		{
			name: "missing forward is reported",
			files: map[string]string{"internal/runlog/runlog.go": opKindFixtureHeader +
				"func Fail(event int, op string, cause error) error { return failure.New(op, cause) }\n" +
				"func Check(ctx int, op string, err error) error { return nil }\n"},
			wantDiag: "exactly one failure.New(op, ...) forward in Check",
		},
		{
			name: "forwarding file with a different identifier is a dynamic Op",
			files: map[string]string{"internal/runlog/runlog.go": opKindFixtureHeader +
				"func Fail(event int, name string, cause error) error { return failure.New(name, cause) }\n" +
				"func Check(ctx int, op string, err error) error { return failure.New(op, err) }\n"},
			wantDiag: "unsupported Op expression",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sites, diags, err := scanOpSites(writeOpKindTree(t, tt.files))
			if err != nil {
				t.Fatal(err)
			}
			if len(sites) != tt.wantSites {
				t.Errorf("sites = %+v, want %d", sites, tt.wantSites)
			}
			joined := strings.Join(diags, "\n")
			if tt.wantDiag == "" && joined != "" {
				t.Errorf("unexpected diagnostics: %s", joined)
			}
			if tt.wantDiag != "" && !strings.Contains(joined, tt.wantDiag) {
				t.Errorf("diagnostics %q do not contain %q", joined, tt.wantDiag)
			}
		})
	}
}

func TestOpKindScannerRejectsUnsupportedShapes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		body     string
		wantDiag string
	}{
		{"dynamic op variable", "func a(op string) error { return runlog.Check(nil, op, nil) }", "a.go:"},
		{"concatenation", "func a() error { return runlog.Check(nil, \"a\"+\"b\", nil) }", "not a string literal"},
		{"named constant", "const c = \"x\"\n\nfunc a() error { return runlog.Check(nil, c, nil) }", "not a string literal"},
		{"non-string literal", "func a() error { return runlog.Check(nil, 5, nil) }", "non-string literal"},
		{"other function call", "func a() error { return runlog.Check(nil, strconv.Itoa(1), nil) }", "not fmt.Sprintf"},
		{"sprintf with variable format", "func a(f string) error { return runlog.Check(nil, fmt.Sprintf(f, 1), nil) }", "not a string literal"},
		{"sprintf with two substitutions", "func a() error { return runlog.Check(nil, fmt.Sprintf(\"%s %s\", 1, 2), nil) }", "exactly one substitution"},
		{"sprintf with no substitution", "func a() error { return runlog.Check(nil, fmt.Sprintf(\"x\"), nil) }", "exactly one substitution"},
		{"sprintf variadic", "func a(v []any) error { return runlog.Check(nil, fmt.Sprintf(\"%s\", v...), nil) }", "ellipsis=true"},
		{"too few arguments", "func a() error { return runlog.Check(nil, \"x\") }", "want exactly 3"},
		{"too many arguments", "func a() error { return failure.New(\"x\", nil, nil) }", "want exactly 2"},
		{"variadic call", "func a(v []any) error { return runlog.Check(v...) }", "ellipsis=true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := writeOpKindTree(t, map[string]string{"a.go": opKindFixtureHeader + tt.body + "\n"})
			sites, diags, err := scanOpSites(root)
			if err != nil {
				t.Fatal(err)
			}
			if len(sites) != 0 {
				t.Errorf("sites = %+v, want none", sites)
			}
			if len(diags) != 1 || !strings.Contains(diags[0], tt.wantDiag) || !strings.HasPrefix(diags[0], "a.go:") {
				t.Errorf("diagnostics = %v, want one at a.go:N containing %q", diags, tt.wantDiag)
			}
		})
	}
}

func TestOpKindScannerRejectsDotImportsAndBadSource(t *testing.T) {
	t.Parallel()
	for _, pkg := range []string{opKindPkgRunlog, opKindPkgFailure, opKindPkgFmt} {
		t.Run("dot import "+pkg, func(t *testing.T) {
			t.Parallel()
			root := writeOpKindTree(t, map[string]string{"a.go": "package p\n\nimport . \"" + pkg + "\"\n"})
			_, diags, err := scanOpSites(root)
			if err != nil {
				t.Fatal(err)
			}
			if len(diags) != 1 || !strings.Contains(diags[0], "dot import") || !strings.HasPrefix(diags[0], "a.go:3:") {
				t.Errorf("diagnostics = %v, want a dot-import diagnostic at a.go:3", diags)
			}
		})
	}
	t.Run("malformed source is an error", func(t *testing.T) {
		t.Parallel()
		root := writeOpKindTree(t, map[string]string{"bad.go": "package p\n\nfunc {"})
		if _, _, err := scanOpSites(root); err == nil || !strings.Contains(err.Error(), "bad.go") {
			t.Errorf("err = %v, want parse error naming bad.go", err)
		}
	})
	t.Run("missing root is an error", func(t *testing.T) {
		t.Parallel()
		if _, _, err := scanOpSites(filepath.Join(t.TempDir(), "absent")); err == nil {
			t.Error("want a walk error for a missing root")
		}
	})
	t.Run("empty tree finds nothing", func(t *testing.T) {
		t.Parallel()
		sites, diags, err := scanOpSites(t.TempDir())
		if err != nil || len(sites) != 0 || len(diags) != 0 {
			t.Errorf("sites=%v diags=%v err=%v, want all empty", sites, diags, err)
		}
	})
}

func TestOpKindTableChecker(t *testing.T) {
	t.Parallel()
	siteLit := opKindSite{Pos: "a.go:1", Key: "Failed to delete index", Shape: opShapeLiteral}
	siteFmt := opKindSite{Pos: "b.go:2", Key: "Reading %s thing", Shape: opShapeFormat}

	tests := []struct {
		name     string
		sites    []opKindSite
		rows     []opKindRow
		wantDiag []string // every entry must appear; nil means no diagnostics
	}{
		{
			name:  "matching table is clean",
			sites: []opKindSite{siteLit, siteFmt},
			rows: []opKindRow{
				opKindLit("Failed to delete index", ErrIndexOperation, ""),
				opKindFmtRow("Reading %s thing", ErrLoaderExecution, "fallback by design", "x"),
			},
		},
		{
			name:     "missing row names file:line",
			sites:    []opKindSite{siteLit},
			wantDiag: []string{"missing row", "a.go:1", "Failed to delete index"},
		},
		{
			name:     "reword reports missing and stale together",
			sites:    []opKindSite{{Pos: "a.go:1", Key: "Failed to drop index", Shape: opShapeLiteral}},
			rows:     []opKindRow{opKindLit("Failed to delete index", ErrIndexOperation, "")},
			wantDiag: []string{"missing row", "Failed to drop index", "stale row \"Failed to delete index\"", "no production position"},
		},
		{
			name:     "duplicate row",
			sites:    []opKindSite{siteLit},
			rows:     []opKindRow{opKindLit("Failed to delete index", ErrIndexOperation, ""), opKindLit("Failed to delete index", ErrIndexOperation, "")},
			wantDiag: []string{"duplicate row"},
		},
		{
			name:     "nil Kind",
			sites:    []opKindSite{siteLit},
			rows:     []opKindRow{opKindLit("Failed to delete index", nil, "")},
			wantDiag: []string{"nil expected Kind"},
		},
		{
			name:     "Kind mismatch names key, expansion, sites, got and want",
			sites:    []opKindSite{siteLit},
			rows:     []opKindRow{opKindLit("Failed to delete index", ErrBulkFailure, "")},
			wantDiag: []string{"Kind mismatch", "Failed to delete index", "a.go:1", "index operation failed", "bulk insert failed"},
		},
		{
			name:     "formatted row with an empty value set",
			sites:    []opKindSite{siteFmt},
			rows:     []opKindRow{opKindFmtRow("Reading %s thing", ErrLoaderExecution, "n")},
			wantDiag: []string{"no substitution values"},
		},
		{
			name:     "literal row carrying values",
			sites:    []opKindSite{siteLit},
			rows:     []opKindRow{{Key: "Failed to delete index", Shape: opShapeLiteral, Kind: ErrIndexOperation, Values: []any{"x"}}},
			wantDiag: []string{"must not carry substitution values"},
		},
		{
			name:     "row and site shape disagree",
			sites:    []opKindSite{siteFmt},
			rows:     []opKindRow{opKindLit("Reading %s thing", ErrLoaderExecution, "n")},
			wantDiag: []string{"is a literal but production uses a format", "b.go:2"},
		},
		{
			name:     "blank fallback note",
			sites:    []opKindSite{siteLit},
			rows:     []opKindRow{opKindLit("Failed to delete index", ErrLoaderExecution, "  ")},
			wantDiag: []string{"needs a note"},
		},
		{
			name:     "key used as both literal and format",
			sites:    []opKindSite{{Pos: "a.go:1", Key: "k", Shape: opShapeLiteral}, {Pos: "b.go:1", Key: "k", Shape: opShapeFormat}},
			rows:     []opKindRow{opKindLit("k", ErrLoaderExecution, "n")},
			wantDiag: []string{"both a literal and a format", "a.go:1", "b.go:1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			joined := strings.Join(checkOpKinds(tt.sites, tt.rows), "\n")
			if tt.wantDiag == nil {
				if joined != "" {
					t.Fatalf("unexpected diagnostics: %s", joined)
				}
				return
			}
			for _, want := range tt.wantDiag {
				if !strings.Contains(joined, want) {
					t.Errorf("diagnostics %q do not contain %q", joined, want)
				}
			}
		})
	}
}

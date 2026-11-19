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
// failure.New (Op is argument 0) under this package directory. Every other way
// to reach those functions or to build a failure.Error is REPORTED as an
// unsupported shape, never silently skipped, because it can carry an Op the
// scan cannot see:
//   - a qualified reference that is not the callee of a call (a function value
//     such as fn := runlog.Check, an argument, a field, a package var). A
//     wrapped callee such as (runlog.Check)(...) is not normalized into a
//     direct call; its selector is reported;
//   - an unqualified Fail/Check (exact directory internal/runlog) or New
//     (exact directory internal/failure), as a call or a value, whether it
//     resolves to a same-file receiver-free function declaration or is
//     unresolved because it is declared in another file;
//   - a failure.Error composite literal, qualified anywhere in the tree or
//     unqualified in the exact directory internal/failure, including an
//     element literal whose type is elided by its slice, array or map.
//
// Unqualified-name rules match the exact slash-normalized parent directory,
// not a prefix: a subdirectory is a different Go package with its own Fail,
// Check, New and Error. Qualified detection stays recursive.
//
// The one exemption is syntactic: the &Error{Op: op, ...} literal inside the
// top-level func New of internal/failure/error.go is the constructor, not an Op
// site, and exactly one must exist when that file is scanned.
//
// Still out of scope: new(failure.Error), assigning Op after construction,
// explicit-Kind RunError construction, named-type composite literal keys whose
// container kind is not visible syntactically, and packages outside pkg/loader.

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
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
	// opKindFailureFile is the only file whose &Error{Op: op, ...} literal inside
	// func New is the constructor itself rather than a bypass of failure.New.
	opKindFailureFile = "internal/failure/error.go"
	// Exact parent directories (never prefixes) that own the unqualified names.
	opKindRunlogDir  = "internal/runlog"
	opKindFailureDir = "internal/failure"
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

	// Every imported name (not only the target packages), so an alias such as
	// `New "errors"` used as New.New(...) is a package qualifier, not a
	// reference to an own-package New.
	importNames := map[string]bool{}
	for _, imp := range file.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue // already reported above
		}
		name := p[strings.LastIndex(p, "/")+1:]
		if imp.Name != nil {
			name = imp.Name.Name
		}
		importNames[name] = true
	}

	// Package-level type declarations of this file. A parsed local type also
	// has a *ast.TypeSpec Decl, so only membership here proves that an
	// unqualified Error is the package's own type rather than a local shadow.
	topTypes := map[*ast.TypeSpec]bool{}
	for _, decl := range file.Decls {
		if gd, ok := decl.(*ast.GenDecl); ok && gd.Tok == token.TYPE {
			for _, spec := range gd.Specs {
				if ts, ok := spec.(*ast.TypeSpec); ok {
					topTypes[ts] = true
				}
			}
		}
	}

	// Unqualified names owned by this file's exact directory. path.Dir on the
	// slash-normalized relative path, never a prefix: a subdirectory is a
	// different package whose Fail, Check, New and Error are its own.
	dir := path.Dir(rel)
	unqualified := map[string]bool{}
	switch dir {
	case opKindRunlogDir:
		unqualified["Fail"], unqualified["Check"] = true, true
	case opKindFailureDir:
		unqualified["New"] = true
	}
	inFailureDir := dir == opKindFailureDir

	forwards := map[string]int{}
	constructors := 0
	for _, decl := range file.Decls {
		funcName := ""
		if fd, ok := decl.(*ast.FuncDecl); ok && fd.Recv == nil {
			funcName = fd.Name.Name
		}
		// callFuns holds selectors that are the callee of a CallExpr. ast.Inspect
		// visits a CallExpr before its Fun, so the call handler marks the selector
		// before the selector handler sees it and a normal call is never also
		// reported as a non-call reference. skip holds identifiers that are not
		// references: selector names, declaration names, struct field names and
		// keys.
		callFuns := map[*ast.SelectorExpr]bool{}
		skip := map[*ast.Ident]bool{}
		elided := map[*ast.CompositeLit]ast.Expr{}
		ast.Inspect(decl, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.ImportSpec:
				return false // alias declarations are not references
			case *ast.FuncDecl:
				skip[n.Name] = true // also methods
			case *ast.Field:
				for _, name := range n.Names {
					skip[name] = true
				}
			case *ast.SelectorExpr:
				skip[n.Sel] = true
				if callFuns[n] {
					return true
				}
				if callee, ok := opKindResolveSelector(n, imports); ok {
					if _, isTarget := opKindTargets[callee]; isTarget {
						diags = append(diags, fmt.Sprintf("%s: reference to %s.%s that is not a direct call is unsupported: an indirect call hides its Op from the scan", posOf(n.Pos()), callee.pkg[strings.LastIndex(callee.pkg, "/")+1:], callee.fn))
					}
				}
			case *ast.CompositeLit:
				// An element, key or value literal may elide its type (as in
				// []*failure.Error{{Op: "x"}}); its enclosing literal, visited
				// first, recorded the implied type. Reading only n.Type would
				// silently skip an elided failure.Error and hide elided map keys.
				typ := n.Type
				if typ == nil {
					typ = elided[n]
				}
				opKindNoteElided(typ, n.Elts, elided)
				// Struct field keys are names, not references. Explicit map, array
				// and slice keys are expressions and stay visible.
				switch typ.(type) {
				case *ast.MapType, *ast.ArrayType:
				default:
					for _, elt := range n.Elts {
						if kv, ok := elt.(*ast.KeyValueExpr); ok {
							if id, ok := kv.Key.(*ast.Ident); ok {
								skip[id] = true
							}
						}
					}
				}
				switch t := typ.(type) {
				case *ast.SelectorExpr:
					if callee, ok := opKindResolveSelector(t, imports); ok && callee == (opKindCallee{opKindPkgFailure, "Error"}) {
						diags = append(diags, fmt.Sprintf("%s: failure.Error composite literal is unsupported: it can set Op directly; construct it with failure.New so the Op is scanned", posOf(n.Pos())))
					}
				case *ast.Ident:
					if !inFailureDir || t.Name != "Error" {
						break
					}
					if t.Obj != nil {
						// Resolved: only the package's own top-level type counts; a
						// local shadow type is unrelated.
						if ts, ok := t.Obj.Decl.(*ast.TypeSpec); !ok || !topTypes[ts] {
							break
						}
					}
					if rel == opKindFailureFile && funcName == "New" && opKindIsConstructorLit(n) {
						constructors++
						break
					}
					diags = append(diags, fmt.Sprintf("%s: failure.Error composite literal is unsupported: it can set Op directly; only the constructor in func New of %s is exempt", posOf(n.Pos()), opKindFailureFile))
				}
			case *ast.Ident:
				if skip[n] || !unqualified[n.Name] {
					return true
				}
				if n.Obj == nil {
					// Unresolved: declared in another file, or an imported package
					// qualifier (never a reference to the own-package function).
					if importNames[n.Name] {
						return true
					}
				} else if fd, ok := n.Obj.Decl.(*ast.FuncDecl); !ok || n.Obj.Kind != ast.Fun || fd.Recv != nil {
					// Resolved to a local variable, parameter, type or other
					// binding: a shadow, not the package-level function. Only a
					// same-file receiver-free function declaration is a reference.
					return true
				}
				diags = append(diags, fmt.Sprintf("%s: unqualified reference to %s in %s is unsupported: a call or function value through it hides its Op from the scan", posOf(n.Pos()), n.Name, dir))
			case *ast.CallExpr:
				if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
					callFuns[sel] = true
				}
				callee, ok := opKindResolve(n, imports)
				if !ok {
					return true
				}
				target, ok := opKindTargets[callee]
				if !ok {
					return true
				}
				pos := posOf(n.Pos())
				if len(n.Args) != target.argc || n.Ellipsis.IsValid() {
					diags = append(diags, fmt.Sprintf("%s: %s.%s call has %d arguments (ellipsis=%t), want exactly %d", pos, callee.pkg[strings.LastIndex(callee.pkg, "/")+1:], callee.fn, len(n.Args), n.Ellipsis.IsValid(), target.argc))
					return true
				}
				opArg := n.Args[target.opIdx]
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
			}
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
	if rel == opKindFailureFile && constructors != 1 {
		diags = append(diags, fmt.Sprintf("%s: expected exactly one &Error{Op: op, ...} constructor literal in New, found %d", rel, constructors))
	}
	return sites, diags
}

// opKindNoteElided records, for each element, key or value literal of an array,
// slice or map literal of type typ that elides its own type, the type the
// container implies. A pointer element type implies its pointee, matching Go's
// elision of &T. Array index keys imply nothing.
func opKindNoteElided(typ ast.Expr, elts []ast.Expr, elided map[*ast.CompositeLit]ast.Expr) {
	var keyType, valType ast.Expr
	switch t := typ.(type) {
	case *ast.ArrayType:
		valType = t.Elt
	case *ast.MapType:
		keyType, valType = t.Key, t.Value
	default:
		return
	}
	note := func(e, implied ast.Expr) {
		lit, ok := e.(*ast.CompositeLit)
		if !ok || lit.Type != nil || implied == nil {
			return
		}
		if star, isStar := implied.(*ast.StarExpr); isStar {
			implied = star.X
		}
		elided[lit] = implied
	}
	for _, elt := range elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			note(kv.Key, keyType)
			note(kv.Value, valType)
			continue
		}
		note(elt, valType)
	}
}

// opKindIsConstructorLit reports whether lit sets its Op field to the identifier
// op. Syntactic only: it does not prove parameter identity or the Err value.
func opKindIsConstructorLit(lit *ast.CompositeLit) bool {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, keyOK := kv.Key.(*ast.Ident)
		val, valOK := kv.Value.(*ast.Ident)
		if keyOK && valOK && key.Name == "Op" && val.Name == "op" {
			return true
		}
	}
	return false
}

// opKindResolve returns the imported package path and function of a
// package-qualified call. A local variable shadowing the package name has a
// non-nil Obj and is not a call into the package.
func opKindResolve(call *ast.CallExpr, imports map[string]string) (opKindCallee, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return opKindCallee{}, false
	}
	return opKindResolveSelector(sel, imports)
}

// opKindResolveSelector resolves a package-qualified selector, whether or not
// it is called, with the same import-path and shadow rules as opKindResolve.
func opKindResolveSelector(sel *ast.SelectorExpr, imports map[string]string) (opKindCallee, bool) {
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
		opKindLit("Failed to build mapping preflight plan", ErrLoaderExecution, "local mappings plan decoding names no classifier keyword; ErrLoaderExecution by design, consistent with the ErrInvalidOptions documentation of file failures; includes non-object sections rejected by decoding and numeric overflow (DEC-OPKIND-01)"),
		opKindLit("Reading settings file", ErrLoaderExecution, "local settings file handling names no classifier keyword; ErrLoaderExecution by design, as the ErrInvalidOptions documentation of file failures states (DEC-OPKIND-01)"),
		opKindLit("Parsing settings file", ErrLoaderExecution, "local settings file handling names no classifier keyword; ErrLoaderExecution by design, as the ErrInvalidOptions documentation of file failures states (DEC-OPKIND-01)"),
		opKindLit("Parsing nested settings file", ErrLoaderExecution, "local settings file handling names no classifier keyword; ErrLoaderExecution by design, as the ErrInvalidOptions documentation of file failures states (DEC-OPKIND-01)"),
		opKindLit("Serializing settings", ErrLoaderExecution, "local settings file handling names no classifier keyword; ErrLoaderExecution by design, as the ErrInvalidOptions documentation of file failures states (DEC-OPKIND-01)"),
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
		opKindFmtRow("Reading %s file", ErrLoaderExecution, "section is only \"mappings\"; local mappings file handling names no classifier keyword; ErrLoaderExecution by design, as the ErrInvalidOptions documentation of file failures states (DEC-OPKIND-01)", "mappings"),
		opKindFmtRow("Parsing %s file", ErrLoaderExecution, "section is only \"mappings\"; local mappings file handling names no classifier keyword; ErrLoaderExecution by design, as the ErrInvalidOptions documentation of file failures states (DEC-OPKIND-01)", "mappings"),
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
		{"function value", "func a() { fn := runlog.Check; _ = fn }", "not a direct call"},
		{"function value argument", "func a() { use(failure.New) }", "not a direct call"},
		{"failure.Error literal", "func a() any { return &failure.Error{Op: \"x\"} }", "failure.Error composite literal"},
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

// ─── Unsupported-reference, own-package and failure-literal shapes ────────────

// opKindAliasHeader imports the target packages under aliases. Fixture bodies
// that follow it start at line 9; those after opKindFixtureHeader start at 10.
const opKindAliasHeader = "package p\n\nimport (\n\tf \"fmt\"\n\trl \"" + opKindPkgRunlog + "\"\n\tfl \"" + opKindPkgFailure + "\"\n)\n\n"

// runOpKindScanCase scans a fixture tree and requires exactly the wanted site
// count and exactly one diagnostic per wanted prefix. Prefixes carry file:line,
// so a wrong line fails; a wanted prefix cannot be satisfied twice.
func runOpKindScanCase(t *testing.T, files map[string]string, wantSites int, want []string) {
	t.Helper()
	sites, diags, err := scanOpSites(writeOpKindTree(t, files))
	if err != nil {
		t.Fatal(err)
	}
	if len(sites) != wantSites {
		t.Errorf("sites = %+v, want %d", sites, wantSites)
	}
	if len(diags) != len(want) {
		t.Fatalf("diagnostics = %q, want %d matching %q", diags, len(want), want)
	}
	used := make([]bool, len(diags))
	for _, w := range want {
		matched := false
		for i, d := range diags {
			if !used[i] && strings.HasPrefix(d, w) {
				used[i], matched = true, true
				break
			}
		}
		if !matched {
			t.Errorf("no diagnostic with prefix %q in %q", w, diags)
		}
	}
}

func TestOpKindScannerRejectsReferenceShapes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		files     map[string]string
		wantSites int
		want      []string
	}{
		{
			name:  "function value assigned to a variable",
			files: map[string]string{"a.go": opKindFixtureHeader + "func a() { fn := runlog.Check; _ = fn }\n"},
			want:  []string{"a.go:10: reference to runlog.Check that is not a direct call"},
		},
		{
			name:  "function value passed as an argument",
			files: map[string]string{"a.go": opKindFixtureHeader + "func a() { use(failure.New) }\n"},
			want:  []string{"a.go:10: reference to failure.New that is not a direct call"},
		},
		{
			name: "struct field and package var initialised to runlog.Fail",
			files: map[string]string{"a.go": opKindFixtureHeader +
				"type s struct{ f any }\n\nvar v = runlog.Fail\n\nfunc a() s { return s{f: runlog.Fail} }\n"},
			want: []string{
				"a.go:12: reference to runlog.Fail that is not a direct call",
				"a.go:14: reference to runlog.Fail that is not a direct call",
			},
		},
		{
			name: "aliased imports",
			files: map[string]string{"a.go": opKindAliasHeader +
				"func a() {\n\tfn := rl.Check\n\tuse(fl.New)\n\t_ = fn\n}\n"},
			want: []string{
				"a.go:10: reference to runlog.Check that is not a direct call",
				"a.go:11: reference to failure.New that is not a direct call",
			},
		},
		{
			name:  "parenthesized callee is not normalized into a direct call",
			files: map[string]string{"a.go": opKindFixtureHeader + "func a() error { return (runlog.Check)(nil, \"x\", nil) }\n"},
			want:  []string{"a.go:10: reference to runlog.Check that is not a direct call"},
		},
		{
			name: "supported call keeps its site and the value is reported once",
			files: map[string]string{"a.go": opKindFixtureHeader +
				"func a() error {\n\tfn := runlog.Check\n\t_ = fn\n\treturn runlog.Check(nil, \"kept\", nil)\n}\n"},
			wantSites: 1,
			want:      []string{"a.go:11: reference to runlog.Check that is not a direct call"},
		},
		{
			name:  "pointer failure.Error literal",
			files: map[string]string{"a.go": opKindFixtureHeader + "func a() any { return &failure.Error{Op: \"x\"} }\n"},
			want:  []string{"a.go:10: failure.Error composite literal is unsupported"},
		},
		{
			name:  "value failure.Error literal",
			files: map[string]string{"a.go": opKindFixtureHeader + "func a() any { return failure.Error{Op: \"x\"} }\n"},
			want:  []string{"a.go:10: failure.Error composite literal is unsupported"},
		},
		{
			name:  "aliased failure.Error literal",
			files: map[string]string{"a.go": opKindAliasHeader + "func a() any { return fl.Error{Op: \"x\"} }\n"},
			want:  []string{"a.go:9: failure.Error composite literal is unsupported"},
		},
		{
			// An element literal may elide its type; the container's element, key
			// or value type still names failure.Error syntactically. Reading only
			// the literal's own Type would skip these silently.
			name: "elided-type failure.Error literals in slices, map values, map keys and nested slices",
			files: map[string]string{"a.go": opKindFixtureHeader +
				"var s = []*failure.Error{{Op: \"x\"}}\n\nvar m = map[string]failure.Error{\"k\": {Op: \"y\"}}\n\n" +
				"var k = map[failure.Error]bool{{Op: \"z\"}: true}\n\nvar n = [][]failure.Error{{{Op: \"w\"}}}\n"},
			want: []string{
				"a.go:10: failure.Error composite literal is unsupported",
				"a.go:12: failure.Error composite literal is unsupported",
				"a.go:14: failure.Error composite literal is unsupported",
				"a.go:16: failure.Error composite literal is unsupported",
			},
		},
		{
			name: "qualified references stay recursive in the defining-package subdirectories",
			files: map[string]string{
				"internal/runlog/helper/x.go":  opKindFixtureHeader + "var v = runlog.Check\n",
				"internal/failure/helper/x.go": opKindFixtureHeader + "var e = &failure.Error{Op: \"x\"}\n\nvar n = failure.New\n",
			},
			want: []string{
				"internal/failure/helper/x.go:10: failure.Error composite literal is unsupported",
				"internal/failure/helper/x.go:12: reference to failure.New that is not a direct call",
				"internal/runlog/helper/x.go:10: reference to runlog.Check that is not a direct call",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runOpKindScanCase(t, tt.files, tt.wantSites, tt.want)
		})
	}
}

func TestOpKindScannerRejectsOwnPackageShapes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{
			// Exact-directory ownership: a subdirectory or lookalike path is a
			// different package with its own Fail, Check, New and Error. Matching
			// by prefix would flag every file here.
			name: "subdirectories and lookalike directories own their names",
			files: map[string]string{
				"internal/runlog/helper/x.go":  "package p\n\nfunc a() error { return Fail(nil, \"x\", nil) }\n\nvar v = Check\n",
				"internal/failure/helper/x.go": "package p\n\nfunc a() error { return New(\"x\", nil) }\n\nvar e = Error{Op: \"x\"}\n",
				"internal/runlogextra/x.go":    "package p\n\nvar v = Check\n\nvar w = Fail\n",
				"internal/failureextra/x.go":   "package p\n\nvar n = New\n\nvar e = Error{Op: \"x\"}\n",
				"other/runlog/x.go":            "package p\n\nvar v = Check\n",
				"other/failure/x.go":           "package p\n\nvar n = New\n\nvar e = Error{Op: \"x\"}\n",
				"x.go":                         "package p\n\nvar v = Check\n\nvar n = New\n\nvar e = Error{Op: \"x\"}\n",
			},
		},
		{
			name:  "unqualified Fail call in runlog",
			files: map[string]string{"internal/runlog/x.go": "package p\n\nfunc a() error { return Fail(nil, \"x\", nil) }\n"},
			want:  []string{"internal/runlog/x.go:3: unqualified reference to Fail in internal/runlog"},
		},
		{
			name:  "unqualified Check value in runlog",
			files: map[string]string{"internal/runlog/x.go": "package p\n\nvar v = Check\n"},
			want:  []string{"internal/runlog/x.go:3: unqualified reference to Check in internal/runlog"},
		},
		{
			name:  "unqualified New call in failure",
			files: map[string]string{"internal/failure/x.go": "package p\n\nfunc a() error { return New(\"x\", nil) }\n"},
			want:  []string{"internal/failure/x.go:3: unqualified reference to New in internal/failure"},
		},
		{
			name: "same-file Check call and value, before and after the declaration",
			files: map[string]string{"internal/runlog/same.go": "package p\n\nvar before = Check\n\n" +
				"func Check(a, b, c int) error { return nil }\n\nfunc use() error { return Check(1, 2, 3) }\n"},
			want: []string{
				"internal/runlog/same.go:3: unqualified reference to Check",
				"internal/runlog/same.go:7: unqualified reference to Check",
			},
		},
		{
			name: "same-file Fail call and value, before and after the declaration",
			files: map[string]string{"internal/runlog/same.go": "package p\n\nfunc use() error { return Fail(1, 2, 3) }\n\n" +
				"func Fail(a, b, c int) error { return nil }\n\nvar after = Fail\n"},
			want: []string{
				"internal/runlog/same.go:3: unqualified reference to Fail",
				"internal/runlog/same.go:7: unqualified reference to Fail",
			},
		},
		{
			name: "same-file New value and call, before and after the declaration",
			files: map[string]string{"internal/failure/same.go": "package p\n\nvar before = New\n\n" +
				"func New(op string, cause error) error { return nil }\n\nfunc use() error { return New(\"x\", nil) }\n"},
			want: []string{
				"internal/failure/same.go:3: unqualified reference to New",
				"internal/failure/same.go:7: unqualified reference to New",
			},
		},
		{
			name:  "map key is an expression, not a field name",
			files: map[string]string{"internal/runlog/m.go": "package p\n\nvar m = map[any]bool{Check: true}\n"},
			want:  []string{"internal/runlog/m.go:3: unqualified reference to Check"},
		},
		{
			name:  "array index key is an expression, not a field name",
			files: map[string]string{"internal/runlog/m.go": "package p\n\nvar m = [2]int{Fail: 1}\n"},
			want:  []string{"internal/runlog/m.go:3: unqualified reference to Fail"},
		},
		{
			name:  "elided map-typed element keeps its keys as expressions",
			files: map[string]string{"internal/runlog/m.go": "package p\n\nvar m = []map[any]bool{{Check: true}}\n"},
			want:  []string{"internal/runlog/m.go:3: unqualified reference to Check"},
		},
		{
			name:  "elided-type Error literal in the failure directory",
			files: map[string]string{"internal/failure/x.go": "package p\n\nvar s = []*Error{{Op: \"x\"}}\n"},
			want:  []string{"internal/failure/x.go:3: failure.Error composite literal is unsupported"},
		},
		{
			name:  "extra unqualified Error literal in the failure directory",
			files: map[string]string{"internal/failure/x.go": "package p\n\nvar e = Error{Op: \"x\"}\n"},
			want:  []string{"internal/failure/x.go:3: failure.Error composite literal is unsupported"},
		},
		{
			name: "exempt-shaped literal in another file of the directory",
			files: map[string]string{
				"internal/failure/error.go": opKindFailureCtor(""),
				"internal/failure/other.go": "package p\n\nfunc New(op string) any { return &Error{Op: op} }\n",
			},
			want: []string{"internal/failure/other.go:3: failure.Error composite literal is unsupported"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runOpKindScanCase(t, tt.files, 0, tt.want)
		})
	}
}

func TestOpKindScannerAcceptsLookalikes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		files     map[string]string
		wantSites int
	}{
		{
			name: "methods, fields and keyed struct fields named like the targets",
			files: map[string]string{
				"internal/runlog/x.go":  "package p\n\ntype t struct{ Fail int }\n\nfunc (t) Check() {}\n\nvar v = t{Fail: 1}\n\nfunc a(x t) { x.Check(); _ = x.Fail }\n",
				"internal/failure/x.go": "package p\n\ntype u struct{ New int }\n\nfunc (u) New() {}\n\nvar v = u{New: 1}\n\nfunc a(x u) { x.New() }\n",
			},
		},
		{
			name: "local function variables and parameters",
			files: map[string]string{
				"internal/runlog/x.go":  "package p\n\nfunc a(Fail func()) { Fail() }\n\nfunc b() { Check := func() {}; Check() }\n",
				"internal/failure/x.go": "package p\n\nfunc a(New func()) { New() }\n",
			},
		},
		{
			name: "import alias named New and errors.New",
			files: map[string]string{"internal/failure/x.go": "package p\n\nimport (\n\tNew \"errors\"\n\t\"errors\"\n)\n\n" +
				"var a = New.New(\"x\")\n\nvar b = errors.New(\"y\")\n"},
		},
		{
			name:  "local type Error shadowing the package type",
			files: map[string]string{"internal/failure/x.go": "package p\n\nfunc a() any {\n\ttype Error struct{ Op string }\n\treturn Error{Op: \"x\"}\n}\n"},
		},
		{
			name: "elided-type elements of local struct and shadow Error types",
			files: map[string]string{
				"internal/runlog/x.go":  "package p\n\ntype t struct{ Fail int }\n\nvar v = []t{{Fail: 1}}\n",
				"internal/failure/x.go": "package p\n\nfunc a() any {\n\ttype Error struct{ Op string }\n\treturn []Error{{Op: \"x\"}}\n}\n",
			},
		},
		{
			name: "shadowed package names, unrelated selectors and a normal call",
			files: map[string]string{"a.go": opKindFixtureHeader +
				"func a() error {\n\trunlog, failure := local{}, local{}\n\tfn := runlog.Check\n\t_ = failure.Error{}\n\tg := fmt.Sprintf\n\t_, _ = fn, g\n\treturn nil\n}\n\n" +
				"var p *failure.Error\n\nfunc b() error { return runlog.Check(nil, \"ok\", nil) }\n"},
			wantSites: 1,
		},
		{
			name:  "genuine failure constructor",
			files: map[string]string{"internal/failure/error.go": opKindFailureCtor("")},
		},
		{
			name:  "constructor-shaped file outside internal/failure gets no exemption and no rule",
			files: map[string]string{"other/failure/error.go": opKindFailureCtor("")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runOpKindScanCase(t, tt.files, tt.wantSites, nil)
		})
	}
}

// opKindFailureCtor returns a copy of the real internal/failure/error.go shape.
// extra is spliced into New just before its return, so a fixture can add a
// literal or a shadow type inside the constructor.
func opKindFailureCtor(extra string) string {
	return "package p\n\nimport \"errors\"\n\ntype Error struct {\n\tOp  string\n\tErr error\n}\n\n" +
		"func New(op string, cause error) *Error {\n\tif cause == nil {\n\t\tcause = errors.New(op)\n\t}\n" +
		extra + "\treturn &Error{Op: op, Err: cause}\n}\n"
}

func TestOpKindScannerFailureConstructorExemption(t *testing.T) {
	t.Parallel()
	const file = "internal/failure/error.go"
	genuine := opKindFailureCtor("")
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{name: "genuine constructor is exempt", files: map[string]string{file: genuine}},
		{
			name: "missing constructor literal is reported",
			files: map[string]string{file: "package p\n\ntype Error struct{ Op string }\n\n" +
				"func New(op string, cause error) *Error { return nil }\n"},
			want: []string{file + ": expected exactly one &Error{Op: op, ...} constructor literal in New, found 0"},
		},
		{
			name:  "second matching literal is reported by the counter only",
			files: map[string]string{file: opKindFailureCtor("\tif op == \"\" {\n\t\treturn &Error{Op: op}\n\t}\n")},
			want:  []string{file + ": expected exactly one &Error{Op: op, ...} constructor literal in New, found 2"},
		},
		{
			name:  "exempt-shaped literal in another function",
			files: map[string]string{file: genuine + "\nfunc Other(op string) *Error { return &Error{Op: op} }\n"},
			want:  []string{file + ":17: failure.Error composite literal is unsupported"},
		},
		{
			name:  "method named New gets no exemption",
			files: map[string]string{file: genuine + "\nfunc (*Error) New(op string) *Error { return &Error{Op: op} }\n"},
			want:  []string{file + ":17: failure.Error composite literal is unsupported"},
		},
		{
			name:  "literal Op value is reported and does not count",
			files: map[string]string{file: strings.Replace(genuine, "Op: op, Err", "Op: \"x\", Err", 1)},
			want: []string{
				file + ":14: failure.Error composite literal is unsupported",
				file + ": expected exactly one &Error{Op: op, ...} constructor literal in New, found 0",
			},
		},
		{
			name:  "different identifier Op value is reported and does not count",
			files: map[string]string{file: strings.Replace(genuine, "Op: op, Err", "Op: name, Err", 1)},
			want: []string{
				file + ":14: failure.Error composite literal is unsupported",
				file + ": expected exactly one &Error{Op: op, ...} constructor literal in New, found 0",
			},
		},
		{
			name: "local shadow type inside New neither reports nor counts",
			files: map[string]string{file: opKindFailureCtor(
				"\t_ = func() any {\n\t\ttype Error struct{ Op string }\n\t\treturn Error{Op: op}\n\t}\n")},
		},
		{
			name: "local shadow type alone does not satisfy the count",
			files: map[string]string{file: "package p\n\ntype Error struct{ Op string }\n\n" +
				"func New(op string, cause error) *Error {\n\ttype Error struct{ Op string }\n\t_ = Error{Op: op}\n\treturn nil\n}\n"},
			want: []string{file + ": expected exactly one &Error{Op: op, ...} constructor literal in New, found 0"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runOpKindScanCase(t, tt.files, 0, tt.want)
		})
	}
}

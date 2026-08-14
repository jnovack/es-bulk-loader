# CLAUDE.md

This is the canonical instruction file for `es-bulk-loader`. The sections below
are specific to this repository. Shared, cross-repo conventions (core rules,
change sizing, documentation, testing philosophy, Go language rules) are
assembled from templates in the marked region at the bottom and inherited from
`~/.claude/CLAUDE.md` — do not restate them here.

## Purpose

This repository is for `es-bulk-loader`, a Go CLI that should handle the full
Elasticsearch data-loading lifecycle, not just raw bulk inserts.

When changing behavior, optimize for the real product goal:

1. Create or recreate indices.
2. Apply settings and mappings.
3. Create or update ingest pipelines.
4. Create or update enrich policies.
5. Bulk load data.
6. Refresh and execute enrich policies when requested.
7. Leave Elasticsearch in a clean, predictable state for `-delete`, `-flush`, and `-add`.

The loader should own that workflow end-to-end.

## Project Preferences

### Loader First

- Prefer testing and validating behavior through `es-bulk-loader` itself.
- Shell scripts are useful, but they should support the Go project, not replace it.
- End-to-end tests should primarily verify the loader’s behavior, flags, and ordering.

### Elasticsearch Lifecycle Expectations

- `-delete` in normal mode means a hard reset for the concrete index and managed resources relevant to that run.
- `-delete -alias` means rollover: create a new `<alias>-YYYYMMDDHHMMSS`
  concrete index, load data there, and repoint the alias.
- In alias mode, `-delete` keeps existing managed resources by default; `-sync-managed` should upsert current definitions.
- If `-delete -alias` is run without `-sync-managed`, the loader should warn and assume sync-managed behavior for that run.
- If `-delete -alias` is run without `-keep-last`, the loader should warn that
  no old generations were deleted and storage may grow over time.
- Use `-nuke` for destructive managed-resource cleanup when alias generations may still reference pipelines.
- `-keep-last N` only applies with `-alias` and should prune older timestamped generations so only the newest `N` remain.
- Timestamped alias generations must use numeric suffix format `YYYYMMDDHHMMSS`.
- Logs should use Elasticsearch nouns explicitly: `alias` for the logical name and `index` for the concrete index name.
- `-flush` means keep index structure and managed resources, but replace data only.
- `-add` means append/update behavior without destructive reset.
- `-enrich` should only execute enrich policies when requested.
- If `-enrich` is requested and no policies exist, warn clearly rather than silently succeeding.
- Unknown enrich policy names should warn and skip, not hard fail the entire run.
- Enrich policies are managed by logical key plus content hash suffix (`<logical>-<sha256[:6]>`).
- Pipeline enrich processors should reference resolved managed policy names, not stale logical names.
- Unreferenced managed policy versions should be garbage-collected during sync-managed runs.
- Refresh source data before executing enrich policies so backing indices are built from visible documents.

### Fixtures and Test Layout

- Keep fixtures organized per index where practical.
- For a single index, multiple pipelines may live in one keyed JSON file.
- For a single index, multiple policies may live in one keyed JSON file.
- Do not collapse unrelated index fixtures into one monolithic file just to reduce file count.
- Preserve the separation between source-index and target-index fixtures in E2E tests.

## Non-Negotiables

These outrank convenience, elegance, and diff size. Where they conflict with a
preference stated elsewhere in this file, these win.

### Working and correct beats clever

- A wrong result that looks like success is the worst outcome this project can
  produce — worse than a crash, worse than a failed run. A failure gets
  investigated; silently wrong enrichment gets shipped and trusted.
- When the loader cannot determine the correct resource, name, or value
  unambiguously, **fail the run and name the candidates in the error**. Do not
  pick a plausible one and log a warning. A warning in a CI log is not a signal
  anyone acts on, and "we warned" is not a defense when the data is wrong.
- Never silently rewrite consumer input. Templating, settings normalization, and
  managed-name rewriting may transform input only where the transformation is
  documented and unambiguous. Config *data* must never be able to collide with
  config *syntax*.
- Prefer a narrow implementation that is provably right over a general one that
  is probably right. A special case that is obviously correct beats an
  abstraction that is correct if you trace it.

### Simplicity that stays simple

- Simplicity is measured by how hard the code is to *verify*, not by how few
  lines it took to write. If explaining why a function is correct requires
  tracing three files, it is not simple regardless of its size.
- `pkg/loader/loader.go` is already past the point where appending to it is
  free. New concerns go into their own package; do not extend the monolith
  because that is the smaller diff today.
- Two near-identical code paths are a defect. Extend the existing shape rather
  than adding a parallel one beside it.
- Clever control flow in lifecycle ordering is prohibited. Order matters here
  and must be readable top to bottom.

### Trusted platform

Consumers point this tool at production clusters and let it create, rewrite, and
delete state there. That trust is the product, and it is lost per-incident, not
per-release.

- Backward compatibility is the default for flags, env vars, file formats, and
  the `pkg/loader` public type surface. Breaking any of them is a deliberate,
  documented decision — never a side effect.
- Any operation that alters or destroys cluster state logs its intent, naming
  the concrete resources, *before* acting — at a level visible by default.
- A behavior change that can silently alter cluster state or loaded data ships
  with a guard test and a release note, even when the old behavior was a bug.

### Testing gate

All available testing is required, not offered. This section is the explicit
standing request that the shared rules defer to.

- Every change ships unit tests, including changes that look too small to need
  them.
- Every change that touches Elasticsearch interaction ships integration
  coverage.
- Changes to lifecycle ordering, destructive operations, alias rollover, or
  managed-resource garbage collection must be exercised through the Docker E2E
  suite before being called done. This overrides the shared rule that E2E tests
  are written only on request — for these areas, consider them requested.
- "Done" means `go test ./...` and `go test -race ./...` were actually run and
  exited 0, plus the relevant E2E path where the rule above applies. Report
  which commands ran. Untested code is not finished code, however obvious it
  looks.

## Elasticsearch Coding & Testing

These are the repo-specific deltas on top of the shared Go rules below.

- Favor explicit logging around Elasticsearch operations, especially create/delete/enrich behavior.
- When handling Elasticsearch responses, log enough detail to debug failures quickly.
- Be careful with operation ordering. In this project, order matters.
- Accept practical input formats when reasonable. If users provide either
  wrapped or raw JSON for settings/mappings, normalize it rather than forcing
  brittle fixtures.
- Keep variables, functions, types, and behavior generic and stand-alone;
  avoid consumer-specific naming or semantics so the project remains reusable
  across integrations.
- For end-to-end coverage, prefer Docker-based tests that prove the loader performs the full workflow correctly.
- An E2E test should verify outcomes, not just fire requests. Verification
  scripts should fail loudly and print useful Elasticsearch responses when
  assertions fail.

## Good Outcomes

A good change in this repository usually has these properties:

- The CLI behavior is clearer.
- Elasticsearch resource handling is more complete.
- The E2E path tests the Go program, not just fixture scripts.
- Logs make failures obvious.
- Docs explain how to structure files and what each flag actually does.

## Project overrides

- The Docker-based E2E path and per-index fixture layout described above
  supersede the Go template's `test/` pyramid and build-tag structure for this
  repository. Follow the repo's existing E2E approach, not the template's.
- The Testing Gate above supersedes the shared "write E2E only when asked" rule
  for lifecycle ordering, destructive operations, alias rollover, and managed
  resource GC.

<!-- templates: lang-golang -->

<!-- BEGIN TEMPLATES v:1 hash:0237d3aaf765 -->
## go

### Formatting

- All Go code must be `gofmt`-clean before committing. Run `gofmt -l .` — any output is a failure.
- Imports must be grouped and ordered by `goimports`: stdlib, then external, then internal. Do not hand-sort.
- Do not leave unused imports or variables — the compiler rejects them, and they indicate incomplete work.

### Error handling

- Check every error. Do not assign errors to `_` unless the function is documented as always-nil.
- Wrap errors with context using `fmt.Errorf("operation: %w", err)` so callers can use `errors.Is`/`errors.As`.
- Do not use `panic` in library or application code for recoverable errors. Reserve it for truly unrecoverable
  programmer errors (e.g. invalid state at init time).
- Do not use `log.Fatal` or `os.Exit` deep inside packages unless the user requires it — only at `main()` or top-level entrypoints.

### Idioms

- Accept interfaces, return concrete types.
- Use `context.Context` as the first argument in any function that does I/O, makes network calls, or may need cancellation.
- Use `defer` for cleanup, but be aware that deferred calls in loops execute at function return, not loop iteration.
- Short variable names (`i`, `v`, `err`) are fine in short scopes. Use descriptive names across function boundaries.

### Documentation (GoDoc)

Every package has a package comment, but only some deserve a full doc.go. Two tiers:

- **Contract packages** get a full doc.go: the package is imported by two or more other
  packages AND defines a contract the source alone does not show — lifecycle ordering,
  concurrency guarantees, wire/data formats, ldflags injection points, or an extension
  point other packages implement against. Document the purpose, the contract, and any
  non-obvious design decisions. Nothing else.
- **Everything else** gets a brief package comment, ten lines or fewer: what the package
  provides and who consumes it. It may live atop the primary source file instead of a
  separate doc.go.

Rules that apply to both tiers:

- Never include file-layout listings, "why this is a separate package" rationale, or
  prose that restates code structure. All of it is derivable from the code and goes
  stale silently the moment the code moves.
- When several packages implement one shared pattern (e.g. pipeline stages), document
  the pattern once in the contract package they implement against; each implementation
  documents only its deltas — data source, outputs, quirks.
- Right-sizing cuts both ways: an oversized doc.go that violates its tier should be
  shrunk, not preserved.

### Concurrency

- Every goroutine must have a defined owner responsible for its lifetime.
- Do not start a goroutine without a way to observe its termination (via `sync.WaitGroup`, channel drain, or context cancellation).
- Prefer channels for communication, mutexes for protecting shared state — do not mix casually.
- Run the race detector on tests: `go test -race ./...`. A race condition is a bug, not a warning.

### Building

- To produce a binary, use `make build` or `go build -o bin/<binary>` — never let `go build` drop a binary
  in the project root.
- To verify a change compiles (no artifact wanted), use `go build ./...` — it discards output and covers
  every package, unlike `go build ./cmd/...`.

### Project Structure

Follow [https://github.com/golang-standards/project-layout/](https://github.com/golang-standards/project-layout/) as
closely as possible.

```text
{repo}/
├── build/
│   └── package/
│       └── Dockerfile         # multi-stage: golang:<current-stable>-alpine → scratch
├── cmd/
│   └── {application}/
│       └── main.go           # entry point only — flag parsing, wiring, shutdown
├── deployments/              # Kubernetes manifests, Helm charts, Grafana dashboards, etc.
├── docs/                     # user-facing documentation
├── internal/                 # all non-exported application logic
│   └── {package}/
│       ├── {file}.go
│       └── {file}_test.go    # test files live next to the code they test
├── scripts/
│   ├── variables.mk          # computed build variables (VERSION, REVISION, etc.)
│   └── go.mk                 # Go-specific Makefile targets (build, test, vet, clean)
├── test/                     # integration, function, smoke, or e2e testing;
│   └── fixtures/             # fixture files used by tests;
├── Makefile                  # includes scripts/go.mk; top-level targets only
├── go.mod                    # module github.com/jnovack/{repo}
└── go.sum
```

- `main.go` is **wiring only** — parse flags, instantiate types, start goroutines, block on signal, shut down cleanly.
- All logic lives under `internal/`. No `pkg/` unless we have specific exportable packages, which is very rare.
- Never put a `main.go` at the repository root.

### Project initialization

For new projects, run the scaffolding script to generate standard boilerplate
(Makefile, Dockerfile, GitHub workflows, build-version wiring in `main.go`):

```bash
code/golang/src/init-go-project.sh <application> [--ghcr|--dockerhub] [--no-windows]
```

The script lives in the dotfiles repo at `code/golang/src/`. Do not hand-write
these files — use the script to ensure exact repeatability.

**Build version rules** (enforced in every binary regardless of scaffolding):

- Expose `version`, `buildRFC3339`, `revision` as ldflags (`-X main.version=...`).
- Call `populateBuildMetadataFromBuildInfo()` as the first line of `main()` to
  populate from `debug.ReadBuildInfo()` when ldflags are absent.
- Always include a `--version` flag that logs all three values and exits 0.
- Log all three values at startup via `slog.Info`.

### Cross-platform

#### Windows

When Windows binaries are needed, please apply the following rules:

- Use `filepath.Join()` everywhere — never hardcode `/` as a path separator.
- Use `os.UserHomeDir()` for home directory detection — never expand `~` manually.
- Use `runtime.GOOS` switch for OS-specific defaults when necessary:
  - Windows: hardcoded system path (e.g. `C:\actions-runner`)
  - darwin: `filepath.Join(os.UserHomeDir(), "...")`
  - default (linux + others): absolute path (e.g. `/actions-runner`)
- Strip `\r` before processing any line read from a file.
- Open files with `os.O_RDONLY` — assume other processes may hold write handles.

### Additional Libraries, Modules and Dependencies

- Do not add a module without calling it out explicitly. Run `go mod tidy` after any dependency change.
- Prefer the standard library. The stdlib covers most needs.  Notable exceptions below:
- Pin indirect dependencies by running `go mod tidy` and committing both `go.mod` and `go.sum`.
- Pin build tools (`mockery`, `golangci-lint`, etc.) with the `tool` directive
  in `go.mod` (`go get -tool <module>`, Go 1.24+) and invoke via `go tool <name>` —
  no untracked global installs.

#### Flags

Use [`github.com/jnovack/flag`](https://github.com/jnovack/flag) — drop-in replacement for `flag` with env-var and
config-file support.

```go
fs := flag.NewFlagSetWithEnvPrefix(os.Args[0], "", flag.ExitOnError)
myFlag := fs.String("my-flag", "default", "description")
_ = fs.Parse(os.Args[1:])
```

- Flag `--my-flag` maps to env var `MY_FLAG` automatically (uppercase, dashes → underscores).
- No prefix needed when env vars are unambiguous; use a prefix if the binary shares a namespace with other tools.
- Always include a `--version` flag (see Build Version above).

#### Logging

Use `log/slog` exclusively for services where log files will be parsed by a computer.  For applications designed to run
by a user, please use `rs/zerolog`.

```go
slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l})))
```

- Set level from `--log-level` flag at startup.
- Use structured key-value pairs, not format strings.
- Log level order: `debug`, `info`, `warn`, `error`. slog has no `fatal` level —
  log at `error` and exit explicitly at the entrypoint; zerolog's `Fatal()` is
  acceptable in user-facing applications.

#### Prometheus Exporters

When the application calls for metrics, apply the following rules:

- Register counters and histograms once (in the type that owns the state machine) — do not recreate them per scrape.
- Implement `prometheus.Collector` (`Describe` + `Collect`) for gauges whose label values change at runtime.
- Use `prometheus.NewRegistry()` (not the default registry) for testability.
- Expose `{namespace}_info` as a gauge = 1 with constant labels for static identity (instance identifier, version,
  revision, OS).
- Use `orUnknown(s string) string` helper — never emit empty label values.
- Histogram buckets for duration metrics: `1, 5, 60, 300, 600, 1800, 3600` seconds.
- Metric label cardinality: ensure label sets are bounded (no unbounded dynamic values like timestamps or UUIDs as labels).
- Use `github.com/prometheus/client_golang/prometheus/testutil` (`CollectAndCompare`, `ToFloat64`) for Prometheus metric
  assertions.

### Testing

The language-agnostic rules (negative cases, smallest sufficient level,
regression tests, independence, no brittle timing) live in the Testing
Philosophy section. Go-specific mechanics follow.

#### Test Pyramid — Layer Definitions

| Layer       | Scope                              | Dependencies     | Speed    |
| ----------- | ---------------------------------- | ---------------- | -------- |
| Unit        | Single function/method             | Mocks/stubs only | < 1s     |
| Integration | Multiple components, single binary | Mocks acceptable | < 30s    |
| Functional  | Full feature slice                 | Mocks acceptable | < 2min   |
| Smoke       | Critical path, post-deploy         | Real             | < 5min   |
| E2E         | Full system, user journey          | Real             | Uncapped |

#### Directory Structure + Build Tags

```text
repo/
└── internal/        # unit tests co-located (*_test.go, no build tag)

repo/test/
├── integration/     # //go:build integration
├── functional/      # //go:build functional
├── smoke/           # //go:build smoke
└── e2e/             # //go:build e2e
```

Every non-unit test file must carry its corresponding build tag as the first
line, e.g.:

```go
//go:build integration
```

Unit tests carry no build tag — they run with plain `go test ./...`.

#### Running Tests

```bash
go test ./...                         # unit only
go test -tags=integration ./...       # integration
go test -tags=functional ./...        # functional
go test -tags=smoke ./...             # smoke
go test -tags=e2e ./...               # e2e
go test -race ./...                   # always run for shared-state code
```

#### What to Write Unprompted

- **Unit:** always — for every function with logic
- **Integration:** always — for any code crossing a component boundary

Do not write functional, smoke, or e2e tests unless explicitly asked.

#### Unit Test Rules

- Table-driven throughout: `[]struct{ name, input, want }`
- Group related cases under a single `t.Run` loop
- Use `t.TempDir()` for filesystem tests — never hardcode `/tmp/`
- Use `t.Context()` (Go 1.24+) when a test needs a context — it is
  canceled automatically when the test ends; do not use `context.Background()`
  in tests on modern toolchains
- Fixture files under `test/fixtures/` — read with
  `os.ReadFile(filepath.Join("..", "..", "test", "fixtures", "..."))`
  relative to the test package (every path segment its own argument —
  no embedded `/`, per the Windows rules above)
- Always test Windows line endings (CRLF) for any log/file parsing code
- Coverage targets: 85%+ on core logic; error paths and OS-conditional
  branches are acceptable gaps

#### Integration Test Rules

- Live in `integration/` with `//go:build integration`
- Interface mocks are acceptable — prefer them over spinning real infra
- One `TestMain` per package for setup/teardown
- No shared mutable state between test cases
- Must clean up all resources regardless of pass/fail — use `t.Cleanup()`

#### Functional Test Rules (when asked)

- Live in `functional/` with `//go:build functional`
- Test complete feature slices, not implementation details
- Mocks acceptable for external services; real for internal components
- Named after the user-facing behaviour: `TestUserCanResetPassword`
- Ask to add or update function tests when behavior is implemented in a pure
  function, parser, formatter, validator, mapper, or other isolated module
  with meaningful branching or edge cases.

#### Smoke Test Rules (when asked)

- Live in `smoke/` with `//go:build smoke`
- Real dependencies only — no mocks
- Designed to run post-deploy against a live environment
- Must be read-only — no mutations to production state
- Fail fast: first failure aborts the suite
- Ask to add or update smoke tests when a change affects a user-visible flow,
  app startup, wiring between modules, routing, IPC, configuration loading, or
  other integration points where basic execution must be verified.

#### E2E Test Rules (when asked)

- Live in `e2e/` with `//go:build e2e`
- Real dependencies only — full system under test
- Environment config via environment variables only — no hardcoded URLs
- Idempotent: safe to run multiple times without manual cleanup
- Document required environment setup in `e2e/README.md`
- Ask to add or update end-to-end tests only when the change affects a critical
  user journey, cross-process behavior, regression-prone workflow, or a bug that
  can only be reliably proven through full-system execution.

#### Mocks

- Generate with `mockery` or hand-roll against interfaces — no monkey patching
- Mock files live alongside the interface they mock: `mock_<name>.go`
- Never mock types you don't own — wrap them behind an interface first
- Never fetch live external services to generate mocks without explicit approval.
- When approved, record the full response — headers, metadata, and body — to
  `test/fixtures/<service>/`. Recorded responses are the source of truth
  for all future mocks; do not hit the live service again if a fixture exists.

#### Shared Test Helpers

- Live in `internal/testutil/` — importable across all layers
- No production logic in testutil — helpers only
- Reuse `testutil` helpers by default instead of duplicating setup logic in
  individual tests.
- Prefer `testcontainers` for integration boundaries that depend on real
  database or service behavior.
- Do not mock external systems when a small, reliable container-backed test
  would better prove correctness.
<!-- END TEMPLATES -->

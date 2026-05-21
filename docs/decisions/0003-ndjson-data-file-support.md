# ADR 0003: NDJSON data file support with transparent format detection

- Status: Accepted
- Date: 2026-05-20

## Context

`es-bulk-loader` accepts `-data` as a JSON array (`[{...}, {...}]`). Large
datasets — such as the 550 MB Scryfall cards export — must be fully parsed
before any document is processed because the array bounds are unknown until
EOF. The format also precludes line-by-line pipe streaming.

NDJSON (one JSON object per line, no enclosing array) is the standard format
for bulk data interchange and for Elasticsearch's own bulk API wire format. It
enables streaming decode with O(1) working memory per document.

The goal is to accept both formats in the same binary with no new flags and no
behavioral change for existing JSON array files.

## Findings

- `json.Decoder` handles NDJSON natively: each `Decode` call reads one
  top-level value. No per-line `bufio.Scanner` + `json.Unmarshal` is needed.
- The first non-whitespace byte distinguishes the two formats: `[` for JSON
  array, `{` for NDJSON. This is unambiguous; any other byte is a malformed
  file.
- The current count pass for JSON array files does a full JSON decode of every
  document just to count them. A byte-level `bufio.Scanner` line count for
  NDJSON is measurably cheaper on large files (~200 ms on 550 MB) and yields
  the same progress-reporting capability.
- `os.File` supports `Seek`; `os.Stdin` and pipes do not. The two-pass design
  only works on named files.
- `bulkInsert` is format-agnostic and requires no changes.

## Alternatives Considered

### Option 1: Transparent detection from first non-whitespace byte (no new flag)

Accepted. Format is inferred at runtime from the file content. No `Options`
field changes, no new CLI flag, no combinatorial validation. Operators get both
formats for free in any existing integration that already passes `-data`.

### Option 2: Explicit `-format` or `-ndjson` flag

Rejected. Forces callers to know and specify the format, adds a validation
case, and creates a mismatch footgun (`-format ndjson -data array.json`). This
repo's style is to normalize input rather than require brittle caller
configuration (see `buildCreateIndexBody` accepting wrapped or raw JSON for the
same reason).

### Option 3: Single-pass NDJSON with unknown total (Option B in spec)

Rejected for the initial implementation. The line-count pre-pass costs one
sequential file read (~200 ms on 550 MB) and buys accurate progress logging
("N/M inserted") identical to the JSON array path. The UX regression of "total
unknown" is not worth the saved pass. Revisit if pipe support is added, since
pipes cannot seek.

### Option 4: bufio.Scanner per line + json.Unmarshal

Rejected. `json.Decoder` already handles multi-object streams natively. A
per-line Scanner + Unmarshal adds a copy per line, fails silently on
multi-line JSON objects, and is strictly worse. No reason to use it when
`Decoder.More` / `Decoder.Decode` is the idiomatic Go approach.

## Decision

Add transparent NDJSON support by:

1. **`peekFirstByte(path string) (byte, error)`** — opens the file, skips
   leading whitespace, returns the first meaningful byte, leaves the file
   closed. Used as a format dispatcher.
2. **`countNDJSONLines(path string) (int, error)`** — byte-level
   `bufio.Scanner` line count (skipping empty lines) with a 10 MB scanner
   buffer to handle large ES documents.
3. **`loadJSONArray`** — the existing two-pass logic extracted verbatim from
   the `requiresDataFile()` block.
4. **`loadNDJSON`** — line-count pass then single `json.Decoder` streaming
   pass, same batch/flush logic as `loadJSONArray`.
5. A **dispatcher** inside `requiresDataFile()` that calls `peekFirstByte` and
   routes to `loadJSONArray` or `loadNDJSON`; `fatal()` for any other byte.

`overallStart`, `succeededTotal`, `failedTotal`, and the final summary log
remain in the outer scope so the bulk load summary is format-independent.

## Consequences

### Positive

- Existing JSON array integrations are unchanged: same code path, same logs.
- NDJSON files stream with O(1) working memory per document.
- The NDJSON line-count pass is cheaper than the JSON array count pass.
- No new flags, no new `Options` fields, no migration cost for callers.
- `peekFirstByte` and `countNDJSONLines` are small, independently testable
  pure functions.

### Tradeoffs

- NDJSON requires two file opens (line-count + decode), same as JSON array.
  Single-pass with unknown total is deferred.
- Pipe / stdin support is explicitly out of scope: two seeks are required.
  Callers that stream from a pipe must write to a temp file first.
- The dispatcher adds one small branch to an already long `Run` function.
  Full extraction of the data-load block into its own function is left for a
  follow-on refactor.

## What Replaces It

The hard-coded `tok != json.Delim('[')` guard at loader.go:825 and the
`fatal("Data file must be a JSON array")` message are replaced by the
`peekFirstByte` dispatcher. JSON array files continue to use the identical
decode path; they are not affected.

## Revisit Criteria

Revisit if pipe/stdin support is added (single-pass with `total = 0` or
full-stream buffering required), or if compressed input (`.gz`, `.zst`) is
added (detection byte changes, seek semantics change).

## References

- [SPEC.ndjson-support.md](../../SPEC.ndjson-support.md)
- [pkg/loader/loader.go](../../pkg/loader/loader.go)
- [pkg/loader/main_test.go](../../pkg/loader/main_test.go)
- [examples/cards/cards.json](../../examples/cards/cards.json)

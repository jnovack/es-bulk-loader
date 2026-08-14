# Spec: NDJSON Data File Support

## Problem

`es-bulk-loader` requires the `-data` file to be a JSON array (`[{...}, {...}]`). This
is a poor fit for large datasets. A 550 MB Scryfall cards export as a single JSON array
must be fully parsed before the first document is processed, and the file cannot be
streamed incrementally from a pipe. NDJSON (one JSON object per line, no enclosing
array) is the industry-standard format for bulk data interchange precisely because it
enables line-by-line streaming with O(1) memory per document.

The goal is to make `es-bulk-loader` accept both formats transparently, with no new
flags and no change to existing behavior for JSON array files.

---

## Background: Current Behavior

**File:** `pkg/loader/loader.go` lines 819–895

The current data-file path is a two-pass approach:

**Pass 1 — count (lines 829–838):** Opens file, creates a `json.Decoder`, reads the
opening `[` token (aborts with `fatal()` if not present), then decodes every document
object just to count them. `total` is used solely for progress reporting.

**Seek (lines 840–847):** Calls `f.Seek(0, 0)` to rewind the file, re-creates the
decoder, and re-consumes the opening `[` token.

**Pass 2 — load (lines 849–895):** Streams documents into `[]map[string]interface{}`
batches of `*batchSize`, calling `bulkInsert` for each full batch and once more for
the final partial batch.

`bulkInsert` (lines 3017–3124) converts each batch to ES bulk-API NDJSON internally
(`action-meta\ndoc\n` pairs) and does not care about source format.

---

## Design Decisions

### Format Detection

Detect format by peeking the first non-whitespace byte of the file **before** creating
a decoder. No new flag or option field is needed.

| First non-whitespace byte | Format     |
| ------------------------- | ---------- |
| `[`                       | JSON array (existing path, unchanged) |
| `{`                       | NDJSON     |
| anything else             | `fatal()` with a clear message |

Implementation: open the file, use `bufio.Reader.ReadByte()` in a skip-whitespace
loop, then `UnreadByte()` so the byte is available to the decoder.

### Progress Reporting for NDJSON

`bulkInsert` receives `inserted int` and `total int`. For JSON array, `total` is known
from the count pass. For NDJSON, there are two viable approaches:

**Option A — cheap line-count pre-pass (recommended):**
After detection, seek back to 0, count non-empty lines with `bufio.Scanner` (byte-level
only, no JSON parsing), then seek to 0 again and stream-decode. This is the same
two-pass structure as today but the first pass is O(n bytes) with no allocations
instead of O(n documents) with full JSON decode. On a 550 MB file this is
measurably faster than the current JSON array count pass.

**Option B — single pass, unknown total:**
Stream without pre-counting. Pass `total = 0` to `bulkInsert`. Progress logs show
"N inserted (total unknown)" instead of "N/M". Simpler to implement, slightly worse
operator UX.

Recommendation: **Option A**. The line-count pass is cheap enough (~200 ms on
550 MB at typical disk speeds) that the UX benefit of accurate progress is worth it.
A helper `countNDJSONLines(path string) (int, error)` isolates the logic and is
independently testable.

### Single-Pass NDJSON Decode

After the line-count pass and seek, decode with `json.Decoder` line by line:

```go
dec := json.NewDecoder(bufio.NewReaderSize(f, 1<<20))
for dec.More() {
    var doc map[string]interface{}
    if err := dec.Decode(&doc); err != nil {
        fatal().Err(err).Msg("Error decoding NDJSON document")
    }
    // append to batch, flush when full — same logic as JSON array path
}
```

`json.Decoder` handles NDJSON natively: it reads one top-level value per `Decode`
call. No per-line `bufio.Scanner` + `json.Unmarshal` is needed.

### No New Option Fields

`Options.DataFile` remains a `string` path. Format is inferred at runtime. No
`DataFileFormat` or `NDJSONMode` field is added. This keeps the API surface stable
and avoids a combinatorial explosion in validation.

### Stdin / Pipe Support (Out of Scope)

The line-count pre-pass requires two seeks. Named files support `Seek`; `os.Stdin`
and pipes do not. Pipe support would require buffering the entire stream or dropping
the pre-count (Option B). This is explicitly out of scope for this change. If the
data source is a pipe, the caller must write to a temp file first (which callers like
moxfall already do via the cache layer). A future spec can address pipe support.

---

## Affected Files

| File | Change |
| ---- | ------ |
| `pkg/loader/loader.go` | Add `peekFirstByte`, `countNDJSONLines` helpers; refactor data-file section into `loadJSONArray` and `loadNDJSON` sub-functions called by a dispatcher |
| `pkg/loader/main_test.go` | Add `writeBulkNDJSONFixture` helper; add test cases for all existing data-file tests duplicated for NDJSON input |
| `README.md` | Update data file format section to document both formats |
| `examples/cards/cards.ndjson` | Add an NDJSON equivalent of `examples/cards/cards.json` for documentation and manual testing |

---

## Detailed Code Changes

### `pkg/loader/loader.go`

#### 1. New helper: `peekFirstByte`

```go
// peekFirstByte opens path and returns the first non-whitespace byte without
// consuming it. Used to detect JSON array vs NDJSON format.
func peekFirstByte(path string) (byte, error) {
    f, err := os.Open(path)
    if err != nil {
        return 0, err
    }
    defer f.Close()
    br := bufio.NewReader(f)
    for {
        b, err := br.ReadByte()
        if err != nil {
            return 0, err
        }
        if b != ' ' && b != '\t' && b != '\r' && b != '\n' {
            return b, nil
        }
    }
}
```

#### 2. New helper: `countNDJSONLines`

```go
// countNDJSONLines counts non-empty lines in path. Used to obtain a document
// total for progress reporting before the NDJSON decode pass.
func countNDJSONLines(path string) (int, error) {
    f, err := os.Open(path)
    if err != nil {
        return 0, err
    }
    defer f.Close()
    sc := bufio.NewScanner(f)
    sc.Buffer(make([]byte, 1<<20), 10<<20) // 10 MB max line (large ES docs)
    var n int
    for sc.Scan() {
        if len(bytes.TrimSpace(sc.Bytes())) > 0 {
            n++
        }
    }
    return n, sc.Err()
}
```

#### 3. Refactor data-file section

Replace the monolithic block at lines 807–895 with a dispatcher:

```go
if action.requiresDataFile() {
    // ... existing mapping preflight block (unchanged) ...

    first, err := peekFirstByte(*dataFile)
    if err != nil {
        fatal().Err(err).Msg("Cannot read data file")
    }
    switch first {
    case '[':
        loadJSONArray(ctx, es, *dataFile, writeIndex, *batchSize,
            *bulkRetryAttempts, *bulkRetryBackoffBase, *bulkRetryBackoffMax, *idField,
            &succeededTotal, &failedTotal)
    case '{':
        loadNDJSON(ctx, es, *dataFile, writeIndex, *batchSize,
            *bulkRetryAttempts, *bulkRetryBackoffBase, *bulkRetryBackoffMax, *idField,
            &succeededTotal, &failedTotal)
    default:
        fatal().Msgf("Data file first byte %q is neither '[' (JSON array) nor '{' (NDJSON)", first)
    }
}
```

`loadJSONArray` is the existing two-pass logic extracted verbatim.

`loadNDJSON` is the new function:

```go
func loadNDJSON(
    ctx context.Context, es *elasticsearch.Client,
    dataFile, index string, batchSize int,
    retryAttempts int, retryBackoffBase, retryBackoffMax time.Duration,
    idField string,
    succeededTotal, failedTotal *int,
) {
    total, err := countNDJSONLines(dataFile)
    if err != nil {
        fatal().Err(err).Msg("Error counting NDJSON lines")
    }

    f, err := os.Open(dataFile)
    if err != nil {
        fatal().Err(err).Msg("Error opening NDJSON data file")
    }
    defer f.Close()

    dec := json.NewDecoder(bufio.NewReaderSize(f, 1<<20))
    batch := make([]map[string]interface{}, 0, batchSize)
    processed := 0

    for dec.More() {
        var doc map[string]interface{}
        if err := dec.Decode(&doc); err != nil {
            fatal().Err(err).Msg("Error decoding NDJSON document")
        }
        batch = append(batch, doc)
        if len(batch) == batchSize {
            result := bulkInsert(ctx, es, index, batch,
                processed+len(batch), total,
                retryAttempts, retryBackoffBase, retryBackoffMax, idField)
            *succeededTotal += result.Succeeded
            *failedTotal += result.Failed
            processed += len(batch)
            batch = batch[:0]
        }
    }

    // Final partial batch.
    if len(batch) > 0 {
        result := bulkInsert(ctx, es, index, batch,
            processed+len(batch), total,
            retryAttempts, retryBackoffBase, retryBackoffMax, idField)
        *succeededTotal += result.Succeeded
        *failedTotal += result.Failed
    }
}
```

Note: `succeededTotal` and `failedTotal` accumulation, the `overallStart` timing, and
the final summary log after both functions must remain in the outer scope. The
extraction should not change observable log output.

### `pkg/loader/main_test.go`

#### New fixture helper

```go
// writeBulkNDJSONFixture writes a minimal NDJSON data file for tests.
// Mirrors writeBulkDataFixture but in NDJSON format.
func writeBulkNDJSONFixture(t *testing.T) string {
    t.Helper()
    path := filepath.Join(t.TempDir(), "bulk.ndjson")
    if err := os.WriteFile(path, []byte("{\"id\":\"1\",\"name\":\"card\"}\n"), 0o644); err != nil {
        t.Fatalf("write NDJSON bulk fixture: %v", err)
    }
    return path
}
```

#### New test cases (mirror existing data-file tests)

Each existing test that calls `writeBulkDataFixture` must gain a parallel NDJSON
variant. Specifically:

- `TestRunRetriesBulkOnRetryableStatus` → `TestRunNDJSONRetriesBulkOnRetryableStatus`
- `TestRunRetriesBulkOnTransportFailure` → `TestRunNDJSONRetriesBulkOnTransportFailure`
- `TestRunDoesNotRetryBulkOnBadRequest` → `TestRunNDJSONDoesNotRetryBulkOnBadRequest`
- `TestRunExhaustsRetriesOnRetryableStatus` → `TestRunNDJSONExhaustsRetriesOnRetryableStatus`
- `TestRunRetryBackoffCapsAtConfiguredMax` → `TestRunNDJSONRetryBackoffCapsAtConfiguredMax`
- `TestRunAliasFirstCreateUpsertsTransformsAfterAliasUpdate` → NDJSON variant
- `TestRunNonAliasCreateUpsertsTransformsAfterBulkLoad` → NDJSON variant

Additionally, add targeted unit tests for the new helpers:

```go
// TestPeekFirstByteJSONArray confirms '[' detected for JSON array file.
// TestPeekFirstByteNDJSON confirms '{' detected for NDJSON file.
// TestPeekFirstByteSkipsLeadingWhitespace confirms detection works with leading
//   spaces/newlines before the first real character.
// TestPeekFirstByteEmptyFile confirms error returned for empty file.
// TestCountNDJSONLines confirms correct count for multi-line, empty-line,
//   and single-line files.
// TestLoadNDJSONProducesCorrectDocuments confirms round-trip: write NDJSON,
//   load against mock ES, verify bulk bodies received match input documents.
```

The `TestLoadNDJSONProducesCorrectDocuments` test should use the same mock ES
server pattern already used by `TestRunNonAliasCreateUpsertsTransformsAfterBulkLoad`.

---

## README.md Changes

In the "Data File" section (currently shows only JSON array example), add:

```markdown
### Data File Formats

Two formats are accepted. Format is detected automatically from the first
non-whitespace byte; no flag is required.

**JSON array** (original format):
\`\`\`json
[
  { "id": "1", "name": "Alice" },
  { "id": "2", "name": "Bob" }
]
\`\`\`

**NDJSON** (newline-delimited JSON, one object per line):
\`\`\`
{"id":"1","name":"Alice"}
{"id":"2","name":"Bob"}
\`\`\`

NDJSON is preferred for large datasets. It enables streaming decode with O(1)
memory per document and is the canonical format for ES bulk-API interchange.
```

---

## Out of Scope

- Pipe / stdin support (requires buffering or dropping progress totals — separate spec)
- NDJSON output from the loader (not relevant; ES bulk API already uses NDJSON internally)
- Compressed input (`.gz`, `.zst`) — separate spec
- Partial-line error recovery (malformed NDJSON line skipping) — caller's responsibility

---

## Testing Checklist Before Merge

- [ ] All existing JSON array tests pass unchanged
- [ ] All new NDJSON mirror tests pass
- [ ] `peekFirstByte` unit tests cover whitespace-skip and empty-file cases
- [ ] `countNDJSONLines` unit tests cover 0-line, 1-line, multi-line, blank-line files
- [ ] Manual smoke test: `es-bulk-loader -data cards.ndjson ...` against a live ES instance
- [ ] Manual smoke test: existing `examples/cards/cards.json` still works
- [ ] `go vet ./...` and `go test ./...` clean

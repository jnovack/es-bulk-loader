# Implementation Plan: NDJSON Data File Support

Reference spec: [SPEC.ndjson-support.md](SPEC.ndjson-support.md)
Reference ADR: [docs/decisions/0003-ndjson-data-file-support.md](docs/decisions/0003-ndjson-data-file-support.md)

This plan is written for an agent with no prior context. Complete steps in order.
Each step states exactly what to change, where, and what the result should look like.
Run `go build ./...` and `go test ./...` before declaring the task complete.

---

## Step 0 — Orientation (read-only)

Read these sections before writing any code:

- `pkg/loader/loader.go` lines 807–914 — the full `requiresDataFile()` block
  you will refactor.
- `pkg/loader/loader.go` lines 3016–3178 — `bulkInsert`. Understand its
  signature and `bulkInsertResult` struct. **Do not modify it.**
- `pkg/loader/main_test.go` lines 1082–1308 — the five retry/backoff tests
  that call `writeBulkDataFixture`, plus that helper's definition at line 1300.
- `pkg/loader/main_test.go` lines 617–865 — `TestRunAliasFirstCreate...` and
  `TestRunNonAliasCreate...`. These are the two integration-style tests that
  exercise the full data-load path with a mock ES server.
- `examples/cards/cards.json` — the existing fixture you will mirror as NDJSON.

---

## Step 1 — Add helper functions to `pkg/loader/loader.go`

Add the two new helpers **before** the `bulkInsert` function (around line 3016).
Both are package-private (lowercase). Both must have GoDoc comments per
`AGENTS.md`.

### 1a. `peekFirstByte`

```go
// peekFirstByte opens path and returns the first non-whitespace byte without
// consuming it. The file is opened and closed internally; the caller does not
// hold an open file handle after this call. Used to detect JSON array vs NDJSON
// format before creating a decoder.
// adr/0003-ndjson-data-file-support.md
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

### 1b. `countNDJSONLines`

```go
// countNDJSONLines counts non-empty lines in path without parsing JSON.
// Used to obtain a document total for progress reporting before the NDJSON
// decode pass. The 10 MB scanner buffer accommodates large Elasticsearch
// documents that would otherwise exceed the default 64 KB scanner limit.
// adr/0003-ndjson-data-file-support.md
func countNDJSONLines(path string) (int, error) {
    f, err := os.Open(path)
    if err != nil {
        return 0, err
    }
    defer f.Close()
    sc := bufio.NewScanner(f)
    sc.Buffer(make([]byte, 1<<20), 10<<20)
    var n int
    for sc.Scan() {
        if len(bytes.TrimSpace(sc.Bytes())) > 0 {
            n++
        }
    }
    return n, sc.Err()
}
```

**Import check:** `bufio` and `bytes` are already imported in `loader.go`.
Verify with `grep -n '"bufio"\|"bytes"' pkg/loader/loader.go`. If missing, add
them to the import block.

---

## Step 2 — Extract `loadJSONArray` from the existing `requiresDataFile()` block

The current block at lines 819–895 is a monolithic inline section. Extract it
into a standalone function. This is a pure refactor — **do not change any
logic**, variable names, or log messages.

### 2a. New function signature

```go
// loadJSONArray loads documents from a JSON array file into index using the
// ES bulk API. It performs two passes: a count pass for progress reporting and
// a decode pass that streams documents in batches of batchSize.
// adr/0003-ndjson-data-file-support.md
func loadJSONArray(
    ctx context.Context,
    es *elasticsearch.Client,
    dataFile, index string,
    batchSize int,
    retryAttempts int,
    retryBackoffBase, retryBackoffMax time.Duration,
    idField string,
    succeededTotal, failedTotal *int,
) {
    // Move the existing lines 819–895 here verbatim.
    // The only changes are:
    //   - Replace *dataFile with dataFile (it is now a value, not a pointer)
    //   - Replace *batchSize with batchSize
    //   - Replace *bulkRetryAttempts with retryAttempts
    //   - Replace *bulkRetryBackoffBase with retryBackoffBase
    //   - Replace *bulkRetryBackoffMax with retryBackoffMax
    //   - Replace *idField with idField
    //   - Replace "succeededTotal +=" with "*succeededTotal +="
    //   - Replace "failedTotal +=" with "*failedTotal +="
    //   - Remove the local declarations of succeededTotal and failedTotal
    //     (they are passed in as pointers)
    // The overallStart, batch, and processed variables stay local to this
    // function. Do NOT move overallStart, the final log, or result assignment
    // out — see Step 3 for how the outer scope changes.
}
```

### 2b. Exact substitution table for variable names

| Old (inline, pointer-dereferenced) | New (function param, value) |
| --- | --- |
| `*dataFile` | `dataFile` |
| `*batchSize` | `batchSize` |
| `*bulkRetryAttempts` | `retryAttempts` |
| `*bulkRetryBackoffBase` | `retryBackoffBase` |
| `*bulkRetryBackoffMax` | `retryBackoffMax` |
| `*idField` | `idField` |
| `succeededTotal +=` | `*succeededTotal +=` |
| `failedTotal +=` | `*failedTotal +=` |

**Important:** The `overallStart := time.Now()` line currently lives at line
849. When you extract `loadJSONArray`, keep it inside the function so the
timing covers only the JSON array decode pass. The outer scope will declare its
own `overallStart` before dispatching (see Step 3).

**Do not move** `result.DocumentsProcessed`, `result.DocumentsSucceeded`,
`result.DocumentsFailed` assignments into the function. Leave them in the outer
scope (Step 3).

---

## Step 3 — Add `loadNDJSON`

Add the new function immediately after `loadJSONArray`. Same signature shape,
same GoDoc style.

```go
// loadNDJSON loads documents from an NDJSON file (one JSON object per line)
// into index using the ES bulk API. It performs a cheap byte-level line-count
// pass for progress reporting, then streams documents with json.Decoder in
// batches of batchSize.
// adr/0003-ndjson-data-file-support.md
func loadNDJSON(
    ctx context.Context,
    es *elasticsearch.Client,
    dataFile, index string,
    batchSize int,
    retryAttempts int,
    retryBackoffBase, retryBackoffMax time.Duration,
    idField string,
    succeededTotal, failedTotal *int,
) {
    total, err := countNDJSONLines(dataFile)
    if err != nil {
        fatal().Err(err).Msg("Error counting NDJSON lines")
    }
    log.Debug().Str("data_file", dataFile).Int("total", total).Msg("NDJSON line count complete")

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
            batchResult := bulkInsert(
                ctx, es, index, batch,
                processed+len(batch), total,
                retryAttempts, retryBackoffBase, retryBackoffMax, idField,
            )
            processed += len(batch)
            *succeededTotal += batchResult.Succeeded
            *failedTotal += batchResult.Failed
            batch = batch[:0]
        }
    }

    if len(batch) > 0 {
        batchResult := bulkInsert(
            ctx, es, index, batch,
            processed+len(batch), total,
            retryAttempts, retryBackoffBase, retryBackoffMax, idField,
        )
        processed += len(batch)
        *succeededTotal += batchResult.Succeeded
        *failedTotal += batchResult.Failed
    }
}
```

---

## Step 4 — Replace the inline block in `requiresDataFile()` with the dispatcher

The section at `loader.go` lines 817–914 currently looks like this after Step 2:

```go
log.Info().Msg("Starting bulk insert")
// adr comment (already added)
f, err := os.Open(*dataFile)
... (old inline code) ...
```

Replace **everything from** `log.Info().Msg("Starting bulk insert")` **through**
`result.DocumentsFailed = failedTotal` with the following dispatcher:

```go
log.Info().Msg("Starting bulk insert")

// adr/0003-ndjson-data-file-support.md — format detected from first non-whitespace byte.
first, err := peekFirstByte(*dataFile)
if err != nil {
    fatal().Err(err).Msg("Cannot read data file")
}

overallStart := time.Now()
succeededTotal := 0
failedTotal := 0
processed := 0

switch first {
case '[':
    loadJSONArray(ctx, es, *dataFile, writeIndex, *batchSize,
        *bulkRetryAttempts, *bulkRetryBackoffBase, *bulkRetryBackoffMax, *idField,
        &succeededTotal, &failedTotal)
    // processed is computed inside loadJSONArray; retrieve from succeededTotal+failedTotal
case '{':
    loadNDJSON(ctx, es, *dataFile, writeIndex, *batchSize,
        *bulkRetryAttempts, *bulkRetryBackoffBase, *bulkRetryBackoffMax, *idField,
        &succeededTotal, &failedTotal)
default:
    fatal().Msgf("Data file first byte %q is neither '[' (JSON array) nor '{' (NDJSON)", first)
}

overallDuration := time.Since(overallStart)
log.Info().
    Int("succeeded", succeededTotal).
    Int("failed", failedTotal).
    Float64("total_time", overallDuration.Seconds()).
    Msg("Bulk load completed")

if failedTotal > 0 {
    log.Warn().
        Int("failed", failedTotal).
        Msg("Bulk load completed with failed items")
}

result.DocumentsSucceeded = succeededTotal
result.DocumentsFailed = failedTotal
```

**Note on `processed` and `result.DocumentsProcessed`:** The current code
tracks `processed` locally and sets `result.DocumentsProcessed = processed`.
You have two options:

- **Option A (preferred):** Add `processed *int` to both `loadJSONArray` and
  `loadNDJSON` signatures and accumulate into it (same pattern as
  `succeededTotal`/`failedTotal`). Then set `result.DocumentsProcessed =
  processed` in the outer scope.
- **Option B (acceptable):** Compute `processed = succeededTotal + failedTotal`
  in the outer scope after both functions return.

Choose whichever keeps the code simplest. The observable behavior (`processed`
logged and stored in `result`) must be preserved.

---

## Step 5 — Add the NDJSON example fixture

Create `examples/cards/cards.ndjson` containing the same documents as
`examples/cards/cards.json` but in NDJSON format (one JSON object per line, no
enclosing array, no trailing comma). Use `jq` to generate it:

```bash
jq -c '.[]' examples/cards/cards.json > examples/cards/cards.ndjson
```

Verify the output has the same number of lines as objects in `cards.json`.

---

## Step 6 — Add tests to `pkg/loader/main_test.go`

### 6a. New fixture helper

Add immediately after `writeBulkDataFixture` (around line 1308):

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

### 6b. Unit tests for new helpers

Add a new `TestPeekFirstByte` table-driven test and a `TestCountNDJSONLines`
table-driven test. Both should be `t.Parallel()`.

**`TestPeekFirstByte` cases:**

| name | file content | expected byte | expect error |
| --- | --- | --- | --- |
| json array | `[{"id":"1"}]` | `'['` | no |
| ndjson | `{"id":"1"}\n` | `'{'` | no |
| leading whitespace before `[` | `  \n[{"id":"1"}]` | `'['` | no |
| leading whitespace before `{` | `\t {"id":"1"}` | `'{'` | no |
| empty file | `` | `0` | yes |

**`TestCountNDJSONLines` cases:**

| name | file content | expected count |
| --- | --- | --- |
| empty | `` | 0 |
| one line | `{"a":1}\n` | 1 |
| two lines | `{"a":1}\n{"b":2}\n` | 2 |
| blank line between docs | `{"a":1}\n\n{"b":2}\n` | 2 |
| no trailing newline | `{"a":1}` | 1 |

### 6c. Mirror the five retry/backoff tests for NDJSON

For each of the following tests, create an NDJSON variant by copying the test,
renaming it, and replacing `writeBulkDataFixture(t)` with
`writeBulkNDJSONFixture(t)`. All other logic is identical.

| Original | NDJSON variant |
| --- | --- |
| `TestRunRetriesBulkOnRetryableStatus` | `TestRunNDJSONRetriesBulkOnRetryableStatus` |
| `TestRunRetriesBulkOnTransportFailure` | `TestRunNDJSONRetriesBulkOnTransportFailure` |
| `TestRunDoesNotRetryBulkOnBadRequest` | `TestRunNDJSONDoesNotRetryBulkOnBadRequest` |
| `TestRunExhaustsRetriesOnRetryableStatus` | `TestRunNDJSONExhaustsRetriesOnRetryableStatus` |
| `TestRunRetryBackoffCapsAtConfiguredMax` | `TestRunNDJSONRetryBackoffCapsAtConfiguredMax` |

### 6d. Mirror the two integration tests for NDJSON

Create NDJSON variants of:

- `TestRunAliasFirstCreateUpsertsTransformsAfterAliasUpdate` →
  `TestRunNDJSONAliasFirstCreateUpsertsTransformsAfterAliasUpdate`
- `TestRunNonAliasCreateUpsertsTransformsAfterBulkLoad` →
  `TestRunNDJSONNonAliasCreateUpsertsTransformsAfterBulkLoad`

In each variant replace `writeBulkDataFixture(t)` with
`writeBulkNDJSONFixture(t)`. Everything else is identical.

### 6e. Round-trip test `TestLoadNDJSONProducesCorrectDocuments`

Write a test that:

1. Writes a 3-document NDJSON temp file with known field values.
2. Creates a mock ES server (use the same `httptest.NewServer` pattern as
   `TestRunNonAliasCreateUpsertsTransformsAfterBulkLoad`) that captures the
   bulk request body.
3. Runs `loader.Run` with `DataFile` pointing at the NDJSON file and
   `BatchSize: 10` (larger than the fixture so all docs land in one batch).
4. Parses the captured bulk body (NDJSON action-meta + doc pairs) and asserts:
   - Three action-meta lines (each `{"index":{...}}`).
   - Three document lines matching the input documents exactly.

---

## Step 7 — Update `README.md`

Locate the "Data File" section (search for `cards.json` or `-data`). Add or
replace the format documentation with:

```markdown
### Data File Formats

Two formats are accepted. Format is detected automatically from the first
non-whitespace byte; no flag is required.

**JSON array** (original format):

```json
[
  { "id": "1", "name": "Alice" },
  { "id": "2", "name": "Bob" }
]
```

**NDJSON** (newline-delimited JSON, one object per line):

```text
{"id":"1","name":"Alice"}
{"id":"2","name":"Bob"}
```

NDJSON is preferred for large datasets. It enables streaming decode with
O(1) memory per document and is the canonical format for ES bulk-API
interchange.
```

---

## Step 8 — Verify

```bash
go vet ./...
go test ./...
```

All existing tests must pass. All new tests must pass. No new compiler
warnings.

If `go test` shows a failure in an existing test you did not touch, check
that the `requiresDataFile()` dispatcher still assigns `result.DocumentsProcessed`,
`result.DocumentsSucceeded`, and `result.DocumentsFailed` correctly.

---

## Checklist

- [ ] `peekFirstByte` added with GoDoc comment and ADR tag
- [ ] `countNDJSONLines` added with GoDoc comment and ADR tag
- [ ] `loadJSONArray` extracted verbatim (zero behavior change)
- [ ] `loadNDJSON` implemented with two-pass (count + decode)
- [ ] Dispatcher in `requiresDataFile()` routes `[` → `loadJSONArray`,
  `{` → `loadNDJSON`, other → `fatal()`
- [ ] `result.DocumentsProcessed/Succeeded/Failed` still set in outer scope
- [ ] `examples/cards/cards.ndjson` created
- [ ] `writeBulkNDJSONFixture` added
- [ ] `TestPeekFirstByte` (5 cases) passing
- [ ] `TestCountNDJSONLines` (5 cases) passing
- [ ] 5 retry/backoff NDJSON mirror tests passing
- [ ] 2 integration NDJSON mirror tests passing
- [ ] `TestLoadNDJSONProducesCorrectDocuments` passing
- [ ] README data-file section updated
- [ ] `go vet ./...` clean
- [ ] `go test ./...` clean

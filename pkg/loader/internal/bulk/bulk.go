// Package bulk loads a data file into one index through the Elasticsearch bulk
// API: format detection (JSON array or NDJSON, ADR 0003), the counting and
// decoding passes, batch requests with retry/backoff, and per-item failure
// accounting. It reports batch progress as internal snapshots and returns
// totals; package loader maps them to its Result and public ProgressEvents.
package bulk

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

const (
	// DefaultRetryAttempts is the total bulk request attempts, including the
	// first, used when the caller passes <= 0.
	DefaultRetryAttempts = 4
	// DefaultRetryBackoffBase is the first retry wait used when the caller
	// passes <= 0.
	DefaultRetryBackoffBase = 500 * time.Millisecond
	// DefaultRetryBackoffMax caps exponential retry waits when the caller
	// passes <= 0.
	DefaultRetryBackoffMax = 5 * time.Second
)

// bulkResponse is the _bulk response body.
type bulkResponse struct {
	Errors bool                          `json:"errors"`
	Items  []map[string]bulkItemResponse `json:"items"`
}

// bulkItemResponse is one item result of a _bulk response.
type bulkItemResponse struct {
	Index  string         `json:"_index"`
	ID     string         `json:"_id"`
	Status int            `json:"status"`
	Error  *bulkItemError `json:"error,omitempty"`
}

// bulkItemError is a failed item's error type and reason.
type bulkItemError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

// ItemFailure is one failed bulk item: the sanitized, bounded response fields
// of a rejected document. Index, ID, Action, Type and Reason each pass
// esresponse.SanitizeText at MaxPublicMessageBytes; Type and Reason are empty
// for a failed item whose response carried a null error. Package loader maps
// it to its public BulkItemFailure.
type ItemFailure struct {
	Index  string
	ID     string
	Action string
	Status int
	Type   string
	Reason string
}

// bulkInsertResult counts one batch's succeeded and failed items and carries
// up to the caller's budget of the failed items, in response order.
type bulkInsertResult struct {
	Succeeded int
	Failed    int
	Sample    []ItemFailure
}

// accumulator is the running total shared by loadJSONArray and loadNDJSON so
// the two loaders cannot diverge. limit is the whole-load sample bound.
type accumulator struct {
	succeeded int
	failed    int
	processed int
	limit     int
	sample    []ItemFailure
}

// budget is how many more failed items the sample may take; never negative.
func (a *accumulator) budget() int {
	return max(0, a.limit-len(a.sample))
}

// add folds one successful batch into the totals. Each batch is added once.
// bulkInsert already honored budget(), so Sample fits the remaining room.
func (a *accumulator) add(r bulkInsertResult) {
	a.succeeded += r.Succeeded
	a.failed += r.Failed
	a.sample = append(a.sample, r.Sample...)
}

// itemErrorText returns a failed item's error type and reason sanitized the
// way the log line and the sample both need them; a null error yields empty
// strings. It is the one place that text is produced.
func itemErrorText(e *bulkItemError) (errorType, errorReason string) {
	if e == nil {
		return "", ""
	}
	return esresponse.SanitizeText(e.Type, esresponse.MaxPublicMessageBytes),
		esresponse.SanitizeText(e.Reason, esresponse.MaxPublicMessageBytes)
}

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
	acc *accumulator,
	seams progress.Seams,
) error {
	start := seams.Now()
	f, err := os.Open(dataFile)
	if err := runlog.Check(ctx, "opening data file", err); err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('[') {
		return runlog.Fail(runlog.FailureEvent(ctx), "Data file must be a JSON array", nil)
	}

	runlog.From(ctx).Debug().Str("data_file", dataFile).Msg("Counting documents in data file")
	total := 0
	for dec.More() {
		var tmp map[string]interface{}
		if err := dec.Decode(&tmp); err != nil {
			return runlog.Fail(runlog.FailureEvent(ctx), "Error counting objects in data file", err)
		}
		total++
	}
	runlog.From(ctx).Debug().Str("data_file", dataFile).Int("total", total).Msg("Document count complete")

	if _, err := f.Seek(0, 0); err != nil {
		return runlog.Fail(runlog.FailureEvent(ctx), "Error rewinding data file", err)
	}
	dec = json.NewDecoder(f)
	_, err = dec.Token()
	if err != nil {
		return runlog.Fail(runlog.FailureEvent(ctx), "Error re-reading data file", err)
	}

	batch := make([]map[string]interface{}, 0, batchSize)
	for dec.More() {
		var doc map[string]interface{}
		if err := dec.Decode(&doc); err != nil {
			return runlog.Fail(runlog.FailureEvent(ctx), "Error decoding object in data file", err)
		}
		batch = append(batch, doc)
		if len(batch) == batchSize {
			batchResult, err := bulkInsert(
				ctx,
				es,
				index,
				batch,
				acc.processed+len(batch),
				total,
				retryAttempts,
				retryBackoffBase,
				retryBackoffMax,
				idField,
				seams.Sleep,
				acc.budget(),
			)
			if err != nil {
				return err
			}
			acc.processed += len(batch)
			reportBatch(seams, index, acc.processed, total, start)
			acc.add(batchResult)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		batchResult, err := bulkInsert(
			ctx,
			es,
			index,
			batch,
			acc.processed+len(batch),
			total,
			retryAttempts,
			retryBackoffBase,
			retryBackoffMax,
			idField,
			seams.Sleep,
			acc.budget(),
		)
		if err != nil {
			return err
		}
		acc.processed += len(batch)
		reportBatch(seams, index, acc.processed, total, start)
		acc.add(batchResult)
	}
	return nil
}

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
	acc *accumulator,
	seams progress.Seams,
) error {
	start := seams.Now()
	total, err := countNDJSONLines(dataFile)
	if err != nil {
		return runlog.Fail(runlog.FailureEvent(ctx), "Error counting NDJSON lines", err)
	}
	runlog.From(ctx).Debug().Str("data_file", dataFile).Int("total", total).Msg("NDJSON line count complete")

	f, err := os.Open(dataFile)
	if err != nil {
		return runlog.Fail(runlog.FailureEvent(ctx), "Error opening NDJSON data file", err)
	}
	defer f.Close()

	dec := json.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	batch := make([]map[string]interface{}, 0, batchSize)

	for dec.More() {
		var doc map[string]interface{}
		if err := dec.Decode(&doc); err != nil {
			return runlog.Fail(runlog.FailureEvent(ctx), "Error decoding NDJSON document", err)
		}
		batch = append(batch, doc)
		if len(batch) == batchSize {
			batchResult, err := bulkInsert(
				ctx,
				es,
				index,
				batch,
				acc.processed+len(batch),
				total,
				retryAttempts,
				retryBackoffBase,
				retryBackoffMax,
				idField,
				seams.Sleep,
				acc.budget(),
			)
			if err != nil {
				return err
			}
			acc.processed += len(batch)
			reportBatch(seams, index, acc.processed, total, start)
			acc.add(batchResult)
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		batchResult, err := bulkInsert(
			ctx,
			es,
			index,
			batch,
			acc.processed+len(batch),
			total,
			retryAttempts,
			retryBackoffBase,
			retryBackoffMax,
			idField,
			seams.Sleep,
			acc.budget(),
		)
		if err != nil {
			return err
		}
		acc.processed += len(batch)
		reportBatch(seams, index, acc.processed, total, start)
		acc.add(batchResult)
	}
	return nil
}

// bulkInsert handles a batch of documents and validates per-item bulk response status.
func bulkInsert(
	ctx context.Context,
	es *elasticsearch.Client,
	index string,
	batch []map[string]interface{},
	inserted, total int,
	retryAttempts int,
	retryBackoffBase, retryBackoffMax time.Duration,
	idField string,
	sleep func(context.Context, time.Duration) error,
	sampleBudget int,
) (bulkInsertResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var buf strings.Builder
	for batchOffset, doc := range batch {
		meta := map[string]map[string]string{"index": {"_index": index}}

		if idField != "" {
			if v, ok := doc[idField]; ok {
				if idStr, ok := v.(string); ok && idStr != "" {
					meta["index"]["_id"] = idStr
				}
			}
		}

		documentNumber := inserted - len(batch) + batchOffset + 1
		metaLine, err := json.Marshal(meta)
		if err != nil {
			return bulkInsertResult{}, runlog.Fail(runlog.FailureEvent(ctx).Int("document", documentNumber), "Unable to serialize bulk action metadata", err)
		}
		docLine, err := json.Marshal(doc)
		if err != nil {
			return bulkInsertResult{}, runlog.Fail(runlog.FailureEvent(ctx).Int("document", documentNumber), "Unable to serialize document for bulk insert", err)
		}
		buf.Write(metaLine)
		buf.WriteByte('\n')
		buf.Write(docLine)
		buf.WriteByte('\n')
	}
	payload := buf.String()
	if retryAttempts <= 0 {
		retryAttempts = DefaultRetryAttempts
	}
	if retryBackoffBase <= 0 {
		retryBackoffBase = DefaultRetryBackoffBase
	}
	if retryBackoffMax <= 0 {
		retryBackoffMax = DefaultRetryBackoffMax
	}
	if retryBackoffMax < retryBackoffBase {
		retryBackoffMax = retryBackoffBase
	}

	var (
		res      *esapi.Response
		err      error
		duration time.Duration
	)
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		startTime := time.Now()
		res, err = es.Bulk(strings.NewReader(payload), es.Bulk.WithContext(ctx))
		duration = time.Since(startTime)

		if err != nil {
			if ctx.Err() != nil {
				return bulkInsertResult{}, runlog.Fail(runlog.FailureEvent(ctx), "Bulk API request failed", ctx.Err())
			}
			if shouldRetryBulkRequest(0, err) && attempt < retryAttempts {
				nextBackoff := computeExponentialBackoff(attempt, retryBackoffBase, retryBackoffMax)
				runlog.From(ctx).Warn().
					Err(err).
					Int("attempt", attempt).
					Int("max_attempts", retryAttempts).
					Str("next_backoff", nextBackoff.String()).
					Msg("Bulk API request failed; retrying")
				if sleepErr := sleep(ctx, nextBackoff); sleepErr != nil {
					return bulkInsertResult{}, runlog.Fail(runlog.FailureEvent(ctx), "Bulk API request failed", sleepErr)
				}
				continue
			}
			return bulkInsertResult{}, runlog.Fail(runlog.FailureEvent(ctx), "Bulk API request failed", err)
		}

		if res.IsError() {
			body, readErr := esresponse.ReadBounded(res.Body)
			_ = res.Body.Close()
			if shouldRetryBulkRequest(res.StatusCode, nil) && attempt < retryAttempts {
				nextBackoff := computeExponentialBackoff(attempt, retryBackoffBase, retryBackoffMax)
				runlog.From(ctx).Warn().
					Int("status_code", res.StatusCode).
					Str("es_error", esresponse.NewRejection(
						"bulk request", "", res.StatusCode, body, readErr,
						esresponse.MaxPublicMessageBytes).Error()).
					Int("attempt", attempt).
					Int("max_attempts", retryAttempts).
					Str("next_backoff", nextBackoff.String()).
					Msg("Bulk API request failed; retrying")
				if sleepErr := sleep(ctx, nextBackoff); sleepErr != nil {
					return bulkInsertResult{}, runlog.Fail(runlog.FailureEvent(ctx), "Bulk API request failed", sleepErr)
				}
				continue
			}
			return bulkInsertResult{}, runlog.Fail(runlog.FailureEvent(ctx).
				Int("status_code", res.StatusCode),
				"Bulk API request failed", esresponse.NewRejection(
					"bulk request", "", res.StatusCode, body, readErr,
					esresponse.MaxPublicMessageBytes))
		}

		if attempt > 1 {
			runlog.From(ctx).Info().
				Int("attempt", attempt).
				Int("max_attempts", retryAttempts).
				Float64("time_taken", duration.Seconds()).
				Msg("Bulk API request retry succeeded")
		}
		break
	}
	defer res.Body.Close()

	var parsed bulkResponse
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return bulkInsertResult{}, runlog.Fail(runlog.FailureEvent(ctx), "Unable to parse bulk response body", err)
	}

	failed := 0
	logged := 0
	var sample []ItemFailure
	for itemIdx, item := range parsed.Items {
		for action, result := range item {
			if result.Status >= 300 || result.Error != nil {
				failed++
				// The sample cap is independent of the ten-per-batch log cap
				// below: sampling must not change counts, retries or logs.
				if len(sample) < sampleBudget {
					errorType, errorReason := itemErrorText(result.Error)
					sample = append(sample, ItemFailure{
						Index:  esresponse.SanitizeText(result.Index, esresponse.MaxPublicMessageBytes),
						ID:     esresponse.SanitizeText(result.ID, esresponse.MaxPublicMessageBytes),
						Action: esresponse.SanitizeText(action, esresponse.MaxPublicMessageBytes),
						Status: result.Status,
						Type:   errorType,
						Reason: errorReason,
					})
				}
				if logged < 10 {
					errorType, errorReason := itemErrorText(result.Error)
					runlog.From(ctx).Error().
						Int("item", itemIdx).
						Str("action", action).
						Str("_index", result.Index).
						Str("_id", result.ID).
						Int("status", result.Status).
						Str("error_type", errorType).
						Str("error_reason", errorReason).
						Msg("Bulk item failed")
					logged++
				}
			}
		}
	}

	if failed > 0 && failed > logged {
		runlog.From(ctx).Error().
			Int("failed_items", failed).
			Int("logged_failures", logged).
			Msg("Additional bulk item failures omitted from logs")
	}

	succeeded := len(batch) - failed
	runlog.From(ctx).Debug().
		Int("inserted", inserted).
		Int("total", total).
		Int("batch_size", len(batch)).
		Int("succeeded", succeeded).
		Int("failed", failed).
		Float64("time_taken", duration.Seconds()).
		Msg("Processed batch")

	return bulkInsertResult{Succeeded: succeeded, Failed: failed, Sample: sample}, nil
}

// shouldRetryBulkRequest centralizes retryability checks for bulk request
// failures, using the shared esresponse classifiers (status first). bulkInsert
// separately stops once its request context has ended, whatever this returns.
func shouldRetryBulkRequest(statusCode int, err error) bool {
	if esresponse.IsRetryableStatus(statusCode) {
		return true
	}
	return esresponse.IsRetryableTransportError(err)
}

// computeExponentialBackoff centralizes capped exponential retry delays.
func computeExponentialBackoff(attempt int, base, max time.Duration) time.Duration {
	if attempt <= 0 || base <= 0 {
		return 0
	}
	backoff := base
	if attempt > 1 {
		backoff = base << (attempt - 1)
	}
	if max > 0 && backoff > max {
		return max
	}
	return backoff
}

// reportBatch reports per-batch bulk progress; shared so the four batch sites stay identical.
func reportBatch(seams progress.Seams, index string, processed, total int, start time.Time) {
	seams.Report(progress.Snapshot{
		Resource: index,
		Done:     int64(processed),
		Total:    int64(total),
		Elapsed:  seams.Now().Sub(start),
	})
}

// Config is one bulk load: the data file, the target index, the batch size,
// the retry policy and the document field whose string value becomes _id.
type Config struct {
	DataFile         string
	Index            string
	BatchSize        int
	RetryAttempts    int
	RetryBackoffBase time.Duration
	RetryBackoffMax  time.Duration
	IDField          string
	// SampleLimit bounds Totals.Sample for the whole load. <= 0 keeps no
	// sample. The caller owns the value (loader.MaxBulkFailureSample), so
	// this package holds no literal bound.
	SampleLimit int
}

// Totals is a completed load: documents processed, succeeded and failed (per
// bulk item), and the time from format detection to the last batch. Sample is
// the first failed items in response order, at most Config.SampleLimit, and
// SampleOmitted is Failed minus len(Sample).
type Totals struct {
	Processed     int
	Succeeded     int
	Failed        int
	Sample        []ItemFailure
	SampleOmitted int
	Elapsed       time.Duration
}

// detectDataFormat returns the data file's format byte, '[' (JSON array)
// or '{' (NDJSON), per ADR 0003. It is the only format check: Run's
// preflight (CheckDataFile) and Load both call it, so unchanged input
// uses the same acceptance rule and failure Ops before and after mutation.
// Load rechecks because the file may change after preflight.
func detectDataFormat(ctx context.Context, path string) (byte, error) {
	first, err := peekFirstByte(path)
	if err != nil {
		return 0, runlog.Fail(runlog.FailureEvent(ctx), "Cannot read data file", err)
	}
	if first != '[' && first != '{' {
		return 0, runlog.Fail(runlog.FailureEvent(ctx), fmt.Sprintf("Data file first byte %q is neither '[' (JSON array) nor '{' (NDJSON)", first), nil)
	}
	return first, nil
}

// CheckDataFile verifies path can be opened and that its first non-whitespace
// byte is '[' or '{', then closes it. Run calls it before destructive steps so
// an unreadable, empty or wrong-format data file cannot half-reset the
// cluster. It does not decode the documents: invalid JSON after an accepted
// first byte still fails inside Load.
func CheckDataFile(ctx context.Context, path string) error {
	_, err := detectDataFormat(ctx, path)
	return err
}

// Load detects the data file's format from its first non-whitespace byte
// (ADR 0003) and loads it into cfg.Index. Batch progress goes to
// seams.OnProgress; retries wait with seams.Sleep. Bulk item failures are
// counted in Totals, not returned. A hard failure returns an operation error
// and zero Totals: the caller must not report partial counts.
func Load(ctx context.Context, es *elasticsearch.Client, cfg Config, seams progress.Seams) (Totals, error) {
	first, err := detectDataFormat(ctx, cfg.DataFile)
	if err != nil {
		return Totals{}, err
	}

	overallStart := seams.Now()
	acc := &accumulator{limit: cfg.SampleLimit}

	// detectDataFormat returned only '[' or '{'. Load still calls it itself
	// because the file can change between preflight and load.
	switch first {
	case '[':
		err = loadJSONArray(
			ctx,
			es,
			cfg.DataFile,
			cfg.Index,
			cfg.BatchSize,
			cfg.RetryAttempts,
			cfg.RetryBackoffBase,
			cfg.RetryBackoffMax,
			cfg.IDField,
			acc,
			seams,
		)
	case '{':
		err = loadNDJSON(
			ctx,
			es,
			cfg.DataFile,
			cfg.Index,
			cfg.BatchSize,
			cfg.RetryAttempts,
			cfg.RetryBackoffBase,
			cfg.RetryBackoffMax,
			cfg.IDField,
			acc,
			seams,
		)
	}
	if err != nil {
		return Totals{}, err
	}

	return Totals{
		Processed:     acc.processed,
		Succeeded:     acc.succeeded,
		Failed:        acc.failed,
		Sample:        acc.sample,
		SampleOmitted: acc.failed - len(acc.sample),
		Elapsed:       seams.Now().Sub(overallStart),
	}, nil
}

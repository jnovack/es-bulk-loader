package transform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

// definition is one entry of the transforms file: the logical source index it
// belongs to and the Elasticsearch transform body.
type definition struct {
	SourceIndex string          `json:"source_index"`
	Body        json.RawMessage `json:"body"`
}

// ResolveForSource validates every declared transform and returns, in file
// order, the bodies of those whose source_index is sourceIndex, so one
// transforms file can serve several loader runs. Errors are plain: the
// caller owns the failure operation.
func ResolveForSource(ctx context.Context, definitions config.Definitions, names []string, sourceIndex string) (config.Definitions, []string, error) {
	if len(names) == 0 {
		return nil, nil, nil
	}

	trimmedSourceIndex := strings.TrimSpace(sourceIndex)
	if trimmedSourceIndex == "" {
		return nil, nil, fmt.Errorf("source index is required")
	}

	selectedDefinitions := make(config.Definitions)
	selectedNames := make([]string, 0)
	for _, name := range names {
		rawDefinition, ok := definitions[name]
		if !ok {
			return nil, nil, fmt.Errorf("transform %q definition missing from parsed transforms file", name)
		}

		var parsed definition
		if err := json.Unmarshal(rawDefinition, &parsed); err != nil {
			return nil, nil, fmt.Errorf("transform %q definition invalid: %w", name, err)
		}

		if strings.TrimSpace(parsed.SourceIndex) == "" {
			return nil, nil, fmt.Errorf("transform %q requires non-empty source_index", name)
		}
		trimmedBody := bytes.TrimSpace(parsed.Body)
		if len(trimmedBody) == 0 || bytes.Equal(trimmedBody, []byte("null")) {
			return nil, nil, fmt.Errorf("transform %q requires non-empty body", name)
		}

		if strings.TrimSpace(parsed.SourceIndex) != trimmedSourceIndex {
			runlog.From(ctx).Trace().
				Str("transform", name).
				Str("source_index", parsed.SourceIndex).
				Str("index", trimmedSourceIndex).
				Msg("Skipping transform that targets a different source index")
			continue
		}

		selectedDefinitions[name] = trimmedBody
		selectedNames = append(selectedNames, name)
	}

	return selectedDefinitions, selectedNames, nil
}

// CreateOrUpdate puts each transform, deleting an existing one first.
func CreateOrUpdate(ctx context.Context, es *elasticsearch.Client, definitions config.Definitions, names []string) error {
	for _, name := range names {
		definition, ok := definitions[name]
		if !ok {
			return runlog.Fail(runlog.FailureEvent(ctx).Str("transform", name), "Transform definition missing from parsed transform file", nil)
		}

		// ES 9.x does not allow updating immutable transform properties
		// (pivot, latest) via TransformUpdateTransform. Delete and recreate
		// instead — StopBestEffort has already stopped the
		// transform, and Delete uses DeleteDestIndex: false to
		// preserve the destination index.
		exists, err := transformExists(ctx, es, name)
		if err != nil {
			return err
		}
		if exists {
			if err := Delete(ctx, es, []string{name}); err != nil {
				return err
			}
		}

		res, err := es.TransformPutTransform(
			strings.NewReader(string(definition)),
			name,
			es.TransformPutTransform.WithContext(ctx),
			es.TransformPutTransform.WithHeader(map[string]string{
				"Content-Type": "application/json",
				"Accept":       "application/json",
			}),
		)
		if err := runlog.Check(ctx, "creating transform", err); err != nil {
			return err
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.IsError() {
			return runlog.Fail(runlog.FailureEvent(ctx).
				Str("transform", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)),
				"Failed to create transform", nil)
		}
		runlog.From(ctx).Info().Str("transform", name).Msg("Transform created")
	}
	return nil
}

// transformExists reports whether the transform exists (GET _transform/<id>).
func transformExists(ctx context.Context, es *elasticsearch.Client, name string) (bool, error) {
	res, err := es.TransformGetTransform(
		es.TransformGetTransform.WithContext(ctx),
		es.TransformGetTransform.WithTransformID(name),
		es.TransformGetTransform.WithAllowNoMatch(true),
	)
	if err := runlog.Check(ctx, "checking if transform exists", err); err != nil {
		return false, err
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if res.IsError() {
		return false, runlog.Fail(runlog.FailureEvent(ctx).
			Str("transform", name).
			Int("status_code", res.StatusCode).
			Str("body", string(body)),
			"Failed to check transform existence", nil)
	}
	var parsed struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(body, &parsed); err == nil {
		return parsed.Count > 0, nil
	}
	return true, nil
}

// StopBestEffort force-stops each named transform before it is re-created. A transform that is already stopped or missing is skipped; any other error stops the loop and fails the run.
func StopBestEffort(ctx context.Context, es *elasticsearch.Client, names []string) error {
	for _, name := range names {
		res, err := es.TransformStopTransform(
			name,
			es.TransformStopTransform.WithContext(ctx),
			es.TransformStopTransform.WithForce(true),
			es.TransformStopTransform.WithWaitForCompletion(true),
			es.TransformStopTransform.WithTimeout(30*time.Second),
			es.TransformStopTransform.WithAllowNoMatch(true),
			es.TransformStopTransform.WithHeader(map[string]string{
				"Accept": "application/json",
			}),
		)
		if err := runlog.Check(ctx, "stopping transform", err); err != nil {
			return err
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusNotFound || stopAlreadyStopped(res.StatusCode, body) {
			runlog.From(ctx).Debug().Str("transform", name).Msg("Transform is not running. Nothing to stop.")
			continue
		}
		if res.IsError() {
			return runlog.Fail(runlog.FailureEvent(ctx).
				Str("transform", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)),
				"Failed to stop transform", nil)
		}
		runlog.From(ctx).Debug().Str("transform", name).Msg("Transform stopped")
	}
	return nil
}

// Delete deletes each transform, keeping its destination index; a missing
// transform is not an error.
func Delete(ctx context.Context, es *elasticsearch.Client, names []string) error {
	for _, name := range names {
		res, err := es.TransformDeleteTransform(
			name,
			es.TransformDeleteTransform.WithContext(ctx),
			es.TransformDeleteTransform.WithForce(true),
			es.TransformDeleteTransform.WithDeleteDestIndex(false),
			es.TransformDeleteTransform.WithHeader(map[string]string{
				"Accept": "application/json",
			}),
		)
		if err := runlog.Check(ctx, "deleting transform", err); err != nil {
			return err
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusNotFound {
			runlog.From(ctx).Debug().Str("transform", name).Msg("Transform does not exist. Nothing to delete.")
			continue
		}
		if res.IsError() {
			return runlog.Fail(runlog.FailureEvent(ctx).
				Str("transform", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)),
				"Failed to delete transform", nil)
		}
		runlog.From(ctx).Info().Str("transform", name).Msg("Transform deleted")
	}
	return nil
}

// Start starts each transform; one that is already running is not an error.
func Start(ctx context.Context, es *elasticsearch.Client, names []string) error {
	for _, name := range names {
		res, err := es.TransformStartTransform(
			name,
			es.TransformStartTransform.WithContext(ctx),
			es.TransformStartTransform.WithTimeout(30*time.Second),
			es.TransformStartTransform.WithHeader(map[string]string{
				"Accept": "application/json",
			}),
		)
		if err := runlog.Check(ctx, "starting transform", err); err != nil {
			return err
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if startAlreadyStarted(res.StatusCode, body) {
			runlog.From(ctx).Info().Str("transform", name).Msg("Transform already running")
			continue
		}
		if res.IsError() {
			return runlog.Fail(runlog.FailureEvent(ctx).
				Str("transform", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)),
				"Failed to start transform", nil)
		}

		runlog.From(ctx).Info().Str("transform", name).Msg("Transform started")
	}
	return nil
}

// stopAlreadyStopped reports a stop rejected because the transform is stopped.
func stopAlreadyStopped(statusCode int, body []byte) bool {
	if statusCode != http.StatusConflict && statusCode != http.StatusBadRequest {
		return false
	}
	lowered := strings.ToLower(string(body))
	return strings.Contains(lowered, "cannot stop transform") && strings.Contains(lowered, "stopped")
}

// startAlreadyStarted reports a start rejected because the transform runs.
func startAlreadyStarted(statusCode int, body []byte) bool {
	if statusCode != http.StatusConflict && statusCode != http.StatusBadRequest {
		return false
	}
	lowered := strings.ToLower(string(body))
	if strings.Contains(lowered, "resource_already_exists_exception") {
		return true
	}
	return strings.Contains(lowered, "already started") || (strings.Contains(lowered, "cannot start transform") && strings.Contains(lowered, "started"))
}

// WaitForCheckpoint blocks until each started transform completes its first
// checkpoint, logging and emitting progress; a failed or timed-out transform fails the run.
//
// A failure emits no Final event for the failing transform: Run's error is
// the single failure signal (see loader.ProgressEvent).
func WaitForCheckpoint(ctx context.Context, es *elasticsearch.Client, definitions config.Definitions, names []string, timeout time.Duration, seams progress.Seams) error {
	for _, name := range names {
		continuous, err := IsContinuous(definitions[name])
		if err != nil {
			return runlog.Fail(runlog.FailureEvent(ctx).Str("transform", name), "Transform wait failed", fmt.Errorf("transform %q: %w", name, err))
		}

		runlog.From(ctx).Info().
			Str("transform", name).
			Bool("continuous", continuous).
			Str("timeout", timeout.String()).
			Msg("Waiting for transform")
		start := seams.Now()
		hb := progress.NewHeartbeat(progress.HeartbeatInterval, seams.Now)

		stats, err := Wait(ctx, statsGetter(es), name, WaitConfig{
			Continuous: continuous,
			Timeout:    timeout,
			Sleep:      seams.Sleep,
			OnPoll: func(s Stats, _ int) {
				elapsed := seams.Now().Sub(start)
				debug := runlog.From(ctx).Debug().
					Str("transform", name).
					Str("state", s.State).
					Int64("checkpoint", s.LastCheckpoint)
				if s.DocsRemaining >= 0 {
					debug = debug.Int64("docs_remaining", s.DocsRemaining)
				}
				if s.TotalDocs >= 0 {
					debug = debug.Int64("total_docs", s.TotalDocs)
				}
				if s.HasPercent {
					debug = debug.Float64("percent_complete", s.PercentComplete)
				}
				if s.DocumentsProcessed >= 0 {
					debug = debug.Int64("documents_processed", s.DocumentsProcessed)
				}
				debug.Msg("Transform in progress")

				seams.Report(progress.Snapshot{
					Resource: name,
					Phase:    s.State,
					Done:     s.DocumentsProcessed,
					Total:    s.TotalDocs,
					Elapsed:  elapsed,
				})

				if hb.Due() {
					runlog.From(ctx).Info().
						Str("transform", name).
						Str("state", s.State).
						Float64("elapsed_s", elapsed.Seconds()).
						Msg("Transform still running")
				}
			},
		})
		if err != nil {
			return runlog.Fail(runlog.FailureEvent(ctx).Str("transform", name), "Transform wait failed", err)
		}

		elapsed := seams.Now().Sub(start)
		done := runlog.From(ctx).Info().
			Str("transform", name).
			Int64("checkpoint", stats.LastCheckpoint)
		// An absent counter is left out of the line rather than shown as 0.
		if stats.DocumentsProcessed >= 0 {
			done = done.Int64("documents_processed", stats.DocumentsProcessed)
		}
		if stats.DocumentsIndexed >= 0 {
			done = done.Int64("documents_indexed", stats.DocumentsIndexed)
		}
		done.Float64("elapsed_s", elapsed.Seconds()).Msg("Transform completed")

		seams.Report(progress.Snapshot{
			Resource: name,
			Phase:    stats.State,
			Done:     stats.DocumentsProcessed,
			Total:    stats.TotalDocs,
			Elapsed:  elapsed,
			Final:    true,
		})
	}
	return nil
}

// ReportStarted reports the single Final snapshot for transforms that were
// started without waiting.
func ReportStarted(seams progress.Seams, names []string) {
	for _, name := range names {
		seams.Report(progress.Snapshot{
			Resource: name,
			Phase:    "started",
			Done:     -1,
			Total:    -1,
			Final:    true,
		})
	}
}

// statsGetter reads GET _transform/<id>/_stats. The context is the
// per-poll one handed over by Wait, so the wait timeout bounds the
// HTTP call.
func statsGetter(es *elasticsearch.Client) Getter {
	return func(pollCtx context.Context, id string) (int, []byte, error) {
		res, err := es.TransformGetTransformStats(
			[]string{id},
			es.TransformGetTransformStats.WithContext(pollCtx),
		)
		if err != nil {
			return 0, nil, err
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return 0, nil, fmt.Errorf("reading _stats response: %w", err)
		}
		return res.StatusCode, body, nil
	}
}

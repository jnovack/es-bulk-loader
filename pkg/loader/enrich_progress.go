package loader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/enrich"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
)

// runEnrichPolicies executes the selected enrich policies one after another.
// A policy that fails is counted and the run continues with the next one;
// emit receives one Final event per policy, whatever way executeEnrichPolicy
// returned, so every early return is covered in one place.
func runEnrichPolicies(ctx context.Context, es *elasticsearch.Client, enrichSelection *enrichFlagValue, declared []string, emit progressEmitter) enrichRunSummary {
	summary := enrichRunSummary{}

	availablePolicies, supported := getEnrichPolicies(ctx, es)
	if !supported {
		return summary
	}
	if len(availablePolicies) == 0 {
		loggerFrom(ctx).Warn().Msg("No enrich policies found; skipping enrich execution")
		return summary
	}

	targets, missing := resolveEnrichTargets(enrichSelection, availablePolicies, declared)
	summary.Selected = append(summary.Selected, targets...)
	summary.Missing = append(summary.Missing, missing...)
	for _, policy := range missing {
		loggerFrom(ctx).Warn().Str("policy", policy).Msg("Enrich policy not found; skipping")
	}

	if len(targets) == 0 {
		loggerFrom(ctx).Warn().Msg("No enrich policies matched the request")
		return summary
	}

	loggerFrom(ctx).Info().
		Int("available", len(availablePolicies)).
		Int("requested", len(targets)+len(missing)).
		Int("selected", len(targets)).
		Msg("Starting enrich policy execution")

	succeeded := 0
	failed := 0
	for i, policy := range targets {
		loggerFrom(ctx).Info().
			Str("policy", policy).
			Int("position", i+1).
			Int("of", len(targets)).
			Msg("Executing enrich policy")

		policyStart := progressNow()
		ok := executeEnrichPolicy(ctx, es, policy, emit)
		final := ProgressEvent{
			Stage:    ProgressStageEnrich,
			Resource: policy,
			Done:     -1,
			Total:    -1,
			Elapsed:  progressNow().Sub(policyStart),
			Final:    true,
		}
		if ok {
			succeeded++
		} else {
			failed++
			final.Err = fmt.Errorf("enrich policy %q failed", policy)
		}
		emit.emit(final)
	}

	event := loggerFrom(ctx).Info()
	if failed > 0 {
		event = loggerFrom(ctx).Error()
	}
	event.
		Int("selected", len(targets)).
		Int("succeeded", succeeded).
		Int("failed", failed).
		Int("missing", len(missing)).
		Msg("Completed enrich policy execution")

	summary.Succeeded = succeeded
	summary.Failed = failed
	return summary
}

// logEnrichResult emits the final success/failure log for an enrich policy
// execution. Success is Info, not Debug: it is the stage-end line that makes a
// finished policy visible at the default level.
func logEnrichResult(ctx context.Context, policy string, status *enrichPhaseStatus, startTime time.Time) bool {
	phase, isFailure := "", false
	if status != nil {
		phase = status.Phase
		isFailure = strings.EqualFold(phase, "FAILED") || strings.EqualFold(phase, "CANCELLED")
	}
	event := loggerFrom(ctx).Info()
	if isFailure {
		event = loggerFrom(ctx).Error()
	}
	entry := event.Str("policy", policy).Float64("time_taken", progressNow().Sub(startTime).Seconds())
	if phase != "" {
		entry = entry.Str("phase", phase)
	}
	msg := "Enrich policy execution succeeded"
	if isFailure {
		msg = "Enrich policy execution failed"
	}
	entry.Msg(msg)
	return !isFailure
}

// executeEnrichPolicy starts one policy and polls its task until it finishes.
// It reports whether the policy succeeded; the caller owns the Final event.
func executeEnrichPolicy(ctx context.Context, es *elasticsearch.Client, policy string, emit progressEmitter) bool {
	startTime := progressNow()
	res, err := es.EnrichExecutePolicy(
		policy,
		es.EnrichExecutePolicy.WithContext(ctx),
		es.EnrichExecutePolicy.WithWaitForCompletion(false),
		es.EnrichExecutePolicy.WithHeader(map[string]string{
			"Accept": "application/json",
		}),
	)
	if err != nil {
		loggerFrom(ctx).Error().Err(err).Str("policy", policy).Msg("Failed to execute enrich policy")
		return false
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		if isUnsupportedEnrichAPI(res.StatusCode, body) {
			loggerFrom(ctx).Warn().
				Str("policy", policy).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Enrich execute endpoint returned a generic 404; check proxy or routing for /_enrich/policy/<name>/_execute and confirm this URL matches the backend used by Dev Tools")
			return false
		}
		loggerFrom(ctx).Error().
			Str("policy", policy).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Float64("time_taken", progressNow().Sub(startTime).Seconds()).
			Msg("Enrich policy execution failed")
		return false
	}

	var kickoff enrichExecuteResponse
	if err := json.NewDecoder(res.Body).Decode(&kickoff); err != nil {
		loggerFrom(ctx).Error().Err(err).Str("policy", policy).Msg("Unable to parse enrich policy execution response")
		return false
	}

	// Synchronous completion — small policy or ES returned immediately.
	if kickoff.Task == nil || *kickoff.Task == "" {
		return logEnrichResult(ctx, policy, kickoff.Status, startTime)
	}

	taskID := *kickoff.Task
	// The task ID lets an operator run GET _tasks/<id> by hand.
	loggerFrom(ctx).Info().Str("policy", policy).Str("task", taskID).Msg("Enrich policy task started")

	hb := progress.NewHeartbeat(progress.HeartbeatInterval, progressNow)
	lastStep := ""
	for pollCount := 1; ; pollCount++ {
		if err := sleepWithContext(ctx, progress.PollInterval(pollCount)); err != nil {
			loggerFrom(ctx).Warn().Err(err).Str("policy", policy).Msg("Context cancelled while polling enrich task")
			return false
		}

		taskRes, err := es.Tasks.Get(taskID, es.Tasks.Get.WithContext(ctx))
		if err != nil {
			loggerFrom(ctx).Error().Err(err).Str("policy", policy).Str("task", taskID).Msg("Failed to poll enrich policy task")
			return false
		}

		// 404 means ES has already removed the task record. On 9.3.1 an async
		// enrich task never stores its result, so this is the normal ending, and
		// a policy that failed between polls ends the same way: the outcome is
		// assumed, not verified. Kept as success per the FEAT-PROGRESS-01 spec
		// (task-404-as-success is existing behavior); changing it needs a
		// decision on how to verify the outcome. See ADR 0004.
		if taskRes.StatusCode == http.StatusNotFound {
			taskRes.Body.Close()
			return logEnrichResult(ctx, policy, nil, startTime)
		}

		// Any other non-2xx is a failed poll, not a task still running:
		// retrying would repeat the same answer and hide the cause.
		if taskRes.IsError() {
			body, _ := io.ReadAll(taskRes.Body)
			taskRes.Body.Close()
			loggerFrom(ctx).Error().
				Str("policy", policy).
				Str("task", taskID).
				Int("status_code", taskRes.StatusCode).
				Str("body", string(body)).
				Msg("Enrich policy task poll returned an error")
			return false
		}

		var task taskGetResponse
		decodeErr := json.NewDecoder(taskRes.Body).Decode(&task)
		taskRes.Body.Close()
		if decodeErr != nil {
			loggerFrom(ctx).Error().Err(decodeErr).Str("policy", policy).Str("task", taskID).Msg("Unable to parse enrich policy task response")
			return false
		}

		// A task that ended with an error is reported next to completed:true;
		// without this check it would be logged as a success.
		if len(task.Error) > 0 && string(task.Error) != "null" {
			loggerFrom(ctx).Error().
				Str("policy", policy).
				Str("task", taskID).
				RawJSON("task_error", task.Error).
				Float64("time_taken", progressNow().Sub(startTime).Seconds()).
				Msg("Enrich policy execution failed")
			return false
		}

		phase, step := "", ""
		if task.Task.Status != nil {
			phase = task.Task.Status.Phase
			step = task.Task.Status.Step
		}

		// completed=true or a response object present both signal the task is done.
		if task.Completed || task.Response != nil {
			status := task.Task.Status
			if task.Response != nil && task.Response.Status != nil {
				status = task.Response.Status
			}
			return logEnrichResult(ctx, policy, status, startTime)
		}

		elapsed := progressNow().Sub(startTime)
		label := enrich.StepLabel(step)
		if step != "" && step != lastStep {
			loggerFrom(ctx).Info().
				Str("policy", policy).
				Str("step", label).
				Float64("elapsed_s", elapsed.Seconds()).
				Msg("Enrich policy step")
			lastStep = step
		}

		docsDone, docsTotal := int64(-1), int64(-1)
		if enrich.IsReindexStep(step) {
			docsDone, docsTotal = enrichReindexProgress(ctx, es, taskID)
		}

		eventPhase := label
		if step == "" {
			eventPhase = phase
		}
		loggerFrom(ctx).Debug().
			Str("policy", policy).
			Str("phase", phase).
			Str("step", label).
			Int64("docs_done", docsDone).
			Int64("docs_total", docsTotal).
			Float64("elapsed_s", elapsed.Seconds()).
			Msg("Enrich policy execution in progress")
		emit.emit(ProgressEvent{
			Stage:    ProgressStageEnrich,
			Resource: policy,
			Phase:    eventPhase,
			Done:     docsDone,
			Total:    docsTotal,
			Elapsed:  elapsed,
		})

		if hb.Due() {
			beat := loggerFrom(ctx).Info().
				Str("policy", policy).
				Str("step", label).
				Float64("elapsed_s", elapsed.Seconds())
			if docsDone >= 0 && docsTotal >= 0 {
				beat = beat.Int64("docs_done", docsDone).Int64("docs_total", docsTotal)
				if docsTotal > 0 {
					beat = beat.Float64("percent", math.Round(float64(docsDone)*1000/float64(docsTotal))/10)
				}
			}
			beat.Msg("Enrich policy execution still running")
		}
	}
}

// enrichReindexProgress reads the documents done and expected by the enrich
// task's child reindex task. Progress is diagnostic and best effort: any
// failure returns (-1, -1) with a Debug line, and it never fails the run or
// changes the policy's outcome.
func enrichReindexProgress(ctx context.Context, es *elasticsearch.Client, taskID string) (done, total int64) {
	res, err := es.Tasks.List(
		es.Tasks.List.WithContext(ctx),
		es.Tasks.List.WithParentTaskID(taskID),
		es.Tasks.List.WithDetailed(true),
		es.Tasks.List.WithGroupBy("none"),
	)
	if err != nil {
		loggerFrom(ctx).Debug().Err(err).Str("task", taskID).Msg("Enrich reindex progress unavailable: child task request failed")
		return -1, -1
	}
	body, readErr := io.ReadAll(res.Body)
	res.Body.Close()
	if readErr != nil {
		loggerFrom(ctx).Debug().Err(readErr).Str("task", taskID).Msg("Enrich reindex progress unavailable: unable to read child task response")
		return -1, -1
	}
	if res.IsError() {
		loggerFrom(ctx).Debug().
			Str("task", taskID).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Enrich reindex progress unavailable: child task request returned an error")
		return -1, -1
	}
	docsDone, docsTotal, ok := enrich.ParseReindexChild(body)
	if !ok {
		loggerFrom(ctx).Debug().
			Str("task", taskID).
			Str("body", string(body)).
			Msg("Enrich reindex progress unavailable: no single reindex child task with readable counters")
		return -1, -1
	}
	return docsDone, docsTotal
}

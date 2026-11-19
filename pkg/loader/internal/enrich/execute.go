package enrich

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/aliasops"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

// Selection is the requested enrich policy selection after the public
// EnrichOptions precedence has been applied by package loader. Enabled turns
// enrich on; All selects every policy; otherwise Raw is a comma-separated list.
type Selection struct {
	Enabled bool
	All     bool
	Raw     string
}

// policySummary is one entry of GET _enrich/policy.
type policySummary struct {
	Config map[string]struct {
		Name string `json:"name"`
	} `json:"config"`
}

// policiesResponse is the GET _enrich/policy response.
type policiesResponse struct {
	Policies []policySummary `json:"policies"`
}

// phaseStatus holds the phase and, while the policy runs, the step
// fields returned by enrich policy execution.
type phaseStatus struct {
	Phase string `json:"phase"`
	// Step is the request class the enrich runner is executing (for example
	// "ReindexRequest"); ES omits it when the policy is not running.
	Step string `json:"step,omitempty"`
}

// executeResponse is the _execute response: a status when ES finished
// synchronously, otherwise the task to poll.
type executeResponse struct {
	Status *phaseStatus `json:"status,omitempty"`
	Task   *string      `json:"task,omitempty"`
}

// taskGetResponse holds the response from the Tasks API for async enrich policy execution.
type taskGetResponse struct {
	Completed bool `json:"completed"`
	Task      struct {
		Status *phaseStatus `json:"status,omitempty"`
	} `json:"task"`
	Response *executeResponse `json:"response,omitempty"`
	// Error is the task's failure object; ES sets it next to completed:true
	// when the task ended with an exception.
	Error json.RawMessage `json:"error,omitempty"`
}

// Summary is the outcome of Execute: the selected and missing policies in
// order, and how many selected policies succeeded or failed.
type Summary struct {
	Selected  []string
	Missing   []string
	Succeeded int
	Failed    int
}

// ExplicitPolicies returns the trimmed, de-duplicated names of an explicit
// selection in request order, or nil for a nil, disabled or All selection.
func (e *Selection) ExplicitPolicies() []string {
	if e == nil || !e.Enabled || e.All {
		return nil
	}

	policies := make([]string, 0)
	seen := make(map[string]struct{})
	for _, policy := range strings.Split(e.Raw, ",") {
		name := strings.TrimSpace(policy)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		policies = append(policies, name)
	}
	return policies
}

// ListPolicies returns every enrich policy name on the cluster, sorted. The
// bool is false when the enrich API is unsupported (generic 404, logged as a
// warning); that is not an error.
func ListPolicies(ctx context.Context, es *elasticsearch.Client) ([]string, bool, error) {
	res, err := es.EnrichGetPolicy(
		es.EnrichGetPolicy.WithContext(ctx),
		es.EnrichGetPolicy.WithHeader(map[string]string{
			"Accept": "application/json",
		}),
	)
	if err := runlog.Check(ctx, "getting enrich policies", err); err != nil {
		return nil, false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		body, readErr := esresponse.ReadBounded(res.Body)
		if esresponse.IsUnsupportedEnrichAPI(res.StatusCode, body, readErr) {
			runlog.From(ctx).Warn().
				Int("status_code", res.StatusCode).
				Str("es_error", esresponse.NewRejection(
					"enrich API", "", res.StatusCode, body, readErr,
					esresponse.MaxPublicMessageBytes).Error()).
				Msg("Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools")
			return nil, false, nil
		}
		return nil, false, runlog.Fail(runlog.FailureEvent(ctx).
			Int("status_code", res.StatusCode),
			"Failed to get enrich policies", esresponse.NewRejection(
				"enrich policy list request", "", res.StatusCode, body, readErr,
				esresponse.MaxPublicMessageBytes))
	}

	var parsed policiesResponse
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return nil, false, runlog.Fail(runlog.FailureEvent(ctx), "Unable to parse enrich policy response", err)
	}

	policies := make([]string, 0, len(parsed.Policies))
	for _, policy := range parsed.Policies {
		for _, config := range policy.Config {
			if config.Name == "" {
				continue
			}
			policies = append(policies, config.Name)
		}
	}
	slices.Sort(policies)
	return policies, true, nil
}

// resolveTargets splits the selection into available targets and missing
// names. An All selection with declared policies runs only those, in
// declared order.
func resolveTargets(sel *Selection, available []string, declared []string) ([]string, []string) {
	if sel == nil || !sel.Enabled {
		return nil, nil
	}

	availableSet := make(map[string]struct{}, len(available))
	for _, policy := range available {
		availableSet[policy] = struct{}{}
	}

	if sel.All {
		if len(declared) > 0 {
			targets := make([]string, 0, len(declared))
			missing := make([]string, 0)
			for _, policy := range declared {
				if _, ok := availableSet[policy]; ok {
					targets = append(targets, policy)
					continue
				}
				missing = append(missing, policy)
			}
			return targets, missing
		}

		targets := append([]string(nil), available...)
		slices.Sort(targets)
		return targets, nil
	}

	targets := make([]string, 0)
	missing := make([]string, 0)
	for _, policy := range sel.ExplicitPolicies() {
		if _, ok := availableSet[policy]; ok {
			targets = append(targets, policy)
			continue
		}
		missing = append(missing, policy)
	}
	return targets, missing
}

// Execute executes the selected enrich policies one after another.
// A policy that fails is counted and the run continues with the next one;
// OnProgress receives one Final snapshot per policy, whatever way executePolicy
// returned, so every early return is covered in one place. The only error is
// a failure to list policies, which ends the run before any policy executes.
func Execute(ctx context.Context, es *elasticsearch.Client, enrichSelection *Selection, declared []string, cfg progress.Seams) (Summary, error) {
	summary := Summary{}

	availablePolicies, supported, err := ListPolicies(ctx, es)
	if err != nil {
		return summary, err
	}
	if !supported {
		return summary, nil
	}
	if len(availablePolicies) == 0 {
		runlog.From(ctx).Warn().Msg("No enrich policies found; skipping enrich execution")
		return summary, nil
	}

	targets, missing := resolveTargets(enrichSelection, availablePolicies, declared)
	summary.Selected = append(summary.Selected, targets...)
	summary.Missing = append(summary.Missing, missing...)
	for _, policy := range missing {
		runlog.From(ctx).Warn().Str("policy", policy).Msg("Enrich policy not found; skipping")
	}

	if len(targets) == 0 {
		runlog.From(ctx).Warn().Msg("No enrich policies matched the request")
		return summary, nil
	}

	runlog.From(ctx).Info().
		Int("available", len(availablePolicies)).
		Int("requested", len(targets)+len(missing)).
		Int("selected", len(targets)).
		Msg("Starting enrich policy execution")

	succeeded := 0
	failed := 0
	for i, policy := range targets {
		runlog.From(ctx).Info().
			Str("policy", policy).
			Int("position", i+1).
			Int("of", len(targets)).
			Msg("Executing enrich policy")

		policyStart := cfg.Now()
		ok := executePolicy(ctx, es, policy, cfg)
		final := progress.Snapshot{
			Resource: policy,
			Done:     -1,
			Total:    -1,
			Elapsed:  cfg.Now().Sub(policyStart),
			Final:    true,
		}
		if ok {
			succeeded++
		} else {
			failed++
			final.Err = fmt.Errorf("enrich policy %q failed", policy)
		}
		cfg.Report(final)
	}

	event := runlog.From(ctx).Info()
	if failed > 0 {
		event = runlog.From(ctx).Error()
	}
	event.
		Int("selected", len(targets)).
		Int("succeeded", succeeded).
		Int("failed", failed).
		Int("missing", len(missing)).
		Msg("Completed enrich policy execution")

	summary.Succeeded = succeeded
	summary.Failed = failed
	return summary, nil
}

// logResult emits the final success/failure log for an enrich policy
// execution. Success is Info, not Debug: it is the stage-end line that makes a
// finished policy visible at the default level.
func logResult(ctx context.Context, policy string, status *phaseStatus, startTime time.Time, now func() time.Time) bool {
	phase, isFailure := "", false
	if status != nil {
		phase = status.Phase
		isFailure = strings.EqualFold(phase, "FAILED") || strings.EqualFold(phase, "CANCELLED")
	}
	event := runlog.From(ctx).Info()
	if isFailure {
		event = runlog.From(ctx).Error()
	}
	entry := event.Str("policy", policy).Float64("time_taken", now().Sub(startTime).Seconds())
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

// enrichAlias returns the alias Elasticsearch points at the newest backing
// index of policy. policy must be the selected policy's actual (managed,
// hash-suffixed) name: the logical key names no alias, and a wrong name would
// read as "absent" both times and fail every task-404 verification.
func enrichAlias(policy string) string {
	return ".enrich-" + policy
}

// verifyEnrichOutcome decides a policy whose task record is gone (task GET
// answered 404), where Elasticsearch gives no status to trust. It succeeds
// only when both alias snapshots were readable and the alias now has a
// non-empty target set that differs from the one read before kickoff: each
// successful execution builds a new .enrich-<policy>-<timestamp> index and
// repoints the alias. An unchanged or absent alias, or either unreadable
// snapshot, fails the policy at Error level; an unreadable snapshot is never
// treated as an absent one, which would turn a lookup failure into a
// first-execution success. A changed set shows the policy produced new output,
// not that this task did: a concurrent execution of the same policy by another
// client is indistinguishable. See ADR 0004.
func verifyEnrichOutcome(
	ctx context.Context, es *elasticsearch.Client, policy, taskID string,
	before []string, beforeErr error, startTime time.Time,
	now func() time.Time,
) bool {
	alias := enrichAlias(policy)
	after, afterErr := aliasops.ResolveTargets(ctx, es, alias)
	elapsed := now().Sub(startTime).Seconds()

	if beforeErr != nil || afterErr != nil {
		// ResolveTargets has already logged the HTTP status and bounded ES
		// error or decode cause; this line adds the policy and task the
		// failure belongs to.
		// .Err carries the first failed read; before_error/after_error carry
		// each one, so a double failure loses neither.
		firstErr := beforeErr
		if firstErr == nil {
			firstErr = afterErr
		}
		event := runlog.From(ctx).Error().
			Err(firstErr).
			Str("policy", policy).
			Str("task", taskID).
			Str("alias", alias).
			Strs("before", before).
			Strs("after", after).
			Float64("time_taken", elapsed)
		if beforeErr != nil {
			event = event.Str("before_error", beforeErr.Error())
		}
		if afterErr != nil {
			event = event.Str("after_error", afterErr.Error())
		}
		event.Msg("Enrich policy outcome could not be verified")
		return false
	}

	// ResolveTargets sorts, so slices.Equal compares sets regardless of the
	// order Elasticsearch listed the indices in.
	if len(after) == 0 || slices.Equal(before, after) {
		runlog.From(ctx).Error().
			Str("policy", policy).
			Str("task", taskID).
			Str("alias", alias).
			Strs("before", before).
			Strs("after", after).
			Float64("time_taken", elapsed).
			Msg("Enrich policy execution failed: the backing alias did not move")
		return false
	}

	runlog.From(ctx).Debug().
		Str("policy", policy).
		Str("task", taskID).
		Str("alias", alias).
		Strs("before", before).
		Strs("after", after).
		Msg("Enrich policy outcome verified via backing alias")
	return logResult(ctx, policy, nil, startTime, now)
}

// maxConsecutivePollFailures is the transient task-poll failure that ends a
// policy: the third consecutive 429/502/503/504 fails it, so a streak gets two
// retries. Unlike the transform wait, enrich has no overall timeout to bound
// retries, and the poll loop sleeps before every request; this cap is what
// keeps a persistent outage from polling forever. It bounds a failure streak,
// not total execution: a decoded task poll resets it, and a healthy running
// task still waits for completion or the caller's cancellation.
const maxConsecutivePollFailures = 3

// withBodyErrors adds the read and close errors of a diagnostic error body
// to event, so a failed read or close is reported rather than discarded.
func withBodyErrors(event *zerolog.Event, readErr, closeErr error) *zerolog.Event {
	if readErr != nil {
		event = event.Str("body_error", readErr.Error())
	}
	if closeErr != nil {
		event = event.Str("body_close_error", closeErr.Error())
	}
	return event
}

// executePolicy starts one policy and polls its task until it finishes.
// It reports whether the policy succeeded; the caller owns the Final event.
// Transient task-poll statuses are retried up to maxConsecutivePollFailures;
// a task 404 is still decided by verifyEnrichOutcome.
func executePolicy(ctx context.Context, es *elasticsearch.Client, policy string, cfg progress.Seams) bool {
	startTime := cfg.Now()
	// The backing alias is read before kickoff, never after: a fast execution
	// could repoint it before a later read, and the task-404 check would then
	// see no change. A failed read is kept, not returned: an explicit execution
	// or task status stays authoritative and ignores it, while a task 404 needs
	// both snapshots and fails on it (verifyEnrichOutcome).
	before, beforeErr := aliasops.ResolveTargets(ctx, es, enrichAlias(policy))
	res, err := es.EnrichExecutePolicy(
		policy,
		es.EnrichExecutePolicy.WithContext(ctx),
		es.EnrichExecutePolicy.WithWaitForCompletion(false),
		es.EnrichExecutePolicy.WithHeader(map[string]string{
			"Accept": "application/json",
		}),
	)
	if err != nil {
		runlog.From(ctx).Error().Err(err).Str("policy", policy).Msg("Failed to execute enrich policy")
		return false
	}
	defer res.Body.Close()

	if res.IsError() {
		body, readErr := esresponse.ReadBounded(res.Body)
		if esresponse.IsUnsupportedEnrichAPI(res.StatusCode, body, readErr) {
			runlog.From(ctx).Warn().
				Str("policy", policy).
				Int("status_code", res.StatusCode).
				Str("es_error", esresponse.NewRejection(
					"enrich API", "", res.StatusCode, body, readErr,
					esresponse.MaxPublicMessageBytes).Error()).
				Msg("Enrich execute endpoint returned a generic 404; check proxy or routing for /_enrich/policy/<name>/_execute and confirm this URL matches the backend used by Dev Tools")
			return false
		}
		runlog.From(ctx).Error().
			Str("policy", policy).
			Int("status_code", res.StatusCode).
			Str("es_error", esresponse.NewRejection(
				"enrich policy", policy, res.StatusCode, body, readErr,
				esresponse.MaxPublicMessageBytes).Error()).
			Float64("time_taken", cfg.Now().Sub(startTime).Seconds()).
			Msg("Enrich policy execution failed")
		return false
	}

	var kickoff executeResponse
	if err := json.NewDecoder(res.Body).Decode(&kickoff); err != nil {
		runlog.From(ctx).Error().Err(err).Str("policy", policy).Msg("Unable to parse enrich policy execution response")
		return false
	}

	// Synchronous completion — small policy or ES returned immediately.
	if kickoff.Task == nil || *kickoff.Task == "" {
		return logResult(ctx, policy, kickoff.Status, startTime, cfg.Now)
	}

	taskID := *kickoff.Task
	// The task ID lets an operator run GET _tasks/<id> by hand.
	runlog.From(ctx).Info().Str("policy", policy).Str("task", taskID).Msg("Enrich policy task started")

	hb := progress.NewHeartbeat(progress.HeartbeatInterval, cfg.Now)
	lastStep := ""
	// failures counts consecutive transient task-poll failures for this policy
	// only; see maxConsecutivePollFailures.
	failures := 0
	for pollCount := 1; ; pollCount++ {
		if err := cfg.Sleep(ctx, progress.PollInterval(pollCount)); err != nil {
			runlog.From(ctx).Warn().Err(err).Str("policy", policy).Msg("Context cancelled while polling enrich task")
			return false
		}

		taskRes, err := es.Tasks.Get(taskID, es.Tasks.Get.WithContext(ctx))
		if err != nil {
			runlog.From(ctx).Error().Err(err).Str("policy", policy).Str("task", taskID).Msg("Failed to poll enrich policy task")
			return false
		}

		// 404 means ES has already removed the task record. On 9.3.1 an async
		// enrich task never stores its result, so this is the normal ending, and
		// a policy that failed between polls ends the same way. The response
		// says nothing about the outcome, so it is verified from the backing
		// alias instead of assumed: success only if the alias moved to a new,
		// non-empty target set since kickoff. Reverting to "404 is success"
		// reports failed enrichment as succeeded. See ADR 0004; guarded by
		// TestExecutePolicyVerifiesTask404, TestProgressEnrichAliasFailureContinues
		// and the delete_sync-managed_enrich E2E scenario.
		if taskRes.StatusCode == http.StatusNotFound {
			taskRes.Body.Close()
			return verifyEnrichOutcome(ctx, es, policy, taskID, before, beforeErr, startTime, cfg.Now)
		}

		// Any other non-2xx is a failed poll, not a task still running. Only a
		// transient status (429/502/503/504) is retried, and only this
		// idempotent task read: the kickoff, alias reads and child-task reads
		// are never repeated. The body is diagnostic text read and closed once
		// here; the status alone decides retry, and the body is never parsed as
		// a task outcome. Every other status fails at once, since retrying it
		// would repeat the same answer and hide the cause.
		if taskRes.IsError() {
			body, readErr := esresponse.ReadBounded(taskRes.Body)
			closeErr := taskRes.Body.Close()
			retryable := esresponse.IsRetryableStatus(taskRes.StatusCode)
			if retryable {
				failures++
			}
			if retryable && failures < maxConsecutivePollFailures {
				warn := runlog.From(ctx).Warn().
					Str("policy", policy).
					Str("task", taskID).
					Int("status_code", taskRes.StatusCode).
					Str("es_error", esresponse.NewRejection(
						"enrich task", taskID, taskRes.StatusCode, body, readErr,
						esresponse.MaxPublicMessageBytes).Error()).
					Int("attempt", failures)
				warn = withBodyErrors(warn, readErr, closeErr)
				warn.Msg("Enrich policy task poll failed; retrying")
				continue
			}
			event := runlog.From(ctx).Error().
				Str("policy", policy).
				Str("task", taskID).
				Int("status_code", taskRes.StatusCode).
				Str("es_error", esresponse.NewRejection(
					"enrich task", taskID, taskRes.StatusCode, body, readErr,
					esresponse.MaxPublicMessageBytes).Error())
			if retryable {
				event = event.Int("attempt", failures)
			}
			event = withBodyErrors(event, readErr, closeErr)
			event.Msg("Enrich policy task poll returned an error")
			return false
		}

		var task taskGetResponse
		decodeErr := json.NewDecoder(taskRes.Body).Decode(&task)
		taskRes.Body.Close()
		if decodeErr != nil {
			runlog.From(ctx).Error().Err(decodeErr).Str("policy", policy).Str("task", taskID).Msg("Unable to parse enrich policy task response")
			return false
		}
		// A decoded task document ends the failure streak: the budget caps
		// consecutive transient failures, not total polls.
		failures = 0

		// A task that ended with an error is reported next to completed:true;
		// without this check it would be logged as a success.
		if len(task.Error) > 0 && string(task.Error) != "null" {
			runlog.From(ctx).Error().
				Str("policy", policy).
				Str("task", taskID).
				Str("es_error", esresponse.ErrorDetail(task.Error, esresponse.MaxPublicMessageBytes)).
				Float64("time_taken", cfg.Now().Sub(startTime).Seconds()).
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
			return logResult(ctx, policy, status, startTime, cfg.Now)
		}

		elapsed := cfg.Now().Sub(startTime)
		label := StepLabel(step)
		if step != "" && step != lastStep {
			runlog.From(ctx).Info().
				Str("policy", policy).
				Str("step", label).
				Float64("elapsed_s", elapsed.Seconds()).
				Msg("Enrich policy step")
			lastStep = step
		}

		docsDone, docsTotal := int64(-1), int64(-1)
		if IsReindexStep(step) {
			docsDone, docsTotal = reindexProgress(ctx, es, taskID)
		}

		eventPhase := label
		if step == "" {
			eventPhase = phase
		}
		runlog.From(ctx).Debug().
			Str("policy", policy).
			Str("phase", phase).
			Str("step", label).
			Int64("docs_done", docsDone).
			Int64("docs_total", docsTotal).
			Float64("elapsed_s", elapsed.Seconds()).
			Msg("Enrich policy execution in progress")
		cfg.Report(progress.Snapshot{
			Resource: policy,
			Phase:    eventPhase,
			Done:     docsDone,
			Total:    docsTotal,
			Elapsed:  elapsed,
		})

		if hb.Due() {
			beat := runlog.From(ctx).Info().
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

// reindexProgress reads the documents done and expected by the enrich
// task's child reindex task. Progress is diagnostic and best effort: any
// failure returns (-1, -1) with a Debug line, and it never fails the run or
// changes the policy's outcome.
func reindexProgress(ctx context.Context, es *elasticsearch.Client, taskID string) (done, total int64) {
	res, err := es.Tasks.List(
		es.Tasks.List.WithContext(ctx),
		es.Tasks.List.WithParentTaskID(taskID),
		es.Tasks.List.WithDetailed(true),
		es.Tasks.List.WithGroupBy("none"),
	)
	if err != nil {
		runlog.From(ctx).Debug().Err(err).Str("task", taskID).Msg("Enrich reindex progress unavailable: child task request failed")
		return -1, -1
	}
	// An error status is read bounded (it only feeds the es_error diagnostic);
	// a 2xx body is parsed for counters, so it is read in full. A read error
	// returns (-1, -1) ahead of any status diagnostic on either branch.
	read := io.ReadAll
	if res.IsError() {
		read = esresponse.ReadBounded
	}
	body, readErr := read(res.Body)
	res.Body.Close()
	if readErr != nil {
		runlog.From(ctx).Debug().Err(readErr).Str("task", taskID).Msg("Enrich reindex progress unavailable: unable to read child task response")
		return -1, -1
	}
	if res.IsError() {
		runlog.From(ctx).Debug().
			Str("task", taskID).
			Int("status_code", res.StatusCode).
			Str("es_error", esresponse.NewRejection(
				"enrich task", taskID, res.StatusCode, body, nil,
				esresponse.MaxPublicMessageBytes).Error()).
			Msg("Enrich reindex progress unavailable: child task request returned an error")
		return -1, -1
	}
	docsDone, docsTotal, ok := ParseReindexChild(body)
	if !ok {
		runlog.From(ctx).Debug().
			Str("task", taskID).
			Int("body_bytes", len(body)).
			Msg("Enrich reindex progress unavailable: no single reindex child task with readable counters")
		return -1, -1
	}
	return docsDone, docsTotal
}

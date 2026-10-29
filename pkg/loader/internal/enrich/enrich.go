// Package enrich lists, selects and executes enrich policies for a Run and
// interprets enrich task status for progress. It logs through internal/runlog
// and reports progress as internal snapshots; package loader maps them to
// public ProgressEvents and its Result. A failed policy is counted, not fatal.
package enrich

import (
	"bytes"
	"encoding/json"
	"strings"
)

// reindexStep is the enrich runner's step name while it copies documents.
const reindexStep = "ReindexRequest"

// StepLabel maps the runner's request-class step names to readable labels;
// unknown values pass through unchanged so a new ES step is still visible.
func StepLabel(step string) string {
	switch step {
	case "ReindexRequest":
		return "reindex"
	case "ForceMergeRequest":
		return "force-merge"
	case "RefreshRequest":
		return "refresh"
	case "IndicesAliasesRequest":
		return "promote"
	default:
		return step
	}
}

// IsReindexStep reports whether step is the reindex request step.
func IsReindexStep(step string) bool {
	return step == reindexStep
}

// childList is the shape of GET _tasks?group_by=none.
type childList struct {
	Tasks        []json.RawMessage `json:"tasks"`
	NodeFailures []json.RawMessage `json:"node_failures"`
	TaskFailures []json.RawMessage `json:"task_failures"`
}

// childTask is one entry of childList.Tasks.
type childTask struct {
	Action string                     `json:"action"`
	Status map[string]json.RawMessage `json:"status"`
}

// ParseReindexChild extracts reindex progress from a
// GET _tasks?parent_task_id=<task>&detailed=true&group_by=none response.
// It selects exactly one reindex action with valid status.total;
// done = created+updated+deleted+noops. Ambiguous or partial results are unknown.
// ok is false when no such child exists or the body is malformed (never an error:
// progress is best effort and an estimate would be wrong data).
func ParseReindexChild(body []byte) (done, total int64, ok bool) {
	var list childList
	if err := json.Unmarshal(body, &list); err != nil {
		return 0, 0, false
	}
	// A partial answer (some node or task failed to respond) may hide the real
	// child or a second one, so it is not trusted.
	if len(list.NodeFailures) > 0 || len(list.TaskFailures) > 0 {
		return 0, 0, false
	}

	var candidate *childTask
	for _, raw := range list.Tasks {
		var task childTask
		if err := json.Unmarshal(raw, &task); err != nil {
			return 0, 0, false
		}
		if !isReindexAction(task.Action) {
			continue
		}
		if candidate != nil {
			return 0, 0, false
		}
		candidate = &task
	}
	if candidate == nil || candidate.Status == nil {
		return 0, 0, false
	}

	total, ok = counter(candidate.Status, "total")
	if !ok {
		return 0, 0, false
	}
	for _, field := range []string{"created", "updated", "deleted", "noops"} {
		value, valid := counter(candidate.Status, field)
		if !valid {
			return 0, 0, false
		}
		done += value
	}
	if done < 0 {
		return 0, 0, false
	}
	return done, total, true
}

// isReindexAction matches the action of the enrich runner's reindex
// ("cluster:admin/xpack/enrich/reindex" on 9.3.1) and the plain reindex action.
func isReindexAction(action string) bool {
	return strings.HasSuffix(action, "/reindex")
}

// counter reads a required non-negative integer. Null, missing, fractional,
// string-typed and negative values are all invalid; an explicit 0 is valid.
func counter(status map[string]json.RawMessage, key string) (int64, bool) {
	raw, present := status[key]
	if !present {
		return 0, false
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] < '0' || raw[0] > '9' {
		return 0, false
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, false
	}
	return value, value >= 0
}

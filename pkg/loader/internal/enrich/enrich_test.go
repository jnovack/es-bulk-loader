package enrich

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// readFixture loads a response recorded from Elasticsearch 9.3.1; see
// test/fixtures/elasticsearch/README.md.
func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "test", "fixtures", "elasticsearch", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return body
}

// stepOf extracts task.status.step from a recorded GET _tasks/<id> response.
func stepOf(t *testing.T, body []byte) string {
	t.Helper()
	var parsed struct {
		Task struct {
			Status struct {
				Step string `json:"step"`
			} `json:"status"`
		} `json:"task"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("decode task fixture: %v", err)
	}
	return parsed.Task.Status.Step
}

func TestStepLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		step string
		want string
	}{
		// Strings below come from recorded 9.3.1 responses.
		{name: "recorded reindex", step: stepOf(t, readFixture(t, "enrich-task-step-reindex.json")), want: "reindex"},
		{name: "recorded force merge", step: stepOf(t, readFixture(t, "enrich-task-step-force-merge.json")), want: "force-merge"},
		{name: "recorded create index passes through", step: stepOf(t, readFixture(t, "enrich-task-step-create-index.json")), want: "CreateIndexRequest"},
		{name: "recorded update settings passes through", step: stepOf(t, readFixture(t, "enrich-task-step-update-settings.json")), want: "UpdateSettingsRequest"},
		// Synthetic: seen only in tagged source (EnrichPolicyRunner.java, v9.3.1).
		{name: "refresh from source", step: "RefreshRequest", want: "refresh"},
		{name: "promote from source", step: "IndicesAliasesRequest", want: "promote"},
		{name: "unknown passes through", step: "SomethingNewRequest", want: "SomethingNewRequest"},
		{name: "empty stays empty", step: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := StepLabel(tt.step); got != tt.want {
				t.Fatalf("StepLabel(%q) = %q, want %q", tt.step, got, tt.want)
			}
		})
	}
}

func TestIsReindexStep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		step string
		want bool
	}{
		{step: stepOf(t, readFixture(t, "enrich-task-step-reindex.json")), want: true},
		{step: stepOf(t, readFixture(t, "enrich-task-step-force-merge.json")), want: false},
		{step: "", want: false},
		{step: "reindex", want: false},
	}
	for _, tt := range tests {
		if got := IsReindexStep(tt.step); got != tt.want {
			t.Fatalf("IsReindexStep(%q) = %v, want %v", tt.step, got, tt.want)
		}
	}
}

func TestParseReindexChild(t *testing.T) {
	t.Parallel()

	// task builds a synthetic task entry with the given action and status body.
	task := func(action, status string) string {
		return `{"node":"n","id":1,"type":"transport","action":"` + action + `","status":` + status + `}`
	}
	const goodStatus = `{"total":100,"updated":1,"created":20,"deleted":3,"noops":4}`
	const reindexAction = "cluster:admin/xpack/enrich/reindex"

	tests := []struct {
		name      string
		body      []byte
		wantDone  int64
		wantTotal int64
		wantOK    bool
	}{
		{name: "recorded child at start", body: readFixture(t, "enrich-task-children-reindex-start.json"), wantDone: 0, wantTotal: 500000, wantOK: true},
		{name: "recorded child mid reindex", body: readFixture(t, "enrich-task-children-reindex-progress.json"), wantDone: 20000, wantTotal: 500000, wantOK: true},
		{name: "recorded empty child list", body: readFixture(t, "enrich-task-children-empty.json"), wantOK: false},
		{name: "done sums created updated deleted noops", body: []byte(`{"tasks":[` + task(reindexAction, goodStatus) + `]}`), wantDone: 28, wantTotal: 100, wantOK: true},
		{name: "plain reindex action accepted", body: []byte(`{"tasks":[` + task("indices:data/write/reindex", goodStatus) + `]}`), wantDone: 28, wantTotal: 100, wantOK: true},
		{name: "explicit zero total is a known zero", body: []byte(`{"tasks":[` + task(reindexAction, `{"total":0,"updated":0,"created":0,"deleted":0,"noops":0}`) + `]}`), wantDone: 0, wantTotal: 0, wantOK: true},
		{name: "unrelated task with total is ignored", body: []byte(`{"tasks":[` + task("indices:data/write/bulk", goodStatus) + `,` + task(reindexAction, `{"total":9,"updated":0,"created":5,"deleted":0,"noops":0}`) + `]}`), wantDone: 5, wantTotal: 9, wantOK: true},
		{name: "empty body", body: nil, wantOK: false},
		{name: "malformed json", body: []byte(`{"tasks":[`), wantOK: false},
		{name: "tasks wrong type", body: []byte(`{"tasks":{"a":1}}`), wantOK: false},
		{name: "no tasks key", body: []byte(`{}`), wantOK: false},
		{name: "only unrelated tasks carrying total", body: []byte(`{"tasks":[` + task("indices:data/write/bulk", goodStatus) + `]}`), wantOK: false},
		{name: "child without total", body: []byte(`{"tasks":[` + task(reindexAction, `{"created":1,"updated":0,"deleted":0,"noops":0}`) + `]}`), wantOK: false},
		{name: "child without status", body: []byte(`{"tasks":[{"action":"` + reindexAction + `"}]}`), wantOK: false},
		{name: "multiple reindex candidates", body: []byte(`{"tasks":[` + task(reindexAction, goodStatus) + `,` + task(reindexAction, goodStatus) + `]}`), wantOK: false},
		{name: "node failures make it untrustworthy", body: []byte(`{"node_failures":[{"type":"x"}],"tasks":[` + task(reindexAction, goodStatus) + `]}`), wantOK: false},
		{name: "task failures make it untrustworthy", body: []byte(`{"task_failures":[{"task_id":1}],"tasks":[` + task(reindexAction, goodStatus) + `]}`), wantOK: false},
		{name: "null total", body: []byte(`{"tasks":[` + task(reindexAction, `{"total":null,"updated":0,"created":0,"deleted":0,"noops":0}`) + `]}`), wantOK: false},
		{name: "null counter", body: []byte(`{"tasks":[` + task(reindexAction, `{"total":5,"updated":0,"created":null,"deleted":0,"noops":0}`) + `]}`), wantOK: false},
		{name: "missing counter", body: []byte(`{"tasks":[` + task(reindexAction, `{"total":5,"updated":0,"created":1,"deleted":0}`) + `]}`), wantOK: false},
		{name: "string total", body: []byte(`{"tasks":[` + task(reindexAction, `{"total":"5","updated":0,"created":0,"deleted":0,"noops":0}`) + `]}`), wantOK: false},
		{name: "fractional counter", body: []byte(`{"tasks":[` + task(reindexAction, `{"total":5,"updated":0,"created":1.5,"deleted":0,"noops":0}`) + `]}`), wantOK: false},
		{name: "negative total", body: []byte(`{"tasks":[` + task(reindexAction, `{"total":-1,"updated":0,"created":0,"deleted":0,"noops":0}`) + `]}`), wantOK: false},
		{name: "negative counter", body: []byte(`{"tasks":[` + task(reindexAction, `{"total":5,"updated":0,"created":-2,"deleted":0,"noops":0}`) + `]}`), wantOK: false},
		{name: "task entry not an object", body: []byte(`{"tasks":[7]}`), wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			done, total, ok := ParseReindexChild(tt.body)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (done=%d total=%d)", ok, tt.wantOK, done, total)
			}
			if !ok {
				if done != 0 || total != 0 {
					t.Fatalf("not ok must return zero values, got done=%d total=%d", done, total)
				}
				return
			}
			if done != tt.wantDone || total != tt.wantTotal {
				t.Fatalf("done,total = %d,%d, want %d,%d", done, total, tt.wantDone, tt.wantTotal)
			}
		})
	}
}

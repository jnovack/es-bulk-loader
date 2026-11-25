package loader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
)

// Every test in this file is serial: they replace the package variables
// progressNow and sleepWithContext through installFakeClock and restore them
// in cleanup. Captures go through Options.Logger, never the global logger.

const (
	progressIndex      = "progress-idx"
	progressTransform  = "tx1"
	progressTaskID     = "node1:1"
	progressDefaultDoc = `[{"id":"1","name":"card"}]`
)

// stubReply is one canned HTTP answer.
type stubReply struct {
	status int
	body   string
}

// progressStubConfig selects how newProgressStub answers the enrich-task,
// child-task and bulk requests. The zero value answers an enrich execute
// synchronously with a COMPLETE status and a successful bulk.
type progressStubConfig struct {
	policies      []string
	executeBody   string      // POST _enrich/policy/<p>/_execute
	tasks         []stubReply // GET _tasks/<id>, in order, last repeats
	children      []stubReply // GET _tasks?parent_task_id=..., in order, last repeats
	bulkStatus    int         // 0 means 200
	bulkFailItems int         // first N items of every bulk request fail
	statsStatus   int         // 0 means 200
	// executeStatus is the HTTP status of the enrich execute (kickoff) answer,
	// carrying executeBody; 0 means 200.
	executeStatus int
	// executeDrop and taskDrop answer the kickoff or the task poll by closing
	// the connection before any header, a transport failure for the client.
	executeDrop, taskDrop bool
	// aliasReplies answers GET _alias/.enrich-<policy>, per policy, in order,
	// last repeats. A policy without an entry gets enrichAliasMoved: absent
	// before kickoff, then a new backing index, as a successful execution.
	aliasReplies map[string][]stubReply
}

// progressStubOption mutates a progressStubConfig.
type progressStubOption func(*progressStubConfig)

func withPolicies(names ...string) progressStubOption {
	return func(c *progressStubConfig) { c.policies = names }
}

// withEnrichTask makes the execute call return a task and serves tasks for it.
func withEnrichTask(tasks ...stubReply) progressStubOption {
	return func(c *progressStubConfig) {
		c.executeBody = fmt.Sprintf(`{"task":%q}`, progressTaskID)
		c.tasks = tasks
	}
}

func withExecuteBody(body string) progressStubOption {
	return func(c *progressStubConfig) { c.executeBody = body }
}

// withExecuteReply makes the kickoff answer status with body, on the existing
// execute route.
func withExecuteReply(status int, body string) progressStubOption {
	return func(c *progressStubConfig) { c.executeStatus, c.executeBody = status, body }
}

// withExecuteDrop makes the kickoff a transport failure.
func withExecuteDrop() progressStubOption {
	return func(c *progressStubConfig) { c.executeDrop = true }
}

// withTaskDrop makes the task poll (a task id must be offered by
// withEnrichTask) a transport failure; the stub's task replies are unused.
func withTaskDrop() progressStubOption {
	return func(c *progressStubConfig) { c.taskDrop = true }
}

func withChildren(children ...stubReply) progressStubOption {
	return func(c *progressStubConfig) { c.children = children }
}

func withBulk(status, failItems int) progressStubOption {
	return func(c *progressStubConfig) { c.bulkStatus, c.bulkFailItems = status, failItems }
}

// withEnrichAliasReplies sets the answers to policy's backing-alias reads (the
// snapshot before kickoff, then the check after a task 404), in order, last
// repeats. It overrides the enrichAliasMoved default for that policy only.
func withEnrichAliasReplies(policy string, replies ...stubReply) progressStubOption {
	return func(c *progressStubConfig) {
		if c.aliasReplies == nil {
			c.aliasReplies = map[string][]stubReply{}
		}
		c.aliasReplies[policy] = replies
	}
}

// enrichAliasMissing is the 404 answer for a policy's absent backing alias.
func enrichAliasMissing(policy string) stubReply {
	return stubReply{http.StatusNotFound, fmt.Sprintf(`{"error":"alias [.enrich-%s] missing","status":404}`, policy)}
}

// enrichAliasAt is a GET _alias/.enrich-<policy> answer listing targets, each
// carrying the requested alias under "aliases" as Elasticsearch returns it.
func enrichAliasAt(policy string, targets ...string) stubReply {
	entries := make([]string, 0, len(targets))
	for _, target := range targets {
		entries = append(entries, fmt.Sprintf(`%q:{"aliases":{%q:{}}}`, target, ".enrich-"+policy))
	}
	return stubReply{http.StatusOK, "{" + strings.Join(entries, ",") + "}"}
}

// enrichAliasMoved is the default alias sequence: absent before kickoff, then
// pointing at a new backing index, which verifies a task-404 ending.
func enrichAliasMoved(policy string) []stubReply {
	return []stubReply{enrichAliasMissing(policy), enrichAliasAt(policy, ".enrich-"+policy+"-2")}
}

// newProgressStub serves a whole Run against progressIndex: index create, bulk,
// refresh, the enrich list/execute/task/child-task routes and the transform
// routes of progressTransform. statsSequence is answered to
// GET _transform/tx1/_stats in order (the last entry repeats); a _stats
// request with an empty sequence is a test failure. The returned function
// yields a copy of the recorded operation names, in arrival order. It is built
// from the shared scenario helpers (opRecorder, registerTransformRoutes); only
// the progress-specific replies are defined here.
func newProgressStub(t *testing.T, statsSequence []string, opts ...progressStubOption) (*esstub.Stub, func() []string) {
	t.Helper()

	cfg := progressStubConfig{policies: []string{"policy-a"}, executeBody: `{"status":{"phase":"COMPLETE"}}`}
	for _, opt := range opts {
		opt(&cfg)
	}

	var (
		rec                             opRecorder
		mu                              sync.Mutex
		present                         bool
		taskCalls, childCalls, statCall int
	)
	next := func(seq []stubReply, calls *int) stubReply {
		mu.Lock()
		defer mu.Unlock()
		i := *calls
		if i >= len(seq) {
			i = len(seq) - 1
		}
		*calls++
		return seq[i]
	}
	write := func(w http.ResponseWriter, status int, body string) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}

	stub := esstub.New(t)
	stub.Handle("HEAD /"+progressIndex, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		exists := present
		mu.Unlock()
		existsReply(w, exists)
	})
	stub.Handle("GET /_nodes", esstub.JSON(http.StatusOK, nodesBody))
	stub.Handle("PUT /"+progressIndex, rec.recorded("index.create", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		present = true
		mu.Unlock()
		write(w, http.StatusOK, ackBody)
	}))
	stub.Handle("POST /_bulk", rec.recorded("bulk", func(w http.ResponseWriter, r *http.Request) {
		if cfg.bulkStatus != 0 && cfg.bulkStatus != http.StatusOK {
			write(w, cfg.bulkStatus, `{"error":{"type":"stub_bulk_failure"}}`)
			return
		}
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read bulk body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		docs := bytes.Count(bytes.TrimSpace(payload), []byte("\n")) / 2
		docs++
		items := make([]string, 0, docs)
		for i := 0; i < docs; i++ {
			if i < cfg.bulkFailItems {
				items = append(items, fmt.Sprintf(`{"index":{"_index":%q,"_id":"%d","status":400,"error":{"type":"mapper_parsing_exception","reason":"boom"}}}`, progressIndex, i))
			} else {
				items = append(items, fmt.Sprintf(`{"index":{"_index":%q,"_id":"%d","status":201}}`, progressIndex, i))
			}
		}
		write(w, http.StatusOK, fmt.Sprintf(`{"errors":%t,"items":[%s]}`, cfg.bulkFailItems > 0, strings.Join(items, ",")))
	}))
	stub.Handle("POST /"+progressIndex+"/_refresh", rec.recorded("index.refresh", esstub.JSON(http.StatusOK, refreshBody)))

	listPolicies := rec.recorded("enrich.list", func(w http.ResponseWriter, _ *http.Request) {
		configs := make([]string, 0, len(cfg.policies))
		for _, name := range cfg.policies {
			configs = append(configs, fmt.Sprintf(`{"config":{"match":{"name":%q}}}`, name))
		}
		write(w, http.StatusOK, fmt.Sprintf(`{"policies":[%s]}`, strings.Join(configs, ",")))
	})
	stub.Handle("GET /_enrich/policy", listPolicies)
	stub.Handle("GET /_enrich/policy/{$}", listPolicies)
	executePolicy := rec.recorded("enrich.execute", func(w http.ResponseWriter, _ *http.Request) {
		if cfg.executeDrop {
			dropConnection(t, w)
			return
		}
		status := cfg.executeStatus
		if status == 0 {
			status = http.StatusOK
		}
		write(w, status, cfg.executeBody)
	})
	stub.Handle("POST /_enrich/policy/{name}/_execute", executePolicy)
	stub.Handle("PUT /_enrich/policy/{name}/_execute", executePolicy)
	// One exact backing-alias route per policy, each with its own counter, so
	// one policy's reads never advance another's sequence.
	for _, policy := range cfg.policies {
		replies := cfg.aliasReplies[policy]
		if len(replies) == 0 {
			replies = enrichAliasMoved(policy)
		}
		calls := new(int)
		stub.Handle("GET /_alias/.enrich-"+policy, rec.recorded("enrich.alias", func(w http.ResponseWriter, _ *http.Request) {
			reply := next(replies, calls)
			write(w, reply.status, reply.body)
		}))
	}
	stub.Handle("GET /_tasks/"+progressTaskID, rec.recorded("enrich.task", func(w http.ResponseWriter, _ *http.Request) {
		if cfg.taskDrop {
			dropConnection(t, w)
			return
		}
		if len(cfg.tasks) == 0 {
			t.Errorf("unexpected task poll with no task replies configured")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		reply := next(cfg.tasks, &taskCalls)
		write(w, reply.status, reply.body)
	}))
	stub.Handle("GET /_tasks", rec.recorded("enrich.children", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("parent_task_id") != progressTaskID {
			t.Errorf("child task list for parent %q, want %q", r.URL.Query().Get("parent_task_id"), progressTaskID)
		}
		if len(cfg.children) == 0 {
			write(w, http.StatusOK, `{"tasks":[]}`)
			return
		}
		reply := next(cfg.children, &childCalls)
		write(w, reply.status, reply.body)
	}))

	registerTransformRoutes(stub, &rec, progressTransform, nil)
	stub.Handle("GET /_transform/"+progressTransform+"/_stats", rec.recorded("transform.stats", func(w http.ResponseWriter, _ *http.Request) {
		if len(statsSequence) == 0 {
			t.Errorf("unexpected _stats request with no stats configured")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		replies := make([]stubReply, 0, len(statsSequence))
		status := cfg.statsStatus
		if status == 0 {
			status = http.StatusOK
		}
		for _, body := range statsSequence {
			replies = append(replies, stubReply{status: status, body: body})
		}
		reply := next(replies, &statCall)
		write(w, reply.status, reply.body)
	}))

	return stub, rec.operations
}

// fakeClock drives progressNow and sleepWithContext without real time: each
// sleep advances the clock by step and returns ctx.Err().
type fakeClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Sleep(ctx context.Context, _ time.Duration) error {
	c.mu.Lock()
	c.now = c.now.Add(c.step)
	c.mu.Unlock()
	return ctx.Err()
}

// installFakeClock replaces progressNow and sleepWithContext for the test and
// restores both in cleanup, so the calling test must stay serial.
func installFakeClock(t *testing.T, step time.Duration) *fakeClock {
	t.Helper()
	clock := &fakeClock{now: time.Unix(1_000_000, 0), step: step}
	previousNow, previousSleep := progressNow, sleepWithContext
	progressNow = clock.Now
	sleepWithContext = clock.Sleep
	t.Cleanup(func() {
		progressNow = previousNow
		sleepWithContext = previousSleep
	})
	return clock
}

// logLine is one decoded JSON log record.
type logLine map[string]any

func (l logLine) str(key string) string {
	value, _ := l[key].(string)
	return value
}

// num returns the numeric field and whether it is present.
func (l logLine) num(key string) (float64, bool) {
	value, ok := l[key].(float64)
	return value, ok
}

func parseLogLines(t *testing.T, raw string) []logLine {
	t.Helper()
	var lines []logLine
	for _, text := range strings.Split(strings.TrimSpace(raw), "\n") {
		if text == "" {
			continue
		}
		var line logLine
		if err := json.Unmarshal([]byte(text), &line); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", text, err)
		}
		lines = append(lines, line)
	}
	return lines
}

// findLogs returns the records with the given message.
func findLogs(lines []logLine, message string) []logLine {
	var found []logLine
	for _, line := range lines {
		if line.str("message") == message {
			found = append(found, line)
		}
	}
	return found
}

// progressRun is the outcome of runProgress.
type progressRun struct {
	result Result
	err    error
	events []ProgressEvent
	lines  []logLine
	ops    []string
}

// finals returns the Final events, in order.
func (r progressRun) finals() []ProgressEvent {
	var finals []ProgressEvent
	for _, event := range r.events {
		if event.Final {
			finals = append(finals, event)
		}
	}
	return finals
}

// eventsFor returns the events of one stage, in order.
func (r progressRun) eventsFor(stage ProgressStage) []ProgressEvent {
	var out []ProgressEvent
	for _, event := range r.events {
		if event.Stage == stage {
			out = append(out, event)
		}
	}
	return out
}

// withTransforms enables -sync-managed with one transform named progressTransform
// on progressIndex; continuous adds a top-level sync to its body.
func withTransforms(t *testing.T, continuous bool, wait bool) func(*Options) {
	t.Helper()
	sync := ""
	if continuous {
		sync = `,"sync":{"time":{"field":"updated","delay":"1s"}}`
	}
	path := filepath.Join(t.TempDir(), "transforms.json")
	body := fmt.Sprintf(`{%q:{"source_index":%q,"body":{"source":{"index":%q},"dest":{"index":"progress-out"},"pivot":{"group_by":{"id":{"terms":{"field":"id"}}},"aggregations":{"n":{"value_count":{"field":"id"}}}}%s}}}`,
		progressTransform, progressIndex, progressIndex, sync)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write transforms: %v", err)
	}
	return func(o *Options) {
		o.SyncManaged = true
		o.TransformsFile = path
		o.Transforms.Wait = wait
	}
}

// withEnrich enables enrich for every policy.
func withEnrich(o *Options) { o.Enrich = EnrichOptions{Enabled: true, All: true} }

// runProgress runs the loader against server with a capturing logger at level
// and a Progress collector, applying mutations to the base options (an -add run
// of progressDefaultDoc into progressIndex).
func runProgress(t *testing.T, server *esstub.Stub, ops func() []string, level zerolog.Level, mutations ...func(*Options)) progressRun {
	t.Helper()
	return runProgressContext(t, t.Context(), server, ops, level, mutations...)
}

// runProgressContext is runProgress with a caller-supplied context.
func runProgressContext(t *testing.T, ctx context.Context, server *esstub.Stub, ops func() []string, level zerolog.Level, mutations ...func(*Options)) progressRun {
	t.Helper()

	dataFile := filepath.Join(t.TempDir(), "data.json")
	if err := os.WriteFile(dataFile, []byte(progressDefaultDoc), 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}
	var buf bytes.Buffer
	logger := zerolog.New(&buf).Level(level)
	run := progressRun{}
	options := Options{
		URL:        server.URL(),
		Index:      progressIndex,
		DataFile:   dataFile,
		AddToIndex: true,
		Logger:     &logger,
		Progress:   func(event ProgressEvent) { run.events = append(run.events, event) },
	}
	for _, mutate := range mutations {
		mutate(&options)
	}
	run.result, run.err = Run(ctx, options)
	run.lines = parseLogLines(t, buf.String())
	run.ops = ops()
	return run
}

// fixtureText reads a recorded Elasticsearch response and renames its
// transform id so it matches the stub's transform.
func fixtureText(t *testing.T, name string, renames ...string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "test", "fixtures", "elasticsearch", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	text := string(raw)
	for i := 0; i+1 < len(renames); i += 2 {
		text = strings.ReplaceAll(text, renames[i], renames[i+1])
	}
	return text
}

func indexOf(ops []string, op string) int {
	for i, current := range ops {
		if current == op {
			return i
		}
	}
	return -1
}

// ─── Transform stage ──────────────────────────────────────────────────────────

// TestProgressTransformWaitOffRequestsUnchanged guards the default: with the
// wait off the transform stage issues exactly today's requests and no _stats,
// and the callback sees one Final "started" transform event.
func TestProgressTransformWaitOffRequestsUnchanged(t *testing.T) {
	installFakeClock(t, time.Second)
	// No stats configured: a _stats request fails the test in the stub.
	server, ops := newProgressStub(t, nil)

	run := runProgress(t, server, ops, zerolog.InfoLevel, withTransforms(t, false, false))
	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}

	want := []string{"index.create", "bulk", "index.refresh", "transform.stop", "transform.get", "transform.put", "transform.start"}
	if !reflect.DeepEqual(run.ops, want) {
		t.Fatalf("operations = %v, want %v", run.ops, want)
	}
	transformEvents := run.eventsFor(ProgressStageTransform)
	wantEvent := ProgressEvent{Stage: ProgressStageTransform, Resource: progressTransform, Phase: "started", Done: -1, Total: -1, Final: true}
	if len(transformEvents) != 1 || !reflect.DeepEqual(transformEvents[0], wantEvent) {
		t.Fatalf("transform events = %+v, want exactly %+v", transformEvents, wantEvent)
	}
}

// TestProgressTransformWaitBatch waits on a batch transform through the
// recorded 9.3.1 _stats shapes: indexing, then stopped at checkpoint 1.
func TestProgressTransformWaitBatch(t *testing.T) {
	installFakeClock(t, time.Second)
	rename := []string{"esbl-progress-test-tx-batch", progressTransform}
	server, ops := newProgressStub(t, []string{
		fixtureText(t, "transform-stats-batch-indexing.json", rename...),
		fixtureText(t, "transform-stats-batch-stopped.json", rename...),
	})

	run := runProgress(t, server, ops, zerolog.InfoLevel, withTransforms(t, false, true))
	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}

	if start, stats := indexOf(run.ops, "transform.start"), indexOf(run.ops, "transform.stats"); start < 0 || stats < start {
		t.Fatalf("_stats must follow transform.start: %v", run.ops)
	}
	if got := operationCount(run.ops, "transform.stats"); got != 2 {
		t.Fatalf("_stats requests = %d, want 2", got)
	}
	completed := findLogs(run.lines, "Transform completed")
	if len(completed) != 1 {
		t.Fatalf("Transform completed lines = %d, want 1: %v", len(completed), run.lines)
	}
	if indexed, ok := completed[0].num("documents_indexed"); !ok || indexed != 500000 {
		t.Fatalf("documents_indexed = %v (present %v), want 500000", indexed, ok)
	}
	if _, ok := completed[0].num("elapsed_s"); !ok {
		t.Fatalf("Transform completed line lacks elapsed_s: %v", completed[0])
	}
	if got := findLogs(run.lines, "Waiting for transform"); len(got) != 1 || got[0].str("timeout") != "30m0s" {
		t.Fatalf("Waiting for transform = %v, want one line with the default 30m0s timeout", got)
	}

	transformEvents := run.eventsFor(ProgressStageTransform)
	if len(transformEvents) != 3 {
		t.Fatalf("transform events = %+v, want 2 polls and a Final", transformEvents)
	}
	if poll := transformEvents[0]; poll.Final || poll.Phase != "indexing" || poll.Done != 0 || poll.Total != -1 {
		t.Fatalf("first poll event = %+v, want non-final indexing, Done 0 (measured zero), Total -1 (batch has no total_docs)", poll)
	}
	if final := transformEvents[2]; !final.Final || final.Phase != "stopped" || final.Done != 500000 || final.Err != nil {
		t.Fatalf("final event = %+v", final)
	}
}

// TestProgressTransformWaitContinuous covers the continuous done rule.
func TestProgressTransformWaitContinuous(t *testing.T) {
	rename := []string{"esbl-progress-test-tx-continuous", progressTransform}
	tests := []struct {
		name      string
		stats     []string
		wantStats int
	}{
		{
			name: "recorded: done once next is gone even though state is started",
			stats: []string{
				fixtureText(t, "transform-stats-continuous-indexing.json", rename...),
				fixtureText(t, "transform-stats-continuous-started.json", rename...),
			},
			wantStats: 2,
		},
		{
			// Synthetic: idle at checkpoint 1 while the state still reads indexing.
			name:      "synthetic: checkpoint 1 without next is done while indexing",
			stats:     []string{`{"count":1,"transforms":[{"id":"tx1","state":"indexing","checkpointing":{"last":{"checkpoint":1}}}]}`},
			wantStats: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installFakeClock(t, time.Second)
			server, ops := newProgressStub(t, tt.stats)

			run := runProgress(t, server, ops, zerolog.InfoLevel, withTransforms(t, true, true))
			if run.err != nil {
				t.Fatalf("Run: %v", run.err)
			}
			if got := operationCount(run.ops, "transform.stats"); got != tt.wantStats {
				t.Fatalf("_stats requests = %d, want %d", got, tt.wantStats)
			}
			if got := findLogs(run.lines, "Waiting for transform"); len(got) != 1 || got[0]["continuous"] != true {
				t.Fatalf("expected a continuous wait, got %v", got)
			}
		})
	}
}

// TestProgressTransformWaitFailed: a failed transform fails the run with a
// managed-resource RunError that names the transform and quotes ES's reason,
// and the callback gets no transform Final (the run-ending rule).
func TestProgressTransformWaitFailed(t *testing.T) {
	installFakeClock(t, time.Second)
	server, ops := newProgressStub(t, []string{
		`{"count":1,"transforms":[{"id":"tx1","state":"failed","reason":"task encountered more than 10 failures; latest failure: boom","checkpointing":{"last":{"checkpoint":0}}}]}`,
	})

	run := runProgress(t, server, ops, zerolog.InfoLevel, withTransforms(t, false, true))

	var runErr *RunError
	if !errors.As(run.err, &runErr) || !errors.Is(run.err, ErrManagedResource) {
		t.Fatalf("error = %v, want *RunError of kind ErrManagedResource", run.err)
	}
	for _, want := range []string{"Transform wait failed", `"tx1"`, "task encountered more than 10 failures; latest failure: boom"} {
		if !strings.Contains(run.err.Error(), want) {
			t.Fatalf("error %q does not contain %q", run.err, want)
		}
	}
	for _, event := range run.eventsFor(ProgressStageTransform) {
		if event.Final {
			t.Fatalf("a failing transform must get no Final event, got %+v", event)
		}
	}
	if last := run.events[len(run.events)-1]; last.Stage != ProgressStageTransform || last.Final {
		t.Fatalf("no event may follow the failure; last event = %+v", last)
	}
	if len(findLogs(run.lines, "Transform completed")) != 0 {
		t.Fatal("a failed transform must not log completion")
	}
}

// TestProgressTransformWaitTimeout: the per-transform timeout fails the run
// with an error naming the transform. The sleep stub waits on its context, so
// only the derived timeout context can end the wait.
func TestProgressTransformWaitTimeout(t *testing.T) {
	installFakeClock(t, time.Second)
	sleepWithContext = func(ctx context.Context, _ time.Duration) error {
		<-ctx.Done()
		return ctx.Err()
	}
	server, ops := newProgressStub(t, []string{`{"count":1,"transforms":[{"id":"tx1","state":"indexing","checkpointing":{"last":{"checkpoint":0},"next":{"checkpoint":1}}}]}`})

	run := runProgress(t, server, ops, zerolog.InfoLevel, withTransforms(t, false, true), func(o *Options) {
		o.Transforms.WaitTimeout = time.Millisecond
	})

	if !errors.Is(run.err, ErrManagedResource) || !errors.Is(run.err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want ErrManagedResource wrapping context.DeadlineExceeded", run.err)
	}
	if !strings.Contains(run.err.Error(), `"tx1"`) || !strings.Contains(run.err.Error(), "timed out after 1ms") {
		t.Fatalf("error %q must name the transform and the timeout", run.err)
	}
	for _, event := range run.eventsFor(ProgressStageTransform) {
		if event.Final {
			t.Fatalf("a timed-out transform must get no Final event, got %+v", event)
		}
	}
}

// TestProgressTransformWaitWarnAndSkip: -wait-transforms with nothing synced
// warns and makes no _stats request.
func TestProgressTransformWaitWarnAndSkip(t *testing.T) {
	const message = "Ignoring transform wait because no transforms were synced for this run"
	otherSource := func(t *testing.T) func(*Options) {
		t.Helper()
		apply := withTransforms(t, false, true)
		return func(o *Options) {
			apply(o)
			// A transform declared for another source index is not selected.
			body, err := os.ReadFile(o.TransformsFile)
			if err != nil {
				t.Fatalf("read transforms: %v", err)
			}
			rewritten := strings.Replace(string(body), `"source_index":"`+progressIndex+`"`, `"source_index":"other-index"`, 1)
			if err := os.WriteFile(o.TransformsFile, []byte(rewritten), 0o600); err != nil {
				t.Fatalf("rewrite transforms: %v", err)
			}
		}
	}
	tests := []struct {
		name      string
		mutations func(*testing.T) []func(*Options)
	}{
		{name: "wait without sync-managed", mutations: func(*testing.T) []func(*Options) {
			return []func(*Options){func(o *Options) { o.Transforms.Wait = true }}
		}},
		{name: "no transform matches the source index", mutations: func(t *testing.T) []func(*Options) {
			return []func(*Options){otherSource(t)}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installFakeClock(t, time.Second)
			server, ops := newProgressStub(t, nil)

			run := runProgress(t, server, ops, zerolog.InfoLevel, tt.mutations(t)...)
			if run.err != nil {
				t.Fatalf("Run: %v", run.err)
			}
			if !slicesContain(run.result.Warnings, message) {
				t.Fatalf("Warnings = %v, want %q", run.result.Warnings, message)
			}
			if operationCount(run.ops, "transform.stats") != 0 {
				t.Fatalf("no _stats request is allowed, got %v", run.ops)
			}
		})
	}
}

func slicesContain(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// TestProgressTransformHeartbeat: while a wait outlasts the heartbeat
// interval the Info line "Transform still running" appears, and per-poll
// lines stay at Debug.
func TestProgressTransformHeartbeat(t *testing.T) {
	installFakeClock(t, 31*time.Second)
	indexing := `{"count":1,"transforms":[{"id":"tx1","state":"indexing","stats":{"documents_processed":10},"checkpointing":{"last":{"checkpoint":0},"next":{"checkpoint":1}}}]}`
	done := `{"count":1,"transforms":[{"id":"tx1","state":"stopped","stats":{"documents_processed":20,"documents_indexed":20},"checkpointing":{"last":{"checkpoint":1}}}]}`
	server, ops := newProgressStub(t, []string{indexing, indexing, indexing, done})

	run := runProgress(t, server, ops, zerolog.InfoLevel, withTransforms(t, false, true))
	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}
	beats := findLogs(run.lines, "Transform still running")
	if len(beats) != 3 {
		t.Fatalf("heartbeats = %d, want one per later poll (3; the first poll is at t=0): %v", len(beats), beats)
	}
	if beats[0].str("level") != "info" || beats[0].str("state") != "indexing" {
		t.Fatalf("heartbeat = %v, want an Info line with the state", beats[0])
	}
	if len(findLogs(run.lines, "Transform in progress")) != 0 {
		t.Fatal("per-poll lines must not appear at Info level")
	}
}

// TestProgressTransformMissingMeasuredValues: an absent documents_processed
// stays -1 in events and is left out of the log; an explicit 0 is reported
// as a real measurement.
func TestProgressTransformMissingMeasuredValues(t *testing.T) {
	tests := []struct {
		name          string
		stats         string
		wantDone      int64
		wantLogged    bool
		wantLoggedNum float64
	}{
		{
			name:     "absent counter stays unknown",
			stats:    `{"count":1,"transforms":[{"id":"tx1","state":"stopped","checkpointing":{"last":{"checkpoint":1}}}]}`,
			wantDone: -1,
		},
		{
			name:     "null counter stays unknown",
			stats:    `{"count":1,"transforms":[{"id":"tx1","state":"stopped","stats":{"documents_processed":null},"checkpointing":{"last":{"checkpoint":1}}}]}`,
			wantDone: -1,
		},
		{
			name:       "explicit zero is a measurement",
			stats:      `{"count":1,"transforms":[{"id":"tx1","state":"stopped","stats":{"documents_processed":0,"documents_indexed":0},"checkpointing":{"last":{"checkpoint":1}}}]}`,
			wantDone:   0,
			wantLogged: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installFakeClock(t, time.Second)
			server, ops := newProgressStub(t, []string{tt.stats})

			run := runProgress(t, server, ops, zerolog.InfoLevel, withTransforms(t, false, true))
			if run.err != nil {
				t.Fatalf("Run: %v", run.err)
			}
			final := run.finals()[len(run.finals())-1]
			if final.Stage != ProgressStageTransform || final.Done != tt.wantDone {
				t.Fatalf("final = %+v, want a transform Final with Done %d", final, tt.wantDone)
			}
			completed := findLogs(run.lines, "Transform completed")
			if len(completed) != 1 {
				t.Fatalf("Transform completed lines = %d", len(completed))
			}
			value, logged := completed[0].num("documents_processed")
			if logged != tt.wantLogged || (logged && value != tt.wantLoggedNum) {
				t.Fatalf("documents_processed logged=%v value=%v, want logged=%v value=%v", logged, value, tt.wantLogged, tt.wantLoggedNum)
			}
		})
	}
}

// ─── Enrich stage ─────────────────────────────────────────────────────────────

// TestProgressEnrichVisibility is the priority behavior: at the default Info
// level a policy logs its start, its task ID, each step change, a heartbeat
// with document counts during reindex, and its completion; the events carry
// the child reindex counters during the reindex step and -1 afterwards. The
// task and child responses are the recorded 9.3.1 ones.
func TestProgressEnrichVisibility(t *testing.T) {
	installFakeClock(t, 31*time.Second)
	server, ops := newProgressStub(t, nil,
		withEnrichTask(
			stubReply{200, fixtureText(t, "enrich-task-step-reindex.json")},
			stubReply{200, fixtureText(t, "enrich-task-step-reindex.json")},
			stubReply{200, fixtureText(t, "enrich-task-step-force-merge.json")},
			// After completion ES answers 404 for the task (recorded).
			stubReply{404, fixtureText(t, "enrich-task-not-found.json")},
		),
		withChildren(
			stubReply{200, fixtureText(t, "enrich-task-children-reindex-start.json")},
			stubReply{200, fixtureText(t, "enrich-task-children-reindex-progress.json")},
		),
	)

	run := runProgress(t, server, ops, zerolog.InfoLevel, withEnrich)
	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}

	executing := findLogs(run.lines, "Executing enrich policy")
	if len(executing) != 1 || executing[0].str("policy") != "policy-a" {
		t.Fatalf("Executing enrich policy = %v", executing)
	}
	if position, _ := executing[0].num("position"); position != 1 {
		t.Fatalf("position = %v, want 1", position)
	}
	if of, _ := executing[0].num("of"); of != 1 {
		t.Fatalf("of = %v, want 1", of)
	}
	if started := findLogs(run.lines, "Enrich policy task started"); len(started) != 1 || started[0].str("task") != progressTaskID {
		t.Fatalf("Enrich policy task started = %v", started)
	}

	var steps []string
	for _, line := range findLogs(run.lines, "Enrich policy step") {
		steps = append(steps, line.str("step"))
	}
	if want := []string{"reindex", "force-merge"}; !reflect.DeepEqual(steps, want) {
		t.Fatalf("step lines = %v, want exactly one per distinct step %v", steps, want)
	}

	beats := findLogs(run.lines, "Enrich policy execution still running")
	if len(beats) != 3 {
		t.Fatalf("heartbeats = %d, want 3 (one per 31s poll): %v", len(beats), beats)
	}
	for i, wantDone := range []float64{0, 20000} {
		done, okDone := beats[i].num("docs_done")
		total, okTotal := beats[i].num("docs_total")
		if !okDone || !okTotal || done != wantDone || total != 500000 {
			t.Fatalf("heartbeat %d docs = %v/%v (present %v/%v), want %v/500000", i, done, total, okDone, okTotal, wantDone)
		}
	}
	if _, ok := beats[2].num("docs_done"); ok {
		t.Fatalf("outside the reindex step the heartbeat must not claim document counts: %v", beats[2])
	}

	succeeded := findLogs(run.lines, "Enrich policy execution succeeded")
	if len(succeeded) != 1 || succeeded[0].str("level") != "info" {
		t.Fatalf("success line = %v, want one at Info", succeeded)
	}
	if len(findLogs(run.lines, "Enrich policy execution in progress")) != 0 {
		t.Fatal("per-poll lines must stay at Debug")
	}

	enrichEvents := run.eventsFor(ProgressStageEnrich)
	type shape struct {
		phase       string
		done, total int64
		final       bool
	}
	var got []shape
	for _, event := range enrichEvents {
		got = append(got, shape{event.Phase, event.Done, event.Total, event.Final})
	}
	want := []shape{
		{"reindex", 0, 500000, false},
		{"reindex", 20000, 500000, false},
		{"force-merge", -1, -1, false},
		{"", -1, -1, true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("enrich events = %+v, want %+v", got, want)
	}
	if enrichEvents[3].Err != nil || enrichEvents[0].Resource != "policy-a" {
		t.Fatalf("unexpected events: %+v", enrichEvents)
	}
	if run.result.EnrichSucceeded != 1 || run.result.EnrichFailed != 0 {
		t.Fatalf("result = %+v", run.result)
	}
}

// TestProgressEnrichHeartbeatThrottle: with a poll every 10s the heartbeat
// fires at most once per 30s, driven by the injected clock.
func TestProgressEnrichHeartbeatThrottle(t *testing.T) {
	installFakeClock(t, 10*time.Second)
	reindex := stubReply{200, fixtureText(t, "enrich-task-step-reindex.json")}
	server, ops := newProgressStub(t, nil,
		withEnrichTask(reindex, reindex, reindex, reindex, reindex, reindex, reindex, stubReply{404, fixtureText(t, "enrich-task-not-found.json")}),
		withChildren(stubReply{200, fixtureText(t, "enrich-task-children-reindex-progress.json")}),
	)

	run := runProgress(t, server, ops, zerolog.InfoLevel, withEnrich)
	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}
	// Seven polls at t=10..70s after a heartbeat start at t=0: due at 30s and 60s.
	beats := findLogs(run.lines, "Enrich policy execution still running")
	if len(beats) != 2 {
		t.Fatalf("heartbeats = %d, want 2 (at 30s and 60s): %v", len(beats), beats)
	}
	if percent, ok := beats[0].num("percent"); !ok || percent != 4 {
		t.Fatalf("percent = %v (present %v), want 4 (20000 of 500000)", percent, ok)
	}
}

// TestProgressEnrichChildTaskFailureIsBestEffort: an unreadable child-task
// list neither fails the policy nor logs above Debug; the events say unknown.
func TestProgressEnrichChildTaskFailureIsBestEffort(t *testing.T) {
	reindexTask := fixtureText(t, "enrich-task-step-reindex.json")
	notFound := stubReply{404, fixtureText(t, "enrich-task-not-found.json")}
	tests := []struct {
		name     string
		children []stubReply
	}{
		{name: "server error", children: []stubReply{{500, `{"error":{"type":"boom"}}`}}},
		{name: "malformed json", children: []stubReply{{200, `{"tasks":[`}}},
		{name: "no child task", children: []stubReply{{200, `{"tasks":[]}`}}},
		{name: "two reindex children are ambiguous", children: []stubReply{{200, `{"tasks":[` +
			`{"action":"cluster:admin/xpack/enrich/reindex","status":{"total":9,"created":1,"updated":0,"deleted":0,"noops":0}},` +
			`{"action":"cluster:admin/xpack/enrich/reindex","status":{"total":9,"created":2,"updated":0,"deleted":0,"noops":0}}]}`}}},
		{name: "child without total", children: []stubReply{{200, `{"tasks":[{"action":"cluster:admin/xpack/enrich/reindex","status":{"created":1}}]}`}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installFakeClock(t, 31*time.Second)
			server, ops := newProgressStub(t, nil,
				withEnrichTask(stubReply{200, reindexTask}, stubReply{200, reindexTask}, notFound),
				withChildren(tt.children...),
			)

			run := runProgress(t, server, ops, zerolog.DebugLevel, withEnrich)
			if run.err != nil {
				t.Fatalf("Run: %v", run.err)
			}
			if run.result.EnrichSucceeded != 1 || run.result.EnrichFailed != 0 {
				t.Fatalf("result = %+v, want the policy to succeed", run.result)
			}
			polls := 0
			for _, event := range run.eventsFor(ProgressStageEnrich) {
				if !event.Final {
					polls++
					if event.Phase != "reindex" || event.Done != -1 || event.Total != -1 {
						t.Fatalf("poll event = %+v, want reindex with unknown -1/-1", event)
					}
				}
			}
			if polls != 2 {
				t.Fatalf("poll events = %d, want 2", polls)
			}
			for _, line := range run.lines {
				if level := line.str("level"); level == "error" || level == "warn" || level == "fatal" {
					t.Fatalf("the progress path must not log %s: %v", level, line)
				}
			}
			if len(findLogs(run.lines, "Enrich policy execution succeeded")) != 1 {
				t.Fatal("expected the success line")
			}
			diagnostics := 0
			for _, line := range run.lines {
				if strings.HasPrefix(line.str("message"), "Enrich reindex progress unavailable") && line.str("level") == "debug" {
					diagnostics++
				}
			}
			if diagnostics != 2 {
				t.Fatalf("debug diagnostics = %d, want one per reindex poll (2)", diagnostics)
			}
		})
	}
}

// TestProgressEnrichWithoutStepField: an older or synchronous response with
// no step produces no step lines, no child-task request and ES's phase as the
// event phase.
func TestProgressEnrichWithoutStepField(t *testing.T) {
	installFakeClock(t, 31*time.Second)
	running := `{"completed":false,"task":{"status":{"phase":"RUNNING"}}}`
	done := `{"completed":true,"task":{"status":{"phase":"COMPLETE"}},"response":{"status":{"phase":"COMPLETE"}}}`
	server, ops := newProgressStub(t, nil, withEnrichTask(stubReply{200, running}, stubReply{200, done}))

	run := runProgress(t, server, ops, zerolog.InfoLevel, withEnrich)
	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}
	if len(findLogs(run.lines, "Enrich policy step")) != 0 {
		t.Fatal("no step lines are expected without a step field")
	}
	if operationCount(run.ops, "enrich.children") != 0 {
		t.Fatalf("no child-task request is expected without a reindex step: %v", run.ops)
	}
	polls := run.eventsFor(ProgressStageEnrich)
	if len(polls) != 2 || polls[0].Phase != "RUNNING" || polls[0].Done != -1 || polls[0].Total != -1 {
		t.Fatalf("enrich events = %+v, want a RUNNING poll then a Final", polls)
	}
	if run.result.EnrichSucceeded != 1 {
		t.Fatalf("result = %+v", run.result)
	}
}

// TestProgressEnrichOutcomes pins the truthful outcome and exactly one Final
// event per policy for every way a policy can end, under the existing
// count-and-continue policy. The retry rows pin FEAT-WAITRETRY-01 through the
// public Run: two transient task polls are retried with one Warn each and no
// extra kickoff, the third consecutive one fails the policy, and neither adds
// to Result.Warnings.
func TestProgressEnrichOutcomes(t *testing.T) {
	failedPhase := `{"status":{"phase":"FAILED"}}`
	unavailable := stubReply{http.StatusServiceUnavailable, `{"error":"unavailable"}`}
	tests := []struct {
		name        string
		opts        []progressStubOption
		cancel      bool
		wantFailed  bool
		wantSuccess bool // the success line is logged
		// Retry rows only (wantTaskPolls > 0): task polls, retry Warns and
		// the terminal poll Error's attempt (0 for none).
		wantTaskPolls    int
		wantRetryWarns   int
		wantErrorAttempt int
	}{
		{
			name:           "transient task polls then completed recovers",
			opts:           []progressStubOption{withEnrichTask(unavailable, unavailable, stubReply{200, `{"completed":true,"response":{"status":{"phase":"COMPLETE"}}}`})},
			wantSuccess:    true,
			wantTaskPolls:  3,
			wantRetryWarns: 2,
		},
		{
			name:             "third consecutive transient task poll fails the policy",
			opts:             []progressStubOption{withEnrichTask(unavailable)},
			wantFailed:       true,
			wantTaskPolls:    3,
			wantRetryWarns:   2,
			wantErrorAttempt: 3,
		},
		{name: "synchronous success", opts: nil, wantSuccess: true},
		{name: "synchronous failed phase", opts: []progressStubOption{withExecuteBody(failedPhase)}, wantFailed: true},
		{
			// The stub's default alias sequence moves the backing alias.
			name:        "task 404 with changed backing alias succeeds",
			opts:        []progressStubOption{withEnrichTask(stubReply{404, fixtureText(t, "enrich-task-not-found.json")})},
			wantSuccess: true,
		},
		{
			name: "task 404 with unchanged backing alias fails",
			opts: []progressStubOption{
				withEnrichTask(stubReply{404, fixtureText(t, "enrich-task-not-found.json")}),
				withEnrichAliasReplies("policy-a", enrichAliasAt("policy-a", ".enrich-policy-a-1")),
			},
			wantFailed: true,
		},
		{
			name: "task 404 with absent backing alias fails",
			opts: []progressStubOption{
				withEnrichTask(stubReply{404, fixtureText(t, "enrich-task-not-found.json")}),
				withEnrichAliasReplies("policy-a", enrichAliasMissing("policy-a")),
			},
			wantFailed: true,
		},
		{
			name: "task 404 with unreadable backing alias fails",
			opts: []progressStubOption{
				withEnrichTask(stubReply{404, fixtureText(t, "enrich-task-not-found.json")}),
				withEnrichAliasReplies("policy-a", enrichAliasMissing("policy-a"), stubReply{http.StatusForbidden, `{"error":{"type":"security_exception"},"status":403}`}),
			},
			wantFailed: true,
		},
		{
			name:        "completed task with a failed response status",
			opts:        []progressStubOption{withEnrichTask(stubReply{200, `{"completed":true,"response":{"status":{"phase":"FAILED"}}}`})},
			wantFailed:  true,
			wantSuccess: false,
		},
		{
			name:       "task poll returns a server error",
			opts:       []progressStubOption{withEnrichTask(stubReply{500, `{"error":{"type":"boom"}}`})},
			wantFailed: true,
		},
		{
			name:       "completed task carries an error",
			opts:       []progressStubOption{withEnrichTask(stubReply{200, `{"completed":true,"error":{"type":"enrich_policy_execution_exception","reason":"boom"},"task":{}}`})},
			wantFailed: true,
		},
		{
			name:       "malformed task response",
			opts:       []progressStubOption{withEnrichTask(stubReply{200, `{"completed":`})},
			wantFailed: true,
		},
		{
			name:       "context cancelled while polling",
			opts:       []progressStubOption{withEnrichTask(stubReply{200, `{"completed":false,"task":{"status":{"phase":"RUNNING"}}}`})},
			cancel:     true,
			wantFailed: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := installFakeClock(t, time.Second)
			server, ops := newProgressStub(t, nil, tt.opts...)

			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			if tt.cancel {
				// The first sleep inside executeEnrichPolicy cancels the run's context.
				sleepWithContext = func(c context.Context, d time.Duration) error {
					cancel()
					return clock.Sleep(c, d)
				}
			}
			run := runProgressContext(t, ctx, server, ops, zerolog.InfoLevel, withEnrich)
			if run.err != nil {
				t.Fatalf("Run: %v", run.err)
			}

			enrichFinals := 0
			for _, event := range run.eventsFor(ProgressStageEnrich) {
				if !event.Final {
					continue
				}
				enrichFinals++
				if (event.Err != nil) != tt.wantFailed {
					t.Fatalf("final = %+v, want failed=%v", event, tt.wantFailed)
				}
			}
			if enrichFinals != 1 {
				t.Fatalf("enrich Final events = %d, want exactly 1", enrichFinals)
			}
			wantFailedCount := 0
			if tt.wantFailed {
				wantFailedCount = 1
			}
			if run.result.EnrichFailed != wantFailedCount || run.result.EnrichSucceeded != 1-wantFailedCount {
				t.Fatalf("result = %+v, want EnrichFailed=%d", run.result, wantFailedCount)
			}
			if got := len(findLogs(run.lines, "Enrich policy execution succeeded")) == 1; got != tt.wantSuccess {
				t.Fatalf("success line logged = %v, want %v: %v", got, tt.wantSuccess, run.lines)
			}
			if tt.wantTaskPolls == 0 {
				return
			}

			if got := operationCount(run.ops, "enrich.task"); got != tt.wantTaskPolls {
				t.Fatalf("enrich.task requests = %d, want %d: %v", got, tt.wantTaskPolls, run.ops)
			}
			if got := operationCount(run.ops, "enrich.execute"); got != 1 {
				t.Fatalf("enrich.execute requests = %d, want 1: a retry never repeats the kickoff: %v", got, run.ops)
			}
			warns := findLogs(run.lines, "Enrich policy task poll failed; retrying")
			if len(warns) != tt.wantRetryWarns {
				t.Fatalf("retry warns = %d, want %d: %v", len(warns), tt.wantRetryWarns, run.lines)
			}
			for i, line := range warns {
				status, _ := line.num("status_code")
				attempt, _ := line.num("attempt")
				if line.str("level") != "warn" || line.str("policy") != "policy-a" || line.str("task") != progressTaskID ||
					status != http.StatusServiceUnavailable || attempt != float64(i+1) {
					t.Fatalf("retry warn %d = %v, want policy-a, task %s, status 503, attempt %d", i, line, progressTaskID, i+1)
				}
			}
			pollErrors := findLogs(run.lines, "Enrich policy task poll returned an error")
			if tt.wantErrorAttempt == 0 {
				if len(pollErrors) != 0 {
					t.Fatalf("poll error lines = %v, want none after a recovery", pollErrors)
				}
			} else {
				if len(pollErrors) != 1 {
					t.Fatalf("poll error lines = %v, want exactly one", pollErrors)
				}
				status, _ := pollErrors[0].num("status_code")
				attempt, _ := pollErrors[0].num("attempt")
				if pollErrors[0].str("level") != "error" || status != http.StatusServiceUnavailable || attempt != float64(tt.wantErrorAttempt) {
					t.Fatalf("poll error line = %v, want error level, status 503, attempt %d", pollErrors[0], tt.wantErrorAttempt)
				}
			}
			// A retry Warn is a log line only; it must never reach Result.Warnings.
			if len(run.result.Warnings) != 0 {
				t.Fatalf("Warnings = %v, want none for this run", run.result.Warnings)
			}
		})
	}
}

// TestProgressEnrichAliasFailureContinues guards enrich's count-and-continue
// contract for an unverified task 404 (FIX-ENRICH404-01): policy-a's alias
// does not move, so it fails with one Final carrying Err; policy-b's alias
// appears, so it still executes and succeeds; Run returns nil. Serial: it
// swaps the loader's clock seams.
func TestProgressEnrichAliasFailureContinues(t *testing.T) {
	installFakeClock(t, time.Second)
	server, ops := newProgressStub(t, nil,
		withPolicies("policy-a", "policy-b"),
		withEnrichTask(stubReply{404, fixtureText(t, "enrich-task-not-found.json")}),
		withEnrichAliasReplies("policy-a", enrichAliasAt("policy-a", ".enrich-policy-a-1")),
		withEnrichAliasReplies("policy-b", enrichAliasMissing("policy-b"), enrichAliasAt("policy-b", ".enrich-policy-b-2")),
	)

	run := runProgress(t, server, ops, zerolog.InfoLevel, withEnrich)
	if run.err != nil {
		t.Fatalf("Run: %v, want nil: a failed policy does not end the Run", run.err)
	}

	var executing []string
	for _, line := range findLogs(run.lines, "Executing enrich policy") {
		executing = append(executing, line.str("policy"))
	}
	if want := []string{"policy-a", "policy-b"}; !reflect.DeepEqual(executing, want) {
		t.Fatalf("executed policies = %v, want %v", executing, want)
	}
	if got := operationCount(run.ops, "enrich.execute"); got != 2 {
		t.Fatalf("enrich.execute requests = %d, want 2 (policy-b still kicked off): %v", got, run.ops)
	}

	finals := map[string][]ProgressEvent{}
	for _, event := range run.eventsFor(ProgressStageEnrich) {
		if event.Final {
			finals[event.Resource] = append(finals[event.Resource], event)
		}
	}
	if len(finals["policy-a"]) != 1 || finals["policy-a"][0].Err == nil {
		t.Fatalf("policy-a finals = %+v, want exactly one with Err set", finals["policy-a"])
	}
	if len(finals["policy-b"]) != 1 || finals["policy-b"][0].Err != nil {
		t.Fatalf("policy-b finals = %+v, want exactly one with Err nil", finals["policy-b"])
	}
	if run.result.EnrichFailed != 1 || run.result.EnrichSucceeded != 1 {
		t.Fatalf("result = %+v, want 1 failed and 1 succeeded", run.result)
	}

	notMoved := findLogs(run.lines, "Enrich policy execution failed: the backing alias did not move")
	if len(notMoved) != 1 || notMoved[0].str("policy") != "policy-a" || notMoved[0].str("level") != "error" {
		t.Fatalf("alias-not-moved lines = %v, want one Error for policy-a", notMoved)
	}
	succeeded := findLogs(run.lines, "Enrich policy execution succeeded")
	if len(succeeded) != 1 || succeeded[0].str("policy") != "policy-b" {
		t.Fatalf("success lines = %v, want one for policy-b", succeeded)
	}
}

// ─── Bulk stage ───────────────────────────────────────────────────────────────

// TestProgressBulkBatches pins the bulk events for both data formats: one
// non-final event per batch with the count-pass total, then one Final.
func TestProgressBulkBatches(t *testing.T) {
	tests := []struct {
		name      string
		data      string
		batch     int
		failItems int
		want      []ProgressEvent // Stage, Resource, Done, Total, Final
		wantFail  int
	}{
		{
			name: "json array in three batches", data: `[{"id":1},{"id":2},{"id":3},{"id":4},{"id":5}]`, batch: 2,
			want: []ProgressEvent{{Done: 2, Total: 5}, {Done: 4, Total: 5}, {Done: 5, Total: 5}, {Done: 5, Total: 5, Final: true}},
		},
		{
			name: "ndjson with CRLF line endings", data: "{\"id\":1}\r\n{\"id\":2}\r\n{\"id\":3}\r\n", batch: 2,
			want: []ProgressEvent{{Done: 2, Total: 3}, {Done: 3, Total: 3}, {Done: 3, Total: 3, Final: true}},
		},
		{
			name: "empty array has only a Final", data: `[]`, batch: 2,
			want: []ProgressEvent{{Done: 0, Total: 0, Final: true}},
		},
		{
			// Item failures are partial: counted in the Result, never an Err.
			name: "partial item failures leave Err nil", data: `[{"id":1},{"id":2}]`, batch: 2, failItems: 1,
			want:     []ProgressEvent{{Done: 2, Total: 2}, {Done: 2, Total: 2, Final: true}},
			wantFail: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installFakeClock(t, time.Second)
			server, ops := newProgressStub(t, nil, withBulk(0, tt.failItems))

			run := runProgress(t, server, ops, zerolog.InfoLevel, func(o *Options) {
				path := filepath.Join(t.TempDir(), "data.json")
				if err := os.WriteFile(path, []byte(tt.data), 0o600); err != nil {
					t.Fatalf("write data: %v", err)
				}
				o.DataFile = path
				o.BatchSize = tt.batch
			})
			if run.err != nil {
				t.Fatalf("Run: %v", run.err)
			}
			if run.result.DocumentsFailed != tt.wantFail {
				t.Fatalf("DocumentsFailed = %d, want %d", run.result.DocumentsFailed, tt.wantFail)
			}
			if len(run.events) != len(tt.want) {
				t.Fatalf("events = %+v, want %d events", run.events, len(tt.want))
			}
			for i, want := range tt.want {
				got := run.events[i]
				if got.Stage != ProgressStageBulk || got.Resource != progressIndex || got.Done != want.Done || got.Total != want.Total || got.Final != want.Final || got.Err != nil {
					t.Fatalf("event %d = %+v, want bulk %s done=%d total=%d final=%v err=nil", i, got, progressIndex, want.Done, want.Total, want.Final)
				}
			}
		})
	}
}

// TestProgressHardBulkFailureEmitsNothing: a bulk request that ends the Run
// gets no event at all, in particular no Final; Run's error is the signal.
func TestProgressHardBulkFailureEmitsNothing(t *testing.T) {
	installFakeClock(t, time.Second)
	server, ops := newProgressStub(t, nil, withBulk(http.StatusBadRequest, 0))

	run := runProgress(t, server, ops, zerolog.InfoLevel, withEnrich)
	if !errors.Is(run.err, ErrBulkFailure) {
		t.Fatalf("error = %v, want ErrBulkFailure", run.err)
	}
	if len(run.events) != 0 {
		t.Fatalf("a failed Run must emit no events for the failing stage or after it, got %+v", run.events)
	}
}

// ─── Whole-run contract ───────────────────────────────────────────────────────

// TestProgressCallbackOrderingAndFinals runs bulk, enrich and a waited
// transform, then checks one Final per (Stage, Resource) in lifecycle order,
// nothing after a Final for the same pair, and -1 where documented.
func TestProgressCallbackOrderingAndFinals(t *testing.T) {
	installFakeClock(t, time.Second)
	reindex := stubReply{200, fixtureText(t, "enrich-task-step-reindex.json")}
	server, ops := newProgressStub(t, []string{
		fixtureText(t, "transform-stats-batch-indexing.json", "esbl-progress-test-tx-batch", progressTransform),
		fixtureText(t, "transform-stats-batch-stopped.json", "esbl-progress-test-tx-batch", progressTransform),
	},
		withEnrichTask(reindex, stubReply{200, fixtureText(t, "enrich-task-step-force-merge.json")}, stubReply{404, fixtureText(t, "enrich-task-not-found.json")}),
		withChildren(stubReply{200, fixtureText(t, "enrich-task-children-reindex-progress.json")}),
	)

	run := runProgress(t, server, ops, zerolog.InfoLevel, withEnrich, withTransforms(t, false, true))
	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}

	type key struct {
		stage    ProgressStage
		resource string
	}
	finals := run.finals()
	wantOrder := []key{{ProgressStageBulk, progressIndex}, {ProgressStageEnrich, "policy-a"}, {ProgressStageTransform, progressTransform}}
	if len(finals) != len(wantOrder) {
		t.Fatalf("finals = %+v, want %d", finals, len(wantOrder))
	}
	for i, want := range wantOrder {
		if got := (key{finals[i].Stage, finals[i].Resource}); got != want {
			t.Fatalf("final %d = %v, want %v (lifecycle order)", i, got, want)
		}
	}

	closed := map[key]bool{}
	for _, event := range run.events {
		k := key{event.Stage, event.Resource}
		if closed[k] {
			t.Fatalf("event %+v follows the Final for %v", event, k)
		}
		if event.Final {
			closed[k] = true
		}
		if event.Stage == ProgressStageEnrich && event.Phase == "force-merge" && (event.Done != -1 || event.Total != -1) {
			t.Fatalf("enrich outside reindex must be -1/-1: %+v", event)
		}
		if event.Stage == ProgressStageEnrich && event.Phase == "reindex" && (event.Done != 20000 || event.Total != 500000) {
			t.Fatalf("enrich reindex event = %+v, want 20000/500000", event)
		}
	}
	// Stage order across the stream: bulk events, then enrich, then transform.
	rank := map[ProgressStage]int{ProgressStageBulk: 0, ProgressStageEnrich: 1, ProgressStageTransform: 2}
	for i := 1; i < len(run.events); i++ {
		if rank[run.events[i].Stage] < rank[run.events[i-1].Stage] {
			t.Fatalf("stages out of lifecycle order at %d: %+v", i, run.events)
		}
	}
}

// TestProgressNilCallbackIsNoOp: the same run with Progress nil succeeds and
// produces the same Result as with a callback.
func TestProgressNilCallbackIsNoOp(t *testing.T) {
	installFakeClock(t, time.Second)
	stats := []string{fixtureText(t, "transform-stats-batch-stopped.json", "esbl-progress-test-tx-batch", progressTransform)}

	withCallbackServer, withCallbackOps := newProgressStub(t, stats)
	withCallback := runProgress(t, withCallbackServer, withCallbackOps, zerolog.InfoLevel, withEnrich, withTransforms(t, false, true))
	nilServer, nilOps := newProgressStub(t, stats)
	withoutCallback := runProgress(t, nilServer, nilOps, zerolog.InfoLevel, withEnrich, withTransforms(t, false, true), func(o *Options) { o.Progress = nil })

	if withCallback.err != nil || withoutCallback.err != nil {
		t.Fatalf("errors: with=%v without=%v", withCallback.err, withoutCallback.err)
	}
	if len(withoutCallback.events) != 0 {
		t.Fatalf("a nil callback must receive nothing, got %+v", withoutCallback.events)
	}
	if !reflect.DeepEqual(withCallback.result, withoutCallback.result) {
		t.Fatalf("results differ: %+v vs %+v", withCallback.result, withoutCallback.result)
	}
	if !reflect.DeepEqual(withCallback.ops, withoutCallback.ops) {
		t.Fatalf("requests differ: %v vs %v", withCallback.ops, withoutCallback.ops)
	}
}

// TestProgressResourceNamesDoNotCollide: a policy named like the bulk index
// still gets its own events; each Final closes only its (Stage, Resource).
func TestProgressResourceNamesDoNotCollide(t *testing.T) {
	installFakeClock(t, time.Second)
	server, ops := newProgressStub(t, nil, withPolicies(progressIndex))

	run := runProgress(t, server, ops, zerolog.InfoLevel, withEnrich)
	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}
	finals := run.finals()
	if len(finals) != 2 || finals[0].Stage != ProgressStageBulk || finals[1].Stage != ProgressStageEnrich {
		t.Fatalf("finals = %+v, want a bulk Final then an enrich Final", finals)
	}
	if finals[0].Resource != progressIndex || finals[1].Resource != progressIndex {
		t.Fatalf("both finals must carry the shared name: %+v", finals)
	}
}

// TestProgressStageAdapterMapsEverySnapshotField guards the single
// Snapshot-to-ProgressEvent mapping shared by the enrich, transform and bulk
// boundaries: every field crosses unchanged under the fixed stage, and a nil
// callback creates nothing.
func TestProgressStageAdapterMapsEverySnapshotField(t *testing.T) {
	t.Parallel()

	cause := errors.New("policy failed")
	snapshot := progress.Snapshot{Resource: "r", Phase: "p", Done: 3, Total: -1, Elapsed: 2 * time.Second, Final: true, Err: cause}
	for _, stage := range []ProgressStage{ProgressStageBulk, ProgressStageEnrich, ProgressStageTransform} {
		var got []ProgressEvent
		progressEmitter(func(e ProgressEvent) { got = append(got, e) }).stage(stage)(snapshot)
		want := ProgressEvent{Stage: stage, Resource: "r", Phase: "p", Done: 3, Total: -1, Elapsed: 2 * time.Second, Final: true, Err: cause}
		if len(got) != 1 || got[0] != want {
			t.Fatalf("stage %s: events = %+v, want [%+v]", stage, got, want)
		}
	}
	progressEmitter(nil).stage(ProgressStageBulk)(snapshot) // must not panic
}

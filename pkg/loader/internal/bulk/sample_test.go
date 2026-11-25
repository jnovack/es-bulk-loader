package bulk

// Guards for the bulk rejection sample (loader-observability phase 3): the
// first failed items in response order across batches, bounded by the caller's
// limit, sanitized for every string field, and independent of the ten-per-batch
// log cap. Responses are scripted per request with json.Marshal so hostile
// strings stay valid JSON. No live Elasticsearch is used.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/logbuf"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

// sampleItem scripts one _bulk response item. A nil errObj yields a null/absent
// error, so a status >= 300 item exercises the empty Type/Reason case.
type sampleItem struct {
	index, id, action string
	status            int
	errObj            map[string]string
}

func (s sampleItem) body() map[string]any {
	action := s.action
	if action == "" {
		action = "index"
	}
	idx := s.index
	if idx == "" {
		idx = "cards"
	}
	b := map[string]any{"_index": idx, "_id": s.id, "status": s.status}
	if s.errObj != nil {
		b["error"] = s.errObj
	}
	return map[string]any{action: b}
}

func okItem(id string) sampleItem { return sampleItem{id: id, status: 201} }

func failItem(id string) sampleItem {
	return sampleItem{id: id, status: 400, errObj: map[string]string{"type": "mapper_parsing_exception", "reason": "bad " + id}}
}

// scriptedBulk answers the n-th _bulk request with replies[n]. A request past
// the script gets HTTP 400, which is a hard bulk failure.
func scriptedBulk(t *testing.T, stub *esstub.Stub, replies [][]sampleItem) *atomic.Int32 {
	t.Helper()
	return scriptedBulkErrorsFlag(t, stub, true, replies)
}

// scriptedBulkErrorsFlag is scriptedBulk with the response's top-level "errors"
// flag set explicitly, so a test can script a response whose flag disagrees
// with its items.
func scriptedBulkErrorsFlag(t *testing.T, stub *esstub.Stub, errorsFlag bool, replies [][]sampleItem) *atomic.Int32 {
	t.Helper()
	bodies := make([][]byte, len(replies))
	for i, items := range replies {
		raw := make([]map[string]any, len(items))
		for j, it := range items {
			raw[j] = it.body()
		}
		b, err := json.Marshal(map[string]any{"errors": errorsFlag, "items": raw})
		if err != nil {
			t.Fatal(err)
		}
		bodies[i] = b
	}
	var calls atomic.Int32
	stub.Handle("POST /_bulk", func(w http.ResponseWriter, _ *http.Request) {
		n := int(calls.Add(1)) - 1
		if n >= len(bodies) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"type":"script_exhausted","reason":"unscripted request"},"status":400}`))
			return
		}
		_, _ = w.Write(bodies[n])
	})
	return &calls
}

func writeData(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// dataFiles returns the same five documents as a JSON array and as NDJSON.
func dataFiles(t *testing.T) map[string]string {
	t.Helper()
	return map[string]string{
		"json array": writeData(t, "a.json", `[{"id":"1"},{"id":"2"},{"id":"3"},{"id":"4"},{"id":"5"}]`),
		"ndjson":     writeData(t, "a.ndjson", "{\"id\":\"1\"}\n{\"id\":\"2\"}\n{\"id\":\"3\"}\n{\"id\":\"4\"}\n{\"id\":\"5\"}\n"),
	}
}

// TestLoadSampleKeepsFirstNFailuresAcrossBatches pins the exact first-N rule,
// the omitted count and the invariant for limits of zero, negative, below,
// exactly at and above the failure count, with a final short batch and for both
// data formats (which must produce the same sample).
func TestLoadSampleKeepsFirstNFailuresAcrossBatches(t *testing.T) {
	t.Parallel()

	// Five documents, BatchSize 2: batches of 2, 2 and a final short 1.
	replies := [][]sampleItem{
		{okItem("s1"), failItem("f1")},
		{failItem("f2"), failItem("f3")},
		{failItem("f4")},
	}
	allFailed := []string{"f1", "f2", "f3", "f4"}
	tests := []struct {
		name    string
		limit   int
		wantIDs []string
	}{
		{"zero keeps nothing", 0, nil},
		{"negative keeps nothing", -5, nil},
		// Batch 2 is full with two failures but only one slot is left, so a
		// full-batch call site passing the whole limit instead of the
		// remaining budget would sample f1,f2,f3 here.
		{"limit inside a full batch", 2, allFailed[:2]},
		{"below the failure count", 3, allFailed[:3]},
		{"exactly the failure count", 4, allFailed},
		{"above the failure count", 100, allFailed},
	}
	for format, path := range dataFiles(t) {
		for _, tt := range tests {
			t.Run(format+"/"+tt.name, func(t *testing.T) {
				t.Parallel()

				stub := esstub.New(t)
				scriptedBulk(t, stub, replies)
				totals, err := Load(t.Context(), stub.MustClient(t),
					Config{DataFile: path, Index: "cards", BatchSize: 2, RetryAttempts: 1, SampleLimit: tt.limit},
					progress.Seams{Now: time.Now})
				if err != nil {
					t.Fatalf("Load: %v", err)
				}
				if totals.Failed != 4 || totals.Succeeded != 1 {
					t.Fatalf("totals = %+v, want 4 failed and 1 succeeded", totals)
				}
				var gotIDs []string
				for _, f := range totals.Sample {
					gotIDs = append(gotIDs, f.ID)
				}
				if !reflect.DeepEqual(gotIDs, tt.wantIDs) {
					t.Fatalf("sample IDs = %v, want %v", gotIDs, tt.wantIDs)
				}
				if got := len(totals.Sample) + totals.SampleOmitted; got != totals.Failed {
					t.Fatalf("len(Sample)+SampleOmitted = %d, want Failed %d", got, totals.Failed)
				}
				if tt.limit <= 0 && totals.SampleOmitted != totals.Failed {
					t.Fatalf("omitted = %d, want %d with sampling off", totals.SampleOmitted, totals.Failed)
				}
				for _, f := range totals.Sample {
					if f.Index != "cards" || f.Action != "index" || f.Status != 400 ||
						f.Type != "mapper_parsing_exception" || f.Reason != "bad "+f.ID {
						t.Fatalf("sample item = %+v, want the scripted fields", f)
					}
				}
			})
		}
	}
}

// TestLoadSampleNoFailures pins a clean load: no sample, nothing omitted.
func TestLoadSampleNoFailures(t *testing.T) {
	t.Parallel()

	stub := esstub.New(t)
	scriptedBulk(t, stub, [][]sampleItem{{okItem("1"), okItem("2")}})
	path := writeData(t, "a.json", `[{"id":"1"},{"id":"2"}]`)
	totals, err := Load(t.Context(), stub.MustClient(t),
		Config{DataFile: path, Index: "cards", BatchSize: 10, RetryAttempts: 1, SampleLimit: 100},
		progress.Seams{Now: time.Now})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if totals.Sample != nil || totals.SampleOmitted != 0 || totals.Failed != 0 {
		t.Fatalf("totals = %+v, want no sample and nothing omitted", totals)
	}
}

// TestBulkInsertSampleIsNotCappedByTheLogCap pins that failures past the tenth
// in one batch still reach the sample while the log keeps its ten-line cap and
// its omission line, and that the budget (zero, negative, smaller than the
// failures) bounds the sample without changing the counts.
func TestBulkInsertSampleIsNotCappedByTheLogCap(t *testing.T) {
	t.Parallel()

	items := make([]sampleItem, 15)
	docs := make([]map[string]interface{}, 15)
	for i := range items {
		items[i] = failItem(fmt.Sprintf("f%02d", i))
		docs[i] = map[string]interface{}{"id": fmt.Sprint(i)}
	}
	tests := []struct {
		name    string
		budget  int
		wantLen int
	}{
		{"budget above the batch", 100, 15},
		{"budget below the batch", 12, 12},
		{"zero budget", 0, 0},
		{"negative budget", -1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			scriptedBulk(t, stub, [][]sampleItem{items})
			logger, buf := logbuf.New()
			ctx := runlog.WithLogger(t.Context(), logger)
			res, err := bulkInsert(ctx, stub.MustClient(t), "cards", docs, 15, 15, 1, time.Millisecond, time.Millisecond, "",
				func(context.Context, time.Duration) error { t.Error("no retry expected"); return nil }, tt.budget)
			if err != nil {
				t.Fatalf("bulkInsert: %v", err)
			}
			if res.Failed != 15 || res.Succeeded != 0 || len(res.Sample) != tt.wantLen {
				t.Fatalf("result failed=%d succeeded=%d sample=%d, want 15/0/%d", res.Failed, res.Succeeded, len(res.Sample), tt.wantLen)
			}
			for i, f := range res.Sample {
				if want := fmt.Sprintf("f%02d", i); f.ID != want {
					t.Fatalf("sample[%d].ID = %q, want %q (response order)", i, f.ID, want)
				}
			}
			detail, omission := 0, 0
			for _, l := range buf.Lines(t) {
				switch l["message"] {
				case "Bulk item failed":
					detail++
				case "Additional bulk item failures omitted from logs":
					omission++
				}
			}
			if detail != 10 || omission != 1 {
				t.Fatalf("log detail lines=%d omission lines=%d, want 10 and 1 regardless of the sample", detail, omission)
			}
		})
	}
}

// TestLoadSampleItemWithNullError pins that a failed item whose response has no
// error object, or a sub-300 status carrying an error, is still sampled, with
// empty Type and Reason for the null error.
func TestLoadSampleItemWithNullError(t *testing.T) {
	t.Parallel()

	stub := esstub.New(t)
	scriptedBulk(t, stub, [][]sampleItem{{
		{id: "n1", status: 503},
		{id: "n2", status: 200, errObj: map[string]string{"type": "t", "reason": "r"}},
	}})
	path := writeData(t, "a.json", `[{"id":"1"},{"id":"2"}]`)
	totals, err := Load(t.Context(), stub.MustClient(t),
		Config{DataFile: path, Index: "cards", BatchSize: 2, RetryAttempts: 1, SampleLimit: 100},
		progress.Seams{Now: time.Now})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []ItemFailure{
		{Index: "cards", ID: "n1", Action: "index", Status: 503},
		{Index: "cards", ID: "n2", Action: "index", Status: 200, Type: "t", Reason: "r"},
	}
	if !reflect.DeepEqual(totals.Sample, want) || totals.Failed != 2 || totals.SampleOmitted != 0 {
		t.Fatalf("totals = %+v, want sample %+v", totals, want)
	}
}

// TestBulkInsertSampleSanitizesEveryStringField pins redaction, single-line
// valid UTF-8 and the 2,048-byte bound for Index, ID, Action, Type and Reason,
// and that the log line keeps the raw _id and _index it always logged.
func TestBulkInsertSampleSanitizesEveryStringField(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("é", 3000)
	rawID := "id\x01\r\n password=hunter2 " + long
	hostile := sampleItem{
		index:  "idx\n password=hunter2 " + long,
		id:     rawID,
		action: "in\ndex https://user:pw@host/x " + long,
		status: 400,
		errObj: map[string]string{"type": "password=hunter2 " + long, "reason": long + " https://user:pw@host/x"},
	}
	stub := esstub.New(t)
	scriptedBulk(t, stub, [][]sampleItem{{hostile}})
	logger, buf := logbuf.New()
	ctx := runlog.WithLogger(t.Context(), logger)
	res, err := bulkInsert(ctx, stub.MustClient(t), "cards", []map[string]interface{}{{"id": "1"}}, 1, 1, 1, time.Millisecond, time.Millisecond, "",
		func(context.Context, time.Duration) error { t.Error("no retry expected"); return nil }, 10)
	if err != nil {
		t.Fatalf("bulkInsert: %v", err)
	}
	if len(res.Sample) != 1 {
		t.Fatalf("sample = %+v, want 1 item", res.Sample)
	}
	f := res.Sample[0]
	for name, v := range map[string]string{"Index": f.Index, "ID": f.ID, "Action": f.Action, "Type": f.Type, "Reason": f.Reason} {
		if len(v) > esresponse.MaxPublicMessageBytes || !utf8.ValidString(v) {
			t.Fatalf("%s is %d bytes / invalid UTF-8", name, len(v))
		}
		for _, r := range v {
			if r < 0x20 || r == 0x7f {
				t.Fatalf("%s carries control character %q: %q", name, r, v)
			}
		}
		for _, bad := range []string{"hunter2", "user:pw"} {
			if strings.Contains(v, bad) {
				t.Fatalf("%s leaked %q: %q", name, bad, v)
			}
		}
	}
	for name, v := range map[string]string{"Index": f.Index, "ID": f.ID, "Action": f.Action, "Type": f.Type, "Reason": f.Reason} {
		if !strings.HasSuffix(v, " [truncated]") {
			t.Fatalf("%s is not marked truncated: %q", name, v[max(0, len(v)-20):])
		}
	}
	// The log keeps the raw response strings: only the sample is sanitized,
	// while the log's type and reason share the sample's sanitizer.
	var logged map[string]any
	for _, l := range buf.Lines(t) {
		if l["message"] == "Bulk item failed" {
			logged = l
		}
	}
	if logged == nil || logged["_id"] != rawID || logged["_index"] != hostile.index {
		t.Fatalf("log line = %v, want the raw _id and _index unchanged", logged)
	}
	if logged["action"] != hostile.action {
		t.Fatalf("logged action = %q, want the raw action %q", logged["action"], hostile.action)
	}
	if logged["error_type"] != f.Type || logged["error_reason"] != f.Reason {
		t.Fatalf("logged error_type/error_reason = %q/%q, want the sample's %q/%q", logged["error_type"], logged["error_reason"], f.Type, f.Reason)
	}
}

// TestLoadHardFailureAfterRejectedBatchesReturnsNoSample pins that a hard bulk
// failure after batches with rejections returns zero Totals, sample included.
func TestLoadHardFailureAfterRejectedBatchesReturnsNoSample(t *testing.T) {
	t.Parallel()

	stub := esstub.New(t)
	// Batch 1 has rejections; batch 2 is unscripted and answers HTTP 400.
	calls := scriptedBulk(t, stub, [][]sampleItem{{failItem("f1"), failItem("f2")}})
	path := writeData(t, "a.json", `[{"id":"1"},{"id":"2"},{"id":"3"}]`)
	totals, err := Load(t.Context(), stub.MustClient(t),
		Config{DataFile: path, Index: "cards", BatchSize: 2, RetryAttempts: 1, SampleLimit: 100},
		progress.Seams{Now: time.Now})
	if err == nil {
		t.Fatal("Load returned nil, want the hard bulk failure")
	}
	// Without this, a Load that failed before the rejected batch was processed
	// would also return zero Totals and pass vacuously.
	if got := calls.Load(); got != 2 {
		t.Fatalf("_bulk requests = %d, want 2 (rejected batch, then the hard failure)", got)
	}
	if !reflect.DeepEqual(totals, Totals{}) {
		t.Fatalf("totals = %+v, want zero Totals (no sample, nothing omitted)", totals)
	}
}

// TestBulkInsertSamplesFailuresWhenErrorsFlagFalse pins that the failure
// predicate is per item and ignores the response's top-level "errors" flag: a
// "skip the item scan when !errors" shortcut would silently drop rejected
// documents (and report them as succeeded) whenever the flag disagrees with the
// items, which is the wrong-result-that-looks-like-success outcome.
func TestBulkInsertSamplesFailuresWhenErrorsFlagFalse(t *testing.T) {
	t.Parallel()

	stub := esstub.New(t)
	scriptedBulkErrorsFlag(t, stub, false, [][]sampleItem{{
		okItem("s1"),
		failItem("f1"),
		{id: "n1", status: 503},
	}})
	path := writeData(t, "a.json", `[{"id":"1"},{"id":"2"},{"id":"3"}]`)
	res, err := Load(t.Context(), stub.MustClient(t),
		Config{DataFile: path, Index: "cards", BatchSize: 3, RetryAttempts: 1, SampleLimit: 10},
		progress.Seams{Now: time.Now})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []ItemFailure{
		{Index: "cards", ID: "f1", Action: "index", Status: 400, Type: "mapper_parsing_exception", Reason: "bad f1"},
		{Index: "cards", ID: "n1", Action: "index", Status: 503},
	}
	if res.Failed != 2 || res.Succeeded != 1 || res.SampleOmitted != 0 || !reflect.DeepEqual(res.Sample, want) {
		t.Fatalf("result = %+v, want failed 2, succeeded 1, nothing omitted, sample %+v", res, want)
	}
}

// TestAccumulatorBudgetNeverNegative pins the budget arithmetic.
func TestAccumulatorBudgetNeverNegative(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		limit int
		have  int
		want  int
	}{
		{"fresh", 5, 0, 5},
		{"partly used", 5, 3, 2},
		{"exhausted", 5, 5, 0},
		{"disabled", 0, 0, 0},
		{"negative limit", -2, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := &accumulator{limit: tt.limit, sample: make([]ItemFailure, tt.have)}
			if got := a.budget(); got != tt.want {
				t.Fatalf("budget = %d, want %d", got, tt.want)
			}
		})
	}
}

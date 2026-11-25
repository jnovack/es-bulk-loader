package loader

// Run-level guards for Result.BulkFailures and Result.BulkFailuresOmitted
// (loader-observability phase 3). They drive Run through newProgressStub with
// scripted _bulk replies, a Nop logger, and assert on the returned error and
// Result only. Serial: they install a fake clock.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/bulk"
)

// bulkItemJSON is one scripted _bulk response item.
func bulkItemJSON(t *testing.T, id string, status int, errObj map[string]string) map[string]any {
	t.Helper()
	body := map[string]any{"_index": progressIndex, "_id": id, "status": status}
	if errObj != nil {
		body["error"] = errObj
	}
	return map[string]any{"index": body}
}

func bulkBodyJSON(t *testing.T, items []map[string]any) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{"errors": true, "items": items})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// writeBulkData writes n one-field documents as a JSON array and returns the
// path.
func writeBulkData(t *testing.T, n int) string {
	t.Helper()
	docs := make([]string, n)
	for i := range docs {
		docs[i] = fmt.Sprintf(`{"id":"%d"}`, i)
	}
	path := filepath.Join(t.TempDir(), "data.json")
	if err := os.WriteFile(path, []byte("["+strings.Join(docs, ",")+"]"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func nopLogger(o *Options) {
	nop := zerolog.Nop()
	o.Logger = &nop
}

// TestRunBulkFailuresSampleAcrossBatches rejects 104 of 130 documents across
// four batches (the last short) with distinct IDs and mixed successes: the
// sample is exactly the first MaxBulkFailureSample rejected items in order and
// the rest are counted as omitted.
func TestRunBulkFailuresSampleAcrossBatches(t *testing.T) {
	installFakeClock(t, time.Second)

	const docs, batch = 130, 40
	var bodies []stubReply
	var wantIDs []string
	failed := 0
	for start := 0; start < docs; start += batch {
		end := min(start+batch, docs)
		var items []map[string]any
		for i := start; i < end; i++ {
			id := fmt.Sprintf("d%03d", i)
			if i%5 == 0 {
				items = append(items, bulkItemJSON(t, id, 201, nil))
				continue
			}
			failed++
			if len(wantIDs) < MaxBulkFailureSample {
				wantIDs = append(wantIDs, id)
			}
			items = append(items, bulkItemJSON(t, id, 400, map[string]string{"type": "mapper_parsing_exception", "reason": "boom " + id}))
		}
		bodies = append(bodies, bulkOKReply(bulkBodyJSON(t, items)))
	}
	server, ops := newProgressStub(t, nil, withBulkReplies(bodies...))

	run := runProgress(t, server, ops, zerolog.InfoLevel, nopLogger, func(o *Options) {
		o.DataFile = writeBulkData(t, docs)
		o.BatchSize = batch
	})
	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}
	r := run.result
	if r.DocumentsFailed != failed || failed != 104 {
		t.Fatalf("DocumentsFailed = %d, want %d (104)", r.DocumentsFailed, failed)
	}
	if len(r.BulkFailures) != MaxBulkFailureSample || r.BulkFailuresOmitted != failed-MaxBulkFailureSample {
		t.Fatalf("sample=%d omitted=%d, want %d and %d", len(r.BulkFailures), r.BulkFailuresOmitted, MaxBulkFailureSample, failed-MaxBulkFailureSample)
	}
	if got := len(r.BulkFailures) + r.BulkFailuresOmitted; got != r.DocumentsFailed {
		t.Fatalf("len(BulkFailures)+BulkFailuresOmitted = %d, want DocumentsFailed %d", got, r.DocumentsFailed)
	}
	for i, f := range r.BulkFailures {
		want := BulkItemFailure{Index: progressIndex, ID: wantIDs[i], Action: "index", Status: 400, Type: "mapper_parsing_exception", Reason: "boom " + wantIDs[i]}
		if f != want {
			t.Fatalf("BulkFailures[%d] = %+v, want %+v", i, f, want)
		}
	}
}

// TestRunBulkFailuresBoundaries pins no failures, fewer than N and exactly N.
func TestRunBulkFailuresBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		docs      int
		failItems int
	}{
		{"no failures", 5, 0},
		{"fewer than the bound", 5, 3},
		{"exactly the bound", MaxBulkFailureSample, MaxBulkFailureSample},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installFakeClock(t, time.Second)
			server, ops := newProgressStub(t, nil, withBulk(0, tt.failItems))
			run := runProgress(t, server, ops, zerolog.InfoLevel, nopLogger, func(o *Options) {
				o.DataFile = writeBulkData(t, tt.docs)
				o.BatchSize = tt.docs
			})
			if run.err != nil {
				t.Fatalf("Run: %v", run.err)
			}
			r := run.result
			if len(r.BulkFailures) != tt.failItems || r.BulkFailuresOmitted != 0 || r.DocumentsFailed != tt.failItems {
				t.Fatalf("sample=%d omitted=%d failed=%d, want %d, 0, %[4]d", len(r.BulkFailures), r.BulkFailuresOmitted, r.DocumentsFailed, tt.failItems)
			}
			if tt.failItems == 0 && r.BulkFailures != nil {
				t.Fatalf("BulkFailures = %#v, want nil when nothing was rejected", r.BulkFailures)
			}
		})
	}
}

// TestRunBulkFailuresAreSanitizedAndNullErrorIsEmpty pins redaction and the
// bound on every string field through Run, and empty Type/Reason for a failed
// item with a null error.
func TestRunBulkFailuresAreSanitizedAndNullErrorIsEmpty(t *testing.T) {
	installFakeClock(t, time.Second)

	long := strings.Repeat("é", 3000)
	// Every one of the five string fields carries a credential and exceeds the
	// 2,048-byte bound, so each per-field length and leak check below can fail.
	rawID := "id\x01\n password=hunter2 " + long
	items := []map[string]any{
		{"in\ndex password=hunter2 " + long: map[string]any{
			"_index": "idx\n password=hunter2 " + long, "_id": rawID, "status": 400,
			"error": map[string]string{"type": "password=hunter2 " + long, "reason": long + " https://user:pw@host/x"},
		}},
		bulkItemJSON(t, "null-error", 503, nil),
	}
	server, ops := newProgressStub(t, nil, withBulkReplies(bulkOKReply(bulkBodyJSON(t, items))))
	run := runProgress(t, server, ops, zerolog.InfoLevel, nopLogger, func(o *Options) {
		o.DataFile = writeBulkData(t, 2)
		o.BatchSize = 2
	})
	if run.err != nil {
		t.Fatalf("Run: %v", run.err)
	}
	got := run.result.BulkFailures
	if len(got) != 2 || run.result.DocumentsFailed != 2 || run.result.BulkFailuresOmitted != 0 {
		t.Fatalf("BulkFailures = %+v, want 2 items", got)
	}
	for name, v := range map[string]string{"Index": got[0].Index, "ID": got[0].ID, "Action": got[0].Action, "Type": got[0].Type, "Reason": got[0].Reason} {
		if len(v) > 2048 || !utf8.ValidString(v) {
			t.Fatalf("%s is %d bytes / invalid UTF-8", name, len(v))
		}
		for _, r := range v {
			if r < 0x20 || r == 0x7f {
				t.Fatalf("%s carries control character %q", name, r)
			}
		}
		if strings.Contains(v, "hunter2") || strings.Contains(v, "user:pw") {
			t.Fatalf("%s leaked a credential: %q", name, v)
		}
	}
	if want := (BulkItemFailure{Index: progressIndex, ID: "null-error", Action: "index", Status: 503}); got[1] != want {
		t.Fatalf("null-error item = %+v, want %+v", got[1], want)
	}
}

// TestRunBulkFailuresPartialResultRules pins when the sample is filled: kept
// when a later stage fails, zero when the bulk stage itself fails (earlier
// Result fields stay populated), and nil/zero when no data action ran.
func TestRunBulkFailuresPartialResultRules(t *testing.T) {
	t.Run("later stage failure keeps the bulk sample", func(t *testing.T) {
		installFakeClock(t, time.Second)
		// A non-retryable _stats status ends the Run after the bulk stage.
		server, ops := newProgressStub(t, []string{`{"error":{"type":"stats_rejected"}}`}, withBulk(0, 2), withStatsStatus(http.StatusBadRequest))
		run := runProgress(t, server, ops, zerolog.InfoLevel, nopLogger, withTransforms(t, false, true), func(o *Options) {
			o.DataFile = writeBulkData(t, 3)
			o.BatchSize = 3
		})
		if run.err == nil {
			t.Fatal("Run returned nil, want the transform stage failure")
		}
		r := run.result
		if r.DocumentsProcessed != 3 || r.DocumentsFailed != 2 || len(r.BulkFailures) != 2 || r.BulkFailuresOmitted != 0 {
			t.Fatalf("result = %+v, want processed 3, failed 2, a 2-item sample and nothing omitted", r)
		}
	})

	t.Run("bulk stage failure leaves only the bulk fields zero", func(t *testing.T) {
		installFakeClock(t, time.Second)
		// Batch 1 holds one success and one rejection (200, errors:true);
		// batch 2 is a non-retryable 400. The zero-field checks below can only
		// pass if Run drops the totals accumulated before the hard failure, and
		// the two-request assertion proves batch 1 was processed first.
		rejected := bulkBodyJSON(t, []map[string]any{
			bulkItemJSON(t, "a", 201, nil),
			bulkItemJSON(t, "b", 400, map[string]string{"type": "mapper_parsing_exception", "reason": "boom b"}),
		})
		server, ops := newProgressStub(t, nil, withBulkReplies(
			bulkOKReply(rejected),
			stubReply{http.StatusBadRequest, `{"error":{"type":"stub_bulk_failure"}}`},
		))
		run := runProgress(t, server, ops, zerolog.InfoLevel, nopLogger, func(o *Options) {
			o.DataFile = writeBulkData(t, 3)
			o.BatchSize = 2
		})
		if run.err == nil {
			t.Fatal("Run returned nil, want the hard bulk failure")
		}
		r := run.result
		if r.WriteIndex != progressIndex {
			t.Fatalf("WriteIndex = %q, want %q kept from before the bulk stage", r.WriteIndex, progressIndex)
		}
		if r.BulkFailures != nil || r.BulkFailuresOmitted != 0 || r.DocumentsProcessed != 0 || r.DocumentsFailed != 0 {
			t.Fatalf("result = %+v, want zero bulk fields", r)
		}
		if r.DocumentsSucceeded != 0 {
			t.Fatalf("DocumentsSucceeded = %d, want 0 after the hard failure", r.DocumentsSucceeded)
		}
		bulkRequests := 0
		for _, op := range run.ops {
			if op == "bulk" {
				bulkRequests++
			}
		}
		if bulkRequests != 2 {
			t.Fatalf("_bulk requests = %d (ops %v), want 2: the rejected batch, then the hard failure", bulkRequests, run.ops)
		}
	})

	t.Run("no data action gives nil and zero", func(t *testing.T) {
		installFakeClock(t, time.Second)
		server, ops := newProgressStub(t, nil, withBulk(0, 2))
		run := runProgress(t, server, ops, zerolog.InfoLevel, nopLogger, withEnrich, func(o *Options) { o.DataFile, o.AddToIndex = "", false })
		if run.err != nil {
			t.Fatalf("Run: %v", run.err)
		}
		if r := run.result; !reflect.DeepEqual(r.BulkFailures, []BulkItemFailure(nil)) || r.BulkFailuresOmitted != 0 || r.DocumentsFailed != 0 {
			t.Fatalf("result = %+v, want nil/zero bulk fields", r)
		}
	})
}

// TestBulkFailuresFrom pins the adapter from the internal sample to the public
// Result.BulkFailures: nothing in gives nil, every field maps one-to-one in
// order. The result cannot alias the input: the element types differ, so the
// type change itself guarantees a copy.
func TestBulkFailuresFrom(t *testing.T) {
	tests := []struct {
		name string
		in   []bulk.ItemFailure
		want []BulkItemFailure
	}{
		{"nil input", nil, nil},
		{"empty input", []bulk.ItemFailure{}, nil},
		{
			name: "fields map one-to-one in order",
			in: []bulk.ItemFailure{
				{Index: "i1", ID: "d1", Action: "index", Status: 400, Type: "t1", Reason: "r1"},
				{Index: "i2", ID: "d2", Action: "create", Status: 503},
			},
			want: []BulkItemFailure{
				{Index: "i1", ID: "d1", Action: "index", Status: 400, Type: "t1", Reason: "r1"},
				{Index: "i2", ID: "d2", Action: "create", Status: 503},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bulkFailuresFrom(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("bulkFailuresFrom = %+v, want %+v", got, tt.want)
			}
		})
	}

}

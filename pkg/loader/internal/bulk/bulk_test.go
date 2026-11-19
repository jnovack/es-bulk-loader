package bulk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/logbuf"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

// TestBulkInsertRejectsUnsupportedDocumentBeforeRequest verifies that payload
// serialization failures stop the batch before Elasticsearch receives it.
func TestBulkInsertRejectsUnsupportedDocumentBeforeRequest(t *testing.T) {
	// The bulk route is registered so a request would be answered; the
	// assertion below is that none arrives at all.
	stub := esstub.New(t)
	stub.Handle("POST /_bulk", esstub.JSON(http.StatusOK, `{"errors":false,"items":[]}`))
	es := stub.MustClient(t)

	_, err := bulkInsert(
		context.Background(),
		es,
		"cards",
		[]map[string]interface{}{
			{"id": "1"},
			{"id": "2", "unsupported": func() {}},
		},
		2,
		2,
		1,
		time.Millisecond,
		time.Millisecond,
		"id",
		func(context.Context, time.Duration) error { t.Fatal("no retry expected"); return nil },
	)
	// Kind for this op is pinned by TestOperationKindsOfMovedHelpers in loader.
	var opErr *failure.Error
	if !errors.As(err, &opErr) || opErr.Op != "Unable to serialize document for bulk insert" {
		t.Fatalf("bulkInsert error = %#v, want the serialization operation error", err)
	}
	if got := len(stub.Requests()); got != 0 {
		t.Fatalf("bulk request count mismatch: got %d want 0", got)
	}
}

// TestPeekFirstByte guards reading the first non-whitespace byte that data-format detection depends on.
func TestPeekFirstByte(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    byte
		wantErr bool
	}{
		{name: "json array", content: `[{"id":"1"}]`, want: '['},
		{name: "ndjson", content: "{\"id\":\"1\"}\n", want: '{'},
		{name: "leading whitespace before array", content: "  \n[{\"id\":\"1\"}]", want: '['},
		{name: "leading whitespace before object", content: "\t {\"id\":\"1\"}", want: '{'},
		{name: "empty file", content: "", wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "data.txt")
			if err := os.WriteFile(path, []byte(tt.content), 0o644); err != nil {
				t.Fatalf("write test file: %v", err)
			}

			got, err := peekFirstByte(path)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("peekFirstByte returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("peekFirstByte(%q) = %q, want %q", path, got, tt.want)
			}
		})
	}
}

// TestCountNDJSONLines guards the non-empty line count that sizes NDJSON progress totals.
func TestCountNDJSONLines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    int
	}{
		{name: "empty", content: "", want: 0},
		{name: "one line", content: "{\"a\":1}\n", want: 1},
		{name: "two lines", content: "{\"a\":1}\n{\"b\":2}\n", want: 2},
		{name: "blank line between docs", content: "{\"a\":1}\n\n{\"b\":2}\n", want: 2},
		{name: "no trailing newline", content: "{\"a\":1}", want: 1},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "data.ndjson")
			if err := os.WriteFile(path, []byte(tt.content), 0o644); err != nil {
				t.Fatalf("write test file: %v", err)
			}

			got, err := countNDJSONLines(path)
			if err != nil {
				t.Fatalf("countNDJSONLines returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("countNDJSONLines(%q) = %d, want %d", path, got, tt.want)
			}
		})
	}
}

// TestLoadRejectsUnreadableOrUnknownFormatBeforeRequest pins the format
// failures: an unreadable file, an empty file and a first byte that is neither
// '[' nor '{' fail with their fixed ops, return zero Totals and send nothing.
func TestLoadRejectsUnreadableOrUnknownFormatBeforeRequest(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		return path
	}
	tests := []struct {
		name   string
		path   string
		wantOp string
	}{
		{"missing file", filepath.Join(dir, "missing.json"), "Cannot read data file"},
		{"empty file", write("empty.json", " \r\n\t"), "Cannot read data file"},
		{"unknown first byte", write("csv.json", "\r\nid,name\r\n"), `Data file first byte 'i' is neither '[' (JSON array) nor '{' (NDJSON)`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t) // no routes: any request fails the test
			totals, err := Load(t.Context(), stub.MustClient(t), Config{DataFile: tt.path, Index: "cards", BatchSize: 1}, progress.Seams{Now: time.Now})
			var opErr *failure.Error
			if !errors.As(err, &opErr) || opErr.Op != tt.wantOp {
				t.Fatalf("Load error = %#v, want operation error %q", err, tt.wantOp)
			}
			if totals != (Totals{}) {
				t.Fatalf("totals = %+v, want zero Totals on a hard failure", totals)
			}
		})
	}
}

// TestLoadReportsBatchesAndTotals pins one snapshot per batch with cumulative
// Done and the file's document Total, and Totals counting per-item failures,
// for both data formats.
func TestLoadReportsBatchesAndTotals(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	tests := []struct {
		name    string
		content string
	}{
		{"json array", `[{"id":"1"},{"id":"2"},{"id":"3"}]`},
		{"ndjson with CRLF", "{\"id\":\"1\"}\r\n{\"id\":\"2\"}\r\n{\"id\":\"3\"}\r\n"},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(dir, fmt.Sprintf("data-%d", i))
			if err := os.WriteFile(path, []byte(tt.content), 0o644); err != nil {
				t.Fatalf("write fixture: %v", err)
			}
			stub := esstub.New(t)
			var calls atomic.Int32
			stub.Handle("POST /_bulk", func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					_, _ = w.Write([]byte(`{"errors":true,"items":[{"index":{"_index":"cards","_id":"1","status":201}},{"index":{"_index":"cards","_id":"2","status":400,"error":{"type":"mapper_parsing_exception","reason":"bad"}}}]}`))
					return
				}
				_, _ = w.Write([]byte(`{"errors":false,"items":[{"index":{"_index":"cards","_id":"3","status":201}}]}`))
			})

			var snapshots []progress.Snapshot
			seams := progress.Seams{Now: time.Now, OnProgress: func(s progress.Snapshot) { snapshots = append(snapshots, s) }}
			totals, err := Load(t.Context(), stub.MustClient(t), Config{DataFile: path, Index: "cards", BatchSize: 2, RetryAttempts: 1}, seams)
			if err != nil {
				t.Fatalf("Load returned error: %v", err)
			}
			if totals.Processed != 3 || totals.Succeeded != 2 || totals.Failed != 1 {
				t.Fatalf("totals = %+v, want processed 3, succeeded 2, failed 1", totals)
			}
			if len(snapshots) != 2 || snapshots[0].Done != 2 || snapshots[1].Done != 3 ||
				snapshots[0].Total != 3 || snapshots[1].Total != 3 || snapshots[0].Resource != "cards" || snapshots[1].Final {
				t.Fatalf("snapshots = %+v, want two non-final batches 2/3 then 3/3 for cards", snapshots)
			}
		})
	}
}

// bulkRoundTripFunc adapts a function to http.RoundTripper so a test can fail
// chosen bulk attempts with real stdlib errors before any bytes reach the stub.
type bulkRoundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip calls f.
func (f bulkRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// TestBulkInsertTransportRetryPolicy drives bulkInsert through the real client
// with injected transport errors. A timeout is retried until the budget runs
// out, a temporary-only DNS failure fails on its first attempt, and an ended
// request context stops the loop before classification. Sleeps are recorded,
// never waited on, and no real DNS is used.
func TestBulkInsertTransportRetryPolicy(t *testing.T) {
	t.Parallel()

	timeoutErr := &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}
	dnsErr := &net.DNSError{Err: "temporary lookup failure", Name: "es.invalid", IsTemporary: true}

	tests := []struct {
		name string
		// fail returns the transport error for the 1-based attempt, or nil to
		// delegate that attempt to the stub.
		fail          func(attempt int, cancel context.CancelFunc) error
		wantAttempts  int
		wantSleeps    int
		wantServer    int
		wantCause     error // nil means the call must succeed
		wantSucceeded int
	}{
		{
			name: "timeout is retried then succeeds",
			fail: func(attempt int, _ context.CancelFunc) error {
				if attempt == 1 {
					return timeoutErr
				}
				return nil
			},
			wantAttempts:  2,
			wantSleeps:    1,
			wantServer:    1,
			wantSucceeded: 1,
		},
		{
			name: "temporary-only DNS failure fails on first attempt FIX-NETTEMP-01",
			fail: func(int, context.CancelFunc) error {
				return dnsErr
			},
			wantAttempts: 1,
			wantCause:    dnsErr,
		},
		{
			name: "timeout on every call exhausts the budget",
			fail: func(int, context.CancelFunc) error {
				return timeoutErr
			},
			wantAttempts: 3,
			wantSleeps:   2,
			wantCause:    timeoutErr,
		},
		{
			name: "request context ended during timeout stops retrying",
			fail: func(attempt int, cancel context.CancelFunc) error {
				if attempt == 1 {
					cancel()
				}
				return timeoutErr
			},
			wantAttempts: 1,
			wantCause:    context.Canceled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			stub := esstub.New(t)
			stub.Handle("POST /_bulk", esstub.JSON(http.StatusOK, `{"errors":false,"items":[{"index":{"_index":"cards","_id":"1","status":201}}]}`))

			base := &http.Transport{}
			t.Cleanup(base.CloseIdleConnections)
			var attempts atomic.Int32
			transport := bulkRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodPost || req.URL.Path != "/_bulk" {
					// Non-bulk traffic (the client's product check) goes to the
					// strict stub, which fails the test on any unexpected route.
					return base.RoundTrip(req)
				}
				attempt := int(attempts.Add(1))
				if err := tt.fail(attempt, cancel); err != nil {
					if req.Body != nil {
						if closeErr := req.Body.Close(); closeErr != nil {
							t.Errorf("close intercepted bulk body: %v", closeErr)
						}
					}
					return nil, err
				}
				return base.RoundTrip(req)
			})

			es, err := elasticsearch.NewClient(elasticsearch.Config{
				Addresses:    []string{stub.URL()},
				Transport:    transport,
				DisableRetry: true,
				MaxRetries:   0,
			})
			if err != nil {
				t.Fatalf("new elasticsearch client: %v", err)
			}

			var sleeps []time.Duration
			sleep := func(ctx context.Context, d time.Duration) error {
				sleeps = append(sleeps, d)
				return ctx.Err()
			}

			got, err := bulkInsert(
				ctx,
				es,
				"cards",
				[]map[string]interface{}{{"id": "1"}},
				1,
				1,
				3,
				time.Millisecond,
				time.Millisecond,
				"id",
				sleep,
			)

			if n := int(attempts.Load()); n != tt.wantAttempts {
				t.Fatalf("transport attempts = %d, want %d", n, tt.wantAttempts)
			}
			if len(sleeps) != tt.wantSleeps {
				t.Fatalf("retry sleeps = %v, want %d", sleeps, tt.wantSleeps)
			}
			for _, d := range sleeps {
				if d != time.Millisecond {
					t.Fatalf("retry sleep = %v, want %v", d, time.Millisecond)
				}
			}
			server := 0
			for _, r := range stub.Requests() {
				if r.Method == http.MethodPost && r.Path == "/_bulk" {
					server++
				}
			}
			if server != tt.wantServer {
				t.Fatalf("server bulk requests = %d, want %d", server, tt.wantServer)
			}

			if tt.wantCause == nil {
				if err != nil {
					t.Fatalf("bulkInsert returned error: %v", err)
				}
				if want := (bulkInsertResult{Succeeded: tt.wantSucceeded}); got != want {
					t.Fatalf("bulkInsert result = %+v, want %+v", got, want)
				}
				return
			}

			var opErr *failure.Error
			if !errors.As(err, &opErr) || opErr.Op != "Bulk API request failed" {
				t.Fatalf("bulkInsert error = %#v, want operation error %q", err, "Bulk API request failed")
			}
			if !errors.Is(err, tt.wantCause) {
				t.Fatalf("bulkInsert error = %v, want cause %v", err, tt.wantCause)
			}
			if got != (bulkInsertResult{}) {
				t.Fatalf("bulkInsert result = %+v, want zero on failure", got)
			}
		})
	}
}

// TestCheckDataFile pins the bounded preflight: open, first non-whitespace
// byte and format ('[' or '{'), using Load's own failure Ops. It must not
// decode the file, so invalid JSON after an accepted first byte passes.
func TestCheckDataFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		return path
	}
	const unreadable = "Cannot read data file"
	tests := []struct {
		name      string
		path      string
		wantFail  bool
		wantOp    string
		wantCause error
	}{
		{"missing", filepath.Join(dir, "missing.json"), true, unreadable, os.ErrNotExist},
		{"directory", dir, true, unreadable, nil},
		{"empty", write("empty.json", ""), true, unreadable, io.EOF},
		{"whitespace only", write("ws.json", " \r\n\t\r\n"), true, unreadable, io.EOF},
		{"array", write("array.json", `[{"id":1}]`), false, "", nil},
		{"ndjson with leading whitespace", write("nd.json", "\r\n  {\"id\":1}\r\n"), false, "", nil},
		{"invalid JSON after array byte", write("bad-array.json", `[not json`), false, "", nil},
		{"invalid JSON after object byte", write("bad-object.json", `{not json`), false, "", nil},
		{"unknown first byte", write("x.json", "x"), true, `Data file first byte 'x' is neither '[' (JSON array) nor '{' (NDJSON)`, nil},
		{"unknown first byte after CRLF whitespace", write("crlf-csv.json", "\r\n\r\nid,name\r\n"), true, `Data file first byte 'i' is neither '[' (JSON array) nor '{' (NDJSON)`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := CheckDataFile(t.Context(), tt.path)
			if !tt.wantFail {
				if err != nil {
					t.Fatalf("CheckDataFile: %v", err)
				}
				return
			}
			var opErr *failure.Error
			if !errors.As(err, &opErr) || opErr.Op != tt.wantOp {
				t.Fatalf("error = %#v, want operation error %q", err, tt.wantOp)
			}
			if opErr.Err == nil {
				t.Fatal("want non-nil cause")
			}
			if tt.wantCause != nil && !errors.Is(err, tt.wantCause) {
				t.Fatalf("error = %v, want cause %v", err, tt.wantCause)
			}
		})
	}
}

// TestLoadRechecksFormatAfterPreflight pins that Load detects the format
// itself: a file that passed CheckDataFile and then changed to an unknown
// format is rejected before any bulk request (time-of-check/time-of-use).
func TestLoadRechecksFormatAfterPreflight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		initial string
		changed string
	}{
		{"array replaced by x", `[{"id":"1"}]`, "x"},
		{"ndjson replaced by CRLF CSV", "{\"id\":\"1\"}\n", "\r\n\r\nid,name\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "data.json")
			if err := os.WriteFile(path, []byte(tt.initial), 0o644); err != nil {
				t.Fatalf("write fixture: %v", err)
			}
			if err := CheckDataFile(t.Context(), path); err != nil {
				t.Fatalf("CheckDataFile on accepted format: %v", err)
			}
			if err := os.WriteFile(path, []byte(tt.changed), 0o644); err != nil {
				t.Fatalf("rewrite fixture: %v", err)
			}

			stub := esstub.New(t) // no routes: any request fails the test
			var progressCalls atomic.Int32
			seams := progress.Seams{
				Now:        time.Now,
				OnProgress: func(progress.Snapshot) { progressCalls.Add(1) },
			}
			totals, err := Load(t.Context(), stub.MustClient(t), Config{DataFile: path, Index: "cards", BatchSize: 1}, seams)

			first := []byte(tt.changed)
			for len(first) > 0 && (first[0] == '\r' || first[0] == '\n') {
				first = first[1:]
			}
			wantOp := fmt.Sprintf("Data file first byte %q is neither '[' (JSON array) nor '{' (NDJSON)", first[0])
			var opErr *failure.Error
			if !errors.As(err, &opErr) || opErr.Op != wantOp {
				t.Fatalf("Load error = %#v, want operation error %q", err, wantOp)
			}
			if opErr.Err == nil {
				t.Fatal("want non-nil substituted cause")
			}
			if totals != (Totals{}) {
				t.Fatalf("totals = %+v, want zero Totals", totals)
			}
			if got := progressCalls.Load(); got != 0 {
				t.Fatalf("progress callbacks = %d, want 0", got)
			}
			if got := len(stub.Requests()); got != 0 {
				t.Fatalf("request count = %d, want 0", got)
			}
		})
	}
}

// TestBulkItemFailureReasonsAreRedacted pins that the logged error_type and
// error_reason of a failed bulk item (decoded from a 2xx response) are
// sanitized and bounded, keep their string type, and keep the ten-item cap.
func TestBulkItemFailureReasonsAreRedacted(t *testing.T) {
	t.Parallel()

	const secrets = `Preview of field's value: 'password=hunter2' at https://user:pw@host/x`
	item := func(id string, status int, errObj map[string]string) map[string]any {
		body := map[string]any{"_index": "cards", "_id": id, "status": status}
		if errObj != nil {
			body["error"] = errObj
		}
		return map[string]any{"index": body}
	}
	run := func(t *testing.T, items []map[string]any) ([]map[string]any, *logbuf.Buffer, int32, bulkInsertResult) {
		t.Helper()
		resp, err := json.Marshal(map[string]any{"errors": true, "items": items})
		if err != nil {
			t.Fatal(err)
		}
		stub := esstub.New(t)
		var calls atomic.Int32
		stub.Handle("POST /_bulk", func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			_, _ = w.Write(resp)
		})
		docs := make([]map[string]interface{}, len(items))
		for i := range docs {
			docs[i] = map[string]interface{}{"id": fmt.Sprint(i)}
		}
		logger, buf := logbuf.New()
		ctx := runlog.WithLogger(t.Context(), logger)
		res, err := bulkInsert(ctx, stub.MustClient(t), "cards", docs, len(docs), len(docs), 1, time.Millisecond, time.Millisecond, "id",
			func(context.Context, time.Duration) error { t.Error("no retry expected"); return nil })
		if err != nil {
			t.Fatalf("bulkInsert: %v", err)
		}
		var lines []map[string]any
		for _, l := range buf.Lines(t) {
			if l["message"] == "Bulk item failed" {
				lines = append(lines, l)
			}
		}
		return lines, buf, calls.Load(), res
	}

	t.Run("secrets and long values", func(t *testing.T) {
		t.Parallel()
		long := strings.Repeat("é", 3000)
		lines, buf, calls, _ := run(t, []map[string]any{
			item("1", 400, map[string]string{"type": "mapper_parsing_exception", "reason": secrets}),
			item("2", 400, map[string]string{"type": "password=hunter2 " + long, "reason": long + " " + secrets}),
		})
		if calls != 1 || len(lines) != 2 {
			t.Fatalf("calls=%d lines=%d, want 1 request and 2 failure lines", calls, len(lines))
		}
		for _, bad := range []string{"hunter2", "user:pw"} {
			if strings.Contains(buf.String(), bad) {
				t.Fatalf("%q leaked in log: %s", bad, buf.String())
			}
		}
		for _, l := range lines {
			for _, k := range []string{"error_type", "error_reason"} {
				v, ok := l[k].(string)
				if !ok {
					t.Fatalf("%s is %T, want string", k, l[k])
				}
				if len(v) > esresponse.MaxPublicMessageBytes || !utf8.ValidString(v) || strings.ContainsAny(v, "\r\n") {
					t.Fatalf("%s is %d bytes / invalid shape", k, len(v))
				}
			}
		}
		if got := lines[0]["error_type"]; got != "mapper_parsing_exception" {
			t.Fatalf("harmless type changed: %v", got)
		}
		reason := lines[1]["error_reason"].(string)
		if !strings.HasSuffix(reason, " [truncated]") {
			t.Fatalf("long reason not marked truncated: %q", reason[len(reason)-20:])
		}
	})

	t.Run("status-only failure and more than ten", func(t *testing.T) {
		t.Parallel()
		items := []map[string]any{item("0", 500, nil)}
		for i := 1; i < 12; i++ {
			items = append(items, item(fmt.Sprint(i), 400, map[string]string{"type": "t", "reason": secrets}))
		}
		lines, buf, calls, res := run(t, items)
		if calls != 1 || len(lines) != 10 {
			t.Fatalf("calls=%d detail lines=%d, want 1 request and exactly 10", calls, len(lines))
		}
		if lines[0]["error_type"] != "" || lines[0]["error_reason"] != "" {
			t.Fatalf("nil-Error line = %v, want empty string fields", lines[0])
		}
		omitted := 0
		for _, l := range buf.Lines(t) {
			if f, ok := l["failed_items"]; ok {
				omitted++
				if f != float64(12) || l["logged_failures"] != float64(10) {
					t.Fatalf("omission line = %v", l)
				}
			}
		}
		if omitted != 1 {
			t.Fatalf("got %d omission lines, want 1: %s", omitted, buf.String())
		}
		if res.Failed != 12 {
			t.Fatalf("result = %+v, want 12 failed", res)
		}
		if strings.Contains(buf.String(), "hunter2") {
			t.Fatal("secret leaked")
		}
	})
}

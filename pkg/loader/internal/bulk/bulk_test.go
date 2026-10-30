package bulk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
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

// TestPeekFirstByte verifies behavior for the related scenario.
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

// TestCountNDJSONLines verifies behavior for the related scenario.
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

// fakeNetError is a net.Error whose Timeout and Temporary results are set
// directly, so the classifier table can pin every flag combination.
type fakeNetError struct{ timeout, temporary bool }

func (e fakeNetError) Error() string   { return "fake net error" }
func (e fakeNetError) Timeout() bool   { return e.timeout }
func (e fakeNetError) Temporary() bool { return e.temporary }

// TestIsRetryableTransportError pins the retry decision for transport errors.
// Only net.Error.Timeout() retries; Temporary() is deprecated and no longer
// consulted, so a temporary-only error must not retry. A revert to
// Timeout() || Temporary() fails the named row.
func TestIsRetryableTransportError(t *testing.T) {
	t.Parallel()

	errnoOp := func(e syscall.Errno) error {
		return &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", e)}
	}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context canceled", context.Canceled, false},
		{"wrapped context canceled", fmt.Errorf("send: %w", context.Canceled), false},
		{"timeout is retried", fakeNetError{timeout: true}, true},
		{"timeout with temporary is retried", fakeNetError{timeout: true, temporary: true}, true},
		{"neither flag is not retried", fakeNetError{}, false},
		{"temporary-only is no longer retried FIX-NETTEMP-01", fakeNetError{temporary: true}, false},
		{"wrapped timeout is retried", fmt.Errorf("send: %w", fakeNetError{timeout: true}), true},
		{"wrapped temporary-only is not retried", fmt.Errorf("send: %w", fakeNetError{temporary: true}), false},
		{"context deadline exceeded is retried", context.DeadlineExceeded, true},
		{"wrapped context deadline exceeded is retried", fmt.Errorf("send: %w", context.DeadlineExceeded), true},
		{"temporary DNS failure is not retried", &net.DNSError{Err: "temporary lookup failure", Name: "es.invalid", IsTemporary: true}, false},
		{"DNS timeout is retried", &net.DNSError{Err: "i/o timeout", Name: "es.invalid", IsTimeout: true}, true},
		{"EINTR is not retried", errnoOp(syscall.EINTR), false},
		{"EMFILE is not retried", errnoOp(syscall.EMFILE), false},
		{"ENFILE is not retried", errnoOp(syscall.ENFILE), false},
		{"ECONNREFUSED is not retried", errnoOp(syscall.ECONNREFUSED), false},
		{"ECONNRESET is not retried", errnoOp(syscall.ECONNRESET), false},
		{"io.EOF is retried", io.EOF, true},
		{"wrapped io.EOF is retried", fmt.Errorf("read: %w", io.EOF), true},
		{"io.ErrUnexpectedEOF is retried", io.ErrUnexpectedEOF, true},
		{"plain error is retried", errors.New("boom"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isRetryableTransportError(tt.err); got != tt.want {
				t.Fatalf("isRetryableTransportError(%v) = %v, want %v", tt.err, got, tt.want)
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

// TestCheckDataFile pins the bounded preflight: open and first
// non-whitespace byte only, with Load's own failure Op.
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
	tests := []struct {
		name      string
		path      string
		wantFail  bool
		wantCause error
	}{
		{"missing", filepath.Join(dir, "missing.json"), true, os.ErrNotExist},
		{"directory", dir, true, nil},
		{"empty", write("empty.json", ""), true, io.EOF},
		{"whitespace only", write("ws.json", " \r\n\t\r\n"), true, io.EOF},
		{"array", write("array.json", `[{"id":1}]`), false, nil},
		{"ndjson with leading whitespace", write("nd.json", "\r\n  {\"id\":1}\r\n"), false, nil},
		{"unknown first byte is left to Load", write("x.json", "x"), false, nil},
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
			if !errors.As(err, &opErr) || opErr.Op != "Cannot read data file" {
				t.Fatalf("error = %#v, want operation error %q", err, "Cannot read data file")
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

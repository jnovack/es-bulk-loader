package managed

// Direct CreatePipelines tests for the FIX-PIPECAUSE-01 rejection boundary.
// They drive the real Elasticsearch client over a test-controlled transport so
// the response body's Read and Close calls, its read error and its context are
// observable; esstub cannot count reads on the client's response body. Every
// body, name and FAKE-SECRET value is synthetic and must change with the
// diagnostic contract.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

// rejectionTransport answers every request with status and a fresh body from
// the body factory, which sees the request context. A non-nil err fails the
// round trip instead. It records each "METHOD /path" in order.
type rejectionTransport struct {
	status int
	body   func(ctx context.Context) io.ReadCloser
	err    error

	trips atomic.Int32
	mu    sync.Mutex
	paths []string
}

func (tr *rejectionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tr.trips.Add(1)
	tr.mu.Lock()
	tr.paths = append(tr.paths, req.Method+" "+req.URL.Path)
	tr.mu.Unlock()
	if tr.err != nil {
		return nil, tr.err
	}
	header := http.Header{}
	header.Set("X-Elastic-Product", "Elasticsearch")
	header.Set("Content-Type", "application/json")
	return &http.Response{
		StatusCode: tr.status,
		Status:     strconv.Itoa(tr.status) + " " + http.StatusText(tr.status),
		Header:     header,
		Body:       tr.body(req.Context()),
		Request:    req,
	}, nil
}

func (tr *rejectionTransport) requested() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return slices.Clone(tr.paths)
}

// observedBody counts Read calls, bytes served and Close calls. With
// readStarted set, the first Read closes it and then blocks until ctx ends,
// returning ctx.Err(); with readErr set, Read fails with it.
type observedBody struct {
	r           io.Reader
	ctx         context.Context
	readStarted chan struct{}
	readErr     error

	reads, served, closes atomic.Int64
	startOnce             sync.Once
}

func (b *observedBody) Read(p []byte) (int, error) {
	b.reads.Add(1)
	if b.readStarted != nil {
		b.startOnce.Do(func() { close(b.readStarted) })
		<-b.ctx.Done()
		return 0, b.ctx.Err()
	}
	if b.readErr != nil {
		return 0, b.readErr
	}
	n, err := b.r.Read(p)
	b.served.Add(int64(n))
	return n, err
}

func (b *observedBody) Close() error {
	b.closes.Add(1)
	return nil
}

// endlessReader never ends: a body that would exhaust an unbounded read.
type endlessReader struct{}

func (endlessReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

// bodyRecorder is a body factory that records each observedBody it creates.
type bodyRecorder struct {
	mu     sync.Mutex
	bodies []*observedBody
	make   func(ctx context.Context) *observedBody
}

func (r *bodyRecorder) factory(ctx context.Context) io.ReadCloser {
	b := r.make(ctx)
	r.mu.Lock()
	r.bodies = append(r.bodies, b)
	r.mu.Unlock()
	return b
}

func (r *bodyRecorder) all() []*observedBody {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.bodies)
}

// staticBodies returns a recorder whose bodies serve s.
func staticBodies(s string) *bodyRecorder {
	return &bodyRecorder{make: func(ctx context.Context) *observedBody {
		return &observedBody{r: strings.NewReader(s), ctx: ctx}
	}}
}

// newTransportClient builds a real client over tr with retries disabled, so
// each declared pipeline is exactly one round trip.
func newTransportClient(t *testing.T, tr *rejectionTransport) *elasticsearch.Client {
	t.Helper()
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:    []string{"http://es.invalid"},
		Transport:    tr,
		DisableRetry: true,
	})
	if err != nil {
		t.Fatalf("new elasticsearch client: %v", err)
	}
	return es
}

// nopContext returns a test context carrying a per-test Nop logger, so no
// test writes the global logger.
func nopContext(t *testing.T) context.Context {
	nop := zerolog.Nop()
	return runlog.WithLogger(t.Context(), &nop)
}

// pipelineDefs declares an empty pipeline for each name, in order.
func pipelineDefs(names ...string) config.Definitions {
	defs := config.Definitions{}
	for _, name := range names {
		defs[name] = json.RawMessage(`{"processors":[]}`)
	}
	return defs
}

// requireRejectionOp returns the operation error after asserting the HTTP
// rejection Op and an *esresponse.Rejection cause.
func requireRejectionOp(t *testing.T, err error) *failure.Error {
	t.Helper()
	var opErr *failure.Error
	if !errors.As(err, &opErr) || opErr.Op != "Failed to create pipeline" {
		t.Fatalf("error = %#v, want Op %q", err, "Failed to create pipeline")
	}
	var rejection *esresponse.Rejection
	if !errors.As(err, &rejection) {
		t.Fatalf("cause = %T %v, want *esresponse.Rejection", opErr.Err, opErr.Err)
	}
	return opErr
}

// requireOneClose fails unless every recorded body was closed exactly once.
func requireOneClose(t *testing.T, bodies []*observedBody, wantBodies int) {
	t.Helper()
	if len(bodies) != wantBodies {
		t.Fatalf("bodies = %d, want %d", len(bodies), wantBodies)
	}
	for i, b := range bodies {
		if got := b.closes.Load(); got != 1 {
			t.Fatalf("body %d closed %d times, want 1", i, got)
		}
	}
}

const rootCauseBody = `{"error":{"root_cause":[{"type":"parse_exception","reason":"No processor type exists with name [fixture_proc]"}],` +
	`"type":"parse_exception","reason":"No processor type exists with name [fixture_proc]","stack_trace":"FAKE-TRACE"},"status":400}`

// TestCreatePipelinesRejectionCause guards that a rejected PUT returns the
// fixed Op with a diagnostic cause instead of the former nil cause, closes the
// body once and stops before the next declared pipeline.
func TestCreatePipelinesRejectionCause(t *testing.T) {
	t.Parallel()

	bodies := staticBodies(rootCauseBody)
	tr := &rejectionTransport{status: http.StatusBadRequest, body: bodies.factory}
	es := newTransportClient(t, tr)

	err := CreatePipelines(nopContext(t), es, pipelineDefs("p", "q"), []string{"p", "q"})
	requireRejectionOp(t, err)
	want := `Failed to create pipeline: pipeline "p" rejected with HTTP 400: parse_exception: No processor type exists with name [fixture_proc]`
	if err.Error() != want {
		t.Fatalf("message mismatch\n got: %s\nwant: %s", err.Error(), want)
	}
	requireOneClose(t, bodies.all(), 1)
	if got := tr.trips.Load(); got != 1 {
		t.Fatalf("round trips = %d, want 1", got)
	}
	if got, want := tr.requested(), []string{"PUT /_ingest/pipeline/p"}; !slices.Equal(got, want) {
		t.Fatalf("requests = %q, want %q", got, want)
	}
}

// TestCreatePipelinesSuccessClosesBody guards the unchanged success path:
// every declared pipeline is put in order and each body is closed once.
func TestCreatePipelinesSuccessClosesBody(t *testing.T) {
	t.Parallel()

	bodies := staticBodies(`{"acknowledged":true}`)
	tr := &rejectionTransport{status: http.StatusOK, body: bodies.factory}
	es := newTransportClient(t, tr)

	names := []string{"p1", "p2", "p3"}
	if err := CreatePipelines(nopContext(t), es, pipelineDefs(names...), names); err != nil {
		t.Fatalf("CreatePipelines: %v", err)
	}
	requireOneClose(t, bodies.all(), len(names))
	want := []string{"PUT /_ingest/pipeline/p1", "PUT /_ingest/pipeline/p2", "PUT /_ingest/pipeline/p3"}
	if got := tr.requested(); !slices.Equal(got, want) {
		t.Fatalf("requests = %q, want %q", got, want)
	}
}

// TestCreatePipelinesOversizedBodyIsBounded guards the bounded read: an
// endless rejection body is read to at most 64 KiB plus one probe byte.
func TestCreatePipelinesOversizedBodyIsBounded(t *testing.T) {
	t.Parallel()

	bodies := &bodyRecorder{make: func(ctx context.Context) *observedBody {
		return &observedBody{r: endlessReader{}, ctx: ctx}
	}}
	tr := &rejectionTransport{status: http.StatusBadRequest, body: bodies.factory}
	es := newTransportClient(t, tr)

	err := CreatePipelines(nopContext(t), es, pipelineDefs("p"), []string{"p"})
	requireRejectionOp(t, err)
	if !strings.Contains(err.Error(), "response exceeded 65536 bytes") {
		t.Fatalf("message %q lacks the oversized fallback", err.Error())
	}
	all := bodies.all()
	requireOneClose(t, all, 1)
	// Exactly the cap plus one probe byte: fewer could not detect oversize,
	// more means the read is not bounded.
	if served := all[0].served.Load(); served != esresponse.MaxRejectionBodyBytes+1 {
		t.Fatalf("served %d bytes, want exactly %d", served, esresponse.MaxRejectionBodyBytes+1)
	}
}

// TestCreatePipelinesBodyReadFailureKeepsCause guards that a body read error
// stays reachable through errors.Is while its text is not rendered.
func TestCreatePipelinesBodyReadFailureKeepsCause(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("FAKE-READ-FAILURE-TEXT")
	bodies := &bodyRecorder{make: func(ctx context.Context) *observedBody {
		return &observedBody{ctx: ctx, readErr: sentinel}
	}}
	tr := &rejectionTransport{status: http.StatusBadRequest, body: bodies.factory}
	es := newTransportClient(t, tr)

	err := CreatePipelines(nopContext(t), es, pipelineDefs("p"), []string{"p"})
	requireRejectionOp(t, err)
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want errors.Is sentinel", err)
	}
	if !strings.Contains(err.Error(), "response body read failed") || strings.Contains(err.Error(), sentinel.Error()) {
		t.Fatalf("message %q: want the read-failure fallback without the read error text", err.Error())
	}
	requireOneClose(t, bodies.all(), 1)
}

// TestCreatePipelinesBodyReadCancellation guards that cancelling while the
// rejection body is being read returns promptly with context.Canceled
// identity, and the body is still closed once.
func TestCreatePipelinesBodyReadCancellation(t *testing.T) {
	t.Parallel()

	readStarted := make(chan struct{})
	bodies := &bodyRecorder{make: func(ctx context.Context) *observedBody {
		return &observedBody{ctx: ctx, readStarted: readStarted}
	}}
	tr := &rejectionTransport{status: http.StatusBadRequest, body: bodies.factory}
	es := newTransportClient(t, tr)

	ctx, cancel := context.WithCancel(nopContext(t))
	done := make(chan error, 1)
	var worker sync.WaitGroup
	worker.Add(1)
	go func() {
		defer worker.Done()
		done <- CreatePipelines(ctx, es, pipelineDefs("p"), []string{"p"})
	}()
	// Registered before the first wait, so an early t.Fatal still cancels the
	// blocked read and joins the worker.
	t.Cleanup(func() {
		cancel()
		worker.Wait()
	})

	select {
	case <-readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("rejection body read did not start")
	}
	cancel()

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("CreatePipelines did not return after cancellation")
	}
	requireRejectionOp(t, err)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	requireOneClose(t, bodies.all(), 1)
}

// TestCreatePipelinesRejectionLogIsRedacted guards that the failure log line
// carries the same safe diagnostic as the returned cause and no raw body.
func TestCreatePipelinesRejectionLogIsRedacted(t *testing.T) {
	t.Parallel()

	secretBody := `{"error":{"root_cause":[{"type":"security_exception","reason":` +
		`"Authorization: Bearer FAKE-SECRET-A from https://u:FAKE-SECRET-B@es.example/x?api_key=FAKE-SECRET-C password=\"FAKE SECRET D\""}],` +
		`"type":"security_exception","reason":"denied","index":"FAKE-METADATA","stack_trace":"FAKE-TRACE"},"status":400}`
	leaks := []string{"FAKE-SECRET", "FAKE SECRET", "FAKE-METADATA", "FAKE-TRACE"}

	run := func(t *testing.T, name string) (string, *rejectionTransport, error) {
		t.Helper()
		var buf bytes.Buffer
		logger := zerolog.New(&buf)
		tr := &rejectionTransport{status: http.StatusBadRequest, body: staticBodies(secretBody).factory}
		es := newTransportClient(t, tr)
		err := CreatePipelines(runlog.WithLogger(t.Context(), &logger), es, pipelineDefs(name), []string{name})
		requireRejectionOp(t, err)
		logs := buf.String()
		for _, leak := range leaks {
			if strings.Contains(err.Error(), leak) {
				t.Fatalf("error leaks %q: %s", leak, err.Error())
			}
			if strings.Contains(logs, leak) {
				t.Fatalf("log leaks %q: %s", leak, logs)
			}
		}
		if strings.Contains(logs, `"body"`) {
			t.Fatalf("log still has a raw body field: %s", logs)
		}
		if !strings.Contains(logs, `"status_code":400`) || !strings.Contains(logs, "security_exception") {
			t.Fatalf("log lost status or diagnostic: %s", logs)
		}
		return logs, tr, err
	}

	t.Run("ordinary name", func(t *testing.T) {
		t.Parallel()
		logs, _, err := run(t, "p")
		if !strings.Contains(logs, `"pipeline":"p"`) {
			t.Fatalf("log lacks the pipeline field: %s", logs)
		}
		for _, want := range []string{`pipeline "p"`, "HTTP 400", "security_exception", "Authorization: [REDACTED]", "https://[REDACTED]@es.example"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q lacks %q", err.Error(), want)
			}
		}
	})

	t.Run("secret in the pipeline name", func(t *testing.T) {
		t.Parallel()
		name := "p password=FAKE-SECRET-NAME"
		logs, tr, err := run(t, name)
		// The request keeps the original name; only the display name is safe.
		if got, want := tr.requested(), []string{"PUT /_ingest/pipeline/" + name}; !slices.Equal(got, want) {
			t.Fatalf("requests = %q, want %q", got, want)
		}
		if !strings.Contains(logs, `"pipeline":"p password=[REDACTED]"`) {
			t.Fatalf("log lacks the sanitized pipeline field: %s", logs)
		}
		if !strings.Contains(err.Error(), `pipeline "p password=[REDACTED]" rejected with HTTP 400`) {
			t.Fatalf("error lacks the sanitized name: %s", err.Error())
		}
	})
}

// TestCreatePipelinesRejectionMessageBound guards the complete public message
// bound, Op prefix included, for a long multibyte reason and a long name that
// %q must escape.
func TestCreatePipelinesRejectionMessageBound(t *testing.T) {
	t.Parallel()

	body := `{"error":{"type":"parse_exception","reason":"` + strings.Repeat("é", 3000) + `"}}`
	tr := &rejectionTransport{status: http.StatusBadRequest, body: staticBodies(body).factory}
	es := newTransportClient(t, tr)
	name := strings.Repeat(`"é\`, 100)

	err := CreatePipelines(nopContext(t), es, pipelineDefs(name), []string{name})
	requireRejectionOp(t, err)
	msg := err.Error()
	if len(msg) > esresponse.MaxPublicMessageBytes || !utf8.ValidString(msg) {
		t.Fatalf("message is %d bytes, valid=%v; want <= %d bytes of valid UTF-8", len(msg), utf8.ValidString(msg), esresponse.MaxPublicMessageBytes)
	}
	if !strings.HasPrefix(msg, `Failed to create pipeline: pipeline "\"é\\`) || !strings.HasSuffix(msg, " [truncated]") {
		t.Fatalf("message %q: want the Op/name prefix and the truncation marker", msg)
	}
	if !strings.Contains(msg, "rejected with HTTP 400: parse_exception: éé") {
		t.Fatalf("message lacks status and reason: %q", msg)
	}
}

// TestCreatePipelinesTransportFailureUnchanged guards that a transport failure
// keeps its own Op and cause, is not turned into a rejection fallback and
// stops before the next pipeline.
func TestCreatePipelinesTransportFailureUnchanged(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("transport sentinel")
	tr := &rejectionTransport{err: sentinel}
	es := newTransportClient(t, tr)

	err := CreatePipelines(nopContext(t), es, pipelineDefs("p", "q"), []string{"p", "q"})
	requireOp(t, err, "creating pipeline")
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want errors.Is transport sentinel", err)
	}
	var rejection *esresponse.Rejection
	if errors.As(err, &rejection) {
		t.Fatalf("transport failure became a rejection: %v", err)
	}
	if got, want := tr.requested(), []string{"PUT /_ingest/pipeline/p"}; !slices.Equal(got, want) {
		t.Fatalf("requests = %q, want %q", got, want)
	}
}

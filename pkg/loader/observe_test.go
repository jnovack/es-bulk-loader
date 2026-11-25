package loader

// LOG-OBSERVE-01 gap 1: the structured Elasticsearch cause. Every Run here
// uses a Nop logger and asserts only on the returned error and the request
// trace. Every secret is synthetic.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/fixture"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/transform"
)

// replyShortBody declares more body bytes than it writes after a valid
// status line and headers, so the client's body read fails with
// io.ErrUnexpectedEOF. dropConnection cannot do this: it closes before any
// header, which is a transport error, not a body-read error.
func replyShortBody(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":{"type":"t","reason":"partial`))
	}
}

// pipelinePutStub serves a -sync-managed Run whose only pipeline "p" is
// answered by put: a direct runlog.Fail rejection site.
func pipelinePutStub(put http.HandlerFunc) func(t *testing.T) (*esstub.Stub, Options) {
	return func(t *testing.T) (*esstub.Stub, Options) {
		stub := newRouteStub(t, map[string]http.HandlerFunc{
			"HEAD /cards":             replyHeadOK,
			"PUT /_ingest/pipeline/p": put,
		})
		return stub, Options{
			URL: stub.URL(), Index: "cards", SyncManaged: true,
			PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), `{"p":{"processors":[]}}`),
		}
	}
}

// requireRunTrace fails unless Run's error has exactly Kind/Op/message and
// the stub saw exactly want, so a case still stops at the same site.
func requireRunTrace(t *testing.T, err error, stub *esstub.Stub, kind error, op, msg string, want []string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected Run to fail")
	}
	assertRunError(t, err, kind, op)
	if err.Error() != msg {
		t.Fatalf("message mismatch\n got: %s\nwant: %s", err.Error(), msg)
	}
	if got := requestLabels(stub); !slices.Equal(got, want) {
		t.Fatalf("requests = %q\n want     %q", got, want)
	}
}

// TestRunElasticsearchErrorReachable drives one Run per rejection site class
// and requires errors.As to reach an ElasticsearchError with the expected
// structure, while Run's message, Op, Kind and request trace stay exactly as
// the pinned run_errors_test.go expectations. Removing RunError.As or losing
// the flush pairs fails a row here. Every message here fits its Op budget, so
// runlog.Fail never cuts it and Bounded's copy path does not run: the
// Run-level Bounded guard is TestRunElasticsearchErrorRedactsAndBounds.
func TestRunElasticsearchErrorReachable(t *testing.T) {
	t.Parallel()

	const parseReason = "No processor type exists with name [fixture_proc]"
	pipelineBody := `{"error":{"root_cause":[{"type":"parse_exception","reason":"` + parseReason + `"}],` +
		`"type":"parse_exception","reason":"` + parseReason + `","stack_trace":"FAKE-TRACE"},"status":400}`
	const pipelinePrefix = `Failed to create pipeline: pipeline "p" rejected with HTTP 400: `
	const pipelineOp = "Failed to create pipeline"
	pipelineTrace := []string{"HEAD /cards", "PUT /_ingest/pipeline/p"}
	shortBody := `{"error":{"type":"t","reason":"r"}}`
	flushBody := `{"failures":[{"shard":0,"reason":{"type":"illegal_state_exception","reason":"disk is full"}},{"shard":1}]}`
	stageHead := []string{"HEAD /cards", "POST /cards/_refresh"}

	tests := []struct {
		name     string
		setup    func(t *testing.T) (*esstub.Stub, Options)
		wantKind error
		wantOp   string
		wantMsg  string
		wantReqs []string
		want     ElasticsearchError // exported fields only
		wantText string             // exact esErr.Error()
		wantIs   error              // read error reachable through both values
	}{
		{
			name:     "direct pipeline PUT rejection",
			setup:    pipelinePutStub(esstub.JSON(http.StatusBadRequest, pipelineBody)),
			wantKind: ErrManagedResource, wantOp: pipelineOp,
			wantMsg:  pipelinePrefix + "parse_exception: " + parseReason,
			wantReqs: pipelineTrace,
			want: ElasticsearchError{Resource: "pipeline", Name: "p", StatusCode: 400,
				Causes: []ElasticsearchCause{{Type: "parse_exception", Reason: parseReason}}},
			wantText: `pipeline "p" rejected with HTTP 400: parse_exception: ` + parseReason,
		},
		{
			name: "bulk request rejection has no name",
			setup: func(t *testing.T) (*esstub.Stub, Options) {
				stub, _ := newCardsBulkStub(t, bulkAlways(http.StatusBadRequest, `{"error":{"type":"illegal_argument_exception","reason":"bad bulk"}}`))
				return stub, Options{URL: stub.URL(), Index: "cards", AddToIndex: true, DataFile: writeBulkDataFixture(t), BatchSize: 1}
			},
			wantKind: ErrBulkFailure, wantOp: "Bulk API request failed",
			wantMsg:  "Bulk API request failed: bulk request rejected with HTTP 400: illegal_argument_exception: bad bulk",
			wantReqs: []string{"HEAD /cards", "POST /_bulk"},
			want: ElasticsearchError{Resource: "bulk request", StatusCode: 400,
				Causes: []ElasticsearchCause{{Type: "illegal_argument_exception", Reason: "bad bulk"}}},
			wantText: "bulk request rejected with HTTP 400: illegal_argument_exception: bad bulk",
		},
		{
			name: "enrich policy list request rejection",
			setup: func(t *testing.T) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards":          replyHeadOK,
					"POST /cards/_refresh": esstub.JSON(http.StatusOK, refreshBody),
				}, policyListRoutes(replyFail))
				return stub, Options{URL: stub.URL(), Index: "cards", Enrich: EnrichOptions{Enabled: true, All: true}}
			},
			wantKind: ErrEnrichExecution, wantOp: "Failed to get enrich policies",
			wantMsg:  "Failed to get enrich policies: enrich policy list request rejected with HTTP 500: stub_failure",
			wantReqs: append(slices.Clone(stageHead), "GET /_enrich/policy"),
			want: ElasticsearchError{Resource: "enrich policy list request", StatusCode: 500,
				Causes: []ElasticsearchCause{{Type: "stub_failure"}}},
			wantText: "enrich policy list request rejected with HTTP 500: stub_failure",
		},
		{
			name: "wrapped mapping fetch keeps ErrBulkFailure",
			setup: func(t *testing.T) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards":         replyHeadOK,
					"GET /cards/_mapping": esstub.JSON(http.StatusInternalServerError, shortBody),
				})
				return stub, Options{
					URL: stub.URL(), Index: "cards", FlushIndex: true, DataFile: writeBulkDataFixture(t),
					MappingsFile: fixture.WriteTempJSON(t, t.TempDir(), `{"properties":{"id":{"type":"keyword"}}}`),
				}
			},
			wantKind: ErrBulkFailure, wantOp: "Mapping preflight failed before bulk insert",
			wantMsg: `Mapping preflight failed before bulk insert: fetching mappings for index "cards" failed with status 500: ` +
				`index "cards" rejected with HTTP 500: t: r`,
			wantReqs: []string{"HEAD /cards", "GET /cards/_mapping"},
			want: ElasticsearchError{Resource: "index", Name: "cards", StatusCode: 500,
				Causes: []ElasticsearchCause{{Type: "t", Reason: "r"}}},
			wantText: `index "cards" rejected with HTTP 500: t: r`,
		},
		{
			name: "wrapped non-retryable transform _stats status",
			setup: func(t *testing.T) (*esstub.Stub, Options) {
				stub := newStageStub(t, map[string]http.HandlerFunc{
					"GET /_transform/tx1/_stats": esstub.JSON(http.StatusBadRequest, shortBody),
				})
				return stub, Options{
					URL: stub.URL(), Index: "cards", SyncManaged: true,
					TransformsFile: fixture.WriteTempJSON(t, t.TempDir(), stopTransforms),
					Transforms:     TransformOptions{Wait: true},
				}
			},
			wantKind: ErrManagedResource, wantOp: "Transform wait failed",
			wantMsg: `Transform wait failed: transform "tx1": _stats returned status 400: transform "tx1" rejected with HTTP 400: t: r`,
			wantReqs: slices.Concat(stageHead,
				[]string{"POST /_transform/tx1/_stop", "POST /_transform/tx2/_stop"},
				[]string{"GET /_transform/tx1", "PUT /_transform/tx1", "GET /_transform/tx2", "PUT /_transform/tx2"},
				[]string{"POST /_transform/tx1/_start", "POST /_transform/tx2/_start", "GET /_transform/tx1/_stats"}),
			want: ElasticsearchError{Resource: "transform", Name: "tx1", StatusCode: 400,
				Causes: []ElasticsearchCause{{Type: "t", Reason: "r"}}},
			wantText: `transform "tx1" rejected with HTTP 400: t: r`,
		},
		{
			name: "flush shard failure inside a 2xx response has status 0",
			setup: func(t *testing.T) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards":                  replyHeadOK,
					"POST /cards/_delete_by_query": esstub.JSON(http.StatusOK, flushBody),
				})
				return stub, Options{URL: stub.URL(), Index: "cards", FlushIndex: true, DataFile: writeBulkDataFixture(t)}
			},
			wantKind: ErrIndexOperation, wantOp: "Index flush completed with shard-level failures",
			wantMsg:  `Index flush completed with shard-level failures: index "cards" flush had 2 shard failures; first: illegal_state_exception: disk is full`,
			wantReqs: []string{"HEAD /cards", "POST /cards/_delete_by_query"},
			want: ElasticsearchError{Resource: "index", Name: "cards", StatusCode: 0,
				Causes: []ElasticsearchCause{{Type: "illegal_state_exception", Reason: "disk is full"}}},
			wantText: `index "cards" flush had 2 shard failures; first: illegal_state_exception: disk is full`,
		},
		{
			name:     "empty body fallback",
			setup:    pipelinePutStub(esstub.JSON(http.StatusBadRequest, "")),
			wantKind: ErrManagedResource, wantOp: pipelineOp,
			wantMsg:  pipelinePrefix + "ES error detail unavailable (empty response body)",
			wantReqs: pipelineTrace,
			want:     ElasticsearchError{Resource: "pipeline", Name: "p", StatusCode: 400},
			wantText: `pipeline "p" rejected with HTTP 400: ES error detail unavailable (empty response body)`,
		},
		{
			name:     "oversized body fallback",
			setup:    pipelinePutStub(esstub.JSON(http.StatusBadRequest, `{"error":{"type":"t","reason":"`+strings.Repeat("x", esresponse.MaxRejectionBodyBytes)+`"}}`)),
			wantKind: ErrManagedResource, wantOp: pipelineOp,
			wantMsg:  pipelinePrefix + "ES error detail unavailable (response exceeded 65536 bytes)",
			wantReqs: pipelineTrace,
			want:     ElasticsearchError{Resource: "pipeline", Name: "p", StatusCode: 400},
			wantText: `pipeline "p" rejected with HTTP 400: ES error detail unavailable (response exceeded 65536 bytes)`,
		},
		{
			name:     "non-JSON body fallback",
			setup:    pipelinePutStub(esstub.JSON(http.StatusBadRequest, "<html>FAKE-PROXY-PAGE</html>")),
			wantKind: ErrManagedResource, wantOp: pipelineOp,
			wantMsg:  pipelinePrefix + "ES error detail unavailable (response is not a single JSON value)",
			wantReqs: pipelineTrace,
			want:     ElasticsearchError{Resource: "pipeline", Name: "p", StatusCode: 400},
			wantText: `pipeline "p" rejected with HTTP 400: ES error detail unavailable (response is not a single JSON value)`,
		},
		{
			name:     "unrecognized JSON fallback",
			setup:    pipelinePutStub(esstub.JSON(http.StatusBadRequest, `{"error":true}`)),
			wantKind: ErrManagedResource, wantOp: pipelineOp,
			wantMsg:  pipelinePrefix + "ES error detail unavailable (no recognizable error field)",
			wantReqs: pipelineTrace,
			want:     ElasticsearchError{Resource: "pipeline", Name: "p", StatusCode: 400},
			wantText: `pipeline "p" rejected with HTTP 400: ES error detail unavailable (no recognizable error field)`,
		},
		{
			name:     "body read error fallback keeps the read error",
			setup:    pipelinePutStub(replyShortBody(http.StatusBadRequest)),
			wantKind: ErrManagedResource, wantOp: pipelineOp,
			wantMsg:  pipelinePrefix + "ES error detail unavailable (response body read failed)",
			wantReqs: pipelineTrace,
			want:     ElasticsearchError{Resource: "pipeline", Name: "p", StatusCode: 400},
			wantText: `pipeline "p" rejected with HTTP 400: ES error detail unavailable (response body read failed)`,
			wantIs:   io.ErrUnexpectedEOF,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stub, opts := tt.setup(t)
			nop := zerolog.Nop()
			opts.Logger = &nop

			_, err := Run(t.Context(), opts)

			requireRunTrace(t, err, stub, tt.wantKind, tt.wantOp, tt.wantMsg, tt.wantReqs)
			var esErr *ElasticsearchError
			if !errors.As(err, &esErr) || esErr == nil {
				t.Fatalf("errors.As(%v, *ElasticsearchError) = false", err)
			}
			if esErr.Resource != tt.want.Resource || esErr.Name != tt.want.Name || esErr.StatusCode != tt.want.StatusCode {
				t.Fatalf("got Resource %q Name %q StatusCode %d, want %q %q %d",
					esErr.Resource, esErr.Name, esErr.StatusCode, tt.want.Resource, tt.want.Name, tt.want.StatusCode)
			}
			if !slices.Equal(esErr.Causes, tt.want.Causes) {
				t.Fatalf("Causes = %q, want %q", esErr.Causes, tt.want.Causes)
			}
			if tt.want.Causes == nil && esErr.Causes != nil {
				t.Fatalf("fallback Causes = %#v, want nil", esErr.Causes)
			}
			// Error is the inner rejection; RunError keeps the outer context.
			if esErr.Error() != tt.wantText || !strings.HasSuffix(err.Error(), ": "+esErr.Error()) {
				t.Fatalf("esErr.Error() = %q, want %q as the tail of %q", esErr.Error(), tt.wantText, err.Error())
			}
			var rejection *esresponse.Rejection
			if !errors.As(err, &rejection) || rejection.Error() != esErr.Error() {
				t.Fatal("the internal rejection is no longer reachable, or its text differs from the projection")
			}
			if tt.wantIs != nil {
				if !errors.Is(err, tt.wantIs) || !errors.Is(esErr, tt.wantIs) {
					t.Fatalf("errors.Is(%v) through RunError = %v, through projection = %v",
						tt.wantIs, errors.Is(err, tt.wantIs), errors.Is(esErr, tt.wantIs))
				}
			} else if esErr.Unwrap() != nil {
				t.Fatalf("Unwrap = %v, want nil without a read error", esErr.Unwrap())
			}
		})
	}
}

// TestRunElasticsearchErrorRedactsAndBounds guards that no struct field of
// the projection holds unsanitized or unbounded text: every credential is
// [REDACTED] in Causes and Name too, and each field obeys its documented
// bound, including a Cause field cut from a 6 KB reason.
func TestRunElasticsearchErrorRedactsAndBounds(t *testing.T) {
	t.Parallel()

	secretReason := `Authorization: Bearer FAKE-SECRET-A at https://user:FAKE-SECRET-B@es.example/x?api_key=FAKE-SECRET-C`
	longReason := strings.Repeat("é", 3000) + " password=FAKE-SECRET-D"
	body := `{"error":{"root_cause":[{"type":"security_exception","reason":` + strconv.Quote(secretReason) + `}],` +
		`"type":"security_exception","reason":` + strconv.Quote(longReason) + `,"stack_trace":"FAKE-TRACE"},"status":400}`
	name := "p password=FAKE-SECRET-E " + strings.Repeat("n", 400)
	stub := newRouteStub(t, map[string]http.HandlerFunc{
		"HEAD /cards":                  replyHeadOK,
		"PUT /_ingest/pipeline/{name}": esstub.JSON(http.StatusBadRequest, body),
	})
	pipelines := `{` + strconv.Quote(name) + `:{"processors":[]}}`
	nop := zerolog.Nop()

	_, err := Run(t.Context(), Options{
		URL: stub.URL(), Index: "cards", SyncManaged: true,
		PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), pipelines), Logger: &nop,
	})

	var esErr *ElasticsearchError
	if !errors.As(err, &esErr) {
		t.Fatalf("errors.As(%v) = false", err)
	}
	if len(esErr.Causes) != 2 {
		t.Fatalf("Causes = %q, want the root cause and the long top-level pair", esErr.Causes)
	}
	fields := []string{esErr.Resource, esErr.Name, esErr.Error()}
	for _, c := range esErr.Causes {
		fields = append(fields, c.Type, c.Reason)
	}
	for _, f := range fields {
		for _, leak := range []string{"FAKE-SECRET", "FAKE-TRACE"} {
			if strings.Contains(f, leak) {
				t.Fatalf("field leaks %q: %.120q", leak, f)
			}
		}
		if !utf8.ValidString(f) || strings.ContainsAny(f, "\r\n") {
			t.Fatalf("field is not single-line UTF-8: %.120q", f)
		}
	}
	if len(esErr.Name) > 256 || !strings.HasSuffix(esErr.Name, " [truncated]") {
		t.Fatalf("Name is %d bytes (%.40q), want <= 256 with the marker", len(esErr.Name), esErr.Name)
	}
	if want := "security_exception"; esErr.Causes[0].Type != want ||
		!strings.Contains(esErr.Causes[0].Reason, "Authorization: [REDACTED]") ||
		!strings.Contains(esErr.Causes[0].Reason, "https://[REDACTED]@es.example") {
		t.Fatalf("Causes[0] = %q, want the redacted root cause", esErr.Causes[0])
	}
	for _, c := range esErr.Causes {
		if len(c.Type) > esresponse.MaxPublicMessageBytes || len(c.Reason) > esresponse.MaxPublicMessageBytes {
			t.Fatalf("cause field over %d bytes: %d/%d", esresponse.MaxPublicMessageBytes, len(c.Type), len(c.Reason))
		}
	}
	if long := esErr.Causes[1].Reason; len(long) < esresponse.MaxPublicMessageBytes-16 || !strings.HasSuffix(long, " [truncated]") {
		t.Fatalf("long reason is %d bytes (%.20q...), want the bounded cut with the marker", len(long), long)
	}
}

// TestRunErrorWithoutElasticsearchCause pins the documented exclusions: these
// failures keep their Kind, Op and unwrap identity and carry no
// ElasticsearchError, so errors.As returns false and leaves the target nil.
func TestRunErrorWithoutElasticsearchCause(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(t *testing.T) (*esstub.Stub, Options)
		wantKind error
		wantOp   string
		wantMsg  string
		wantReqs []string
		wantIs   error
	}{
		{
			name: "status-only index deletion",
			setup: func(t *testing.T) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards":   replyHeadOK,
					"DELETE /cards": replyFail,
				})
				return stub, Options{URL: stub.URL(), Index: "cards", DeleteIndex: true, DataFile: writeBulkDataFixture(t)}
			},
			wantKind: ErrIndexOperation, wantOp: "Failed to delete index",
			wantMsg:  "Failed to delete index: Failed to delete index",
			wantReqs: []string{"HEAD /cards", "DELETE /cards"},
		},
		{
			name: "mapping fetch body read failure",
			setup: func(t *testing.T) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards":         replyHeadOK,
					"GET /cards/_mapping": replyShortBody(http.StatusInternalServerError),
				})
				return stub, Options{
					URL: stub.URL(), Index: "cards", FlushIndex: true, DataFile: writeBulkDataFixture(t),
					MappingsFile: fixture.WriteTempJSON(t, t.TempDir(), `{"properties":{"id":{"type":"keyword"}}}`),
				}
			},
			wantKind: ErrBulkFailure, wantOp: "Mapping preflight failed before bulk insert",
			wantMsg:  `Mapping preflight failed before bulk insert: reading mappings response for index "cards": unexpected EOF`,
			wantReqs: []string{"HEAD /cards", "GET /cards/_mapping"},
			wantIs:   io.ErrUnexpectedEOF,
		},
		{
			// A failed state inside a 2xx _stats body keeps its sanitized
			// reason as text only (transform.Wait's fmt.Errorf), unlike the
			// flush shard failure, which builds a rejection. The transform
			// _stats read-failure exclusion is pinned at its source, in
			// transform's TestStatsGetterBoundsErrorBodiesButReadsSuccessInFull:
			// Wait retries that error, so a Run-level row would need the clock.
			name: "transform failed state inside a 2xx _stats body",
			setup: func(t *testing.T) (*esstub.Stub, Options) {
				stub := newStageStub(t, map[string]http.HandlerFunc{
					"GET /_transform/tx1/_stats": esstub.JSON(http.StatusOK,
						`{"count":1,"transforms":[{"id":"tx1","state":"failed","reason":"boom","checkpointing":{"last":{"checkpoint":0}}}]}`),
				})
				return stub, Options{
					URL: stub.URL(), Index: "cards", SyncManaged: true,
					TransformsFile: fixture.WriteTempJSON(t, t.TempDir(), stopTransforms),
					Transforms:     TransformOptions{Wait: true},
				}
			},
			wantKind: ErrManagedResource, wantOp: "Transform wait failed",
			wantMsg: `Transform wait failed: transform "tx1" failed: boom`,
			wantReqs: []string{
				"HEAD /cards", "POST /cards/_refresh",
				"POST /_transform/tx1/_stop", "POST /_transform/tx2/_stop",
				"GET /_transform/tx1", "PUT /_transform/tx1", "GET /_transform/tx2", "PUT /_transform/tx2",
				"POST /_transform/tx1/_start", "POST /_transform/tx2/_start", "GET /_transform/tx1/_stats",
			},
		},
		{
			name: "option validation",
			setup: func(t *testing.T) (*esstub.Stub, Options) {
				stub := esstub.New(t)
				return stub, Options{URL: stub.URL(), SyncManaged: true}
			},
			wantKind: ErrInvalidOptions, wantOp: "validating index option",
			wantMsg:  "validating index option: -index is required",
			wantReqs: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stub, opts := tt.setup(t)
			nop := zerolog.Nop()
			opts.Logger = &nop

			_, err := Run(t.Context(), opts)

			requireRunTrace(t, err, stub, tt.wantKind, tt.wantOp, tt.wantMsg, tt.wantReqs)
			var esErr *ElasticsearchError
			if errors.As(err, &esErr) || esErr != nil {
				t.Fatalf("errors.As found %#v, want no ElasticsearchError", esErr)
			}
			if tt.wantIs != nil && !errors.Is(err, tt.wantIs) {
				t.Fatalf("errors.Is(%v, %v) = false", err, tt.wantIs)
			}
		})
	}
}

// TestRetryableStatsTimeoutStaysTextOnly pins the documented limit: a wait
// timeout whose latest poll failed with a retryable _stats status keeps that
// rejection only as text, so no ElasticsearchError is reachable. It drives
// transform.Wait through its Getter seam in a synctest bubble (no wall-clock
// sleep) and wraps the error exactly as WaitForCheckpoint and Run do.
func TestRetryableStatsTimeoutStaysTextOnly(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		nop := zerolog.Nop()
		ctx := runlog.WithLogger(t.Context(), &nop)
		get := func(context.Context, string) (int, []byte, error) {
			return http.StatusServiceUnavailable, []byte(`{"error":{"type":"t","reason":"busy"}}`), nil
		}
		sleep := func(c context.Context, d time.Duration) error {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-timer.C:
				return nil
			case <-c.Done():
				return c.Err()
			}
		}

		_, waitErr := transform.Wait(ctx, get, "tx1", transform.WaitConfig{Timeout: 3 * time.Second, Sleep: sleep})
		err := runFailure(runlog.Fail(runlog.FailureEvent(ctx), "Transform wait failed", waitErr))

		assertRunError(t, err, ErrManagedResource, "Transform wait failed")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want context.DeadlineExceeded", err)
		}
		if !strings.Contains(err.Error(), `last poll failed: _stats returned status 503: transform "tx1" rejected with HTTP 503: t: busy`) {
			t.Fatalf("message %q lacks the retried rejection text", err.Error())
		}
		var esErr *ElasticsearchError
		if errors.As(err, &esErr) || esErr != nil {
			t.Fatalf("errors.As found %#v, want the timeout to stay text-only", esErr)
		}
	})
}

// TestRunErrorAsContract covers RunError.As and the projection at unit level:
// nil and zero values, wrappers, unrelated targets, preserved internal
// identities, independent slices and no target overwrite on a failed match.
func TestRunErrorAsContract(t *testing.T) {
	t.Parallel()

	const op = "Failed to create pipeline"
	body := []byte(`{"error":{"type":"t","reason":"r","caused_by":{"type":"c","reason":"cr"}}}`)
	newRunErr := func() (*RunError, *esresponse.Rejection) {
		rejection := esresponse.NewRejection("pipeline", "p", 400, body, nil, esresponse.MaxPublicMessageBytes)
		runErr, ok := runFailure(failure.New(op, rejection)).(*RunError)
		if !ok {
			t.Fatal("runFailure did not return *RunError")
		}
		return runErr, rejection
	}

	t.Run("nil and zero RunError", func(t *testing.T) {
		t.Parallel()
		for _, e := range []*RunError{nil, {}, {Kind: ErrManagedResource, Op: op}} {
			var esErr *ElasticsearchError
			if e.As(&esErr) || esErr != nil {
				t.Fatalf("%#v.As = true or set the target", e)
			}
		}
	})

	t.Run("nil and zero ElasticsearchError", func(t *testing.T) {
		t.Parallel()
		for _, e := range []*ElasticsearchError{nil, {}} {
			if e.Error() != "" || e.Unwrap() != nil {
				t.Fatalf("%#v: Error %q Unwrap %v, want empty and nil", e, e.Error(), e.Unwrap())
			}
		}
		if got := (&ElasticsearchError{Resource: "index", Name: "i"}).Error(); got != "" {
			t.Fatalf("a caller-built value has Error %q, want empty", got)
		}
	})

	t.Run("outer wrapper and exact fields", func(t *testing.T) {
		t.Parallel()
		runErr, _ := newRunErr()
		wrapped := fmt.Errorf("caller context: %w", runErr)
		var esErr *ElasticsearchError
		if !errors.As(wrapped, &esErr) {
			t.Fatal("errors.As through an outer %w wrapper = false")
		}
		want := []ElasticsearchCause{{Type: "t", Reason: "r"}, {Type: "c", Reason: "cr"}}
		if esErr.Resource != "pipeline" || esErr.Name != "p" || esErr.StatusCode != 400 || !slices.Equal(esErr.Causes, want) {
			t.Fatalf("projection = %+v", esErr)
		}
		if wrapped.Error() != "caller context: "+op+": "+esErr.Error() {
			t.Fatalf("message = %q", wrapped.Error())
		}
	})

	t.Run("RunError surface and internal identities are unchanged", func(t *testing.T) {
		t.Parallel()
		runErr, rejection := newRunErr()
		if runErr.Err != rejection || errors.Unwrap(runErr) != rejection {
			t.Fatal("RunError.Err/Unwrap no longer return the internal cause")
		}
		var gotRejection *esresponse.Rejection
		if !errors.As(runErr, &gotRejection) || gotRejection != rejection {
			t.Fatal("errors.As no longer reaches the internal rejection by identity")
		}
		var gotRun *RunError
		if !errors.As(fmt.Errorf("w: %w", runErr), &gotRun) || gotRun != runErr {
			t.Fatal("errors.As no longer reaches *RunError by identity")
		}
		nested := &RunError{Kind: ErrManagedResource, Op: op, Err: failure.New("inner", rejection)}
		var opErr *failure.Error
		if !errors.As(nested, &opErr) || opErr.Err != rejection {
			t.Fatal("errors.As no longer reaches a nested *failure.Error")
		}
		if !errors.Is(runErr, ErrManagedResource) || errors.Is(runErr, ErrIndexOperation) {
			t.Fatal("Is changed")
		}
		if runErr.Error() != op+": "+rejection.Error() {
			t.Fatalf("Error() = %q", runErr.Error())
		}
	})

	t.Run("read error identity through both values", func(t *testing.T) {
		t.Parallel()
		runErr := &RunError{Kind: ErrManagedResource, Op: op, Err: failure.New(op,
			esresponse.NewRejection("pipeline", "p", 400, nil, context.Canceled, esresponse.MaxPublicMessageBytes))}
		if !errors.Is(runErr, context.Canceled) {
			t.Fatal("errors.Is(RunError, context.Canceled) = false")
		}
		var esErr *ElasticsearchError
		if !errors.As(runErr, &esErr) || !errors.Is(esErr, context.Canceled) || esErr.Unwrap() != context.Canceled {
			t.Fatalf("projection lost the read error: %v", esErr)
		}
	})

	t.Run("unrelated targets and failed matches leave the target alone", func(t *testing.T) {
		t.Parallel()
		runErr, _ := newRunErr()
		var netErr net.Error
		if runErr.As(&netErr) || netErr != nil {
			t.Fatal("As matched an unrelated interface target")
		}
		var cause *ElasticsearchCause
		if runErr.As(&cause) || cause != nil {
			t.Fatal("As matched *ElasticsearchCause")
		}
		if runErr.As((**ElasticsearchError)(nil)) || runErr.As(nil) {
			t.Fatal("As accepted a nil target")
		}

		keep := &ElasticsearchError{Resource: "keep"}
		target := keep
		noRejection := &RunError{Kind: ErrInvalidOptions, Op: "validating index option", Err: errors.New("-index is required")}
		if errors.As(noRejection, &target) || target != keep {
			t.Fatalf("a failed match changed the target to %#v", target)
		}
		var empty *ElasticsearchError
		if errors.As(noRejection, &empty) || empty != nil {
			t.Fatal("errors.As on a chain with no rejection = true")
		}
	})

	t.Run("projections are independent", func(t *testing.T) {
		t.Parallel()
		runErr, rejection := newRunErr()
		var first, second *ElasticsearchError
		if !errors.As(runErr, &first) {
			t.Fatal("first projection missing")
		}
		first.Causes[0] = ElasticsearchCause{Type: "mutated"}
		first.Name = "mutated"
		if !errors.As(runErr, &second) || first == second {
			t.Fatal("second projection missing or shared")
		}
		if second.Causes[0] != (ElasticsearchCause{Type: "t", Reason: "r"}) || second.Name != "p" {
			t.Fatalf("mutating one projection changed the next: %+v", second)
		}
		if rejection.Pairs()[0] != (esresponse.Pair{Type: "t", Reason: "r"}) {
			t.Fatal("mutating a projection changed the internal rejection")
		}
	})

	t.Run("helper rejects nil and typed-nil inputs", func(t *testing.T) {
		t.Parallel()
		for _, err := range []error{nil, errors.New("plain"), failure.New(op, (*esresponse.Rejection)(nil))} {
			if got, ok := elasticsearchErrorFrom(err); ok || got != nil {
				t.Fatalf("elasticsearchErrorFrom(%T) = %#v, %v", err, got, ok)
			}
		}
	})
}

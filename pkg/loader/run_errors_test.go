package loader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/fixture"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/managed"
)

// TestRunErrorContract pins the public error contract at the Run boundary for
// every failure origin: the sentinel Kind (errors.Is), the operation string
// (errors.As to *RunError), the original cause where one exists, the exact
// message (no new prefix or wrapping; a failure without a cause keeps its
// duplicated "op: op" text, except a rejected pipeline PUT, which carries the
// bounded ES reason), and the partial Result returned alongside the
// error. Internal failures moved from panics to returned errors; a regression
// in classification, Op, cause or partial-Result population fails here.
func TestRunErrorContract(t *testing.T) {
	t.Parallel()

	type runErrorCase struct {
		name string
		// setup returns the Options for the failing Run; it may start a stub.
		setup      func(t *testing.T) Options
		wantKind   error
		wantOp     string
		wantCause  error  // checked with errors.Is when non-nil
		wantMsg    string // exact err.Error() when non-empty
		wantPrefix string // err.Error() prefix when non-empty
		wantResult *Result
	}
	tests := []runErrorCase{
		{
			name: "explicit auth validation",
			setup: func(*testing.T) Options {
				return Options{Index: "cards", SyncManaged: true, User: "u", Pass: "p", APIKey: "k"}
			},
			wantKind:   ErrInvalidOptions,
			wantOp:     "validating auth options",
			wantMsg:    "validating auth options: cannot use both basic auth and API key",
			wantResult: &Result{},
		},
		{
			name:     "explicit index validation",
			setup:    func(*testing.T) Options { return Options{SyncManaged: true} },
			wantKind: ErrInvalidOptions,
			wantOp:   "validating index option",
			wantMsg:  "validating index option: -index is required",
		},
		{
			name: "strict template preflight stays ErrInvalidOptions",
			setup: func(t *testing.T) Options {
				stub := esstub.New(t) // no routes: any request fails the test
				return Options{
					URL:           stub.URL(),
					Index:         "cards",
					SyncManaged:   true,
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), `{"p":{"v":"${ENRICH_BULK_TYPO}"}}`),
				}
			},
			wantKind:   ErrInvalidOptions,
			wantOp:     "validating template variables",
			wantPrefix: "validating template variables: unresolved template variables in ",
		},
		{
			name: "missing pipelines file keeps cause",
			setup: func(t *testing.T) Options {
				return Options{
					URL:           "http://127.0.0.1:1",
					Index:         "cards",
					SyncManaged:   true,
					PipelinesFile: filepath.Join(t.TempDir(), "missing.json"),
				}
			},
			wantKind:  ErrManagedResource,
			wantOp:    "Reading pipeline definitions file",
			wantCause: os.ErrNotExist,
		},
		{
			name: "malformed mappings file",
			setup: func(t *testing.T) Options {
				return Options{
					URL:          "http://127.0.0.1:1",
					Index:        "cards",
					AddToIndex:   true,
					DataFile:     writeBulkDataFixture(t),
					MappingsFile: fixture.WriteTempJSON(t, t.TempDir(), `{"mappings":`),
				}
			},
			wantKind: ErrLoaderExecution,
			wantOp:   "Parsing mappings file",
		},
		{
			name: "missing settings file fails before any request",
			setup: func(t *testing.T) Options {
				stub := esstub.New(t) // no routes: any request fails the test
				return Options{
					URL:          stub.URL(),
					Index:        "cards",
					AddToIndex:   true,
					DataFile:     writeBulkDataFixture(t),
					SettingsFile: filepath.Join(t.TempDir(), "missing-settings.json"),
				}
			},
			wantKind:   ErrLoaderExecution,
			wantOp:     "Reading settings file",
			wantCause:  os.ErrNotExist,
			wantResult: &Result{},
		},
		{
			name: "index existence transport failure",
			setup: func(*testing.T) Options {
				return Options{URL: "http://127.0.0.1:1", Index: "cards", Nuke: true}
			},
			wantKind:   ErrIndexOperation,
			wantOp:     "checking if index exists",
			wantPrefix: "checking if index exists: ",
		},
		{
			name: "index create rejected",
			setup: func(t *testing.T) Options {
				stub := esstub.New(t)
				stub.Handle("HEAD /cards", esstub.JSON(http.StatusNotFound, ""))
				stub.Handle("GET /_nodes", esstub.JSON(http.StatusOK, nodesBody))
				stub.Handle("PUT /cards", esstub.JSON(http.StatusInternalServerError, `{"error":{"type":"stub_failure"}}`))
				return Options{URL: stub.URL(), Index: "cards", AddToIndex: true, DataFile: writeBulkDataFixture(t)}
			},
			wantKind:   ErrIndexOperation,
			wantOp:     "Failed to create index",
			wantMsg:    "Failed to create index: index \"cards\" rejected with HTTP 500: stub_failure",
			wantResult: &Result{WriteIndex: "cards"},
		},
		{
			name: "mapping preflight keeps ErrBulkFailure",
			setup: func(t *testing.T) Options {
				fixtures := writePreflightRunFixtures(t)
				stub := newPreflightRunStub(t, preflightStubConfig{indexExists: true, liveMappings: preflightLiveMappings})
				return Options{URL: stub.url, Index: "cards", FlushIndex: true, DataFile: fixtures.data, MappingsFile: fixtures.divergentMappings}
			},
			wantKind:   ErrBulkFailure,
			wantOp:     "Mapping preflight failed before bulk insert",
			wantPrefix: `Mapping preflight failed before bulk insert: index "cards" mappings do not match the mappings file: `,
			wantResult: &Result{},
		},
		{
			name: "ambiguous policy reference",
			setup: func(t *testing.T) Options {
				stub := esstub.New(t)
				stub.Handle("GET /_enrich/policy", esstub.JSON(http.StatusOK, `{"policies":[{"config":{"match":{"name":"cards-bbbbbb"}}},{"config":{"match":{"name":"cards-aaaaaa"}}}]}`))
				return Options{
					URL:           stub.URL(),
					Index:         "products",
					SyncManaged:   true,
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), `{"p":{"processors":[{"enrich":{"policy_name":"cards"}}]}}`),
				}
			},
			wantKind: ErrManagedResource,
			wantOp:   "Ambiguous policy reference",
			wantMsg: `Ambiguous policy reference: logical policy "cards" matches managed generations cards-aaaaaa, cards-bbbbbb ` +
				`and this run supplied no definition for it; supply its definition or delete the stale generations`,
			wantResult: &Result{},
		},
		{
			// Deliberately changed with FIX-PIPECAUSE-01: the cause now carries
			// the pipeline, status and bounded ES type/reason instead of the
			// duplicated "op: op" text. Op and Kind are unchanged. The Nop
			// logger and nil Progress prove the message is built without them.
			name: "managed pipeline put rejected",
			setup: func(t *testing.T) Options {
				stub := esstub.New(t)
				stub.Handle("HEAD /cards", esstub.JSON(http.StatusOK, ""))
				stub.Handle("PUT /_ingest/pipeline/p", esstub.JSON(http.StatusBadRequest,
					`{"error":{"root_cause":[{"type":"parse_exception","reason":"No processor type exists with name [fixture_proc]"}],`+
						`"type":"parse_exception","reason":"No processor type exists with name [fixture_proc]","stack_trace":"FAKE-TRACE"},"status":400}`))
				nop := zerolog.Nop()
				return Options{
					URL:           stub.URL(),
					Index:         "cards",
					SyncManaged:   true,
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), `{"p":{"processors":[]}}`),
					Logger:        &nop,
				}
			},
			wantKind:   ErrManagedResource,
			wantOp:     "Failed to create pipeline",
			wantMsg:    `Failed to create pipeline: pipeline "p" rejected with HTTP 400: parse_exception: No processor type exists with name [fixture_proc]`,
			wantResult: &Result{WriteIndex: "cards"},
		},
		{
			name: "bulk request rejected",
			setup: func(t *testing.T) Options {
				stub, _ := newCardsBulkStub(t, bulkAlways(http.StatusBadRequest, `{"error":true}`))
				return Options{URL: stub.URL(), Index: "cards", AddToIndex: true, DataFile: writeBulkDataFixture(t), BatchSize: 1}
			},
			wantKind:   ErrBulkFailure,
			wantOp:     "Bulk API request failed",
			wantMsg:    "Bulk API request failed: bulk request rejected with HTTP 400: ES error detail unavailable (no recognizable error field)",
			wantResult: &Result{WriteIndex: "cards"},
		},
		{
			name: "enrich policy listing rejected",
			setup: func(t *testing.T) Options {
				stub := esstub.New(t)
				stub.Handle("HEAD /cards", esstub.JSON(http.StatusOK, ""))
				stub.Handle("POST /cards/_refresh", esstub.JSON(http.StatusOK, refreshBody))
				listing := esstub.JSON(http.StatusInternalServerError, `{"error":{"type":"stub_failure"}}`)
				stub.Handle("GET /_enrich/policy", listing)
				stub.Handle("GET /_enrich/policy/{$}", listing)
				return Options{URL: stub.URL(), Index: "cards", Enrich: EnrichOptions{Enabled: true, All: true}}
			},
			wantKind:   ErrEnrichExecution,
			wantOp:     "Failed to get enrich policies",
			wantMsg:    "Failed to get enrich policies: enrich policy list request rejected with HTTP 500: stub_failure",
			wantResult: &Result{WriteIndex: "cards"},
		},
	}

	// Pure option validation. Every row supplies valid earlier prerequisites
	// and points URL at a stub with no routes, so a validation that stops
	// returning early would reach the network and fail the test as an
	// unexpected request.
	validation := func(name, op, msg string, opts Options) {
		tests = append(tests, runErrorCase{
			name: name,
			setup: func(t *testing.T) Options {
				opts.URL = esstub.New(t).URL()
				return opts
			},
			wantKind:   ErrInvalidOptions,
			wantOp:     op,
			wantMsg:    op + ": " + msg,
			wantResult: &Result{},
		})
	}
	const exclusive = "-add, -flush, and -delete are mutually exclusive"
	validation("add and flush", "selecting data action", exclusive, Options{Index: "cards", AddToIndex: true, FlushIndex: true, DataFile: "d.json"})
	validation("add and delete", "selecting data action", exclusive, Options{Index: "cards", AddToIndex: true, DeleteIndex: true, DataFile: "d.json"})
	validation("flush and delete", "selecting data action", exclusive, Options{Index: "cards", FlushIndex: true, DeleteIndex: true, DataFile: "d.json"})
	validation("add, flush and delete", "selecting data action", exclusive, Options{Index: "cards", AddToIndex: true, FlushIndex: true, DeleteIndex: true, DataFile: "d.json"})
	const needsData = "-data is required for -add, -flush, and -delete"
	validation("add without data", "validating data option", needsData, Options{Index: "cards", AddToIndex: true})
	validation("flush without data", "validating data option", needsData, Options{Index: "cards", FlushIndex: true})
	validation("delete without data", "validating data option", needsData, Options{Index: "cards", DeleteIndex: true})
	validation("no action selected", "validating action selection",
		"one of data action, -sync-managed, -nuke, or -enrich is required", Options{Index: "cards"})
	validation("negative keep-last", "validating keep-last", "-keep-last must be 0 or greater",
		Options{Index: "cards", SyncManaged: true, KeepLast: -1})
	negative := -1
	validation("negative replicas", "validating replicas", "replicas must be 0 or greater",
		Options{Index: "cards", SyncManaged: true, Replicas: &negative})
	const bothAuth = "cannot use both basic auth and API key"
	validation("user with API key", "validating auth options", bothAuth, Options{Index: "cards", SyncManaged: true, User: "u", APIKey: "k"})
	validation("pass with API key", "validating auth options", bothAuth, Options{Index: "cards", SyncManaged: true, Pass: "p", APIKey: "k"})
	// A lone credential must fail loudly rather than connect anonymously.
	// The values are distinctive so a message that leaked them would differ
	// from the exact expected text.
	validation("user only", "validating auth options", "basic auth requires both -user and -pass: -pass is empty",
		Options{Index: "cards", SyncManaged: true, User: "lone-user-value"})
	validation("pass only", "validating auth options", "basic auth requires both -user and -pass: -user is empty",
		Options{Index: "cards", SyncManaged: true, Pass: "lone-pass-value"})
	// Ordering guard: the lone-credential check must run before action and
	// index validation. These rows also carry invalid later options, so moving
	// the check below them would change the reported Op and fail here.
	validation("user only before action validation", "validating auth options", "basic auth requires both -user and -pass: -pass is empty",
		Options{Index: "cards", AddToIndex: true, FlushIndex: true, DataFile: "d.json", User: "lone-user-value"})
	validation("pass only before index validation", "validating auth options", "basic auth requires both -user and -pass: -user is empty",
		Options{SyncManaged: true, KeepLast: -1, Pass: "lone-pass-value"})
	// Client construction is a setup failure, not option validation: its Op keeps
	// the loader-execution classification and the parser's own cause.
	tests = append(tests, runErrorCase{
		name:       "invalid client URL",
		setup:      func(*testing.T) Options { return Options{URL: "http://%", Index: "cards", SyncManaged: true} },
		wantKind:   ErrLoaderExecution,
		wantOp:     "creating Elasticsearch client",
		wantPrefix: "creating Elasticsearch client: ",
		wantResult: &Result{},
	})

	// FIX-DATAFMT-01 boundary: plain -delete with an unknown-format file keeps
	// the duplicated "op: op" message and a zero Result (no warnings apply).
	tests = append(tests, runErrorCase{
		name: "unknown data format preflight",
		setup: func(t *testing.T) Options {
			return Options{
				URL: esstub.New(t).URL(), Index: "cards", DeleteIndex: true,
				DataFile: writeInputFile(t, "data.json", "id,name\n1,Alice\n"),
			}
		},
		wantKind:   ErrLoaderExecution,
		wantOp:     unknownDataFormatOp,
		wantMsg:    unknownDataFormatOp + ": " + unknownDataFormatOp,
		wantResult: &Result{},
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := Run(t.Context(), tt.setup(t))
			if err == nil {
				t.Fatal("expected Run to fail")
			}
			if !errors.Is(err, tt.wantKind) {
				t.Fatalf("error = %v, want kind %v", err, tt.wantKind)
			}
			var runErr *RunError
			if !errors.As(err, &runErr) {
				t.Fatalf("error = %T %v, want *RunError", err, err)
			}
			if runErr.Kind != tt.wantKind {
				t.Fatalf("Kind = %v, want exactly %v", runErr.Kind, tt.wantKind)
			}
			if runErr.Op != tt.wantOp {
				t.Fatalf("Op = %q, want %q", runErr.Op, tt.wantOp)
			}
			if runErr.Err == nil {
				t.Fatal("RunError.Err is nil; the original cause must be kept")
			}
			if tt.wantCause != nil && !errors.Is(err, tt.wantCause) {
				t.Fatalf("error = %v, want cause %v", err, tt.wantCause)
			}
			if tt.wantMsg != "" && err.Error() != tt.wantMsg {
				t.Fatalf("message mismatch\n got: %s\nwant: %s", err.Error(), tt.wantMsg)
			}
			if tt.wantPrefix != "" && !strings.HasPrefix(err.Error(), tt.wantPrefix) {
				t.Fatalf("message %q does not start with %q", err.Error(), tt.wantPrefix)
			}
			if tt.wantResult != nil && !reflect.DeepEqual(result, *tt.wantResult) {
				t.Fatalf("partial result = %+v, want %+v", result, *tt.wantResult)
			}
		})
	}
}

// assertRunError fails the test unless err is a *RunError with exactly
// wantKind, an Op equal to one of wantOps, and a non-nil cause. Several ops
// are accepted only where the failing step is legitimately timing-dependent.
func assertRunError(t *testing.T, err error, wantKind error, wantOps ...string) {
	t.Helper()
	var runErr *RunError
	if !errors.As(err, &runErr) {
		t.Fatalf("error = %T %v, want *RunError", err, err)
	}
	if runErr.Kind != wantKind {
		t.Fatalf("Kind = %v, want exactly %v (error %v)", runErr.Kind, wantKind, err)
	}
	if !slices.Contains(wantOps, runErr.Op) {
		t.Fatalf("Op = %q, want one of %q", runErr.Op, wantOps)
	}
	if runErr.Err == nil {
		t.Fatal("RunError.Err is nil; the original cause must be kept")
	}
}

// failBody is the error body of an injected Elasticsearch failure.
const failBody = `{"error":{"type":"stub_failure"}}`

// Fixed replies shared by the stop-after-failure tables. They are stateless, so
// parallel cases may use them in any number of stubs.
var (
	replyAck         = esstub.JSON(http.StatusOK, ackBody)
	replyFail        = esstub.JSON(http.StatusInternalServerError, failBody)
	replyHeadOK      = esstub.JSON(http.StatusOK, "")
	replyHeadMissing = esstub.JSON(http.StatusNotFound, "")
)

// generationSuffix matches the 14-digit wall-clock suffix of a generated alias
// generation; tsNames cases replace it so their traces stay deterministic.
var generationSuffix = regexp.MustCompile(`-\d{14}`)

// runStopCase is one stop-after-failure scenario for Run. setup builds a fresh
// strict stub and the Options that drive Run into one failing step; assertRunStops
// then requires the exact Kind and Op and that the stub saw exactly the expected
// requests, so a failing step that no longer ends the run extends the trace.
type runStopCase struct {
	name  string
	setup func(t *testing.T, rec *opRecorder) (*esstub.Stub, Options)

	wantKind  error
	wantOp    string
	wantCause error // checked with errors.Is when non-nil

	// wantRequests is the exact "METHOD /path" trace, the failing request last;
	// an empty non-nil slice asserts Run failed before sending anything.
	// wantAnyOf lists acceptable traces where map order makes the failing
	// resource undefined. tsNames normalizes generated -YYYYMMDDHHMMSS suffixes
	// to -<ts> before comparing.
	wantRequests []string
	wantAnyOf    [][]string
	tsNames      bool

	// wantLast and forbidden are the older opRecorder-based form: the last
	// recorded operation, and operations that must not appear at all.
	wantLast  string
	forbidden []string

	// checkEvents, when set, receives every ProgressEvent Run emitted.
	checkEvents func(t *testing.T, events []ProgressEvent)
	// fakeClock installs installFakeClock for the case; it swaps package-global
	// seams, so it is only valid in a serial table.
	fakeClock bool
}

// assertRunStops runs the case's Run and checks the error and the request
// trace. It is the single assertion body behind every stop-after-failure table.
func assertRunStops(t *testing.T, tc runStopCase) {
	t.Helper()

	if tc.wantRequests == nil && tc.wantAnyOf == nil && tc.wantLast == "" {
		t.Fatal("runStopCase needs wantRequests, wantAnyOf or wantLast")
	}
	rec := &opRecorder{}
	stub, opts := tc.setup(t, rec)
	var events []ProgressEvent
	if tc.checkEvents != nil {
		opts.Progress = func(event ProgressEvent) { events = append(events, event) }
	}

	_, err := Run(t.Context(), opts)
	if err == nil {
		t.Fatal("expected Run to fail")
	}
	assertRunError(t, err, tc.wantKind, tc.wantOp)
	if tc.wantCause != nil && !errors.Is(err, tc.wantCause) {
		t.Fatalf("error = %v, want cause %v", err, tc.wantCause)
	}

	labels := requestLabels(stub)
	if tc.tsNames {
		for i, label := range labels {
			labels[i] = generationSuffix.ReplaceAllString(label, "-<ts>")
		}
	}
	switch {
	case tc.wantRequests != nil:
		if !slices.Equal(labels, tc.wantRequests) {
			t.Fatalf("requests = %q\n want     %q", labels, tc.wantRequests)
		}
	case tc.wantAnyOf != nil:
		if !slices.ContainsFunc(tc.wantAnyOf, func(want []string) bool { return slices.Equal(labels, want) }) {
			t.Fatalf("requests = %q, want one of %q", labels, tc.wantAnyOf)
		}
	}
	if tc.wantLast != "" {
		ops := rec.operations()
		if len(ops) == 0 || ops[len(ops)-1] != tc.wantLast {
			t.Fatalf("operations = %v, want %q to be the last request", ops, tc.wantLast)
		}
		for _, op := range tc.forbidden {
			if slices.Contains(ops, op) {
				t.Fatalf("operations = %v: %q ran after the failure", ops, op)
			}
		}
	}
	if tc.checkEvents != nil {
		tc.checkEvents(t, events)
	}
}

// runStopCases runs each case as a subtest. parallel tables must not set
// fakeClock: installFakeClock replaces package globals and its test and every
// ancestor must stay serial.
func runStopCases(t *testing.T, parallel bool, cases []runStopCase) {
	t.Helper()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.fakeClock {
				if parallel {
					t.Fatal("fakeClock cases must run in a serial table")
				}
				installFakeClock(t, time.Second)
			}
			if parallel {
				t.Parallel()
			}
			assertRunStops(t, tc)
		})
	}
}

// newRouteStub starts a strict stub serving exactly routes. A map cannot
// register one pattern twice, which is what makes per-case overrides of a base
// route set safe (Stub.Handle panics on a duplicate pattern).
func newRouteStub(t *testing.T, routes ...map[string]http.HandlerFunc) *esstub.Stub {
	t.Helper()

	merged := map[string]http.HandlerFunc{}
	for _, set := range routes {
		maps.Copy(merged, set)
	}
	stub := esstub.New(t)
	for pattern, handler := range merged {
		stub.Handle(pattern, handler)
	}
	return stub
}

// sequenced answers the nth request with replies[n], repeating the last reply.
func sequenced(replies ...http.HandlerFunc) http.HandlerFunc {
	var calls atomic.Int32
	return func(w http.ResponseWriter, r *http.Request) {
		i := min(int(calls.Add(1))-1, len(replies)-1)
		replies[i](w, r)
	}
}

// policyListRoutes serves the policy listing on both client spellings of the
// path, so whichever the client uses reaches handler (and one shared counter).
func policyListRoutes(handler http.HandlerFunc) map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"GET /_enrich/policy":     handler,
		"GET /_enrich/policy/{$}": handler,
	}
}

// managedPolicyName returns the content-hashed name Run gives the logical
// policy declared with body, computed by the same planner Run uses.
func managedPolicyName(t *testing.T, logical, body string) string {
	t.Helper()
	plan, err := managed.BuildPolicyPlan(t.Context(), config.Definitions{logical: json.RawMessage(body)}, []string{logical})
	if err != nil {
		t.Fatalf("BuildPolicyPlan: %v", err)
	}
	return plan.LogicalToDesired[logical]
}

// Fixtures for the stop-after-failure tables.
const (
	stopPipelines  = `{"p1":{"processors":[]},"p2":{"processors":[]}}`
	stopPolicyBody = `{"match":{"indices":"cards","match_field":"id","enrich_fields":["name"]}}`
	stopTransforms = `{"tx1":{"source_index":"cards","body":{"source":{"index":"cards"},"dest":{"index":"dest"},"pivot":{}}},` +
		`"tx2":{"source_index":"cards","body":{"source":{"index":"cards"},"dest":{"index":"dest"},"pivot":{}}}}`
)

// TestRunFailureBoundary pins every branch of the single internal-to-public
// error conversion. A wrapped operation error must keep its Op and Kind: a
// type switch would let it fall through to ErrLoaderExecution with an empty
// Op, and an already-classified *RunError must not be re-classified by
// substring (strict-template validation would become ErrLoaderExecution).
func TestRunFailureBoundary(t *testing.T) {
	t.Parallel()

	cause := errors.New("cause")
	classified := &RunError{Kind: ErrInvalidOptions, Op: "validating template variables", Err: cause}
	opErr := failure.New("Failed to update alias", cause)
	plain := errors.New("plain failure")

	tests := []struct {
		name      string
		in        error
		wantNil   bool
		wantSame  error // when non-nil, the result must be this exact value
		wantKind  error
		wantOp    string
		wantCause error
	}{
		{name: "nil stays nil", in: nil, wantNil: true},
		{name: "classified RunError unchanged", in: classified, wantSame: classified},
		{name: "operation error classified from Op", in: opErr, wantKind: ErrIndexOperation, wantOp: "Failed to update alias", wantCause: cause},
		{name: "wrapped operation error keeps Op and Kind", in: fmt.Errorf("context: %w", opErr), wantKind: ErrIndexOperation, wantOp: "Failed to update alias", wantCause: cause},
		{name: "nil-cause operation error keeps errors.New(op)", in: failure.New("Bulk API request failed", nil), wantKind: ErrBulkFailure, wantOp: "Bulk API request failed"},
		{name: "plain error falls back to ErrLoaderExecution", in: plain, wantKind: ErrLoaderExecution, wantOp: "", wantCause: plain},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := runFailure(tt.in)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("runFailure(nil) = %v, want nil", got)
				}
				return
			}
			if tt.wantSame != nil {
				if got != tt.wantSame {
					t.Fatalf("runFailure returned %#v, want the same *RunError %#v", got, tt.wantSame)
				}
				return
			}
			var runErr *RunError
			if !errors.As(got, &runErr) {
				t.Fatalf("runFailure = %T %v, want *RunError", got, got)
			}
			if runErr.Kind != tt.wantKind || runErr.Op != tt.wantOp {
				t.Fatalf("got Kind=%v Op=%q, want Kind=%v Op=%q", runErr.Kind, runErr.Op, tt.wantKind, tt.wantOp)
			}
			if runErr.Err == nil {
				t.Fatal("RunError.Err is nil; the cause must be kept")
			}
			if tt.wantCause != nil && !errors.Is(got, tt.wantCause) {
				t.Fatalf("runFailure = %v, want cause %v", got, tt.wantCause)
			}
		})
	}
}

// TestRunErrorErrorNeverPanics pins RunError.Error for every combination of
// Op, Kind and Err, including the zero value. Error is called directly because
// callers such as loggers format a RunError outside Run. A nil Err with an
// empty Op must fall back to Kind or ErrLoaderExecution rather than
// dereference nil, and formatting must never write to the value or change its
// errors.Is/Unwrap results.
func TestRunErrorErrorNeverPanics(t *testing.T) {
	t.Parallel()

	cause := errors.New("cause")
	custom := errors.New("custom kind")

	tests := []struct {
		name       string
		in         *RunError
		want       string
		wantUnwrap error   // expected Unwrap result; nil means nil
		wantIs     []error // each must match through errors.Is
		notIs      []error // each must not match through errors.Is
	}{
		{name: "nil receiver", in: nil, want: ""},
		{name: "Op and Err", in: &RunError{Op: "op", Err: cause}, want: "op: cause", wantUnwrap: cause, wantIs: []error{cause}},
		{name: "Op only", in: &RunError{Op: "op"}, want: "op"},
		{name: "Err only", in: &RunError{Err: cause}, want: "cause", wantUnwrap: cause, wantIs: []error{cause}},
		{name: "Kind only", in: &RunError{Kind: ErrBulkFailure}, want: "bulk insert failed", wantIs: []error{ErrBulkFailure}},
		{name: "execution Kind only", in: &RunError{Kind: ErrLoaderExecution}, want: "loader execution failed", wantIs: []error{ErrLoaderExecution}},
		{name: "custom Kind only", in: &RunError{Kind: custom}, want: "custom kind", wantIs: []error{custom}},
		{name: "zero value", in: &RunError{}, want: "loader execution failed", notIs: []error{ErrLoaderExecution, ErrBulkFailure}},
		{name: "Kind with Op", in: &RunError{Kind: ErrBulkFailure, Op: "op"}, want: "op", wantIs: []error{ErrBulkFailure}},
		{name: "Kind with Err", in: &RunError{Kind: ErrBulkFailure, Err: cause}, want: "cause", wantUnwrap: cause, wantIs: []error{ErrBulkFailure, cause}},
		{name: "all fields", in: &RunError{Kind: ErrBulkFailure, Op: "op", Err: cause}, want: "op: cause", wantUnwrap: cause, wantIs: []error{ErrBulkFailure, cause}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.in == nil {
				if got := tt.in.Error(); got != tt.want {
					t.Fatalf("Error() = %q, want %q", got, tt.want)
				}
				return
			}

			before := *tt.in
			if got := tt.in.Error(); got != tt.want {
				t.Fatalf("Error() = %q, want %q", got, tt.want)
			}
			if got := tt.in.Unwrap(); got != tt.wantUnwrap {
				t.Fatalf("Unwrap() = %v, want %v", got, tt.wantUnwrap)
			}
			for _, target := range tt.wantIs {
				if !errors.Is(tt.in, target) {
					t.Fatalf("errors.Is(%v, %v) = false, want true", tt.in, target)
				}
			}
			for _, target := range tt.notIs {
				if errors.Is(tt.in, target) {
					t.Fatalf("errors.Is(%v, %v) = true, want false", tt.in, target)
				}
			}
			if tt.in.Kind != before.Kind || tt.in.Op != before.Op || tt.in.Err != before.Err {
				t.Fatalf("Error() mutated fields: got %+v, want %+v", *tt.in, before)
			}
		})
	}
}

// TestOperationKindsOfMovedHelpers keeps the public Kind evidence for helper
// failures whose direct tests now live in their owner packages, where only the
// internal Op is visible. Kind is derived from Op at the Run boundary, so each
// op string an owner test pins must still classify as it did before the move.
func TestOperationKindsOfMovedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		op   string
		want error
	}{
		{"waiting for index creation", ErrIndexOperation},
		{"Context cancelled while waiting for index to become visible", ErrIndexOperation},
		{"Index flush completed with shard-level failures", ErrIndexOperation},
		{"Unable to parse index flush response body", ErrIndexOperation},
		{"Failed to create index", ErrIndexOperation},
		{"Mapping preflight failed before bulk insert", ErrBulkFailure},
		{"creating enrich policy", ErrEnrichExecution},
		{"Context cancelled during enrich policy retry", ErrEnrichExecution},
		{"deleting pipeline", ErrManagedResource},
		{"Ambiguous policy reference", ErrManagedResource},
		{"Unable to serialize document for bulk insert", ErrBulkFailure},
		{"Transform wait failed", ErrManagedResource},
		{"Reading settings file", ErrLoaderExecution},
		{"Reading pipeline definitions file", ErrManagedResource},
		{"listing timestamped indices", ErrIndexOperation},
		{"Failed to list timestamped indices", ErrIndexOperation},
		{"Failed to delete pipeline; nuke mode could not find indices using it as default pipeline", ErrManagedResource},
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			t.Parallel()
			assertRunError(t, runFailure(failure.New(tt.op, errors.New("cause"))), tt.want, tt.op)
		})
	}
}

// TestTimestampedListingErrorBoundaryPreservesCause guards that correcting the
// Kind of the generation-listing failures leaves Op, cause and message intact.
func TestTimestampedListingErrorBoundaryPreservesCause(t *testing.T) {
	t.Parallel()

	tests := []struct {
		op    string
		cause error
	}{
		{"listing timestamped indices", errors.New("unique transport cause")},
		{"Failed to list timestamped indices", errors.New("unique status cause")},
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			t.Parallel()

			got := runFailure(failure.New(tt.op, tt.cause))
			var runErr *RunError
			if !errors.As(got, &runErr) {
				t.Fatalf("runFailure = %T %v, want *RunError", got, got)
			}
			if !errors.Is(got, ErrIndexOperation) {
				t.Fatalf("Kind = %v, want ErrIndexOperation", runErr.Kind)
			}
			if runErr.Op != tt.op {
				t.Fatalf("Op = %q, want %q", runErr.Op, tt.op)
			}
			if runErr.Err != tt.cause || !errors.Is(got, tt.cause) {
				t.Fatalf("Err = %v, want exact cause %v", runErr.Err, tt.cause)
			}
			if want := tt.op + ": " + tt.cause.Error(); got.Error() != want {
				t.Fatalf("message = %q, want %q", got.Error(), want)
			}
		})
	}
}

// TestRunCallbackPanicContract pins the caller-callback panic semantics that
// Run's narrow outer recover owns now that internal failures are returned: an
// error value fails Run as Kind ErrLoaderExecution with Op "panic", a *RunError
// value is returned unchanged, and any other value propagates.
func TestRunCallbackPanicContract(t *testing.T) {
	boom := errors.New("callback exploded")
	callerRunErr := &RunError{Kind: ErrEnrichExecution, Op: "caller op", Err: boom}

	tests := []struct {
		name      string
		value     any
		wantSame  *RunError // a *RunError panic must come back as this pointer
		wantKind  error
		wantOp    string
		propagate bool
	}{
		{name: "error panic becomes a failed Run", value: boom, wantKind: ErrLoaderExecution, wantOp: "panic"},
		{name: "RunError panic returned unchanged", value: callerRunErr, wantSame: callerRunErr},
		{name: "non-error panic propagates", value: "string panic", propagate: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Not parallel: installFakeClock swaps package clock seams.
			installFakeClock(t, time.Second)
			server, ops := newProgressStub(t, nil)
			panicking := func(o *Options) { o.Progress = func(ProgressEvent) { panic(tt.value) } }

			if tt.propagate {
				defer func() {
					if recovered := recover(); recovered != tt.value {
						t.Fatalf("recovered %v, want the original panic value to propagate", recovered)
					}
				}()
				runProgress(t, server, ops, zerolog.InfoLevel, panicking)
				t.Fatal("Run returned; the non-error panic should have propagated")
			}

			run := runProgress(t, server, ops, zerolog.InfoLevel, panicking)
			if tt.wantSame != nil {
				var runErr *RunError
				if !errors.As(run.err, &runErr) || runErr != tt.wantSame {
					t.Fatalf("error = %#v, want the panicked *RunError unchanged", run.err)
				}
				return
			}
			assertRunError(t, run.err, tt.wantKind, tt.wantOp)
			if !errors.Is(run.err, boom) {
				t.Fatalf("error = %v, want it to wrap the panic value", run.err)
			}
		})
	}
}

// TestRunStopsAfterDestructiveFailure guards the early returns that replaced
// the panic bridge's automatic unwinding. Each case fails one destructive or
// lifecycle step and asserts that the failing request is the last one Run
// sends: nothing destructive, no managed-resource write and no bulk request
// may follow. Dropping one of Run's hand-written `return result, runFailure`
// lines (or a helper's `return err`) lets the run continue and fails here.
func TestRunStopsAfterDestructiveFailure(t *testing.T) {
	t.Parallel()

	runStopCases(t, true, []runStopCase{
		{
			name: "index delete failure issues no managed-resource delete",
			setup: func(t *testing.T, rec *opRecorder) (*esstub.Stub, Options) {
				stub := esstub.New(t)
				stub.Handle("HEAD /cards", rec.recorded("index.head", esstub.JSON(http.StatusOK, "")))
				stub.Handle("DELETE /cards", rec.recorded("index.delete", esstub.JSON(http.StatusInternalServerError, failBody)))
				stub.Handle("DELETE /_ingest/pipeline/{name}", rec.recorded("pipeline.delete", esstub.JSON(http.StatusOK, ackBody)))
				return stub, Options{
					URL:           stub.URL(),
					Index:         "cards",
					DeleteIndex:   true,
					DataFile:      writeBulkDataFixture(t),
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), `{"p":{"processors":[]}}`),
				}
			},
			wantKind:  ErrIndexOperation,
			wantOp:    "Failed to delete index",
			wantLast:  "index.delete",
			forbidden: []string{"pipeline.delete"},
		},
		{
			name: "alias update failure issues no policy create, GC or prune",
			setup: func(t *testing.T, rec *opRecorder) (*esstub.Stub, Options) {
				stub := esstub.New(t)
				var mu sync.Mutex
				present := map[string]bool{}
				stub.Handle("/{name}", func(w http.ResponseWriter, r *http.Request) {
					name := r.PathValue("name")
					switch r.Method {
					case http.MethodHead:
						rec.record("index.head")
						mu.Lock()
						exists := present[name]
						mu.Unlock()
						existsReply(w, exists)
					case http.MethodPut:
						rec.record("index.create")
						mu.Lock()
						present[name] = true
						mu.Unlock()
						_, _ = w.Write([]byte(ackBody))
					case http.MethodGet:
						// GET /cards-* is the retention listing.
						rec.record("index.list")
						_, _ = w.Write([]byte(`{}`))
					default:
						t.Errorf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
						w.WriteHeader(http.StatusInternalServerError)
					}
				})
				listPolicies := rec.recorded("policy.get", esstub.JSON(http.StatusOK, `{"policies":[]}`))
				stub.Handle("GET /_enrich/policy", listPolicies)
				stub.Handle("GET /_enrich/policy/{$}", listPolicies)
				stub.Handle("PUT /_enrich/policy/{name}", rec.recorded("policy.put", esstub.JSON(http.StatusOK, ackBody)))
				stub.Handle("GET /_alias/cards", rec.recorded("alias.get", esstub.JSON(http.StatusNotFound, `{"error":{"type":"aliases_not_found_exception"},"status":404}`)))
				stub.Handle("GET /_nodes", rec.recorded("nodes.get", esstub.JSON(http.StatusOK, nodesBody)))
				stub.Handle("POST /_bulk", rec.recorded("bulk", esstub.JSON(http.StatusOK, bulkOKBody("cards"))))
				stub.Handle("POST /_aliases", rec.recorded("alias.update", esstub.JSON(http.StatusInternalServerError, failBody)))
				return stub, Options{
					URL:          stub.URL(),
					Index:        "cards",
					AliasMode:    true,
					AddToIndex:   true,
					SyncManaged:  true,
					KeepLast:     1,
					DataFile:     writeBulkDataFixture(t),
					PoliciesFile: fixture.WriteTempJSON(t, t.TempDir(), `{"pol":{"match":{"indices":"cards","match_field":"id","enrich_fields":["name"]}}}`),
				}
			},
			wantKind:  ErrIndexOperation,
			wantOp:    "Failed to update alias",
			wantLast:  "alias.update",
			forbidden: []string{"policy.put", "index.list"},
		},
		{
			name: "flush failure issues no bulk request",
			setup: func(t *testing.T, rec *opRecorder) (*esstub.Stub, Options) {
				stub := esstub.New(t)
				stub.Handle("HEAD /cards", rec.recorded("index.head", esstub.JSON(http.StatusOK, "")))
				stub.Handle("POST /cards/_delete_by_query", rec.recorded("flush", esstub.JSON(http.StatusInternalServerError, failBody)))
				stub.Handle("POST /_bulk", rec.recorded("bulk", esstub.JSON(http.StatusOK, bulkOKBody("cards"))))
				return stub, Options{URL: stub.URL(), Index: "cards", FlushIndex: true, DataFile: writeBulkDataFixture(t)}
			},
			wantKind:  ErrIndexOperation,
			wantOp:    "Failed to flush index",
			wantLast:  "flush",
			forbidden: []string{"bulk"},
		},
		{
			name: "nuke pipeline delete failure stops the teardown",
			setup: func(t *testing.T, rec *opRecorder) (*esstub.Stub, Options) {
				stub := esstub.New(t)
				listPolicies := rec.recorded("policy.get", esstub.JSON(http.StatusOK, `{"policies":[{"config":{"match":{"name":"pol"}}}]}`))
				stub.Handle("GET /_enrich/policy", listPolicies)
				stub.Handle("GET /_enrich/policy/{$}", listPolicies)
				stub.Handle("HEAD /cards", rec.recorded("index.head", esstub.JSON(http.StatusOK, "")))
				stub.Handle("DELETE /cards", rec.recorded("index.delete", esstub.JSON(http.StatusOK, ackBody)))
				stub.Handle("DELETE /_ingest/pipeline/{name}", func(w http.ResponseWriter, r *http.Request) {
					name := r.PathValue("name")
					rec.record("pipeline.delete " + name)
					if name == "p1" {
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = w.Write([]byte(failBody))
						return
					}
					_, _ = w.Write([]byte(ackBody))
				})
				stub.Handle("DELETE /_enrich/policy/{name}", rec.recorded("policy.delete", esstub.JSON(http.StatusOK, ackBody)))
				return stub, Options{
					URL:           stub.URL(),
					Index:         "cards",
					Nuke:          true,
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), `{"p1":{"processors":[]},"p2":{"processors":[]}}`),
					PoliciesFile:  fixture.WriteTempJSON(t, t.TempDir(), `{"pol":{"match":{"indices":"cards","match_field":"id","enrich_fields":["name"]}}}`),
				}
			},
			wantKind:  ErrManagedResource,
			wantOp:    "Failed to delete pipeline",
			wantLast:  "pipeline.delete p1",
			forbidden: []string{"pipeline.delete p2", "policy.delete"},
		},
		{
			// Run's alias-nuke DeleteIndices return. The first target's DELETE
			// fails after the alias read; the second target, the generation
			// listing and the pipeline teardown are all routed, so a missing
			// return would reach them and extend the trace.
			name: "alias nuke target delete failure stops before the second target, listing and teardown",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"GET /_alias/cards":               esstub.JSON(http.StatusOK, `{"cards-a":{},"cards-b":{}}`),
					"DELETE /cards-a":                 replyFail,
					"DELETE /cards-b":                 replyAck,
					"GET /cards-*":                    esstub.JSON(http.StatusOK, `{}`),
					"DELETE /_ingest/pipeline/{name}": replyAck,
				})
				return stub, Options{
					URL: stub.URL(), Index: "cards", AliasMode: true, Nuke: true, DeleteIndex: true,
					DataFile:      writeBulkDataFixture(t),
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), stopPipelines),
				}
			},
			wantKind:     ErrIndexOperation,
			wantOp:       "Failed to delete index",
			wantRequests: []string{"GET /_alias/cards", "DELETE /cards-a"},
		},
		{
			// Run's alias-nuke ListTimestampedIndices return, after a successful
			// target delete.
			name: "alias nuke generation listing failure stops before generation delete and teardown",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"GET /_alias/cards":               esstub.JSON(http.StatusOK, `{"cards-a":{}}`),
					"DELETE /cards-a":                 replyAck,
					"GET /cards-*":                    replyFail,
					"DELETE /cards-20260101000000":    replyAck,
					"DELETE /_ingest/pipeline/{name}": replyAck,
				})
				return stub, Options{
					URL: stub.URL(), Index: "cards", AliasMode: true, Nuke: true, DeleteIndex: true,
					DataFile:      writeBulkDataFixture(t),
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), stopPipelines),
				}
			},
			// Generation listing is an index operation: classifyRunErrorKind matches "indices" (FIX-LISTKIND-01).
			wantKind:     ErrIndexOperation,
			wantOp:       "Failed to list timestamped indices",
			wantRequests: []string{"GET /_alias/cards", "DELETE /cards-a", "GET /cards-*"},
		},
		{
			// Run's second alias-nuke DeleteIndices return (generations). The
			// listing is a map, so which generation is first is not defined: both
			// deletes fail and exactly one may be attempted.
			name: "alias nuke generation delete failure stops before the next generation and teardown",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"GET /_alias/cards":               esstub.JSON(http.StatusNotFound, `{"error":{"type":"aliases_not_found_exception"},"status":404}`),
					"HEAD /cards":                     replyHeadMissing,
					"GET /cards-*":                    esstub.JSON(http.StatusOK, `{"cards-20260101000000":{},"cards-20260102000000":{}}`),
					"DELETE /cards-20260101000000":    replyFail,
					"DELETE /cards-20260102000000":    replyFail,
					"DELETE /_ingest/pipeline/{name}": replyAck,
				})
				return stub, Options{
					URL: stub.URL(), Index: "cards", AliasMode: true, Nuke: true, DeleteIndex: true,
					DataFile:      writeBulkDataFixture(t),
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), stopPipelines),
				}
			},
			wantKind: ErrIndexOperation,
			wantOp:   "Failed to delete index",
			wantAnyOf: [][]string{
				{"GET /_alias/cards", "HEAD /cards", "GET /cards-*", "DELETE /cards-20260101000000"},
				{"GET /_alias/cards", "HEAD /cards", "GET /cards-*", "DELETE /cards-20260102000000"},
			},
		},
		{
			// Run's plain-nuke indexops.Delete return, with managed resources
			// declared so the teardown after it is real.
			name: "plain nuke index delete failure stops before managed teardown",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards":                     replyHeadOK,
					"DELETE /cards":                   replyFail,
					"DELETE /_ingest/pipeline/{name}": replyAck,
				})
				return stub, Options{
					URL: stub.URL(), Index: "cards", Nuke: true,
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), stopPipelines),
				}
			},
			wantKind:     ErrIndexOperation,
			wantOp:       "Failed to delete index",
			wantRequests: []string{"HEAD /cards", "DELETE /cards"},
		},
		{
			// Run's plain -delete managed teardown return (non-nuke
			// DeleteResources). Index create and bulk are routed and enabled.
			name: "plain delete managed teardown failure stops before the second pipeline, create and bulk",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards":                 replyHeadOK,
					"DELETE /cards":               replyAck,
					"DELETE /_ingest/pipeline/p1": replyFail,
					"DELETE /_ingest/pipeline/p2": replyAck,
					"GET /_nodes":                 esstub.JSON(http.StatusOK, nodesBody),
					"PUT /cards":                  replyAck,
					"POST /_bulk":                 esstub.JSON(http.StatusOK, bulkOKBody("cards")),
				})
				return stub, Options{
					URL: stub.URL(), Index: "cards", DeleteIndex: true,
					DataFile:      writeBulkDataFixture(t),
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), stopPipelines),
				}
			},
			wantKind:     ErrManagedResource,
			wantOp:       "Failed to delete pipeline",
			wantRequests: []string{"HEAD /cards", "DELETE /cards", "DELETE /_ingest/pipeline/p1"},
		},
		{
			// An oversized policy-delete 404 cannot be classified, and predicate
			// no-match alone would have reported an absent policy and let the
			// reset continue into index create and bulk. It must stop at the
			// existing Failed to delete enrich policy Op (ErrEnrichExecution).
			name: "oversized policy delete 404 stops before index create and bulk",
			setup: func(t *testing.T, rec *opRecorder) (*esstub.Stub, Options) {
				oversized := esstub.JSON(http.StatusNotFound,
					`{"error":{"type":"resource_not_found_exception"}}`+strings.Repeat(" ", 70_000))
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards":                   replyHeadOK,
					"DELETE /cards":                 replyAck,
					"DELETE /_enrich/policy/{name}": rec.recorded("policy.delete", oversized),
					"GET /_nodes":                   esstub.JSON(http.StatusOK, nodesBody),
					"PUT /cards":                    rec.recorded("index.create", replyAck),
					"POST /_bulk":                   rec.recorded("bulk", esstub.JSON(http.StatusOK, bulkOKBody("cards"))),
				}, policyListRoutes(esstub.JSON(http.StatusOK, `{"policies":[{"config":{"match":{"name":"pol"}}}]}`)))
				return stub, Options{
					URL: stub.URL(), Index: "cards", DeleteIndex: true,
					DataFile:     writeBulkDataFixture(t),
					PoliciesFile: fixture.WriteTempJSON(t, t.TempDir(), `{"pol":`+stopPolicyBody+`}`),
				}
			},
			wantKind:  ErrEnrichExecution,
			wantOp:    "Failed to delete enrich policy",
			wantLast:  "policy.delete",
			forbidden: []string{"index.create", "bulk"},
		},
		{
			// Run's alias -flush loop return. The first target has already been
			// emptied, so no rollback is expected; the second target is the
			// failing last request and no managed upsert or bulk follows.
			name: "alias flush second target failure stops before managed upsert and bulk",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"GET /_alias/cards":              esstub.JSON(http.StatusOK, `{"cards-a":{},"cards-b":{}}`),
					"POST /cards-a/_delete_by_query": esstub.JSON(http.StatusOK, `{"failures":[]}`),
					"POST /cards-b/_delete_by_query": replyFail,
					"PUT /_ingest/pipeline/{name}":   replyAck,
					"POST /_bulk":                    esstub.JSON(http.StatusOK, bulkOKBody("cards")),
				})
				return stub, Options{
					URL: stub.URL(), Index: "cards", AliasMode: true, FlushIndex: true, SyncManaged: true,
					DataFile:      writeBulkDataFixture(t),
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), stopPipelines),
				}
			},
			wantKind:     ErrIndexOperation,
			wantOp:       "Failed to flush index",
			wantRequests: []string{"GET /_alias/cards", "POST /cards-a/_delete_by_query", "POST /cards-b/_delete_by_query"},
		},
	})
}

// newGenerationStub serves the alias-mode first-create flow for alias "cards":
// the alias is absent, the concrete "cards" and each cards-<ts> generation are
// absent until PUT, and bulk, alias update, refresh and policy routes succeed.
// overrides replace routes by pattern before registration, so a case swaps one
// reply without registering a pattern twice.
func newGenerationStub(t *testing.T, overrides ...map[string]http.HandlerFunc) *esstub.Stub {
	t.Helper()

	var mu sync.Mutex
	present := map[string]bool{}
	base := map[string]http.HandlerFunc{
		"/{name}": func(w http.ResponseWriter, r *http.Request) {
			name := r.PathValue("name")
			switch r.Method {
			case http.MethodHead:
				mu.Lock()
				exists := present[name]
				mu.Unlock()
				existsReply(w, exists)
			case http.MethodPut:
				mu.Lock()
				present[name] = true
				mu.Unlock()
				_, _ = w.Write([]byte(ackBody))
			case http.MethodGet: // GET /cards-* is the retention listing
				_, _ = w.Write([]byte(`{}`))
			default:
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
				w.WriteHeader(http.StatusInternalServerError)
			}
		},
		"GET /_alias/cards":          esstub.JSON(http.StatusNotFound, `{"error":{"type":"aliases_not_found_exception"},"status":404}`),
		"GET /_nodes":                esstub.JSON(http.StatusOK, nodesBody),
		"POST /_bulk":                esstub.JSON(http.StatusOK, bulkOKBody("cards")),
		"POST /_aliases":             replyAck,
		"POST /{name}/_refresh":      esstub.JSON(http.StatusOK, refreshBody),
		"PUT /_enrich/policy/{name}": replyAck,
	}
	maps.Copy(base, policyListRoutes(esstub.JSON(http.StatusOK, `{"policies":[]}`)))
	return newRouteStub(t, append([]map[string]http.HandlerFunc{base}, overrides...)...)
}

// TestRunStopsAfterLifecycleFailure guards Run's lifecycle-write returns: each
// case fails one write (or the read that gates it) and asserts the exact request
// trace, so a dropped `return result, runFailure(err)` lets the run continue into
// routed later work and extends the trace. Cases name the Run call site because
// several helpers (pipelines, policies, policy GC, index delete) are called from
// more than one place and each call site has its own return.
func TestRunStopsAfterLifecycleFailure(t *testing.T) {
	t.Parallel()

	polName := managedPolicyName(t, "pol", stopPolicyBody)
	policiesJSON := `{"pol":` + stopPolicyBody + `}`
	emptyPolicies := esstub.JSON(http.StatusOK, `{"policies":[]}`)
	putPolicy := "PUT /_enrich/policy/" + polName
	// The alias first-create flow up to and including the alias swap.
	aliasCreate := []string{
		"GET /_enrich/policy", "GET /_alias/cards", "HEAD /cards", "HEAD /cards-<ts>",
		"GET /_nodes", "PUT /cards-<ts>", "HEAD /cards-<ts>", "POST /_bulk",
		"GET /_alias/cards", "POST /_aliases",
	}
	aliasOpts := func(t *testing.T, stub *esstub.Stub) Options {
		return Options{
			URL: stub.URL(), Index: "cards", AliasMode: true, AddToIndex: true, SyncManaged: true,
			KeepLast: 1, Enrich: EnrichOptions{Enabled: true, All: true},
			DataFile:     writeBulkDataFixture(t),
			PoliciesFile: fixture.WriteTempJSON(t, t.TempDir(), policiesJSON),
		}
	}

	runStopCases(t, true, []runStopCase{
		{
			// Run's CreatePipelines call before index create.
			name: "pipeline create before index create stops before the second pipeline and create",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards":              replyHeadMissing,
					"PUT /_ingest/pipeline/p1": replyFail,
					"PUT /_ingest/pipeline/p2": replyAck,
					"GET /_nodes":              esstub.JSON(http.StatusOK, nodesBody),
					"PUT /cards":               replyAck,
					"POST /_bulk":              esstub.JSON(http.StatusOK, bulkOKBody("cards")),
				})
				return stub, Options{
					URL: stub.URL(), Index: "cards", AddToIndex: true, SyncManaged: true,
					DataFile:      writeBulkDataFixture(t),
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), stopPipelines),
				}
			},
			wantKind:     ErrManagedResource,
			wantOp:       "Failed to create pipeline",
			wantRequests: []string{"HEAD /cards", "PUT /_ingest/pipeline/p1"},
		},
		{
			// Run's CreatePipelines call on an existing index.
			name: "pipeline create on existing index stops before the second pipeline and enrich",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards":              replyHeadOK,
					"PUT /_ingest/pipeline/p1": replyFail,
					"PUT /_ingest/pipeline/p2": replyAck,
					"POST /cards/_refresh":     esstub.JSON(http.StatusOK, refreshBody),
				}, policyListRoutes(emptyPolicies))
				return stub, Options{
					URL: stub.URL(), Index: "cards", SyncManaged: true,
					Enrich:        EnrichOptions{Enabled: true, All: true},
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), stopPipelines),
				}
			},
			wantKind:     ErrManagedResource,
			wantOp:       "Failed to create pipeline",
			wantRequests: []string{"HEAD /cards", "PUT /_ingest/pipeline/p1"},
		},
		{
			name: "index create failure stops before visibility wait and bulk",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards": replyHeadMissing,
					"GET /_nodes": esstub.JSON(http.StatusOK, nodesBody),
					"PUT /cards":  replyFail,
					"POST /_bulk": esstub.JSON(http.StatusOK, bulkOKBody("cards")),
				})
				return stub, Options{URL: stub.URL(), Index: "cards", AddToIndex: true, DataFile: writeBulkDataFixture(t)}
			},
			wantKind:     ErrIndexOperation,
			wantOp:       "Failed to create index",
			wantRequests: []string{"HEAD /cards", "GET /_nodes", "PUT /cards"},
		},
		{
			// The visibility HEAD fails immediately (500), so no sleep or clock
			// seam is involved.
			name: "index visibility failure stops before bulk",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards": sequenced(replyHeadMissing, esstub.JSON(http.StatusInternalServerError, "")),
					"GET /_nodes": esstub.JSON(http.StatusOK, nodesBody),
					"PUT /cards":  replyAck,
					"POST /_bulk": esstub.JSON(http.StatusOK, bulkOKBody("cards")),
				})
				return stub, Options{URL: stub.URL(), Index: "cards", AddToIndex: true, DataFile: writeBulkDataFixture(t)}
			},
			wantKind:     ErrIndexOperation,
			wantOp:       "waiting for index creation",
			wantRequests: []string{"HEAD /cards", "GET /_nodes", "PUT /cards", "HEAD /cards"},
		},
		{
			// Run's CreatePolicies call before bulk.
			name: "policy create before bulk stops before GC and bulk",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards":                replyHeadOK,
					"PUT /_enrich/policy/{name}": replyFail,
					"POST /_bulk":                esstub.JSON(http.StatusOK, bulkOKBody("cards")),
				}, policyListRoutes(emptyPolicies))
				return stub, Options{
					URL: stub.URL(), Index: "cards", AddToIndex: true, SyncManaged: true,
					DataFile:     writeBulkDataFixture(t),
					PoliciesFile: fixture.WriteTempJSON(t, t.TempDir(), policiesJSON),
				}
			},
			wantKind:     ErrEnrichExecution,
			wantOp:       "Failed to create enrich policy",
			wantRequests: []string{"GET /_enrich/policy", "HEAD /cards", putPolicy},
		},
		{
			// Run's GarbageCollectPolicies call before bulk: the first listing
			// (policy-delete resolution) succeeds, the GC listing fails. A GC
			// DELETE failure is best effort and would not fail Run.
			name: "policy GC before bulk stops before bulk",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards":                replyHeadOK,
					"PUT /_enrich/policy/{name}": replyAck,
					"POST /_bulk":                esstub.JSON(http.StatusOK, bulkOKBody("cards")),
				}, policyListRoutes(sequenced(emptyPolicies, replyFail)))
				return stub, Options{
					URL: stub.URL(), Index: "cards", AddToIndex: true, SyncManaged: true,
					DataFile:     writeBulkDataFixture(t),
					PoliciesFile: fixture.WriteTempJSON(t, t.TempDir(), policiesJSON),
				}
			},
			wantKind:     ErrEnrichExecution,
			wantOp:       "Failed to get enrich policies",
			wantRequests: []string{"GET /_enrich/policy", "HEAD /cards", putPolicy, "GET /_enrich/policy"},
		},
		{
			// Run's deferred CreatePolicies call, after the alias swap.
			name: "deferred policy create after alias swap stops before GC, prune and enrich",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newGenerationStub(t, map[string]http.HandlerFunc{"PUT /_enrich/policy/{name}": replyFail})
				return stub, aliasOpts(t, stub)
			},
			wantKind:     ErrEnrichExecution,
			wantOp:       "Failed to create enrich policy",
			tsNames:      true,
			wantRequests: append(slices.Clone(aliasCreate), putPolicy),
		},
		{
			// Run's deferred GarbageCollectPolicies call, after the alias swap.
			name: "deferred policy GC after alias swap stops before prune and enrich",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newGenerationStub(t, policyListRoutes(sequenced(emptyPolicies, replyFail)))
				return stub, aliasOpts(t, stub)
			},
			wantKind:     ErrEnrichExecution,
			wantOp:       "Failed to get enrich policies",
			tsNames:      true,
			wantRequests: append(slices.Clone(aliasCreate), putPolicy, "GET /_enrich/policy"),
		},
		{
			// Run's aliasops.Prune return: the older generation's DELETE fails and
			// the enrich stage that follows prune must not run.
			name: "alias prune delete failure stops before enrich",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"GET /_alias/cards":            esstub.JSON(http.StatusOK, `{"cards-20260102000000":{}}`),
					"GET /cards-*":                 esstub.JSON(http.StatusOK, `{"cards-20260101000000":{},"cards-20260102000000":{}}`),
					"DELETE /cards-20260101000000": replyFail,
					"POST /cards/_refresh":         esstub.JSON(http.StatusOK, refreshBody),
				}, policyListRoutes(emptyPolicies))
				return stub, Options{
					URL: stub.URL(), Index: "cards", AliasMode: true, KeepLast: 1,
					Enrich: EnrichOptions{Enabled: true, All: true},
				}
			},
			wantKind:     ErrIndexOperation,
			wantOp:       "Failed to delete index",
			wantRequests: []string{"GET /_alias/cards", "GET /cards-*", "DELETE /cards-20260101000000"},
		},
		{
			// Run's aliasops.Prune return when listing <alias>-* fails: no
			// generation is deleted and the enrich stage after prune must not run.
			name: "alias prune listing failure stops before generation delete and enrich",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"GET /_alias/cards":            esstub.JSON(http.StatusOK, `{"cards-20260102000000":{}}`),
					"GET /cards-*":                 replyFail,
					"DELETE /cards-20260101000000": replyAck,
					"POST /cards/_refresh":         esstub.JSON(http.StatusOK, refreshBody),
				}, policyListRoutes(emptyPolicies))
				return stub, Options{
					URL: stub.URL(), Index: "cards", AliasMode: true, KeepLast: 1,
					Enrich: EnrichOptions{Enabled: true, All: true},
				}
			},
			wantKind:     ErrIndexOperation,
			wantOp:       "Failed to list timestamped indices",
			wantRequests: []string{"GET /_alias/cards", "GET /cards-*"},
		},
		{
			// Run's post-create requireMappingPreflight return (expected == nil).
			name: "created-index mapping preflight failure stops before bulk",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newLifecycleStub(t, lifecycleConfig{index: "cards", mappingStatus: http.StatusInternalServerError})
				return stub.Stub, Options{
					URL: stub.URL(), Index: "cards", AddToIndex: true,
					DataFile:     writeBulkDataFixture(t),
					MappingsFile: fixture.WriteTempJSON(t, t.TempDir(), `{"properties":{"id":{"type":"keyword"}}}`),
				}
			},
			wantKind:     ErrBulkFailure,
			wantOp:       "Mapping preflight failed before bulk insert",
			wantRequests: []string{"HEAD /cards", "GET /_nodes", "PUT /cards", "HEAD /cards", "GET /cards/_mapping"},
		},
		{
			// Run's bulk.Load return, in the alias create flow so that the alias
			// swap, prune and enrich that follow are all routed.
			name: "bulk failure stops before alias update, prune and enrich",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newGenerationStub(t, map[string]http.HandlerFunc{"POST /_bulk": esstub.JSON(http.StatusBadRequest, `{"error":true}`)})
				opts := aliasOpts(t, stub)
				opts.SyncManaged, opts.PoliciesFile = false, ""
				return stub, opts
			},
			wantKind: ErrBulkFailure,
			wantOp:   "Bulk API request failed",
			tsNames:  true,
			wantRequests: []string{
				"GET /_alias/cards", "HEAD /cards", "HEAD /cards-<ts>", "GET /_nodes",
				"PUT /cards-<ts>", "HEAD /cards-<ts>", "POST /_bulk",
			},
		},
	})
}

// TestRunPipelineRejectionReasonAndPrivacy guards FIX-PIPECAUSE-01 at the
// real HTTP response boundary: a rejected pipeline PUT on an existing index
// returns its bounded, redacted ES reason through RunError (same Op and Kind),
// the raw body never reaches the returned message or the log, and the run
// still stops at the PUT. Refresh, bulk and policy listing are routed and a
// data action plus enrich are enabled, so a lost return would extend the
// trace instead of failing on an unexpected route. Every body, name and
// FAKE-* value is synthetic and must change with the diagnostic contract.
// The two-pipeline stop rows in TestRunStopsAfterLifecycleFailure keep
// guarding the second-pipeline and index-create paths.
func TestRunPipelineRejectionReasonAndPrivacy(t *testing.T) {
	t.Parallel()

	const reason = "No processor type exists with name [fixture_proc]"

	// run drives one Run whose only pipeline, name, is rejected with body.
	run := func(t *testing.T, name, body string, logger *zerolog.Logger) error {
		t.Helper()
		pipelines, err := json.Marshal(map[string]any{name: map[string]any{"processors": []any{}}})
		if err != nil {
			t.Fatalf("marshal pipelines: %v", err)
		}
		stub := newRouteStub(t, map[string]http.HandlerFunc{
			"HEAD /cards": replyHeadOK,
			// A wildcard route with an exact decoded-name check avoids putting
			// quotes or backslashes into a ServeMux pattern.
			"PUT /_ingest/pipeline/{name}": func(w http.ResponseWriter, r *http.Request) {
				if got := r.PathValue("name"); got != name {
					t.Errorf("pipeline PUT name = %q, want %q", got, name)
				}
				esstub.JSON(http.StatusBadRequest, body)(w, r)
			},
			"POST /cards/_refresh": esstub.JSON(http.StatusOK, refreshBody),
			"POST /_bulk":          esstub.JSON(http.StatusOK, bulkOKBody("cards")),
		}, policyListRoutes(esstub.JSON(http.StatusOK, `{"policies":[]}`)))

		_, runErr := Run(t.Context(), Options{
			URL: stub.URL(), Index: "cards", AddToIndex: true, SyncManaged: true,
			DataFile:      writeBulkDataFixture(t),
			PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), string(pipelines)),
			Enrich:        EnrichOptions{Enabled: true, All: true},
			Logger:        logger,
		})
		if runErr == nil {
			t.Fatal("expected Run to fail")
		}
		assertRunError(t, runErr, ErrManagedResource, "Failed to create pipeline")
		if !errors.Is(runErr, ErrManagedResource) {
			t.Fatalf("error = %v, want ErrManagedResource", runErr)
		}
		if got, want := requestLabels(stub), []string{"HEAD /cards", "PUT /_ingest/pipeline/" + name}; !slices.Equal(got, want) {
			t.Fatalf("requests = %q\n want     %q", got, want)
		}
		msg := runErr.Error()
		if len(msg) > 2048 || !utf8.ValidString(msg) {
			t.Fatalf("message is %d bytes, valid=%v; want <= 2048 bytes of valid UTF-8", len(msg), utf8.ValidString(msg))
		}
		return runErr
	}

	t.Run("nop logger", func(t *testing.T) {
		t.Parallel()
		nop := zerolog.Nop()
		body := `{"error":{"root_cause":[{"type":"parse_exception","reason":"` + reason + `"}],` +
			`"type":"parse_exception","reason":"` + reason + `"},"status":400}`
		err := run(t, "p", body, &nop)
		for _, want := range []string{`pipeline "p"`, "HTTP 400", "parse_exception", reason} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("message %q lacks %q", err.Error(), want)
			}
		}
		if strings.Contains(err.Error(), "Failed to create pipeline: Failed to create pipeline") {
			t.Fatalf("message still duplicates the Op: %q", err.Error())
		}
	})

	t.Run("captured logs", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		logger := zerolog.New(&buf)
		secretReason := `Authorization: Bearer FAKE-SECRET-A at https://user:FAKE-SECRET-B@es.example/x?api_key=FAKE-SECRET-C ` +
			`with password="FAKE SECRET D"`
		body := `{"error":{"root_cause":[{"type":"security_exception","reason":` + strconv.Quote(secretReason) + `}],` +
			`"type":"security_exception","reason":"denied","index":"FAKE-METADATA","stack_trace":"FAKE-TRACE"},"status":400}`
		err := run(t, "p", body, &logger)
		logs := buf.String()
		for _, leak := range []string{"FAKE-SECRET", "FAKE SECRET", "FAKE-METADATA", "FAKE-TRACE"} {
			if strings.Contains(err.Error(), leak) {
				t.Fatalf("error leaks %q: %s", leak, err.Error())
			}
			if strings.Contains(logs, leak) {
				t.Fatalf("log leaks %q: %s", leak, logs)
			}
		}
		for _, want := range []string{"security_exception: Authorization: [REDACTED]", "https://[REDACTED]@es.example", "denied"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("message %q lacks %q", err.Error(), want)
			}
		}
		if !strings.Contains(logs, `"pipeline":"p"`) || !strings.Contains(logs, `"status_code":400`) || strings.Contains(logs, `"body"`) {
			t.Fatalf("failure log: want pipeline and status fields and no raw body field: %s", logs)
		}
	})

	t.Run("public message bound", func(t *testing.T) {
		t.Parallel()
		nop := zerolog.Nop()
		name := strings.Repeat(`"é\`, 100)
		body := `{"error":{"type":"parse_exception","reason":"` + strings.Repeat("é", 3000) + `"},"status":400}`
		err := run(t, name, body, &nop)
		msg := err.Error()
		if !strings.HasPrefix(msg, `Failed to create pipeline: pipeline "\"é\\`) || !strings.HasSuffix(msg, " [truncated]") {
			t.Fatalf("message %q: want the fixed Op, quoted name and truncation marker", msg)
		}
		if !strings.Contains(msg, "rejected with HTTP 400: parse_exception: é") {
			t.Fatalf("message lacks status and reason: %q", msg)
		}
	})
}

// TestRunStopsAfterResolutionFailure guards Run's resolution and setup returns:
// every definition read, plan, fallback, existence and name-selection failure
// must end the run before any destructive or creating request. Cases with no
// HTTP stage assert an empty trace on a stub that serves nothing.
func TestRunStopsAfterResolutionFailure(t *testing.T) {
	t.Parallel()

	missing := func(t *testing.T) string { return filepath.Join(t.TempDir(), "missing.json") }
	empty := func(t *testing.T, opts Options) (*esstub.Stub, Options) {
		stub := newRouteStub(t)
		opts.URL = stub.URL()
		return stub, opts
	}
	malformed := `{"mappings":`

	runStopCases(t, true, []runStopCase{
		{
			// Run's ReadNamedDefinitions returns for pipelines, policies and
			// transforms: three call sites, one case each.
			name: "pipeline definitions file unreadable",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				return empty(t, Options{Index: "cards", SyncManaged: true, PipelinesFile: missing(t)})
			},
			wantKind: ErrManagedResource, wantOp: "Reading pipeline definitions file", wantCause: os.ErrNotExist,
			wantRequests: []string{},
		},
		{
			name: "policy definitions file unreadable",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				return empty(t, Options{Index: "cards", SyncManaged: true, PoliciesFile: missing(t)})
			},
			wantKind: ErrManagedResource, wantOp: "Reading policy definitions file", wantCause: os.ErrNotExist,
			wantRequests: []string{},
		},
		{
			name: "transform definitions file unreadable",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				return empty(t, Options{Index: "cards", SyncManaged: true, TransformsFile: missing(t)})
			},
			wantKind: ErrManagedResource, wantOp: "Reading transform definitions file", wantCause: os.ErrNotExist,
			wantRequests: []string{},
		},
		{
			// 1e999 is a valid JSON number the raw-definition parser keeps but
			// that cannot be canonicalized into float64 for the policy hash.
			name: "policy definition that cannot be canonicalized",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				return empty(t, Options{
					Index: "cards", SyncManaged: true,
					PoliciesFile: fixture.WriteTempJSON(t, t.TempDir(), `{"pol":{"match":{"indices":"cards","match_field":"id","enrich_fields":["name"],"n":1e999}}}`),
				})
			},
			wantKind: ErrManagedResource, wantOp: "Failed to canonicalize policy definition for hashing",
			wantRequests: []string{},
		},
		{
			// Run's ResolvePipelinePolicyFallbacks return: a pipeline references a
			// policy with no definition, so the cluster listing is read and fails.
			name: "pipeline policy fallback listing failure",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{"HEAD /cards": replyHeadOK}, policyListRoutes(replyFail))
				return stub, Options{
					URL: stub.URL(), Index: "cards", SyncManaged: true,
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), `{"p":{"processors":[{"enrich":{"policy_name":"cards"}}]}}`),
				}
			},
			wantKind: ErrEnrichExecution, wantOp: "Failed to get enrich policies",
			wantRequests: []string{"GET /_enrich/policy"},
		},
		{
			// Run's ResolvePolicyDeleteNames return.
			name: "policy delete-name listing failure",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{"HEAD /cards": replyHeadOK}, policyListRoutes(replyFail))
				return stub, Options{
					URL: stub.URL(), Index: "cards", SyncManaged: true,
					PoliciesFile: fixture.WriteTempJSON(t, t.TempDir(), `{"pol":`+stopPolicyBody+`}`),
				}
			},
			wantKind: ErrEnrichExecution, wantOp: "Failed to get enrich policies",
			wantRequests: []string{"GET /_enrich/policy"},
		},
		{
			// Run's transform.ResolveForSource return: source_index is missing.
			name: "transform definition without source index",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				return empty(t, Options{
					Index: "cards", SyncManaged: true,
					TransformsFile: fixture.WriteTempJSON(t, t.TempDir(), `{"tx1":{"body":{}}}`),
				})
			},
			wantKind: ErrManagedResource, wantOp: "Failed to resolve transform definitions",
			wantRequests: []string{},
		},
		{
			// Run's BuildMappingPlan return (data action, before PreflightIndexInputs).
			// The file must be valid JSON that only the plan builder rejects
			// (1e999 overflows float64 there): a malformed file also fails the
			// later PreflightIndexInputs with the same Op and empty trace, so a
			// dropped return here would go unnoticed.
			name: "mapping plan parse failure",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				return empty(t, Options{
					Index: "cards", AddToIndex: true, DataFile: writeBulkDataFixture(t),
					MappingsFile: fixture.WriteTempJSON(t, t.TempDir(), `{"properties":{"a":{"type":"keyword","n":1e999}}}`),
				})
			},
			wantKind: ErrLoaderExecution, wantOp: "Failed to build mapping preflight plan",
			wantRequests: []string{},
		},
		{
			// Run's PreflightIndexInputs return, settings half, with no data
			// action so BuildMappingPlan is skipped.
			name: "sync-managed settings preflight failure",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				return empty(t, Options{
					Index: "cards", SyncManaged: true,
					SettingsFile: fixture.WriteTempJSON(t, t.TempDir(), `{"settings":`),
				})
			},
			wantKind: ErrLoaderExecution, wantOp: "Parsing settings file",
			wantRequests: []string{},
		},
		{
			// Run's PreflightIndexInputs return, mappings half: only the
			// preflight (not BuildMappingPlan) reads mappings on this run.
			name: "sync-managed mappings preflight failure",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				return empty(t, Options{
					Index: "cards", SyncManaged: true,
					MappingsFile: fixture.WriteTempJSON(t, t.TempDir(), malformed),
				})
			},
			wantKind: ErrLoaderExecution, wantOp: "Parsing mappings file",
			wantRequests: []string{},
		},
		{
			// Run's bulk.CheckDataFile return, before any destructive step.
			name: "data file preflight failure",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				return empty(t, Options{Index: "cards", DeleteIndex: true, DataFile: missing(t)})
			},
			wantKind: ErrLoaderExecution, wantOp: "Cannot read data file", wantCause: os.ErrNotExist,
			wantRequests: []string{},
		},
		{
			// Run's bulk.CheckDataFile return for a readable file of unknown
			// format: it must fail before -delete, not inside bulk.Load after it
			// (FIX-DATAFMT-01).
			name: "data file unknown format preflight failure",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				return empty(t, Options{Index: "cards", DeleteIndex: true,
					DataFile: fixture.WriteTempJSON(t, t.TempDir(), "id,name\n1,Alice\n")})
			},
			wantKind:     ErrLoaderExecution,
			wantOp:       `Data file first byte 'i' is neither '[' (JSON array) nor '{' (NDJSON)`,
			wantRequests: []string{},
		},
		{
			// Run's aliasops.ResolveTargets return: a 500 from the alias read.
			name: "alias resolution failure",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"GET /_alias/cards": replyFail,
					"HEAD /cards":       replyHeadMissing,
					"GET /_nodes":       esstub.JSON(http.StatusOK, nodesBody),
					"PUT /cards":        replyAck,
					"POST /_bulk":       esstub.JSON(http.StatusOK, bulkOKBody("cards")),
				})
				return stub, Options{URL: stub.URL(), Index: "cards", AliasMode: true, AddToIndex: true, DataFile: writeBulkDataFixture(t)}
			},
			wantKind: ErrIndexOperation, wantOp: "Failed to resolve alias targets",
			wantRequests: []string{"GET /_alias/cards"},
		},
		{
			// A JSON null alias body must fail the same return, never read as
			// "alias absent" (FIX-ENRICH404-01).
			name: "alias resolution null body",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"GET /_alias/cards": esstub.JSON(http.StatusOK, `null`),
					"HEAD /cards":       replyHeadMissing,
					"GET /_nodes":       esstub.JSON(http.StatusOK, nodesBody),
					"PUT /cards":        replyAck,
					"POST /_bulk":       esstub.JSON(http.StatusOK, bulkOKBody("cards")),
				})
				return stub, Options{URL: stub.URL(), Index: "cards", AliasMode: true, AddToIndex: true, DataFile: writeBulkDataFixture(t)}
			},
			wantKind: ErrIndexOperation, wantOp: "Unable to parse alias response",
			wantRequests: []string{"GET /_alias/cards"},
		},
		{
			// Run's alias-mode fallback indexops.Exists return.
			name: "alias fallback existence failure",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"GET /_alias/cards": esstub.JSON(http.StatusNotFound, `{"error":{"type":"aliases_not_found_exception"},"status":404}`),
					"HEAD /cards":       esstub.JSON(http.StatusInternalServerError, ""),
					"GET /_nodes":       esstub.JSON(http.StatusOK, nodesBody),
					"PUT /cards":        replyAck,
					"POST /_bulk":       esstub.JSON(http.StatusOK, bulkOKBody("cards")),
				})
				return stub, Options{URL: stub.URL(), Index: "cards", AliasMode: true, AddToIndex: true, DataFile: writeBulkDataFixture(t)}
			},
			wantKind: ErrIndexOperation, wantOp: "checking if index exists",
			wantRequests: []string{"GET /_alias/cards", "HEAD /cards"},
		},
		{
			// Run's plain-mode indexops.Exists return.
			name: "plain existence failure",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards": esstub.JSON(http.StatusInternalServerError, ""),
					"GET /_nodes": esstub.JSON(http.StatusOK, nodesBody),
					"PUT /cards":  replyAck,
					"POST /_bulk": esstub.JSON(http.StatusOK, bulkOKBody("cards")),
				})
				return stub, Options{URL: stub.URL(), Index: "cards", AddToIndex: true, DataFile: writeBulkDataFixture(t)}
			},
			wantKind: ErrIndexOperation, wantOp: "checking if index exists",
			wantRequests: []string{"HEAD /cards"},
		},
		{
			// Run's aliasops.NextAvailableIndexName return: the alias and the
			// concrete name are absent, then the first generation probe fails.
			name: "generation name selection failure",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"GET /_alias/cards": esstub.JSON(http.StatusNotFound, `{"error":{"type":"aliases_not_found_exception"},"status":404}`),
					"/{name}": func(w http.ResponseWriter, r *http.Request) {
						if r.Method == http.MethodHead && r.PathValue("name") == "cards" {
							w.WriteHeader(http.StatusNotFound)
							return
						}
						w.WriteHeader(http.StatusInternalServerError)
					},
					"GET /_nodes": esstub.JSON(http.StatusOK, nodesBody),
					"POST /_bulk": esstub.JSON(http.StatusOK, bulkOKBody("cards")),
				})
				return stub, Options{URL: stub.URL(), Index: "cards", AliasMode: true, AddToIndex: true, DataFile: writeBulkDataFixture(t)}
			},
			wantKind: ErrIndexOperation, wantOp: "finding available timestamped index name",
			tsNames:      true,
			wantRequests: []string{"GET /_alias/cards", "HEAD /cards", "HEAD /cards-<ts>"},
		},
		{
			// Run's config.BuildCreateIndexBody return. The settings file is valid
			// for the early preflight; the logical-index HEAD handler removes it
			// synchronously before answering, so the create-body re-read fails
			// deterministically (no sleep, no concurrent writer).
			name: "create body settings re-read failure",
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				settings := writeInputFile(t, "settings.json", `{}`)
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards": func(w http.ResponseWriter, _ *http.Request) {
						if err := os.Remove(settings); err != nil {
							t.Errorf("remove settings fixture: %v", err)
						}
						w.WriteHeader(http.StatusNotFound)
					},
					"GET /_nodes": esstub.JSON(http.StatusOK, nodesBody),
					"PUT /cards":  replyAck,
					"POST /_bulk": esstub.JSON(http.StatusOK, bulkOKBody("cards")),
				})
				return stub, Options{
					URL: stub.URL(), Index: "cards", AddToIndex: true,
					DataFile: writeBulkDataFixture(t), SettingsFile: settings,
				}
			},
			wantKind: ErrLoaderExecution, wantOp: "Reading settings file", wantCause: os.ErrNotExist,
			wantRequests: []string{"HEAD /cards", "GET /_nodes"},
		},
	})
}

// newStageStub serves a Run against the existing index "cards" with managed
// transforms tx1 and tx2 and an empty enrich listing. overrides replace routes
// by pattern, so a case fails exactly one request (always against tx1) while
// every later request in the stage is routed and would extend the trace.
func newStageStub(t *testing.T, overrides ...map[string]http.HandlerFunc) *esstub.Stub {
	t.Helper()

	base := map[string]http.HandlerFunc{
		"HEAD /cards":                 replyHeadOK,
		"POST /cards/_refresh":        esstub.JSON(http.StatusOK, refreshBody),
		"POST /_transform/tx1/_stop":  esstub.JSON(http.StatusNotFound, `{"error":{"type":"resource_not_found_exception"}}`),
		"GET /_transform/tx1":         esstub.JSON(http.StatusOK, `{"count":0,"transforms":[]}`),
		"PUT /_transform/tx1":         replyAck,
		"POST /_transform/tx1/_start": replyAck,
		"GET /_transform/tx1/_stats":  esstub.JSON(http.StatusOK, stoppedStats("tx1")),
	}
	maps.Copy(base, policyListRoutes(esstub.JSON(http.StatusOK, `{"policies":[]}`)))
	stub := newRouteStub(t, append([]map[string]http.HandlerFunc{base}, overrides...)...)
	registerTransformRoutes(stub, &opRecorder{}, "tx2", nil)
	stub.Handle("GET /_transform/tx2/_stats", esstub.JSON(http.StatusOK, stoppedStats("tx2")))
	return stub
}

// stoppedStats is a finished batch transform's _stats body.
func stoppedStats(id string) string {
	return `{"count":1,"transforms":[{"id":"` + id + `","state":"stopped","checkpointing":{"last":{"checkpoint":1}}}]}`
}

// TestRunStopsAfterStageFailure guards Run's stage returns: refresh, enrich
// listing and the stage-major transform lifecycle (stop all, create all, start
// all, wait all). It is serial because the cases install the fake clock, which
// swaps package-global seams; nothing in its ancestry may be parallel.
func TestRunStopsAfterStageFailure(t *testing.T) {
	stageOpts := func(t *testing.T, stub *esstub.Stub, mutate func(*Options)) Options {
		opts := Options{
			URL: stub.URL(), Index: "cards", SyncManaged: true,
			TransformsFile: fixture.WriteTempJSON(t, t.TempDir(), stopTransforms),
		}
		if mutate != nil {
			mutate(&opts)
		}
		return opts
	}
	enrichOn := func(o *Options) { o.Enrich = EnrichOptions{Enabled: true, All: true} }
	noEvents := func(t *testing.T, events []ProgressEvent) {
		t.Helper()
		if len(events) != 0 {
			t.Fatalf("progress events after a failed stage = %+v, want none", events)
		}
	}
	head := []string{"HEAD /cards", "POST /cards/_refresh"}
	stops := []string{"POST /_transform/tx1/_stop", "POST /_transform/tx2/_stop"}
	creates := []string{"GET /_transform/tx1", "PUT /_transform/tx1", "GET /_transform/tx2", "PUT /_transform/tx2"}
	starts := []string{"POST /_transform/tx1/_start", "POST /_transform/tx2/_start"}

	runStopCases(t, false, []runStopCase{
		{
			name:      "refresh before enrich failure stops before enrich and transforms",
			fakeClock: true,
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newStageStub(t, map[string]http.HandlerFunc{"POST /cards/_refresh": replyFail})
				return stub, stageOpts(t, stub, enrichOn)
			},
			wantKind: ErrIndexOperation, wantOp: "Failed to refresh index",
			wantRequests: head, checkEvents: noEvents,
		},
		{
			name:      "enrich policy listing failure stops before transforms",
			fakeClock: true,
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newStageStub(t, policyListRoutes(replyFail))
				return stub, stageOpts(t, stub, enrichOn)
			},
			wantKind: ErrEnrichExecution, wantOp: "Failed to get enrich policies",
			wantRequests: append(slices.Clone(head), "GET /_enrich/policy"), checkEvents: noEvents,
		},
		{
			name:      "refresh before transforms failure stops before any transform request",
			fakeClock: true,
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newStageStub(t, map[string]http.HandlerFunc{"POST /cards/_refresh": replyFail})
				return stub, stageOpts(t, stub, nil)
			},
			wantKind: ErrIndexOperation, wantOp: "Failed to refresh index",
			wantRequests: head, checkEvents: noEvents,
		},
		{
			name:      "transform stop failure stops before the second stop, create and start",
			fakeClock: true,
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newStageStub(t, map[string]http.HandlerFunc{"POST /_transform/tx1/_stop": replyFail})
				return stub, stageOpts(t, stub, nil)
			},
			wantKind: ErrManagedResource, wantOp: "Failed to stop transform",
			wantRequests: append(slices.Clone(head), stops[0]), checkEvents: noEvents,
		},
		{
			name:      "transform create failure stops before the second create and any start",
			fakeClock: true,
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newStageStub(t, map[string]http.HandlerFunc{"PUT /_transform/tx1": replyFail})
				return stub, stageOpts(t, stub, nil)
			},
			wantKind: ErrManagedResource, wantOp: "Failed to create transform",
			wantRequests: slices.Concat(head, stops, creates[:2]), checkEvents: noEvents,
		},
		{
			name:      "transform start failure stops before the second start and stats",
			fakeClock: true,
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newStageStub(t, map[string]http.HandlerFunc{"POST /_transform/tx1/_start": replyFail})
				return stub, stageOpts(t, stub, func(o *Options) { o.Transforms.Wait = true })
			},
			wantKind: ErrManagedResource, wantOp: "Failed to start transform",
			wantRequests: slices.Concat(head, stops, creates, starts[:1]), checkEvents: noEvents,
		},
		{
			// A decoded failed state is fatal (HTTP 200, not a retryable status).
			// The failing poll may emit its non-Final progress event; tx1 gets no
			// Final and tx2 is never waited on.
			name:      "transform wait failed state stops before the second transform",
			fakeClock: true,
			setup: func(t *testing.T, _ *opRecorder) (*esstub.Stub, Options) {
				stub := newStageStub(t, map[string]http.HandlerFunc{
					"GET /_transform/tx1/_stats": esstub.JSON(http.StatusOK, `{"count":1,"transforms":[{"id":"tx1","state":"failed","reason":"boom","checkpointing":{"last":{"checkpoint":0}}}]}`),
				})
				return stub, stageOpts(t, stub, func(o *Options) { o.Transforms.Wait = true })
			},
			wantKind: ErrManagedResource, wantOp: "Transform wait failed",
			wantRequests: slices.Concat(head, stops, creates, starts, []string{"GET /_transform/tx1/_stats"}),
			checkEvents: func(t *testing.T, events []ProgressEvent) {
				t.Helper()
				for _, event := range events {
					if event.Stage != ProgressStageTransform || event.Resource != "tx1" || event.Final {
						t.Fatalf("event %+v after a failed wait; want only non-Final tx1 transform events", event)
					}
				}
			},
		},
	})
}

// TestRunTransportFailureKeepsNetworkCause pins that a transport error reaches
// the caller as the original network error, not a re-created message.
func TestRunTransportFailureKeepsNetworkCause(t *testing.T) {
	t.Parallel()

	_, err := Run(t.Context(), Options{URL: "http://127.0.0.1:1", Index: "cards", Nuke: true})
	var netErr net.Error
	if !errors.As(err, &netErr) {
		t.Fatalf("error = %v, want a wrapped net.Error cause", err)
	}
}

// TestPublicTypeIdentity guards that the public contract types are declared in
// package loader itself. Moving one into an internal package and aliasing it
// back would change its reflected package path, which callers that reflect,
// encode or switch on type identity observe.
func TestPublicTypeIdentity(t *testing.T) {
	t.Parallel()

	const pkgPath = "github.com/jnovack/es-bulk-loader/pkg/loader"
	tests := []struct {
		value any
		name  string
	}{
		{Options{}, "Options"},
		{Result{}, "Result"},
		{EnrichOptions{}, "EnrichOptions"},
		{RunError{}, "RunError"},
		{ProgressEvent{}, "ProgressEvent"},
		{ProgressStage(""), "ProgressStage"},
		{TransformOptions{}, "TransformOptions"},
	}
	for _, tt := range tests {
		typ := reflect.TypeOf(tt.value)
		if typ.PkgPath() != pkgPath || typ.Name() != tt.name {
			t.Fatalf("type %v declared as %s.%s, want %s.%s", typ, typ.PkgPath(), typ.Name(), pkgPath, tt.name)
		}
	}
}

// inputPreflightStub serves only read requests for index "cards": any write
// reaches no route and fails the test.
func inputPreflightStub(t *testing.T) *esstub.Stub {
	t.Helper()
	stub := esstub.New(t)
	stub.Handle("HEAD /cards", esstub.JSON(http.StatusOK, ""))
	stub.Handle("GET /_nodes", esstub.JSON(http.StatusOK, nodesBody))
	stub.Handle("GET /_alias/cards", esstub.JSON(http.StatusNotFound, ""))
	return stub
}

func writeInputFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

// TestRunRejectsBadInputBeforeAnyMutation guards FIX-SETTINGSPRE-01: a bad
// settings, mappings or data file must fail before the first -nuke, -delete
// or -flush request. Moving the preflight after the destructive step makes
// the stub see a DELETE/POST and fails this test.
func TestRunRejectsBadInputBeforeAnyMutation(t *testing.T) {
	t.Parallel()

	type modeFn func(o *Options)
	modes := []struct {
		name string
		set  modeFn
	}{
		{"delete", func(o *Options) { o.DeleteIndex = true }},
		{"flush", func(o *Options) { o.FlushIndex = true }},
		{"nuke sync-managed", func(o *Options) { o.Nuke = true; o.SyncManaged = true }},
		{"nuke delete", func(o *Options) { o.Nuke = true; o.DeleteIndex = true }},
		{"delete alias", func(o *Options) { o.DeleteIndex = true; o.AliasMode = true }},
	}
	dataModes := []struct {
		name string
		set  modeFn
	}{
		{"delete", func(o *Options) { o.DeleteIndex = true }},
		{"flush", func(o *Options) { o.FlushIndex = true }},
		{"nuke delete", func(o *Options) { o.Nuke = true; o.DeleteIndex = true }},
		{"delete alias", func(o *Options) { o.DeleteIndex = true; o.AliasMode = true }},
	}

	type row struct {
		name      string
		opts      func(t *testing.T, stub *esstub.Stub) Options
		wantOp    string
		wantCause error
	}
	var rows []row
	missing := func(t *testing.T) string { return filepath.Join(t.TempDir(), "missing.json") }
	dirPath := func(t *testing.T) string { return t.TempDir() }

	base := func(t *testing.T, stub *esstub.Stub) Options {
		return Options{
			URL:          stub.URL(),
			Index:        "cards",
			DataFile:     writeBulkDataFixture(t),
			SettingsFile: writeInputFile(t, "settings.json", `{}`),
			MappingsFile: writeInputFile(t, "mappings.json", `{}`),
		}
	}

	for _, m := range modes {
		rows = append(rows,
			row{m.name + "/missing settings", func(t *testing.T, stub *esstub.Stub) Options {
				o := base(t, stub)
				m.set(&o)
				o.SettingsFile = missing(t)
				return o
			}, "Reading settings file", os.ErrNotExist},
			row{m.name + "/malformed settings", func(t *testing.T, stub *esstub.Stub) Options {
				o := base(t, stub)
				m.set(&o)
				o.SettingsFile = writeInputFile(t, "settings.json", `{"settings":`)
				return o
			}, "Parsing settings file", nil},
		)
	}
	rows = append(rows,
		row{"delete/directory settings", func(t *testing.T, stub *esstub.Stub) Options {
			o := base(t, stub)
			o.DeleteIndex = true
			o.SettingsFile = dirPath(t)
			return o
		}, "Reading settings file", nil},
		row{"delete/nested settings wrong type", func(t *testing.T, stub *esstub.Stub) Options {
			o := base(t, stub)
			o.DeleteIndex = true
			o.SettingsFile = writeInputFile(t, "settings.json", `{"settings":[]}`)
			return o
		}, "Parsing nested settings file", nil},
	)

	// Mappings on a nuke run with no data action are otherwise parsed only at
	// create time.
	for name, mk := range map[string]struct {
		path func(t *testing.T) string
		op   string
		is   error
	}{
		"missing":   {missing, "Reading mappings file", os.ErrNotExist},
		"directory": {dirPath, "Reading mappings file", nil},
		"malformed": {func(t *testing.T) string { return writeInputFile(t, "mappings.json", `{"mappings":`) }, "Parsing mappings file", nil},
	} {
		rows = append(rows, row{"nuke sync-managed/" + name + " mappings", func(t *testing.T, stub *esstub.Stub) Options {
			o := base(t, stub)
			o.DataFile = ""
			o.Nuke = true
			o.SyncManaged = true
			o.MappingsFile = mk.path(t)
			return o
		}, mk.op, mk.is})
	}

	for _, m := range dataModes {
		for name, mk := range map[string]struct {
			path func(t *testing.T) string
			is   error
		}{
			"missing":    {missing, os.ErrNotExist},
			"directory":  {dirPath, nil},
			"empty":      {func(t *testing.T) string { return writeInputFile(t, "d.json", "") }, io.EOF},
			"whitespace": {func(t *testing.T) string { return writeInputFile(t, "d.json", " \r\n\t") }, io.EOF},
		} {
			rows = append(rows, row{m.name + "/" + name + " data", func(t *testing.T, stub *esstub.Stub) Options {
				o := base(t, stub)
				m.set(&o)
				o.DataFile = mk.path(t)
				return o
			}, "Cannot read data file", mk.is})
		}
	}

	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			t.Parallel()
			stub := inputPreflightStub(t)
			opts := r.opts(t, stub)
			result, err := Run(t.Context(), opts)
			if err == nil {
				t.Fatal("expected Run to fail")
			}
			for _, req := range stub.Requests() {
				if req.Method != http.MethodHead && req.Method != http.MethodGet {
					t.Fatalf("mutating request %s %s before input failure", req.Method, req.Path)
				}
			}
			assertRunError(t, err, ErrLoaderExecution, r.wantOp)
			if r.wantCause != nil && !errors.Is(err, r.wantCause) {
				t.Fatalf("error = %v, want cause %v", err, r.wantCause)
			}
			if result.WriteIndex != "" || result.CreatedIndex != "" || result.ResolvedAliasTarget != "" {
				t.Fatalf("result = %+v, want no selected index", result)
			}
		})
	}
}

// unknownDataFormatOp is the Op bulk.detectDataFormat gives a CSV whose first
// byte is 'i'.
const unknownDataFormatOp = `Data file first byte 'i' is neither '[' (JSON array) nor '{' (NDJSON)`

// TestRunRejectsUnknownDataFormatBeforeMutation guards FIX-DATAFMT-01: a
// readable data file whose first byte is neither '[' nor '{' must fail in
// Run's preflight, before any request, for every data action with alias and
// nuke on or off. Moving format detection back into bulk.Load (after -delete
// or -flush) makes these runs send requests and fail here. Warnings Run
// appends before preflight (alias-delete) are kept, so no zero Result is
// required.
func TestRunRejectsUnknownDataFormatBeforeMutation(t *testing.T) {
	t.Parallel()

	actions := []struct {
		name string
		set  func(o *Options)
	}{
		{"add", func(o *Options) { o.AddToIndex = true }},
		{"flush", func(o *Options) { o.FlushIndex = true }},
		{"delete", func(o *Options) { o.DeleteIndex = true }},
	}
	for _, action := range actions {
		for _, alias := range []bool{false, true} {
			for _, nuke := range []bool{false, true} {
				name := fmt.Sprintf("%s/alias=%t/nuke=%t", action.name, alias, nuke)
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					stub := inputPreflightStub(t)
					opts := Options{
						URL:       stub.URL(),
						Index:     "cards",
						DataFile:  writeInputFile(t, "data.json", "id,name\n1,Alice\n"),
						AliasMode: alias,
						Nuke:      nuke,
					}
					action.set(&opts)

					result, err := Run(t.Context(), opts)
					if err == nil {
						t.Fatal("expected Run to fail")
					}
					assertRunError(t, err, ErrLoaderExecution, unknownDataFormatOp)
					var runErr *RunError
					if !errors.As(err, &runErr) || runErr.Err.Error() != unknownDataFormatOp {
						t.Fatalf("cause = %v, want text %q", err, unknownDataFormatOp)
					}
					if got := stub.Requests(); len(got) != 0 {
						t.Fatalf("requests = %v, want none", got)
					}
					if result.WriteIndex != "" || result.CreatedIndex != "" || result.ResolvedAliasTarget != "" {
						t.Fatalf("result = %+v, want no selected index", result)
					}
				})
			}
		}
	}
}

// TestRunSkipsUnusedInputPreflight pins the preflight scope: only runs that
// can use settings/mappings (data action or effective sync-managed) check
// them, and the conservative potential-create predicate still checks them for
// -add and sync-managed-only runs against an existing index.
func TestRunSkipsUnusedInputPreflight(t *testing.T) {
	t.Parallel()

	unused := func(t *testing.T) (settings, mappings string) {
		return filepath.Join(t.TempDir(), "missing.json"), filepath.Join(t.TempDir(), "missing.json")
	}
	malformed := func(t *testing.T) (settings, mappings string) {
		return writeInputFile(t, "s.json", `{"settings":`), writeInputFile(t, "m.json", `{"mappings":`)
	}
	inputs := map[string]func(t *testing.T) (string, string){"missing": unused, "malformed": malformed}

	for name, mk := range inputs {
		t.Run("enrich-only/"+name, func(t *testing.T) {
			t.Parallel()
			stub := esstub.New(t)
			stub.Handle("HEAD /cards", esstub.JSON(http.StatusOK, ""))
			stub.Handle("POST /cards/_refresh", esstub.JSON(http.StatusOK, refreshBody))
			listing := esstub.JSON(http.StatusInternalServerError, `{"error":{"type":"stub_failure"}}`)
			stub.Handle("GET /_enrich/policy", listing)
			stub.Handle("GET /_enrich/policy/{$}", listing)
			s, m := mk(t)
			_, err := Run(t.Context(), Options{
				URL: stub.URL(), Index: "cards",
				SettingsFile: s, MappingsFile: m,
				DataFile: filepath.Join(t.TempDir(), "unused.json"),
				Enrich:   EnrichOptions{Enabled: true, All: true},
			})
			if err == nil {
				t.Fatal("expected the enrich listing failure")
			}
			assertRunError(t, err, ErrEnrichExecution, "Failed to get enrich policies")
		})
		t.Run("nuke-only/"+name, func(t *testing.T) {
			t.Parallel()
			stub := esstub.New(t)
			stub.Handle("HEAD /cards", esstub.JSON(http.StatusNotFound, ""))
			s, m := mk(t)
			_, err := Run(t.Context(), Options{
				URL: stub.URL(), Index: "cards", Nuke: true,
				SettingsFile: s, MappingsFile: m,
				DataFile: filepath.Join(t.TempDir(), "unused.json"),
			})
			if err != nil {
				t.Fatalf("nuke-only run must not inspect unused inputs: %v", err)
			}
		})
	}

	t.Run("add to existing index still preflights settings", func(t *testing.T) {
		t.Parallel()
		stub := esstub.New(t)
		stub.Handle("HEAD /cards", esstub.JSON(http.StatusOK, ""))
		stub.Handle("GET /_nodes", esstub.JSON(http.StatusOK, nodesBody))
		_, err := Run(t.Context(), Options{
			URL: stub.URL(), Index: "cards", AddToIndex: true,
			DataFile:     writeBulkDataFixture(t),
			SettingsFile: writeInputFile(t, "s.json", `{"settings":`),
		})
		if err == nil {
			t.Fatal("expected settings failure")
		}
		assertRunError(t, err, ErrLoaderExecution, "Parsing settings file")
	})
	t.Run("sync-managed only still preflights settings", func(t *testing.T) {
		t.Parallel()
		stub := esstub.New(t)
		stub.Handle("HEAD /cards", esstub.JSON(http.StatusOK, ""))
		stub.Handle("GET /_nodes", esstub.JSON(http.StatusOK, nodesBody))
		_, err := Run(t.Context(), Options{
			URL: stub.URL(), Index: "cards", SyncManaged: true,
			SettingsFile: writeInputFile(t, "s.json", `{"settings":`),
		})
		if err == nil {
			t.Fatal("expected settings failure")
		}
		assertRunError(t, err, ErrLoaderExecution, "Parsing settings file")
	})
}

// TestRunInputPreflightLogsOnlyAtCreate guards that preflight neither selects
// a replica count nor logs it: the Info line appears once, at create time.
// The stub has no managed pipelines, so this pins only the replica log at Run
// level; the "Using first declared pipeline" injection and the explicit-default
// override are pinned in config's TestPreflightIndexInputsPreservesCreateBodyAndLogs.
func TestRunInputPreflightLogsOnlyAtCreate(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		settings string
	}{
		{"no default pipeline", `{"number_of_shards":1}`},
		{"explicit default pipeline", `{"default_pipeline":"p"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stub := newLifecycleStub(t, lifecycleConfig{index: "cards", indexExists: true})
			var buf bytes.Buffer
			logger := zerolog.New(&buf)
			_, err := Run(t.Context(), Options{
				URL: stub.URL(), Index: "cards", DeleteIndex: true,
				DataFile:     writeBulkDataFixture(t),
				SettingsFile: writeInputFile(t, "s.json", tt.settings),
				Logger:       &logger,
			})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			logs := buf.String()
			if got := strings.Count(logs, "Setting index replica count"); got != 1 {
				t.Fatalf("replica count logged %d times, want 1:\n%s", got, logs)
			}
			if strings.Contains(logs, "Using first declared pipeline") {
				t.Fatalf("no managed pipelines exist; injection log must be absent:\n%s", logs)
			}
		})
	}
}

// TestRunAliasPreflightRejectsDifferentTargetSets guards FIX-ALIASSET-01:
// alias -flush and -add verify exactly the set of indices the run goes on to
// mutate. The alias read and the mapping read are separate snapshots, so a
// response omitting a resolved target (or adding one) must stop the run before
// any flush, bulk or managed upsert. The strict stub registers no write
// routes: a mutation, or losing the early return, fails the test. The error
// kind stays ErrBulkFailure because the wrapper Op text is unchanged.
func TestRunAliasPreflightRejectsDifferentTargetSets(t *testing.T) {
	t.Parallel()

	const good = `{"mappings":{"properties":{"id":{"type":"keyword"}}}}`
	const prefix = `alias "cards": mappings response does not match the alias targets: `
	aliasBody := `{"cards-a":{},"cards-b":{}}`
	onlyA := `{"cards-a":` + good + `}`
	extraC := `{"cards-a":` + good + `,"cards-b":` + good + `,"cards-c":` + good + `}`

	type action struct {
		name string
		set  func(*Options)
	}
	actions := []action{
		{"flush", func(o *Options) { o.FlushIndex = true }},
		{"add", func(o *Options) { o.AddToIndex = true }},
	}
	tests := []struct {
		name         string
		aliasStatus  int
		aliasBody    string
		fallback     bool
		mappingsBody string
		syncManaged  bool
		want         string
	}{
		{name: "missing one", aliasStatus: http.StatusOK, aliasBody: aliasBody, mappingsBody: onlyA, want: prefix + "missing cards-b"},
		{name: "unexpected one", aliasStatus: http.StatusOK, aliasBody: aliasBody, mappingsBody: extraC, want: prefix + "unexpected cards-c"},
		{name: "empty object", aliasStatus: http.StatusOK, aliasBody: aliasBody, mappingsBody: `{}`, want: prefix + "missing cards-a, cards-b"},
		{name: "missing one sync-managed", aliasStatus: http.StatusOK, aliasBody: aliasBody, mappingsBody: onlyA, syncManaged: true, want: prefix + "missing cards-b"},
	}
	fallbackRows := []struct {
		name         string
		mappingsBody string
		want         string
	}{
		{"fallback only other", `{"cards-x":` + good + `}`, prefix + "missing cards; unexpected cards-x"},
		{"fallback with extra", `{"cards":` + good + `,"cards-x":` + good + `}`, prefix + "unexpected cards-x"},
	}

	run := func(t *testing.T, act action, aliasStatus int, aliasJSON string, fallback bool, mappingsBody string, syncManaged bool, want string) {
		t.Helper()

		stub := esstub.New(t)
		stub.Handle("GET /_alias/cards", esstub.JSON(aliasStatus, aliasJSON))
		if fallback {
			stub.Handle("HEAD /cards", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		}
		stub.Handle("GET /cards/_mapping", esstub.JSON(http.StatusOK, mappingsBody))

		opts := Options{
			URL: stub.URL(), Index: "cards", AliasMode: true,
			DataFile:     writeBulkDataFixture(t),
			MappingsFile: fixture.WriteTempJSON(t, t.TempDir(), `{"properties":{"id":{"type":"keyword"}}}`),
		}
		if syncManaged {
			opts.SyncManaged = true
			opts.PipelinesFile = fixture.WriteTempJSON(t, t.TempDir(), `{"p":{"processors":[]}}`)
		}
		act.set(&opts)

		result, err := Run(t.Context(), opts)
		assertRunError(t, err, ErrBulkFailure, "Mapping preflight failed before bulk insert")
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %v, want containing %q", err, want)
		}
		if result.DocumentsProcessed != 0 || result.DocumentsSucceeded != 0 {
			t.Fatalf("result = %+v, want no documents loaded", result)
		}

		var got []string
		for _, r := range stub.Requests() {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				t.Fatalf("unexpected mutating request %s %s", r.Method, r.Path)
			}
			got = append(got, r.Method+" "+r.Path)
		}
		wantSeq := []string{"GET /_alias/cards", "GET /cards/_mapping"}
		if fallback {
			wantSeq = []string{"GET /_alias/cards", "HEAD /cards", "GET /cards/_mapping"}
		}
		if !slices.Equal(got, wantSeq) {
			t.Fatalf("requests = %v, want %v", got, wantSeq)
		}
	}

	for _, act := range actions {
		for _, tt := range tests {
			t.Run(act.name+"/"+tt.name, func(t *testing.T) {
				t.Parallel()
				run(t, act, tt.aliasStatus, tt.aliasBody, false, tt.mappingsBody, tt.syncManaged, tt.want)
			})
		}
	}
	for _, tt := range fallbackRows {
		t.Run("flush/"+tt.name, func(t *testing.T) {
			t.Parallel()
			run(t, actions[0], http.StatusNotFound, `{}`, true, tt.mappingsBody, false, tt.want)
		})
	}
}

// TestRunAliasPreflightSkipsEmptyMappingPlan pins the intentional
// HasExpectations bypass: without mapping expectations (no file, or a file
// containing {}) no mapping request is made, so the set cross-check is
// skipped too.
func TestRunAliasPreflightSkipsEmptyMappingPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		mappingsFile func(t *testing.T) string
	}{
		{"no file", func(*testing.T) string { return "" }},
		{"empty object file", func(t *testing.T) string { return fixture.WriteTempJSON(t, t.TempDir(), `{}`) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			stub.Handle("GET /_alias/cards", esstub.JSON(http.StatusOK, `{"cards-a":{},"cards-b":{}}`))
			stub.Handle("POST /_bulk", esstub.JSON(http.StatusOK, bulkOKBody("cards")))

			result, err := Run(t.Context(), Options{
				URL: stub.URL(), Index: "cards", AliasMode: true, AddToIndex: true,
				DataFile: writeBulkDataFixture(t), MappingsFile: tt.mappingsFile(t),
			})
			if err != nil {
				t.Fatalf("Run returned error: %v", err)
			}
			if result.DocumentsSucceeded != 1 {
				t.Fatalf("DocumentsSucceeded = %d, want 1", result.DocumentsSucceeded)
			}
			var got []string
			for _, r := range stub.Requests() {
				got = append(got, r.Method+" "+r.Path)
			}
			if want := []string{"GET /_alias/cards", "POST /_bulk"}; !slices.Equal(got, want) {
				t.Fatalf("requests = %v, want %v", got, want)
			}
		})
	}
}

// TestBasicAuthValidationEntrypoints guards that the public convenience
// entrypoints inherit Run's lone-credential rejection. They delegate to Run
// unchanged, so a future entrypoint that touched auth fields or ran its own
// validation first would let a lone credential through silently.
func TestBasicAuthValidationEntrypoints(t *testing.T) {
	t.Parallel()
	entrypoints := []struct {
		name string
		run  func(ctx context.Context, opts Options) (Result, error)
	}{
		{"SyncManaged", SyncManaged},
		{"LoadData", LoadData},
		{"ExecuteEnrich", ExecuteEnrich},
	}
	inputs := []struct {
		name    string
		user    string
		pass    string
		wantMsg string
	}{
		{"user only", "lone-user-value", "", "validating auth options: basic auth requires both -user and -pass: -pass is empty"},
		{"pass only", "", "lone-pass-value", "validating auth options: basic auth requires both -user and -pass: -user is empty"},
	}
	for _, ep := range entrypoints {
		for _, in := range inputs {
			t.Run(ep.name+"/"+in.name, func(t *testing.T) {
				t.Parallel()
				// Route-less stub: any request fails the test.
				result, err := ep.run(t.Context(), Options{URL: esstub.New(t).URL(), Index: "cards", User: in.user, Pass: in.pass})
				if !errors.Is(err, ErrInvalidOptions) {
					t.Fatalf("err = %v, want ErrInvalidOptions", err)
				}
				var runErr *RunError
				if !errors.As(err, &runErr) || runErr.Op != "validating auth options" {
					t.Fatalf("err = %#v, want RunError with auth Op", err)
				}
				if err.Error() != in.wantMsg {
					t.Fatalf("message = %q, want %q", err.Error(), in.wantMsg)
				}
				if !reflect.DeepEqual(result, Result{}) {
					t.Fatalf("result = %+v, want zero", result)
				}
			})
		}
	}
}

// TestRunWrappedRejectionTailsKeepOpKindAndCapInnerText pins LOG-RAWBODY-01's
// Q2 decision through the public Run boundary: the mapping-fetch and
// transform _stats errors keep their Op and Kind, their raw body tail is
// replaced by a Rejection whose own text is at most MaxPublicMessageBytes, and
// the outer context is preserved, so the complete public message can be
// longer than that bound.
func TestRunWrappedRejectionTailsKeepOpKindAndCapInnerText(t *testing.T) {
	t.Parallel()

	longBody := `{"error":{"type":"t","reason":"` + strings.Repeat("é", 3000) + `"}}`
	tests := []struct {
		name       string
		setup      func(t *testing.T) Options
		wantKind   error
		wantOp     string
		wantPrefix string
	}{
		{
			name: "mapping fetch",
			setup: func(t *testing.T) Options {
				stub := newRouteStub(t, map[string]http.HandlerFunc{
					"HEAD /cards":          replyHeadOK,
					"GET /cards/_mapping":  esstub.JSON(http.StatusInternalServerError, longBody),
					"POST /cards/_refresh": esstub.JSON(http.StatusOK, refreshBody),
				})
				return Options{
					URL: stub.URL(), Index: "cards", FlushIndex: true,
					DataFile:     writeBulkDataFixture(t),
					MappingsFile: fixture.WriteTempJSON(t, t.TempDir(), `{"properties":{"id":{"type":"keyword"}}}`),
				}
			},
			wantKind:   ErrBulkFailure,
			wantOp:     "Mapping preflight failed before bulk insert",
			wantPrefix: `Mapping preflight failed before bulk insert: fetching mappings for index "cards" failed with status 500: index "cards" rejected with HTTP 500: t: ééé`,
		},
		{
			name: "transform _stats status",
			setup: func(t *testing.T) Options {
				stub := newStageStub(t, map[string]http.HandlerFunc{
					"GET /_transform/tx1/_stats": esstub.JSON(http.StatusBadRequest, longBody),
				})
				return Options{
					URL: stub.URL(), Index: "cards", SyncManaged: true,
					TransformsFile: fixture.WriteTempJSON(t, t.TempDir(), stopTransforms),
					Transforms:     TransformOptions{Wait: true},
				}
			},
			wantKind:   ErrManagedResource,
			wantOp:     "Transform wait failed",
			wantPrefix: `Transform wait failed: transform "tx1": _stats returned status 400: transform "tx1" rejected with HTTP 400: t: ééé`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := Run(t.Context(), tt.setup(t))

			if err == nil {
				t.Fatal("expected Run to fail")
			}
			assertRunError(t, err, tt.wantKind, tt.wantOp)
			if !strings.HasPrefix(err.Error(), tt.wantPrefix) {
				t.Fatalf("message = %.200q\nwant prefix %.200q", err.Error(), tt.wantPrefix)
			}
			var rejection *esresponse.Rejection
			if !errors.As(err, &rejection) {
				t.Fatalf("error %v does not wrap an *esresponse.Rejection", err)
			}
			if len(rejection.Error()) > esresponse.MaxPublicMessageBytes {
				t.Fatalf("inner Rejection is %d bytes, want <= %d", len(rejection.Error()), esresponse.MaxPublicMessageBytes)
			}
			if len(err.Error()) <= len(rejection.Error()) {
				t.Fatal("the preserved outer context must make the complete message longer than the inner Rejection")
			}
			if strings.Contains(err.Error(), longBody) {
				t.Fatal("the raw body must not appear in the message")
			}
		})
	}
}

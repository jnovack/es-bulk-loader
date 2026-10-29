package loader

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/fixture"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
)

// TestRunErrorContract pins the public error contract at the Run boundary for
// every failure origin: the sentinel Kind (errors.Is), the operation string
// (errors.As to *RunError), the original cause where one exists, the exact
// message (no new prefix or wrapping; a failure without a cause keeps its
// duplicated "op: op" text), and the partial Result returned alongside the
// error. Internal failures moved from panics to returned errors; a regression
// in classification, Op, cause or partial-Result population fails here.
func TestRunErrorContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// setup returns the Options for the failing Run; it may start a stub.
		setup      func(t *testing.T) Options
		wantKind   error
		wantOp     string
		wantCause  error  // checked with errors.Is when non-nil
		wantMsg    string // exact err.Error() when non-empty
		wantPrefix string // err.Error() prefix when non-empty
		wantResult *Result
	}{
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
			name: "missing settings file at create time",
			setup: func(t *testing.T) Options {
				stub := esstub.New(t)
				stub.Handle("HEAD /cards", esstub.JSON(http.StatusNotFound, ""))
				stub.Handle("GET /_nodes", esstub.JSON(http.StatusOK, nodesBody))
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
			wantResult: &Result{WriteIndex: "cards"},
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
			wantMsg:    "Failed to create index: Failed to create index",
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
			name: "managed pipeline put rejected",
			setup: func(t *testing.T) Options {
				stub := esstub.New(t)
				stub.Handle("HEAD /cards", esstub.JSON(http.StatusOK, ""))
				stub.Handle("PUT /_ingest/pipeline/p", esstub.JSON(http.StatusBadRequest, `{"error":{"type":"parse_exception"}}`))
				return Options{
					URL:           stub.URL(),
					Index:         "cards",
					SyncManaged:   true,
					PipelinesFile: fixture.WriteTempJSON(t, t.TempDir(), `{"p":{"processors":[]}}`),
				}
			},
			wantKind:   ErrManagedResource,
			wantOp:     "Failed to create pipeline",
			wantMsg:    "Failed to create pipeline: Failed to create pipeline",
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
			wantMsg:    "Bulk API request failed: Bulk API request failed",
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
			wantMsg:    "Failed to get enrich policies: Failed to get enrich policies",
			wantResult: &Result{WriteIndex: "cards"},
		},
	}

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
	}
	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			t.Parallel()
			assertRunError(t, runFailure(failure.New(tt.op, errors.New("cause"))), tt.want, tt.op)
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

	const failBody = `{"error":{"type":"stub_failure"}}`
	tests := []struct {
		name      string
		setup     func(t *testing.T, rec *opRecorder) Options
		wantKind  error
		wantOp    string
		wantLast  string
		forbidden []string
	}{
		{
			name: "index delete failure issues no managed-resource delete",
			setup: func(t *testing.T, rec *opRecorder) Options {
				stub := esstub.New(t)
				stub.Handle("HEAD /cards", rec.recorded("index.head", esstub.JSON(http.StatusOK, "")))
				stub.Handle("DELETE /cards", rec.recorded("index.delete", esstub.JSON(http.StatusInternalServerError, failBody)))
				stub.Handle("DELETE /_ingest/pipeline/{name}", rec.recorded("pipeline.delete", esstub.JSON(http.StatusOK, ackBody)))
				return Options{
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
			setup: func(t *testing.T, rec *opRecorder) Options {
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
				return Options{
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
			setup: func(t *testing.T, rec *opRecorder) Options {
				stub := esstub.New(t)
				stub.Handle("HEAD /cards", rec.recorded("index.head", esstub.JSON(http.StatusOK, "")))
				stub.Handle("POST /cards/_delete_by_query", rec.recorded("flush", esstub.JSON(http.StatusInternalServerError, failBody)))
				stub.Handle("POST /_bulk", rec.recorded("bulk", esstub.JSON(http.StatusOK, bulkOKBody("cards"))))
				return Options{URL: stub.URL(), Index: "cards", FlushIndex: true, DataFile: writeBulkDataFixture(t)}
			},
			wantKind:  ErrIndexOperation,
			wantOp:    "Failed to flush index",
			wantLast:  "flush",
			forbidden: []string{"bulk"},
		},
		{
			name: "nuke pipeline delete failure stops the teardown",
			setup: func(t *testing.T, rec *opRecorder) Options {
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
				return Options{
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := &opRecorder{}
			_, err := Run(t.Context(), tt.setup(t, rec))
			assertRunError(t, err, tt.wantKind, tt.wantOp)

			ops := rec.operations()
			if len(ops) == 0 || ops[len(ops)-1] != tt.wantLast {
				t.Fatalf("operations = %v, want %q to be the last request", ops, tt.wantLast)
			}
			for _, op := range tt.forbidden {
				if slices.Contains(ops, op) {
					t.Fatalf("operations = %v: %q ran after the failure", ops, op)
				}
			}
		})
	}
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

package loader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// loggerRunStubConfig selects how a loggerRunStub behaves.
type loggerRunStubConfig struct {
	// indexExists reports the index as already present.
	indexExists bool
	// hold parks the index-create request until release is closed, so a test
	// can keep several Run lifetimes open at the same moment.
	hold bool
	// failCreate answers the index-create request with a 500.
	failCreate bool
}

// loggerRunStub is a minimal Elasticsearch stub for one concrete index.
type loggerRunStub struct {
	url     string
	arrived chan struct{}
	release chan struct{}
}

// newLoggerRunStub serves the requests of an -add Run against index. With
// hold set, the create request announces itself on arrived and waits for
// release (or the request context) before answering.
func newLoggerRunStub(t *testing.T, index string, cfg loggerRunStubConfig) *loggerRunStub {
	t.Helper()

	stub := &loggerRunStub{
		arrived: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	var mu sync.Mutex
	present := cfg.indexExists
	write := func(w http.ResponseWriter, status int, body string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/"+index:
			mu.Lock()
			exists := present
			mu.Unlock()
			if exists {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/_nodes":
			write(w, http.StatusOK, `{"nodes":{"node1":{}}}`)
		case r.Method == http.MethodPut && r.URL.Path == "/"+index:
			if cfg.hold {
				stub.arrived <- struct{}{}
				select {
				case <-stub.release:
				case <-r.Context().Done():
					return
				case <-time.After(15 * time.Second):
					t.Errorf("stub for %s was never released", index)
				}
			}
			if cfg.failCreate {
				write(w, http.StatusInternalServerError, `{"error":{"type":"stub_create_failure"},"status":500}`)
				return
			}
			mu.Lock()
			present = true
			mu.Unlock()
			write(w, http.StatusOK, `{"acknowledged":true}`)
		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			write(w, http.StatusOK, fmt.Sprintf(`{"errors":false,"items":[{"index":{"_index":%q,"_id":"1","status":201}}]}`, index))
		case r.Method == http.MethodPost && r.URL.Path == "/"+index+"/_refresh":
			write(w, http.StatusOK, `{"_shards":{"failed":0}}`)
		case r.Method == http.MethodGet && (r.URL.Path == "/_enrich/policy" || r.URL.Path == "/_enrich/policy/"):
			write(w, http.StatusOK, `{"policies":[]}`)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(server.Close)
	stub.url = server.URL
	return stub
}

// waitArrived blocks until the stub's create request is parked.
func (s *loggerRunStub) waitArrived(ctx context.Context) error {
	select {
	case <-s.arrived:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestConcurrentRunsWithInjectedLoggersDoNotInterleave holds two Runs open at
// the same moment, each with its own logger, and proves every log line lands
// in the buffer of the Run that produced it. Before Options.Logger, Run
// swapped the package-global logger, so overlapping Runs wrote into each
// other's output (and raced).
func TestConcurrentRunsWithInjectedLoggersDoNotInterleave(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		failB bool
	}{
		{name: "both succeed"},
		{name: "second run fails after its own log lines", failB: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			const indexA, indexB = "logger-alpha", "logger-beta"
			stubA := newLoggerRunStub(t, indexA, loggerRunStubConfig{hold: true})
			stubB := newLoggerRunStub(t, indexB, loggerRunStubConfig{hold: true, failCreate: tt.failB})
			dataFile := writeBulkDataFixture(t)

			var bufA, bufB bytes.Buffer
			loggerA := zerolog.New(&bufA)
			loggerB := zerolog.New(&bufB)

			// Registered first so it runs last: cancel (below) must free any
			// Run still parked in a stub before the join.
			var wg sync.WaitGroup
			t.Cleanup(wg.Wait)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			t.Cleanup(cancel)

			var errA, errB error
			wg.Add(2)
			go func() {
				defer wg.Done()
				_, errA = Run(ctx, Options{URL: stubA.url, Index: indexA, DataFile: dataFile, AddToIndex: true, Logger: &loggerA})
			}()
			go func() {
				defer wg.Done()
				_, errB = Run(ctx, Options{URL: stubB.url, Index: indexB, DataFile: dataFile, AddToIndex: true, Logger: &loggerB})
			}()

			if err := stubA.waitArrived(ctx); err != nil {
				t.Fatalf("run A never reached its create request: %v", err)
			}
			if err := stubB.waitArrived(ctx); err != nil {
				t.Fatalf("run B never reached its create request: %v", err)
			}
			// Both Runs are now mid-flight; let them finish together.
			close(stubA.release)
			close(stubB.release)
			wg.Wait()

			if errA != nil {
				t.Fatalf("run A: %v", errA)
			}
			logsA, logsB := bufA.String(), bufB.String()
			if !strings.Contains(logsA, indexA) || !strings.Contains(logsB, indexB) {
				t.Fatalf("each logger must record its own index (guards a vacuous pass); A=%q B=%q", logsA, logsB)
			}
			if strings.Contains(logsA, indexB) {
				t.Fatalf("logger A received run B output: %s", logsA)
			}
			if strings.Contains(logsB, indexA) {
				t.Fatalf("logger B received run A output: %s", logsB)
			}
			if !strings.Contains(logsA, "Index created") {
				t.Fatalf("run A log missing its post-barrier line: %s", logsA)
			}

			if tt.failB {
				var runErr *RunError
				if !errors.As(errB, &runErr) || !errors.Is(errB, ErrIndexOperation) {
					t.Fatalf("run B error = %v, want *RunError of kind ErrIndexOperation", errB)
				}
				if !strings.Contains(logsB, `"level":"fatal"`) || !strings.Contains(logsB, "Failed to create index") {
					t.Fatalf("run B fatal event missing from its own logger: %s", logsB)
				}
				if strings.Contains(logsA, "Failed to create index") {
					t.Fatalf("run B fatal event leaked into logger A: %s", logsA)
				}
				return
			}
			if errB != nil {
				t.Fatalf("run B: %v", errB)
			}
			if !strings.Contains(logsB, "Index created") {
				t.Fatalf("run B log missing its post-barrier line: %s", logsB)
			}
		})
	}
}

// unreachableWarnOptions yields a Run that logs one warning and then fails to
// reach Elasticsearch, without any stub.
func unreachableWarnOptions() Options {
	return Options{URL: "http://127.0.0.1:1", Index: "cards", KeepLast: 1, Nuke: true}
}

// TestInjectedNopLoggerSilencesRun guards the Disabled-logger trap: zerolog's
// WithContext refuses to store a Disabled logger, so carrying the logger with
// it would drop an injected zerolog.Nop() and let Run's output appear on the
// global logger instead, a wrong result that looks like success.
func TestInjectedNopLoggerSilencesRun(t *testing.T) {
	// Not parallel: replaces the package-global zerolog logger.
	var global bytes.Buffer
	previous := log.Logger
	log.Logger = zerolog.New(&global)
	t.Cleanup(func() { log.Logger = previous })

	// Control: with no injected logger the same Run does reach the global.
	if _, err := Run(t.Context(), unreachableWarnOptions()); err == nil {
		t.Fatal("expected Run to fail for unreachable Elasticsearch")
	}
	if global.Len() == 0 {
		t.Fatal("control Run wrote nothing to the global logger; the test would pass vacuously")
	}
	global.Reset()

	nop := zerolog.Nop()
	opts := unreachableWarnOptions()
	opts.Logger = &nop
	if _, err := Run(t.Context(), opts); err == nil {
		t.Fatal("expected Run to fail for unreachable Elasticsearch")
	}
	if global.Len() != 0 {
		t.Fatalf("injected Nop logger must silence Run, but the global received: %s", global.String())
	}
}

// TestRunLeavesGlobalLoggerUntouched proves an injected-logger Run neither
// replaces nor writes the global logger, which is what lets consumers drop
// their swap-and-restore mutex. The global is checked while Run is parked
// mid-flight as well as after it returns: a Run that swapped the global in and
// restored it on exit would pass an after-the-fact check alone.
func TestRunLeavesGlobalLoggerUntouched(t *testing.T) {
	// Not parallel: replaces the package-global zerolog logger.
	var global bytes.Buffer
	previous := log.Logger
	log.Logger = zerolog.New(&global)
	t.Cleanup(func() { log.Logger = previous })
	before := log.Logger

	const index = "logger-untouched"
	stub := newLoggerRunStub(t, index, loggerRunStubConfig{hold: true})
	dataFile := writeBulkDataFixture(t)
	var captured bytes.Buffer
	injected := zerolog.New(&captured)

	// Registered first so it runs last: cancel (below) must free a Run still
	// parked in the stub before the join.
	var wg sync.WaitGroup
	t.Cleanup(wg.Wait)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	var runErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, runErr = Run(ctx, Options{URL: stub.url, Index: index, DataFile: dataFile, AddToIndex: true, Logger: &injected})
	}()

	if err := stub.waitArrived(ctx); err != nil {
		t.Fatalf("run never reached its create request: %v", err)
	}
	// Error, not Fatal: the stub must still be released and the Run joined.
	if !reflect.DeepEqual(before, log.Logger) {
		t.Error("Run replaced the package-global logger while in flight")
	}
	close(stub.release)
	wg.Wait()

	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	if !reflect.DeepEqual(before, log.Logger) {
		t.Fatal("Run modified the package-global logger")
	}
	if global.Len() != 0 {
		t.Fatalf("Run wrote to the global logger despite an injected one: %s", global.String())
	}
	if captured.Len() == 0 {
		t.Fatal("injected logger received nothing")
	}
}

// TestNilLoggerUsesGlobalWithTimestamp is the nil-logger regression: today's
// behavior is that Run logs to the global, decorated with a timestamp, and
// leaves the global itself unchanged.
func TestNilLoggerUsesGlobalWithTimestamp(t *testing.T) {
	// Not parallel: replaces the package-global zerolog logger.
	var global bytes.Buffer
	previous := log.Logger
	log.Logger = zerolog.New(&global)
	t.Cleanup(func() { log.Logger = previous })
	before := log.Logger

	if _, err := Run(t.Context(), unreachableWarnOptions()); err == nil {
		t.Fatal("expected Run to fail for unreachable Elasticsearch")
	}

	logs := global.String()
	if !strings.Contains(logs, "Ignoring -keep-last because -alias is not enabled") {
		t.Fatalf("expected warning on the global logger, got: %s", logs)
	}
	if !strings.Contains(logs, `"time":`) {
		t.Fatalf("expected timestamp decoration on the global logger output, got: %s", logs)
	}
	if !reflect.DeepEqual(before, log.Logger) {
		t.Fatal("Run modified the package-global logger")
	}
}

// TestFatalLogsToInjectedLogger proves fatal(ctx) writes to the injected
// logger and that the RunError classification is unchanged by the new logger
// source.
func TestFatalLogsToInjectedLogger(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	missing := filepath.Join(t.TempDir(), "missing-pipelines.json")
	_, err := Run(t.Context(), Options{
		URL:           "http://127.0.0.1:1",
		Index:         "cards",
		SyncManaged:   true,
		PipelinesFile: missing,
		Logger:        &logger,
	})

	var runErr *RunError
	if !errors.As(err, &runErr) {
		t.Fatalf("error = %v, want *RunError", err)
	}
	if !errors.Is(err, ErrManagedResource) {
		t.Fatalf("error kind = %v, want ErrManagedResource", runErr.Kind)
	}
	if runErr.Op != "Reading pipeline definitions file" {
		t.Fatalf("op = %q, want %q", runErr.Op, "Reading pipeline definitions file")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cause must wrap os.ErrNotExist, got %v", runErr.Err)
	}
	logs := buf.String()
	if !strings.Contains(logs, `"level":"fatal"`) || !strings.Contains(logs, "Reading pipeline definitions file") {
		t.Fatalf("fatal event missing from injected logger: %s", logs)
	}
}

// TestConvenienceEntrypointsPropagateLogger proves LoadData, SyncManaged and
// ExecuteEnrich hand Options.Logger to Run and survive a nil context. The
// Disabled-logger leg only proves the entrypoints still succeed with one; that
// a Disabled logger silences Run is TestInjectedNopLoggerSilencesRun's job,
// which needs the global replaced and so cannot run in parallel here.
func TestConvenienceEntrypointsPropagateLogger(t *testing.T) {
	t.Parallel()

	const warning = "Ignoring -keep-last because -alias is not enabled"
	entrypoints := []struct {
		name string
		run  func(context.Context, Options) (Result, error)
	}{
		{"LoadData", LoadData},
		{"SyncManaged", SyncManaged},
		{"ExecuteEnrich", ExecuteEnrich},
	}
	for _, ep := range entrypoints {
		t.Run(ep.name, func(t *testing.T) {
			t.Parallel()

			newOptions := func(logger *zerolog.Logger) Options {
				stub := newLoggerRunStub(t, "cards", loggerRunStubConfig{indexExists: true})
				return Options{
					URL:      stub.url,
					Index:    "cards",
					DataFile: writeBulkDataFixture(t),
					KeepLast: 1,
					Logger:   logger,
				}
			}

			var nilCtx context.Context
			var buf bytes.Buffer
			logger := zerolog.New(&buf)
			result, err := ep.run(nilCtx, newOptions(&logger))
			if err != nil {
				t.Fatalf("%s with nil context: %v", ep.name, err)
			}
			if !strings.Contains(buf.String(), warning) {
				t.Fatalf("%s did not log to the injected logger: %q", ep.name, buf.String())
			}
			if len(result.Warnings) == 0 {
				t.Fatalf("%s result lost its warning", ep.name)
			}

			nop := zerolog.Nop()
			if _, err := ep.run(t.Context(), newOptions(&nop)); err != nil {
				t.Fatalf("%s with Disabled logger: %v", ep.name, err)
			}
		})
	}
}

// TestLoggerFromFallsBackToGlobal pins the documented fallback and that a
// Disabled logger carried on ctx is returned as-is, not replaced.
func TestLoggerFromFallsBackToGlobal(t *testing.T) {
	t.Parallel()

	var nilCtx context.Context
	if got := loggerFrom(nilCtx); got != &log.Logger {
		t.Fatal("loggerFrom(nil) must return the global logger")
	}
	if got := loggerFrom(t.Context()); got != &log.Logger {
		t.Fatal("loggerFrom(ctx without logger) must return the global logger")
	}

	nop := zerolog.Nop()
	if got := loggerFrom(withRunLogger(t.Context(), &nop)); got != &nop {
		t.Fatal("loggerFrom must return the logger attached with withRunLogger, even when Disabled")
	}
}

package indexops

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

// TestExistsUsesCallerContext verifies cancellation reaches an in-flight
// Elasticsearch existence request.
func TestExistsUsesCallerContext(t *testing.T) {
	requestStarted := make(chan struct{})
	stub := esstub.New(t)
	stub.Handle("HEAD /cards", func(_ http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-r.Context().Done()
	})

	es := stub.MustClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := Exists(ctx, es, "cards")
		result <- err
	}()

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("existence request did not start")
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("indexExists error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("indexExists did not return promptly after cancellation")
	}
}

// TestWaitForIndexCancellationStopsPolling verifies cancellation during the
// polling delay prevents another existence request.
func TestWaitForIndexCancellationStopsPolling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var requests atomic.Int32
	stub := esstub.New(t)
	stub.Handle("HEAD /cards", func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		cancel()
		w.WriteHeader(http.StatusNotFound)
	})

	es := stub.MustClient(t)

	// The injected sleep honours ctx like the production seam but never waits.
	sleep := func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	err := WaitForIndex(ctx, es, "cards", sleep)
	// The cancel can surface on the in-flight HEAD or on the poll sleep; both
	// ops classify the same way (TestOperationKindsOfMovedHelpers in package
	// loader), and either way no second request is sent.
	var opErr *failure.Error
	if !errors.As(err, &opErr) || (opErr.Op != "waiting for index creation" && opErr.Op != "Context cancelled while waiting for index to become visible") {
		t.Fatalf("WaitForIndex error = %#v, want a cancellation operation error", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForIndex error = %v, want context.Canceled", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("existence requests = %d, want 1", got)
	}
}

// TestVerifyMappings verifies behavior for the related scenario.
func TestVerifyMappings(t *testing.T) {
	t.Parallel()

	plan := config.MappingPlan{
		DateDetectionExpected: boolPointer(false),
		RootFieldTypes: map[string]string{
			"notes": "keyword",
		},
	}

	t.Run("passes with expected date_detection and field types", func(t *testing.T) {
		t.Parallel()

		es := newMappingResponseClient(t, "target-index", http.StatusOK, `{"target-index":{"mappings":{"date_detection":false,"properties":{"notes":{"type":"keyword"}}}}}`)

		if err := VerifyMappings(t.Context(), es, "target-index", plan, nil); err != nil {
			t.Fatalf("verifyMappingPreflight returned error: %v", err)
		}
	})

	t.Run("fails when root field type mismatches", func(t *testing.T) {
		t.Parallel()

		es := newMappingResponseClient(t, "target-index", http.StatusOK, `{"target-index":{"mappings":{"date_detection":false,"properties":{"notes":{"type":"date"}}}}}`)

		err := VerifyMappings(t.Context(), es, "target-index", plan, nil)
		if err == nil {
			t.Fatal("expected mapping preflight error")
		}
		if !strings.Contains(err.Error(), `field "notes" has type "date", expected "keyword"`) {
			t.Fatalf("unexpected preflight error: %v", err)
		}
	})

	t.Run("fails when date_detection mismatches", func(t *testing.T) {
		t.Parallel()

		es := newMappingResponseClient(t, "target-index", http.StatusOK, `{"target-index":{"mappings":{"date_detection":true,"properties":{"notes":{"type":"keyword"}}}}}`)

		err := VerifyMappings(t.Context(), es, "target-index", plan, nil)
		if err == nil {
			t.Fatal("expected date_detection preflight error")
		}
		if !strings.Contains(err.Error(), "mappings.date_detection is true, expected false") {
			t.Fatalf("unexpected preflight error: %v", err)
		}
	})
}

// TestVerifyMappingsReportsEveryMismatchInFieldOrder guards the
// aggregated preflight error: every mismatch class (date_detection against the
// omitted-means-true default, missing fields, type mismatches) is reported in
// one error, date_detection first and then fields in sorted order. Repeating
// the call makes map-iteration nondeterminism fail the exact-string check.
func TestVerifyMappingsReportsEveryMismatchInFieldOrder(t *testing.T) {
	t.Parallel()

	es := newMappingResponseClient(t, "target-index", http.StatusOK, `{"target-index":{"mappings":{"properties":{"beta":{"type":"text"}}}}}`)

	plan := config.MappingPlan{
		DateDetectionExpected: boolPointer(false),
		RootFieldTypes:        map[string]string{"gamma": "keyword", "alpha": "long", "beta": "keyword"},
	}
	want := `index "target-index" mappings do not match the mappings file: ` +
		`mappings.date_detection is true, expected false; ` +
		`required mapping for field "alpha" is missing; ` +
		`field "beta" has type "text", expected "keyword"; ` +
		`required mapping for field "gamma" is missing`

	for attempt := 0; attempt < 20; attempt++ {
		err := VerifyMappings(t.Context(), es, "target-index", plan, nil)
		if err == nil {
			t.Fatalf("attempt %d: expected mapping preflight error", attempt)
		}
		if got := err.Error(); got != want {
			t.Fatalf("attempt %d: preflight error mismatch\n got: %s\nwant: %s", attempt, got, want)
		}
	}
}

// TestVerifyMappingsChecksEveryAliasTarget guards FIX-ALIASPRE-01:
// the preflight verifies every concrete index behind an alias and names each
// failing one in sorted order. It fails if the preflight goes back to checking
// one map-order entry, because the stale sibling would then pass or the
// reported index would vary between calls.
func TestVerifyMappingsChecksEveryAliasTarget(t *testing.T) {
	t.Parallel()

	plan := config.MappingPlan{
		RootFieldTypes: map[string]string{"lookup_id": "keyword", "base_value": "integer"},
	}
	const good = `{"mappings":{"properties":{"lookup_id":{"type":"keyword"},"base_value":{"type":"integer"}}}}`
	const staleBase = `{"mappings":{"properties":{"lookup_id":{"type":"keyword"},"base_value":{"type":"keyword"}}}}`
	const missingLookup = `{"mappings":{"properties":{"base_value":{"type":"integer"}}}}`
	staleMsg := `index "cards-a" mappings do not match the mappings file: field "base_value" has type "keyword", expected "integer"`

	tests := []struct {
		name      string
		requested string
		body      string
		want      string
	}{
		{
			name:      "both targets match",
			requested: "cards",
			body:      `{"cards-a":` + good + `,"cards-b":` + good + `}`,
		},
		{
			name:      "one target stale",
			requested: "cards",
			body:      `{"cards-a":` + good + `,"cards-b":` + staleBase + `}`,
			want:      `alias "cards": index "cards-b" mappings do not match the mappings file: field "base_value" has type "keyword", expected "integer"`,
		},
		{
			name:      "both targets stale, sorted",
			requested: "cards",
			body:      `{"cards-b":` + missingLookup + `,"cards-a":` + staleBase + `}`,
			want: `alias "cards": ` + staleMsg +
				` | index "cards-b" mappings do not match the mappings file: required mapping for field "lookup_id" is missing`,
		},
		{
			name:      "single-target alias",
			requested: "cards",
			body:      `{"cards-a":` + staleBase + `}`,
			want:      `alias "cards": ` + staleMsg,
		},
		{
			name:      "concrete index",
			requested: "cards-a",
			body:      `{"cards-a":` + staleBase + `}`,
			want:      staleMsg,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			es := newMappingResponseClient(t, tt.requested, http.StatusOK, tt.body)
			for attempt := 0; attempt < 20; attempt++ {
				err := VerifyMappings(t.Context(), es, tt.requested, plan, nil)
				if tt.want == "" {
					if err != nil {
						t.Fatalf("attempt %d: unexpected error: %v", attempt, err)
					}
					continue
				}
				if err == nil {
					t.Fatalf("attempt %d: expected mapping preflight error", attempt)
				}
				if got := err.Error(); got != tt.want {
					t.Fatalf("attempt %d: preflight error mismatch\n got: %s\nwant: %s", attempt, got, tt.want)
				}
			}
		})
	}
}

// TestVerifyMappingsChecksDateDetectionOnEveryAliasTarget guards that
// the date_detection rule is applied per target, not only the field types.
func TestVerifyMappingsChecksDateDetectionOnEveryAliasTarget(t *testing.T) {
	t.Parallel()

	plan := config.MappingPlan{DateDetectionExpected: boolPointer(false)}
	body := `{"cards-a":{"mappings":{"date_detection":false}},"cards-b":{"mappings":{"date_detection":true}}}`
	es := newMappingResponseClient(t, "cards", http.StatusOK, body)

	err := VerifyMappings(t.Context(), es, "cards", plan, nil)
	want := `alias "cards": index "cards-b" mappings do not match the mappings file: mappings.date_detection is true, expected false`
	if err == nil || err.Error() != want {
		t.Fatalf("got %v, want %s", err, want)
	}
}

// TestFetchIndexMappingsRejectsMalformedResponses guards that every
// fetch-failure shape stays a fetch failure: never a nil error (a pass) and
// never a mismatch message. Two malformed entries must always name the same
// (first, sorted) one.
func TestFetchIndexMappingsRejectsMalformedResponses(t *testing.T) {
	t.Parallel()

	plan := config.MappingPlan{RootFieldTypes: map[string]string{"lookup_id": "keyword"}}
	tests := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"server error", http.StatusInternalServerError, `{"error":"boom"}`, `failed with status 500`},
		{"undecodable body", http.StatusOK, `not json`, `decoding mappings response`},
		{"json null", http.StatusOK, `null`, `was empty`},
		{"non-object entry", http.StatusOK, `{"cards-a":1}`, `"cards-a" did not include an object entry`},
		{"entry without mappings", http.StatusOK, `{"cards-a":{}}`, `"cards-a" did not include a mappings object`},
		{"two malformed entries name the first", http.StatusOK, `{"cards-z":1,"cards-b":{},"cards-a":2}`, `"cards-a" did not include an object entry`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			es := newMappingResponseClient(t, "cards", tt.status, tt.body)
			for attempt := 0; attempt < 20; attempt++ {
				_, err := fetchIndexMappings(es, "cards")
				if err == nil || !strings.Contains(err.Error(), tt.want) {
					t.Fatalf("attempt %d: fetchIndexMappings error = %v, want containing %q", attempt, err, tt.want)
				}
				err = VerifyMappings(t.Context(), es, "cards", plan, nil)
				if err == nil || !strings.Contains(err.Error(), tt.want) || strings.Contains(err.Error(), "do not match the mappings file") {
					t.Fatalf("attempt %d: verifyMappingPreflight error = %v, want fetch failure containing %q", attempt, err, tt.want)
				}
			}
		})
	}

	t.Run("transport error", func(t *testing.T) {
		t.Parallel()

		// Explicit early close: the connection must be refused so the
		// client reports a transport error, not an HTTP status.
		stub := esstub.New(t)
		es := stub.MustClient(t)
		stub.Close()

		_, err := fetchIndexMappings(es, "cards")
		if err == nil || !strings.Contains(err.Error(), `fetching mappings for index "cards"`) {
			t.Fatalf("expected transport error, got %v", err)
		}
	})

	t.Run("response body read error", func(t *testing.T) {
		t.Parallel()

		stub := esstub.New(t)
		stub.Handle("GET /cards/_mapping", func(w http.ResponseWriter, _ *http.Request) {
			// Declare more bytes than are sent so the body read fails.
			w.Header().Set("Content-Length", "1000")
			_, _ = w.Write([]byte(`{"cards-a":`))
		})
		es := stub.MustClient(t)

		err := VerifyMappings(t.Context(), es, "cards", plan, nil)
		if err == nil || !strings.Contains(err.Error(), `reading mappings response for index "cards"`) {
			t.Fatalf("expected body read error, got %v", err)
		}
	})
}

// TestVerifyMappingsLogsAliasAndIndexFields guards the alias/index
// noun rule for the passing-target log: an alias request logs both fields, a
// concrete request logs only index.
func TestVerifyMappingsLogsAliasAndIndexFields(t *testing.T) {
	t.Parallel()

	plan := config.MappingPlan{RootFieldTypes: map[string]string{"lookup_id": "keyword"}}
	const good = `{"mappings":{"properties":{"lookup_id":{"type":"keyword"}}}}`

	capture := func(requested, body string) []map[string]any {
		var output bytes.Buffer
		logger := zerolog.New(&output).Level(zerolog.TraceLevel)
		es := newMappingResponseClient(t, requested, http.StatusOK, body)
		if err := VerifyMappings(runlog.WithLogger(t.Context(), &logger), es, requested, plan, nil); err != nil {
			t.Fatalf("verifyMappingPreflight returned error: %v", err)
		}
		var entries []map[string]any
		for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
			var entry map[string]any
			if err := json.Unmarshal([]byte(line), &entry); err != nil {
				t.Fatalf("decoding log line %q: %v", line, err)
			}
			entries = append(entries, entry)
		}
		return entries
	}

	aliasEntries := capture("cards", `{"cards-a":`+good+`,"cards-b":`+good+`}`)
	if len(aliasEntries) != 2 {
		t.Fatalf("expected one passed log per target, got %v", aliasEntries)
	}
	for i, wantIndex := range []string{"cards-a", "cards-b"} {
		if aliasEntries[i]["index"] != wantIndex || aliasEntries[i]["alias"] != "cards" {
			t.Fatalf("alias log %d = %v, want index %q and alias %q", i, aliasEntries[i], wantIndex, "cards")
		}
	}

	concreteEntries := capture("cards-a", `{"cards-a":`+good+`}`)
	if len(concreteEntries) != 1 || concreteEntries[0]["index"] != "cards-a" {
		t.Fatalf("unexpected concrete log: %v", concreteEntries)
	}
	if _, ok := concreteEntries[0]["alias"]; ok {
		t.Fatalf("concrete request must not log an alias field: %v", concreteEntries[0])
	}
}

// TestVerifyMappingsResponseSetGuards guards that the empty and
// concrete-plus-other set-shape errors are raised by VerifyMappings, not the
// fetch helper: the fetch helper must hand back the decoded valid object so
// the expected-set comparison can see the real key set.
func TestVerifyMappingsResponseSetGuards(t *testing.T) {
	t.Parallel()

	plan := config.MappingPlan{RootFieldTypes: map[string]string{"lookup_id": "keyword"}}
	tests := []struct {
		name     string
		body     string
		wantKeys int
		want     string
	}{
		{"empty object", `{}`, 0, `mappings response for index "cards" was empty`},
		{"concrete plus other index", `{"cards":{"mappings":{}},"cards-x":{"mappings":{}}}`, 2, `mappings response for index "cards" included other indices: cards, cards-x`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			es := newMappingResponseClient(t, "cards", http.StatusOK, tt.body)
			got, err := fetchIndexMappings(es, "cards")
			if err != nil || got == nil || len(got) != tt.wantKeys {
				t.Fatalf("fetchIndexMappings = %v, %v; want %d decoded keys and no error", got, err, tt.wantKeys)
			}
			err = VerifyMappings(t.Context(), es, "cards", plan, nil)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("VerifyMappings error = %v, want %q", err, tt.want)
			}
		})
	}
}

// TestVerifyMappingsExpectedTargets guards FIX-ALIASSET-01: the response key
// set must equal the expected target set before any field check. A revert
// would let an omitted alias target be flushed without ever being verified.
func TestVerifyMappingsExpectedTargets(t *testing.T) {
	t.Parallel()

	plan := config.MappingPlan{RootFieldTypes: map[string]string{"id": "keyword"}}
	const good = `{"mappings":{"properties":{"id":{"type":"keyword"}}}}`
	const stale = `{"mappings":{"properties":{"id":{"type":"long"}}}}`
	const prefix = `alias "cards": mappings response does not match the alias targets: `

	tests := []struct {
		name      string
		requested string
		expected  []string
		body      string
		want      string // empty: success
	}{
		{"missing one", "cards", []string{"cards-a", "cards-b"}, `{"cards-a":` + good + `}`, prefix + `missing cards-b`},
		{"unexpected one", "cards", []string{"cards-a"}, `{"cards-a":` + good + `,"cards-b":` + good + `}`, prefix + `unexpected cards-b`},
		{"both groups sorted and deduplicated", "cards", []string{"cards-c", "cards-a", "cards-c"}, `{"cards-d":` + good + `,"cards-b":` + good + `}`, prefix + `missing cards-a, cards-c; unexpected cards-b, cards-d`},
		{"unsorted duplicate expected, equal sets", "cards", []string{"cards-b", "cards-a", "cards-a"}, `{"cards-a":` + good + `,"cards-b":` + good + `}`, ""},
		{"equal sets stale field", "cards", []string{"cards-a", "cards-b"}, `{"cards-a":` + good + `,"cards-b":` + stale + `}`, `alias "cards": index "cards-b" mappings do not match the mappings file: field "id" has type "long", expected "keyword"`},
		{"concrete equal sets", "cards", []string{"cards"}, `{"cards":` + good + `}`, ""},
		{"concrete equal sets stale", "cards", []string{"cards"}, `{"cards":` + stale + `}`, `index "cards" mappings do not match the mappings file: field "id" has type "long", expected "keyword"`},
		{"empty object", "cards", []string{"cards-b", "cards-a"}, `{}`, prefix + `missing cards-a, cards-b`},
		{"concrete fallback with extra", "cards", []string{"cards"}, `{"cards":` + good + `,"cards-x":` + good + `}`, prefix + `unexpected cards-x`},
		{"concrete fallback only other", "cards", []string{"cards"}, `{"cards-x":` + good + `}`, prefix + `missing cards; unexpected cards-x`},
		{"set mismatch wins over stale field", "cards", []string{"cards-a", "cards-b"}, `{"cards-a":` + stale + `}`, prefix + `missing cards-b`},
		{"nil expected keeps field errors", "cards", nil, `{"cards-a":` + good + `,"cards-b":` + stale + `}`, `alias "cards": index "cards-b" mappings do not match the mappings file: field "id" has type "long", expected "keyword"`},
		{"empty expected keeps success", "cards", []string{}, `{"cards-a":` + good + `,"cards-b":` + good + `}`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			stub.Handle("GET /"+tt.requested+"/_mapping", esstub.JSON(http.StatusOK, tt.body))
			es := stub.MustClient(t)
			original := slices.Clone(tt.expected)

			for attempt := 0; attempt < 20; attempt++ {
				var output bytes.Buffer
				logger := zerolog.New(&output).Level(zerolog.TraceLevel)
				err := VerifyMappings(runlog.WithLogger(t.Context(), &logger), es, tt.requested, plan, tt.expected)
				if tt.want == "" {
					if err != nil {
						t.Fatalf("attempt %d: unexpected error: %v", attempt, err)
					}
				} else {
					if err == nil || err.Error() != tt.want {
						t.Fatalf("attempt %d: error = %v, want %q", attempt, err, tt.want)
					}
					if strings.Contains(tt.want, "alias targets") && strings.Contains(output.String(), "Mapping preflight passed") {
						t.Fatalf("attempt %d: set failure logged a passed preflight: %s", attempt, output.String())
					}
				}
				if !slices.Equal(tt.expected, original) {
					t.Fatalf("attempt %d: expected slice mutated: %v, want %v", attempt, tt.expected, original)
				}
			}

			requests := stub.Requests()
			if len(requests) != 20 {
				t.Fatalf("mapping requests = %d, want exactly one per call (20)", len(requests))
			}
			for _, r := range requests {
				if r.Method != http.MethodGet || r.Path != "/"+tt.requested+"/_mapping" {
					t.Fatalf("unexpected request %s %s", r.Method, r.Path)
				}
			}
		})
	}

	t.Run("fetch failures stay fetch failures with an expected set", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name   string
			status int
			body   string
			want   string
		}{
			{"malformed entry", http.StatusOK, `{"cards-a":1}`, `"cards-a" did not include an object entry`},
			{"undecodable", http.StatusOK, `not json`, `decoding mappings response`},
			{"http failure", http.StatusInternalServerError, `{"error":"boom"}`, `failed with status 500`},
			{"json null", http.StatusOK, `null`, `was empty`},
		}
		for _, c := range cases {
			es := newMappingResponseClient(t, "cards", c.status, c.body)
			err := VerifyMappings(t.Context(), es, "cards", plan, []string{"cards-a"})
			if err == nil || !strings.Contains(err.Error(), c.want) || strings.Contains(err.Error(), "alias targets") {
				t.Fatalf("%s: error = %v, want fetch failure containing %q", c.name, err, c.want)
			}
		}
	})
}

// TestFlushValidatesDeleteByQueryResponse verifies that a flush fails
// on shard failures or malformed responses while tolerating version conflicts.
func TestFlushValidatesDeleteByQueryResponse(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		wantOp string // empty: Flush must succeed
	}{
		{
			name: "empty failures succeeds",
			body: `{"version_conflicts":0,"failures":[]}`,
		},
		{
			name: "version conflicts remain accepted",
			body: `{"version_conflicts":2,"failures":[]}`,
		},
		{
			name:   "shard failure fails",
			body:   `{"failures":[{"shard":0,"reason":{"type":"illegal_state_exception","reason":"failed"}}]}`,
			wantOp: "Index flush completed with shard-level failures",
		},
		{
			name:   "malformed success body fails",
			body:   `{`,
			wantOp: "Unable to parse index flush response body",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			stub.Handle("POST /cards/_delete_by_query", func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query().Get("conflicts"); got != "proceed" {
					t.Errorf("conflicts query mismatch: got %q want %q", got, "proceed")
				}
				if got := r.URL.Query().Get("refresh"); got != "true" {
					t.Errorf("refresh query mismatch: got %q want %q", got, "true")
				}
				_, _ = w.Write([]byte(tt.body))
			})
			es := stub.MustClient(t)

			err := Flush(t.Context(), es, "cards")
			if tt.wantOp == "" {
				if err != nil {
					t.Fatalf("Flush returned error: %v", err)
				}
				return
			}

			// Kind for these ops is pinned by TestOperationKindsOfMovedHelpers
			// in package loader.
			var opErr *failure.Error
			if !errors.As(err, &opErr) || opErr.Op != tt.wantOp {
				t.Fatalf("Flush error = %#v, want operation error %q", err, tt.wantOp)
			}
		})
	}
}

// TestClusterReplicaCount verifies happy and sad paths for ADR 0001.
// ADR: docs/decisions/0001-replica-count-from-node-count.md
func TestClusterReplicaCount(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		handler  http.HandlerFunc
		wantNil  bool // use nil client instead of server
		expected int
	}{
		{
			name:     "nil client returns 0",
			wantNil:  true,
			expected: 0,
		},
		{
			name:     "single node returns 0",
			handler:  esstub.JSON(http.StatusOK, `{"nodes":{"node1":{}}}`),
			expected: 0,
		},
		{
			name:     "multi-node returns 1",
			handler:  esstub.JSON(http.StatusOK, `{"nodes":{"node1":{},"node2":{},"node3":{}}}`),
			expected: 1,
		},
		{
			name:     "non-2xx response returns 0",
			handler:  esstub.JSON(http.StatusForbidden, `{"error":{"type":"security_exception"}}`),
			expected: 0,
		},
		{
			name:     "unparseable body returns 0",
			handler:  esstub.JSON(http.StatusOK, `not json`),
			expected: 0,
		},
		{
			name:     "empty nodes object returns 0",
			handler:  esstub.JSON(http.StatusOK, `{"nodes":{}}`),
			expected: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var es *elasticsearch.Client
			if !tc.wantNil {
				stub := esstub.New(t)
				stub.Handle("GET /_nodes", tc.handler)
				es = stub.MustClient(t)
			}

			got := ClusterReplicaCount(t.Context(), es)
			if got != tc.expected {
				t.Fatalf("ClusterReplicaCount() = %d, want %d", got, tc.expected)
			}
		})
	}
}

// newReplicaStubClient serves a 2-node _nodes body and counts /_nodes hits.
func newReplicaStubClient(t *testing.T, nodeHits *atomic.Int32) *elasticsearch.Client {
	t.Helper()

	stub := esstub.New(t)
	stub.Handle("GET /_nodes", func(w http.ResponseWriter, _ *http.Request) {
		nodeHits.Add(1)
		_, _ = w.Write([]byte(`{"nodes":{"n1":{},"n2":{}}}`))
	})
	return stub.MustClient(t)
}

// newMappingResponseClient starts an isolated stub that answers
// GET /<requested>/_mapping with the given status and body, and returns a
// client for it. Each caller gets its own immutable stub so parallel subtests
// never share a mutable response. Any other request is an unexpected route.
func newMappingResponseClient(t *testing.T, requested string, status int, body string) *elasticsearch.Client {
	t.Helper()

	stub := esstub.New(t)
	stub.Handle("GET /"+requested+"/_mapping", esstub.JSON(status, body))
	return stub.MustClient(t)
}

// boolPointer centralizes this code path so package behavior stays consistent.
func boolPointer(value bool) *bool {
	return &value
}

// TestSelectReplicaCount verifies override selection: nil derives from
// topology (and queries /_nodes), any non-nil value wins and never does. The
// explicit-zero case guards a "simplify to > 0" refactor, which would silently
// turn -replicas 0 into the topology default of 1 on a multi-node cluster.
// ADR: docs/decisions/0001-replica-count-from-node-count.md
func TestSelectReplicaCount(t *testing.T) {
	t.Parallel()

	zero, one, three := 0, 1, 3
	cases := []struct {
		name         string
		override     *int
		want         int
		wantSource   string
		wantNodeHits int32
	}{
		{name: "nil derives from 2-node topology", override: nil, want: 1, wantSource: "cluster topology", wantNodeHits: 1},
		{name: "explicit 0 is honoured on a multi-node cluster", override: &zero, want: 0, wantSource: "explicit"},
		{name: "explicit 1", override: &one, want: 1, wantSource: "explicit"},
		{name: "explicit 3", override: &three, want: 3, wantSource: "explicit"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var hits atomic.Int32
			es := newReplicaStubClient(t, &hits)

			replicas, source := SelectReplicaCount(t.Context(), es, tc.override)
			if replicas != tc.want || source != tc.wantSource {
				t.Fatalf("SelectReplicaCount = (%d, %q), want (%d, %q)", replicas, source, tc.want, tc.wantSource)
			}
			if got := hits.Load(); got != tc.wantNodeHits {
				t.Fatalf("/_nodes hits = %d, want %d", got, tc.wantNodeHits)
			}
		})
	}
}

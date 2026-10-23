package loader

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
)

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
				es = newStubClient(t, stub)
			}

			got := clusterReplicaCount(t.Context(), es)
			if got != tc.expected {
				t.Fatalf("clusterReplicaCount() = %d, want %d", got, tc.expected)
			}
		})
	}
}

// TestNormalizeIndexSettingsReplicaOverridesFixture verifies that the replica count
// passed to normalizeIndexSettings always wins over the value in the settings file,
// regardless of what the fixture specifies.
// ADR: docs/decisions/0001-replica-count-from-node-count.md
func TestNormalizeIndexSettingsReplicaOverridesFixture(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		fixture  string
		replicas int
		expected float64
	}{
		{
			name:     "fixture 0 replicas overridden to 1 for multi-node",
			fixture:  `{"settings":{"index":{"number_of_replicas":0}}}`,
			replicas: 1,
			expected: 1,
		},
		{
			name:     "fixture 2 replicas overridden to 0 for single-node",
			fixture:  `{"settings":{"index":{"number_of_replicas":2}}}`,
			replicas: 0,
			expected: 0,
		},
		{
			name:     "missing replica key in fixture gets value from topology",
			fixture:  `{"settings":{"number_of_shards":1}}`,
			replicas: 1,
			expected: 1,
		},
		{
			name:     "fixture string 5 replicas overridden by explicit 2",
			fixture:  `{"settings":{"index":{"number_of_replicas":"5"}}}`,
			replicas: 2,
			expected: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := writeTempJSON(t, dir, tc.fixture)

			normalized := normalizeIndexSettings(t.Context(), path, "", tc.replicas, "explicit", nil)

			var parsed map[string]any
			if err := json.Unmarshal([]byte(normalized), &parsed); err != nil {
				t.Fatalf("parse normalized settings: %v", err)
			}

			if got := parsed["number_of_replicas"]; got != tc.expected {
				t.Fatalf("number_of_replicas = %v, want %v", got, tc.expected)
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
	return newStubClient(t, stub)
}

// replicasInBody parses the create-index body and returns settings.number_of_replicas.
func replicasInBody(t *testing.T, body string) float64 {
	t.Helper()

	var parsed struct {
		Settings map[string]any `json:"settings"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("parse create body %q: %v", body, err)
	}
	got, ok := parsed.Settings["number_of_replicas"].(float64)
	if !ok {
		t.Fatalf("number_of_replicas missing or not a number in %q", body)
	}
	return got
}

// TestBuildCreateIndexBodyReplicas verifies override selection: nil derives
// from topology (and queries /_nodes), any non-nil value wins and never does.
// ADR: docs/decisions/0001-replica-count-from-node-count.md
func TestBuildCreateIndexBodyReplicas(t *testing.T) {
	t.Parallel()

	zero, one, three := 0, 1, 3
	cases := []struct {
		name         string
		override     *int
		want         float64
		wantNodeHits int32
	}{
		{name: "nil derives from 2-node topology", override: nil, want: 1, wantNodeHits: 1},
		{name: "explicit 0", override: &zero, want: 0},
		{name: "explicit 1", override: &one, want: 1},
		{name: "explicit 3", override: &three, want: 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var hits atomic.Int32
			es := newReplicaStubClient(t, &hits)

			body := buildCreateIndexBody(t.Context(), es, tc.override, "", "", "", nil)

			if got := replicasInBody(t, body); got != tc.want {
				t.Fatalf("number_of_replicas = %v, want %v", got, tc.want)
			}
			if got := hits.Load(); got != tc.wantNodeHits {
				t.Fatalf("/_nodes hits = %d, want %d", got, tc.wantNodeHits)
			}
		})
	}
}

// TestExplicitZeroReplicasIsHonouredOnMultiNodeCluster guards the explicit-zero
// branch: a "simplify to > 0" refactor would silently turn -replicas 0 into the
// topology default of 1 on a multi-node cluster (a throwaway bulk-load index
// that deliberately wants no redundancy).
func TestExplicitZeroReplicasIsHonouredOnMultiNodeCluster(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	es := newReplicaStubClient(t, &hits)
	zero := 0

	body := buildCreateIndexBody(t.Context(), es, &zero, "", "", "", nil)

	if got := replicasInBody(t, body); got != 0 {
		t.Fatalf("number_of_replicas = %v, want 0 on a 2-node cluster", got)
	}
	if hits.Load() != 0 {
		t.Fatalf("topology was queried despite an explicit override")
	}
}

// TestRunRejectsNegativeReplicas verifies the single range check lives in Run.
// Index and an action are set so no earlier validation can produce the same
// ErrInvalidOptions for the wrong reason; the Op pins the reason.
func TestRunRejectsNegativeReplicas(t *testing.T) {
	t.Parallel()
	for _, v := range []int{-1, -2} {
		neg := v
		_, err := Run(t.Context(), Options{Index: "products", SyncManaged: true, Replicas: &neg})
		if !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("replicas %d: err = %v, want ErrInvalidOptions", v, err)
		}
		var runErr *RunError
		if !errors.As(err, &runErr) || runErr.Op != "validating replicas" {
			t.Fatalf("replicas %d: err = %v, want Op %q", v, err, "validating replicas")
		}
	}
}

// TestRunExistingIndexWithReplicasOverrideNeitherUpdatesSettingsNorQueriesTopology
// guards that the override affects creation only: add/flush on an existing
// index must retain its replicas (no settings PUT) and skip the _nodes query.
func TestRunExistingIndexWithReplicasOverrideNeitherUpdatesSettingsNorQueriesTopology(t *testing.T) {
	t.Parallel()
	fixtures := writePreflightRunFixtures(t)
	two := 2

	for _, opts := range []Options{{FlushIndex: true}, {AddToIndex: true}} {
		stub := newPreflightRunStub(t, preflightStubConfig{indexExists: true, liveMappings: preflightLiveMappings})
		opts.URL = stub.url
		opts.Index = "cards"
		opts.DataFile = fixtures.data
		opts.MappingsFile = fixtures.matchingMappings
		opts.Replicas = &two

		if _, err := Run(t.Context(), opts); err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
		ops := stub.operations()
		if operationCount(ops, "nodes.get") != 0 || operationCount(ops, "index.create") != 0 {
			t.Fatalf("existing index touched creation path with an override: %v", ops)
		}
	}
}

package loader

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"
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
			name: "nil client returns 0",
			wantNil: true,
			expected: 0,
		},
		{
			name: "single node returns 0",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"nodes":{"node1":{}}}`))
			},
			expected: 0,
		},
		{
			name: "multi-node returns 1",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"nodes":{"node1":{},"node2":{},"node3":{}}}`))
			},
			expected: 1,
		},
		{
			name: "non-2xx response returns 0",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":{"type":"security_exception"}}`))
			},
			expected: 0,
		},
		{
			name: "unparseable body returns 0",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`not json`))
			},
			expected: 0,
		},
		{
			name: "empty nodes object returns 0",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"nodes":{}}`))
			},
			expected: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var es *elasticsearch.Client
			if !tc.wantNil {
				server := httptest.NewServer(tc.handler)
				t.Cleanup(server.Close)

				var err error
				es, err = elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{server.URL}})
				if err != nil {
					t.Fatalf("NewClient: %v", err)
				}
			}

			got := clusterReplicaCount(es)
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := writeTempJSON(t, dir, tc.fixture)

			normalized := normalizeIndexSettings(path, "", tc.replicas, nil)

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

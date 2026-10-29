package loader

import (
	"errors"
	"testing"
)

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

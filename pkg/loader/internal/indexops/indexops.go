// Package indexops performs the concrete-index operations of a Run: existence
// and visibility checks, creation, deletion, flush, refresh, replica-count
// selection, live-mapping verification and the index.default_pipeline
// settings that managed teardown clears. Failures are returned as operation
// errors (see internal/failure); best-effort reads log and fall back.
package indexops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

const (
	// indexVisibilityMaxAttempts bounds index-creation visibility checks.
	indexVisibilityMaxAttempts = 20
	// indexVisibilityPollInterval controls the delay between visibility checks.
	indexVisibilityPollInterval = 250 * time.Millisecond
)

// VerifyMappings compares the live mappings behind name with plan and
// returns one error listing every mismatch. Within an index the
// date_detection mismatch comes first, then root fields in sorted order.
// Returning at the first mismatch while ranging over the RootFieldTypes map
// made the reported field depend on map order and forced the operator to fix
// one field per failed run; collecting everything gives a deterministic
// message that can be fixed in one pass.
//
// Every concrete index behind an alias is checked, because alias-mode -flush
// empties every target: verifying only one let a stale sibling be flushed
// (FIX-ALIASPRE-01). A failure lists each failing index between " | "
// separators so the indices stay distinct from the "; " between fields. A
// concrete-index request keeps the plain single-index message. A mapping fetch
// failure is returned as-is, because it is not a mismatch.
//
// expected, when non-empty, is the set of concrete indices the caller will go
// on to mutate (alias -flush empties each one). The response key set must
// equal it, checked before any field check: the caller resolves targets with
// a separate alias read, so trusting only the response entries would leave a
// target the response omitted flushed unverified (FIX-ALIASSET-01). Order and
// duplicates in expected do not matter and the slice is not mutated. Nil or
// empty skips the cross-check (non-alias and post-create callers).
func VerifyMappings(ctx context.Context, es *elasticsearch.Client, index string, plan config.MappingPlan, expected []string) error {
	byIndex, err := fetchIndexMappings(es, index)
	if err != nil {
		return err
	}

	targets := make([]string, 0, len(byIndex))
	for target := range byIndex {
		targets = append(targets, target)
	}
	slices.Sort(targets)

	if len(expected) > 0 {
		want := make(map[string]struct{}, len(expected))
		for _, name := range expected {
			want[name] = struct{}{}
		}
		var missing, unexpected []string
		for name := range want {
			if _, ok := byIndex[name]; !ok {
				missing = append(missing, name)
			}
		}
		for _, name := range targets {
			if _, ok := want[name]; !ok {
				unexpected = append(unexpected, name)
			}
		}
		if len(missing) > 0 || len(unexpected) > 0 {
			slices.Sort(missing)
			var groups []string
			if len(missing) > 0 {
				groups = append(groups, "missing "+strings.Join(missing, ", "))
			}
			if len(unexpected) > 0 {
				groups = append(groups, "unexpected "+strings.Join(unexpected, ", "))
			}
			return fmt.Errorf("alias %q: mappings response does not match the alias targets: %s", index, strings.Join(groups, "; "))
		}
	}

	if len(targets) == 0 {
		return fmt.Errorf("mappings response for index %q was empty", index)
	}
	_, concrete := byIndex[index]
	if concrete && len(targets) > 1 {
		// A concrete-index request returns exactly its own entry; anything more
		// is an unexpected response and must not be checked under the wrong label.
		return fmt.Errorf("mappings response for index %q included other indices: %s", index, strings.Join(targets, ", "))
	}

	var failures []string
	for _, target := range targets {
		if m := mappingMismatches(byIndex[target], plan); len(m) > 0 {
			failures = append(failures, fmt.Sprintf("index %q mappings do not match the mappings file: %s", target, strings.Join(m, "; ")))
			continue
		}

		event := runlog.From(ctx).Info().
			Str("index", target).
			Int("root_field_type_checks", len(plan.RootFieldTypes))
		if !concrete {
			event = event.Str("alias", index)
		}
		if plan.DateDetectionExpected != nil {
			event = event.Bool("date_detection_expected", *plan.DateDetectionExpected)
		}
		event.Msg("Mapping preflight passed")
	}
	if len(failures) == 0 {
		return nil
	}
	if concrete {
		return errors.New(failures[0])
	}
	return fmt.Errorf("alias %q: %s", index, strings.Join(failures, " | "))
}

// mappingMismatches returns one index's mismatches in a fixed order, so every
// target of an alias is judged by the same rules: date_detection first, then
// root fields in sorted order.
func mappingMismatches(mappings map[string]any, plan config.MappingPlan) []string {
	var mismatches []string
	if plan.DateDetectionExpected != nil {
		actualDateDetection, present := extractDateDetection(mappings)
		if !present {
			actualDateDetection = true
		}
		if actualDateDetection != *plan.DateDetectionExpected {
			mismatches = append(mismatches, fmt.Sprintf("mappings.date_detection is %t, expected %t", actualDateDetection, *plan.DateDetectionExpected))
		}
	}

	fieldPaths := make([]string, 0, len(plan.RootFieldTypes))
	for fieldPath := range plan.RootFieldTypes {
		fieldPaths = append(fieldPaths, fieldPath)
	}
	slices.Sort(fieldPaths)
	for _, fieldPath := range fieldPaths {
		expectedType := plan.RootFieldTypes[fieldPath]
		actualType, ok := resolveFieldType(mappings, fieldPath)
		if !ok {
			mismatches = append(mismatches, fmt.Sprintf("required mapping for field %q is missing", fieldPath))
			continue
		}
		if actualType != expectedType {
			mismatches = append(mismatches, fmt.Sprintf("field %q has type %q, expected %q", fieldPath, actualType, expectedType))
		}
	}
	return mismatches
}

// fetchIndexMappings returns the mappings of every index in the _mapping
// response for name, keyed by concrete index name. Elasticsearch keys the
// response by concrete index, so an alias name is never a key; the old
// first-entry fallback checked one arbitrary target of a multi-target alias
// and let the others go unverified (FIX-ALIASPRE-01). Returning every entry
// lets the caller check them all. It owns transport, status, decoding and
// per-entry shape failures (including JSON null); judging the key set (empty,
// concrete-plus-other, expected targets) belongs to VerifyMappings.
func fetchIndexMappings(es *elasticsearch.Client, name string) (map[string]map[string]any, error) {
	res, err := es.Indices.GetMapping(es.Indices.GetMapping.WithIndex(name))
	if err != nil {
		return nil, fmt.Errorf("fetching mappings for index %q: %w", name, err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading mappings response for index %q: %w", name, err)
	}
	if res.IsError() {
		return nil, fmt.Errorf("fetching mappings for index %q failed with status %d: %s", name, res.StatusCode, string(body))
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decoding mappings response for index %q: %w", name, err)
	}
	// JSON null decodes to a nil map and is never a valid response. A decoded
	// "{}" is returned as an empty map: VerifyMappings owns set-shape judgement.
	if payload == nil {
		return nil, fmt.Errorf("mappings response for index %q was empty", name)
	}

	keys := make([]string, 0, len(payload))
	for key := range payload {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	result := make(map[string]map[string]any, len(payload))
	for _, key := range keys {
		entry, ok := payload[key].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("mappings response for index %q did not include an object entry", key)
		}
		mappings, ok := entry["mappings"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("mappings response for index %q did not include a mappings object", key)
		}
		result[key] = mappings
	}
	return result, nil
}

// extractDateDetection centralizes this code path so package behavior stays consistent.
func extractDateDetection(mappings map[string]any) (bool, bool) {
	value, ok := mappings["date_detection"]
	if !ok {
		return false, false
	}

	typed, ok := value.(bool)
	if !ok {
		return false, false
	}
	return typed, true
}

// resolveFieldType centralizes this code path so package behavior stays consistent.
func resolveFieldType(mappings map[string]any, fieldPath string) (string, bool) {
	current, ok := mappings["properties"].(map[string]any)
	if !ok {
		return "", false
	}

	segments := strings.Split(strings.TrimSpace(fieldPath), ".")
	for i, segment := range segments {
		if segment == "" {
			return "", false
		}

		fieldRaw, ok := current[segment]
		if !ok {
			return "", false
		}
		fieldMap, ok := fieldRaw.(map[string]any)
		if !ok {
			return "", false
		}

		if i == len(segments)-1 {
			fieldType, ok := fieldMap["type"].(string)
			if !ok {
				return "", false
			}
			return fieldType, true
		}

		current, ok = fieldMap["properties"].(map[string]any)
		if !ok {
			return "", false
		}
	}

	return "", false
}

// Exists reports whether index exists (HEAD /<index>): 200 is true, 404 is
// false and any other status is an error. The request honors ctx.
func Exists(ctx context.Context, es *elasticsearch.Client, index string) (bool, error) {
	res, err := es.Indices.Exists(
		[]string{index},
		es.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status code %d", res.StatusCode)
	}
}

// WaitForIndex polls Exists until a just-created index is visible, waiting
// between checks with sleep (the caller's seam, so tests never really wait).
func WaitForIndex(ctx context.Context, es *elasticsearch.Client, index string, sleep func(context.Context, time.Duration) error) error {
	for i := 0; i < indexVisibilityMaxAttempts; i++ {
		exists, err := Exists(ctx, es, index)
		if err := runlog.Check(ctx, "waiting for index creation", err); err != nil {
			return err
		}
		if exists {
			return nil
		}
		if err := sleep(ctx, indexVisibilityPollInterval); err != nil {
			return runlog.Fail(runlog.FailureEvent(ctx).Str("index", index), "Context cancelled while waiting for index to become visible", err)
		}
	}

	return runlog.Fail(runlog.FailureEvent(ctx).Str("index", index), "Index create was acknowledged but the index did not become visible", nil)
}

// Delete deletes index and fails on any error response.
func Delete(ctx context.Context, es *elasticsearch.Client, index string) error {
	res, err := es.Indices.Delete([]string{index})
	if err := runlog.Check(ctx, "deleting index", err); err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return runlog.Fail(runlog.FailureEvent(ctx).Str("index", index), "Failed to delete index", nil)
	}
	return nil
}

// Flush deletes every document in index (delete-by-query, conflicts=proceed,
// refresh=true) and fails on an error response or any shard-level failure.
func Flush(ctx context.Context, es *elasticsearch.Client, index string) error {
	query := `{"query":{"match_all":{}}}`
	res, err := es.DeleteByQuery(
		[]string{index},
		strings.NewReader(query),
		es.DeleteByQuery.WithConflicts("proceed"),
		es.DeleteByQuery.WithRefresh(true),
	)
	if err := runlog.Check(ctx, "flushing index", err); err != nil {
		return err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return runlog.Fail(runlog.FailureEvent(ctx).Str("index", index), "Unable to read index flush response body", err)
	}
	if res.IsError() {
		return runlog.Fail(runlog.FailureEvent(ctx).
			Str("index", index).
			Int("status_code", res.StatusCode).
			Str("body", string(body)),
			"Failed to flush index", nil)
	}

	var result struct {
		Failures []json.RawMessage `json:"failures"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return runlog.Fail(runlog.FailureEvent(ctx).Str("index", index), "Unable to parse index flush response body", err)
	}
	if len(result.Failures) > 0 {
		return runlog.Fail(runlog.FailureEvent(ctx).
			Str("index", index).
			Int("shard_failures", len(result.Failures)).
			Str("first_failure", string(result.Failures[0])),
			"Index flush completed with shard-level failures", nil)
	}
	return nil
}

// SelectReplicaCount returns the number_of_replicas for an index this run
// creates and the rule that chose it (ADR 0001). override, when non-nil, wins
// and ClusterReplicaCount is not called (no pointless _nodes round trip); it
// is read-only: the caller owns the pointee.
func SelectReplicaCount(ctx context.Context, es *elasticsearch.Client, override *int) (int, string) {
	if override != nil {
		return *override, "explicit"
	}
	return ClusterReplicaCount(ctx, es), "cluster topology"
}

// ClusterReplicaCount queries the cluster node count and returns the appropriate
// number_of_replicas value for the topology.
// ADR: docs/decisions/0001-replica-count-from-node-count.md
func ClusterReplicaCount(ctx context.Context, es *elasticsearch.Client) int {
	if es == nil {
		return 0
	}
	res, err := es.Nodes.Info()
	if err != nil {
		runlog.From(ctx).Warn().Err(err).Msg("Could not query cluster nodes; defaulting to 0 replicas")
		return 0
	}
	defer res.Body.Close()
	if res.IsError() {
		runlog.From(ctx).Warn().Int("status_code", res.StatusCode).Msg("Cluster nodes query failed; defaulting to 0 replicas")
		return 0
	}

	var body struct {
		Nodes map[string]json.RawMessage `json:"nodes"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		runlog.From(ctx).Warn().Err(err).Msg("Could not parse cluster nodes response; defaulting to 0 replicas")
		return 0
	}

	if len(body.Nodes) > 1 {
		return 1
	}
	return 0
}

// FindIndicesUsingDefaultPipeline lists, sorted, every index whose
// index.default_pipeline is pipeline.
func FindIndicesUsingDefaultPipeline(ctx context.Context, es *elasticsearch.Client, pipeline string) ([]string, error) {
	res, err := es.Indices.GetSettings(es.Indices.GetSettings.WithName("*"), es.Indices.GetSettings.WithContext(ctx))
	if err := runlog.Check(ctx, "getting index settings", err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return nil, runlog.Fail(runlog.FailureEvent(ctx).
			Int("status_code", res.StatusCode).
			Str("body", string(body)),
			"Failed to get index settings", nil)
	}

	var parsed map[string]struct {
		Settings struct {
			Index struct {
				DefaultPipeline string `json:"default_pipeline"`
			} `json:"index"`
		} `json:"settings"`
	}
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return nil, runlog.Fail(runlog.FailureEvent(ctx), "Unable to parse index settings response", err)
	}

	indices := make([]string, 0)
	for index, settings := range parsed {
		if settings.Settings.Index.DefaultPipeline == pipeline {
			indices = append(indices, index)
		}
	}
	slices.Sort(indices)
	return indices, nil
}

// ClearDefaultPipeline sets index.default_pipeline to null on each index so
// nuke teardown can delete the pipeline.
func ClearDefaultPipeline(ctx context.Context, es *elasticsearch.Client, indices []string) error {
	for _, index := range indices {
		res, err := es.Indices.PutSettings(
			strings.NewReader(`{"index.default_pipeline":null}`),
			es.Indices.PutSettings.WithIndex(index),
			es.Indices.PutSettings.WithContext(ctx),
		)
		if err := runlog.Check(ctx, "clearing index.default_pipeline", err); err != nil {
			return err
		}

		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			return runlog.Fail(runlog.FailureEvent(ctx).
				Str("index", index).
				Int("status_code", res.StatusCode).
				Str("body", string(body)),
				"Failed to clear index.default_pipeline", nil)
		}
		res.Body.Close()

		runlog.From(ctx).Warn().Str("index", index).Msg("Cleared index.default_pipeline to allow nuke cleanup")
	}
	return nil
}

// Refresh forces an immediate index refresh so subsequent reads see all
// recently written documents. Called before enrich policy execution and before
// batch transform start to eliminate the ES refresh-window race.
func Refresh(ctx context.Context, es *elasticsearch.Client, index string) error {
	res, err := es.Indices.Refresh(es.Indices.Refresh.WithIndex(index))
	if err := runlog.Check(ctx, "refreshing index", err); err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return runlog.Fail(runlog.FailureEvent(ctx).
			Str("index", index).
			Int("status_code", res.StatusCode).
			Str("body", string(body)),
			"Failed to refresh index", nil)
	}

	runlog.From(ctx).Info().Str("index", index).Msg("Refreshed index")
	return nil
}

// Create creates index with the prepared request body and fails on a
// transport error or an error response. It does not wait for visibility; the
// caller follows it with WaitForIndex.
func Create(ctx context.Context, es *elasticsearch.Client, index, body string) error {
	res, err := es.Indices.Create(index, es.Indices.Create.WithBody(strings.NewReader(body)))
	if err := runlog.Check(ctx, "creating index", err); err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		responseBody, _ := io.ReadAll(res.Body)
		return runlog.Fail(runlog.FailureEvent(ctx).
			Str("index", index).
			Int("status_code", res.StatusCode).
			Str("body", string(responseBody)),
			"Failed to create index", nil)
	}
	return nil
}

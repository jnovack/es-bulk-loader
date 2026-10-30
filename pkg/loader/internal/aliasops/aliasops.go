// Package aliasops owns alias-mode naming and lifecycle: numeric UTC
// <alias>-YYYYMMDDHHMMSS generation names, alias target resolution, the alias
// swap to a new write index and keep-last retention. It uses indexops for
// concrete-index existence and deletion; indexops never imports it.
package aliasops

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/indexops"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

// timestampedIndexCollisionLimit bounds existence checks while searching
// consecutive one-second timestamp suffixes.
const timestampedIndexCollisionLimit = 300

// buildTimestampedIndexName formats now as the numeric <alias>-YYYYMMDDHHMMSS name. Callers pass UTC times, so the name does not depend on the host time zone.
func buildTimestampedIndexName(alias string, now time.Time) string {
	return fmt.Sprintf("%s-%s", alias, now.Format("20060102150405"))
}

// NextAvailableIndexName returns the first <alias>-YYYYMMDDHHMMSS name, from
// base onward in one-second steps, that does not exist yet.
func NextAvailableIndexName(ctx context.Context, es *elasticsearch.Client, alias string, base time.Time) (string, error) {
	name, err := nextAvailableTimestampedIndexNameWithCheck(ctx, alias, base, func(candidate string) (bool, error) {
		return indexops.Exists(ctx, es, candidate)
	})
	if err := runlog.Check(ctx, "finding available timestamped index name", err); err != nil {
		return "", err
	}
	return name, nil
}

// nextAvailableTimestampedIndexNameWithCheck is the testable core of NextAvailableIndexName. existsFn is asked about each candidate, and a taken name advances the candidate by one second, up to timestampedIndexCollisionLimit attempts.
func nextAvailableTimestampedIndexNameWithCheck(ctx context.Context, alias string, base time.Time, existsFn func(candidate string) (bool, error)) (string, error) {
	candidateTime := base.UTC()
	for attempt := 0; attempt < timestampedIndexCollisionLimit; attempt++ {
		candidate := buildTimestampedIndexName(alias, candidateTime)
		exists, err := existsFn(candidate)
		if err != nil {
			return "", err
		}
		if !exists {
			return candidate, nil
		}

		runlog.From(ctx).Warn().
			Str("alias", alias).
			Str("index", candidate).
			Msg("Timestamped index name already exists; advancing by one second")
		candidateTime = candidateTime.Add(time.Second)
	}

	return "", fmt.Errorf("unable to find an available timestamped index name for alias %q", alias)
}

// ResolveTargets returns the concrete indices alias points to, sorted, or nil
// when the alias does not exist.
func ResolveTargets(ctx context.Context, es *elasticsearch.Client, alias string) ([]string, error) {
	res, err := es.Indices.GetAlias(es.Indices.GetAlias.WithName(alias), es.Indices.GetAlias.WithContext(ctx))
	if err := runlog.Check(ctx, "resolving alias targets", err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return nil, runlog.Fail(runlog.FailureEvent(ctx).
			Str("alias", alias).
			Int("status_code", res.StatusCode).
			Str("body", string(body)),
			"Failed to resolve alias targets", nil)
	}

	var parsed map[string]json.RawMessage
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return nil, runlog.Fail(runlog.FailureEvent(ctx).Str("alias", alias), "Unable to parse alias response", err)
	}
	// A JSON null body decodes without error into a nil map. Returning nil, nil
	// would make an unreadable response indistinguishable from a missing alias
	// (404), and the enrich task-404 check would then accept an unreadable
	// before-state as "absent" and report a first execution as verified.
	if parsed == nil {
		return nil, runlog.Fail(runlog.FailureEvent(ctx).Str("alias", alias), "Unable to parse alias response", fmt.Errorf("alias response was null"))
	}

	targets := make([]string, 0, len(parsed))
	for index := range parsed {
		targets = append(targets, index)
	}
	slices.Sort(targets)
	return targets, nil
}

// Update atomically repoints alias from all current targets to index and makes
// index the write index.
func Update(ctx context.Context, es *elasticsearch.Client, alias, index string) error {
	current, err := ResolveTargets(ctx, es, alias)
	if err != nil {
		return err
	}

	actions := make([]map[string]map[string]any, 0, len(current)+1)
	for _, existing := range current {
		actions = append(actions, map[string]map[string]any{
			"remove": {
				"index": existing,
				"alias": alias,
			},
		})
	}
	actions = append(actions, map[string]map[string]any{
		"add": {
			"index":          index,
			"alias":          alias,
			"is_write_index": true,
		},
	})

	payload, err := json.Marshal(map[string]any{"actions": actions})
	if err := runlog.Check(ctx, "serializing alias actions", err); err != nil {
		return err
	}
	res, err := es.Indices.UpdateAliases(strings.NewReader(string(payload)), es.Indices.UpdateAliases.WithContext(ctx))
	if err := runlog.Check(ctx, "updating alias", err); err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return runlog.Fail(runlog.FailureEvent(ctx).
			Str("alias", alias).
			Str("index", index).
			Int("status_code", res.StatusCode).
			Str("body", string(body)),
			"Failed to update alias", nil)
	}

	runlog.From(ctx).Info().Str("alias", alias).Str("index", index).Msg("Alias now points to index")
	return nil
}

// DeleteIndices deletes each index in order and stops at the first failure.
func DeleteIndices(ctx context.Context, es *elasticsearch.Client, indices []string) error {
	for _, index := range indices {
		if err := indexops.Delete(ctx, es, index); err != nil {
			return err
		}
		runlog.From(ctx).Info().Str("index", index).Msg("Index deleted")
	}
	return nil
}

// TimestampedIndex is one generation index of an alias, with its name and the time parsed from its numeric suffix.
type TimestampedIndex struct {
	Name      string
	Timestamp time.Time
}

// ListTimestampedIndices lists the indices matching <alias>-* whose suffix is a
// valid 14-digit timestamp; anything else under the pattern is ignored.
func ListTimestampedIndices(ctx context.Context, es *elasticsearch.Client, alias string) ([]TimestampedIndex, error) {
	pattern := alias + "-*"
	res, err := es.Indices.Get([]string{pattern}, es.Indices.Get.WithContext(ctx))
	if err := runlog.Check(ctx, "listing timestamped indices", err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return nil, runlog.Fail(runlog.FailureEvent(ctx).
			Str("alias", alias).
			Int("status_code", res.StatusCode).
			Str("body", string(body)),
			"Failed to list timestamped indices", nil)
	}

	var parsed map[string]json.RawMessage
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return nil, runlog.Fail(runlog.FailureEvent(ctx).Str("alias", alias), "Unable to parse timestamped index response", err)
	}

	result := make([]TimestampedIndex, 0, len(parsed))
	for index := range parsed {
		timestamp, ok := ParseTimestampedIndexName(alias, index)
		if !ok {
			continue
		}
		result = append(result, TimestampedIndex{Name: index, Timestamp: timestamp})
	}
	return result, nil
}

// ParseTimestampedIndexName returns the time encoded in index when it is
// exactly <alias>- followed by 14 digits.
func ParseTimestampedIndexName(alias, index string) (time.Time, bool) {
	prefix := alias + "-"
	if !strings.HasPrefix(index, prefix) {
		return time.Time{}, false
	}
	suffix := strings.TrimPrefix(index, prefix)
	if len(suffix) != 14 {
		return time.Time{}, false
	}
	for _, ch := range suffix {
		if ch < '0' || ch > '9' {
			return time.Time{}, false
		}
	}

	parsed, err := time.Parse("20060102150405", suffix)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

// Prune deletes all but the newest keepLast generations of alias.
func Prune(ctx context.Context, es *elasticsearch.Client, alias string, keepLast int) error {
	if keepLast <= 0 {
		return nil
	}

	all, err := ListTimestampedIndices(ctx, es, alias)
	if err != nil {
		return err
	}
	if len(all) <= keepLast {
		return nil
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Timestamp.After(all[j].Timestamp)
	})

	toDelete := make([]string, 0, len(all)-keepLast)
	for _, entry := range all[keepLast:] {
		toDelete = append(toDelete, entry.Name)
	}

	runlog.From(ctx).Info().Str("alias", alias).Int("keep_last", keepLast).Strs("indices", toDelete).Msg("Pruning old timestamped indices")
	return DeleteIndices(ctx, es, toDelete)
}

// Package managed owns the managed resources of a Run: content-hashed enrich
// policy names and the pipeline rewrites that reference them, policy and
// pipeline upserts, unreferenced-policy garbage collection, and teardown
// (including -nuke recovery). It uses enrich listing, indexops default-pipeline
// settings and transform deletion; none of those packages imports managed.
package managed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/enrich"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/indexops"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/transform"
)

// PolicyPlan maps each declared logical policy to its content-hashed managed
// name (<logical>-<sha256[:6]>) and holds the definitions under those names.
// Names are content-hashed because an enrich policy cannot be updated in place:
// a changed definition gets a new name, identical content reuses its name, and
// dependent pipelines are rewritten to it. Older versions stay recognisable as
// <logical>-<hash>, so garbage collection can delete them once no pipeline
// references them.
type PolicyPlan struct {
	LogicalNames     []string
	DesiredNames     []string
	Definitions      config.Definitions
	LogicalToDesired map[string]string
	DesiredSet       map[string]struct{}
}

// BuildPolicyPlan hashes each declared policy definition into its managed name.
func BuildPolicyPlan(ctx context.Context, definitions config.Definitions, logicalNames []string) (PolicyPlan, error) {
	plan := PolicyPlan{
		LogicalNames:     append([]string(nil), logicalNames...),
		DesiredNames:     make([]string, 0, len(logicalNames)),
		Definitions:      make(config.Definitions, len(logicalNames)),
		LogicalToDesired: make(map[string]string, len(logicalNames)),
		DesiredSet:       make(map[string]struct{}, len(logicalNames)),
	}
	for _, logicalName := range logicalNames {
		definition, ok := definitions[logicalName]
		if !ok {
			return PolicyPlan{}, runlog.Fail(runlog.FailureEvent(ctx).Str("policy", logicalName), "Policy definition missing from parsed policy file", nil)
		}

		desiredName, err := hashedPolicyName(ctx, logicalName, definition)
		if err != nil {
			return PolicyPlan{}, err
		}
		plan.DesiredNames = append(plan.DesiredNames, desiredName)
		plan.Definitions[desiredName] = definition
		plan.LogicalToDesired[logicalName] = desiredName
		plan.DesiredSet[desiredName] = struct{}{}

		runlog.From(ctx).Debug().
			Str("logical_policy", logicalName).
			Str("policy", desiredName).
			Msg("Resolved managed policy name from policy definition hash")
	}
	return plan, nil
}

// hashedPolicyName returns <logical>-<first 6 hex of sha256(canonical JSON)>.
func hashedPolicyName(ctx context.Context, logicalName string, definition json.RawMessage) (string, error) {
	canonical, err := canonicalizeRawJSON(definition)
	if err != nil {
		return "", runlog.Fail(runlog.FailureEvent(ctx).Str("policy", logicalName), "Failed to canonicalize policy definition for hashing", err)
	}

	sum := sha256.Sum256(canonical)
	suffix := hex.EncodeToString(sum[:])[:6]
	return logicalName + "-" + suffix, nil
}

// canonicalizeRawJSON re-encodes raw so formatting and key order do not change
// the hash.
func canonicalizeRawJSON(raw json.RawMessage) ([]byte, error) {
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	return json.Marshal(parsed)
}

// RewritePipelinePolicyReferences rewrites enrich processors' policy_name from
// logical names to the mapped managed names.
func RewritePipelinePolicyReferences(ctx context.Context, definitions config.Definitions, names []string, logicalToDesired map[string]string) (config.Definitions, error) {
	if len(definitions) == 0 || len(logicalToDesired) == 0 {
		return definitions, nil
	}

	rewritten := make(config.Definitions, len(definitions))
	for _, name := range names {
		definition := definitions[name]
		var parsed any
		if err := json.Unmarshal(definition, &parsed); err != nil {
			return nil, runlog.Fail(runlog.FailureEvent(ctx).Str("pipeline", name), "Unable to parse pipeline definition for policy rewrite", err)
		}

		replacements := rewriteEnrichPolicyNameReferences(parsed, logicalToDesired)
		encoded, err := json.Marshal(parsed)
		if err != nil {
			return nil, runlog.Fail(runlog.FailureEvent(ctx).Str("pipeline", name), "Unable to serialize rewritten pipeline definition", err)
		}
		rewritten[name] = encoded

		if replacements > 0 {
			runlog.From(ctx).Info().
				Str("pipeline", name).
				Int("replacements", replacements).
				Msg("Rewrote enrich policy references in pipeline to managed policy names")
		} else {
			runlog.From(ctx).Trace().Str("pipeline", name).Msg("No enrich policy reference rewrites needed for pipeline")
		}
	}

	// Preserve any map entries not present in the ordered names list.
	for name, definition := range definitions {
		if _, ok := rewritten[name]; ok {
			continue
		}
		rewritten[name] = definition
	}
	return rewritten, nil
}

// ResolvePipelinePolicyFallbacks maps logical enrich policy references that the
// run supplied no definition for onto existing cluster policies. Pre-mapped
// and exact names win; a single managed generation is adopted; none warns and
// skips. Several managed generations fail the run: the suffix is a content
// hash with no recency, so picking one silently binds a pipeline to a stale
// definition (moxfall: a field fed by it was populated on 0 of 109,373
// documents, with no error). Never guess between generations.
func ResolvePipelinePolicyFallbacks(ctx context.Context, es *elasticsearch.Client, definitions config.Definitions, names []string, policyNameMapping map[string]string) error {
	referenced, err := collectReferencedPolicyNamesFromPipelines(ctx, definitions, names)
	if err != nil {
		return err
	}
	if len(referenced) == 0 {
		return nil
	}

	available, supported, err := enrich.ListPolicies(ctx, es)
	if err != nil {
		return err
	}
	if !supported {
		return nil
	}
	availableSet := make(map[string]struct{}, len(available))
	for _, name := range available {
		availableSet[name] = struct{}{}
	}

	for _, policy := range referenced {
		if _, mapped := policyNameMapping[policy]; mapped {
			continue
		}
		if _, exact := availableSet[policy]; exact {
			policyNameMapping[policy] = policy
			runlog.From(ctx).Debug().Str("policy", policy).Msg("Using existing exact enrich policy reference from pipeline definition")
			continue
		}

		matches := make([]string, 0)
		for _, candidate := range available {
			if nameMatchesLogical(policy, candidate) {
				matches = append(matches, candidate)
			}
		}
		if len(matches) == 0 {
			runlog.From(ctx).Warn().Str("logical_policy", policy).Msg("Pipeline references enrich policy that does not exist as exact or managed policy name")
			continue
		}

		if len(matches) > 1 {
			slices.Sort(matches)
			// The candidates go in the wrapped error, not the message:
			// classifyRunErrorKind reads the message, and a real policy name
			// containing "enrich" or "bulk" would reclassify this error away
			// from ErrManagedResource. Do not assign policyNameMapping here.
			ambiguous := fmt.Errorf("logical policy %q matches managed generations %s and this run supplied no definition for it; supply its definition or delete the stale generations",
				policy, strings.Join(matches, ", "))
			return runlog.Fail(runlog.FailureEvent(ctx).
				Str("logical_policy", policy).Strs("candidates", matches),
				"Ambiguous policy reference", ambiguous)
		}

		chosen := matches[0]
		policyNameMapping[policy] = chosen
		runlog.From(ctx).Info().Str("logical_policy", policy).Str("policy", chosen).Msg("Resolved logical enrich policy reference to managed policy name")
	}
	return nil
}

// rewriteEnrichPolicyNameReferences rewrites every mapped enrich.policy_name in
// value and returns how many it changed.
func rewriteEnrichPolicyNameReferences(value any, logicalToDesired map[string]string) int {
	replacements := 0
	switch typed := value.(type) {
	case map[string]any:
		if enrich, ok := typed["enrich"].(map[string]any); ok {
			if policyName, ok := enrich["policy_name"].(string); ok {
				if desired, mapped := logicalToDesired[policyName]; mapped && desired != policyName {
					enrich["policy_name"] = desired
					replacements++
				}
			}
		}
		for _, nested := range typed {
			replacements += rewriteEnrichPolicyNameReferences(nested, logicalToDesired)
		}
	case []any:
		for _, nested := range typed {
			replacements += rewriteEnrichPolicyNameReferences(nested, logicalToDesired)
		}
	}
	return replacements
}

// collectReferencedPolicyNamesFromPipelines returns, sorted, every policy name
// the pipelines' enrich processors reference.
func collectReferencedPolicyNamesFromPipelines(ctx context.Context, definitions config.Definitions, names []string) ([]string, error) {
	seen := make(map[string]struct{})
	for _, name := range names {
		var parsed any
		if err := json.Unmarshal(definitions[name], &parsed); err != nil {
			return nil, runlog.Fail(runlog.FailureEvent(ctx).Str("pipeline", name), "Unable to parse pipeline definition while collecting policy references", err)
		}
		collectPolicyNamesInValue(parsed, seen)
	}

	referenced := make([]string, 0, len(seen))
	for policy := range seen {
		referenced = append(referenced, policy)
	}
	slices.Sort(referenced)
	return referenced, nil
}

// collectPolicyNamesInValue adds every enrich.policy_name in value to seen.
func collectPolicyNamesInValue(value any, seen map[string]struct{}) {
	switch typed := value.(type) {
	case map[string]any:
		if enrich, ok := typed["enrich"].(map[string]any); ok {
			if policyName, ok := enrich["policy_name"].(string); ok && policyName != "" {
				seen[policyName] = struct{}{}
			}
		}
		for _, nested := range typed {
			collectPolicyNamesInValue(nested, seen)
		}
	case []any:
		for _, nested := range typed {
			collectPolicyNamesInValue(nested, seen)
		}
	}
}

// ResolvePolicyDeleteNames returns, sorted, the existing policies that are a
// declared logical name or one of its managed generations.
func ResolvePolicyDeleteNames(ctx context.Context, es *elasticsearch.Client, logicalNames []string) ([]string, error) {
	if len(logicalNames) == 0 {
		return nil, nil
	}

	available, supported, err := enrich.ListPolicies(ctx, es)
	if err != nil {
		return nil, err
	}
	if !supported {
		return nil, nil
	}

	deleteSet := make(map[string]struct{})
	for _, name := range available {
		for _, logical := range logicalNames {
			if name == logical || nameMatchesLogical(logical, name) {
				deleteSet[name] = struct{}{}
				break
			}
		}
	}

	deleteNames := make([]string, 0, len(deleteSet))
	for name := range deleteSet {
		deleteNames = append(deleteNames, name)
	}
	slices.Sort(deleteNames)
	return deleteNames, nil
}

// nameMatchesLogical reports whether policyName is <logicalName>-<6 hex>.
func nameMatchesLogical(logicalName, policyName string) bool {
	prefix := logicalName + "-"
	if !strings.HasPrefix(policyName, prefix) {
		return false
	}

	suffix := strings.TrimPrefix(policyName, prefix)
	if len(suffix) != 6 {
		return false
	}
	for _, ch := range suffix {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

// RemapEnrichSelection maps explicit logical policy names in selection to their
// managed names. It returns selection itself (including nil) when nothing
// changes, otherwise a remapped copy; the caller's value is never modified.
func RemapEnrichSelection(ctx context.Context, selection *enrich.Selection, logicalToDesired map[string]string) *enrich.Selection {
	if selection == nil || !selection.Enabled || selection.All || len(logicalToDesired) == 0 {
		return selection
	}

	explicit := selection.ExplicitPolicies()
	if len(explicit) == 0 {
		return selection
	}

	remapped := make([]string, 0, len(explicit))
	changed := false
	for _, policy := range explicit {
		if desired, ok := logicalToDesired[policy]; ok {
			remapped = append(remapped, desired)
			changed = true
			continue
		}
		remapped = append(remapped, policy)
	}
	if !changed {
		return selection
	}

	cloned := *selection
	cloned.Raw = strings.Join(remapped, ",")
	cloned.All = false
	cloned.Enabled = true
	runlog.From(ctx).Debug().Str("requested", selection.Raw).Str("resolved", cloned.Raw).Msg("Mapped explicit enrich policy names to managed policy names")
	return &cloned
}

// DescribeDeclared renders the resources a sync would have applied, for
// operator-facing errors: "pipelines [a, b]; no policies; transforms [t]", or
// one sentence when all three are empty. Input order is kept and the slices are
// not modified. Names only: no bodies or other configuration.
func DescribeDeclared(pipelines, policies, transforms []string) string {
	if len(pipelines) == 0 && len(policies) == 0 && len(transforms) == 0 {
		return "no pipelines, policies or transforms are declared for this run"
	}
	describe := func(kind string, names []string) string {
		if len(names) == 0 {
			return "no " + kind
		}
		return kind + " [" + strings.Join(names, ", ") + "]"
	}
	return strings.Join([]string{
		describe("pipelines", pipelines),
		describe("policies", policies),
		describe("transforms", transforms),
	}, "; ")
}

// GarbageCollectPolicies deletes, best effort, managed generations of the
// declared policies that are not desired and no pipeline references.
func GarbageCollectPolicies(ctx context.Context, es *elasticsearch.Client, logicalNames []string, desiredSet map[string]struct{}) error {
	if len(logicalNames) == 0 {
		return nil
	}

	available, supported, err := enrich.ListPolicies(ctx, es)
	if err != nil {
		return err
	}
	if !supported {
		return nil
	}

	for _, policy := range available {
		if _, keep := desiredSet[policy]; keep {
			continue
		}

		managed := false
		for _, logical := range logicalNames {
			if nameMatchesLogical(logical, policy) {
				managed = true
				break
			}
		}
		if !managed {
			continue
		}

		referencing, err := findPipelinesReferencingPolicy(ctx, es, policy)
		if err != nil {
			return err
		}
		if len(referencing) > 0 {
			runlog.From(ctx).Debug().
				Str("policy", policy).
				Strs("pipelines", referencing).
				Msg("Skipping managed policy GC because policy is still referenced by pipelines")
			continue
		}

		deletePolicyBestEffort(ctx, es, policy)
	}
	return nil
}

// deletePolicyBestEffort deletes policy, logging rather than failing on error.
func deletePolicyBestEffort(ctx context.Context, es *elasticsearch.Client, policy string) {
	res, err := es.EnrichDeletePolicy(
		policy,
		es.EnrichDeletePolicy.WithContext(ctx),
		es.EnrichDeletePolicy.WithHeader(map[string]string{
			"Accept": "application/json",
		}),
	)
	if err != nil {
		runlog.From(ctx).Warn().Err(err).Str("policy", policy).Msg("Managed policy GC failed while deleting policy")
		return
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		runlog.From(ctx).Trace().Str("policy", policy).Msg("Managed policy GC found policy already deleted")
		return
	}
	if res.IsError() {
		body, readErr := esresponse.ReadBounded(res.Body)
		runlog.From(ctx).Warn().
			Str("policy", policy).
			Int("status_code", res.StatusCode).
			Str("es_error", esresponse.NewRejection(
				"enrich policy", policy, res.StatusCode, body, readErr,
				esresponse.MaxPublicMessageBytes).Error()).
			Msg("Managed policy GC could not delete policy")
		return
	}

	runlog.From(ctx).Info().Str("policy", policy).Msg("Managed policy GC deleted unreferenced policy")
}

// CreatePipelines puts each pipeline in declared order and stops at the first
// failure. A transport failure returns Op "creating pipeline" with its cause.
// An HTTP rejection returns Op "Failed to create pipeline" whose cause is an
// *esresponse.Rejection: the pipeline, HTTP status and bounded, redacted
// Elasticsearch type/reason pairs (or a fixed explanation), never the raw body.
func CreatePipelines(ctx context.Context, es *elasticsearch.Client, definitions config.Definitions, names []string) error {
	for _, name := range names {
		res, err := es.Ingest.PutPipeline(
			name,
			strings.NewReader(string(definitions[name])),
			es.Ingest.PutPipeline.WithContext(ctx),
		)
		if err := runlog.Check(ctx, "creating pipeline", err); err != nil {
			return err
		}

		if res.IsError() {
			// The rejection reason is the only actionable part of this failure,
			// and it used to be dropped: a nil cause rendered as
			// "Failed to create pipeline: Failed to create pipeline", and the raw
			// body went only to the log, unbounded and unredacted. The cause now
			// carries bounded, redacted ES type/reason pairs, and the log reuses
			// it through runlog.Fail's error field. The Op stays fixed so the
			// RunError Kind (ErrManagedResource) does not change.
			data, readErr := esresponse.ReadBounded(res.Body)
			res.Body.Close()
			cause := esresponse.NewRejection(
				"pipeline", name, res.StatusCode, data, readErr,
				esresponse.MaxPublicMessageBytes,
			)
			return runlog.Fail(runlog.FailureEvent(ctx).
				Str("pipeline", cause.ResourceName()).
				Int("status_code", res.StatusCode),
				"Failed to create pipeline", cause)
		}
		res.Body.Close()

		runlog.From(ctx).Info().Str("pipeline", name).Msg("Pipeline created or updated")
	}
	return nil
}

// deletePipelines deletes each pipeline; a missing one is not an error.
func deletePipelines(ctx context.Context, es *elasticsearch.Client, names []string) error {
	for _, name := range names {
		res, err := es.Ingest.DeletePipeline(
			name,
			es.Ingest.DeletePipeline.WithContext(ctx),
		)
		if err := runlog.Check(ctx, "deleting pipeline", err); err != nil {
			return err
		}

		if res.StatusCode == http.StatusNotFound {
			res.Body.Close()
			runlog.From(ctx).Info().Str("pipeline", name).Msg("Pipeline does not exist. Nothing to delete.")
			continue
		}
		if res.IsError() {
			body, readErr := esresponse.ReadBounded(res.Body)
			res.Body.Close()
			return runlog.Fail(runlog.FailureEvent(ctx).
				Str("pipeline", name).
				Int("status_code", res.StatusCode),
				"Failed to delete pipeline", esresponse.NewRejection(
					"pipeline", name, res.StatusCode, body, readErr,
					esresponse.MaxPublicMessageBytes))
		}
		res.Body.Close()

		runlog.From(ctx).Info().Str("pipeline", name).Msg("Pipeline deleted")
	}
	return nil
}

// CreatePolicies puts each policy, retrying while its source index is not yet
// visible (waiting with sleep, the caller's seam). An existing policy with the
// same hashed name is already up to date.
func CreatePolicies(ctx context.Context, es *elasticsearch.Client, definitions config.Definitions, names []string, sleep func(context.Context, time.Duration) error) error {
	for _, name := range names {
		for attempt := 1; attempt <= 5; attempt++ {
			res, err := putPolicy(ctx, es, name, definitions[name])
			if err := runlog.Check(ctx, "creating enrich policy", err); err != nil {
				return err
			}

			if !res.IsError() {
				res.Body.Close()
				runlog.From(ctx).Info().Str("policy", name).Msg("Created enrich policy")
				break
			}

			body, readErr := esresponse.ReadBounded(res.Body)
			res.Body.Close()

			if esresponse.IsUnsupportedEnrichAPI(res.StatusCode, body) {
				runlog.From(ctx).Warn().
					Int("status_code", res.StatusCode).
					Str("es_error", esresponse.NewRejection(
						"enrich API", "", res.StatusCode, body, readErr,
						esresponse.MaxPublicMessageBytes).Error()).
					Msg("Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools")
				return nil
			}
			if esresponse.HasErrorType(body, "index_not_found_exception") && attempt < 5 {
				runlog.From(ctx).Warn().
					Str("policy", name).
					Int("attempt", attempt).
					Int("status_code", res.StatusCode).
					Str("es_error", esresponse.NewRejection(
						"enrich policy", name, res.StatusCode, body, readErr,
						esresponse.MaxPublicMessageBytes).Error()).
					Msg("Source index for enrich policy is not visible yet; retrying enrich policy creation")
				if err := sleep(ctx, 500*time.Millisecond); err != nil {
					return runlog.Fail(runlog.FailureEvent(ctx).Str("policy", name), "Context cancelled during enrich policy retry", err)
				}
				continue
			}
			if esresponse.HasErrorType(body, "resource_already_exists_exception") {
				runlog.From(ctx).Info().
					Str("policy", name).
					Msg("Managed enrich policy already exists for this definition hash")
				break
			}

			return runlog.Fail(runlog.FailureEvent(ctx).
				Str("policy", name).
				Int("status_code", res.StatusCode),
				"Failed to create enrich policy", esresponse.NewRejection(
					"enrich policy", name, res.StatusCode, body, readErr,
					esresponse.MaxPublicMessageBytes))
		}
	}
	return nil
}

// putPolicy sends one PUT _enrich/policy/<name> with the caller's context.
func putPolicy(ctx context.Context, es *elasticsearch.Client, name string, definition json.RawMessage) (*esapi.Response, error) {
	return es.EnrichPutPolicy(
		name,
		strings.NewReader(string(definition)),
		es.EnrichPutPolicy.WithContext(ctx),
		es.EnrichPutPolicy.WithHeader(map[string]string{
			"Content-Type": "application/json",
			"Accept":       "application/json",
		}),
	)
}

// deletePolicies deletes each policy. In nuke mode a delete blocked by a
// referencing pipeline deletes those pipelines and retries once.
func deletePolicies(ctx context.Context, es *elasticsearch.Client, names []string, nuke bool) error {
	for _, name := range names {
		for attempt := 1; attempt <= 2; attempt++ {
			res, err := es.EnrichDeletePolicy(
				name,
				es.EnrichDeletePolicy.WithContext(ctx),
				es.EnrichDeletePolicy.WithHeader(map[string]string{
					"Accept": "application/json",
				}),
			)
			if err := runlog.Check(ctx, "deleting enrich policy", err); err != nil {
				return err
			}

			if res.StatusCode == http.StatusNotFound {
				body, readErr := esresponse.ReadBounded(res.Body)
				res.Body.Close()
				if esresponse.Oversized(body) {
					// A body past the read bound cannot be classified, and the
					// classifier below would read "no ES error object" as an
					// absent policy. An unclassifiable 404 must not be reported
					// as success, so fail before the generic-404 and
					// missing-policy split.
					return runlog.Fail(runlog.FailureEvent(ctx).
						Str("policy", name).
						Int("status_code", res.StatusCode),
						"Failed to delete enrich policy", esresponse.NewRejection(
							"enrich policy", name, res.StatusCode, body, readErr,
							esresponse.MaxPublicMessageBytes))
				}
				if esresponse.IsUnsupportedEnrichAPI(res.StatusCode, body) {
					runlog.From(ctx).Warn().
						Int("status_code", res.StatusCode).
						Str("es_error", esresponse.NewRejection(
							"enrich API", "", res.StatusCode, body, readErr,
							esresponse.MaxPublicMessageBytes).Error()).
						Msg("Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools")
					return nil
				}
				runlog.From(ctx).Debug().Str("policy", name).Msg("Enrich policy does not exist. Nothing to delete.")
				break
			}
			if res.IsError() {
				body, readErr := esresponse.ReadBounded(res.Body)
				res.Body.Close()
				if res.StatusCode == http.StatusConflict && nuke && policyDeleteBlockedByPipelineReference(body) && attempt == 1 {
					referencing, err := findPipelinesReferencingPolicy(ctx, es, name)
					if err != nil {
						return err
					}
					if len(referencing) == 0 {
						return runlog.Fail(runlog.FailureEvent(ctx).
							Str("policy", name).
							Int("status_code", res.StatusCode),
							"Failed to delete enrich policy; nuke mode could not find referencing pipelines", esresponse.NewRejection(
								"enrich policy", name, res.StatusCode, body, readErr,
								esresponse.MaxPublicMessageBytes))
					}

					runlog.From(ctx).Warn().
						Str("policy", name).
						Strs("pipelines", referencing).
						Msg("Nuke mode deleting pipelines that reference this enrich policy before retrying policy deletion")
					if err := deletePipelinesForNuke(ctx, es, referencing); err != nil {
						return err
					}
					continue
				}

				return runlog.Fail(runlog.FailureEvent(ctx).
					Str("policy", name).
					Int("status_code", res.StatusCode),
					"Failed to delete enrich policy", esresponse.NewRejection(
						"enrich policy", name, res.StatusCode, body, readErr,
						esresponse.MaxPublicMessageBytes))
			}
			res.Body.Close()

			runlog.From(ctx).Info().Str("policy", name).Msg("Deleted enrich policy")
			break
		}
	}
	return nil
}

// DeleteResources deletes pipelines, then policies, then transforms, stopping
// at the first failure. nuke enables the cross-resource recovery paths.
func DeleteResources(ctx context.Context, es *elasticsearch.Client, pipelineNames []string, policyNames []string, transformNames []string, nuke bool) error {
	if len(pipelineNames) > 0 {
		if nuke {
			if err := deletePipelinesForNuke(ctx, es, pipelineNames); err != nil {
				return err
			}
		} else {
			if err := deletePipelines(ctx, es, pipelineNames); err != nil {
				return err
			}
		}
	}
	if len(policyNames) > 0 {
		if err := deletePolicies(ctx, es, policyNames, nuke); err != nil {
			return err
		}
	}
	if len(transformNames) > 0 {
		if err := transform.Delete(ctx, es, transformNames); err != nil {
			return err
		}
	}
	return nil
}

// deletePipelinesForNuke deletes each pipeline; one blocked as an index's
// default pipeline has that setting cleared and is deleted again.
func deletePipelinesForNuke(ctx context.Context, es *elasticsearch.Client, names []string) error {
	for _, name := range names {
		res, err := es.Ingest.DeletePipeline(
			name,
			es.Ingest.DeletePipeline.WithContext(ctx),
		)
		if err := runlog.Check(ctx, "deleting pipeline", err); err != nil {
			return err
		}

		if res.StatusCode == http.StatusNotFound {
			res.Body.Close()
			runlog.From(ctx).Info().Str("pipeline", name).Msg("Pipeline does not exist. Nothing to delete.")
			continue
		}
		if res.IsError() {
			body, readErr := esresponse.ReadBounded(res.Body)
			res.Body.Close()
			if res.StatusCode == http.StatusBadRequest && pipelineDeleteBlockedByDefaultIndex(body) {
				indices, err := indexops.FindIndicesUsingDefaultPipeline(ctx, es, name)
				if err != nil {
					return err
				}
				if len(indices) == 0 {
					return runlog.Fail(runlog.FailureEvent(ctx).
						Str("pipeline", name).
						Int("status_code", res.StatusCode),
						"Failed to delete pipeline; nuke mode could not find indices using it as default pipeline", esresponse.NewRejection(
							"pipeline", name, res.StatusCode, body, readErr,
							esresponse.MaxPublicMessageBytes))
				}

				runlog.From(ctx).Warn().
					Str("pipeline", name).
					Strs("indices", indices).
					Msg("Nuke mode clearing index.default_pipeline on indices that use this pipeline before retrying pipeline deletion")
				if err := indexops.ClearDefaultPipeline(ctx, es, indices); err != nil {
					return err
				}
				if err := deletePipelines(ctx, es, []string{name}); err != nil {
					return err
				}
				continue
			}
			return runlog.Fail(runlog.FailureEvent(ctx).
				Str("pipeline", name).
				Int("status_code", res.StatusCode),
				"Failed to delete pipeline", esresponse.NewRejection(
					"pipeline", name, res.StatusCode, body, readErr,
					esresponse.MaxPublicMessageBytes))
		}
		res.Body.Close()

		runlog.From(ctx).Info().Str("pipeline", name).Msg("Pipeline deleted")
	}
	return nil
}

// findPipelinesReferencingPolicy lists, sorted, the pipelines referencing policy.
func findPipelinesReferencingPolicy(ctx context.Context, es *elasticsearch.Client, policy string) ([]string, error) {
	res, err := es.Ingest.GetPipeline(
		es.Ingest.GetPipeline.WithContext(ctx),
	)
	if err := runlog.Check(ctx, "getting ingest pipelines", err); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if res.IsError() {
		body, readErr := esresponse.ReadBounded(res.Body)
		return nil, runlog.Fail(runlog.FailureEvent(ctx).
			Int("status_code", res.StatusCode),
			"Failed to get ingest pipelines", esresponse.NewRejection(
				"ingest pipeline list request", "", res.StatusCode, body, readErr,
				esresponse.MaxPublicMessageBytes))
	}

	var definitions config.Definitions
	if err := json.NewDecoder(res.Body).Decode(&definitions); err != nil {
		return nil, runlog.Fail(runlog.FailureEvent(ctx), "Unable to parse ingest pipeline response", err)
	}

	return pipelineNamesReferencingPolicy(definitions, policy), nil
}

// pipelineNamesReferencingPolicy filters definitions to those referencing policy.
func pipelineNamesReferencingPolicy(definitions config.Definitions, policy string) []string {
	names := make([]string, 0)
	for name, definition := range definitions {
		if pipelineDefinitionReferencesPolicy(definition, policy) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// pipelineDefinitionReferencesPolicy reports whether one definition references
// policy; unparseable JSON references nothing.
func pipelineDefinitionReferencesPolicy(definition json.RawMessage, policy string) bool {
	var parsed any
	if err := json.Unmarshal(definition, &parsed); err != nil {
		return false
	}
	return valueReferencesPolicy(parsed, policy)
}

// valueReferencesPolicy reports whether any enrich.policy_name in value is policy.
func valueReferencesPolicy(value any, policy string) bool {
	switch typed := value.(type) {
	case map[string]any:
		if enrich, ok := typed["enrich"].(map[string]any); ok {
			if policyName, ok := enrich["policy_name"].(string); ok && policyName == policy {
				return true
			}
		}
		for _, nested := range typed {
			if valueReferencesPolicy(nested, policy) {
				return true
			}
		}
	case []any:
		for _, nested := range typed {
			if valueReferencesPolicy(nested, policy) {
				return true
			}
		}
	}
	return false
}

// policyDeleteBlockedByPipelineReference reports a policy delete refused because
// a pipeline references the policy.
func policyDeleteBlockedByPipelineReference(body []byte) bool {
	return !esresponse.Oversized(body) && bytes.Contains(body, []byte("pipeline is referencing it"))
}

// pipelineDeleteBlockedByDefaultIndex reports a pipeline delete refused because
// an index uses it as index.default_pipeline.
func pipelineDeleteBlockedByDefaultIndex(body []byte) bool {
	return !esresponse.Oversized(body) && bytes.Contains(body, []byte("cannot be deleted because it is the default pipeline"))
}

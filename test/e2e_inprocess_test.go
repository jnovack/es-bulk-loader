//go:build e2e

package e2e

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/pkg/loader"
)

// This file holds the one scenario that calls loader.Run in-process instead of
// driving the CLI binary. It is the only file in the package that imports
// pkg/loader and zerolog. It asserts the typed observability contract
// (Result.BulkFailures, Options.Lifecycle, Result.EnrichOutcomes and
// ElasticsearchError) against a real Elasticsearch.

// wantBulkRejectionType is the error type ES 9.3.1 reports for an object sent
// to a mapped keyword field, observed against the pinned container image
// (docker.elastic.co/elasticsearch/elasticsearch:9.3.1) during Phase 7. A
// different Elasticsearch version may name it differently; update it with the
// observed value, never loosen the assertion to "any non-empty Type".
const wantBulkRejectionType = "document_parsing_exception"

// inProcessMixedDataJSON holds two valid documents and one whose source_name,
// a mapped keyword, is an object, which Elasticsearch rejects at item level.
const inProcessMixedDataJSON = `[
  { "lookup_id": "A1", "base_value": 7, "multiplier": 3, "source_name": "alpha" },
  { "lookup_id": "BAD1", "base_value": 5, "multiplier": 4, "source_name": { "nested": "object" } },
  { "lookup_id": "B2", "base_value": 5, "multiplier": 4, "source_name": "beta" }
]`

// inProcessInvalidPipelinesJSON is a pipeline whose only processor has a type
// Elasticsearch does not know, so the PUT is rejected with HTTP 400. Argument:
// the pipeline name.
const inProcessInvalidPipelinesJSON = `{
  %[1]q: {
    "description": "Pipeline with an unknown processor",
    "processors": [ { "no_such_processor_type": {} } ]
  }
}`

// scenarioInProcessObservability runs three sequential probes through
// loader.Run: a bulk rejection sample, two alias rollovers with the full
// lifecycle sequence, and a rejected pipeline PUT.
func scenarioInProcessObservability(t *testing.T, ctx *scenarioContext) {
	t.Run("bulk_rejection_sample", func(t *testing.T) {
		dir := writeSourceVariant(t, ctx, "source-bulk-mixed", map[string]string{"data.json": inProcessMixedDataJSON})
		var events []loader.LifecycleEvent
		res, err := runInProcess(t, ctx, loader.Options{
			Index:        ctx.targetIndex,
			SettingsFile: filepath.Join(dir, "settings.json"),
			MappingsFile: filepath.Join(dir, "mappings.json"),
			DataFile:     filepath.Join(dir, "data.json"),
			IDField:      "lookup_id",
			DeleteIndex:  true,
		}, &events)
		fail := func(format string, args ...any) {
			t.Helper()
			probeFail(t, fmt.Sprintf(format, args...), res, events, err)
		}
		if err != nil {
			fail("Run returned an error")
		}
		// The fixture is exactly 2 valid documents and 1 invalid one.
		if res.DocumentsSucceeded != 2 || res.DocumentsFailed != 1 {
			fail("expected 2 succeeded and 1 failed documents")
		}
		if got := len(res.BulkFailures) + res.BulkFailuresOmitted; got != res.DocumentsFailed {
			fail("len(BulkFailures)+Omitted = %d, DocumentsFailed = %d", got, res.DocumentsFailed)
		}
		if len(res.BulkFailures) != 1 || res.BulkFailuresOmitted != 0 {
			fail("expected exactly one sampled failure and none omitted")
		}
		bad := res.BulkFailures[0]
		if bad.ID != "BAD1" {
			fail("rejected item BAD1 missing from BulkFailures, got ID %q", bad.ID)
		}
		if bad.Status != 400 || bad.Index != ctx.targetIndex || bad.Action != "index" || bad.Type != wantBulkRejectionType {
			fail("unexpected rejected item %+v, want status 400 index %s action index type %s", bad, ctx.targetIndex, wantBulkRejectionType)
		}
		if bad.Reason == "" {
			fail("expected a non-empty Reason, got %+v", bad)
		}
		withProbeContext(t, res, events, err, func() { assertDocCount(t, ctx, ctx.targetIndex, 2) })
	})

	rolloverOK := t.Run("alias_rollover_lifecycle", func(t *testing.T) {
		newOpts := func() loader.Options {
			return loader.Options{
				Index:         ctx.sourceIndex,
				SettingsFile:  filepath.Join(ctx.sourceBaseDir, "settings.json"),
				MappingsFile:  filepath.Join(ctx.sourceBaseDir, "mappings.json"),
				PipelinesFile: filepath.Join(ctx.sourceBaseDir, "pipelines.json"),
				PoliciesFile:  filepath.Join(ctx.sourceBaseDir, "policies.json"),
				DataFile:      filepath.Join(ctx.sourceBaseDir, "data.json"),
				IDField:       "lookup_id",
				DeleteIndex:   true,
				SyncManaged:   true,
				AliasMode:     true,
				KeepLast:      1,
				Enrich:        loader.EnrichOptions{Enabled: true, All: true},
			}
		}
		// prefix returns the events common to both runs, in contract order.
		prefix := func(created string) []loader.LifecycleEvent {
			return []loader.LifecycleEvent{
				{Action: loader.LifecycleActionResolved, ResourceKind: loader.ResourceKindPipeline, Name: ctx.sourcePipelinePrimary},
				{Action: loader.LifecycleActionResolved, ResourceKind: loader.ResourceKindPipeline, Name: ctx.sourcePipelineSecondary},
				{Action: loader.LifecycleActionResolved, ResourceKind: loader.ResourceKindPolicy, Name: ctx.sourcePolicyManaged, Logical: ctx.sourcePolicy},
				{Action: loader.LifecycleActionUpserted, ResourceKind: loader.ResourceKindPipeline, Name: ctx.sourcePipelinePrimary},
				{Action: loader.LifecycleActionUpserted, ResourceKind: loader.ResourceKindPipeline, Name: ctx.sourcePipelineSecondary},
				{Action: loader.LifecycleActionCreated, ResourceKind: loader.ResourceKindIndex, Name: created},
				{Action: loader.LifecycleActionRepointed, ResourceKind: loader.ResourceKindAlias, Name: created, Alias: ctx.sourceIndex},
			}
		}
		check := func(run string, res loader.Result, err error, events []loader.LifecycleEvent, want []loader.LifecycleEvent) {
			t.Helper()
			fail := func(format string, args ...any) {
				t.Helper()
				probeFail(t, run+": "+fmt.Sprintf(format, args...), res, events, err)
			}
			if err != nil {
				fail("Run returned an error")
			}
			if res.CreatedIndex == "" || res.CreatedIndex != res.WriteIndex {
				fail("CreatedIndex %q must be non-empty and equal WriteIndex %q", res.CreatedIndex, res.WriteIndex)
			}
			if res.DocumentsFailed != 0 || len(res.BulkFailures) != 0 || res.DocumentsSucceeded != 2 {
				fail("expected 2 loaded documents and no bulk failures")
			}
			if !slices.Equal(res.EnrichSelected, []string{ctx.sourcePolicyManaged}) || res.EnrichSucceeded != 1 || res.EnrichFailed != 0 {
				fail("unexpected enrich counts")
			}
			if len(res.EnrichOutcomes) != 1 || res.EnrichOutcomes[0].Policy != ctx.sourcePolicyManaged ||
				res.EnrichOutcomes[0].Status != loader.EnrichStatusSucceeded || res.EnrichOutcomes[0].Err != nil {
				fail("expected exactly one succeeded outcome for %s", ctx.sourcePolicyManaged)
			}
			if !slices.Equal(events, want) {
				fail("lifecycle sequence mismatch\nwant=%+v", want)
			}
		}

		var firstEvents []loader.LifecycleEvent
		first, err := runInProcess(t, ctx, newOpts(), &firstEvents)
		firstWant := append(prefix(first.CreatedIndex),
			loader.LifecycleEvent{Action: loader.LifecycleActionUpserted, ResourceKind: loader.ResourceKindPolicy, Name: ctx.sourcePolicyManaged},
			loader.LifecycleEvent{Action: loader.LifecycleActionRefreshed, ResourceKind: loader.ResourceKindIndex, Name: first.WriteIndex},
		)
		check("first rollover", first, err, firstEvents, firstWant)
		withProbeContext(t, first, firstEvents, err, func() {
			assertEqual(t, singleAliasTarget(t, ctx, ctx.sourceIndex), first.CreatedIndex)
		})
		var firstTargets []string
		withProbeContext(t, first, firstEvents, err, func() {
			firstTargets = enrichAliasTargets(t, ctx, ctx.sourcePolicyManaged)
		})
		if len(firstTargets) == 0 {
			probeFail(t, fmt.Sprintf("first rollover: expected enrich alias for %s to have targets", ctx.sourcePolicyManaged), first, firstEvents, err)
		}

		var secondEvents []loader.LifecycleEvent
		second, err := runInProcess(t, ctx, newOpts(), &secondEvents)
		secondWant := append(prefix(second.CreatedIndex),
			loader.LifecycleEvent{Action: loader.LifecycleActionDeleted, ResourceKind: loader.ResourceKindIndex, Name: first.CreatedIndex},
			loader.LifecycleEvent{Action: loader.LifecycleActionRefreshed, ResourceKind: loader.ResourceKindIndex, Name: second.WriteIndex},
		)
		check("second rollover", second, err, secondEvents, secondWant)

		// failSecond reports the second run's context; the first run's
		// CreatedIndex and events are in the message so both runs are visible.
		failSecond := func(format string, args ...any) {
			t.Helper()
			probeFail(t, fmt.Sprintf("second rollover: %s\nfirst CreatedIndex=%q\nfirst events=%+v", fmt.Sprintf(format, args...), first.CreatedIndex, firstEvents), second, secondEvents, err)
		}
		if first.CreatedIndex == second.CreatedIndex {
			failSecond("expected distinct generations, both %q", first.CreatedIndex)
		}
		withProbeContext(t, second, secondEvents, err, func() {
			assertEqual(t, singleAliasTarget(t, ctx, ctx.sourceIndex), second.CreatedIndex)
		})
		var pruned []string
		withProbeContext(t, second, secondEvents, err, func() {
			pruned = timestampedIndices(t, ctx, ctx.sourceIndex)
		})
		if !slices.Equal(pruned, []string{second.CreatedIndex}) {
			failSecond("expected only %q after pruning, got %v", second.CreatedIndex, pruned)
		}
		withProbeContext(t, second, secondEvents, err, func() { assertDocCount(t, ctx, ctx.sourceIndex, 2) })
		withProbeContext(t, second, secondEvents, err, func() { assertEnrichBackingIndexExists(t, ctx, ctx.sourcePolicy) })
		// Each enrich execution builds a new .enrich-<managed>-* index and
		// repoints the alias, so an unchanged target means run 2 did not execute.
		var secondTargets []string
		withProbeContext(t, second, secondEvents, err, func() {
			secondTargets = enrichAliasTargets(t, ctx, ctx.sourcePolicyManaged)
		})
		if len(secondTargets) == 0 || slices.Equal(firstTargets, secondTargets) {
			failSecond("expected enrich alias for %s to be rebuilt by run 2, targets after run 1 %v, after run 2 %v", ctx.sourcePolicyManaged, firstTargets, secondTargets)
		}
	})

	// The invalid-pipeline probe reads the pipeline and alias the rollover probe
	// creates; running it after that probe failed would only add a misleading
	// secondary failure.
	if !rolloverOK {
		t.Log("alias_rollover_lifecycle failed; skipping invalid_pipeline_rejected, which needs its alias and pipeline")
		return
	}

	t.Run("invalid_pipeline_rejected", func(t *testing.T) {
		dir := writeSourceVariant(t, ctx, "source-invalid-pipeline", map[string]string{
			"pipelines.json": fmt.Sprintf(inProcessInvalidPipelinesJSON, ctx.sourcePipelinePrimary),
		})
		before := pipelineDefinition(t, ctx, ctx.sourcePipelinePrimary)

		var events []loader.LifecycleEvent
		res, err := runInProcess(t, ctx, loader.Options{
			Index:         ctx.sourceIndex,
			PipelinesFile: filepath.Join(dir, "pipelines.json"),
			AliasMode:     true,
			SyncManaged:   true,
		}, &events)
		fail := func(format string, args ...any) {
			t.Helper()
			probeFail(t, fmt.Sprintf(format, args...), res, events, err)
		}
		if err == nil {
			fail("expected the invalid pipeline to fail Run")
		}
		var runErr *loader.RunError
		if !errors.As(err, &runErr) {
			fail("expected *RunError, got %T", err)
		}
		var esErr *loader.ElasticsearchError
		if !errors.As(err, &esErr) {
			fail("expected errors.As to reach *ElasticsearchError")
		}
		if esErr.StatusCode != 400 || esErr.Resource != "pipeline" || esErr.Name != ctx.sourcePipelinePrimary {
			fail("unexpected ElasticsearchError %+v, want status 400 resource pipeline name %s", *esErr, ctx.sourcePipelinePrimary)
		}
		if len(esErr.Causes) == 0 || esErr.Causes[0].Type == "" {
			fail("expected at least one cause with a Type, got %+v", esErr.Causes)
		}

		want := []loader.LifecycleEvent{
			{Action: loader.LifecycleActionResolved, ResourceKind: loader.ResourceKindPipeline, Name: ctx.sourcePipelinePrimary},
		}
		if !slices.Equal(events, want) {
			fail("events mismatch, want only %+v", want)
		}
		if after := pipelineDefinition(t, ctx, ctx.sourcePipelinePrimary); after != before {
			fail("rejected PUT changed the existing pipeline\nbefore=%v\nafter=%v", before, after)
		}
	})
}

// probeFail fails the probe with msg plus the Result (including Notices), the
// recorded lifecycle events and the error, so a failure in any probe is
// diagnosable from the log alone.
func probeFail(t *testing.T, msg string, res loader.Result, events []loader.LifecycleEvent, err error) {
	t.Helper()
	t.Fatalf("%s\n%s", msg, probeContext(res, events, err))
}

// probeContext is the one formatter for the Result, Notices, lifecycle events
// and error that every failure in this file prints.
func probeContext(res loader.Result, events []loader.LifecycleEvent, err error) string {
	return fmt.Sprintf("result=%+v\nnotices=%+v\nevents=%+v\nerr=%v", res, res.Notices, events, err)
}

// withProbeContext runs fn, a call into a harness helper that fails the test
// itself with t.Fatalf and so cannot take a message. If fn does not return
// normally, the deferred func (which runs on the Goexit t.Fatalf triggers)
// logs the probe context first.
func withProbeContext(t *testing.T, res loader.Result, events []loader.LifecycleEvent, err error, fn func()) {
	t.Helper()
	returned := false
	defer func() {
		if !returned {
			t.Logf("harness check failed; probe context:\n%s", probeContext(res, events, err))
		}
	}()
	fn()
	returned = true
}

// runInProcess calls loader.Run against the shared container with a Nop logger
// and records every lifecycle event into a fresh slice that *events points at.
// It fills in the URL; callers pass the rest of the options.
func runInProcess(t *testing.T, ctx *scenarioContext, opts loader.Options, events *[]loader.LifecycleEvent) (loader.Result, error) {
	t.Helper()
	logger := zerolog.Nop()
	opts.URL = ctx.env.esURL
	opts.TemplateVariables = ctx.templateVariables()
	opts.Logger = &logger
	*events = nil
	opts.Lifecycle = func(e loader.LifecycleEvent) { *events = append(*events, e) }
	return loader.Run(t.Context(), opts)
}

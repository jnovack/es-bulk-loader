package loader

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/transform"
)

// waitForTransforms blocks until each started transform completes its first
// checkpoint, logging and emitting progress; a failed or timed-out transform fails the run.
//
// A failure emits no Final event for the failing transform: Run's error is
// the single failure signal (see ProgressEvent).
func waitForTransforms(ctx context.Context, es *elasticsearch.Client, definitions namedDefinitions, names []string, timeout time.Duration, emit progressEmitter) {
	for _, name := range names {
		continuous, err := transform.IsContinuous(definitions[name])
		if err != nil {
			fatal(ctx).Err(fmt.Errorf("transform %q: %w", name, err)).Str("transform", name).Msg("Transform wait failed")
		}

		loggerFrom(ctx).Info().
			Str("transform", name).
			Bool("continuous", continuous).
			Str("timeout", timeout.String()).
			Msg("Waiting for transform")
		start := progressNow()
		hb := progress.NewHeartbeat(progress.HeartbeatInterval, progressNow)

		stats, err := transform.Wait(ctx, transformStatsGetter(es), name, transform.WaitConfig{
			Continuous: continuous,
			Timeout:    timeout,
			Sleep:      sleepWithContext,
			OnPoll: func(s transform.Stats, _ int) {
				elapsed := progressNow().Sub(start)
				debug := loggerFrom(ctx).Debug().
					Str("transform", name).
					Str("state", s.State).
					Int64("checkpoint", s.LastCheckpoint)
				if s.DocsRemaining >= 0 {
					debug = debug.Int64("docs_remaining", s.DocsRemaining)
				}
				if s.TotalDocs >= 0 {
					debug = debug.Int64("total_docs", s.TotalDocs)
				}
				if s.HasPercent {
					debug = debug.Float64("percent_complete", s.PercentComplete)
				}
				if s.DocumentsProcessed >= 0 {
					debug = debug.Int64("documents_processed", s.DocumentsProcessed)
				}
				debug.Msg("Transform in progress")

				emit.emit(ProgressEvent{
					Stage:    ProgressStageTransform,
					Resource: name,
					Phase:    s.State,
					Done:     s.DocumentsProcessed,
					Total:    s.TotalDocs,
					Elapsed:  elapsed,
				})

				if hb.Due() {
					loggerFrom(ctx).Info().
						Str("transform", name).
						Str("state", s.State).
						Float64("elapsed_s", elapsed.Seconds()).
						Msg("Transform still running")
				}
			},
		})
		if err != nil {
			fatal(ctx).Err(err).Str("transform", name).Msg("Transform wait failed")
		}

		elapsed := progressNow().Sub(start)
		done := loggerFrom(ctx).Info().
			Str("transform", name).
			Int64("checkpoint", stats.LastCheckpoint)
		// An absent counter is left out of the line rather than shown as 0.
		if stats.DocumentsProcessed >= 0 {
			done = done.Int64("documents_processed", stats.DocumentsProcessed)
		}
		if stats.DocumentsIndexed >= 0 {
			done = done.Int64("documents_indexed", stats.DocumentsIndexed)
		}
		done.Float64("elapsed_s", elapsed.Seconds()).Msg("Transform completed")

		emit.emit(ProgressEvent{
			Stage:    ProgressStageTransform,
			Resource: name,
			Phase:    stats.State,
			Done:     stats.DocumentsProcessed,
			Total:    stats.TotalDocs,
			Elapsed:  elapsed,
			Final:    true,
		})
	}
}

// emitTransformsStarted emits the single Final event for transforms that were
// started without waiting.
func emitTransformsStarted(emit progressEmitter, names []string) {
	for _, name := range names {
		emit.emit(ProgressEvent{
			Stage:    ProgressStageTransform,
			Resource: name,
			Phase:    "started",
			Done:     -1,
			Total:    -1,
			Final:    true,
		})
	}
}

// transformStatsGetter reads GET _transform/<id>/_stats. The context is the
// per-poll one handed over by transform.Wait, so the wait timeout bounds the
// HTTP call.
func transformStatsGetter(es *elasticsearch.Client) transform.Getter {
	return func(pollCtx context.Context, id string) (int, []byte, error) {
		res, err := es.TransformGetTransformStats(
			[]string{id},
			es.TransformGetTransformStats.WithContext(pollCtx),
		)
		if err != nil {
			return 0, nil, err
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return 0, nil, fmt.Errorf("reading _stats response: %w", err)
		}
		return res.StatusCode, body, nil
	}
}

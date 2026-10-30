// Package transform owns the managed transform lifecycle of a Run: source
// filtering, stop/put/start calls and the wait on the first checkpoint, which
// parses _stats and decides when a started transform is done. The lifecycle
// and Wait log through internal/runlog (Wait only to warn that a transient
// _stats failure will be retried) and report progress as internal snapshots;
// the _stats parsing (ParseStats) and done rules (Evaluate) are pure and do
// not log.
package transform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

// Stats is the part of a transform's _stats entry that the wait uses.
// Fields ES did not report hold -1 (or HasPercent=false); nothing is
// estimated, and an explicit zero from ES stays 0.
type Stats struct {
	State              string  // ES "state"
	Reason             string  // ES "reason" (set when failed)
	LastCheckpoint     int64   // checkpointing.last.checkpoint, 0 if absent
	NextInProgress     bool    // non-null checkpointing.next with positive checkpoint
	TotalDocs          int64   // checkpointing.next.checkpoint_progress.total_docs, -1 if absent
	DocsRemaining      int64   // ...docs_remaining, -1 if absent
	PercentComplete    float64 // ...percent_complete
	HasPercent         bool
	DocumentsProcessed int64 // stats.documents_processed, -1 when absent/null
	DocumentsIndexed   int64 // stats.documents_indexed, -1 when absent/null
}

// statsEntry mirrors one element of GET _transform/<id>/_stats "transforms".
// Pointers distinguish an omitted or null value from a measured zero.
type statsEntry struct {
	ID     string `json:"id"`
	State  string `json:"state"`
	Reason string `json:"reason"`
	Stats  *struct {
		DocumentsProcessed *int64 `json:"documents_processed"`
		DocumentsIndexed   *int64 `json:"documents_indexed"`
	} `json:"stats"`
	Checkpointing *struct {
		Last *struct {
			Checkpoint *int64 `json:"checkpoint"`
		} `json:"last"`
		Next *struct {
			Checkpoint         *int64 `json:"checkpoint"`
			CheckpointProgress *struct {
				TotalDocs       *int64   `json:"total_docs"`
				DocsRemaining   *int64   `json:"docs_remaining"`
				PercentComplete *float64 `json:"percent_complete"`
			} `json:"checkpoint_progress"`
		} `json:"next"`
	} `json:"checkpointing"`
}

// ParseStats decodes a GET _transform/<id>/_stats response and returns the
// entry whose id matches id. It fails on malformed JSON, wrong-typed values,
// no matching entry, more than one matching entry, or a missing state: a
// zero-valued Stats must never be mistaken for a finished transform.
func ParseStats(id string, body []byte) (Stats, error) {
	var envelope struct {
		Transforms []json.RawMessage `json:"transforms"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return Stats{}, fmt.Errorf("transform %q: parsing _stats response: %w", id, err)
	}

	var matched json.RawMessage
	for _, raw := range envelope.Transforms {
		var head struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &head); err != nil {
			return Stats{}, fmt.Errorf("transform %q: parsing _stats entry: %w", id, err)
		}
		if head.ID != id {
			continue
		}
		if matched != nil {
			return Stats{}, fmt.Errorf("transform %q: _stats response has more than one entry for this id", id)
		}
		matched = raw
	}
	if matched == nil {
		return Stats{}, fmt.Errorf("transform %q: _stats response has no entry for this id (%d entries)", id, len(envelope.Transforms))
	}

	var entry statsEntry
	if err := json.Unmarshal(matched, &entry); err != nil {
		return Stats{}, fmt.Errorf("transform %q: parsing _stats entry: %w", id, err)
	}
	if entry.State == "" {
		return Stats{}, fmt.Errorf("transform %q: _stats entry has no state", id)
	}

	s := Stats{
		State:              entry.State,
		Reason:             entry.Reason,
		TotalDocs:          -1,
		DocsRemaining:      -1,
		DocumentsProcessed: -1,
		DocumentsIndexed:   -1,
	}
	if entry.Stats != nil {
		if entry.Stats.DocumentsProcessed != nil {
			s.DocumentsProcessed = *entry.Stats.DocumentsProcessed
		}
		if entry.Stats.DocumentsIndexed != nil {
			s.DocumentsIndexed = *entry.Stats.DocumentsIndexed
		}
	}
	if cp := entry.Checkpointing; cp != nil {
		if cp.Last != nil && cp.Last.Checkpoint != nil {
			s.LastCheckpoint = *cp.Last.Checkpoint
		}
		// ES serializes "next" only while a checkpoint is being built; an
		// absent or null next, or one without a positive checkpoint, is idle.
		if cp.Next != nil && cp.Next.Checkpoint != nil && *cp.Next.Checkpoint > 0 {
			s.NextInProgress = true
			if cprog := cp.Next.CheckpointProgress; cprog != nil {
				if cprog.TotalDocs != nil {
					s.TotalDocs = *cprog.TotalDocs
				}
				if cprog.DocsRemaining != nil {
					s.DocsRemaining = *cprog.DocsRemaining
				}
				if cprog.PercentComplete != nil {
					s.PercentComplete = *cprog.PercentComplete
					s.HasPercent = true
				}
			}
		}
	}
	return s, nil
}

// IsContinuous reports whether a transform body has a non-null top-level
// "sync" key, which is what makes a transform continuous in Elasticsearch.
func IsContinuous(body json.RawMessage) (bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return false, fmt.Errorf("parsing transform body: %w", err)
	}
	sync, present := fields["sync"]
	return present && !bytes.Equal(bytes.TrimSpace(sync), []byte("null")), nil
}

// Outcome is the verdict of Evaluate.
type Outcome int

const (
	// Running means the transform has not reached its first completed checkpoint.
	Running Outcome = iota
	// Done means the transform completed its first checkpoint.
	Done
	// Failed means Elasticsearch reports the transform failed.
	Failed
)

// Evaluate applies the done rules. A failed state wins. A batch transform is
// done when stopped at checkpoint >= 1. A continuous transform never stops by
// itself, so it is done when checkpoint >= 1 and no later checkpoint is in
// progress. Everything else, including a batch transform stopped at
// checkpoint 0 (not yet started), is still running.
func Evaluate(s Stats, continuous bool) Outcome {
	if strings.EqualFold(s.State, "failed") {
		return Failed
	}
	if continuous {
		if s.LastCheckpoint >= 1 && !s.NextInProgress {
			return Done
		}
		return Running
	}
	if strings.EqualFold(s.State, "stopped") && s.LastCheckpoint >= 1 {
		return Done
	}
	return Running
}

// Getter fetches GET _transform/<id>/_stats. It must honor ctx, so the wait
// timeout bounds HTTP work and not only the sleeps.
type Getter func(ctx context.Context, id string) (status int, body []byte, err error)

// WaitConfig configures Wait.
type WaitConfig struct {
	// Continuous selects the continuous done rule.
	Continuous bool
	// Timeout bounds the whole wait, requests, retries and sleeps included.
	// Must be > 0.
	Timeout time.Duration
	// Sleep waits between polls and must return ctx.Err() once ctx is done.
	// Required: pkg/loader passes its test-swappable sleepWithContext.
	Sleep func(context.Context, time.Duration) error
	// OnPoll, when non-nil, is called after each successfully parsed poll.
	// A failed poll that is retried never calls it.
	OnPoll func(s Stats, pollCount int)
}

// Wait polls the transform's _stats until it is done, failed, timed out or
// cancelled. On Done it returns the final Stats and a nil error. Every other
// return is an error that names the transform id.
//
// A transient poll failure is retried rather than failing the wait: an HTTP
// 429/502/503/504 (esresponse.IsRetryableStatus; its body is diagnostic text,
// never parsed as stats) or a getter error that
// esresponse.IsRetryableTransportError accepts. Each retry logs one Warn
// naming the transform, the 1-based poll number and the cause, then takes the
// same PollInterval sleep as a running poll. Any other getter error, any other
// non-2xx status, a malformed 2xx body and a failed state still fail at once.
//
// Retries are bounded only by cfg.Timeout: the one deadline set at entry
// covers every request and sleep, so a persistent outage ends the wait at the
// original deadline, never later. When that deadline or a cancellation
// interrupts the wait, the error appends the latest retried failure since the
// last valid poll ("; last poll failed: ...") to the last-state description;
// a valid poll clears it, so a stale failure never taints a later timeout.
func Wait(ctx context.Context, get Getter, id string, cfg WaitConfig) (Stats, error) {
	if get == nil || cfg.Sleep == nil {
		return Stats{}, fmt.Errorf("transform %q: wait requires a getter and a sleep function", id)
	}
	if cfg.Timeout <= 0 {
		return Stats{}, fmt.Errorf("transform %q: wait timeout must be positive, got %s", id, cfg.Timeout)
	}

	// Every request, sleep and check below uses waitCtx, never ctx, so the
	// deadline cannot be bypassed by a call that only sees the caller's context.
	// It is created once: retries share it, so they can never extend the wait
	// past cfg.Timeout (a per-attempt context would make the timeout unbounded
	// under a persistent outage).
	waitCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	var last Stats
	polled := false
	// lastFailure is the latest retried poll failure since the last valid
	// poll, reported only if the wait is interrupted before another valid one.
	var lastFailure error
	interrupted := func() error {
		cause := waitCtx.Err()
		if cause == nil {
			return nil
		}
		verb := "cancelled"
		if errors.Is(cause, context.DeadlineExceeded) {
			verb = fmt.Sprintf("timed out after %s", cfg.Timeout)
		}
		state := describe(last, polled)
		if lastFailure != nil {
			state += "; last poll failed: " + lastFailure.Error()
		}
		return fmt.Errorf("transform %q %s waiting for its first checkpoint (%s): %w", id, verb, state, cause)
	}

	for poll := 1; ; poll++ {
		if err := interrupted(); err != nil {
			return last, err
		}

		status, body, err := get(waitCtx, id)
		// A deadline that expires inside the HTTP call surfaces as a getter
		// error; report it as the timeout it is, not as a transport failure.
		// This check also outranks retry: an ended wait is never retried.
		if cerr := interrupted(); cerr != nil {
			return last, cerr
		}

		switch {
		case err != nil:
			if !esresponse.IsRetryableTransportError(err) {
				return last, fmt.Errorf("transform %q: requesting _stats: %w", id, err)
			}
			lastFailure = fmt.Errorf("requesting _stats: %w", err)
		case status < 200 || status > 299:
			statusErr := fmt.Errorf("_stats returned status %d: %s", status, strings.TrimSpace(string(body)))
			if !esresponse.IsRetryableStatus(status) {
				return last, fmt.Errorf("transform %q: %w", id, statusErr)
			}
			lastFailure = statusErr
		default:
			stats, err := ParseStats(id, body)
			if err != nil {
				return last, err
			}
			last, polled, lastFailure = stats, true, nil

			if cfg.OnPoll != nil {
				cfg.OnPoll(stats, poll)
			}
			if cerr := interrupted(); cerr != nil {
				return last, cerr
			}

			switch Evaluate(stats, cfg.Continuous) {
			case Done:
				return stats, nil
			case Failed:
				reason := stats.Reason
				if reason == "" {
					reason = "no reason reported"
				}
				return stats, fmt.Errorf("transform %q failed: %s", id, reason)
			}
		}

		// A valid poll cleared lastFailure above, so a non-nil value here is
		// this poll's retried failure.
		if lastFailure != nil {
			retry := runlog.From(ctx).Warn().
				Str("transform", id).
				Int("attempt", poll)
			if err == nil {
				retry = retry.Int("status_code", status)
			}
			retry.Err(lastFailure).Msg("Transform _stats poll failed; retrying")
		}

		// One sleep for both a running poll and a retried failure, so retries
		// keep the shared cadence (including escalation) and the same
		// interruption handling.
		if err := cfg.Sleep(waitCtx, progress.PollInterval(poll)); err != nil {
			if cerr := interrupted(); cerr != nil {
				return last, cerr
			}
			return last, fmt.Errorf("transform %q: sleeping between polls: %w", id, err)
		}
	}
}

// describe renders the last observed state for timeout and cancel errors.
func describe(s Stats, polled bool) string {
	if !polled {
		return "no _stats response received yet"
	}
	text := fmt.Sprintf("last state %q, checkpoint %d", s.State, s.LastCheckpoint)
	if s.DocumentsProcessed >= 0 {
		text += fmt.Sprintf(", documents_processed %d", s.DocumentsProcessed)
	}
	if s.TotalDocs >= 0 {
		text += fmt.Sprintf(" of %d in this checkpoint", s.TotalDocs)
	}
	return text
}

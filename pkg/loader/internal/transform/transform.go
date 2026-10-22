// Package transform parses transform _stats responses and decides when a
// started transform is done. It is used by pkg/loader and does not log.
package transform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
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
	// Timeout bounds the whole wait, requests and sleeps included. Must be > 0.
	Timeout time.Duration
	// Sleep waits between polls and must return ctx.Err() once ctx is done.
	// Required: pkg/loader passes its test-swappable sleepWithContext.
	Sleep func(context.Context, time.Duration) error
	// OnPoll, when non-nil, is called after each successfully parsed poll.
	OnPoll func(s Stats, pollCount int)
}

// Wait polls the transform's _stats until it is done, failed, timed out or
// cancelled. On Done it returns the final Stats and a nil error. Every other
// return is an error that names the transform id.
func Wait(ctx context.Context, get Getter, id string, cfg WaitConfig) (Stats, error) {
	if get == nil || cfg.Sleep == nil {
		return Stats{}, fmt.Errorf("transform %q: wait requires a getter and a sleep function", id)
	}
	if cfg.Timeout <= 0 {
		return Stats{}, fmt.Errorf("transform %q: wait timeout must be positive, got %s", id, cfg.Timeout)
	}

	// Every request, sleep and check below uses waitCtx, never ctx, so the
	// deadline cannot be bypassed by a call that only sees the caller's context.
	waitCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	var last Stats
	polled := false
	interrupted := func() error {
		cause := waitCtx.Err()
		if cause == nil {
			return nil
		}
		verb := "cancelled"
		if errors.Is(cause, context.DeadlineExceeded) {
			verb = fmt.Sprintf("timed out after %s", cfg.Timeout)
		}
		return fmt.Errorf("transform %q %s waiting for its first checkpoint (%s): %w", id, verb, describe(last, polled), cause)
	}

	for poll := 1; ; poll++ {
		if err := interrupted(); err != nil {
			return last, err
		}

		status, body, err := get(waitCtx, id)
		// A deadline that expires inside the HTTP call surfaces as a getter
		// error; report it as the timeout it is, not as a transport failure.
		if cerr := interrupted(); cerr != nil {
			return last, cerr
		}
		if err != nil {
			return last, fmt.Errorf("transform %q: requesting _stats: %w", id, err)
		}
		if status < 200 || status > 299 {
			return last, fmt.Errorf("transform %q: _stats returned status %d: %s", id, status, strings.TrimSpace(string(body)))
		}
		stats, err := ParseStats(id, body)
		if err != nil {
			return last, err
		}
		last, polled = stats, true

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

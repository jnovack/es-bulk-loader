// Package progress provides the poll cadence and heartbeat throttle shared by
// the enrich and transform waits, plus the Snapshot and Seams contract that
// enrich, transform and bulk use to report progress to package loader without
// importing it. It does not log.
package progress

import (
	"context"
	"time"
)

const (
	// initialPollInterval keeps the first few task checks responsive.
	initialPollInterval = 2 * time.Second
	// subsequentPollInterval reduces load for longer-running tasks.
	subsequentPollInterval = 5 * time.Second
	// pollIntervalEscalationCount is the first poll to use the longer interval.
	pollIntervalEscalationCount = 6

	// HeartbeatInterval is the minimum gap between "still running" Info lines.
	HeartbeatInterval = 30 * time.Second
)

// PollInterval returns the polling cadence for pollCount (1-based). Enrich
// task polling and the transform wait share it so there is one cadence.
func PollInterval(pollCount int) time.Duration {
	if pollCount >= pollIntervalEscalationCount {
		return subsequentPollInterval
	}
	return initialPollInterval
}

// Heartbeat throttles "still running" Info lines so a long wait is visible
// at the default level without one line per poll.
type Heartbeat struct {
	interval time.Duration
	now      func() time.Time
	last     time.Time
}

// NewHeartbeat returns a Heartbeat whose first line is due one interval after
// now() was read here, so a wait that finishes quickly logs no heartbeat.
func NewHeartbeat(interval time.Duration, now func() time.Time) *Heartbeat {
	return &Heartbeat{interval: interval, now: now, last: now()}
}

// Due reports true once at least one interval has passed since the last time
// it reported true (or since NewHeartbeat), and then restarts the interval.
func (h *Heartbeat) Due() bool {
	current := h.now()
	if current.Sub(h.last) < h.interval {
		return false
	}
	h.last = current
	return true
}

// Snapshot is one progress observation that an internal owner (enrich,
// transform, bulk) hands to package loader through a synchronous
// func(Snapshot) callback. It deliberately has no stage: loader's single
// adapter adds the public ProgressStage, so the field mapping to the public
// ProgressEvent exists once and no internal package imports loader. Done and
// Total are -1 when unknown.
type Snapshot struct {
	Resource string
	Phase    string
	Done     int64
	Total    int64
	Elapsed  time.Duration
	Final    bool
	Err      error
}

// Seams carries a Run's clock, sleep and progress callback into an internal
// owner (enrich, transform, bulk), so no owner keeps its own copy of the
// package-level seams that tests swap. Now times elapsed values and
// heartbeats; Sleep waits and must return ctx.Err() once ctx is done;
// OnProgress, when non-nil, receives each Snapshot synchronously on the
// calling goroutine.
type Seams struct {
	Now        func() time.Time
	Sleep      func(context.Context, time.Duration) error
	OnProgress func(Snapshot)
}

// Report delivers s to OnProgress, or does nothing when there is none.
func (s Seams) Report(snapshot Snapshot) {
	if s.OnProgress != nil {
		s.OnProgress(snapshot)
	}
}

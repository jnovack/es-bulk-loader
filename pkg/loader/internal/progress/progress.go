// Package progress provides the poll cadence and the heartbeat throttle that
// pkg/loader uses while it waits on enrich tasks and transforms. It does not
// log.
package progress

import "time"

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

package progress

import (
	"testing"
	"time"
)

// TestPollInterval verifies the polling cadence transitions after five checks.
func TestPollInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pollCount int
		want      time.Duration
	}{
		{name: "first poll", pollCount: 1, want: initialPollInterval},
		{name: "fifth poll", pollCount: 5, want: initialPollInterval},
		{name: "sixth poll", pollCount: 6, want: subsequentPollInterval},
		{name: "far past escalation", pollCount: 1000, want: subsequentPollInterval},
		{name: "zero poll stays on the short interval", pollCount: 0, want: initialPollInterval},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := PollInterval(tt.pollCount); got != tt.want {
				t.Fatalf("PollInterval(%d) = %s, want %s", tt.pollCount, got, tt.want)
			}
		})
	}
}

// TestHeartbeatDue drives Heartbeat with a fake clock; no real time passes.
func TestHeartbeatDue(t *testing.T) {
	t.Parallel()

	const interval = 30 * time.Second

	tests := []struct {
		name string
		// steps advance the clock by advance, then expect Due() == want.
		steps []struct {
			advance time.Duration
			want    bool
		}
	}{
		{
			name: "not due before the interval",
			steps: []struct {
				advance time.Duration
				want    bool
			}{{0, false}, {29 * time.Second, false}},
		},
		{
			name: "due exactly at the interval",
			steps: []struct {
				advance time.Duration
				want    bool
			}{{interval, true}},
		},
		{
			name: "resets after it fires",
			steps: []struct {
				advance time.Duration
				want    bool
			}{{interval, true}, {time.Second, false}, {interval - 2*time.Second, false}},
		},
		{
			name: "due again after a second interval",
			steps: []struct {
				advance time.Duration
				want    bool
			}{{interval, true}, {interval, true}},
		},
		{
			name: "a long gap fires once, not once per missed interval",
			steps: []struct {
				advance time.Duration
				want    bool
			}{{5 * interval, true}, {0, false}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clock := time.Unix(1000, 0)
			hb := NewHeartbeat(interval, func() time.Time { return clock })
			for i, step := range tt.steps {
				clock = clock.Add(step.advance)
				if got := hb.Due(); got != step.want {
					t.Fatalf("step %d (advance %s): Due() = %v, want %v", i, step.advance, got, step.want)
				}
			}
		})
	}
}

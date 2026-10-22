package transform

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	batchID      = "esbl-progress-test-tx-batch"
	continuousID = "esbl-progress-test-tx-continuous"
)

// readFixture loads a response recorded from Elasticsearch 9.3.1; see
// test/fixtures/elasticsearch/README.md.
func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "test", "fixtures", "elasticsearch", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return body
}

// statsBody builds a synthetic _stats response with one entry.
func statsBody(entry string) []byte {
	return []byte(`{"count":1,"transforms":[` + entry + `]}`)
}

func TestParseStats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      string
		body    []byte
		want    Stats
		wantErr string
	}{
		{
			name: "recorded batch before start",
			id:   batchID, body: readFixture(t, "transform-stats-batch-before-start.json"),
			want: Stats{State: "stopped", TotalDocs: -1, DocsRemaining: -1, DocumentsProcessed: 0, DocumentsIndexed: 0},
		},
		{
			// Batch next.checkpoint_progress has no total_docs or percent_complete.
			name: "recorded batch indexing without total",
			id:   batchID, body: readFixture(t, "transform-stats-batch-indexing.json"),
			want: Stats{State: "indexing", NextInProgress: true, TotalDocs: -1, DocsRemaining: -1, DocumentsProcessed: 0, DocumentsIndexed: 0},
		},
		{
			name: "recorded batch stopped at checkpoint 1",
			id:   batchID, body: readFixture(t, "transform-stats-batch-stopped.json"),
			want: Stats{State: "stopped", LastCheckpoint: 1, TotalDocs: -1, DocsRemaining: -1, DocumentsProcessed: 500000, DocumentsIndexed: 500000},
		},
		{
			name: "recorded continuous indexing with full checkpoint_progress",
			id:   continuousID, body: readFixture(t, "transform-stats-continuous-indexing.json"),
			want: Stats{State: "indexing", NextInProgress: true, TotalDocs: 500000, DocsRemaining: 500000, PercentComplete: 0, HasPercent: true, DocumentsProcessed: 0, DocumentsIndexed: 0},
		},
		{
			name: "recorded continuous idle after first checkpoint",
			id:   continuousID, body: readFixture(t, "transform-stats-continuous-started.json"),
			want: Stats{State: "started", LastCheckpoint: 1, TotalDocs: -1, DocsRemaining: -1, DocumentsProcessed: 500000, DocumentsIndexed: 500000},
		},
		{
			name: "failed with a reason",
			id:   "t1", body: statsBody(`{"id":"t1","state":"failed","reason":"task encountered more than 10 failures","checkpointing":{"last":{"checkpoint":0}}}`),
			want: Stats{State: "failed", Reason: "task encountered more than 10 failures", TotalDocs: -1, DocsRemaining: -1, DocumentsProcessed: -1, DocumentsIndexed: -1},
		},
		{
			name: "null stats counters stay unknown",
			id:   "t1", body: statsBody(`{"id":"t1","state":"indexing","stats":{"documents_processed":null},"checkpointing":null}`),
			want: Stats{State: "indexing", TotalDocs: -1, DocsRemaining: -1, DocumentsProcessed: -1, DocumentsIndexed: -1},
		},
		{
			name: "explicit zero progress values stay zero",
			id:   "t1", body: statsBody(`{"id":"t1","state":"indexing","stats":{"documents_processed":0,"documents_indexed":0},"checkpointing":{"last":{"checkpoint":1},"next":{"checkpoint":2,"checkpoint_progress":{"total_docs":0,"docs_remaining":0,"percent_complete":0}}}}`),
			want: Stats{State: "indexing", LastCheckpoint: 1, NextInProgress: true, TotalDocs: 0, DocsRemaining: 0, HasPercent: true, DocumentsProcessed: 0, DocumentsIndexed: 0},
		},
		{
			name: "next with null checkpoint is inactive",
			id:   "t1", body: statsBody(`{"id":"t1","state":"started","checkpointing":{"last":{"checkpoint":3},"next":{"checkpoint":null}}}`),
			want: Stats{State: "started", LastCheckpoint: 3, TotalDocs: -1, DocsRemaining: -1, DocumentsProcessed: -1, DocumentsIndexed: -1},
		},
		{
			name: "next with checkpoint zero is inactive",
			id:   "t1", body: statsBody(`{"id":"t1","state":"started","checkpointing":{"last":{"checkpoint":3},"next":{"checkpoint":0}}}`),
			want: Stats{State: "started", LastCheckpoint: 3, TotalDocs: -1, DocsRemaining: -1, DocumentsProcessed: -1, DocumentsIndexed: -1},
		},
		{
			name: "picks the matching entry among several",
			id:   "t2", body: []byte(`{"count":2,"transforms":[{"id":"t1","state":"failed"},{"id":"t2","state":"stopped","checkpointing":{"last":{"checkpoint":2}}}]}`),
			want: Stats{State: "stopped", LastCheckpoint: 2, TotalDocs: -1, DocsRemaining: -1, DocumentsProcessed: -1, DocumentsIndexed: -1},
		},
		{name: "empty transforms array", id: "t1", body: []byte(`{"count":0,"transforms":[]}`), wantErr: `transform "t1"`},
		{name: "id mismatch", id: "t1", body: statsBody(`{"id":"other","state":"stopped"}`), wantErr: `transform "t1"`},
		{name: "malformed json", id: "t1", body: []byte(`{"transforms":[`), wantErr: `transform "t1"`},
		{name: "empty body", id: "t1", body: nil, wantErr: `transform "t1"`},
		{name: "duplicate matching entries", id: "t1", body: []byte(`{"transforms":[{"id":"t1","state":"stopped"},{"id":"t1","state":"stopped"}]}`), wantErr: "more than one entry"},
		{name: "missing state is not completion", id: "t1", body: statsBody(`{"id":"t1","checkpointing":{"last":{"checkpoint":1}}}`), wantErr: "no state"},
		{name: "empty state", id: "t1", body: statsBody(`{"id":"t1","state":""}`), wantErr: "no state"},
		{name: "state wrong type", id: "t1", body: statsBody(`{"id":"t1","state":7}`), wantErr: `transform "t1"`},
		{name: "checkpoint wrong type", id: "t1", body: statsBody(`{"id":"t1","state":"stopped","checkpointing":{"last":{"checkpoint":"1"}}}`), wantErr: `transform "t1"`},
		{name: "entry not an object", id: "t1", body: []byte(`{"transforms":[1]}`), wantErr: `transform "t1"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseStats(tt.id, tt.body)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("stats = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestIsContinuous(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		want    bool
		wantErr bool
	}{
		{name: "sync present", body: `{"source":{"index":"a"},"sync":{"time":{"field":"ts"}}}`, want: true},
		{name: "sync absent", body: `{"source":{"index":"a"},"pivot":{}}`, want: false},
		{name: "sync null is not continuous", body: `{"sync":null}`, want: false},
		{name: "nested sync is not top-level", body: `{"pivot":{"sync":{}}}`, want: false},
		{name: "invalid json", body: `{"sync":`, wantErr: true},
		{name: "not an object", body: `[1]`, wantErr: true},
		{name: "empty", body: ``, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := IsContinuous(json.RawMessage(tt.body))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("IsContinuous = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvaluate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stats      Stats
		continuous bool
		want       Outcome
	}{
		{name: "batch stopped at checkpoint 1 is done", stats: Stats{State: "stopped", LastCheckpoint: 1}, want: Done},
		{name: "batch stopped at checkpoint 0 is running", stats: Stats{State: "stopped", LastCheckpoint: 0}, want: Running},
		{name: "batch indexing at checkpoint 1 is running", stats: Stats{State: "indexing", LastCheckpoint: 1}, want: Running},
		{name: "batch started is running", stats: Stats{State: "started", LastCheckpoint: 1}, want: Running},
		{name: "batch ignores next in progress once stopped", stats: Stats{State: "stopped", LastCheckpoint: 1, NextInProgress: true}, want: Done},
		{name: "batch failed", stats: Stats{State: "failed", LastCheckpoint: 1}, want: Failed},
		{name: "failed is case-insensitive", stats: Stats{State: "FAILED"}, want: Failed},
		{name: "continuous checkpoint 1 idle while started is done", stats: Stats{State: "started", LastCheckpoint: 1}, continuous: true, want: Done},
		{name: "continuous checkpoint 1 idle while indexing is done", stats: Stats{State: "indexing", LastCheckpoint: 1}, continuous: true, want: Done},
		{name: "continuous checkpoint 1 with next in progress is running", stats: Stats{State: "indexing", LastCheckpoint: 1, NextInProgress: true}, continuous: true, want: Running},
		{name: "continuous checkpoint 0 is running", stats: Stats{State: "started", LastCheckpoint: 0}, continuous: true, want: Running},
		{name: "continuous failed", stats: Stats{State: "failed", LastCheckpoint: 2}, continuous: true, want: Failed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Evaluate(tt.stats, tt.continuous); got != tt.want {
				t.Fatalf("Evaluate(%+v, %v) = %v, want %v", tt.stats, tt.continuous, got, tt.want)
			}
		})
	}
}

// sequenceGetter returns the given bodies in order with status 200, repeating
// the last one, and counts calls.
func sequenceGetter(bodies ...[]byte) (Getter, *int) {
	calls := 0
	return func(ctx context.Context, id string) (int, []byte, error) {
		i := calls
		if i >= len(bodies) {
			i = len(bodies) - 1
		}
		calls++
		return 200, bodies[i], nil
	}, &calls
}

// recordingSleep records durations and returns ctx.Err() once ctx is done.
func recordingSleep(durations *[]time.Duration) func(context.Context, time.Duration) error {
	return func(ctx context.Context, d time.Duration) error {
		*durations = append(*durations, d)
		return ctx.Err()
	}
}

func TestWaitDoneAfterThreePolls(t *testing.T) {
	t.Parallel()

	get, calls := sequenceGetter(
		readFixture(t, "transform-stats-batch-before-start.json"),
		readFixture(t, "transform-stats-batch-indexing.json"),
		readFixture(t, "transform-stats-batch-stopped.json"),
	)
	var sleeps []time.Duration
	var polls []int
	var states []string
	stats, err := Wait(t.Context(), get, batchID, WaitConfig{
		Timeout: time.Hour,
		Sleep:   recordingSleep(&sleeps),
		OnPoll: func(s Stats, n int) {
			polls = append(polls, n)
			states = append(states, s.State)
		},
	})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if stats.State != "stopped" || stats.LastCheckpoint != 1 || stats.DocumentsIndexed != 500000 {
		t.Fatalf("final stats = %+v", stats)
	}
	if *calls != 3 {
		t.Fatalf("getter calls = %d, want 3", *calls)
	}
	if want := []int{1, 2, 3}; !reflect.DeepEqual(polls, want) {
		t.Fatalf("OnPoll counts = %v, want %v", polls, want)
	}
	if want := []string{"stopped", "indexing", "stopped"}; !reflect.DeepEqual(states, want) {
		t.Fatalf("OnPoll states = %v, want %v", states, want)
	}
	if want := []time.Duration{2 * time.Second, 2 * time.Second}; !reflect.DeepEqual(sleeps, want) {
		t.Fatalf("sleep durations = %v, want %v", sleeps, want)
	}
}

func TestWaitContinuousDoneWhileStillStarted(t *testing.T) {
	t.Parallel()

	get, _ := sequenceGetter(
		readFixture(t, "transform-stats-continuous-indexing.json"),
		readFixture(t, "transform-stats-continuous-started.json"),
	)
	var sleeps []time.Duration
	stats, err := Wait(t.Context(), get, continuousID, WaitConfig{Continuous: true, Timeout: time.Hour, Sleep: recordingSleep(&sleeps)})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if stats.State != "started" || stats.LastCheckpoint != 1 {
		t.Fatalf("final stats = %+v", stats)
	}
}

func TestWaitSleepCadenceEscalates(t *testing.T) {
	t.Parallel()

	running := statsBody(`{"id":"t1","state":"indexing"}`)
	done := statsBody(`{"id":"t1","state":"stopped","checkpointing":{"last":{"checkpoint":1}}}`)
	bodies := make([][]byte, 0, 8)
	for i := 0; i < 7; i++ {
		bodies = append(bodies, running)
	}
	bodies = append(bodies, done)
	get, _ := sequenceGetter(bodies...)
	var sleeps []time.Duration
	if _, err := Wait(t.Context(), get, "t1", WaitConfig{Timeout: time.Hour, Sleep: recordingSleep(&sleeps)}); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	want := []time.Duration{2 * time.Second, 2 * time.Second, 2 * time.Second, 2 * time.Second, 2 * time.Second, 5 * time.Second, 5 * time.Second}
	if !reflect.DeepEqual(sleeps, want) {
		t.Fatalf("sleep durations = %v, want %v", sleeps, want)
	}
}

func TestWaitErrors(t *testing.T) {
	t.Parallel()

	failedBody := statsBody(`{"id":"t1","state":"failed","reason":"search_phase_execution_exception: boom"}`)
	transportErr := errors.New("connection refused")

	tests := []struct {
		name       string
		get        Getter
		wantErr    []string
		wantNotErr []string
		wantIs     error
	}{
		{
			name:    "failed quotes the reason",
			get:     func(context.Context, string) (int, []byte, error) { return 200, failedBody, nil },
			wantErr: []string{`transform "t1" failed: search_phase_execution_exception: boom`},
		},
		{
			name: "failed without a reason says so",
			get: func(context.Context, string) (int, []byte, error) {
				return 200, statsBody(`{"id":"t1","state":"failed"}`), nil
			},
			wantErr: []string{`transform "t1" failed: no reason reported`},
		},
		{
			name:    "404 reports status and body",
			get:     func(context.Context, string) (int, []byte, error) { return 404, []byte(`{"error":"missing"}`), nil },
			wantErr: []string{`transform "t1"`, "status 404", `{"error":"missing"}`},
		},
		{
			name:       "getter error with a live context",
			get:        func(context.Context, string) (int, []byte, error) { return 0, nil, transportErr },
			wantErr:    []string{`transform "t1"`, "connection refused"},
			wantNotErr: []string{"timed out", "cancelled"},
			wantIs:     transportErr,
		},
		{
			name:    "malformed stats body",
			get:     func(context.Context, string) (int, []byte, error) { return 200, []byte(`{`), nil },
			wantErr: []string{`transform "t1"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var sleeps []time.Duration
			_, err := Wait(t.Context(), tt.get, "t1", WaitConfig{Timeout: time.Hour, Sleep: recordingSleep(&sleeps)})
			if err == nil {
				t.Fatal("expected an error")
			}
			for _, want := range tt.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not contain %q", err, want)
				}
			}
			for _, notWant := range tt.wantNotErr {
				if strings.Contains(err.Error(), notWant) {
					t.Fatalf("error %q must not contain %q", err, notWant)
				}
			}
			if tt.wantIs != nil && !errors.Is(err, tt.wantIs) {
				t.Fatalf("error %v does not wrap %v", err, tt.wantIs)
			}
			if len(sleeps) != 0 {
				t.Fatalf("a terminal poll must not sleep, got %v", sleeps)
			}
		})
	}
}

func TestWaitRejectsBadConfig(t *testing.T) {
	t.Parallel()

	get, calls := sequenceGetter(statsBody(`{"id":"t1","state":"indexing"}`))
	sleep := func(context.Context, time.Duration) error { return nil }
	tests := []struct {
		name string
		get  Getter
		cfg  WaitConfig
	}{
		{name: "zero timeout", get: get, cfg: WaitConfig{Timeout: 0, Sleep: sleep}},
		{name: "negative timeout", get: get, cfg: WaitConfig{Timeout: -time.Second, Sleep: sleep}},
		{name: "nil sleep", get: get, cfg: WaitConfig{Timeout: time.Hour}},
		{name: "nil getter", get: nil, cfg: WaitConfig{Timeout: time.Hour, Sleep: sleep}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Wait(t.Context(), tt.get, "t1", tt.cfg); err == nil || !strings.Contains(err.Error(), `transform "t1"`) {
				t.Fatalf("err = %v, want an error naming the transform", err)
			}
		})
	}
	if *calls != 0 {
		t.Fatalf("bad config must not poll, got %d calls", *calls)
	}
}

func TestWaitNilOnPoll(t *testing.T) {
	t.Parallel()

	get, _ := sequenceGetter(statsBody(`{"id":"t1","state":"stopped","checkpointing":{"last":{"checkpoint":1}}}`))
	var sleeps []time.Duration
	if _, err := Wait(t.Context(), get, "t1", WaitConfig{Timeout: time.Hour, Sleep: recordingSleep(&sleeps), OnPoll: nil}); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestWaitCancelAndTimeout(t *testing.T) {
	t.Parallel()

	indexing := statsBody(`{"id":"t1","state":"indexing","stats":{"documents_processed":120},"checkpointing":{"last":{"checkpoint":0},"next":{"checkpoint":1,"checkpoint_progress":{"total_docs":900}}}}`)
	done := statsBody(`{"id":"t1","state":"stopped","checkpointing":{"last":{"checkpoint":1}}}`)

	t.Run("parent already cancelled never polls", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		get, calls := sequenceGetter(done)
		_, err := Wait(ctx, get, "t1", WaitConfig{Timeout: time.Hour, Sleep: recordingSleep(new([]time.Duration))})
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), `transform "t1"`) {
			t.Fatalf("err = %v, want a Canceled error naming the transform", err)
		}
		if *calls != 0 {
			t.Fatalf("getter calls = %d, want 0", *calls)
		}
	})

	t.Run("cancel during sleep reports last state and progress", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		get, _ := sequenceGetter(indexing)
		// Sleep cancels the parent and returns what a real sleep returns.
		sleep := func(c context.Context, _ time.Duration) error {
			cancel()
			return c.Err()
		}
		_, err := Wait(ctx, get, "t1", WaitConfig{Timeout: time.Hour, Sleep: sleep})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want it to wrap context.Canceled", err)
		}
		for _, want := range []string{`transform "t1"`, `"indexing"`, "checkpoint 0", "documents_processed 120", "of 900"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q does not contain %q", err, want)
			}
		}
	})

	t.Run("timeout bounds a blocked HTTP call", func(t *testing.T) {
		t.Parallel()
		// The getter blocks until the derived context ends, which only
		// happens if Wait hands the getter its timeout context.
		get := func(c context.Context, _ string) (int, []byte, error) {
			<-c.Done()
			return 0, nil, c.Err()
		}
		_, err := Wait(t.Context(), get, "t1", WaitConfig{Timeout: time.Millisecond, Sleep: recordingSleep(new([]time.Duration))})
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "timed out") || !strings.Contains(err.Error(), `transform "t1"`) {
			t.Fatalf("err = %v, want a DeadlineExceeded timeout naming the transform", err)
		}
	})

	t.Run("non-context getter error after the deadline is still a timeout", func(t *testing.T) {
		t.Parallel()
		get := func(c context.Context, _ string) (int, []byte, error) {
			<-c.Done()
			return 0, nil, errors.New("transport: use of closed connection")
		}
		_, err := Wait(t.Context(), get, "t1", WaitConfig{Timeout: time.Millisecond, Sleep: recordingSleep(new([]time.Duration))})
		if !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "requesting _stats") {
			t.Fatalf("err = %v, want a timeout, not a transport error", err)
		}
	})

	t.Run("deadline during sleep is a timeout", func(t *testing.T) {
		t.Parallel()
		get, _ := sequenceGetter(indexing)
		sleep := func(c context.Context, _ time.Duration) error {
			<-c.Done()
			return c.Err()
		}
		_, err := Wait(t.Context(), get, "t1", WaitConfig{Timeout: 200 * time.Millisecond, Sleep: sleep})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want DeadlineExceeded", err)
		}
	})

	t.Run("cancel while the getter is blocked", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		entered := make(chan struct{})
		get := func(c context.Context, _ string) (int, []byte, error) {
			close(entered)
			<-c.Done()
			return 0, nil, c.Err()
		}
		result := make(chan error, 1)
		go func() {
			_, err := Wait(ctx, get, "t1", WaitConfig{Timeout: time.Hour, Sleep: recordingSleep(new([]time.Duration))})
			result <- err
		}()
		<-entered
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "timed out") {
			t.Fatalf("err = %v, want a Canceled error", err)
		}
	})

	t.Run("cancel before a nominal done result wins", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		get, _ := sequenceGetter(done)
		_, err := Wait(ctx, get, "t1", WaitConfig{
			Timeout: time.Hour,
			Sleep:   recordingSleep(new([]time.Duration)),
			OnPoll:  func(Stats, int) { cancel() },
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want Canceled even though stats say done", err)
		}
	})

	t.Run("deadline during the request cannot become success", func(t *testing.T) {
		t.Parallel()
		// The getter ends the deadline and still returns a done body.
		get := func(c context.Context, _ string) (int, []byte, error) {
			<-c.Done()
			return 200, done, nil
		}
		_, err := Wait(t.Context(), get, "t1", WaitConfig{Timeout: time.Millisecond, Sleep: recordingSleep(new([]time.Duration))})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want DeadlineExceeded", err)
		}
	})
}

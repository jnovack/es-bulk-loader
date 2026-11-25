package esresponse

// LOG-OBSERVE-01 gap 1 guards: a Rejection retains its resource literal,
// HTTP status and sanitized pairs (each field bounded) for the public
// ElasticsearchError projection, without changing one byte of its message.
// Every body, name and secret is synthetic.

import (
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

// fieldPastCap is longer than MaxPublicMessageBytes, so two values that share
// it as a prefix differ only past the retained-field cut.
var fieldPastCap = strings.Repeat("é", MaxPublicMessageBytes)

// requirePairsBounded fails unless every retained field is bounded, single
// line UTF-8 and free of the synthetic secrets.
func requirePairsBounded(t *testing.T, pairs []Pair) {
	t.Helper()
	if len(pairs) > maxDiagnosticPairs {
		t.Fatalf("%d pairs, want <= %d", len(pairs), maxDiagnosticPairs)
	}
	for _, p := range pairs {
		requireBounded(t, p.Type, MaxPublicMessageBytes)
		requireBounded(t, p.Reason, MaxPublicMessageBytes)
		requireNoSecrets(t, p.Type)
		requireNoSecrets(t, p.Reason)
		if p == (Pair{}) {
			t.Fatal("retained an empty pair")
		}
	}
}

func TestRejectionRetainsStructure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		body      string
		readErr   error
		wantPairs []Pair
	}{
		{
			name: "root_cause, top-level and caused_by in order, deduplicated",
			body: `{"error":{"root_cause":[{"type":"r","reason":"rr"},{"type":"t","reason":"tr"}],` +
				`"type":"t","reason":"tr","caused_by":{"type":"c","reason":"cr"}}}`,
			wantPairs: []Pair{{"r", "rr"}, {"t", "tr"}, {"c", "cr"}},
		},
		{
			name:      "deduplicated after sanitization",
			body:      `{"error":{"root_cause":[{"type":"t","reason":"a  b"}],"type":"t","reason":"a\nb"}}`,
			wantPairs: []Pair{{"t", "a b"}},
		},
		{
			name:      "pair cap keeps the first four",
			body:      `{"error":{"root_cause":[{"type":"a"},{"type":"b"},{"type":"c"},{"type":"d"},{"type":"e"}]}}`,
			wantPairs: []Pair{{Type: "a"}, {Type: "b"}, {Type: "c"}, {Type: "d"}},
		},
		{
			name: "depth cap stops at four caused_by levels",
			body: `{"error":{"type":"a","caused_by":{"type":"b","caused_by":{"type":"c","caused_by":` +
				`{"type":"d","caused_by":{"type":"e"}}}}}}`,
			wantPairs: []Pair{{Type: "a"}, {Type: "b"}, {Type: "c"}, {Type: "d"}},
		},
		{name: "plain-string error is a reason", body: `{"error":"no handler"}`, wantPairs: []Pair{{Reason: "no handler"}}},
		{name: "type only", body: `{"error":{"type":"x"}}`, wantPairs: []Pair{{Type: "x"}}},
		{name: "empty body", body: ""},
		{name: "not JSON", body: "<html>"},
		{name: "two JSON values", body: `{} {}`},
		{name: "no recognizable error", body: `{"acknowledged":false}`},
		{name: "empty error object", body: `{"error":{}}`},
		{name: "oversized", body: string(bodyOfSize(t, MaxRejectionBodyBytes+1))},
		{name: "read error wins over a parseable body", body: `{"error":{"type":"t","reason":"r"}}`, readErr: io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := NewRejection("pipeline", "p", 400, []byte(tt.body), tt.readErr, MaxPublicMessageBytes)
			if r.Resource() != "pipeline" || r.ResourceName() != "p" || r.Status() != 400 {
				t.Fatalf("resource %q name %q status %d", r.Resource(), r.ResourceName(), r.Status())
			}
			got := r.Pairs()
			if !slices.Equal(got, tt.wantPairs) {
				t.Fatalf("Pairs = %q, want %q", got, tt.wantPairs)
			}
			if tt.wantPairs == nil && got != nil {
				t.Fatalf("fallback Pairs = %#v, want nil", got)
			}
			if r.Unwrap() != tt.readErr {
				t.Fatalf("read error identity lost: %v", r.Unwrap())
			}
		})
	}
}

// TestRetainedFieldsAreCutAfterRendering pins the order extraction → render
// → bounded retention. A per-field cut moved into extraction or rendering
// would change the text at budgets above the field cap, merge pairs distinct
// only past the cap, or drop the omitted-causes marker.
func TestRetainedFieldsAreCutAfterRendering(t *testing.T) {
	t.Parallel()

	longType := "t" + fieldPastCap
	longReason := strings.Repeat("r", 3000)
	// fieldCut is the retained form of a value over the cap, stated with the
	// truncation primitive rather than boundMessage. "é" is two bytes, so
	// the cut lands on a rune boundary at 2,036 bytes.
	fieldCut := func(s string) string {
		return truncateUTF8(s, MaxPublicMessageBytes-len(truncationMarker)) + truncationMarker
	}
	tests := []struct {
		name      string
		value     any
		wantText  string
		wantPairs []Pair
	}{
		{
			name:      "long type and reason render whole",
			value:     map[string]any{"type": longType, "reason": longReason},
			wantText:  longType + ": " + longReason,
			wantPairs: []Pair{{fieldCut(longType), fieldCut(longReason)}},
		},
		{
			name: "pairs distinct only past the cap keep both slots",
			value: map[string]any{"type": "t", "reason": fieldPastCap + "1",
				"caused_by": map[string]any{"type": "t", "reason": fieldPastCap + "2"}},
			wantText:  "t: " + fieldPastCap + "1; t: " + fieldPastCap + "2",
			wantPairs: []Pair{{"t", fieldCut(fieldPastCap)}, {"t", fieldCut(fieldPastCap)}},
		},
		{
			name: "a fifth pair distinct only past the cap still sets the omitted marker",
			value: map[string]any{"root_cause": []any{
				map[string]any{"reason": fieldPastCap + "1"}, map[string]any{"reason": fieldPastCap + "2"},
				map[string]any{"reason": fieldPastCap + "3"}, map[string]any{"reason": fieldPastCap + "4"},
				map[string]any{"reason": fieldPastCap + "5"},
			}},
			wantText: fieldPastCap + "1; " + fieldPastCap + "2; " + fieldPastCap + "3; " +
				fieldPastCap + "4; " + omittedMarker,
			wantPairs: slices.Repeat([]Pair{{Reason: fieldCut(fieldPastCap)}}, 4),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			raw := mustMarshal(t, tt.value)
			// A budget far above the field cap exposes any early field cut.
			if got := ErrorDetail(raw, 1<<20); got != tt.wantText {
				t.Fatalf("ErrorDetail text changed (%d bytes, want %d)", len(got), len(tt.wantText))
			}
			d := ParseErrorValue(raw)
			if d.Text(1<<20) != tt.wantText {
				t.Fatal("ParseErrorValue text differs from ErrorDetail")
			}
			// The production budget still cuts the whole text once.
			if got, want := d.Text(MaxPublicMessageBytes), boundMessage(tt.wantText, MaxPublicMessageBytes); got != want {
				t.Fatalf("production-budget text = %.80q, want %.80q", got, want)
			}

			body := mustMarshal(t, map[string]any{"error": tt.value})
			r := NewRejection("pipeline", "p", 400, body, nil, 1<<20)
			if want := msgP + tt.wantText; r.Error() != want {
				t.Fatalf("NewRejection text changed (%d bytes, want %d)", len(r.Error()), len(want))
			}
			for _, pairs := range [][]Pair{r.Pairs(), NewDetailFrom("index", "i", "x: ", d).Pairs()} {
				if !slices.Equal(pairs, tt.wantPairs) {
					t.Fatalf("Pairs differ from the cut-after-render form (%d pairs, want %d)", len(pairs), len(tt.wantPairs))
				}
				requirePairsBounded(t, pairs)
			}
		})
	}
}

func TestRetainedPairsRedactAndBound(t *testing.T) {
	t.Parallel()

	secretName := "p password=hunter2"
	secretValue := map[string]any{
		"type":   "t " + secretJSON,
		"reason": strings.Repeat("é", 3000) + " " + secretJSON,
	}
	httpBody := mustMarshal(t, map[string]any{"error": secretValue})
	shard := mustMarshal(t, map[string]any{"reason": map[string]any{"type": "t", "reason": "credential " + secretJSON}})
	tests := []struct {
		name string
		r    *Rejection
	}{
		{"HTTP rejection", NewRejection("pipeline", secretName, 400, httpBody, nil, MaxPublicMessageBytes)},
		{"Bounded HTTP rejection", NewRejection("pipeline", secretName, 400, httpBody, nil, MaxPublicMessageBytes).Bounded(100)},
		{"NewDetailFrom", NewDetailFrom("index", secretName, "flush had 1 shard failures; first: ", ParseShardFailure(shard))},
		{"NewDetailFrom error value", NewDetailFrom("enrich task", secretName, "failed: ", ParseErrorValue(mustMarshal(t, secretValue)))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pairs := tt.r.Pairs()
			if len(pairs) == 0 {
				t.Fatal("no pairs retained")
			}
			requirePairsBounded(t, pairs)
			requireNoSecrets(t, tt.r.ResourceName())
			requireNoSecrets(t, tt.r.Error())
			if !strings.Contains(pairs[0].Reason, redacted) && !strings.Contains(pairs[0].Type, redacted) {
				t.Fatalf("pair %q shows no redaction", pairs[0])
			}
		})
	}

	t.Run("sanitize precedes the field cut", func(t *testing.T) {
		t.Parallel()
		// Slide the secret across the 2,048-byte field boundary: cutting
		// before sanitizing would leave "user:pw" without its '@'.
		for shift := MaxPublicMessageBytes - 60; shift < MaxPublicMessageBytes+10; shift++ {
			reason := strings.Repeat("a", shift) + cutSweepSecret
			r := NewRejection("pipeline", "p", 400, mustMarshal(t, map[string]any{"error": map[string]any{"reason": reason}}), nil, MaxPublicMessageBytes)
			requirePairsBounded(t, r.Pairs())
		}
	})
}

func TestRejectionBoundedKeepsStructure(t *testing.T) {
	t.Parallel()

	body := []byte(`{"error":{"type":"t","reason":"` + strings.Repeat("é", 3000) + `"}}`)
	boom := errors.New("read failed")
	tests := []struct {
		name string
		r    *Rejection
	}{
		{"HTTP rejection with pairs", NewRejection("pipeline", "p", 409, body, nil, MaxPublicMessageBytes)},
		{"read error fallback", NewRejection("bulk request", "", 400, body, boom, MaxPublicMessageBytes)},
		{"2xx detail", NewDetailFrom("index", "i", "first: ", ParseShardFailure(mustMarshal(t, map[string]any{
			"cause": map[string]any{"type": "c", "reason": strings.Repeat("x", 3000)}})))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			orig := tt.r.Error()
			wantPairs := tt.r.Pairs()
			if fits := tt.r.Bounded(len(orig)); fits != tt.r {
				t.Fatal("a fitting budget must return the receiver")
			}
			for _, budget := range []int{40, 0, -1} {
				cut := tt.r.Bounded(budget)
				if cut == tt.r {
					t.Fatalf("budget %d returned the receiver", budget)
				}
				requireBounded(t, cut.Error(), max(budget, 0))
				if cut.Resource() != tt.r.Resource() || cut.ResourceName() != tt.r.ResourceName() ||
					cut.Status() != tt.r.Status() || cut.Unwrap() != tt.r.Unwrap() {
					t.Fatalf("budget %d lost structure: %q %q %d %v", budget, cut.Resource(), cut.ResourceName(), cut.Status(), cut.Unwrap())
				}
				if !slices.Equal(cut.Pairs(), wantPairs) {
					t.Fatalf("budget %d pairs = %q, want %q", budget, cut.Pairs(), wantPairs)
				}
			}
			if tt.r.Error() != orig {
				t.Fatal("Bounded modified the receiver")
			}
		})
	}
}

func TestPairsReturnsFreshSlices(t *testing.T) {
	t.Parallel()

	r := NewRejection("pipeline", "p", 400, []byte(`{"error":{"type":"t","reason":"r","caused_by":{"type":"c"}}}`), nil, MaxPublicMessageBytes)
	cut := r.Bounded(10)
	want := []Pair{{"t", "r"}, {Type: "c"}}

	got := r.Pairs()
	got[0] = Pair{Type: "mutated"}
	got[1] = Pair{Type: "mutated too"}
	for _, view := range [][]Pair{r.Pairs(), cut.Pairs()} {
		if !slices.Equal(view, want) {
			t.Fatalf("Pairs after caller mutation = %q, want %q", view, want)
		}
	}
	cutView := cut.Pairs()
	cutView[1].Reason = "mutated"
	if !slices.Equal(r.Pairs(), want) {
		t.Fatal("mutating a Bounded copy's Pairs changed the receiver")
	}
}

func TestDetailStructure(t *testing.T) {
	t.Parallel()

	t.Run("text-only NewDetail keeps resource and invents nothing", func(t *testing.T) {
		t.Parallel()
		r := NewDetail("index", "cards", "flush had 1 shard failures; first: t: r")
		if r.Resource() != "index" || r.ResourceName() != "cards" || r.Status() != 0 || r.Pairs() != nil {
			t.Fatalf("resource %q name %q status %d pairs %q", r.Resource(), r.ResourceName(), r.Status(), r.Pairs())
		}
	})

	tests := []struct {
		name      string
		raw       string
		wantPairs []Pair
	}{
		{"cause object", `{"cause":{"type":"version_conflict_engine_exception","reason":"conflict"}}`, []Pair{{"version_conflict_engine_exception", "conflict"}}},
		{"reason object", `{"reason":{"type":"illegal_state_exception","reason":"failed"}}`, []Pair{{"illegal_state_exception", "failed"}}},
		{"reason string", `{"reason":"plain  failure"}`, []Pair{{Reason: "plain failure"}}},
		{"cause wins over reason", `{"cause":{"type":"a","reason":"b"},"reason":{"type":"c","reason":"d"}}`, []Pair{{"a", "b"}}},
		{"empty cause wins over reason", `{"cause":{},"reason":{"type":"c","reason":"d"}}`, nil},
		{"neither", `{"index":"i"}`, nil},
		{"non-object", `"x"`, nil},
		{"malformed", `{`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const prefix = "flush had 3 shard failures; first: "
			d := ParseShardFailure(json.RawMessage(tt.raw))
			r := NewDetailFrom("index", "cards", prefix, d)
			// Byte-identical to the former flush composition.
			want := NewDetail("index", "cards", prefix+ShardFailureDetail(json.RawMessage(tt.raw), MaxPublicMessageBytes))
			if r.Error() != want.Error() {
				t.Fatalf("message = %q, want %q", r.Error(), want.Error())
			}
			if r.Status() != 0 || r.Resource() != "index" || r.ResourceName() != "cards" || r.Unwrap() != nil {
				t.Fatalf("status %d resource %q name %q unwrap %v", r.Status(), r.Resource(), r.ResourceName(), r.Unwrap())
			}
			if got := r.Pairs(); !slices.Equal(got, tt.wantPairs) || (tt.wantPairs == nil && got != nil) {
				t.Fatalf("Pairs = %#v, want %#v", got, tt.wantPairs)
			}
		})
	}

	t.Run("caller-supplied verb for an error value", func(t *testing.T) {
		t.Parallel()
		d := ParseErrorValue(json.RawMessage(`{"type":"t","reason":"r"}`))
		r := NewDetailFrom("enrich task", "node:1", "failed: ", d)
		if want := `enrich task "node:1" failed: t: r`; r.Error() != want {
			t.Fatalf("message = %q, want %q", r.Error(), want)
		}
		if !slices.Equal(r.Pairs(), []Pair{{"t", "r"}}) {
			t.Fatalf("Pairs = %q", r.Pairs())
		}
		if got := ParseErrorValue(json.RawMessage(`[1]`)); got.Text(MaxPublicMessageBytes) != detailUnrecognized || got.pairs != nil {
			t.Fatalf("array value = %q / %v, want the fixed fallback without pairs", got.Text(MaxPublicMessageBytes), got.pairs)
		}
	})

	t.Run("zero FailureDetail", func(t *testing.T) {
		t.Parallel()
		r := NewDetailFrom("index", "", "prefix", FailureDetail{})
		if r.Error() != "index prefix" || r.Pairs() != nil {
			t.Fatalf("message %q pairs %q", r.Error(), r.Pairs())
		}
	})
}

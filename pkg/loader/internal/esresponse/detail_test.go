package esresponse

// Every secret in this file is synthetic and asserted absent from output.

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
)

const (
	secretJSON = `"password":"hunter2" at https://user:pw@host/x`
	// cutSweepSecret discriminates sanitize-before-cut from cut-before-
	// sanitize: a password value cut short is still redacted either way, but
	// URL userinfo cut before its '@' no longer matches userinfoPattern, so a
	// reversed order leaks "user:pw" at the budget that lands between them.
	// The trailing text must outlast the truncation marker, or no budget
	// below the input length can cut between "pw" and '@'.
	cutSweepSecret = ` password=hunter2 https://user:pw@host/x trailing context`
)

// mustMarshal fails the test on a marshal error: a silent empty value would
// take the detailNotJSON path and make the redaction assertions vacuous.
func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return raw
}

func requireNoSecrets(t *testing.T, s string) {
	t.Helper()
	for _, secret := range []string{"hunter2", "user:pw"} {
		if strings.Contains(s, secret) {
			t.Fatalf("%q leaked in %q", secret, s)
		}
	}
}

func TestErrorDetail(t *testing.T) {
	t.Parallel()

	deep := `{"type":"a","reason":"r1","caused_by":{"type":"b","reason":"r2","caused_by":{"type":"c","reason":"r3","caused_by":{"type":"d","reason":"r4","caused_by":{"type":"e","reason":"r5","caused_by":{"type":"f","reason":"r6"}}}}}}`
	// Five caused_by levels of one repeated pair: deduplication keeps the
	// pair count at one, so only the depth cap can add the omitted marker.
	sameLevel := `{"type":"b","reason":"1"`
	depthOnly := sameLevel + strings.Repeat(`,"caused_by":`+sameLevel, 5) + strings.Repeat("}", 6)
	tests := []struct {
		name string
		raw  string
		max  int
		want string
	}{
		{"plain string", `"boom"`, 2048, "boom"},
		{"type and reason", `{"type":"t","reason":"r"}`, 2048, "t: r"},
		{"caused_by chain", `{"type":"t","reason":"r","caused_by":{"type":"u","reason":"s"}}`, 2048, "t: r; u: s"},
		{"root_cause first then dedup", `{"root_cause":[{"type":"t","reason":"r"}],"type":"t","reason":"r"}`, 2048, "t: r"},
		{"type only", `{"type":"t"}`, 2048, "t"},
		{"reason only", `{"reason":"r"}`, 2048, "r"},
		{"root, top, root chain, top chain order with dedup", `{"root_cause":[{"type":"r","reason":"1","caused_by":{"type":"rc","reason":"2"}}],"type":"t","reason":"3","caused_by":{"type":"r","reason":"1"}}`, 2048, "r: 1; t: 3; rc: 2"},
		{"pair cap", deep, 2048, "a: r1; b: r2; c: r3; d: r4; [further causes omitted]"},
		{"depth cap", depthOnly, 2048, "b: 1; [further causes omitted]"},
		{"whitespace-only object strings", `{"type":"  ","reason":"\t"}`, 2048, detailUnrecognized},
		{"ignored metadata", `{"type":"t","reason":"r","stack_trace":"hunter2","header":{"password":"hunter2"}}`, 2048, "t: r"},
		{"empty", ``, 2048, detailNotJSON},
		{"malformed", `{"type":`, 2048, detailNotJSON},
		{"trailing garbage", `{"type":"t"} x`, 2048, detailNotJSON},
		{"two values", `{"type":"t"} {"type":"u"}`, 2048, detailNotJSON},
		{"null", `null`, 2048, detailUnrecognized},
		{"number", `42`, 2048, detailUnrecognized},
		{"array", `["x"]`, 2048, detailUnrecognized},
		{"empty object", `{}`, 2048, detailUnrecognized},
		{"empty string", `"  "`, 2048, detailUnrecognized},
		{"wrong-shaped object", `{"type":1,"reason":2}`, 2048, detailUnrecognized},
		{"tiny budget", `"abcdefghij"`, 3, "abc"},
		{"nonpositive budget", `"abc"`, 0, ""},
		{"exact budget", `"abc"`, 3, "abc"},
		{"over budget marker", `"` + strings.Repeat("x", 100) + `"`, 30, strings.Repeat("x", 18) + truncationMarker},
		{"multibyte cut", `"` + strings.Repeat("é", 100) + `"`, 31, strings.Repeat("é", 9) + truncationMarker},
		{"budget equal to marker cuts without it", `"abcdefghijklmnopqrstuvwxyz"`, 12, "abcdefghijkl"},
		{"budget one over marker keeps it", `"abcdefghijklmnopqrstuvwxyz"`, 13, "a" + truncationMarker},
		{"multibyte hard cut", `"éééé"`, 3, "é"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ErrorDetail(json.RawMessage(tt.raw), tt.max)
			if got != tt.want {
				t.Fatalf("ErrorDetail = %q, want %q", got, tt.want)
			}
			if tt.max > 0 {
				requireBounded(t, got, tt.max)
			}
		})
	}

	t.Run("redaction", func(t *testing.T) {
		t.Parallel()
		raw := mustMarshal(t, map[string]string{"type": "t", "reason": secretJSON})
		got := ErrorDetail(raw, 2048)
		requireNoSecrets(t, got)
		if !strings.HasPrefix(got, "t: ") {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("sanitize precedes cut", func(t *testing.T) {
		t.Parallel()
		reason := strings.Repeat("a", 20) + cutSweepSecret
		for _, raw := range []json.RawMessage{
			mustMarshal(t, reason),
			mustMarshal(t, map[string]string{"type": "t", "reason": reason}),
		} {
			for max := 10; max < len(reason)+20; max++ {
				requireNoSecrets(t, ErrorDetail(raw, max))
			}
		}
	})

	t.Run("beyond 64 KiB still extracts", func(t *testing.T) {
		t.Parallel()
		raw := mustMarshal(t, map[string]string{"type": "t", "reason": strings.Repeat("x", MaxRejectionBodyBytes+10)})
		got := ErrorDetail(raw, 100)
		if !strings.HasPrefix(got, "t: xxx") || !strings.HasSuffix(got, truncationMarker) {
			t.Fatalf("got %q", got)
		}
		requireBounded(t, got, 100)
	})
}

func TestShardFailureDetail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"cause object", `{"index":"i","cause":{"type":"version_conflict_engine_exception","reason":"conflict"},"status":409}`, "version_conflict_engine_exception: conflict"},
		{"reason object", `{"shard":0,"reason":{"type":"illegal_state_exception","reason":"failed"}}`, "illegal_state_exception: failed"},
		{"reason string", `{"reason":"plain  failure"}`, "plain failure"},
		{"cause wins over reason", `{"cause":{"type":"a","reason":"b"},"reason":{"type":"c","reason":"d"}}`, "a: b"},
		{"empty cause wins over reason", `{"cause":{},"reason":{"type":"c","reason":"d"}}`, detailUnrecognized},
		{"wrong-shaped cause falls back to reason", `{"cause":"x","reason":{"type":"c","reason":"d"}}`, "c: d"},
		{"neither", `{"index":"i","status":409}`, detailUnrecognized},
		{"reason number", `{"reason":5}`, detailUnrecognized},
		{"non-object", `"x"`, detailUnrecognized},
		{"array", `[1]`, detailUnrecognized},
		{"malformed", `{`, detailNotJSON},
		{"two values", `{} {}`, detailNotJSON},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ShardFailureDetail(json.RawMessage(tt.raw), 2048)
			if got != tt.want {
				t.Fatalf("ShardFailureDetail = %q, want %q", got, tt.want)
			}
			requireBounded(t, got, 2048)
		})
	}

	budgets := []struct {
		name string
		raw  string
		max  int
		want string
	}{
		{"nonpositive", `{"reason":"abc"}`, 0, ""},
		{"tiny", `{"reason":"abcdef"}`, 4, "abcd"},
		{"exact", `{"reason":"abc"}`, 3, "abc"},
		{"over", `{"cause":{"type":"t","reason":"` + strings.Repeat("x", 50) + `"}}`, 20, "t: xxxxx" + truncationMarker},
		{"fallback bounded too", `{`, 5, detailNotJSON[:5]},
	}
	for _, tt := range budgets {
		t.Run("budget "+tt.name, func(t *testing.T) {
			t.Parallel()
			got := ShardFailureDetail(json.RawMessage(tt.raw), tt.max)
			if got != tt.want {
				t.Fatalf("ShardFailureDetail = %q, want %q", got, tt.want)
			}
		})
	}

	t.Run("redaction and bound", func(t *testing.T) {
		t.Parallel()
		raw := mustMarshal(t, map[string]any{"reason": map[string]string{"type": "t", "reason": secretJSON + strings.Repeat("é", 3000)}})
		got := ShardFailureDetail(raw, 200)
		requireNoSecrets(t, got)
		requireBounded(t, got, 200)
		if !strings.HasSuffix(got, truncationMarker) {
			t.Fatalf("no marker: %q", got)
		}
	})

	t.Run("redaction precedes cut", func(t *testing.T) {
		t.Parallel()
		reason := strings.Repeat("a", 20) + cutSweepSecret
		raw := mustMarshal(t, map[string]any{"cause": map[string]string{"type": "t", "reason": reason}})
		for max := 10; max < len(reason)+20; max++ {
			requireNoSecrets(t, ShardFailureDetail(raw, max))
		}
	})
}

func TestSanitizeText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		max  int
		want string
	}{
		{"harmless", "field [x] bad", 2048, "field [x] bad"},
		{"empty", "", 2048, ""},
		{"whitespace only", " \t\r\n ", 2048, ""},
		{"crlf normalized", "a\r\nb", 2048, "a b"},
		{"control", "a\x00b", 2048, "a b"},
		{"invalid utf8", "a\xffb", 2048, "a�b"},
		{"exact", "abc", 3, "abc"},
		{"tiny", "abcdef", 4, "abcd"},
		{"nonpositive", "abc", -1, ""},
		{"over", strings.Repeat("x", 50), 20, strings.Repeat("x", 8) + truncationMarker},
		{"multibyte", strings.Repeat("é", 50), 21, strings.Repeat("é", 4) + truncationMarker},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := SanitizeText(tt.in, tt.max)
			if got != tt.want {
				t.Fatalf("SanitizeText = %q, want %q", got, tt.want)
			}
		})
	}

	t.Run("redacts", func(t *testing.T) {
		t.Parallel()
		requireNoSecrets(t, SanitizeText(secretJSON, 2048))
	})
	t.Run("redaction precedes cut", func(t *testing.T) {
		t.Parallel()
		in := strings.Repeat("a", 20) + ` "password":"hunter2"` + cutSweepSecret
		for max := 5; max < len(in)+5; max++ {
			requireNoSecrets(t, SanitizeText(in, max))
		}
	})
}

func TestNewDetail(t *testing.T) {
	t.Parallel()

	t.Run("named", func(t *testing.T) {
		t.Parallel()
		r := NewDetail("index", "cards", "flush had 1 shard failures; first: x")
		if want := `index "cards" flush had 1 shard failures; first: x`; r.Error() != want {
			t.Fatalf("got %q, want %q", r.Error(), want)
		}
		if r.ResourceName() != "cards" || r.Unwrap() != nil {
			t.Fatalf("name %q unwrap %v", r.ResourceName(), r.Unwrap())
		}
	})
	t.Run("nameless", func(t *testing.T) {
		t.Parallel()
		r := NewDetail("index", "", "text")
		if r.Error() != "index text" || r.ResourceName() != "" {
			t.Fatalf("got %q / %q", r.Error(), r.ResourceName())
		}
	})
	t.Run("long escaped multibyte name", func(t *testing.T) {
		t.Parallel()
		// The secret sits inside the 256-byte cap so redaction, not the cut,
		// must remove it.
		name := "a\"\nb password=hunter2 " + strings.Repeat("é", 400)
		r := NewDetail("index", name, "t")
		requireNoSecrets(t, r.Error())
		display := r.ResourceName()
		requireBounded(t, display, maxResourceNameBytes)
		if !strings.HasPrefix(display, `a" b password=[REDACTED] é`) || !strings.HasSuffix(display, truncationMarker) {
			t.Fatalf("display name %q", display)
		}
		if want := "index " + strconv.Quote(display) + " t"; r.Error() != want {
			t.Fatalf("Error() = %q, want %q", r.Error(), want)
		}
		requireBounded(t, r.Error(), MaxPublicMessageBytes)
	})
	t.Run("initial cap and Bounded copies", func(t *testing.T) {
		t.Parallel()
		r := NewDetail("index", "i", strings.Repeat("x", 5000))
		requireBounded(t, r.Error(), MaxPublicMessageBytes)
		if !strings.HasSuffix(r.Error(), truncationMarker) {
			t.Fatalf("no marker")
		}
		orig := r.Error()
		b := r.Bounded(40)
		requireBounded(t, b.Error(), 40)
		if r.Error() != orig || b.ResourceName() != "i" || errors.Unwrap(b) != nil {
			t.Fatalf("Bounded changed receiver or lost name")
		}
	})
}

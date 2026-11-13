package esresponse

// Every body, name and secret in this file is synthetic: FAKE-SECRET-* values
// stand in for credentials and are asserted absent from the output. The rows
// pin the FIX-PIPECAUSE-01 diagnostic contract (extraction order, pair and
// depth budgets, fallbacks, 64 KiB body and 2,048-byte message bounds,
// redaction) and must change with that contract, not to fit a new behavior.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"unicode/utf8"
)

// opPrefix is the RunError prefix CreatePipelines' Op adds in front of the
// cause; package managed cannot be imported here (import cycle), so the
// length is restated and the managed/Run tests assert the full message.
const opPrefix = "Failed to create pipeline: "

const msgP = `pipeline "p" rejected with HTTP 400: `

func TestNewRejectionExtraction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string // detail after msgP
	}{
		{
			name: "root_cause deduplicated with matching top-level error",
			body: `{"error":{"root_cause":[{"type":"parse_exception","reason":"No processor type exists with name [fixture_proc]"}],` +
				`"type":"parse_exception","reason":"No processor type exists with name [fixture_proc]"},"status":400}`,
			want: "parse_exception: No processor type exists with name [fixture_proc]",
		},
		{
			name: "top-level object only",
			body: `{"error":{"type":"illegal_argument_exception","reason":"bad processor"},"status":400}`,
			want: "illegal_argument_exception: bad processor",
		},
		{
			name: "nested caused_by at three levels",
			body: `{"error":{"type":"a","reason":"ra","caused_by":{"type":"b","reason":"rb","caused_by":{"type":"c","reason":"rc",` +
				`"caused_by":{"type":"d","reason":"rd"}}}}}`,
			want: "a: ra; b: rb; c: rc; d: rd",
		},
		{
			name: "plain-string error",
			body: `{"error":"no handler found for uri [/_ingest/pipeline/p]","status":400}`,
			want: "no handler found for uri [/_ingest/pipeline/p]",
		},
		{
			name: "type only",
			body: `{"error":{"type":"x_exception"}}`,
			want: "x_exception",
		},
		{
			name: "reason only",
			body: `{"error":{"reason":"only a reason"}}`,
			want: "only a reason",
		},
		{
			name: "duplicate root_cause entries",
			body: `{"error":{"root_cause":[{"type":"t","reason":"r"},{"type":"t","reason":"r"}]}}`,
			want: "t: r",
		},
		{
			name: "irrelevant metadata is ignored",
			body: `{"error":{"type":"t","reason":"r","stack_trace":"FAKE-TRACE","index":"FAKE-INDEX","index_uuid":"FAKE-UUID",` +
				`"header":{"WWW-Authenticate":"FAKE-HEADER"},"processor_type":"FAKE-PROC","doc":{"field":"FAKE-DOC"}},` +
				`"status":400,"_source":{"field":"FAKE-SOURCE"}}`,
			want: "t: r",
		},
		{
			name: "roots in array order, then top-level, then chains",
			body: `{"error":{"root_cause":[{"type":"a"},{"type":"b","caused_by":{"type":"d"}}],"type":"c"}}`,
			want: "a; b; c; d",
		},
		{
			name: "root chains precede the top-level chain",
			body: `{"error":{"root_cause":[{"type":"a","caused_by":{"type":"b"}}],"type":"c","caused_by":{"type":"d"}}}`,
			want: "a; c; b; d",
		},
		{
			name: "six distinct pairs keep four and mark the rest",
			body: `{"error":{"root_cause":[{"type":"r1"},{"type":"r2"},{"type":"r3"},{"type":"r4"},{"type":"r5"},{"type":"r6"}]}}`,
			want: "r1; r2; r3; r4; [further causes omitted]",
		},
		{
			name: "exactly four pairs need no marker",
			body: `{"error":{"root_cause":[{"type":"r1"},{"type":"r2"},{"type":"r3"},{"type":"r4"}]}}`,
			want: "r1; r2; r3; r4",
		},
		{
			name: "trailing duplicates and empties are not a fifth pair",
			body: `{"error":{"root_cause":[{"type":"r1"},{"type":"r2"},{"type":"r3"},{"type":"r4"},{"type":"r1"},{},{"type":" "}],` +
				`"type":"r2","caused_by":{"type":"r3"}}}`,
			want: "r1; r2; r3; r4",
		},
		{
			// Owner and levels one to three repeat or are empty, so the pair
			// cap cannot be what drops level five: only the depth limit can.
			name: "depth limit keeps level four and marks level five",
			body: `{"error":{"type":"dup","reason":"x","caused_by":{"type":"dup","reason":"x","caused_by":{"reason":"   ",` +
				`"caused_by":{"type":"dup","reason":"x","caused_by":{"type":"level4","caused_by":{"type":"level5"}}}}}}}`,
			want: "dup: x; level4; [further causes omitted]",
		},
		{
			name: "chain ending at level four needs no marker",
			body: `{"error":{"type":"dup","caused_by":{"type":"dup","caused_by":{"type":"dup",` +
				`"caused_by":{"type":"dup","caused_by":{"type":"level4","caused_by":"wrong shape"}}}}}}`,
			want: "dup; level4",
		},
		{
			name: "pairs equal after sanitization are deduplicated",
			body: `{"error":{"root_cause":[{"type":"t","reason":"a  b"},{"type":"t","reason":"a\nb"}],"type":"t","reason":"\ta b "}}`,
			want: "t: a b",
		},
		{
			name: "whitespace-only fields fall back",
			body: `{"error":{"type":"  ","reason":"\t\n","root_cause":[{"type":"\r"}]}}`,
			want: detailUnrecognized,
		},
		{name: "error as wrong-shaped array", body: `{"error":["parse_exception"]}`, want: detailUnrecognized},
		{
			name: "error object with empty or wrong-shaped parts",
			body: `{"error":{"root_cause":[],"caused_by":{},"type":5,"reason":{"nested":"FAKE-NESTED"}}}`,
			want: detailUnrecognized,
		},
		{name: "wrong-shaped root_cause", body: `{"error":{"root_cause":{"type":"t"}}}`, want: detailUnrecognized},
		{name: "wrong-shaped root_cause elements", body: `{"error":{"root_cause":[null,5,"s",[]]}}`, want: detailUnrecognized},
		{name: "null body", body: `null`, want: detailUnrecognized},
		{name: "null error", body: `{"error":null}`, want: detailUnrecognized},
		{name: "top-level array", body: `[{"error":"x"}]`, want: detailUnrecognized},
		{name: "no error field", body: `{}`, want: detailUnrecognized},
		{name: "invalid JSON", body: `{"error":`, want: detailNotJSON},
		{name: "trailing garbage", body: `{"error":"x"} trailing`, want: detailNotJSON},
		{name: "second JSON value", body: `{"error":"x"} {"error":"y"}`, want: detailNotJSON},
		{name: "HTML", body: `<html><body>502 Bad Gateway FAKE-HTML</body></html>`, want: detailNotJSON},
		{name: "empty body", body: ``, want: detailEmpty},
		{name: "whitespace body", body: " \r\n\t", want: detailEmpty},
		{name: "trailing whitespace is accepted", body: "{\"error\":\"x\"}\n\n", want: "x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := NewRejection("pipeline", "p", 400, []byte(tt.body), nil, MaxPublicMessageBytes)
			if got, want := r.Error(), msgP+tt.want; got != want {
				t.Fatalf("message mismatch\n got: %s\nwant: %s", got, want)
			}
			if r.Unwrap() != nil {
				t.Fatalf("Unwrap = %v, want nil without a read error", r.Unwrap())
			}
			if strings.Contains(r.Error(), "FAKE-") {
				t.Fatalf("message %q contains ignored metadata", r.Error())
			}
		})
	}
}

// bodyOfSize returns a valid error body of exactly n bytes whose reason starts
// with "PREFIX-" and is padded with 'x' (not spaces: sanitization would
// collapse those).
func bodyOfSize(t *testing.T, n int) []byte {
	t.Helper()
	head, tail := `{"error":{"type":"big_exception","reason":"PREFIX-`, `"}}`
	pad := n - len(head) - len(tail)
	if pad < 0 {
		t.Fatalf("size %d too small", n)
	}
	return []byte(head + strings.Repeat("x", pad) + tail)
}

// requireBounded fails unless msg is valid UTF-8 within limit bytes.
func requireBounded(t *testing.T, msg string, limit int) {
	t.Helper()
	if len(msg) > limit {
		t.Fatalf("message is %d bytes, want <= %d", len(msg), limit)
	}
	if !utf8.ValidString(msg) {
		t.Fatalf("message is not valid UTF-8: %q", msg)
	}
	if strings.ContainsAny(msg, "\r\n") {
		t.Fatalf("message spans lines: %q", msg)
	}
}

func TestNewRejectionBoundaries(t *testing.T) {
	t.Parallel()

	t.Run("exactly 65536 bytes extracts", func(t *testing.T) {
		body := bodyOfSize(t, MaxRejectionBodyBytes)
		if len(body) != 65536 {
			t.Fatalf("body is %d bytes", len(body))
		}
		msg := NewRejection("pipeline", "p", 400, body, nil, MaxPublicMessageBytes).Error()
		if !strings.HasPrefix(msg, msgP+"big_exception: PREFIX-xxx") {
			t.Fatalf("message %q lacks the extracted pair", msg[:min(len(msg), 120)])
		}
		if !strings.HasSuffix(msg, truncationMarker) {
			t.Fatalf("message does not end with %q", truncationMarker)
		}
		requireBounded(t, msg, MaxPublicMessageBytes)
	})

	t.Run("65537 bytes is oversized", func(t *testing.T) {
		body := bodyOfSize(t, MaxRejectionBodyBytes+1)
		msg := NewRejection("pipeline", "p", 400, body, nil, MaxPublicMessageBytes).Error()
		if want := msgP + detailOversized; msg != want {
			t.Fatalf("message = %q, want %q", msg, want)
		}
	})

	t.Run("valid JSON prefix followed by overflow bytes is oversized", func(t *testing.T) {
		body := append(bodyOfSize(t, MaxRejectionBodyBytes), []byte(`{"FAKE-OVERFLOW":1}`)...)
		msg := NewRejection("pipeline", "p", 400, body, nil, MaxPublicMessageBytes).Error()
		if want := msgP + detailOversized; msg != want {
			t.Fatalf("message = %q, want %q", msg, want)
		}
	})

	t.Run("long multibyte reason is cut on a rune boundary", func(t *testing.T) {
		for _, budget := range []int{MaxPublicMessageBytes, MaxPublicMessageBytes - len(opPrefix)} {
			for shift := range 4 {
				// Shift the cut point across every byte of the 2-byte rune.
				reason := strings.Repeat("a", shift) + strings.Repeat("é", 3000)
				body := `{"error":{"type":"t","reason":"` + reason + `"}}`
				msg := NewRejection("pipeline", "p", 400, []byte(body), nil, budget).Error()
				requireBounded(t, msg, budget)
				if !strings.HasSuffix(msg, truncationMarker) {
					t.Fatalf("budget %d shift %d: message does not end with %q", budget, shift, truncationMarker)
				}
				if budget == MaxPublicMessageBytes-len(opPrefix) {
					requireBounded(t, opPrefix+msg, MaxPublicMessageBytes)
				}
			}
		}
	})

	t.Run("deeply nested caused_by stays bounded", func(t *testing.T) {
		var b strings.Builder
		const depth = 1000
		for i := range depth {
			fmt.Fprintf(&b, `{"type":"t%d","caused_by":`, i)
		}
		b.WriteString(`null`)
		b.WriteString(strings.Repeat("}", depth))
		body := `{"error":` + b.String() + `}`
		msg := NewRejection("pipeline", "p", 400, []byte(body), nil, MaxPublicMessageBytes).Error()
		if want := msgP + "t0; t1; t2; t3; " + omittedMarker; msg != want {
			t.Fatalf("message = %q, want %q", msg, want)
		}
	})

	t.Run("many roots stay bounded", func(t *testing.T) {
		roots := make([]string, 1000)
		for i := range roots {
			roots[i] = fmt.Sprintf(`{"type":"r%d","caused_by":{"type":"c%d"}}`, i, i)
		}
		body := `{"error":{"root_cause":[` + strings.Join(roots, ",") + `]}}`
		msg := NewRejection("pipeline", "p", 400, []byte(body), nil, MaxPublicMessageBytes).Error()
		if want := msgP + "r0; r1; r2; r3; " + omittedMarker; msg != want {
			t.Fatalf("message = %q, want %q", msg, want)
		}
	})

	t.Run("many duplicate roots stay bounded", func(t *testing.T) {
		roots := strings.Repeat(`{"type":"same"},{},`, 2000)
		body := `{"error":{"root_cause":[` + roots + `{"type":"same"}]}}`
		msg := NewRejection("pipeline", "p", 400, []byte(body), nil, MaxPublicMessageBytes).Error()
		if want := msgP + "same"; msg != want {
			t.Fatalf("message = %q, want %q", msg, want)
		}
	})

	names := []struct {
		name string
		in   string
	}{
		{name: "ascii", in: strings.Repeat("n", 300)},
		{name: "multibyte", in: strings.Repeat("名", 100)},
		{name: "invalid UTF-8", in: strings.Repeat("a\xff", 200)},
		{name: "quotes and backslashes", in: strings.Repeat(`"\`, 150)},
		{name: "mixed escapes and multibyte", in: strings.Repeat(`é"\`, 100)},
	}
	for _, tt := range names {
		t.Run("long name "+tt.name, func(t *testing.T) {
			longReason := `{"error":{"type":"t","reason":"` + strings.Repeat("é", 3000) + `"}}`
			for _, budget := range []int{MaxPublicMessageBytes, MaxPublicMessageBytes - len(opPrefix)} {
				r := NewRejection("pipeline", tt.in, 400, []byte(longReason), nil, budget)
				display := r.ResourceName()
				if len(display) > maxResourceNameBytes || !utf8.ValidString(display) {
					t.Fatalf("display name is %d bytes, valid=%v, want <= %d valid UTF-8", len(display), utf8.ValidString(display), maxResourceNameBytes)
				}
				if !strings.HasSuffix(display, truncationMarker) {
					t.Fatalf("display name %q lacks %q", display, truncationMarker)
				}
				msg := r.Error()
				requireBounded(t, msg, budget)
				if budget == MaxPublicMessageBytes-len(opPrefix) {
					requireBounded(t, opPrefix+msg, MaxPublicMessageBytes)
				}
				if want := fmt.Sprintf("pipeline %q rejected with HTTP 400: t: é", display); !strings.HasPrefix(msg, want) {
					t.Fatalf("message %q does not keep the prefix %q", msg[:min(len(msg), 80)], want)
				}
			}
		})
	}

	t.Run("short name is kept whole", func(t *testing.T) {
		r := NewRejection("pipeline", "my-pipeline", 400, nil, nil, MaxPublicMessageBytes)
		if r.ResourceName() != "my-pipeline" {
			t.Fatalf("ResourceName = %q", r.ResourceName())
		}
	})

	t.Run("tiny budget still bounds the message", func(t *testing.T) {
		for _, budget := range []int{0, 5, len(truncationMarker), len(truncationMarker) + 1, 40} {
			msg := NewRejection("pipeline", "p", 400, []byte(`{"error":"x"}`), nil, budget).Error()
			requireBounded(t, msg, max(budget, 0))
		}
	})
}

func TestNewRejectionReadFailure(t *testing.T) {
	t.Parallel()

	sentinelText := "FAKE-READ-ERROR-TEXT"
	tests := []struct {
		name    string
		data    []byte
		readErr error
	}{
		{name: "unexpected EOF with partial data", data: []byte(`{"error":{"type":"t","reason":"partial`), readErr: io.ErrUnexpectedEOF},
		{name: "context canceled", data: nil, readErr: context.Canceled},
		{name: "wrapped error text is not rendered", data: []byte(`{"error":"x"}`), readErr: fmt.Errorf("%s: %w", sentinelText, context.Canceled)},
		{name: "read error wins over oversize", data: bodyOfSize(t, MaxRejectionBodyBytes+1), readErr: io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := NewRejection("pipeline", "p", 400, tt.data, tt.readErr, MaxPublicMessageBytes)
			if want := msgP + detailReadFailed; r.Error() != want {
				t.Fatalf("message = %q, want %q", r.Error(), want)
			}
			if !errors.Is(r, tt.readErr) {
				t.Fatalf("errors.Is(rejection, %v) = false", tt.readErr)
			}
			for _, leak := range []string{tt.readErr.Error(), sentinelText, "partial"} {
				if strings.Contains(r.Error(), leak) {
					t.Fatalf("message %q renders %q", r.Error(), leak)
				}
			}
		})
	}
}

func TestSanitizeRedactsSecrets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    string // exact output when non-empty
		secrets []string
	}{
		{
			name:    "URL userinfo",
			in:      "connect to https://elastic:FAKE-SECRET-1@es.example:9200/_bulk failed",
			want:    "connect to https://[REDACTED]@es.example:9200/_bulk failed",
			secrets: []string{"FAKE-SECRET-1", "elastic:"},
		},
		{
			name:    "URL userinfo with @ in the password",
			in:      "url http://u:FAKE@SECRET-2@host/x",
			want:    "url http://[REDACTED]@host/x",
			secrets: []string{"FAKE@", "SECRET-2"},
		},
		{
			name:    "Authorization Bearer",
			in:      "header Authorization: Bearer FAKE-SECRET-3 rejected",
			want:    "header Authorization: [REDACTED] rejected",
			secrets: []string{"FAKE-SECRET-3"},
		},
		{
			name:    "Authorization Basic",
			in:      "Authorization: Basic FAKE-SECRET-4",
			want:    "Authorization: [REDACTED]",
			secrets: []string{"FAKE-SECRET-4"},
		},
		{
			name:    "Authorization letters-only credential",
			in:      "authorization=bearer FAKESECRETLETTERS then",
			want:    "authorization=[REDACTED] then",
			secrets: []string{"FAKESECRETLETTERS"},
		},
		{
			name:    "quoted JSON authorization",
			in:      `{"authorization":"ApiKey FAKE-SECRET-5 more"}`,
			want:    `{"authorization":[REDACTED]}`,
			secrets: []string{"FAKE-SECRET-5"},
		},
		{
			name:    "mixed-case Password assignment",
			in:      "login Password=FAKE-SECRET-6 rejected",
			want:    "login Password=[REDACTED] rejected",
			secrets: []string{"FAKE-SECRET-6"},
		},
		{
			name:    "API_KEY colon",
			in:      "API_KEY: FAKE-SECRET-7",
			want:    "API_KEY: [REDACTED]",
			secrets: []string{"FAKE-SECRET-7"},
		},
		{
			name:    "apiKey quoted with spaces",
			in:      `apiKey="with spaces FAKE-SECRET-8 inside" ok`,
			want:    "apiKey=[REDACTED] ok",
			secrets: []string{"FAKE-SECRET-8", "spaces", "inside"},
		},
		{
			name:    "single-quoted secret key",
			in:      `{'secret': 'FAKE-SECRET-9'}`,
			want:    `{'secret': [REDACTED]}`,
			secrets: []string{"FAKE-SECRET-9"},
		},
		{
			name:    "client_secret and refresh_token",
			in:      "client_secret=FAKE-SECRET-10 refresh_token: FAKE-SECRET-11",
			want:    "client_secret=[REDACTED] refresh_token: [REDACTED]",
			secrets: []string{"FAKE-SECRET-10", "FAKE-SECRET-11"},
		},
		{
			name:    "X-Api-Key header",
			in:      "X-Api-Key: FAKE-SECRET-12",
			want:    "X-Api-Key: [REDACTED]",
			secrets: []string{"FAKE-SECRET-12"},
		},
		{
			name:    "double-quoted escaped quote",
			in:      `password="a\"FAKE-SECRET-13\" b" next`,
			want:    "password=[REDACTED] next",
			secrets: []string{"FAKE-SECRET-13"},
		},
		{
			name:    "single-quoted escaped quote and backslash",
			in:      `passwd='x\'FAKE-SECRET-14\\' next`,
			want:    "passwd=[REDACTED] next",
			secrets: []string{"FAKE-SECRET-14"},
		},
		{
			name:    "escaped backslash closes the value",
			in:      `secret="FAKE-SECRET-15\\" visible`,
			want:    "secret=[REDACTED] visible",
			secrets: []string{"FAKE-SECRET-15"},
		},
		{
			name:    "unterminated quoted credential",
			in:      `token: "FAKE SECRET 16 never closed`,
			want:    "token: [REDACTED]",
			secrets: []string{"FAKE SECRET 16", "closed"},
		},
		{
			name:    "quoted sensitive query value",
			in:      `GET https://es.example/x?token="FAKE SECRET 17"&format=json`,
			want:    "GET https://es.example/x?token=[REDACTED]&format=json",
			secrets: []string{"FAKE SECRET 17", "SECRET"},
		},
		{
			name:    "access_token key",
			in:      "access_token=FAKE-SECRET-18",
			want:    "access_token=[REDACTED]",
			secrets: []string{"FAKE-SECRET-18"},
		},
		{
			name:    "pass key with spaced equals",
			in:      "pass = FAKE-SECRET-19",
			want:    "pass = [REDACTED]",
			secrets: []string{"FAKE-SECRET-19"},
		},
		{
			name: "control characters and separators become one line",
			in:   "line1\r\nline2\nline3\rline4\tline5 line6 line7\x00line8\u0085line9",
			want: "line1 line2 line3 line4 line5 line6 line7 line8 line9",
		},
		{
			name:    "line break cannot split a key from its value",
			in:      "password=\nFAKE-SECRET-20",
			want:    "password= [REDACTED]",
			secrets: []string{"FAKE-SECRET-20"},
		},
		{name: "invalid UTF-8 is replaced", in: "bad\xffbyte", want: "bad�byte"},
		{name: "harmless processor reason", in: "No processor type exists with name [fixture_proc]", want: "No processor type exists with name [fixture_proc]"},
		{name: "harmless token prose", in: "unexpected token at position 5", want: "unexpected token at position 5"},
		{name: "harmless basic prose", in: "basic authentication failed", want: "basic authentication failed"},
		{name: "longer word is not a secret key", in: "passport=visible bypass=visible password_hint=visible", want: "passport=visible bypass=visible password_hint=visible"},
		// '_' is a key boundary: these are real credential variable names
		// (ELASTIC_PASSWORD is the Elasticsearch image's own), and leaking one
		// is worse than over-redacting a harmless compound key.
		{
			name:    "underscore-prefixed compound keys",
			in:      `ELASTIC_PASSWORD=FAKE-SECRET-21 DB_PASSWORD: FAKE-SECRET-22 {"elastic_password":"FAKE-SECRET-23"} ES_API_KEY=FAKE-SECRET-24`,
			want:    `ELASTIC_PASSWORD=[REDACTED] DB_PASSWORD: [REDACTED] {"elastic_password":[REDACTED]} ES_API_KEY=[REDACTED]`,
			secrets: []string{"FAKE-SECRET-21", "FAKE-SECRET-22", "FAKE-SECRET-23", "FAKE-SECRET-24"},
		},
		{
			name:    "escaped-quote JSON fragment inside a reason",
			in:      `body {\"password\":\"FAKE-SECRET-26\",\"api_key\":\"FAKE SECRET 27\"} end`,
			want:    `body {\"password\":[REDACTED],\"api_key\":[REDACTED]} end`,
			secrets: []string{"FAKE-SECRET-26", "FAKE SECRET 27", "SECRET 27"},
		},
		{
			name:    "camelCase multi-word keys",
			in:      "accessToken=FAKE-SECRET-28 authToken: FAKE-SECRET-29 clientSecret=FAKE-SECRET-30 refreshToken=FAKE-SECRET-31",
			want:    "accessToken=[REDACTED] authToken: [REDACTED] clientSecret=[REDACTED] refreshToken=[REDACTED]",
			secrets: []string{"FAKE-SECRET-28", "FAKE-SECRET-29", "FAKE-SECRET-30", "FAKE-SECRET-31"},
		},
		// Known limit, pinned so it is a decision rather than a surprise: an
		// arbitrary camelCase prefix is not a key boundary (RE2 under (?i)
		// cannot see a case change). Changing this row means widening the
		// recognized-credential guarantee in the README, not just the regex.
		{name: "camelCase prefix is not recognized (known limit)", in: "userPassword=visible", want: "userPassword=visible"},
		{
			name:    "scheme word after a non-Authorization key",
			in:      "token: Bearer FAKE-SECRET-25 rejected",
			want:    "token: [REDACTED] rejected",
			secrets: []string{"FAKE-SECRET-25"},
		},
		// Accepted over-redaction: "pass" and "token" are spec-listed keys, and
		// telling a credential from prose in key: value form would need the
		// heuristic guessing the spec rules out. Pinned so a "fix" that makes
		// this prose survive cannot quietly reopen a credential leak.
		{name: "key-colon prose is redacted (accepted)", in: "did not pass: expected 3", want: "did not pass: [REDACTED] 3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := sanitize(tt.in)
			if tt.want != "" && got != tt.want {
				t.Fatalf("sanitize(%q)\n got: %q\nwant: %q", tt.in, got, tt.want)
			}
			for _, s := range tt.secrets {
				if strings.Contains(got, s) {
					t.Fatalf("sanitize(%q) = %q still contains %q", tt.in, got, s)
				}
			}
			if !utf8.ValidString(got) || strings.ContainsAny(got, "\r\n\t  ") {
				t.Fatalf("sanitize(%q) = %q is not one valid UTF-8 line", tt.in, got)
			}
		})
	}

	t.Run("each sensitive query key", func(t *testing.T) {
		t.Parallel()
		keys := []string{
			"password", "passwd", "pass", "api_key", "api-key", "apiKey", "token",
			"access_token", "refresh_token", "client_secret", "secret", "Authorization",
		}
		for _, key := range keys {
			in := "GET https://es.example/x?a=1&" + key + "=FAKE-SECRET-Q-" + key + "&b=2"
			got := sanitize(in)
			if strings.Contains(got, "FAKE-SECRET-Q") {
				t.Fatalf("sanitize(%q) = %q leaks the %s value", in, got, key)
			}
			if !strings.Contains(got, key+"=[REDACTED]") {
				t.Fatalf("sanitize(%q) = %q, want %s=[REDACTED]", in, got, key)
			}
		}
	})

	t.Run("redaction precedes truncation", func(t *testing.T) {
		t.Parallel()
		// The quoted value spans the 2,048-byte cut point. Truncating first
		// would leave a partial value for redaction to miss or mangle.
		reason := `safe reason prefix password="FAKE-SECRET-HEAD ` + strings.Repeat("z", 3000) +
			` FAKE-SECRET-TAIL" trailing safe text ` + strings.Repeat("w", 3000)
		body := `{"error":{"type":"t","reason":` + fmt.Sprintf("%q", reason) + `}}`
		msg := NewRejection("pipeline", "p", 400, []byte(body), nil, MaxPublicMessageBytes-len(opPrefix)).Error()
		for _, s := range []string{"FAKE-SECRET-HEAD", "FAKE-SECRET-TAIL", "zzzz"} {
			if strings.Contains(msg, s) {
				t.Fatalf("message leaks %q: %q", s, msg[:min(len(msg), 160)])
			}
		}
		if !strings.HasPrefix(msg, msgP+"t: safe reason prefix password=[REDACTED] trailing safe text www") {
			t.Fatalf("message lost its safe prefix: %q", msg[:min(len(msg), 160)])
		}
		requireBounded(t, opPrefix+msg, MaxPublicMessageBytes)
	})
}

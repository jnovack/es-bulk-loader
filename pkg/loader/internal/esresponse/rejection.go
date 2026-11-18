package esresponse

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// MaxRejectionBodyBytes is the most response content diagnostics read;
	// one more byte is read only to detect an oversized body.
	MaxRejectionBodyBytes = 64 << 10 // 65536
	// MaxPublicMessageBytes bounds the complete public err.Error() at direct
	// Rejection failure sites, operation prefix included (runlog.Fail applies
	// the Op-relative bound). For the two wrapped diagnostic paths (mapping
	// fetch and transform _stats) it bounds only the inner Rejection text.
	MaxPublicMessageBytes = 2048

	maxDiagnosticPairs   = 4
	maxCausedByDepth     = 4
	maxResourceNameBytes = 256
	truncationMarker     = " [truncated]"
	omittedMarker        = "[further causes omitted]"
	redacted             = "[REDACTED]"
)

// Fixed fallback details. None of them quotes the body or the read error:
// an excerpt of an unparsed or partial response is exactly the raw text the
// diagnostics must never render.
const (
	detailReadFailed   = "ES error detail unavailable (response body read failed)"
	detailOversized    = "ES error detail unavailable (response exceeded 65536 bytes)"
	detailEmpty        = "ES error detail unavailable (empty response body)"
	detailNotJSON      = "ES error detail unavailable (response is not a single JSON value)"
	detailUnrecognized = "ES error detail unavailable (no recognizable error field)"
)

// Rejection is the cause attached to an Elasticsearch HTTP rejection. Its
// message is computed once, already sanitized and bounded; Unwrap exposes a
// body-read error (for example context.Canceled) so errors.Is identity
// survives without its text being rendered. It keeps no raw body or field.
type Rejection struct {
	msg          string
	err          error
	resourceName string
}

// Error returns the sanitized, bounded rejection message.
func (r *Rejection) Error() string { return r.msg }

// Unwrap returns the response-body read error, or nil.
func (r *Rejection) Unwrap() error { return r.err }

// ResourceName returns the sanitized, bounded display name for logging, the
// same name the message shows. Requests must keep using the original name.
func (r *Rejection) ResourceName() string { return r.resourceName }

// NewRejection returns the cause for a rejected request on resource name
// with HTTP status. data is the response body as read by the caller, at
// most MaxRejectionBodyBytes+1 bytes (more than MaxRejectionBodyBytes means
// oversized); readErr is the read error, if any, and takes precedence over
// every other fallback. It performs no I/O. The message is
// `<resource> "<name>" rejected with HTTP <status>: <detail>`, at most
// maxMessageBytes bytes of valid UTF-8 on one line.
//
// detail is up to four sanitized Elasticsearch type/reason pairs (root_cause
// entries in order, then the top-level error object, then each one's
// caused_by chain to four levels), or a fixed explanation when the body is
// unreadable, oversized, empty, not a single JSON value or has no
// recognizable error. Sanitization redacts URL userinfo and the values of
// common secret keys, query parameters and Authorization headers; free-text
// prose it cannot recognize as a credential is kept. maxMessageBytes must
// leave room for the prefix (the name is capped at 256 bytes) and the
// truncation marker; a smaller budget still bounds the message but may cut
// the prefix.
func NewRejection(
	resource, name string, status int, data []byte,
	readErr error, maxMessageBytes int,
) *Rejection {
	displayName := sanitize(name)
	if len(displayName) > maxResourceNameBytes {
		displayName = truncateUTF8(displayName, maxResourceNameBytes-len(truncationMarker)) + truncationMarker
	}

	detail := rejectionDetail(data, readErr)
	var msg string
	if name == "" {
		// Some requests have no single resource (bulk, list requests); an empty
		// quoted name would print `bulk ""`, so the name is omitted instead.
		msg = fmt.Sprintf("%s rejected with HTTP %d: %s", resource, status, detail)
	} else {
		msg = fmt.Sprintf("%s %q rejected with HTTP %d: %s", resource, displayName, status, detail)
	}

	return &Rejection{msg: boundMessage(msg, maxMessageBytes), err: readErr, resourceName: displayName}
}

// boundMessage cuts msg to at most max bytes at a rune boundary, ending with
// the truncation marker when the budget can fit it. It is the one cut
// algorithm shared by NewRejection and Bounded. Cutting the whole message at
// the budget only ever cuts the detail when the budget fits the prefix;
// sanitization already ran, so a cut can split a placeholder but never expose a
// secret.
func boundMessage(msg string, max int) string {
	if len(msg) <= max {
		return msg
	}
	if max > len(truncationMarker) {
		return truncateUTF8(msg, max-len(truncationMarker)) + truncationMarker
	}
	return truncateUTF8(msg, max)
}

// Bounded returns r unchanged when its message fits max bytes, otherwise a
// copy whose message is cut at a rune boundary to max with the truncation
// marker when the budget can fit it, exactly as NewRejection cuts.
// max <= 0 yields an empty message. The copy preserves the read error and
// resource display name and never modifies the receiver.
func (r *Rejection) Bounded(max int) *Rejection {
	if max < 0 {
		max = 0
	}
	if len(r.msg) <= max {
		return r
	}
	return &Rejection{msg: boundMessage(r.msg, max), err: r.err, resourceName: r.resourceName}
}

// ReadBounded reads at most MaxRejectionBodyBytes+1 bytes of an error
// response body, so a hostile or huge body cannot drive an unbounded read or
// allocation; the probe byte lets NewRejection detect oversize. It reads only
// the caller-supplied reader, never closes it, sends no request and does not
// log.
func ReadBounded(body io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(body, MaxRejectionBodyBytes+1))
}

// Oversized reports whether data, as returned by ReadBounded, hit the probe
// byte and is therefore a cut body. Body classifiers must treat a cut body as
// no match: they are substring checks, so a cut body could still match a token
// in its first MaxRejectionBodyBytes bytes or lose the "error":{ marker they
// look for.
func Oversized(data []byte) bool { return len(data) > MaxRejectionBodyBytes }

// rejectionDetail selects the fallback or the extracted pairs, in the
// documented precedence: read failure, oversize, empty, not one JSON value,
// no recognizable pair.
func rejectionDetail(data []byte, readErr error) string {
	switch {
	case readErr != nil:
		return detailReadFailed
	case len(data) > MaxRejectionBodyBytes:
		return detailOversized
	case len(bytes.TrimSpace(data)) == 0:
		return detailEmpty
	}

	v, ok := decodeSingleJSON(data)
	if !ok {
		return detailNotJSON
	}
	top, _ := v.(map[string]any)
	pairs, more := extractErrorPairs(top["error"])
	if len(pairs) == 0 {
		return detailUnrecognized
	}
	parts := make([]string, 0, len(pairs)+1)
	for _, p := range pairs {
		switch {
		case p.typ != "" && p.reason != "":
			parts = append(parts, p.typ+": "+p.reason)
		case p.typ != "":
			parts = append(parts, p.typ)
		default:
			parts = append(parts, p.reason)
		}
	}
	if more {
		parts = append(parts, omittedMarker)
	}
	return strings.Join(parts, "; ")
}

// decodeSingleJSON decodes data as exactly one JSON value: trailing
// non-whitespace (garbage or a second value) is not a diagnostic.
func decodeSingleJSON(data []byte) (any, bool) {
	dec := json.NewDecoder(bytes.NewReader(data))
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, false
	}
	return v, true
}

// errorPair is one sanitized Elasticsearch type/reason; either may be empty.
type errorPair struct {
	typ, reason string
}

// pairCollector keeps at most maxDiagnosticPairs distinct non-empty pairs.
// more records omitted causes: a fifth distinct pair, or a chain continuing
// past maxCausedByDepth.
type pairCollector struct {
	pairs []errorPair
	more  bool
	full  bool // a fifth distinct pair was seen; stop scanning
}

// add sanitizes candidate's string type and reason and keeps the pair unless
// it is empty or already kept. It reports false once a fifth distinct pair
// has been detected, which ends the scan.
func (c *pairCollector) add(candidate map[string]any) bool {
	typ, _ := candidate["type"].(string)
	reason, _ := candidate["reason"].(string)
	p := errorPair{typ: sanitize(typ), reason: sanitize(reason)}
	if p == (errorPair{}) || slices.Contains(c.pairs, p) {
		return true
	}
	if len(c.pairs) == maxDiagnosticPairs {
		c.more, c.full = true, true
		return false
	}
	c.pairs = append(c.pairs, p)
	return true
}

// chain walks owner's caused_by objects, level one being owner's first
// caused_by, for at most maxCausedByDepth levels. A further well-shaped
// caused_by object sets more without being inspected.
func (c *pairCollector) chain(owner map[string]any) bool {
	cur := owner
	for range maxCausedByDepth {
		next, ok := cur["caused_by"].(map[string]any)
		if !ok {
			return true
		}
		if !c.add(next) {
			return false
		}
		cur = next
	}
	if _, ok := cur["caused_by"].(map[string]any); ok {
		c.more = true
	}
	return true
}

// extractErrorPairs reads only the type/reason strings of an Elasticsearch
// error value: a plain string is a reason; an object contributes its
// root_cause objects in array order, itself, then each root's caused_by chain
// followed by its own. Every other key and every wrong shape is ignored.
// It is iterative and scans the already bounded decoded value in place, so
// work and retained storage are bounded by the body limit and the pair cap.
func extractErrorPairs(errValue any) (pairs []errorPair, more bool) {
	switch e := errValue.(type) {
	case string:
		if r := sanitize(e); r != "" {
			return []errorPair{{reason: r}}, false
		}
		return nil, false
	case map[string]any:
		c := &pairCollector{}
		c.collect(e)
		return c.pairs, c.more
	default:
		return nil, false
	}
}

// collect scans top in the documented order and returns early once a fifth
// distinct pair ends the scan.
func (c *pairCollector) collect(top map[string]any) {
	roots, _ := top["root_cause"].([]any)
	for _, root := range roots {
		if obj, ok := root.(map[string]any); ok && !c.add(obj) {
			return
		}
	}
	if !c.add(top) {
		return
	}
	for _, root := range roots {
		if obj, ok := root.(map[string]any); ok && !c.chain(obj) {
			return
		}
	}
	c.chain(top)
}

// userinfoPattern matches a URL's scheme and userinfo (greedy up to the last
// '@' before the path, so a password containing '@' is covered too).
var userinfoPattern = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.\-]*://)[^/\s?#]+@`)

// secretKeyPattern matches a sensitive key name, optionally quoted, followed
// by '=' or ':' and the whitespace before its value. The leading boundary
// excludes only letters and digits, so "passport" or "bypass" is not a key,
// while '_' and '-' are boundaries: ELASTIC_PASSWORD, DB_PASSWORD, ES_API_KEY
// and X-Api-Key are real credential names, and leaking them is worse than
// over-redacting a harmless "my_token=". The key must be followed directly by
// ':' or '=', so "password_hint=" is not matched. It covers assignments,
// JSON/YAML keys, headers and URL query parameters (preceded by '?' or '&')
// in one pass. The quote around a key may be backslash-escaped, because a
// JSON fragment quoted inside a reason string arrives as {\"password\":...}.
// Multi-word keys also match their camelCase spelling (accessToken), as
// apiKey does; an arbitrary camelCase prefix (userPassword) is not
// recognized, since RE2 under (?i) cannot express a case-change boundary.
var secretKeyPattern = regexp.MustCompile(
	`(?i)(?:^|[^a-z0-9])(?:\\*["'])?(password|passwd|pass|api[_-]?key|access[_-]?token|refresh[_-]?token|auth[_-]?token|client[_-]?secret|token|secret|authorization)(?:\\*["'])?\s*[:=]\s*`)

// authSchemes are the Authorization scheme words whose credential follows
// after a space and must be redacted with them.
var authSchemes = []string{"basic", "bearer", "apikey", "digest", "negotiate", "token"}

// sanitize returns s as one line of valid UTF-8 with recognizable secrets
// replaced by [REDACTED]: invalid UTF-8 becomes U+FFFD, control runes become
// spaces, whitespace (including U+2028/U+2029) collapses to single spaces,
// then URL userinfo and the values of sensitive keys are redacted. It is
// bounded text sanitization, not a promise to recognize encoded or
// free-text credentials.
func sanitize(s string) string {
	s = strings.ToValidUTF8(s, string(utf8.RuneError))
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	if s == "" {
		return ""
	}
	s = userinfoPattern.ReplaceAllString(s, "${1}"+redacted+"@")
	return redactSecretValues(s)
}

// redactSecretValues replaces the value after every sensitive key with
// [REDACTED], scanning left to right so a redacted value is never rescanned.
func redactSecretValues(s string) string {
	var b strings.Builder
	pos := 0
	for pos < len(s) {
		m := secretKeyPattern.FindStringSubmatchIndex(s[pos:])
		if m == nil {
			break
		}
		start := pos + m[1]
		end := valueEnd(s, start)
		// A bare scheme word ("Bearer", "Basic", ...) is not the secret; the
		// credential follows it. This applies after any sensitive key, not only
		// Authorization: "token: Bearer X" must lose X too.
		if end > start && isAuthScheme(s[start:end]) && end+1 < len(s) && s[end] == ' ' {
			end = valueEnd(s, end+1)
		}
		if end == start {
			// Nothing to redact after this key; continue past it.
			b.WriteString(s[pos:start])
			pos = start
			continue
		}
		b.WriteString(s[pos:start])
		b.WriteString(redacted)
		pos = end
	}
	b.WriteString(s[pos:])
	return b.String()
}

// isAuthScheme reports whether v is a bare Authorization scheme word.
func isAuthScheme(v string) bool {
	return slices.ContainsFunc(authSchemes, func(s string) bool { return strings.EqualFold(v, s) })
}

// valueEnd returns the end of the value starting at s[i]. A value opened by
// a double or single quote ends after its matching unescaped quote; backslash
// escapes are consumed in pairs so an escaped quote never terminates it, and
// an unterminated quoted value runs to the end of s. An unquoted value ends at
// the next space (s is already whitespace-collapsed).
func valueEnd(s string, i int) int {
	if i >= len(s) {
		return len(s)
	}
	// An escaped-quote value (\"a b\") comes from a JSON fragment quoted
	// inside the reason. It ends at the same escaped quote, not at the first
	// space, so a spaced credential is redacted whole.
	if s[i] == '\\' {
		j := i
		for j < len(s) && s[j] == '\\' {
			j++
		}
		if j < len(s) && (s[j] == '"' || s[j] == '\'') {
			closer := s[i : j+1]
			if k := strings.Index(s[j+1:], closer); k >= 0 {
				return j + 1 + k + len(closer)
			}
			return len(s)
		}
	}
	if q := s[i]; q == '"' || q == '\'' {
		for j := i + 1; j < len(s); j++ {
			switch s[j] {
			case '\\':
				j++
			case q:
				return j + 1
			}
		}
		return len(s)
	}
	if j := strings.IndexByte(s[i:], ' '); j >= 0 {
		return i + j
	}
	return len(s)
}

// truncateUTF8 returns the longest prefix of s of at most max bytes that
// ends on a rune boundary.
func truncateUTF8(s string, max int) string {
	if max <= 0 {
		return ""
	}
	if max >= len(s) {
		return s
	}
	i := max
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}
	return s[:i]
}

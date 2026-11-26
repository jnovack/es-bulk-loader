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
	// fetch and transform _stats) it bounds only the inner Rejection text. For
	// failure text from 2xx responses it is the per-field budget (enrich
	// es_error, bulk error_type/error_reason, transform reason) or, for the
	// flush cause, the complete direct-cause bound.
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
//
// It also retains the structure the message was rendered from (the resource
// literal, the HTTP status and the sanitized type/reason pairs, each field
// bounded) so package loader can project it into its public
// ElasticsearchError without re-parsing the text. Every field is immutable
// after construction: Bounded copies share the pairs slice, and Pairs returns
// a fresh copy.
type Rejection struct {
	msg          string
	err          error
	resource     string
	resourceName string
	status       int
	pairs        []Pair
}

// Error returns the sanitized, bounded rejection message.
func (r *Rejection) Error() string { return r.msg }

// Unwrap returns the response-body read error, or nil.
func (r *Rejection) Unwrap() error { return r.err }

// ResourceName returns the sanitized, bounded display name for logging, the
// same name the message shows. Requests must keep using the original name.
func (r *Rejection) ResourceName() string { return r.resourceName }

// Resource returns the internal resource literal the caller passed ("index",
// "pipeline", "bulk request", ...): a request noun, not a resource kind.
func (r *Rejection) Resource() string { return r.resource }

// Status returns the HTTP status of the rejected response, or 0 for a failure
// reported inside a successful response (NewDetail, NewDetailFrom).
func (r *Rejection) Status() int { return r.status }

// Pair is one sanitized Elasticsearch type/reason, each field at most
// MaxPublicMessageBytes bytes of single-line valid UTF-8; either may be empty,
// never both. It is exported only so package loader can read Pairs. It is
// also the retained storage form, built once by retainPairs from the
// extraction's errorPair values.
type Pair struct {
	Type, Reason string
}

// Pairs returns a fresh copy of the retained type/reason pairs (at most four,
// in extraction order), or nil when the message is a fixed fallback or the
// text-only NewDetail form. Mutating the result never changes r or any copy
// Bounded made of it.
func (r *Rejection) Pairs() []Pair {
	if len(r.pairs) == 0 {
		return nil
	}
	return slices.Clone(r.pairs)
}

// NewRejection returns the cause for a rejected request on resource name
// with HTTP status. data is the response body as read by the caller, at
// most MaxRejectionBodyBytes+1 bytes (more than MaxRejectionBodyBytes means
// oversized); readErr is the read error, if any, and takes precedence over
// every other fallback. It performs no I/O. The message is
// `<resource> "<name>" rejected with HTTP <status>: <detail>`, at most
// maxMessageBytes bytes of valid UTF-8 on one line. An empty name omits the
// quoted name: `<resource> rejected with HTTP <status>: <detail>`.
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
	displayName := capDisplayName(name)

	detail := rejectionDetail(data, readErr)
	var msg string
	if name == "" {
		// Some requests have no single resource (bulk, list requests); an empty
		// quoted name would print `bulk ""`, so the name is omitted instead.
		msg = fmt.Sprintf("%s rejected with HTTP %d: %s", resource, status, detail.text)
	} else {
		msg = fmt.Sprintf("%s %q rejected with HTTP %d: %s", resource, displayName, status, detail.text)
	}

	return &Rejection{
		msg: boundMessage(msg, maxMessageBytes), err: readErr,
		resource: resource, resourceName: displayName, status: status, pairs: detail.pairs,
	}
}

// capDisplayName returns name sanitized and capped at maxResourceNameBytes.
// It is shared by NewRejection and NewDetail so both display names agree.
func capDisplayName(name string) string {
	displayName := sanitize(name)
	if len(displayName) > maxResourceNameBytes {
		displayName = truncateUTF8(displayName, maxResourceNameBytes-len(truncationMarker)) + truncationMarker
	}
	return displayName
}

// NewDetail returns a Rejection for a failure reported inside a successful
// response: `<resource> "<name>" <text>`, or `<resource> <text>` when name is
// empty. text must already be sanitized (ErrorDetail, ShardFailureDetail,
// SanitizeText); resource is an internal literal; name is sanitized and
// capped like NewRejection's. It carries no read error. The message is capped
// at MaxPublicMessageBytes and runlog.Fail bounds it again to its Op.
//
// It is the text-only form: Resource and ResourceName are kept, Status is 0
// and Pairs is nil, because text alone carries no decoded structure. Use
// NewDetailFrom when the text comes from a FailureDetail.
func NewDetail(resource, name, text string) *Rejection {
	return newDetail(resource, name, text, nil)
}

// NewDetailFrom is NewDetail for a decoded FailureDetail: the message is
// `<resource> "<name>" <prefix><detail>` (or `<resource> <prefix><detail>`
// when name is empty), where detail is d.Text(MaxPublicMessageBytes), and the
// Rejection retains d's pairs. Status is 0. prefix is the caller's verb and
// context, for example `flush had 3 shard failures; first: ` or `failed: `;
// it is an internal literal or already sanitized, like NewDetail's text.
// Taking d instead of rendered text keeps the message and the retained pairs
// from one extraction, so they cannot disagree.
func NewDetailFrom(resource, name, prefix string, d FailureDetail) *Rejection {
	return newDetail(resource, name, prefix+d.Text(MaxPublicMessageBytes), d.pairs)
}

// newDetail is the one constructor behind NewDetail and NewDetailFrom.
func newDetail(resource, name, text string, pairs []Pair) *Rejection {
	displayName := capDisplayName(name)
	var msg string
	if name == "" {
		msg = resource + " " + text
	} else {
		msg = fmt.Sprintf("%s %q %s", resource, displayName, text)
	}
	return &Rejection{
		msg: boundMessage(msg, MaxPublicMessageBytes), resource: resource,
		resourceName: displayName, pairs: pairs,
	}
}

// FailureDetail is one Elasticsearch error value decoded from a successful
// response: its sanitized rendered text and the sanitized type/reason pairs
// that text was rendered from, each pair field bounded to
// MaxPublicMessageBytes. A fixed "ES error detail unavailable (...)" fallback
// has no pairs. The rendered text is not bounded until Text; like ErrorDetail,
// a successful body has no 64 KiB input cap. The zero value has empty text and
// no pairs. ParseErrorValue and ParseShardFailure are the producers;
// NewDetailFrom turns one into a Rejection.
type FailureDetail struct {
	text  string
	pairs []Pair
}

// Text returns the detail text bounded to max bytes, with the same cut and
// marker as every other helper here.
func (d FailureDetail) Text(max int) string { return boundMessage(d.text, max) }

// ParseErrorValue decodes an Elasticsearch error value carried inside a
// successful response (for example a task's "error"), with ErrorDetail's
// rules: a value that does not decode, or yields no type/reason pair (a
// number, an array, an empty object), is the fixed fallback, never the raw
// value.
func ParseErrorValue(raw json.RawMessage) FailureDetail {
	v, ok := decodeSingleJSON(raw)
	if !ok {
		return FailureDetail{text: detailNotJSON}
	}
	return errorValueDetail(v)
}

// ParseShardFailure decodes one delete-by-query "failures" entry with
// ShardFailureDetail's rules: the entry's "cause" object (bulk-phase
// failures), else its "reason" object or string (search-phase failures); any
// other shape is the fixed fallback. A cause object wins even when it has no
// recognizable pair; every other key is ignored.
func ParseShardFailure(raw json.RawMessage) FailureDetail {
	v, ok := decodeSingleJSON(raw)
	if !ok {
		return FailureDetail{text: detailNotJSON}
	}
	entry, ok := v.(map[string]any)
	if !ok {
		return FailureDetail{text: detailUnrecognized}
	}
	if cause, ok := entry["cause"].(map[string]any); ok {
		return errorValueDetail(cause)
	}
	switch reason := entry["reason"].(type) {
	case map[string]any:
		return errorValueDetail(reason)
	case string:
		return errorValueDetail(reason)
	}
	return FailureDetail{text: detailUnrecognized}
}

// ErrorDetail returns the sanitized "<type>: <reason>[; ...]" text of an
// Elasticsearch error value carried inside a successful response (for example
// a task's "error"), bounded to max bytes. raw is decoded here; a value that
// does not decode, or yields no type/reason pair (a number, an array, an empty
// object), returns the fixed "ES error detail unavailable (...)" text, never
// the raw value. The 64 KiB HTTP-rejection input limit does not apply: the
// successful body was already fully decoded by the caller. It is the text
// adapter of ParseErrorValue.
func ErrorDetail(raw json.RawMessage, max int) string {
	return ParseErrorValue(raw).Text(max)
}

// ShardFailureDetail returns the sanitized detail of one delete-by-query
// "failures" entry, bounded to max bytes. The error is taken from the entry's
// "cause" object (bulk-phase failures), else its "reason" object or string
// (search-phase failures); any other shape returns the fixed "ES error detail
// unavailable (...)" text. A cause object wins even when it has no
// recognizable pair; every other key is ignored. It is the text adapter of
// ParseShardFailure.
func ShardFailureDetail(raw json.RawMessage, max int) string {
	return ParseShardFailure(raw).Text(max)
}

// SanitizeText returns s with recognizable secrets redacted, as one line of
// valid UTF-8, bounded to max bytes. It is for Elasticsearch reason strings
// already decoded from a successful response; it does not recognize every
// secret in free-text prose.
func SanitizeText(s string, max int) string {
	return boundMessage(sanitize(s), max)
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
// max <= 0 yields an empty message. The copy preserves the read error,
// resource literal, display name, status and pairs (runlog.Fail re-bounds
// every direct cause, so dropping one here would erase the structure of
// exactly the long rejections that need it), and never modifies the receiver.
func (r *Rejection) Bounded(max int) *Rejection {
	if max < 0 {
		max = 0
	}
	if len(r.msg) <= max {
		return r
	}
	cut := *r
	cut.msg = boundMessage(r.msg, max)
	return &cut
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
// look for. An absence-based classifier (IsUnsupportedEnrichAPI) must also
// treat a failed read as no match; a token-presence classifier may still match
// a token in the bytes that did arrive.
func Oversized(data []byte) bool { return len(data) > MaxRejectionBodyBytes }

// rejectionDetail selects the fallback or the extracted pairs, in the
// documented precedence: read failure, oversize, empty, not one JSON value,
// no recognizable pair. Every fallback has no pairs.
func rejectionDetail(data []byte, readErr error) FailureDetail {
	switch {
	case readErr != nil:
		return FailureDetail{text: detailReadFailed}
	case len(data) > MaxRejectionBodyBytes:
		return FailureDetail{text: detailOversized}
	case len(bytes.TrimSpace(data)) == 0:
		return FailureDetail{text: detailEmpty}
	}

	v, ok := decodeSingleJSON(data)
	if !ok {
		return FailureDetail{text: detailNotJSON}
	}
	top, _ := v.(map[string]any)
	return errorValueDetail(top["error"])
}

// errorValueDetail renders an already-decoded Elasticsearch error value as
// "<type>: <reason>[; ...]", or detailUnrecognized (with no pairs) when it has
// no pair. It is the one extraction-to-text path for HTTP-error bodies and 2xx
// error values: the pairs are extracted once, the text is rendered from the
// full sanitized pairs, and only then are bounded copies retained.
//
// The order matters. Cutting each field before rendering would change the
// text whenever a caller's budget exceeds the field cap, and cutting inside
// extraction would merge pairs that differ only past the cap, changing
// de-duplication, slot use and the omitted-causes marker. Retained copies map
// one-to-one onto the rendered pairs, so pairs that become equal only after
// the cut keep their original positions.
func errorValueDetail(v any) FailureDetail {
	pairs, more := extractErrorPairs(v)
	if len(pairs) == 0 {
		return FailureDetail{text: detailUnrecognized}
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
	return FailureDetail{text: strings.Join(parts, "; "), pairs: retainPairs(pairs)}
}

// retainPairs returns a new slice of pairs with each field bounded to
// MaxPublicMessageBytes, for storage only. It is the one conversion from the
// extraction's errorPair to the retained Pair. Fields are already sanitized,
// so the cut can split a placeholder but never expose a secret.
func retainPairs(pairs []errorPair) []Pair {
	out := make([]Pair, len(pairs))
	for i, p := range pairs {
		out[i] = Pair{
			Type:   boundMessage(p.typ, MaxPublicMessageBytes),
			Reason: boundMessage(p.reason, MaxPublicMessageBytes),
		}
	}
	return out
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
// It is kept separate from the exported Pair because it is the unbounded
// transient form that de-duplication and rendering must see in full (bounding
// first would change existing text, order and omission markers), while Pair
// holds values already cut to MaxPublicMessageBytes by retainPairs; folding
// them would let unbounded values satisfy Pair's bound.
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
// It is iterative and scans the decoded value in place; retained storage is
// bounded by the pair cap. HTTP-error bodies are bounded by the body limit,
// but a 2xx field is not body-bounded, so scanning and sanitizing its strings
// is work proportional to its input.
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

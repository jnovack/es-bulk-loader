// Package logbuf gives tests a per-test JSON logger writing to a buffer, so a
// test reads its own log lines without touching the zerolog global. The caller
// attaches the logger with the runlog package, which this package cannot import. It is test
// support only and must not be imported by production packages.
package logbuf

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
)

// Buffer holds the JSON log lines one test's logger wrote.
type Buffer struct {
	buf bytes.Buffer
}

// New returns a JSON logger writing to the returned Buffer at zerolog's default
// Trace level, so "no field X on any line" sweeps also see Trace lines. Do not
// narrow it (for example to Debug): that silently drops Trace lines from every
// sweep. A per-logger level cannot override zerolog.SetGlobalLevel, which drops
// events below the global level regardless; tests must not raise it.
func New() (*zerolog.Logger, *Buffer) {
	b := &Buffer{}
	logger := zerolog.New(&b.buf)
	return &logger, b
}

// Nop returns a logger that discards everything, for tests that need a context
// logger but read no log lines.
func Nop() *zerolog.Logger {
	logger := zerolog.Nop()
	return &logger
}

// String returns the raw log text.
func (b *Buffer) String() string { return b.buf.String() }

// Lines decodes every log line.
func (b *Buffer) Lines(t testing.TB) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, raw := range bytes.Split(bytes.TrimSpace(b.buf.Bytes()), []byte("\n")) {
		raw = bytes.TrimSuffix(raw, []byte("\r"))
		if len(raw) == 0 {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal(raw, &line); err != nil {
			t.Fatalf("decode log line %q: %v", raw, err)
		}
		out = append(out, line)
	}
	return out
}

package logbuf

import "testing"

func TestNewCapturesDecodedLines(t *testing.T) {
	t.Parallel()

	logger, buf := New()
	if got := buf.Lines(t); len(got) != 0 {
		t.Fatalf("Lines on empty input = %v, want none", got)
	}
	logger.Trace().Msg("zero")
	logger.Debug().Str("k", "v").Msg("one")
	logger.Info().Msg("two")

	// Trace must be captured: the per-package "no body field on any line"
	// sweeps rely on seeing every level a production site can log at.
	lines := buf.Lines(t)
	if len(lines) != 3 || lines[0]["message"] != "zero" || lines[1]["message"] != "one" || lines[1]["k"] != "v" || lines[2]["message"] != "two" {
		t.Fatalf("Lines = %v, want the three logged lines in order (trace and debug included)", lines)
	}
	if buf.String() == "" {
		t.Fatal("String is empty")
	}
}

func TestLinesToleratesCRLF(t *testing.T) {
	t.Parallel()

	b := &Buffer{}
	b.buf.WriteString("{\"message\":\"a\"}\r\n{\"message\":\"b\"}\r\n")

	lines := b.Lines(t)
	if len(lines) != 2 || lines[0]["message"] != "a" || lines[1]["message"] != "b" {
		t.Fatalf("Lines = %v, want a and b", lines)
	}
}

func TestNopDiscards(t *testing.T) {
	t.Parallel()

	logger := Nop()
	logger.Error().Msg("dropped") // must not panic
	if logger == nil {
		t.Fatal("Nop returned nil")
	}
}

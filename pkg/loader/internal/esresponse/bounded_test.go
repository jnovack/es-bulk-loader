package esresponse

// LOG-RAWBODY-01 guards: the shared bounded body reader, the oversize
// classifier guard, the nameless Rejection form and the Op-relative bound.
// All bodies are synthetic.

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"unicode/utf8"
)

// countingReader serves n bytes of 'x' and records how much was requested and
// served and whether it was closed, so a test can prove the read bound.
type countingReader struct {
	remaining int
	served    int
	reads     int
	closed    bool
	// failAfterPartial makes the final read return its bytes and an error
	// together, the (n > 0, err != nil) case io.Reader permits.
	failAfterPartial error
}

func (c *countingReader) Read(p []byte) (int, error) {
	c.reads++
	if c.remaining == 0 {
		if c.failAfterPartial != nil {
			return 0, c.failAfterPartial
		}
		return 0, io.EOF
	}
	n := min(len(p), c.remaining)
	for i := range n {
		p[i] = 'x'
	}
	c.remaining -= n
	c.served += n
	if c.remaining == 0 && c.failAfterPartial != nil {
		return n, c.failAfterPartial
	}
	return n, nil
}

func (c *countingReader) Close() error { c.closed = true; return nil }

func TestReadBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		size      int
		wantLen   int
		oversized bool
	}{
		{"empty", 0, 0, false},
		{"under the limit is returned whole", 100, 100, false},
		{"exactly the limit is not oversized", MaxRejectionBodyBytes, MaxRejectionBodyBytes, false},
		{"one past the limit returns the probe byte", MaxRejectionBodyBytes + 1, MaxRejectionBodyBytes + 1, true},
		{"far past the limit stops at the probe byte", 50 * MaxRejectionBodyBytes, MaxRejectionBodyBytes + 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := &countingReader{remaining: tt.size}
			got, err := ReadBounded(src)
			if err != nil {
				t.Fatalf("ReadBounded: %v", err)
			}
			if len(got) != tt.wantLen {
				t.Fatalf("len = %d, want %d", len(got), tt.wantLen)
			}
			if src.served > MaxRejectionBodyBytes+1 {
				t.Fatalf("source served %d bytes, want at most %d", src.served, MaxRejectionBodyBytes+1)
			}
			if Oversized(got) != tt.oversized {
				t.Fatalf("Oversized = %t, want %t", Oversized(got), tt.oversized)
			}
			if src.closed {
				t.Fatal("ReadBounded must never close the reader it was given")
			}
		})
	}
}

func TestReadBoundedPropagatesReadError(t *testing.T) {
	t.Parallel()

	boom := errors.New("injected read failure")
	t.Run("error with no bytes", func(t *testing.T) {
		t.Parallel()
		got, err := ReadBounded(io.MultiReader(bytes.NewReader(nil), errReader{boom}))
		if !errors.Is(err, boom) || len(got) != 0 {
			t.Fatalf("got %q, %v; want empty and %v", got, err, boom)
		}
	})
	t.Run("partial bytes returned with the error in one read", func(t *testing.T) {
		t.Parallel()
		src := &countingReader{remaining: 10, failAfterPartial: boom}
		got, err := ReadBounded(src)
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want %v", err, boom)
		}
		if string(got) != strings.Repeat("x", 10) {
			t.Fatalf("partial bytes = %q, want the 10 bytes read before the error", got)
		}
	})
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestOversized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size int
		want bool
	}{
		{"nil", 0, false},
		{"exactly the bound", MaxRejectionBodyBytes, false},
		{"bound plus the probe byte", MaxRejectionBodyBytes + 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Oversized(make([]byte, tt.size)); got != tt.want {
				t.Fatalf("Oversized(%d bytes) = %t, want %t", tt.size, got, tt.want)
			}
		})
	}
}

// TestClassifiersTreatACutBodyAsNoMatch pins AC5 for the two esresponse body
// classifiers: a body past the read bound is cut, so a match in its first
// bytes must not count.
func TestClassifiersTreatACutBodyAsNoMatch(t *testing.T) {
	t.Parallel()

	pad := func(prefix string, size int) []byte {
		b := append([]byte(prefix), bytes.Repeat([]byte(" "), size-len(prefix))...)
		return b
	}
	typeMatch := `{"error":{"type":"index_not_found_exception"}}`
	tests := []struct {
		name string
		got  func(body []byte) bool
		body string
		want bool // result at exactly 64 KiB; false at 64 KiB + 1
	}{
		{"HasErrorType", func(b []byte) bool { return HasErrorType(b, "index_not_found_exception") }, typeMatch, true},
		{"IsUnsupportedEnrichAPI", func(b []byte) bool { return IsUnsupportedEnrichAPI(404, b) }, `no es error here`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.got(pad(tt.body, MaxRejectionBodyBytes)); got != tt.want {
				t.Fatalf("at exactly %d bytes = %t, want %t", MaxRejectionBodyBytes, got, tt.want)
			}
			if tt.got(pad(tt.body, MaxRejectionBodyBytes+1)) {
				t.Fatalf("a body of %d bytes matched; a cut body must be no match", MaxRejectionBodyBytes+1)
			}
		})
	}
}

func TestNewRejectionNamelessForm(t *testing.T) {
	t.Parallel()

	body := []byte(`{"error":{"type":"x","reason":"busy"}}`)
	tests := []struct {
		name, resource, rname, want string
	}{
		{"nameless omits the quoted name", "bulk request", "", "bulk request rejected with HTTP 503: x: busy"},
		{"named form is unchanged", "pipeline", "p", `pipeline "p" rejected with HTTP 503: x: busy`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := NewRejection(tt.resource, tt.rname, 503, body, nil, MaxPublicMessageBytes)
			if r.Error() != tt.want {
				t.Fatalf("message = %q, want %q", r.Error(), tt.want)
			}
		})
	}
	if got := NewRejection("bulk request", "", 503, body, nil, MaxPublicMessageBytes).ResourceName(); got != "" {
		t.Fatalf("ResourceName = %q, want empty", got)
	}
}

// productionOpBudget mirrors runlog.Fail's derivation for an Op literal.
func productionOpBudget(op string) int {
	return max(0, MaxPublicMessageBytes-len(op)-len(": "))
}

func TestRejectionBounded(t *testing.T) {
	t.Parallel()

	ops := []string{
		"Failed to create pipeline",
		"Failed to update alias",
		"Failed to delete pipeline; nuke mode could not find indices using it as default pipeline",
		"Failed to delete enrich policy; nuke mode could not find referencing pipelines",
	}
	t.Run("a fitting message is returned unchanged", func(t *testing.T) {
		t.Parallel()
		r := NewRejection("alias", "a", 500, []byte(`{"error":"short"}`), nil, MaxPublicMessageBytes)
		if got := r.Bounded(MaxPublicMessageBytes); got != r {
			t.Fatal("Bounded must return the receiver when the message already fits")
		}
	})
	t.Run("composition equals direct construction at production Op budgets", func(t *testing.T) {
		t.Parallel()
		for _, op := range ops {
			budget := productionOpBudget(op)
			for shift := range 4 {
				reason := strings.Repeat("a", shift) + strings.Repeat("é", 3000)
				body := []byte(`{"error":{"type":"t","reason":"` + reason + `"}}`)
				for _, name := range []string{"p", "", `we"ird` + strings.Repeat("n", 300)} {
					viaFail := NewRejection("pipeline", name, 400, body, nil, MaxPublicMessageBytes).Bounded(budget)
					direct := NewRejection("pipeline", name, 400, body, nil, budget)
					if viaFail.Error() != direct.Error() {
						t.Fatalf("op %q shift %d name %.8q: composed message differs from direct\n got: %q\nwant: %q",
							op, shift, name, viaFail.Error(), direct.Error())
					}
					if len(op)+len(": ")+len(viaFail.Error()) > MaxPublicMessageBytes {
						t.Fatalf("op %q: complete message is %d bytes, want <= %d", op, len(op)+2+len(viaFail.Error()), MaxPublicMessageBytes)
					}
				}
			}
		}
	})
	t.Run("tiny budgets respect size and UTF-8 without panicking", func(t *testing.T) {
		t.Parallel()
		body := []byte(`{"error":{"type":"é","reason":"` + strings.Repeat("é", 50) + `"}}`)
		r := NewRejection("pipeline", "é", 400, body, nil, MaxPublicMessageBytes)
		for budget := -2; budget <= len(truncationMarker)+5; budget++ {
			got := r.Bounded(budget).Error()
			if len(got) > max(budget, 0) {
				t.Fatalf("budget %d: message is %d bytes", budget, len(got))
			}
			if !utf8.ValidString(got) {
				t.Fatalf("budget %d: invalid UTF-8 %q", budget, got)
			}
		}
		if got := r.Bounded(0).Error(); got != "" {
			t.Fatalf("Bounded(0) = %q, want empty", got)
		}
		if got := r.Bounded(-1).Error(); got != "" {
			t.Fatalf("Bounded(-1) = %q, want empty", got)
		}
	})
	t.Run("a cut ends with the marker and preserves Unwrap and ResourceName", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("read failed")
		r := NewRejection("pipeline", "p", 400, nil, boom, MaxPublicMessageBytes)
		cut := r.Bounded(40)
		if !strings.HasSuffix(cut.Error(), truncationMarker) || len(cut.Error()) > 40 {
			t.Fatalf("cut = %q", cut.Error())
		}
		if !errors.Is(cut, boom) || cut.ResourceName() != "p" {
			t.Fatalf("Unwrap/ResourceName lost: %v %q", cut.Unwrap(), cut.ResourceName())
		}
		if r.Error() == cut.Error() {
			t.Fatal("Bounded must not modify the receiver")
		}
	})
}

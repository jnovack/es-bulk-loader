package failure

import (
	"errors"
	"testing"
)

// TestError pins the rendering and unwrap contract the loader boundary relies
// on: the same "op: cause" text as RunError, a nil cause replaced by
// errors.New(op), and nil-receiver safety.
func TestError(t *testing.T) {
	t.Parallel()

	cause := errors.New("boom")
	tests := []struct {
		name      string
		err       *Error
		wantMsg   string
		wantCause error
	}{
		{name: "with cause", err: New("creating index", cause), wantMsg: "creating index: boom", wantCause: cause},
		{name: "nil cause duplicates op", err: New("Failed to create index", nil), wantMsg: "Failed to create index: Failed to create index"},
		{name: "literal without cause", err: &Error{Op: "op"}, wantMsg: "op"},
		{name: "nil receiver", err: nil, wantMsg: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.err.Error(); got != tt.wantMsg {
				t.Fatalf("Error() = %q, want %q", got, tt.wantMsg)
			}
			if tt.wantCause != nil && !errors.Is(tt.err, tt.wantCause) {
				t.Fatalf("errors.Is(%v, cause) = false", tt.err)
			}
		})
	}

	if got := (*Error)(nil).Unwrap(); got != nil {
		t.Fatalf("nil Unwrap() = %v, want nil", got)
	}
	if New("op", nil).Unwrap() == nil {
		t.Fatal("New with a nil cause must still unwrap to a non-nil cause")
	}
}

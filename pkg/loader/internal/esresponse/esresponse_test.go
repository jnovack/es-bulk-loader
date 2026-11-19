package esresponse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"syscall"
	"testing"
)

// TestIsRetryableStatus pins the transient status set shared by bulk retries
// and the transform/enrich poll loops: exactly 429, 502, 503 and 504. A 500 or
// other definitive answer must not retry.
func TestIsRetryableStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
		want   bool
	}{
		{"429 too many requests", http.StatusTooManyRequests, true},
		{"502 bad gateway", http.StatusBadGateway, true},
		{"503 service unavailable", http.StatusServiceUnavailable, true},
		{"504 gateway timeout", http.StatusGatewayTimeout, true},
		{"0 no status", 0, false},
		{"200 ok", http.StatusOK, false},
		{"299 last 2xx", 299, false},
		{"400 bad request", http.StatusBadRequest, false},
		{"404 not found", http.StatusNotFound, false},
		{"500 internal server error", http.StatusInternalServerError, false},
		{"501 not implemented", http.StatusNotImplemented, false},
		{"505 http version not supported", http.StatusHTTPVersionNotSupported, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsRetryableStatus(tt.status); got != tt.want {
				t.Fatalf("IsRetryableStatus(%d) = %t, want %t", tt.status, got, tt.want)
			}
		})
	}
}

// fakeNetError is a net.Error whose Timeout and Temporary results are set
// directly, so the classifier table can pin every flag combination.
type fakeNetError struct{ timeout, temporary bool }

func (e fakeNetError) Error() string   { return "fake net error" }
func (e fakeNetError) Timeout() bool   { return e.timeout }
func (e fakeNetError) Temporary() bool { return e.temporary }

// TestIsRetryableTransportError pins the retry decision for transport errors.
// Only net.Error.Timeout() retries; Temporary() is deprecated and no longer
// consulted, so a temporary-only error must not retry. A revert to
// Timeout() || Temporary() fails the named row.
func TestIsRetryableTransportError(t *testing.T) {
	t.Parallel()

	errnoOp := func(e syscall.Errno) error {
		return &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", e)}
	}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context canceled", context.Canceled, false},
		{"wrapped context canceled", fmt.Errorf("send: %w", context.Canceled), false},
		{"timeout is retried", fakeNetError{timeout: true}, true},
		{"timeout with temporary is retried", fakeNetError{timeout: true, temporary: true}, true},
		{"neither flag is not retried", fakeNetError{}, false},
		{"temporary-only is no longer retried FIX-NETTEMP-01", fakeNetError{temporary: true}, false},
		{"wrapped timeout is retried", fmt.Errorf("send: %w", fakeNetError{timeout: true}), true},
		{"wrapped temporary-only is not retried", fmt.Errorf("send: %w", fakeNetError{temporary: true}), false},
		{"context deadline exceeded is retried", context.DeadlineExceeded, true},
		{"wrapped context deadline exceeded is retried", fmt.Errorf("send: %w", context.DeadlineExceeded), true},
		{"temporary DNS failure is not retried", &net.DNSError{Err: "temporary lookup failure", Name: "es.invalid", IsTemporary: true}, false},
		{"DNS timeout is retried", &net.DNSError{Err: "i/o timeout", Name: "es.invalid", IsTimeout: true}, true},
		{"EINTR is not retried", errnoOp(syscall.EINTR), false},
		{"EMFILE is not retried", errnoOp(syscall.EMFILE), false},
		{"ENFILE is not retried", errnoOp(syscall.ENFILE), false},
		{"ECONNREFUSED is not retried", errnoOp(syscall.ECONNREFUSED), false},
		{"ECONNRESET is not retried", errnoOp(syscall.ECONNRESET), false},
		{"io.EOF is retried", io.EOF, true},
		{"wrapped io.EOF is retried", fmt.Errorf("read: %w", io.EOF), true},
		{"io.ErrUnexpectedEOF is retried", io.ErrUnexpectedEOF, true},
		{"plain error is retried", errors.New("boom"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsRetryableTransportError(tt.err); got != tt.want {
				t.Fatalf("IsRetryableTransportError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestIsUnsupportedEnrichAPI pins that only a generic 404 without an ES error
// object counts as an unsupported (proxied or misrouted) enrich API.
func TestIsUnsupportedEnrichAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  int
		body    string
		readErr error
		want    bool
	}{
		{"generic 404", http.StatusNotFound, `{"error":true,"message":"Not Found"}`, nil, true},
		{"empty 404", http.StatusNotFound, ``, nil, true},
		{"missing policy is not unsupported", http.StatusNotFound, `{"error":{"type":"resource_not_found_exception"}}`, nil, false},
		{"index_not_found is an ES error", http.StatusNotFound, `{"error":{"type":"index_not_found_exception"}}`, nil, false},
		{"bare resource_not_found text", http.StatusNotFound, `resource_not_found_exception`, nil, false},
		{"non-404 never unsupported", http.StatusBadRequest, `{"error":true}`, nil, false},
		{"read error with empty 404", http.StatusNotFound, ``, errors.New("injected read failure"), false},
		{"read error with partial generic 404", http.StatusNotFound, `{"error":true`, errors.New("injected read failure"), false},
		{"read error with partial JSON 404", http.StatusNotFound, `{"err`, errors.New("injected read failure"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsUnsupportedEnrichAPI(tt.status, []byte(tt.body), tt.readErr); got != tt.want {
				t.Fatalf("IsUnsupportedEnrichAPI(%d, %s, %v) = %t, want %t", tt.status, tt.body, tt.readErr, got, tt.want)
			}
		})
	}
}

// TestHasErrorType pins the literal "type":"<name>" match.
func TestHasErrorType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		typ  string
		want bool
	}{
		{"exact type", `{"error":{"type":"index_not_found_exception"}}`, "index_not_found_exception", true},
		{"other type", `{"error":{"type":"resource_already_exists_exception"}}`, "index_not_found_exception", false},
		{"spaced JSON does not match", `{"error":{"type": "index_not_found_exception"}}`, "index_not_found_exception", false},
		{"empty body", ``, "index_not_found_exception", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := HasErrorType([]byte(tt.body), tt.typ); got != tt.want {
				t.Fatalf("HasErrorType(%s, %q) = %t, want %t", tt.body, tt.typ, got, tt.want)
			}
		})
	}
}

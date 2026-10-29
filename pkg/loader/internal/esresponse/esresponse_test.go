package esresponse

import (
	"net/http"
	"testing"
)

// TestIsUnsupportedEnrichAPI pins that only a generic 404 without an ES error
// object counts as an unsupported (proxied or misrouted) enrich API.
func TestIsUnsupportedEnrichAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"generic 404", http.StatusNotFound, `{"error":true,"message":"Not Found"}`, true},
		{"empty 404", http.StatusNotFound, ``, true},
		{"missing policy is not unsupported", http.StatusNotFound, `{"error":{"type":"resource_not_found_exception"}}`, false},
		{"index_not_found is an ES error", http.StatusNotFound, `{"error":{"type":"index_not_found_exception"}}`, false},
		{"bare resource_not_found text", http.StatusNotFound, `resource_not_found_exception`, false},
		{"non-404 never unsupported", http.StatusBadRequest, `{"error":true}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsUnsupportedEnrichAPI(tt.status, []byte(tt.body)); got != tt.want {
				t.Fatalf("IsUnsupportedEnrichAPI(%d, %s) = %t, want %t", tt.status, tt.body, got, tt.want)
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

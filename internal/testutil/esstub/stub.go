// Package esstub provides a strict, standard-library HTTP stub for tests that
// talk to Elasticsearch through the real client. It is test support only and
// must not be imported by production packages; routes are ordinary
// http.ServeMux method/path patterns and unexpected requests fail the test.
package esstub

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"
)

// Request is a by-value snapshot of one request seen by a Stub.
type Request struct {
	Method   string
	Path     string
	RawQuery string
	Header   http.Header
	Body     []byte
}

// reporter is the slice of testing.TB the stub needs. It is a seam so the
// harness's own tests can observe an expected Errorf without failing the
// enclosing test.
type reporter interface {
	Errorf(format string, args ...any)
}

// Stub is a strict fake Elasticsearch HTTP endpoint. Register routes with
// Handle before issuing requests; any request without a matching route is
// reported through Errorf and answered with HTTP 500.
type Stub struct {
	rep    reporter
	server *httptest.Server
	mux    *http.ServeMux

	mu    sync.Mutex
	trace []Request

	closeOnce sync.Once
}

// New starts a Stub and registers its Close with t.Cleanup. Cleanup waits for
// in-flight handlers, so the stub's goroutines never outlive the test.
func New(t testing.TB) *Stub {
	t.Helper()
	s := newStub(t)
	t.Cleanup(s.Close)
	return s
}

func newStub(rep reporter) *Stub {
	s := &Stub{rep: rep, mux: http.NewServeMux()}
	s.server = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

// URL returns the stub's base URL.
func (s *Stub) URL() string { return s.server.URL }

// Client returns an Elasticsearch client with retries disabled so tests count
// exactly the requests the code under test chose to send.
func (s *Stub) Client() (*elasticsearch.Client, error) {
	return elasticsearch.NewClient(elasticsearch.Config{
		Addresses:    []string{s.server.URL},
		DisableRetry: true,
		MaxRetries:   0,
	})
}

// Handle registers handler for a Go ServeMux pattern such as
// "PUT /_ingest/pipeline/{name}". Register routes before requests begin.
func (s *Stub) Handle(pattern string, handler http.HandlerFunc) {
	s.mux.HandleFunc(pattern, handler)
}

// Requests returns a deep copy of every request seen so far, in arrival order.
func (s *Stub) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Request, len(s.trace))
	for i, r := range s.trace {
		out[i] = Request{
			Method:   r.Method,
			Path:     r.Path,
			RawQuery: r.RawQuery,
			Header:   r.Header.Clone(),
			Body:     bytes.Clone(r.Body),
		}
	}
	return out
}

// Close shuts the server down. It is safe to call more than once, so a
// transport test may close early and still leave cleanup to New.
func (s *Stub) Close() {
	s.closeOnce.Do(s.server.Close)
}

// JSON returns a handler that replies with a fixed status and JSON body.
func JSON(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

func (s *Stub) serve(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.rep.Errorf("esstub: read request body for %s %s: %v", r.Method, r.URL.RequestURI(), err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))

	s.mu.Lock()
	s.trace = append(s.trace, Request{
		Method:   r.Method,
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
		Header:   r.Header.Clone(),
		Body:     bytes.Clone(body),
	})
	s.mu.Unlock()

	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.Header().Set("Content-Type", "application/json")

	// ServeMux lets a "GET" pattern answer HEAD. Treat that as unmatched: a
	// scenario must register the HEAD route the code under test depends on,
	// or a missing existence check would pass as success.
	_, pattern := s.mux.Handler(r)
	if pattern == "" || (r.Method == http.MethodHead && strings.HasPrefix(pattern, "GET ")) {
		s.rep.Errorf("esstub: unexpected request: %s %s", r.Method, r.URL.RequestURI())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// Dispatch through ServeMux itself, not the handler Handler returned:
	// only ServeMux.ServeHTTP records the matched wildcards that
	// r.PathValue reads. The lock is released above: a handler may block on
	// cancellation or a test barrier while other requests keep recording.
	s.mux.ServeHTTP(w, r)
}

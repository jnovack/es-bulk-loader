package esstub

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// recorder captures Errorf calls so the strictness branch can be asserted
// without failing the enclosing test.
type recorder struct {
	mu   sync.Mutex
	msgs []string
}

func (r *recorder) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, fmt.Sprintf(format, args...))
}

func (r *recorder) messages() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.msgs...)
}

func do(t *testing.T, method, target, body string, header http.Header) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, target, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp, string(data)
}

func TestDefaultHeadersAndOverride(t *testing.T) {
	s := New(t)
	s.Handle("GET /plain", JSON(http.StatusOK, `{"ok":true}`))
	s.Handle("GET /override", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Elastic-Product", "Other")
		w.Header().Set("Content-Type", "text/plain")
	})

	resp, body := do(t, http.MethodGet, s.URL()+"/plain", "", nil)
	if got := resp.Header.Get("X-Elastic-Product"); got != "Elasticsearch" {
		t.Fatalf("product header = %q, want Elasticsearch", got)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q, want application/json", got)
	}
	if body != `{"ok":true}` {
		t.Fatalf("body = %q", body)
	}

	resp, _ = do(t, http.MethodGet, s.URL()+"/override", "", nil)
	if got := resp.Header.Get("X-Elastic-Product"); got != "Other" {
		t.Fatalf("overridden product header = %q, want Other", got)
	}
	if got := resp.Header.Get("Content-Type"); got != "text/plain" {
		t.Fatalf("overridden content type = %q, want text/plain", got)
	}
}

func TestPathWildcardsReachHandlers(t *testing.T) {
	s := New(t)
	s.Handle("GET /{index}/_mapping", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.PathValue("index"))
	})

	_, body := do(t, http.MethodGet, s.URL()+"/cards-a/_mapping", "", nil)
	if body != "cards-a" {
		t.Fatalf("PathValue = %q, want cards-a", body)
	}
}

func TestConfiguredErrorResponse(t *testing.T) {
	s := New(t)
	s.Handle("PUT /idx", JSON(http.StatusBadRequest, `{"error":"bad"}`))

	resp, body := do(t, http.MethodPut, s.URL()+"/idx", "{}", nil)
	if resp.StatusCode != http.StatusBadRequest || body != `{"error":"bad"}` {
		t.Fatalf("got %d %q, want 400 error body", resp.StatusCode, body)
	}
	if got := len(s.Requests()); got != 1 {
		t.Fatalf("recorded %d requests, want 1", got)
	}
}

func TestCapturesQueryAndBodyWithoutConsumingHandlerBody(t *testing.T) {
	s := New(t)
	s.Handle("POST /_bulk", func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("handler read: %v", err)
		}
		_, _ = w.Write(data)
	})

	header := http.Header{"X-Test": []string{"v"}}
	_, echoed := do(t, http.MethodPost, s.URL()+"/_bulk?refresh=true&x=1", "payload", header)
	if echoed != "payload" {
		t.Fatalf("handler saw body %q, want payload", echoed)
	}

	got := s.Requests()
	if len(got) != 1 {
		t.Fatalf("recorded %d requests, want 1", len(got))
	}
	r := got[0]
	if r.Method != http.MethodPost || r.Path != "/_bulk" || r.RawQuery != "refresh=true&x=1" {
		t.Fatalf("request = %+v", r)
	}
	if string(r.Body) != "payload" || r.Header.Get("X-Test") != "v" {
		t.Fatalf("body/header not captured: %+v", r)
	}
}

func TestRequestsReturnsIndependentSnapshots(t *testing.T) {
	s := New(t)
	s.Handle("POST /a", JSON(http.StatusOK, `{}`))
	do(t, http.MethodPost, s.URL()+"/a", "abc", http.Header{"X-Test": []string{"v"}})

	first := s.Requests()
	first[0].Body[0] = 'z'
	first[0].Header.Set("X-Test", "mutated")
	first[0].Path = "/mutated"
	first = append(first, Request{})

	second := s.Requests()
	if len(second) != 1 {
		t.Fatalf("snapshot length = %d, want 1", len(second))
	}
	if string(second[0].Body) != "abc" || second[0].Header.Get("X-Test") != "v" || second[0].Path != "/a" {
		t.Fatalf("snapshot was aliased: %+v", second[0])
	}
}

func TestUnknownRouteIsReportedWith500(t *testing.T) {
	rec := &recorder{}
	s := newStub(rec)
	t.Cleanup(s.Close)
	s.Handle("GET /known", JSON(http.StatusOK, `{}`))

	resp, _ := do(t, http.MethodGet, s.URL()+"/unknown?a=b", "", nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("unknown route status = %d, want 500", resp.StatusCode)
	}
	resp, _ = do(t, http.MethodDelete, s.URL()+"/known", "", nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("unknown method status = %d, want 500", resp.StatusCode)
	}

	msgs := rec.messages()
	if len(msgs) != 2 {
		t.Fatalf("reported %d errors, want 2: %v", len(msgs), msgs)
	}
	if !strings.Contains(msgs[0], "GET") || !strings.Contains(msgs[0], "/unknown?a=b") {
		t.Fatalf("first report lacks method and URI: %q", msgs[0])
	}
	if !strings.Contains(msgs[1], "DELETE") || !strings.Contains(msgs[1], "/known") {
		t.Fatalf("second report lacks method and URI: %q", msgs[1])
	}
}

// ServeMux matches HEAD against a "GET" pattern. The stub must not: production
// issues HEAD existence checks, and a GET-only scenario answering them would
// pass without the HEAD route the code under test depends on.
func TestHeadDoesNotFallBackToGetRoute(t *testing.T) {
	rec := &recorder{}
	s := newStub(rec)
	t.Cleanup(s.Close)
	s.Handle("GET /get-only", JSON(http.StatusOK, `{}`))
	s.Handle("GET /both", JSON(http.StatusOK, `{}`))
	s.Handle("HEAD /both", JSON(http.StatusNoContent, ``))

	resp, _ := do(t, http.MethodHead, s.URL()+"/get-only", "", nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("HEAD on GET-only route status = %d, want 500", resp.StatusCode)
	}
	msgs := rec.messages()
	if len(msgs) != 1 || !strings.Contains(msgs[0], "HEAD") || !strings.Contains(msgs[0], "/get-only") {
		t.Fatalf("HEAD on GET-only route reports = %v, want one naming HEAD /get-only", msgs)
	}

	resp, _ = do(t, http.MethodHead, s.URL()+"/both", "", nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("HEAD on explicit HEAD route status = %d, want 204", resp.StatusCode)
	}
	resp, _ = do(t, http.MethodGet, s.URL()+"/both", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET on GET route status = %d, want 200", resp.StatusCode)
	}
	if got := len(rec.messages()); got != 1 {
		t.Fatalf("reported %d errors, want only the first: %v", got, rec.messages())
	}
}

func TestConcurrentRecording(t *testing.T) {
	s := New(t)
	s.Handle("GET /n/{id}", JSON(http.StatusOK, `{}`))

	const n = 32
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("%s/n/%d", s.URL(), i), nil)
			if err != nil {
				t.Errorf("new request: %v", err)
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("request %d: %v", i, err)
				return
			}
			_ = resp.Body.Close()
		}()
	}
	wg.Wait()

	seen := map[string]bool{}
	for _, r := range s.Requests() {
		seen[r.Path] = true
	}
	if len(seen) != n {
		t.Fatalf("recorded %d distinct requests, want %d", len(seen), n)
	}
}

func TestBlockedHandlerDoesNotBlockRecording(t *testing.T) {
	s := New(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	s.Handle("GET /block", func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	})
	s.Handle("GET /free", JSON(http.StatusOK, `{}`))

	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, s.URL()+"/block", nil)
		if err != nil {
			t.Errorf("new request: %v", err)
			return
		}
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("blocking handler was not entered")
	}

	freeDone := make(chan struct{})
	go func() {
		defer close(freeDone)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, s.URL()+"/free", nil)
		if err != nil {
			t.Errorf("new request: %v", err)
			return
		}
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
		}
		_ = s.Requests()
	}()

	select {
	case <-freeDone:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("recording deadlocked behind a blocked handler")
	}
	close(release)
	<-blocked
	if got := len(s.Requests()); got != 2 {
		t.Fatalf("recorded %d requests, want 2", got)
	}
}

func TestHijackerIsPreserved(t *testing.T) {
	s := New(t)
	s.Handle("POST /drop", func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Errorf("ResponseWriter %T does not implement http.Hijacker", w)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_ = conn.Close()
	})

	es, err := s.Client()
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/drop", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	res, err := es.Perform(req)
	if err == nil {
		_ = res.Body.Close()
		t.Fatal("expected a transport error from the dropped connection")
	}
	if ctx.Err() != nil {
		t.Fatalf("request hung until timeout: %v", err)
	}
	if got := len(s.Requests()); got != 1 {
		t.Fatalf("recorded %d requests, want 1", got)
	}
}

func TestCloseIsIdempotentAndRefusesConnections(t *testing.T) {
	s := New(t)
	s.Handle("GET /x", JSON(http.StatusOK, `{}`))
	target := s.URL()
	s.Close()
	s.Close()

	u, err := url.Parse(target)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	conn, err := net.DialTimeout("tcp", u.Host, time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected connection to be refused after Close")
	}
}

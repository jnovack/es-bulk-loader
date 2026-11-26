// Package bodytransport lets a test drive the real Elasticsearch client over a
// transport whose response bodies it controls and can observe: how many bytes
// were served, whether a read failed and how often Close ran. esstub cannot
// count reads on the client's response body. It is test support only and must
// not be imported by production packages.
package bodytransport

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"
)

// Body is an observable response body. It serves R, then returns Err instead
// of io.EOF when Err is set.
type Body struct {
	R   io.Reader
	Err error

	Served, Closes atomic.Int64
}

// Read serves R and counts the bytes served.
func (b *Body) Read(p []byte) (int, error) {
	n, err := b.R.Read(p)
	b.Served.Add(int64(n))
	if err == io.EOF && b.Err != nil {
		return n, b.Err
	}
	return n, err
}

// Close counts the call.
func (b *Body) Close() error {
	b.Closes.Add(1)
	return nil
}

// Endless serves 'x' bytes forever: a body that would exhaust an unbounded
// read.
type Endless struct{}

// Read fills p.
func (Endless) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

// Reply is one canned response. A non-nil Err makes RoundTrip fail with it (a
// transport failure) instead of returning a response. Bodies still records the
// configured Body for that request, which may be nil.
type Reply struct {
	Status int
	Body   *Body
	Err    error
}

// Transport answers the Nth request with Replies[N] (the last reply repeats)
// and records every body it handed out and every "METHOD /path".
type Transport struct {
	Replies []func() Reply

	mu     sync.Mutex
	paths  []string
	bodies []*Body
}

// Static returns a func producing status with a fresh body serving s.
func Static(status int, s string) func() Reply {
	return func() Reply { return Reply{Status: status, Body: &Body{R: strings.NewReader(s)}} }
}

// Fail returns a func producing a transport failure with err (no response).
func Fail(err error) func() Reply {
	return func() Reply { return Reply{Err: err} }
}

// ReadFail returns a func answering status with a body that serves partial and
// then fails with err instead of io.EOF. Each call returns a fresh Body.
func ReadFail(status int, partial string, err error) func() Reply {
	return func() Reply {
		return Reply{Status: status, Body: &Body{R: strings.NewReader(partial), Err: err}}
	}
}

// RoundTrip implements http.RoundTripper.
func (tr *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	tr.mu.Lock()
	idx := min(len(tr.paths), len(tr.Replies)-1)
	tr.paths = append(tr.paths, req.Method+" "+req.URL.Path)
	reply := tr.Replies[idx]()
	tr.bodies = append(tr.bodies, reply.Body)
	tr.mu.Unlock()

	if reply.Err != nil {
		return nil, reply.Err
	}
	header := http.Header{}
	header.Set("X-Elastic-Product", "Elasticsearch")
	header.Set("Content-Type", "application/json")
	return &http.Response{
		StatusCode: reply.Status,
		Status:     strconv.Itoa(reply.Status) + " " + http.StatusText(reply.Status),
		Header:     header,
		Body:       reply.Body,
		Request:    req,
	}, nil
}

// Paths returns the "METHOD /path" of every request, in order.
func (tr *Transport) Paths() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]string(nil), tr.paths...)
}

// Bodies returns every body handed out, in order.
func (tr *Transport) Bodies() []*Body {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]*Body(nil), tr.bodies...)
}

// Client returns a real client over tr with retries disabled.
func (tr *Transport) Client(t testing.TB) *elasticsearch.Client {
	t.Helper()
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:    []string{"http://es.invalid"},
		Transport:    tr,
		DisableRetry: true,
	})
	if err != nil {
		t.Fatalf("new elasticsearch client: %v", err)
	}
	return es
}

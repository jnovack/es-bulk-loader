package bodytransport

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestBodyCountsServedBytesAndReplacesEOFWithErr(t *testing.T) {
	t.Parallel()

	boom := errors.New("injected")
	t.Run("served counts every byte and Close is counted", func(t *testing.T) {
		t.Parallel()
		b := &Body{R: strings.NewReader("hello world")}
		got, err := io.ReadAll(b)
		if err != nil || string(got) != "hello world" {
			t.Fatalf("ReadAll = %q, %v", got, err)
		}
		if b.Served.Load() != 11 {
			t.Fatalf("Served = %d, want 11", b.Served.Load())
		}
		_ = b.Close()
		_ = b.Close()
		if b.Closes.Load() != 2 {
			t.Fatalf("Closes = %d, want 2", b.Closes.Load())
		}
	})
	t.Run("Err replaces EOF after the final bytes", func(t *testing.T) {
		t.Parallel()
		b := &Body{R: strings.NewReader("abc"), Err: boom}
		got, err := io.ReadAll(b)
		if !errors.Is(err, boom) || string(got) != "abc" {
			t.Fatalf("ReadAll = %q, %v; want abc and the injected error", got, err)
		}
		if b.Served.Load() != 3 {
			t.Fatalf("Served = %d, want 3", b.Served.Load())
		}
	})
	t.Run("Endless never ends", func(t *testing.T) {
		t.Parallel()
		b := &Body{R: Endless{}}
		got, err := io.ReadAll(io.LimitReader(b, 1000))
		if err != nil || len(got) != 1000 || b.Served.Load() < 1000 {
			t.Fatalf("read %d bytes, err %v, served %d", len(got), err, b.Served.Load())
		}
	})
}

func TestTransportRepliesInOrderAndRecordsPaths(t *testing.T) {
	t.Parallel()

	tr := &Transport{Replies: []func() Reply{Static(404, "first"), Static(200, "second")}}
	es := tr.Client(t)

	var statuses []int
	for _, name := range []string{"a", "b", "c"} {
		res, err := es.Indices.Exists([]string{name})
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		statuses = append(statuses, res.StatusCode)
		_ = res.Body.Close()
	}

	if want := []int{404, 200, 200}; statuses[0] != want[0] || statuses[1] != want[1] || statuses[2] != want[2] {
		t.Fatalf("statuses = %v, want %v (the last reply repeats)", statuses, want)
	}
	paths := tr.Paths()
	if len(paths) != 3 || paths[0] != "HEAD /a" || paths[2] != "HEAD /c" {
		t.Fatalf("paths = %q, want HEAD /a, /b, /c in order", paths)
	}
	bodies := tr.Bodies()
	if len(bodies) != 3 || bodies[0].Closes.Load() != 1 {
		t.Fatalf("bodies = %d, first closed %d times; want 3 and 1", len(bodies), bodies[0].Closes.Load())
	}
}

// TestFailReturnsTransportErrorAndRecordsRequest guards the shared
// transport-failure factory: the request fails with exactly err (no
// response), and the path and a nil body are still recorded in order.
func TestFailReturnsTransportErrorAndRecordsRequest(t *testing.T) {
	t.Parallel()

	boom := errors.New("dial injected")
	tr := &Transport{Replies: []func() Reply{Fail(boom), Static(200, "ok")}}
	es := tr.Client(t)

	res, err := es.Indices.Exists([]string{"a"})
	if !errors.Is(err, boom) || res != nil {
		t.Fatalf("first request = %v, %v; want no response and the injected error", res, err)
	}
	res, err = es.Indices.Exists([]string{"b"})
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("second request = %v, %v; want 200", res, err)
	}
	_ = res.Body.Close()

	if paths := tr.Paths(); len(paths) != 2 || paths[0] != "HEAD /a" || paths[1] != "HEAD /b" {
		t.Fatalf("paths = %q, want HEAD /a then HEAD /b", paths)
	}
	if bodies := tr.Bodies(); len(bodies) != 2 || bodies[0] != nil || bodies[1] == nil {
		t.Fatalf("bodies = %v, want a nil body for the failure then the served body", bodies)
	}
}

// TestReadFailServesPartialThenErrorWithFreshBodies guards the shared
// read-failure factory: the status is answered, the body serves exactly the
// partial text and then ends in the injected error instead of io.EOF, and
// every invocation yields a distinct Body.
func TestReadFailServesPartialThenErrorWithFreshBodies(t *testing.T) {
	t.Parallel()

	injected := errors.New("read injected")
	tr := &Transport{Replies: []func() Reply{ReadFail(500, `{"par`, injected)}}
	es := tr.Client(t)

	for _, name := range []string{"a", "b"} {
		res, err := es.Indices.Exists([]string{name})
		if err != nil || res.StatusCode != 500 {
			t.Fatalf("request %s = %v, %v; want status 500", name, res, err)
		}
		got, err := io.ReadAll(res.Body)
		if string(got) != `{"par` || !errors.Is(err, injected) {
			t.Fatalf("ReadAll = %q, %v; want the partial text then the injected error", got, err)
		}
		_ = res.Body.Close()
	}

	bodies := tr.Bodies()
	if len(bodies) != 2 || bodies[0] == nil || bodies[0] == bodies[1] {
		t.Fatalf("bodies = %v; want two distinct non-nil bodies", bodies)
	}
}

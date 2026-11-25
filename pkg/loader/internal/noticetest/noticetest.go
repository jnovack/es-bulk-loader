// Package noticetest holds the shared recorder and assertion for tests of the
// observe notice sites (enrich, managed, indexops). It is test support only
// and must not be imported by production packages.
package noticetest

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/jnovack/es-bulk-loader/internal/testutil/logbuf"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

// Recorder holds what a test's sink received, in delivery order.
type Recorder struct {
	Notices []observe.Notice
}

// New returns a context with a Nop runlog logger and a sink recording every
// notice into the returned Recorder.
func New(t *testing.T) (context.Context, *Recorder) {
	t.Helper()
	rec := &Recorder{}
	ctx := runlog.WithLogger(t.Context(), logbuf.Nop())
	ctx = observe.WithSink(ctx, &observe.Sink{OnNotice: func(n observe.Notice) { rec.Notices = append(rec.Notices, n) }})
	return ctx, rec
}

// RequireNotices fails unless got equals want exactly and no Message contains
// any forbidden substring (response or error text that must never leak).
func RequireNotices(t *testing.T, got, want []observe.Notice, forbidden ...string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("notices = %+v\nwant      %+v", got, want)
	}
	for _, n := range got {
		for _, f := range forbidden {
			if strings.Contains(n.Message, f) {
				t.Fatalf("notice Message %q carries response text %q", n.Message, f)
			}
		}
	}
}

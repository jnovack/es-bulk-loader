package observe

import (
	"context"
	"errors"
	"testing"
	"time"
)

// recorder collects what a Sink receives.
type recorder struct {
	notices []Notice
	events  []Event
}

func (r *recorder) sink() *Sink {
	return &Sink{
		OnNotice: func(n Notice) { r.notices = append(r.notices, n) },
		OnEvent:  func(e Event) { r.events = append(r.events, e) },
	}
}

func (r *recorder) count() int { return len(r.notices) + len(r.events) }

var (
	testNotice = Notice{Code: CodeEnrichPolicyMissing, Message: "m", Kind: KindPolicy, Name: "p"}
	testEvent  = Event{Action: "created", Kind: KindIndex, Name: "i", Alias: "a", Logical: "l"}
)

// emitBoth sends testNotice and testEvent through ctx.
func emitBoth(ctx context.Context) {
	Notify(ctx, testNotice)
	Record(ctx, testEvent)
}

// TestNotifyAndRecordNoOps guards the documented no-op cases: none may panic
// or deliver, mirroring runlog.From's nil handling.
func TestNotifyAndRecordNoOps(t *testing.T) {
	t.Parallel()

	var nilCtx context.Context
	var nilSink *Sink
	tests := []struct {
		name string
		ctx  context.Context
	}{
		{"nil context", nilCtx},
		{"no sink", t.Context()},
		{"nil callbacks", WithSink(t.Context(), &Sink{})},
		{"typed nil sink", WithSink(t.Context(), nilSink)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			emitBoth(tt.ctx) // must not panic
		})
	}
}

// TestSinkDeliversExactValues guards that a set sink receives each value
// unchanged, and that each callback receives only its own kind.
func TestSinkDeliversExactValues(t *testing.T) {
	t.Parallel()

	var r recorder
	emitBoth(WithSink(t.Context(), r.sink()))
	if len(r.notices) != 1 || r.notices[0] != testNotice {
		t.Fatalf("notices = %+v, want [%+v]", r.notices, testNotice)
	}
	if len(r.events) != 1 || r.events[0] != testEvent {
		t.Fatalf("events = %+v, want [%+v]", r.events, testEvent)
	}

	// One nil callback does not disable the other.
	var onlyNotice []Notice
	emitBoth(WithSink(t.Context(), &Sink{OnNotice: func(n Notice) { onlyNotice = append(onlyNotice, n) }}))
	if len(onlyNotice) != 1 {
		t.Fatalf("OnNotice with nil OnEvent got %d notices, want 1", len(onlyNotice))
	}
	var onlyEvent []Event
	emitBoth(WithSink(t.Context(), &Sink{OnEvent: func(e Event) { onlyEvent = append(onlyEvent, e) }}))
	if len(onlyEvent) != 1 {
		t.Fatalf("OnEvent with nil OnNotice got %d events, want 1", len(onlyEvent))
	}
}

// TestWithSinkNilParentUsesBackground guards WithSink(nil, sink): it must
// deliver rather than panic or drop the sink.
func TestWithSinkNilParentUsesBackground(t *testing.T) {
	t.Parallel()

	var nilCtx context.Context
	var r recorder
	ctx := WithSink(nilCtx, r.sink())
	if ctx == nil {
		t.Fatal("WithSink(nil, sink) returned a nil context")
	}
	if ctx.Done() != nil {
		t.Fatal("WithSink(nil, sink) is cancellable; want a Background child")
	}
	emitBoth(ctx)
	if r.count() != 2 {
		t.Fatalf("delivered %d values, want 2", r.count())
	}
}

// TestWithSinkReplacesAndIsolates guards replacement and parent isolation: a
// child sink replaces an inherited one, a nil child sink disables delivery
// without falling back to the parent's sink, and neither child changes what
// the parent delivers.
func TestWithSinkReplacesAndIsolates(t *testing.T) {
	t.Parallel()

	var parentRec, childRec recorder
	parent := WithSink(t.Context(), parentRec.sink())
	child := WithSink(parent, childRec.sink())
	disabled := WithSink(parent, nil)

	emitBoth(child)
	if childRec.count() != 2 || parentRec.count() != 0 {
		t.Fatalf("child emit: child=%d parent=%d, want 2 and 0", childRec.count(), parentRec.count())
	}
	emitBoth(disabled)
	if childRec.count() != 2 || parentRec.count() != 0 {
		t.Fatalf("disabled emit reached a sink: child=%d parent=%d", childRec.count(), parentRec.count())
	}
	emitBoth(parent)
	if parentRec.count() != 2 || childRec.count() != 2 {
		t.Fatalf("parent emit: parent=%d child=%d, want 2 and 2", parentRec.count(), childRec.count())
	}
	// A grandchild without its own sink inherits the nearest one.
	type otherKey struct{}
	emitBoth(context.WithValue(child, otherKey{}, 1))
	if childRec.count() != 4 || parentRec.count() != 2 {
		t.Fatalf("grandchild emit: child=%d parent=%d, want 4 and 2", childRec.count(), parentRec.count())
	}
}

// TestWithSinkKeepsParentValuesDeadlineAndCancellation guards that installing
// a sink derives a child: the parent's values, deadline and cancellation reach
// it.
func TestWithSinkKeepsParentValuesDeadlineAndCancellation(t *testing.T) {
	t.Parallel()

	type valueKey struct{}
	deadline := time.Now().Add(time.Hour)
	base, cancelDeadline := context.WithDeadline(context.WithValue(t.Context(), valueKey{}, "kept"), deadline)
	defer cancelDeadline()
	parent, cancel := context.WithCancel(base)

	ctx := WithSink(parent, &Sink{})
	if got := ctx.Value(valueKey{}); got != "kept" {
		t.Fatalf("parent value = %v, want kept", got)
	}
	if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
		t.Fatalf("deadline = %v, %v; want %v, true", got, ok, deadline)
	}
	cancel()
	select {
	case <-ctx.Done():
	default:
		t.Fatal("cancelling the parent did not cancel the sink context")
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("ctx.Err() = %v, want context.Canceled", ctx.Err())
	}
}

// TestDeliveryIsInline guards synchronous delivery: each callback has finished
// before Notify or Record returns, on the caller's goroutine (an async
// callback writing these plain variables would also be a race).
func TestDeliveryIsInline(t *testing.T) {
	t.Parallel()

	noticeDone, eventDone := false, false
	ctx := WithSink(t.Context(), &Sink{
		OnNotice: func(Notice) { noticeDone = true },
		OnEvent:  func(Event) { eventDone = true },
	})
	Notify(ctx, testNotice)
	if !noticeDone {
		t.Fatal("Notify returned before OnNotice finished")
	}
	Record(ctx, testEvent)
	if !eventDone {
		t.Fatal("Record returned before OnEvent finished")
	}
}

// panicked runs fn and returns what it panicked with, or nil.
func panicked(fn func()) (recovered any) {
	defer func() { recovered = recover() }()
	fn()
	return nil
}

// TestCallbackPanicsPropagateByIdentity guards that the package has no local
// recover: an error panic and a non-error panic reach the caller unchanged, so
// Run's recover can apply the Progress panic rule.
func TestCallbackPanicsPropagateByIdentity(t *testing.T) {
	t.Parallel()

	errValue := errors.New("callback failed")
	nonError := &struct{ n int }{1}
	for _, value := range []any{errValue, nonError} {
		ctx := WithSink(t.Context(), &Sink{
			OnNotice: func(Notice) { panic(value) },
			OnEvent:  func(Event) { panic(value) },
		})
		if got := panicked(func() { Notify(ctx, testNotice) }); got != value {
			t.Fatalf("Notify panic = %v, want %v by identity", got, value)
		}
		if got := panicked(func() { Record(ctx, testEvent) }); got != value {
			t.Fatalf("Record panic = %v, want %v by identity", got, value)
		}
	}
}

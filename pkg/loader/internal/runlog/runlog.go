// Package runlog carries one Run's logger on its context and provides the
// log-then-return helpers that pkg/loader's internal packages use to fail an
// operation. Package loader resolves and attaches the logger before any other
// call; every internal package reads it back with From, so a Run's output goes
// to its injected logger without touching the zerolog global.
package runlog

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
)

// loggerKey is a private context key: zerolog's own WithContext refuses to
// store a Disabled logger, which would silently turn an injected zerolog.Nop()
// back into global output.
type loggerKey struct{}

// WithLogger attaches l to ctx for every internal log call in this Run.
func WithLogger(ctx context.Context, l *zerolog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// From returns the logger Run attached to ctx, or &log.Logger when ctx is nil
// or carries none (direct internal calls in tests). Reading the global
// requires callers not to reassign it concurrently.
func From(ctx context.Context) *zerolog.Logger {
	if ctx != nil {
		if l, ok := ctx.Value(loggerKey{}).(*zerolog.Logger); ok && l != nil {
			return l
		}
	}
	return &log.Logger
}

// Resolve snapshots the injected logger, or the global only when injected is
// nil, then adds the timestamp decoration. The injected branch never reads the
// global, so concurrent Runs with injected loggers share no state.
func Resolve(injected *zerolog.Logger) zerolog.Logger {
	if injected != nil {
		return withTimestamp(*injected)
	}
	return withTimestamp(log.Logger)
}

// withTimestamp returns a copy of base that stamps every event with a
// timestamp.
func withTimestamp(base zerolog.Logger) zerolog.Logger {
	return base.With().Timestamp().Logger()
}

// FailureEvent starts the fatal-level event logged just before a helper
// returns an operation error. The level is kept from the former panic bridge
// so log consumers still see "fatal"; it is only a level: Msg neither exits
// nor panics, unlike Logger.Fatal.
func FailureEvent(ctx context.Context) *zerolog.Event {
	return From(ctx).WithLevel(zerolog.FatalLevel)
}

// Fail attaches cause to event, emits it with op as its message and returns
// the operation error for op and cause. Taking op and cause once keeps the
// logged message/error field and the returned Op/cause identical, so callers
// must not add their own .Err: a second copy could drift from the returned
// cause. A nil cause logs no error field (as the former panic bridge did) and
// is returned as errors.New(op) (see failure.New).
func Fail(event *zerolog.Event, op string, cause error) error {
	event.Err(cause).Msg(op)
	return failure.New(op, cause)
}

// Check traces op and, when err is non-nil, returns it as op's operation error
// without a fatal-level line: the former checkErr behaved the same way.
func Check(ctx context.Context, op string, err error) error {
	From(ctx).Trace().Msg(op)
	if err != nil {
		return failure.New(op, err)
	}
	return nil
}

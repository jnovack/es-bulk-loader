package loader

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// runLoggerKey is a private context key: zerolog's own WithContext refuses
// to store a Disabled logger, which would silently turn an injected
// zerolog.Nop() back into global output.
type runLoggerKey struct{}

// withRunLogger attaches l to ctx for every internal log call in this Run.
func withRunLogger(ctx context.Context, l *zerolog.Logger) context.Context {
	return context.WithValue(ctx, runLoggerKey{}, l)
}

// loggerFrom returns the logger Run attached to ctx, or &log.Logger when ctx
// is nil or carries none (direct internal calls in tests). Reading the global
// requires callers not to reassign it concurrently.
func loggerFrom(ctx context.Context) *zerolog.Logger {
	if ctx != nil {
		if l, ok := ctx.Value(runLoggerKey{}).(*zerolog.Logger); ok && l != nil {
			return l
		}
	}
	return &log.Logger
}

// resolveRunLogger snapshots the injected logger, or the global only when
// injected is nil, then adds the timestamp decoration. The injected branch
// never reads the global, so concurrent Runs with injected loggers share no
// state.
func resolveRunLogger(injected *zerolog.Logger) zerolog.Logger {
	if injected != nil {
		return withTimestampLogger(*injected)
	}
	return withTimestampLogger(log.Logger)
}

// withTimestampLogger returns a copy of base that stamps every event with a
// timestamp.
func withTimestampLogger(base zerolog.Logger) zerolog.Logger {
	return base.With().Timestamp().Logger()
}

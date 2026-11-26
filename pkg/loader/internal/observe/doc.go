// Package observe carries one Run's notice and lifecycle sink on its context.
// Package loader installs the sink; managed, enrich, indexops and the other
// internal owners report an actionable condition (a Notice) through Notify and
// a lifecycle event (an Event: a completed cluster change, or the local
// "resolved" event) through Record, next to their existing log lines, which
// they never replace.
//
// # Context propagation
//
// WithSink returns a child of its parent that carries the sink under a private
// key, as runlog carries the logger. The parent's cancellation, deadline and
// other values pass through unchanged, and neither the parent nor a sink it
// already carries is modified: delivery through the parent still reaches the
// parent's sink. A sink installed on a child replaces any inherited one for
// that child and its descendants. A nil parent is treated as
// context.Background().
//
// # Nil behavior
//
// Notify and Record do nothing when ctx is nil, carries no sink, carries a nil
// *Sink, or the matching callback is nil. WithSink(ctx, nil) therefore
// disables delivery below ctx; it never falls back to an inherited sink.
// Internal owners called directly (unit tests) without a sink emit nothing.
//
// # Delivery
//
// Delivery is synchronous: Notify and Record call the callback inline on the
// caller's goroutine and return after it does. There is no goroutine, queue,
// global state or recovery here, so the sink is not a concurrent dispatcher.
// Run owns one sink per invocation and its emit sites run on Run's goroutine.
// A callback panic propagates to the caller unchanged; inside Run it unwinds
// to Run's existing recover, which follows the Options.Progress rule (an error
// value fails the Run, any other value propagates).
//
// # Values
//
// The string values of the Code*, Kind* and Action* constants are the public contract:
// package loader declares a typed twin of each by conversion (for example
// NoticeEnrichPolicyMissing = NoticeCode(CodeEnrichPolicyMissing)), and a
// parity test fails when a constant here has no twin. A Notice Message is the
// fixed text of the log line beside its emit site (or Run's warning text); it
// never carries an Elasticsearch response, cause or error. Name may hold a
// name read from a response; package loader sanitizes and bounds it before
// exposing it.
package observe

// Package failure provides the operation error that pkg/loader's internal
// packages return instead of panicking. It carries only the failing operation
// and its cause; package loader converts it once, at its public boundary, into
// a *loader.RunError whose Kind is derived from Op. It holds no logger, client
// or copy of the public sentinels.
package failure

import "errors"

// Error is a failed operation. Op is the fixed operation text that the loader
// boundary classifies and reports as RunError.Op; Err is the original cause.
type Error struct {
	Op  string
	Err error
}

// Error renders "Op: cause", the same text RunError renders for the same
// values, so converting at the boundary adds no prefix and drops none.
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return e.Op
	}
	return e.Op + ": " + e.Err.Error()
}

// Unwrap exposes the cause for errors.Is and errors.As.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// New returns an operation error. A nil cause becomes errors.New(op): the
// former panic bridge did the same, so a failure without a cause still renders
// as "op: op" and callers' errors.Unwrap never returns nil.
func New(op string, cause error) *Error {
	if cause == nil {
		cause = errors.New(op)
	}
	return &Error{Op: op, Err: cause}
}

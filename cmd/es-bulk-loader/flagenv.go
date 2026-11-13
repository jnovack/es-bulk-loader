package main

import (
	"fmt"
	"os"
)

// withEnvHidden runs fn with key removed from the process environment, then
// restores its exact original state (absent, empty, or the original value).
//
// It exists for the -user flag: jnovack/flag binds every flag to an env var
// with no per-flag opt-out and no prefix, so -user would read the shell's
// USER. That made -apiKey fail with a basic-auth conflict whenever USER was
// set, and let $USER override a `user=` line in the -config file. A prefix
// would rename every documented binding (INDEX, URL, ...), so USER is hidden
// only while flags are parsed.
//
// Restoration is required: ${USER} in loader templates falls back to the
// process environment, so dropping USER for good would break those templates.
// The restore is deferred so it also runs while a panic unwinds; a restore
// error during a panic is dropped, because the panic propagates instead of
// the returned error. flag.Parse
// may call os.Exit on help or errors; the process ends then and no later
// code needs the variable.
//
// This is a startup helper for single-goroutine use, not a concurrency-safe
// environment abstraction. Values are never logged.
func withEnvHidden(key string, fn func()) error {
	orig, present := os.LookupEnv(key)
	if present {
		if err := os.Unsetenv(key); err != nil {
			return fmt.Errorf("hiding environment variable %s: %w", key, err)
		}
	}

	var restoreErr error
	func() {
		defer func() {
			if !present {
				return
			}
			if err := os.Setenv(key, orig); err != nil {
				restoreErr = fmt.Errorf("restoring environment variable %s: %w", key, err)
			}
		}()
		fn()
	}()
	return restoreErr
}

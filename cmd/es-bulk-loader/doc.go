// Package main is the es-bulk-loader command. It parses flags into
// loader.Options, logs build metadata at startup, and calls loader.Run once.
// A run failure is logged and exits 1; ErrInvalidOptions also prints usage.
package main

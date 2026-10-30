//go:build e2e

// Package e2e runs the built es-bulk-loader binary against a disposable
// Elasticsearch container. Its tests need Docker; go vet -tags=e2e ./test
// compiles them without starting Docker.
package e2e

package main

import (
	"os/exec"
	"path/filepath"
	"testing"
)

// buildCLI compiles the real binary once for a test and returns its path.
// The CLI tests exercise flag/env wiring that building alone would not prove.
func buildCLI(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "es-bulk-loader")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

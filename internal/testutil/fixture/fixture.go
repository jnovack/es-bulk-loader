// Package fixture writes throwaway input files for tests in pkg/loader and
// its internal packages. It is test support only and must not be imported by
// production packages.
package fixture

import (
	"os"
	"testing"
)

// WriteTempJSON writes content to a new *.json file in dir and returns its
// path. Any failure stops the calling test.
func WriteTempJSON(t testing.TB, dir, content string) string {
	t.Helper()

	tmp, err := os.CreateTemp(dir, "*.json")
	if err != nil {
		t.Fatalf("CreateTemp returned error: %v", err)
	}
	if _, err := tmp.WriteString(content); err != nil {
		t.Fatalf("WriteString returned error: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	return tmp.Name()
}

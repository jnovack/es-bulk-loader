package fixture

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWriteTempJSON verifies the file lands in dir with exactly the content,
// including CRLF line endings and an empty body.
func TestWriteTempJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
	}{
		{name: "object", content: `{"a":1}`},
		{name: "crlf preserved", content: "{\r\n\"a\":1\r\n}\r\n"},
		{name: "empty", content: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := WriteTempJSON(t, dir, tt.content)
			if filepath.Dir(path) != dir || filepath.Ext(path) != ".json" {
				t.Fatalf("path = %q, want a .json file in %q", path, dir)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if string(got) != tt.content {
				t.Fatalf("content = %q, want %q", got, tt.content)
			}
		})
	}
}

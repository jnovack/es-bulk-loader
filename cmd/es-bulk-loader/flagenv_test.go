package main

import (
	"os"
	"testing"
)

// TestWithEnvHidden guards that the key is invisible to the callback yet
// exactly restored afterward. Restoring present-but-empty distinctly from
// absent matters because ${USER} template lookup uses os.LookupEnv.
// Not parallel: it edits the process environment.
func TestWithEnvHidden(t *testing.T) {
	const key = "ES_BULK_LOADER_TEST_HIDDEN_ENV"
	tests := []struct {
		name   string
		set    bool
		value  string
		panics bool
	}{
		{name: "non-empty value", set: true, value: "someone"},
		{name: "present but empty", set: true, value: ""},
		{name: "absent", set: false},
		{name: "panic in callback still restores", set: true, value: "someone", panics: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				t.Setenv(key, tt.value)
			} else {
				// Register cleanup to restore the original state, then unset.
				t.Setenv(key, "x")
				if err := os.Unsetenv(key); err != nil {
					t.Fatalf("unset: %v", err)
				}
			}

			calls := 0
			run := func() error {
				return withEnvHidden(key, func() {
					calls++
					if _, ok := os.LookupEnv(key); ok {
						t.Errorf("%s visible inside callback", key)
					}
					if tt.panics {
						panic("boom")
					}
				})
			}

			var err error
			if tt.panics {
				func() {
					defer func() {
						if recover() == nil {
							t.Errorf("expected panic to propagate")
						}
					}()
					err = run()
				}()
			} else {
				err = run()
			}
			if err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
			if calls != 1 {
				t.Fatalf("callback calls = %d, want 1", calls)
			}
			got, ok := os.LookupEnv(key)
			if ok != tt.set || got != tt.value {
				t.Fatalf("after: (%q, present=%t), want (%q, present=%t)", got, ok, tt.value, tt.set)
			}
		})
	}
}

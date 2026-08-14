package main

import (
	"bytes"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// TestEnrichFlagValueParsing verifies the production CLI enrich flag parser.
func TestEnrichFlagValueParsing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		value        string
		wantEnabled  bool
		wantAll      bool
		wantRaw      string
		wantPolicies []string
	}{
		{name: "bare true", value: "true", wantEnabled: true, wantAll: true},
		{name: "bare false", value: "false"},
		{
			name:         "explicit policies",
			value:        " policy-a,policy-b, policy-a ,, policy-c ",
			wantEnabled:  true,
			wantRaw:      "policy-a,policy-b, policy-a ,, policy-c",
			wantPolicies: []string{"policy-a", "policy-b", "policy-c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var enrich enrichFlagValue
			if err := enrich.Set(tt.value); err != nil {
				t.Fatalf("Set returned error: %v", err)
			}
			if enrich.enabled != tt.wantEnabled {
				t.Fatalf("enabled = %t, want %t", enrich.enabled, tt.wantEnabled)
			}
			if enrich.all != tt.wantAll {
				t.Fatalf("all = %t, want %t", enrich.all, tt.wantAll)
			}
			if enrich.raw != tt.wantRaw {
				t.Fatalf("raw = %q, want %q", enrich.raw, tt.wantRaw)
			}
			if got := enrich.explicitPolicies(); !reflect.DeepEqual(got, tt.wantPolicies) {
				t.Fatalf("explicitPolicies() = %v, want %v", got, tt.wantPolicies)
			}
			if !enrich.IsBoolFlag() {
				t.Fatal("IsBoolFlag() = false, want true")
			}
		})
	}
}

// TestParseLogLevel verifies that command-line log levels are normalized and validated.
func TestParseLogLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    zerolog.Level
		wantErr bool
	}{
		{name: "trace", input: "trace", want: zerolog.TraceLevel},
		{name: "debug uppercase", input: "DEBUG", want: zerolog.DebugLevel},
		{name: "info spaced", input: " info ", want: zerolog.InfoLevel},
		{name: "warn", input: "warn", want: zerolog.WarnLevel},
		{name: "error", input: "error", want: zerolog.ErrorLevel},
		{name: "invalid", input: "verbose", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseLogLevel(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLogLevel returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseLogLevel mismatch: got %v want %v", got, tt.want)
			}
		})
	}
}

// TestNewConsoleLoggerIncludesTimestamp verifies behavior for the related scenario.
func TestNewConsoleLoggerIncludesTimestamp(t *testing.T) {
	var output bytes.Buffer
	logger := newConsoleLogger(&output)
	logger.Info().Msg("timestamp-check")

	logs := output.String()
	if !strings.Contains(logs, "timestamp-check") {
		t.Fatalf("expected log output to contain message, got: %s", logs)
	}
	if strings.Contains(logs, "<nil>") {
		t.Fatalf("expected timestamped console output without <nil>, got: %s", logs)
	}
	if !regexp.MustCompile(`\d{1,2}:\d{2}:\d{2}`).MatchString(logs) {
		t.Fatalf("expected HH:MM:SS timestamp in console output, got: %s", logs)
	}
}

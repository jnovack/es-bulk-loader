package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/jnovack/es-bulk-loader/pkg/loader"
	"github.com/jnovack/flag"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ─── Build Metadata ────────────────────────────────────────────────────────────

// Release builds override these variables with -ldflags. When a value remains
// at its default, populateBuildMetadataFromBuildInfo fills it from available
// module or VCS build information.
var (
	version      = "dev"
	buildRFC3339 = "1970-01-01T00:00:00Z"
	revision     = "local"
)

// These defaults are both development values and sentinels used to detect
// fields that were not overridden at link time.
const (
	defaultVersion      = "dev"
	defaultBuildRFC3339 = "1970-01-01T00:00:00Z"
	defaultRevision     = "local"
)

// ─── Enrich Flag Parsing ───────────────────────────────────────────────────────

// enrichFlagValue groups state used to coordinate related package behavior.
type enrichFlagValue struct {
	enabled bool
	all     bool
	raw     string
}

// String returns the canonical textual form used by callers and logs.
func (e *enrichFlagValue) String() string {
	if e == nil {
		return ""
	}
	if e.all {
		return "all"
	}
	return e.raw
}

// Set parses and stores caller-provided configuration input.
func (e *enrichFlagValue) Set(value string) error {
	e.enabled = true
	trimmed := strings.TrimSpace(value)
	switch trimmed {
	case "", "true":
		e.all = true
		e.raw = ""
	case "false":
		e.enabled = false
		e.all = false
		e.raw = ""
	default:
		e.all = false
		e.raw = trimmed
	}
	return nil
}

// IsBoolFlag reports support for bare boolean flag syntax.
func (e *enrichFlagValue) IsBoolFlag() bool {
	return true
}

// explicitPolicies applies method-specific behavior to keep package workflows consistent.
func (e *enrichFlagValue) explicitPolicies() []string {
	if e == nil || !e.enabled || e.all {
		return nil
	}

	policies := make([]string, 0)
	seen := make(map[string]struct{})
	for _, policy := range strings.Split(e.raw, ",") {
		name := strings.TrimSpace(policy)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		policies = append(policies, name)
	}
	return policies
}

// ─── Runtime Helpers ───────────────────────────────────────────────────────────

// populateBuildMetadataFromBuildInfo centralizes this code path so package behavior stays consistent.
func populateBuildMetadataFromBuildInfo() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}

	if version == defaultVersion && info.Main.Version != "" && info.Main.Version != "(devel)" {
		version = info.Main.Version
	}

	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			if revision == defaultRevision && setting.Value != "" {
				revision = setting.Value
			}
		case "vcs.time":
			if buildRFC3339 == defaultBuildRFC3339 && setting.Value != "" {
				buildRFC3339 = setting.Value
			}
		}
	}
}

// parseLogLevel centralizes this code path so package behavior stays consistent.
func parseLogLevel(level string) (zerolog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "trace":
		return zerolog.TraceLevel, nil
	case "debug":
		return zerolog.DebugLevel, nil
	case "info":
		return zerolog.InfoLevel, nil
	case "warn":
		return zerolog.WarnLevel, nil
	case "error":
		return zerolog.ErrorLevel, nil
	default:
		return zerolog.NoLevel, fmt.Errorf("expected one of trace, debug, info, warn, error")
	}
}

// newConsoleLogger centralizes this code path so package behavior stays consistent.
func newConsoleLogger(out io.Writer) zerolog.Logger {
	return zerolog.New(zerolog.ConsoleWriter{Out: out, TimeFormat: "15:04:05"}).With().Timestamp().Logger()
}

// ─── Main Execution ────────────────────────────────────────────────────────────

// main centralizes this code path so package behavior stays consistent.
func main() {
	populateBuildMetadataFromBuildInfo()

	url := flag.String("url", "http://localhost:9200", "Elasticsearch URL")
	insecure := flag.Bool("insecureSkipVerify", false, "Skip TLS verification")
	index := flag.String("index", "", "Elasticsearch index name")
	settingsFile := flag.String("settings", "", "Path to index settings JSON file (optional)")
	mappingsFile := flag.String("mappings", "", "Path to index mappings JSON file (optional)")
	pipelinesFile := flag.String("pipelines", "", "Path to JSON file containing one or more ingest pipeline definitions (optional)")
	policiesFile := flag.String("policies", "", "Path to JSON file containing one or more enrich policy definitions (optional)")
	transformsFile := flag.String("transforms", "", "Path to JSON file containing one or more transform definitions (optional)")
	dataFile := flag.String("data", "", "Path to bulk JSON data file (array of objects)")
	batchSize := flag.Int("batch", 1000, "Batch size for bulk inserts")
	bulkRetryAttempts := flag.Int("bulk-retry-attempts", 4, "Total bulk request attempts, including first try")
	bulkRetryBackoffBase := flag.Duration("bulk-retry-backoff-base", 500*time.Millisecond, "Base backoff for retryable bulk failures")
	bulkRetryBackoffMax := flag.Duration("bulk-retry-backoff-max", 5*time.Second, "Maximum backoff for retryable bulk failures")
	deleteIndex := flag.Bool("delete", false, "Delete index if it exists")
	addToIndex := flag.Bool("add", false, "Add documents to existing index")
	flushIndex := flag.Bool("flush", false, "Delete existing documents without deleting the index, then load replacement documents from -data (-data is required)")
	syncManaged := flag.Bool("sync-managed", false, "Create or update declared ingest pipelines, enrich policies, and transforms")
	aliasMode := flag.Bool("alias", false, "Treat -index as an alias; create timestamped indices as <alias>-YYYYMMDDHHMMSS and repoint the alias on recreate")
	keepLast := flag.Int("keep-last", 0, "When -alias is set, keep only the newest N timestamped indices matching <alias>-YYYYMMDDHHMMSS (0 disables pruning)")
	nuke := flag.Bool("nuke", false, "Delete the current index and declared managed resources, including dependent pipelines that reference declared enrich policies")
	idField := flag.String("id", "", "Field to use to override _id (not normal)")
	user := flag.String("user", "", "Username for basic auth (optional)")
	pass := flag.String("pass", "", "Password for basic auth (optional)")
	apiKey := flag.String("apiKey", "", "Elasticsearch API key (optional)")
	logLevel := flag.String("level", "info", "Log level (trace, debug, info, warn, error)")
	enrich := &enrichFlagValue{}
	flag.Var(enrich, "enrich", "Run enrich policies after the bulk insert; provide a comma-separated policy list or omit the value to run all policies")
	showVersion := flag.Bool("version", false, "print version and exit")

	flag.String(flag.DefaultConfigFlagname, "", "path to config file")
	flag.Parse()

	zerolog.TimeFieldFormat = time.RFC3339
	parsedLogLevel, err := parseLogLevel(*logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -level value %q: %v\n", *logLevel, err)
		flag.Usage()
		os.Exit(1)
	}
	zerolog.SetGlobalLevel(parsedLogLevel)
	log.Logger = newConsoleLogger(os.Stderr)

	if *showVersion {
		log.Info().
			Str("version", version).
			Str("build_rfc3339", buildRFC3339).
			Str("revision", revision).
			Msg("jnovack/es-bulk-loader")
		os.Exit(0)
	}

	log.Info().
		Str("version", version).
		Str("build_rfc3339", buildRFC3339).
		Str("revision", revision).
		Msg("jnovack/es-bulk-loader starting...")

	opts := loader.Options{
		URL:                  *url,
		InsecureSkipVerify:   *insecure,
		Index:                *index,
		SettingsFile:         *settingsFile,
		MappingsFile:         *mappingsFile,
		PipelinesFile:        *pipelinesFile,
		PoliciesFile:         *policiesFile,
		TransformsFile:       *transformsFile,
		DataFile:             *dataFile,
		BatchSize:            *batchSize,
		BulkRetryAttempts:    *bulkRetryAttempts,
		BulkRetryBackoffBase: *bulkRetryBackoffBase,
		BulkRetryBackoffMax:  *bulkRetryBackoffMax,
		DeleteIndex:          *deleteIndex,
		AddToIndex:           *addToIndex,
		FlushIndex:           *flushIndex,
		SyncManaged:          *syncManaged,
		AliasMode:            *aliasMode,
		KeepLast:             *keepLast,
		Nuke:                 *nuke,
		IDField:              *idField,
		User:                 *user,
		Pass:                 *pass,
		APIKey:               *apiKey,
		Enrich: loader.EnrichOptions{
			Enabled:  enrich.enabled,
			All:      enrich.all,
			Raw:      enrich.raw,
			Policies: enrich.explicitPolicies(),
		},
	}

	_, err = loader.Run(context.Background(), opts)
	if err != nil {
		if errors.Is(err, loader.ErrInvalidOptions) {
			flag.Usage()
		}
		log.Error().Err(err).Msg("Loader run failed")
		os.Exit(1)
	}
}

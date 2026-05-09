package loader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ─── Error Sentinels ───────────────────────────────────────────────────────────

var (
	// ErrInvalidOptions defines package-level state shared by related execution paths.
	ErrInvalidOptions = errors.New("invalid options")
	// ErrIndexOperation defines package-level state shared by related execution paths.
	ErrIndexOperation = errors.New("index operation failed")
	// ErrManagedResource defines package-level state shared by related execution paths.
	ErrManagedResource = errors.New("managed resource failed")
	// ErrBulkFailure defines package-level state shared by related execution paths.
	ErrBulkFailure = errors.New("bulk insert failed")
	// ErrEnrichExecution defines package-level state shared by related execution paths.
	ErrEnrichExecution = errors.New("enrich execution failed")
	// ErrLoaderExecution defines package-level state shared by related execution paths.
	ErrLoaderExecution = errors.New("loader execution failed")
)

// ─── Core Runtime Types ────────────────────────────────────────────────────────

// RunError groups state used to coordinate related package behavior.
type RunError struct {
	Kind error
	Op   string
	Err  error
}

// Error returns the composed error message used by callers.
func (e *RunError) Error() string {
	if e == nil {
		return ""
	}
	if e.Op == "" {
		return e.Err.Error()
	}
	if e.Err == nil {
		return e.Op
	}
	return e.Op + ": " + e.Err.Error()
}

// Unwrap exposes the wrapped error for errors.Is and errors.As checks.
func (e *RunError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Is matches this error against sentinel kinds used by callers.
func (e *RunError) Is(target error) bool {
	if e == nil {
		return false
	}
	if target == nil {
		return false
	}
	return errors.Is(e.Kind, target)
}

// EnrichOptions groups state used to coordinate related package behavior.
type EnrichOptions struct {
	Enabled  bool
	All      bool
	Raw      string
	Policies []string
}

// Options groups state used to coordinate related package behavior.
type Options struct {
	URL                string
	InsecureSkipVerify bool
	Index              string
	SettingsFile       string
	MappingsFile       string
	PipelinesFile      string
	PoliciesFile       string
	TransformsFile     string
	DataFile           string
	BatchSize          int
	DeleteIndex        bool
	AddToIndex         bool
	FlushIndex         bool
	SyncManaged        bool
	AliasMode          bool
	KeepLast           int
	Nuke               bool
	IDField            string
	// BulkRetryAttempts controls total bulk request attempts, including the first attempt.
	BulkRetryAttempts int
	// BulkRetryBackoffBase controls the first retry wait for retryable bulk failures.
	BulkRetryBackoffBase time.Duration
	// BulkRetryBackoffMax caps exponential bulk retry waits.
	BulkRetryBackoffMax time.Duration
	User                string
	Pass                string
	APIKey              string
	TemplateVariables   map[string]string
	Enrich              EnrichOptions
}

// Result groups state used to coordinate related package behavior.
type Result struct {
	ResolvedAliasTarget string
	WriteIndex          string
	CreatedIndex        string
	DocumentsProcessed  int
	DocumentsSucceeded  int
	DocumentsFailed     int
	EnrichSelected      []string
	EnrichMissing       []string
	EnrichSucceeded     int
	EnrichFailed        int
	Warnings            []string
}

// bulkResponse groups state used to coordinate related package behavior.
type bulkResponse struct {
	Errors bool                          `json:"errors"`
	Items  []map[string]bulkItemResponse `json:"items"`
}

// bulkItemResponse groups state used to coordinate related package behavior.
type bulkItemResponse struct {
	Index  string         `json:"_index"`
	ID     string         `json:"_id"`
	Status int            `json:"status"`
	Error  *bulkItemError `json:"error,omitempty"`
}

// bulkItemError groups state used to coordinate related package behavior.
type bulkItemError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

// bulkInsertResult groups state used to coordinate related package behavior.
type bulkInsertResult struct {
	Succeeded int
	Failed    int
}

// namedDefinitions groups state used to coordinate related package behavior.
type namedDefinitions map[string]json.RawMessage

// templateVariables groups state used to coordinate related package behavior.
type templateVariables map[string]string

// enrichFlagValue groups state used to coordinate related package behavior.
type enrichFlagValue struct {
	enabled bool
	all     bool
	raw     string
}

// dataAction groups state used to coordinate related package behavior.
type dataAction string

const (
	// dataActionNone defines package-level values shared by related execution paths.
	dataActionNone dataAction = ""
	// dataActionAdd defines package-level values shared by related execution paths.
	dataActionAdd dataAction = "add"
	// dataActionFlush defines package-level values shared by related execution paths.
	dataActionFlush dataAction = "flush"
	// dataActionDelete defines package-level values shared by related execution paths.
	dataActionDelete dataAction = "delete"
)

const (
	// defaultBulkRetryAttempts defines package-level values shared by related execution paths.
	defaultBulkRetryAttempts = 4
	// defaultBulkRetryBackoffBase defines package-level values shared by related execution paths.
	defaultBulkRetryBackoffBase = 500 * time.Millisecond
	// defaultBulkRetryBackoffMax defines package-level values shared by related execution paths.
	defaultBulkRetryBackoffMax = 5 * time.Second
)

// enrichPolicySummary groups state used to coordinate related package behavior.
type enrichPolicySummary struct {
	Config map[string]struct {
		Name string `json:"name"`
	} `json:"config"`
}

// enrichPoliciesResponse groups state used to coordinate related package behavior.
type enrichPoliciesResponse struct {
	Policies []enrichPolicySummary `json:"policies"`
}

// enrichPhaseStatus holds the phase field returned by enrich policy execution.
type enrichPhaseStatus struct {
	Phase string `json:"phase"`
}

// enrichExecuteResponse groups state used to coordinate related package behavior.
type enrichExecuteResponse struct {
	Status *enrichPhaseStatus `json:"status,omitempty"`
	Task   *string            `json:"task,omitempty"`
}

// taskGetResponse holds the response from the Tasks API for async enrich policy execution.
type taskGetResponse struct {
	Completed bool `json:"completed"`
	Task      struct {
		Status *enrichPhaseStatus `json:"status,omitempty"`
	} `json:"task"`
	Response *enrichExecuteResponse `json:"response,omitempty"`
}

// managedPolicyPlan groups state used to coordinate related package behavior.
type managedPolicyPlan struct {
	LogicalNames     []string
	DesiredNames     []string
	Definitions      namedDefinitions
	LogicalToDesired map[string]string
	DesiredSet       map[string]struct{}
}

// transformDefinition groups state used to coordinate related package behavior.
type transformDefinition struct {
	SourceIndex string          `json:"source_index"`
	Body        json.RawMessage `json:"body"`
}

// mappingPreflightPlan groups state used to coordinate related package behavior.
type mappingPreflightPlan struct {
	DateDetectionExpected *bool
	RootFieldTypes        map[string]string
}

// enrichRunSummary groups state used to coordinate related package behavior.
type enrichRunSummary struct {
	Selected  []string
	Missing   []string
	Succeeded int
	Failed    int
}

// sleepWithContext centralizes retry sleeps so the code path can be reused by tests.
var sleepWithContext = sleepForDuration

// sleepForDuration centralizes this code path so package behavior stays consistent.
func sleepForDuration(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ─── Option Parsing Helpers ────────────────────────────────────────────────────

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

// enrichFromOptions centralizes this code path so package behavior stays consistent.
func enrichFromOptions(enrich EnrichOptions) *enrichFlagValue {
	selection := &enrichFlagValue{
		enabled: enrich.Enabled,
		all:     enrich.All,
		raw:     strings.TrimSpace(enrich.Raw),
	}
	if len(enrich.Policies) > 0 {
		selection.enabled = true
		selection.all = false
		selection.raw = strings.Join(enrich.Policies, ",")
	}
	if !selection.enabled {
		selection.all = false
		selection.raw = ""
	}
	if selection.enabled && !selection.all && selection.raw == "" {
		selection.all = true
	}
	return selection
}

// classifyRunErrorKind centralizes this code path so package behavior stays consistent.
func classifyRunErrorKind(op string) error {
	lowered := strings.ToLower(op)
	switch {
	case strings.Contains(lowered, "bulk"):
		return ErrBulkFailure
	case strings.Contains(lowered, "enrich"):
		return ErrEnrichExecution
	case strings.Contains(lowered, "pipeline"), strings.Contains(lowered, "policy"), strings.Contains(lowered, "transform"), strings.Contains(lowered, "managed"):
		return ErrManagedResource
	case strings.Contains(lowered, "index"), strings.Contains(lowered, "alias"):
		return ErrIndexOperation
	default:
		return ErrLoaderExecution
	}
}

// ─── Fatal/Error Bridging ──────────────────────────────────────────────────────

// fatalEvent groups state used to coordinate related package behavior.
type fatalEvent struct {
	event *zerolog.Event
	cause error
}

// fatal centralizes this code path so package behavior stays consistent.
func fatal() *fatalEvent {
	return &fatalEvent{event: log.WithLevel(zerolog.FatalLevel)}
}

// Str applies method-specific behavior to keep package workflows consistent.
func (f *fatalEvent) Str(key, value string) *fatalEvent {
	f.event.Str(key, value)
	return f
}

// Strs applies method-specific behavior to keep package workflows consistent.
func (f *fatalEvent) Strs(key string, value []string) *fatalEvent {
	f.event.Strs(key, value)
	return f
}

// Int applies method-specific behavior to keep package workflows consistent.
func (f *fatalEvent) Int(key string, value int) *fatalEvent {
	f.event.Int(key, value)
	return f
}

// Float64 applies method-specific behavior to keep package workflows consistent.
func (f *fatalEvent) Float64(key string, value float64) *fatalEvent {
	f.event.Float64(key, value)
	return f
}

// Err applies method-specific behavior to keep package workflows consistent.
func (f *fatalEvent) Err(err error) *fatalEvent {
	if err != nil {
		f.cause = err
	}
	f.event.Err(err)
	return f
}

// panicWithMessage applies method-specific behavior to keep package workflows consistent.
func (f *fatalEvent) panicWithMessage(message string) {
	f.event.Msg(message)
	cause := f.cause
	if cause == nil {
		cause = errors.New(message)
	}
	panic(&RunError{
		Kind: classifyRunErrorKind(message),
		Op:   message,
		Err:  cause,
	})
}

// Msg emits a fatal event message and terminates the current run path.
func (f *fatalEvent) Msg(message string) {
	f.panicWithMessage(message)
}

// Msgf formats a fatal event message and terminates the current run path.
func (f *fatalEvent) Msgf(format string, args ...any) {
	f.panicWithMessage(fmt.Sprintf(format, args...))
}

// withTimestampLogger centralizes this code path so package behavior stays consistent.
func withTimestampLogger(base zerolog.Logger) zerolog.Logger {
	return base.With().Timestamp().Logger()
}

// ─── Primary Execution Entry Points ────────────────────────────────────────────

// Run centralizes this code path so package behavior stays consistent.
func Run(ctx context.Context, opts Options) (result Result, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	previousLogger := log.Logger
	log.Logger = withTimestampLogger(log.Logger)
	defer func() {
		log.Logger = previousLogger
	}()

	url := &opts.URL
	insecure := &opts.InsecureSkipVerify
	index := &opts.Index
	settingsFile := &opts.SettingsFile
	mappingsFile := &opts.MappingsFile
	pipelinesFile := &opts.PipelinesFile
	policiesFile := &opts.PoliciesFile
	transformsFile := &opts.TransformsFile
	dataFile := &opts.DataFile
	batchSize := &opts.BatchSize
	deleteIndex := &opts.DeleteIndex
	addToIndex := &opts.AddToIndex
	flushIndex := &opts.FlushIndex
	syncManaged := &opts.SyncManaged
	aliasMode := &opts.AliasMode
	keepLast := &opts.KeepLast
	nuke := &opts.Nuke
	idField := &opts.IDField
	bulkRetryAttempts := &opts.BulkRetryAttempts
	bulkRetryBackoffBase := &opts.BulkRetryBackoffBase
	bulkRetryBackoffMax := &opts.BulkRetryBackoffMax
	user := &opts.User
	pass := &opts.Pass
	apiKey := &opts.APIKey
	templateVariables := &opts.TemplateVariables
	enrich := enrichFromOptions(opts.Enrich)

	if *url == "" {
		*url = "http://localhost:9200"
	}
	if *batchSize <= 0 {
		*batchSize = 1000
	}
	if *bulkRetryAttempts <= 0 {
		*bulkRetryAttempts = defaultBulkRetryAttempts
	}
	if *bulkRetryBackoffBase <= 0 {
		*bulkRetryBackoffBase = defaultBulkRetryBackoffBase
	}
	if *bulkRetryBackoffMax <= 0 {
		*bulkRetryBackoffMax = defaultBulkRetryBackoffMax
	}
	if *bulkRetryBackoffMax < *bulkRetryBackoffBase {
		*bulkRetryBackoffMax = *bulkRetryBackoffBase
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			switch typed := recovered.(type) {
			case *RunError:
				err = typed
			case error:
				err = &RunError{Kind: ErrLoaderExecution, Op: "panic", Err: typed}
			default:
				panic(recovered)
			}
		}
	}()

	warn := func(message string) {
		result.Warnings = append(result.Warnings, message)
		log.Warn().Msg(message)
	}

	if (*user != "" || *pass != "") && *apiKey != "" {
		return result, &RunError{Kind: ErrInvalidOptions, Op: "validating auth options", Err: fmt.Errorf("cannot use both basic auth and API key")}
	}

	action, err := selectedDataAction(*addToIndex, *flushIndex, *deleteIndex)
	if err != nil {
		return result, &RunError{Kind: ErrInvalidOptions, Op: "selecting data action", Err: err}
	}
	effectiveSyncManaged := *syncManaged

	if *index == "" {
		return result, &RunError{Kind: ErrInvalidOptions, Op: "validating index option", Err: fmt.Errorf("-index is required")}
	}

	if action.requiresDataFile() && *dataFile == "" {
		return result, &RunError{Kind: ErrInvalidOptions, Op: "validating data option", Err: fmt.Errorf("-data is required for -add, -flush, and -delete")}
	}

	if action == dataActionNone && !*syncManaged && !*nuke && !enrich.enabled {
		return result, &RunError{Kind: ErrInvalidOptions, Op: "validating action selection", Err: fmt.Errorf("one of data action, -sync-managed, -nuke, or -enrich is required")}
	}
	if *keepLast < 0 {
		return result, &RunError{Kind: ErrInvalidOptions, Op: "validating keep-last", Err: fmt.Errorf("-keep-last must be 0 or greater")}
	}
	if *keepLast > 0 && !*aliasMode {
		warn("Ignoring -keep-last because -alias is not enabled")
	}
	if *aliasMode && action == dataActionDelete && !*syncManaged {
		effectiveSyncManaged = true
		warn("Alias delete detected without -sync-managed; assuming -sync-managed for this run. Add -sync-managed explicitly on the command line.")
	}
	if *aliasMode && action == dataActionDelete && *keepLast == 0 {
		warn("Alias delete detected without -keep-last; no old timestamped indices will be deleted and storage usage can grow over time.")
	}
	if strings.TrimSpace(*transformsFile) != "" && !effectiveSyncManaged {
		warn("Ignoring -transforms because -sync-managed is not enabled")
	}

	cfg := elasticsearch.Config{
		Addresses:    []string{*url},
		DisableRetry: true,
		MaxRetries:   0,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: *insecure,
			},
		},
	}

	if *user != "" && *pass != "" {
		cfg.Username = *user
		cfg.Password = *pass
	}
	if *apiKey != "" {
		cfg.APIKey = *apiKey
	}
	es, err := elasticsearch.NewClient(cfg)
	checkErr("creating Elasticsearch client", err)

	variables := buildTemplateVariables(*index, *templateVariables)
	pipelineDefinitions, pipelineNames := readNamedDefinitions(*pipelinesFile, "pipeline", variables)
	logicalPolicyDefinitions, logicalPolicyNames := readNamedDefinitions(*policiesFile, "policy", variables)
	logicalTransformDefinitions, logicalTransformNames := readNamedDefinitions(*transformsFile, "transform", variables)
	policyPlan := buildManagedPolicyPlan(logicalPolicyDefinitions, logicalPolicyNames)
	policyNameMapping := make(map[string]string, len(policyPlan.LogicalToDesired))
	for logical, desired := range policyPlan.LogicalToDesired {
		policyNameMapping[logical] = desired
	}
	resolvePipelinePolicyFallbacks(es, pipelineDefinitions, pipelineNames, policyNameMapping)
	pipelineDefinitions = rewritePipelinePolicyReferences(pipelineDefinitions, pipelineNames, policyNameMapping)
	policyDefinitions := policyPlan.Definitions
	policyNames := policyPlan.DesiredNames
	policyDeleteNames := resolveManagedPolicyDeleteNames(es, policyPlan.LogicalNames)
	transformDefinitions, transformNames, transformErr := resolveTransformsForSource(logicalTransformDefinitions, logicalTransformNames, *index)
	if transformErr != nil {
		fatal().Err(transformErr).Msg("Failed to resolve transform definitions")
	}
	logEffectiveManagedAssets(
		*index,
		*settingsFile,
		*mappingsFile,
		*pipelinesFile,
		*policiesFile,
		*transformsFile,
		pipelineNames,
		policyNames,
		logicalTransformNames,
		transformNames,
	)
	enrichSelection := remapEnrichSelection(enrich, policyNameMapping)
	defaultPipeline := ""
	if effectiveSyncManaged && len(pipelineNames) > 0 {
		defaultPipeline = pipelineNames[0]
	}

	aliasTargets := []string(nil)
	exists := false
	if *aliasMode {
		aliasTargets = resolveAliasTargets(es, *index)
		if len(aliasTargets) == 0 {
			indexPresent, err := indexExists(es, *index)
			checkErr("checking if index exists", err)
			if indexPresent {
				aliasTargets = []string{*index}
				warn(fmt.Sprintf("Found concrete index with alias name %q; treating it as the current write target", *index))
			}
		}
		exists = len(aliasTargets) > 0
		if exists {
			log.Info().Str("alias", *index).Strs("indices", aliasTargets).Msg("Resolved alias targets")
		} else {
			log.Info().Str("alias", *index).Msg("Alias has no current indices")
		}
	} else {
		var err error
		exists, err = indexExists(es, *index)
		checkErr("checking if index exists", err)
	}

	if *nuke {
		if *aliasMode {
			if len(aliasTargets) > 0 {
				log.Warn().Str("alias", *index).Strs("indices", aliasTargets).Msg("Nuke deleting alias target indices and declared managed resources")
				deleteIndices(es, aliasTargets)
				aliasTargets = nil
			} else {
				warn(fmt.Sprintf("Alias %q does not currently resolve to an index. Nuke will still remove declared managed resources", *index))
			}
			generations := listTimestampedIndices(es, *index)
			if len(generations) > 0 {
				names := make([]string, 0, len(generations))
				for _, generation := range generations {
					names = append(names, generation.Name)
				}
				log.Warn().Str("alias", *index).Strs("indices", names).Msg("Nuke deleting timestamped indices that match alias pattern")
				deleteIndices(es, names)
			}
			exists = false
		} else {
			if exists {
				log.Warn().Str("index", *index).Msg("Nuke deleting index and declared managed resources")
				deleteAndCheck(es, *index)
				exists = false
			} else {
				warn(fmt.Sprintf("Index %q does not exist. Nuke will still remove declared managed resources", *index))
			}
		}

		deleteManagedResources(es, pipelineNames, policyDeleteNames, transformNames, true)
	}

	switch action {
	case dataActionDelete:
		if *aliasMode {
			if len(aliasTargets) > 0 {
				log.Info().Str("alias", *index).Strs("indices", aliasTargets).Msg("Alias mode delete will roll forward to a new timestamped index")
			} else {
				warn(fmt.Sprintf("Alias %q has no indices. Nothing to delete.", *index))
			}
			exists = false
		} else {
			if exists {
				log.Info().Str("index", *index).Msg("Deleting index before reloading data")
				deleteAndCheck(es, *index)
				exists = false
			} else {
				warn(fmt.Sprintf("Index %q does not exist. Nothing to delete.", *index))
			}
		}

		if *aliasMode {
			log.Info().Str("alias", *index).Msg("Alias mode delete keeps existing managed resources (pipelines, policies, transforms); use -nuke for destructive managed-resource cleanup")
		} else {
			deleteManagedResources(es, pipelineNames, policyDeleteNames, transformNames, false)
		}
	case dataActionFlush:
		if *aliasMode {
			if len(aliasTargets) > 0 {
				log.Info().Str("alias", *index).Strs("indices", aliasTargets).Msg("Flushing alias target indices before loading replacement data")
				for _, target := range aliasTargets {
					flushAndCheck(es, target)
				}
				exists = true
			} else {
				warn(fmt.Sprintf("Alias %q has no indices. Nothing to flush.", *index))
				exists = false
			}
		} else {
			if exists {
				log.Info().Str("index", *index).Msg("Flushing existing index before loading replacement data")
				flushAndCheck(es, *index)
			} else {
				warn(fmt.Sprintf("Index %q does not exist. Nothing to flush.", *index))
			}
		}
	case dataActionAdd:
		if *aliasMode {
			if len(aliasTargets) > 0 {
				log.Info().Str("alias", *index).Strs("indices", aliasTargets).Msg("Appending documents to existing alias target index")
				exists = true
			} else {
				log.Info().Str("alias", *index).Msg("Creating first timestamped index for alias before appending documents")
				exists = false
			}
		} else {
			if exists {
				log.Info().Str("index", *index).Msg("Appending documents to existing index")
			} else {
				log.Info().Str("index", *index).Msg("Creating index to append documents")
			}
		}
	}

	shouldCreateIndex := !exists && (action.requiresDataFile() || (effectiveSyncManaged && (*settingsFile != "" || *mappingsFile != "")))
	writeIndex := *index
	createdIndex := ""
	if *aliasMode && shouldCreateIndex {
		createdIndex = nextAvailableTimestampedIndexName(es, *index, time.Now().UTC())
		writeIndex = createdIndex
		log.Info().Str("alias", *index).Str("index", createdIndex).Msg("Preparing timestamped index for alias")
	}
	result.WriteIndex = writeIndex
	result.CreatedIndex = createdIndex
	if *aliasMode {
		result.ResolvedAliasTarget = *index
	}

	if shouldCreateIndex && effectiveSyncManaged {
		createPipelines(es, pipelineDefinitions, pipelineNames)
	}

	if shouldCreateIndex {
		body := buildCreateIndexBody(es, *settingsFile, *mappingsFile, defaultPipeline, variables)
		createIndex := *index
		if *aliasMode {
			createIndex = createdIndex
		}
		res, err := es.Indices.Create(createIndex, es.Indices.Create.WithBody(strings.NewReader(body)))
		checkErr("creating index", err)
		defer res.Body.Close()
		if res.IsError() {
			responseBody, _ := io.ReadAll(res.Body)
			fatal().
				Str("index", createIndex).
				Int("status_code", res.StatusCode).
				Str("body", string(responseBody)).
				Msg("Failed to create index")
		}
		waitForIndex(es, createIndex)
		exists = true
		if *aliasMode {
			log.Info().Str("alias", *index).Str("index", createIndex).Msg("Index created for alias")
		} else {
			log.Info().Str("index", *index).Msg("Index created")
		}
	}

	deferPolicyCreationUntilAliasSwap := effectiveSyncManaged && *aliasMode && shouldCreateIndex
	transformSyncNames := make([]string, 0)
	if effectiveSyncManaged && exists {
		if !shouldCreateIndex {
			createPipelines(es, pipelineDefinitions, pipelineNames)
		}
		if len(transformNames) > 0 {
			transformSyncNames = append(transformSyncNames, transformNames...)
		}
		if !deferPolicyCreationUntilAliasSwap {
			createPolicies(es, policyDefinitions, policyNames)
			garbageCollectManagedPolicies(es, policyPlan.LogicalNames, policyPlan.DesiredSet)
		}
	}

	if action.requiresDataFile() {
		preflightPlan, err := buildMappingPreflightPlan(*mappingsFile, variables)
		if err != nil {
			fatal().Err(err).Str("path", *mappingsFile).Msg("Failed to build mapping preflight plan")
		}
		if preflightPlan.hasExpectations() {
			if err := verifyMappingPreflight(es, writeIndex, preflightPlan); err != nil {
				fatal().Err(err).Str("index", writeIndex).Msg("Mapping preflight failed before bulk insert")
			}
		}
		log.Info().Msg("Starting bulk insert")

		f, err := os.Open(*dataFile)
		checkErr("opening data file", err)
		defer f.Close()

		dec := json.NewDecoder(f)
		tok, err := dec.Token()
		if err != nil || tok != json.Delim('[') {
			fatal().Msg("Data file must be a JSON array")
		}

		log.Debug().Str("data_file", *dataFile).Msg("Counting documents in data file")
		total := 0
		for dec.More() {
			var tmp map[string]interface{}
			if err := dec.Decode(&tmp); err != nil {
				fatal().Err(err).Msg("Error counting objects in data file")
			}
			total++
		}
		log.Debug().Str("data_file", *dataFile).Int("total", total).Msg("Document count complete")

		if _, err := f.Seek(0, 0); err != nil {
			fatal().Err(err).Msg("Error rewinding data file")
		}
		dec = json.NewDecoder(f)
		_, err = dec.Token()
		if err != nil {
			fatal().Err(err).Msg("Error re-reading data file")
		}

		overallStart := time.Now()
		batch := make([]map[string]interface{}, 0, *batchSize)
		processed := 0
		succeededTotal := 0
		failedTotal := 0
		for dec.More() {
			var doc map[string]interface{}
			if err := dec.Decode(&doc); err != nil {
				fatal().Err(err).Msg("Error decoding object in data file")
			}
			batch = append(batch, doc)
			if len(batch) == *batchSize {
				batchResult := bulkInsert(
					ctx,
					es,
					writeIndex,
					batch,
					processed+len(batch),
					total,
					*bulkRetryAttempts,
					*bulkRetryBackoffBase,
					*bulkRetryBackoffMax,
					*idField,
				)
				processed += len(batch)
				succeededTotal += batchResult.Succeeded
				failedTotal += batchResult.Failed
				batch = batch[:0]
			}
		}
		if len(batch) > 0 {
			batchResult := bulkInsert(
				ctx,
				es,
				writeIndex,
				batch,
				processed+len(batch),
				total,
				*bulkRetryAttempts,
				*bulkRetryBackoffBase,
				*bulkRetryBackoffMax,
				*idField,
			)
			processed += len(batch)
			succeededTotal += batchResult.Succeeded
			failedTotal += batchResult.Failed
		}

		overallDuration := time.Since(overallStart)
		log.Info().
			Int("processed", processed).
			Int("succeeded", succeededTotal).
			Int("failed", failedTotal).
			Float64("total_time", overallDuration.Seconds()).
			Msg("Bulk load completed")

		if failedTotal > 0 {
			log.Warn().
				Int("failed", failedTotal).
				Msg("Bulk load completed with failed items")
		}

		result.DocumentsProcessed = processed
		result.DocumentsSucceeded = succeededTotal
		result.DocumentsFailed = failedTotal
	}

	if *aliasMode && shouldCreateIndex {
		updateAlias(es, *index, createdIndex)
	}
	if deferPolicyCreationUntilAliasSwap {
		createPolicies(es, policyDefinitions, policyNames)
		garbageCollectManagedPolicies(es, policyPlan.LogicalNames, policyPlan.DesiredSet)
	}
	if *aliasMode && *keepLast > 0 {
		pruneTimestampedIndices(es, *index, *keepLast)
	}

	if enrich.enabled {
		refreshIndex(es, writeIndex)
		enrichResult := runEnrichPolicies(es, enrichSelection, policyNames)
		result.EnrichSelected = enrichResult.Selected
		result.EnrichMissing = enrichResult.Missing
		result.EnrichSucceeded = enrichResult.Succeeded
		result.EnrichFailed = enrichResult.Failed
	}
	if effectiveSyncManaged && len(transformSyncNames) > 0 {
		stopTransformsBestEffort(es, transformSyncNames)
		createOrUpdateTransforms(es, transformDefinitions, transformSyncNames)
		startTransforms(es, transformSyncNames)
	}

	return result, nil
}

// SyncManaged centralizes this code path so package behavior stays consistent.
func SyncManaged(ctx context.Context, opts Options) (Result, error) {
	opts.AddToIndex = false
	opts.FlushIndex = false
	opts.DeleteIndex = false
	opts.Nuke = false
	opts.SyncManaged = true
	return Run(ctx, opts)
}

// LoadData centralizes this code path so package behavior stays consistent.
func LoadData(ctx context.Context, opts Options) (Result, error) {
	if !opts.AddToIndex && !opts.FlushIndex && !opts.DeleteIndex {
		opts.AddToIndex = true
	}
	return Run(ctx, opts)
}

// ExecuteEnrich centralizes this code path so package behavior stays consistent.
func ExecuteEnrich(ctx context.Context, opts Options) (Result, error) {
	opts.AddToIndex = false
	opts.FlushIndex = false
	opts.DeleteIndex = false
	opts.Nuke = false
	if !opts.Enrich.Enabled {
		opts.Enrich = EnrichOptions{Enabled: true, All: true}
	}
	return Run(ctx, opts)
}

// ─── Managed Asset and Mapping Preflight Helpers ──────────────────────────────

// logEffectiveManagedAssets centralizes this code path so package behavior stays consistent.
func logEffectiveManagedAssets(index, settingsFile, mappingsFile, pipelinesFile, policiesFile, transformsFile string, pipelineNames, policyNames, logicalTransformNames, selectedTransformNames []string) {
	log.Info().
		Str("index", index).
		Str("settings_file", displayManagedAsset(settingsFile)).
		Str("mappings_file", displayManagedAsset(mappingsFile)).
		Str("pipelines_file", displayManagedAsset(pipelinesFile)).
		Int("pipelines_declared", len(pipelineNames)).
		Str("policies_file", displayManagedAsset(policiesFile)).
		Int("policies_declared", len(policyNames)).
		Str("transforms_file", displayManagedAsset(transformsFile)).
		Int("transforms_declared", len(logicalTransformNames)).
		Int("transforms_selected", len(selectedTransformNames)).
		Msg("Effective managed assets resolved")
}

// displayManagedAsset centralizes this code path so package behavior stays consistent.
func displayManagedAsset(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "none"
	}
	return trimmed
}

// hasExpectations applies method-specific behavior to keep package workflows consistent.
func (p mappingPreflightPlan) hasExpectations() bool {
	return p.DateDetectionExpected != nil || len(p.RootFieldTypes) > 0
}

// buildMappingPreflightPlan centralizes this code path so package behavior stays consistent.
func buildMappingPreflightPlan(mappingsFile string, variables templateVariables) (mappingPreflightPlan, error) {
	trimmed := strings.TrimSpace(mappingsFile)
	if trimmed == "" {
		return mappingPreflightPlan{}, nil
	}

	normalized := normalizeIndexSection(trimmed, "mappings", variables)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(normalized), &parsed); err != nil {
		return mappingPreflightPlan{}, fmt.Errorf("parsing mappings preflight source: %w", err)
	}

	plan := mappingPreflightPlan{
		RootFieldTypes: make(map[string]string),
	}
	if value, ok := parsed["date_detection"]; ok {
		if expected, ok := value.(bool); ok {
			plan.DateDetectionExpected = &expected
		}
	}

	properties, _ := parsed["properties"].(map[string]any)
	for fieldName, raw := range properties {
		fieldMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		fieldType, _ := fieldMap["type"].(string)
		if strings.TrimSpace(fieldType) == "" {
			continue
		}
		plan.RootFieldTypes[fieldName] = fieldType
	}

	return plan, nil
}

// verifyMappingPreflight centralizes this code path so package behavior stays consistent.
func verifyMappingPreflight(es *elasticsearch.Client, index string, plan mappingPreflightPlan) error {
	mappings, err := fetchIndexMappings(es, index)
	if err != nil {
		return err
	}

	if plan.DateDetectionExpected != nil {
		actualDateDetection, present := extractDateDetection(mappings)
		if !present {
			actualDateDetection = true
		}
		if actualDateDetection != *plan.DateDetectionExpected {
			return fmt.Errorf("mappings.date_detection is %t, expected %t", actualDateDetection, *plan.DateDetectionExpected)
		}
	}

	for fieldPath, expectedType := range plan.RootFieldTypes {
		actualType, ok := resolveFieldType(mappings, fieldPath)
		if !ok {
			return fmt.Errorf("required mapping for field %q is missing", fieldPath)
		}
		if actualType != expectedType {
			return fmt.Errorf("field %q has type %q, expected %q", fieldPath, actualType, expectedType)
		}
	}

	event := log.Info().
		Str("index", index).
		Int("root_field_type_checks", len(plan.RootFieldTypes))
	if plan.DateDetectionExpected != nil {
		event = event.Bool("date_detection_expected", *plan.DateDetectionExpected)
	}
	event.Msg("Mapping preflight passed")
	return nil
}

// fetchIndexMappings centralizes this code path so package behavior stays consistent.
func fetchIndexMappings(es *elasticsearch.Client, index string) (map[string]any, error) {
	res, err := es.Indices.GetMapping(es.Indices.GetMapping.WithIndex(index))
	if err != nil {
		return nil, fmt.Errorf("fetching mappings for index %q: %w", index, err)
	}
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)
	if res.IsError() {
		return nil, fmt.Errorf("fetching mappings for index %q failed with status %d: %s", index, res.StatusCode, string(body))
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decoding mappings response for index %q: %w", index, err)
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("mappings response for index %q was empty", index)
	}

	var selected map[string]any
	if raw, ok := payload[index]; ok {
		selected, _ = raw.(map[string]any)
	} else {
		for _, raw := range payload {
			selected, _ = raw.(map[string]any)
			if selected != nil {
				break
			}
		}
	}
	if selected == nil {
		return nil, fmt.Errorf("mappings response for index %q did not include an object entry", index)
	}

	mappings, ok := selected["mappings"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("mappings response for index %q did not include a mappings object", index)
	}
	return mappings, nil
}

// extractDateDetection centralizes this code path so package behavior stays consistent.
func extractDateDetection(mappings map[string]any) (bool, bool) {
	value, ok := mappings["date_detection"]
	if !ok {
		return false, false
	}

	typed, ok := value.(bool)
	if !ok {
		return false, false
	}
	return typed, true
}

// resolveFieldType centralizes this code path so package behavior stays consistent.
func resolveFieldType(mappings map[string]any, fieldPath string) (string, bool) {
	current, ok := mappings["properties"].(map[string]any)
	if !ok {
		return "", false
	}

	segments := strings.Split(strings.TrimSpace(fieldPath), ".")
	for i, segment := range segments {
		if segment == "" {
			return "", false
		}

		fieldRaw, ok := current[segment]
		if !ok {
			return "", false
		}
		fieldMap, ok := fieldRaw.(map[string]any)
		if !ok {
			return "", false
		}

		if i == len(segments)-1 {
			fieldType, ok := fieldMap["type"].(string)
			if !ok {
				return "", false
			}
			return fieldType, true
		}

		current, ok = fieldMap["properties"].(map[string]any)
		if !ok {
			return "", false
		}
	}

	return "", false
}

// ─── Index/Alias Utilities ─────────────────────────────────────────────────────

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

// buildTimestampedIndexName centralizes this code path so package behavior stays consistent.
func buildTimestampedIndexName(alias string, now time.Time) string {
	return fmt.Sprintf("%s-%s", alias, now.Format("20060102150405"))
}

// nextAvailableTimestampedIndexName centralizes this code path so package behavior stays consistent.
func nextAvailableTimestampedIndexName(es *elasticsearch.Client, alias string, base time.Time) string {
	name, err := nextAvailableTimestampedIndexNameWithCheck(alias, base, func(candidate string) (bool, error) {
		return indexExists(es, candidate)
	})
	checkErr("finding available timestamped index name", err)
	return name
}

// nextAvailableTimestampedIndexNameWithCheck centralizes this code path so package behavior stays consistent.
func nextAvailableTimestampedIndexNameWithCheck(alias string, base time.Time, existsFn func(candidate string) (bool, error)) (string, error) {
	candidateTime := base.UTC()
	for attempt := 0; attempt < 300; attempt++ {
		candidate := buildTimestampedIndexName(alias, candidateTime)
		exists, err := existsFn(candidate)
		if err != nil {
			return "", err
		}
		if !exists {
			return candidate, nil
		}

		log.Warn().
			Str("alias", alias).
			Str("index", candidate).
			Msg("Timestamped index name already exists; advancing by one second")
		candidateTime = candidateTime.Add(time.Second)
	}

	return "", fmt.Errorf("unable to find an available timestamped index name for alias %q", alias)
}

// resolveAliasTargets centralizes this code path so package behavior stays consistent.
func resolveAliasTargets(es *elasticsearch.Client, alias string) []string {
	res, err := es.Indices.GetAlias(es.Indices.GetAlias.WithName(alias))
	checkErr("resolving alias targets", err)
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		fatal().
			Str("alias", alias).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to resolve alias targets")
	}

	var parsed map[string]json.RawMessage
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		fatal().Err(err).Str("alias", alias).Msg("Unable to parse alias response")
	}

	targets := make([]string, 0, len(parsed))
	for index := range parsed {
		targets = append(targets, index)
	}
	slices.Sort(targets)
	return targets
}

// updateAlias centralizes this code path so package behavior stays consistent.
func updateAlias(es *elasticsearch.Client, alias, index string) {
	current := resolveAliasTargets(es, alias)

	actions := make([]map[string]map[string]any, 0, len(current)+1)
	for _, existing := range current {
		actions = append(actions, map[string]map[string]any{
			"remove": {
				"index": existing,
				"alias": alias,
			},
		})
	}
	actions = append(actions, map[string]map[string]any{
		"add": {
			"index":          index,
			"alias":          alias,
			"is_write_index": true,
		},
	})

	payload, err := json.Marshal(map[string]any{"actions": actions})
	checkErr("serializing alias actions", err)
	res, err := es.Indices.UpdateAliases(strings.NewReader(string(payload)))
	checkErr("updating alias", err)
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		fatal().
			Str("alias", alias).
			Str("index", index).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to update alias")
	}

	log.Info().Str("alias", alias).Str("index", index).Msg("Alias now points to index")
}

// deleteIndices centralizes this code path so package behavior stays consistent.
func deleteIndices(es *elasticsearch.Client, indices []string) {
	for _, index := range indices {
		deleteAndCheck(es, index)
		log.Info().Str("index", index).Msg("Index deleted")
	}
}

// timestampedIndex groups state used to coordinate related package behavior.
type timestampedIndex struct {
	Name      string
	Timestamp time.Time
}

// listTimestampedIndices centralizes this code path so package behavior stays consistent.
func listTimestampedIndices(es *elasticsearch.Client, alias string) []timestampedIndex {
	pattern := alias + "-*"
	res, err := es.Indices.Get([]string{pattern})
	checkErr("listing timestamped indices", err)
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		fatal().
			Str("alias", alias).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to list timestamped indices")
	}

	var parsed map[string]json.RawMessage
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		fatal().Err(err).Str("alias", alias).Msg("Unable to parse timestamped index response")
	}

	result := make([]timestampedIndex, 0, len(parsed))
	for index := range parsed {
		timestamp, ok := parseTimestampedIndexName(alias, index)
		if !ok {
			continue
		}
		result = append(result, timestampedIndex{Name: index, Timestamp: timestamp})
	}
	return result
}

// parseTimestampedIndexName centralizes this code path so package behavior stays consistent.
func parseTimestampedIndexName(alias, index string) (time.Time, bool) {
	prefix := alias + "-"
	if !strings.HasPrefix(index, prefix) {
		return time.Time{}, false
	}
	suffix := strings.TrimPrefix(index, prefix)
	if len(suffix) != 14 {
		return time.Time{}, false
	}
	for _, ch := range suffix {
		if ch < '0' || ch > '9' {
			return time.Time{}, false
		}
	}

	parsed, err := time.Parse("20060102150405", suffix)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

// pruneTimestampedIndices centralizes this code path so package behavior stays consistent.
func pruneTimestampedIndices(es *elasticsearch.Client, alias string, keepLast int) {
	if keepLast <= 0 {
		return
	}

	all := listTimestampedIndices(es, alias)
	if len(all) <= keepLast {
		return
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Timestamp.After(all[j].Timestamp)
	})

	toDelete := make([]string, 0, len(all)-keepLast)
	for _, entry := range all[keepLast:] {
		toDelete = append(toDelete, entry.Name)
	}

	log.Info().Str("alias", alias).Int("keep_last", keepLast).Strs("indices", toDelete).Msg("Pruning old timestamped indices")
	deleteIndices(es, toDelete)
}

// ─── Settings, Mappings, and Template Utilities ───────────────────────────────

// checkErr centralizes this code path so package behavior stays consistent.
func checkErr(op string, err error) {
	log.Trace().Msg(op)
	if err != nil {
		panic(&RunError{
			Kind: classifyRunErrorKind(op),
			Op:   op,
			Err:  err,
		})
	}
}

// requiresDataFile applies method-specific behavior to keep package workflows consistent.
func (a dataAction) requiresDataFile() bool {
	return a != dataActionNone
}

// selectedDataAction centralizes this code path so package behavior stays consistent.
func selectedDataAction(addToIndex, flushIndex, deleteIndex bool) (dataAction, error) {
	actionCount := 0
	if addToIndex {
		actionCount++
	}
	if flushIndex {
		actionCount++
	}
	if deleteIndex {
		actionCount++
	}
	if actionCount > 1 {
		return dataActionNone, fmt.Errorf("-add, -flush, and -delete are mutually exclusive")
	}

	switch {
	case addToIndex:
		return dataActionAdd, nil
	case flushIndex:
		return dataActionFlush, nil
	case deleteIndex:
		return dataActionDelete, nil
	default:
		return dataActionNone, nil
	}
}

// indexExists centralizes this code path so package behavior stays consistent.
func indexExists(es *elasticsearch.Client, index string) (bool, error) {
	res, err := es.Indices.Exists([]string{index})
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status code %d", res.StatusCode)
	}
}

// waitForIndex centralizes this code path so package behavior stays consistent.
func waitForIndex(es *elasticsearch.Client, index string) {
	for i := 0; i < 20; i++ {
		exists, err := indexExists(es, index)
		checkErr("waiting for index creation", err)
		if exists {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}

	fatal().Str("index", index).Msg("Index create was acknowledged but the index did not become visible")
}

// deleteAndCheck centralizes this code path so package behavior stays consistent.
func deleteAndCheck(es *elasticsearch.Client, index string) {
	res, err := es.Indices.Delete([]string{index})
	checkErr("deleting index", err)
	defer res.Body.Close()

	if res.IsError() {
		fatal().Str("index", index).Msg("Failed to delete index")
	}
}

// flushAndCheck centralizes this code path so package behavior stays consistent.
func flushAndCheck(es *elasticsearch.Client, index string) {
	query := `{"query":{"match_all":{}}}`
	res, err := es.DeleteByQuery(
		[]string{index},
		strings.NewReader(query),
		es.DeleteByQuery.WithConflicts("proceed"),
		es.DeleteByQuery.WithRefresh(true),
	)
	checkErr("flushing index", err)
	defer res.Body.Close()

	if res.IsError() {
		fatal().Str("index", index).Msg("Failed to flush index")
	}
}

// clusterReplicaCount queries the cluster node count and returns the appropriate
// number_of_replicas value for the topology.
// ADR: docs/decisions/0001-replica-count-from-node-count.md
func clusterReplicaCount(es *elasticsearch.Client) int {
	if es == nil {
		return 0
	}
	res, err := es.Nodes.Info()
	if err != nil {
		log.Warn().Err(err).Msg("Could not query cluster nodes; defaulting to 0 replicas")
		return 0
	}
	defer res.Body.Close()
	if res.IsError() {
		log.Warn().Int("status_code", res.StatusCode).Msg("Cluster nodes query failed; defaulting to 0 replicas")
		return 0
	}

	var body struct {
		Nodes map[string]json.RawMessage `json:"nodes"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		log.Warn().Err(err).Msg("Could not parse cluster nodes response; defaulting to 0 replicas")
		return 0
	}

	if len(body.Nodes) > 1 {
		return 1
	}
	return 0
}

// buildCreateIndexBody centralizes this code path so package behavior stays consistent.
func buildCreateIndexBody(es *elasticsearch.Client, settingsFile, mappingsFile, defaultPipeline string, variables templateVariables) string {
	// ADR: docs/decisions/0001-replica-count-from-node-count.md
	replicas := clusterReplicaCount(es)
	settings := normalizeIndexSettings(settingsFile, defaultPipeline, replicas, variables)
	mappings := normalizeIndexSection(mappingsFile, "mappings", variables)

	return fmt.Sprintf(`{"settings": %s, "mappings": %s}`, settings, mappings)
}

// normalizeIndexSettings centralizes this code path so package behavior stays consistent.
func normalizeIndexSettings(path, defaultPipeline string, replicas int, variables templateVariables) string {
	settings := make(map[string]json.RawMessage)
	if path != "" {
		content, err := readTemplatedFile(path, variables)
		if err != nil {
			fatal().Err(err).Str("path", path).Msg("Reading settings file")
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(content, &raw); err != nil {
			fatal().Err(err).Str("path", path).Msg("Parsing settings file")
		}

		source := raw
		if nested, ok := raw["settings"]; ok {
			source = nil
			if err := json.Unmarshal(nested, &source); err != nil {
				fatal().Err(err).Str("path", path).Msg("Parsing nested settings file")
			}
		}

		for key, value := range source {
			if key == "index" {
				var nested map[string]json.RawMessage
				if err := json.Unmarshal(value, &nested); err == nil {
					for nestedKey, nestedValue := range nested {
						normalizedKey := normalizeSettingKey(nestedKey)
						if _, exists := settings[normalizedKey]; exists {
							continue
						}
						settings[normalizedKey] = nestedValue
					}
					continue
				}
			}

			settings[normalizeSettingKey(key)] = value
		}
	}

	if defaultPipeline != "" {
		if _, ok := settings["default_pipeline"]; !ok {
			settings["default_pipeline"] = json.RawMessage(strconv.Quote(defaultPipeline))
			log.Info().Str("pipeline", defaultPipeline).Msg("Using first declared pipeline as index.default_pipeline")
		}
	}

	// ADR: docs/decisions/0001-replica-count-from-node-count.md — override whatever
	// the settings file specifies so the value is always topology-correct.
	settings["number_of_replicas"] = json.RawMessage(strconv.Itoa(replicas))
	log.Info().Int("number_of_replicas", replicas).Msg("Setting index replica count from cluster topology")

	normalized, err := json.Marshal(settings)
	if err != nil {
		fatal().Err(err).Str("path", path).Msg("Serializing settings")
	}

	return string(normalized)
}

// normalizeSettingKey centralizes this code path so package behavior stays consistent.
func normalizeSettingKey(key string) string {
	return strings.TrimPrefix(key, "index.")
}

// normalizeIndexSection centralizes this code path so package behavior stays consistent.
func normalizeIndexSection(path, section string, variables templateVariables) string {
	if path == "" {
		return "{}"
	}

	content, err := readTemplatedFile(path, variables)
	if err != nil {
		fatal().Err(err).Str("path", path).Msgf("Reading %s file", section)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(content, &raw); err != nil {
		fatal().Err(err).Str("path", path).Msgf("Parsing %s file", section)
	}

	if nested, ok := raw[section]; ok {
		return string(nested)
	}

	return string(content)
}

// readNamedDefinitions centralizes this code path so package behavior stays consistent.
func readNamedDefinitions(path, resourceType string, variables templateVariables) (namedDefinitions, []string) {
	if path == "" {
		return nil, nil
	}

	content, err := readTemplatedFile(path, variables)
	if err != nil {
		fatal().Err(err).Str("path", path).Msgf("Reading %s definitions file", resourceType)
	}

	decoder := json.NewDecoder(strings.NewReader(string(content)))
	token, err := decoder.Token()
	if err != nil {
		fatal().Err(err).Str("path", path).Msgf("Parsing %s definitions file", resourceType)
	}

	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		fatal().Str("path", path).Msgf("%s definitions file must contain a JSON object", resourceType)
	}

	definitions := make(namedDefinitions)
	names := make([]string, 0)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			fatal().Err(err).Str("path", path).Msgf("Reading %s definition name", resourceType)
		}

		name, ok := keyToken.(string)
		if !ok {
			fatal().Str("path", path).Msgf("Invalid %s definition name", resourceType)
		}

		var definition json.RawMessage
		if err := decoder.Decode(&definition); err != nil {
			fatal().Err(err).Str("path", path).Msgf("Parsing %s definition body", resourceType)
		}

		definitions[name] = definition
		names = append(names, name)
	}

	if _, err := decoder.Token(); err != nil {
		fatal().Err(err).Str("path", path).Msgf("Parsing %s definitions file", resourceType)
	}

	return definitions, names
}

// buildManagedPolicyPlan centralizes this code path so package behavior stays consistent.
func buildManagedPolicyPlan(definitions namedDefinitions, logicalNames []string) managedPolicyPlan {
	plan := managedPolicyPlan{
		LogicalNames:     append([]string(nil), logicalNames...),
		DesiredNames:     make([]string, 0, len(logicalNames)),
		Definitions:      make(namedDefinitions, len(logicalNames)),
		LogicalToDesired: make(map[string]string, len(logicalNames)),
		DesiredSet:       make(map[string]struct{}, len(logicalNames)),
	}
	for _, logicalName := range logicalNames {
		definition, ok := definitions[logicalName]
		if !ok {
			fatal().Str("policy", logicalName).Msg("Policy definition missing from parsed policy file")
		}

		desiredName := managedPolicyName(logicalName, definition)
		plan.DesiredNames = append(plan.DesiredNames, desiredName)
		plan.Definitions[desiredName] = definition
		plan.LogicalToDesired[logicalName] = desiredName
		plan.DesiredSet[desiredName] = struct{}{}

		log.Debug().
			Str("logical_policy", logicalName).
			Str("policy", desiredName).
			Msg("Resolved managed policy name from policy definition hash")
	}
	return plan
}

// managedPolicyName centralizes this code path so package behavior stays consistent.
func managedPolicyName(logicalName string, definition json.RawMessage) string {
	canonical, err := canonicalizeRawJSON(definition)
	if err != nil {
		fatal().Err(err).Str("policy", logicalName).Msg("Failed to canonicalize policy definition for hashing")
	}

	sum := sha256.Sum256(canonical)
	suffix := hex.EncodeToString(sum[:])[:6]
	return logicalName + "-" + suffix
}

// canonicalizeRawJSON centralizes this code path so package behavior stays consistent.
func canonicalizeRawJSON(raw json.RawMessage) ([]byte, error) {
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	return json.Marshal(parsed)
}

// rewritePipelinePolicyReferences centralizes this code path so package behavior stays consistent.
func rewritePipelinePolicyReferences(definitions namedDefinitions, names []string, logicalToDesired map[string]string) namedDefinitions {
	if len(definitions) == 0 || len(logicalToDesired) == 0 {
		return definitions
	}

	rewritten := make(namedDefinitions, len(definitions))
	for _, name := range names {
		definition := definitions[name]
		var parsed any
		if err := json.Unmarshal(definition, &parsed); err != nil {
			fatal().Err(err).Str("pipeline", name).Msg("Unable to parse pipeline definition for policy rewrite")
		}

		replacements := rewriteEnrichPolicyNameReferences(parsed, logicalToDesired)
		encoded, err := json.Marshal(parsed)
		if err != nil {
			fatal().Err(err).Str("pipeline", name).Msg("Unable to serialize rewritten pipeline definition")
		}
		rewritten[name] = encoded

		if replacements > 0 {
			log.Info().
				Str("pipeline", name).
				Int("replacements", replacements).
				Msg("Rewrote enrich policy references in pipeline to managed policy names")
		} else {
			log.Trace().Str("pipeline", name).Msg("No enrich policy reference rewrites needed for pipeline")
		}
	}

	// Preserve any map entries not present in the ordered names list.
	for name, definition := range definitions {
		if _, ok := rewritten[name]; ok {
			continue
		}
		rewritten[name] = definition
	}
	return rewritten
}

// resolvePipelinePolicyFallbacks centralizes this code path so package behavior stays consistent.
func resolvePipelinePolicyFallbacks(es *elasticsearch.Client, definitions namedDefinitions, names []string, policyNameMapping map[string]string) {
	referenced := collectReferencedPolicyNamesFromPipelines(definitions, names)
	if len(referenced) == 0 {
		return
	}

	available, supported := getEnrichPolicies(es)
	if !supported {
		return
	}
	availableSet := make(map[string]struct{}, len(available))
	for _, name := range available {
		availableSet[name] = struct{}{}
	}

	for _, policy := range referenced {
		if _, mapped := policyNameMapping[policy]; mapped {
			continue
		}
		if _, exact := availableSet[policy]; exact {
			policyNameMapping[policy] = policy
			log.Debug().Str("policy", policy).Msg("Using existing exact enrich policy reference from pipeline definition")
			continue
		}

		matches := make([]string, 0)
		for _, candidate := range available {
			if managedPolicyNameMatchesLogical(policy, candidate) {
				matches = append(matches, candidate)
			}
		}
		if len(matches) == 0 {
			log.Warn().Str("logical_policy", policy).Msg("Pipeline references enrich policy that does not exist as exact or managed policy name")
			continue
		}

		slices.Sort(matches)
		chosen := matches[len(matches)-1]
		policyNameMapping[policy] = chosen
		if len(matches) > 1 {
			log.Warn().
				Str("logical_policy", policy).
				Str("policy", chosen).
				Strs("candidates", matches).
				Msg("Multiple managed enrich policy names matched logical policy reference; using the lexicographically latest match")
		} else {
			log.Info().Str("logical_policy", policy).Str("policy", chosen).Msg("Resolved logical enrich policy reference to managed policy name")
		}
	}
}

// rewriteEnrichPolicyNameReferences centralizes this code path so package behavior stays consistent.
func rewriteEnrichPolicyNameReferences(value any, logicalToDesired map[string]string) int {
	replacements := 0
	switch typed := value.(type) {
	case map[string]any:
		if enrich, ok := typed["enrich"].(map[string]any); ok {
			if policyName, ok := enrich["policy_name"].(string); ok {
				if desired, mapped := logicalToDesired[policyName]; mapped && desired != policyName {
					enrich["policy_name"] = desired
					replacements++
				}
			}
		}
		for _, nested := range typed {
			replacements += rewriteEnrichPolicyNameReferences(nested, logicalToDesired)
		}
	case []any:
		for _, nested := range typed {
			replacements += rewriteEnrichPolicyNameReferences(nested, logicalToDesired)
		}
	}
	return replacements
}

// collectReferencedPolicyNamesFromPipelines centralizes this code path so package behavior stays consistent.
func collectReferencedPolicyNamesFromPipelines(definitions namedDefinitions, names []string) []string {
	seen := make(map[string]struct{})
	for _, name := range names {
		var parsed any
		if err := json.Unmarshal(definitions[name], &parsed); err != nil {
			fatal().Err(err).Str("pipeline", name).Msg("Unable to parse pipeline definition while collecting policy references")
		}
		collectPolicyNamesInValue(parsed, seen)
	}

	referenced := make([]string, 0, len(seen))
	for policy := range seen {
		referenced = append(referenced, policy)
	}
	slices.Sort(referenced)
	return referenced
}

// collectPolicyNamesInValue centralizes this code path so package behavior stays consistent.
func collectPolicyNamesInValue(value any, seen map[string]struct{}) {
	switch typed := value.(type) {
	case map[string]any:
		if enrich, ok := typed["enrich"].(map[string]any); ok {
			if policyName, ok := enrich["policy_name"].(string); ok && policyName != "" {
				seen[policyName] = struct{}{}
			}
		}
		for _, nested := range typed {
			collectPolicyNamesInValue(nested, seen)
		}
	case []any:
		for _, nested := range typed {
			collectPolicyNamesInValue(nested, seen)
		}
	}
}

// resolveManagedPolicyDeleteNames centralizes this code path so package behavior stays consistent.
func resolveManagedPolicyDeleteNames(es *elasticsearch.Client, logicalNames []string) []string {
	if len(logicalNames) == 0 {
		return nil
	}

	available, supported := getEnrichPolicies(es)
	if !supported {
		return nil
	}

	deleteSet := make(map[string]struct{})
	for _, name := range available {
		for _, logical := range logicalNames {
			if name == logical || managedPolicyNameMatchesLogical(logical, name) {
				deleteSet[name] = struct{}{}
				break
			}
		}
	}

	deleteNames := make([]string, 0, len(deleteSet))
	for name := range deleteSet {
		deleteNames = append(deleteNames, name)
	}
	slices.Sort(deleteNames)
	return deleteNames
}

// managedPolicyNameMatchesLogical centralizes this code path so package behavior stays consistent.
func managedPolicyNameMatchesLogical(logicalName, policyName string) bool {
	prefix := logicalName + "-"
	if !strings.HasPrefix(policyName, prefix) {
		return false
	}

	suffix := strings.TrimPrefix(policyName, prefix)
	if len(suffix) != 6 {
		return false
	}
	for _, ch := range suffix {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

// remapEnrichSelection centralizes this code path so package behavior stays consistent.
func remapEnrichSelection(enrich *enrichFlagValue, logicalToDesired map[string]string) *enrichFlagValue {
	if enrich == nil || !enrich.enabled || enrich.all || len(logicalToDesired) == 0 {
		return enrich
	}

	explicit := enrich.explicitPolicies()
	if len(explicit) == 0 {
		return enrich
	}

	remapped := make([]string, 0, len(explicit))
	changed := false
	for _, policy := range explicit {
		if desired, ok := logicalToDesired[policy]; ok {
			remapped = append(remapped, desired)
			changed = true
			continue
		}
		remapped = append(remapped, policy)
	}
	if !changed {
		return enrich
	}

	cloned := *enrich
	cloned.raw = strings.Join(remapped, ",")
	cloned.all = false
	cloned.enabled = true
	log.Debug().Str("requested", enrich.raw).Str("resolved", cloned.raw).Msg("Mapped explicit enrich policy names to managed policy names")
	return &cloned
}

// resolveTransformsForSource centralizes this code path so package behavior stays consistent.
func resolveTransformsForSource(definitions namedDefinitions, names []string, sourceIndex string) (namedDefinitions, []string, error) {
	if len(names) == 0 {
		return nil, nil, nil
	}

	trimmedSourceIndex := strings.TrimSpace(sourceIndex)
	if trimmedSourceIndex == "" {
		return nil, nil, fmt.Errorf("source index is required")
	}

	selectedDefinitions := make(namedDefinitions)
	selectedNames := make([]string, 0)
	for _, name := range names {
		rawDefinition, ok := definitions[name]
		if !ok {
			return nil, nil, fmt.Errorf("transform %q definition missing from parsed transforms file", name)
		}

		var parsed transformDefinition
		if err := json.Unmarshal(rawDefinition, &parsed); err != nil {
			return nil, nil, fmt.Errorf("transform %q definition invalid: %w", name, err)
		}

		if strings.TrimSpace(parsed.SourceIndex) == "" {
			return nil, nil, fmt.Errorf("transform %q requires non-empty source_index", name)
		}
		trimmedBody := bytes.TrimSpace(parsed.Body)
		if len(trimmedBody) == 0 || bytes.Equal(trimmedBody, []byte("null")) {
			return nil, nil, fmt.Errorf("transform %q requires non-empty body", name)
		}

		if strings.TrimSpace(parsed.SourceIndex) != trimmedSourceIndex {
			log.Trace().
				Str("transform", name).
				Str("source_index", parsed.SourceIndex).
				Str("index", trimmedSourceIndex).
				Msg("Skipping transform that targets a different source index")
			continue
		}

		selectedDefinitions[name] = trimmedBody
		selectedNames = append(selectedNames, name)
	}

	return selectedDefinitions, selectedNames, nil
}

// garbageCollectManagedPolicies centralizes this code path so package behavior stays consistent.
func garbageCollectManagedPolicies(es *elasticsearch.Client, logicalNames []string, desiredSet map[string]struct{}) {
	if len(logicalNames) == 0 {
		return
	}

	available, supported := getEnrichPolicies(es)
	if !supported {
		return
	}

	for _, policy := range available {
		if _, keep := desiredSet[policy]; keep {
			continue
		}

		managed := false
		for _, logical := range logicalNames {
			if managedPolicyNameMatchesLogical(logical, policy) {
				managed = true
				break
			}
		}
		if !managed {
			continue
		}

		referencing := findPipelinesReferencingPolicy(es, policy)
		if len(referencing) > 0 {
			log.Debug().
				Str("policy", policy).
				Strs("pipelines", referencing).
				Msg("Skipping managed policy GC because policy is still referenced by pipelines")
			continue
		}

		deletePolicyBestEffort(es, policy)
	}
}

// deletePolicyBestEffort centralizes this code path so package behavior stays consistent.
func deletePolicyBestEffort(es *elasticsearch.Client, policy string) {
	res, err := es.EnrichDeletePolicy(
		policy,
		es.EnrichDeletePolicy.WithContext(context.Background()),
		es.EnrichDeletePolicy.WithHeader(map[string]string{
			"Accept": "application/json",
		}),
	)
	if err != nil {
		log.Warn().Err(err).Str("policy", policy).Msg("Managed policy GC failed while deleting policy")
		return
	}
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)
	if res.StatusCode == http.StatusNotFound {
		log.Trace().Str("policy", policy).Msg("Managed policy GC found policy already deleted")
		return
	}
	if res.IsError() {
		log.Warn().
			Str("policy", policy).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Managed policy GC could not delete policy")
		return
	}

	log.Info().Str("policy", policy).Msg("Managed policy GC deleted unreferenced policy")
}

// buildTemplateVariables centralizes this code path so package behavior stays consistent.
func buildTemplateVariables(index string, additional map[string]string) templateVariables {
	variables := templateVariables{
		"INDEX": index,
	}
	for key, value := range additional {
		trimmedKey := strings.TrimSpace(key)
		if trimmedKey == "" {
			continue
		}
		variables[trimmedKey] = value
	}
	return variables
}

// templateVariablePattern defines package-level state shared by related execution paths.
var templateVariablePattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)`)

// readTemplatedFile centralizes this code path so package behavior stays consistent.
func readTemplatedFile(path string, variables templateVariables) ([]byte, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	expanded := templateVariablePattern.ReplaceAllStringFunc(string(content), func(match string) string {
		name := strings.TrimPrefix(match, "$")
		name = strings.TrimPrefix(name, "{")
		name = strings.TrimSuffix(name, "}")

		if value, ok := variables[name]; ok {
			return value
		}
		if value, ok := os.LookupEnv(name); ok {
			return value
		}
		return match
	})

	return []byte(expanded), nil
}

// ─── Managed Resource Lifecycle ────────────────────────────────────────────────

// createPipelines centralizes this code path so package behavior stays consistent.
func createPipelines(es *elasticsearch.Client, definitions namedDefinitions, names []string) {
	for _, name := range names {
		res, err := es.Ingest.PutPipeline(
			name,
			strings.NewReader(string(definitions[name])),
			es.Ingest.PutPipeline.WithContext(context.Background()),
		)
		checkErr("creating pipeline", err)

		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			fatal().
				Str("pipeline", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to create pipeline")
		}
		res.Body.Close()

		log.Info().Str("pipeline", name).Msg("Pipeline created or updated")
	}
}

// deletePipelines centralizes this code path so package behavior stays consistent.
func deletePipelines(es *elasticsearch.Client, names []string) {
	for _, name := range names {
		res, err := es.Ingest.DeletePipeline(
			name,
			es.Ingest.DeletePipeline.WithContext(context.Background()),
		)
		checkErr("deleting pipeline", err)

		if res.StatusCode == http.StatusNotFound {
			res.Body.Close()
			log.Info().Str("pipeline", name).Msg("Pipeline does not exist. Nothing to delete.")
			continue
		}
		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			fatal().
				Str("pipeline", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to delete pipeline")
		}
		res.Body.Close()

		log.Info().Str("pipeline", name).Msg("Pipeline deleted")
	}
}

// createPolicies centralizes this code path so package behavior stays consistent.
func createPolicies(es *elasticsearch.Client, definitions namedDefinitions, names []string) {
	for _, name := range names {
		for attempt := 1; attempt <= 5; attempt++ {
			res, err := putPolicy(es, name, definitions[name])
			checkErr("creating enrich policy", err)

			if !res.IsError() {
				res.Body.Close()
				log.Info().Str("policy", name).Msg("Created enrich policy")
				break
			}

			body, _ := io.ReadAll(res.Body)
			res.Body.Close()

			if isUnsupportedEnrichAPI(res.StatusCode, body) {
				log.Warn().
					Int("status_code", res.StatusCode).
					Str("body", string(body)).
					Msg("Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools")
				return
			}
			if hasElasticsearchErrorType(body, "index_not_found_exception") && attempt < 5 {
				log.Warn().
					Str("policy", name).
					Int("attempt", attempt).
					Int("status_code", res.StatusCode).
					Str("body", string(body)).
					Msg("Source index for enrich policy is not visible yet; retrying enrich policy creation")
				time.Sleep(500 * time.Millisecond)
				continue
			}
			if hasElasticsearchErrorType(body, "resource_already_exists_exception") {
				log.Info().
					Str("policy", name).
					Msg("Managed enrich policy already exists for this definition hash")
				break
			}

			fatal().
				Str("policy", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to create enrich policy")
		}
	}
}

// putPolicy centralizes this code path so package behavior stays consistent.
func putPolicy(es *elasticsearch.Client, name string, definition json.RawMessage) (*esapi.Response, error) {
	return es.EnrichPutPolicy(
		name,
		strings.NewReader(string(definition)),
		es.EnrichPutPolicy.WithContext(context.Background()),
		es.EnrichPutPolicy.WithHeader(map[string]string{
			"Content-Type": "application/json",
			"Accept":       "application/json",
		}),
	)
}

// deletePolicies centralizes this code path so package behavior stays consistent.
func deletePolicies(es *elasticsearch.Client, names []string, nuke bool) {
	for _, name := range names {
		for attempt := 1; attempt <= 2; attempt++ {
			res, err := es.EnrichDeletePolicy(
				name,
				es.EnrichDeletePolicy.WithContext(context.Background()),
				es.EnrichDeletePolicy.WithHeader(map[string]string{
					"Accept": "application/json",
				}),
			)
			checkErr("deleting enrich policy", err)

			if res.StatusCode == http.StatusNotFound {
				body, _ := io.ReadAll(res.Body)
				res.Body.Close()
				if isUnsupportedEnrichAPI(res.StatusCode, body) {
					log.Warn().
						Int("status_code", res.StatusCode).
						Str("body", string(body)).
						Msg("Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools")
					return
				}
				log.Debug().Str("policy", name).Msg("Enrich policy does not exist. Nothing to delete.")
				break
			}
			if res.IsError() {
				body, _ := io.ReadAll(res.Body)
				res.Body.Close()
				if res.StatusCode == http.StatusConflict && nuke && policyDeleteBlockedByPipelineReference(body) && attempt == 1 {
					referencing := findPipelinesReferencingPolicy(es, name)
					if len(referencing) == 0 {
						fatal().
							Str("policy", name).
							Int("status_code", res.StatusCode).
							Str("body", string(body)).
							Msg("Failed to delete enrich policy; nuke mode could not find referencing pipelines")
					}

					log.Warn().
						Str("policy", name).
						Strs("pipelines", referencing).
						Msg("Nuke mode deleting pipelines that reference this enrich policy before retrying policy deletion")
					deletePipelinesForNuke(es, referencing)
					continue
				}

				fatal().
					Str("policy", name).
					Int("status_code", res.StatusCode).
					Str("body", string(body)).
					Msg("Failed to delete enrich policy")
			}
			res.Body.Close()

			log.Info().Str("policy", name).Msg("Deleted enrich policy")
			break
		}
	}
}

// deleteManagedResources centralizes this code path so package behavior stays consistent.
func deleteManagedResources(es *elasticsearch.Client, pipelineNames []string, policyNames []string, transformNames []string, nuke bool) {
	if len(pipelineNames) > 0 {
		deletePipelines(es, pipelineNames)
	}
	if len(policyNames) > 0 {
		deletePolicies(es, policyNames, nuke)
	}
	if len(transformNames) > 0 {
		deleteTransforms(es, transformNames)
	}
}

// createOrUpdateTransforms centralizes this code path so package behavior stays consistent.
func createOrUpdateTransforms(es *elasticsearch.Client, definitions namedDefinitions, names []string) {
	for _, name := range names {
		definition, ok := definitions[name]
		if !ok {
			fatal().Str("transform", name).Msg("Transform definition missing from parsed transform file")
		}

		// ES 9.x does not allow updating immutable transform properties
		// (pivot, latest) via TransformUpdateTransform. Delete and recreate
		// instead — stopTransformsBestEffort has already stopped the
		// transform, and deleteTransforms uses DeleteDestIndex: false to
		// preserve the destination index.
		if transformExists(es, name) {
			deleteTransforms(es, []string{name})
		}

		res, err := es.TransformPutTransform(
			strings.NewReader(string(definition)),
			name,
			es.TransformPutTransform.WithContext(context.Background()),
			es.TransformPutTransform.WithHeader(map[string]string{
				"Content-Type": "application/json",
				"Accept":       "application/json",
			}),
		)
		checkErr("creating transform", err)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.IsError() {
			fatal().
				Str("transform", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to create transform")
		}
		log.Info().Str("transform", name).Msg("Transform created")
	}
}

// transformExists centralizes this code path so package behavior stays consistent.
func transformExists(es *elasticsearch.Client, name string) bool {
	res, err := es.TransformGetTransform(
		es.TransformGetTransform.WithContext(context.Background()),
		es.TransformGetTransform.WithTransformID(name),
		es.TransformGetTransform.WithAllowNoMatch(true),
	)
	checkErr("checking if transform exists", err)
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return false
	}
	if res.IsError() {
		fatal().
			Str("transform", name).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to check transform existence")
	}
	var parsed struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(body, &parsed); err == nil {
		return parsed.Count > 0
	}
	return true
}

// stopTransformsBestEffort centralizes this code path so package behavior stays consistent.
func stopTransformsBestEffort(es *elasticsearch.Client, names []string) {
	for _, name := range names {
		res, err := es.TransformStopTransform(
			name,
			es.TransformStopTransform.WithContext(context.Background()),
			es.TransformStopTransform.WithForce(true),
			es.TransformStopTransform.WithWaitForCompletion(true),
			es.TransformStopTransform.WithTimeout(30*time.Second),
			es.TransformStopTransform.WithAllowNoMatch(true),
			es.TransformStopTransform.WithHeader(map[string]string{
				"Accept": "application/json",
			}),
		)
		checkErr("stopping transform", err)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusNotFound || transformStopAlreadyStopped(res.StatusCode, body) {
			log.Debug().Str("transform", name).Msg("Transform is not running. Nothing to stop.")
			continue
		}
		if res.IsError() {
			fatal().
				Str("transform", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to stop transform")
		}
		log.Debug().Str("transform", name).Msg("Transform stopped")
	}
}

// deleteTransforms centralizes this code path so package behavior stays consistent.
func deleteTransforms(es *elasticsearch.Client, names []string) {
	for _, name := range names {
		res, err := es.TransformDeleteTransform(
			name,
			es.TransformDeleteTransform.WithContext(context.Background()),
			es.TransformDeleteTransform.WithForce(true),
			es.TransformDeleteTransform.WithDeleteDestIndex(false),
			es.TransformDeleteTransform.WithHeader(map[string]string{
				"Accept": "application/json",
			}),
		)
		checkErr("deleting transform", err)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusNotFound {
			log.Debug().Str("transform", name).Msg("Transform does not exist. Nothing to delete.")
			continue
		}
		if res.IsError() {
			fatal().
				Str("transform", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to delete transform")
		}
		log.Info().Str("transform", name).Msg("Transform deleted")
	}
}

// startTransforms centralizes this code path so package behavior stays consistent.
func startTransforms(es *elasticsearch.Client, names []string) {
	for _, name := range names {
		res, err := es.TransformStartTransform(
			name,
			es.TransformStartTransform.WithContext(context.Background()),
			es.TransformStartTransform.WithTimeout(30*time.Second),
			es.TransformStartTransform.WithHeader(map[string]string{
				"Accept": "application/json",
			}),
		)
		checkErr("starting transform", err)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if transformStartAlreadyStarted(res.StatusCode, body) {
			log.Info().Str("transform", name).Msg("Transform already running")
			continue
		}
		if res.IsError() {
			fatal().
				Str("transform", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to start transform")
		}

		log.Info().Str("transform", name).Msg("Transform started")
	}
}

// deletePipelinesForNuke centralizes this code path so package behavior stays consistent.
func deletePipelinesForNuke(es *elasticsearch.Client, names []string) {
	for _, name := range names {
		res, err := es.Ingest.DeletePipeline(
			name,
			es.Ingest.DeletePipeline.WithContext(context.Background()),
		)
		checkErr("deleting pipeline", err)

		if res.StatusCode == http.StatusNotFound {
			res.Body.Close()
			log.Info().Str("pipeline", name).Msg("Pipeline does not exist. Nothing to delete.")
			continue
		}
		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode == http.StatusBadRequest && pipelineDeleteBlockedByDefaultIndex(body) {
				indices := findIndicesUsingDefaultPipeline(es, name)
				if len(indices) == 0 {
					fatal().
						Str("pipeline", name).
						Int("status_code", res.StatusCode).
						Str("body", string(body)).
						Msg("Failed to delete pipeline; nuke mode could not find indices using it as default pipeline")
				}

				log.Warn().
					Str("pipeline", name).
					Strs("indices", indices).
					Msg("Nuke mode clearing index.default_pipeline on indices that use this pipeline before retrying pipeline deletion")
				clearDefaultPipelineForIndices(es, indices)
				deletePipelines(es, []string{name})
				continue
			}
			fatal().
				Str("pipeline", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to delete pipeline")
		}
		res.Body.Close()

		log.Info().Str("pipeline", name).Msg("Pipeline deleted")
	}
}

// findPipelinesReferencingPolicy centralizes this code path so package behavior stays consistent.
func findPipelinesReferencingPolicy(es *elasticsearch.Client, policy string) []string {
	res, err := es.Ingest.GetPipeline(
		es.Ingest.GetPipeline.WithContext(context.Background()),
	)
	checkErr("getting ingest pipelines", err)
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		fatal().
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to get ingest pipelines")
	}

	var definitions namedDefinitions
	if err := json.NewDecoder(res.Body).Decode(&definitions); err != nil {
		fatal().Err(err).Msg("Unable to parse ingest pipeline response")
	}

	return pipelineNamesReferencingPolicy(definitions, policy)
}

// findIndicesUsingDefaultPipeline centralizes this code path so package behavior stays consistent.
func findIndicesUsingDefaultPipeline(es *elasticsearch.Client, pipeline string) []string {
	res, err := es.Indices.GetSettings(es.Indices.GetSettings.WithName("*"))
	checkErr("getting index settings", err)
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		fatal().
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to get index settings")
	}

	var parsed map[string]struct {
		Settings struct {
			Index struct {
				DefaultPipeline string `json:"default_pipeline"`
			} `json:"index"`
		} `json:"settings"`
	}
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		fatal().Err(err).Msg("Unable to parse index settings response")
	}

	indices := make([]string, 0)
	for index, settings := range parsed {
		if settings.Settings.Index.DefaultPipeline == pipeline {
			indices = append(indices, index)
		}
	}
	slices.Sort(indices)
	return indices
}

// clearDefaultPipelineForIndices centralizes this code path so package behavior stays consistent.
func clearDefaultPipelineForIndices(es *elasticsearch.Client, indices []string) {
	for _, index := range indices {
		res, err := es.Indices.PutSettings(
			strings.NewReader(`{"index.default_pipeline":null}`),
			es.Indices.PutSettings.WithIndex(index),
		)
		checkErr("clearing index.default_pipeline", err)

		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			fatal().
				Str("index", index).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to clear index.default_pipeline")
		}
		res.Body.Close()

		log.Warn().Str("index", index).Msg("Cleared index.default_pipeline to allow nuke cleanup")
	}
}

// pipelineNamesReferencingPolicy centralizes this code path so package behavior stays consistent.
func pipelineNamesReferencingPolicy(definitions namedDefinitions, policy string) []string {
	names := make([]string, 0)
	for name, definition := range definitions {
		if pipelineDefinitionReferencesPolicy(definition, policy) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// pipelineDefinitionReferencesPolicy centralizes this code path so package behavior stays consistent.
func pipelineDefinitionReferencesPolicy(definition json.RawMessage, policy string) bool {
	var parsed any
	if err := json.Unmarshal(definition, &parsed); err != nil {
		return false
	}
	return valueReferencesPolicy(parsed, policy)
}

// valueReferencesPolicy centralizes this code path so package behavior stays consistent.
func valueReferencesPolicy(value any, policy string) bool {
	switch typed := value.(type) {
	case map[string]any:
		if enrich, ok := typed["enrich"].(map[string]any); ok {
			if policyName, ok := enrich["policy_name"].(string); ok && policyName == policy {
				return true
			}
		}
		for _, nested := range typed {
			if valueReferencesPolicy(nested, policy) {
				return true
			}
		}
	case []any:
		for _, nested := range typed {
			if valueReferencesPolicy(nested, policy) {
				return true
			}
		}
	}
	return false
}

// ─── Enrich Execution ──────────────────────────────────────────────────────────

// refreshIndex centralizes this code path so package behavior stays consistent.
func refreshIndex(es *elasticsearch.Client, index string) {
	res, err := es.Indices.Refresh(es.Indices.Refresh.WithIndex(index))
	checkErr("refreshing index before enrich execution", err)
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		fatal().
			Str("index", index).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to refresh index before enrich execution")
	}

	log.Info().Str("index", index).Msg("Refreshed index before executing enrich policy")
}

// runEnrichPolicies centralizes this code path so package behavior stays consistent.
func runEnrichPolicies(es *elasticsearch.Client, enrich *enrichFlagValue, declared []string) enrichRunSummary {
	summary := enrichRunSummary{}

	availablePolicies, supported := getEnrichPolicies(es)
	if !supported {
		return summary
	}
	if len(availablePolicies) == 0 {
		log.Warn().Msg("No enrich policies found; skipping enrich execution")
		return summary
	}

	targets, missing := resolveEnrichTargets(enrich, availablePolicies, declared)
	summary.Selected = append(summary.Selected, targets...)
	summary.Missing = append(summary.Missing, missing...)
	for _, policy := range missing {
		log.Warn().Str("policy", policy).Msg("Enrich policy not found; skipping")
	}

	if len(targets) == 0 {
		log.Warn().Msg("No enrich policies matched the request")
		return summary
	}

	log.Info().
		Int("available", len(availablePolicies)).
		Int("requested", len(targets)+len(missing)).
		Int("selected", len(targets)).
		Msg("Starting enrich policy execution")

	succeeded := 0
	failed := 0
	for _, policy := range targets {
		if executeEnrichPolicy(es, policy) {
			succeeded++
		} else {
			failed++
		}
	}

	event := log.Info()
	if failed > 0 {
		event = log.Error()
	}
	event.
		Int("selected", len(targets)).
		Int("succeeded", succeeded).
		Int("failed", failed).
		Int("missing", len(missing)).
		Msg("Completed enrich policy execution")

	summary.Succeeded = succeeded
	summary.Failed = failed
	return summary
}

// getEnrichPolicies centralizes this code path so package behavior stays consistent.
func getEnrichPolicies(es *elasticsearch.Client) ([]string, bool) {
	res, err := es.EnrichGetPolicy(
		es.EnrichGetPolicy.WithContext(context.Background()),
		es.EnrichGetPolicy.WithHeader(map[string]string{
			"Accept": "application/json",
		}),
	)
	checkErr("getting enrich policies", err)
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		if isUnsupportedEnrichAPI(res.StatusCode, body) {
			log.Warn().
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools")
			return nil, false
		}
		fatal().
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to get enrich policies")
	}

	var parsed enrichPoliciesResponse
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		fatal().Err(err).Msg("Unable to parse enrich policy response")
	}

	policies := make([]string, 0, len(parsed.Policies))
	for _, policy := range parsed.Policies {
		for _, config := range policy.Config {
			if config.Name == "" {
				continue
			}
			policies = append(policies, config.Name)
		}
	}
	slices.Sort(policies)
	return policies, true
}

// resolveEnrichTargets centralizes this code path so package behavior stays consistent.
func resolveEnrichTargets(enrich *enrichFlagValue, available []string, declared []string) ([]string, []string) {
	if enrich == nil || !enrich.enabled {
		return nil, nil
	}

	availableSet := make(map[string]struct{}, len(available))
	for _, policy := range available {
		availableSet[policy] = struct{}{}
	}

	if enrich.all {
		if len(declared) > 0 {
			targets := make([]string, 0, len(declared))
			missing := make([]string, 0)
			for _, policy := range declared {
				if _, ok := availableSet[policy]; ok {
					targets = append(targets, policy)
					continue
				}
				missing = append(missing, policy)
			}
			return targets, missing
		}

		targets := append([]string(nil), available...)
		slices.Sort(targets)
		return targets, nil
	}

	targets := make([]string, 0)
	missing := make([]string, 0)
	for _, policy := range enrich.explicitPolicies() {
		if _, ok := availableSet[policy]; ok {
			targets = append(targets, policy)
			continue
		}
		missing = append(missing, policy)
	}
	return targets, missing
}

// logEnrichResult emits the final success/failure log for an enrich policy execution.
func logEnrichResult(policy string, status *enrichPhaseStatus, startTime time.Time) bool {
	phase, isFailure := "", false
	if status != nil {
		phase = status.Phase
		isFailure = strings.EqualFold(phase, "FAILED") || strings.EqualFold(phase, "CANCELLED")
	}
	event := log.Debug()
	if isFailure {
		event = log.Error()
	}
	entry := event.Str("policy", policy).Float64("time_taken", time.Since(startTime).Seconds())
	if phase != "" {
		entry = entry.Str("phase", phase)
	}
	msg := "Enrich policy execution succeeded"
	if isFailure {
		msg = "Enrich policy execution failed"
	}
	entry.Msg(msg)
	return !isFailure
}

// executeEnrichPolicy centralizes this code path so package behavior stays consistent.
func executeEnrichPolicy(es *elasticsearch.Client, policy string) bool {
	startTime := time.Now()
	res, err := es.EnrichExecutePolicy(
		policy,
		es.EnrichExecutePolicy.WithContext(context.Background()),
		es.EnrichExecutePolicy.WithWaitForCompletion(false),
		es.EnrichExecutePolicy.WithHeader(map[string]string{
			"Accept": "application/json",
		}),
	)
	if err != nil {
		log.Error().Err(err).Str("policy", policy).Msg("Failed to execute enrich policy")
		return false
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		if isUnsupportedEnrichAPI(res.StatusCode, body) {
			log.Warn().
				Str("policy", policy).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Enrich execute endpoint returned a generic 404; check proxy or routing for /_enrich/policy/<name>/_execute and confirm this URL matches the backend used by Dev Tools")
			return false
		}
		log.Error().
			Str("policy", policy).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Float64("time_taken", time.Since(startTime).Seconds()).
			Msg("Enrich policy execution failed")
		return false
	}

	var kickoff enrichExecuteResponse
	if err := json.NewDecoder(res.Body).Decode(&kickoff); err != nil {
		log.Error().Err(err).Str("policy", policy).Msg("Unable to parse enrich policy execution response")
		return false
	}

	// Synchronous completion — small policy or ES returned immediately.
	if kickoff.Task == nil || *kickoff.Task == "" {
		return logEnrichResult(policy, kickoff.Status, startTime)
	}

	taskID := *kickoff.Task
	pollInterval := 2 * time.Second
	for pollCount := 1; ; pollCount++ {
		time.Sleep(pollInterval)

		taskRes, err := es.Tasks.Get(taskID)
		if err != nil {
			log.Error().Err(err).Str("policy", policy).Str("task", taskID).Msg("Failed to poll enrich policy task")
			return false
		}

		// 404 means ES has already removed the completed task record.
		if taskRes.StatusCode == http.StatusNotFound {
			taskRes.Body.Close()
			return logEnrichResult(policy, nil, startTime)
		}

		var task taskGetResponse
		decodeErr := json.NewDecoder(taskRes.Body).Decode(&task)
		taskRes.Body.Close()
		if decodeErr != nil {
			log.Error().Err(decodeErr).Str("policy", policy).Str("task", taskID).Msg("Unable to parse enrich policy task response")
			return false
		}

		phase := ""
		if task.Task.Status != nil {
			phase = task.Task.Status.Phase
		}

		// completed=true or a response object present both signal the task is done.
		if task.Completed || task.Response != nil {
			status := task.Task.Status
			if task.Response != nil && task.Response.Status != nil {
				status = task.Response.Status
			}
			return logEnrichResult(policy, status, startTime)
		}

		log.Debug().
			Str("policy", policy).
			Str("phase", phase).
			Float64("elapsed_s", time.Since(startTime).Seconds()).
			Msg("Enrich policy execution in progress")

		if pollCount == 5 {
			pollInterval = 5 * time.Second
		}
	}
}

// isUnsupportedEnrichAPI centralizes this code path so package behavior stays consistent.
func isUnsupportedEnrichAPI(statusCode int, body []byte) bool {
	if statusCode != http.StatusNotFound {
		return false
	}

	if bytes.Contains(body, []byte(`"error":{`)) {
		return false
	}

	return !bytes.Contains(body, []byte("resource_not_found_exception"))
}

// hasElasticsearchErrorType centralizes this code path so package behavior stays consistent.
func hasElasticsearchErrorType(body []byte, errorType string) bool {
	return bytes.Contains(body, []byte(`"type":"`+errorType+`"`))
}

// policyDeleteBlockedByPipelineReference centralizes this code path so package behavior stays consistent.
func policyDeleteBlockedByPipelineReference(body []byte) bool {
	return bytes.Contains(body, []byte("pipeline is referencing it"))
}

// pipelineDeleteBlockedByDefaultIndex centralizes this code path so package behavior stays consistent.
func pipelineDeleteBlockedByDefaultIndex(body []byte) bool {
	return bytes.Contains(body, []byte("cannot be deleted because it is the default pipeline"))
}

// transformStopAlreadyStopped centralizes this code path so package behavior stays consistent.
func transformStopAlreadyStopped(statusCode int, body []byte) bool {
	if statusCode != http.StatusConflict && statusCode != http.StatusBadRequest {
		return false
	}
	lowered := strings.ToLower(string(body))
	return strings.Contains(lowered, "cannot stop transform") && strings.Contains(lowered, "stopped")
}

// transformStartAlreadyStarted centralizes this code path so package behavior stays consistent.
func transformStartAlreadyStarted(statusCode int, body []byte) bool {
	if statusCode != http.StatusConflict && statusCode != http.StatusBadRequest {
		return false
	}
	lowered := strings.ToLower(string(body))
	if strings.Contains(lowered, "resource_already_exists_exception") {
		return true
	}
	return strings.Contains(lowered, "already started") || (strings.Contains(lowered, "cannot start transform") && strings.Contains(lowered, "started"))
}

// ─── Bulk Insert ───────────────────────────────────────────────────────────────

// bulkInsert handles a batch of documents and validates per-item bulk response status.
func bulkInsert(
	ctx context.Context,
	es *elasticsearch.Client,
	index string,
	batch []map[string]interface{},
	inserted, total int,
	retryAttempts int,
	retryBackoffBase, retryBackoffMax time.Duration,
	idField string,
) bulkInsertResult {
	if ctx == nil {
		ctx = context.Background()
	}
	var buf strings.Builder
	for _, doc := range batch {
		meta := map[string]map[string]string{"index": {"_index": index}}

		if idField != "" {
			if v, ok := doc[idField]; ok {
				if idStr, ok := v.(string); ok && idStr != "" {
					meta["index"]["_id"] = idStr
				}
			}
		}

		metaLine, _ := json.Marshal(meta)
		docLine, _ := json.Marshal(doc)
		buf.Write(metaLine)
		buf.WriteByte('\n')
		buf.Write(docLine)
		buf.WriteByte('\n')
	}
	payload := buf.String()
	if retryAttempts <= 0 {
		retryAttempts = defaultBulkRetryAttempts
	}
	if retryBackoffBase <= 0 {
		retryBackoffBase = defaultBulkRetryBackoffBase
	}
	if retryBackoffMax <= 0 {
		retryBackoffMax = defaultBulkRetryBackoffMax
	}
	if retryBackoffMax < retryBackoffBase {
		retryBackoffMax = retryBackoffBase
	}

	var (
		res      *esapi.Response
		err      error
		duration time.Duration
	)
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		startTime := time.Now()
		res, err = es.Bulk(strings.NewReader(payload), es.Bulk.WithContext(ctx))
		duration = time.Since(startTime)

		if err != nil {
			if ctx.Err() != nil {
				fatal().Err(ctx.Err()).Msg("Bulk API request failed")
			}
			if shouldRetryBulkRequest(0, err) && attempt < retryAttempts {
				nextBackoff := computeExponentialBackoff(attempt, retryBackoffBase, retryBackoffMax)
				log.Warn().
					Err(err).
					Int("attempt", attempt).
					Int("max_attempts", retryAttempts).
					Str("next_backoff", nextBackoff.String()).
					Msg("Bulk API request failed; retrying")
				if sleepErr := sleepWithContext(ctx, nextBackoff); sleepErr != nil {
					fatal().Err(sleepErr).Msg("Bulk API request failed")
				}
				continue
			}
			fatal().Err(err).Msg("Bulk API request failed")
		}

		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			_ = res.Body.Close()
			if shouldRetryBulkRequest(res.StatusCode, nil) && attempt < retryAttempts {
				nextBackoff := computeExponentialBackoff(attempt, retryBackoffBase, retryBackoffMax)
				log.Warn().
					Int("status_code", res.StatusCode).
					Str("body", string(body)).
					Int("attempt", attempt).
					Int("max_attempts", retryAttempts).
					Str("next_backoff", nextBackoff.String()).
					Msg("Bulk API request failed; retrying")
				if sleepErr := sleepWithContext(ctx, nextBackoff); sleepErr != nil {
					fatal().Err(sleepErr).Msg("Bulk API request failed")
				}
				continue
			}
			fatal().
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Bulk API request failed")
		}

		if attempt > 1 {
			log.Info().
				Int("attempt", attempt).
				Int("max_attempts", retryAttempts).
				Float64("time_taken", duration.Seconds()).
				Msg("Bulk API request retry succeeded")
		}
		break
	}
	defer res.Body.Close()

	var parsed bulkResponse
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		fatal().Err(err).Msg("Unable to parse bulk response body")
	}

	failed := 0
	logged := 0
	for itemIdx, item := range parsed.Items {
		for action, result := range item {
			if result.Status >= 300 || result.Error != nil {
				failed++
				if logged < 10 {
					errorType := ""
					errorReason := ""
					if result.Error != nil {
						errorType = result.Error.Type
						errorReason = result.Error.Reason
					}
					log.Error().
						Int("item", itemIdx).
						Str("action", action).
						Str("_index", result.Index).
						Str("_id", result.ID).
						Int("status", result.Status).
						Str("error_type", errorType).
						Str("error_reason", errorReason).
						Msg("Bulk item failed")
					logged++
				}
			}
		}
	}

	if failed > 0 && failed > logged {
		log.Error().
			Int("failed_items", failed).
			Int("logged_failures", logged).
			Msg("Additional bulk item failures omitted from logs")
	}

	succeeded := len(batch) - failed
	log.Debug().
		Int("inserted", inserted).
		Int("total", total).
		Int("batch_size", len(batch)).
		Int("succeeded", succeeded).
		Int("failed", failed).
		Float64("time_taken", duration.Seconds()).
		Msg("Processed batch")

	// TODO: Persist non-retryable item failures to a dead-letter file for later replay.
	return bulkInsertResult{Succeeded: succeeded, Failed: failed}
}

// shouldRetryBulkRequest centralizes retryability checks for bulk request failures.
func shouldRetryBulkRequest(statusCode int, err error) bool {
	if isRetryableBulkStatus(statusCode) {
		return true
	}
	return isRetryableTransportError(err)
}

// isRetryableBulkStatus centralizes retryable bulk HTTP status classification.
func isRetryableBulkStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// isRetryableTransportError centralizes retryable transport error classification.
func isRetryableTransportError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}
	return true
}

// computeExponentialBackoff centralizes capped exponential retry delays.
func computeExponentialBackoff(attempt int, base, max time.Duration) time.Duration {
	if attempt <= 0 || base <= 0 {
		return 0
	}
	backoff := base
	if attempt > 1 {
		backoff = base << (attempt - 1)
	}
	if max > 0 && backoff > max {
		return max
	}
	return backoff
}

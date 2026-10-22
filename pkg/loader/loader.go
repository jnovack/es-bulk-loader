package loader

import (
	"bufio"
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
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
	"github.com/rs/zerolog"
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

// EnrichOptions selects enrich policies for execution. When Policies is
// non-empty, it enables enrich and takes precedence over All and Raw.
// Otherwise Enabled must be true; All, or an empty Raw value, selects all
// policies, while a non-empty Raw value is parsed as a comma-separated list.
type EnrichOptions struct {
	// Enabled enables selection when Policies is empty.
	Enabled bool
	// All selects every policy when Enabled is true and Policies is empty.
	All bool
	// Raw is a comma-separated policy list used when Enabled is true and
	// Policies is empty.
	Raw string
	// Policies is an explicit policy list. A non-empty list enables enrich
	// and takes precedence over All and Raw.
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
	// Replicas, when non-nil, sets number_of_replicas on indices this run
	// creates, overriding the cluster-topology default (ADR 0001). nil keeps
	// the topology rule; an explicit 0 is honoured. Must be >= 0.
	Replicas *int
	Nuke     bool
	IDField  string
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
	// Transforms controls waiting on managed transforms after they start.
	Transforms TransformOptions
	// Progress, when non-nil, receives ProgressEvents; see ProgressEvent for
	// the delivery contract. nil creates no events.
	Progress func(ProgressEvent)
	// Logger, when non-nil, receives all of Run's log output — including a
	// Disabled logger such as zerolog.Nop(), which silences Run. nil uses the
	// zerolog global (log.Logger), read but never modified, so concurrent
	// Runs with different loggers do not interfere.
	Logger *zerolog.Logger
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
	// indexVisibilityMaxAttempts bounds index-creation visibility checks.
	indexVisibilityMaxAttempts = 20
	// indexVisibilityPollInterval controls the delay between visibility checks.
	indexVisibilityPollInterval = 250 * time.Millisecond
)

// timestampedIndexCollisionLimit bounds existence checks while searching
// consecutive one-second timestamp suffixes.
const timestampedIndexCollisionLimit = 300

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

// enrichPhaseStatus holds the phase and, while the policy runs, the step
// fields returned by enrich policy execution.
type enrichPhaseStatus struct {
	Phase string `json:"phase"`
	// Step is the request class the enrich runner is executing (for example
	// "ReindexRequest"); ES omits it when the policy is not running.
	Step string `json:"step,omitempty"`
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
	// Error is the task's failure object; ES sets it next to completed:true
	// when the task ended with an exception.
	Error json.RawMessage `json:"error,omitempty"`
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

// progressNow is the clock for progress timing (elapsed values, heartbeats).
// It exists so tests can drive heartbeats without sleeping.
var progressNow = time.Now

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
func fatal(ctx context.Context) *fatalEvent {
	return &fatalEvent{event: loggerFrom(ctx).WithLevel(zerolog.FatalLevel)}
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

// ─── Primary Execution Entry Points ────────────────────────────────────────────

// Run executes the loader lifecycle for one index; see the package
// documentation for stage ordering. opts is received by value and zero-valued
// fields are normalized on Run's private copy, so the caller's Options —
// including the TemplateVariables map and Enrich.Policies slice — is never
// modified. TestRunDoesNotMutateCallerOptions guards that contract.
func Run(ctx context.Context, opts Options) (result Result, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Never write the package-global logger here: swapping it and restoring on
	// return was a data race between concurrent Runs, which forced consumers to
	// serialize Runs behind a mutex. The logger travels on ctx instead.
	logger := resolveRunLogger(opts.Logger)
	ctx = withRunLogger(ctx, &logger)

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
	if opts.Transforms.WaitTimeout <= 0 {
		opts.Transforms.WaitTimeout = DefaultTransformWaitTimeout
	}
	emit := progressEmitter(opts.Progress)

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
		loggerFrom(ctx).Warn().Msg(message)
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
	if opts.Replicas != nil && *opts.Replicas < 0 {
		return result, &RunError{Kind: ErrInvalidOptions, Op: "validating replicas", Err: fmt.Errorf("replicas must be 0 or greater")}
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
	checkErr(ctx, "creating Elasticsearch client", err)

	variables := buildTemplateVariables(*index, *templateVariables)

	// Validate every supplied template file before any create, delete, flush or
	// put. Settings are otherwise templated only at index-create time, after
	// -delete has already removed the index (mappings are templated at create
	// time too, apart from the earlier preflight-plan read), so a typo there
	// would leave the cluster half-reset. Fail closed here instead.
	if err := preflightTemplateFiles(variables, *settingsFile, *mappingsFile, *pipelinesFile, *policiesFile, *transformsFile); err != nil {
		return result, err
	}
	pipelineDefinitions, pipelineNames := readNamedDefinitions(ctx, *pipelinesFile, "pipeline", variables)
	logicalPolicyDefinitions, logicalPolicyNames := readNamedDefinitions(ctx, *policiesFile, "policy", variables)
	logicalTransformDefinitions, logicalTransformNames := readNamedDefinitions(ctx, *transformsFile, "transform", variables)
	policyPlan := buildManagedPolicyPlan(ctx, logicalPolicyDefinitions, logicalPolicyNames)
	policyNameMapping := make(map[string]string, len(policyPlan.LogicalToDesired))
	for logical, desired := range policyPlan.LogicalToDesired {
		policyNameMapping[logical] = desired
	}
	resolvePipelinePolicyFallbacks(ctx, es, pipelineDefinitions, pipelineNames, policyNameMapping)
	pipelineDefinitions = rewritePipelinePolicyReferences(ctx, pipelineDefinitions, pipelineNames, policyNameMapping)
	policyDefinitions := policyPlan.Definitions
	policyNames := policyPlan.DesiredNames
	policyDeleteNames := resolveManagedPolicyDeleteNames(ctx, es, policyPlan.LogicalNames)
	transformDefinitions, transformNames, transformErr := resolveTransformsForSource(ctx, logicalTransformDefinitions, logicalTransformNames, *index)
	if transformErr != nil {
		fatal(ctx).Err(transformErr).Msg("Failed to resolve transform definitions")
	}
	logEffectiveManagedAssets(ctx,
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
	enrichSelection := remapEnrichSelection(ctx, enrich, policyNameMapping)
	defaultPipeline := ""
	if effectiveSyncManaged && len(pipelineNames) > 0 {
		defaultPipeline = pipelineNames[0]
	}

	// Parse the mappings file before the existence check, -nuke and the data
	// action, so a malformed file fails the run before anything is deleted.
	preflightPlan := mappingPreflightPlan{}
	if action.requiresDataFile() {
		plan, planErr := buildMappingPreflightPlan(ctx, *mappingsFile, variables)
		if planErr != nil {
			fatal(ctx).Err(planErr).Str("path", *mappingsFile).Msg("Failed to build mapping preflight plan")
		}
		preflightPlan = plan
	}

	aliasTargets := []string(nil)
	exists := false
	if *aliasMode {
		aliasTargets = resolveAliasTargets(ctx, es, *index)
		if len(aliasTargets) == 0 {
			indexPresent, err := indexExists(ctx, es, *index)
			checkErr(ctx, "checking if index exists", err)
			if indexPresent {
				aliasTargets = []string{*index}
				warn(fmt.Sprintf("Found concrete index with alias name %q; treating it as the current write target", *index))
			}
		}
		exists = len(aliasTargets) > 0
		if exists {
			loggerFrom(ctx).Info().Str("alias", *index).Strs("indices", aliasTargets).Msg("Resolved alias targets")
		} else {
			loggerFrom(ctx).Info().Str("alias", *index).Msg("Alias has no current indices")
		}
	} else {
		var err error
		exists, err = indexExists(ctx, es, *index)
		checkErr(ctx, "checking if index exists", err)
	}

	if *nuke {
		if *aliasMode {
			if len(aliasTargets) > 0 {
				loggerFrom(ctx).Warn().Str("alias", *index).Strs("indices", aliasTargets).Msg("Nuke deleting alias target indices and declared managed resources")
				deleteIndices(ctx, es, aliasTargets)
				aliasTargets = nil
			} else {
				warn(fmt.Sprintf("Alias %q does not currently resolve to an index. Nuke will still remove declared managed resources", *index))
			}
			generations := listTimestampedIndices(ctx, es, *index)
			if len(generations) > 0 {
				names := make([]string, 0, len(generations))
				for _, generation := range generations {
					names = append(names, generation.Name)
				}
				loggerFrom(ctx).Warn().Str("alias", *index).Strs("indices", names).Msg("Nuke deleting timestamped indices that match alias pattern")
				deleteIndices(ctx, es, names)
			}
			exists = false
		} else {
			if exists {
				loggerFrom(ctx).Warn().Str("index", *index).Msg("Nuke deleting index and declared managed resources")
				deleteAndCheck(ctx, es, *index)
				exists = false
			} else {
				warn(fmt.Sprintf("Index %q does not exist. Nuke will still remove declared managed resources", *index))
			}
		}

		deleteManagedResources(ctx, es, pipelineNames, policyDeleteNames, transformNames, true)
	}

	// An existing index must already match the mappings file before -flush
	// deletes its documents or -sync-managed upserts pipelines and policies
	// below. -flush and -add never write mappings, so this run cannot repair
	// a mismatch. Checking after the flush left an emptied index behind a
	// failed run. Read `exists` here: the switch below rewrites it.
	preflightExistingIndex := exists && (action == dataActionFlush || action == dataActionAdd)
	if preflightExistingIndex {
		requireMappingPreflight(ctx, es, *index, preflightPlan)
	}

	switch action {
	case dataActionDelete:
		if *aliasMode {
			if len(aliasTargets) > 0 {
				loggerFrom(ctx).Info().Str("alias", *index).Strs("indices", aliasTargets).Msg("Alias mode delete will roll forward to a new timestamped index")
			} else {
				warn(fmt.Sprintf("Alias %q has no indices. Nothing to delete.", *index))
			}
			exists = false
		} else {
			if exists {
				loggerFrom(ctx).Info().Str("index", *index).Msg("Deleting index before reloading data")
				deleteAndCheck(ctx, es, *index)
				exists = false
			} else {
				warn(fmt.Sprintf("Index %q does not exist. Nothing to delete.", *index))
			}
		}

		if *aliasMode {
			loggerFrom(ctx).Info().Str("alias", *index).Msg("Alias mode delete keeps existing managed resources (pipelines, policies, transforms); use -nuke for destructive managed-resource cleanup")
		} else {
			deleteManagedResources(ctx, es, pipelineNames, policyDeleteNames, transformNames, false)
		}
	case dataActionFlush:
		if *aliasMode {
			if len(aliasTargets) > 0 {
				loggerFrom(ctx).Info().Str("alias", *index).Strs("indices", aliasTargets).Msg("Flushing alias target indices before loading replacement data")
				for _, target := range aliasTargets {
					flushAndCheck(ctx, es, target)
				}
				exists = true
			} else {
				warn(fmt.Sprintf("Alias %q has no indices. Nothing to flush.", *index))
				exists = false
			}
		} else {
			if exists {
				loggerFrom(ctx).Info().Str("index", *index).Msg("Flushing existing index before loading replacement data")
				flushAndCheck(ctx, es, *index)
			} else {
				warn(fmt.Sprintf("Index %q does not exist. Nothing to flush.", *index))
			}
		}
	case dataActionAdd:
		if *aliasMode {
			if len(aliasTargets) > 0 {
				loggerFrom(ctx).Info().Str("alias", *index).Strs("indices", aliasTargets).Msg("Appending documents to existing alias target index")
				exists = true
			} else {
				loggerFrom(ctx).Info().Str("alias", *index).Msg("Creating first timestamped index for alias before appending documents")
				exists = false
			}
		} else {
			if exists {
				loggerFrom(ctx).Info().Str("index", *index).Msg("Appending documents to existing index")
			} else {
				loggerFrom(ctx).Info().Str("index", *index).Msg("Creating index to append documents")
			}
		}
	}

	shouldCreateIndex := !exists && (action.requiresDataFile() || (effectiveSyncManaged && (*settingsFile != "" || *mappingsFile != "")))
	writeIndex := *index
	createdIndex := ""
	if *aliasMode && shouldCreateIndex {
		createdIndex = nextAvailableTimestampedIndexName(ctx, es, *index, time.Now().UTC())
		writeIndex = createdIndex
		loggerFrom(ctx).Info().Str("alias", *index).Str("index", createdIndex).Msg("Preparing timestamped index for alias")
	}
	result.WriteIndex = writeIndex
	result.CreatedIndex = createdIndex
	if *aliasMode {
		result.ResolvedAliasTarget = *index
	}

	if shouldCreateIndex && effectiveSyncManaged {
		createPipelines(ctx, es, pipelineDefinitions, pipelineNames)
	}

	if shouldCreateIndex {
		body := buildCreateIndexBody(ctx, es, opts.Replicas, *settingsFile, *mappingsFile, defaultPipeline, variables)
		createIndex := *index
		if *aliasMode {
			createIndex = createdIndex
		}
		res, err := es.Indices.Create(createIndex, es.Indices.Create.WithBody(strings.NewReader(body)))
		checkErr(ctx, "creating index", err)
		defer res.Body.Close()
		if res.IsError() {
			responseBody, _ := io.ReadAll(res.Body)
			fatal(ctx).
				Str("index", createIndex).
				Int("status_code", res.StatusCode).
				Str("body", string(responseBody)).
				Msg("Failed to create index")
		}
		waitForIndex(ctx, es, createIndex)
		exists = true
		if *aliasMode {
			loggerFrom(ctx).Info().Str("alias", *index).Str("index", createIndex).Msg("Index created for alias")
		} else {
			loggerFrom(ctx).Info().Str("index", *index).Msg("Index created")
		}
	}

	deferPolicyCreationUntilAliasSwap := effectiveSyncManaged && *aliasMode && shouldCreateIndex
	transformSyncNames := make([]string, 0)
	if effectiveSyncManaged && exists {
		if !shouldCreateIndex {
			createPipelines(ctx, es, pipelineDefinitions, pipelineNames)
		}
		if len(transformNames) > 0 {
			transformSyncNames = append(transformSyncNames, transformNames...)
		}
		if !deferPolicyCreationUntilAliasSwap {
			createPolicies(ctx, es, policyDefinitions, policyNames)
			garbageCollectManagedPolicies(ctx, es, policyPlan.LogicalNames, policyPlan.DesiredSet)
		}
	}

	if action.requiresDataFile() {
		// Created-index path: the index was created above from the mappings
		// file, so verify it now. The existing-index path was verified before
		// the data action and is not checked twice.
		if !preflightExistingIndex {
			requireMappingPreflight(ctx, es, writeIndex, preflightPlan)
		}
		loggerFrom(ctx).Info().Msg("Starting bulk insert")

		// adr/0003-ndjson-data-file-support.md — format detected from first non-whitespace byte.
		first, err := peekFirstByte(*dataFile)
		if err != nil {
			fatal(ctx).Err(err).Msg("Cannot read data file")
		}

		overallStart := progressNow()
		succeededTotal := 0
		failedTotal := 0
		processed := 0

		switch first {
		case '[':
			loadJSONArray(
				ctx,
				es,
				*dataFile,
				writeIndex,
				*batchSize,
				*bulkRetryAttempts,
				*bulkRetryBackoffBase,
				*bulkRetryBackoffMax,
				*idField,
				&succeededTotal,
				&failedTotal,
				&processed,
				emit,
			)
		case '{':
			loadNDJSON(
				ctx,
				es,
				*dataFile,
				writeIndex,
				*batchSize,
				*bulkRetryAttempts,
				*bulkRetryBackoffBase,
				*bulkRetryBackoffMax,
				*idField,
				&succeededTotal,
				&failedTotal,
				&processed,
				emit,
			)
		default:
			fatal(ctx).Msgf("Data file first byte %q is neither '[' (JSON array) nor '{' (NDJSON)", first)
		}

		overallDuration := progressNow().Sub(overallStart)
		loggerFrom(ctx).Info().
			Int("processed", processed).
			Int("succeeded", succeededTotal).
			Int("failed", failedTotal).
			Float64("total_time", overallDuration.Seconds()).
			Msg("Bulk load completed")
		emit.emit(ProgressEvent{
			Stage:    ProgressStageBulk,
			Resource: writeIndex,
			Done:     int64(processed),
			Total:    int64(processed),
			Elapsed:  overallDuration,
			Final:    true,
		})

		if failedTotal > 0 {
			loggerFrom(ctx).Warn().
				Int("failed", failedTotal).
				Msg("Bulk load completed with failed items")
		}

		result.DocumentsProcessed = processed
		result.DocumentsSucceeded = succeededTotal
		result.DocumentsFailed = failedTotal
	}

	if *aliasMode && shouldCreateIndex {
		updateAlias(ctx, es, *index, createdIndex)
	}
	if deferPolicyCreationUntilAliasSwap {
		createPolicies(ctx, es, policyDefinitions, policyNames)
		garbageCollectManagedPolicies(ctx, es, policyPlan.LogicalNames, policyPlan.DesiredSet)
	}
	if *aliasMode && *keepLast > 0 {
		pruneTimestampedIndices(ctx, es, *index, *keepLast)
	}

	if enrich.enabled {
		refreshIndex(ctx, es, writeIndex)
		enrichResult := runEnrichPolicies(ctx, es, enrichSelection, policyNames, emit)
		result.EnrichSelected = enrichResult.Selected
		result.EnrichMissing = enrichResult.Missing
		result.EnrichSucceeded = enrichResult.Succeeded
		result.EnrichFailed = enrichResult.Failed
	}
	if effectiveSyncManaged && len(transformSyncNames) > 0 {
		// ADR: Keep transform readiness local to this stage; see
		// adr/0002-refresh-write-index-before-starting-managed-transforms.md.
		refreshIndex(ctx, es, writeIndex)
		stopTransformsBestEffort(ctx, es, transformSyncNames)
		createOrUpdateTransforms(ctx, es, transformDefinitions, transformSyncNames)
		startTransforms(ctx, es, transformSyncNames)
		if opts.Transforms.Wait {
			waitForTransforms(ctx, es, transformDefinitions, transformSyncNames, opts.Transforms.WaitTimeout, emit)
		} else {
			emitTransformsStarted(emit, transformSyncNames)
		}
	} else if opts.Transforms.Wait {
		warn("Ignoring transform wait because no transforms were synced for this run")
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
func logEffectiveManagedAssets(ctx context.Context, index, settingsFile, mappingsFile, pipelinesFile, policiesFile, transformsFile string, pipelineNames, policyNames, logicalTransformNames, selectedTransformNames []string) {
	loggerFrom(ctx).Info().
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
func buildMappingPreflightPlan(ctx context.Context, mappingsFile string, variables templateVariables) (mappingPreflightPlan, error) {
	trimmed := strings.TrimSpace(mappingsFile)
	if trimmed == "" {
		return mappingPreflightPlan{}, nil
	}

	normalized := normalizeIndexSection(ctx, trimmed, "mappings", variables)
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

// requireMappingPreflight fails the run when index's live mappings do not
// match plan. Both Run call sites (the early existing-index check and the
// post-create check) go through this helper, so the check cannot drift between
// the two paths.
//
// The fatal message must stay byte-identical: panicWithMessage passes it to
// classifyRunErrorKind, which matches "bulk" first, so a preflight failure is
// classified as ErrBulkFailure. TestRunExistingIndexMappingMismatchFailsBeforeMutation
// pins that kind; changing it is a deliberate decision, not a rename.
func requireMappingPreflight(ctx context.Context, es *elasticsearch.Client, index string, plan mappingPreflightPlan) {
	if !plan.hasExpectations() {
		return
	}
	if err := verifyMappingPreflight(ctx, es, index, plan); err != nil {
		fatal(ctx).Err(err).Str("index", index).Msg("Mapping preflight failed before bulk insert")
	}
}

// verifyMappingPreflight compares the live mappings behind name with plan and
// returns one error listing every mismatch. Within an index the
// date_detection mismatch comes first, then root fields in sorted order.
// Returning at the first mismatch while ranging over the RootFieldTypes map
// made the reported field depend on map order and forced the operator to fix
// one field per failed run; collecting everything gives a deterministic
// message that can be fixed in one pass.
//
// Every concrete index behind an alias is checked, because alias-mode -flush
// empties every target: verifying only one let a stale sibling be flushed
// (FIX-ALIASPRE-01). A failure lists each failing index between " | "
// separators so the indices stay distinct from the "; " between fields. A
// concrete-index request keeps the plain single-index message. A mapping fetch
// failure is returned as-is, because it is not a mismatch.
func verifyMappingPreflight(ctx context.Context, es *elasticsearch.Client, index string, plan mappingPreflightPlan) error {
	byIndex, err := fetchIndexMappings(es, index)
	if err != nil {
		return err
	}
	_, concrete := byIndex[index]

	targets := make([]string, 0, len(byIndex))
	for target := range byIndex {
		targets = append(targets, target)
	}
	slices.Sort(targets)

	var failures []string
	for _, target := range targets {
		if m := mappingMismatches(byIndex[target], plan); len(m) > 0 {
			failures = append(failures, fmt.Sprintf("index %q mappings do not match the mappings file: %s", target, strings.Join(m, "; ")))
			continue
		}

		event := loggerFrom(ctx).Info().
			Str("index", target).
			Int("root_field_type_checks", len(plan.RootFieldTypes))
		if !concrete {
			event = event.Str("alias", index)
		}
		if plan.DateDetectionExpected != nil {
			event = event.Bool("date_detection_expected", *plan.DateDetectionExpected)
		}
		event.Msg("Mapping preflight passed")
	}
	if len(failures) == 0 {
		return nil
	}
	if concrete {
		return errors.New(failures[0])
	}
	return fmt.Errorf("alias %q: %s", index, strings.Join(failures, " | "))
}

// mappingMismatches returns one index's mismatches in a fixed order, so every
// target of an alias is judged by the same rules: date_detection first, then
// root fields in sorted order.
func mappingMismatches(mappings map[string]any, plan mappingPreflightPlan) []string {
	var mismatches []string
	if plan.DateDetectionExpected != nil {
		actualDateDetection, present := extractDateDetection(mappings)
		if !present {
			actualDateDetection = true
		}
		if actualDateDetection != *plan.DateDetectionExpected {
			mismatches = append(mismatches, fmt.Sprintf("mappings.date_detection is %t, expected %t", actualDateDetection, *plan.DateDetectionExpected))
		}
	}

	fieldPaths := make([]string, 0, len(plan.RootFieldTypes))
	for fieldPath := range plan.RootFieldTypes {
		fieldPaths = append(fieldPaths, fieldPath)
	}
	slices.Sort(fieldPaths)
	for _, fieldPath := range fieldPaths {
		expectedType := plan.RootFieldTypes[fieldPath]
		actualType, ok := resolveFieldType(mappings, fieldPath)
		if !ok {
			mismatches = append(mismatches, fmt.Sprintf("required mapping for field %q is missing", fieldPath))
			continue
		}
		if actualType != expectedType {
			mismatches = append(mismatches, fmt.Sprintf("field %q has type %q, expected %q", fieldPath, actualType, expectedType))
		}
	}
	return mismatches
}

// fetchIndexMappings returns the mappings of every index in the _mapping
// response for name, keyed by concrete index name. Elasticsearch keys the
// response by concrete index, so an alias name is never a key; the old
// first-entry fallback checked one arbitrary target of a multi-target alias
// and let the others go unverified (FIX-ALIASPRE-01). Returning every entry
// lets the caller check them all.
func fetchIndexMappings(es *elasticsearch.Client, name string) (map[string]map[string]any, error) {
	res, err := es.Indices.GetMapping(es.Indices.GetMapping.WithIndex(name))
	if err != nil {
		return nil, fmt.Errorf("fetching mappings for index %q: %w", name, err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading mappings response for index %q: %w", name, err)
	}
	if res.IsError() {
		return nil, fmt.Errorf("fetching mappings for index %q failed with status %d: %s", name, res.StatusCode, string(body))
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decoding mappings response for index %q: %w", name, err)
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("mappings response for index %q was empty", name)
	}

	keys := make([]string, 0, len(payload))
	for key := range payload {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	result := make(map[string]map[string]any, len(payload))
	for _, key := range keys {
		entry, ok := payload[key].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("mappings response for index %q did not include an object entry", key)
		}
		mappings, ok := entry["mappings"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("mappings response for index %q did not include a mappings object", key)
		}
		result[key] = mappings
	}

	// A concrete-index request returns exactly its own entry; anything more is
	// an unexpected response and must not be checked under the wrong label.
	if _, ok := payload[name]; ok && len(payload) > 1 {
		return nil, fmt.Errorf("mappings response for index %q included other indices: %s", name, strings.Join(keys, ", "))
	}
	return result, nil
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

// buildTimestampedIndexName centralizes this code path so package behavior stays consistent.
func buildTimestampedIndexName(alias string, now time.Time) string {
	return fmt.Sprintf("%s-%s", alias, now.Format("20060102150405"))
}

// nextAvailableTimestampedIndexName centralizes this code path so package behavior stays consistent.
func nextAvailableTimestampedIndexName(ctx context.Context, es *elasticsearch.Client, alias string, base time.Time) string {
	name, err := nextAvailableTimestampedIndexNameWithCheck(ctx, alias, base, func(candidate string) (bool, error) {
		return indexExists(ctx, es, candidate)
	})
	checkErr(ctx, "finding available timestamped index name", err)
	return name
}

// nextAvailableTimestampedIndexNameWithCheck centralizes this code path so package behavior stays consistent.
func nextAvailableTimestampedIndexNameWithCheck(ctx context.Context, alias string, base time.Time, existsFn func(candidate string) (bool, error)) (string, error) {
	candidateTime := base.UTC()
	for attempt := 0; attempt < timestampedIndexCollisionLimit; attempt++ {
		candidate := buildTimestampedIndexName(alias, candidateTime)
		exists, err := existsFn(candidate)
		if err != nil {
			return "", err
		}
		if !exists {
			return candidate, nil
		}

		loggerFrom(ctx).Warn().
			Str("alias", alias).
			Str("index", candidate).
			Msg("Timestamped index name already exists; advancing by one second")
		candidateTime = candidateTime.Add(time.Second)
	}

	return "", fmt.Errorf("unable to find an available timestamped index name for alias %q", alias)
}

// resolveAliasTargets centralizes this code path so package behavior stays consistent.
func resolveAliasTargets(ctx context.Context, es *elasticsearch.Client, alias string) []string {
	res, err := es.Indices.GetAlias(es.Indices.GetAlias.WithName(alias), es.Indices.GetAlias.WithContext(ctx))
	checkErr(ctx, "resolving alias targets", err)
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		fatal(ctx).
			Str("alias", alias).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to resolve alias targets")
	}

	var parsed map[string]json.RawMessage
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		fatal(ctx).Err(err).Str("alias", alias).Msg("Unable to parse alias response")
	}

	targets := make([]string, 0, len(parsed))
	for index := range parsed {
		targets = append(targets, index)
	}
	slices.Sort(targets)
	return targets
}

// updateAlias centralizes this code path so package behavior stays consistent.
func updateAlias(ctx context.Context, es *elasticsearch.Client, alias, index string) {
	current := resolveAliasTargets(ctx, es, alias)

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
	checkErr(ctx, "serializing alias actions", err)
	res, err := es.Indices.UpdateAliases(strings.NewReader(string(payload)), es.Indices.UpdateAliases.WithContext(ctx))
	checkErr(ctx, "updating alias", err)
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		fatal(ctx).
			Str("alias", alias).
			Str("index", index).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to update alias")
	}

	loggerFrom(ctx).Info().Str("alias", alias).Str("index", index).Msg("Alias now points to index")
}

// deleteIndices centralizes this code path so package behavior stays consistent.
func deleteIndices(ctx context.Context, es *elasticsearch.Client, indices []string) {
	for _, index := range indices {
		deleteAndCheck(ctx, es, index)
		loggerFrom(ctx).Info().Str("index", index).Msg("Index deleted")
	}
}

// timestampedIndex groups state used to coordinate related package behavior.
type timestampedIndex struct {
	Name      string
	Timestamp time.Time
}

// listTimestampedIndices centralizes this code path so package behavior stays consistent.
func listTimestampedIndices(ctx context.Context, es *elasticsearch.Client, alias string) []timestampedIndex {
	pattern := alias + "-*"
	res, err := es.Indices.Get([]string{pattern}, es.Indices.Get.WithContext(ctx))
	checkErr(ctx, "listing timestamped indices", err)
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		fatal(ctx).
			Str("alias", alias).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to list timestamped indices")
	}

	var parsed map[string]json.RawMessage
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		fatal(ctx).Err(err).Str("alias", alias).Msg("Unable to parse timestamped index response")
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
func pruneTimestampedIndices(ctx context.Context, es *elasticsearch.Client, alias string, keepLast int) {
	if keepLast <= 0 {
		return
	}

	all := listTimestampedIndices(ctx, es, alias)
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

	loggerFrom(ctx).Info().Str("alias", alias).Int("keep_last", keepLast).Strs("indices", toDelete).Msg("Pruning old timestamped indices")
	deleteIndices(ctx, es, toDelete)
}

// ─── Settings, Mappings, and Template Utilities ───────────────────────────────

// checkErr centralizes this code path so package behavior stays consistent.
func checkErr(ctx context.Context, op string, err error) {
	loggerFrom(ctx).Trace().Msg(op)
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
func indexExists(ctx context.Context, es *elasticsearch.Client, index string) (bool, error) {
	res, err := es.Indices.Exists(
		[]string{index},
		es.Indices.Exists.WithContext(ctx),
	)
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
func waitForIndex(ctx context.Context, es *elasticsearch.Client, index string) {
	for i := 0; i < indexVisibilityMaxAttempts; i++ {
		exists, err := indexExists(ctx, es, index)
		checkErr(ctx, "waiting for index creation", err)
		if exists {
			return
		}
		if err := sleepWithContext(ctx, indexVisibilityPollInterval); err != nil {
			fatal(ctx).Err(err).Str("index", index).Msg("Context cancelled while waiting for index to become visible")
		}
	}

	fatal(ctx).Str("index", index).Msg("Index create was acknowledged but the index did not become visible")
}

// deleteAndCheck centralizes this code path so package behavior stays consistent.
func deleteAndCheck(ctx context.Context, es *elasticsearch.Client, index string) {
	res, err := es.Indices.Delete([]string{index})
	checkErr(ctx, "deleting index", err)
	defer res.Body.Close()

	if res.IsError() {
		fatal(ctx).Str("index", index).Msg("Failed to delete index")
	}
}

// flushAndCheck centralizes this code path so package behavior stays consistent.
func flushAndCheck(ctx context.Context, es *elasticsearch.Client, index string) {
	query := `{"query":{"match_all":{}}}`
	res, err := es.DeleteByQuery(
		[]string{index},
		strings.NewReader(query),
		es.DeleteByQuery.WithConflicts("proceed"),
		es.DeleteByQuery.WithRefresh(true),
	)
	checkErr(ctx, "flushing index", err)
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fatal(ctx).Err(err).Str("index", index).Msg("Unable to read index flush response body")
	}
	if res.IsError() {
		fatal(ctx).
			Str("index", index).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to flush index")
	}

	var result struct {
		Failures []json.RawMessage `json:"failures"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		fatal(ctx).Err(err).Str("index", index).Msg("Unable to parse index flush response body")
	}
	if len(result.Failures) > 0 {
		fatal(ctx).
			Str("index", index).
			Int("shard_failures", len(result.Failures)).
			Str("first_failure", string(result.Failures[0])).
			Msg("Index flush completed with shard-level failures")
	}
}

// clusterReplicaCount queries the cluster node count and returns the appropriate
// number_of_replicas value for the topology.
// ADR: docs/decisions/0001-replica-count-from-node-count.md
func clusterReplicaCount(ctx context.Context, es *elasticsearch.Client) int {
	if es == nil {
		return 0
	}
	res, err := es.Nodes.Info()
	if err != nil {
		loggerFrom(ctx).Warn().Err(err).Msg("Could not query cluster nodes; defaulting to 0 replicas")
		return 0
	}
	defer res.Body.Close()
	if res.IsError() {
		loggerFrom(ctx).Warn().Int("status_code", res.StatusCode).Msg("Cluster nodes query failed; defaulting to 0 replicas")
		return 0
	}

	var body struct {
		Nodes map[string]json.RawMessage `json:"nodes"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		loggerFrom(ctx).Warn().Err(err).Msg("Could not parse cluster nodes response; defaulting to 0 replicas")
		return 0
	}

	if len(body.Nodes) > 1 {
		return 1
	}
	return 0
}

// buildCreateIndexBody centralizes this code path so package behavior stays consistent.
//
// replicasOverride, when non-nil, wins over the cluster-topology rule and
// clusterReplicaCount is not called (no pointless _nodes round trip). It is
// read-only: the caller owns the pointee.
func buildCreateIndexBody(ctx context.Context, es *elasticsearch.Client, replicasOverride *int, settingsFile, mappingsFile, defaultPipeline string, variables templateVariables) string {
	// ADR: docs/decisions/0001-replica-count-from-node-count.md
	var replicas int
	replicasSource := "cluster topology"
	if replicasOverride != nil {
		replicas = *replicasOverride
		replicasSource = "explicit"
	} else {
		replicas = clusterReplicaCount(ctx, es)
	}
	settings := normalizeIndexSettings(ctx, settingsFile, defaultPipeline, replicas, replicasSource, variables)
	mappings := normalizeIndexSection(ctx, mappingsFile, "mappings", variables)

	return fmt.Sprintf(`{"settings": %s, "mappings": %s}`, settings, mappings)
}

// normalizeIndexSettings centralizes this code path so package behavior stays consistent.
func normalizeIndexSettings(ctx context.Context, path, defaultPipeline string, replicas int, replicasSource string, variables templateVariables) string {
	settings := make(map[string]json.RawMessage)
	if path != "" {
		content, err := readTemplatedFile(path, variables)
		if err != nil {
			fatal(ctx).Err(err).Str("path", path).Msg("Reading settings file")
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(content, &raw); err != nil {
			fatal(ctx).Err(err).Str("path", path).Msg("Parsing settings file")
		}

		source := raw
		if nested, ok := raw["settings"]; ok {
			source = nil
			if err := json.Unmarshal(nested, &source); err != nil {
				fatal(ctx).Err(err).Str("path", path).Msg("Parsing nested settings file")
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
			loggerFrom(ctx).Info().Str("pipeline", defaultPipeline).Msg("Using first declared pipeline as index.default_pipeline")
		}
	}

	// ADR: docs/decisions/0001-replica-count-from-node-count.md — override whatever
	// the settings file specifies; the caller chose the value (explicit override
	// or cluster topology) and replicasSource names which rule applied.
	settings["number_of_replicas"] = json.RawMessage(strconv.Itoa(replicas))
	loggerFrom(ctx).Info().Int("number_of_replicas", replicas).Str("source", replicasSource).Msg("Setting index replica count")

	normalized, err := json.Marshal(settings)
	if err != nil {
		fatal(ctx).Err(err).Str("path", path).Msg("Serializing settings")
	}

	return string(normalized)
}

// normalizeSettingKey centralizes this code path so package behavior stays consistent.
func normalizeSettingKey(key string) string {
	return strings.TrimPrefix(key, "index.")
}

// normalizeIndexSection centralizes this code path so package behavior stays consistent.
func normalizeIndexSection(ctx context.Context, path, section string, variables templateVariables) string {
	if path == "" {
		return "{}"
	}

	content, err := readTemplatedFile(path, variables)
	if err != nil {
		fatal(ctx).Err(err).Str("path", path).Msgf("Reading %s file", section)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(content, &raw); err != nil {
		fatal(ctx).Err(err).Str("path", path).Msgf("Parsing %s file", section)
	}

	if nested, ok := raw[section]; ok {
		return string(nested)
	}

	return string(content)
}

// readNamedDefinitions centralizes this code path so package behavior stays consistent.
func readNamedDefinitions(ctx context.Context, path, resourceType string, variables templateVariables) (namedDefinitions, []string) {
	if path == "" {
		return nil, nil
	}

	content, err := readTemplatedFile(path, variables)
	if err != nil {
		fatal(ctx).Err(err).Str("path", path).Msgf("Reading %s definitions file", resourceType)
	}

	decoder := json.NewDecoder(strings.NewReader(string(content)))
	token, err := decoder.Token()
	if err != nil {
		fatal(ctx).Err(err).Str("path", path).Msgf("Parsing %s definitions file", resourceType)
	}

	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		fatal(ctx).Str("path", path).Msgf("%s definitions file must contain a JSON object", resourceType)
	}

	definitions := make(namedDefinitions)
	names := make([]string, 0)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			fatal(ctx).Err(err).Str("path", path).Msgf("Reading %s definition name", resourceType)
		}

		name, ok := keyToken.(string)
		if !ok {
			fatal(ctx).Str("path", path).Msgf("Invalid %s definition name", resourceType)
		}

		var definition json.RawMessage
		if err := decoder.Decode(&definition); err != nil {
			fatal(ctx).Err(err).Str("path", path).Msgf("Parsing %s definition body", resourceType)
		}

		definitions[name] = definition
		names = append(names, name)
	}

	if _, err := decoder.Token(); err != nil {
		fatal(ctx).Err(err).Str("path", path).Msgf("Parsing %s definitions file", resourceType)
	}

	return definitions, names
}

// buildManagedPolicyPlan centralizes this code path so package behavior stays consistent.
func buildManagedPolicyPlan(ctx context.Context, definitions namedDefinitions, logicalNames []string) managedPolicyPlan {
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
			fatal(ctx).Str("policy", logicalName).Msg("Policy definition missing from parsed policy file")
		}

		desiredName := managedPolicyName(ctx, logicalName, definition)
		plan.DesiredNames = append(plan.DesiredNames, desiredName)
		plan.Definitions[desiredName] = definition
		plan.LogicalToDesired[logicalName] = desiredName
		plan.DesiredSet[desiredName] = struct{}{}

		loggerFrom(ctx).Debug().
			Str("logical_policy", logicalName).
			Str("policy", desiredName).
			Msg("Resolved managed policy name from policy definition hash")
	}
	return plan
}

// managedPolicyName centralizes this code path so package behavior stays consistent.
func managedPolicyName(ctx context.Context, logicalName string, definition json.RawMessage) string {
	canonical, err := canonicalizeRawJSON(definition)
	if err != nil {
		fatal(ctx).Err(err).Str("policy", logicalName).Msg("Failed to canonicalize policy definition for hashing")
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
func rewritePipelinePolicyReferences(ctx context.Context, definitions namedDefinitions, names []string, logicalToDesired map[string]string) namedDefinitions {
	if len(definitions) == 0 || len(logicalToDesired) == 0 {
		return definitions
	}

	rewritten := make(namedDefinitions, len(definitions))
	for _, name := range names {
		definition := definitions[name]
		var parsed any
		if err := json.Unmarshal(definition, &parsed); err != nil {
			fatal(ctx).Err(err).Str("pipeline", name).Msg("Unable to parse pipeline definition for policy rewrite")
		}

		replacements := rewriteEnrichPolicyNameReferences(parsed, logicalToDesired)
		encoded, err := json.Marshal(parsed)
		if err != nil {
			fatal(ctx).Err(err).Str("pipeline", name).Msg("Unable to serialize rewritten pipeline definition")
		}
		rewritten[name] = encoded

		if replacements > 0 {
			loggerFrom(ctx).Info().
				Str("pipeline", name).
				Int("replacements", replacements).
				Msg("Rewrote enrich policy references in pipeline to managed policy names")
		} else {
			loggerFrom(ctx).Trace().Str("pipeline", name).Msg("No enrich policy reference rewrites needed for pipeline")
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

// resolvePipelinePolicyFallbacks maps logical enrich policy references that the
// run supplied no definition for onto existing cluster policies. Pre-mapped
// and exact names win; a single managed generation is adopted; none warns and
// skips. Several managed generations fail the run: the suffix is a content
// hash with no recency, so picking one silently binds a pipeline to a stale
// definition (moxfall: a field fed by it was populated on 0 of 109,373
// documents, with no error). Never guess between generations.
func resolvePipelinePolicyFallbacks(ctx context.Context, es *elasticsearch.Client, definitions namedDefinitions, names []string, policyNameMapping map[string]string) {
	referenced := collectReferencedPolicyNamesFromPipelines(ctx, definitions, names)
	if len(referenced) == 0 {
		return
	}

	available, supported := getEnrichPolicies(ctx, es)
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
			loggerFrom(ctx).Debug().Str("policy", policy).Msg("Using existing exact enrich policy reference from pipeline definition")
			continue
		}

		matches := make([]string, 0)
		for _, candidate := range available {
			if managedPolicyNameMatchesLogical(policy, candidate) {
				matches = append(matches, candidate)
			}
		}
		if len(matches) == 0 {
			loggerFrom(ctx).Warn().Str("logical_policy", policy).Msg("Pipeline references enrich policy that does not exist as exact or managed policy name")
			continue
		}

		if len(matches) > 1 {
			slices.Sort(matches)
			// The candidates go in the wrapped error, not the message:
			// classifyRunErrorKind reads the message, and a real policy name
			// containing "enrich" or "bulk" would reclassify this error away
			// from ErrManagedResource. Do not assign policyNameMapping here.
			fatal(ctx).
				Err(fmt.Errorf("logical policy %q matches managed generations %s and this run supplied no definition for it; supply its definition or delete the stale generations",
					policy, strings.Join(matches, ", "))).
				Str("logical_policy", policy).Strs("candidates", matches).
				Msg("Ambiguous policy reference")
		}

		chosen := matches[0]
		policyNameMapping[policy] = chosen
		loggerFrom(ctx).Info().Str("logical_policy", policy).Str("policy", chosen).Msg("Resolved logical enrich policy reference to managed policy name")
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
func collectReferencedPolicyNamesFromPipelines(ctx context.Context, definitions namedDefinitions, names []string) []string {
	seen := make(map[string]struct{})
	for _, name := range names {
		var parsed any
		if err := json.Unmarshal(definitions[name], &parsed); err != nil {
			fatal(ctx).Err(err).Str("pipeline", name).Msg("Unable to parse pipeline definition while collecting policy references")
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
func resolveManagedPolicyDeleteNames(ctx context.Context, es *elasticsearch.Client, logicalNames []string) []string {
	if len(logicalNames) == 0 {
		return nil
	}

	available, supported := getEnrichPolicies(ctx, es)
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
func remapEnrichSelection(ctx context.Context, enrich *enrichFlagValue, logicalToDesired map[string]string) *enrichFlagValue {
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
	loggerFrom(ctx).Debug().Str("requested", enrich.raw).Str("resolved", cloned.raw).Msg("Mapped explicit enrich policy names to managed policy names")
	return &cloned
}

// resolveTransformsForSource centralizes this code path so package behavior stays consistent.
func resolveTransformsForSource(ctx context.Context, definitions namedDefinitions, names []string, sourceIndex string) (namedDefinitions, []string, error) {
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
			loggerFrom(ctx).Trace().
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
func garbageCollectManagedPolicies(ctx context.Context, es *elasticsearch.Client, logicalNames []string, desiredSet map[string]struct{}) {
	if len(logicalNames) == 0 {
		return
	}

	available, supported := getEnrichPolicies(ctx, es)
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

		referencing := findPipelinesReferencingPolicy(ctx, es, policy)
		if len(referencing) > 0 {
			loggerFrom(ctx).Debug().
				Str("policy", policy).
				Strs("pipelines", referencing).
				Msg("Skipping managed policy GC because policy is still referenced by pipelines")
			continue
		}

		deletePolicyBestEffort(ctx, es, policy)
	}
}

// deletePolicyBestEffort centralizes this code path so package behavior stays consistent.
func deletePolicyBestEffort(ctx context.Context, es *elasticsearch.Client, policy string) {
	res, err := es.EnrichDeletePolicy(
		policy,
		es.EnrichDeletePolicy.WithContext(ctx),
		es.EnrichDeletePolicy.WithHeader(map[string]string{
			"Accept": "application/json",
		}),
	)
	if err != nil {
		loggerFrom(ctx).Warn().Err(err).Str("policy", policy).Msg("Managed policy GC failed while deleting policy")
		return
	}
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)
	if res.StatusCode == http.StatusNotFound {
		loggerFrom(ctx).Trace().Str("policy", policy).Msg("Managed policy GC found policy already deleted")
		return
	}
	if res.IsError() {
		loggerFrom(ctx).Warn().
			Str("policy", policy).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Managed policy GC could not delete policy")
		return
	}

	loggerFrom(ctx).Info().Str("policy", policy).Msg("Managed policy GC deleted unreferenced policy")
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

// ─── Managed Resource Lifecycle ────────────────────────────────────────────────

// createPipelines centralizes this code path so package behavior stays consistent.
func createPipelines(ctx context.Context, es *elasticsearch.Client, definitions namedDefinitions, names []string) {
	for _, name := range names {
		res, err := es.Ingest.PutPipeline(
			name,
			strings.NewReader(string(definitions[name])),
			es.Ingest.PutPipeline.WithContext(ctx),
		)
		checkErr(ctx, "creating pipeline", err)

		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			fatal(ctx).
				Str("pipeline", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to create pipeline")
		}
		res.Body.Close()

		loggerFrom(ctx).Info().Str("pipeline", name).Msg("Pipeline created or updated")
	}
}

// deletePipelines centralizes this code path so package behavior stays consistent.
func deletePipelines(ctx context.Context, es *elasticsearch.Client, names []string) {
	for _, name := range names {
		res, err := es.Ingest.DeletePipeline(
			name,
			es.Ingest.DeletePipeline.WithContext(ctx),
		)
		checkErr(ctx, "deleting pipeline", err)

		if res.StatusCode == http.StatusNotFound {
			res.Body.Close()
			loggerFrom(ctx).Info().Str("pipeline", name).Msg("Pipeline does not exist. Nothing to delete.")
			continue
		}
		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			fatal(ctx).
				Str("pipeline", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to delete pipeline")
		}
		res.Body.Close()

		loggerFrom(ctx).Info().Str("pipeline", name).Msg("Pipeline deleted")
	}
}

// createPolicies centralizes this code path so package behavior stays consistent.
func createPolicies(ctx context.Context, es *elasticsearch.Client, definitions namedDefinitions, names []string) {
	for _, name := range names {
		for attempt := 1; attempt <= 5; attempt++ {
			res, err := putPolicy(ctx, es, name, definitions[name])
			checkErr(ctx, "creating enrich policy", err)

			if !res.IsError() {
				res.Body.Close()
				loggerFrom(ctx).Info().Str("policy", name).Msg("Created enrich policy")
				break
			}

			body, _ := io.ReadAll(res.Body)
			res.Body.Close()

			if isUnsupportedEnrichAPI(res.StatusCode, body) {
				loggerFrom(ctx).Warn().
					Int("status_code", res.StatusCode).
					Str("body", string(body)).
					Msg("Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools")
				return
			}
			if hasElasticsearchErrorType(body, "index_not_found_exception") && attempt < 5 {
				loggerFrom(ctx).Warn().
					Str("policy", name).
					Int("attempt", attempt).
					Int("status_code", res.StatusCode).
					Str("body", string(body)).
					Msg("Source index for enrich policy is not visible yet; retrying enrich policy creation")
				if err := sleepWithContext(ctx, 500*time.Millisecond); err != nil {
					fatal(ctx).Err(err).Str("policy", name).Msg("Context cancelled during enrich policy retry")
				}
				continue
			}
			if hasElasticsearchErrorType(body, "resource_already_exists_exception") {
				loggerFrom(ctx).Info().
					Str("policy", name).
					Msg("Managed enrich policy already exists for this definition hash")
				break
			}

			fatal(ctx).
				Str("policy", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to create enrich policy")
		}
	}
}

// putPolicy centralizes this code path so package behavior stays consistent.
func putPolicy(ctx context.Context, es *elasticsearch.Client, name string, definition json.RawMessage) (*esapi.Response, error) {
	return es.EnrichPutPolicy(
		name,
		strings.NewReader(string(definition)),
		es.EnrichPutPolicy.WithContext(ctx),
		es.EnrichPutPolicy.WithHeader(map[string]string{
			"Content-Type": "application/json",
			"Accept":       "application/json",
		}),
	)
}

// deletePolicies centralizes this code path so package behavior stays consistent.
func deletePolicies(ctx context.Context, es *elasticsearch.Client, names []string, nuke bool) {
	for _, name := range names {
		for attempt := 1; attempt <= 2; attempt++ {
			res, err := es.EnrichDeletePolicy(
				name,
				es.EnrichDeletePolicy.WithContext(ctx),
				es.EnrichDeletePolicy.WithHeader(map[string]string{
					"Accept": "application/json",
				}),
			)
			checkErr(ctx, "deleting enrich policy", err)

			if res.StatusCode == http.StatusNotFound {
				body, _ := io.ReadAll(res.Body)
				res.Body.Close()
				if isUnsupportedEnrichAPI(res.StatusCode, body) {
					loggerFrom(ctx).Warn().
						Int("status_code", res.StatusCode).
						Str("body", string(body)).
						Msg("Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools")
					return
				}
				loggerFrom(ctx).Debug().Str("policy", name).Msg("Enrich policy does not exist. Nothing to delete.")
				break
			}
			if res.IsError() {
				body, _ := io.ReadAll(res.Body)
				res.Body.Close()
				if res.StatusCode == http.StatusConflict && nuke && policyDeleteBlockedByPipelineReference(body) && attempt == 1 {
					referencing := findPipelinesReferencingPolicy(ctx, es, name)
					if len(referencing) == 0 {
						fatal(ctx).
							Str("policy", name).
							Int("status_code", res.StatusCode).
							Str("body", string(body)).
							Msg("Failed to delete enrich policy; nuke mode could not find referencing pipelines")
					}

					loggerFrom(ctx).Warn().
						Str("policy", name).
						Strs("pipelines", referencing).
						Msg("Nuke mode deleting pipelines that reference this enrich policy before retrying policy deletion")
					deletePipelinesForNuke(ctx, es, referencing)
					continue
				}

				fatal(ctx).
					Str("policy", name).
					Int("status_code", res.StatusCode).
					Str("body", string(body)).
					Msg("Failed to delete enrich policy")
			}
			res.Body.Close()

			loggerFrom(ctx).Info().Str("policy", name).Msg("Deleted enrich policy")
			break
		}
	}
}

// deleteManagedResources centralizes this code path so package behavior stays consistent.
func deleteManagedResources(ctx context.Context, es *elasticsearch.Client, pipelineNames []string, policyNames []string, transformNames []string, nuke bool) {
	if len(pipelineNames) > 0 {
		if nuke {
			deletePipelinesForNuke(ctx, es, pipelineNames)
		} else {
			deletePipelines(ctx, es, pipelineNames)
		}
	}
	if len(policyNames) > 0 {
		deletePolicies(ctx, es, policyNames, nuke)
	}
	if len(transformNames) > 0 {
		deleteTransforms(ctx, es, transformNames)
	}
}

// createOrUpdateTransforms centralizes this code path so package behavior stays consistent.
func createOrUpdateTransforms(ctx context.Context, es *elasticsearch.Client, definitions namedDefinitions, names []string) {
	for _, name := range names {
		definition, ok := definitions[name]
		if !ok {
			fatal(ctx).Str("transform", name).Msg("Transform definition missing from parsed transform file")
		}

		// ES 9.x does not allow updating immutable transform properties
		// (pivot, latest) via TransformUpdateTransform. Delete and recreate
		// instead — stopTransformsBestEffort has already stopped the
		// transform, and deleteTransforms uses DeleteDestIndex: false to
		// preserve the destination index.
		if transformExists(ctx, es, name) {
			deleteTransforms(ctx, es, []string{name})
		}

		res, err := es.TransformPutTransform(
			strings.NewReader(string(definition)),
			name,
			es.TransformPutTransform.WithContext(ctx),
			es.TransformPutTransform.WithHeader(map[string]string{
				"Content-Type": "application/json",
				"Accept":       "application/json",
			}),
		)
		checkErr(ctx, "creating transform", err)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.IsError() {
			fatal(ctx).
				Str("transform", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to create transform")
		}
		loggerFrom(ctx).Info().Str("transform", name).Msg("Transform created")
	}
}

// transformExists centralizes this code path so package behavior stays consistent.
func transformExists(ctx context.Context, es *elasticsearch.Client, name string) bool {
	res, err := es.TransformGetTransform(
		es.TransformGetTransform.WithContext(ctx),
		es.TransformGetTransform.WithTransformID(name),
		es.TransformGetTransform.WithAllowNoMatch(true),
	)
	checkErr(ctx, "checking if transform exists", err)
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return false
	}
	if res.IsError() {
		fatal(ctx).
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
func stopTransformsBestEffort(ctx context.Context, es *elasticsearch.Client, names []string) {
	for _, name := range names {
		res, err := es.TransformStopTransform(
			name,
			es.TransformStopTransform.WithContext(ctx),
			es.TransformStopTransform.WithForce(true),
			es.TransformStopTransform.WithWaitForCompletion(true),
			es.TransformStopTransform.WithTimeout(30*time.Second),
			es.TransformStopTransform.WithAllowNoMatch(true),
			es.TransformStopTransform.WithHeader(map[string]string{
				"Accept": "application/json",
			}),
		)
		checkErr(ctx, "stopping transform", err)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusNotFound || transformStopAlreadyStopped(res.StatusCode, body) {
			loggerFrom(ctx).Debug().Str("transform", name).Msg("Transform is not running. Nothing to stop.")
			continue
		}
		if res.IsError() {
			fatal(ctx).
				Str("transform", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to stop transform")
		}
		loggerFrom(ctx).Debug().Str("transform", name).Msg("Transform stopped")
	}
}

// deleteTransforms centralizes this code path so package behavior stays consistent.
func deleteTransforms(ctx context.Context, es *elasticsearch.Client, names []string) {
	for _, name := range names {
		res, err := es.TransformDeleteTransform(
			name,
			es.TransformDeleteTransform.WithContext(ctx),
			es.TransformDeleteTransform.WithForce(true),
			es.TransformDeleteTransform.WithDeleteDestIndex(false),
			es.TransformDeleteTransform.WithHeader(map[string]string{
				"Accept": "application/json",
			}),
		)
		checkErr(ctx, "deleting transform", err)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusNotFound {
			loggerFrom(ctx).Debug().Str("transform", name).Msg("Transform does not exist. Nothing to delete.")
			continue
		}
		if res.IsError() {
			fatal(ctx).
				Str("transform", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to delete transform")
		}
		loggerFrom(ctx).Info().Str("transform", name).Msg("Transform deleted")
	}
}

// startTransforms centralizes this code path so package behavior stays consistent.
func startTransforms(ctx context.Context, es *elasticsearch.Client, names []string) {
	for _, name := range names {
		res, err := es.TransformStartTransform(
			name,
			es.TransformStartTransform.WithContext(ctx),
			es.TransformStartTransform.WithTimeout(30*time.Second),
			es.TransformStartTransform.WithHeader(map[string]string{
				"Accept": "application/json",
			}),
		)
		checkErr(ctx, "starting transform", err)
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if transformStartAlreadyStarted(res.StatusCode, body) {
			loggerFrom(ctx).Info().Str("transform", name).Msg("Transform already running")
			continue
		}
		if res.IsError() {
			fatal(ctx).
				Str("transform", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to start transform")
		}

		loggerFrom(ctx).Info().Str("transform", name).Msg("Transform started")
	}
}

// deletePipelinesForNuke centralizes this code path so package behavior stays consistent.
func deletePipelinesForNuke(ctx context.Context, es *elasticsearch.Client, names []string) {
	for _, name := range names {
		res, err := es.Ingest.DeletePipeline(
			name,
			es.Ingest.DeletePipeline.WithContext(ctx),
		)
		checkErr(ctx, "deleting pipeline", err)

		if res.StatusCode == http.StatusNotFound {
			res.Body.Close()
			loggerFrom(ctx).Info().Str("pipeline", name).Msg("Pipeline does not exist. Nothing to delete.")
			continue
		}
		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode == http.StatusBadRequest && pipelineDeleteBlockedByDefaultIndex(body) {
				indices := findIndicesUsingDefaultPipeline(ctx, es, name)
				if len(indices) == 0 {
					fatal(ctx).
						Str("pipeline", name).
						Int("status_code", res.StatusCode).
						Str("body", string(body)).
						Msg("Failed to delete pipeline; nuke mode could not find indices using it as default pipeline")
				}

				loggerFrom(ctx).Warn().
					Str("pipeline", name).
					Strs("indices", indices).
					Msg("Nuke mode clearing index.default_pipeline on indices that use this pipeline before retrying pipeline deletion")
				clearDefaultPipelineForIndices(ctx, es, indices)
				deletePipelines(ctx, es, []string{name})
				continue
			}
			fatal(ctx).
				Str("pipeline", name).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to delete pipeline")
		}
		res.Body.Close()

		loggerFrom(ctx).Info().Str("pipeline", name).Msg("Pipeline deleted")
	}
}

// findPipelinesReferencingPolicy centralizes this code path so package behavior stays consistent.
func findPipelinesReferencingPolicy(ctx context.Context, es *elasticsearch.Client, policy string) []string {
	res, err := es.Ingest.GetPipeline(
		es.Ingest.GetPipeline.WithContext(ctx),
	)
	checkErr(ctx, "getting ingest pipelines", err)
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		fatal(ctx).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to get ingest pipelines")
	}

	var definitions namedDefinitions
	if err := json.NewDecoder(res.Body).Decode(&definitions); err != nil {
		fatal(ctx).Err(err).Msg("Unable to parse ingest pipeline response")
	}

	return pipelineNamesReferencingPolicy(definitions, policy)
}

// findIndicesUsingDefaultPipeline centralizes this code path so package behavior stays consistent.
func findIndicesUsingDefaultPipeline(ctx context.Context, es *elasticsearch.Client, pipeline string) []string {
	res, err := es.Indices.GetSettings(es.Indices.GetSettings.WithName("*"), es.Indices.GetSettings.WithContext(ctx))
	checkErr(ctx, "getting index settings", err)
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		fatal(ctx).
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
		fatal(ctx).Err(err).Msg("Unable to parse index settings response")
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
func clearDefaultPipelineForIndices(ctx context.Context, es *elasticsearch.Client, indices []string) {
	for _, index := range indices {
		res, err := es.Indices.PutSettings(
			strings.NewReader(`{"index.default_pipeline":null}`),
			es.Indices.PutSettings.WithIndex(index),
			es.Indices.PutSettings.WithContext(ctx),
		)
		checkErr(ctx, "clearing index.default_pipeline", err)

		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			fatal(ctx).
				Str("index", index).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Failed to clear index.default_pipeline")
		}
		res.Body.Close()

		loggerFrom(ctx).Warn().Str("index", index).Msg("Cleared index.default_pipeline to allow nuke cleanup")
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

// refreshIndex forces an immediate index refresh so subsequent reads see all
// recently written documents. Called before enrich policy execution and before
// batch transform start to eliminate the ES refresh-window race.
func refreshIndex(ctx context.Context, es *elasticsearch.Client, index string) {
	res, err := es.Indices.Refresh(es.Indices.Refresh.WithIndex(index))
	checkErr(ctx, "refreshing index", err)
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		fatal(ctx).
			Str("index", index).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to refresh index")
	}

	loggerFrom(ctx).Info().Str("index", index).Msg("Refreshed index")
}

// getEnrichPolicies centralizes this code path so package behavior stays consistent.
func getEnrichPolicies(ctx context.Context, es *elasticsearch.Client) ([]string, bool) {
	res, err := es.EnrichGetPolicy(
		es.EnrichGetPolicy.WithContext(ctx),
		es.EnrichGetPolicy.WithHeader(map[string]string{
			"Accept": "application/json",
		}),
	)
	checkErr(ctx, "getting enrich policies", err)
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		if isUnsupportedEnrichAPI(res.StatusCode, body) {
			loggerFrom(ctx).Warn().
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Enrich policy endpoint returned a generic 404; check proxy or routing for /_enrich/policy and confirm this URL matches the backend used by Dev Tools")
			return nil, false
		}
		fatal(ctx).
			Int("status_code", res.StatusCode).
			Str("body", string(body)).
			Msg("Failed to get enrich policies")
	}

	var parsed enrichPoliciesResponse
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		fatal(ctx).Err(err).Msg("Unable to parse enrich policy response")
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

// peekFirstByte opens path and returns the first non-whitespace byte without
// consuming it. The file is opened and closed internally; the caller does not
// hold an open file handle after this call. Used to detect JSON array vs NDJSON
// format before creating a decoder.
// adr/0003-ndjson-data-file-support.md
func peekFirstByte(path string) (byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, err
		}
		if b != ' ' && b != '\t' && b != '\r' && b != '\n' {
			return b, nil
		}
	}
}

// countNDJSONLines counts non-empty lines in path without parsing JSON.
// Used to obtain a document total for progress reporting before the NDJSON
// decode pass. The 10 MB scanner buffer accommodates large Elasticsearch
// documents that would otherwise exceed the default 64 KB scanner limit.
// adr/0003-ndjson-data-file-support.md
func countNDJSONLines(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 10<<20)
	var n int
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) > 0 {
			n++
		}
	}

	return n, sc.Err()
}

// loadJSONArray loads documents from a JSON array file into index using the
// ES bulk API. It performs two passes: a count pass for progress reporting and
// a decode pass that streams documents in batches of batchSize.
// adr/0003-ndjson-data-file-support.md
func loadJSONArray(
	ctx context.Context,
	es *elasticsearch.Client,
	dataFile, index string,
	batchSize int,
	retryAttempts int,
	retryBackoffBase, retryBackoffMax time.Duration,
	idField string,
	succeededTotal, failedTotal, processed *int,
	emit progressEmitter,
) {
	start := progressNow()
	f, err := os.Open(dataFile)
	checkErr(ctx, "opening data file", err)
	defer f.Close()

	dec := json.NewDecoder(f)
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('[') {
		fatal(ctx).Msg("Data file must be a JSON array")
	}

	loggerFrom(ctx).Debug().Str("data_file", dataFile).Msg("Counting documents in data file")
	total := 0
	for dec.More() {
		var tmp map[string]interface{}
		if err := dec.Decode(&tmp); err != nil {
			fatal(ctx).Err(err).Msg("Error counting objects in data file")
		}
		total++
	}
	loggerFrom(ctx).Debug().Str("data_file", dataFile).Int("total", total).Msg("Document count complete")

	if _, err := f.Seek(0, 0); err != nil {
		fatal(ctx).Err(err).Msg("Error rewinding data file")
	}
	dec = json.NewDecoder(f)
	_, err = dec.Token()
	if err != nil {
		fatal(ctx).Err(err).Msg("Error re-reading data file")
	}

	batch := make([]map[string]interface{}, 0, batchSize)
	for dec.More() {
		var doc map[string]interface{}
		if err := dec.Decode(&doc); err != nil {
			fatal(ctx).Err(err).Msg("Error decoding object in data file")
		}
		batch = append(batch, doc)
		if len(batch) == batchSize {
			batchResult := bulkInsert(
				ctx,
				es,
				index,
				batch,
				*processed+len(batch),
				total,
				retryAttempts,
				retryBackoffBase,
				retryBackoffMax,
				idField,
			)
			*processed += len(batch)
			emitBulkBatch(emit, index, *processed, total, start)
			*succeededTotal += batchResult.Succeeded
			*failedTotal += batchResult.Failed
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		batchResult := bulkInsert(
			ctx,
			es,
			index,
			batch,
			*processed+len(batch),
			total,
			retryAttempts,
			retryBackoffBase,
			retryBackoffMax,
			idField,
		)
		*processed += len(batch)
		emitBulkBatch(emit, index, *processed, total, start)
		*succeededTotal += batchResult.Succeeded
		*failedTotal += batchResult.Failed
	}
}

// loadNDJSON loads documents from an NDJSON file (one JSON object per line)
// into index using the ES bulk API. It performs a cheap byte-level line-count
// pass for progress reporting, then streams documents with json.Decoder in
// batches of batchSize.
// adr/0003-ndjson-data-file-support.md
func loadNDJSON(
	ctx context.Context,
	es *elasticsearch.Client,
	dataFile, index string,
	batchSize int,
	retryAttempts int,
	retryBackoffBase, retryBackoffMax time.Duration,
	idField string,
	succeededTotal, failedTotal, processed *int,
	emit progressEmitter,
) {
	start := progressNow()
	total, err := countNDJSONLines(dataFile)
	if err != nil {
		fatal(ctx).Err(err).Msg("Error counting NDJSON lines")
	}
	loggerFrom(ctx).Debug().Str("data_file", dataFile).Int("total", total).Msg("NDJSON line count complete")

	f, err := os.Open(dataFile)
	if err != nil {
		fatal(ctx).Err(err).Msg("Error opening NDJSON data file")
	}
	defer f.Close()

	dec := json.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	batch := make([]map[string]interface{}, 0, batchSize)

	for dec.More() {
		var doc map[string]interface{}
		if err := dec.Decode(&doc); err != nil {
			fatal(ctx).Err(err).Msg("Error decoding NDJSON document")
		}
		batch = append(batch, doc)
		if len(batch) == batchSize {
			batchResult := bulkInsert(
				ctx,
				es,
				index,
				batch,
				*processed+len(batch),
				total,
				retryAttempts,
				retryBackoffBase,
				retryBackoffMax,
				idField,
			)
			*processed += len(batch)
			emitBulkBatch(emit, index, *processed, total, start)
			*succeededTotal += batchResult.Succeeded
			*failedTotal += batchResult.Failed
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		batchResult := bulkInsert(
			ctx,
			es,
			index,
			batch,
			*processed+len(batch),
			total,
			retryAttempts,
			retryBackoffBase,
			retryBackoffMax,
			idField,
		)
		*processed += len(batch)
		emitBulkBatch(emit, index, *processed, total, start)
		*succeededTotal += batchResult.Succeeded
		*failedTotal += batchResult.Failed
	}
}

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
	for batchOffset, doc := range batch {
		meta := map[string]map[string]string{"index": {"_index": index}}

		if idField != "" {
			if v, ok := doc[idField]; ok {
				if idStr, ok := v.(string); ok && idStr != "" {
					meta["index"]["_id"] = idStr
				}
			}
		}

		documentNumber := inserted - len(batch) + batchOffset + 1
		metaLine, err := json.Marshal(meta)
		if err != nil {
			fatal(ctx).Err(err).Int("document", documentNumber).Msg("Unable to serialize bulk action metadata")
		}
		docLine, err := json.Marshal(doc)
		if err != nil {
			fatal(ctx).Err(err).Int("document", documentNumber).Msg("Unable to serialize document for bulk insert")
		}
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
				fatal(ctx).Err(ctx.Err()).Msg("Bulk API request failed")
			}
			if shouldRetryBulkRequest(0, err) && attempt < retryAttempts {
				nextBackoff := computeExponentialBackoff(attempt, retryBackoffBase, retryBackoffMax)
				loggerFrom(ctx).Warn().
					Err(err).
					Int("attempt", attempt).
					Int("max_attempts", retryAttempts).
					Str("next_backoff", nextBackoff.String()).
					Msg("Bulk API request failed; retrying")
				if sleepErr := sleepWithContext(ctx, nextBackoff); sleepErr != nil {
					fatal(ctx).Err(sleepErr).Msg("Bulk API request failed")
				}
				continue
			}
			fatal(ctx).Err(err).Msg("Bulk API request failed")
		}

		if res.IsError() {
			body, _ := io.ReadAll(res.Body)
			_ = res.Body.Close()
			if shouldRetryBulkRequest(res.StatusCode, nil) && attempt < retryAttempts {
				nextBackoff := computeExponentialBackoff(attempt, retryBackoffBase, retryBackoffMax)
				loggerFrom(ctx).Warn().
					Int("status_code", res.StatusCode).
					Str("body", string(body)).
					Int("attempt", attempt).
					Int("max_attempts", retryAttempts).
					Str("next_backoff", nextBackoff.String()).
					Msg("Bulk API request failed; retrying")
				if sleepErr := sleepWithContext(ctx, nextBackoff); sleepErr != nil {
					fatal(ctx).Err(sleepErr).Msg("Bulk API request failed")
				}
				continue
			}
			fatal(ctx).
				Int("status_code", res.StatusCode).
				Str("body", string(body)).
				Msg("Bulk API request failed")
		}

		if attempt > 1 {
			loggerFrom(ctx).Info().
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
		fatal(ctx).Err(err).Msg("Unable to parse bulk response body")
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
					loggerFrom(ctx).Error().
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
		loggerFrom(ctx).Error().
			Int("failed_items", failed).
			Int("logged_failures", logged).
			Msg("Additional bulk item failures omitted from logs")
	}

	succeeded := len(batch) - failed
	loggerFrom(ctx).Debug().
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

package loader

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/aliasops"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/bulk"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/config"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/enrich"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/indexops"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/managed"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/progress"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/template"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/transform"
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

// enrichFromOptions adapts the public EnrichOptions to the internal selection:
// a non-empty Policies list enables enrich and wins over All and Raw; an
// enabled selection with no names selects all; a disabled one selects none.
func enrichFromOptions(options EnrichOptions) *enrich.Selection {
	selection := &enrich.Selection{
		Enabled: options.Enabled,
		All:     options.All,
		Raw:     strings.TrimSpace(options.Raw),
	}
	if len(options.Policies) > 0 {
		selection.Enabled = true
		selection.All = false
		selection.Raw = strings.Join(options.Policies, ",")
	}
	if !selection.Enabled {
		selection.All = false
		selection.Raw = ""
	}
	if selection.Enabled && !selection.All && selection.Raw == "" {
		selection.All = true
	}
	return selection
}

// classifyRunErrorKind maps an internal operation error's Op to the public
// RunError.Kind by case-insensitive substring. The first match wins, in the
// order: bulk, enrich, managed-resource nouns (pipeline, policy, transform,
// managed), index/indices/alias, otherwise ErrLoaderExecution.
// "indices" is listed because it does not contain "index"; without it the
// timestamped-generation listing Ops in aliasops fell through to
// ErrLoaderExecution (FIX-LISTKIND-01). Order matters: an Op naming both a
// managed resource and indices (the nuke pipeline-delete Op) stays managed.
func classifyRunErrorKind(op string) error {
	lowered := strings.ToLower(op)
	switch {
	case strings.Contains(lowered, "bulk"):
		return ErrBulkFailure
	case strings.Contains(lowered, "enrich"):
		return ErrEnrichExecution
	case strings.Contains(lowered, "pipeline"), strings.Contains(lowered, "policy"), strings.Contains(lowered, "transform"), strings.Contains(lowered, "managed"):
		return ErrManagedResource
	case strings.Contains(lowered, "index"), strings.Contains(lowered, "indices"), strings.Contains(lowered, "alias"):
		return ErrIndexOperation
	default:
		return ErrLoaderExecution
	}
}

// ─── Public Error Boundary ─────────────────────────────────────────────────────

// runFailure converts an error returned by an internal helper into the public
// *RunError, exactly once, at a Run call site. An operation error is
// classified from its Op by classifyRunErrorKind and keeps its original cause,
// so Kind, Op, cause and message match what the former panic bridge produced.
// An already-classified *RunError (explicit validation, including the
// strict-template ErrInvalidOptions) is returned unchanged: re-classifying its
// Op by substring would turn it into ErrLoaderExecution.
//
// errors.As, not a type switch, finds the operation error: a helper that
// wraps one (fmt.Errorf("...: %w", opErr)) must still report its Op and Kind
// rather than fall through to ErrLoaderExecution with an empty Op.
func runFailure(err error) error {
	if err == nil {
		return nil
	}
	if runErr, ok := err.(*RunError); ok {
		return runErr
	}
	var opErr *failure.Error
	if errors.As(err, &opErr) {
		return &RunError{Kind: classifyRunErrorKind(opErr.Op), Op: opErr.Op, Err: opErr.Err}
	}
	// Every internal helper returns *failure.Error; this keeps an unexpected
	// plain error typed rather than losing it.
	return &RunError{Kind: ErrLoaderExecution, Err: err}
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
	logger := runlog.Resolve(opts.Logger)
	ctx = runlog.WithLogger(ctx, &logger)

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
	requestedEnrich := enrichFromOptions(opts.Enrich)

	if *url == "" {
		*url = "http://localhost:9200"
	}
	if *batchSize <= 0 {
		*batchSize = 1000
	}
	if *bulkRetryAttempts <= 0 {
		*bulkRetryAttempts = bulk.DefaultRetryAttempts
	}
	if *bulkRetryBackoffBase <= 0 {
		*bulkRetryBackoffBase = bulk.DefaultRetryBackoffBase
	}
	if *bulkRetryBackoffMax <= 0 {
		*bulkRetryBackoffMax = bulk.DefaultRetryBackoffMax
	}
	if *bulkRetryBackoffMax < *bulkRetryBackoffBase {
		*bulkRetryBackoffMax = *bulkRetryBackoffBase
	}
	if opts.Transforms.WaitTimeout <= 0 {
		opts.Transforms.WaitTimeout = DefaultTransformWaitTimeout
	}
	emit := progressEmitter(opts.Progress)

	// Internal failures are returned, never panicked. This recover exists
	// only for panics Run does not own, such as an Options.Progress callback:
	// an error value fails the Run as documented on ProgressEvent, and any
	// other value propagates unchanged.
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
		runlog.From(ctx).Warn().Msg(message)
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

	if action == dataActionNone && !*syncManaged && !*nuke && !requestedEnrich.Enabled {
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
	if err := runlog.Check(ctx, "creating Elasticsearch client", err); err != nil {
		return result, runFailure(err)
	}

	variables := template.BuildVariables(*index, *templateVariables)

	// Validate every supplied template file for unresolved variables before
	// any create, delete, flush or put, so a typo cannot leave the cluster
	// half-reset. Read and parse failures of settings, mappings and the data
	// file are preflighted separately below. Fail closed here instead.
	//
	// The classification is explicit, not runFailure: classifying this Op by
	// substring would turn an invalid input into ErrLoaderExecution.
	if err := template.Preflight(variables, *settingsFile, *mappingsFile, *pipelinesFile, *policiesFile, *transformsFile); err != nil {
		return result, &RunError{Kind: ErrInvalidOptions, Op: "validating template variables", Err: err}
	}
	pipelineDefinitions, pipelineNames, err := config.ReadNamedDefinitions(ctx, *pipelinesFile, "pipeline", variables)
	if err != nil {
		return result, runFailure(err)
	}
	logicalPolicyDefinitions, logicalPolicyNames, err := config.ReadNamedDefinitions(ctx, *policiesFile, "policy", variables)
	if err != nil {
		return result, runFailure(err)
	}
	logicalTransformDefinitions, logicalTransformNames, err := config.ReadNamedDefinitions(ctx, *transformsFile, "transform", variables)
	if err != nil {
		return result, runFailure(err)
	}
	policyPlan, err := managed.BuildPolicyPlan(ctx, logicalPolicyDefinitions, logicalPolicyNames)
	if err != nil {
		return result, runFailure(err)
	}
	policyNameMapping := make(map[string]string, len(policyPlan.LogicalToDesired))
	for logical, desired := range policyPlan.LogicalToDesired {
		policyNameMapping[logical] = desired
	}
	if err := managed.ResolvePipelinePolicyFallbacks(ctx, es, pipelineDefinitions, pipelineNames, policyNameMapping); err != nil {
		return result, runFailure(err)
	}
	pipelineDefinitions, err = managed.RewritePipelinePolicyReferences(ctx, pipelineDefinitions, pipelineNames, policyNameMapping)
	if err != nil {
		return result, runFailure(err)
	}
	policyDefinitions := policyPlan.Definitions
	policyNames := policyPlan.DesiredNames
	policyDeleteNames, err := managed.ResolvePolicyDeleteNames(ctx, es, policyPlan.LogicalNames)
	if err != nil {
		return result, runFailure(err)
	}
	transformDefinitions, transformNames, transformErr := transform.ResolveForSource(ctx, logicalTransformDefinitions, logicalTransformNames, *index)
	if transformErr != nil {
		return result, runFailure(runlog.Fail(runlog.FailureEvent(ctx), "Failed to resolve transform definitions", transformErr))
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
	enrichSelection := managed.RemapEnrichSelection(ctx, requestedEnrich, policyNameMapping)
	defaultPipeline := ""
	if effectiveSyncManaged && len(pipelineNames) > 0 {
		defaultPipeline = pipelineNames[0]
	}

	// Parse the mappings file before the existence check, -nuke and the data
	// action, so a malformed file fails the run before anything is deleted.
	preflightPlan := config.MappingPlan{}
	if action.requiresDataFile() {
		plan, planErr := config.BuildMappingPlan(ctx, *mappingsFile, variables)
		if planErr != nil {
			return result, runFailure(planErr)
		}
		preflightPlan = plan
	}

	// Settings/mappings parsing and data-file readability used to surface only
	// at create/load time, after -nuke, -delete or -flush had already run,
	// leaving the cluster half-reset. Check them here, using the create step's
	// own parsers. The scope is conservative: whether an index will be created
	// is unknown until after the destructive step. The data check covers
	// open, first non-whitespace byte and format ('[' or '{'); invalid JSON
	// later in the file can still fail inside bulk.Load. For data actions the
	// mappings file is parsed again here after BuildMappingPlan; that is
	// deliberate, so the settings and no-data sync-managed paths share this one
	// call. Do not split the settings half out of it.
	if action.requiresDataFile() || effectiveSyncManaged {
		if err := config.PreflightIndexInputs(ctx, *settingsFile, *mappingsFile, variables); err != nil {
			return result, runFailure(err)
		}
	}
	if action.requiresDataFile() {
		if err := bulk.CheckDataFile(ctx, *dataFile); err != nil {
			return result, runFailure(err)
		}
	}

	aliasTargets := []string(nil)
	exists := false
	if *aliasMode {
		aliasTargets, err = aliasops.ResolveTargets(ctx, es, *index)
		if err != nil {
			return result, runFailure(err)
		}
		if len(aliasTargets) == 0 {
			indexPresent, existsErr := indexops.Exists(ctx, es, *index)
			if err := runlog.Check(ctx, "checking if index exists", existsErr); err != nil {
				return result, runFailure(err)
			}
			if indexPresent {
				aliasTargets = []string{*index}
				warn(fmt.Sprintf("Found concrete index with alias name %q; treating it as the current write target", *index))
			}
		}
		exists = len(aliasTargets) > 0
		if exists {
			runlog.From(ctx).Info().Str("alias", *index).Strs("indices", aliasTargets).Msg("Resolved alias targets")
		} else {
			runlog.From(ctx).Info().Str("alias", *index).Msg("Alias has no current indices")
		}
	} else {
		var existsErr error
		exists, existsErr = indexops.Exists(ctx, es, *index)
		if err := runlog.Check(ctx, "checking if index exists", existsErr); err != nil {
			return result, runFailure(err)
		}
	}

	if *nuke {
		if *aliasMode {
			if len(aliasTargets) > 0 {
				runlog.From(ctx).Warn().Str("alias", *index).Strs("indices", aliasTargets).Msg("Nuke deleting alias target indices and declared managed resources")
				if err := aliasops.DeleteIndices(ctx, es, aliasTargets); err != nil {
					return result, runFailure(err)
				}
				aliasTargets = nil
			} else {
				warn(fmt.Sprintf("Alias %q does not currently resolve to an index. Nuke will still remove declared managed resources", *index))
			}
			generations, err := aliasops.ListTimestampedIndices(ctx, es, *index)
			if err != nil {
				return result, runFailure(err)
			}
			if len(generations) > 0 {
				names := make([]string, 0, len(generations))
				for _, generation := range generations {
					names = append(names, generation.Name)
				}
				runlog.From(ctx).Warn().Str("alias", *index).Strs("indices", names).Msg("Nuke deleting timestamped indices that match alias pattern")
				if err := aliasops.DeleteIndices(ctx, es, names); err != nil {
					return result, runFailure(err)
				}
			}
			exists = false
		} else {
			if exists {
				runlog.From(ctx).Warn().Str("index", *index).Msg("Nuke deleting index and declared managed resources")
				if err := indexops.Delete(ctx, es, *index); err != nil {
					return result, runFailure(err)
				}
				exists = false
			} else {
				warn(fmt.Sprintf("Index %q does not exist. Nuke will still remove declared managed resources", *index))
			}
		}

		if err := managed.DeleteResources(ctx, es, pipelineNames, policyDeleteNames, transformNames, true); err != nil {
			return result, runFailure(err)
		}
	}

	// An existing index must already match the mappings file before -flush
	// deletes its documents or -sync-managed upserts pipelines and policies
	// below. -flush and -add never write mappings, so this run cannot repair
	// a mismatch. Checking after the flush left an emptied index behind a
	// failed run. Read `exists` here: the switch below rewrites it.
	preflightExistingIndex := exists && (action == dataActionFlush || action == dataActionAdd)
	if preflightExistingIndex {
		if err := requireMappingPreflight(ctx, es, *index, preflightPlan, aliasTargets); err != nil {
			return result, runFailure(err)
		}
	}

	switch action {
	case dataActionDelete:
		if *aliasMode {
			if len(aliasTargets) > 0 {
				runlog.From(ctx).Info().Str("alias", *index).Strs("indices", aliasTargets).Msg("Alias mode delete will roll forward to a new timestamped index")
			} else {
				warn(fmt.Sprintf("Alias %q has no indices. Nothing to delete.", *index))
			}
			exists = false
		} else {
			if exists {
				runlog.From(ctx).Info().Str("index", *index).Msg("Deleting index before reloading data")
				if err := indexops.Delete(ctx, es, *index); err != nil {
					return result, runFailure(err)
				}
				exists = false
			} else {
				warn(fmt.Sprintf("Index %q does not exist. Nothing to delete.", *index))
			}
		}

		if *aliasMode {
			runlog.From(ctx).Info().Str("alias", *index).Msg("Alias mode delete keeps existing managed resources (pipelines, policies, transforms); use -nuke for destructive managed-resource cleanup")
		} else {
			if err := managed.DeleteResources(ctx, es, pipelineNames, policyDeleteNames, transformNames, false); err != nil {
				return result, runFailure(err)
			}
		}
	case dataActionFlush:
		if *aliasMode {
			if len(aliasTargets) > 0 {
				runlog.From(ctx).Info().Str("alias", *index).Strs("indices", aliasTargets).Msg("Flushing alias target indices before loading replacement data")
				for _, target := range aliasTargets {
					if err := indexops.Flush(ctx, es, target); err != nil {
						return result, runFailure(err)
					}
				}
				exists = true
			} else {
				warn(fmt.Sprintf("Alias %q has no indices. Nothing to flush.", *index))
				exists = false
			}
		} else {
			if exists {
				runlog.From(ctx).Info().Str("index", *index).Msg("Flushing existing index before loading replacement data")
				if err := indexops.Flush(ctx, es, *index); err != nil {
					return result, runFailure(err)
				}
			} else {
				warn(fmt.Sprintf("Index %q does not exist. Nothing to flush.", *index))
			}
		}
	case dataActionAdd:
		if *aliasMode {
			if len(aliasTargets) > 0 {
				runlog.From(ctx).Info().Str("alias", *index).Strs("indices", aliasTargets).Msg("Appending documents to existing alias target index")
				exists = true
			} else {
				runlog.From(ctx).Info().Str("alias", *index).Msg("Creating first timestamped index for alias before appending documents")
				exists = false
			}
		} else {
			if exists {
				runlog.From(ctx).Info().Str("index", *index).Msg("Appending documents to existing index")
			} else {
				runlog.From(ctx).Info().Str("index", *index).Msg("Creating index to append documents")
			}
		}
	}

	shouldCreateIndex := !exists && (action.requiresDataFile() || (effectiveSyncManaged && (*settingsFile != "" || *mappingsFile != "")))
	writeIndex := *index
	createdIndex := ""
	if *aliasMode && shouldCreateIndex {
		createdIndex, err = aliasops.NextAvailableIndexName(ctx, es, *index, time.Now().UTC())
		if err != nil {
			return result, runFailure(err)
		}
		writeIndex = createdIndex
		runlog.From(ctx).Info().Str("alias", *index).Str("index", createdIndex).Msg("Preparing timestamped index for alias")
	}
	result.WriteIndex = writeIndex
	result.CreatedIndex = createdIndex
	if *aliasMode {
		result.ResolvedAliasTarget = *index
	}

	if shouldCreateIndex && effectiveSyncManaged {
		if err := managed.CreatePipelines(ctx, es, pipelineDefinitions, pipelineNames); err != nil {
			return result, runFailure(err)
		}
	}

	if shouldCreateIndex {
		replicas, replicasSource := indexops.SelectReplicaCount(ctx, es, opts.Replicas)
		body, err := config.BuildCreateIndexBody(ctx, replicas, replicasSource, *settingsFile, *mappingsFile, defaultPipeline, variables)
		if err != nil {
			return result, runFailure(err)
		}
		createIndex := *index
		if *aliasMode {
			createIndex = createdIndex
		}
		if err := indexops.Create(ctx, es, createIndex, body); err != nil {
			return result, runFailure(err)
		}
		if err := indexops.WaitForIndex(ctx, es, createIndex, sleepWithContext); err != nil {
			return result, runFailure(err)
		}
		exists = true
		if *aliasMode {
			runlog.From(ctx).Info().Str("alias", *index).Str("index", createIndex).Msg("Index created for alias")
		} else {
			runlog.From(ctx).Info().Str("index", *index).Msg("Index created")
		}
	}

	deferPolicyCreationUntilAliasSwap := effectiveSyncManaged && *aliasMode && shouldCreateIndex
	transformSyncNames := make([]string, 0)
	if effectiveSyncManaged && exists {
		if !shouldCreateIndex {
			if err := managed.CreatePipelines(ctx, es, pipelineDefinitions, pipelineNames); err != nil {
				return result, runFailure(err)
			}
		}
		if len(transformNames) > 0 {
			transformSyncNames = append(transformSyncNames, transformNames...)
		}
		if !deferPolicyCreationUntilAliasSwap {
			if err := managed.CreatePolicies(ctx, es, policyDefinitions, policyNames, sleepWithContext); err != nil {
				return result, runFailure(err)
			}
			if err := managed.GarbageCollectPolicies(ctx, es, policyPlan.LogicalNames, policyPlan.DesiredSet); err != nil {
				return result, runFailure(err)
			}
		}
	}

	if action.requiresDataFile() {
		// Created-index path: the index was created above from the mappings
		// file, so verify it now. The existing-index path was verified before
		// the data action and is not checked twice.
		if !preflightExistingIndex {
			if err := requireMappingPreflight(ctx, es, writeIndex, preflightPlan, nil); err != nil {
				return result, runFailure(err)
			}
		}
		runlog.From(ctx).Info().Msg("Starting bulk insert")

		bulkProgress := emit.stage(ProgressStageBulk)
		totals, err := bulk.Load(ctx, es, bulk.Config{
			DataFile:         *dataFile,
			Index:            writeIndex,
			BatchSize:        *batchSize,
			RetryAttempts:    *bulkRetryAttempts,
			RetryBackoffBase: *bulkRetryBackoffBase,
			RetryBackoffMax:  *bulkRetryBackoffMax,
			IDField:          *idField,
		}, progress.Seams{Now: progressNow, Sleep: sleepWithContext, OnProgress: bulkProgress})
		if err != nil {
			return result, runFailure(err)
		}
		processed, succeededTotal, failedTotal, overallDuration := totals.Processed, totals.Succeeded, totals.Failed, totals.Elapsed
		runlog.From(ctx).Info().
			Int("processed", processed).
			Int("succeeded", succeededTotal).
			Int("failed", failedTotal).
			Float64("total_time", overallDuration.Seconds()).
			Msg("Bulk load completed")
		bulkProgress(progress.Snapshot{
			Resource: writeIndex,
			Done:     int64(processed),
			Total:    int64(processed),
			Elapsed:  overallDuration,
			Final:    true,
		})

		if failedTotal > 0 {
			runlog.From(ctx).Warn().
				Int("failed", failedTotal).
				Msg("Bulk load completed with failed items")
		}

		result.DocumentsProcessed = processed
		result.DocumentsSucceeded = succeededTotal
		result.DocumentsFailed = failedTotal
	}

	if *aliasMode && shouldCreateIndex {
		if err := aliasops.Update(ctx, es, *index, createdIndex); err != nil {
			return result, runFailure(err)
		}
	}
	if deferPolicyCreationUntilAliasSwap {
		if err := managed.CreatePolicies(ctx, es, policyDefinitions, policyNames, sleepWithContext); err != nil {
			return result, runFailure(err)
		}
		if err := managed.GarbageCollectPolicies(ctx, es, policyPlan.LogicalNames, policyPlan.DesiredSet); err != nil {
			return result, runFailure(err)
		}
	}
	if *aliasMode && *keepLast > 0 {
		if err := aliasops.Prune(ctx, es, *index, *keepLast); err != nil {
			return result, runFailure(err)
		}
	}

	if requestedEnrich.Enabled {
		if err := indexops.Refresh(ctx, es, writeIndex); err != nil {
			return result, runFailure(err)
		}
		enrichResult, err := enrich.Execute(ctx, es, enrichSelection, policyNames, progress.Seams{
			Now:        progressNow,
			Sleep:      sleepWithContext,
			OnProgress: emit.stage(ProgressStageEnrich),
		})
		if err != nil {
			return result, runFailure(err)
		}
		result.EnrichSelected = enrichResult.Selected
		result.EnrichMissing = enrichResult.Missing
		result.EnrichSucceeded = enrichResult.Succeeded
		result.EnrichFailed = enrichResult.Failed
	}
	if effectiveSyncManaged && len(transformSyncNames) > 0 {
		// ADR: Keep transform readiness local to this stage; see
		// adr/0002-refresh-write-index-before-starting-managed-transforms.md.
		if err := indexops.Refresh(ctx, es, writeIndex); err != nil {
			return result, runFailure(err)
		}
		if err := transform.StopBestEffort(ctx, es, transformSyncNames); err != nil {
			return result, runFailure(err)
		}
		if err := transform.CreateOrUpdate(ctx, es, transformDefinitions, transformSyncNames); err != nil {
			return result, runFailure(err)
		}
		if err := transform.Start(ctx, es, transformSyncNames); err != nil {
			return result, runFailure(err)
		}
		transformSeams := progress.Seams{Now: progressNow, Sleep: sleepWithContext, OnProgress: emit.stage(ProgressStageTransform)}
		if opts.Transforms.Wait {
			if err := transform.WaitForCheckpoint(ctx, es, transformDefinitions, transformSyncNames, opts.Transforms.WaitTimeout, transformSeams); err != nil {
				return result, runFailure(err)
			}
		} else {
			transform.ReportStarted(transformSeams, transformSyncNames)
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
	runlog.From(ctx).Info().
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

// requireMappingPreflight fails the run when index's live mappings do not
// match plan. Both Run call sites (the early existing-index check and the
// post-create check) go through this helper, so the check cannot drift between
// the two paths.
//
// The failure message must stay byte-identical: runFailure passes it to
// classifyRunErrorKind, which matches "bulk" first, so a preflight failure is
// classified as ErrBulkFailure. TestRunExistingIndexMappingMismatchFailsBeforeMutation
// pins that kind; changing it is a deliberate decision, not a rename.
//
// expected is the concrete-index set the run will mutate (aliasTargets in
// alias mode, nil otherwise, and nil for the post-create check). A non-empty
// set must equal the mappings response's key set, so a flushed target cannot
// go unverified (FIX-ALIASSET-01). With no mapping expectations the check is
// skipped entirely, including the set comparison.
func requireMappingPreflight(ctx context.Context, es *elasticsearch.Client, index string, plan config.MappingPlan, expected []string) error {
	if !plan.HasExpectations() {
		return nil
	}
	if err := indexops.VerifyMappings(ctx, es, index, plan, expected); err != nil {
		return runlog.Fail(runlog.FailureEvent(ctx).Str("index", index), "Mapping preflight failed before bulk insert", err)
	}
	return nil
}

// ─── Data Action Selection ─────────────────────────────────────────────────────

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

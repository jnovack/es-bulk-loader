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
	// ErrInvalidOptions reports an Options value Run rejects during validation,
	// or an unresolved ${NAME} in a template file, before any cluster request
	// that changes state. Run sets it explicitly, not from the operation name.
	// A malformed or unreadable settings, mappings, definitions or data file is
	// not this Kind: it is classified from its Op like any other failure (see
	// RunError), usually as ErrLoaderExecution or ErrManagedResource.
	ErrInvalidOptions = errors.New("invalid options")
	// ErrIndexOperation reports a failure whose Op mentions index, indices or
	// alias and matched none of the earlier rules (see RunError). For example,
	// "clearing index.default_pipeline" names a pipeline and is
	// ErrManagedResource.
	ErrIndexOperation = errors.New("index operation failed")
	// ErrManagedResource reports a failure whose Op mentions pipeline, policy,
	// transform or managed but not bulk or enrich. Enrich policy create, delete
	// and garbage-collection Ops name "enrich", so they are ErrEnrichExecution,
	// not this Kind.
	ErrManagedResource = errors.New("managed resource failed")
	// ErrBulkFailure reports a failure whose Op mentions bulk: a failed _bulk
	// request (transport, non-2xx after retries, unreadable response,
	// unserializable document) or a mapping preflight failure, whose Op names
	// the bulk insert. A data file that cannot be opened or decoded during the
	// load is ErrLoaderExecution. Rejected items are not errors; they are
	// counted in Result.DocumentsFailed.
	ErrBulkFailure = errors.New("bulk insert failed")
	// ErrEnrichExecution reports a run-ending failure whose Op mentions enrich:
	// listing policies before execution, and creating, deleting or
	// garbage-collecting enrich policies. A policy that fails to execute does
	// not end the run; it is counted in Result.EnrichFailed.
	ErrEnrichExecution = errors.New("enrich execution failed")
	// ErrLoaderExecution reports any other failure: an Op that matches no rule
	// (see RunError), an untyped internal error (empty Op), or a panic whose
	// value is an error (Op "panic"). A panic carrying a *RunError is
	// returned as that error; any other panic value propagates.
	// Settings and mappings file read/parse failures and settings serialization
	// failures use this Kind by design (see ErrInvalidOptions). So does failure
	// to decode mappings while building the preflight plan, including numeric
	// overflow or a section that cannot decode into an object. Template
	// preflight validation remains ErrInvalidOptions. A failed check against
	// the index's live mappings names the bulk insert and is ErrBulkFailure.
	ErrLoaderExecution = errors.New("loader execution failed")
)

// ─── Core Runtime Types ────────────────────────────────────────────────────────

// RunError is the error type Run returns for every failure. Kind is one of the
// Err sentinels and matches through errors.Is; Op names the failed operation;
// Err is the underlying cause, returned by Unwrap.
//
// Except for ErrInvalidOptions, which Run sets explicitly, Kind is chosen from
// Op by case-insensitive substring, first match wins: bulk (ErrBulkFailure),
// then enrich (ErrEnrichExecution), then pipeline, policy, transform or managed
// (ErrManagedResource), then index, indices or alias (ErrIndexOperation),
// otherwise ErrLoaderExecution.
type RunError struct {
	Kind error
	Op   string
	Err  error
}

// Error returns the composed error message used by callers.
//
// A nil receiver returns "". Otherwise the message is Op alone when there is
// no cause, "Op: cause" when both are set, and the cause's text when Op is
// empty. A value with neither Op nor cause (zero value or Kind only) returns
// Kind's text, or ErrLoaderExecution's text when Kind is nil. The fallback only
// formats: it never sets Kind, synthesizes a cause, or changes errors.Is/As
// results. Formatting must not panic on a nil cause, because callers such as
// loggers report failures through Error() directly.
func (e *RunError) Error() string {
	if e == nil {
		return ""
	}
	if e.Op == "" {
		if e.Err != nil {
			return e.Err.Error()
		}
		if e.Kind != nil {
			return e.Kind.Error()
		}
		// Falling back to a fixed text rather than a default Kind keeps a
		// zero-value message from implying a classification it never had.
		return ErrLoaderExecution.Error()
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

// Options configures one Run. Zero values select the defaults documented on
// each field. Run works on its own copy, so the caller's Options, including
// the TemplateVariables map and Enrich.Policies slice, is never modified.
type Options struct {
	// URL is the Elasticsearch base URL. Empty means http://localhost:9200.
	URL string
	// InsecureSkipVerify disables TLS certificate verification for URL.
	InsecureSkipVerify bool
	// Index is required. It names the concrete index, or the alias when AliasMode is set.
	Index string
	// SettingsFile is an optional index settings JSON file, either wrapped in
	// "settings" or a raw object. It is applied only when Run creates an index.
	// It is parsed before any destructive step when a data action or SyncManaged
	// is set.
	SettingsFile string
	// MappingsFile is an optional mappings JSON file, either wrapped in
	// "mappings" or a raw object. It is applied when Run creates an index. For
	// -add and -flush, an existing index's live mappings must match the file's
	// date_detection and root field types before any write.
	MappingsFile string
	// PipelinesFile is an optional keyed JSON file of ingest pipelines. Pipelines
	// are upserted only with SyncManaged. With SyncManaged, the first declared
	// pipeline becomes index.default_pipeline on a created index whose settings
	// do not set one.
	PipelinesFile string
	// PoliciesFile is an optional keyed JSON file of enrich policies. Policies
	// are upserted only with SyncManaged, under content-hashed managed names.
	PoliciesFile string
	// TransformsFile is an optional keyed JSON file of transforms. Only the
	// transforms whose source index is Index are used, and they are started only
	// with SyncManaged.
	TransformsFile string
	// DataFile is the data to load: a JSON array or NDJSON, detected from the
	// first non-whitespace byte. It is required for AddToIndex, FlushIndex and
	// DeleteIndex.
	DataFile string
	// BatchSize is the number of documents per bulk request. Values <= 0 mean 1000.
	BatchSize int
	// DeleteIndex (-delete) drops the index before loading, or rolls an alias
	// forward in AliasMode. It is mutually exclusive with AddToIndex and FlushIndex.
	DeleteIndex bool
	// AddToIndex (-add) appends documents without resetting the index. LoadData
	// sets it when no data action is chosen.
	AddToIndex bool
	// FlushIndex (-flush) deletes existing documents but keeps the index and its
	// managed resources, then loads DataFile.
	FlushIndex bool
	// SyncManaged upserts the declared pipelines, policies and transforms, and
	// garbage-collects unreferenced policy versions. Both run only when the
	// index or alias target exists or this run creates it. A sync-only run
	// (no data action, no Nuke) whose index or alias resolves to nothing and
	// that has neither SettingsFile nor MappingsFile fails with
	// ErrIndexOperation before any cluster write, naming the resources it
	// would have synced; supply a settings or mappings file, or a data action.
	// A Nuke run is not rejected by this rule. After Nuke, SyncManaged rebuilds
	// only when the run creates an index from SettingsFile/MappingsFile or a
	// data action with DataFile; otherwise it is a reset that recreates nothing
	// and Run warns. Alias -delete implies SyncManaged, with a warning, when it
	// is not set.
	SyncManaged bool
	// AliasMode treats Index as an alias. Creating or recreating the index makes
	// a timestamped <alias>-YYYYMMDDHHMMSS index, loads it, and then repoints the
	// alias.
	AliasMode bool
	// KeepLast keeps only the newest N timestamped generations. It prunes at the
	// end of every AliasMode run, not only a rollover. Zero keeps all of them.
	// Negative values are rejected. A non-zero value without AliasMode is
	// ignored with a warning.
	KeepLast int
	// Replicas, when non-nil, sets number_of_replicas on indices this run
	// creates, overriding the cluster-topology default (ADR 0001). nil keeps
	// the topology rule; an explicit 0 is honoured. Must be >= 0.
	Replicas *int
	// Nuke (-nuke) deletes the index, or in AliasMode the alias's target indices
	// and timestamped generations, plus the declared managed resources, before
	// the data action runs. Nuke with SyncManaged and no data action, Settings
	// or Mappings is a reset only: nothing is recreated, and Run adds a warning
	// to Result.Warnings before any request. Rebuilding needs Settings/Mappings
	// or a data action with DataFile.
	Nuke bool
	// IDField names a document field whose non-empty string value becomes the
	// bulk _id. Otherwise Elasticsearch assigns the ID.
	IDField string
	// BulkRetryAttempts controls total bulk request attempts, including the first attempt.
	// Values <= 0 use the package default.
	BulkRetryAttempts int
	// BulkRetryBackoffBase controls the first retry wait for retryable bulk failures.
	// Values <= 0 use the package default.
	BulkRetryBackoffBase time.Duration
	// BulkRetryBackoffMax caps exponential bulk retry waits. A value below
	// BulkRetryBackoffBase is raised to it.
	BulkRetryBackoffMax time.Duration
	// User and Pass enable basic auth together; both must be non-empty. Setting
	// exactly one of them fails Run with ErrInvalidOptions before any request;
	// leaving both empty allows anonymous access.
	User string
	// Pass is the basic-auth password; see User.
	Pass string
	// APIKey authenticates with an Elasticsearch API key. It cannot be combined
	// with User or Pass.
	APIKey string
	// TemplateVariables adds ${NAME} values for file templates. Precedence: this
	// map (including an INDEX override), then the run's Index for INDEX, then the
	// process environment. A name none of them resolve fails Run with
	// ErrInvalidOptions before any cluster change. Run reads the map and never
	// writes it.
	TemplateVariables map[string]string
	// Enrich selects the enrich policies to execute after the load; see EnrichOptions.
	Enrich EnrichOptions
	// Transforms controls waiting on managed transforms after they start. Its
	// zero value starts transforms without waiting.
	Transforms TransformOptions
	// Progress, when non-nil, receives ProgressEvents; see ProgressEvent for
	// the delivery contract. nil creates no events.
	Progress func(ProgressEvent)
	// Logger, when non-nil, receives all of Run's log output — including a
	// Disabled logger such as zerolog.Nop(), which silences Run. nil uses the
	// zerolog global (log.Logger), read but never modified, so concurrent
	// Runs with different loggers do not interfere. Concurrent Runs that share
	// a logger, including the nil fallback, write to the same writer, which
	// must be safe for concurrent use. With nil, do not reassign log.Logger
	// while a Run is in progress.
	Logger *zerolog.Logger
}

// Result reports what a Run did. Fields are filled as each stage completes, so
// a Run that returns an error reports only the stages that finished before the
// failure. Result is not a rollback record.
type Result struct {
	// ResolvedAliasTarget is Options.Index in AliasMode, set once the run reaches
	// the index stage, and empty otherwise. It names the alias or, when a
	// concrete index already has that name, that index (Run warns).
	ResolvedAliasTarget string
	// WriteIndex is the name the bulk load wrote to: the new timestamped index when
	// one was created, otherwise Index. In AliasMode with existing targets, that is
	// the alias, which Elasticsearch resolves to its write target.
	WriteIndex string
	// CreatedIndex is the timestamped index this run created in AliasMode. It is
	// empty when no index was created.
	CreatedIndex string
	// DocumentsProcessed is the number of documents the bulk load attempted. It is
	// zero when no data action loaded a file.
	DocumentsProcessed int
	// DocumentsSucceeded counts bulk items Elasticsearch accepted.
	DocumentsSucceeded int
	// DocumentsFailed counts bulk items Elasticsearch rejected. Item failures do
	// not make Run return an error, so callers must check this field.
	DocumentsFailed int
	// EnrichSelected lists the managed policy names Run executed.
	EnrichSelected []string
	// EnrichMissing lists requested policy names absent from a cluster that has
	// at least one enrich policy. Each is skipped with a log warning and is not
	// an error. It stays empty when the cluster has no enrich policies or the
	// enrich API is unavailable; neither case is an error, so check
	// EnrichSelected too.
	EnrichMissing []string
	// EnrichSucceeded counts enrich policies that completed.
	EnrichSucceeded int
	// EnrichFailed counts enrich policies that failed. The run continues after a
	// failed policy, so callers must check this field.
	EnrichFailed int
	// Warnings holds, in order, the messages Run routes through its internal
	// warning helper. Other Warn-level logs, such as the nuke deletion notices and
	// skipped missing policies, are logged but not listed here.
	Warnings []string
}

// dataAction is the data action chosen by -add, -flush or -delete. dataActionNone means the run loads no data file.
type dataAction string

const (
	// dataActionNone selects no data action. The run may still sync managed resources or enrich.
	dataActionNone dataAction = ""
	// dataActionAdd appends documents to the existing index or alias target (-add).
	dataActionAdd dataAction = "add"
	// dataActionFlush deletes existing documents, keeps the index, then loads replacement documents (-flush).
	dataActionFlush dataAction = "flush"
	// dataActionDelete drops the index, or rolls an alias forward, before loading (-delete).
	dataActionDelete dataAction = "delete"
)

// sleepWithContext is the sleep seam for retries and polls; tests replace it to avoid real waits.
var sleepWithContext = sleepForDuration

// progressNow is the clock for progress timing (elapsed values, heartbeats).
// It exists so tests can drive heartbeats without sleeping.
var progressNow = time.Now

// sleepForDuration waits for d or until ctx is done, returning ctx.Err() on cancellation. A non-positive d returns at once.
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

// nukeResetWarning is the Run warning for -nuke with -sync-managed and no data
// action, -settings or -mappings. %s is the quoted target phrase.
const nukeResetWarning = "-nuke with -sync-managed and no data action or -settings/-mappings is a managed-resource reset: " +
	"it deletes %s and the declared pipelines, policies and transforms for this run, and recreates nothing. " +
	"To rebuild, add -settings/-mappings or a data action with -data (for example -nuke -delete -sync-managed -data <file>)"

// Run executes one lifecycle pass for opts.Index, in the stage order listed in
// the package documentation. It normalizes zero-valued options on a private copy,
// so the caller's Options is never modified (TestRunDoesNotMutateCallerOptions).
// A nil ctx is replaced with context.Background(). The first run-ending error
// stops the run and is returned as a *RunError, with the Result filled so far.
// A bulk item rejection or a failed enrich policy is counted in the Result and
// does not stop the run.
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
	// A lone credential used to be dropped silently, connecting anonymously
	// and reporting success. Reject it here, before any I/O. The API-key
	// conflict check above stays first so its message wins for prior inputs.
	// Messages name the missing field only, never a credential value.
	if *user != "" && *pass == "" {
		return result, &RunError{Kind: ErrInvalidOptions, Op: "validating auth options", Err: fmt.Errorf("basic auth requires both -user and -pass: -pass is empty")}
	}
	if *user == "" && *pass != "" {
		return result, &RunError{Kind: ErrInvalidOptions, Op: "validating auth options", Err: fmt.Errorf("basic auth requires both -user and -pass: -user is empty")}
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
	// -nuke with sync but no data action and no -settings/-mappings creates no
	// index afterwards (see shouldCreateIndex), so the managed-upsert gate
	// (effectiveSyncManaged && exists) is false and nothing is rebuilt. Docs
	// once implied sync rebuilds; a silent reset looks like success. Exact ""
	// comparisons mirror shouldCreateIndex: do not trim. Warn here, before any
	// cluster request, and keep the reset itself unchanged (decided 2026-11-01).
	if *nuke && effectiveSyncManaged && action == dataActionNone && *settingsFile == "" && *mappingsFile == "" {
		target := fmt.Sprintf("index %q", *index)
		if *aliasMode {
			target = fmt.Sprintf("alias %q's target indices and timestamped generations", *index)
		}
		warn(fmt.Sprintf(nukeResetWarning, target))
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

	// FIX-SYNCNOOP-01: a sync-only run with no target and no settings/mappings
	// to create one used to skip every managed upsert and report success. Fail
	// here, before any write (create, upsert, nuke, prune, enrich), so a typo
	// or an uncreated alias cannot look like a completed sync. Nuke runs are a
	// separate reset-only case and are excluded.
	if effectiveSyncManaged && action == dataActionNone && !*nuke && !exists && *settingsFile == "" && *mappingsFile == "" {
		return result, &RunError{
			Kind: ErrIndexOperation,
			Op:   "checking index for sync-only run",
			Err:  syncTargetMissingError(*aliasMode, *index, pipelineNames, policyPlan.LogicalNames, transformNames),
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

// SyncManaged runs Run with every data action and Nuke cleared and SyncManaged
// set. It loads no documents. It creates the index only when it is missing and
// settings or mappings are supplied; a missing index or alias without either
// fails with ErrIndexOperation before any write (see Options.SyncManaged). It
// upserts managed resources and garbage-collects unreferenced policy versions
// when the index exists or is created. With AliasMode and KeepLast > 0 it
// still prunes older generations after a successful sync.
func SyncManaged(ctx context.Context, opts Options) (Result, error) {
	opts.AddToIndex = false
	opts.FlushIndex = false
	opts.DeleteIndex = false
	opts.Nuke = false
	opts.SyncManaged = true
	return Run(ctx, opts)
}

// LoadData runs Run and selects -add when no data action is set. Every other option, including SyncManaged and Nuke, passes through unchanged.
func LoadData(ctx context.Context, opts Options) (Result, error) {
	if !opts.AddToIndex && !opts.FlushIndex && !opts.DeleteIndex {
		opts.AddToIndex = true
	}
	return Run(ctx, opts)
}

// ExecuteEnrich runs Run with every data action and Nuke cleared, so it loads
// no documents. When Enrich.Enabled is false it defaults to all policies only
// if Policies is empty and Raw is blank; otherwise it sets Enabled and keeps
// the supplied Policies, All and Raw, so a Policies or Raw request needs no
// Enabled. A selection with Enabled already true is left unchanged. The usual
// EnrichOptions precedence applies: a non-empty Policies wins, then All, then
// Raw. SyncManaged passes through unchanged, so when set it still upserts
// managed resources and garbage-collects policy versions (see
// Options.SyncManaged, including its failure when the target is missing and no
// settings or mappings can create it), and AliasMode with KeepLast > 0 still
// prunes older generations.
func ExecuteEnrich(ctx context.Context, opts Options) (Result, error) {
	opts.AddToIndex = false
	opts.FlushIndex = false
	opts.DeleteIndex = false
	opts.Nuke = false
	if !opts.Enrich.Enabled {
		// Replacing an explicit request with all policies would rebuild
		// unrequested .enrich-* indices. Enabled must be set for Raw too:
		// enrichFromOptions clears Raw while disabled, and an enrich-only Run
		// would then fail action validation.
		if len(opts.Enrich.Policies) == 0 && strings.TrimSpace(opts.Enrich.Raw) == "" {
			opts.Enrich = EnrichOptions{Enabled: true, All: true}
		} else {
			opts.Enrich.Enabled = true
		}
	}
	return Run(ctx, opts)
}

// ─── Managed Asset and Mapping Preflight Helpers ──────────────────────────────

// logEffectiveManagedAssets logs, at Info, the settings, mappings, pipelines, policies and transforms files the run resolved, with declared and selected counts. It runs before any destructive step, so the log shows what the run intends to apply.
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

// syncTargetMissingError builds the cause for a sync-only run whose target is
// absent and which has no settings or mappings to create it. The names stay in
// the cause, not the Op, so resource names cannot change the error Kind.
func syncTargetMissingError(aliasMode bool, index string, pipelines, policies, transforms []string) error {
	target := fmt.Sprintf("index %q does not exist", index)
	if aliasMode {
		target = fmt.Sprintf("alias %q resolves to no index and no concrete index has that name", index)
	}
	return fmt.Errorf("-sync-managed without a data action cannot sync because %s; nothing was synced (%s); "+
		"supply -settings or -mappings so the index can be created, or choose a data action "+
		"(-add, -flush or -delete, which also requires -data)",
		target, managed.DescribeDeclared(pipelines, policies, transforms))
}

// displayManagedAsset returns the trimmed file path for logs, or "none" when the option is unset.
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

// requiresDataFile reports whether the action loads documents from Options.DataFile. dataActionNone does not.
func (a dataAction) requiresDataFile() bool {
	return a != dataActionNone
}

// selectedDataAction maps the -add, -flush and -delete flags to one dataAction. Setting more than one is an error; setting none selects dataActionNone.
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

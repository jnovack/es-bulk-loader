package observe

import "context"

// Resource kinds a Notice or Event names. Each value is the public
// loader.ResourceKind<X> twin of Kind<X>.
const (
	KindIndex     = "index"
	KindAlias     = "alias"
	KindPipeline  = "pipeline"
	KindPolicy    = "policy"
	KindTransform = "transform"
)

// Notice codes, one per actionable condition. Each value is the public
// loader.Notice<X> twin of Code<X>.
const (
	CodeKeepLastIgnored          = "keep_last_ignored"
	CodeSyncManagedAssumed       = "sync_managed_assumed"
	CodeKeepLastMissing          = "keep_last_missing"
	CodeTransformsIgnored        = "transforms_ignored"
	CodeNukeResetOnly            = "nuke_reset_only"
	CodeAliasIsConcreteIndex     = "alias_is_concrete_index"
	CodeNukeTargetMissing        = "nuke_target_missing"
	CodeDeleteTargetMissing      = "delete_target_missing"
	CodeFlushTargetMissing       = "flush_target_missing"
	CodeTransformWaitIgnored     = "transform_wait_ignored"
	CodeEnrichPolicyMissing      = "enrich_policy_missing"
	CodeEnrichNoPolicies         = "enrich_no_policies"
	CodeEnrichNoMatch            = "enrich_no_match"
	CodeEnrichAPIUnsupported     = "enrich_api_unsupported"
	CodePolicyGCFailed           = "policy_gc_failed"
	CodeReplicasDefaulted        = "replicas_defaulted"
	CodePipelinePolicyUnresolved = "pipeline_policy_unresolved"
)

// Notice is one actionable condition. Code is a Code* constant; Kind is a
// Kind* constant or "" when no single resource applies, and Name is then "".
type Notice struct {
	Code    string
	Message string
	Kind    string
	Name    string
}

// Event is one cluster change. Action names what happened; Kind and Name name
// the resource; Alias and Logical are set only where the change involves an
// alias or a logical (unhashed) name.
type Event struct {
	Action  string
	Kind    string
	Name    string
	Alias   string
	Logical string
}

// Sink receives one Run's notices and events. Either callback may be nil.
type Sink struct {
	OnNotice func(Notice)
	OnEvent  func(Event)
}

// sinkKey is the private context key for the Run's *Sink.
type sinkKey struct{}

// WithSink returns a child of ctx carrying sink; see the package doc for
// propagation and nil behavior. A nil ctx means context.Background().
func WithSink(ctx context.Context, sink *Sink) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, sinkKey{}, sink)
}

// from returns the sink ctx carries, or nil. A stored typed-nil *Sink is
// returned as nil, which is what disables delivery below WithSink(ctx, nil).
func from(ctx context.Context) *Sink {
	if ctx == nil {
		return nil
	}
	sink, _ := ctx.Value(sinkKey{}).(*Sink)
	return sink
}

// Notify delivers n to the sink's OnNotice inline, or does nothing.
func Notify(ctx context.Context, n Notice) {
	if sink := from(ctx); sink != nil && sink.OnNotice != nil {
		sink.OnNotice(n)
	}
}

// Record delivers e to the sink's OnEvent inline, or does nothing.
func Record(ctx context.Context, e Event) {
	if sink := from(ctx); sink != nil && sink.OnEvent != nil {
		sink.OnEvent(e)
	}
}

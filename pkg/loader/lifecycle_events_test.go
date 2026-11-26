package loader

// Phase 5 (loader-observability) guards for Options.Lifecycle: the exact
// ordered event sequences of each lifecycle mode, the prefix that survives a
// run-ending failure, the single shared sink, the Progress panic rule,
// isolation of concurrent Runs and sanitization of every event string. They
// drive Run with a Nop logger and assert on events, Result, the error and the
// request trace only.

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"
)

// eventLog collects the events of one Run. Delivery is synchronous on Run's
// goroutine, so the mutex only protects the test's later read.
type eventLog struct {
	mu     sync.Mutex
	events []LifecycleEvent
}

func (l *eventLog) record(e LifecycleEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) all() []LifecycleEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events)
}

func lev(action LifecycleAction, kind ResourceKind, name string) LifecycleEvent {
	return LifecycleEvent{Action: action, ResourceKind: kind, Name: name}
}

func requireLifecycle(t *testing.T, got, want []LifecycleEvent) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("events:\n got %+v\nwant %+v", got, want)
	}
}

// runLifecycle runs opts against url with a recording Lifecycle callback.
func runLifecycle(t *testing.T, url string, opts Options) (Result, error, []LifecycleEvent) {
	t.Helper()
	var log eventLog
	opts.URL = url
	opts.Lifecycle = log.record
	nopLogger(&opts)
	result, err := Run(t.Context(), opts)
	return result, err, log.all()
}

// rolloverStub serves an alias rollover for "cards": the alias points at
// cards-20260101000000, which a -keep-last 1 prune deletes once the new
// generation exists. newRolloverStub's aliasUpdate handler and invisible flag
// select the failure.
type rolloverStub struct {
	*esstub.Stub
	mu      sync.Mutex
	created string
}

func (g *rolloverStub) createdName() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.created
}

func newRolloverStub(t *testing.T, aliasUpdate http.HandlerFunc, invisible bool) *rolloverStub {
	t.Helper()
	g := &rolloverStub{}
	g.Stub = newRouteStub(t, map[string]http.HandlerFunc{
		"GET /_alias/cards":         esstub.JSON(http.StatusOK, `{"cards-20260101000000":{"aliases":{"cards":{}}}}`),
		"GET /_nodes":               esstub.JSON(http.StatusOK, nodesBody),
		"POST /_bulk":               esstub.JSON(http.StatusOK, bulkOKBody("cards")),
		"POST /_aliases":            aliasUpdate,
		"PUT /_ingest/pipeline/{p}": replyAck,
		"GET /cards-*": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprintf(w, `{"cards-20260101000000":{},%q:{}}`, g.createdName())
		},
		"/{name}": func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPut:
				g.mu.Lock()
				g.created = r.PathValue("name")
				g.mu.Unlock()
				replyAck(w, r)
			case http.MethodHead:
				existsReply(w, !invisible && r.PathValue("name") == g.createdName())
			case http.MethodDelete:
				replyAck(w, r)
			default:
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
				w.WriteHeader(http.StatusInternalServerError)
			}
		},
	})
	return g
}

// TestRunLifecycleEventSequences pins the exact ordered events of each mode.
func TestRunLifecycleEventSequences(t *testing.T) {
	t.Parallel()

	polName := managedPolicyName(t, "pol", stopPolicyBody)
	declared := func(t *testing.T, opts Options) Options {
		return syncOnlyDeclared(t, opts, stopPipelines, syncOnlyPolicies, nukeResetTx)
	}
	resolved := []LifecycleEvent{
		lev(LifecycleActionResolved, ResourceKindPipeline, "p1"),
		lev(LifecycleActionResolved, ResourceKindPipeline, "p2"),
		{Action: LifecycleActionResolved, ResourceKind: ResourceKindPolicy, Name: polName, Logical: "pol"},
		lev(LifecycleActionResolved, ResourceKindTransform, "tx"),
	}
	deletes := []LifecycleEvent{
		lev(LifecycleActionDeleted, ResourceKindPipeline, "p1"),
		lev(LifecycleActionDeleted, ResourceKindPipeline, "p2"),
		lev(LifecycleActionDeleted, ResourceKindPolicy, polName),
		lev(LifecycleActionDeleted, ResourceKindTransform, "tx"),
	}

	tests := []struct {
		name  string
		stub  func(t *testing.T) (*esstub.Stub, func() string) // second value: the generated index name, if any
		opts  func(t *testing.T) Options
		check func(t *testing.T, created string) []LifecycleEvent
	}{
		{
			name: "-delete",
			stub: func(t *testing.T) (*esstub.Stub, func() string) {
				return newLifecycleStub(t, lifecycleConfig{index: "cards", indexExists: true}).Stub, nil
			},
			opts: func(t *testing.T) Options {
				return Options{Index: "cards", DataFile: writeBulkDataFixture(t), DeleteIndex: true}
			},
			check: func(*testing.T, string) []LifecycleEvent {
				return []LifecycleEvent{lev(LifecycleActionDeleted, ResourceKindIndex, "cards"), lev(LifecycleActionCreated, ResourceKindIndex, "cards")}
			},
		},
		{
			name: "-flush",
			stub: func(t *testing.T) (*esstub.Stub, func() string) {
				return newLifecycleStub(t, lifecycleConfig{index: "cards", indexExists: true}).Stub, nil
			},
			opts: func(t *testing.T) Options {
				return Options{Index: "cards", DataFile: writeBulkDataFixture(t), FlushIndex: true}
			},
			check: func(*testing.T, string) []LifecycleEvent {
				return []LifecycleEvent{lev(LifecycleActionFlushed, ResourceKindIndex, "cards")}
			},
		},
		{
			name: "-nuke -sync-managed",
			stub: func(t *testing.T) (*esstub.Stub, func() string) { return nukeResetStub(t, false, polName), nil },
			opts: func(t *testing.T) Options {
				return declared(t, Options{Index: "cards", SyncManaged: true, Nuke: true})
			},
			check: func(*testing.T, string) []LifecycleEvent {
				return slices.Concat(resolved, []LifecycleEvent{lev(LifecycleActionDeleted, ResourceKindIndex, "cards")}, deletes)
			},
		},
		{
			// Recovery: p1 is the default pipeline of another index, so the first
			// delete is refused, the index's default_pipeline is cleared and the
			// retry succeeds. Each resource reports as soon as it is done, so the
			// settings change falls between the index delete and p1's own delete.
			name: "-nuke -sync-managed recovers a default_pipeline block",
			stub: func(t *testing.T) (*esstub.Stub, func() string) {
				return nukeResetStub(t, false, polName, map[string]http.HandlerFunc{
					"DELETE /_ingest/pipeline/p1": sequenced(
						esstub.JSON(http.StatusBadRequest, `{"error":{"reason":"pipeline [p1] cannot be deleted because it is the default pipeline for 1 index(es) including [other-index]"}}`),
						replyAck),
					"GET /_settings/*":           esstub.JSON(http.StatusOK, `{"other-index":{"settings":{"index":{"default_pipeline":"p1"}}}}`),
					"PUT /other-index/_settings": replyAck,
				}), nil
			},
			opts: func(t *testing.T) Options {
				return declared(t, Options{Index: "cards", SyncManaged: true, Nuke: true})
			},
			check: func(*testing.T, string) []LifecycleEvent {
				return slices.Concat(resolved, []LifecycleEvent{
					lev(LifecycleActionDeleted, ResourceKindIndex, "cards"),
					lev(LifecycleActionSettingsUpdated, ResourceKindIndex, "other-index"),
				}, deletes)
			},
		},
		{
			name: "-nuke -sync-managed -alias deletes each generation once, without an Alias",
			stub: func(t *testing.T) (*esstub.Stub, func() string) { return nukeResetStub(t, true, polName), nil },
			opts: func(t *testing.T) Options {
				return declared(t, Options{Index: "cards", AliasMode: true, SyncManaged: true, Nuke: true})
			},
			check: func(*testing.T, string) []LifecycleEvent {
				return slices.Concat(resolved, []LifecycleEvent{
					lev(LifecycleActionDeleted, ResourceKindIndex, "cards-a"),
					lev(LifecycleActionDeleted, ResourceKindIndex, "cards-20260101000000"),
				}, deletes)
			},
		},
		{
			name: "-delete -alias -sync-managed -keep-last 1",
			stub: func(t *testing.T) (*esstub.Stub, func() string) {
				g := newRolloverStub(t, replyAck, false)
				return g.Stub, g.createdName
			},
			opts: func(t *testing.T) Options {
				return Options{
					Index: "cards", DataFile: writeBulkDataFixture(t), DeleteIndex: true, AliasMode: true, SyncManaged: true, KeepLast: 1,
					PipelinesFile: writeInputFile(t, "p.json", `{"p1":{"processors":[]}}`),
				}
			},
			check: func(_ *testing.T, created string) []LifecycleEvent {
				return []LifecycleEvent{
					lev(LifecycleActionResolved, ResourceKindPipeline, "p1"),
					lev(LifecycleActionUpserted, ResourceKindPipeline, "p1"),
					lev(LifecycleActionCreated, ResourceKindIndex, created),
					{Action: LifecycleActionRepointed, ResourceKind: ResourceKindAlias, Name: created, Alias: "cards"},
					lev(LifecycleActionDeleted, ResourceKindIndex, "cards-20260101000000"),
				}
			},
		},
		{
			name: "-sync-managed with a transform",
			stub: func(t *testing.T) (*esstub.Stub, func() string) {
				return newLifecycleStub(t, lifecycleConfig{index: "collection", transforms: []string{"collection-to-binder"}}).Stub, nil
			},
			opts: func(t *testing.T) Options {
				data, transforms := writeCollectionFixtures(t, "data.json", `[{"id":"1"}]`)
				return Options{Index: "collection", DataFile: data, AddToIndex: true, SyncManaged: true, TransformsFile: transforms}
			},
			check: func(*testing.T, string) []LifecycleEvent {
				return []LifecycleEvent{
					lev(LifecycleActionResolved, ResourceKindTransform, "collection-to-binder"),
					lev(LifecycleActionCreated, ResourceKindIndex, "collection"),
					lev(LifecycleActionRefreshed, ResourceKindIndex, "collection"),
					lev(LifecycleActionUpserted, ResourceKindTransform, "collection-to-binder"),
					lev(LifecycleActionStarted, ResourceKindTransform, "collection-to-binder"),
				}
			},
		},
		{
			name: "-enrich",
			stub: func(t *testing.T) (*esstub.Stub, func() string) {
				return newLifecycleStub(t, lifecycleConfig{index: "collection", indexExists: true, policies: []string{"policy-a"}}).Stub, nil
			},
			opts: func(t *testing.T) Options {
				data, _ := writeCollectionFixtures(t, "data.json", `[{"id":"1"}]`)
				return Options{Index: "collection", DataFile: data, AddToIndex: true, Enrich: EnrichOptions{Enabled: true, All: true}}
			},
			check: func(*testing.T, string) []LifecycleEvent {
				return []LifecycleEvent{lev(LifecycleActionRefreshed, ResourceKindIndex, "collection")}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stub, created := tt.stub(t)
			result, err, events := runLifecycle(t, stub.URL(), tt.opts(t))
			if err != nil {
				t.Fatalf("Run error = %v", err)
			}
			createdName := ""
			if created != nil {
				createdName = created()
				if createdName == "" || createdName != result.CreatedIndex {
					t.Fatalf("generated index = %q, Result.CreatedIndex = %q", createdName, result.CreatedIndex)
				}
			}
			requireLifecycle(t, events, tt.check(t, createdName))
		})
	}
}

// TestRunLifecycleEventsSurviveLaterFailure pins the exact successful prefix
// when the Run ends mid-sequence: no event for the failed action or any later
// one, and an acknowledged create stays reported when its visibility check
// fails.
func TestRunLifecycleEventsSurviveLaterFailure(t *testing.T) {
	t.Run("batch: first pipeline succeeds, second fails", func(t *testing.T) {
		t.Parallel()
		stub := newRouteStub(t, map[string]http.HandlerFunc{
			"PUT /_ingest/pipeline/p1": replyAck,
			"PUT /_ingest/pipeline/p2": replyFail,
			"HEAD /cards":              replyHeadOK,
		})
		_, err, events := runLifecycle(t, stub.URL(), Options{
			Index: "cards", AddToIndex: true, SyncManaged: true, DataFile: writeBulkDataFixture(t),
			PipelinesFile: writeInputFile(t, "p.json", stopPipelines),
		})
		if err == nil {
			t.Fatal("Run error = nil, want the second pipeline's failure")
		}
		assertRunError(t, err, ErrManagedResource, "Failed to create pipeline")
		requireLifecycle(t, events, []LifecycleEvent{
			lev(LifecycleActionResolved, ResourceKindPipeline, "p1"),
			lev(LifecycleActionResolved, ResourceKindPipeline, "p2"),
			lev(LifecycleActionUpserted, ResourceKindPipeline, "p1"),
		})
	})

	t.Run("alias update fails after create", func(t *testing.T) {
		t.Parallel()
		g := newRolloverStub(t, replyFail, false)
		_, err, events := runLifecycle(t, g.URL(), Options{
			Index: "cards", DataFile: writeBulkDataFixture(t), DeleteIndex: true, AliasMode: true, KeepLast: 1,
		})
		if err == nil {
			t.Fatal("Run error = nil, want the alias update failure")
		}
		assertRunError(t, err, ErrIndexOperation, "Failed to update alias")
		requireLifecycle(t, events, []LifecycleEvent{lev(LifecycleActionCreated, ResourceKindIndex, g.createdName())})
	})

	t.Run("acknowledged create followed by a failed visibility check", func(t *testing.T) {
		// Serial: the fake clock stands in for the visibility polling sleeps.
		installFakeClock(t, time.Second)
		g := newRolloverStub(t, replyAck, true)
		_, err, events := runLifecycle(t, g.URL(), Options{
			Index: "cards", DataFile: writeBulkDataFixture(t), DeleteIndex: true, AliasMode: true,
		})
		if err == nil {
			t.Fatal("Run error = nil, want the visibility failure")
		}
		assertRunError(t, err, ErrIndexOperation, "Index create was acknowledged but the index did not become visible")
		requireLifecycle(t, events, []LifecycleEvent{lev(LifecycleActionCreated, ResourceKindIndex, g.createdName())})
	})

	t.Run("failed delete reports nothing for the failed action or later ones", func(t *testing.T) {
		t.Parallel()
		polName := managedPolicyName(t, "pol", stopPolicyBody)
		stub := nukeResetStub(t, false, polName,
			map[string]http.HandlerFunc{"DELETE /cards": replyFail})
		_, err, events := runLifecycle(t, stub.URL(), syncOnlyDeclared(t, Options{Index: "cards", SyncManaged: true, Nuke: true},
			stopPipelines, syncOnlyPolicies, nukeResetTx))
		if err == nil {
			t.Fatal("Run error = nil, want the index delete failure")
		}
		assertRunError(t, err, ErrIndexOperation, "Failed to delete index")
		requireLifecycle(t, events, []LifecycleEvent{
			lev(LifecycleActionResolved, ResourceKindPipeline, "p1"),
			lev(LifecycleActionResolved, ResourceKindPipeline, "p2"),
			{Action: LifecycleActionResolved, ResourceKind: ResourceKindPolicy, Name: polName, Logical: "pol"},
			lev(LifecycleActionResolved, ResourceKindTransform, "tx"),
		})
	})
}

// TestRunLifecycleGCFailureKeepsLaterEvents guards that a best-effort policy GC
// failure is a notice with no deleted event, while a later candidate's
// successful delete is still reported after it, in listing order.
func TestRunLifecycleGCFailureKeepsLaterEvents(t *testing.T) {
	t.Parallel()

	polName := managedPolicyName(t, "pol", stopPolicyBody)
	// Listed in this order: the first old version's delete fails, the second
	// acks and must still produce its deleted event.
	failedOld, ackedOld := "pol-aaaaaa", "pol-bbbbbb"
	if polName == failedOld || polName == ackedOld {
		t.Fatalf("hashed policy name %q collides with a fixed old version", polName)
	}
	stub := newRouteStub(t, policyListRoutes(esstub.JSON(http.StatusOK,
		`{"policies":[{"config":{"match":{"name":"`+polName+`"}}},{"config":{"match":{"name":"`+failedOld+`"}}},{"config":{"match":{"name":"`+ackedOld+`"}}}]}`)),
		map[string]http.HandlerFunc{
			"HEAD /cards":                         replyHeadOK,
			"PUT /_enrich/policy/" + polName:      replyAck,
			"GET /_ingest/pipeline":               esstub.JSON(http.StatusOK, `{}`),
			"DELETE /_enrich/policy/" + failedOld: replyFail,
			"DELETE /_enrich/policy/" + ackedOld:  replyAck,
		})
	result, err, events := runLifecycle(t, stub.URL(), Options{
		Index: "cards", SyncManaged: true, PoliciesFile: writeInputFile(t, "policies.json", syncOnlyPolicies),
	})
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	requireLifecycle(t, events, []LifecycleEvent{
		{Action: LifecycleActionResolved, ResourceKind: ResourceKindPolicy, Name: polName, Logical: "pol"},
		lev(LifecycleActionUpserted, ResourceKindPolicy, polName),
		lev(LifecycleActionDeleted, ResourceKindPolicy, ackedOld),
	})
	if len(result.Notices) != 1 || result.Notices[0].Code != NoticePolicyGCFailed || result.Notices[0].Name != failedOld {
		t.Fatalf("Notices = %+v, want one policy_gc_failed", result.Notices)
	}
}

// TestRunLifecycleResolvedOrder pins the resolved events: pipelines, then
// policies (hashed name plus logical key), then only the transforms selected
// for this source index, each in file order (not sorted); empty declarations
// report nothing; SyncManaged false still reports names and writes nothing; and a
// later input-preflight failure follows them with no cluster write.
func TestRunLifecycleResolvedOrder(t *testing.T) {
	t.Parallel()

	twoSources := `{"tx-cards":{"source_index":"cards","body":{"source":{"index":"cards"},"dest":{"index":"d"},"pivot":{}}},` +
		`"tx-other":{"source_index":"other","body":{"source":{"index":"other"},"dest":{"index":"d"},"pivot":{}}}}`

	t.Run("transforms selected for this source only", func(t *testing.T) {
		t.Parallel()
		stub := newRouteStub(t, map[string]http.HandlerFunc{"HEAD /cards": replyHeadOK, "POST /_bulk": esstub.JSON(http.StatusOK, bulkOKBody("cards"))})
		_, err, events := runLifecycle(t, stub.URL(), Options{Index: "cards", AddToIndex: true, DataFile: writeBulkDataFixture(t), TransformsFile: writeInputFile(t, "t.json", twoSources)})
		if err != nil {
			t.Fatalf("Run error = %v", err)
		}
		requireLifecycle(t, events, []LifecycleEvent{lev(LifecycleActionResolved, ResourceKindTransform, "tx-cards")})
	})

	t.Run("several resources per group keep file order, not sorted order", func(t *testing.T) {
		t.Parallel()
		// Declared out of lexical order in every group: a sort, or iteration of
		// a definitions map, would reorder them. Each policy's hashed Name must
		// stay paired with its own Logical key.
		pipelines := `{"p2":{"processors":[]},"p1":{"processors":[]}}`
		policies := `{"zeta":` + stopPolicyBody + `,"alpha":` + stopPolicyBody + `}`
		cardsTx := func(source string) string {
			return `"source_index":"` + source + `","body":{"source":{"index":"` + source + `"},"dest":{"index":"d"},"pivot":{}}`
		}
		transforms := `{"tx-b":{` + cardsTx("cards") + `},"tx-x":{` + cardsTx("other") + `},"tx-a":{` + cardsTx("cards") + `}}`
		zeta, alpha := managedPolicyName(t, "zeta", stopPolicyBody), managedPolicyName(t, "alpha", stopPolicyBody)
		stub := inputPreflightStub(t)
		for pattern, handler := range policyListRoutes(esstub.JSON(http.StatusOK, `{"policies":[]}`)) {
			stub.Handle(pattern, handler)
		}
		_, err, events := runLifecycle(t, stub.URL(), syncOnlyDeclared(t, Options{
			Index: "cards", AddToIndex: true, DataFile: writeInputFile(t, "bad.json", `{"not":"an array"`),
		}, pipelines, policies, transforms))
		if err == nil {
			t.Fatal("Run error = nil, want the data preflight failure")
		}
		requireLifecycle(t, events, []LifecycleEvent{
			lev(LifecycleActionResolved, ResourceKindPipeline, "p2"),
			lev(LifecycleActionResolved, ResourceKindPipeline, "p1"),
			{Action: LifecycleActionResolved, ResourceKind: ResourceKindPolicy, Name: zeta, Logical: "zeta"},
			{Action: LifecycleActionResolved, ResourceKind: ResourceKindPolicy, Name: alpha, Logical: "alpha"},
			lev(LifecycleActionResolved, ResourceKindTransform, "tx-b"),
			lev(LifecycleActionResolved, ResourceKindTransform, "tx-a"),
		})
	})

	t.Run("empty declarations report nothing", func(t *testing.T) {
		t.Parallel()
		stub := newRouteStub(t, map[string]http.HandlerFunc{"HEAD /cards": replyHeadOK, "POST /_bulk": esstub.JSON(http.StatusOK, bulkOKBody("cards"))})
		_, err, events := runLifecycle(t, stub.URL(), Options{Index: "cards", AddToIndex: true, DataFile: writeBulkDataFixture(t)})
		if err != nil || len(events) != 0 {
			t.Fatalf("err = %v, events = %+v, want none", err, events)
		}
	})

	t.Run("SyncManaged false reports names, writes nothing, and a later preflight failure keeps them", func(t *testing.T) {
		t.Parallel()
		stub := inputPreflightStub(t)
		_, err, events := runLifecycle(t, stub.URL(), syncOnlyDeclared(t, Options{
			Index: "cards", AddToIndex: true, DataFile: writeInputFile(t, "bad.json", `{"not":"an array"`),
		}, stopPipelines, "", ""))
		if err == nil {
			t.Fatal("Run error = nil, want the data preflight failure")
		}
		requireLifecycle(t, events, []LifecycleEvent{
			lev(LifecycleActionResolved, ResourceKindPipeline, "p1"),
			lev(LifecycleActionResolved, ResourceKindPipeline, "p2"),
		})
		for _, r := range stub.Requests() {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				t.Fatalf("request %s %s: a write reached the cluster before the preflight failure", r.Method, r.Path)
			}
		}
	})
}

// TestRunLifecycleNilCallbackAndSharedSink guards that nil Lifecycle creates
// no delivery (not even to an inherited event sink), that Run behaves
// identically with and without a callback, and that notices still reach
// Result.Notices when Lifecycle is set (one shared sink).
func TestRunLifecycleNilCallbackAndSharedSink(t *testing.T) {
	t.Parallel()

	t.Run("withRunSink with nil Lifecycle delivers no event", func(t *testing.T) {
		t.Parallel()
		var inherited atomic.Int32
		parent := observe.WithSink(t.Context(), &observe.Sink{OnEvent: func(observe.Event) { inherited.Add(1) }})
		var result Result
		ctx := withRunSink(parent, &result, nil)
		observe.Record(ctx, observe.Event{Action: observe.ActionDeleted, Kind: observe.KindIndex, Name: "x"})
		if inherited.Load() != 0 {
			t.Fatalf("inherited event sink received %d events, want 0", inherited.Load())
		}
	})

	t.Run("with and without a callback Run is identical", func(t *testing.T) {
		t.Parallel()
		run := func(withCallback bool) (Result, []string, []LifecycleEvent) {
			// The GET /_nodes failure raises a replicas_defaulted notice, so the
			// Notices comparison below is not vacuous.
			stub := newRouteStub(t, map[string]http.HandlerFunc{
				"HEAD /cards": sequenced(replyHeadMissing, replyHeadOK),
				"GET /_nodes": replyFail,
				"PUT /cards":  replyAck,
				"POST /_bulk": esstub.JSON(http.StatusOK, bulkOKBody("cards")),
			})
			opts := Options{URL: stub.URL(), Index: "cards", DataFile: writeBulkDataFixture(t), AddToIndex: true}
			nopLogger(&opts)
			var log eventLog
			if withCallback {
				opts.Lifecycle = log.record
			}
			result, err := Run(t.Context(), opts)
			if err != nil {
				t.Fatalf("Run error = %v", err)
			}
			return result, requestLabels(stub), log.all()
		}
		bare, bareReqs, bareEvents := run(false)
		with, withReqs, withEvents := run(true)
		if len(bare.Notices) == 0 {
			t.Fatal("bare run raised no notices; the comparison would be vacuous")
		}
		if len(bareEvents) != 0 || len(withEvents) == 0 {
			t.Fatalf("events without/with callback = %d/%d", len(bareEvents), len(withEvents))
		}
		if !slices.Equal(bareReqs, withReqs) || fmt.Sprint(bare) != fmt.Sprint(with) {
			t.Fatalf("Run differs with a callback:\n requests %q vs %q\n result %+v vs %+v", bareReqs, withReqs, bare, with)
		}
	})

	t.Run("notices still reach Result.Notices when Lifecycle is set", func(t *testing.T) {
		t.Parallel()
		stub := newRouteStub(t, map[string]http.HandlerFunc{
			"HEAD /cards": sequenced(replyHeadMissing, replyHeadOK),
			"GET /_nodes": replyFail,
			"PUT /cards":  replyAck,
			"POST /_bulk": esstub.JSON(http.StatusOK, bulkOKBody("cards")),
		})
		result, err, events := runLifecycle(t, stub.URL(), Options{Index: "cards", DataFile: writeBulkDataFixture(t), AddToIndex: true})
		if err != nil {
			t.Fatalf("Run error = %v", err)
		}
		requireLifecycle(t, events, []LifecycleEvent{lev(LifecycleActionCreated, ResourceKindIndex, "cards")})
		if len(result.Notices) != 1 || result.Notices[0].Code != NoticeReplicasDefaulted {
			t.Fatalf("Notices = %+v, want one replicas_defaulted", result.Notices)
		}
	})
}

// TestRunLifecycleCallbackPanicContract follows Progress's rule for the
// Lifecycle callback and proves no later action runs after the panic.
func TestRunLifecycleCallbackPanicContract(t *testing.T) {
	t.Parallel()

	boom := errors.New("lifecycle callback exploded")
	callerRunErr := &RunError{Kind: ErrEnrichExecution, Op: "caller op", Err: boom}
	tests := []struct {
		name      string
		value     any
		wantSame  *RunError
		propagate bool
	}{
		{name: "error panic becomes the Run error", value: boom},
		{name: "RunError panic returned unchanged", value: callerRunErr, wantSame: callerRunErr},
		{name: "non-error panic propagates", value: "string panic", propagate: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stub := newLifecycleStub(t, lifecycleConfig{index: "cards", indexExists: true})
			var calls atomic.Int32
			opts := Options{
				URL: stub.URL(), Index: "cards", DataFile: writeBulkDataFixture(t), DeleteIndex: true,
				Lifecycle: func(LifecycleEvent) { calls.Add(1); panic(tt.value) },
			}
			nopLogger(&opts)
			if tt.propagate {
				defer func() {
					if recovered := recover(); recovered != tt.value {
						t.Fatalf("recovered %v, want the original panic value", recovered)
					}
					requireNoCreate(t, stub, calls.Load())
				}()
				_, _ = Run(t.Context(), opts)
				t.Fatal("Run returned; the non-error panic should have propagated")
			}
			_, err := Run(t.Context(), opts)
			if tt.wantSame != nil {
				var runErr *RunError
				if !errors.As(err, &runErr) || runErr != tt.wantSame {
					t.Fatalf("error = %#v, want the panicked *RunError unchanged", err)
				}
			} else {
				assertRunError(t, err, ErrLoaderExecution, "panic")
				if !errors.Is(err, boom) {
					t.Fatalf("error = %v, want errors.Is the panic value", err)
				}
			}
			requireNoCreate(t, stub, calls.Load())
		})
	}
}

// requireNoCreate proves the callback ran once (on the delete) and the create
// that would have followed never reached the cluster.
func requireNoCreate(t *testing.T, stub *lifecycleStub, calls int32) {
	t.Helper()
	if calls != 1 {
		t.Fatalf("callback calls = %d, want exactly 1", calls)
	}
	if slices.Contains(stub.operations(), "index.create") {
		t.Fatalf("operations = %q: a later action ran after the callback panicked", stub.operations())
	}
}

// TestRunLifecycleInlineAndIsolated guards the delivery contract under the
// race detector: each event is delivered inline, between the acknowledging
// response and Run's next request (the request trace at delivery time
// corroborates this with no timing, but is evidence, not proof;
// TestRunLifecycleCallbackPanicContract is the deterministic same-goroutine
// guard, since a panic in another goroutine could not become Run's error or
// re-panic to Run's caller), and concurrent Runs each receive only their own
// events.
func TestRunLifecycleInlineAndIsolated(t *testing.T) {
	t.Parallel()

	const runs = 8
	var wg sync.WaitGroup
	logs := make([]eventLog, runs)
	for i := range runs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			index := fmt.Sprintf("idx-%d", i)
			stub := newLifecycleStub(t, lifecycleConfig{index: index, indexExists: true})
			opts := Options{
				URL: stub.URL(), Index: index, DataFile: writeBulkDataFixture(t), DeleteIndex: true,
				Lifecycle: func(e LifecycleEvent) {
					logs[i].record(e)
					// Inline delivery: the trace so far ends at the request that
					// produced this event, and the next one has not been sent.
					trace := requestLabels(stub.Stub)
					switch e.Action {
					case LifecycleActionDeleted:
						if last := trace[len(trace)-1]; last != "DELETE /"+index || slices.Contains(trace, "PUT /"+index) {
							t.Errorf("run %d: deleted delivered with trace %v, want it to end at DELETE /%s before any PUT", i, trace, index)
						}
					case LifecycleActionCreated:
						if last := trace[len(trace)-1]; last != "PUT /"+index {
							t.Errorf("run %d: created delivered with trace %v, want it to end at PUT /%s", i, trace, index)
						}
					}
				},
			}
			nopLogger(&opts)
			if _, err := Run(t.Context(), opts); err != nil {
				t.Errorf("run %d error = %v", i, err)
			}
		}()
	}
	wg.Wait()
	for i := range runs {
		index := fmt.Sprintf("idx-%d", i)
		requireLifecycle(t, logs[i].all(), []LifecycleEvent{lev(LifecycleActionDeleted, ResourceKindIndex, index), lev(LifecycleActionCreated, ResourceKindIndex, index)})
	}
}

// TestEventAdapterSanitizesAndBounds guards the one adapter: every string field
// is redacted, single-line, valid UTF-8 and at most 2,048 bytes, independently
// for Name, Alias and Logical, while Action and ResourceKind pass through.
func TestEventAdapterSanitizesAndBounds(t *testing.T) {
	t.Parallel()

	// The invalid byte \xff sits inside the kept prefix, before the 2,048-byte
	// cut, so ToValidUTF8 repair (not truncation) is what makes the value valid.
	hostile := "idx\t\xffpassword=hunter2\x01" + strings.Repeat("é", 1500)
	for _, field := range []string{"Name", "Alias", "Logical"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			in := observe.Event{Action: observe.ActionRepointed, Kind: observe.KindAlias, Name: "n", Alias: "a", Logical: "l"}
			switch field {
			case "Name":
				in.Name = hostile
			case "Alias":
				in.Alias = hostile
			case "Logical":
				in.Logical = hostile
			}
			got := eventFrom(in)
			if got.Action != LifecycleActionRepointed || got.ResourceKind != ResourceKindAlias {
				t.Fatalf("Action/ResourceKind = %q/%q", got.Action, got.ResourceKind)
			}
			value := map[string]string{"Name": got.Name, "Alias": got.Alias, "Logical": got.Logical}[field]
			requireSanitizedName(t, value)
			for other, v := range map[string]string{"Name": got.Name, "Alias": got.Alias, "Logical": got.Logical} {
				if other != field && len(v) != 1 {
					t.Fatalf("%s = %q, want the untouched one-byte value", other, v)
				}
			}
		})
	}
	t.Run("short invalid UTF-8 is repaired and redacted, not truncated", func(t *testing.T) {
		t.Parallel()
		const bad = "a\xffpassword=hunter2\x01"
		got := eventFrom(observe.Event{Action: observe.ActionRepointed, Kind: observe.KindAlias, Name: bad, Alias: bad, Logical: bad})
		for field, v := range map[string]string{"Name": got.Name, "Alias": got.Alias, "Logical": got.Logical} {
			if !utf8.ValidString(v) || strings.Contains(v, "hunter2") || !strings.Contains(v, "[REDACTED]") ||
				strings.IndexByte(v, 0x01) >= 0 || strings.IndexByte(v, 0xff) >= 0 || strings.HasSuffix(v, "[truncated]") {
				t.Fatalf("%s = %q, want valid UTF-8, redacted and not truncated", field, v)
			}
		}
	})
	t.Run("short clean values are unchanged", func(t *testing.T) {
		t.Parallel()
		got := eventFrom(observe.Event{Action: observe.ActionResolved, Kind: observe.KindPolicy, Name: "pol-abc123", Logical: "pol"})
		want := LifecycleEvent{Action: LifecycleActionResolved, ResourceKind: ResourceKindPolicy, Name: "pol-abc123", Logical: "pol"}
		if got != want {
			t.Fatalf("eventFrom = %+v, want %+v", got, want)
		}
	})
}

// TestRunLifecycleHostileIndexNameIsSanitized drives an Elasticsearch-supplied
// index name containing a credential and an oversized multibyte tail through
// Run: the delete event's Name is redacted and bounded, never raw. A control
// character cannot travel this path (the client rejects it in the DELETE URL
// before sending) and invalid UTF-8 cannot survive JSON decoding, so
// TestEventAdapterSanitizesAndBounds covers both at the adapter (its invalid
// byte sits inside the kept prefix, so UTF-8 repair is what it exercises).
func TestRunLifecycleHostileIndexNameIsSanitized(t *testing.T) {
	t.Parallel()

	hostile := "cards-a password=hunter2 " + strings.Repeat("é", 1500)
	stub := newRouteStub(t, map[string]http.HandlerFunc{
		"GET /_alias/cards": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprintf(w, `{%q:{"aliases":{"cards":{}}}}`, hostile)
		},
		"GET /cards-*": esstub.JSON(http.StatusOK, `{}`),
		"/{name}":      replyAck,
	})
	_, err, events := runLifecycle(t, stub.URL(), Options{Index: "cards", AliasMode: true, SyncManaged: true, Nuke: true})
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if len(events) != 1 || events[0].Action != LifecycleActionDeleted || events[0].ResourceKind != ResourceKindIndex {
		t.Fatalf("events = %+v, want one deleted index event", events)
	}
	requireSanitizedName(t, events[0].Name)
	if !utf8.ValidString(events[0].Name) || events[0].Alias != "" {
		t.Fatalf("event = %+v, want valid UTF-8 and no Alias on deleted", events[0])
	}
}

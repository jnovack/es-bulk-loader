package indexops

// Phase 4 (loader-observability) guards: each of ClusterReplicaCount's three
// _nodes fallbacks emits one replicas_defaulted notice with its fixed log
// message and never the error text, while a nil client, a successful query
// and an explicit replica override emit none.

import (
	"errors"
	"net/http"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/jnovack/es-bulk-loader/internal/testutil/bodytransport"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/noticetest"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/observe"
)

// TestClusterReplicaCountNotices pins one replicas_defaulted notice per
// fallback path, its fixed Message and the silent paths.
func TestClusterReplicaCountNotices(t *testing.T) {
	t.Parallel()

	defaulted := func(message string) []observe.Notice {
		return []observe.Notice{{Code: observe.CodeReplicasDefaulted, Message: message}}
	}
	one := 1
	tests := []struct {
		name     string
		reply    func() bodytransport.Reply // nil: a nil client
		override *int
		want     []observe.Notice
	}{
		{"transport failure", func() bodytransport.Reply { return bodytransport.Reply{Err: errors.New("dial FAKE-SECRET")} },
			nil, defaulted("Could not query cluster nodes; defaulting to 0 replicas")},
		{"HTTP failure", bodytransport.Static(http.StatusForbidden, `{"error":{"type":"security_exception","reason":"FAKE-SECRET"}}`),
			nil, defaulted("Cluster nodes query failed; defaulting to 0 replicas")},
		{"undecodable response", bodytransport.Static(http.StatusOK, `FAKE-SECRET not json`),
			nil, defaulted("Could not parse cluster nodes response; defaulting to 0 replicas")},
		{"successful query emits nothing", bodytransport.Static(http.StatusOK, `{"nodes":{"n1":{}}}`), nil, nil},
		{"nil client emits nothing", nil, nil, nil},
		{"explicit override emits nothing", func() bodytransport.Reply { return bodytransport.Reply{Err: errors.New("must not be sent")} }, &one, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, rec := noticetest.New(t)
			var es *elasticsearch.Client
			var tr *bodytransport.Transport
			if tt.reply != nil {
				tr = &bodytransport.Transport{Replies: []func() bodytransport.Reply{tt.reply}}
				es = tr.Client(t)
			}

			replicas, _ := SelectReplicaCount(ctx, es, tt.override)

			if tt.override == nil && replicas != 0 {
				t.Fatalf("replicas = %d, want 0", replicas)
			}
			if tt.override != nil && tr != nil && len(tr.Paths()) != 0 {
				t.Fatalf("override sent %q, want no request", tr.Paths())
			}
			noticetest.RequireNotices(t, rec.Notices, tt.want, "FAKE")
		})
	}
}

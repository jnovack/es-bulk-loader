package aliasops

// LOG-RAWBODY-01 guards for the alias failure sites: unchanged Op, a bounded
// redacted Rejection cause (message and log), no raw body in the log. Bodies
// and secrets are synthetic (FAKE-SECRET-*).

import (
	"context"
	"errors"
	"testing"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/rs/zerolog"

	"github.com/jnovack/es-bulk-loader/internal/testutil/bodytransport"
	"github.com/jnovack/es-bulk-loader/internal/testutil/esstub"
	"github.com/jnovack/es-bulk-loader/internal/testutil/logbuf"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/failure"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/rejectiontest"
	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/runlog"
)

func TestAliasFailureSitesCarryRejectionCause(t *testing.T) {
	t.Parallel()

	const reason = "x: rejected password=[REDACTED] now"
	tests := []struct {
		name    string
		handle  func(*esstub.Stub)
		run     func(ctx context.Context, es *elasticsearch.Client) error
		wantOp  string
		wantMsg string
	}{
		{
			name:   "resolve alias targets",
			handle: func(s *esstub.Stub) { s.Handle("GET /_alias/cards", esstub.JSON(500, rejectiontest.SecretBody)) },
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				_, err := ResolveTargets(ctx, es, "cards")
				return err
			},
			wantOp:  "Failed to resolve alias targets",
			wantMsg: `Failed to resolve alias targets: alias "cards" rejected with HTTP 500: ` + reason,
		},
		{
			name: "update alias",
			handle: func(s *esstub.Stub) {
				s.Handle("GET /_alias/cards", esstub.JSON(404, `{"error":"alias [cards] missing","status":404}`))
				s.Handle("POST /_aliases", esstub.JSON(500, rejectiontest.SecretBody))
			},
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				return Update(ctx, es, "cards", "cards-20260101000000")
			},
			wantOp:  "Failed to update alias",
			wantMsg: `Failed to update alias: alias "cards" rejected with HTTP 500: ` + reason,
		},
		{
			name:   "list timestamped indices",
			handle: func(s *esstub.Stub) { s.Handle("GET /cards-*", esstub.JSON(500, rejectiontest.SecretBody)) },
			run: func(ctx context.Context, es *elasticsearch.Client) error {
				_, err := ListTimestampedIndices(ctx, es, "cards")
				return err
			},
			wantOp:  "Failed to list timestamped indices",
			wantMsg: `Failed to list timestamped indices: alias "cards" rejected with HTTP 500: ` + reason,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := esstub.New(t)
			tt.handle(stub)
			logger, buf := logbuf.New()
			ctx := runlog.WithLogger(t.Context(), logger)

			err := tt.run(ctx, stub.MustClient(t))

			rejectiontest.RequireRejection(t, err, tt.wantOp, tt.wantMsg)
			rejectiontest.RequireFailureLog(t, buf, err)
		})
	}
}

// TestAliasErrorReadIsBoundedAndClosedOnce proves the error body read stops at
// the probe byte and the body is closed once.
func TestAliasErrorReadIsBoundedAndClosedOnce(t *testing.T) {
	t.Parallel()

	tr := &bodytransport.Transport{Replies: []func() bodytransport.Reply{func() bodytransport.Reply {
		return bodytransport.Reply{Status: 500, Body: &bodytransport.Body{R: bodytransport.Endless{}}}
	}}}
	nop := zerolog.Nop()
	ctx := runlog.WithLogger(t.Context(), &nop)

	_, err := ResolveTargets(ctx, tr.Client(t), "cards")

	var opErr *failure.Error
	if !errors.As(err, &opErr) || opErr.Op != "Failed to resolve alias targets" {
		t.Fatalf("error = %#v, want Op %q", err, "Failed to resolve alias targets")
	}
	body := tr.Bodies()[0]
	if body.Served.Load() != esresponse.MaxRejectionBodyBytes+1 || body.Closes.Load() != 1 {
		t.Fatalf("served %d, closed %d; want %d and 1", body.Served.Load(), body.Closes.Load(), esresponse.MaxRejectionBodyBytes+1)
	}
}

package managed

import (
	"net/http"
	"strings"
	"testing"

	"github.com/jnovack/es-bulk-loader/pkg/loader/internal/esresponse"
)

// TestCreatePipelinesRejectionBudgetIdentity guards LOG-RAWBODY-01's fold of
// the pipeline site onto runlog.Fail's Op-relative bound. The pipeline message
// must stay byte-identical to the former hand-derived budget
// (MaxPublicMessageBytes minus the "Failed to create pipeline: " prefix):
// the expected text is built directly at that old budget, and the new path,
// built at the full 2,048 and re-bounded by Fail, must equal it exactly. The
// size/prefix assertions in pipeline_rejection_test.go alone would not catch a
// different truncation byte.
func TestCreatePipelinesRejectionBudgetIdentity(t *testing.T) {
	t.Parallel()

	const op = "Failed to create pipeline"
	oldBudget := esresponse.MaxPublicMessageBytes - len(op+": ")

	tests := []struct {
		name, pipeline, reason string
	}{
		{"long ascii reason", "p", strings.Repeat("a", 4000)},
		{"long multibyte reason", "p", strings.Repeat("é", 3000)},
		{"multibyte reason shifted one byte", "p", "a" + strings.Repeat("é", 3000)},
		{"escaped pipeline name", `we"ird\name é`, strings.Repeat("é", 3000)},
		{"long pipeline name", strings.Repeat("n", 400), strings.Repeat("a", 4000)},
		{"short reason fits untouched", "p", "short"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := `{"error":{"type":"t","reason":"` + tt.reason + `"}}`
			want := esresponse.NewRejection("pipeline", tt.pipeline, 400, []byte(body), nil, oldBudget).Error()

			bodies := staticBodies(body)
			tr := &rejectionTransport{status: http.StatusBadRequest, body: bodies.factory}
			es := newTransportClient(t, tr)
			err := CreatePipelines(nopContext(t), es, pipelineDefs(tt.pipeline), []string{tt.pipeline})

			if got := strings.TrimPrefix(err.Error(), op+": "); got != want {
				t.Fatalf("pipeline message changed\n got: %q\nwant: %q", got, want)
			}
			if len(err.Error()) > esresponse.MaxPublicMessageBytes {
				t.Fatalf("complete message is %d bytes", len(err.Error()))
			}
		})
	}
}

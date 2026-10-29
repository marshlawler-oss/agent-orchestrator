package pr

import (
	"context"
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/deliverytelemetry"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/telemetrymeta"
)

type mergeSink struct{ events []ports.TelemetryEvent }

func (s *mergeSink) Emit(_ context.Context, ev ports.TelemetryEvent) { s.events = append(s.events, ev) }
func (*mergeSink) Close(context.Context) error                       { return nil }

type mergeReads struct{}

func (mergeReads) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	return domain.SessionRecord{ID: id, Kind: domain.KindWorker}, true, nil
}
func (mergeReads) GetSessionTelemetry(context.Context, domain.SessionID) (domain.SessionTelemetryFacts, error) {
	return domain.SessionTelemetryFacts{}, nil
}
func (mergeReads) ListPRsBySession(context.Context, domain.SessionID) ([]domain.PullRequest, error) {
	return nil, nil
}
func (mergeReads) ListReportsBySession(context.Context, domain.SessionID) ([]domain.ReportRecord, error) {
	return nil, nil
}
func (mergeReads) ListPRReviews(context.Context, string) ([]domain.PullRequestReview, error) {
	return nil, nil
}
func (mergeReads) ListReviewRunsBySession(context.Context, domain.SessionID) ([]domain.ReviewRun, error) {
	return nil, nil
}
func (mergeReads) CountPullRequests(context.Context) (int64, error)       { return 1, nil }
func (mergeReads) CountMergedPullRequests(context.Context) (int64, error) { return 1, nil }

func TestActionServiceMerge_ReportsMergedViaRequestSurface(t *testing.T) {
	for _, tc := range []struct {
		surface domain.RequestSurface
		want    string
	}{
		{domain.RequestSurfaceDesktop, "desktop"},
		{domain.RequestSurfaceMobile, "mobile_swipe"},
		{domain.RequestSurfaceCLI, "other"},
	} {
		t.Run(string(tc.surface), func(t *testing.T) {
			pr, scm := mergeableActionFixture()
			pr.SessionID = "session-1"
			sink := &mergeSink{}
			svc := NewActionService(ActionDeps{
				Store: &fakeActionStore{pr: pr, ok: true}, Reader: scm, Merger: scm,
				MergeWriter: &fakeActionWriter{}, Delivery: deliverytelemetry.New(sink, mergeReads{}, nil),
			})
			ctx := telemetrymeta.WithSurface(context.Background(), tc.surface)
			if _, err := svc.Merge(ctx, MergeRequest{PRID: "42", PRURL: pr.URL, ExpectedHeadSHA: pr.HeadSHA}); err != nil {
				t.Fatal(err)
			}
			if len(sink.events) != 1 || sink.events[0].Name != "ao.pr.merged" || sink.events[0].Payload["merged_via"] != tc.want {
				t.Fatalf("events = %#v, want one ao.pr.merged via %s", sink.events, tc.want)
			}
		})
	}
}

// Without a durable merged row the observer will see the merge and report it,
// so the action must not report it too.
func TestActionServiceMerge_UnpersistedMergeIsLeftToTheObserver(t *testing.T) {
	pr, scm := mergeableActionFixture()
	pr.SessionID = "session-1"
	sink := &mergeSink{}
	svc := NewActionService(ActionDeps{
		Store: &fakeActionStore{pr: pr, ok: true}, Reader: scm, Merger: scm,
		MergeWriter: &fakeActionWriter{writeErr: errors.New("locked")}, Delivery: deliverytelemetry.New(sink, mergeReads{}, nil),
	})
	if _, err := svc.Merge(context.Background(), MergeRequest{PRID: "42", PRURL: pr.URL, ExpectedHeadSHA: pr.HeadSHA}); err != nil {
		t.Fatal(err)
	}
	if len(sink.events) != 0 {
		t.Fatalf("events = %#v, want none", sink.events)
	}
}

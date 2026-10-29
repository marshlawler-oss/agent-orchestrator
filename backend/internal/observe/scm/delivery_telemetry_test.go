package scm

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/deliverytelemetry"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type deliverySink struct {
	mu     sync.Mutex
	events []ports.TelemetryEvent
}

func (s *deliverySink) Emit(_ context.Context, ev ports.TelemetryEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
}
func (*deliverySink) Close(context.Context) error { return nil }

func (s *deliverySink) named(name string) []ports.TelemetryEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ports.TelemetryEvent
	for _, ev := range s.events {
		if ev.Name == name {
			out = append(out, ev)
		}
	}
	return out
}

// deliveryReads answers the recorder's reads: every session exists.
type deliveryReads struct{}

func (deliveryReads) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	return domain.SessionRecord{ID: id, Kind: domain.KindWorker, Harness: domain.HarnessCodex}, true, nil
}
func (deliveryReads) GetSessionTelemetry(context.Context, domain.SessionID) (domain.SessionTelemetryFacts, error) {
	return domain.SessionTelemetryFacts{}, nil
}
func (deliveryReads) ListPRsBySession(context.Context, domain.SessionID) ([]domain.PullRequest, error) {
	return nil, nil
}
func (deliveryReads) ListReportsBySession(context.Context, domain.SessionID) ([]domain.ReportRecord, error) {
	return nil, nil
}
func (deliveryReads) ListPRReviews(context.Context, string) ([]domain.PullRequestReview, error) {
	return nil, nil
}
func (deliveryReads) ListReviewRunsBySession(context.Context, domain.SessionID) ([]domain.ReviewRun, error) {
	return nil, nil
}
func (deliveryReads) CountPullRequests(context.Context) (int64, error)       { return 5, nil }
func (deliveryReads) CountMergedPullRequests(context.Context) (int64, error) { return 5, nil }

func TestPoll_ReportsPRTransitionsMadeOutsideAO(t *testing.T) {
	store := &fakeStore{projects: map[string]domain.ProjectRecord{"p": {ID: "p", RepoOriginURL: "https://github.com/o/r.git"}}, prs: map[domain.SessionID][]domain.PullRequest{}, checks: map[string][]domain.PullRequestCheck{}}
	provider := &fakeProvider{repoGuards: map[string]ports.SCMGuardResult{prKey(testRepo, 0): {ETag: "repo"}}, observations: map[string]ports.SCMObservation{}}
	for i := 1; i <= 3; i++ {
		id := domain.SessionID("p-" + fmt.Sprint(i))
		store.sessions = append(store.sessions, domain.SessionRecord{ID: id, ProjectID: "p", Metadata: domain.SessionMetadata{Branch: "b" + fmt.Sprint(i)}})
		pr := knownPR(i)
		pr.SessionID = id
		pr.MetadataHash = "" // force candidate
		store.prs[id] = []domain.PullRequest{pr}
		provider.observations[prKey(testRepo, i)] = testObs(i)
	}
	merged := testObs(1)
	merged.PR.State, merged.PR.Merged = string(domain.PRStateMerged), true
	provider.observations[prKey(testRepo, 1)] = merged
	closed := testObs(2)
	closed.PR.State, closed.PR.Closed = string(domain.PRStateClosed), true
	provider.observations[prKey(testRepo, 2)] = closed

	sink := &deliverySink{}
	obs := newTestObserver(store, provider, &fakeLifecycle{}, time.Unix(1, 0).UTC())
	obs.delivery = deliverytelemetry.New(sink, deliveryReads{}, nil)
	if err := obs.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	mergedEvents := sink.named("ao.pr.merged")
	if len(mergedEvents) != 1 || *mergedEvents[0].SessionID != "p-1" || mergedEvents[0].Payload["merged_via"] != "github" {
		t.Fatalf("merged events = %#v, want one github merge for p-1", mergedEvents)
	}
	closedEvents := sink.named("ao.pr.closed_unmerged")
	if len(closedEvents) != 1 || *closedEvents[0].SessionID != "p-2" {
		t.Fatalf("closed events = %#v, want one for p-2", closedEvents)
	}
	if opened := sink.named("ao.pr.opened"); len(opened) != 0 {
		t.Fatalf("already tracked PRs reported as opened: %#v", opened)
	}
}

func TestPoll_ReportsDiscoveredPROpened(t *testing.T) {
	store := testStoreWithSession()
	provider := &fakeProvider{
		repoGuards:   map[string]ports.SCMGuardResult{prKey(testRepo, 0): {ETag: "v2"}},
		openPRs:      map[string][]ports.SCMPRObservation{prKey(testRepo, 0): {{ProviderID: "PR_1", URL: "https://github.com/o/r/pull/1", Number: 1, SourceBranch: "feat", HeadRepo: "o/r", TargetBranch: "main", HeadSHA: "sha1"}}},
		observations: map[string]ports.SCMObservation{prKey(testRepo, 1): testObs(1)},
	}
	sink := &deliverySink{}
	obs := newTestObserver(store, provider, &fakeLifecycle{}, time.Unix(1, 0).UTC())
	obs.delivery = deliverytelemetry.New(sink, deliveryReads{}, nil)
	if err := obs.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	opened := sink.named("ao.pr.opened")
	if len(opened) != 1 || *opened[0].SessionID != "p-1" || opened[0].Payload["attribution"] != "discovered" || opened[0].Payload["scm_provider"] != "github" {
		t.Fatalf("opened = %#v, want one discovered PR for p-1", opened)
	}
	if got := sink.named("ao.pr.merged"); len(got) != 0 {
		t.Fatalf("open PR reported merged: %#v", got)
	}
}

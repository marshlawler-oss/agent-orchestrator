package deliverytelemetry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type captureSink struct {
	mu     sync.Mutex
	events []ports.TelemetryEvent
}

func (c *captureSink) Emit(_ context.Context, ev ports.TelemetryEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, ev)
}

func (c *captureSink) Close(context.Context) error { return nil }

func (c *captureSink) only(t *testing.T, name string) ports.TelemetryEvent {
	t.Helper()
	var found []ports.TelemetryEvent
	for _, ev := range c.events {
		if ev.Name == name {
			found = append(found, ev)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%s events = %d, want 1 (all: %+v)", name, len(found), c.events)
	}
	return found[0]
}

type fakeStore struct {
	sessions   map[domain.SessionID]domain.SessionRecord
	facts      map[domain.SessionID]domain.SessionTelemetryFacts
	prs        map[domain.SessionID][]domain.PullRequest
	reports    map[domain.SessionID][]domain.ReportRecord
	reviews    map[string][]domain.PullRequestReview
	runs       map[domain.SessionID][]domain.ReviewRun
	prCount    int64
	mergeCount int64
	countErr   error
}

func (f *fakeStore) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	rec, ok := f.sessions[id]
	return rec, ok, nil
}

func (f *fakeStore) GetSessionTelemetry(_ context.Context, id domain.SessionID) (domain.SessionTelemetryFacts, error) {
	return f.facts[id], nil
}

func (f *fakeStore) ListPRsBySession(_ context.Context, id domain.SessionID) ([]domain.PullRequest, error) {
	return f.prs[id], nil
}

func (f *fakeStore) ListReportsBySession(_ context.Context, id domain.SessionID) ([]domain.ReportRecord, error) {
	return f.reports[id], nil
}

func (f *fakeStore) ListPRReviews(_ context.Context, url string) ([]domain.PullRequestReview, error) {
	return f.reviews[url], nil
}

func (f *fakeStore) ListReviewRunsBySession(_ context.Context, id domain.SessionID) ([]domain.ReviewRun, error) {
	return f.runs[id], nil
}

func (f *fakeStore) CountPullRequests(context.Context) (int64, error) { return f.prCount, f.countErr }

func (f *fakeStore) CountMergedPullRequests(context.Context) (int64, error) {
	return f.mergeCount, f.countErr
}

type fakeMilestones struct{ claimed map[string]bool }

func (m *fakeMilestones) Claim(name string) (bool, bool) {
	if m.claimed[name] {
		return false, true
	}
	m.claimed[name] = true
	return true, true
}

var t0 = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

func newRecorder(store *fakeStore) (*Recorder, *captureSink, *fakeMilestones) {
	sink := &captureSink{}
	milestones := &fakeMilestones{claimed: map[string]bool{}}
	r := New(sink, store, milestones)
	r.now = func() time.Time { return t0 }
	return r, sink, milestones
}

func TestSessionOutcome(t *testing.T) {
	cases := []struct {
		name     string
		reason   EndReason
		activity domain.ActivityState
		hadPR    bool
		done     bool
		want     string
	}{
		{"crash wins even with a PR", EndRuntimeDied, domain.ActivityIdle, true, true, OutcomeFailed},
		{"merge teardown", EndPRMerged, domain.ActivityIdle, true, false, OutcomePROpened},
		{"killed after opening a PR", EndKilled, domain.ActivityActive, true, false, OutcomePROpened},
		{"done report without a PR", EndKilled, domain.ActivityIdle, false, true, OutcomeCompleted},
		{"issue closed without a PR", EndIssueClosed, domain.ActivityIdle, false, false, OutcomeCompleted},
		{"killed mid-turn", EndKilled, domain.ActivityActive, false, false, OutcomeKilled},
		{"killed while waiting on the user", EndKilled, domain.ActivityWaitingInput, false, false, OutcomeAbandoned},
		{"killed while idle", EndKilled, domain.ActivityIdle, false, false, OutcomeAbandoned},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sessionOutcome(tc.reason, tc.activity, tc.hadPR, tc.done); got != tc.want {
				t.Fatalf("outcome = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSessionEndedPayload(t *testing.T) {
	rec := domain.SessionRecord{
		ID: "w-1", ProjectID: "proj", Kind: domain.KindWorker, Harness: domain.HarnessCodex,
		CreatedAt: t0.Add(-90 * time.Second), Activity: domain.Activity{State: domain.ActivityIdle},
	}
	store := &fakeStore{
		facts:   map[domain.SessionID]domain.SessionTelemetryFacts{"w-1": {CreatedBy: domain.SessionCreatedByOrchestrator, WaitingInputCount: 3}},
		reports: map[domain.SessionID][]domain.ReportRecord{"w-1": {{State: domain.ReportCheckpoint}, {State: domain.ReportDone}}},
	}
	r, sink, _ := newRecorder(store)
	r.SessionEnded(context.Background(), rec, EndKilled)

	ev := sink.only(t, "ao.session.ended")
	want := map[string]any{
		"kind": "worker", "harness": "codex", "outcome": OutcomeCompleted, "had_pr": false,
		"waiting_input_count": int64(3), "duration_s": int64(90), "created_by": "orchestrator",
	}
	for k, v := range want {
		if ev.Payload[k] != v {
			t.Errorf("payload[%s] = %#v, want %#v", k, ev.Payload[k], v)
		}
	}
	if len(ev.Payload) != len(want) {
		t.Errorf("payload = %#v, want exactly %d keys", ev.Payload, len(want))
	}
	if ev.SessionID == nil || *ev.SessionID != "w-1" || ev.ProjectID == nil || *ev.ProjectID != "proj" {
		t.Errorf("event ids = %v/%v", ev.SessionID, ev.ProjectID)
	}
}

func TestSessionEndedOmitsUnknownCreator(t *testing.T) {
	rec := domain.SessionRecord{ID: "w-1", Kind: domain.KindWorker, CreatedAt: t0}
	r, sink, _ := newRecorder(&fakeStore{prs: map[domain.SessionID][]domain.PullRequest{"w-1": {{Number: 4}}}})
	r.SessionEnded(context.Background(), rec, EndKilled)
	ev := sink.only(t, "ao.session.ended")
	if _, ok := ev.Payload["created_by"]; ok {
		t.Fatalf("created_by present for a session created before it was recorded: %#v", ev.Payload)
	}
	if ev.Payload["outcome"] != OutcomePROpened || ev.Payload["had_pr"] != true {
		t.Fatalf("payload = %#v, want pr_opened with had_pr", ev.Payload)
	}
}

func TestPROpenedFirstForInstall(t *testing.T) {
	store := &fakeStore{
		sessions: map[domain.SessionID]domain.SessionRecord{"w-1": {ID: "w-1", Kind: domain.KindWorker, Harness: domain.HarnessClaudeCode, CreatedAt: t0.Add(-time.Minute)}},
	}
	r, sink, _ := newRecorder(store)
	pr := domain.PullRequest{URL: "https://github.com/o/r/pull/1", SessionID: "w-1", Number: 1, Provider: "github", Title: "secret title"}
	claim := r.BeginPROpened(context.Background())
	store.prCount = 1 // the row is written between Begin and the event
	r.PROpened(context.Background(), pr, AttributionDiscovered, claim)
	second := r.BeginPROpened(context.Background())
	store.prCount = 2
	r.PROpened(context.Background(), pr, AttributionClaimed, second)

	opened := sink.events
	if len(opened) != 2 {
		t.Fatalf("events = %d, want 2", len(opened))
	}
	first := opened[0].Payload
	if first["first_for_install"] != true || first["attribution"] != "discovered" || first["ms_since_spawn"] != int64(60_000) ||
		first["harness"] != "claude-code" || first["session_kind"] != "worker" || first["scm_provider"] != "github" {
		t.Fatalf("first payload = %#v", first)
	}
	if opened[1].Payload["first_for_install"] != false {
		t.Fatalf("second PR first_for_install = %#v, want false", opened[1].Payload["first_for_install"])
	}
	for _, ev := range opened {
		for _, v := range ev.Payload {
			if s, ok := v.(string); ok && (s == pr.Title || s == pr.URL) {
				t.Fatalf("payload leaks PR text: %#v", ev.Payload)
			}
		}
	}
}

// Two first PRs written before either reports: deciding after the writes
// would let each see the other's row and neither report true.
func TestPROpenedFirstSurvivesConcurrentWrites(t *testing.T) {
	store := &fakeStore{sessions: map[domain.SessionID]domain.SessionRecord{"w-1": {ID: "w-1"}, "w-2": {ID: "w-2"}}}
	r, sink, _ := newRecorder(store)
	a := r.BeginPROpened(context.Background())
	b := r.BeginPROpened(context.Background())
	store.prCount = 2
	r.PROpened(context.Background(), domain.PullRequest{SessionID: "w-2", Number: 2}, AttributionClaimed, b)
	r.PROpened(context.Background(), domain.PullRequest{SessionID: "w-1", Number: 1}, AttributionDiscovered, a)
	trues := 0
	for _, ev := range sink.events {
		if ev.Payload["first_for_install"] == true {
			trues++
		}
	}
	if trues != 1 {
		t.Fatalf("first_for_install=true count = %d, want exactly 1 (%#v)", trues, sink.events)
	}
}

// A transition that saw prior rows must not take the marker while one that saw
// none is still between its write and its event.
func TestIneligibleTransitionDoesNotTakeAnInFlightClaim(t *testing.T) {
	store := &fakeStore{sessions: map[domain.SessionID]domain.SessionRecord{"w-1": {ID: "w-1"}, "w-2": {ID: "w-2"}}}
	r, sink, milestones := newRecorder(store)
	eligible := r.BeginPROpened(context.Background())
	store.prCount = 1
	ineligible := r.BeginPROpened(context.Background())
	r.PROpened(context.Background(), domain.PullRequest{SessionID: "w-2", Number: 2}, AttributionClaimed, ineligible)
	if milestones.claimed[milestonePROpened] {
		t.Fatal("ineligible transition claimed the marker while an eligible one was in flight")
	}
	r.PROpened(context.Background(), domain.PullRequest{SessionID: "w-1", Number: 1}, AttributionDiscovered, eligible)
	if sink.events[0].Payload["first_for_install"] != false || sink.events[1].Payload["first_for_install"] != true {
		t.Fatalf("payloads = %#v / %#v, want false then true", sink.events[0].Payload, sink.events[1].Payload)
	}
}

func TestAbandonedClaimLeavesMarkerForTheNextFirst(t *testing.T) {
	store := &fakeStore{sessions: map[domain.SessionID]domain.SessionRecord{"w-1": {ID: "w-1"}}}
	r, sink, milestones := newRecorder(store)
	r.BeginPROpened(context.Background()).Abandon()
	if milestones.claimed[milestonePROpened] {
		t.Fatal("a failed write claimed the marker")
	}
	r.PROpened(context.Background(), domain.PullRequest{SessionID: "w-1", Number: 1}, AttributionDiscovered, r.BeginPROpened(context.Background()))
	if got := sink.only(t, "ao.pr.opened").Payload["first_for_install"]; got != true {
		t.Fatalf("first_for_install = %#v, want true", got)
	}
}

func TestPROpenedNotFirstOnUpgradedInstall(t *testing.T) {
	store := &fakeStore{
		sessions: map[domain.SessionID]domain.SessionRecord{"w-1": {ID: "w-1"}},
		prCount:  7,
	}
	r, sink, milestones := newRecorder(store)
	r.PROpened(context.Background(), domain.PullRequest{SessionID: "w-1", Number: 8}, AttributionDiscovered, r.BeginPROpened(context.Background()))
	if got := sink.only(t, "ao.pr.opened").Payload["first_for_install"]; got != false {
		t.Fatalf("first_for_install = %#v, want false with prior PR rows", got)
	}
	if !milestones.claimed[milestonePROpened] {
		t.Fatal("marker not claimed; a later empty table would report a false first")
	}
}

func TestPROpenedOmitsFirstWhenCountFails(t *testing.T) {
	store := &fakeStore{sessions: map[domain.SessionID]domain.SessionRecord{"w-1": {ID: "w-1"}}, countErr: errors.New("db")}
	r, sink, milestones := newRecorder(store)
	r.PROpened(context.Background(), domain.PullRequest{SessionID: "w-1", Number: 1}, AttributionDiscovered, r.BeginPROpened(context.Background()))
	if _, ok := sink.only(t, "ao.pr.opened").Payload["first_for_install"]; ok {
		t.Fatal("first_for_install present although the prior-row count failed")
	}
	if milestones.claimed[milestonePROpened] {
		t.Fatal("unknown prior rows claimed the marker")
	}
}

func TestPRMergedPayload(t *testing.T) {
	url := "https://github.com/o/r/pull/9"
	store := &fakeStore{
		sessions: map[domain.SessionID]domain.SessionRecord{"w-1": {ID: "w-1", Kind: domain.KindWorker, Harness: domain.HarnessCodex, CreatedAt: t0.Add(-time.Hour)}},
		reviews:  map[string][]domain.PullRequestReview{url: {{ID: "a"}, {ID: "b"}}},
		runs:     map[domain.SessionID][]domain.ReviewRun{"w-1": {{PRURL: "https://github.com/o/r/pull/8"}, {PRURL: url}}},
	}
	r, sink, _ := newRecorder(store)
	claim := r.BeginPRMerged(context.Background())
	store.mergeCount = 1
	r.PRMerged(context.Background(), domain.PullRequest{URL: url, SessionID: "w-1", Number: 9, Provider: "github", CreatedAtProvider: t0.Add(-10 * time.Minute)}, MergedViaMobileSwipe, claim)

	payload := sink.only(t, "ao.pr.merged").Payload
	want := map[string]any{
		"harness": "codex", "session_kind": "worker", "scm_provider": "github", "ms_since_spawn": int64(3_600_000),
		"ms_since_open": int64(600_000), "review_rounds": 2, "ao_review_used": true, "merged_via": "mobile_swipe",
		"first_for_install": true,
	}
	for k, v := range want {
		if payload[k] != v {
			t.Errorf("payload[%s] = %#v, want %#v", k, payload[k], v)
		}
	}
}

func TestPRClosedUnmergedWithoutAOReview(t *testing.T) {
	store := &fakeStore{sessions: map[domain.SessionID]domain.SessionRecord{"w-1": {ID: "w-1", Kind: domain.KindWorker}}}
	r, sink, _ := newRecorder(store)
	r.PRClosedUnmerged(context.Background(), domain.PullRequest{URL: "u", SessionID: "w-1", Number: 2, Provider: "gitlab"})
	payload := sink.only(t, "ao.pr.closed_unmerged").Payload
	if payload["ao_review_used"] != false || payload["review_rounds"] != 0 || payload["scm_provider"] != "gitlab" {
		t.Fatalf("payload = %#v", payload)
	}
	if _, ok := payload["merged_via"]; ok {
		t.Fatalf("closed_unmerged carries merged_via: %#v", payload)
	}
}

func TestMergedVia(t *testing.T) {
	if got := MergedViaSurface(domain.RequestSurfaceDesktop, true); got != MergedViaDesktop {
		t.Errorf("desktop = %q", got)
	}
	if got := MergedViaSurface(domain.RequestSurfaceMobile, true); got != MergedViaMobileSwipe {
		t.Errorf("mobile = %q", got)
	}
	if got := MergedViaSurface(domain.RequestSurfaceCLI, true); got != MergedViaOther {
		t.Errorf("cli = %q", got)
	}
	if got := MergedViaSurface("", false); got != MergedViaOther {
		t.Errorf("no surface = %q", got)
	}
	if got := MergedOutsideAO(domain.PullRequest{Provider: "github"}); got != MergedViaGitHub {
		t.Errorf("github = %q", got)
	}
	if got := MergedOutsideAO(domain.PullRequest{Provider: "gitlab"}); got != MergedViaOther {
		t.Errorf("gitlab = %q", got)
	}
}

func TestNilRecorderIsInert(t *testing.T) {
	var r *Recorder
	r.SessionEnded(context.Background(), domain.SessionRecord{ID: "x"}, EndKilled)
	r.PROpened(context.Background(), domain.PullRequest{SessionID: "x"}, AttributionClaimed, r.BeginPROpened(context.Background()))
	r.PRMerged(context.Background(), domain.PullRequest{SessionID: "x"}, MergedViaDesktop, r.BeginPRMerged(context.Background()))
	r.BeginPRMerged(context.Background()).Abandon()
	r.PRClosedUnmerged(context.Background(), domain.PullRequest{SessionID: "x"})
}

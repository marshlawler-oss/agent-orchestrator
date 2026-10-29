package session

import (
	"context"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/deliverytelemetry"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/telemetrymeta"
)

// createdByFakeStore records created_by writes on top of the shared fake.
type createdByFakeStore struct {
	*fakeStore
	createdBy map[domain.SessionID]domain.SessionCreator
}

func (s *createdByFakeStore) RecordSessionCreatedBy(_ context.Context, id domain.SessionID, by domain.SessionCreator) error {
	s.createdBy[id] = by
	return nil
}

func lastEvent(t *testing.T, sink *fakeTelemetrySink, name string) ports.TelemetryEvent {
	t.Helper()
	for i := len(sink.events) - 1; i >= 0; i-- {
		if sink.events[i].Name == name {
			return sink.events[i]
		}
	}
	t.Fatalf("no %s in %#v", name, sink.events)
	return ports.TelemetryEvent{}
}

func TestSpawnCreatedByAndSurface(t *testing.T) {
	run := domain.AutomationRunID("automation-run-1")
	cases := []struct {
		name    string
		cfg     ports.SpawnConfig
		surface domain.RequestSurface
		want    domain.SessionCreator
	}{
		{"desktop task", ports.SpawnConfig{Kind: domain.KindWorker}, domain.RequestSurfaceDesktop, domain.SessionCreatedByUser},
		{"cli outside a session", ports.SpawnConfig{Kind: domain.KindWorker}, domain.RequestSurfaceCLI, domain.SessionCreatedByUser},
		{"orchestrator ao spawn", ports.SpawnConfig{Kind: domain.KindWorker, ParentSessionID: "orch-1"}, domain.RequestSurfaceCLI, domain.SessionCreatedByOrchestrator},
		{"worker ao spawn", ports.SpawnConfig{Kind: domain.KindWorker, ParentSessionID: "work-1"}, domain.RequestSurfaceCLI, domain.SessionCreatedByAutomation},
		{"schedule", ports.SpawnConfig{Kind: domain.KindWorker, AutomationRunID: &run}, "", domain.SessionCreatedByScheduled},
		{"tracker intake", ports.SpawnConfig{Kind: domain.KindWorker, CreatedBy: domain.SessionCreatedByAutomation}, "", domain.SessionCreatedByAutomation},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &createdByFakeStore{fakeStore: newFakeStore(), createdBy: map[domain.SessionID]domain.SessionCreator{}}
			st.projects["mer"] = domain.ProjectRecord{ID: "mer"}
			st.sessions["orch-1"] = domain.SessionRecord{ID: "orch-1", ProjectID: "mer", Kind: domain.KindOrchestrator}
			st.sessions["work-1"] = domain.SessionRecord{ID: "work-1", ProjectID: "mer", Kind: domain.KindWorker}
			sink := &fakeTelemetrySink{}
			svc := NewWithDeps(Deps{Manager: &fakeCommander{}, Store: st, Telemetry: sink})
			ctx := context.Background()
			if tc.surface != "" {
				ctx = telemetrymeta.WithSurface(ctx, tc.surface)
			}
			cfg := tc.cfg
			cfg.ProjectID = "mer"
			if _, _, _, err := svc.Spawn(ctx, cfg); err != nil {
				t.Fatalf("Spawn: %v", err)
			}
			ev := lastEvent(t, sink, "ao.session.spawned")
			if ev.Payload["created_by"] != string(tc.want) {
				t.Fatalf("created_by = %#v, want %q", ev.Payload["created_by"], tc.want)
			}
			surface, hasSurface := ev.Payload["surface"]
			if tc.surface == "" && hasSurface {
				t.Fatalf("surface = %#v for a spawn no request started", surface)
			}
			if tc.surface != "" && surface != string(tc.surface) {
				t.Fatalf("surface = %#v, want %q", surface, tc.surface)
			}
			if st.createdBy["mer-9"] != tc.want {
				t.Fatalf("persisted created_by = %q, want %q", st.createdBy["mer-9"], tc.want)
			}
		})
	}
}

func TestRecordUserMessage(t *testing.T) {
	st := newFakeStore()
	st.sessions["orch-1"] = domain.SessionRecord{ID: "orch-1", ProjectID: "mer", Kind: domain.KindOrchestrator, Harness: domain.HarnessClaudeCode}
	st.sessions["legacy"] = domain.SessionRecord{ID: "legacy", ProjectID: "mer", Harness: domain.HarnessCodex}
	sink := &fakeTelemetrySink{}
	svc := NewWithDeps(Deps{Manager: &fakeCommander{}, Store: st, Telemetry: sink})

	svc.RecordUserMessage(telemetrymeta.WithSurface(context.Background(), domain.RequestSurfaceMobile), "orch-1", true)
	ev := lastEvent(t, sink, "ao.session.user_message_sent")
	want := map[string]any{"kind": "orchestrator", "harness": "claude-code", "is_steer": true, "surface": "mobile"}
	for k, v := range want {
		if ev.Payload[k] != v {
			t.Errorf("payload[%s] = %#v, want %#v", k, ev.Payload[k], v)
		}
	}
	if len(ev.Payload) != len(want) {
		t.Errorf("payload = %#v, want only enums", ev.Payload)
	}

	svc.RecordUserMessage(context.Background(), "legacy", false)
	if got := lastEvent(t, sink, "ao.session.user_message_sent").Payload["kind"]; got != "worker" {
		t.Errorf("kindless row kind = %#v, want worker", got)
	}

	before := len(sink.events)
	svc.RecordUserMessage(context.Background(), "missing", false)
	if len(sink.events) != before {
		t.Errorf("unknown session emitted %#v", sink.events[before:])
	}
}

// deliveryFakeStore is the recorder's read surface over fixed facts.
type deliveryFakeStore struct {
	prs []domain.PullRequest
}

func (deliveryFakeStore) GetSession(context.Context, domain.SessionID) (domain.SessionRecord, bool, error) {
	return domain.SessionRecord{}, false, nil
}
func (deliveryFakeStore) GetSessionTelemetry(context.Context, domain.SessionID) (domain.SessionTelemetryFacts, error) {
	return domain.SessionTelemetryFacts{CreatedBy: domain.SessionCreatedByUser}, nil
}
func (d deliveryFakeStore) ListPRsBySession(context.Context, domain.SessionID) ([]domain.PullRequest, error) {
	return d.prs, nil
}
func (deliveryFakeStore) ListReportsBySession(context.Context, domain.SessionID) ([]domain.ReportRecord, error) {
	return nil, nil
}
func (deliveryFakeStore) ListPRReviews(context.Context, string) ([]domain.PullRequestReview, error) {
	return nil, nil
}
func (deliveryFakeStore) ListReviewRunsBySession(context.Context, domain.SessionID) ([]domain.ReviewRun, error) {
	return nil, nil
}
func (deliveryFakeStore) CountPullRequests(context.Context) (int64, error)       { return 0, nil }
func (deliveryFakeStore) CountMergedPullRequests(context.Context) (int64, error) { return 0, nil }

func TestKillReportsSessionEndedOnce(t *testing.T) {
	st := newFakeStore()
	st.sessions["mer-1"] = domain.SessionRecord{
		ID: "mer-1", ProjectID: "mer", Kind: domain.KindWorker, Harness: domain.HarnessCodex,
		CreatedAt: time.Now().Add(-time.Minute), Activity: domain.Activity{State: domain.ActivityActive},
	}
	st.sessions["done"] = domain.SessionRecord{ID: "done", ProjectID: "mer", IsTerminated: true}
	sink := &fakeTelemetrySink{}
	fc := &fakeCommander{}
	svc := NewWithDeps(Deps{Manager: fc, Store: st, Delivery: deliverytelemetry.New(sink, deliveryFakeStore{}, nil)})

	if _, err := svc.Kill(context.Background(), "mer-1"); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	ev := lastEvent(t, sink, "ao.session.ended")
	if ev.Payload["outcome"] != "killed" || ev.Payload["created_by"] != "user" || ev.Payload["harness"] != "codex" {
		t.Fatalf("payload = %#v, want killed mid-turn by a user-created codex worker", ev.Payload)
	}

	before := len(sink.events)
	if _, err := svc.RequestKill(context.Background(), "done"); err != nil {
		t.Fatalf("RequestKill: %v", err)
	}
	if len(sink.events) != before {
		t.Fatalf("killing an already terminated session reported again: %#v", sink.events[before:])
	}
}

// knownSessionReads answers the recorder's session lookup for claim tests.
type knownSessionReads struct{ deliveryFakeStore }

func (knownSessionReads) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	return domain.SessionRecord{ID: id, Kind: domain.KindWorker}, true, nil
}

func TestClaimPRReportsOpenedOnlyForPRsNewToAO(t *testing.T) {
	cases := []struct {
		name      string
		outcome   ports.ClaimOutcome
		ownedURL  string
		wantEvent bool
	}{
		{"new PR", ports.ClaimOutcome{}, "", true},
		{"takeover from another session", ports.ClaimOutcome{PreviousOwner: "demo-2"}, "", false},
		{"re-claim of its own PR", ports.ClaimOutcome{}, "https://gitlab.example.com:8443/alice/repo/-/merge_requests/7", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := newFakeStore()
			st.sessions["demo-1"] = domain.SessionRecord{ID: "demo-1", ProjectID: "demo", Kind: domain.KindWorker, Metadata: domain.SessionMetadata{WorkspacePath: "/ws"}}
			st.projects["demo"] = domain.ProjectRecord{ID: "demo", RepoOriginURL: "https://gitlab.example.com:8443/alice/repo.git"}
			claimer := &fakePRClaimer{}
			claimer.out.ClaimOutcome = tc.outcome
			sink := &fakeTelemetrySink{}
			svc := NewWithDeps(Deps{Store: st, SCM: &claimTargetSCM{}, PRClaimer: claimer, Delivery: deliverytelemetry.New(sink, knownSessionReads{}, nil)})
			if tc.ownedURL == "" {
				st.prs["demo-1"] = nil
			} else {
				if _, err := svc.ClaimPR(context.Background(), "demo-1", "7", ClaimPROptions{}); err != nil {
					t.Fatal(err)
				}
				st.prs["demo-1"] = []domain.PullRequest{{URL: claimer.gotPR.URL, SessionID: "demo-1", Number: 7}}
				sink.events = nil
			}
			if _, err := svc.ClaimPR(context.Background(), "demo-1", "7", ClaimPROptions{}); err != nil {
				t.Fatal(err)
			}
			var opened []ports.TelemetryEvent
			for _, ev := range sink.events {
				if ev.Name == "ao.pr.opened" {
					opened = append(opened, ev)
				}
			}
			if tc.wantEvent != (len(opened) == 1) || len(opened) > 1 {
				t.Fatalf("opened = %#v, want event=%v", opened, tc.wantEvent)
			}
			if tc.wantEvent && opened[0].Payload["attribution"] != "claimed" {
				t.Fatalf("attribution = %#v", opened[0].Payload["attribution"])
			}
		})
	}
}

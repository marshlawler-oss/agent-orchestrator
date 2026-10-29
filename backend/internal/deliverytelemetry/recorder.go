// Package deliverytelemetry emits the product events that say whether a
// session delivered anything: ao.session.ended and the ao.pr.* lifecycle.
// Several owners observe these moments (session kill, lifecycle teardown, the
// SCM poller, PR claim, the merge action), so the payloads are built here once.
//
// Payloads carry enums, counts and durations only: never a title, branch, URL,
// path or message text.
package deliverytelemetry

import (
	"context"
	"sync"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/reqid"
)

// Store is the read surface the payloads are derived from.
type Store interface {
	GetSession(ctx context.Context, id domain.SessionID) (domain.SessionRecord, bool, error)
	GetSessionTelemetry(ctx context.Context, id domain.SessionID) (domain.SessionTelemetryFacts, error)
	ListPRsBySession(ctx context.Context, id domain.SessionID) ([]domain.PullRequest, error)
	ListReportsBySession(ctx context.Context, id domain.SessionID) ([]domain.ReportRecord, error)
	ListPRReviews(ctx context.Context, prURL string) ([]domain.PullRequestReview, error)
	ListReviewRunsBySession(ctx context.Context, id domain.SessionID) ([]domain.ReviewRun, error)
	CountPullRequests(ctx context.Context) (int64, error)
	CountMergedPullRequests(ctx context.Context) (int64, error)
}

// EndReason is what ended a session, as known by the code that ended it.
type EndReason string

// End reasons.
const (
	// EndKilled is an explicit kill from a person or an agent.
	EndKilled EndReason = "killed"
	// EndPRMerged is the teardown after the session's PRs merged.
	EndPRMerged EndReason = "pr_merged"
	// EndIssueClosed is the teardown after the tracker issue was closed.
	EndIssueClosed EndReason = "issue_closed"
	// EndRuntimeDied is the reaper finding the runtime conclusively dead.
	EndRuntimeDied EndReason = "runtime_died"
)

// Session outcomes reported on ao.session.ended.
const (
	OutcomePROpened  = "pr_opened"
	OutcomeCompleted = "completed"
	OutcomeKilled    = "killed"
	OutcomeFailed    = "failed"
	OutcomeAbandoned = "abandoned"
)

// How a merge happened, reported as merged_via on ao.pr.merged.
const (
	MergedViaDesktop     = "desktop"
	MergedViaMobileSwipe = "mobile_swipe"
	MergedViaGitHub      = "github"
	MergedViaOther       = "other"
)

// How a PR became attributed to a session, reported on ao.pr.opened.
const (
	AttributionDiscovered = "discovered"
	AttributionClaimed    = "claimed"
)

// Recorder emits delivery events. A nil Recorder, or one without a sink, does
// nothing, so optional wiring stays optional.
type Recorder struct {
	sink       ports.EventSink
	store      Store
	milestones ports.InstallMilestones
	now        func() time.Time

	// claimsMu guards eligibleInFlight: per milestone, how many transitions
	// that saw no prior rows are between Begin and their event.
	claimsMu         sync.Mutex
	eligibleInFlight map[string]int
}

// New returns a Recorder. milestones may be nil: first_for_install is then
// omitted.
func New(sink ports.EventSink, store Store, milestones ports.InstallMilestones) *Recorder {
	return &Recorder{sink: sink, store: store, milestones: milestones, now: time.Now, eligibleInFlight: map[string]int{}}
}

// Milestones behind first_for_install on PR events (marker files
// telemetry_first_<name> under the data dir).
const (
	milestonePROpened = "pr_opened"
	milestonePRMerged = "pr_merged"
)

// FirstClaim is one transition's first_for_install decision, begun before the
// row that would make later transitions ineligible is written. This follows
// the session_spawned fix in the session service: deciding after the write
// lets two near-simultaneous first PRs each see the other's row, so neither
// would report true. A nil FirstClaim omits the flag.
type FirstClaim struct {
	r        *Recorder
	name     string
	eligible bool
	known    bool
	done     bool
}

// BeginPROpened decides, before a new PR row is written, whether that PR could
// be the install's first. Pass the claim to PROpened, or Abandon it if the
// write fails.
func (r *Recorder) BeginPROpened(ctx context.Context) *FirstClaim {
	if !r.enabled() {
		return nil
	}
	return r.begin(milestonePROpened, func() (int64, error) { return r.store.CountPullRequests(ctx) })
}

// BeginPRMerged decides, before a PR row is written as merged, whether that
// merge could be the install's first.
func (r *Recorder) BeginPRMerged(ctx context.Context) *FirstClaim {
	if !r.enabled() {
		return nil
	}
	return r.begin(milestonePRMerged, func() (int64, error) { return r.store.CountMergedPullRequests(ctx) })
}

func (r *Recorder) begin(name string, count func() (int64, error)) *FirstClaim {
	n, err := count()
	c := &FirstClaim{r: r, name: name, eligible: err == nil && n == 0, known: err == nil}
	if c.eligible {
		r.claimsMu.Lock()
		r.eligibleInFlight[name]++
		r.claimsMu.Unlock()
	}
	return c
}

// Abandon ends a claim whose write failed, without claiming the marker.
func (c *FirstClaim) Abandon() { c.finish(false) }

// finish settles the claim once. While an eligible transition is in flight, an
// ineligible one reports false without claiming the marker, so it cannot take
// the eligible one's claim. With none in flight, an ineligible transition
// claims the marker, so an install upgraded with PR history never reports a
// later PR as its first.
func (c *FirstClaim) finish(succeeded bool) (first, known bool) {
	if c == nil || c.done {
		return false, false
	}
	c.done = true
	r := c.r
	r.claimsMu.Lock()
	defer r.claimsMu.Unlock()
	if c.eligible {
		r.eligibleInFlight[c.name]--
	}
	if !succeeded || !c.known || r.milestones == nil {
		return false, false
	}
	if !c.eligible && r.eligibleInFlight[c.name] > 0 {
		return false, true
	}
	return ports.FirstForInstall(r.milestones, c.name, c.eligible)
}

func (r *Recorder) enabled() bool { return r != nil && r.sink != nil && r.store != nil }

// SessionEnded reports a session that has just been terminated. rec is the
// record as it was before termination, so the activity it was in when it ended
// is still visible.
func (r *Recorder) SessionEnded(ctx context.Context, rec domain.SessionRecord, reason EndReason) {
	if !r.enabled() || rec.ID == "" {
		return
	}
	prs, err := r.store.ListPRsBySession(ctx, rec.ID)
	if err != nil {
		return
	}
	hadPR := false
	for _, pr := range prs {
		if pr.Number > 0 {
			hadPR = true
			break
		}
	}
	doneReported := false
	if reports, err := r.store.ListReportsBySession(ctx, rec.ID); err == nil {
		for _, report := range reports {
			if report.State == domain.ReportDone {
				doneReported = true
				break
			}
		}
	}
	facts, _ := r.store.GetSessionTelemetry(ctx, rec.ID)
	payload := map[string]any{
		"kind":                string(rec.Kind),
		"harness":             string(rec.Harness),
		"outcome":             sessionOutcome(reason, rec.Activity.State, hadPR, doneReported),
		"had_pr":              hadPR,
		"waiting_input_count": facts.WaitingInputCount,
	}
	if !rec.CreatedAt.IsZero() {
		payload["duration_s"] = nonNegative(int64(r.now().Sub(rec.CreatedAt) / time.Second))
	}
	if facts.CreatedBy.Valid() {
		payload["created_by"] = string(facts.CreatedBy)
	}
	r.emit(ctx, "ao.session.ended", rec, payload)
}

// sessionOutcome folds how a session ended into one outcome. A PR wins over
// everything but a crash, because a PR is the deliverable; a done report (or a
// closed issue) without a PR is a completed non-PR task. An explicit kill with
// nothing delivered is "killed" while the agent was mid-turn and "abandoned"
// when it had already stopped: idle, waiting on the user, or exited.
func sessionOutcome(reason EndReason, activity domain.ActivityState, hadPR, doneReported bool) string {
	switch {
	case reason == EndRuntimeDied:
		return OutcomeFailed
	case reason == EndPRMerged || hadPR:
		return OutcomePROpened
	case reason == EndIssueClosed || doneReported:
		return OutcomeCompleted
	case activity == domain.ActivityActive:
		return OutcomeKilled
	default:
		return OutcomeAbandoned
	}
}

// PROpened reports a PR newly attributed to a session. pr must already be
// persisted; claim comes from BeginPROpened before that write.
func (r *Recorder) PROpened(ctx context.Context, pr domain.PullRequest, attribution string, claim *FirstClaim) {
	// Settled first: the marker describes the install even when the event is
	// not sent.
	first, known := claim.finish(true)
	if !r.enabled() || pr.SessionID == "" {
		return
	}
	rec, ok, err := r.store.GetSession(ctx, pr.SessionID)
	if err != nil || !ok {
		return
	}
	payload := r.prBase(rec, pr)
	payload["attribution"] = attribution
	if known {
		payload["first_for_install"] = first
	}
	r.emit(ctx, "ao.pr.opened", rec, payload)
}

// PRMerged reports a PR that has just merged. pr must already be persisted as
// merged; claim comes from BeginPRMerged before that write, and mergedVia is
// one of the MergedVia constants.
func (r *Recorder) PRMerged(ctx context.Context, pr domain.PullRequest, mergedVia string, claim *FirstClaim) {
	first, known := claim.finish(true)
	if !r.enabled() || pr.SessionID == "" {
		return
	}
	rec, ok, err := r.store.GetSession(ctx, pr.SessionID)
	if err != nil || !ok {
		return
	}
	payload := r.prOutcome(ctx, rec, pr)
	payload["merged_via"] = mergedVia
	if known {
		payload["first_for_install"] = first
	}
	r.emit(ctx, "ao.pr.merged", rec, payload)
}

// PRClosedUnmerged reports a PR that has just been closed without merging.
func (r *Recorder) PRClosedUnmerged(ctx context.Context, pr domain.PullRequest) {
	if !r.enabled() || pr.SessionID == "" {
		return
	}
	rec, ok, err := r.store.GetSession(ctx, pr.SessionID)
	if err != nil || !ok {
		return
	}
	r.emit(ctx, "ao.pr.closed_unmerged", rec, r.prOutcome(ctx, rec, pr))
}

// MergedViaSurface maps the client that asked AO to merge to merged_via. The
// mobile app merges with a swipe on the PR card.
func MergedViaSurface(surface domain.RequestSurface, ok bool) string {
	if !ok {
		return MergedViaOther
	}
	switch surface {
	case domain.RequestSurfaceDesktop:
		return MergedViaDesktop
	case domain.RequestSurfaceMobile:
		return MergedViaMobileSwipe
	default:
		return MergedViaOther
	}
}

// MergedOutsideAO is merged_via for a merge AO only observed: done on the
// provider's own site (or by another tool).
func MergedOutsideAO(pr domain.PullRequest) string {
	if pr.Provider == "github" {
		return MergedViaGitHub
	}
	return MergedViaOther
}

func (r *Recorder) prBase(rec domain.SessionRecord, pr domain.PullRequest) map[string]any {
	payload := map[string]any{
		"harness":      string(rec.Harness),
		"session_kind": string(rec.Kind),
	}
	if pr.Provider == "github" || pr.Provider == "gitlab" {
		payload["scm_provider"] = pr.Provider
	}
	if !rec.CreatedAt.IsZero() {
		payload["ms_since_spawn"] = nonNegative(r.now().Sub(rec.CreatedAt).Milliseconds())
	}
	return payload
}

func (r *Recorder) prOutcome(ctx context.Context, rec domain.SessionRecord, pr domain.PullRequest) map[string]any {
	payload := r.prBase(rec, pr)
	if !pr.CreatedAtProvider.IsZero() {
		payload["ms_since_open"] = nonNegative(r.now().Sub(pr.CreatedAtProvider).Milliseconds())
	}
	if reviews, err := r.store.ListPRReviews(ctx, pr.URL); err == nil {
		payload["review_rounds"] = len(reviews)
	}
	if runs, err := r.store.ListReviewRunsBySession(ctx, rec.ID); err == nil {
		used := false
		for _, run := range runs {
			if run.PRURL == pr.URL {
				used = true
				break
			}
		}
		payload["ao_review_used"] = used
	}
	return payload
}

func (r *Recorder) emit(ctx context.Context, name string, rec domain.SessionRecord, payload map[string]any) {
	sessionID := rec.ID
	ev := ports.TelemetryEvent{
		Name:       name,
		Source:     "delivery_telemetry",
		OccurredAt: r.now(),
		Level:      ports.TelemetryLevelInfo,
		SessionID:  &sessionID,
		RequestID:  reqid.FromContext(ctx),
		Payload:    payload,
	}
	if rec.ProjectID != "" {
		projectID := rec.ProjectID
		ev.ProjectID = &projectID
	}
	r.sink.Emit(context.WithoutCancel(ctx), ev)
}

func nonNegative(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}

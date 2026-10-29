package session

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/deliverytelemetry"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/reqid"
	"github.com/aoagents/agent-orchestrator/backend/internal/telemetrymeta"
)

// createdByStore persists created_by for ao.session.ended. It is optional so
// focused service fakes need not implement it.
type createdByStore interface {
	RecordSessionCreatedBy(ctx context.Context, id domain.SessionID, createdBy domain.SessionCreator) error
}

// spawnCreatedBy says who caused this spawn. Only daemon-side facts count: an
// explicit unattended source, a schedule occurrence, or the AO session that ran
// `ao spawn` (validated against the store, never trusted from the caller).
func (s *Service) spawnCreatedBy(ctx context.Context, cfg ports.SpawnConfig) domain.SessionCreator {
	switch {
	case cfg.CreatedBy.Valid():
		return cfg.CreatedBy
	case cfg.AutomationRunID != nil:
		return domain.SessionCreatedByScheduled
	case cfg.ParentSessionID != "":
		if s.store != nil {
			if parent, ok, err := s.store.GetSession(ctx, cfg.ParentSessionID); err == nil && ok && parent.Kind != domain.KindOrchestrator {
				return domain.SessionCreatedByAutomation
			}
		}
		return domain.SessionCreatedByOrchestrator
	default:
		return domain.SessionCreatedByUser
	}
}

func (s *Service) recordCreatedBy(ctx context.Context, id domain.SessionID, createdBy domain.SessionCreator) {
	store, ok := s.store.(createdByStore)
	if !ok || id == "" {
		return
	}
	if err := store.RecordSessionCreatedBy(context.WithoutCancel(ctx), id, createdBy); err != nil && s.logger != nil {
		s.logger.Warn("telemetry: record created_by", "session", id, "err", err)
	}
}

// RecordUserMessage emits ao.session.user_message_sent for a message a person
// sent to a session. Callers decide authorship: the HTTP controllers call it
// only for requests that no AO session sent. The message itself is never read.
func (s *Service) RecordUserMessage(ctx context.Context, id domain.SessionID, isSteer bool) {
	if s.telemetry == nil || s.store == nil {
		return
	}
	rec, ok, err := s.store.GetSession(ctx, id)
	if err != nil || !ok {
		return
	}
	payload := map[string]any{
		"kind":     string(sessionKindOrWorker(rec.Kind)),
		"harness":  string(rec.Harness),
		"is_steer": isSteer,
	}
	if surface, ok := telemetrymeta.SurfaceFromContext(ctx); ok {
		payload["surface"] = string(surface)
	}
	projectID := rec.ProjectID
	sessionID := rec.ID
	s.telemetry.Emit(context.WithoutCancel(ctx), ports.TelemetryEvent{
		Name:       "ao.session.user_message_sent",
		Source:     "session_service",
		OccurredAt: s.now(),
		Level:      ports.TelemetryLevelInfo,
		ProjectID:  &projectID,
		SessionID:  &sessionID,
		RequestID:  reqid.FromContext(ctx),
		Payload:    payload,
	})
}

// sessionKindOrWorker reads rows written before kinds existed as workers, as
// the session list does.
func sessionKindOrWorker(kind domain.SessionKind) domain.SessionKind {
	if kind == domain.KindOrchestrator {
		return kind
	}
	return domain.KindWorker
}

// liveSessionBeforeEnd returns the record of a session that is about to be
// ended, or ok=false when it is unknown or already terminated (so a repeated
// kill reports nothing).
func (s *Service) liveSessionBeforeEnd(ctx context.Context, id domain.SessionID) (domain.SessionRecord, bool) {
	if s.delivery == nil || s.store == nil {
		return domain.SessionRecord{}, false
	}
	rec, ok, err := s.store.GetSession(ctx, id)
	if err != nil || !ok || rec.IsTerminated {
		return domain.SessionRecord{}, false
	}
	return rec, true
}

func (s *Service) recordKilled(ctx context.Context, before domain.SessionRecord, live bool) {
	if !live {
		return
	}
	s.delivery.SessionEnded(ctx, before, deliverytelemetry.EndKilled)
}

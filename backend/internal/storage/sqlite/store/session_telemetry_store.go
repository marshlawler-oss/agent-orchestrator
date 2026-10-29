package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// RecordSessionCreatedBy stores who caused the session to exist.
func (s *Store) RecordSessionCreatedBy(ctx context.Context, id domain.SessionID, createdBy domain.SessionCreator) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.qw.RecordSessionCreatedBy(ctx, gen.RecordSessionCreatedByParams{SessionID: string(id), CreatedBy: string(createdBy)}); err != nil {
		return fmt.Errorf("record created_by for session %s: %w", id, err)
	}
	return nil
}

// IncrementSessionWaitingInputCount counts one more stop to wait on the user.
func (s *Store) IncrementSessionWaitingInputCount(ctx context.Context, id domain.SessionID) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.qw.IncrementSessionWaitingInputCount(ctx, string(id)); err != nil {
		return fmt.Errorf("count waiting input for session %s: %w", id, err)
	}
	return nil
}

// GetSessionTelemetry reads the session's analytics facts. A session without a
// row reports zero values, which readers treat as unknown.
func (s *Store) GetSessionTelemetry(ctx context.Context, id domain.SessionID) (domain.SessionTelemetryFacts, error) {
	row, err := s.qr.GetSessionTelemetry(ctx, string(id))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.SessionTelemetryFacts{}, nil
	}
	if err != nil {
		return domain.SessionTelemetryFacts{}, fmt.Errorf("get telemetry facts for session %s: %w", id, err)
	}
	return domain.SessionTelemetryFacts{CreatedBy: domain.SessionCreator(row.CreatedBy), WaitingInputCount: row.WaitingInputCount}, nil
}

// CountPullRequests counts every tracked PR row, so a "first PR for this
// install" marker can tell a fresh install from one upgraded with PR history.
func (s *Store) CountPullRequests(ctx context.Context) (int64, error) {
	n, err := s.qr.CountPullRequests(ctx)
	if err != nil {
		return 0, fmt.Errorf("count pull requests: %w", err)
	}
	return n, nil
}

// CountMergedPullRequests counts tracked PRs that merged.
func (s *Store) CountMergedPullRequests(ctx context.Context) (int64, error) {
	n, err := s.qr.CountMergedPullRequests(ctx)
	if err != nil {
		return 0, fmt.Errorf("count merged pull requests: %w", err)
	}
	return n, nil
}

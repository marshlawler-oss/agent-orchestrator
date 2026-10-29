package controllers

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// UserMessageRecorder reports a message a person sent to a session. It receives
// no message content. Controllers call it only after the send was accepted and
// only when no AO session sent the request, so agent-to-agent traffic (`ao send`
// inside a session) and daemon-injected feedback never count.
type UserMessageRecorder interface {
	RecordUserMessage(ctx context.Context, id domain.SessionID, isSteer bool)
}

func recordUserMessage(ctx context.Context, recorder UserMessageRecorder, id domain.SessionID, senderSessionID string, isSteer bool) {
	if recorder == nil || senderSessionID != "" {
		return
	}
	recorder.RecordUserMessage(ctx, id, isSteer)
}

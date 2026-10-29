package codexappserver

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestUserInputBlockingProtocolCompatibility(t *testing.T) {
	d, srv := newTestDriver(t)
	conv, err := d.Start(context.Background(), ports.ChatStartConfig{WorkspacePath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conv.Close() }()
	// isBlocking supersedes the deprecated timer in current app-server schemas.
	params := strings.Replace(inputParams, `"itemId":`, `"isBlocking":true,"autoResolutionMs":0,"itemId":`, 1)
	srv.push(`{"id":"blocking","method":"item/tool/requestUserInput","params":` + params + `}`)
	ev := nextEvent(t, conv.Events(), ports.ChatEventInputRequested)
	select {
	case unexpected := <-conv.Events():
		t.Fatalf("blocking request resolved before an answer: %#v", unexpected)
	case <-time.After(50 * time.Millisecond):
	}
	err = conv.(ports.ChatInputResponder).ResolveInput(context.Background(), ev.RequestID, ports.ChatInputResponse{
		Action: ports.ChatInputActionAccept, Content: map[string]any{"first_value": "Bridge"},
	})
	if err != nil {
		t.Fatal(err)
	}
	reply := srv.awaitFrame(func(f frame) bool { return f.ID != nil && string(*f.ID) == `"blocking"` && f.Method == "" })
	var payload userInputReply
	if err := json.Unmarshal(reply.Result, &payload); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(payload.Answers["first_value"].Answers, []string{"Bridge"}) {
		t.Fatalf("answer did not reach native protocol: %s", reply.Result)
	}
}

func TestUserInputBlockingFlagValidation(t *testing.T) {
	for _, flag := range []string{`false`, `"true"`, `1`, `{}`} {
		t.Run(flag, func(t *testing.T) {
			params := strings.Replace(inputParams, `"itemId":`, `"isBlocking":`+flag+`,"itemId":`, 1)
			if _, _, err := parseUserInput(json.RawMessage(params)); err == nil {
				t.Fatal("unsupported or malformed blocking semantics accepted")
			}
		})
	}
}

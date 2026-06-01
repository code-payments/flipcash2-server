package memory

import (
	"testing"

	chat_memory "github.com/code-payments/flipcash2-server/chat/memory"
	"github.com/code-payments/flipcash2-server/messaging/tests"
)

func TestMessaging_MemoryServer(t *testing.T) {
	chats := chat_memory.NewInMemory()
	messages := NewInMemory()
	teardown := func() {
		messages.(*memory).reset()
	}
	tests.RunServerTests(t, chats, messages, teardown)
}

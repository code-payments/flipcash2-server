package memory

import (
	"testing"

	chat_memory "github.com/code-payments/flipcash2-server/chat/memory"
	"github.com/code-payments/flipcash2-server/messaging/tests"
	profile_memory "github.com/code-payments/flipcash2-server/profile/memory"
)

func TestMessaging_MemoryServer(t *testing.T) {
	chats := chat_memory.NewInMemory()
	profiles := profile_memory.NewInMemory()
	messages := NewInMemory()
	teardown := func() {
		messages.(*memory).reset()
	}
	tests.RunServerTests(t, chats, messages, profiles, teardown)
}

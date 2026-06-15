package memory

import (
	"testing"

	badge_memory "github.com/code-payments/flipcash2-server/badge/memory"
	chat_memory "github.com/code-payments/flipcash2-server/chat/memory"
	"github.com/code-payments/flipcash2-server/messaging/tests"
	profile_memory "github.com/code-payments/flipcash2-server/profile/memory"
)

func TestMessaging_MemoryServer(t *testing.T) {
	badges := badge_memory.NewInMemory()
	chats := chat_memory.NewInMemory()
	profiles := profile_memory.NewInMemory()
	messages := NewInMemory()
	teardown := func() {
		messages.(*memory).reset()
	}
	tests.RunServerTests(t, badges, chats, messages, profiles, teardown)
}

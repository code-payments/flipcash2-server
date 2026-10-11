package memory

import (
	"testing"

	chat_memory "github.com/code-payments/flipcash2-server/chat/memory"
	"github.com/code-payments/flipcash2-server/poll/tests"
)

func TestPoll_MemoryServer(t *testing.T) {
	chats := chat_memory.NewInMemory(nil)
	testStore := NewInMemory()
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunServerTests(t, chats, testStore, teardown)
}

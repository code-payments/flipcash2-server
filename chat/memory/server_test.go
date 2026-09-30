package memory

import (
	"testing"

	"github.com/code-payments/flipcash2-server/chat/tests"
)

func TestChat_MemoryServer(t *testing.T) {
	testStore := NewInMemory(nil)
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunServerTests(t, testStore, teardown)
}

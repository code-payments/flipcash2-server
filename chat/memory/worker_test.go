package memory

import (
	"testing"

	"github.com/code-payments/flipcash2-server/chat/tests"
)

func TestChat_MemoryHoldingsWorker(t *testing.T) {
	testStore := NewInMemory(nil)
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunHoldingsWorkerTests(t, testStore, teardown)
}

package memory

import (
	"testing"

	"github.com/code-payments/flipcash2-server/push/tests"
)

func TestPush_MemoryPusher(t *testing.T) {
	testStore := NewInMemory()
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunPusherTests(t, testStore, teardown)
}

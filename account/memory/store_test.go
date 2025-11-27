package memory

import (
	"testing"

	"github.com/code-payments/flipcash2-server/account/tests"
)

func TestAccount_MemoryStore(t *testing.T) {
	testStore := NewInMemory()
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}

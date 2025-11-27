package memory

import (
	"testing"

	"github.com/code-payments/flipcash2-server/iap/tests"
)

func TestIap_MemoryStore(t *testing.T) {
	testStore := NewInMemory()
	teardown := func() {
		testStore.(*InMemoryStore).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}

package memory

import (
	"testing"

	"github.com/code-payments/flipcash2-server/blob/tests"
)

func TestBlobAccess_MemoryStore(t *testing.T) {
	testStore := NewInMemoryAccessStore()
	teardown := func() {
		testStore.(*accessMemory).reset()
	}
	tests.RunAccessStoreTests(t, testStore, teardown)
}

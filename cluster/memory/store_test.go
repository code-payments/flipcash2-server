package memory

import (
	"testing"

	"github.com/code-payments/flipcash2-server/cluster/tests"
)

func TestCluster_MemoryStore(t *testing.T) {
	testStore := NewInMemory()
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}

func TestCluster_MemoryRuntime(t *testing.T) {
	testStore := NewInMemory()
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunClusterTests(t, testStore, teardown)
}

func TestCluster_MemoryE2E(t *testing.T) {
	testStore := NewInMemory()
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunE2ETests(t, testStore, teardown)
}

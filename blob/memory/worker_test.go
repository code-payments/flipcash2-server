package memory

import (
	"testing"

	"github.com/code-payments/flipcash2-server/blob/tests"
)

func TestBlob_MemoryWorker(t *testing.T) {
	blobs := NewInMemory()
	storage := NewInMemoryStorage()
	access := NewInMemoryAccessStore()
	teardown := func() {
		blobs.(*memory).reset()
		access.(*accessMemory).reset()
		storage.reset()
	}
	tests.RunWorkerTests(t, blobs, storage, access, storage.PutObject, teardown)
}

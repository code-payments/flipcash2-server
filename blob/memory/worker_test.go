package memory

import (
	"testing"

	"github.com/code-payments/flipcash2-server/blob/tests"
)

func TestBlob_MemoryWorker(t *testing.T) {
	blobs := NewInMemory()
	storage := NewInMemoryStorage()
	teardown := func() {
		blobs.(*memory).reset()
		storage.reset()
	}
	tests.RunWorkerTests(t, blobs, storage, storage.PutObject, teardown)
}

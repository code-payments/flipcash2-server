package memory

import (
	"testing"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/blob/tests"
)

func TestBlob_MemoryServer(t *testing.T) {
	accounts := account_memory.NewInMemory()
	blobs := NewInMemory()
	storage := NewInMemoryStorage()
	teardown := func() {
		blobs.(*memory).reset()
		storage.reset()
	}
	tests.RunServerTests(t, accounts, blobs, storage, storage.SimulateUpload, teardown)
}

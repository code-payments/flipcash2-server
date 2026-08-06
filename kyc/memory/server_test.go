package memory

import (
	"testing"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/kyc/tests"
)

func TestKyc_MemoryServer(t *testing.T) {
	accounts := account_memory.NewInMemory()
	testStore := NewInMemory()
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunServerTests(t, accounts, testStore, teardown)
}

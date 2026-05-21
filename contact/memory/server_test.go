package memory

import (
	"testing"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/contact/tests"
	profile_memory "github.com/code-payments/flipcash2-server/profile/memory"
)

func TestContact_MemoryServer(t *testing.T) {
	accounts := account_memory.NewInMemory()
	profiles := profile_memory.NewInMemory()
	testStore := NewInMemory()
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunServerTests(t, accounts, profiles, testStore, teardown)
}

package memory

import (
	"testing"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/profile/tests"
)

func TestProfile_MemoryServer(t *testing.T) {
	accounts := account_memory.NewInMemory()
	profiles := NewInMemory()
	teardown := func() {
	}
	tests.RunServerTests(t, accounts, profiles, teardown)
}

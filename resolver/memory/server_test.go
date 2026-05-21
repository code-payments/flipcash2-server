package memory

import (
	"testing"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	profile_memory "github.com/code-payments/flipcash2-server/profile/memory"
	"github.com/code-payments/flipcash2-server/resolver/tests"
)

func TestResolver_MemoryServer(t *testing.T) {
	accounts := account_memory.NewInMemory()
	profiles := profile_memory.NewInMemory()
	tests.RunServerTests(t, accounts, profiles)
}

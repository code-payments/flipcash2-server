package memory

import (
	"testing"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/event/tests"
)

func TestEvent_MemoryServer(t *testing.T) {
	accounts := account_memory.NewInMemory()
	events := NewInMemory()
	teardown := func() {
	}
	tests.RunServerTests(t, accounts, events, teardown)
}

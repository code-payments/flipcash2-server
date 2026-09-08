package event_test

import (
	"testing"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/event/tests"
)

func TestEvent_Server(t *testing.T) {
	accounts := account_memory.NewInMemory()
	tests.RunServerTests(t, accounts, func() {})
}

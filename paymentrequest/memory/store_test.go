package memory

import (
	"testing"

	"github.com/code-payments/flipcash2-server/paymentrequest/tests"
)

func TestPaymentRequest_MemoryStore(t *testing.T) {
	testStore := NewInMemory()
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}

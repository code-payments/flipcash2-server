package memory

import (
	"testing"

	"github.com/code-payments/flipcash2-server/paymentrequest/tests"
)

func TestPaymentRequest_MemoryWorker(t *testing.T) {
	testStore := NewInMemory()
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunWorkerTests(t, testStore, teardown)
}

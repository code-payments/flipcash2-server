package memory

import (
	"testing"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/chat/tests"
)

func TestChat_MemoryStore(t *testing.T) {
	testStore := NewInMemory(nil)
	teardown := func() {
		testStore.(*memory).reset()
	}
	newStore := func(excludedFromFeed []*commonpb.UserId) chat.Store {
		return NewInMemory(excludedFromFeed)
	}
	tests.RunStoreTests(t, testStore, newStore, teardown)
}

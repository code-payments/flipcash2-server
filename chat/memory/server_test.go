package memory

import (
	"testing"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/chat/tests"
)

func TestChat_MemoryServer(t *testing.T) {
	testStore := NewInMemory([]*commonpb.UserId{tests.TeamUserID})
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunServerTests(t, testStore, teardown)
}

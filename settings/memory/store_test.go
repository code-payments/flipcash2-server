package memory

import (
	"context"
	"testing"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/settings"
	"github.com/code-payments/flipcash2-server/settings/tests"
)

func TestSettings_MemoryStore(t *testing.T) {
	testStore := NewInMemory()
	createUser := func(t *testing.T) *commonpb.UserId {
		userID := model.MustGenerateUserID()
		testStore.SetRegion(context.Background(), userID, settings.DefaultRegion)
		testStore.SetLocale(context.Background(), userID, settings.DefaultLocale)
		return userID
	}
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunStoreTests(t, testStore, createUser, teardown)
}

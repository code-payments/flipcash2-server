package memory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	account_memory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/contact/tests"
	"github.com/code-payments/flipcash2-server/model"
)

func TestContact_MemoryStore(t *testing.T) {
	accounts := account_memory.NewInMemory()
	testStore := NewInMemory()
	createUser := func(t *testing.T) *commonpb.UserId {
		userID := model.MustGenerateUserID()
		_, err := accounts.Bind(context.Background(), userID, model.MustGenerateKeyPair().Proto())
		require.NoError(t, err)
		return userID
	}
	teardown := func() {
		testStore.(*memory).reset()
	}
	tests.RunStoreTests(t, testStore, createUser, teardown)
}

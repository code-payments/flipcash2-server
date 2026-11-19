//go:build integration

package dynamodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/model"
)

// TestMessaging_DynamoDBStore_PutMessages_RetryTakesFreshTimestamp pins that
// a send which loses the chat's counter to another send is not stored with
// the timestamp it came with. The send that got in ahead took the lower ID
// and, being sent later, a later time, so the retry reads the clock again and
// time does not run backwards against the IDs. A send that needs no retry is
// stored exactly as sent.
func TestMessaging_DynamoDBStore_PutMessages_RetryTakesFreshTimestamp(t *testing.T) {
	ctx := context.Background()
	s := newLayoutStore(t)
	defer s.reset()

	chatID := randomChatID(t)
	sender := model.MustGenerateUserID()
	ids := layoutClientIDs(t, 3)

	// One send reads the counter and, before its transaction, a second is
	// sent whole: the first loses the counter and retries.
	var ahead *messaging.Message
	s.beforeSendWrite = func() {
		s.beforeSendWrite = nil
		var err error
		ahead, _, err = s.PutMessage(ctx, chatID, sender, nil, time.Now().UTC(), ids[1], true)
		require.NoError(t, err)
	}
	sentAt := time.Now().UTC()
	waited, created, err := s.PutMessage(ctx, chatID, sender, nil, sentAt, ids[0], true)
	require.NoError(t, err)
	require.True(t, created)

	require.NotNil(t, ahead)
	require.Equal(t, uint64(1), ahead.ID.Value)
	require.Equal(t, uint64(2), waited.ID.Value)
	require.True(t, ahead.Timestamp.After(sentAt))
	require.True(t, waited.Timestamp.After(ahead.Timestamp), "message 2 stamped %v, not after message 1 at %v", waited.Timestamp, ahead.Timestamp)

	// What was returned is what was stored.
	stored, err := s.GetMessage(ctx, chatID, waited.ID)
	require.NoError(t, err)
	require.True(t, stored.Timestamp.Equal(waited.Timestamp))

	// A send that is not retried keeps the timestamp it came with, whatever
	// the clock or the chat's other messages say.
	plain, created, err := s.PutMessage(ctx, chatID, sender, nil, time.Unix(100, 0), ids[2], true)
	require.NoError(t, err)
	require.True(t, created)
	require.True(t, plain.Timestamp.Equal(time.Unix(100, 0)))
}

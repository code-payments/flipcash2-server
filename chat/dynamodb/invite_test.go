//go:build integration

package dynamodb

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	"google.golang.org/protobuf/proto"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

// TestChat_DynamoDBInviteItems pins the chat_invites layout, which no Store
// method observes: the copy is its own item, carrying the TTL exactly when
// the invite expires (rounded up to the second) and no index attribute, and
// deleted by the revoke; the record never carries expires_at, which would
// have the sweep delete it, and leaves the active index when revoked or used
// up; and the table has TTL enabled on the attribute.
func TestChat_DynamoDBInviteItems(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable))
	testStore := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable, nil)
	defer testStore.(*store).reset()

	ttl, err := testEnv.Client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(invitesTable)})
	require.NoError(t, err)
	require.NotNil(t, ttl.TimeToLiveDescription)
	require.Contains(t, []types.TimeToLiveStatus{types.TimeToLiveStatusEnabled, types.TimeToLiveStatusEnabling}, ttl.TimeToLiveDescription.TimeToLiveStatus)
	require.Equal(t, attrExpiresAt, aws.ToString(ttl.TimeToLiveDescription.AttributeName))

	now := time.Now()
	groupID := chat.MustGenerateGroupChatID()
	creator := model.MustGenerateUserID()
	newInvite := func(maxUses uint32, expiresAt *time.Time) chat.Invite {
		token := make([]byte, chat.InviteTokenSize)
		_, err := rand.Read(token)
		require.NoError(t, err)
		return chat.Invite{
			ID:        chat.MustDeriveInviteID(&chatpb.InviteToken{Value: token}),
			ChatID:    groupID,
			CreatedBy: creator,
			MaxUses:   maxUses,
			CreatedAt: now,
			ExpiresAt: expiresAt,
			SecretEnvelope: &chatpb.InviteSecretEnvelope{
				Scheme:     chatpb.InviteSecretEnvelope_X25519_XCHACHA20POLY1305,
				Nonce:      bytes.Repeat([]byte{1}, 24),
				Ciphertext: bytes.Repeat([]byte{1}, 48),
			},
		}
	}
	keyCopy := &chatpb.InviteKeyEnvelope{
		Scheme:     chatpb.InviteKeyEnvelope_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{2}, 24),
		Ciphertext: bytes.Repeat([]byte{2}, 48),
		Signature:  bytes.Repeat([]byte{2}, 64),
	}
	item := func(inviteID *chatpb.InviteId, sk string) map[string]types.AttributeValue {
		t.Helper()
		out, err := testEnv.Client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(invitesTable),
			Key:            inviteKey(inviteID, sk),
			ConsistentRead: aws.Bool(true),
		})
		require.NoError(t, err)
		return out.Item
	}
	indexAttrs := []string{attrChat, attrCreatedSort, attrCreatorSort, attrActiveSort}

	// An invite that expires: the copy carries the TTL, rounded up to the
	// second, and the record does not.
	expiresAt := now.Add(time.Hour).Truncate(time.Second).Add(time.Millisecond)
	expiring := newInvite(0, &expiresAt)
	_, created, err := testStore.CreateInvite(ctx, expiring, keyCopy, chat.MaxActiveInvites, now)
	require.NoError(t, err)
	require.True(t, created)
	meta := item(expiring.ID, skMeta)
	require.NotContains(t, meta, attrExpiresAt)
	require.Contains(t, meta, attrInviteExpiresAt)
	for _, attr := range indexAttrs {
		require.Contains(t, meta, attr)
	}
	copyItem := item(expiring.ID, skInviteKey)
	require.NotEmpty(t, copyItem)
	for _, attr := range indexAttrs {
		require.NotContains(t, copyItem, attr)
	}
	secs, err := parseN(copyItem[attrExpiresAt])
	require.NoError(t, err)
	require.EqualValues(t, expiresAt.Unix()+1, secs)
	got := &chatpb.InviteKeyEnvelope{}
	require.NoError(t, unmarshalInviteEnvelope(copyItem, attrKeyEnvelope, got))
	require.True(t, proto.Equal(keyCopy, got))
	require.NotContains(t, copyItem, attrSignature)
	gotSecret := &chatpb.InviteSecretEnvelope{}
	require.NoError(t, unmarshalInviteEnvelope(meta, attrSecretEnvelope, gotSecret))
	require.True(t, proto.Equal(expiring.SecretEnvelope, gotSecret))
	require.NotContains(t, meta, attrSecretScheme)

	// An invite that never expires: neither item carries the TTL, or an
	// expiry at all.
	lasting := newInvite(1, nil)
	_, created, err = testStore.CreateInvite(ctx, lasting, keyCopy, chat.MaxActiveInvites, now)
	require.NoError(t, err)
	require.True(t, created)
	require.NotContains(t, item(lasting.ID, skMeta), attrExpiresAt)
	require.NotContains(t, item(lasting.ID, skMeta), attrInviteExpiresAt)
	require.NotContains(t, item(lasting.ID, skInviteKey), attrExpiresAt)

	// A revoke removes the record from the active index and deletes the
	// copy, and the record stays, listed.
	_, changed, err := testStore.RevokeInvite(ctx, groupID, expiring.ID, now)
	require.NoError(t, err)
	require.True(t, changed)
	meta = item(expiring.ID, skMeta)
	require.NotContains(t, meta, attrActiveSort)
	require.Contains(t, meta, attrRevokedAt)
	require.Contains(t, meta, attrCreatedSort)
	require.Empty(t, item(expiring.ID, skInviteKey))

	// A used-up invite is not counted toward the cap: its last use, written
	// here as the join will write it, removes it from the active index.
	count, err := testStore.(*store).countActiveInvites(ctx, groupID, now, chat.MaxActiveInvites)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	_, err = testEnv.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(invitesTable),
		Key:              inviteKey(lasting.ID, skMeta),
		UpdateExpression: aws.String("SET #useCount = :one REMOVE #active"),
		ExpressionAttributeNames: map[string]string{
			"#useCount": attrUseCount,
			"#active":   attrActiveSort,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{":one": avN(1)},
	})
	require.NoError(t, err)
	count, err = testStore.(*store).countActiveInvites(ctx, groupID, now, chat.MaxActiveInvites)
	require.NoError(t, err)
	require.Zero(t, count)
	_, _, err = testStore.CreateInvite(ctx, newInvite(0, nil), keyCopy, 1, now)
	require.NoError(t, err)
	used, err := testStore.GetInvite(ctx, lasting.ID)
	require.NoError(t, err)
	require.Equal(t, chatpb.Invite_EXHAUSTED, used.StatusAt(now))

	// A revoke of a used-up invite, which a join may also have moved, is
	// conditioned on the use count it read.
	revoked, changed, err := testStore.RevokeInvite(ctx, groupID, lasting.ID, now)
	require.NoError(t, err)
	require.True(t, changed)
	require.EqualValues(t, 1, revoked.UseCount)
}

// TestChat_DynamoDBKeyEnvelopeCopyItem pins that an envelope a member wraps
// for themself, replacing an invite's copy, leaves none of the copy's
// attributes behind in chat_key_envelopes.
func TestChat_DynamoDBKeyEnvelopeCopyItem(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable))
	testStore := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable, nil)
	defer testStore.(*store).reset()

	groupID := chat.MustGenerateGroupChatID()
	creator := model.MustGenerateUserID()
	joiner := model.MustGenerateUserID()
	invite := chat.Invite{
		ID:        &chatpb.InviteId{Value: bytes.Repeat([]byte{3}, chat.InviteIDSize)},
		ChatID:    groupID,
		CreatedBy: creator,
	}
	copied := chat.NewInviteKeyEnvelope(invite,
		&chatpb.InviteKeyEnvelope{Scheme: chatpb.InviteKeyEnvelope_XCHACHA20POLY1305, Nonce: bytes.Repeat([]byte{4}, 24), Ciphertext: bytes.Repeat([]byte{4}, 48), Signature: bytes.Repeat([]byte{4}, 64)},
		&chatpb.InviteSecretEnvelope{Scheme: chatpb.InviteSecretEnvelope_X25519_XCHACHA20POLY1305, Nonce: bytes.Repeat([]byte{5}, 24), Ciphertext: bytes.Repeat([]byte{5}, 48)},
	)
	_, err := testStore.SetKeyEnvelope(ctx, groupID, joiner, copied)
	require.NoError(t, err)

	read := func() map[string]types.AttributeValue {
		t.Helper()
		out, err := testEnv.Client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(keyEnvelopesTable),
			Key:            keyEnvelopeKey(groupID, joiner),
			ConsistentRead: aws.Bool(true),
		})
		require.NoError(t, err)
		return out.Item
	}
	copyAttrs := []string{attrInviteID, attrSignature, attrSecretScheme, attrSecretNonce, attrSecretCiphertext}
	got := read()
	for _, attr := range copyAttrs {
		require.Contains(t, got, attr)
	}
	require.Equal(t, creator.Value, asB(got[attrWrappedBy]))

	own := chat.KeyEnvelope{
		Scheme:     chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{6}, 24),
		Ciphertext: bytes.Repeat([]byte{6}, 48),
		WrappedBy:  &commonpb.UserId{Value: joiner.Value},
	}
	_, err = testStore.SetKeyEnvelope(ctx, groupID, joiner, own)
	require.NoError(t, err)
	got = read()
	for _, attr := range copyAttrs {
		require.NotContains(t, got, attr)
	}
	require.Equal(t, joiner.Value, asB(got[attrWrappedBy]))
}

// TestChat_DynamoDBInviteJoinItems pins what a join with an invite writes to
// chat_invites, which no Store method observes: a join before the last use
// moves use_count and leaves the record in the active index and the copy in
// place; the join that takes the last use removes active_sort and deletes
// the copy in the same write; and a member's join, the no-op, writes nothing.
func TestChat_DynamoDBInviteJoinItems(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable))
	testStore := NewInDynamoDB(testEnv.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable, holdingsTable, invitesTable, pinsTable, nil)
	defer testStore.(*store).reset()

	now := time.Now()
	creator := model.MustGenerateUserID()
	groupID := chat.MustGenerateGroupChatID()
	require.NoError(t, testStore.PutChat(ctx, &chat.Chat{
		ID:           groupID,
		Type:         chatpb.ChatType_GROUP,
		Members:      []*commonpb.UserId{creator},
		Title:        "Private",
		IsPrivate:    true,
		CreatorID:    creator,
		LastActivity: now,
	}))

	token := make([]byte, chat.InviteTokenSize)
	_, err := rand.Read(token)
	require.NoError(t, err)
	secret := &chatpb.InviteSecretEnvelope{
		Scheme:     chatpb.InviteSecretEnvelope_X25519_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{1}, 24),
		Ciphertext: bytes.Repeat([]byte{1}, 48),
	}
	expiresAt := now.Add(time.Hour)
	invite := chat.Invite{
		ID:             chat.MustDeriveInviteID(&chatpb.InviteToken{Value: token}),
		ChatID:         groupID,
		CreatedBy:      creator,
		MaxUses:        2,
		CreatedAt:      now,
		ExpiresAt:      &expiresAt,
		SecretEnvelope: secret,
	}
	keyCopy := &chatpb.InviteKeyEnvelope{
		Scheme:     chatpb.InviteKeyEnvelope_XCHACHA20POLY1305,
		Nonce:      bytes.Repeat([]byte{2}, 24),
		Ciphertext: bytes.Repeat([]byte{2}, 48),
		Signature:  bytes.Repeat([]byte{2}, 64),
	}
	_, created, err := testStore.CreateInvite(ctx, invite, keyCopy, chat.MaxActiveInvites, now)
	require.NoError(t, err)
	require.True(t, created)

	item := func(sk string) map[string]types.AttributeValue {
		t.Helper()
		out, err := testEnv.Client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(invitesTable),
			Key:            inviteKey(invite.ID, sk),
			ConsistentRead: aws.Bool(true),
		})
		require.NoError(t, err)
		return out.Item
	}
	useCount := func(meta map[string]types.AttributeValue) uint64 {
		t.Helper()
		n, err := parseN(meta[attrUseCount])
		require.NoError(t, err)
		return n
	}

	// Before the last use: counted, still active, the copy in place.
	first := model.MustGenerateUserID()
	result, err := testStore.JoinWithInvite(ctx, groupID, first, invite.ID, secret, now)
	require.NoError(t, err)
	require.True(t, result.Changed)
	meta := item(skMeta)
	require.EqualValues(t, 1, useCount(meta))
	require.Contains(t, meta, attrActiveSort)
	require.NotEmpty(t, item(skInviteKey))

	// A member's join writes nothing.
	result, err = testStore.JoinWithInvite(ctx, groupID, first, invite.ID, secret, now)
	require.NoError(t, err)
	require.False(t, result.Changed)
	require.Equal(t, meta, item(skMeta))

	// The last use: out of the active index, and the copy gone, while the
	// record keeps its other index attributes.
	result, err = testStore.JoinWithInvite(ctx, groupID, model.MustGenerateUserID(), invite.ID, secret, now)
	require.NoError(t, err)
	require.True(t, result.Changed)
	meta = item(skMeta)
	require.EqualValues(t, 2, useCount(meta))
	require.NotContains(t, meta, attrActiveSort)
	require.Contains(t, meta, attrCreatedSort)
	require.Contains(t, meta, attrCreatorSort)
	require.Empty(t, item(skInviteKey))

	// The joiners' envelopes carry no index attribute of the invite's.
	out, err := testEnv.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(keyEnvelopesTable),
		Key:            keyEnvelopeKey(groupID, first),
		ConsistentRead: aws.Bool(true),
	})
	require.NoError(t, err)
	for _, attr := range []string{attrChat, attrCreatedSort, attrCreatorSort, attrActiveSort, attrExpiresAt} {
		require.NotContains(t, out.Item, attr)
	}
	require.Equal(t, invite.ID.Value, asB(out.Item[attrInviteID]))
}

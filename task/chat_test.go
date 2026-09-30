package task_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	accountmemory "github.com/code-payments/flipcash2-server/account/memory"
	badgememory "github.com/code-payments/flipcash2-server/badge/memory"
	"github.com/code-payments/flipcash2-server/blob"
	blobmemory "github.com/code-payments/flipcash2-server/blob/memory"
	blocklistmemory "github.com/code-payments/flipcash2-server/blocklist/memory"
	"github.com/code-payments/flipcash2-server/chat"
	chatmemory "github.com/code-payments/flipcash2-server/chat/memory"
	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/intent"
	"github.com/code-payments/flipcash2-server/messaging"
	messagingmemory "github.com/code-payments/flipcash2-server/messaging/memory"
	"github.com/code-payments/flipcash2-server/model"
	profilememory "github.com/code-payments/flipcash2-server/profile/memory"
	"github.com/code-payments/flipcash2-server/push"
	"github.com/code-payments/flipcash2-server/task"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
)

func TestExecutor_SendContactDmPaymentMessage(t *testing.T) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	accounts := accountmemory.NewInMemory()
	badges := badgememory.NewInMemory()
	blocklists := blocklistmemory.NewInMemory()
	chats := chatmemory.NewInMemory(nil)
	messages := messagingmemory.NewInMemory()
	profiles := profilememory.NewInMemory()
	ocpData := ocp_data.NewTestDataProvider()
	bus := event.NewBus[*commonpb.UserId, *eventpb.Event]()
	chatBus := event.NewBus[*commonpb.ChatId, *eventpb.ChatEvent]()

	media := blob.NewIntegration(blobmemory.NewInMemory(), blobmemory.NewInMemoryStorage(), blobmemory.NewInMemoryAccessStore())
	sender := messaging.NewSender(log, badges, chats, messages, profiles, blocklists, media, ocpData, push.NewNoOpPusher(), bus, chatBus, nil)
	executor := task.NewExecutor(accounts, chats, sender, ocpData)
	integration := intent.NewIntegration(accounts, chats, profiles, nil, nil)

	senderUserID := model.MustGenerateUserID()
	senderKeys := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, senderUserID, senderKeys.Proto())
	require.NoError(t, err)

	recipientUserID := model.MustGenerateUserID()
	recipientKeys := model.MustGenerateKeyPair()
	_, err = accounts.Bind(ctx, recipientUserID, recipientKeys.Proto())
	require.NoError(t, err)

	chatID := chat.MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, senderUserID, recipientUserID)

	rawIntentID := []byte(model.MustGenerateKeyPair().Public())
	intentID := base58.Encode(rawIntentID)

	appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
		Domain: &intentpb.AppMetadata_Chat{
			Chat: &intentpb.ChatMetadata{
				ChatId: chatID,
				Type: &intentpb.ChatMetadata_ContactDmPayment_{
					ContactDmPayment: &intentpb.ChatMetadata_ContactDmPayment{
						Source:      &commonpb.PhoneNumber{Value: "+12223334444"},
						Destination: &commonpb.PhoneNumber{Value: "+13334445555"},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	intentRecord := &ocp_intent.Record{
		IntentId:              intentID,
		IntentType:            ocp_intent.SendPublicPayment,
		MintAccount:           base58.Encode(model.MustGenerateKeyPair().Public()),
		InitiatorOwnerAccount: base58.Encode(senderKeys.Public()),
		SendPublicPaymentMetadata: &ocp_intent.SendPublicPaymentMetadata{
			DestinationOwnerAccount: base58.Encode(recipientKeys.Public()),
			DestinationTokenAccount: base58.Encode(model.MustGenerateKeyPair().Public()),
			Quantity:                10_000,
			ExchangeCurrency:        "usd",
			ExchangeRate:            1.0,
			NativeAmount:            1.0,
			UsdMarketValue:          1.0,
		},
		AppMetadata: appMetadata,
		State:       ocp_intent.StatePending,
	}
	require.NoError(t, ocpData.SaveIntent(ctx, intentRecord))

	tasks, err := integration.GetTasksToSchedule(ctx, intentRecord)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, intent.TaskTypeSendContactDmPaymentMessage, tasks[0].Type)
	_, err = uuid.Parse(tasks[0].TaskId)
	assert.NoError(t, err)
	require.NotNil(t, tasks[0].ReferenceId)
	assert.Equal(t, intentID, *tasks[0].ReferenceId)
	assert.Empty(t, tasks[0].Data)
	require.NoError(t, tasks[0].Validate())

	// Executing the task creates the canonical DM and injects the cash message.
	require.NoError(t, executor.Execute(ctx, tasks[0]))

	_, err = chats.GetChatByID(ctx, chatID)
	require.NoError(t, err)

	msgs, err := messages.GetMessages(ctx, chatID)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Len(t, msgs[0].Content, 1)
	require.True(t, proto.Equal(senderUserID, msgs[0].SenderID))

	cash := msgs[0].Content[0].GetCash()
	require.NotNil(t, cash)
	assert.Equal(t, rawIntentID, cash.IntentId.GetValue())
	assert.EqualValues(t, 10_000, cash.Amount.GetQuarks())
	assert.Equal(t, "usd", cash.Amount.GetCurrency())
	assert.EqualValues(t, 1.0, cash.Amount.GetNativeAmount())
	assert.Equal(t, messagingpb.CashContent_SENT, cash.GetVerb())

	// Tasks are delivered at least once, so re-execution must not duplicate
	// the message.
	require.NoError(t, executor.Execute(ctx, tasks[0]))

	msgs, err = messages.GetMessages(ctx, chatID)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
}

func TestExecutor_SendTipDmPaymentMessage(t *testing.T) {
	// Only an explicit TIP is a tip: an unset action is a plain send from
	// either location. A payment created before the cutoff keeps the old rule,
	// where an unset action from the Flipcard was a tip. An explicit action
	// decides the verb either way.
	now := time.Now()
	legacy := time.Date(2026, time.September, 29, 23, 59, 59, 0, time.UTC)
	for _, tc := range []struct {
		name         string
		location     intentpb.ChatMetadata_DmPayment_Location
		action       intentpb.ChatMetadata_DmPayment_Action
		createdAt    time.Time
		expectedVerb messagingpb.CashContent_Verb
	}{
		{"flipcard", intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.ChatMetadata_DmPayment_DEFAULT, now, messagingpb.CashContent_SENT},
		{"chat", intentpb.ChatMetadata_DmPayment_CHAT, intentpb.ChatMetadata_DmPayment_DEFAULT, now, messagingpb.CashContent_SENT},
		{"flipcard_send", intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.ChatMetadata_DmPayment_SEND, now, messagingpb.CashContent_SENT},
		{"flipcard_tip", intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.ChatMetadata_DmPayment_TIP, now, messagingpb.CashContent_TIPPED},
		{"chat_tip", intentpb.ChatMetadata_DmPayment_CHAT, intentpb.ChatMetadata_DmPayment_TIP, now, messagingpb.CashContent_TIPPED},
		{"legacy_flipcard", intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.ChatMetadata_DmPayment_DEFAULT, legacy, messagingpb.CashContent_TIPPED},
		{"legacy_chat", intentpb.ChatMetadata_DmPayment_CHAT, intentpb.ChatMetadata_DmPayment_DEFAULT, legacy, messagingpb.CashContent_SENT},
		{"legacy_flipcard_send", intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.ChatMetadata_DmPayment_SEND, legacy, messagingpb.CashContent_SENT},
		{"legacy_chat_tip", intentpb.ChatMetadata_DmPayment_CHAT, intentpb.ChatMetadata_DmPayment_TIP, legacy, messagingpb.CashContent_TIPPED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testExecutor_SendTipDmPaymentMessage(t, tc.location, tc.action, tc.createdAt, tc.expectedVerb, false)
		})
	}

	// A payment to the team account creates the DM with the team excluded
	// from the feed, by the store's configuration alone.
	t.Run("to_team", func(t *testing.T) {
		testExecutor_SendTipDmPaymentMessage(t, intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.ChatMetadata_DmPayment_TIP, now, messagingpb.CashContent_TIPPED, true)
	})
}

// testExecutor_SendTipDmPaymentMessage pays a tip DM payment through the task.
// toTeam makes the recipient the team account, which the chat store is built
// to exclude from the feed as the parent builds it.
func testExecutor_SendTipDmPaymentMessage(t *testing.T, location intentpb.ChatMetadata_DmPayment_Location, action intentpb.ChatMetadata_DmPayment_Action, createdAt time.Time, expectedVerb messagingpb.CashContent_Verb, toTeam bool) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	accounts := accountmemory.NewInMemory()
	badges := badgememory.NewInMemory()
	messages := messagingmemory.NewInMemory()
	profiles := profilememory.NewInMemory()
	ocpData := ocp_data.NewTestDataProvider()
	bus := event.NewBus[*commonpb.UserId, *eventpb.Event]()
	chatBus := event.NewBus[*commonpb.ChatId, *eventpb.ChatEvent]()

	media := blob.NewIntegration(blobmemory.NewInMemory(), blobmemory.NewInMemoryStorage(), blobmemory.NewInMemoryAccessStore())
	blocklists := blocklistmemory.NewInMemory()

	senderUserID := model.MustGenerateUserID()
	senderKeys := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, senderUserID, senderKeys.Proto())
	require.NoError(t, err)

	recipientUserID := model.MustGenerateUserID()
	recipientKeys := model.MustGenerateKeyPair()
	_, err = accounts.Bind(ctx, recipientUserID, recipientKeys.Proto())
	require.NoError(t, err)

	var teamUserID *commonpb.UserId
	if toTeam {
		teamUserID = recipientUserID
	}
	chats := chatmemory.NewInMemory([]*commonpb.UserId{teamUserID})
	sender := messaging.NewSender(log, badges, chats, messages, profiles, blocklists, media, ocpData, push.NewNoOpPusher(), bus, chatBus, teamUserID)
	executor := task.NewExecutor(accounts, chats, sender, ocpData)
	integration := intent.NewIntegration(accounts, chats, profiles, nil, nil)

	chatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, senderUserID, recipientUserID)

	rawIntentID := []byte(model.MustGenerateKeyPair().Public())
	intentID := base58.Encode(rawIntentID)

	appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
		Domain: &intentpb.AppMetadata_Chat{
			Chat: &intentpb.ChatMetadata{
				ChatId: chatID,
				Type: &intentpb.ChatMetadata_DmPayment_{
					DmPayment: &intentpb.ChatMetadata_DmPayment{
						Location: location,
						Action:   action,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	intentRecord := &ocp_intent.Record{
		IntentId:              intentID,
		IntentType:            ocp_intent.SendPublicPayment,
		MintAccount:           base58.Encode(model.MustGenerateKeyPair().Public()),
		InitiatorOwnerAccount: base58.Encode(senderKeys.Public()),
		SendPublicPaymentMetadata: &ocp_intent.SendPublicPaymentMetadata{
			DestinationOwnerAccount: base58.Encode(recipientKeys.Public()),
			DestinationTokenAccount: base58.Encode(model.MustGenerateKeyPair().Public()),
			Quantity:                10_000,
			ExchangeCurrency:        "usd",
			ExchangeRate:            1.0,
			NativeAmount:            1.0,
			UsdMarketValue:          1.0,
		},
		AppMetadata: appMetadata,
		State:       ocp_intent.StatePending,
		CreatedAt:   createdAt,
	}
	require.NoError(t, ocpData.SaveIntent(ctx, intentRecord))

	tasks, err := integration.GetTasksToSchedule(ctx, intentRecord)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, intent.TaskTypeSendTipDmPaymentMessage, tasks[0].Type)
	require.NotNil(t, tasks[0].ReferenceId)
	assert.Equal(t, intentID, *tasks[0].ReferenceId)
	require.NoError(t, tasks[0].Validate())

	// Executing the task creates the canonical tip DM and injects the cash
	// message.
	require.NoError(t, executor.Execute(ctx, tasks[0]))

	created, err := chats.GetChatByID(ctx, chatID)
	require.NoError(t, err)
	assert.Equal(t, chatpb.ChatType_DM, created.Type)

	// Only the team account is ever excluded from the feed, and it is a
	// member all the same.
	recipientFeed, err := chats.GetDmFeedPage(ctx, recipientUserID, chatpb.ChatType_DM, time.Now().Add(time.Hour), nil, 0)
	require.NoError(t, err)
	if toTeam {
		require.Empty(t, recipientFeed)
	} else {
		require.Len(t, recipientFeed, 1)
	}
	isMember, err := chats.IsMember(ctx, chatID, recipientUserID)
	require.NoError(t, err)
	require.True(t, isMember)

	msgs, err := messages.GetMessages(ctx, chatID)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Len(t, msgs[0].Content, 1)
	require.True(t, proto.Equal(senderUserID, msgs[0].SenderID))

	cash := msgs[0].Content[0].GetCash()
	require.NotNil(t, cash)
	assert.Equal(t, rawIntentID, cash.IntentId.GetValue())
	assert.EqualValues(t, 10_000, cash.Amount.GetQuarks())
	assert.Equal(t, expectedVerb, cash.GetVerb())

	// Tasks are delivered at least once, so re-execution must not duplicate
	// the message.
	require.NoError(t, executor.Execute(ctx, tasks[0]))

	msgs, err = messages.GetMessages(ctx, chatID)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
}

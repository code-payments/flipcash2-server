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

	"github.com/code-payments/flipcash2-server/account"
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
	ocp_integration "github.com/code-payments/ocp-server/ocp/integration"
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
	integration := intent.NewIntegration(accounts, chats, messages, profiles, nil, nil, nil)

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
	assert.True(t, proto.Equal(recipientUserID, cash.GetReceiver()))
	assert.Equal(t, messagingpb.CashContent_SENT, cash.GetVerb())

	// Tasks are delivered at least once, so re-execution must not duplicate
	// the message.
	require.NoError(t, executor.Execute(ctx, tasks[0]))

	msgs, err = messages.GetMessages(ctx, chatID)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
}

func TestExecutor_SendDmPaymentMessage(t *testing.T) {
	// Only an explicit TIP is a tip: an unset action is a plain send from
	// either location. A payment created before the cutoff keeps the old rule,
	// where an unset action from the Flipcard was a tip. An explicit action
	// decides the verb either way.
	now := time.Now()
	legacy := time.Date(2026, time.September, 29, 23, 59, 59, 0, time.UTC)
	for _, tc := range []struct {
		name         string
		location     intentpb.ChatMetadata_DmPayment_Location
		action       intentpb.PaymentAction
		createdAt    time.Time
		expectedVerb messagingpb.CashContent_Verb
	}{
		{"flipcard", intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.PaymentAction_DEFAULT, now, messagingpb.CashContent_SENT},
		{"chat", intentpb.ChatMetadata_DmPayment_CHAT, intentpb.PaymentAction_DEFAULT, now, messagingpb.CashContent_SENT},
		{"flipcard_send", intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.PaymentAction_SEND, now, messagingpb.CashContent_SENT},
		{"flipcard_tip", intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.PaymentAction_TIP, now, messagingpb.CashContent_TIPPED},
		{"chat_tip", intentpb.ChatMetadata_DmPayment_CHAT, intentpb.PaymentAction_TIP, now, messagingpb.CashContent_TIPPED},
		{"legacy_flipcard", intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.PaymentAction_DEFAULT, legacy, messagingpb.CashContent_TIPPED},
		{"legacy_chat", intentpb.ChatMetadata_DmPayment_CHAT, intentpb.PaymentAction_DEFAULT, legacy, messagingpb.CashContent_SENT},
		{"legacy_flipcard_send", intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.PaymentAction_SEND, legacy, messagingpb.CashContent_SENT},
		{"legacy_chat_tip", intentpb.ChatMetadata_DmPayment_CHAT, intentpb.PaymentAction_TIP, legacy, messagingpb.CashContent_TIPPED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testExecutor_SendDmPaymentMessage(t, tc.location, tc.action, tc.createdAt, tc.expectedVerb, false)
		})
	}

	// A payment to the team account creates the DM with the team excluded
	// from the feed, by the store's configuration alone.
	t.Run("to_team", func(t *testing.T) {
		testExecutor_SendDmPaymentMessage(t, intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.PaymentAction_TIP, now, messagingpb.CashContent_TIPPED, true)
	})
}

// testExecutor_SendDmPaymentMessage pays a DM payment through the task.
// toTeam makes the recipient the team account, which the chat store is built
// to exclude from the feed as the parent builds it.
func testExecutor_SendDmPaymentMessage(t *testing.T, location intentpb.ChatMetadata_DmPayment_Location, action intentpb.PaymentAction, createdAt time.Time, expectedVerb messagingpb.CashContent_Verb, toTeam bool) {
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
	integration := intent.NewIntegration(accounts, chats, messages, profiles, nil, nil, nil)

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
	assert.Equal(t, intent.TaskTypeSendDmPaymentMessage, tasks[0].Type)
	require.NotNil(t, tasks[0].ReferenceId)
	assert.Equal(t, intentID, *tasks[0].ReferenceId)
	require.NoError(t, tasks[0].Validate())

	// Executing the task creates the canonical DM and injects the cash
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
	assert.True(t, proto.Equal(recipientUserID, cash.GetReceiver()))
	assert.Equal(t, expectedVerb, cash.GetVerb())

	// Tasks are delivered at least once, so re-execution must not duplicate
	// the message.
	require.NoError(t, executor.Execute(ctx, tasks[0]))

	msgs, err = messages.GetMessages(ctx, chatID)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
}

// staffAccounts marks the users in staff as staff, which multi payments
// require while they roll out.
type staffAccounts struct {
	account.Store

	staff map[string]bool
}

func (s *staffAccounts) IsStaff(_ context.Context, userID *commonpb.UserId) (bool, error) {
	return s.staff[string(userID.Value)], nil
}

// groupPaymentEnv executes group payment tasks against in-memory stores.
type groupPaymentEnv struct {
	ctx         context.Context
	accounts    *staffAccounts
	chats       chat.Store
	messages    messaging.Store
	ocpData     ocp_data.Provider
	executor    ocp_integration.TaskExecutor
	integration ocp_integration.SubmitIntent
}

func newGroupPaymentEnv(t *testing.T) *groupPaymentEnv {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	accounts := &staffAccounts{Store: accountmemory.NewInMemory(), staff: make(map[string]bool)}
	chats := chatmemory.NewInMemory(nil)
	messages := messagingmemory.NewInMemory()
	profiles := profilememory.NewInMemory()
	ocpData := ocp_data.NewTestDataProvider()
	bus := event.NewBus[*commonpb.UserId, *eventpb.Event]()
	chatBus := event.NewBus[*commonpb.ChatId, *eventpb.ChatEvent]()

	media := blob.NewIntegration(blobmemory.NewInMemory(), blobmemory.NewInMemoryStorage(), blobmemory.NewInMemoryAccessStore())
	sender := messaging.NewSender(log, badgememory.NewInMemory(), chats, messages, profiles, blocklistmemory.NewInMemory(), media, ocpData, push.NewNoOpPusher(), bus, chatBus, nil)
	access := chat.NewAccess(chats, chat.NewRuleEvaluator(accounts, nil, chats, nil), chat.WithSpeakerAdmissionTTL(0))

	return &groupPaymentEnv{
		ctx:         ctx,
		accounts:    accounts,
		chats:       chats,
		messages:    messages,
		ocpData:     ocpData,
		executor:    task.NewExecutor(accounts, chats, sender, ocpData),
		integration: intent.NewIntegration(accounts, chats, messages, profiles, access, nil, nil),
	}
}

func (e *groupPaymentEnv) bindUser(t *testing.T) (*commonpb.UserId, model.KeyPair) {
	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	_, err := e.accounts.Bind(e.ctx, userID, keys.Proto())
	require.NoError(t, err)
	return userID, keys
}

// pay validates and saves a payment from one user to another carrying
// chatMetadata, returning the intent record and its raw ID.
func (e *groupPaymentEnv) pay(t *testing.T, chatMetadata *intentpb.ChatMetadata, from, to model.KeyPair) (*ocp_intent.Record, []byte) {
	appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
		Domain: &intentpb.AppMetadata_Chat{Chat: chatMetadata},
	})
	require.NoError(t, err)

	rawIntentID := []byte(model.MustGenerateKeyPair().Public())
	intentRecord := &ocp_intent.Record{
		IntentId:              base58.Encode(rawIntentID),
		IntentType:            ocp_intent.SendPublicPayment,
		MintAccount:           base58.Encode(model.MustGenerateKeyPair().Public()),
		InitiatorOwnerAccount: base58.Encode(from.Public()),
		SendPublicPaymentMetadata: &ocp_intent.SendPublicPaymentMetadata{
			DestinationOwnerAccount: base58.Encode(to.Public()),
			DestinationTokenAccount: base58.Encode(model.MustGenerateKeyPair().Public()),
			Quantity:                25_000,
			ExchangeCurrency:        "usd",
			ExchangeRate:            1.0,
			NativeAmount:            2.5,
			UsdMarketValue:          2.5,
		},
		AppMetadata: appMetadata,
		State:       ocp_intent.StatePending,
		CreatedAt:   time.Now(),
	}
	require.NoError(t, e.ocpData.SaveIntent(e.ctx, intentRecord))
	return intentRecord, rawIntentID
}

// multiPayment is one destination of a multi payment made by multiPay.
type multiPayment struct {
	to     model.KeyPair
	quarks uint64
}

// multiPay validates and saves a multi payment carrying chatMetadata from one
// user to several, each paid its quarks at a USD rate of 10_000 quarks to the
// dollar, returning the intent record.
func (e *groupPaymentEnv) multiPay(t *testing.T, chatMetadata *intentpb.ChatMetadata, from model.KeyPair, payments ...multiPayment) *ocp_intent.Record {
	appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
		Domain: &intentpb.AppMetadata_Chat{Chat: chatMetadata},
	})
	require.NoError(t, err)

	metadata := &ocp_intent.SendPublicMultiPaymentMetadata{
		Source:           base58.Encode(model.MustGenerateKeyPair().Public()),
		ExchangeCurrency: "usd",
		ExchangeRate:     1.0,
	}
	for _, payment := range payments {
		usd := float64(payment.quarks) / 10_000
		metadata.Payments = append(metadata.Payments, &ocp_intent.Payment{
			DestinationOwnerAccount: base58.Encode(payment.to.Public()),
			DestinationTokenAccount: base58.Encode(model.MustGenerateKeyPair().Public()),
			Quantity:                payment.quarks,
			NativeAmount:            usd,
			UsdMarketValue:          usd,
		})
		metadata.Quantity += payment.quarks
		metadata.NativeAmount += usd
		metadata.UsdMarketValue += usd
	}

	intentRecord := &ocp_intent.Record{
		IntentId:                       base58.Encode(model.MustGenerateKeyPair().Public()),
		IntentType:                     ocp_intent.SendPublicMultiPayment,
		MintAccount:                    base58.Encode(model.MustGenerateKeyPair().Public()),
		InitiatorOwnerAccount:          base58.Encode(from.Public()),
		SendPublicMultiPaymentMetadata: metadata,
		AppMetadata:                    appMetadata,
		State:                          ocp_intent.StatePending,
		CreatedAt:                      time.Now(),
	}
	require.NoError(t, e.ocpData.SaveIntent(e.ctx, intentRecord))
	return intentRecord
}

func groupPaymentMetadata(chatID *commonpb.ChatId) *intentpb.ChatMetadata {
	return &intentpb.ChatMetadata{
		ChatId: chatID,
		Type: &intentpb.ChatMetadata_GroupPayment_{
			GroupPayment: &intentpb.ChatMetadata_GroupPayment{Action: intentpb.PaymentAction_SEND},
		},
	}
}

func TestExecutor_SendGroupPaymentMessage(t *testing.T) {
	e := newGroupPaymentEnv(t)

	payerUserID, payerKeys := e.bindUser(t)
	recipientUserID, recipientKeys := e.bindUser(t)
	bystanderUserID, _ := e.bindUser(t)

	groupID := chat.MustGenerateGroupChatID()
	require.NoError(t, e.chats.PutChat(e.ctx, &chat.Chat{
		ID:           groupID,
		Type:         chatpb.ChatType_GROUP,
		Title:        "Group",
		CreatorID:    bystanderUserID,
		Members:      []*commonpb.UserId{bystanderUserID, payerUserID, recipientUserID},
		LastActivity: time.Now(),
	}))

	// A validated payment schedules the group task, and executing it posts
	// the cash message into the group as the payer, naming the recipient.
	intentRecord, rawIntentID := e.pay(t, groupPaymentMetadata(groupID), payerKeys, recipientKeys)
	require.NoError(t, e.integration.AllowCreation(e.ctx, intentRecord, nil, nil))

	tasks, err := e.integration.GetTasksToSchedule(e.ctx, intentRecord)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, intent.TaskTypeSendGroupPaymentMessage, tasks[0].Type)

	require.NoError(t, e.executor.Execute(e.ctx, tasks[0]))

	msgs, err := e.messages.GetMessages(e.ctx, groupID)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Len(t, msgs[0].Content, 1)
	require.True(t, proto.Equal(payerUserID, msgs[0].SenderID))

	cash := msgs[0].Content[0].GetCash()
	require.NotNil(t, cash)
	assert.Equal(t, rawIntentID, cash.IntentId.GetValue())
	assert.EqualValues(t, 25_000, cash.Amount.GetQuarks())
	assert.Equal(t, "usd", cash.Amount.GetCurrency())
	assert.EqualValues(t, 2.5, cash.Amount.GetNativeAmount())
	assert.True(t, proto.Equal(recipientUserID, cash.GetReceiver()))
	assert.Equal(t, messagingpb.CashContent_SENT, cash.GetVerb())

	// Tasks are delivered at least once, so re-execution must not duplicate
	// the message.
	require.NoError(t, e.executor.Execute(e.ctx, tasks[0]))
	msgs, err = e.messages.GetMessages(e.ctx, groupID)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	// Standing is judged at validation: once a payment is made, its message
	// lands even if both parties have left the group before the task runs.
	intentRecord, _ = e.pay(t, groupPaymentMetadata(groupID), payerKeys, recipientKeys)
	require.NoError(t, e.integration.AllowCreation(e.ctx, intentRecord, nil, nil))
	tasks, err = e.integration.GetTasksToSchedule(e.ctx, intentRecord)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	for _, userID := range []*commonpb.UserId{payerUserID, recipientUserID} {
		_, _, err := e.chats.RemoveGroupMember(e.ctx, groupID, userID, false)
		require.NoError(t, err)
	}
	require.NoError(t, e.executor.Execute(e.ctx, tasks[0]))

	msgs, err = e.messages.GetMessages(e.ctx, groupID)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	require.True(t, proto.Equal(payerUserID, msgs[1].SenderID))
	assert.True(t, proto.Equal(recipientUserID, msgs[1].Content[0].GetCash().GetReceiver()))
}

// A group payment task only ever sends a group payment into a group: one that
// references any other payment, or a chat that is not a group, fails rather
// than sending.
func TestExecutor_SendGroupPaymentMessage_Mismatched(t *testing.T) {
	e := newGroupPaymentEnv(t)

	payerUserID, payerKeys := e.bindUser(t)
	recipientUserID, recipientKeys := e.bindUser(t)
	dmChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, payerUserID, recipientUserID)

	dmPayment, _ := e.pay(t, &intentpb.ChatMetadata{
		ChatId: dmChatID,
		Type: &intentpb.ChatMetadata_DmPayment_{
			DmPayment: &intentpb.ChatMetadata_DmPayment{Action: intentpb.PaymentAction_SEND},
		},
	}, payerKeys, recipientKeys)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewSendGroupPaymentMessageTask(dmPayment)), "not a group payment")

	groupPaymentToDm, _ := e.pay(t, groupPaymentMetadata(dmChatID), payerKeys, recipientKeys)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewSendGroupPaymentMessageTask(groupPaymentToDm)), "chat id is not a group")

	// And a DM payment task refuses a group payment.
	groupPayment, _ := e.pay(t, groupPaymentMetadata(chat.MustGenerateGroupChatID()), payerKeys, recipientKeys)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewSendDmPaymentMessageTask(groupPayment)), "not a dm payment")

	msgs, err := e.messages.GetMessages(e.ctx, dmChatID)
	require.NoError(t, err)
	require.Empty(t, msgs)
}

func reactionBoostMetadata(chatID *commonpb.ChatId, messageID *messagingpb.MessageId, emoji string) *intentpb.ChatMetadata {
	return &intentpb.ChatMetadata{
		ChatId: chatID,
		Type: &intentpb.ChatMetadata_ReactionBoost_{
			ReactionBoost: &intentpb.ChatMetadata_ReactionBoost{
				MessageId: messageID,
				Emoji:     &messagingpb.Emoji{Value: emoji},
			},
		},
	}
}

// putText sends a text message into chatID as senderID.
func (e *groupPaymentEnv) putText(t *testing.T, chatID *commonpb.ChatId, senderID *commonpb.UserId) *messaging.Message {
	msg, _, err := e.messages.PutMessage(
		e.ctx,
		chatID,
		senderID,
		[]*messagingpb.Content{{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: "hello"}}}},
		time.Now(),
		&messagingpb.ClientMessageId{Value: model.MustGenerateUserID().Value},
		true,
	)
	require.NoError(t, err)
	return msg
}

func TestExecutor_RecordReactionBoost(t *testing.T) {
	e := newGroupPaymentEnv(t)

	creatorUserID, creatorKeys := e.bindUser(t)
	payerUserID, payerKeys := e.bindUser(t)

	groupID := chat.MustGenerateGroupChatID()
	require.NoError(t, e.chats.PutChat(e.ctx, &chat.Chat{
		ID:           groupID,
		Type:         chatpb.ChatType_GROUP,
		Title:        "Group",
		CreatorID:    creatorUserID,
		Members:      []*commonpb.UserId{creatorUserID, payerUserID},
		LastActivity: time.Now(),
	}))
	msg := e.putText(t, groupID, creatorUserID)

	// A boost backs a reaction the payer already holds.
	_, _, _, err := e.messages.AddReaction(e.ctx, groupID, msg.ID, payerUserID, "🔥", time.Now())
	require.NoError(t, err)

	// A validated boost schedules the boost task, and executing it puts the
	// payment's whole USD value behind the payer's reaction.
	intentRecord, _ := e.pay(t, reactionBoostMetadata(groupID, msg.ID, "🔥"), payerKeys, creatorKeys)
	require.NoError(t, e.integration.AllowCreation(e.ctx, intentRecord, nil, nil))

	tasks, err := e.integration.GetTasksToSchedule(e.ctx, intentRecord)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, intent.TaskTypeRecordReactionBoost, tasks[0].Type)

	// The reaction is required when the payment is validated, not when the
	// boost lands: one removed in between is added back by the boost, since
	// the money has moved.
	_, removed, err := e.messages.RemoveReaction(e.ctx, groupID, msg.ID, payerUserID, "🔥")
	require.NoError(t, err)
	require.True(t, removed)

	require.NoError(t, e.executor.Execute(e.ctx, tasks[0]))

	requireBoost := func(count, boostMicros uint64) {
		summary, err := e.messages.GetReactionSummary(e.ctx, groupID, msg.ID)
		require.NoError(t, err)
		require.Len(t, summary, 1)
		assert.Equal(t, "🔥", summary[0].Emoji)
		assert.Equal(t, count, summary[0].Count)
		assert.Equal(t, boostMicros, summary[0].BoostUsdMicros)
		require.Len(t, summary[0].SampleReactors, 1)
		assert.True(t, proto.Equal(payerUserID, summary[0].SampleReactors[0].UserID))
		assert.Equal(t, boostMicros, summary[0].SampleReactors[0].BoostUsdMicros)
	}
	requireBoost(1, 2_500_000)

	// Tasks are delivered at least once, so re-execution must not credit the
	// boost twice.
	require.NoError(t, e.executor.Execute(e.ctx, tasks[0]))
	requireBoost(1, 2_500_000)

	// Standing is judged at validation: once a boost is paid, it lands even
	// if the payer has left the group before the task runs.
	intentRecord, _ = e.pay(t, reactionBoostMetadata(groupID, msg.ID, "🔥"), payerKeys, creatorKeys)
	require.NoError(t, e.integration.AllowCreation(e.ctx, intentRecord, nil, nil))
	tasks, err = e.integration.GetTasksToSchedule(e.ctx, intentRecord)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	_, _, err = e.chats.RemoveGroupMember(e.ctx, groupID, payerUserID, false)
	require.NoError(t, err)
	require.NoError(t, e.executor.Execute(e.ctx, tasks[0]))
	requireBoost(1, 5_000_000)
}

// A boost that pays both the message's sender and the group's creator is a
// multi payment, and the boost credits the whole of it, the creator's cut
// included.
func TestExecutor_RecordReactionBoost_MultiPayment(t *testing.T) {
	e := newGroupPaymentEnv(t)

	creatorUserID, creatorKeys := e.bindUser(t)
	senderUserID, senderKeys := e.bindUser(t)
	payerUserID, payerKeys := e.bindUser(t)
	e.accounts.staff[string(payerUserID.Value)] = true

	groupID := chat.MustGenerateGroupChatID()
	require.NoError(t, e.chats.PutChat(e.ctx, &chat.Chat{
		ID:           groupID,
		Type:         chatpb.ChatType_GROUP,
		Title:        "Group",
		CreatorID:    creatorUserID,
		Members:      []*commonpb.UserId{creatorUserID, senderUserID, payerUserID},
		LastActivity: time.Now(),
	}))
	msg := e.putText(t, groupID, senderUserID)
	_, _, _, err := e.messages.AddReaction(e.ctx, groupID, msg.ID, payerUserID, "🔥", time.Now())
	require.NoError(t, err)

	// $2.50: $2.00 to the sender and $0.50 to the creator.
	intentRecord := e.multiPay(t, reactionBoostMetadata(groupID, msg.ID, "🔥"), payerKeys,
		multiPayment{to: senderKeys, quarks: 20_000},
		multiPayment{to: creatorKeys, quarks: 5_000},
	)
	require.NoError(t, e.integration.AllowCreation(e.ctx, intentRecord, nil, nil))

	tasks, err := e.integration.GetTasksToSchedule(e.ctx, intentRecord)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, intent.TaskTypeRecordReactionBoost, tasks[0].Type)
	require.NoError(t, e.executor.Execute(e.ctx, tasks[0]))

	summary, err := e.messages.GetReactionSummary(e.ctx, groupID, msg.ID)
	require.NoError(t, err)
	require.Len(t, summary, 1)
	assert.EqualValues(t, 2_500_000, summary[0].BoostUsdMicros)
	require.Len(t, summary[0].SampleReactors, 1)
	assert.True(t, proto.Equal(payerUserID, summary[0].SampleReactors[0].UserID))
	assert.EqualValues(t, 2_500_000, summary[0].SampleReactors[0].BoostUsdMicros)
}

// A boost task only ever records a reaction boost in a group: one that
// references any other payment, or a chat that is not a group, fails rather
// than recording.
func TestExecutor_RecordReactionBoost_Mismatched(t *testing.T) {
	e := newGroupPaymentEnv(t)

	payerUserID, payerKeys := e.bindUser(t)
	recipientUserID, recipientKeys := e.bindUser(t)
	dmChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, payerUserID, recipientUserID)

	groupPayment, _ := e.pay(t, groupPaymentMetadata(chat.MustGenerateGroupChatID()), payerKeys, recipientKeys)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewRecordReactionBoostTask(groupPayment)), "not a reaction boost")

	boostInDm, _ := e.pay(t, reactionBoostMetadata(dmChatID, &messagingpb.MessageId{Value: 1}, "🔥"), payerKeys, recipientKeys)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewRecordReactionBoostTask(boostInDm)), "chat id is not a group")

	// And a group payment task refuses a boost.
	groupID := chat.MustGenerateGroupChatID()
	boost, _ := e.pay(t, reactionBoostMetadata(groupID, &messagingpb.MessageId{Value: 1}, "🔥"), payerKeys, recipientKeys)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewSendGroupPaymentMessageTask(boost)), "not a group payment")

	summary, err := e.messages.GetReactionSummary(e.ctx, groupID, &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)
	require.Empty(t, summary)
}

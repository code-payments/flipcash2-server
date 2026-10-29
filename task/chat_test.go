package task_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

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
	"github.com/code-payments/flipcash2-server/paymentrequest"
	paymentrequestmemory "github.com/code-payments/flipcash2-server/paymentrequest/memory"
	profilememory "github.com/code-payments/flipcash2-server/profile/memory"
	"github.com/code-payments/flipcash2-server/push"
	"github.com/code-payments/flipcash2-server/task"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_task "github.com/code-payments/ocp-server/ocp/data/task"
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
	requests := paymentrequestmemory.NewInMemory()
	executor := task.NewExecutor(accounts, chats, requests, sender, ocpData, chatBus)
	integration := intent.NewIntegration(accounts, chats, messages, profiles, requests, nil, nil, nil)

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
	assert.EqualValues(t, 10_000, cash.TotalAmount.GetQuarks())
	assert.Equal(t, "usd", cash.TotalAmount.GetCurrency())
	assert.EqualValues(t, 1.0, cash.TotalAmount.GetNativeAmount())
	assert.True(t, proto.Equal(recipientUserID, cash.GetSingleReceiver()))
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
	requests := paymentrequestmemory.NewInMemory()
	executor := task.NewExecutor(accounts, chats, requests, sender, ocpData, chatBus)
	integration := intent.NewIntegration(accounts, chats, messages, profiles, requests, nil, nil, nil)

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
	assert.EqualValues(t, 10_000, cash.TotalAmount.GetQuarks())
	assert.True(t, proto.Equal(recipientUserID, cash.GetSingleReceiver()))
	assert.Equal(t, expectedVerb, cash.GetVerb())

	// Tasks are delivered at least once, so re-execution must not duplicate
	// the message.
	require.NoError(t, executor.Execute(ctx, tasks[0]))

	msgs, err = messages.GetMessages(ctx, chatID)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
}

// staffAccounts marks the users in staff as staff, which a group payment's
// multi payment requires while it rolls out.
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
	requests    paymentrequest.Store
	ocpData     ocp_data.Provider
	executor    ocp_integration.TaskExecutor
	integration ocp_integration.SubmitIntent

	// chatObserver sees every event published on the chat-keyed bus.
	chatObserver *event.TestEventObserver[*commonpb.ChatId, *eventpb.ChatEvent]
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
	chatObserver := event.NewTestEventObserver[*commonpb.ChatId, *eventpb.ChatEvent]()
	chatBus.AddHandler(chatObserver)

	media := blob.NewIntegration(blobmemory.NewInMemory(), blobmemory.NewInMemoryStorage(), blobmemory.NewInMemoryAccessStore())
	sender := messaging.NewSender(log, badgememory.NewInMemory(), chats, messages, profiles, blocklistmemory.NewInMemory(), media, ocpData, push.NewNoOpPusher(), bus, chatBus, nil)
	access := chat.NewAccess(chats, chat.NewRuleEvaluator(accounts, nil, chats, nil), chat.WithSpeakerAdmissionTTL(0))
	requests := paymentrequestmemory.NewInMemory()

	return &groupPaymentEnv{
		ctx:         ctx,
		accounts:    accounts,
		chats:       chats,
		messages:    messages,
		requests:    requests,
		ocpData:     ocpData,
		executor:    task.NewExecutor(accounts, chats, requests, sender, ocpData, chatBus),
		integration: intent.NewIntegration(accounts, chats, messages, profiles, requests, access, nil, nil),

		chatObserver: chatObserver,
	}
}

// requestUpdates waits for r's PaymentRequestUpdates on its chat's topic to
// number n, then gives the bus a moment more and asserts no more arrived,
// returning them in publish order. Each is checked to be on r's chat topic,
// for r's chat, excluding no one.
func (e *groupPaymentEnv) requestUpdates(t *testing.T, r *paymentrequest.PaymentRequest, n int) []*paymentrequestpb.PaymentRequestUpdate {
	collect := func() []*paymentrequestpb.PaymentRequestUpdate {
		var updates []*paymentrequestpb.PaymentRequestUpdate
		for _, ev := range e.chatObserver.GetEvents(func(k *commonpb.ChatId) bool { return bytes.Equal(k.Value, r.ChatID.Value) }) {
			for _, u := range ev.Event.Event.GetChatUpdate().GetPaymentRequestUpdates().GetPaymentRequestUpdates() {
				if !bytes.Equal(u.PaymentRequestId.Value, r.ID.Value) {
					continue
				}
				require.Equal(t, r.ChatID.Value, ev.Event.ChatId.Value)
				require.Equal(t, r.ChatID.Value, ev.Event.Event.GetChatUpdate().GetChat().GetValue())
				require.Empty(t, ev.Event.ExcludeUserIds)
				require.NoError(t, ev.Event.Event.ValidateAll())
				updates = append(updates, u)
			}
		}
		return updates
	}
	if n > 0 {
		e.chatObserver.WaitFor(t, func([]*event.KeyAndEvent[*commonpb.ChatId, *eventpb.ChatEvent]) bool {
			return len(collect()) >= n
		})
	}
	time.Sleep(100 * time.Millisecond)
	updates := collect()
	require.Len(t, updates, n)
	return updates
}

// sharePaymentOf is the share an update's first change pays.
func sharePaymentOf(u *paymentrequestpb.PaymentRequestUpdate) *paymentrequestpb.Share {
	return u.GetUpdates().GetUpdates()[0].GetSharePaymentUpdate().GetShare()
}

// requireSharePaymentUpdate asserts that u is the change of r's share of userID being
// paid, at version, as stored names it, and, when completed, the request's
// completion after it at stored's ClosedAt; and nothing else.
func requireSharePaymentUpdate(t *testing.T, u *paymentrequestpb.PaymentRequestUpdate, stored *paymentrequest.PaymentRequest, userID *commonpb.UserId, completed bool) {
	changes := u.GetUpdates()
	require.NotNil(t, changes)
	assert.Equal(t, stored.Version, changes.Version)
	want := 1
	if completed {
		want = 2
	}
	require.Len(t, changes.Updates, want)
	assert.True(t, proto.Equal(stored.Share(userID), changes.Updates[0].GetSharePaymentUpdate().GetShare()))
	if completed {
		status := changes.Updates[1].GetStatusUpdate()
		require.NotNil(t, status)
		assert.Equal(t, paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_COMPLETED, status.Status)
		assert.True(t, stored.ClosedAt.Equal(status.ClosedAt.AsTime()))
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
	return e.payWithIntentID(t, chatMetadata, from, to, model.MustGenerateKeyPair().Public())
}

// payWithIntentID is pay under the intent ID rawIntentID.
func (e *groupPaymentEnv) payWithIntentID(t *testing.T, chatMetadata *intentpb.ChatMetadata, from, to model.KeyPair, rawIntentID []byte) (*ocp_intent.Record, []byte) {
	appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
		Domain: &intentpb.AppMetadata_Chat{Chat: chatMetadata},
	})
	require.NoError(t, err)

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
	assert.EqualValues(t, 25_000, cash.TotalAmount.GetQuarks())
	assert.Equal(t, "usd", cash.TotalAmount.GetCurrency())
	assert.EqualValues(t, 2.5, cash.TotalAmount.GetNativeAmount())
	assert.True(t, proto.Equal(recipientUserID, cash.GetSingleReceiver()))
	assert.Equal(t, messagingpb.CashContent_SENT, cash.GetVerb())
	assert.Nil(t, cash.GetPaymentRequestId())

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
	assert.True(t, proto.Equal(recipientUserID, msgs[1].Content[0].GetCash().GetSingleReceiver()))
}

// A group payment to several members at once posts one cash message listing
// every recipient with their part, in the payment's order, with the whole
// payment as its total.
func TestExecutor_SendGroupPaymentMessage_MultiPayment(t *testing.T) {
	e := newGroupPaymentEnv(t)

	payerUserID, payerKeys := e.bindUser(t)
	e.accounts.staff[string(payerUserID.Value)] = true
	recipient1UserID, recipient1Keys := e.bindUser(t)
	recipient2UserID, recipient2Keys := e.bindUser(t)
	recipient3UserID, recipient3Keys := e.bindUser(t)

	groupID := chat.MustGenerateGroupChatID()
	require.NoError(t, e.chats.PutChat(e.ctx, &chat.Chat{
		ID:           groupID,
		Type:         chatpb.ChatType_GROUP,
		Title:        "Group",
		CreatorID:    payerUserID,
		Members:      []*commonpb.UserId{payerUserID, recipient1UserID, recipient2UserID, recipient3UserID},
		LastActivity: time.Now(),
	}))

	// $1.00, $2.50 and $0.50: $4.00 in all.
	intentRecord := e.multiPay(t, groupPaymentMetadata(groupID), payerKeys,
		multiPayment{to: recipient2Keys, quarks: 10_000},
		multiPayment{to: recipient1Keys, quarks: 25_000},
		multiPayment{to: recipient3Keys, quarks: 5_000},
	)
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
	rawIntentID, err := base58.Decode(intentRecord.IntentId)
	require.NoError(t, err)
	assert.Equal(t, rawIntentID, cash.IntentId.GetValue())
	assert.EqualValues(t, 40_000, cash.TotalAmount.GetQuarks())
	assert.Equal(t, "usd", cash.TotalAmount.GetCurrency())
	assert.InDelta(t, 4.0, cash.TotalAmount.GetNativeAmount(), 1e-9)
	assert.Equal(t, messagingpb.CashContent_SENT, cash.GetVerb())
	assert.Nil(t, cash.GetSingleReceiver())

	items := cash.GetMultiReceiver().GetItems()
	require.Len(t, items, 3)
	for j, expected := range []struct {
		userID *commonpb.UserId
		usd    float64
		quarks uint64
	}{
		{recipient2UserID, 1.0, 10_000},
		{recipient1UserID, 2.5, 25_000},
		{recipient3UserID, 0.5, 5_000},
	} {
		assert.True(t, proto.Equal(expected.userID, items[j].GetReceiver()), j)
		assert.InDelta(t, expected.usd, items[j].GetNativeAmount(), 1e-9, j)
		assert.EqualValues(t, expected.quarks, items[j].GetQuarks(), j)
	}
	require.NoError(t, cash.ValidateAll())

	// Tasks are delivered at least once, so re-execution must not duplicate
	// the message.
	require.NoError(t, e.executor.Execute(e.ctx, tasks[0]))
	msgs, err = e.messages.GetMessages(e.ctx, groupID)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
}

// A payment message task fails, to be retried, rather than send a message it
// cannot make whole: a DM payment task for a payment to several people, or a
// group payment task naming a recipient who is not a Flipcash user.
func TestExecutor_SendPaymentMessage_InvalidRecipients(t *testing.T) {
	e := newGroupPaymentEnv(t)

	payerUserID, payerKeys := e.bindUser(t)
	recipient1UserID, recipient1Keys := e.bindUser(t)
	_, recipient2Keys := e.bindUser(t)

	dmChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, payerUserID, recipient1UserID)
	dmMultiPayment := e.multiPay(t, &intentpb.ChatMetadata{
		ChatId: dmChatID,
		Type: &intentpb.ChatMetadata_DmPayment_{
			DmPayment: &intentpb.ChatMetadata_DmPayment{Action: intentpb.PaymentAction_SEND},
		},
	}, payerKeys,
		multiPayment{to: recipient1Keys, quarks: 10_000},
		multiPayment{to: recipient2Keys, quarks: 10_000},
	)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewSendDmPaymentMessageTask(dmMultiPayment)), "dm payment has 2 recipients")

	msgs, err := e.messages.GetMessages(e.ctx, dmChatID)
	require.NoError(t, err)
	require.Empty(t, msgs)

	groupID := chat.MustGenerateGroupChatID()
	toStranger := e.multiPay(t, groupPaymentMetadata(groupID), payerKeys,
		multiPayment{to: recipient1Keys, quarks: 10_000},
		multiPayment{to: model.MustGenerateKeyPair(), quarks: 10_000},
	)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewSendGroupPaymentMessageTask(toStranger)), "is not a flipcash user")

	msgs, err = e.messages.GetMessages(e.ctx, groupID)
	require.NoError(t, err)
	require.Empty(t, msgs)
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

// A DM boost pays the other member for their message, and is recorded as a
// group's is: once, crediting the whole payment.
func TestExecutor_RecordReactionBoost_Dm(t *testing.T) {
	e := newGroupPaymentEnv(t)

	senderUserID, senderKeys := e.bindUser(t)
	payerUserID, payerKeys := e.bindUser(t)

	dmChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, senderUserID, payerUserID)
	require.NoError(t, e.chats.PutChat(e.ctx, &chat.Chat{
		ID:           dmChatID,
		Type:         chatpb.ChatType_DM,
		Members:      []*commonpb.UserId{senderUserID, payerUserID},
		LastActivity: time.Now(),
	}))
	msg := e.putText(t, dmChatID, senderUserID)
	_, _, _, err := e.messages.AddReaction(e.ctx, dmChatID, msg.ID, payerUserID, "🔥", time.Now())
	require.NoError(t, err)

	intentRecord, _ := e.pay(t, reactionBoostMetadata(dmChatID, msg.ID, "🔥"), payerKeys, senderKeys)
	require.NoError(t, e.integration.AllowCreation(e.ctx, intentRecord, nil, nil))
	tasks, err := e.integration.GetTasksToSchedule(e.ctx, intentRecord)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, intent.TaskTypeRecordReactionBoost, tasks[0].Type)

	for range 2 {
		require.NoError(t, e.executor.Execute(e.ctx, tasks[0]))

		summary, err := e.messages.GetReactionSummary(e.ctx, dmChatID, msg.ID)
		require.NoError(t, err)
		require.Len(t, summary, 1)
		assert.EqualValues(t, 1, summary[0].Count)
		assert.EqualValues(t, 2_500_000, summary[0].BoostUsdMicros)
		require.Len(t, summary[0].SampleReactors, 1)
		assert.True(t, proto.Equal(payerUserID, summary[0].SampleReactors[0].UserID))
		assert.EqualValues(t, 2_500_000, summary[0].SampleReactors[0].BoostUsdMicros)
	}
}

// A boost that pays both the message's sender and the group's creator is a
// multi payment, and the boost credits the whole of it, the creator's cut
// included.
func TestExecutor_RecordReactionBoost_MultiPayment(t *testing.T) {
	e := newGroupPaymentEnv(t)

	creatorUserID, creatorKeys := e.bindUser(t)
	senderUserID, senderKeys := e.bindUser(t)
	payerUserID, payerKeys := e.bindUser(t)

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

// A private group's boosts are recorded as a public group's are, on its
// encrypted messages, whichever payment carries them, each crediting the
// whole payment.
func TestExecutor_RecordReactionBoost_PrivateGroup(t *testing.T) {
	e := newGroupPaymentEnv(t)

	creatorUserID, creatorKeys := e.bindUser(t)
	senderUserID, senderKeys := e.bindUser(t)
	payerUserID, payerKeys := e.bindUser(t)

	groupID := chat.MustGenerateGroupChatID()
	require.NoError(t, e.chats.PutChat(e.ctx, &chat.Chat{
		ID:           groupID,
		Type:         chatpb.ChatType_GROUP,
		Title:        "Group",
		CreatorID:    creatorUserID,
		Members:      []*commonpb.UserId{creatorUserID, senderUserID, payerUserID},
		IsPrivate:    true,
		LastActivity: time.Now(),
	}))
	putEncrypted := func(senderID *commonpb.UserId) *messaging.Message {
		msg, _, err := e.messages.PutMessage(
			e.ctx,
			groupID,
			senderID,
			[]*messagingpb.Content{{Type: &messagingpb.Content_Encrypted{Encrypted: &messagingpb.EncryptedContent{
				Scheme:     messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305,
				Nonce:      make([]byte, 24),
				Ciphertext: []byte("ciphertext"),
			}}}},
			time.Now(),
			&messagingpb.ClientMessageId{Value: model.MustGenerateUserID().Value},
			true,
		)
		require.NoError(t, err)
		return msg
	}

	for _, tc := range []struct {
		name  string
		msg   *messaging.Message
		multi bool
	}{
		// $2.50 to the creator for their own message.
		{name: "single payment", msg: putEncrypted(creatorUserID)},
		// $2.50: $2.00 to the sender and $0.50 to the creator.
		{name: "multi payment", msg: putEncrypted(senderUserID), multi: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := e.messages.AddReaction(e.ctx, groupID, tc.msg.ID, payerUserID, "🔥", time.Now())
			require.NoError(t, err)

			metadata := reactionBoostMetadata(groupID, tc.msg.ID, "🔥")
			var intentRecord *ocp_intent.Record
			if tc.multi {
				intentRecord = e.multiPay(t, metadata, payerKeys,
					multiPayment{to: senderKeys, quarks: 20_000},
					multiPayment{to: creatorKeys, quarks: 5_000},
				)
			} else {
				intentRecord, _ = e.pay(t, metadata, payerKeys, creatorKeys)
			}
			require.NoError(t, e.integration.AllowCreation(e.ctx, intentRecord, nil, nil))

			tasks, err := e.integration.GetTasksToSchedule(e.ctx, intentRecord)
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			require.NoError(t, e.executor.Execute(e.ctx, tasks[0]))

			summary, err := e.messages.GetReactionSummary(e.ctx, groupID, tc.msg.ID)
			require.NoError(t, err)
			require.Len(t, summary, 1)
			assert.EqualValues(t, 2_500_000, summary[0].BoostUsdMicros)
		})
	}
}

// A boost task only ever records a reaction boost in a group or a DM: one
// that references any other payment, or a chat ID that is neither, fails
// rather than recording.
func TestExecutor_RecordReactionBoost_Mismatched(t *testing.T) {
	e := newGroupPaymentEnv(t)

	_, payerKeys := e.bindUser(t)
	_, recipientKeys := e.bindUser(t)

	groupPayment, _ := e.pay(t, groupPaymentMetadata(chat.MustGenerateGroupChatID()), payerKeys, recipientKeys)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewRecordReactionBoostTask(groupPayment)), "not a reaction boost")

	malformedID := &commonpb.ChatId{Value: make([]byte, 20)}
	malformed, _ := e.pay(t, reactionBoostMetadata(malformedID, &messagingpb.MessageId{Value: 1}, "🔥"), payerKeys, recipientKeys)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewRecordReactionBoostTask(malformed)), "not a group or a dm")
	malformedSummary, err := e.messages.GetReactionSummary(e.ctx, malformedID, &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)
	require.Empty(t, malformedSummary)

	// And a group payment task refuses a boost.
	groupID := chat.MustGenerateGroupChatID()
	boost, _ := e.pay(t, reactionBoostMetadata(groupID, &messagingpb.MessageId{Value: 1}, "🔥"), payerKeys, recipientKeys)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewSendGroupPaymentMessageTask(boost)), "not a group payment")

	summary, err := e.messages.GetReactionSummary(e.ctx, groupID, &messagingpb.MessageId{Value: 1})
	require.NoError(t, err)
	require.Empty(t, summary)
}

// putGroup creates a public group with no rules, created by creator, whose
// members are creator and members.
func (e *groupPaymentEnv) putGroup(t *testing.T, creator *commonpb.UserId, members ...*commonpb.UserId) *commonpb.ChatId {
	groupID := chat.MustGenerateGroupChatID()
	require.NoError(t, e.chats.PutChat(e.ctx, &chat.Chat{
		ID:           groupID,
		Type:         chatpb.ChatType_GROUP,
		Title:        "Group",
		CreatorID:    creator,
		Members:      append([]*commonpb.UserId{creator}, members...),
		LastActivity: time.Now(),
	}))
	return groupID
}

// putRequest creates an open USD split by creator in chatID, ending in an
// hour, asking each of debtors for $2.50, what pay pays.
func (e *groupPaymentEnv) putRequest(t *testing.T, chatID *commonpb.ChatId, creator *commonpb.UserId, debtors ...*commonpb.UserId) *paymentrequest.PaymentRequest {
	key := make([]byte, paymentrequest.IdempotencyKeySize)
	_, err := rand.Read(key)
	require.NoError(t, err)

	split := &paymentrequestpb.Split{
		Text: &paymentrequestpb.Split_Plaintext{
			Plaintext: &paymentrequestpb.PaymentRequestText{Title: "Dinner"},
		},
		Currency: "usd",
		EndsAt:   timestamppb.New(time.Now().Add(time.Hour)),
	}
	for _, debtor := range debtors {
		split.Shares = append(split.Shares, &paymentrequestpb.Share{UserId: debtor, NativeAmount: 2.5})
	}

	id := paymentrequest.MustDeriveID(creator, &paymentrequestpb.IdempotencyKey{Value: key})
	r, err := paymentrequest.NewSplitRequest(id, creator, chatID, split, time.Now())
	require.NoError(t, err)
	require.NoError(t, e.requests.Create(e.ctx, r))
	return r
}

func requestedPaymentMetadata(chatID *commonpb.ChatId, requestID *paymentrequestpb.PaymentRequestId) *intentpb.ChatMetadata {
	return &intentpb.ChatMetadata{
		ChatId: chatID,
		Type: &intentpb.ChatMetadata_RequestedPayment_{
			RequestedPayment: &intentpb.ChatMetadata_RequestedPayment{PaymentRequestId: requestID},
		},
	}
}

// payShare saves and validates a $2.50 payment of the payer's share of r,
// under the payment intent ID r holds for it, returning the intent record and
// its raw ID.
func (e *groupPaymentEnv) payShare(t *testing.T, r *paymentrequest.PaymentRequest, from, to model.KeyPair) (*ocp_intent.Record, []byte) {
	payerUserID, err := e.accounts.GetUserId(e.ctx, &commonpb.PublicKey{Value: from.Public()})
	require.NoError(t, err)
	intentID := r.PaymentIntentID(payerUserID)
	require.NotNil(t, intentID)

	intentRecord, rawIntentID := e.payWithIntentID(t, requestedPaymentMetadata(r.ChatID, r.ID), from, to, intentID)
	require.NoError(t, e.integration.AllowCreation(e.ctx, intentRecord, nil, nil))
	return intentRecord, rawIntentID
}

// scheduled returns the one task a validated intent schedules.
func (e *groupPaymentEnv) scheduled(t *testing.T, intentRecord *ocp_intent.Record) *ocp_task.Record {
	tasks, err := e.integration.GetTasksToSchedule(e.ctx, intentRecord)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	return tasks[0]
}

func TestExecutor_RecordRequestedPayment(t *testing.T) {
	e := newGroupPaymentEnv(t)

	creatorUserID, creatorKeys := e.bindUser(t)
	payerUserID, payerKeys := e.bindUser(t)
	otherUserID, otherKeys := e.bindUser(t)

	groupID := e.putGroup(t, creatorUserID, payerUserID, otherUserID)
	r := e.putRequest(t, groupID, creatorUserID, creatorUserID, payerUserID, otherUserID)

	// A validated payment schedules the requested payment task, and executing
	// it marks the payer's share paid with the payment's intent. No cash
	// message is posted into the group.
	intentRecord, rawIntentID := e.payShare(t, r, payerKeys, creatorKeys)
	task := e.scheduled(t, intentRecord)
	assert.Equal(t, intent.TaskTypeRecordRequestedPayment, task.Type)
	require.NoError(t, e.executor.Execute(e.ctx, task))

	stored, err := e.requests.Get(e.ctx, r.ID)
	require.NoError(t, err)
	assert.Equal(t, paymentrequest.StatusOpen, stored.Status)
	assert.EqualValues(t, 2, stored.Version)
	paid := stored.Share(payerUserID).GetPaid()
	require.NotNil(t, paid)
	assert.Equal(t, rawIntentID, paid.GetIntent().GetValue())
	assert.Nil(t, stored.Share(otherUserID).GetPaid())

	// The share was paid when the payment was made, not when the task ran.
	assert.True(t, intentRecord.CreatedAt.Equal(paid.PaidAt.AsTime()), "paid_at %s, intent created %s", paid.PaidAt.AsTime(), intentRecord.CreatedAt)

	msgs, err := e.messages.GetMessages(e.ctx, groupID)
	require.NoError(t, err)
	require.Empty(t, msgs)

	// The mark is announced on the group's topic: the share as paid, at the
	// request's new version.
	updates := e.requestUpdates(t, r, 1)
	requireSharePaymentUpdate(t, updates[0], stored, payerUserID, false)

	// Tasks are delivered at least once, so re-execution changes nothing and
	// announces nothing: a change is announced only by the write that made
	// it.
	require.NoError(t, e.executor.Execute(e.ctx, task))
	e.requestUpdates(t, r, 1)
	again, err := e.requests.Get(e.ctx, r.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 2, again.Version)
	assert.True(t, proto.Equal(paid, again.Share(payerUserID).GetPaid()))

	// The last share paid completes the request, closed when that payment
	// was made, not when its task ran.
	intentRecord, otherIntentID := e.payShare(t, r, otherKeys, creatorKeys)
	require.NoError(t, e.executor.Execute(e.ctx, e.scheduled(t, intentRecord)))

	stored, err = e.requests.Get(e.ctx, r.ID)
	require.NoError(t, err)
	assert.Equal(t, paymentrequest.StatusCompleted, stored.Status)
	assert.EqualValues(t, 3, stored.Version)
	assert.True(t, stored.AllSharesPaid())
	assert.Equal(t, otherIntentID, stored.Share(otherUserID).GetPaid().GetIntent().GetValue())
	assert.True(t, intentRecord.CreatedAt.Equal(stored.Share(otherUserID).GetPaid().PaidAt.AsTime()))
	require.NotNil(t, stored.ClosedAt)
	assert.True(t, intentRecord.CreatedAt.Equal(*stored.ClosedAt), "closed_at %s, intent created %s", *stored.ClosedAt, intentRecord.CreatedAt)

	msgs, err = e.messages.GetMessages(e.ctx, groupID)
	require.NoError(t, err)
	require.Empty(t, msgs)

	// Announced with the completion it made, after it.
	updates = e.requestUpdates(t, r, 2)
	requireSharePaymentUpdate(t, updates[1], stored, otherUserID, true)
}

// A second payment of a share made before the first's task has marked it
// passes validation, which reads the share unpaid, but cannot commit: it must
// use the share's one payment intent ID, which OCP stores at most once. So
// the share is paid once.
func TestExecutor_RecordRequestedPayment_OncePerShare(t *testing.T) {
	e := newGroupPaymentEnv(t)

	creatorUserID, creatorKeys := e.bindUser(t)
	payerUserID, payerKeys := e.bindUser(t)

	groupID := e.putGroup(t, creatorUserID, payerUserID)
	r := e.putRequest(t, groupID, creatorUserID, payerUserID)

	intentRecord, rawIntentID := e.payShare(t, r, payerKeys, creatorKeys)

	second := intentRecord.Clone()
	second.Id = 0
	second.Version = 0
	second.CreatedAt = time.Now()
	require.NoError(t, e.integration.AllowCreation(e.ctx, &second, nil, nil))
	require.ErrorIs(t, e.ocpData.SaveIntent(e.ctx, &second), ocp_intent.ErrStaleVersion)

	require.NoError(t, e.executor.Execute(e.ctx, e.scheduled(t, intentRecord)))

	stored, err := e.requests.Get(e.ctx, r.ID)
	require.NoError(t, err)
	assert.Equal(t, rawIntentID, stored.Share(payerUserID).GetPaid().GetIntent().GetValue())

	msgs, err := e.messages.GetMessages(e.ctx, groupID)
	require.NoError(t, err)
	require.Empty(t, msgs)
}

// A share the creator marks paid by hand between the payment's validation
// and its task is replaced by the payment, so the record shows the money that
// arrived.
func TestExecutor_RecordRequestedPayment_PaidByHand(t *testing.T) {
	e := newGroupPaymentEnv(t)

	creatorUserID, creatorKeys := e.bindUser(t)
	payerUserID, payerKeys := e.bindUser(t)

	groupID := e.putGroup(t, creatorUserID, payerUserID)
	r := e.putRequest(t, groupID, creatorUserID, payerUserID)

	intentRecord, rawIntentID := e.payShare(t, r, payerKeys, creatorKeys)

	manual := &paymentrequestpb.PaidShare{
		PaidAt: timestamppb.Now(),
		How:    &paymentrequestpb.PaidShare_Manual_{Manual: &paymentrequestpb.PaidShare_Manual{}},
	}
	marked, changed, _, err := e.requests.PayShare(e.ctx, r.ID, payerUserID, manual, time.Now())
	require.NoError(t, err)
	require.True(t, changed)

	require.Equal(t, paymentrequest.StatusCompleted, marked.Status)

	task := e.scheduled(t, intentRecord)
	require.NoError(t, e.executor.Execute(e.ctx, task))

	// The hand mark completed the request; the payment replaces it at the
	// next version and leaves the completion as it was.
	stored, err := e.requests.Get(e.ctx, r.ID)
	require.NoError(t, err)
	assert.Equal(t, marked.Version+1, stored.Version)
	assert.Equal(t, paymentrequest.StatusCompleted, stored.Status)
	assert.True(t, marked.ClosedAt.Equal(*stored.ClosedAt))
	paid := stored.Share(payerUserID).GetPaid()
	assert.Equal(t, rawIntentID, paid.GetIntent().GetValue())
	assert.True(t, intentRecord.CreatedAt.Equal(paid.PaidAt.AsTime()))

	// The replacement is announced at its version, as the share alone: the
	// request was already complete. The hand mark, made on the store
	// directly here, announced nothing.
	updates := e.requestUpdates(t, r, 1)
	requireSharePaymentUpdate(t, updates[0], stored, payerUserID, false)
	assert.NotNil(t, sharePaymentOf(updates[0]).GetPaid().GetIntent())

	// A repeat of the task changes nothing.
	require.NoError(t, e.executor.Execute(e.ctx, task))
	again, err := e.requests.Get(e.ctx, r.ID)
	require.NoError(t, err)
	assert.Equal(t, stored.Version, again.Version)

	msgs, err := e.messages.GetMessages(e.ctx, groupID)
	require.NoError(t, err)
	require.Empty(t, msgs)
}

// A request closed between the payment's validation and its task still has
// the share recorded, and stays closed, even if the payer has left the group
// in between.
func TestExecutor_RecordRequestedPayment_ClosedSinceValidation(t *testing.T) {
	e := newGroupPaymentEnv(t)

	creatorUserID, creatorKeys := e.bindUser(t)
	payerUserID, payerKeys := e.bindUser(t)
	otherUserID, _ := e.bindUser(t)

	groupID := e.putGroup(t, creatorUserID, payerUserID, otherUserID)
	r := e.putRequest(t, groupID, creatorUserID, payerUserID, otherUserID)

	intentRecord, rawIntentID := e.payShare(t, r, payerKeys, creatorKeys)

	closed, changed, err := e.requests.Close(e.ctx, r.ID, time.Now())
	require.NoError(t, err)
	require.True(t, changed)
	_, _, err = e.chats.RemoveGroupMember(e.ctx, groupID, payerUserID, false)
	require.NoError(t, err)

	require.NoError(t, e.executor.Execute(e.ctx, e.scheduled(t, intentRecord)))

	stored, err := e.requests.Get(e.ctx, r.ID)
	require.NoError(t, err)
	assert.Equal(t, paymentrequest.StatusClosed, stored.Status)
	assert.Equal(t, closed.Version+1, stored.Version)
	assert.Equal(t, rawIntentID, stored.Share(payerUserID).GetPaid().GetIntent().GetValue())

	// Announced as the share alone: a payment after a close moves no status.
	updates := e.requestUpdates(t, r, 1)
	requireSharePaymentUpdate(t, updates[0], stored, payerUserID, false)

	msgs, err := e.messages.GetMessages(e.ctx, groupID)
	require.NoError(t, err)
	require.Empty(t, msgs)
}

// A requested payment task fails, to be retried, rather than mark a share it
// cannot make whole: for a payment that is not a requested
// payment, one to several people, one naming a request that does not exist,
// or one by a payer with no share.
func TestExecutor_RecordRequestedPayment_Mismatched(t *testing.T) {
	e := newGroupPaymentEnv(t)

	creatorUserID, creatorKeys := e.bindUser(t)
	payerUserID, payerKeys := e.bindUser(t)
	otherUserID, otherKeys := e.bindUser(t)

	groupID := e.putGroup(t, creatorUserID, payerUserID, otherUserID)
	r := e.putRequest(t, groupID, creatorUserID, payerUserID)

	groupPayment, _ := e.pay(t, groupPaymentMetadata(groupID), payerKeys, creatorKeys)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewRecordRequestedPaymentTask(groupPayment)), "not a requested payment")

	multi := e.multiPay(t, requestedPaymentMetadata(groupID, r.ID), payerKeys,
		multiPayment{to: creatorKeys, quarks: 10_000},
		multiPayment{to: otherKeys, quarks: 10_000},
	)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewRecordRequestedPaymentTask(multi)), "requested payment has 2 recipients")

	toDm, _ := e.pay(t, requestedPaymentMetadata(chat.MustDeriveDmChatID(chatpb.ChatType_DM, payerUserID, creatorUserID), r.ID), payerKeys, creatorKeys)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewRecordRequestedPaymentTask(toDm)), "chat id is not a group")

	missing := paymentrequest.MustDeriveID(creatorUserID, &paymentrequestpb.IdempotencyKey{Value: make([]byte, paymentrequest.IdempotencyKeySize)})
	toMissing, _ := e.pay(t, requestedPaymentMetadata(groupID, missing), payerKeys, creatorKeys)
	require.ErrorIs(t, e.executor.Execute(e.ctx, intent.NewRecordRequestedPaymentTask(toMissing)), paymentrequest.ErrNotFound)

	noShare, _ := e.pay(t, requestedPaymentMetadata(groupID, r.ID), otherKeys, creatorKeys)
	require.ErrorIs(t, e.executor.Execute(e.ctx, intent.NewRecordRequestedPaymentTask(noShare)), paymentrequest.ErrNotAShare)

	// A payment of the share under any intent but the share's, which
	// validation would have refused, is refused by the store too.
	wrongIntent, _ := e.pay(t, requestedPaymentMetadata(groupID, r.ID), payerKeys, creatorKeys)
	require.ErrorIs(t, e.executor.Execute(e.ctx, intent.NewRecordRequestedPaymentTask(wrongIntent)), paymentrequest.ErrWrongIntent)

	// And a group payment task refuses a requested payment.
	requested, _ := e.pay(t, requestedPaymentMetadata(groupID, r.ID), payerKeys, creatorKeys)
	require.ErrorContains(t, e.executor.Execute(e.ctx, intent.NewSendGroupPaymentMessageTask(requested)), "not a group payment")

	stored, err := e.requests.Get(e.ctx, r.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stored.Version)
	assert.Nil(t, stored.Share(payerUserID).GetPaid())

	msgs, err := e.messages.GetMessages(e.ctx, groupID)
	require.NoError(t, err)
	require.Empty(t, msgs)
}

// A payment of a share already paid under an intent that is not the share's
// can only come from a path that skipped validation. Its task fails, on every
// retry, rather than passing as a no-op on the paid share.
func TestExecutor_RecordRequestedPayment_WrongIntentOnPaidShare(t *testing.T) {
	e := newGroupPaymentEnv(t)

	creatorUserID, creatorKeys := e.bindUser(t)
	payerUserID, payerKeys := e.bindUser(t)

	groupID := e.putGroup(t, creatorUserID, payerUserID)
	r := e.putRequest(t, groupID, creatorUserID, payerUserID)

	intentRecord, rawIntentID := e.payShare(t, r, payerKeys, creatorKeys)
	require.NoError(t, e.executor.Execute(e.ctx, e.scheduled(t, intentRecord)))

	unvalidated, _ := e.pay(t, requestedPaymentMetadata(groupID, r.ID), payerKeys, creatorKeys)
	task := intent.NewRecordRequestedPaymentTask(unvalidated)
	for range 2 {
		require.ErrorIs(t, e.executor.Execute(e.ctx, task), paymentrequest.ErrWrongIntent)
	}

	// Only the validated payment's mark was announced; a refused one
	// announces nothing.
	e.requestUpdates(t, r, 1)

	stored, err := e.requests.Get(e.ctx, r.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 2, stored.Version)
	assert.Equal(t, rawIntentID, stored.Share(payerUserID).GetPaid().GetIntent().GetValue())

	msgs, err := e.messages.GetMessages(e.ctx, groupID)
	require.NoError(t, err)
	require.Empty(t, msgs)
}

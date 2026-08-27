package intent_test

import (
	"context"
	"testing"
	"time"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	intentpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/intent/v1"

	"github.com/code-payments/flipcash2-server/account"
	accountmemory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/chat"
	chatmemory "github.com/code-payments/flipcash2-server/chat/memory"
	"github.com/code-payments/flipcash2-server/intent"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	profilememory "github.com/code-payments/flipcash2-server/profile/memory"
	currency_lib "github.com/code-payments/ocp-server/currency"
	ocp_currency_util "github.com/code-payments/ocp-server/ocp/currency"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_currency "github.com/code-payments/ocp-server/ocp/data/currency"
	exchange_memory "github.com/code-payments/ocp-server/ocp/data/currency/exchange/memory"
	holder_memory "github.com/code-payments/ocp-server/ocp/data/currency/holder/memory"
	reserve_memory "github.com/code-payments/ocp-server/ocp/data/currency/reserve/memory"
	ocp_intent "github.com/code-payments/ocp-server/ocp/data/intent"
	ocp_integration "github.com/code-payments/ocp-server/ocp/integration"
	ocp_testutil "github.com/code-payments/ocp-server/testutil"
)

// testExchangeRates are the live rates the env's mint data provider serves,
// quoted as fiat units per USD, since the core mint is a USD stablecoin.
var testExchangeRates = map[string]float64{
	"usd": 1.0,
	"jpy": 150.0,
	"eur": 0.9,
}

type integrationEnv struct {
	ctx         context.Context
	accounts    account.Store
	chats       chat.Store
	profiles    profile.Store
	integration ocp_integration.SubmitIntent
}

func newIntegrationEnv(t *testing.T) *integrationEnv {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	accounts := accountmemory.NewInMemory()
	profiles := profilememory.NewInMemory()
	chats := chatmemory.NewInMemory()

	// The mint data provider signs the rates it serves with the subsidizer, so
	// one has to exist before its first poll.
	ocpData := ocp_data.NewTestDataProvider()
	ocp_testutil.SetupRandomSubsidizer(t, ocpData)

	exchangeRates := exchange_memory.New()
	require.NoError(t, exchangeRates.PutExchangeRates(ctx, &ocp_currency.MultiRateRecord{
		Time:  time.Now(),
		Rates: testExchangeRates,
	}))

	mintDataProvider := ocp_currency_util.NewMintDataProvider(log, ocpData, exchangeRates, reserve_memory.New(), holder_memory.New(), 0, time.Second, time.Second)
	require.NoError(t, mintDataProvider.Start(ctx))
	t.Cleanup(mintDataProvider.Stop)

	return &integrationEnv{
		ctx:         ctx,
		accounts:    accounts,
		chats:       chats,
		profiles:    profiles,
		integration: intent.NewIntegration(accounts, chats, profiles, mintDataProvider),
	}
}

// bindUser creates a Flipcash user bound to a fresh key pair.
func (e *integrationEnv) bindUser(t *testing.T) (*commonpb.UserId, model.KeyPair) {
	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	_, err := e.accounts.Bind(e.ctx, userID, keys.Proto())
	require.NoError(t, err)
	return userID, keys
}

// linkPhoneForPayment links a phone number to the user and enables it for
// payment, satisfying the contact DM validator's phone-ownership checks.
func (e *integrationEnv) linkPhoneForPayment(t *testing.T, userID *commonpb.UserId, phone string) {
	require.NoError(t, e.profiles.LinkPhoneNumber(e.ctx, userID, phone, &commonpb.Hash{Value: make([]byte, 32)}))
	_, err := e.profiles.LinkPhoneNumberForPayment(e.ctx, userID, phone)
	require.NoError(t, err)
}

// dmPaymentIntentRecord builds a direct SendPublicPayment intent record
// carrying the given chat metadata.
func dmPaymentIntentRecord(t *testing.T, chatMetadata *intentpb.ChatMetadata, initiatorOwner, destinationOwner string) *ocp_intent.Record {
	appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
		Domain: &intentpb.AppMetadata_Chat{Chat: chatMetadata},
	})
	require.NoError(t, err)

	return &ocp_intent.Record{
		IntentId:              base58.Encode(model.MustGenerateKeyPair().Public()),
		IntentType:            ocp_intent.SendPublicPayment,
		MintAccount:           base58.Encode(model.MustGenerateKeyPair().Public()),
		InitiatorOwnerAccount: initiatorOwner,
		SendPublicPaymentMetadata: &ocp_intent.SendPublicPaymentMetadata{
			DestinationOwnerAccount: destinationOwner,
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
}

func tipDmChatMetadata(chatID *commonpb.ChatId) *intentpb.ChatMetadata {
	return tipDmChatMetadataFrom(chatID, intentpb.ChatMetadata_TipDmPayment_TIPCARD)
}

func tipDmChatMetadataFrom(chatID *commonpb.ChatId, location intentpb.ChatMetadata_TipDmPayment_Location) *intentpb.ChatMetadata {
	return &intentpb.ChatMetadata{
		ChatId: chatID,
		Type: &intentpb.ChatMetadata_TipDmPayment_{
			TipDmPayment: &intentpb.ChatMetadata_TipDmPayment{Location: location},
		},
	}
}

func contactDmChatMetadata(chatID *commonpb.ChatId, sourcePhone, destinationPhone string) *intentpb.ChatMetadata {
	return &intentpb.ChatMetadata{
		ChatId: chatID,
		Type: &intentpb.ChatMetadata_ContactDmPayment_{
			ContactDmPayment: &intentpb.ChatMetadata_ContactDmPayment{
				Source:      &commonpb.PhoneNumber{Value: sourcePhone},
				Destination: &commonpb.PhoneNumber{Value: destinationPhone},
			},
		},
	}
}

func TestIntegration_AllowCreation_TipDmPayment(t *testing.T) {
	e := newIntegrationEnv(t)

	senderUserID, senderKeys := e.bindUser(t)
	recipientUserID, recipientKeys := e.bindUser(t)

	tipChatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, senderUserID, recipientUserID)

	validRecord := func() *ocp_intent.Record {
		return dmPaymentIntentRecord(t, tipDmChatMetadata(tipChatID), base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))
	}

	// A valid tip requires no phone numbers anywhere: neither party has one
	// linked in this env, which is the defining difference from contact DMs.
	t.Run("valid", func(t *testing.T) {
		require.NoError(t, e.integration.AllowCreation(e.ctx, validRecord(), nil, nil))
	})

	t.Run("denied_indirect_payment_flags", func(t *testing.T) {
		for _, mutate := range []func(*ocp_intent.SendPublicPaymentMetadata){
			func(m *ocp_intent.SendPublicPaymentMetadata) { m.IsWithdrawal = true },
			func(m *ocp_intent.SendPublicPaymentMetadata) { m.IsIndirectSend = true },
			func(m *ocp_intent.SendPublicPaymentMetadata) { m.IsSwapSell = true },
		} {
			record := validRecord()
			mutate(record.SendPublicPaymentMetadata)
			require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "direct payment")
		}
	})

	t.Run("denied_no_destination_owner", func(t *testing.T) {
		record := validRecord()
		record.SendPublicPaymentMetadata.DestinationOwnerAccount = ""
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "not a flipcash user")
	})

	t.Run("denied_sender_not_flipcash_user", func(t *testing.T) {
		record := validRecord()
		record.InitiatorOwnerAccount = base58.Encode(model.MustGenerateKeyPair().Public())
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "sender is not a flipcash user")
	})

	t.Run("denied_recipient_not_flipcash_user", func(t *testing.T) {
		record := validRecord()
		record.SendPublicPaymentMetadata.DestinationOwnerAccount = base58.Encode(model.MustGenerateKeyPair().Public())
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "recipient is not a flipcash user")
	})

	t.Run("denied_self_tip", func(t *testing.T) {
		selfChatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, senderUserID, senderUserID)
		record := dmPaymentIntentRecord(t, tipDmChatMetadata(selfChatID), base58.Encode(senderKeys.Public()), base58.Encode(senderKeys.Public()))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "tip to yourself")
	})

	// Tips below the per-currency minimum are denied. The minimum itself, and
	// anything above it, is allowed, as is anything within half of the
	// currency's smallest transferable unit below the minimum.
	t.Run("minimum_amount", func(t *testing.T) {
		for _, tc := range []struct {
			currency       string
			nativeAmount   float64
			usdMarketValue float64
			allowed        bool
		}{
			{"usd", 1.0, 1.0, true},      // exactly the usd minimum
			{"usd", 5.0, 5.0, true},      // above the usd minimum
			{"usd", 0.995, 0.995, true},  // within half a cent of the usd minimum
			{"usd", 0.99, 0.99, false},   // below the usd tolerance band
			{"jpy", 100, 0.68, true},     // exactly the jpy minimum
			{"jpy", 99.5, 0.677, true},   // within half a yen of the jpy minimum
			{"jpy", 99, 0.674, false},    // below the jpy tolerance band
			{"jpy", 50, 0.34, false},     // below the jpy minimum
			{"kwd", 0.25, 0.82, true},    // fractional minimum
			{"kwd", 0.2495, 0.818, true}, // within half a fils of the kwd minimum
			{"kwd", 0.249, 0.816, false}, // below the kwd tolerance band
			{"kwd", 0.1, 0.33, false},    // below a fractional minimum
			{"bgn", 5.0, 2.85, true},     // no preset: usd market value clears the floor
			{"bgn", 1.0, 0.995, true},    // no preset: usd market value within the usd tolerance band
			{"bgn", 1.0, 0.57, false},    // no preset: usd market value below the floor
		} {
			record := validRecord()
			record.SendPublicPaymentMetadata.ExchangeCurrency = currency_lib.Code(tc.currency)
			record.SendPublicPaymentMetadata.NativeAmount = tc.nativeAmount
			record.SendPublicPaymentMetadata.UsdMarketValue = tc.usdMarketValue

			err := e.integration.AllowCreation(e.ctx, record, nil, nil)
			if tc.allowed {
				require.NoError(t, err, "%s %v", tc.currency, tc.nativeAmount)
			} else {
				require.ErrorContains(t, err, "tip amount is below the minimum", "%s %v", tc.currency, tc.nativeAmount)
			}
		}
	})

	// A tip payment referencing the pair's *contact* DM must fail: the two
	// chat types derive distinct canonical IDs.
	t.Run("rejected_contact_dm_chat_id", func(t *testing.T) {
		contactChatID := chat.MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, senderUserID, recipientUserID)
		record := dmPaymentIntentRecord(t, tipDmChatMetadata(contactChatID), base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "chat id does not match")
	})

	// A send from within the chat is only allowed once the tip DM has been
	// initialized (by a tip card tip), and has no minimum amount. The env is
	// fresh here, so a separate pair keeps the uninitialized case isolated.
	t.Run("send_from_chat", func(t *testing.T) {
		sendRecord := func(amount float64) *ocp_intent.Record {
			record := dmPaymentIntentRecord(t, tipDmChatMetadataFrom(tipChatID, intentpb.ChatMetadata_TipDmPayment_CHAT), base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))
			record.SendPublicPaymentMetadata.ExchangeCurrency = currency_lib.USD
			record.SendPublicPaymentMetadata.NativeAmount = amount
			record.SendPublicPaymentMetadata.UsdMarketValue = amount
			return record
		}

		// Chat doesn't exist yet: denied regardless of amount, while a tip card
		// tip into the same uninitialized chat is still allowed.
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, sendRecord(5.0), nil, nil), "not been initialized")
		require.NoError(t, e.integration.AllowCreation(e.ctx, validRecord(), nil, nil))

		require.NoError(t, e.chats.PutChat(e.ctx, &chat.Chat{
			ID:      tipChatID,
			Type:    chatpb.ChatType_TIP_DM,
			Members: []*commonpb.UserId{senderUserID, recipientUserID},
		}))

		// Initialized: allowed, including amounts below the tip minimum.
		require.NoError(t, e.integration.AllowCreation(e.ctx, sendRecord(5.0), nil, nil))
		require.NoError(t, e.integration.AllowCreation(e.ctx, sendRecord(0.01), nil, nil))

		// The tip minimum still applies to tip card tips.
		tipRecord := validRecord()
		tipRecord.SendPublicPaymentMetadata.ExchangeCurrency = currency_lib.USD
		tipRecord.SendPublicPaymentMetadata.NativeAmount = 0.01
		tipRecord.SendPublicPaymentMetadata.UsdMarketValue = 0.01
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, tipRecord, nil, nil), "below the minimum")
	})

	// A recipient's minimum DM chat initialization fee applies only to the tip
	// that initializes the chat. A fresh env keeps the chat uninitialized here.
	t.Run("min_dm_chat_init_fee", func(t *testing.T) {
		e := newIntegrationEnv(t)
		senderUserID, senderKeys := e.bindUser(t)
		recipientUserID, recipientKeys := e.bindUser(t)
		tipChatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, senderUserID, recipientUserID)

		record := func(location intentpb.ChatMetadata_TipDmPayment_Location, currency string, nativeAmount float64) *ocp_intent.Record {
			record := dmPaymentIntentRecord(t, tipDmChatMetadataFrom(tipChatID, location), base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))
			record.SendPublicPaymentMetadata.ExchangeCurrency = currency_lib.Code(currency)
			record.SendPublicPaymentMetadata.NativeAmount = nativeAmount
			record.SendPublicPaymentMetadata.UsdMarketValue = nativeAmount
			if rate, ok := testExchangeRates[currency]; ok {
				record.SendPublicPaymentMetadata.UsdMarketValue = nativeAmount / rate
			}
			return record
		}
		tip := func(currency string, nativeAmount float64) *ocp_intent.Record {
			return record(intentpb.ChatMetadata_TipDmPayment_TIPCARD, currency, nativeAmount)
		}
		setFee := func(currency string, nativeAmount float64) {
			require.NoError(t, e.profiles.SetMinDmChatInitFee(e.ctx, recipientUserID, &commonpb.FiatPaymentAmount{Currency: currency, NativeAmount: nativeAmount}))
		}

		// No fee set: only the preset minimum applies.
		require.NoError(t, e.integration.AllowCreation(e.ctx, tip("usd", 1.0), nil, nil))

		setFee("usd", 10)
		for _, tc := range []struct {
			currency     string
			nativeAmount float64
			allowed      bool
			denial       string
		}{
			{"usd", 10.0, true, ""},  // exactly the fee
			{"usd", 25.0, true, ""},  // above the fee
			{"usd", 9.995, true, ""}, // within half a cent of the fee
			{"usd", 9.99, false, "chat initialization fee"},
			{"usd", 1.0, false, "chat initialization fee"}, // clears the preset minimum, not the fee
			{"jpy", 1_500, true, ""},                       // cross-currency: converted at the live rate
			{"jpy", 1_499, false, "chat initialization fee"},
			{"jpy", 50, false, "below the minimum"}, // preset minimum is checked first
			{"eur", 9.0, true, ""},
			{"eur", 8.9, false, "chat initialization fee"},
		} {
			err := e.integration.AllowCreation(e.ctx, tip(tc.currency, tc.nativeAmount), nil, nil)
			if tc.allowed {
				require.NoError(t, err, "%s %v", tc.currency, tc.nativeAmount)
			} else {
				require.ErrorContains(t, err, tc.denial, "%s %v", tc.currency, tc.nativeAmount)
			}
		}

		// A fee in a non-USD currency compares natively when paid in it, and
		// through the live rate otherwise, with the rounding slack in the fee's
		// currency.
		setFee("jpy", 1_000)
		require.NoError(t, e.integration.AllowCreation(e.ctx, tip("jpy", 1_000), nil, nil))
		require.NoError(t, e.integration.AllowCreation(e.ctx, tip("jpy", 999.5), nil, nil))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, tip("jpy", 999), nil, nil), "chat initialization fee")
		require.NoError(t, e.integration.AllowCreation(e.ctx, tip("usd", 6.67), nil, nil))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, tip("usd", 6.66), nil, nil), "chat initialization fee")

		// A fee in a currency with no live rate cannot be compared against a
		// payment in another currency, and is denied rather than waved through.
		setFee("kwd", 1)
		require.NoError(t, e.integration.AllowCreation(e.ctx, tip("kwd", 1), nil, nil))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, tip("usd", 100), nil, nil), "no exchange rate")

		// A send from within the chat is still denied before initialization,
		// whatever the amount.
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record(intentpb.ChatMetadata_TipDmPayment_CHAT, "usd", 100), nil, nil), "not been initialized")

		// Once the chat exists the fee no longer applies: tips are held to the
		// preset minimum only, and sends to nothing.
		require.NoError(t, e.chats.PutChat(e.ctx, &chat.Chat{
			ID:      tipChatID,
			Type:    chatpb.ChatType_TIP_DM,
			Members: []*commonpb.UserId{senderUserID, recipientUserID},
		}))
		require.NoError(t, e.integration.AllowCreation(e.ctx, tip("usd", 1.0), nil, nil))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, tip("usd", 0.5), nil, nil), "below the minimum")
		require.NoError(t, e.integration.AllowCreation(e.ctx, record(intentpb.ChatMetadata_TipDmPayment_CHAT, "usd", 0.01), nil, nil))
	})
}

func TestIntegration_AllowCreation_ContactDmPayment(t *testing.T) {
	t.Skip("contact send feature disabled")

	e := newIntegrationEnv(t)

	senderUserID, senderKeys := e.bindUser(t)
	recipientUserID, recipientKeys := e.bindUser(t)

	const senderPhone = "+12223334444"
	const recipientPhone = "+13334445555"
	e.linkPhoneForPayment(t, senderUserID, senderPhone)
	e.linkPhoneForPayment(t, recipientUserID, recipientPhone)

	contactChatID := chat.MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, senderUserID, recipientUserID)

	record := func(chatMetadata *intentpb.ChatMetadata) *ocp_intent.Record {
		return dmPaymentIntentRecord(t, chatMetadata, base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))
	}
	validRecord := func() *ocp_intent.Record {
		return record(contactDmChatMetadata(contactChatID, senderPhone, recipientPhone))
	}

	t.Run("valid", func(t *testing.T) {
		require.NoError(t, e.integration.AllowCreation(e.ctx, validRecord(), nil, nil))
	})

	t.Run("denied_indirect_payment_flags", func(t *testing.T) {
		for _, mutate := range []func(*ocp_intent.SendPublicPaymentMetadata){
			func(m *ocp_intent.SendPublicPaymentMetadata) { m.IsWithdrawal = true },
			func(m *ocp_intent.SendPublicPaymentMetadata) { m.IsIndirectSend = true },
			func(m *ocp_intent.SendPublicPaymentMetadata) { m.IsSwapSell = true },
		} {
			r := validRecord()
			mutate(r.SendPublicPaymentMetadata)
			require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "direct payment")
		}
	})

	t.Run("denied_same_phone", func(t *testing.T) {
		r := record(contactDmChatMetadata(contactChatID, senderPhone, senderPhone))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "no-op between the same phone number")
	})

	t.Run("denied_source_phone_not_linked", func(t *testing.T) {
		r := record(contactDmChatMetadata(contactChatID, "+19998887777", recipientPhone))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "source phone number is not linked for payment")
	})

	t.Run("denied_sender_not_owner_of_source_phone", func(t *testing.T) {
		// The recipient's phone is linked for payment, but not to the sender.
		r := record(contactDmChatMetadata(contactChatID, recipientPhone, senderPhone))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "sender is not linked to the source phone number")
	})

	t.Run("denied_destination_phone_not_linked", func(t *testing.T) {
		r := record(contactDmChatMetadata(contactChatID, senderPhone, "+19998887777"))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "destination phone number is not linked for payment")
	})

	t.Run("denied_recipient_not_owner_of_destination_phone", func(t *testing.T) {
		// A third user's phone is linked for payment, but not to the recipient.
		otherUserID, _ := e.bindUser(t)
		const otherPhone = "+14445556666"
		e.linkPhoneForPayment(t, otherUserID, otherPhone)

		r := record(contactDmChatMetadata(contactChatID, senderPhone, otherPhone))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "recipient is not linked to the destination phone number")
	})

	// A contact payment referencing the pair's *tip* DM must fail: the two
	// chat types derive distinct canonical IDs.
	t.Run("rejected_tip_dm_chat_id", func(t *testing.T) {
		tipChatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, senderUserID, recipientUserID)
		r := record(contactDmChatMetadata(tipChatID, senderPhone, recipientPhone))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "chat id does not match")
	})
}

func TestIntegration_GetTasksToSchedule_TipDmPayment(t *testing.T) {
	e := newIntegrationEnv(t)

	senderUserID, senderKeys := e.bindUser(t)
	recipientUserID, recipientKeys := e.bindUser(t)

	tipChatID := chat.MustDeriveDmChatID(chatpb.ChatType_TIP_DM, senderUserID, recipientUserID)
	record := dmPaymentIntentRecord(t, tipDmChatMetadata(tipChatID), base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))

	tasks, err := e.integration.GetTasksToSchedule(e.ctx, record)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, intent.TaskTypeSendTipDmPaymentMessage, tasks[0].Type)
	require.NotNil(t, tasks[0].ReferenceId)
	assert.Equal(t, record.IntentId, *tasks[0].ReferenceId)
	require.NoError(t, tasks[0].Validate())
}

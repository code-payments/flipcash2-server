package intent_test

import (
	"bytes"
	"context"
	"math"
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
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/account"
	accountmemory "github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/chat"
	chatmemory "github.com/code-payments/flipcash2-server/chat/memory"
	"github.com/code-payments/flipcash2-server/intent"
	"github.com/code-payments/flipcash2-server/messaging"
	messagingmemory "github.com/code-payments/flipcash2-server/messaging/memory"
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
	messages    messaging.Store
	profiles    profile.Store
	access      *chat.Access
	integration ocp_integration.SubmitIntent

	// team is the Flipcash team account the integration is built with, and
	// teamKeys the key pair it is bound to.
	team     *commonpb.UserId
	teamKeys model.KeyPair

	// staff is the set of staff users, keyed by user ID, that accounts
	// answers IsStaff from
	staff map[string]bool
}

// staffAccounts is an account.Store that answers IsStaff from a set, since
// the in-memory store has no staff flag. Everything else is the embedded
// store's.
type staffAccounts struct {
	account.Store

	staff map[string]bool
}

func (s *staffAccounts) IsStaff(_ context.Context, userID *commonpb.UserId) (bool, error) {
	return s.staff[string(userID.Value)], nil
}

func newIntegrationEnv(t *testing.T) *integrationEnv {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	staff := make(map[string]bool)
	accounts := &staffAccounts{Store: accountmemory.NewInMemory(), staff: staff}
	profiles := profilememory.NewInMemory()
	chats := chatmemory.NewInMemory(nil)
	messages := messagingmemory.NewInMemory()

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

	team := model.MustGenerateUserID()
	teamKeys := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, team, teamKeys.Proto())
	require.NoError(t, err)

	// No group in these tests carries a balance rule, so the evaluator never
	// values a balance. Nothing is remembered between asks, so a test's
	// membership changes show on the next payment.
	access := chat.NewAccess(
		chats,
		chat.NewRuleEvaluator(accounts, nil, chats, team),
		chat.WithListenerAdmissionTTL(0),
		chat.WithSpeakerAdmissionTTL(0),
	)

	return &integrationEnv{
		ctx:         ctx,
		accounts:    accounts,
		chats:       chats,
		messages:    messages,
		profiles:    profiles,
		access:      access,
		integration: intent.NewIntegration(accounts, chats, messages, profiles, access, mintDataProvider, team),
		team:        team,
		teamKeys:    teamKeys,
		staff:       staff,
	}
}

// setStaff flags the user as staff.
func (e *integrationEnv) setStaff(userID *commonpb.UserId) {
	e.staff[string(userID.Value)] = true
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

// multiPaymentIntentRecord builds a SendPublicMultiPayment intent record
// paying each destination owner, carrying no app metadata.
func multiPaymentIntentRecord(initiatorOwner string, destinationOwners ...string) *ocp_intent.Record {
	metadata := &ocp_intent.SendPublicMultiPaymentMetadata{
		Source:           base58.Encode(model.MustGenerateKeyPair().Public()),
		ExchangeCurrency: "usd",
		ExchangeRate:     1.0,
	}
	for _, destinationOwner := range destinationOwners {
		metadata.Payments = append(metadata.Payments, &ocp_intent.Payment{
			DestinationOwnerAccount: destinationOwner,
			DestinationTokenAccount: base58.Encode(model.MustGenerateKeyPair().Public()),
			Quantity:                10_000,
			NativeAmount:            1.0,
			UsdMarketValue:          1.0,
		})
		metadata.Quantity += 10_000
		metadata.NativeAmount += 1.0
		metadata.UsdMarketValue += 1.0
	}

	return &ocp_intent.Record{
		IntentId:                       base58.Encode(model.MustGenerateKeyPair().Public()),
		IntentType:                     ocp_intent.SendPublicMultiPayment,
		MintAccount:                    base58.Encode(model.MustGenerateKeyPair().Public()),
		InitiatorOwnerAccount:          initiatorOwner,
		SendPublicMultiPaymentMetadata: metadata,
		State:                          ocp_intent.StatePending,
	}
}

func dmChatMetadata(chatID *commonpb.ChatId) *intentpb.ChatMetadata {
	return dmChatMetadataFrom(chatID, intentpb.ChatMetadata_DmPayment_FLIPCARD)
}

func dmChatMetadataFrom(chatID *commonpb.ChatId, location intentpb.ChatMetadata_DmPayment_Location) *intentpb.ChatMetadata {
	return dmChatMetadataWithAction(chatID, location, intentpb.PaymentAction_DEFAULT)
}

func dmChatMetadataWithAction(chatID *commonpb.ChatId, location intentpb.ChatMetadata_DmPayment_Location, action intentpb.PaymentAction) *intentpb.ChatMetadata {
	return &intentpb.ChatMetadata{
		ChatId: chatID,
		Type: &intentpb.ChatMetadata_DmPayment_{
			DmPayment: &intentpb.ChatMetadata_DmPayment{Location: location, Action: action},
		},
	}
}

func groupChatMetadata(chatID *commonpb.ChatId, action intentpb.PaymentAction) *intentpb.ChatMetadata {
	return &intentpb.ChatMetadata{
		ChatId: chatID,
		Type: &intentpb.ChatMetadata_GroupPayment_{
			GroupPayment: &intentpb.ChatMetadata_GroupPayment{Action: action},
		},
	}
}

// putGroup creates a public group with no rules, created by creator, whose
// members are creator and members.
func (e *integrationEnv) putGroup(t *testing.T, creator *commonpb.UserId, members ...*commonpb.UserId) *commonpb.ChatId {
	return e.putGroupChat(t, &chat.Chat{CreatorID: creator, Members: append([]*commonpb.UserId{creator}, members...)})
}

// putGroupChat creates a group from c, filling in its ID, type and title.
func (e *integrationEnv) putGroupChat(t *testing.T, c *chat.Chat) *commonpb.ChatId {
	c.ID = chat.MustGenerateGroupChatID()
	c.Type = chatpb.ChatType_GROUP
	c.Title = "Group"
	c.LastActivity = time.Now()
	require.NoError(t, e.chats.PutChat(e.ctx, c))
	return c.ID
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

func TestIntegration_AllowCreation_DmPayment(t *testing.T) {
	e := newIntegrationEnv(t)

	senderUserID, senderKeys := e.bindUser(t)
	recipientUserID, recipientKeys := e.bindUser(t)

	dmChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, senderUserID, recipientUserID)

	validRecord := func() *ocp_intent.Record {
		return dmPaymentIntentRecord(t, dmChatMetadata(dmChatID), base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))
	}

	// A valid payment requires no phone numbers anywhere: neither party has one
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

	t.Run("denied_self_payment", func(t *testing.T) {
		selfChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, senderUserID, senderUserID)
		record := dmPaymentIntentRecord(t, dmChatMetadata(selfChatID), base58.Encode(senderKeys.Public()), base58.Encode(senderKeys.Public()))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "payment to yourself")
	})

	// Payments that initialize the chat and fall below the per-currency minimum are
	// denied. The minimum itself, and anything above it, is allowed, as is
	// anything within half of the currency's smallest transferable unit below
	// the minimum. The chat is uninitialized throughout this case.
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
				require.ErrorContains(t, err, "payment amount is below the minimum", "%s %v", tc.currency, tc.nativeAmount)
			}
		}
	})

	// A DM payment referencing the pair's *contact* DM must fail: the two
	// chat types derive distinct canonical IDs.
	t.Run("rejected_contact_dm_chat_id", func(t *testing.T) {
		contactChatID := chat.MustDeriveDmChatID(chatpb.ChatType_CONTACT_DM, senderUserID, recipientUserID)
		record := dmPaymentIntentRecord(t, dmChatMetadata(contactChatID), base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "chat id does not match")
	})

	// Any payment initializes the chat, whatever its location or action, and
	// the one that does is held to the minimum. Once the chat exists, every
	// payment is any amount. A fresh env per case keeps the chat uninitialized.
	t.Run("any_payment_initializes", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			location intentpb.ChatMetadata_DmPayment_Location
			action   intentpb.PaymentAction
		}{
			{"flipcard_default", intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.PaymentAction_DEFAULT},
			{"flipcard_send", intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.PaymentAction_SEND},
			{"flipcard_tip", intentpb.ChatMetadata_DmPayment_FLIPCARD, intentpb.PaymentAction_TIP},
			{"chat_default", intentpb.ChatMetadata_DmPayment_CHAT, intentpb.PaymentAction_DEFAULT},
			{"chat_send", intentpb.ChatMetadata_DmPayment_CHAT, intentpb.PaymentAction_SEND},
			{"chat_tip", intentpb.ChatMetadata_DmPayment_CHAT, intentpb.PaymentAction_TIP},
		} {
			t.Run(tc.name, func(t *testing.T) {
				e := newIntegrationEnv(t)
				senderUserID, senderKeys := e.bindUser(t)
				recipientUserID, recipientKeys := e.bindUser(t)
				dmChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, senderUserID, recipientUserID)

				record := func(nativeAmount float64) *ocp_intent.Record {
					record := dmPaymentIntentRecord(t, dmChatMetadataWithAction(dmChatID, tc.location, tc.action), base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))
					record.SendPublicPaymentMetadata.ExchangeCurrency = currency_lib.USD
					record.SendPublicPaymentMetadata.NativeAmount = nativeAmount
					record.SendPublicPaymentMetadata.UsdMarketValue = nativeAmount
					return record
				}

				// Uninitialized: held to the minimum.
				require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record(0.5), nil, nil), "below the minimum")
				require.NoError(t, e.integration.AllowCreation(e.ctx, record(1.0), nil, nil))

				require.NoError(t, e.chats.PutChat(e.ctx, &chat.Chat{
					ID:      dmChatID,
					Type:    chatpb.ChatType_DM,
					Members: []*commonpb.UserId{senderUserID, recipientUserID},
				}))

				// Initialized: the gate was paid, so any amount.
				require.NoError(t, e.integration.AllowCreation(e.ctx, record(0.01), nil, nil))
			})
		}
	})

	// A recipient's minimum DM chat initialization fee applies only to the
	// payment that initializes the chat, and where set it replaces the preset
	// minimum as that payment's floor rather than stacking on it. A fresh env keeps the
	// chat uninitialized here.
	t.Run("min_dm_chat_init_fee", func(t *testing.T) {
		e := newIntegrationEnv(t)
		senderUserID, senderKeys := e.bindUser(t)
		recipientUserID, recipientKeys := e.bindUser(t)
		dmChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, senderUserID, recipientUserID)

		record := func(location intentpb.ChatMetadata_DmPayment_Location, currency string, nativeAmount float64) *ocp_intent.Record {
			record := dmPaymentIntentRecord(t, dmChatMetadataFrom(dmChatID, location), base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))
			record.SendPublicPaymentMetadata.ExchangeCurrency = currency_lib.Code(currency)
			record.SendPublicPaymentMetadata.NativeAmount = nativeAmount
			record.SendPublicPaymentMetadata.UsdMarketValue = nativeAmount
			if rate, ok := testExchangeRates[currency]; ok {
				record.SendPublicPaymentMetadata.UsdMarketValue = nativeAmount / rate
			}
			return record
		}
		pay := func(currency string, nativeAmount float64) *ocp_intent.Record {
			return record(intentpb.ChatMetadata_DmPayment_FLIPCARD, currency, nativeAmount)
		}
		setFee := func(currency string, nativeAmount float64) {
			require.NoError(t, e.profiles.SetMinDmChatInitFee(e.ctx, recipientUserID, &commonpb.FiatPaymentAmount{Currency: currency, NativeAmount: nativeAmount}))
		}

		// No fee set: only the preset minimum applies.
		require.NoError(t, e.integration.AllowCreation(e.ctx, pay("usd", 1.0), nil, nil))

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
			{"jpy", 50, false, "chat initialization fee"}, // the fee is the only floor, so it names the denial
			{"eur", 9.0, true, ""},
			{"eur", 8.9, false, "chat initialization fee"},
		} {
			err := e.integration.AllowCreation(e.ctx, pay(tc.currency, tc.nativeAmount), nil, nil)
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
		require.NoError(t, e.integration.AllowCreation(e.ctx, pay("jpy", 1_000), nil, nil))
		require.NoError(t, e.integration.AllowCreation(e.ctx, pay("jpy", 999.5), nil, nil))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, pay("jpy", 999), nil, nil), "chat initialization fee")
		require.NoError(t, e.integration.AllowCreation(e.ctx, pay("usd", 6.67), nil, nil))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, pay("usd", 6.66), nil, nil), "chat initialization fee")

		// The fee replaces the preset minimum of the payer's currency: a fee at
		// JPY's preset minimum is cleared by EUR 0.70 (JPY 116.67 at the live
		// rate) even though EUR's own preset minimum is 1.00, while EUR 0.59
		// (JPY 98.33) is short of the fee, not the preset.
		setFee("jpy", 100)
		require.NoError(t, e.integration.AllowCreation(e.ctx, pay("eur", 0.7), nil, nil))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, pay("eur", 0.59), nil, nil), "chat initialization fee")

		// A fee in a currency with no live rate cannot be compared against a
		// payment in another currency, and is denied rather than waved through.
		setFee("kwd", 1)
		require.NoError(t, e.integration.AllowCreation(e.ctx, pay("kwd", 1), nil, nil))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, pay("usd", 100), nil, nil), "no exchange rate")

		// A send from within the chat is held to the same fee before
		// initialization.
		setFee("usd", 10)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record(intentpb.ChatMetadata_DmPayment_CHAT, "usd", 9.99), nil, nil), "chat initialization fee")
		require.NoError(t, e.integration.AllowCreation(e.ctx, record(intentpb.ChatMetadata_DmPayment_CHAT, "usd", 10), nil, nil))

		// Once the chat exists nothing applies: tips and sends alike are any
		// amount, below the fee and below the preset minimum.
		require.NoError(t, e.chats.PutChat(e.ctx, &chat.Chat{
			ID:      dmChatID,
			Type:    chatpb.ChatType_DM,
			Members: []*commonpb.UserId{senderUserID, recipientUserID},
		}))
		require.NoError(t, e.integration.AllowCreation(e.ctx, pay("usd", 1.0), nil, nil))
		require.NoError(t, e.integration.AllowCreation(e.ctx, pay("usd", 0.5), nil, nil))
		require.NoError(t, e.integration.AllowCreation(e.ctx, record(intentpb.ChatMetadata_DmPayment_CHAT, "usd", 0.01), nil, nil))
	})
}

// No DM payment is allowed to or from the Flipcash team account, tip or
// send, whether or not the DM exists (the team's welcome creates it).
func TestIntegration_AllowCreation_DmPayment_TeamAccount(t *testing.T) {
	e := newIntegrationEnv(t)
	userID, userKeys := e.bindUser(t)
	chatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, userID, e.team)

	parties := []struct {
		name     string
		from, to model.KeyPair
	}{
		{"to_team", userKeys, e.teamKeys},
		{"from_team", e.teamKeys, userKeys},
	}
	payment := func(from, to model.KeyPair, action intentpb.PaymentAction) *ocp_intent.Record {
		metadata := dmChatMetadataWithAction(chatID, intentpb.ChatMetadata_DmPayment_CHAT, action)
		return dmPaymentIntentRecord(t, metadata, base58.Encode(from.Public()), base58.Encode(to.Public()))
	}
	requireDenied := func(t *testing.T) {
		for _, p := range parties {
			for _, action := range []intentpb.PaymentAction{intentpb.PaymentAction_TIP, intentpb.PaymentAction_SEND} {
				err := e.integration.AllowCreation(e.ctx, payment(p.from, p.to, action), nil, nil)
				require.ErrorContains(t, err, "flipcash team", "%s %s", p.name, action)
			}
		}
	}

	t.Run("uninitialized", requireDenied)

	require.NoError(t, e.chats.PutChat(e.ctx, &chat.Chat{
		ID:      chatID,
		Type:    chatpb.ChatType_DM,
		Members: []*commonpb.UserId{userID, e.team},
	}))
	t.Run("initialized", requireDenied)

	// Without a team account configured, the same user is anyone else.
	t.Run("no_team_configured", func(t *testing.T) {
		unconfigured := intent.NewIntegration(e.accounts, e.chats, e.messages, e.profiles, e.access, nil, nil)
		require.NoError(t, unconfigured.AllowCreation(e.ctx, payment(userKeys, e.teamKeys, intentpb.PaymentAction_SEND), nil, nil))
	})
}

func TestIntegration_AllowCreation_GroupPayment(t *testing.T) {
	e := newIntegrationEnv(t)

	payerUserID, payerKeys := e.bindUser(t)
	recipientUserID, recipientKeys := e.bindUser(t)
	outsiderUserID, outsiderKeys := e.bindUser(t)

	groupID := e.putGroup(t, payerUserID, recipientUserID)

	payment := func(chatID *commonpb.ChatId, from, to model.KeyPair, action intentpb.PaymentAction) *ocp_intent.Record {
		return dmPaymentIntentRecord(t, groupChatMetadata(chatID, action), base58.Encode(from.Public()), base58.Encode(to.Public()))
	}
	validRecord := func() *ocp_intent.Record {
		return payment(groupID, payerKeys, recipientKeys, intentpb.PaymentAction_DEFAULT)
	}

	t.Run("valid", func(t *testing.T) {
		for _, action := range []intentpb.PaymentAction{intentpb.PaymentAction_DEFAULT, intentpb.PaymentAction_SEND} {
			require.NoError(t, e.integration.AllowCreation(e.ctx, payment(groupID, payerKeys, recipientKeys, action), nil, nil), action)
		}
		// Either member may pay the other: the recipient is not the creator.
		require.NoError(t, e.integration.AllowCreation(e.ctx, payment(groupID, recipientKeys, payerKeys, intentpb.PaymentAction_SEND), nil, nil))
	})

	// No floor: a payment is any amount.
	t.Run("any_amount", func(t *testing.T) {
		record := validRecord()
		record.SendPublicPaymentMetadata.NativeAmount = 0.01
		record.SendPublicPaymentMetadata.UsdMarketValue = 0.01
		require.NoError(t, e.integration.AllowCreation(e.ctx, record, nil, nil))
	})

	t.Run("denied_tip", func(t *testing.T) {
		record := payment(groupID, payerKeys, recipientKeys, intentpb.PaymentAction_TIP)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "action is not supported")
	})

	t.Run("denied_dm_chat_id", func(t *testing.T) {
		dmChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, payerUserID, recipientUserID)
		record := payment(dmChatID, payerKeys, recipientKeys, intentpb.PaymentAction_SEND)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "not a group")
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

	t.Run("denied_recipient_not_flipcash_user", func(t *testing.T) {
		record := validRecord()
		record.SendPublicPaymentMetadata.DestinationOwnerAccount = base58.Encode(model.MustGenerateKeyPair().Public())
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "recipient is not a flipcash user")
	})

	t.Run("denied_self_payment", func(t *testing.T) {
		record := payment(groupID, payerKeys, payerKeys, intentpb.PaymentAction_SEND)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "payment to yourself")
	})

	t.Run("denied_group_not_found", func(t *testing.T) {
		record := payment(chat.MustGenerateGroupChatID(), payerKeys, recipientKeys, intentpb.PaymentAction_SEND)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "sender cannot send")
	})

	t.Run("denied_payer_not_member", func(t *testing.T) {
		record := payment(groupID, outsiderKeys, recipientKeys, intentpb.PaymentAction_SEND)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "sender cannot send")
	})

	t.Run("denied_recipient_not_member", func(t *testing.T) {
		record := payment(groupID, payerKeys, outsiderKeys, intentpb.PaymentAction_SEND)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "recipient is not a member")
	})

	// A departed member can neither pay nor be paid, and a rejoin restores
	// both.
	t.Run("departed_member", func(t *testing.T) {
		groupID := e.putGroup(t, payerUserID, recipientUserID, outsiderUserID)
		_, _, err := e.chats.RemoveGroupMember(e.ctx, groupID, outsiderUserID, false)
		require.NoError(t, err)

		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, payment(groupID, outsiderKeys, recipientKeys, intentpb.PaymentAction_SEND), nil, nil), "sender cannot send")
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, payment(groupID, payerKeys, outsiderKeys, intentpb.PaymentAction_SEND), nil, nil), "recipient is not a member")

		_, _, err = e.chats.AddGroupMembers(e.ctx, groupID, []*commonpb.UserId{outsiderUserID})
		require.NoError(t, err)
		require.NoError(t, e.integration.AllowCreation(e.ctx, payment(groupID, outsiderKeys, recipientKeys, intentpb.PaymentAction_SEND), nil, nil))
		require.NoError(t, e.integration.AllowCreation(e.ctx, payment(groupID, payerKeys, outsiderKeys, intentpb.PaymentAction_SEND), nil, nil))
	})

	// In a creator-only group, only the creator may pay, since a payment
	// speaks; anyone may be paid, since receiving does not.
	t.Run("creator_only_speaker", func(t *testing.T) {
		groupID := e.putGroupChat(t, &chat.Chat{
			CreatorID:            payerUserID,
			Members:              []*commonpb.UserId{payerUserID, recipientUserID},
			IsCreatorOnlySpeaker: true,
		})
		require.NoError(t, e.integration.AllowCreation(e.ctx, payment(groupID, payerKeys, recipientKeys, intentpb.PaymentAction_SEND), nil, nil))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, payment(groupID, recipientKeys, payerKeys, intentpb.PaymentAction_SEND), nil, nil), "sender cannot send")
	})

	// A private group takes payments once it has its key, as it takes
	// messages.
	t.Run("private_group", func(t *testing.T) {
		groupID := e.putGroupChat(t, &chat.Chat{
			CreatorID: payerUserID,
			Members:   []*commonpb.UserId{payerUserID, recipientUserID},
			IsPrivate: true,
		})
		record := func() *ocp_intent.Record {
			return payment(groupID, recipientKeys, payerKeys, intentpb.PaymentAction_SEND)
		}
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record(), nil, nil), "sender cannot send")

		_, err := e.chats.SetKeyEnvelope(e.ctx, groupID, payerUserID, chat.KeyEnvelopeFromProto(&chatpb.KeyEnvelope{
			Scheme:     chatpb.KeyEnvelope_X25519_XCHACHA20POLY1305,
			Nonce:      bytes.Repeat([]byte{1}, 24),
			Ciphertext: bytes.Repeat([]byte{1}, 48),
		}, payerUserID))
		require.NoError(t, err)
		require.NoError(t, e.integration.AllowCreation(e.ctx, record(), nil, nil))
	})

	// No payment is allowed to or from the Flipcash team account, even as a
	// member of the group.
	t.Run("team_account", func(t *testing.T) {
		groupID := e.putGroup(t, payerUserID, e.team)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, payment(groupID, payerKeys, e.teamKeys, intentpb.PaymentAction_SEND), nil, nil), "flipcash team")
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, payment(groupID, e.teamKeys, payerKeys, intentpb.PaymentAction_SEND), nil, nil), "flipcash team")
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

	// A contact payment referencing the pair's DM must fail: the two
	// chat types derive distinct canonical IDs.
	t.Run("rejected_dm_chat_id", func(t *testing.T) {
		dmChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, senderUserID, recipientUserID)
		r := record(contactDmChatMetadata(dmChatID, senderPhone, recipientPhone))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, r, nil, nil), "chat id does not match")
	})
}

func TestIntegration_GetTasksToSchedule_DmPayment(t *testing.T) {
	e := newIntegrationEnv(t)

	senderUserID, senderKeys := e.bindUser(t)
	recipientUserID, recipientKeys := e.bindUser(t)

	dmChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, senderUserID, recipientUserID)
	record := dmPaymentIntentRecord(t, dmChatMetadata(dmChatID), base58.Encode(senderKeys.Public()), base58.Encode(recipientKeys.Public()))

	tasks, err := e.integration.GetTasksToSchedule(e.ctx, record)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, intent.TaskTypeSendDmPaymentMessage, tasks[0].Type)
	require.NotNil(t, tasks[0].ReferenceId)
	assert.Equal(t, record.IntentId, *tasks[0].ReferenceId)
	require.NoError(t, tasks[0].Validate())
}

func TestIntegration_GetTasksToSchedule_GroupPayment(t *testing.T) {
	e := newIntegrationEnv(t)

	payerUserID, payerKeys := e.bindUser(t)
	recipientUserID, recipientKeys := e.bindUser(t)

	groupID := e.putGroup(t, payerUserID, recipientUserID)
	record := dmPaymentIntentRecord(t, groupChatMetadata(groupID, intentpb.PaymentAction_SEND), base58.Encode(payerKeys.Public()), base58.Encode(recipientKeys.Public()))

	tasks, err := e.integration.GetTasksToSchedule(e.ctx, record)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, intent.TaskTypeSendGroupPaymentMessage, tasks[0].Type)
	require.NotNil(t, tasks[0].ReferenceId)
	assert.Equal(t, record.IntentId, *tasks[0].ReferenceId)
	require.NoError(t, tasks[0].Validate())
}

// A multi payment is made for a reaction boost alone, and is limited to staff
// while multi payments roll out. A staff boost that passes is covered by
// TestIntegration_AllowCreation_ReactionBoost.
func TestIntegration_AllowCreation_MultiPayment(t *testing.T) {
	e := newIntegrationEnv(t)

	staffUserID, staffKeys := e.bindUser(t)
	e.setStaff(staffUserID)
	nonStaffUserID, nonStaffKeys := e.bindUser(t)
	recipient1UserID, recipient1Keys := e.bindUser(t)
	recipient2UserID, recipient2Keys := e.bindUser(t)

	recipients := []string{base58.Encode(recipient1Keys.Public()), base58.Encode(recipient2Keys.Public())}

	withChatMetadata := func(record *ocp_intent.Record, chatMetadata *intentpb.ChatMetadata) *ocp_intent.Record {
		appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
			Domain: &intentpb.AppMetadata_Chat{Chat: chatMetadata},
		})
		require.NoError(t, err)
		record.AppMetadata = appMetadata
		return record
	}
	boost := func(payer model.KeyPair) *ocp_intent.Record {
		record := multiPaymentIntentRecord(base58.Encode(payer.Public()), recipients...)
		return withChatMetadata(record, reactionBoostMetadata(chat.MustGenerateGroupChatID(), &messagingpb.MessageId{Value: 1}, "🔥"))
	}

	// Whoever pays, staff included.
	t.Run("no app metadata", func(t *testing.T) {
		for _, payer := range []model.KeyPair{staffKeys, nonStaffKeys} {
			record := multiPaymentIntentRecord(base58.Encode(payer.Public()), recipients...)
			require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "only supported for reaction boosts")
		}
	})

	t.Run("other app metadata", func(t *testing.T) {
		dmChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, staffUserID, nonStaffUserID)
		record := withChatMetadata(multiPaymentIntentRecord(base58.Encode(staffKeys.Public()), recipients...), dmChatMetadata(dmChatID))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "only supported for reaction boosts")
	})

	t.Run("non-staff payer", func(t *testing.T) {
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, boost(nonStaffKeys), nil, nil), "multi payments are limited to staff")
	})

	t.Run("payer is not a flipcash user", func(t *testing.T) {
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, boost(model.MustGenerateKeyPair()), nil, nil), "payer is not a flipcash user")
	})

	// Staff recipients don't make a non-staff payer's multi payment allowed
	t.Run("staff recipients", func(t *testing.T) {
		e.setStaff(recipient1UserID)
		e.setStaff(recipient2UserID)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, boost(nonStaffKeys), nil, nil), "multi payments are limited to staff")
	})
}

// A group payment made as a multi payment pays several members at once,
// under the same gates as a payment to one, plus the multi payment's staff
// gate.
func TestIntegration_AllowCreation_GroupMultiPayment(t *testing.T) {
	e := newIntegrationEnv(t)

	payerUserID, payerKeys := e.bindUser(t)
	e.setStaff(payerUserID)
	nonStaffUserID, nonStaffKeys := e.bindUser(t)
	outsiderUserID, outsiderKeys := e.bindUser(t)
	e.setStaff(outsiderUserID)

	recipientUserIDs := make([]*commonpb.UserId, intent.MaxGroupPaymentReceivers+1)
	recipientKeys := make([]model.KeyPair, intent.MaxGroupPaymentReceivers+1)
	for j := range recipientUserIDs {
		recipientUserIDs[j], recipientKeys[j] = e.bindUser(t)
	}

	// Every recipient but the last is a member, so the last can stand in for
	// an eleventh.
	groupID := e.putGroup(t, payerUserID, append([]*commonpb.UserId{nonStaffUserID}, recipientUserIDs[:intent.MaxGroupPaymentReceivers]...)...)

	payment := func(chatID *commonpb.ChatId, action intentpb.PaymentAction, from model.KeyPair, to ...model.KeyPair) *ocp_intent.Record {
		owners := make([]string, len(to))
		for j, keys := range to {
			owners[j] = base58.Encode(keys.Public())
		}
		record := multiPaymentIntentRecord(base58.Encode(from.Public()), owners...)
		appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
			Domain: &intentpb.AppMetadata_Chat{Chat: groupChatMetadata(chatID, action)},
		})
		require.NoError(t, err)
		record.AppMetadata = appMetadata
		return record
	}
	send := func(from model.KeyPair, to ...model.KeyPair) *ocp_intent.Record {
		return payment(groupID, intentpb.PaymentAction_SEND, from, to...)
	}

	t.Run("valid", func(t *testing.T) {
		for _, action := range []intentpb.PaymentAction{intentpb.PaymentAction_DEFAULT, intentpb.PaymentAction_SEND} {
			require.NoError(t, e.integration.AllowCreation(e.ctx, payment(groupID, action, payerKeys, recipientKeys[:2]...), nil, nil), action)
		}
		require.NoError(t, e.integration.AllowCreation(e.ctx, send(payerKeys, recipientKeys[:intent.MaxGroupPaymentReceivers]...), nil, nil))
	})

	t.Run("denied_non_staff_payer", func(t *testing.T) {
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, send(nonStaffKeys, recipientKeys[:2]...), nil, nil), "multi payments are limited to staff")
	})

	t.Run("denied_tip", func(t *testing.T) {
		record := payment(groupID, intentpb.PaymentAction_TIP, payerKeys, recipientKeys[:2]...)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "action is not supported")
	})

	t.Run("denied_dm_chat_id", func(t *testing.T) {
		dmChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, payerUserID, recipientUserIDs[0])
		record := payment(dmChatID, intentpb.PaymentAction_SEND, payerKeys, recipientKeys[:2]...)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "not a group")
	})

	t.Run("denied_too_many_recipients", func(t *testing.T) {
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, send(payerKeys, recipientKeys...), nil, nil), "more than 10 members")
	})

	t.Run("denied_recipient_not_flipcash_user", func(t *testing.T) {
		record := send(payerKeys, recipientKeys[0], model.MustGenerateKeyPair())
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "recipient is not a flipcash user")
	})

	t.Run("denied_self_payment", func(t *testing.T) {
		record := send(payerKeys, recipientKeys[0], payerKeys)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "payment to yourself")
	})

	t.Run("denied_repeated_recipient", func(t *testing.T) {
		record := send(payerKeys, recipientKeys[0], recipientKeys[1], recipientKeys[0])
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "more than once")
	})

	t.Run("denied_group_not_found", func(t *testing.T) {
		record := payment(chat.MustGenerateGroupChatID(), intentpb.PaymentAction_SEND, payerKeys, recipientKeys[:2]...)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "sender cannot send")
	})

	t.Run("denied_payer_not_member", func(t *testing.T) {
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, send(outsiderKeys, recipientKeys[:2]...), nil, nil), "sender cannot send")
	})

	// One recipient who is not a member refuses the whole payment, wherever
	// they are in it.
	t.Run("denied_recipient_not_member", func(t *testing.T) {
		for _, record := range []*ocp_intent.Record{
			send(payerKeys, outsiderKeys, recipientKeys[0]),
			send(payerKeys, recipientKeys[0], recipientKeys[1], outsiderKeys),
		} {
			require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "recipient is not a member")
		}
	})

	// In a creator-only group, only the creator may pay, since a payment
	// speaks; anyone may be paid, since receiving does not.
	t.Run("creator_only_speaker", func(t *testing.T) {
		groupID := e.putGroupChat(t, &chat.Chat{
			CreatorID:            payerUserID,
			Members:              []*commonpb.UserId{payerUserID, outsiderUserID, recipientUserIDs[0], recipientUserIDs[1]},
			IsCreatorOnlySpeaker: true,
		})
		require.NoError(t, e.integration.AllowCreation(e.ctx, payment(groupID, intentpb.PaymentAction_SEND, payerKeys, recipientKeys[0], recipientKeys[1]), nil, nil))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, payment(groupID, intentpb.PaymentAction_SEND, outsiderKeys, recipientKeys[0], recipientKeys[1]), nil, nil), "sender cannot send")
	})

	t.Run("denied_private_group_without_key", func(t *testing.T) {
		groupID := e.putGroupChat(t, &chat.Chat{
			CreatorID: payerUserID,
			Members:   []*commonpb.UserId{payerUserID, recipientUserIDs[0], recipientUserIDs[1]},
			IsPrivate: true,
		})
		record := payment(groupID, intentpb.PaymentAction_SEND, payerKeys, recipientKeys[0], recipientKeys[1])
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "sender cannot send")
	})

	// No payment is allowed to the Flipcash team account, even as one of
	// several members paid.
	t.Run("team_account", func(t *testing.T) {
		groupID := e.putGroup(t, payerUserID, recipientUserIDs[0], e.team)
		record := payment(groupID, intentpb.PaymentAction_SEND, payerKeys, recipientKeys[0], e.teamKeys)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "flipcash team")
	})
}

// A multi payment schedules the boost task when it carries a reaction boost,
// the group payment message when it carries a group payment, and nothing
// otherwise.
func TestIntegration_GetTasksToSchedule_MultiPayment(t *testing.T) {
	e := newIntegrationEnv(t)

	staffUserID, staffKeys := e.bindUser(t)
	e.setStaff(staffUserID)
	_, recipient1Keys := e.bindUser(t)
	_, recipient2Keys := e.bindUser(t)

	record := multiPaymentIntentRecord(base58.Encode(staffKeys.Public()), base58.Encode(recipient1Keys.Public()), base58.Encode(recipient2Keys.Public()))
	tasks, err := e.integration.GetTasksToSchedule(e.ctx, record)
	require.NoError(t, err)
	assert.Empty(t, tasks)

	for _, tc := range []struct {
		chatMetadata *intentpb.ChatMetadata
		taskType     uint32
	}{
		{reactionBoostMetadata(chat.MustGenerateGroupChatID(), &messagingpb.MessageId{Value: 1}, "🔥"), intent.TaskTypeRecordReactionBoost},
		{groupChatMetadata(chat.MustGenerateGroupChatID(), intentpb.PaymentAction_SEND), intent.TaskTypeSendGroupPaymentMessage},
	} {
		appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
			Domain: &intentpb.AppMetadata_Chat{Chat: tc.chatMetadata},
		})
		require.NoError(t, err)
		record.AppMetadata = appMetadata
		tasks, err = e.integration.GetTasksToSchedule(e.ctx, record)
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		assert.Equal(t, tc.taskType, tasks[0].Type)
		require.NotNil(t, tasks[0].ReferenceId)
		assert.Equal(t, record.IntentId, *tasks[0].ReferenceId)
	}
}

// IsUserChatPayment holds for the chat payments whose parties know each other
// by user ID, and nothing else.
func TestIsUserChatPayment(t *testing.T) {
	marshal := func(chatMetadata *intentpb.ChatMetadata) []byte {
		appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
			Domain: &intentpb.AppMetadata_Chat{Chat: chatMetadata},
		})
		require.NoError(t, err)
		return appMetadata
	}
	dmChatID := &commonpb.ChatId{Value: make([]byte, 32)}

	assert.True(t, intent.IsUserChatPayment(marshal(dmChatMetadata(dmChatID))))
	assert.True(t, intent.IsUserChatPayment(marshal(groupChatMetadata(chat.MustGenerateGroupChatID(), intentpb.PaymentAction_SEND))))
	assert.True(t, intent.IsUserChatPayment(marshal(reactionBoostMetadata(chat.MustGenerateGroupChatID(), &messagingpb.MessageId{Value: 1}, "🔥"))))
	assert.False(t, intent.IsUserChatPayment(marshal(contactDmChatMetadata(dmChatID, "+12223334444", "+13334445555"))))
	assert.False(t, intent.IsUserChatPayment(nil))
	assert.False(t, intent.IsUserChatPayment([]byte{0xff, 0xff}))
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

// putMessage sends a text message into chatID as senderID (nil for a system
// message, which carries system content instead).
func (e *integrationEnv) putMessage(t *testing.T, chatID *commonpb.ChatId, senderID *commonpb.UserId) *messaging.Message {
	content := []*messagingpb.Content{{Type: &messagingpb.Content_Text{Text: &messagingpb.TextContent{Text: "hello"}}}}
	if senderID == nil {
		content = []*messagingpb.Content{{Type: &messagingpb.Content_System{System: &messagingpb.SystemContent{}}}}
	}
	msg, _, err := e.messages.PutMessage(e.ctx, chatID, senderID, content, time.Now(), &messagingpb.ClientMessageId{Value: model.MustGenerateUserID().Value}, true)
	require.NoError(t, err)
	return msg
}

// putEncryptedMessage sends a message encrypted under a private group's chat
// key into chatID as senderID. The server never reads the ciphertext.
func (e *integrationEnv) putEncryptedMessage(t *testing.T, chatID *commonpb.ChatId, senderID *commonpb.UserId) *messaging.Message {
	content := []*messagingpb.Content{{Type: &messagingpb.Content_Encrypted{Encrypted: &messagingpb.EncryptedContent{
		Scheme:     messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305,
		Nonce:      make([]byte, 24),
		Ciphertext: []byte("ciphertext"),
	}}}}
	msg, _, err := e.messages.PutMessage(e.ctx, chatID, senderID, content, time.Now(), &messagingpb.ClientMessageId{Value: model.MustGenerateUserID().Value}, true)
	require.NoError(t, err)
	return msg
}

// react adds userID's reaction with emoji on a message.
func (e *integrationEnv) react(t *testing.T, chatID *commonpb.ChatId, messageID *messagingpb.MessageId, userID *commonpb.UserId, emoji string) {
	_, _, tooManyTypes, err := e.messages.AddReaction(e.ctx, chatID, messageID, userID, emoji, time.Now())
	require.NoError(t, err)
	require.False(t, tooManyTypes)
}

// A boost that pays one person (the creator for their own message, the sender
// when the creator boosts) is a SendPublicPayment, and one that pays both, 80%
// to the sender and 20% to the creator, a SendPublicMultiPayment.
func TestIntegration_AllowCreation_ReactionBoost(t *testing.T) {
	e := newIntegrationEnv(t)

	creatorUserID, creatorKeys := e.bindUser(t)
	memberUserID, memberKeys := e.bindUser(t)
	payerUserID, payerKeys := e.bindUser(t)
	_, outsiderKeys := e.bindUser(t)

	groupID := e.putGroup(t, creatorUserID, memberUserID, payerUserID)
	byCreator := e.putMessage(t, groupID, creatorUserID)
	byMember := e.putMessage(t, groupID, memberUserID)
	byPayer := e.putMessage(t, groupID, payerUserID)

	// A boost backs a reaction the payer already holds.
	for _, msg := range []*messaging.Message{byCreator, byMember, byPayer} {
		e.react(t, groupID, msg.ID, payerUserID, "🔥")
		e.react(t, groupID, msg.ID, creatorUserID, "🔥")
	}

	boost := func(chatID *commonpb.ChatId, messageID *messagingpb.MessageId, emoji string, from, to model.KeyPair) *ocp_intent.Record {
		return dmPaymentIntentRecord(t, reactionBoostMetadata(chatID, messageID, emoji), base58.Encode(from.Public()), base58.Encode(to.Public()))
	}
	validRecord := func() *ocp_intent.Record {
		return boost(groupID, byCreator.ID, "🔥", payerKeys, creatorKeys)
	}

	t.Run("valid_one_payee", func(t *testing.T) {
		for name, record := range map[string]*ocp_intent.Record{
			"creator's message pays the creator": boost(groupID, byCreator.ID, "🔥", payerKeys, creatorKeys),
			"creator boosting pays the sender":   boost(groupID, byMember.ID, "🔥", creatorKeys, memberKeys),
		} {
			require.NoError(t, e.integration.AllowCreation(e.ctx, record, nil, nil), name)
		}
	})

	// A SendPublicPayment carries a boost that pays one person alone: one
	// owed to both the sender and the creator is a multi payment, whichever
	// of them the single payment would pay.
	t.Run("denied_split_needs_multi_payment", func(t *testing.T) {
		for name, record := range map[string]*ocp_intent.Record{
			"paying the sender":  boost(groupID, byMember.ID, "🔥", payerKeys, memberKeys),
			"paying the creator": boost(groupID, byMember.ID, "🔥", payerKeys, creatorKeys),
		} {
			require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "must be a multi payment", name)
		}
	})

	// A boost that pays both the sender and the creator is a multi payment,
	// 80% to the sender and 20% to the creator, the creator's share within a
	// quark of exact.
	t.Run("multi_payment", func(t *testing.T) {
		e.setStaff(payerUserID)
		e.setStaff(creatorUserID)
		defer delete(e.staff, string(payerUserID.Value))
		defer delete(e.staff, string(creatorUserID.Value))

		// Each payment is a fiat amount, in USD, carried by the quarks worth
		// it (10_000 to the dollar here), as OCP has checked by validation.
		type pay struct {
			to     model.KeyPair
			amount float64
		}
		multiBoost := func(messageID *messagingpb.MessageId, from model.KeyPair, payments ...pay) *ocp_intent.Record {
			var owners []string
			for _, payment := range payments {
				owners = append(owners, base58.Encode(payment.to.Public()))
			}
			record := multiPaymentIntentRecord(base58.Encode(from.Public()), owners...)
			metadata := record.SendPublicMultiPaymentMetadata
			metadata.Quantity, metadata.NativeAmount, metadata.UsdMarketValue = 0, 0, 0
			for j, payment := range payments {
				quarks := uint64(math.Round(payment.amount * 10_000))
				metadata.Payments[j].Quantity = quarks
				metadata.Payments[j].NativeAmount = payment.amount
				metadata.Payments[j].UsdMarketValue = payment.amount
				metadata.Quantity += quarks
				metadata.NativeAmount += payment.amount
				metadata.UsdMarketValue += payment.amount
			}
			appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
				Domain: &intentpb.AppMetadata_Chat{Chat: reactionBoostMetadata(groupID, messageID, "🔥")},
			})
			require.NoError(t, err)
			record.AppMetadata = appMetadata
			return record
		}

		// The split is judged on each payment's fiat amount, to within half a
		// cent of its share, so a share rounded to the cent either way passes.
		for name, record := range map[string]*ocp_intent.Record{
			"80/20":         multiBoost(byMember.ID, payerKeys, pay{memberKeys, 0.80}, pay{creatorKeys, 0.20}),
			"creator first": multiBoost(byMember.ID, payerKeys, pay{creatorKeys, 0.20}, pay{memberKeys, 0.80}),
			// A fifth of $0.07 is $0.014, and of $0.08, $0.016.
			"creator rounded down": multiBoost(byMember.ID, payerKeys, pay{memberKeys, 0.06}, pay{creatorKeys, 0.01}),
			"creator rounded up":   multiBoost(byMember.ID, payerKeys, pay{memberKeys, 0.06}, pay{creatorKeys, 0.02}),
			"the smallest split":   multiBoost(byMember.ID, payerKeys, pay{memberKeys, 0.04}, pay{creatorKeys, 0.01}),
		} {
			require.NoError(t, e.integration.AllowCreation(e.ctx, record, nil, nil), name)
		}

		for name, tc := range map[string]struct {
			record *ocp_intent.Record
			err    string
		}{
			"creator a cent over":               {multiBoost(byMember.ID, payerKeys, pay{memberKeys, 0.79}, pay{creatorKeys, 0.21}), "boost payouts are not allowed"},
			"creator a cent under":              {multiBoost(byMember.ID, payerKeys, pay{memberKeys, 0.81}, pay{creatorKeys, 0.19}), "boost payouts are not allowed"},
			"even split":                        {multiBoost(byMember.ID, payerKeys, pay{memberKeys, 0.50}, pay{creatorKeys, 0.50}), "boost payouts are not allowed"},
			"20/80":                             {multiBoost(byMember.ID, payerKeys, pay{memberKeys, 0.20}, pay{creatorKeys, 0.80}), "boost payouts are not allowed"},
			"a stranger instead of the creator": {multiBoost(byMember.ID, payerKeys, pay{memberKeys, 0.80}, pay{outsiderKeys, 0.20}), "boost payouts are not allowed"},
			"the creator's own message":         {multiBoost(byCreator.ID, payerKeys, pay{creatorKeys, 0.80}, pay{memberKeys, 0.20}), "must be a single payment"},
			"the creator boosting":              {multiBoost(byMember.ID, creatorKeys, pay{memberKeys, 0.80}, pay{payerKeys, 0.20}), "must be a single payment"},
			"the payer's own message":           {multiBoost(byPayer.ID, payerKeys, pay{payerKeys, 0.80}, pay{creatorKeys, 0.20}), "cannot boost their own message"},
		} {
			require.ErrorContains(t, e.integration.AllowCreation(e.ctx, tc.record, nil, nil), tc.err, name)
		}

		// Every share is at least the currency's minimum transfer value,
		// rounded to its minor unit, so an 80/20 boost is at least 5 cents.
		for _, tc := range []struct {
			sender, creator float64
			expectedErr     string
		}{
			{0.04, 0.01, ""},
			{0.04, 0.005, ""}, // rounds to a cent, and is within half a cent of a fifth of $0.045
			{0.04, 0.0049, "boost share is below the minimum of 0.01 USD"},
			{0.0049, 0.04, "boost share is below the minimum of 0.01 USD"},
		} {
			record := multiBoost(byMember.ID, payerKeys, pay{memberKeys, tc.sender}, pay{creatorKeys, tc.creator})
			err := e.integration.AllowCreation(e.ctx, record, nil, nil)
			if tc.expectedErr == "" {
				require.NoError(t, err, "%v/%v", tc.sender, tc.creator)
			} else {
				require.ErrorContains(t, err, tc.expectedErr, "%v/%v", tc.sender, tc.creator)
			}
		}

		// Multi payments are limited to staff while they roll out, a boost's
		// included.
		delete(e.staff, string(payerUserID.Value))
		record := multiBoost(byMember.ID, payerKeys, pay{memberKeys, 0.80}, pay{creatorKeys, 0.20})
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "multi payments are limited to staff")
	})

	// Paying the wrong person pays a stranger to the boost.
	t.Run("denied_payouts", func(t *testing.T) {
		for name, record := range map[string]*ocp_intent.Record{
			"a stranger to the boost": boost(groupID, byCreator.ID, "🔥", payerKeys, memberKeys),
			"the payer themself":      boost(groupID, byCreator.ID, "🔥", payerKeys, payerKeys),
		} {
			require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "boost payouts are not allowed", name)
		}
	})

	// No one boosts their own message, the creator included, whoever the
	// payment pays.
	t.Run("denied_own_message", func(t *testing.T) {
		for name, record := range map[string]*ocp_intent.Record{
			"member paying the creator":  boost(groupID, byPayer.ID, "🔥", payerKeys, creatorKeys),
			"creator paying a member":    boost(groupID, byCreator.ID, "🔥", creatorKeys, memberKeys),
			"creator paying no one else": boost(groupID, byCreator.ID, "🔥", creatorKeys, creatorKeys),
		} {
			require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "cannot boost their own message", name)
		}
	})

	t.Run("denied_dm_chat_id", func(t *testing.T) {
		dmChatID := chat.MustDeriveDmChatID(chatpb.ChatType_DM, payerUserID, creatorUserID)
		record := boost(dmChatID, byCreator.ID, "🔥", payerKeys, creatorKeys)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "not a group")
	})

	t.Run("denied_invalid_emoji", func(t *testing.T) {
		for _, emoji := range []string{"a", "🔥🔥", "🔥 "} {
			record := boost(groupID, byCreator.ID, emoji, payerKeys, creatorKeys)
			require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "not a single emoji", emoji)
		}
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

	// A boost is at least its currency's minimum transfer value, rounded to
	// the currency's minor unit, and worth at least a micro-dollar.
	t.Run("minimum_amount", func(t *testing.T) {
		for _, tc := range []struct {
			currency       string
			nativeAmount   float64
			usdMarketValue float64
			expectedErr    string
		}{
			{"usd", 0.01, 0.01, ""},   // exactly a cent
			{"usd", 0.005, 0.005, ""}, // rounds to a cent
			{"usd", 0.0049, 0.0049, "below the minimum of 0.01 USD"},
			{"usd", 0, 0, "below the minimum of 0.01 USD"},
			{"jpy", 1, 0.0067, ""}, // exactly a yen
			{"jpy", 0.4, 0.0027, "below the minimum of 1 JPY"},
			{"kwd", 0.001, 0.0033, ""}, // exactly a fils
			{"kwd", 0.0004, 0.0013, "below the minimum of 0.001 KWD"},
			{"usd", 0.01, 0.0000004, "worth less than a micro-dollar"},
		} {
			record := validRecord()
			record.SendPublicPaymentMetadata.ExchangeCurrency = currency_lib.Code(tc.currency)
			record.SendPublicPaymentMetadata.NativeAmount = tc.nativeAmount
			record.SendPublicPaymentMetadata.UsdMarketValue = tc.usdMarketValue

			err := e.integration.AllowCreation(e.ctx, record, nil, nil)
			if tc.expectedErr == "" {
				require.NoError(t, err, "%s %v", tc.currency, tc.nativeAmount)
			} else {
				require.ErrorContains(t, err, tc.expectedErr, "%s %v", tc.currency, tc.nativeAmount)
			}
		}
	})

	t.Run("denied_payer_not_flipcash_user", func(t *testing.T) {
		record := validRecord()
		record.InitiatorOwnerAccount = base58.Encode(model.MustGenerateKeyPair().Public())
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "payer is not a flipcash user")
	})

	t.Run("denied_recipient_not_flipcash_user", func(t *testing.T) {
		record := validRecord()
		record.SendPublicPaymentMetadata.DestinationOwnerAccount = base58.Encode(model.MustGenerateKeyPair().Public())
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "recipient is not a flipcash user")
	})

	t.Run("denied_payer_not_member", func(t *testing.T) {
		record := boost(groupID, byCreator.ID, "🔥", outsiderKeys, creatorKeys)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "not a member")

		record = boost(chat.MustGenerateGroupChatID(), byCreator.ID, "🔥", payerKeys, creatorKeys)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "not a member")
	})

	t.Run("denied_message_not_found", func(t *testing.T) {
		record := boost(groupID, &messagingpb.MessageId{Value: 1_000}, "🔥", payerKeys, creatorKeys)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "message not found")
	})

	t.Run("denied_system_message", func(t *testing.T) {
		system := e.putMessage(t, groupID, nil)
		record := boost(groupID, system.ID, "🔥", payerKeys, creatorKeys)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, record, nil, nil), "cannot be reacted to")
	})

	// A deleted message is reactable, so it may be boosted, and its
	// tombstone keeps its sender, who is paid as before.
	t.Run("deleted_message", func(t *testing.T) {
		msg := e.putMessage(t, groupID, memberUserID)
		e.react(t, groupID, msg.ID, creatorUserID, "🔥")
		e.react(t, groupID, msg.ID, payerUserID, "🔥")
		_, err := e.messages.DeleteMessage(e.ctx, groupID, msg.ID, memberUserID, time.Now(), msg.EventSequence)
		require.NoError(t, err)

		require.NoError(t, e.integration.AllowCreation(e.ctx, boost(groupID, msg.ID, "🔥", creatorKeys, memberKeys), nil, nil))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, boost(groupID, msg.ID, "🔥", payerKeys, creatorKeys), nil, nil), "must be a multi payment")
	})

	// A boost backs a reaction the payer holds: an emoji they have not
	// reacted with, or one they have removed, cannot be boosted, even when
	// others hold it.
	t.Run("denied_not_reacted", func(t *testing.T) {
		msg := e.putMessage(t, groupID, creatorUserID)
		e.react(t, groupID, msg.ID, memberUserID, "🔥")
		e.react(t, groupID, msg.ID, payerUserID, "👍")
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, boost(groupID, msg.ID, "🔥", payerKeys, creatorKeys), nil, nil), "has not reacted")
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, boost(groupID, msg.ID, "🎉", payerKeys, creatorKeys), nil, nil), "has not reacted")

		e.react(t, groupID, msg.ID, payerUserID, "🔥")
		require.NoError(t, e.integration.AllowCreation(e.ctx, boost(groupID, msg.ID, "🔥", payerKeys, creatorKeys), nil, nil))

		_, removed, err := e.messages.RemoveReaction(e.ctx, groupID, msg.ID, payerUserID, "🔥")
		require.NoError(t, err)
		require.True(t, removed)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, boost(groupID, msg.ID, "🔥", payerKeys, creatorKeys), nil, nil), "has not reacted")
	})

	// A private group takes boosts as a public one does: its messages are
	// encrypted under the chat key but reactable, and the payer need only be
	// a member.
	t.Run("private_group", func(t *testing.T) {
		privateID := e.putGroupChat(t, &chat.Chat{
			CreatorID: creatorUserID,
			Members:   []*commonpb.UserId{creatorUserID, memberUserID, payerUserID},
			IsPrivate: true,
		})
		rules, err := e.chats.GetGroupRules(e.ctx, privateID)
		require.NoError(t, err)
		require.True(t, rules.IsPrivate)
		byCreator := e.putEncryptedMessage(t, privateID, creatorUserID)
		byMember := e.putEncryptedMessage(t, privateID, memberUserID)
		for _, msg := range []*messaging.Message{byCreator, byMember} {
			e.react(t, privateID, msg.ID, payerUserID, "🔥")
			e.react(t, privateID, msg.ID, creatorUserID, "🔥")
		}

		// One payee: a single payment.
		require.NoError(t, e.integration.AllowCreation(e.ctx, boost(privateID, byCreator.ID, "🔥", payerKeys, creatorKeys), nil, nil))
		require.NoError(t, e.integration.AllowCreation(e.ctx, boost(privateID, byMember.ID, "🔥", creatorKeys, memberKeys), nil, nil))
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, boost(privateID, byMember.ID, "🔥", payerKeys, memberKeys), nil, nil), "must be a multi payment")

		// Two payees: a multi payment, 80% to the sender and 20% to the
		// creator.
		e.setStaff(payerUserID)
		defer delete(e.staff, string(payerUserID.Value))
		split := multiPaymentIntentRecord(base58.Encode(payerKeys.Public()), base58.Encode(memberKeys.Public()), base58.Encode(creatorKeys.Public()))
		metadata := split.SendPublicMultiPaymentMetadata
		metadata.Payments[0].Quantity, metadata.Payments[1].Quantity, metadata.Quantity = 8_000, 2_000, 10_000
		metadata.Payments[0].NativeAmount, metadata.Payments[1].NativeAmount, metadata.NativeAmount = 0.80, 0.20, 1.00
		appMetadata, err := proto.Marshal(&intentpb.AppMetadata{
			Domain: &intentpb.AppMetadata_Chat{Chat: reactionBoostMetadata(privateID, byMember.ID, "🔥")},
		})
		require.NoError(t, err)
		split.AppMetadata = appMetadata
		require.NoError(t, e.integration.AllowCreation(e.ctx, split, nil, nil))

		// A private group has no non-member standing at all.
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, boost(privateID, byCreator.ID, "🔥", outsiderKeys, creatorKeys), nil, nil), "not a member")
	})

	// No boost is allowed to or from the Flipcash team account.
	t.Run("team_account", func(t *testing.T) {
		groupID := e.putGroup(t, e.team, payerUserID)
		byTeam := e.putMessage(t, groupID, e.team)
		byPayer := e.putMessage(t, groupID, payerUserID)
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, boost(groupID, byTeam.ID, "🔥", payerKeys, e.teamKeys), nil, nil), "flipcash team")
		require.ErrorContains(t, e.integration.AllowCreation(e.ctx, boost(groupID, byPayer.ID, "🔥", e.teamKeys, payerKeys), nil, nil), "flipcash team")
	})
}

func TestIntegration_GetTasksToSchedule_ReactionBoost(t *testing.T) {
	e := newIntegrationEnv(t)

	creatorUserID, creatorKeys := e.bindUser(t)
	payerUserID, payerKeys := e.bindUser(t)

	groupID := e.putGroup(t, creatorUserID, payerUserID)
	msg := e.putMessage(t, groupID, creatorUserID)
	record := dmPaymentIntentRecord(t, reactionBoostMetadata(groupID, msg.ID, "🔥"), base58.Encode(payerKeys.Public()), base58.Encode(creatorKeys.Public()))

	tasks, err := e.integration.GetTasksToSchedule(e.ctx, record)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, intent.TaskTypeRecordReactionBoost, tasks[0].Type)
	require.NotNil(t, tasks[0].ReferenceId)
	assert.Equal(t, record.IntentId, *tasks[0].ReferenceId)
	require.NoError(t, tasks[0].Validate())
}

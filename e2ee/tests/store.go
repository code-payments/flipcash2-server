package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	e2eepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/e2ee/v1"

	"github.com/code-payments/flipcash2-server/e2ee"
	"github.com/code-payments/flipcash2-server/model"
)

// RunStoreTests runs the shared e2ee.Store test suite against s. teardown is
// called between tests to reset the store.
func RunStoreTests(t *testing.T, s e2ee.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s e2ee.Store){
		testStore_RegisterDevice_AssignsIDs,
		testStore_RegisterDevice_Idempotent,
		testStore_RegisterDevice_Refusals,
		testStore_UnregisterDevice,
		testStore_LastSeen,
		testStore_AddCapabilities,
		testStore_SetKeys,
		testStore_TakePreKeyBundle,
		testStore_TakePreKeyBundle_Concurrent,
		testStore_TakePreKeyBundle_DuringReplacement,
		testStore_Deliver,
		testStore_Deliver_Concurrent,
		testStore_Deliver_Large,
		testStore_Deliver_Ephemeral,
		testStore_SetKeys_Concurrent,
		testStore_GetEnvelopes,
		testStore_AckEnvelopes,
		testStore_TakeTokens,
		testStore_TakeTokens_Concurrent,
	} {
		tf(t, s)
		teardown()
	}
}

type storeEnv struct {
	t     *testing.T
	ctx   context.Context
	store e2ee.Store
}

func newStoreEnv(t *testing.T, s e2ee.Store) *storeEnv {
	return &storeEnv{t: t, ctx: context.Background(), store: s}
}

// register registers a fresh device for userID with pools of the given sizes
// and returns the record and the fixture that holds its keys.
func (e *storeEnv) register(userID *commonpb.UserId, oneTime, kemOneTime int) (*e2ee.Device, *deviceFixture) {
	f := newDeviceFixture()
	d, created, err := e.store.RegisterDevice(e.ctx, newIdempotencyKey(), f.device(userID, model.MustGenerateKeyPair()), f.keys(oneTime, kemOneTime))
	require.NoError(e.t, err)
	require.True(e.t, created)
	return d, f
}

// envelope is a stored-shape envelope from source to recipient at clientTS.
func envelope(source, recipient e2ee.DeviceAddress, clientTS time.Time) *e2ee.Envelope {
	src := source.Clone()
	ts := clientTS
	return &e2ee.Envelope{
		Recipient:            recipient,
		ChatID:               e2ee.DeriveDmChatID(source.UserID, recipient.UserID),
		Source:               &src,
		Type:                 e2eepb.Envelope_CIPHERTEXT,
		Content:              randBytes(64),
		ClientTS:             &ts,
		ServerTS:             time.Now().UTC(),
		WantsDeliveryReceipt: true,
		// Whole seconds, as a store's TTL attribute holds it, so a stored
		// envelope compares equal to what was sent.
		ExpiresAt: time.Now().UTC().Add(time.Hour).Truncate(time.Second),
	}
}

func testStore_RegisterDevice_AssignsIDs(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	userID := model.MustGenerateUserID()

	for want := uint32(1); want <= e2ee.MaxDevicesPerUser; want++ {
		d, f := e.register(userID, 2, 2)
		require.Equal(t, want, d.Address.DeviceID)
		require.Equal(t, userID.Value, d.Address.UserID.Value)
		require.Nil(t, d.LastSeen)

		// The record round-trips whole, capabilities and app install
		// included.
		got, err := s.GetDevice(e.ctx, d.Address)
		require.NoError(t, err)
		require.Equal(t, fixtureCapabilities, got.Capabilities)
		require.Equal(t, f.appInstall, got.AppInstall)
	}

	f := newDeviceFixture()
	_, _, err := s.RegisterDevice(e.ctx, newIdempotencyKey(), f.device(userID, model.MustGenerateKeyPair()), f.keys(0, 0))
	require.ErrorIs(t, err, e2ee.ErrTooManyDevices)

	devices, err := s.GetDevices(e.ctx, userID)
	require.NoError(t, err)
	require.Len(t, devices, e2ee.MaxDevicesPerUser)
	for i, d := range devices {
		require.Equal(t, uint32(i+1), d.Address.DeviceID)
	}

	// Freeing an id in the middle makes it the next assigned: the lowest
	// free one.
	require.NoError(t, s.UnregisterDevice(e.ctx, e2ee.DeviceAddress{UserID: userID, DeviceID: 3}))
	d, _ := e.register(userID, 0, 0)
	require.Equal(t, uint32(3), d.Address.DeviceID)

	// Another user's ids are their own.
	other, _ := e.register(model.MustGenerateUserID(), 0, 0)
	require.Equal(t, uint32(1), other.Address.DeviceID)
}

func testStore_RegisterDevice_Idempotent(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	userID := model.MustGenerateUserID()
	account := model.MustGenerateKeyPair()
	key := newIdempotencyKey()

	f := newDeviceFixture()
	first, created, err := s.RegisterDevice(e.ctx, key, f.device(userID, account), f.keys(1, 1))
	require.NoError(t, err)
	require.True(t, created)

	// A retry under the same key returns the first device whatever it
	// carries, and registers nothing.
	g := newDeviceFixture()
	again, created, err := s.RegisterDevice(e.ctx, key, g.device(userID, account), g.keys(3, 3))
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, first.Address.DeviceID, again.Address.DeviceID)
	require.Equal(t, first.IdentityKey, again.IdentityKey)

	devices, err := s.GetDevices(e.ctx, userID)
	require.NoError(t, err)
	require.Len(t, devices, 1)

	counts, err := s.GetKeyStatus(e.ctx, first.Address)
	require.NoError(t, err)
	require.Equal(t, e2ee.PreKeyCounts{OneTimePreKeys: 1, KemOneTimePreKeys: 1}, counts.Counts)

	// Once the device is gone the key no longer binds.
	require.NoError(t, s.UnregisterDevice(e.ctx, first.Address))
	fresh, created, err := s.RegisterDevice(e.ctx, key, g.device(userID, account), g.keys(0, 0))
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, g.identityKey(), fresh.IdentityKey)
}

func testStore_RegisterDevice_Refusals(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	userID := model.MustGenerateUserID()
	account := model.MustGenerateKeyPair()

	f := newDeviceFixture()
	_, _, err := s.RegisterDevice(e.ctx, newIdempotencyKey(), f.device(userID, account), f.keys(1, 1))
	require.NoError(t, err)

	// The same identity key again, under a new idempotency key.
	_, _, err = s.RegisterDevice(e.ctx, newIdempotencyKey(), f.device(userID, account), f.keys(1, 1))
	require.ErrorIs(t, err, e2ee.ErrDuplicateIdentityKey)

	// Another user may hold the same key: uniqueness is per user.
	_, created, err := s.RegisterDevice(e.ctx, newIdempotencyKey(), f.device(model.MustGenerateUserID(), account), f.keys(1, 1))
	require.NoError(t, err)
	require.True(t, created)

	// A KEM id shared between the last-resort key and the pool.
	g := newDeviceFixture()
	keys := g.keys(0, 2)
	keys.KemOneTimePreKeys[1].ID = keys.KemLastResortPreKey.ID
	_, _, err = s.RegisterDevice(e.ctx, newIdempotencyKey(), g.device(userID, account), keys)
	require.ErrorIs(t, err, e2ee.ErrDuplicatePreKeyID)

	// Within the pool.
	keys = g.keys(0, 2)
	keys.KemOneTimePreKeys[1].ID = keys.KemOneTimePreKeys[0].ID
	_, _, err = s.RegisterDevice(e.ctx, newIdempotencyKey(), g.device(userID, account), keys)
	require.ErrorIs(t, err, e2ee.ErrDuplicatePreKeyID)

	// Two one-time curve prekeys sharing an id.
	keys = g.keys(2, 0)
	keys.OneTimePreKeys[1].ID = keys.OneTimePreKeys[0].ID
	_, _, err = s.RegisterDevice(e.ctx, newIdempotencyKey(), g.device(userID, account), keys)
	require.ErrorIs(t, err, e2ee.ErrDuplicatePreKeyID)

	// A curve prekey may share an id with a KEM one: separate spaces.
	keys = g.keys(1, 1)
	keys.OneTimePreKeys[0].ID = keys.KemOneTimePreKeys[0].ID
	_, created, err = s.RegisterDevice(e.ctx, newIdempotencyKey(), g.device(userID, account), keys)
	require.NoError(t, err)
	require.True(t, created)

	devices, err := s.GetDevices(e.ctx, userID)
	require.NoError(t, err)
	require.Len(t, devices, 2)
}

func testStore_UnregisterDevice(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	userID := model.MustGenerateUserID()

	require.ErrorIs(t, s.UnregisterDevice(e.ctx, e2ee.DeviceAddress{UserID: userID, DeviceID: 1}), e2ee.ErrDeviceNotFound)

	d, _ := e.register(userID, 1, 1)
	peer, _ := e.register(model.MustGenerateUserID(), 0, 0)
	_, err := s.Deliver(e.ctx, []*e2ee.Envelope{envelope(peer.Address, d.Address, time.Now())})
	require.NoError(t, err)

	require.NoError(t, s.UnregisterDevice(e.ctx, d.Address))
	require.ErrorIs(t, s.UnregisterDevice(e.ctx, d.Address), e2ee.ErrDeviceNotFound)

	_, err = s.GetDevice(e.ctx, d.Address)
	require.ErrorIs(t, err, e2ee.ErrDeviceNotFound)
	_, err = s.GetEnvelopes(e.ctx, d.Address, 0, 0, 0)
	require.ErrorIs(t, err, e2ee.ErrDeviceNotFound)
	_, err = s.GetKeyStatus(e.ctx, d.Address)
	require.ErrorIs(t, err, e2ee.ErrDeviceNotFound)
	_, err = s.TakePreKeyBundle(e.ctx, d.Address)
	require.ErrorIs(t, err, e2ee.ErrDeviceNotFound)
	_, err = s.Deliver(e.ctx, []*e2ee.Envelope{envelope(peer.Address, d.Address, time.Now())})
	require.ErrorIs(t, err, e2ee.ErrDeviceNotFound)

	devices, err := s.GetDevices(e.ctx, userID)
	require.NoError(t, err)
	require.Empty(t, devices)

	// The mailbox went with the device: a new device under the old id starts
	// empty and from sequence 1.
	again, _ := e.register(userID, 0, 0)
	require.Equal(t, d.Address.DeviceID, again.Address.DeviceID)
	envelopes, err := s.GetEnvelopes(e.ctx, again.Address, 0, 0, 0)
	require.NoError(t, err)
	require.Empty(t, envelopes)
	stored, err := s.Deliver(e.ctx, []*e2ee.Envelope{envelope(peer.Address, again.Address, time.Now())})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stored[0].Sequence)
}

func testStore_LastSeen(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	d, _ := e.register(model.MustGenerateUserID(), 0, 0)

	// Recorded at day granularity, whatever time is given.
	day := time.Now().UTC().Truncate(24 * time.Hour)
	require.NoError(t, s.TouchLastSeen(e.ctx, d.Address, day.Add(3*time.Hour)))
	got, err := s.GetDevice(e.ctx, d.Address)
	require.NoError(t, err)
	require.NotNil(t, got.LastSeen)
	require.True(t, got.LastSeen.Equal(day))

	// The same day again, or an older one, moves nothing.
	require.NoError(t, s.TouchLastSeen(e.ctx, d.Address, day))
	require.NoError(t, s.TouchLastSeen(e.ctx, d.Address, day.Add(-48*time.Hour)))
	got, err = s.GetDevice(e.ctx, d.Address)
	require.NoError(t, err)
	require.True(t, got.LastSeen.Equal(day))

	// A later day does.
	require.NoError(t, s.TouchLastSeen(e.ctx, d.Address, day.Add(24*time.Hour)))
	got, err = s.GetDevice(e.ctx, d.Address)
	require.NoError(t, err)
	require.True(t, got.LastSeen.Equal(day.Add(24*time.Hour)))

	require.ErrorIs(t, s.TouchLastSeen(e.ctx, e2ee.DeviceAddress{UserID: d.Address.UserID, DeviceID: 9}, day), e2ee.ErrDeviceNotFound)
}

func testStore_SetKeys(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	d, f := e.register(model.MustGenerateUserID(), 2, 2)

	_, err := s.SetKeys(e.ctx, e2ee.DeviceAddress{UserID: d.Address.UserID, DeviceID: 9}, e2ee.KeyUpdate{})
	require.ErrorIs(t, err, e2ee.ErrDeviceNotFound)

	// An empty update changes nothing.
	counts, err := s.SetKeys(e.ctx, d.Address, e2ee.KeyUpdate{})
	require.NoError(t, err)
	require.Equal(t, e2ee.PreKeyCounts{OneTimePreKeys: 2, KemOneTimePreKeys: 2}, counts.Counts)

	// A supplied pool replaces, never adds.
	counts, err = s.SetKeys(e.ctx, d.Address, e2ee.KeyUpdate{OneTimePreKeys: f.oneTimePreKeys(5)})
	require.NoError(t, err)
	require.Equal(t, e2ee.PreKeyCounts{OneTimePreKeys: 5, KemOneTimePreKeys: 2}, counts.Counts)

	// An empty non-nil pool clears.
	counts, err = s.SetKeys(e.ctx, d.Address, e2ee.KeyUpdate{KemOneTimePreKeys: []*e2ee.KemSignedPreKey{}})
	require.NoError(t, err)
	require.Equal(t, e2ee.PreKeyCounts{OneTimePreKeys: 5, KemOneTimePreKeys: 0}, counts.Counts)

	// The signed prekey and last-resort key are replaced, reported and
	// served.
	signed := f.signedPreKey()
	lastResort := f.kemPreKey()
	keyStatus, err := s.SetKeys(e.ctx, d.Address, e2ee.KeyUpdate{SignedPreKey: signed, KemLastResortPreKey: lastResort})
	require.NoError(t, err)
	require.Equal(t, signed, keyStatus.SignedPreKey)
	require.Equal(t, lastResort, keyStatus.KemLastResortPreKey)
	require.Equal(t, d.Address, keyStatus.Device.Address)
	keyStatus, err = s.GetKeyStatus(e.ctx, d.Address)
	require.NoError(t, err)
	require.Equal(t, signed, keyStatus.SignedPreKey)
	require.Equal(t, lastResort, keyStatus.KemLastResortPreKey)
	bundle, err := s.TakePreKeyBundle(e.ctx, d.Address)
	require.NoError(t, err)
	require.Equal(t, signed, bundle.SignedPreKey)
	require.Equal(t, lastResort, bundle.KemPreKey)

	// A pool key colliding with the stored last-resort id applies nothing.
	pool := f.kemPreKeys(2)
	pool[0].ID = lastResort.ID
	_, err = s.SetKeys(e.ctx, d.Address, e2ee.KeyUpdate{KemOneTimePreKeys: pool, OneTimePreKeys: f.oneTimePreKeys(1)})
	require.ErrorIs(t, err, e2ee.ErrDuplicatePreKeyID)
	counts, err = s.GetKeyStatus(e.ctx, d.Address)
	require.NoError(t, err)
	require.Equal(t, e2ee.PreKeyCounts{OneTimePreKeys: 4, KemOneTimePreKeys: 0}, counts.Counts)

	// Two one-time curve prekeys sharing an id, likewise.
	curve := f.oneTimePreKeys(3)
	curve[2].ID = curve[0].ID
	_, err = s.SetKeys(e.ctx, d.Address, e2ee.KeyUpdate{OneTimePreKeys: curve})
	require.ErrorIs(t, err, e2ee.ErrDuplicatePreKeyID)
	counts, err = s.GetKeyStatus(e.ctx, d.Address)
	require.NoError(t, err)
	require.Equal(t, e2ee.PreKeyCounts{OneTimePreKeys: 4, KemOneTimePreKeys: 0}, counts.Counts)

	// A new last-resort key colliding with the stored pool, likewise.
	pool = f.kemPreKeys(2)
	_, err = s.SetKeys(e.ctx, d.Address, e2ee.KeyUpdate{KemOneTimePreKeys: pool})
	require.NoError(t, err)
	collides := f.kemPreKey()
	collides.ID = pool[1].ID
	_, err = s.SetKeys(e.ctx, d.Address, e2ee.KeyUpdate{KemLastResortPreKey: collides})
	require.ErrorIs(t, err, e2ee.ErrDuplicatePreKeyID)

	// Replacing both at once is checked against the result, not the past.
	fresh := f.kemPreKey()
	fresh.ID = pool[1].ID
	_, err = s.SetKeys(e.ctx, d.Address, e2ee.KeyUpdate{KemLastResortPreKey: fresh, KemOneTimePreKeys: f.kemPreKeys(1)})
	require.NoError(t, err)
}

func testStore_TakePreKeyBundle(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	d, f := e.register(model.MustGenerateUserID(), 2, 1)
	require.NoError(t, s.TouchLastSeen(e.ctx, d.Address, time.Now().UTC()))

	keys, err := s.SetKeys(e.ctx, d.Address, e2ee.KeyUpdate{})
	require.NoError(t, err)
	require.Equal(t, e2ee.PreKeyCounts{OneTimePreKeys: 2, KemOneTimePreKeys: 1}, keys.Counts)

	// First take: a one-time key of each kind.
	first, err := s.TakePreKeyBundle(e.ctx, d.Address)
	require.NoError(t, err)
	require.Equal(t, d.Address, first.Device.Address)
	require.Equal(t, f.identityKey(), first.Device.IdentityKey)
	require.NotNil(t, first.OneTimePreKey)
	require.NotNil(t, first.KemPreKey)
	counts, err := s.GetKeyStatus(e.ctx, d.Address)
	require.NoError(t, err)
	require.Equal(t, e2ee.PreKeyCounts{OneTimePreKeys: 1, KemOneTimePreKeys: 0}, counts.Counts)

	// Second: the KEM pool is empty, so the last-resort key; the next
	// curve one-time key in upload order (the fixture mints ascending ids).
	second, err := s.TakePreKeyBundle(e.ctx, d.Address)
	require.NoError(t, err)
	require.NotNil(t, second.OneTimePreKey)
	require.Less(t, first.OneTimePreKey.ID, second.OneTimePreKey.ID)
	require.NotEqual(t, first.KemPreKey.ID, second.KemPreKey.ID)
	require.Equal(t, first.SignedPreKey, second.SignedPreKey)

	// Third: nothing one-time left; the last-resort key again, no curve key.
	third, err := s.TakePreKeyBundle(e.ctx, d.Address)
	require.NoError(t, err)
	require.Nil(t, third.OneTimePreKey)
	require.Equal(t, second.KemPreKey, third.KemPreKey)
	counts, err = s.GetKeyStatus(e.ctx, d.Address)
	require.NoError(t, err)
	require.Equal(t, e2ee.PreKeyCounts{}, counts.Counts)
}

func testStore_Deliver(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	alice, _ := e.register(model.MustGenerateUserID(), 0, 0)
	bob1, _ := e.register(model.MustGenerateUserID(), 0, 0)
	bob2, _ := e.register(bob1.Address.UserID, 0, 0)

	ts := time.Now().UTC().Truncate(time.Millisecond)
	in := []*e2ee.Envelope{
		envelope(alice.Address, bob1.Address, ts),
		envelope(alice.Address, bob2.Address, ts),
	}
	stored, err := s.Deliver(e.ctx, in)
	require.NoError(t, err)
	require.Len(t, stored, 2)
	for i := range in {
		require.Len(t, stored[i].ID, e2ee.EnvelopeIDSize)
		seq, ok := e2ee.EnvelopeSequence(stored[i].ID)
		require.True(t, ok)
		require.Equal(t, uint64(1), seq)
		require.Equal(t, uint64(1), stored[i].Sequence)
		require.Equal(t, in[i].Recipient, stored[i].Recipient)
	}

	// Sequences are per mailbox and increase with each delivery.
	stored, err = s.Deliver(e.ctx, []*e2ee.Envelope{envelope(alice.Address, bob1.Address, ts.Add(time.Millisecond))})
	require.NoError(t, err)
	require.Equal(t, uint64(2), stored[0].Sequence)

	// A retry of the first send is stored again, a copy per recipient at
	// the next position: nothing is deduplicated.
	retry := []*e2ee.Envelope{
		envelope(alice.Address, bob1.Address, ts),
		envelope(alice.Address, bob2.Address, ts.Add(5*time.Millisecond)),
	}
	stored, err = s.Deliver(e.ctx, retry)
	require.NoError(t, err)
	require.Equal(t, uint64(3), stored[0].Sequence)
	require.Equal(t, uint64(2), stored[1].Sequence)

	envelopes, err := s.GetEnvelopes(e.ctx, bob1.Address, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, envelopes, 3)

	// A receipt is an envelope like any other.
	receipt := &e2ee.Envelope{
		Recipient: alice.Address,
		ChatID:    in[0].ChatID,
		Type:      e2eepb.Envelope_SERVER_DELIVERY_RECEIPT,
		ServerTS:  time.Now().UTC(),
		DeliveryReceipt: &e2ee.DeliveryReceipt{
			Acknowledgements: []e2ee.Acknowledgement{{Recipient: bob1.Address, ClientTS: []time.Time{ts}}},
		},
	}
	for i := range 2 {
		stored, err = s.Deliver(e.ctx, []*e2ee.Envelope{receipt.Clone()})
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), stored[0].Sequence)
	}

	// All or nothing: an unknown recipient in the batch stores none of it.
	_, err = s.Deliver(e.ctx, []*e2ee.Envelope{
		envelope(alice.Address, bob1.Address, ts.Add(time.Second)),
		envelope(alice.Address, e2ee.DeviceAddress{UserID: bob1.Address.UserID, DeviceID: 9}, ts.Add(time.Second)),
	})
	require.ErrorIs(t, err, e2ee.ErrDeviceNotFound)
	envelopes, err = s.GetEnvelopes(e.ctx, bob1.Address, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, envelopes, 3)
}

func testStore_GetEnvelopes(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	alice, _ := e.register(model.MustGenerateUserID(), 0, 0)
	bob, _ := e.register(model.MustGenerateUserID(), 0, 0)

	_, err := s.GetEnvelopes(e.ctx, e2ee.DeviceAddress{UserID: bob.Address.UserID, DeviceID: 9}, 0, 0, 0)
	require.ErrorIs(t, err, e2ee.ErrDeviceNotFound)

	envelopes, err := s.GetEnvelopes(e.ctx, bob.Address, 0, 0, 0)
	require.NoError(t, err)
	require.Empty(t, envelopes)

	base := time.Now().UTC().Truncate(time.Millisecond)
	var sent []*e2ee.Envelope
	for i := range 5 {
		env := envelope(alice.Address, bob.Address, base.Add(time.Duration(i)*time.Millisecond))
		// As the handler stamps it: without the app install.
		env.SourceDevice = alice.Clone()
		env.SourceDevice.AppInstall = ""
		sent = append(sent, env)
	}
	stored, err := s.Deliver(e.ctx, sent)
	require.NoError(t, err)

	// Everything, in order, exactly as stored.
	envelopes, err = s.GetEnvelopes(e.ctx, bob.Address, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, envelopes, 5)
	for i, env := range envelopes {
		require.Equal(t, uint64(i+1), env.Sequence)
		require.Equal(t, stored[i].ID, env.ID)
		require.Equal(t, stored[i], env)
		require.Equal(t, sent[i].Content, env.Content)
		require.True(t, sent[i].ClientTS.Equal(*env.ClientTS))
		require.Equal(t, sent[i].SourceDevice, env.SourceDevice)
		require.True(t, env.WantsDeliveryReceipt)
	}

	// After a cursor, bounded.
	envelopes, err = s.GetEnvelopes(e.ctx, bob.Address, 2, 2, 0)
	require.NoError(t, err)
	require.Len(t, envelopes, 2)
	require.Equal(t, uint64(3), envelopes[0].Sequence)
	require.Equal(t, uint64(4), envelopes[1].Sequence)

	envelopes, err = s.GetEnvelopes(e.ctx, bob.Address, 5, 0, 0)
	require.NoError(t, err)
	require.Empty(t, envelopes)

	// By bytes: each envelope carries 64 bytes of content. A budget of 150
	// stops after the third, the one that crossed it, whatever the limit;
	// a budget of exactly 128 fits two and stops at the third; a budget no
	// envelope fits still serves one.
	envelopes, err = s.GetEnvelopes(e.ctx, bob.Address, 0, 0, 150)
	require.NoError(t, err)
	require.Len(t, envelopes, 3)
	require.Equal(t, uint64(3), envelopes[2].Sequence)

	envelopes, err = s.GetEnvelopes(e.ctx, bob.Address, 0, 0, 128)
	require.NoError(t, err)
	require.Len(t, envelopes, 3)

	envelopes, err = s.GetEnvelopes(e.ctx, bob.Address, 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, envelopes, 1)

	// The limit still binds below the budget.
	envelopes, err = s.GetEnvelopes(e.ctx, bob.Address, 0, 2, 150)
	require.NoError(t, err)
	require.Len(t, envelopes, 2)
}

func testStore_AckEnvelopes(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	alice, _ := e.register(model.MustGenerateUserID(), 0, 0)
	bob, _ := e.register(model.MustGenerateUserID(), 0, 0)

	_, err := s.AckEnvelopes(e.ctx, e2ee.DeviceAddress{UserID: bob.Address.UserID, DeviceID: 9}, nil)
	require.ErrorIs(t, err, e2ee.ErrDeviceNotFound)

	base := time.Now().UTC().Truncate(time.Millisecond)
	var sent []*e2ee.Envelope
	for i := range 4 {
		sent = append(sent, envelope(alice.Address, bob.Address, base.Add(time.Duration(i)*time.Millisecond)))
	}
	sent, err = s.Deliver(e.ctx, sent)
	require.NoError(t, err)

	// Acknowledge two, one of them twice, plus an id that was never there
	// and one that names a real position under the wrong random tail.
	wrongTail := e2ee.NewEnvelopeID(sent[0].Sequence)
	acked, err := s.AckEnvelopes(e.ctx, bob.Address, [][]byte{sent[1].ID, sent[3].ID, sent[1].ID, randBytes(e2ee.EnvelopeIDSize), wrongTail})
	require.NoError(t, err)
	require.Len(t, acked, 2)
	require.Equal(t, uint64(2), acked[0].Sequence)
	require.Equal(t, uint64(4), acked[1].Sequence)
	require.Equal(t, sent[1].ID, acked[0].ID)

	remaining, err := s.GetEnvelopes(e.ctx, bob.Address, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, remaining, 2)
	require.Equal(t, uint64(1), remaining[0].Sequence)
	require.Equal(t, uint64(3), remaining[1].Sequence)

	// Idempotent: nothing left to remove.
	acked, err = s.AckEnvelopes(e.ctx, bob.Address, [][]byte{sent[1].ID})
	require.NoError(t, err)
	require.Empty(t, acked)

	// Sequences keep climbing past what was acknowledged; nothing is reused.
	stored, err := s.Deliver(e.ctx, []*e2ee.Envelope{envelope(alice.Address, bob.Address, base.Add(time.Second))})
	require.NoError(t, err)
	require.Equal(t, uint64(5), stored[0].Sequence)
}

// Concurrent takes never hand the same one-time prekey to two callers, and
// the pools drain exactly.
func testStore_TakePreKeyBundle_Concurrent(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	const otk, kem, takers = 12, 8, 20
	d, _ := e.register(model.MustGenerateUserID(), otk, kem)

	var mu sync.Mutex
	var wg sync.WaitGroup
	seenOtk := make(map[uint32]int)
	seenKem := make(map[uint32]int)
	for range takers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bundle, err := s.TakePreKeyBundle(e.ctx, d.Address)
			require.NoError(t, err)
			mu.Lock()
			defer mu.Unlock()
			if bundle.OneTimePreKey != nil {
				seenOtk[bundle.OneTimePreKey.ID]++
			}
			seenKem[bundle.KemPreKey.ID]++
		}()
	}
	wg.Wait()

	require.Len(t, seenOtk, otk, "every curve one-time key served exactly once")
	for id, n := range seenOtk {
		require.Equal(t, 1, n, "curve prekey %d", id)
	}
	// Every KEM one-time key once, and the last-resort key for the rest.
	lastResortTakes := 0
	for _, n := range seenKem {
		if n > 1 {
			lastResortTakes = n
		}
	}
	require.Len(t, seenKem, kem+1)
	require.Equal(t, takers-kem, lastResortTakes)

	counts, err := s.GetKeyStatus(e.ctx, d.Address)
	require.NoError(t, err)
	require.Equal(t, e2ee.PreKeyCounts{}, counts.Counts)
}

// Takes that straddle a pool replacement each serve a key of the old pool
// or the new one, never one twice, and the new pool is charged exactly for
// what was served from it.
func testStore_TakePreKeyBundle_DuringReplacement(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	const takers = 12
	d, f := e.register(model.MustGenerateUserID(), 6, 6)
	replacementOtk := f.oneTimePreKeys(8)
	replacementKem := f.kemPreKeys(8)
	newOtk := make(map[uint32]struct{}, len(replacementOtk))
	for _, k := range replacementOtk {
		newOtk[k.ID] = struct{}{}
	}
	newKem := make(map[uint32]struct{}, len(replacementKem))
	for _, k := range replacementKem {
		newKem[k.ID] = struct{}{}
	}

	var wg sync.WaitGroup
	errs := make(chan error, takers+1)
	bundles := make(chan *e2ee.PreKeyBundle, takers)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := s.SetKeys(e.ctx, d.Address, e2ee.KeyUpdate{OneTimePreKeys: replacementOtk, KemOneTimePreKeys: replacementKem})
		errs <- err
	}()
	for range takers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bundle, err := s.TakePreKeyBundle(e.ctx, d.Address)
			errs <- err
			if err == nil {
				bundles <- bundle
			}
		}()
	}
	wg.Wait()
	close(errs)
	close(bundles)
	for err := range errs {
		require.NoError(t, err)
	}

	seenOtk := make(map[uint32]int)
	seenKem := make(map[uint32]int)
	servedNewOtk, servedNewKem := 0, 0
	for b := range bundles {
		if b.OneTimePreKey != nil {
			seenOtk[b.OneTimePreKey.ID]++
			if _, ok := newOtk[b.OneTimePreKey.ID]; ok {
				servedNewOtk++
			}
		}
		if _, ok := newKem[b.KemPreKey.ID]; ok {
			seenKem[b.KemPreKey.ID]++
			servedNewKem++
		} else if b.KemPreKey.ID != f.lastResortID() {
			seenKem[b.KemPreKey.ID]++
		}
	}
	for id, n := range seenOtk {
		require.Equal(t, 1, n, "curve prekey %d served twice", id)
	}
	for id, n := range seenKem {
		require.Equal(t, 1, n, "KEM prekey %d served twice", id)
	}

	counts, err := s.GetKeyStatus(e.ctx, d.Address)
	require.NoError(t, err)
	require.Equal(t, len(replacementOtk)-servedNewOtk, counts.Counts.OneTimePreKeys)
	require.Equal(t, len(replacementKem)-servedNewKem, counts.Counts.KemOneTimePreKeys)
}

// Concurrent deliveries to one mailbox get distinct, increasing sequences,
// and a concurrent retry of the same send stores a copy each time: nothing
// is deduplicated.
func testStore_Deliver_Concurrent(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	alice, _ := e.register(model.MustGenerateUserID(), 0, 0)
	bob, _ := e.register(model.MustGenerateUserID(), 0, 0)

	const senders = 16
	base := time.Now().UTC().Truncate(time.Millisecond)
	var wg sync.WaitGroup
	for i := range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Deliver(e.ctx, []*e2ee.Envelope{envelope(alice.Address, bob.Address, base.Add(time.Duration(i)*time.Millisecond))})
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	envelopes, err := s.GetEnvelopes(e.ctx, bob.Address, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, envelopes, senders)
	for i, env := range envelopes {
		require.Equal(t, uint64(i+1), env.Sequence)
	}

	// The same send retried from several goroutines at once: every retry
	// lands, each at its own position after the first batch.
	retryTS := base.Add(time.Second)
	for range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := s.Deliver(e.ctx, []*e2ee.Envelope{envelope(alice.Address, bob.Address, retryTS)})
			require.NoError(t, err)
			require.NotNil(t, out[0])
			require.Greater(t, out[0].Sequence, uint64(senders))
		}()
	}
	wg.Wait()
	envelopes, err = s.GetEnvelopes(e.ctx, bob.Address, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, envelopes, 2*senders)
	for i := 1; i < len(envelopes); i++ {
		require.Greater(t, envelopes[i].Sequence, envelopes[i-1].Sequence)
	}
}

// A call larger than one write batch (a long drain's receipts, a group
// send) lands whole and in order, and a retry of it lands whole again.
func testStore_Deliver_Large(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	alice, _ := e.register(model.MustGenerateUserID(), 0, 0)
	bob, _ := e.register(model.MustGenerateUserID(), 0, 0)
	carol, _ := e.register(model.MustGenerateUserID(), 0, 0)

	const n = 150
	base := time.Now().UTC().Truncate(time.Millisecond)
	var in []*e2ee.Envelope
	for i := range n {
		// Two mailboxes interleaved, so the split across mailboxes and
		// within one are both exercised.
		recipient := bob.Address
		if i%3 == 0 {
			recipient = carol.Address
		}
		in = append(in, envelope(alice.Address, recipient, base.Add(time.Duration(i)*time.Millisecond)))
	}
	stored, err := s.Deliver(e.ctx, in)
	require.NoError(t, err)
	require.Len(t, stored, n)
	for _, env := range stored {
		require.NotNil(t, env)
	}

	for _, d := range []*e2ee.Device{bob, carol} {
		envelopes, err := s.GetEnvelopes(e.ctx, d.Address, 0, 0, 0)
		require.NoError(t, err)
		var want []*e2ee.Envelope
		for _, env := range stored {
			if env.Recipient.Key() == d.Address.Key() {
				want = append(want, env)
			}
		}
		require.Len(t, envelopes, len(want))
		for i, env := range envelopes {
			require.Equal(t, uint64(i+1), env.Sequence)
			require.Equal(t, want[i].ID, env.ID)
			require.Equal(t, want[i].Content, env.Content)
		}
	}

	// The whole call retried: a second copy of everything, after the first.
	var retry []*e2ee.Envelope
	for i := range n {
		recipient := bob.Address
		if i%3 == 0 {
			recipient = carol.Address
		}
		retry = append(retry, envelope(alice.Address, recipient, base.Add(time.Duration(i)*time.Millisecond)))
	}
	stored, err = s.Deliver(e.ctx, retry)
	require.NoError(t, err)
	for _, env := range stored {
		require.NotNil(t, env)
	}
	envelopes, err := s.GetEnvelopes(e.ctx, bob.Address, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, envelopes, 2*(n-n/3))

	// A large batch of receipts to one mailbox lands whole too.
	var receipts []*e2ee.Envelope
	for range 120 {
		receipts = append(receipts, &e2ee.Envelope{
			Recipient: alice.Address,
			ChatID:    in[0].ChatID,
			Type:      e2eepb.Envelope_SERVER_DELIVERY_RECEIPT,
			ServerTS:  time.Now().UTC(),
			DeliveryReceipt: &e2ee.DeliveryReceipt{
				Acknowledgements: []e2ee.Acknowledgement{{Recipient: bob.Address, ClientTS: []time.Time{base}}},
			},
		})
	}
	stored, err = s.Deliver(e.ctx, receipts)
	require.NoError(t, err)
	require.Len(t, stored, 120)
	envelopes, err = s.GetEnvelopes(e.ctx, alice.Address, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, envelopes, 120)
	require.Equal(t, uint64(120), envelopes[119].Sequence)
}

// An envelope past its ExpiresAt is never returned, and its position is a
// gap the mailbox keeps: the next envelope takes the position after it.
// One inside its window is returned, flagged as the ephemeral it is.
func testStore_Deliver_Ephemeral(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	alice, _ := e.register(model.MustGenerateUserID(), 0, 0)
	bob, _ := e.register(model.MustGenerateUserID(), 0, 0)
	base := time.Now().UTC().Truncate(time.Millisecond)

	live := envelope(alice.Address, bob.Address, base)
	lapsed := envelope(alice.Address, bob.Address, base.Add(time.Millisecond))
	lapsed.Ephemeral = true
	lapsed.ExpiresAt = time.Now().UTC().Add(-time.Second).Truncate(time.Second)
	after := envelope(alice.Address, bob.Address, base.Add(2*time.Millisecond))
	stored, err := s.Deliver(e.ctx, []*e2ee.Envelope{live, lapsed, after})
	require.NoError(t, err)
	require.Equal(t, uint64(2), stored[1].Sequence)

	envelopes, err := s.GetEnvelopes(e.ctx, bob.Address, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, envelopes, 2)
	require.Equal(t, uint64(1), envelopes[0].Sequence)
	require.Equal(t, uint64(3), envelopes[1].Sequence)
	require.False(t, envelopes[0].Ephemeral)

	soon := envelope(alice.Address, bob.Address, base.Add(3*time.Millisecond))
	soon.Ephemeral = true
	soon.ExpiresAt = time.Now().UTC().Add(time.Minute).Truncate(time.Second)
	_, err = s.Deliver(e.ctx, []*e2ee.Envelope{soon})
	require.NoError(t, err)
	envelopes, err = s.GetEnvelopes(e.ctx, bob.Address, 3, 0, 0)
	require.NoError(t, err)
	require.Len(t, envelopes, 1)
	require.True(t, envelopes[0].Ephemeral)
	require.Equal(t, uint64(4), envelopes[0].Sequence)
	require.Equal(t, soon.ExpiresAt, envelopes[0].ExpiresAt)
}

// Two replacements at once: whichever wins, the counts describe a pool
// that is really there, and the loser's staged keys are never served.
func testStore_SetKeys_Concurrent(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	d, f := e.register(model.MustGenerateUserID(), 2, 2)

	sizes := []int{3, 5, 7, 4}
	var wg sync.WaitGroup
	for _, n := range sizes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.SetKeys(e.ctx, d.Address, e2ee.KeyUpdate{
				OneTimePreKeys:    f.oneTimePreKeys(n),
				KemOneTimePreKeys: f.kemPreKeys(n),
			})
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	counts, err := s.GetKeyStatus(e.ctx, d.Address)
	require.NoError(t, err)
	require.Contains(t, sizes, counts.Counts.OneTimePreKeys)
	require.Equal(t, counts.Counts.OneTimePreKeys, counts.Counts.KemOneTimePreKeys)

	// Drain: exactly the counted keys are served, each once.
	otk := map[uint32]struct{}{}
	kem := map[uint32]struct{}{}
	for range counts.Counts.OneTimePreKeys + 2 {
		bundle, err := s.TakePreKeyBundle(e.ctx, d.Address)
		require.NoError(t, err)
		if bundle.OneTimePreKey != nil {
			_, dup := otk[bundle.OneTimePreKey.ID]
			require.False(t, dup)
			otk[bundle.OneTimePreKey.ID] = struct{}{}
		}
		kem[bundle.KemPreKey.ID] = struct{}{}
	}
	require.Len(t, otk, counts.Counts.OneTimePreKeys)
	require.Len(t, kem, counts.Counts.KemOneTimePreKeys+1, "the pool plus the last-resort key")
	final, err := s.GetKeyStatus(e.ctx, d.Address)
	require.NoError(t, err)
	require.Equal(t, e2ee.PreKeyCounts{}, final.Counts)
}

// A bucket starts full, refuses a take it cannot cover with the time the
// missing tokens take to refill, and refills at its rate; buckets are
// independent per kind and key.
// Limits are sliding windows of Size permits over Size×Refill, aligned
// to the epoch, judged at the time the caller names (see
// e2ee.BucketConfig). Times here start at a window boundary so the
// arithmetic is exact.
func testStore_TakeTokens(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	cfg := e2ee.BucketConfig{Size: 2, Refill: time.Minute} // a 2-minute window
	window := cfg.Window()
	require.Equal(t, 2*time.Minute, window)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	require.Equal(t, int64(0), t0.UnixNano()%int64(window), "t0 is a window boundary")

	take := func(kind, key string, cost int64, at time.Time) time.Duration {
		retry, err := s.TakeTokens(e.ctx, kind, key, cost, cfg, at)
		require.NoError(t, err)
		return retry
	}

	require.Zero(t, take("test", "a", 1, t0))
	require.Zero(t, take("test", "a", 1, t0))

	// Full. A quarter through the window, the wait is the rest of it plus
	// the half of the next it takes for this window's two to decay to one.
	require.InDelta(t, 150*time.Second, take("test", "a", 1, t0.Add(30*time.Second)), float64(time.Millisecond))

	// Another key, and another kind under the same key, are their own.
	require.Zero(t, take("test", "b", 2, t0))
	require.Zero(t, take("other", "a", 2, t0))

	// A cost above the size is refused outright, for a whole window, and
	// consumes nothing.
	require.Equal(t, window, take("test", "c", 3, t0))
	require.Zero(t, take("test", "c", 2, t0))

	// The next window, halfway through: the previous window's two count
	// for one, so one fits and the second does not. Its wait is the rest
	// of the window, when the previous one's share is gone. The refused
	// take earlier consumed nothing, or nothing would fit here.
	t1 := t0.Add(window + time.Minute)
	require.Zero(t, take("test", "a", 1, t1))
	require.InDelta(t, time.Minute, take("test", "a", 1, t1), float64(time.Millisecond))

	// Two windows on, nothing is in view.
	require.Zero(t, take("test", "a", 2, t0.Add(3*window)))
}

// A herd on a fresh key: exactly Size fit, whoever rolls the window in.
func testStore_TakeTokens_Concurrent(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	cfg := e2ee.BucketConfig{Size: 10, Refill: time.Minute}
	now := time.Now()

	const takers = 25
	results := make([]time.Duration, takers)
	var wg sync.WaitGroup
	for i := range takers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			retry, err := s.TakeTokens(e.ctx, "herd", "k", 1, cfg, now)
			require.NoError(t, err)
			results[i] = retry
		}()
	}
	wg.Wait()

	var fit int
	for _, retry := range results {
		if retry == 0 {
			fit++
		}
	}
	require.Equal(t, int(cfg.Size), fit)
}

func testStore_AddCapabilities(t *testing.T, s e2ee.Store) {
	e := newStoreEnv(t, s)
	userID := model.MustGenerateUserID()
	f := newDeviceFixture()
	bare := f.device(userID, model.MustGenerateKeyPair())
	bare.Capabilities = nil
	d, _, err := s.RegisterDevice(e.ctx, newIdempotencyKey(), bare, f.keys(0, 0))
	require.NoError(t, err)
	require.Empty(t, d.Capabilities)

	// Added in place, sorted, and read back the same.
	got, err := s.AddCapabilities(e.ctx, d.Address, []e2eepb.Device_Capability{e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET})
	require.NoError(t, err)
	require.Equal(t, []e2eepb.Device_Capability{e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET}, got.Capabilities)
	got, err = s.GetDevice(e.ctx, d.Address)
	require.NoError(t, err)
	require.Equal(t, []e2eepb.Device_Capability{e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET}, got.Capabilities)

	// Adding what is declared, or nothing, changes nothing; nothing is ever
	// removed.
	got, err = s.AddCapabilities(e.ctx, d.Address, []e2eepb.Device_Capability{e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET, e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET})
	require.NoError(t, err)
	require.Equal(t, []e2eepb.Device_Capability{e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET}, got.Capabilities)
	got, err = s.AddCapabilities(e.ctx, d.Address, nil)
	require.NoError(t, err)
	require.Equal(t, []e2eepb.Device_Capability{e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET}, got.Capabilities)

	// The rest of the record is untouched by the write.
	require.Equal(t, d.IdentityKey, got.IdentityKey)
	require.Equal(t, d.RegistrationID, got.RegistrationID)
	require.Equal(t, d.AppInstall, got.AppInstall)

	_, err = s.AddCapabilities(e.ctx, e2ee.DeviceAddress{UserID: userID, DeviceID: 9}, []e2eepb.Device_Capability{e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET})
	require.ErrorIs(t, err, e2ee.ErrDeviceNotFound)
}

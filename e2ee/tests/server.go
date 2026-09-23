package tests

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	e2eepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/e2ee/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/e2ee"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/testutil"
)

// RunServerTests runs the shared KeyDistribution and Mailbox server test
// suite against s, using accounts to authorize callers and to decide which
// users exist. teardown is called between tests to reset the e2ee store.
func RunServerTests(t *testing.T, accounts account.Store, s e2ee.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, accounts account.Store, s e2ee.Store){
		testServer_RegisterDevice_OK,
		testServer_RegisterDevice_Unregistered,
		testServer_RegisterDevice_Idempotent,
		testServer_RegisterDevice_InvalidSignature,
		testServer_RegisterDevice_DuplicatePreKeyID,
		testServer_RegisterDevice_TooManyDevices,
		testServer_RegisterDevice_DuplicateIdentityKey,
		testServer_RegisterDevice_Capabilities,
		testServer_SetCapabilities,
		testServer_UnregisterDevice,
		testServer_GetDevices,
		testServer_SetKeys,
		testServer_GetPreKeyCounts,
		testServer_GetPreKeyBundles,
		testServer_GetPreKeyBundles_OneDevice,
		testServer_GetPreKeyBundles_RateLimited,
		testServer_SendEnvelopes_OK,
		testServer_SendEnvelopes_Siblings,
		testServer_SendEnvelopes_MismatchedDevices,
		testServer_SendEnvelopes_InvalidRecipient,
		testServer_SendEnvelopes_InvalidTimestamp,
		testServer_SendEnvelopes_Refusals,
		testServer_SendEnvelopes_Online,
		testServer_SendEnvelopes_RetryStoresAgain,
		testServer_SendEnvelopes_RateLimited,
		testServer_GetEnvelopes_Paging,
		testServer_GetEnvelopes_ByteBudget,
		testServer_LastSeen,
		testServer_AckEnvelopes_DeliveryReceipts,
		testServer_AckEnvelopes_NoReceipt,
	} {
		tf(t, accounts, s)
		teardown()
	}
}

type serverEnv struct {
	t        *testing.T
	ctx      context.Context
	keys     e2eepb.KeyDistributionClient
	mailbox  e2eepb.MailboxClient
	accounts account.Store
	store    e2ee.Store
}

// user is a registered account acting through the RPCs.
type user struct {
	id   *commonpb.UserId
	keys model.KeyPair
}

// device is one of a user's registered devices, with the client-side
// fixture that holds its keys.
type device struct {
	user    *user
	id      uint32
	regID   uint32
	fixture *deviceFixture
	proto   *e2eepb.Device
}

func (d *device) address() e2ee.DeviceAddress {
	return e2ee.DeviceAddress{UserID: d.user.id, DeviceID: d.id}
}

func (d *device) addressProto() *e2eepb.DeviceAddress {
	return d.address().ToProto()
}

func newServerEnv(t *testing.T, accounts account.Store, s e2ee.Store) *serverEnv {
	return newServerEnvWith(t, accounts, s, nil, nil)
}

// newServerEnvWith is newServerEnv with options on the two servers.
func newServerEnvWith(t *testing.T, accounts account.Store, s e2ee.Store, keyOpts []e2ee.KeyDistributionOption, mailboxOpts []e2ee.MailboxOption) *serverEnv {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	keyServer := e2ee.NewKeyDistributionServer(log, authz, accounts, s, keyOpts...)
	mailboxServer := e2ee.NewMailboxServer(log, authz, accounts, s, mailboxOpts...)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(srv *grpc.Server) {
		e2eepb.RegisterKeyDistributionServer(srv, keyServer)
		e2eepb.RegisterMailboxServer(srv, mailboxServer)
	}))

	return &serverEnv{
		t:        t,
		ctx:      ctx,
		keys:     e2eepb.NewKeyDistributionClient(cc),
		mailbox:  e2eepb.NewMailboxClient(cc),
		accounts: accounts,
		store:    s,
	}
}

// newUser creates an account with a bound key.
func (e *serverEnv) newUser() *user {
	u := e.newUnregisteredUser()
	require.NoError(e.t, e.accounts.SetRegistrationFlag(e.ctx, u.id, true))
	return u
}

// newUnregisteredUser is a user with a bound key who has not completed
// registration: authenticated, but not yet allowed to hold devices.
func (e *serverEnv) newUnregisteredUser() *user {
	u := &user{id: model.MustGenerateUserID(), keys: model.MustGenerateKeyPair()}
	_, err := e.accounts.Bind(e.ctx, u.id, u.keys.Proto())
	require.NoError(e.t, err)
	return u
}

// register registers a device for u through the RPC, with pools of the
// given sizes, and requires OK.
func (e *serverEnv) register(u *user, oneTime, kemOneTime int) *device {
	f := newDeviceFixture()
	req := f.registerRequest(u.keys, oneTime, kemOneTime)
	resp := e.registerDevice(u, req)
	require.Equal(e.t, e2eepb.RegisterDeviceResponse_OK, resp.Result)
	require.NotNil(e.t, resp.Device)
	return &device{
		user:    u,
		id:      resp.Device.DeviceId.Value,
		regID:   resp.Device.RegistrationId.Value,
		fixture: f,
		proto:   resp.Device,
	}
}

func (e *serverEnv) registerDevice(u *user, req *e2eepb.RegisterDeviceRequest) *e2eepb.RegisterDeviceResponse {
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.keys.RegisterDevice(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) unregisterDevice(u *user, deviceID uint32) *e2eepb.UnregisterDeviceResponse {
	req := &e2eepb.UnregisterDeviceRequest{DeviceId: &e2eepb.DeviceId{Value: deviceID}}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.keys.UnregisterDevice(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) getDevices(u *user, userIDs ...*commonpb.UserId) (*e2eepb.GetDevicesResponse, error) {
	req := &e2eepb.GetDevicesRequest{UserIds: userIDs}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	return e.keys.GetDevices(e.ctx, req)
}

func (e *serverEnv) setKeys(u *user, req *e2eepb.SetKeysRequest) *e2eepb.SetKeysResponse {
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.keys.SetKeys(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) getPreKeyCounts(u *user, deviceID uint32) *e2eepb.GetPreKeyCountsResponse {
	req := &e2eepb.GetPreKeyCountsRequest{DeviceId: &e2eepb.DeviceId{Value: deviceID}}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.keys.GetPreKeyCounts(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func (e *serverEnv) getPreKeyBundles(caller *device, req *e2eepb.GetPreKeyBundlesRequest) (*e2eepb.GetPreKeyBundlesResponse, error) {
	req.CallerDeviceId = &e2eepb.DeviceId{Value: caller.id}
	require.NoError(e.t, caller.user.keys.Auth(req, &req.Auth))
	return e.keys.GetPreKeyBundles(e.ctx, req)
}

// pairwise addresses one CIPHERTEXT envelope to each of recipients under
// the registration IDs the sender holds for them.
func pairwise(recipients ...*device) *e2eepb.SendEnvelopesRequest_Pairwise {
	p := &e2eepb.SendEnvelopesRequest_PairwiseEnvelopes{}
	for _, r := range recipients {
		p.Envelopes = append(p.Envelopes, &e2eepb.SendEnvelopesRequest_PairwiseEnvelope{
			Recipient:      r.addressProto(),
			RegistrationId: &e2eepb.RegistrationId{Value: r.regID},
			Type:           e2eepb.Envelope_CIPHERTEXT,
			Content:        randBytes(48),
		})
	}
	return &e2eepb.SendEnvelopesRequest_Pairwise{Pairwise: p}
}

// clientTS is a valid client_ts: millisecond precision, near now, and
// distinct across calls within a test.
var lastClientTS time.Time

func clientTS() *timestamppb.Timestamp {
	now := time.Now().UTC().Truncate(time.Millisecond)
	if !now.After(lastClientTS) {
		now = lastClientTS.Add(time.Millisecond)
	}
	lastClientTS = now
	return timestamppb.New(now)
}

// sendRequest is a send from sender in its DM with peer, unauthenticated
// and with a fresh client_ts.
func sendRequest(sender *device, peer *user, payload *e2eepb.SendEnvelopesRequest_Pairwise) *e2eepb.SendEnvelopesRequest {
	return &e2eepb.SendEnvelopesRequest{
		ChatId:   e2ee.DeriveDmChatID(sender.user.id, peer.id),
		DeviceId: &e2eepb.DeviceId{Value: sender.id},
		Payload:  payload,
		ClientTs: clientTS(),
	}
}

func (e *serverEnv) send(sender *device, req *e2eepb.SendEnvelopesRequest) (*e2eepb.SendEnvelopesResponse, error) {
	require.NoError(e.t, sender.user.keys.Auth(req, &req.Auth))
	return e.mailbox.SendEnvelopes(e.ctx, req)
}

func (e *serverEnv) sendOK(sender *device, req *e2eepb.SendEnvelopesRequest) *e2eepb.SendEnvelopesResponse {
	resp, err := e.send(sender, req)
	require.NoError(e.t, err)
	require.Equal(e.t, e2eepb.SendEnvelopesResponse_OK, resp.Result)
	require.NotNil(e.t, resp.ServerTs)
	return resp
}

func (e *serverEnv) getEnvelopes(d *device, after uint64, pageSize int32) *e2eepb.GetEnvelopesResponse {
	req := &e2eepb.GetEnvelopesRequest{
		DeviceId:      &e2eepb.DeviceId{Value: d.id},
		AfterSequence: after,
	}
	if pageSize > 0 {
		req.QueryOptions = &commonpb.QueryOptions{PageSize: pageSize}
	}
	require.NoError(e.t, d.user.keys.Auth(req, &req.Auth))
	resp, err := e.mailbox.GetEnvelopes(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// drain reads the whole mailbox.
func (e *serverEnv) drain(d *device) []*e2eepb.Envelope {
	resp := e.getEnvelopes(d, 0, 0)
	require.Equal(e.t, e2eepb.GetEnvelopesResponse_OK, resp.Result)
	require.False(e.t, resp.HasMore)
	return resp.GetEnvelopes().GetEnvelopes()
}

func (e *serverEnv) ack(d *device, envelopes ...*e2eepb.Envelope) *e2eepb.AckEnvelopesResponse {
	req := &e2eepb.AckEnvelopesRequest{DeviceId: &e2eepb.DeviceId{Value: d.id}}
	for _, env := range envelopes {
		req.EnvelopeIds = append(req.EnvelopeIds, env.Id)
	}
	require.NoError(e.t, d.user.keys.Auth(req, &req.Auth))
	resp, err := e.mailbox.AckEnvelopes(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

func requireCode(t *testing.T, err error, code codes.Code) {
	require.Error(t, err)
	require.Equal(t, code, status.Code(err), err.Error())
}

func testServer_RegisterDevice_OK(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	u := e.newUser()

	f := newDeviceFixture()
	req := f.registerRequest(u.keys, 3, 2)
	resp := e.registerDevice(u, req)
	require.Equal(t, e2eepb.RegisterDeviceResponse_OK, resp.Result)

	d := resp.Device
	require.Equal(t, uint32(1), d.DeviceId.Value)
	require.Equal(t, f.registrationID, d.RegistrationId.Value)
	require.Equal(t, f.identityKey(), d.IdentityKey.Value)
	require.Equal(t, req.IdentityKeySignature.Value, d.IdentityKeySignature.Value)
	require.Equal(t, []byte(u.keys.Public()), d.AccountKey.Value)
	require.NotNil(t, d.RegisteredAt)
	require.Nil(t, d.LastSeen)

	// The chain a peer verifies holds.
	require.True(t, e2ee.VerifyIdentityKeySignature(d.AccountKey.Value, d.IdentityKey.Value, d.IdentityKeySignature.Value))

	counts := e.getPreKeyCounts(u, 1)
	require.Equal(t, e2eepb.GetPreKeyCountsResponse_OK, counts.Result)
	require.Equal(t, uint32(3), counts.Counts.OneTimePrekeys)
	require.Equal(t, uint32(2), counts.Counts.KemOneTimePrekeys)

	// A second device gets the next id; no pools is fine.
	second := e.registerDevice(u, newDeviceFixture().registerRequest(u.keys, 0, 0))
	require.Equal(t, e2eepb.RegisterDeviceResponse_OK, second.Result)
	require.Equal(t, uint32(2), second.Device.DeviceId.Value)
}

func testServer_RegisterDevice_Idempotent(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	u := e.newUser()

	f := newDeviceFixture()
	req := f.registerRequest(u.keys, 1, 1)
	first := e.registerDevice(u, req)
	require.Equal(t, e2eepb.RegisterDeviceResponse_OK, first.Result)

	// Same key, different everything: the first device, and nothing new.
	retry := newDeviceFixture().registerRequest(u.keys, 4, 4)
	retry.IdempotencyKey = req.IdempotencyKey
	again := e.registerDevice(u, retry)
	require.Equal(t, e2eepb.RegisterDeviceResponse_OK, again.Result)
	require.Equal(t, first.Device.DeviceId.Value, again.Device.DeviceId.Value)
	require.Equal(t, first.Device.IdentityKey.Value, again.Device.IdentityKey.Value)

	devices, err := e.getDevices(u)
	require.NoError(t, err)
	require.Len(t, devices.Users, 1)
	require.Len(t, devices.Users[0].Devices, 1)

	// The key is the caller's: another user's retry under it is their own
	// first registration.
	other := e.newUser()
	otherReq := newDeviceFixture().registerRequest(other.keys, 0, 0)
	otherReq.IdempotencyKey = req.IdempotencyKey
	resp := e.registerDevice(other, otherReq)
	require.Equal(t, e2eepb.RegisterDeviceResponse_OK, resp.Result)
	require.Equal(t, uint32(1), resp.Device.DeviceId.Value)
	require.NotEqual(t, first.Device.IdentityKey.Value, resp.Device.IdentityKey.Value)
}

func testServer_RegisterDevice_InvalidSignature(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	u := e.newUser()

	// Identity key certified by a key that is not the account's.
	f := newDeviceFixture()
	req := f.registerRequest(u.keys, 1, 1)
	req.IdentityKeySignature = &commonpb.Signature{Value: f.identitySignature(model.MustGenerateKeyPair())}
	require.Equal(t, e2eepb.RegisterDeviceResponse_INVALID_SIGNATURE, e.registerDevice(u, req).Result)

	// Signed prekey signed by another identity key.
	req = f.registerRequest(u.keys, 1, 1)
	req.SignedPrekey = newDeviceFixture().signedPreKey().ToProto()
	require.Equal(t, e2eepb.RegisterDeviceResponse_INVALID_SIGNATURE, e.registerDevice(u, req).Result)

	// Last-resort KEM prekey, likewise.
	req = f.registerRequest(u.keys, 1, 1)
	req.KemLastResortPrekey = newDeviceFixture().kemPreKey().ToProto()
	require.Equal(t, e2eepb.RegisterDeviceResponse_INVALID_SIGNATURE, e.registerDevice(u, req).Result)

	// One bad key in the one-time KEM pool.
	req = f.registerRequest(u.keys, 1, 2)
	req.KemOneTimePrekeys.Prekeys[1] = newDeviceFixture().kemPreKey().ToProto()
	require.Equal(t, e2eepb.RegisterDeviceResponse_INVALID_SIGNATURE, e.registerDevice(u, req).Result)

	// A signature over different bytes than the key served.
	req = f.registerRequest(u.keys, 1, 1)
	req.SignedPrekey.PublicKey.Value[5] ^= 0xff
	require.Equal(t, e2eepb.RegisterDeviceResponse_INVALID_SIGNATURE, e.registerDevice(u, req).Result)

	// Nothing was registered by any of them.
	devices, err := e.getDevices(u)
	require.NoError(t, err)
	require.Empty(t, devices.Users[0].Devices)
}

func testServer_RegisterDevice_Unregistered(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	u := e.newUnregisteredUser()

	// Refused before anything is checked or stored: a well-formed request
	// from an unregistered account is DENIED, and the account holds no
	// device afterwards.
	f := newDeviceFixture()
	resp := e.registerDevice(u, f.registerRequest(u.keys, 1, 1))
	require.Equal(t, e2eepb.RegisterDeviceResponse_DENIED, resp.Result)
	require.Nil(t, resp.Device)
	devices, err := s.GetDevices(e.ctx, u.id)
	require.NoError(t, err)
	require.Empty(t, devices)

	// Registration lifts it, and the same request is then OK.
	require.NoError(t, e.accounts.SetRegistrationFlag(e.ctx, u.id, true))
	require.Equal(t, e2eepb.RegisterDeviceResponse_OK, e.registerDevice(u, f.registerRequest(u.keys, 1, 1)).Result)

	// Losing it afterwards does not strand the device: unregistering is
	// not gated.
	require.NoError(t, e.accounts.SetRegistrationFlag(e.ctx, u.id, false))
	require.Equal(t, e2eepb.UnregisterDeviceResponse_OK, e.unregisterDevice(u, 1).Result)
}

func testServer_RegisterDevice_DuplicatePreKeyID(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	u := e.newUser()

	f := newDeviceFixture()
	req := f.registerRequest(u.keys, 0, 2)
	req.KemOneTimePrekeys.Prekeys[0].Id.Value = req.KemLastResortPrekey.Id.Value
	require.Equal(t, e2eepb.RegisterDeviceResponse_DUPLICATE_PREKEY_ID, e.registerDevice(u, req).Result)

	req = f.registerRequest(u.keys, 0, 2)
	req.KemOneTimePrekeys.Prekeys[1].Id.Value = req.KemOneTimePrekeys.Prekeys[0].Id.Value
	require.Equal(t, e2eepb.RegisterDeviceResponse_DUPLICATE_PREKEY_ID, e.registerDevice(u, req).Result)

	// Two one-time curve prekeys sharing an id.
	req = f.registerRequest(u.keys, 2, 0)
	req.OneTimePrekeys.Prekeys[1].Id.Value = req.OneTimePrekeys.Prekeys[0].Id.Value
	require.Equal(t, e2eepb.RegisterDeviceResponse_DUPLICATE_PREKEY_ID, e.registerDevice(u, req).Result)

	// Curve and KEM ids are separate spaces.
	req = f.registerRequest(u.keys, 1, 1)
	req.OneTimePrekeys.Prekeys[0].Id.Value = req.KemOneTimePrekeys.Prekeys[0].Id.Value
	require.Equal(t, e2eepb.RegisterDeviceResponse_OK, e.registerDevice(u, req).Result)
}

func testServer_RegisterDevice_TooManyDevices(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	u := e.newUser()

	for range e2ee.MaxDevicesPerUser {
		e.register(u, 0, 0)
	}
	resp := e.registerDevice(u, newDeviceFixture().registerRequest(u.keys, 0, 0))
	require.Equal(t, e2eepb.RegisterDeviceResponse_TOO_MANY_DEVICES, resp.Result)
	require.Nil(t, resp.Device)

	// Unregistering one makes room, and its id is what the newcomer gets.
	require.Equal(t, e2eepb.UnregisterDeviceResponse_OK, e.unregisterDevice(u, 2).Result)
	resp = e.registerDevice(u, newDeviceFixture().registerRequest(u.keys, 0, 0))
	require.Equal(t, e2eepb.RegisterDeviceResponse_OK, resp.Result)
	require.Equal(t, uint32(2), resp.Device.DeviceId.Value)
}

func testServer_RegisterDevice_DuplicateIdentityKey(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	u := e.newUser()

	d := e.register(u, 0, 0)
	resp := e.registerDevice(u, d.fixture.registerRequest(u.keys, 0, 0))
	require.Equal(t, e2eepb.RegisterDeviceResponse_DUPLICATE_IDENTITY_KEY, resp.Result)

	// Gone, the key is free again.
	require.Equal(t, e2eepb.UnregisterDeviceResponse_OK, e.unregisterDevice(u, d.id).Result)
	resp = e.registerDevice(u, d.fixture.registerRequest(u.keys, 0, 0))
	require.Equal(t, e2eepb.RegisterDeviceResponse_OK, resp.Result)
}

// Capabilities declared at registration are served on the device and on
// its bundles; a required one that is missing refuses the registration.
func testServer_RegisterDevice_Capabilities(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	u := e.newUser()
	peer := e.newUser()
	d := e.register(u, 1, 1)
	require.Equal(t, fixtureCapabilities, d.proto.Capabilities)

	listed, err := e.getDevices(peer, u.id)
	require.NoError(t, err)
	require.Equal(t, fixtureCapabilities, listed.Users[0].Devices[0].Capabilities)

	p := e.register(peer, 0, 0)
	bundles, err := e.getPreKeyBundles(p, &e2eepb.GetPreKeyBundlesRequest{UserIds: []*commonpb.UserId{u.id}})
	require.NoError(t, err)
	require.Equal(t, fixtureCapabilities, bundles.Bundles[0].Capabilities)

	// A build below the floor is kept off the roster: the post-quantum
	// ratchet is required by default.
	v := e.newUser()
	req := newDeviceFixture().registerRequest(v.keys, 0, 0)
	req.Capabilities = nil
	require.Equal(t, e2eepb.RegisterDeviceResponse_MISSING_REQUIRED_CAPABILITY, e.registerDevice(v, req).Result)
	devices, err := e.getDevices(v)
	require.NoError(t, err)
	require.Empty(t, devices.Users[0].Devices)
	require.Equal(t, e2eepb.RegisterDeviceResponse_OK, e.registerDevice(v, newDeviceFixture().registerRequest(v.keys, 0, 0)).Result)

	// An option lowers the floor.
	lax := newServerEnvWith(t, accounts, s, []e2ee.KeyDistributionOption{e2ee.WithRequiredCapabilities()}, nil)
	w := lax.newUser()
	req = newDeviceFixture().registerRequest(w.keys, 0, 0)
	req.Capabilities = nil
	require.Equal(t, e2eepb.RegisterDeviceResponse_OK, lax.registerDevice(w, req).Result)
}

func testServer_UnregisterDevice(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	u := e.newUser()
	other := e.newUser()

	require.Equal(t, e2eepb.UnregisterDeviceResponse_UNKNOWN_DEVICE, e.unregisterDevice(u, 1).Result)

	d := e.register(u, 1, 1)

	// Only the owner's own devices are theirs to remove.
	require.Equal(t, e2eepb.UnregisterDeviceResponse_UNKNOWN_DEVICE, e.unregisterDevice(other, d.id).Result)

	require.Equal(t, e2eepb.UnregisterDeviceResponse_OK, e.unregisterDevice(u, d.id).Result)
	require.Equal(t, e2eepb.UnregisterDeviceResponse_UNKNOWN_DEVICE, e.unregisterDevice(u, d.id).Result)

	require.Equal(t, e2eepb.GetPreKeyCountsResponse_UNKNOWN_DEVICE, e.getPreKeyCounts(u, d.id).Result)
	devices, err := e.getDevices(u)
	require.NoError(t, err)
	require.Empty(t, devices.Users[0].Devices)
}

func testServer_GetDevices(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()
	nobody := model.MustGenerateUserID()

	a1 := e.register(alice, 0, 0)
	a2 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)

	// Alice opens a mailbox, which last_seen records.
	e.drain(a1)

	// Empty lists the caller's own, last_seen included.
	resp, err := e.getDevices(alice)
	require.NoError(t, err)
	require.Equal(t, e2eepb.GetDevicesResponse_OK, resp.Result)
	require.Len(t, resp.Users, 1)
	require.Empty(t, resp.NotFound)
	require.Equal(t, alice.id.Value, resp.Users[0].UserId.Value)
	require.Len(t, resp.Users[0].Devices, 2)
	require.Equal(t, a1.id, resp.Users[0].Devices[0].DeviceId.Value)
	require.NotNil(t, resp.Users[0].Devices[0].LastSeen)
	require.Equal(t, a2.id, resp.Users[0].Devices[1].DeviceId.Value)
	require.Nil(t, resp.Users[0].Devices[1].LastSeen)

	// Bob sees Alice's devices, without last_seen, and his own with it; a
	// user that does not exist is reported, not an error.
	e.drain(b1)
	resp, err = e.getDevices(bob, alice.id, nobody, bob.id)
	require.NoError(t, err)
	require.Equal(t, e2eepb.GetDevicesResponse_OK, resp.Result)
	require.Len(t, resp.Users, 2)
	require.Len(t, resp.NotFound, 1)
	require.Equal(t, nobody.Value, resp.NotFound[0].Value)
	for _, ud := range resp.Users {
		switch {
		case bytes.Equal(ud.UserId.Value, alice.id.Value):
			require.Len(t, ud.Devices, 2)
			for _, d := range ud.Devices {
				require.Nil(t, d.LastSeen)
				require.Equal(t, []byte(alice.keys.Public()), d.AccountKey.Value)
			}
		case bytes.Equal(ud.UserId.Value, bob.id.Value):
			require.Len(t, ud.Devices, 1)
			require.NotNil(t, ud.Devices[0].LastSeen)
		default:
			t.Fatalf("unexpected user %s", model.UserIDString(ud.UserId))
		}
	}

	// A user with no devices is listed empty.
	carol := e.newUser()
	resp, err = e.getDevices(bob, carol.id)
	require.NoError(t, err)
	require.Len(t, resp.Users, 1)
	require.Empty(t, resp.Users[0].Devices)

	_, err = e.getDevices(bob, alice.id, alice.id)
	requireCode(t, err, codes.InvalidArgument)
}

func testServer_SetKeys(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	u := e.newUser()
	d := e.register(u, 2, 2)
	sibling := e.register(u, 0, 0)

	// Unknown device.
	resp := e.setKeys(u, &e2eepb.SetKeysRequest{DeviceId: &e2eepb.DeviceId{Value: 9}, OneTimePrekeys: oneTimeBatch(d.fixture.oneTimePreKeys(1))})
	require.Equal(t, e2eepb.SetKeysResponse_UNKNOWN_DEVICE, resp.Result)

	// Replace the curve pool only.
	resp = e.setKeys(u, &e2eepb.SetKeysRequest{DeviceId: &e2eepb.DeviceId{Value: d.id}, OneTimePrekeys: oneTimeBatch(d.fixture.oneTimePreKeys(5))})
	require.Equal(t, e2eepb.SetKeysResponse_OK, resp.Result)
	require.Equal(t, uint32(5), resp.Counts.OneTimePrekeys)
	require.Equal(t, uint32(2), resp.Counts.KemOneTimePrekeys)

	// A sibling, authenticated as the same account, cannot set this
	// device's keys with keys it signed itself.
	resp = e.setKeys(u, &e2eepb.SetKeysRequest{DeviceId: &e2eepb.DeviceId{Value: d.id}, SignedPrekey: sibling.fixture.signedPreKey().ToProto()})
	require.Equal(t, e2eepb.SetKeysResponse_INVALID_SIGNATURE, resp.Result)
	resp = e.setKeys(u, &e2eepb.SetKeysRequest{DeviceId: &e2eepb.DeviceId{Value: d.id}, KemOneTimePrekeys: kemBatch(sibling.fixture.kemPreKeys(1))})
	require.Equal(t, e2eepb.SetKeysResponse_INVALID_SIGNATURE, resp.Result)

	// A bad signature applies nothing, even alongside good parts.
	resp = e.setKeys(u, &e2eepb.SetKeysRequest{
		DeviceId:            &e2eepb.DeviceId{Value: d.id},
		OneTimePrekeys:      oneTimeBatch(d.fixture.oneTimePreKeys(1)),
		KemLastResortPrekey: sibling.fixture.kemPreKey().ToProto(),
	})
	require.Equal(t, e2eepb.SetKeysResponse_INVALID_SIGNATURE, resp.Result)
	require.Equal(t, uint32(5), e.getPreKeyCounts(u, d.id).Counts.OneTimePrekeys)

	// A KEM id colliding with the stored last-resort key.
	lastResort := d.fixture.kemPreKey()
	require.Equal(t, e2eepb.SetKeysResponse_OK, e.setKeys(u, &e2eepb.SetKeysRequest{DeviceId: &e2eepb.DeviceId{Value: d.id}, KemLastResortPrekey: lastResort.ToProto()}).Result)
	pool := d.fixture.kemPreKeys(2)
	pool[0].ID = lastResort.ID
	resp = e.setKeys(u, &e2eepb.SetKeysRequest{DeviceId: &e2eepb.DeviceId{Value: d.id}, KemOneTimePrekeys: kemBatch(pool)})
	require.Equal(t, e2eepb.SetKeysResponse_DUPLICATE_PREKEY_ID, resp.Result)
	require.Equal(t, uint32(2), e.getPreKeyCounts(u, d.id).Counts.KemOneTimePrekeys)

	// Within the request.
	pool = d.fixture.kemPreKeys(2)
	pool[1].ID = pool[0].ID
	resp = e.setKeys(u, &e2eepb.SetKeysRequest{DeviceId: &e2eepb.DeviceId{Value: d.id}, KemOneTimePrekeys: kemBatch(pool)})
	require.Equal(t, e2eepb.SetKeysResponse_DUPLICATE_PREKEY_ID, resp.Result)

	// Two one-time curve prekeys sharing an id; the stored pool is untouched.
	curve := d.fixture.oneTimePreKeys(3)
	curve[2].ID = curve[0].ID
	resp = e.setKeys(u, &e2eepb.SetKeysRequest{DeviceId: &e2eepb.DeviceId{Value: d.id}, OneTimePrekeys: oneTimeBatch(curve)})
	require.Equal(t, e2eepb.SetKeysResponse_DUPLICATE_PREKEY_ID, resp.Result)
	require.Equal(t, uint32(5), e.getPreKeyCounts(u, d.id).Counts.OneTimePrekeys)

	// Everything at once.
	signed := d.fixture.signedPreKey()
	resp = e.setKeys(u, &e2eepb.SetKeysRequest{
		DeviceId:            &e2eepb.DeviceId{Value: d.id},
		SignedPrekey:        signed.ToProto(),
		KemLastResortPrekey: d.fixture.kemPreKey().ToProto(),
		OneTimePrekeys:      oneTimeBatch(d.fixture.oneTimePreKeys(1)),
		KemOneTimePrekeys:   kemBatch(d.fixture.kemPreKeys(3)),
	})
	require.Equal(t, e2eepb.SetKeysResponse_OK, resp.Result)
	require.Equal(t, uint32(1), resp.Counts.OneTimePrekeys)
	require.Equal(t, uint32(3), resp.Counts.KemOneTimePrekeys)

	// And served: a bundle carries the new signed prekey.
	bundles, err := e.getPreKeyBundles(sibling, &e2eepb.GetPreKeyBundlesRequest{UserIds: []*commonpb.UserId{u.id}})
	require.NoError(t, err)
	require.Len(t, bundles.Bundles, 1)
	require.Equal(t, signed.ToProto().PublicKey.Value, bundles.Bundles[0].SignedPrekey.PublicKey.Value)
}

func testServer_GetPreKeyCounts(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	u := e.newUser()
	other := e.newUser()

	require.Equal(t, e2eepb.GetPreKeyCountsResponse_UNKNOWN_DEVICE, e.getPreKeyCounts(u, 1).Result)

	d := e.register(u, 4, 1)
	resp := e.getPreKeyCounts(u, d.id)
	require.Equal(t, e2eepb.GetPreKeyCountsResponse_OK, resp.Result)
	require.Equal(t, uint32(4), resp.Counts.OneTimePrekeys)
	require.Equal(t, uint32(1), resp.Counts.KemOneTimePrekeys)

	// The digest is over the repeated-use keys as registered, and follows
	// a replacement: what SetKeys reports is what the next read reports.
	require.Equal(t, e2ee.RepeatedUseDigest(d.fixture.identityKey(), d.fixture.signed, d.fixture.lastResort), resp.RepeatedUseDigest)
	signed := d.fixture.signedPreKey()
	set := e.setKeys(u, &e2eepb.SetKeysRequest{DeviceId: &e2eepb.DeviceId{Value: d.id}, SignedPrekey: signed.ToProto()})
	require.Equal(t, e2eepb.SetKeysResponse_OK, set.Result)
	want := e2ee.RepeatedUseDigest(d.fixture.identityKey(), signed, d.fixture.lastResort)
	require.Equal(t, want, set.RepeatedUseDigest)
	require.NotEqual(t, resp.RepeatedUseDigest, want)
	require.Equal(t, want, e.getPreKeyCounts(u, d.id).RepeatedUseDigest)

	// Another user's device of the same id is not the caller's.
	require.Equal(t, e2eepb.GetPreKeyCountsResponse_UNKNOWN_DEVICE, e.getPreKeyCounts(other, d.id).Result)
}

func testServer_GetPreKeyBundles(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()
	nobody := model.MustGenerateUserID()

	a1 := e.register(alice, 2, 1)
	a2 := e.register(alice, 0, 0)
	b1 := e.register(bob, 1, 1)
	b2 := e.register(bob, 0, 1)

	// The caller's device must be its own.
	resp, err := e.getPreKeyBundles(&device{user: alice, id: 9}, &e2eepb.GetPreKeyBundlesRequest{UserIds: []*commonpb.UserId{bob.id}})
	require.NoError(t, err)
	require.Equal(t, e2eepb.GetPreKeyBundlesResponse_UNKNOWN_DEVICE, resp.Result)

	// Alice's first device fetches Bob and herself: every device of Bob's,
	// and her own other device, but not herself. A missing user is reported.
	resp, err = e.getPreKeyBundles(a1, &e2eepb.GetPreKeyBundlesRequest{UserIds: []*commonpb.UserId{bob.id, alice.id, nobody}})
	require.NoError(t, err)
	require.Equal(t, e2eepb.GetPreKeyBundlesResponse_OK, resp.Result)
	require.Len(t, resp.NotFound, 1)
	require.Equal(t, nobody.Value, resp.NotFound[0].Value)
	require.Len(t, resp.Bundles, 3)

	byDevice := make(map[string]*e2eepb.PreKeyBundle)
	for _, b := range resp.Bundles {
		byDevice[e2ee.AddressFromProto(b.Address).Key()] = b
	}
	require.NotContains(t, byDevice, a1.address().Key())

	bundle := byDevice[b1.address().Key()]
	require.NotNil(t, bundle)
	require.Equal(t, b1.regID, bundle.RegistrationId.Value)
	require.Equal(t, b1.fixture.identityKey(), bundle.IdentityKey.Value)
	require.Equal(t, b1.proto.IdentityKeySignature.Value, bundle.IdentityKeySignature.Value)
	require.Equal(t, []byte(bob.keys.Public()), bundle.AccountKey.Value)
	require.NotNil(t, bundle.SignedPrekey)
	require.NotNil(t, bundle.KemPrekey)
	require.NotNil(t, bundle.OneTimePrekey, "b1 had a one-time curve prekey")
	// The whole chain verifies as a peer would verify it.
	require.True(t, e2ee.VerifyIdentityKeySignature(bundle.AccountKey.Value, bundle.IdentityKey.Value, bundle.IdentityKeySignature.Value))
	require.True(t, e2ee.VerifyPreKeySignature(bundle.IdentityKey.Value, bundle.SignedPrekey.PublicKey.Value, bundle.SignedPrekey.Signature.Value))
	require.True(t, e2ee.VerifyPreKeySignature(bundle.IdentityKey.Value, bundle.KemPrekey.PublicKey.Value, bundle.KemPrekey.Signature.Value))

	// b2 had no curve one-time prekey; a2 had nothing one-time at all and
	// gets the last-resort KEM key.
	require.Nil(t, byDevice[b2.address().Key()].OneTimePrekey)
	require.Nil(t, byDevice[a2.address().Key()].OneTimePrekey)
	require.NotNil(t, byDevice[a2.address().Key()].KemPrekey)

	// Consumed: Bob's pools are down by one each, and the next fetch of b1
	// falls back to the last-resort KEM key with no curve key.
	require.Equal(t, uint32(0), e.getPreKeyCounts(bob, b1.id).Counts.OneTimePrekeys)
	require.Equal(t, uint32(0), e.getPreKeyCounts(bob, b1.id).Counts.KemOneTimePrekeys)
	firstKem := bundle.KemPrekey.Id.Value
	resp, err = e.getPreKeyBundles(a2, &e2eepb.GetPreKeyBundlesRequest{UserIds: []*commonpb.UserId{bob.id}})
	require.NoError(t, err)
	require.Len(t, resp.Bundles, 2)
	for _, b := range resp.Bundles {
		if e2ee.AddressFromProto(b.Address).Key() == b1.address().Key() {
			require.Nil(t, b.OneTimePrekey)
			require.NotEqual(t, firstKem, b.KemPrekey.Id.Value)
		}
	}

	// Duplicates are refused before anything is consumed.
	_, err = e.getPreKeyBundles(a1, &e2eepb.GetPreKeyBundlesRequest{UserIds: []*commonpb.UserId{bob.id, bob.id}})
	requireCode(t, err, codes.InvalidArgument)
}

func testServer_GetPreKeyBundles_OneDevice(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()

	a1 := e.register(alice, 0, 0)
	e.register(bob, 1, 1)
	b2 := e.register(bob, 1, 1)

	resp, err := e.getPreKeyBundles(a1, &e2eepb.GetPreKeyBundlesRequest{
		UserIds:  []*commonpb.UserId{bob.id},
		DeviceId: &e2eepb.DeviceId{Value: b2.id},
	})
	require.NoError(t, err)
	require.Equal(t, e2eepb.GetPreKeyBundlesResponse_OK, resp.Result)
	require.Len(t, resp.Bundles, 1)
	require.Equal(t, b2.id, resp.Bundles[0].Address.DeviceId.Value)
	require.Equal(t, uint32(1), e.getPreKeyCounts(bob, 1).Counts.OneTimePrekeys, "the other device's pool is untouched")

	// A device Bob does not have.
	resp, err = e.getPreKeyBundles(a1, &e2eepb.GetPreKeyBundlesRequest{
		UserIds:  []*commonpb.UserId{bob.id},
		DeviceId: &e2eepb.DeviceId{Value: 9},
	})
	require.NoError(t, err)
	require.Equal(t, e2eepb.GetPreKeyBundlesResponse_NOT_FOUND, resp.Result)
	require.Empty(t, resp.Bundles)

	// One device of more than one user is not a request.
	_, err = e.getPreKeyBundles(a1, &e2eepb.GetPreKeyBundlesRequest{
		UserIds:  []*commonpb.UserId{bob.id, alice.id},
		DeviceId: &e2eepb.DeviceId{Value: b2.id},
	})
	requireCode(t, err, codes.InvalidArgument)

	// Nor is the caller's own device, which would only spend its prekeys.
	a1 = e.register(alice, 1, 1) // fresh, with a pool to protect
	_, err = e.getPreKeyBundles(a1, &e2eepb.GetPreKeyBundlesRequest{
		UserIds:  []*commonpb.UserId{alice.id},
		DeviceId: &e2eepb.DeviceId{Value: a1.id},
	})
	requireCode(t, err, codes.InvalidArgument)
	require.Equal(t, uint32(1), e.getPreKeyCounts(alice, a1.id).Counts.OneTimePrekeys)
}

// A fetch past the per-target or per-caller budget is refused with a
// wait, and spends no prekey.
func testServer_GetPreKeyBundles_RateLimited(t *testing.T, accounts account.Store, s e2ee.Store) {
	limits := e2ee.DefaultLimits
	limits.BundlesPerTarget = e2ee.BucketConfig{Size: 1, Refill: time.Hour}
	limits.BundlesPerCaller = e2ee.BucketConfig{Size: 2, Refill: time.Hour}
	e := newServerEnvWith(t, accounts, s, []e2ee.KeyDistributionOption{e2ee.WithKeyDistributionLimits(limits)}, nil)
	alice := e.newUser()
	bob := e.newUser()
	dave := e.newUser()
	a1 := e.register(alice, 0, 0)
	b1 := e.register(bob, 3, 3)
	e.register(dave, 3, 3)
	e.register(dave, 3, 3)

	// One fetch of Bob is within the target's budget; the next is not, and
	// leaves his pool alone.
	resp, err := e.getPreKeyBundles(a1, &e2eepb.GetPreKeyBundlesRequest{UserIds: []*commonpb.UserId{bob.id}})
	require.NoError(t, err)
	require.Equal(t, e2eepb.GetPreKeyBundlesResponse_OK, resp.Result)
	require.Len(t, resp.Bundles, 1)
	require.Equal(t, uint32(2), e.getPreKeyCounts(bob, b1.id).Counts.OneTimePrekeys)
	resp, err = e.getPreKeyBundles(a1, &e2eepb.GetPreKeyBundlesRequest{UserIds: []*commonpb.UserId{bob.id}})
	require.NoError(t, err)
	require.Equal(t, e2eepb.GetPreKeyBundlesResponse_RATE_LIMITED, resp.Result)
	require.Empty(t, resp.Bundles)
	require.Greater(t, resp.RetryAfter.AsDuration(), time.Duration(0))
	require.Equal(t, uint32(2), e.getPreKeyCounts(bob, b1.id).Counts.OneTimePrekeys)

	// Dave's two devices exceed what is left of the caller's budget: refused
	// before either target is charged or spent.
	resp, err = e.getPreKeyBundles(a1, &e2eepb.GetPreKeyBundlesRequest{UserIds: []*commonpb.UserId{dave.id}})
	require.NoError(t, err)
	require.Equal(t, e2eepb.GetPreKeyBundlesResponse_RATE_LIMITED, resp.Result)
	listed, err := e.getDevices(alice, dave.id)
	require.NoError(t, err)
	for _, d := range listed.Users[0].Devices {
		require.Equal(t, uint32(3), e.getPreKeyCounts(dave, d.DeviceId.Value).Counts.OneTimePrekeys)
	}
}

// A send past the per-pair or inbound budget is refused with a wait and
// stores nothing; the sender's own devices are never limited.
func testServer_SendEnvelopes_RateLimited(t *testing.T, accounts account.Store, s e2ee.Store) {
	limits := e2ee.DefaultLimits
	limits.SendsPerPair = e2ee.BucketConfig{Size: 2, Refill: time.Hour}
	e := newServerEnvWith(t, accounts, s, nil, []e2ee.MailboxOption{e2ee.WithMailboxLimits(limits)})
	alice := e.newUser()
	bob := e.newUser()
	a1 := e.register(alice, 0, 0)
	a2 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)

	e.sendOK(a1, sendRequest(a1, bob, pairwise(b1, a2)))
	e.sendOK(a1, sendRequest(a1, bob, pairwise(b1, a2)))
	resp, err := e.send(a1, sendRequest(a1, bob, pairwise(b1, a2)))
	require.NoError(t, err)
	require.Equal(t, e2eepb.SendEnvelopesResponse_RATE_LIMITED, resp.Result)
	require.Greater(t, resp.RetryAfter.AsDuration(), time.Duration(0))
	require.Nil(t, resp.ServerTs)
	require.Len(t, e.drain(b1), 2)
	require.Len(t, e.drain(a2), 2)

	// The pair is directional, and the sibling is not a peer.
	e.sendOK(b1, sendRequest(b1, alice, pairwise(a1, a2)))
	e.sendOK(a1, sendRequest(a1, bob, pairwise(a2)))
	require.Len(t, e.drain(a2), 4)

	// Inbound bytes: a bucket smaller than one envelope refuses every send
	// to the peer, and none to the sender's own devices.
	limits.SendsPerPair = e2ee.DefaultLimits.SendsPerPair
	limits.InboundBytes = e2ee.BucketConfig{Size: 1, Refill: time.Hour}
	e = newServerEnvWith(t, accounts, s, nil, []e2ee.MailboxOption{e2ee.WithMailboxLimits(limits)})
	resp, err = e.send(a1, sendRequest(a1, bob, pairwise(b1, a2)))
	require.NoError(t, err)
	require.Equal(t, e2eepb.SendEnvelopesResponse_RATE_LIMITED, resp.Result)
	require.Len(t, e.drain(b1), 2)
	e.sendOK(a1, sendRequest(a1, bob, pairwise(a2)))
	require.Len(t, e.drain(a2), 5)
}

func testServer_SendEnvelopes_OK(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()

	a1 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)
	b2 := e.register(bob, 0, 0)

	// A first message: a PREKEY_MESSAGE to each of Bob's devices, stamped
	// with Alice's device record.
	req := sendRequest(a1, bob, pairwise(b1, b2))
	for _, pe := range req.GetPairwise().Envelopes {
		pe.Type = e2eepb.Envelope_PREKEY_MESSAGE
	}
	req.ContentHint = e2eepb.Envelope_RESENDABLE
	resp := e.sendOK(a1, req)
	require.Empty(t, resp.MismatchedDevices)

	for _, b := range []*device{b1, b2} {
		envelopes := e.drain(b)
		require.Len(t, envelopes, 1)
		env := envelopes[0]
		require.Len(t, env.Id.Value, e2ee.EnvelopeIDSize)
		require.Equal(t, uint64(1), env.Sequence)
		require.Equal(t, req.ChatId.Value, env.ChatId.Value)
		require.Equal(t, alice.id.Value, env.Source.UserId.Value)
		require.Equal(t, a1.id, env.Source.DeviceId.Value)
		require.Equal(t, e2eepb.Envelope_PREKEY_MESSAGE, env.Type)
		require.True(t, req.ClientTs.AsTime().Equal(env.ClientTs.AsTime()))
		require.True(t, resp.ServerTs.AsTime().Equal(env.ServerTs.AsTime()))
		require.Equal(t, e2eepb.Envelope_RESENDABLE, env.ContentHint)
		require.False(t, env.LowPriority)
		require.Nil(t, env.DeliveryReceipt)
		require.NotNil(t, env.SourceDevice)
		require.Equal(t, a1.fixture.identityKey(), env.SourceDevice.IdentityKey.Value)
		require.Equal(t, a1.regID, env.SourceDevice.RegistrationId.Value)
		require.Equal(t, []byte(alice.keys.Public()), env.SourceDevice.AccountKey.Value)
		require.Nil(t, env.SourceDevice.LastSeen)
	}
	for i, b := range []*device{b1, b2} {
		want := req.GetPairwise().Envelopes[i].Content
		require.Equal(t, want, e.drain(b)[0].Content, "each device gets its own ciphertext")
	}

	// A follow-up CIPHERTEXT is not stamped, and lands at sequence 2.
	req = sendRequest(a1, bob, pairwise(b1, b2))
	req.LowPriority = true
	e.sendOK(a1, req)
	envelopes := e.drain(b1)
	require.Len(t, envelopes, 2)
	require.Equal(t, uint64(2), envelopes[1].Sequence)
	require.Equal(t, e2eepb.Envelope_CIPHERTEXT, envelopes[1].Type)
	require.Nil(t, envelopes[1].SourceDevice)
	require.True(t, envelopes[1].LowPriority)

	// Alice's own mailbox got nothing.
	require.Empty(t, e.drain(a1))
}

func testServer_SendEnvelopes_Siblings(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()

	a1 := e.register(alice, 0, 0)
	a2 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)

	// A device never addresses itself.
	_, err := e.send(a1, sendRequest(a1, bob, pairwise(b1, a1)))
	requireCode(t, err, codes.InvalidArgument)

	// The peer and the sibling together.
	req := sendRequest(a1, bob, pairwise(b1, a2))
	e.sendOK(a1, req)
	envelopes := e.drain(a2)
	require.Len(t, envelopes, 1)
	require.Equal(t, alice.id.Value, envelopes[0].Source.UserId.Value, "a sibling reads its own user as the source")
	require.Len(t, e.drain(b1), 1)

	// Siblings only, with no peer to check the chat against, is taken on
	// the sender's word.
	req = sendRequest(a1, bob, pairwise(a2))
	e.sendOK(a1, req)
	require.Len(t, e.drain(a2), 2)
	require.Len(t, e.drain(b1), 1)

	// The peer alone, with a sibling left out, is refused with the sender's
	// own user and the sibling it omitted: a sibling is never left with a
	// conversation missing its own side.
	resp, err := e.send(a1, sendRequest(a1, bob, pairwise(b1)))
	require.NoError(t, err)
	require.Equal(t, e2eepb.SendEnvelopesResponse_MISMATCHED_DEVICES, resp.Result)
	require.Len(t, resp.MismatchedDevices, 1)
	require.Equal(t, alice.id.Value, resp.MismatchedDevices[0].UserId.Value)
	require.Len(t, resp.MismatchedDevices[0].MissingDevices, 1)
	require.Equal(t, a2.id, resp.MismatchedDevices[0].MissingDevices[0].Value)
	require.Empty(t, resp.MismatchedDevices[0].ExtraDevices)
	require.Len(t, e.drain(b1), 1, "nothing stored")

	// A sender with no other devices has nothing to omit.
	carol := e.newUser()
	c1 := e.register(carol, 0, 0)
	e.sendOK(c1, sendRequest(c1, bob, pairwise(b1)))
	require.Len(t, e.drain(b1), 2)
}

func testServer_SendEnvelopes_MismatchedDevices(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()

	a1 := e.register(alice, 0, 0)
	a2 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)
	b2 := e.register(bob, 0, 0)
	b3 := e.register(bob, 0, 0)

	// Missing b3, extra device 7, stale b2: one entry for Bob (Alice's
	// sibling is named, so none for her), and nothing stored.
	stale := &device{user: bob, id: b2.id, regID: b2.regID + 1}
	extra := &device{user: bob, id: 7, regID: 1}
	resp, err := e.send(a1, sendRequest(a1, bob, pairwise(b1, stale, extra, a2)))
	require.NoError(t, err)
	require.Equal(t, e2eepb.SendEnvelopesResponse_MISMATCHED_DEVICES, resp.Result)
	require.Nil(t, resp.ServerTs)
	require.Len(t, resp.MismatchedDevices, 1)
	m := resp.MismatchedDevices[0]
	require.Equal(t, bob.id.Value, m.UserId.Value)
	require.Equal(t, []uint32{b3.id}, deviceIDs(m.MissingDevices))
	require.Equal(t, []uint32{7}, deviceIDs(m.ExtraDevices))
	require.Equal(t, []uint32{b2.id}, deviceIDs(m.StaleDevices))
	require.Empty(t, e.drain(b1))

	// Alice's own device set is held to the same rule, her sending device
	// excepted: a sibling omitted is missing, a device she no longer has
	// is extra.
	resp, err = e.send(a1, sendRequest(a1, bob, pairwise(b1, b2, b3, &device{user: alice, id: 5, regID: 1})))
	require.NoError(t, err)
	require.Equal(t, e2eepb.SendEnvelopesResponse_MISMATCHED_DEVICES, resp.Result)
	require.Len(t, resp.MismatchedDevices, 1)
	require.Equal(t, alice.id.Value, resp.MismatchedDevices[0].UserId.Value)
	require.Equal(t, []uint32{a2.id}, deviceIDs(resp.MismatchedDevices[0].MissingDevices))
	require.Equal(t, []uint32{5}, deviceIDs(resp.MismatchedDevices[0].ExtraDevices))

	// Corrected, it goes.
	e.sendOK(a1, sendRequest(a1, bob, pairwise(b1, b2, b3, a2)))
	require.Len(t, e.drain(b3), 1)

	// Bob reinstalls b2 under the same id: the id is reused, the
	// registration id is new, and Alice's session with it is stale.
	require.Equal(t, e2eepb.UnregisterDeviceResponse_OK, e.unregisterDevice(bob, b2.id).Result)
	f := newDeviceFixture()
	f.registrationID = b2.regID + 100
	reg := e.registerDevice(bob, f.registerRequest(bob.keys, 0, 0))
	require.Equal(t, b2.id, reg.Device.DeviceId.Value)
	resp, err = e.send(a1, sendRequest(a1, bob, pairwise(b1, b2, b3, a2)))
	require.NoError(t, err)
	require.Equal(t, e2eepb.SendEnvelopesResponse_MISMATCHED_DEVICES, resp.Result)
	require.Equal(t, []uint32{b2.id}, deviceIDs(resp.MismatchedDevices[0].StaleDevices))
}

func deviceIDs(ids []*e2eepb.DeviceId) []uint32 {
	var out []uint32
	for _, id := range ids {
		out = append(out, id.Value)
	}
	return out
}

func testServer_SendEnvelopes_InvalidRecipient(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()
	carol := e.newUser()

	a1 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)
	c1 := e.register(carol, 0, 0)

	// The chat is Alice and Bob's; Carol is not in it.
	resp, err := e.send(a1, sendRequest(a1, bob, pairwise(c1)))
	require.NoError(t, err)
	require.Equal(t, e2eepb.SendEnvelopesResponse_INVALID_RECIPIENT, resp.Result)

	// Two peers is not a DM.
	resp, err = e.send(a1, sendRequest(a1, bob, pairwise(b1, c1)))
	require.NoError(t, err)
	require.Equal(t, e2eepb.SendEnvelopesResponse_INVALID_RECIPIENT, resp.Result)

	// A peer with no account.
	ghost := &user{id: model.MustGenerateUserID()}
	resp, err = e.send(a1, sendRequest(a1, ghost, pairwise(&device{user: ghost, id: 1, regID: 1})))
	require.NoError(t, err)
	require.Equal(t, e2eepb.SendEnvelopesResponse_INVALID_RECIPIENT, resp.Result)

	// A DM ID of the right width that is not this pair's.
	req := sendRequest(a1, bob, pairwise(b1))
	req.ChatId = e2ee.DeriveDmChatID(alice.id, carol.id)
	resp, err = e.send(a1, req)
	require.NoError(t, err)
	require.Equal(t, e2eepb.SendEnvelopesResponse_INVALID_RECIPIENT, resp.Result)

	require.Empty(t, e.drain(b1))
	require.Empty(t, e.drain(c1))
}

func testServer_SendEnvelopes_InvalidTimestamp(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()
	a1 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)

	// Sub-millisecond precision, or a value years ahead, is refused.
	for _, ts := range []time.Time{
		time.Now().Add(400 * 24 * time.Hour).Truncate(time.Millisecond),
		time.Now().Truncate(time.Millisecond).Add(time.Microsecond),
	} {
		req := sendRequest(a1, bob, pairwise(b1))
		req.ClientTs = timestamppb.New(ts)
		resp, err := e.send(a1, req)
		require.NoError(t, err)
		require.Equal(t, e2eepb.SendEnvelopesResponse_INVALID_TIMESTAMP, resp.Result, ts)
	}
	require.Empty(t, e.drain(b1))

	// Otherwise the value is the sender's, however far its clock is off:
	// a day ahead, a month behind.
	for _, ts := range []time.Time{
		time.Now().Add(25 * time.Hour).Truncate(time.Millisecond),
		time.Now().Add(-30 * 24 * time.Hour).Truncate(time.Millisecond),
	} {
		req := sendRequest(a1, bob, pairwise(b1))
		req.ClientTs = timestamppb.New(ts)
		e.sendOK(a1, req)
	}
	require.Len(t, e.drain(b1), 2)
}

func testServer_SendEnvelopes_Refusals(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()
	a1 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)

	// A sending device the caller does not have.
	req := sendRequest(&device{user: alice, id: 9}, bob, pairwise(b1))
	resp, err := e.send(a1, req)
	require.NoError(t, err)
	require.Equal(t, e2eepb.SendEnvelopesResponse_UNKNOWN_DEVICE, resp.Result)

	// A group chat ID names nothing here.
	req = sendRequest(a1, bob, pairwise(b1))
	req.ChatId = &commonpb.ChatId{Value: randBytes(16)}
	resp, err = e.send(a1, req)
	require.NoError(t, err)
	require.Equal(t, e2eepb.SendEnvelopesResponse_NOT_FOUND, resp.Result)

	// A sender key send is a group's.
	req = sendRequest(a1, bob, nil)
	req.Payload = &e2eepb.SendEnvelopesRequest_MultiRecipient{
		MultiRecipient: &e2eepb.SendEnvelopesRequest_MultiRecipientEnvelope{
			Content: randBytes(32),
			Recipients: []*e2eepb.SendEnvelopesRequest_MultiRecipientEnvelope_Recipient{
				{Address: b1.addressProto(), RegistrationId: &e2eepb.RegistrationId{Value: b1.regID}},
			},
		},
	}
	_, err = e.send(a1, req)
	requireCode(t, err, codes.InvalidArgument)

	// Plaintext over 1 KiB.
	req = sendRequest(a1, bob, pairwise(b1))
	req.GetPairwise().Envelopes[0].Type = e2eepb.Envelope_PLAINTEXT_CONTENT
	req.GetPairwise().Envelopes[0].Content = randBytes(1025)
	_, err = e.send(a1, req)
	requireCode(t, err, codes.InvalidArgument)

	// At the bound it is relayed.
	req = sendRequest(a1, bob, pairwise(b1))
	req.GetPairwise().Envelopes[0].Type = e2eepb.Envelope_PLAINTEXT_CONTENT
	req.GetPairwise().Envelopes[0].Content = randBytes(1024)
	e.sendOK(a1, req)

	// The same device twice.
	req = sendRequest(a1, bob, pairwise(b1, b1))
	_, err = e.send(a1, req)
	requireCode(t, err, codes.InvalidArgument)

	// A SERVER_DELIVERY_RECEIPT is the server's to send: refused by
	// validation before the handler.
	req = sendRequest(a1, bob, pairwise(b1))
	req.GetPairwise().Envelopes[0].Type = e2eepb.Envelope_SERVER_DELIVERY_RECEIPT
	_, err = e.send(a1, req)
	requireCode(t, err, codes.InvalidArgument)

	envelopes := e.drain(b1)
	require.Len(t, envelopes, 1)
	require.Equal(t, e2eepb.Envelope_PLAINTEXT_CONTENT, envelopes[0].Type)
}

func testServer_SendEnvelopes_Online(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()
	a1 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)

	// Accepted, checked like any send, and held briefly, flagged: a device
	// that drains within the window gets it, one that does not never will.
	req := sendRequest(a1, bob, pairwise(b1))
	req.Online = true
	e.sendOK(a1, req)
	drained := e.drain(b1)
	require.Len(t, drained, 1)
	require.True(t, drained[0].Ephemeral)
	require.Equal(t, uint64(1), drained[0].Sequence)

	req = sendRequest(a1, bob, pairwise(&device{user: bob, id: b1.id, regID: b1.regID + 1}))
	req.Online = true
	resp, err := e.send(a1, req)
	require.NoError(t, err)
	require.Equal(t, e2eepb.SendEnvelopesResponse_MISMATCHED_DEVICES, resp.Result)
}

func testServer_SendEnvelopes_RetryStoresAgain(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()
	a1 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)
	b2 := e.register(bob, 0, 0)

	// A retry after a timeout: same device, same client_ts. The server does
	// not deduplicate, so each recipient holds two copies, OK both times;
	// the recipient absorbs the second.
	req := sendRequest(a1, bob, pairwise(b1, b2))
	e.sendOK(a1, req)
	retry := sendRequest(a1, bob, pairwise(b1, b2))
	retry.ClientTs = req.ClientTs
	e.sendOK(a1, retry)
	drained := e.drain(b1)
	require.Len(t, drained, 2)
	require.True(t, drained[0].ClientTs.AsTime().Equal(drained[1].ClientTs.AsTime()))
	require.Len(t, e.drain(b2), 2)

	// A fresh client_ts is a new send.
	e.sendOK(a1, sendRequest(a1, bob, pairwise(b1, b2)))
	require.Len(t, e.drain(b1), 3)
}

func testServer_GetEnvelopes_Paging(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()
	a1 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)

	resp := e.getEnvelopes(&device{user: bob, id: 9}, 0, 0)
	require.Equal(t, e2eepb.GetEnvelopesResponse_UNKNOWN_DEVICE, resp.Result)

	// Empty: no batch at all, not an empty one.
	resp = e.getEnvelopes(b1, 0, 0)
	require.Equal(t, e2eepb.GetEnvelopesResponse_OK, resp.Result)
	require.Nil(t, resp.Envelopes)
	require.False(t, resp.HasMore)

	for range 5 {
		e.sendOK(a1, sendRequest(a1, bob, pairwise(b1)))
	}

	resp = e.getEnvelopes(b1, 0, 2)
	require.Len(t, resp.Envelopes.Envelopes, 2)
	require.True(t, resp.HasMore)
	require.Equal(t, uint64(1), resp.Envelopes.Envelopes[0].Sequence)
	require.Equal(t, uint64(2), resp.Envelopes.Envelopes[1].Sequence)

	resp = e.getEnvelopes(b1, 2, 2)
	require.Len(t, resp.Envelopes.Envelopes, 2)
	require.True(t, resp.HasMore)
	require.Equal(t, uint64(3), resp.Envelopes.Envelopes[0].Sequence)

	resp = e.getEnvelopes(b1, 4, 2)
	require.Len(t, resp.Envelopes.Envelopes, 1)
	require.False(t, resp.HasMore)
	require.Equal(t, uint64(5), resp.Envelopes.Envelopes[0].Sequence)

	// A page larger than the cap is capped.
	resp = e.getEnvelopes(b1, 0, 500)
	require.Len(t, resp.Envelopes.Envelopes, 5)
	require.False(t, resp.HasMore)

	// A drain leaves everything in place until acknowledged, and records
	// last_seen, as the device's day, for the owner's eyes only.
	require.Len(t, e.drain(b1), 5)
	own, err := e.getDevices(bob)
	require.NoError(t, err)
	seen := own.Users[0].Devices[0].LastSeen
	require.NotNil(t, seen)
	require.True(t, seen.AsTime().Equal(e2ee.LastSeenDay(bob.id, time.Now())))
	theirs, err := e.getDevices(alice, bob.id)
	require.NoError(t, err)
	require.Nil(t, theirs.Users[0].Devices[0].LastSeen)

	// A drain later the same day changes nothing.
	e.drain(b1)
	own, err = e.getDevices(bob)
	require.NoError(t, err)
	require.True(t, seen.AsTime().Equal(own.Users[0].Devices[0].LastSeen.AsTime()))
}

func testServer_GetEnvelopes_ByteBudget(t *testing.T, accounts account.Store, s e2ee.Store) {
	// Envelopes carry 48 bytes of content each (see pairwise); a budget of
	// 100 fits two, and the third, which crosses it, is held for the next
	// page.
	e := newServerEnvWith(t, accounts, s, nil, []e2ee.MailboxOption{e2ee.WithGetEnvelopesPageBytes(100)})
	alice := e.newUser()
	bob := e.newUser()
	a1 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)
	for range 5 {
		e.sendOK(a1, sendRequest(a1, bob, pairwise(b1)))
	}

	resp := e.getEnvelopes(b1, 0, 0)
	require.Len(t, resp.Envelopes.Envelopes, 2)
	require.True(t, resp.HasMore)
	require.Equal(t, uint64(2), resp.Envelopes.Envelopes[1].Sequence)

	resp = e.getEnvelopes(b1, 2, 0)
	require.Len(t, resp.Envelopes.Envelopes, 2)
	require.True(t, resp.HasMore)
	require.Equal(t, uint64(4), resp.Envelopes.Envelopes[1].Sequence)

	// The last envelope fits the budget alone: the end, and said so.
	resp = e.getEnvelopes(b1, 4, 0)
	require.Len(t, resp.Envelopes.Envelopes, 1)
	require.False(t, resp.HasMore)

	// page_size below what the budget allows still binds.
	resp = e.getEnvelopes(b1, 0, 1)
	require.Len(t, resp.Envelopes.Envelopes, 1)
	require.True(t, resp.HasMore)

	// A budget no envelope fits serves one per page, and the mailbox's
	// last envelope is answered has_more once too often: the next page is
	// empty, and nothing is lost.
	e = newServerEnvWith(t, accounts, s, nil, []e2ee.MailboxOption{e2ee.WithGetEnvelopesPageBytes(10)})
	var seen []uint64
	var after uint64
	for {
		resp := e.getEnvelopes(b1, after, 0)
		if resp.Envelopes == nil {
			require.False(t, resp.HasMore)
			break
		}
		require.Len(t, resp.Envelopes.Envelopes, 1)
		require.True(t, resp.HasMore)
		after = resp.Envelopes.Envelopes[0].Sequence
		seen = append(seen, after)
	}
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, seen)
}

// Every mailbox call records the calling device's day: a send, a drain, an
// acknowledgement. The day is the user's own reckoning, shifted from UTC by
// an offset fixed by the user ID, so the fleet's daily writes spread over
// the day rather than all landing after midnight.
func testServer_LastSeen(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()
	a1 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)

	lastSeen := func(u *user, d *device) *time.Time {
		resp, err := e.getDevices(u)
		require.NoError(t, err)
		for _, got := range resp.Users[0].Devices {
			if got.DeviceId.Value == d.id {
				if got.LastSeen == nil {
					return nil
				}
				ts := got.LastSeen.AsTime()
				return &ts
			}
		}
		t.Fatalf("device %d not listed", d.id)
		return nil
	}
	require.Nil(t, lastSeen(alice, a1))
	require.Nil(t, lastSeen(bob, b1))

	// A send records the sender, not the recipient.
	e.sendOK(a1, sendRequest(a1, bob, pairwise(b1)))
	now := time.Now()
	require.NotNil(t, lastSeen(alice, a1))
	require.True(t, lastSeen(alice, a1).Equal(e2ee.LastSeenDay(alice.id, now)))
	require.Nil(t, lastSeen(bob, b1))

	// An acknowledgement records the acknowledger, drain or not: the drain
	// here goes through the store so the handler's memo has not seen b1.
	envelopes, err := s.GetEnvelopes(e.ctx, b1.address(), 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, envelopes, 1)
	require.Nil(t, lastSeen(bob, b1))
	ack := &e2eepb.AckEnvelopesRequest{DeviceId: &e2eepb.DeviceId{Value: b1.id}, EnvelopeIds: []*e2eepb.EnvelopeId{{Value: envelopes[0].ID}}}
	require.NoError(t, b1.user.keys.Auth(ack, &ack.Auth))
	resp, err := e.mailbox.AckEnvelopes(e.ctx, ack)
	require.NoError(t, err)
	require.Equal(t, e2eepb.AckEnvelopesResponse_OK, resp.Result)
	require.True(t, lastSeen(bob, b1).Equal(e2ee.LastSeenDay(bob.id, now)))

	// The day is the UTC day of now less the user's offset: a UTC day
	// start, today or yesterday, and the offset is fixed by the ID and
	// within a day.
	offset := e2ee.LastSeenOffset(alice.id)
	require.Equal(t, offset, e2ee.LastSeenOffset(alice.id))
	require.GreaterOrEqual(t, offset, time.Duration(0))
	require.Less(t, offset, 24*time.Hour)
	day := e2ee.LastSeenDay(alice.id, now)
	require.True(t, day.Equal(now.Add(-offset).UTC().Truncate(24*time.Hour)))
	today := now.UTC().Truncate(24 * time.Hour)
	require.True(t, day.Equal(today) || day.Equal(today.Add(-24*time.Hour)))
}

func testServer_AckEnvelopes_DeliveryReceipts(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()
	a1 := e.register(alice, 0, 0)
	a2 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)
	b2 := e.register(bob, 0, 0)

	resp := e.ack(&device{user: bob, id: 9}, &e2eepb.Envelope{Id: &e2eepb.EnvelopeId{Value: randBytes(16)}})
	require.Equal(t, e2eepb.AckEnvelopesResponse_UNKNOWN_DEVICE, resp.Result)

	// Two sends from a1 to Bob, both to his devices and to a2.
	first := sendRequest(a1, bob, pairwise(b1, b2, a2))
	e.sendOK(a1, first)
	second := sendRequest(a1, bob, pairwise(b1, b2, a2))
	e.sendOK(a1, second)

	// b1 acknowledges both, plus an unknown id: gone from its mailbox, and
	// ONE receipt naming both sends lands in every one of Alice's mailboxes,
	// a1's included, with b1 as the acknowledger.
	envelopes := e.drain(b1)
	require.Len(t, envelopes, 2)
	require.Equal(t, e2eepb.AckEnvelopesResponse_OK, e.ack(b1, envelopes[0], envelopes[1], &e2eepb.Envelope{Id: &e2eepb.EnvelopeId{Value: randBytes(16)}}).Result)
	require.Empty(t, e.drain(b1))

	for _, a := range []*device{a1, a2} {
		mailbox := e.drain(a)
		var receipts []*e2eepb.Envelope
		for _, env := range mailbox {
			if env.Type == e2eepb.Envelope_SERVER_DELIVERY_RECEIPT {
				receipts = append(receipts, env)
			}
		}
		require.Len(t, receipts, 1, "one receipt per acknowledging call for %s", a.address().Key())
		r := receipts[0]
		require.Nil(t, r.Source)
		require.Empty(t, r.Content)
		require.Nil(t, r.ClientTs)
		require.NotNil(t, r.ServerTs)
		require.True(t, r.LowPriority)
		require.Equal(t, first.ChatId.Value, r.ChatId.Value)
		require.NotNil(t, r.DeliveryReceipt)
		require.Len(t, r.DeliveryReceipt.Acknowledgements, 1)
		ack := r.DeliveryReceipt.Acknowledgements[0]
		require.Equal(t, bob.id.Value, ack.Recipient.UserId.Value)
		require.Equal(t, b1.id, ack.Recipient.DeviceId.Value)
		require.Len(t, ack.ClientTs, 2)
		require.True(t, first.ClientTs.AsTime().Equal(ack.ClientTs[0].AsTime()))
		require.True(t, second.ClientTs.AsTime().Equal(ack.ClientTs[1].AsTime()))
	}
	// a2 also holds the two sibling copies of the sends, which earn no
	// receipt when a2 acknowledges them: a sibling's acknowledgement is not
	// a delivery.
	a2Mailbox := e.drain(a2)
	require.Len(t, a2Mailbox, 3)
	require.Equal(t, e2eepb.AckEnvelopesResponse_OK, e.ack(a2, a2Mailbox...).Result)
	require.Empty(t, e.drain(a2))
	require.Len(t, e.drain(a1), 1, "no receipt for a sibling's acknowledgement")

	// Acknowledging again is a no-op with no second receipt.
	require.Equal(t, e2eepb.AckEnvelopesResponse_OK, e.ack(b1, envelopes[0]).Result)
	require.Len(t, e.drain(a1), 1)

	// b2's acknowledgement is its own receipt.
	b2Mailbox := e.drain(b2)
	require.Equal(t, e2eepb.AckEnvelopesResponse_OK, e.ack(b2, b2Mailbox[0]).Result)
	a1Mailbox := e.drain(a1)
	require.Len(t, a1Mailbox, 2)
	ack := a1Mailbox[1].DeliveryReceipt.Acknowledgements[0]
	require.Equal(t, b2.id, ack.Recipient.DeviceId.Value)
	require.Len(t, ack.ClientTs, 1)
	require.True(t, first.ClientTs.AsTime().Equal(ack.ClientTs[0].AsTime()))

	// Sends in two chats acknowledged together earn two receipts, one per
	// chat, since a receipt's chat is its envelope's.
	carol := e.newUser()
	c1 := e.register(carol, 0, 0)
	toBob := sendRequest(a1, bob, pairwise(b1, b2, a2))
	e.sendOK(a1, toBob)
	toCarol := sendRequest(a1, carol, pairwise(c1, a2))
	e.sendOK(a1, toCarol)
	e.drain(a1)
	require.Equal(t, e2eepb.AckEnvelopesResponse_OK, e.ack(a1, e.drain(a1)...).Result)
	require.Equal(t, e2eepb.AckEnvelopesResponse_OK, e.ack(b1, e.drain(b1)...).Result)
	require.Equal(t, e2eepb.AckEnvelopesResponse_OK, e.ack(c1, e.drain(c1)...).Result)
	var chats [][]byte
	for _, env := range e.drain(a1) {
		require.Equal(t, e2eepb.Envelope_SERVER_DELIVERY_RECEIPT, env.Type)
		chats = append(chats, env.ChatId.Value)
	}
	require.Len(t, chats, 2)
	require.NotEqual(t, chats[0], chats[1])
}

func testServer_AckEnvelopes_NoReceipt(t *testing.T, accounts account.Store, s e2ee.Store) {
	e := newServerEnv(t, accounts, s)
	alice := e.newUser()
	bob := e.newUser()
	a1 := e.register(alice, 0, 0)
	b1 := e.register(bob, 0, 0)

	// The sender declined one.
	req := sendRequest(a1, bob, pairwise(b1))
	req.NoDeliveryReceipt = true
	e.sendOK(a1, req)

	// Plaintext never earns one.
	req = sendRequest(a1, bob, pairwise(b1))
	req.GetPairwise().Envelopes[0].Type = e2eepb.Envelope_PLAINTEXT_CONTENT
	req.GetPairwise().Envelopes[0].Content = randBytes(64)
	e.sendOK(a1, req)

	envelopes := e.drain(b1)
	require.Len(t, envelopes, 2)
	require.Equal(t, e2eepb.AckEnvelopesResponse_OK, e.ack(b1, envelopes...).Result)
	require.Empty(t, e.drain(a1))

	// A sender that has unregistered every device has no mailbox for one.
	req = sendRequest(a1, bob, pairwise(b1))
	e.sendOK(a1, req)
	require.Equal(t, e2eepb.UnregisterDeviceResponse_OK, e.unregisterDevice(alice, a1.id).Result)
	envelopes = e.drain(b1)
	require.Len(t, envelopes, 1)
	require.Equal(t, e2eepb.AckEnvelopesResponse_OK, e.ack(b1, envelopes...).Result)
	require.Empty(t, e.drain(b1))
}

func (e *serverEnv) setCapabilities(u *user, deviceID uint32, caps ...e2eepb.Device_Capability) *e2eepb.SetCapabilitiesResponse {
	req := &e2eepb.SetCapabilitiesRequest{DeviceId: &e2eepb.DeviceId{Value: deviceID}, Capabilities: caps}
	require.NoError(e.t, u.keys.Auth(req, &req.Auth))
	resp, err := e.keys.SetCapabilities(e.ctx, req)
	require.NoError(e.t, err)
	return resp
}

// A device registered without a capability gains it in place: senders see
// it on the device list and on bundles, a repeat changes nothing, and only
// the owner's own devices are theirs to extend.
func testServer_SetCapabilities(t *testing.T, accounts account.Store, s e2ee.Store) {
	// Nothing required, so a device can register bare and gain the one
	// capability that exists afterwards.
	e := newServerEnvWith(t, accounts, s, []e2ee.KeyDistributionOption{e2ee.WithRequiredCapabilities()}, nil)
	u := e.newUser()
	peer := e.newUser()

	require.Equal(t, e2eepb.SetCapabilitiesResponse_UNKNOWN_DEVICE, e.setCapabilities(u, 1, e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET).Result)

	req := newDeviceFixture().registerRequest(u.keys, 1, 1)
	req.Capabilities = nil
	registered := e.registerDevice(u, req)
	require.Equal(t, e2eepb.RegisterDeviceResponse_OK, registered.Result)
	require.Empty(t, registered.Device.Capabilities)
	id := registered.Device.DeviceId.Value

	resp := e.setCapabilities(u, id, e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET)
	require.Equal(t, e2eepb.SetCapabilitiesResponse_OK, resp.Result)
	require.Equal(t, fixtureCapabilities, resp.Device.Capabilities)
	require.NotNil(t, resp.Device.RegisteredAt)

	listed, err := e.getDevices(peer, u.id)
	require.NoError(t, err)
	require.Equal(t, fixtureCapabilities, listed.Users[0].Devices[0].Capabilities)
	p := e.register(peer, 0, 0)
	bundles, err := e.getPreKeyBundles(p, &e2eepb.GetPreKeyBundlesRequest{UserIds: []*commonpb.UserId{u.id}})
	require.NoError(t, err)
	require.Equal(t, fixtureCapabilities, bundles.Bundles[0].Capabilities)

	// Declaring it again is a no-op with the same answer.
	again := e.setCapabilities(u, id, e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET)
	require.Equal(t, e2eepb.SetCapabilitiesResponse_OK, again.Result)
	require.Equal(t, fixtureCapabilities, again.Device.Capabilities)

	// A user without that device cannot extend it: the id names the
	// caller's own device or nothing.
	require.Equal(t, e2eepb.SetCapabilitiesResponse_UNKNOWN_DEVICE, e.setCapabilities(e.newUser(), id, e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET).Result)
}

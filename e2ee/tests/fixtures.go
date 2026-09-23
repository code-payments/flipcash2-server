package tests

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"time"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	e2eepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/e2ee/v1"

	"github.com/code-payments/flipcash2-server/e2ee"
	"github.com/code-payments/flipcash2-server/e2ee/xeddsa"
	"github.com/code-payments/flipcash2-server/model"
)

const (
	ecKeyType  = 0x05
	kemKeyType = 0x08
)

// deviceFixture is what a client holds to register a device: its identity
// key and the prekeys it signs with it. The server checks only signatures
// and shapes, so prekey values are random bytes of the right form.
type deviceFixture struct {
	identity       *xeddsa.KeyPair
	registrationID uint32
	nextPreKeyID   atomic.Uint32

	// signed and lastResort are the repeated-use keys the fixture registered
	// with, so a test can tell the last-resort key from the one-time pool
	// and compute the repeated-use digest.
	signed     *e2ee.SignedPreKey
	lastResort *e2ee.KemSignedPreKey

	// appInstall is the install the fixture registers under.
	appInstall string
}

func newDeviceFixture() *deviceFixture {
	f := &deviceFixture{
		identity:       xeddsa.MustGenerateKeyPair(),
		registrationID: uint32(1 + randByte()%100),
	}
	f.nextPreKeyID.Store(1)
	f.appInstall = hex.EncodeToString(randBytes(8))
	return f
}

// fixtureCapabilities is what every fixture device declares.
var fixtureCapabilities = []e2eepb.Device_Capability{e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET}

func randBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

func randByte() byte {
	return randBytes(1)[0]
}

func (f *deviceFixture) identityKey() []byte {
	return append([]byte{ecKeyType}, f.identity.PublicKey()...)
}

// lastResortID is the id of the last-resort KEM prekey keys() minted.
func (f *deviceFixture) lastResortID() uint32 {
	return f.lastResort.ID
}

// preKeyID mints the next id; safe from several goroutines.
func (f *deviceFixture) preKeyID() uint32 {
	return f.nextPreKeyID.Add(1) - 1
}

// identitySignature certifies the identity key with the account key.
func (f *deviceFixture) identitySignature(account model.KeyPair) []byte {
	return ed25519.Sign(account.Private(), e2ee.IdentityKeyMessage(f.identityKey()))
}

func (f *deviceFixture) signedPreKey() *e2ee.SignedPreKey {
	pub := append([]byte{ecKeyType}, randBytes(32)...)
	return &e2ee.SignedPreKey{ID: f.preKeyID(), PublicKey: pub, Signature: f.identity.Sign(pub)}
}

func (f *deviceFixture) oneTimePreKeys(n int) []*e2ee.OneTimePreKey {
	out := make([]*e2ee.OneTimePreKey, 0, n)
	for range n {
		out = append(out, &e2ee.OneTimePreKey{ID: f.preKeyID(), PublicKey: append([]byte{ecKeyType}, randBytes(32)...)})
	}
	return out
}

func (f *deviceFixture) kemPreKey() *e2ee.KemSignedPreKey {
	pub := append([]byte{kemKeyType}, randBytes(1568)...)
	return &e2ee.KemSignedPreKey{ID: f.preKeyID(), PublicKey: pub, Signature: f.identity.Sign(pub)}
}

func (f *deviceFixture) kemPreKeys(n int) []*e2ee.KemSignedPreKey {
	out := make([]*e2ee.KemSignedPreKey, 0, n)
	for range n {
		out = append(out, f.kemPreKey())
	}
	return out
}

// keys is a full initial key set with pools of the given sizes.
func (f *deviceFixture) keys(oneTime, kemOneTime int) *e2ee.DeviceKeys {
	f.signed = f.signedPreKey()
	f.lastResort = f.kemPreKey()
	return &e2ee.DeviceKeys{
		SignedPreKey:        f.signed,
		KemLastResortPreKey: f.lastResort,
		OneTimePreKeys:      f.oneTimePreKeys(oneTime),
		KemOneTimePreKeys:   f.kemPreKeys(kemOneTime),
	}
}

// device is the fixed record the store is asked to register, before an id
// is assigned.
func (f *deviceFixture) device(userID *commonpb.UserId, account model.KeyPair) *e2ee.Device {
	return &e2ee.Device{
		Address:              e2ee.DeviceAddress{UserID: userID},
		RegistrationID:       f.registrationID,
		IdentityKey:          f.identityKey(),
		IdentityKeySignature: f.identitySignature(account),
		AccountKey:           account.Public(),
		// UTC() drops the monotonic reading, so a record read back from a
		// store compares equal to this one.
		RegisteredAt: time.Now().UTC(),
		Capabilities: fixtureCapabilities,
		AppInstall:   f.appInstall,
	}
}

// registerRequest is the RPC form of device + keys, unauthenticated.
func (f *deviceFixture) registerRequest(account model.KeyPair, oneTime, kemOneTime int) *e2eepb.RegisterDeviceRequest {
	keys := f.keys(oneTime, kemOneTime)
	req := &e2eepb.RegisterDeviceRequest{
		IdentityKey:          &e2eepb.EcPublicKey{Value: f.identityKey()},
		IdentityKeySignature: &commonpb.Signature{Value: f.identitySignature(account)},
		RegistrationId:       &e2eepb.RegistrationId{Value: f.registrationID},
		SignedPrekey:         keys.SignedPreKey.ToProto(),
		KemLastResortPrekey:  keys.KemLastResortPreKey.ToProto(),
		IdempotencyKey:       newIdempotencyKey(),
		AppInstall:           &commonpb.AppInstallId{Value: f.appInstall},
		Capabilities:         fixtureCapabilities,
	}
	if oneTime > 0 {
		req.OneTimePrekeys = oneTimeBatch(keys.OneTimePreKeys)
	}
	if kemOneTime > 0 {
		req.KemOneTimePrekeys = kemBatch(keys.KemOneTimePreKeys)
	}
	return req
}

func oneTimeBatch(keys []*e2ee.OneTimePreKey) *e2eepb.OneTimePreKeyBatch {
	b := &e2eepb.OneTimePreKeyBatch{}
	for _, k := range keys {
		b.Prekeys = append(b.Prekeys, k.ToProto())
	}
	return b
}

func kemBatch(keys []*e2ee.KemSignedPreKey) *e2eepb.KemSignedPreKeyBatch {
	b := &e2eepb.KemSignedPreKeyBatch{}
	for _, k := range keys {
		b.Prekeys = append(b.Prekeys, k.ToProto())
	}
	return b
}

func newIdempotencyKey() *chatpb.IdempotencyKey {
	return &chatpb.IdempotencyKey{Value: randBytes(16)}
}

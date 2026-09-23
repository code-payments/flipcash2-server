// Package e2ee is the server side of end-to-end encrypted messaging: the
// KeyDistribution and Mailbox services of e2ee.v1, which implement the
// server's role under the Signal protocol suite (PQXDH, Double Ratchet,
// Sesame). The protos are the specification; this package holds what the
// server must decide and remember to play its part, and nothing it could
// read of a message.
//
// The package is standalone by design, for now. It does not read chat
// records, evaluate chat rules, publish on the event bus or send pushes: an
// E2EE chat is a DM whose membership is derived from its ID (see
// DeriveDmChatID), live delivery is the client draining its mailbox, and
// groups (sender keys) are refused. Integration with chat/, event/ and push/
// is the next step and is meant to replace those three stubs without
// changing the store.
//
// What the server knows about a device is a Device: its identity key with
// the account's certification of it, its registration ID, and its prekeys,
// which are consumed as bundles are handed out. What it knows about a
// message is an Envelope in a mailbox: routing metadata and bytes it cannot
// read, held until the recipient device acknowledges it.
package e2ee

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"slices"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	e2eepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/e2ee/v1"
)

const (
	// MaxDevicesPerUser is the most devices a user may hold at once
	// (RegisterDeviceResponse.TOO_MANY_DEVICES).
	MaxDevicesPerUser = 5

	// MinDeviceID and MaxDeviceID bound the DeviceId space: libsignal holds
	// device ids in a byte, and 0 is not a device.
	MinDeviceID = 1
	MaxDeviceID = 127

	// EnvelopeIDSize is the length of an EnvelopeId.
	EnvelopeIDSize = 16

	// EcPublicKeySize is the serialized length of an EcPublicKey: the type
	// byte followed by the 32-byte u-coordinate.
	EcPublicKeySize = 33

	// KemPublicKeySize is the serialized length of a KemPublicKey: the type
	// byte followed by the 1568-byte Kyber-1024 encapsulation key.
	KemPublicKeySize = 1569
)

var (
	// ErrDeviceNotFound is returned when a named device is not one of the
	// named user's current devices.
	ErrDeviceNotFound = errors.New("e2ee: device not found")

	// ErrTooManyDevices is returned by a registration that would exceed
	// MaxDevicesPerUser.
	ErrTooManyDevices = errors.New("e2ee: too many devices")

	// ErrDuplicateIdentityKey is returned by a registration whose identity key
	// is already registered to another of the user's devices.
	ErrDuplicateIdentityKey = errors.New("e2ee: identity key already registered")

	// ErrDuplicatePreKeyID is returned when two KEM prekeys of a device would
	// share an id, or two one-time curve prekeys of one pool would (see
	// PreKeyId in the proto, and HasDuplicateKemID / HasDuplicateOneTimeID).
	ErrDuplicatePreKeyID = errors.New("e2ee: duplicate prekey id")
)

// DeviceAddress is Sesame's (UserID, DeviceID) pair: the address a session is
// held with and an envelope is delivered to.
type DeviceAddress struct {
	UserID   *commonpb.UserId
	DeviceID uint32
}

// Key returns a string usable as a map key for the address.
func (a DeviceAddress) Key() string {
	return string(a.UserID.GetValue()) + "#" + strconv.FormatUint(uint64(a.DeviceID), 10)
}

// Clone returns a deep copy.
func (a DeviceAddress) Clone() DeviceAddress {
	return DeviceAddress{UserID: cloneUserID(a.UserID), DeviceID: a.DeviceID}
}

// ToProto projects the address.
func (a DeviceAddress) ToProto() *e2eepb.DeviceAddress {
	return &e2eepb.DeviceAddress{
		UserId:   cloneUserID(a.UserID),
		DeviceId: &e2eepb.DeviceId{Value: a.DeviceID},
	}
}

// AddressFromProto reads a DeviceAddress.
func AddressFromProto(p *e2eepb.DeviceAddress) DeviceAddress {
	return DeviceAddress{UserID: cloneUserID(p.GetUserId()), DeviceID: p.GetDeviceId().GetValue()}
}

// Device is the server's record of one of a user's devices: the public half
// of its identity, the account's certification of it, and its registration
// ID. Its prekeys are held separately (DeviceKeys) because they change and
// are consumed; the fields here are fixed for the life of the device.
type Device struct {
	Address        DeviceAddress
	RegistrationID uint32

	// IdentityKey is the device's Curve25519 identity key in libsignal's
	// serialized form (EcPublicKeySize bytes, type byte first).
	IdentityKey []byte

	// IdentityKeySignature is the account key's Ed25519 signature over the
	// domain-separated identity key (see IdentityKeyMessage).
	IdentityKeySignature []byte

	// AccountKey is the Ed25519 public key that made IdentityKeySignature:
	// the key the registration was authenticated with.
	AccountKey []byte

	RegisteredAt time.Time

	// LastSeen is the day the device was last active on its mailbox (a
	// send, a drain or an acknowledgement), as LastSeenDay reckons days for
	// its user; nil until it has been. Day granularity, never finer: a
	// liveness signal, not presence (see Store.TouchLastSeen).
	LastSeen *time.Time

	// Capabilities is what the device's client build can handle, declared
	// at registration, extended in place and never reduced
	// (Store.AddCapabilities), sorted ascending, and served to
	// senders on Device and PreKeyBundle so they encrypt in a form the
	// device can read.
	Capabilities []e2eepb.Device_Capability

	// AppInstall is the app install the device runs in: the one its push
	// tokens are registered under, so the server can wake this device and
	// no other of the user's. Server-only; never served.
	AppInstall string
}

// Clone returns a deep copy.
func (d *Device) Clone() *Device {
	out := &Device{
		Address:              d.Address.Clone(),
		RegistrationID:       d.RegistrationID,
		IdentityKey:          append([]byte(nil), d.IdentityKey...),
		IdentityKeySignature: append([]byte(nil), d.IdentityKeySignature...),
		AccountKey:           append([]byte(nil), d.AccountKey...),
		RegisteredAt:         d.RegisteredAt,
		Capabilities:         append([]e2eepb.Device_Capability(nil), d.Capabilities...),
		AppInstall:           d.AppInstall,
	}
	if d.LastSeen != nil {
		t := *d.LastSeen
		out.LastSeen = &t
	}
	return out
}

// ToProto projects the device. last_seen is carried only when includeLastSeen
// is set: it is returned to the device's own user and withheld from everyone
// else.
func (d *Device) ToProto(includeLastSeen bool) *e2eepb.Device {
	out := &e2eepb.Device{
		DeviceId:             &e2eepb.DeviceId{Value: d.Address.DeviceID},
		RegistrationId:       &e2eepb.RegistrationId{Value: d.RegistrationID},
		IdentityKey:          &e2eepb.EcPublicKey{Value: append([]byte(nil), d.IdentityKey...)},
		IdentityKeySignature: &commonpb.Signature{Value: append([]byte(nil), d.IdentityKeySignature...)},
		RegisteredAt:         timestamppb.New(d.RegisteredAt),
		AccountKey:           &commonpb.PublicKey{Value: append([]byte(nil), d.AccountKey...)},
		Capabilities:         append([]e2eepb.Device_Capability(nil), d.Capabilities...),
	}
	if includeLastSeen && d.LastSeen != nil {
		out.LastSeen = timestamppb.New(*d.LastSeen)
	}
	return out
}

// SignedPreKey is PQXDH's SPK: a Curve25519 prekey with its id and the
// identity key's XEdDSA signature over the serialized public key.
type SignedPreKey struct {
	ID        uint32
	PublicKey []byte
	Signature []byte
}

// Clone returns a deep copy.
func (k *SignedPreKey) Clone() *SignedPreKey {
	if k == nil {
		return nil
	}
	return &SignedPreKey{ID: k.ID, PublicKey: append([]byte(nil), k.PublicKey...), Signature: append([]byte(nil), k.Signature...)}
}

// ToProto projects the prekey.
func (k *SignedPreKey) ToProto() *e2eepb.SignedPreKey {
	return &e2eepb.SignedPreKey{
		Id:        &e2eepb.PreKeyId{Value: k.ID},
		PublicKey: &e2eepb.EcPublicKey{Value: append([]byte(nil), k.PublicKey...)},
		Signature: &commonpb.Signature{Value: append([]byte(nil), k.Signature...)},
	}
}

// SignedPreKeyFromProto reads a SignedPreKey.
func SignedPreKeyFromProto(p *e2eepb.SignedPreKey) *SignedPreKey {
	if p == nil {
		return nil
	}
	return &SignedPreKey{
		ID:        p.GetId().GetValue(),
		PublicKey: append([]byte(nil), p.GetPublicKey().GetValue()...),
		Signature: append([]byte(nil), p.GetSignature().GetValue()...),
	}
}

// OneTimePreKey is one of PQXDH's OPK_n: an unsigned Curve25519 prekey served
// at most once.
type OneTimePreKey struct {
	ID        uint32
	PublicKey []byte
}

// Clone returns a deep copy.
func (k *OneTimePreKey) Clone() *OneTimePreKey {
	if k == nil {
		return nil
	}
	return &OneTimePreKey{ID: k.ID, PublicKey: append([]byte(nil), k.PublicKey...)}
}

// ToProto projects the prekey.
func (k *OneTimePreKey) ToProto() *e2eepb.OneTimePreKey {
	return &e2eepb.OneTimePreKey{
		Id:        &e2eepb.PreKeyId{Value: k.ID},
		PublicKey: &e2eepb.EcPublicKey{Value: append([]byte(nil), k.PublicKey...)},
	}
}

// OneTimePreKeysFromProto reads a batch; nil for a nil batch.
func OneTimePreKeysFromProto(p *e2eepb.OneTimePreKeyBatch) []*OneTimePreKey {
	if p == nil {
		return nil
	}
	out := make([]*OneTimePreKey, 0, len(p.GetPrekeys()))
	for _, k := range p.GetPrekeys() {
		out = append(out, &OneTimePreKey{ID: k.GetId().GetValue(), PublicKey: append([]byte(nil), k.GetPublicKey().GetValue()...)})
	}
	return out
}

// KemSignedPreKey is a Kyber-1024 prekey with its id and the identity key's
// XEdDSA signature over the serialized public key. The same shape serves as
// the last-resort PQSPK and the one-time PQOPK_n; DeviceKeys keeps them
// apart.
type KemSignedPreKey struct {
	ID        uint32
	PublicKey []byte
	Signature []byte
}

// Clone returns a deep copy.
func (k *KemSignedPreKey) Clone() *KemSignedPreKey {
	if k == nil {
		return nil
	}
	return &KemSignedPreKey{ID: k.ID, PublicKey: append([]byte(nil), k.PublicKey...), Signature: append([]byte(nil), k.Signature...)}
}

// ToProto projects the prekey.
func (k *KemSignedPreKey) ToProto() *e2eepb.KemSignedPreKey {
	return &e2eepb.KemSignedPreKey{
		Id:        &e2eepb.PreKeyId{Value: k.ID},
		PublicKey: &e2eepb.KemPublicKey{Value: append([]byte(nil), k.PublicKey...)},
		Signature: &commonpb.Signature{Value: append([]byte(nil), k.Signature...)},
	}
}

// KemSignedPreKeyFromProto reads a KemSignedPreKey.
func KemSignedPreKeyFromProto(p *e2eepb.KemSignedPreKey) *KemSignedPreKey {
	if p == nil {
		return nil
	}
	return &KemSignedPreKey{
		ID:        p.GetId().GetValue(),
		PublicKey: append([]byte(nil), p.GetPublicKey().GetValue()...),
		Signature: append([]byte(nil), p.GetSignature().GetValue()...),
	}
}

// KemSignedPreKeysFromProto reads a batch; nil for a nil batch.
func KemSignedPreKeysFromProto(p *e2eepb.KemSignedPreKeyBatch) []*KemSignedPreKey {
	if p == nil {
		return nil
	}
	out := make([]*KemSignedPreKey, 0, len(p.GetPrekeys()))
	for _, k := range p.GetPrekeys() {
		out = append(out, KemSignedPreKeyFromProto(k))
	}
	return out
}

// DeviceKeys is the set of prekeys the server holds for a device: what PQXDH
// has a device publish beyond its identity key. A registration supplies all
// of it; a SetKeys replaces the parts it names.
type DeviceKeys struct {
	// SignedPreKey is SPK_B: exactly one, replaced on a cadence.
	SignedPreKey *SignedPreKey

	// KemLastResortPreKey is PQSPK_B: exactly one, never consumed.
	KemLastResortPreKey *KemSignedPreKey

	// OneTimePreKeys is the pool of OPK_Bn, each consumed by one bundle.
	OneTimePreKeys []*OneTimePreKey

	// KemOneTimePreKeys is the pool of PQOPK_Bn, each consumed by one bundle.
	// Its ids and KemLastResortPreKey's share one space.
	KemOneTimePreKeys []*KemSignedPreKey
}

// Clone returns a deep copy.
func (k *DeviceKeys) Clone() *DeviceKeys {
	out := &DeviceKeys{
		SignedPreKey:        k.SignedPreKey.Clone(),
		KemLastResortPreKey: k.KemLastResortPreKey.Clone(),
	}
	for _, p := range k.OneTimePreKeys {
		out.OneTimePreKeys = append(out.OneTimePreKeys, p.Clone())
	}
	for _, p := range k.KemOneTimePreKeys {
		out.KemOneTimePreKeys = append(out.KemOneTimePreKeys, p.Clone())
	}
	return out
}

// KeyUpdate is a SetKeys: each part replaces the device's when non-nil and
// leaves it alone when nil. A supplied pool replaces the whole pool, never
// adds to it; an empty non-nil pool clears it.
type KeyUpdate struct {
	SignedPreKey        *SignedPreKey
	KemLastResortPreKey *KemSignedPreKey
	OneTimePreKeys      []*OneTimePreKey
	KemOneTimePreKeys   []*KemSignedPreKey
}

// IsEmpty reports whether the update names nothing.
func (u KeyUpdate) IsEmpty() bool {
	return u.SignedPreKey == nil && u.KemLastResortPreKey == nil && u.OneTimePreKeys == nil && u.KemOneTimePreKeys == nil
}

// PreKeyCounts is how many one-time prekeys of each kind remain for a device.
type PreKeyCounts struct {
	OneTimePreKeys    int
	KemOneTimePreKeys int
}

// ToProto projects the counts.
func (c PreKeyCounts) ToProto() *e2eepb.PreKeyCounts {
	return &e2eepb.PreKeyCounts{
		OneTimePrekeys:    uint32(c.OneTimePreKeys),
		KemOneTimePrekeys: uint32(c.KemOneTimePreKeys),
	}
}

// KeyStatus is what a device's owner learns about the keys the server holds
// for it: the device record, the repeated-use keys as stored (what
// RepeatedUseDigest covers, so the owner can check them against its own),
// and the one-time counts.
type KeyStatus struct {
	Device              *Device
	SignedPreKey        *SignedPreKey
	KemLastResortPreKey *KemSignedPreKey
	Counts              PreKeyCounts
}

// PreKeyBundle is what a sender gets to start a session with a device: the
// device's fixed record plus its signed prekey, a KEM prekey (a one-time one
// if any remained, else the last-resort one) and, if any remained, a one-time
// curve prekey.
type PreKeyBundle struct {
	Device        *Device
	SignedPreKey  *SignedPreKey
	KemPreKey     *KemSignedPreKey
	OneTimePreKey *OneTimePreKey
}

// ToProto projects the bundle.
func (b *PreKeyBundle) ToProto() *e2eepb.PreKeyBundle {
	out := &e2eepb.PreKeyBundle{
		Address:              b.Device.Address.ToProto(),
		RegistrationId:       &e2eepb.RegistrationId{Value: b.Device.RegistrationID},
		IdentityKey:          &e2eepb.EcPublicKey{Value: append([]byte(nil), b.Device.IdentityKey...)},
		IdentityKeySignature: &commonpb.Signature{Value: append([]byte(nil), b.Device.IdentityKeySignature...)},
		SignedPrekey:         b.SignedPreKey.ToProto(),
		KemPrekey:            b.KemPreKey.ToProto(),
		AccountKey:           &commonpb.PublicKey{Value: append([]byte(nil), b.Device.AccountKey...)},
		Capabilities:         append([]e2eepb.Device_Capability(nil), b.Device.Capabilities...),
	}
	if b.OneTimePreKey != nil {
		out.OneTimePrekey = b.OneTimePreKey.ToProto()
	}
	return out
}

// DeliveryReceipt is the server telling a sender's devices that recipient
// devices acknowledged sends of theirs: per acknowledging device, the
// client_ts of every send it acknowledged. The chat is the enclosing
// envelope's.
type DeliveryReceipt struct {
	Acknowledgements []Acknowledgement
}

// Acknowledgement is one device's acknowledgement of one or more sends.
type Acknowledgement struct {
	Recipient DeviceAddress
	ClientTS  []time.Time
}

// Clone returns a deep copy.
func (r *DeliveryReceipt) Clone() *DeliveryReceipt {
	if r == nil {
		return nil
	}
	out := &DeliveryReceipt{}
	for _, a := range r.Acknowledgements {
		out.Acknowledgements = append(out.Acknowledgements, Acknowledgement{
			Recipient: a.Recipient.Clone(),
			ClientTS:  append([]time.Time(nil), a.ClientTS...),
		})
	}
	return out
}

// ToProto projects the receipt.
func (r *DeliveryReceipt) ToProto() *e2eepb.DeliveryReceipt {
	out := &e2eepb.DeliveryReceipt{}
	for _, a := range r.Acknowledgements {
		ack := &e2eepb.DeliveryReceipt_Acknowledgement{Recipient: a.Recipient.ToProto()}
		for _, ts := range a.ClientTS {
			ack.ClientTs = append(ack.ClientTs, timestamppb.New(ts))
		}
		out.Acknowledgements = append(out.Acknowledgements, ack)
	}
	return out
}

// LastSeenOffset is how far a user's days are shifted from UTC days for
// the purpose of last_seen: a value in [0, 24h) fixed by the user's ID.
// Every device records its day once, at its first activity in it, so
// without an offset a fleet's daily writes would all fall in the minutes
// after 00:00 UTC; with one they are spread over the day. Signal-Server
// offsets each account's day the same way, by its ID.
func LastSeenOffset(userID *commonpb.UserId) time.Duration {
	h := fnv.New64a()
	h.Write(userID.GetValue())
	return time.Duration(h.Sum64()%uint64(24*time.Hour/time.Second)) * time.Second
}

// LastSeenDay is the day a user's device is recorded as seen on when it is
// active at now: the start of the UTC day that now, shifted back by
// LastSeenOffset, falls in. It is what Device.LastSeen holds, and what a
// device idle for a while is measured against. A device's day can trail
// the UTC calendar by up to the offset, never more than a day, which a
// liveness signal read in days does not mind.
func LastSeenDay(userID *commonpb.UserId, now time.Time) time.Time {
	return now.Add(-LastSeenOffset(userID)).UTC().Truncate(24 * time.Hour)
}

// NewEnvelopeID mints the EnvelopeId of the envelope at sequence in a
// mailbox: the sequence, big-endian, followed by random bytes. The id is
// opaque to clients, but a store that keys a mailbox by sequence can read
// the position back with EnvelopeSequence and acknowledge by a keyed
// delete, with the random tail as the condition that it is the same
// envelope.
func NewEnvelopeID(sequence uint64) []byte {
	id := make([]byte, EnvelopeIDSize)
	binary.BigEndian.PutUint64(id[:8], sequence)
	if _, err := rand.Read(id[8:]); err != nil {
		panic(err)
	}
	return id
}

// EnvelopeSequence reads the mailbox position out of an id made by
// NewEnvelopeID. ok is false for an id of the wrong length.
func EnvelopeSequence(id []byte) (sequence uint64, ok bool) {
	if len(id) != EnvelopeIDSize {
		return 0, false
	}
	return binary.BigEndian.Uint64(id[:8]), true
}

// Envelope is one item in one device's mailbox: bytes the server relays and
// the routing metadata it is allowed to see. The proto's Envelope is the
// client's view; this holds it plus what the server keeps for itself.
type Envelope struct {
	// ID is the server-assigned identity the recipient acknowledges by.
	// Assigned by the store on delivery (NewEnvelopeID), with Sequence.
	ID []byte

	// Recipient is the mailbox the envelope is in.
	Recipient DeviceAddress

	// Sequence is the envelope's position in the mailbox, from 1: strictly
	// increasing, never reused, with gaps where a delivery reserved a
	// position and failed, or an ephemeral envelope lapsed. Assigned by the
	// store on delivery; 0 before then.
	Sequence uint64

	ChatID *commonpb.ChatId

	// Source is the sending device; nil on a server-generated receipt.
	Source *DeviceAddress

	Type        e2eepb.Envelope_Type
	Content     []byte
	ClientTS    *time.Time
	ServerTS    time.Time
	LowPriority bool
	ContentHint e2eepb.Envelope_ContentHint

	// DeliveryReceipt is set only when Type is SERVER_DELIVERY_RECEIPT.
	DeliveryReceipt *DeliveryReceipt

	// SourceDevice is the server's record of the source device at the time of
	// the send, stamped only on a PREKEY_MESSAGE so the recipient can verify
	// the session it opens without a call. LastSeen and AppInstall are never
	// carried; the handler clears them before the store sees the envelope.
	SourceDevice *Device

	// WantsDeliveryReceipt records that the sender did not set
	// no_delivery_receipt: acknowledging the envelope earns the source a
	// DeliveryReceipt. Server-only; never projected.
	WantsDeliveryReceipt bool

	// Ephemeral marks an `online` send: held for a short window and lapsed
	// if not drained within it (see ExpiresAt). Projected, so the client
	// knows not to persist it.
	Ephemeral bool

	// ExpiresAt is when the store may forget the envelope unacknowledged:
	// the retention for an ordinary send, the ephemeral window for an
	// online one. Set by the handler; a store returns nothing past it.
	// Zero means never. Server-only; never projected.
	ExpiresAt time.Time
}

// Clone returns a deep copy.
func (e *Envelope) Clone() *Envelope {
	out := &Envelope{
		ID:                   append([]byte(nil), e.ID...),
		Recipient:            e.Recipient.Clone(),
		Sequence:             e.Sequence,
		ChatID:               cloneChatID(e.ChatID),
		Type:                 e.Type,
		Content:              append([]byte(nil), e.Content...),
		ServerTS:             e.ServerTS,
		LowPriority:          e.LowPriority,
		ContentHint:          e.ContentHint,
		DeliveryReceipt:      e.DeliveryReceipt.Clone(),
		WantsDeliveryReceipt: e.WantsDeliveryReceipt,
		Ephemeral:            e.Ephemeral,
		ExpiresAt:            e.ExpiresAt,
	}
	if e.Source != nil {
		s := e.Source.Clone()
		out.Source = &s
	}
	if e.ClientTS != nil {
		t := *e.ClientTS
		out.ClientTS = &t
	}
	if e.SourceDevice != nil {
		out.SourceDevice = e.SourceDevice.Clone()
	}
	return out
}

// ToProto projects the envelope as its recipient sees it.
func (e *Envelope) ToProto() *e2eepb.Envelope {
	out := &e2eepb.Envelope{
		Id:          &e2eepb.EnvelopeId{Value: append([]byte(nil), e.ID...)},
		Sequence:    e.Sequence,
		ChatId:      cloneChatID(e.ChatID),
		Type:        e.Type,
		Content:     append([]byte(nil), e.Content...),
		ServerTs:    timestamppb.New(e.ServerTS),
		LowPriority: e.LowPriority,
		ContentHint: e.ContentHint,
		Ephemeral:   e.Ephemeral,
	}
	if e.Source != nil {
		out.Source = e.Source.ToProto()
	}
	if e.ClientTS != nil {
		out.ClientTs = timestamppb.New(*e.ClientTS)
	}
	if e.DeliveryReceipt != nil {
		out.DeliveryReceipt = e.DeliveryReceipt.ToProto()
	}
	if e.SourceDevice != nil {
		out.SourceDevice = e.SourceDevice.ToProto(false)
	}
	return out
}

func cloneUserID(id *commonpb.UserId) *commonpb.UserId {
	if id == nil {
		return nil
	}
	return &commonpb.UserId{Value: append([]byte(nil), id.Value...)}
}

func cloneChatID(id *commonpb.ChatId) *commonpb.ChatId {
	if id == nil {
		return nil
	}
	return &commonpb.ChatId{Value: append([]byte(nil), id.Value...)}
}

// HasDuplicateKemID reports whether two KEM prekeys would share an id:
// within the pool, or between the pool and the last-resort key when both
// are given. KEM ids are one space per device across both roles (see
// PreKeyId in the proto). Collisions against keys a store already holds
// are the store's to find.
func HasDuplicateKemID(lastResort *KemSignedPreKey, pool []*KemSignedPreKey) bool {
	seen := make(map[uint32]struct{}, len(pool)+1)
	if lastResort != nil {
		seen[lastResort.ID] = struct{}{}
	}
	for _, k := range pool {
		if _, dup := seen[k.ID]; dup {
			return true
		}
		seen[k.ID] = struct{}{}
	}
	return false
}

// HasDuplicateOneTimeID reports whether two one-time curve prekeys of the
// pool share an id. The pool is its own space: a curve id may equal the
// signed prekey's or a KEM key's. A pool is stored one item per id, so a
// duplicate is not a policy refusal alone; it is two keys under one key.
func HasDuplicateOneTimeID(pool []*OneTimePreKey) bool {
	seen := make(map[uint32]struct{}, len(pool))
	for _, k := range pool {
		if _, dup := seen[k.ID]; dup {
			return true
		}
		seen[k.ID] = struct{}{}
	}
	return false
}

// MergeCapabilities returns the union of have and add, sorted ascending and
// without duplicates, and whether it differs from have: the set a device
// declares after Store.AddCapabilities. Capabilities only grow; a build
// that has lost one registers anew.
func MergeCapabilities(have, add []e2eepb.Device_Capability) (merged []e2eepb.Device_Capability, changed bool) {
	set := make(map[e2eepb.Device_Capability]struct{}, len(have)+len(add))
	for _, c := range have {
		set[c] = struct{}{}
	}
	for _, c := range add {
		if _, ok := set[c]; !ok {
			set[c] = struct{}{}
			changed = true
		}
	}
	merged = make([]e2eepb.Device_Capability, 0, len(set))
	for c := range set {
		merged = append(merged, c)
	}
	slices.Sort(merged)
	return merged, changed
}

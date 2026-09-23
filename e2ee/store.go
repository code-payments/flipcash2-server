package e2ee

import (
	"context"
	"time"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	e2eepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/e2ee/v1"
)

// Store persists devices, their prekeys, per-device mailboxes, and the
// rate-limit buckets the handlers take from.
//
// Every write is atomic per device: a registration either assigns an id
// and stores every key or does nothing, a key update applies wholly or not
// at all, a bundle read consumes at most one prekey of each kind and returns
// exactly what it consumed. Delivery is not atomic across envelopes: each
// lands on its own once the call's checks pass, as Signal-Server inserts
// per device queue, and a call that fails partway may have stored some. The
// caller's retry stores the whole call again and the recipient absorbs the
// second copy, which SendEnvelopes' at-least-once contract already asks of
// it. The DynamoDB layout is described in e2ee/dynamodb/table.go.
type Store interface {
	// RegisterDevice creates a device for the user with the lowest free
	// DeviceId in [MinDeviceID, MaxDeviceID], storing device's fixed record
	// (Address.DeviceID is ignored on input and assigned) and keys, and
	// remembers idempotencyKey for the user so that a retry returns the
	// device the first attempt registered, with created false, whatever the
	// retry's parameters. A retry after the device was unregistered
	// registers anew.
	//
	// It returns ErrTooManyDevices when the user already holds
	// MaxDevicesPerUser, ErrDuplicateIdentityKey when device.IdentityKey is
	// registered to another of the user's devices, and ErrDuplicatePreKeyID
	// when two KEM prekeys in keys share an id.
	RegisterDevice(ctx context.Context, idempotencyKey *chatpb.IdempotencyKey, device *Device, keys *DeviceKeys) (registered *Device, created bool, err error)

	// UnregisterDevice removes the device, its keys and its mailbox, freeing
	// its id. ErrDeviceNotFound if the user has no such device.
	UnregisterDevice(ctx context.Context, address DeviceAddress) error

	// GetDevice returns one device. ErrDeviceNotFound if the user has no such
	// device.
	GetDevice(ctx context.Context, address DeviceAddress) (*Device, error)

	// GetDevices returns a user's current devices in ascending DeviceId
	// order; empty, not an error, for a user with none.
	GetDevices(ctx context.Context, userID *commonpb.UserId) ([]*Device, error)

	// TouchLastSeen records that the device was active on day, a UTC day
	// start (the caller's reckoning: LastSeenDay), unless it already
	// records that day or a later one: a day never moves backwards, and a
	// day already recorded costs no write. Signal records last-seen the
	// same way, day-truncated and at most once a day. ErrDeviceNotFound if
	// the user has no such device.
	TouchLastSeen(ctx context.Context, address DeviceAddress, day time.Time) error

	// AddCapabilities adds caps to the device's capabilities (the union,
	// sorted; see MergeCapabilities) and returns the device afterwards.
	// Additive only: nothing is ever removed, and adding what is already
	// declared writes nothing. ErrDeviceNotFound if the user has no such
	// device.
	AddCapabilities(ctx context.Context, address DeviceAddress, caps []e2eepb.Device_Capability) (*Device, error)

	// SetKeys applies update to the device's keys (see KeyUpdate) and returns
	// the key status afterwards. It returns ErrDuplicatePreKeyID, and
	// applies nothing, when the resulting KEM prekeys (last-resort and
	// one-time together, as they stand after the update) would share an id.
	// ErrDeviceNotFound if the user has no such device.
	SetKeys(ctx context.Context, address DeviceAddress, update KeyUpdate) (KeyStatus, error)

	// GetKeyStatus returns the device's record, repeated-use keys and
	// one-time counts. ErrDeviceNotFound if the user has no such device.
	GetKeyStatus(ctx context.Context, address DeviceAddress) (KeyStatus, error)

	// TakePreKeyBundle returns a bundle for the device, consuming one one-time
	// curve prekey and one one-time KEM prekey where any remain, each pool
	// served in upload order and each key at most once, so no two calls
	// return the same one. When no one-time KEM prekey remains the bundle
	// carries the last-resort one; when no one-time curve prekey remains the
	// bundle carries none. A take that straddles a SetKeys replacement of a
	// pool serves a key of either the replaced pool or the new one (the
	// device holds both for a time), and the new pool is charged only for
	// what was served from it. ErrDeviceNotFound if the user has no such
	// device.
	TakePreKeyBundle(ctx context.Context, address DeviceAddress) (*PreKeyBundle, error)

	// Deliver appends each envelope to its recipient's mailbox, assigning
	// Sequence (strictly increasing per mailbox, never reused, gaps
	// allowed) and ID (NewEnvelopeID of it), and returning the stored
	// envelopes in input order. Nothing is deduplicated: a caller retrying
	// a send stores a second copy at a later position, which the recipient
	// absorbs. An envelope's ExpiresAt is its retention: GetEnvelopes never
	// returns one past it, and the store forgets it on its own after.
	//
	// Every recipient must be a current device (ErrDeviceNotFound
	// otherwise, before anything is written). Past that check nothing is
	// all or nothing: envelopes are written independently, an error may
	// leave some of them stored, and positions a failed call reserved stay
	// unfilled. A recipient that is unregistered later loses its mailbox
	// with it.
	Deliver(ctx context.Context, envelopes []*Envelope) ([]*Envelope, error)

	// GetEnvelopes returns the device's unacknowledged, unexpired envelopes
	// with Sequence greater than afterSequence, ascending, at most limit
	// (limit <= 0 means unbounded) and, by content bytes, at most maxBytes
	// (maxBytes <= 0 means unbounded): the page stops after the envelope
	// that takes its total content past maxBytes, that envelope included,
	// so a caller can tell a page cut short by bytes (its total exceeds
	// maxBytes) from the end of the mailbox, and a single envelope larger
	// than the budget is still returned. Bytes are counted on Content
	// alone; the caller's budget allows for the rest of an envelope.
	// ErrDeviceNotFound if the user has no such device.
	GetEnvelopes(ctx context.Context, address DeviceAddress, afterSequence uint64, limit int, maxBytes int64) ([]*Envelope, error)

	// TakeTokens takes cost from the limit kind names for key as of now,
	// creating it empty on first sight, and returns 0 when it fit. When
	// it does not fit it takes nothing and returns how long until it
	// would (BucketConfig.Decide over the key's WindowCounts); a cost
	// above the size is never served. now is the caller's clock, so a
	// take is decided against the time it is made for. Limit state is
	// transient: a key whose windows have both passed may be forgotten.
	TakeTokens(ctx context.Context, kind, key string, cost int64, cfg BucketConfig, now time.Time) (time.Duration, error)

	// AckEnvelopes removes the named envelopes from the device's mailbox and
	// returns the ones that were there, so the caller can issue delivery
	// receipts for them. Ids that are not in the mailbox (already
	// acknowledged, or never there) are ignored. ErrDeviceNotFound if the
	// user has no such device.
	AckEnvelopes(ctx context.Context, address DeviceAddress, ids [][]byte) ([]*Envelope, error)
}

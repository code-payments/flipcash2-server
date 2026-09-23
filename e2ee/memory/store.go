package memory

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	e2eepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/e2ee/v1"

	"github.com/code-payments/flipcash2-server/e2ee"
)

type memory struct {
	sync.Mutex

	// users maps a user ID to their devices and registration keys.
	users map[string]*userState

	// limits maps a rate-limit kind and key to its window counts.
	limits map[string]e2ee.WindowCounts
}

type userState struct {
	// devices by DeviceId.
	devices map[uint32]*deviceState

	// idempotency maps a RegisterDevice key to the DeviceId it registered,
	// while that device exists.
	idempotency map[string]uint32
}

type deviceState struct {
	device *e2ee.Device
	keys   *e2ee.DeviceKeys

	// mailbox holds the unacknowledged envelopes in ascending Sequence.
	mailbox []*e2ee.Envelope

	// nextSequence is the Sequence the next delivered envelope takes: never
	// reused, so it survives acknowledgements.
	nextSequence uint64
}

// NewInMemory returns an in-memory e2ee.Store, for tests.
func NewInMemory() e2ee.Store {
	return &memory{
		users:  make(map[string]*userState),
		limits: make(map[string]e2ee.WindowCounts),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.users = make(map[string]*userState)
	m.limits = make(map[string]e2ee.WindowCounts)
}

// user returns the user's state, creating it when create is set.
func (m *memory) user(userID *commonpb.UserId, create bool) *userState {
	key := string(userID.GetValue())
	u, ok := m.users[key]
	if !ok && create {
		u = &userState{
			devices:     make(map[uint32]*deviceState),
			idempotency: make(map[string]uint32),
		}
		m.users[key] = u
	}
	return u
}

// device returns the addressed device's state, or nil.
func (m *memory) device(address e2ee.DeviceAddress) *deviceState {
	u := m.user(address.UserID, false)
	if u == nil {
		return nil
	}
	return u.devices[address.DeviceID]
}

func (m *memory) RegisterDevice(_ context.Context, idempotencyKey *chatpb.IdempotencyKey, device *e2ee.Device, keys *e2ee.DeviceKeys) (*e2ee.Device, bool, error) {
	m.Lock()
	defer m.Unlock()

	u := m.user(device.Address.UserID, true)

	if id, ok := u.idempotency[string(idempotencyKey.GetValue())]; ok {
		if d, ok := u.devices[id]; ok {
			return d.device.Clone(), false, nil
		}
		// The device it named is gone; the key no longer binds.
		delete(u.idempotency, string(idempotencyKey.GetValue()))
	}

	if len(u.devices) >= e2ee.MaxDevicesPerUser {
		return nil, false, e2ee.ErrTooManyDevices
	}
	for _, d := range u.devices {
		if bytes.Equal(d.device.IdentityKey, device.IdentityKey) {
			return nil, false, e2ee.ErrDuplicateIdentityKey
		}
	}
	if e2ee.HasDuplicateKemID(keys.KemLastResortPreKey, keys.KemOneTimePreKeys) || e2ee.HasDuplicateOneTimeID(keys.OneTimePreKeys) {
		return nil, false, e2ee.ErrDuplicatePreKeyID
	}

	var id uint32
	for candidate := uint32(e2ee.MinDeviceID); candidate <= e2ee.MaxDeviceID; candidate++ {
		if _, taken := u.devices[candidate]; !taken {
			id = candidate
			break
		}
	}
	if id == 0 {
		return nil, false, e2ee.ErrTooManyDevices
	}

	registered := device.Clone()
	registered.Address.DeviceID = id
	u.devices[id] = &deviceState{
		device:       registered,
		keys:         keys.Clone(),
		nextSequence: 1,
	}
	u.idempotency[string(idempotencyKey.GetValue())] = id

	return registered.Clone(), true, nil
}

func (m *memory) UnregisterDevice(_ context.Context, address e2ee.DeviceAddress) error {
	m.Lock()
	defer m.Unlock()

	u := m.user(address.UserID, false)
	if u == nil {
		return e2ee.ErrDeviceNotFound
	}
	if _, ok := u.devices[address.DeviceID]; !ok {
		return e2ee.ErrDeviceNotFound
	}
	delete(u.devices, address.DeviceID)
	for key, id := range u.idempotency {
		if id == address.DeviceID {
			delete(u.idempotency, key)
		}
	}
	return nil
}

func (m *memory) GetDevice(_ context.Context, address e2ee.DeviceAddress) (*e2ee.Device, error) {
	m.Lock()
	defer m.Unlock()

	d := m.device(address)
	if d == nil {
		return nil, e2ee.ErrDeviceNotFound
	}
	return d.device.Clone(), nil
}

func (m *memory) GetDevices(_ context.Context, userID *commonpb.UserId) ([]*e2ee.Device, error) {
	m.Lock()
	defer m.Unlock()

	u := m.user(userID, false)
	if u == nil {
		return nil, nil
	}
	var out []*e2ee.Device
	for _, d := range u.devices {
		out = append(out, d.device.Clone())
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Address.DeviceID < out[j].Address.DeviceID
	})
	return out, nil
}

func (m *memory) TouchLastSeen(_ context.Context, address e2ee.DeviceAddress, day time.Time) error {
	m.Lock()
	defer m.Unlock()

	d := m.device(address)
	if d == nil {
		return e2ee.ErrDeviceNotFound
	}
	day = day.UTC().Truncate(24 * time.Hour)
	if d.device.LastSeen == nil || d.device.LastSeen.Before(day) {
		d.device.LastSeen = &day
	}
	return nil
}

func (m *memory) AddCapabilities(_ context.Context, address e2ee.DeviceAddress, caps []e2eepb.Device_Capability) (*e2ee.Device, error) {
	m.Lock()
	defer m.Unlock()

	d := m.device(address)
	if d == nil {
		return nil, e2ee.ErrDeviceNotFound
	}
	if merged, changed := e2ee.MergeCapabilities(d.device.Capabilities, caps); changed {
		d.device.Capabilities = merged
	}
	return d.device.Clone(), nil
}

func (m *memory) SetKeys(_ context.Context, address e2ee.DeviceAddress, update e2ee.KeyUpdate) (e2ee.KeyStatus, error) {
	m.Lock()
	defer m.Unlock()

	d := m.device(address)
	if d == nil {
		return e2ee.KeyStatus{}, e2ee.ErrDeviceNotFound
	}

	// Compose the resulting key set, check it, then swap it in whole.
	next := d.keys.Clone()
	if update.SignedPreKey != nil {
		next.SignedPreKey = update.SignedPreKey.Clone()
	}
	if update.KemLastResortPreKey != nil {
		next.KemLastResortPreKey = update.KemLastResortPreKey.Clone()
	}
	if update.OneTimePreKeys != nil {
		next.OneTimePreKeys = nil
		for _, k := range update.OneTimePreKeys {
			next.OneTimePreKeys = append(next.OneTimePreKeys, k.Clone())
		}
	}
	if update.KemOneTimePreKeys != nil {
		next.KemOneTimePreKeys = nil
		for _, k := range update.KemOneTimePreKeys {
			next.KemOneTimePreKeys = append(next.KemOneTimePreKeys, k.Clone())
		}
	}
	if e2ee.HasDuplicateKemID(next.KemLastResortPreKey, next.KemOneTimePreKeys) || e2ee.HasDuplicateOneTimeID(next.OneTimePreKeys) {
		return e2ee.KeyStatus{}, e2ee.ErrDuplicatePreKeyID
	}

	d.keys = next
	return status(d), nil
}

func (m *memory) GetKeyStatus(_ context.Context, address e2ee.DeviceAddress) (e2ee.KeyStatus, error) {
	m.Lock()
	defer m.Unlock()

	d := m.device(address)
	if d == nil {
		return e2ee.KeyStatus{}, e2ee.ErrDeviceNotFound
	}
	return status(d), nil
}

func (m *memory) TakePreKeyBundle(_ context.Context, address e2ee.DeviceAddress) (*e2ee.PreKeyBundle, error) {
	m.Lock()
	defer m.Unlock()

	d := m.device(address)
	if d == nil {
		return nil, e2ee.ErrDeviceNotFound
	}

	bundle := &e2ee.PreKeyBundle{
		Device:       d.device.Clone(),
		SignedPreKey: d.keys.SignedPreKey.Clone(),
		KemPreKey:    d.keys.KemLastResortPreKey.Clone(),
	}
	if n := len(d.keys.KemOneTimePreKeys); n > 0 {
		bundle.KemPreKey = d.keys.KemOneTimePreKeys[0].Clone()
		d.keys.KemOneTimePreKeys = d.keys.KemOneTimePreKeys[1:]
	}
	if n := len(d.keys.OneTimePreKeys); n > 0 {
		bundle.OneTimePreKey = d.keys.OneTimePreKeys[0].Clone()
		d.keys.OneTimePreKeys = d.keys.OneTimePreKeys[1:]
	}
	return bundle, nil
}

func (m *memory) Deliver(_ context.Context, envelopes []*e2ee.Envelope) ([]*e2ee.Envelope, error) {
	m.Lock()
	defer m.Unlock()

	// Every recipient is resolved before anything is appended, so an
	// unknown device stores nothing. (The appends then land together under
	// the lock, which is stricter than the contract asks; nothing may rely
	// on it.)
	targets := make([]*deviceState, len(envelopes))
	for i, e := range envelopes {
		d := m.device(e.Recipient)
		if d == nil {
			return nil, e2ee.ErrDeviceNotFound
		}
		targets[i] = d
	}

	out := make([]*e2ee.Envelope, len(envelopes))
	for i, e := range envelopes {
		stored := e.Clone()
		stored.Sequence = targets[i].nextSequence
		stored.ID = e2ee.NewEnvelopeID(stored.Sequence)
		targets[i].nextSequence++
		targets[i].mailbox = append(targets[i].mailbox, stored)
		out[i] = stored.Clone()
	}
	return out, nil
}

func (m *memory) GetEnvelopes(_ context.Context, address e2ee.DeviceAddress, afterSequence uint64, limit int, maxBytes int64) ([]*e2ee.Envelope, error) {
	m.Lock()
	defer m.Unlock()

	d := m.device(address)
	if d == nil {
		return nil, e2ee.ErrDeviceNotFound
	}

	now := time.Now()
	var out []*e2ee.Envelope
	var bytes int64
	for _, e := range d.mailbox {
		if e.Sequence <= afterSequence {
			continue
		}
		if !e.ExpiresAt.IsZero() && !e.ExpiresAt.After(now) {
			continue
		}
		if limit > 0 && len(out) >= limit {
			break
		}
		out = append(out, e.Clone())
		if bytes += int64(len(e.Content)); maxBytes > 0 && bytes > maxBytes {
			break // The envelope that crossed the budget is the page's last.
		}
	}
	return out, nil
}

func (m *memory) AckEnvelopes(_ context.Context, address e2ee.DeviceAddress, ids [][]byte) ([]*e2ee.Envelope, error) {
	m.Lock()
	defer m.Unlock()

	d := m.device(address)
	if d == nil {
		return nil, e2ee.ErrDeviceNotFound
	}

	acked := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		acked[string(id)] = struct{}{}
	}

	var removed, kept []*e2ee.Envelope
	for _, e := range d.mailbox {
		if _, ok := acked[string(e.ID)]; ok {
			removed = append(removed, e.Clone())
		} else {
			kept = append(kept, e)
		}
	}
	d.mailbox = kept
	return removed, nil
}

func (m *memory) TakeTokens(_ context.Context, kind, key string, cost int64, cfg e2ee.BucketConfig, now time.Time) (time.Duration, error) {
	m.Lock()
	defer m.Unlock()

	w := m.limits[kind+"#"+key].At(cfg, now)
	ok, retry := cfg.Decide(w, now, cost)
	if !ok {
		return retry, nil
	}
	w.Count += cost
	m.limits[kind+"#"+key] = w
	return 0, nil
}

func status(d *deviceState) e2ee.KeyStatus {
	return e2ee.KeyStatus{
		Device:              d.device.Clone(),
		SignedPreKey:        d.keys.SignedPreKey.Clone(),
		KemLastResortPreKey: d.keys.KemLastResortPreKey.Clone(),
		Counts: e2ee.PreKeyCounts{
			OneTimePreKeys:    len(d.keys.OneTimePreKeys),
			KemOneTimePreKeys: len(d.keys.KemOneTimePreKeys),
		},
	}
}

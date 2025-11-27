package memory

import (
	"bytes"
	"context"
	"sync"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"

	"google.golang.org/protobuf/proto"
)

type memory struct {
	sync.Mutex

	// maps a userID to a slice of public keys associated with that user (both are stored as strings).
	users map[string][]string

	// maps a publicKey (string representation) to a userID (also stored as a string). This allows quick lookups of the user by their public key.
	keys map[string]string

	// set of registered users
	registeredUsers map[string]any
}

func NewInMemory() account.Store {
	return &memory{
		users:           make(map[string][]string),
		keys:            make(map[string]string),
		registeredUsers: make(map[string]any),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.users = make(map[string][]string)
	m.keys = make(map[string]string)
}

func (m *memory) Bind(_ context.Context, userID *commonpb.UserId, pubKey *commonpb.PublicKey) (*commonpb.UserId, error) {
	m.Lock()
	defer m.Unlock()

	if prev, ok := m.keys[string(pubKey.Value)]; ok {
		return &commonpb.UserId{Value: []byte(prev)}, nil
	}

	keys := m.users[string(userID.Value)]
	if len(keys) > 0 {
		return nil, account.ErrManyPublicKeys
	}
	keys = append(keys, string(pubKey.Value))
	m.users[string(userID.Value)] = keys

	m.keys[string(pubKey.Value)] = string(userID.Value)

	return proto.Clone(userID).(*commonpb.UserId), nil
}

func (m *memory) GetUserId(_ context.Context, pubKey *commonpb.PublicKey) (*commonpb.UserId, error) {
	m.Lock()
	defer m.Unlock()

	userID, ok := m.keys[string(pubKey.Value)]
	if !ok {
		return nil, account.ErrNotFound
	}

	return &commonpb.UserId{Value: []byte(userID)}, nil
}

func (m *memory) GetPubKeys(_ context.Context, userID *commonpb.UserId) ([]*commonpb.PublicKey, error) {
	m.Lock()
	defer m.Unlock()

	var keys []*commonpb.PublicKey
	for _, key := range m.users[string(userID.Value)] {
		keys = append(keys, &commonpb.PublicKey{Value: []byte(key)})
	}

	return keys, nil
}

func (m *memory) IsAuthorized(_ context.Context, userID *commonpb.UserId, pubKey *commonpb.PublicKey) (bool, error) {
	m.Lock()
	defer m.Unlock()

	keys, ok := m.users[string(userID.Value)]
	if !ok {
		return false, nil
	}

	for _, key := range keys {
		if bytes.Equal([]byte(key), pubKey.Value) {
			return true, nil
		}
	}

	return false, nil
}

func (m *memory) IsStaff(ctx context.Context, userID *commonpb.UserId) (bool, error) {
	return false, nil
}

func (m *memory) IsRegistered(ctx context.Context, userID *commonpb.UserId) (bool, error) {
	m.Lock()
	defer m.Unlock()

	_, ok := m.registeredUsers[string(userID.Value)]
	return ok, nil
}

func (m *memory) SetRegistrationFlag(ctx context.Context, userID *commonpb.UserId, isRegistered bool) error {
	m.Lock()
	defer m.Unlock()

	_, ok := m.users[string(userID.Value)]
	if !ok {
		return account.ErrNotFound
	}

	if isRegistered {
		m.registeredUsers[string(userID.Value)] = true
	} else {
		delete(m.registeredUsers, string(userID.Value))
	}
	return nil
}

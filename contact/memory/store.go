package memory

import (
	"bytes"
	"context"
	"sync"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/contact"
)

type userState struct {
	checksum []byte
	hashes   map[string]struct{}
}

type memory struct {
	sync.Mutex

	users map[string]*userState
}

func NewInMemory() contact.Store {
	return &memory{
		users: make(map[string]*userState),
	}
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.users = make(map[string]*userState)
}

func (m *memory) GetChecksum(_ context.Context, userID *commonpb.UserId) (*commonpb.Hash, error) {
	m.Lock()
	defer m.Unlock()

	state, ok := m.users[string(userID.Value)]
	if !ok {
		return nil, contact.ErrNotFound
	}
	return &commonpb.Hash{Value: append([]byte(nil), state.checksum...)}, nil
}

func (m *memory) GetHashes(_ context.Context, userID *commonpb.UserId) ([]*commonpb.Hash, error) {
	m.Lock()
	defer m.Unlock()

	state, ok := m.users[string(userID.Value)]
	if !ok {
		return nil, contact.ErrNotFound
	}

	out := make([]*commonpb.Hash, 0, len(state.hashes))
	for h := range state.hashes {
		out = append(out, &commonpb.Hash{Value: []byte(h)})
	}
	return out, nil
}

func (m *memory) GetUserIdsByPhoneHash(_ context.Context, phoneNumberHash *commonpb.Hash) ([]*commonpb.UserId, error) {
	m.Lock()
	defer m.Unlock()

	target := string(phoneNumberHash.Value)
	var out []*commonpb.UserId
	for userID, state := range m.users {
		if _, ok := state.hashes[target]; ok {
			out = append(out, &commonpb.UserId{Value: []byte(userID)})
		}
	}
	return out, nil
}

func (m *memory) IsContact(_ context.Context, userID *commonpb.UserId, phoneNumberHash *commonpb.Hash) (bool, error) {
	m.Lock()
	defer m.Unlock()

	state, ok := m.users[string(userID.Value)]
	if !ok {
		return false, nil
	}
	_, ok = state.hashes[string(phoneNumberHash.Value)]
	return ok, nil
}

func (m *memory) ApplyDelta(
	_ context.Context,
	userID *commonpb.UserId,
	addHashes []*commonpb.Hash,
	removeHashes []*commonpb.Hash,
	oldChecksum *commonpb.Hash,
	newChecksum *commonpb.Hash,
) error {
	m.Lock()
	defer m.Unlock()

	state, ok := m.users[string(userID.Value)]
	if !ok {
		state = &userState{
			checksum: zeroChecksum(),
			hashes:   make(map[string]struct{}),
		}
	}

	switch {
	case bytes.Equal(state.checksum, newChecksum.Value):
		return nil // Idempotent retry.
	case !bytes.Equal(state.checksum, oldChecksum.Value):
		return contact.ErrChecksumDrift
	}

	projected := make(map[string]struct{}, len(state.hashes))
	for h := range state.hashes {
		projected[h] = struct{}{}
	}
	for _, h := range removeHashes {
		delete(projected, string(h.Value))
	}
	for _, h := range addHashes {
		projected[string(h.Value)] = struct{}{}
	}

	if len(projected) > contact.MaxContactsPerUser {
		return contact.ErrTooManyContacts
	}

	state.hashes = projected
	state.checksum = append([]byte(nil), newChecksum.Value...)
	m.users[string(userID.Value)] = state
	return nil
}

func (m *memory) Replace(
	_ context.Context,
	userID *commonpb.UserId,
	hashes []*commonpb.Hash,
	expectedChecksum *commonpb.Hash,
) error {
	if len(hashes) > contact.MaxContactsPerUser {
		return contact.ErrTooManyContacts
	}

	m.Lock()
	defer m.Unlock()

	set := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		set[string(h.Value)] = struct{}{}
	}

	m.users[string(userID.Value)] = &userState{
		checksum: append([]byte(nil), expectedChecksum.Value...),
		hashes:   set,
	}
	return nil
}

func zeroChecksum() []byte {
	return make([]byte, contact.ChecksumSize)
}

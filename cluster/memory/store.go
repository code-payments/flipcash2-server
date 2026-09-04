package memory

import (
	"context"
	"encoding/hex"
	"sync"

	"github.com/code-payments/flipcash2-server/cluster"
)

type memberRow struct {
	member           *cluster.Member
	heartbeatCounter uint64
}

// claimRow persists across release (owner cleared, fence kept) so the fence
// stays monotonic per key across acquire/release cycles, mirroring the
// DynamoDB implementation.
type claimRow struct {
	namespace    string
	key          []byte
	ownerID      string
	ownerAddress string
	fence        uint64
}

type memory struct {
	mu            sync.Mutex
	members       map[string]*memberRow
	claims        map[string]*claimRow
	subscriptions map[string]map[string]*cluster.Subscription // topic id → instance id → row
}

// NewInMemory returns an in-memory cluster.Store for tests.
func NewInMemory() cluster.Store {
	return &memory{
		members:       make(map[string]*memberRow),
		claims:        make(map[string]*claimRow),
		subscriptions: make(map[string]map[string]*cluster.Subscription),
	}
}

func (m *memory) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.members = make(map[string]*memberRow)
	m.claims = make(map[string]*claimRow)
	m.subscriptions = make(map[string]map[string]*cluster.Subscription)
}

func (m *memory) PutMember(_ context.Context, member *cluster.Member, heartbeatCounter uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.members[member.InstanceID] = &memberRow{
		member:           member.Clone(),
		heartbeatCounter: heartbeatCounter,
	}
	return nil
}

func (m *memory) Heartbeat(_ context.Context, instanceID string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.members[instanceID]
	if !ok {
		return 0, cluster.ErrMemberNotFound
	}
	row.heartbeatCounter++
	return row.heartbeatCounter, nil
}

func (m *memory) SetDraining(_ context.Context, instanceID string, draining bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.members[instanceID]
	if !ok {
		return cluster.ErrMemberNotFound
	}
	row.member.Draining = draining
	return nil
}

func (m *memory) DeleteMember(_ context.Context, instanceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.members, instanceID)
	return nil
}

func (m *memory) GetMembers(_ context.Context) ([]*cluster.MemberRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*cluster.MemberRecord, 0, len(m.members))
	for _, row := range m.members {
		out = append(out, &cluster.MemberRecord{
			Member:           *row.member.Clone(),
			HeartbeatCounter: row.heartbeatCounter,
		})
	}
	return out, nil
}

// claimID hex-encodes the key (matching the DynamoDB pk) so raw key bytes
// containing the separator can never collide across namespaces. Subscription
// topics use the same encoding.
func claimID(namespace string, key []byte) string {
	return namespace + "#" + hex.EncodeToString(key)
}

func (r *claimRow) toClaim() *cluster.Claim {
	key := make([]byte, len(r.key))
	copy(key, r.key)
	return &cluster.Claim{
		Namespace:       r.namespace,
		Key:             key,
		OwnerInstanceID: r.ownerID,
		OwnerAddress:    r.ownerAddress,
		Fence:           r.fence,
	}
}

func (m *memory) AcquireClaim(_ context.Context, namespace string, key []byte, self *cluster.Member, takeover *cluster.TakeoverTarget) (*cluster.Claim, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := claimID(namespace, key)
	row, exists := m.claims[id]

	if exists && row.ownerID == self.InstanceID {
		// Owner re-acquire: fence unchanged.
		return row.toClaim(), nil
	}

	if exists && row.ownerID != "" {
		if takeover == nil || takeover.InstanceID != row.ownerID {
			return row.toClaim(), cluster.ErrClaimHeld
		}
		// Displacement is honored only if the holder's registry record is
		// absent or its heartbeat counter still matches the caller's stale
		// observation — checked atomically with the claim update, exactly as
		// the DynamoDB transaction does.
		if holder, ok := m.members[row.ownerID]; ok && holder.heartbeatCounter != takeover.HeartbeatCounter {
			return row.toClaim(), cluster.ErrClaimHeld
		}
	}

	if !exists {
		row = &claimRow{
			namespace: namespace,
			key:       append([]byte(nil), key...),
		}
		m.claims[id] = row
	}
	row.ownerID = self.InstanceID
	row.ownerAddress = self.Address
	row.fence++
	return row.toClaim(), nil
}

func (m *memory) GetClaim(_ context.Context, namespace string, key []byte) (*cluster.Claim, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.claims[claimID(namespace, key)]
	if !ok || row.ownerID == "" {
		return nil, cluster.ErrClaimNotFound
	}
	return row.toClaim(), nil
}

func (m *memory) ReleaseClaim(_ context.Context, namespace string, key []byte, instanceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.claims[claimID(namespace, key)]
	if !ok || row.ownerID != instanceID {
		return nil
	}
	row.ownerID = ""
	row.ownerAddress = ""
	return nil
}

func (m *memory) PutSubscription(_ context.Context, namespace string, key []byte, member *cluster.Member) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := claimID(namespace, key)
	topic, ok := m.subscriptions[id]
	if !ok {
		topic = make(map[string]*cluster.Subscription)
		m.subscriptions[id] = topic
	}
	topic[member.InstanceID] = &cluster.Subscription{
		Namespace:  namespace,
		Key:        append([]byte(nil), key...),
		InstanceID: member.InstanceID,
		Address:    member.Address,
	}
	return nil
}

func (m *memory) DeleteSubscription(_ context.Context, namespace string, key []byte, instanceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := claimID(namespace, key)
	topic, ok := m.subscriptions[id]
	if !ok {
		return nil
	}
	delete(topic, instanceID)
	if len(topic) == 0 {
		delete(m.subscriptions, id)
	}
	return nil
}

func (m *memory) GetSubscribers(_ context.Context, namespace string, key []byte) ([]*cluster.Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	topic := m.subscriptions[claimID(namespace, key)]
	out := make([]*cluster.Subscription, 0, len(topic))
	for _, sub := range topic {
		out = append(out, sub.Clone())
	}
	return out, nil
}

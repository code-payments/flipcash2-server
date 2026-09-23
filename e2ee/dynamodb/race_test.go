//go:build integration

package dynamodb

import (
	"context"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"

	"github.com/code-payments/flipcash2-server/e2ee"
	"github.com/code-payments/flipcash2-server/e2ee/xeddsa"
	"github.com/code-payments/flipcash2-server/model"
)

const (
	raceDevicesTable   = "e2ee_devices_race_test"
	raceMailboxesTable = "e2ee_mailboxes_race_test"
)

// raceDevice is one registered device with three one-time keys of each
// kind, and generators for more, for the tests that land a replacement or
// a removal in a gap the shared suite cannot reach.
type raceDevice struct {
	address e2ee.DeviceAddress
	otk     func(n int) []*e2ee.OneTimePreKey
	kem     func(n int) []*e2ee.KemSignedPreKey
}

func newRaceStore(t *testing.T) *store {
	ctx := context.Background()
	require.NoError(t, CreateTables(ctx, testEnv.Client, raceDevicesTable, raceMailboxesTable))
	s := NewInDynamoDB(testEnv.Client, raceDevicesTable, raceMailboxesTable).(*store)
	t.Cleanup(s.reset)
	return s
}

func newRaceDevice(t *testing.T, s *store) *raceDevice {
	ctx := context.Background()
	identity := xeddsa.MustGenerateKeyPair()
	sign := func(pub []byte) []byte { return identity.Sign(pub) }
	ecKey := func() []byte { return append([]byte{0x05}, model.MustGenerateKeyPair().Public()[:32]...) }
	kemKey := func() []byte { return append([]byte{0x08}, make([]byte, 1568)...) }
	nextID := uint32(1)
	d := &raceDevice{}
	d.otk = func(n int) []*e2ee.OneTimePreKey {
		var out []*e2ee.OneTimePreKey
		for range n {
			out = append(out, &e2ee.OneTimePreKey{ID: nextID, PublicKey: ecKey()})
			nextID++
		}
		return out
	}
	d.kem = func(n int) []*e2ee.KemSignedPreKey {
		var out []*e2ee.KemSignedPreKey
		for range n {
			pub := kemKey()
			out = append(out, &e2ee.KemSignedPreKey{ID: nextID, PublicKey: pub, Signature: sign(pub)})
			nextID++
		}
		return out
	}

	spk := ecKey()
	lr := kemKey()
	device, _, err := s.RegisterDevice(ctx, &chatpb.IdempotencyKey{Value: make([]byte, 16)}, &e2ee.Device{
		Address:              e2ee.DeviceAddress{UserID: model.MustGenerateUserID()},
		RegistrationID:       7,
		IdentityKey:          append([]byte{0x05}, identity.PublicKey()...),
		IdentityKeySignature: make([]byte, 64),
		AccountKey:           make([]byte, 32),
	}, &e2ee.DeviceKeys{
		SignedPreKey:        &e2ee.SignedPreKey{ID: 1000, PublicKey: spk, Signature: sign(spk)},
		KemLastResortPreKey: &e2ee.KemSignedPreKey{ID: 1001, PublicKey: lr, Signature: sign(lr)},
		OneTimePreKeys:      d.otk(3),
		KemOneTimePreKeys:   d.kem(3),
	})
	require.NoError(t, err)
	d.address = device.Address
	return d
}

func ids[K interface {
	*e2ee.OneTimePreKey | *e2ee.KemSignedPreKey
}](keys []K, id func(K) uint32) map[uint32]struct{} {
	out := map[uint32]struct{}{}
	for _, k := range keys {
		out[id(k)] = struct{}{}
	}
	return out
}

func otkID(k *e2ee.OneTimePreKey) uint32   { return k.ID }
func kemID(k *e2ee.KemSignedPreKey) uint32 { return k.ID }

// A take that reserves an index, then loses the gap before its read to a
// replacement that has flipped and swept the old generation, finds no key
// at the index it reserved. It reserves again, on the new generation, and
// serves from it rather than falling back to the last-resort keys while a
// full pool sits there; the reservation on the retired generation costs
// the new pool nothing.
func TestE2ee_DynamoDBStore_TakeRetriesAfterSweptReservation(t *testing.T) {
	ctx := context.Background()
	s := newRaceStore(t)
	d := newRaceDevice(t, s)

	// Both pools are taken side by side, so both may reach the gap. The
	// replacement runs once, and a taker that reaches the gap while it
	// runs waits for it, so every read that follows sees the swept state.
	replacementOtk := d.otk(4)
	replacementKem := d.kem(4)
	var once sync.Once
	var replaceErr error
	s.afterTakeReserve = func() {
		once.Do(func() {
			_, replaceErr = s.SetKeys(ctx, d.address, e2ee.KeyUpdate{OneTimePreKeys: replacementOtk, KemOneTimePreKeys: replacementKem})
		})
	}

	bundle, err := s.TakePreKeyBundle(ctx, d.address)
	require.NoError(t, err)
	require.NoError(t, replaceErr)

	require.NotNil(t, bundle.OneTimePreKey, "served on the last-resort keys beside a full pool")
	require.Contains(t, ids(replacementOtk, otkID), bundle.OneTimePreKey.ID)
	require.Contains(t, ids(replacementKem, kemID), bundle.KemPreKey.ID)

	counts, err := s.GetKeyStatus(ctx, d.address)
	require.NoError(t, err)
	require.Equal(t, e2ee.PreKeyCounts{OneTimePreKeys: 3, KemOneTimePreKeys: 3}, counts.Counts)

	// Pools with nothing left reserve nothing: the take is served without
	// a key off one read of the keys item, and never spins.
	s.afterTakeReserve = nil
	for range 3 {
		_, err := s.TakePreKeyBundle(ctx, d.address)
		require.NoError(t, err)
	}
	reserved := 0
	s.afterTakeReserve = func() { reserved++ }
	bundle, err = s.TakePreKeyBundle(ctx, d.address)
	require.NoError(t, err)
	require.Nil(t, bundle.OneTimePreKey)
	require.Equal(t, uint32(1001), bundle.KemPreKey.ID)
	require.Equal(t, 0, reserved)
}

// A flip sweeps the generation it replaced and any orphan under the nonce
// old enough not to be a replacement in progress; a freshly staged
// generation it leaves alone, and a later flip collects it once it ages.
func TestE2ee_DynamoDBStore_SetKeysSweepsStaleGenerations(t *testing.T) {
	ctx := context.Background()
	s := newRaceStore(t)
	d := newRaceDevice(t, s)
	entry, err := s.entry(ctx, d.address)
	require.NoError(t, err)
	pk := userPK(d.address.UserID)

	// Two orphans nothing points at: one staged long ago, one just now.
	stage := func(gen string, n int, writtenAt time.Time) {
		for i := range n {
			_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(raceDevicesTable), Item: map[string]types.AttributeValue{
				attrPK:        avS(pk),
				attrSK:        avS(poolSK(entry.nonce, otkSegment, gen, int64(i))),
				attrKeyID:     avN(uint64(500 + i)),
				attrPub:       avB(make([]byte, 33)),
				attrWrittenAt: avNInt(writtenAt.Unix()),
			}})
			require.NoError(t, err)
		}
	}
	stage("oldorphan", 2, time.Now().Add(-2*time.Hour))
	stage("neworphan", 2, time.Now())
	countOtk := func() int {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(raceDevicesTable),
			KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :prefix)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":     avS(pk),
				":prefix": avS(deviceKeyPrefix + hex.EncodeToString(entry.nonce) + otkSegment),
			},
			Select:         types.SelectCount,
			ConsistentRead: aws.Bool(true),
		})
		require.NoError(t, err)
		return int(out.Count)
	}
	require.Equal(t, 3+2+2, countOtk(), "current pool and two orphans")

	// The flip sweeps the replaced generation and the old orphan.
	_, err = s.SetKeys(ctx, d.address, e2ee.KeyUpdate{OneTimePreKeys: d.otk(4)})
	require.NoError(t, err)
	require.Equal(t, 4+2, countOtk(), "new pool and the fresh orphan")

	// Once the fresh orphan is past the grace, the next flip collects it.
	grace := orphanGrace
	orphanGrace = 0
	t.Cleanup(func() { orphanGrace = grace })
	_, err = s.SetKeys(ctx, d.address, e2ee.KeyUpdate{OneTimePreKeys: d.otk(1)})
	require.NoError(t, err)
	require.Equal(t, 1, countOtk(), "only the current pool")

	// The keys themselves are untouched: the current pool is served whole.
	bundle, err := s.TakePreKeyBundle(ctx, d.address)
	require.NoError(t, err)
	require.NotNil(t, bundle.OneTimePreKey)
	counts, err := s.GetKeyStatus(ctx, d.address)
	require.NoError(t, err)
	require.Equal(t, 0, counts.Counts.OneTimePreKeys)
}

// A delivery moves the mailbox's counters by what it stored; an ack moves
// count and bytes back by what it removed and leaves the sequence alone.
func TestE2ee_DynamoDBStore_MailboxCounters(t *testing.T) {
	ctx := context.Background()
	s := newRaceStore(t)
	d := newRaceDevice(t, s)
	entry, err := s.entry(ctx, d.address)
	require.NoError(t, err)
	pk := mailboxPK(d.address.UserID, entry.nonce)
	readCounter := func() [3]int64 {
		out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(raceMailboxesTable),
			Key:            map[string]types.AttributeValue{attrPK: avS(pk), attrSK: avS(counterSK)},
			ConsistentRead: aws.Bool(true),
		})
		require.NoError(t, err)
		require.NotNil(t, out.Item)
		var c [3]int64
		for i, attr := range []string{attrSeq, attrCount, attrBytes} {
			c[i], err = parseInt(out.Item[attr])
			require.NoError(t, err)
		}
		return c
	}

	chat := e2ee.DeriveDmChatID(d.address.UserID, model.MustGenerateUserID())
	var in []*e2ee.Envelope
	for i := range 3 {
		in = append(in, &e2ee.Envelope{
			Recipient: d.address,
			ChatID:    chat,
			Content:   make([]byte, 10*(i+1)),
			ServerTS:  time.Now(),
			ExpiresAt: time.Now().Add(time.Hour),
		})
	}
	stored, err := s.Deliver(ctx, in)
	require.NoError(t, err)
	require.Equal(t, [3]int64{3, 3, 60}, readCounter(), "seq, count, bytes")

	acked, err := s.AckEnvelopes(ctx, d.address, [][]byte{stored[0].ID, stored[2].ID})
	require.NoError(t, err)
	require.Len(t, acked, 2)
	require.Equal(t, [3]int64{3, 1, 20}, readCounter())

	// The sequence continues past what was acknowledged.
	stored, err = s.Deliver(ctx, in[:1])
	require.NoError(t, err)
	require.Equal(t, uint64(4), stored[0].Sequence)
	require.Equal(t, [3]int64{4, 2, 30}, readCounter())
}

// An unregistration returns once the roster no longer names the device;
// its keys and mailbox are swept afterwards, off the call.
func TestE2ee_DynamoDBStore_UnregisterSweepsDetached(t *testing.T) {
	ctx := context.Background()
	s := newRaceStore(t)
	d := newRaceDevice(t, s)

	_, err := s.Deliver(ctx, []*e2ee.Envelope{{
		Recipient: d.address,
		ChatID:    e2ee.DeriveDmChatID(d.address.UserID, model.MustGenerateUserID()),
		Content:   []byte("hello"),
		ServerTS:  time.Now(),
	}})
	require.NoError(t, err)

	countItems := func(table string) int {
		out, err := s.client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(table), Select: "COUNT"})
		require.NoError(t, err)
		return int(out.Count)
	}
	require.Equal(t, 1+1+3+3, countItems(raceDevicesTable), "roster, keys, two pools")
	require.Equal(t, 2, countItems(raceMailboxesTable), "counter and envelope")

	swept := make(chan struct{})
	s.afterSweep = func() { close(swept) }

	// The call has a deadline that a sweep on it would not meet; the
	// removal is not held to it.
	short, cancel := context.WithTimeout(ctx, 30*time.Second)
	require.NoError(t, s.UnregisterDevice(short, d.address))
	cancel()
	_, err = s.GetDevice(ctx, d.address)
	require.ErrorIs(t, err, e2ee.ErrDeviceNotFound)

	select {
	case <-swept:
	case <-time.After(10 * time.Second):
		t.Fatal("sweep did not finish")
	}
	require.Equal(t, 1, countItems(raceDevicesTable), "only the (now empty) roster remains")
	require.Equal(t, 0, countItems(raceMailboxesTable))
}

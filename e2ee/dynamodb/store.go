package dynamodb

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	e2eepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/e2ee/v1"

	"github.com/code-payments/flipcash2-server/e2ee"
)

type store struct {
	client         *dynamodb.Client
	devicesTable   string
	mailboxesTable string

	// afterTakeReserve, when set, runs between a take's reservation of a
	// pool index and its read of the key at it. Tests use it to land a
	// replacement in that gap; it is nil in production.
	afterTakeReserve func()

	// afterSweep, when set, runs when an unregistration's detached sweep
	// finishes. Tests use it to wait for one; it is nil in production.
	afterSweep func()
}

// NewInDynamoDB returns an e2ee.Store backed by the given DynamoDB tables.
// Use CreateTables to provision them.
func NewInDynamoDB(client *dynamodb.Client, devicesTable, mailboxesTable string) e2ee.Store {
	return &store{
		client:         client,
		devicesTable:   devicesTable,
		mailboxesTable: mailboxesTable,
	}
}

// roster is a user's #roster item as read.
type roster struct {
	exists  bool
	version uint64
	devices map[uint32]*rosterEntry
}

// rosterEntry is one device's entry in the roster map.
type rosterEntry struct {
	device *e2ee.Device
	nonce  []byte
	idem   []byte
}

func (r *roster) byIdempotencyKey(key []byte) *rosterEntry {
	for _, e := range r.devices {
		if bytes.Equal(e.idem, key) {
			return e
		}
	}
	return nil
}

func (r *roster) byIdentityKey(key []byte) *rosterEntry {
	for _, e := range r.devices {
		if bytes.Equal(e.device.IdentityKey, key) {
			return e
		}
	}
	return nil
}

// lowestFreeID returns the lowest DeviceId not in the roster, or 0 when the
// space is full.
func (r *roster) lowestFreeID() uint32 {
	for id := uint32(e2ee.MinDeviceID); id <= e2ee.MaxDeviceID; id++ {
		if _, taken := r.devices[id]; !taken {
			return id
		}
	}
	return 0
}

// readRoster reads a user's roster, strongly consistent.
func (s *store) readRoster(ctx context.Context, userID *commonpb.UserId) (*roster, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.devicesTable),
		Key:            rosterKey(userID),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	r := &roster{devices: make(map[uint32]*rosterEntry)}
	if out.Item == nil {
		return r, nil
	}
	r.exists = true
	if r.version, err = parseUint(out.Item[attrVersion]); err != nil {
		return nil, fmt.Errorf("roster version: %w", err)
	}
	entries, _ := out.Item[attrDevices].(*types.AttributeValueMemberM)
	for idStr, av := range entriesOf(entries) {
		id, err := strconv.ParseUint(idStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("roster device id %q: %w", idStr, err)
		}
		entry, err := rosterEntryFromItem(userID, uint32(id), av)
		if err != nil {
			return nil, err
		}
		r.devices[uint32(id)] = entry
	}
	return r, nil
}

// entry returns the addressed device's roster entry, or ErrDeviceNotFound.
func (s *store) entry(ctx context.Context, address e2ee.DeviceAddress) (*rosterEntry, error) {
	r, err := s.readRoster(ctx, address.UserID)
	if err != nil {
		return nil, err
	}
	e, ok := r.devices[address.DeviceID]
	if !ok {
		return nil, e2ee.ErrDeviceNotFound
	}
	return e, nil
}

func (s *store) RegisterDevice(ctx context.Context, idempotencyKey *chatpb.IdempotencyKey, device *e2ee.Device, keys *e2ee.DeviceKeys) (*e2ee.Device, bool, error) {
	// Refused before anything is written: a pool is one item per id, so a
	// duplicate one-time id would be two puts of one key in a batch, which
	// DynamoDB rejects as a whole.
	if e2ee.HasDuplicateKemID(keys.KemLastResortPreKey, keys.KemOneTimePreKeys) || e2ee.HasDuplicateOneTimeID(keys.OneTimePreKeys) {
		return nil, false, e2ee.ErrDuplicatePreKeyID
	}
	userID := device.Address.UserID

	// The keys are written first, under a nonce nobody can reach until the
	// roster names it, and the roster update is the commit: a device becomes
	// visible with every key in place, and a registration that loses the
	// roster race keeps its keys (they are keyed by nonce, not id) and only
	// re-picks an id. Keys written for a registration that is then refused
	// are orphans, unreachable and swept best effort here.
	var nonce []byte
	var written bool
	discard := func() {
		if written {
			_ = s.clearPrefix(ctx, s.devicesTable, userPK(userID), deviceKeyPrefix+hex.EncodeToString(nonce)+"#")
		}
	}

	for attempt := 0; attempt < maxRosterAttempts; attempt++ {
		r, err := s.readRoster(ctx, userID)
		if err != nil {
			discard()
			return nil, false, err
		}
		if e := r.byIdempotencyKey(idempotencyKey.GetValue()); e != nil {
			discard()
			return e.device.Clone(), false, nil
		}
		if len(r.devices) >= e2ee.MaxDevicesPerUser {
			discard()
			return nil, false, e2ee.ErrTooManyDevices
		}
		if r.byIdentityKey(device.IdentityKey) != nil {
			discard()
			return nil, false, e2ee.ErrDuplicateIdentityKey
		}
		id := r.lowestFreeID()
		if id == 0 {
			discard()
			return nil, false, e2ee.ErrTooManyDevices
		}

		if !written {
			nonce = make([]byte, 8)
			if _, err := rand.Read(nonce); err != nil {
				return nil, false, err
			}
			if err := s.writeKeys(ctx, userID, nonce, keys); err != nil {
				// Whatever landed under the nonce before the failure is
				// this call's to remove.
				written = true
				discard()
				return nil, false, err
			}
			written = true
		}

		registered := device.Clone()
		registered.Address.DeviceID = id
		entry := &rosterEntry{device: registered, nonce: nonce, idem: append([]byte(nil), idempotencyKey.GetValue()...)}

		input := &dynamodb.UpdateItemInput{
			TableName: aws.String(s.devicesTable),
			Key:       rosterKey(userID),
			ExpressionAttributeNames: map[string]string{
				"#devices": attrDevices,
				"#version": attrVersion,
			},
		}
		if r.exists {
			input.ExpressionAttributeNames["#id"] = strconv.FormatUint(uint64(id), 10)
			input.UpdateExpression = aws.String("SET #devices.#id = :entry, #version = :next")
			input.ConditionExpression = aws.String("#version = :version")
			input.ExpressionAttributeValues = map[string]types.AttributeValue{
				":entry":   rosterEntryItem(entry),
				":next":    avN(r.version + 1),
				":version": avN(r.version),
			}
		} else {
			input.UpdateExpression = aws.String("SET #devices = :devices, #version = :next")
			input.ConditionExpression = aws.String("attribute_not_exists(#version)")
			input.ExpressionAttributeValues = map[string]types.AttributeValue{
				":devices": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
					strconv.FormatUint(uint64(id), 10): rosterEntryItem(entry),
				}},
				":next": avN(1),
			}
		}

		if _, err := s.client.UpdateItem(ctx, input); err != nil {
			if isConditionalCheckFailed(err) {
				continue // Lost the race: re-read and re-pick.
			}
			discard()
			return nil, false, err
		}
		return registered.Clone(), true, nil
	}

	discard()
	return nil, false, errors.New("e2ee/dynamodb: roster contention registering device")
}

func (s *store) UnregisterDevice(ctx context.Context, address e2ee.DeviceAddress) error {
	for attempt := 0; attempt < maxRosterAttempts; attempt++ {
		r, err := s.readRoster(ctx, address.UserID)
		if err != nil {
			return err
		}
		entry, ok := r.devices[address.DeviceID]
		if !ok {
			return e2ee.ErrDeviceNotFound
		}

		_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(s.devicesTable),
			Key:       rosterKey(address.UserID),
			ExpressionAttributeNames: map[string]string{
				"#devices": attrDevices,
				"#id":      strconv.FormatUint(uint64(address.DeviceID), 10),
				"#version": attrVersion,
			},
			UpdateExpression:    aws.String("REMOVE #devices.#id SET #version = :next"),
			ConditionExpression: aws.String("#version = :version"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":next":    avN(r.version + 1),
				":version": avN(r.version),
			},
		})
		if err != nil {
			if isConditionalCheckFailed(err) {
				continue
			}
			return err
		}

		// The device is gone; its keys and mailbox are unreachable from
		// here on (both are addressed by the nonce the roster no longer
		// holds). Sweeping them is a courtesy, not the contract: the mailbox
		// expires by TTL, and a failed sweep leaves nothing anyone can read.
		// It runs detached from the call on its own budget, since a full
		// mailbox is thousands of deletes the caller has no reason to wait
		// for. Pool items carry no TTL, so a pool the sweep does not reach
		// (at most a few hundred kilobytes per registration) stays until a
		// sweeper exists; none does today.
		sweep := context.WithoutCancel(ctx)
		go func() {
			ctx, cancel := context.WithTimeout(sweep, sweepTimeout)
			defer cancel()
			_ = s.clearPrefix(ctx, s.devicesTable, userPK(address.UserID), deviceKeyPrefix+hex.EncodeToString(entry.nonce)+"#")
			_ = s.clearPrefix(ctx, s.mailboxesTable, mailboxPK(address.UserID, entry.nonce), "")
			if s.afterSweep != nil {
				s.afterSweep()
			}
		}()
		return nil
	}
	return errors.New("e2ee/dynamodb: roster contention unregistering device")
}

func (s *store) GetDevice(ctx context.Context, address e2ee.DeviceAddress) (*e2ee.Device, error) {
	e, err := s.entry(ctx, address)
	if err != nil {
		return nil, err
	}
	return e.device.Clone(), nil
}

func (s *store) GetDevices(ctx context.Context, userID *commonpb.UserId) ([]*e2ee.Device, error) {
	r, err := s.readRoster(ctx, userID)
	if err != nil {
		return nil, err
	}
	out := make([]*e2ee.Device, 0, len(r.devices))
	for _, e := range r.devices {
		out = append(out, e.device.Clone())
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Address.DeviceID < out[j].Address.DeviceID
	})
	return out, nil
}

func (s *store) TouchLastSeen(ctx context.Context, address e2ee.DeviceAddress, day time.Time) error {
	day = day.UTC().Truncate(24 * time.Hour)
	// One attribute of one map entry, written only when the recorded day
	// is older, and conditioned on nothing else: it never contends with a
	// registration's version.
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.devicesTable),
		Key:       rosterKey(address.UserID),
		ExpressionAttributeNames: map[string]string{
			"#devices":  attrDevices,
			"#id":       strconv.FormatUint(uint64(address.DeviceID), 10),
			"#lastSeen": attrLastSeen,
		},
		UpdateExpression:    aws.String("SET #devices.#id.#lastSeen = :t"),
		ConditionExpression: aws.String("attribute_exists(#devices.#id) AND (attribute_not_exists(#devices.#id.#lastSeen) OR #devices.#id.#lastSeen < :t)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":t": avNInt(day.UnixNano()),
		},
	})
	if isConditionalCheckFailed(err) {
		// No such device, or nothing to move: one read tells which.
		_, err := s.entry(ctx, address)
		return err
	}
	return err
}

func (s *store) AddCapabilities(ctx context.Context, address e2ee.DeviceAddress, caps []e2eepb.Device_Capability) (*e2ee.Device, error) {
	// The union is computed from the roster as read and written back under
	// its version, like a registration: a lost race re-reads and re-merges.
	// Rare (an app update), so the compare-and-set costs nothing in practice.
	for attempt := 0; attempt < maxRosterAttempts; attempt++ {
		r, err := s.readRoster(ctx, address.UserID)
		if err != nil {
			return nil, err
		}
		entry, ok := r.devices[address.DeviceID]
		if !ok {
			return nil, e2ee.ErrDeviceNotFound
		}
		merged, changed := e2ee.MergeCapabilities(entry.device.Capabilities, caps)
		if !changed {
			return entry.device.Clone(), nil
		}

		list := make([]types.AttributeValue, 0, len(merged))
		for _, c := range merged {
			list = append(list, avNInt(int64(c)))
		}
		_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(s.devicesTable),
			Key:       rosterKey(address.UserID),
			ExpressionAttributeNames: map[string]string{
				"#devices": attrDevices,
				"#id":      strconv.FormatUint(uint64(address.DeviceID), 10),
				"#caps":    attrCapabilities,
				"#version": attrVersion,
			},
			UpdateExpression:    aws.String("SET #devices.#id.#caps = :caps, #version = :next"),
			ConditionExpression: aws.String("#version = :version AND attribute_exists(#devices.#id)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":caps":    &types.AttributeValueMemberL{Value: list},
				":next":    avN(r.version + 1),
				":version": avN(r.version),
			},
		})
		if err != nil {
			if isConditionalCheckFailed(err) {
				continue
			}
			return nil, err
		}
		out := entry.device.Clone()
		out.Capabilities = merged
		return out, nil
	}
	return nil, errors.New("e2ee/dynamodb: roster contention adding capabilities")
}

// deviceKeys is a device's dev#<nonce>#keys item as read.
type deviceKeys struct {
	version      uint64
	signedPreKey *e2ee.SignedPreKey
	lastResort   *e2ee.KemSignedPreKey
	otk          poolState
	kem          poolState
}

// poolState is one one-time pool as the keys item describes it: the
// generation its items are written under, how many keys were uploaded, and
// the index of the next to serve.
type poolState struct {
	gen string
	idx int64
	n   int64
}

// remaining is how many keys the pool has left to serve.
func (p poolState) remaining() int {
	return int(max(p.n-p.idx, 0))
}

func (k *deviceKeys) counts() e2ee.PreKeyCounts {
	return e2ee.PreKeyCounts{
		OneTimePreKeys:    k.otk.remaining(),
		KemOneTimePreKeys: k.kem.remaining(),
	}
}

// status is the keys item as the device's owner sees it.
func (k *deviceKeys) status(device *e2ee.Device) e2ee.KeyStatus {
	return e2ee.KeyStatus{
		Device:              device.Clone(),
		SignedPreKey:        k.signedPreKey.Clone(),
		KemLastResortPreKey: k.lastResort.Clone(),
		Counts:              k.counts(),
	}
}

// pool names one one-time pool: the segment its items are keyed under and
// the keys-item attributes that describe it.
type pool struct {
	segment string
	genAttr string
	idxAttr string
	nAttr   string
}

var (
	otkPool = pool{segment: otkSegment, genAttr: attrOtkGen, idxAttr: attrOtkIdx, nAttr: attrOtkN}
	kemPool = pool{segment: kemSegment, genAttr: attrKemGen, idxAttr: attrKemIdx, nAttr: attrKemN}
)

// state reads the pool's description off a keys item.
func (p pool) state(item map[string]types.AttributeValue) (poolState, error) {
	st := poolState{gen: asS(item[p.genAttr])}
	var err error
	if st.idx, err = parseInt(item[p.idxAttr]); err != nil {
		return st, fmt.Errorf("keys %s: %w", p.idxAttr, err)
	}
	if st.n, err = parseInt(item[p.nAttr]); err != nil {
		return st, fmt.Errorf("keys %s: %w", p.nAttr, err)
	}
	return st, nil
}

func keysKey(userID *commonpb.UserId, nonce []byte) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{attrPK: avS(userPK(userID)), attrSK: avS(keysSK(nonce))}
}

// readKeys reads a device's keys item, strongly consistent. A device on the
// roster always has one (written before the roster names it).
func (s *store) readKeys(ctx context.Context, userID *commonpb.UserId, nonce []byte) (*deviceKeys, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.devicesTable),
		Key:            keysKey(userID, nonce),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, e2ee.ErrDeviceNotFound
	}
	return keysFromItem(out.Item)
}

// writeKeys writes a fresh keys item and its pools for a new registration.
func (s *store) writeKeys(ctx context.Context, userID *commonpb.UserId, nonce []byte, keys *e2ee.DeviceKeys) error {
	otkGen, kemGen := newGeneration(), newGeneration()
	if err := s.writePool(ctx, userID, nonce, otkGen, keys.OneTimePreKeys, nil); err != nil {
		return err
	}
	if err := s.writePool(ctx, userID, nonce, kemGen, nil, keys.KemOneTimePreKeys); err != nil {
		return err
	}
	item := map[string]types.AttributeValue{
		attrPK:      avS(userPK(userID)),
		attrSK:      avS(keysSK(nonce)),
		attrVersion: avN(1),
		attrOtkGen:  avS(otkGen),
		attrOtkIdx:  avNInt(0),
		attrOtkN:    avNInt(int64(len(keys.OneTimePreKeys))),
		attrKemGen:  avS(kemGen),
		attrKemIdx:  avNInt(0),
		attrKemN:    avNInt(int64(len(keys.KemOneTimePreKeys))),
	}
	putSignedPreKey(item, keys.SignedPreKey)
	putLastResort(item, keys.KemLastResortPreKey)
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(s.devicesTable), Item: item})
	return err
}

// writePool writes one-time prekeys under a generation, each at its upload
// index, which is the order they are served in. Either pool may be nil; the
// other is written.
func (s *store) writePool(ctx context.Context, userID *commonpb.UserId, nonce []byte, gen string, otk []*e2ee.OneTimePreKey, kem []*e2ee.KemSignedPreKey) error {
	// Every item is stamped with when it was staged, which is what tells a
	// later sweep an orphan (see sweepStaleGenerations) from a generation
	// another replacement is staging right now.
	writtenAt := avNInt(time.Now().Unix())
	items := make([]map[string]types.AttributeValue, 0, len(otk)+len(kem))
	for i, k := range otk {
		items = append(items, map[string]types.AttributeValue{
			attrPK:        avS(userPK(userID)),
			attrSK:        avS(poolSK(nonce, otkSegment, gen, int64(i))),
			attrKeyID:     avN(uint64(k.ID)),
			attrPub:       avB(k.PublicKey),
			attrWrittenAt: writtenAt,
		})
	}
	for i, k := range kem {
		items = append(items, map[string]types.AttributeValue{
			attrPK:        avS(userPK(userID)),
			attrSK:        avS(poolSK(nonce, kemSegment, gen, int64(i))),
			attrKeyID:     avN(uint64(k.ID)),
			attrPub:       avB(k.PublicKey),
			attrSig:       avB(k.Signature),
			attrWrittenAt: writtenAt,
		})
	}
	return batchPut(ctx, s.client, s.devicesTable, items)
}

func (s *store) SetKeys(ctx context.Context, address e2ee.DeviceAddress, update e2ee.KeyUpdate) (e2ee.KeyStatus, error) {
	// What the request alone decides is refused before any write (see
	// RegisterDevice); the KEM check needs the stored last-resort key and
	// runs below.
	if e2ee.HasDuplicateOneTimeID(update.OneTimePreKeys) {
		return e2ee.KeyStatus{}, e2ee.ErrDuplicatePreKeyID
	}
	entry, err := s.entry(ctx, address)
	if err != nil {
		return e2ee.KeyStatus{}, err
	}
	userID, nonce := address.UserID, entry.nonce

	// New pools go in under fresh generations first, unreachable until the
	// keys item is flipped to them; the flip is the commit and is
	// compare-and-set on the item's version, so two replacements cannot
	// interleave. A random generation, not a counter, so a retry after a
	// lost race never shares a prefix with the winner's pool.
	// Pools written and not flipped to are this call's to remove: on any
	// refusal or failure they would otherwise sit unreachable forever, and
	// nothing else may touch them (see the sweep below).
	var otkGen, kemGen string
	discard := func() {
		if otkGen != "" {
			_ = s.clearPrefix(ctx, s.devicesTable, userPK(userID), poolPrefix(nonce, otkSegment, otkGen))
		}
		if kemGen != "" {
			_ = s.clearPrefix(ctx, s.devicesTable, userPK(userID), poolPrefix(nonce, kemSegment, kemGen))
		}
	}
	if update.OneTimePreKeys != nil {
		otkGen = newGeneration()
		if err := s.writePool(ctx, userID, nonce, otkGen, update.OneTimePreKeys, nil); err != nil {
			discard()
			return e2ee.KeyStatus{}, err
		}
	}
	if update.KemOneTimePreKeys != nil {
		kemGen = newGeneration()
		if err := s.writePool(ctx, userID, nonce, kemGen, nil, update.KemOneTimePreKeys); err != nil {
			discard()
			return e2ee.KeyStatus{}, err
		}
	}

	for attempt := 0; attempt < maxKeysAttempts; attempt++ {
		k, err := s.readKeys(ctx, userID, nonce)
		if err != nil {
			discard()
			return e2ee.KeyStatus{}, err
		}

		// The KEM id space is one across the last-resort key and the pool,
		// as they stand after this update. A supplied pool was checked
		// against a supplied last-resort key by the caller; what remains is
		// each against the stored other.
		lastResort := k.lastResort
		if update.KemLastResortPreKey != nil {
			lastResort = update.KemLastResortPreKey
		}
		if update.KemOneTimePreKeys != nil {
			if e2ee.HasDuplicateKemID(lastResort, update.KemOneTimePreKeys) {
				discard()
				return e2ee.KeyStatus{}, e2ee.ErrDuplicatePreKeyID
			}
		} else if update.KemLastResortPreKey != nil {
			inPool, err := s.poolHasID(ctx, userID, nonce, kemPool, k.kem.gen, lastResort.ID)
			if err != nil {
				discard()
				return e2ee.KeyStatus{}, err
			}
			if inPool {
				discard()
				return e2ee.KeyStatus{}, e2ee.ErrDuplicatePreKeyID
			}
		}

		names := map[string]string{"#version": attrVersion}
		values := map[string]types.AttributeValue{
			":version": avN(k.version),
			":next":    avN(k.version + 1),
		}
		sets := []string{"#version = :next"}
		next := *k
		if update.SignedPreKey != nil {
			names["#spkId"], names["#spkPub"], names["#spkSig"] = attrSignedPreKeyID, attrSignedPreKeyPub, attrSignedPreKeySig
			values[":spkId"], values[":spkPub"], values[":spkSig"] = avN(uint64(update.SignedPreKey.ID)), avB(update.SignedPreKey.PublicKey), avB(update.SignedPreKey.Signature)
			sets = append(sets, "#spkId = :spkId", "#spkPub = :spkPub", "#spkSig = :spkSig")
			next.signedPreKey = update.SignedPreKey
		}
		if update.KemLastResortPreKey != nil {
			names["#lrId"], names["#lrPub"], names["#lrSig"] = attrLastResortID, attrLastResortPub, attrLastResortSig
			values[":lrId"], values[":lrPub"], values[":lrSig"] = avN(uint64(update.KemLastResortPreKey.ID)), avB(update.KemLastResortPreKey.PublicKey), avB(update.KemLastResortPreKey.Signature)
			sets = append(sets, "#lrId = :lrId", "#lrPub = :lrPub", "#lrSig = :lrSig")
			next.lastResort = update.KemLastResortPreKey
		}
		if update.OneTimePreKeys != nil {
			names["#otkGen"], names["#otkIdx"], names["#otkN"] = attrOtkGen, attrOtkIdx, attrOtkN
			values[":otkGen"], values[":otkN"], values[":zero"] = avS(otkGen), avNInt(int64(len(update.OneTimePreKeys))), avNInt(0)
			sets = append(sets, "#otkGen = :otkGen", "#otkIdx = :zero", "#otkN = :otkN")
			next.otk = poolState{gen: otkGen, n: int64(len(update.OneTimePreKeys))}
		}
		if update.KemOneTimePreKeys != nil {
			names["#kemGen"], names["#kemIdx"], names["#kemN"] = attrKemGen, attrKemIdx, attrKemN
			values[":kemGen"], values[":kemN"], values[":zero"] = avS(kemGen), avNInt(int64(len(update.KemOneTimePreKeys))), avNInt(0)
			sets = append(sets, "#kemGen = :kemGen", "#kemIdx = :zero", "#kemN = :kemN")
			next.kem = poolState{gen: kemGen, n: int64(len(update.KemOneTimePreKeys))}
		}

		_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                 aws.String(s.devicesTable),
			Key:                       keysKey(userID, nonce),
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: values,
			UpdateExpression:          aws.String("SET " + strings.Join(sets, ", ")),
			ConditionExpression:       aws.String("#version = :version"),
		})
		if err != nil {
			if isConditionalCheckFailed(err) {
				continue
			}
			discard()
			return e2ee.KeyStatus{}, err
		}

		// The generation just replaced is unreachable now and is swept,
		// along with any other generation under the nonce that nothing
		// points at and that is old enough not to be one a concurrent
		// replacement is staging right now (see sweepStaleGenerations).
		// Best effort; a leftover costs storage, never correctness, and
		// the next replacement sweeps whatever this one leaves. A take
		// that reserved an index of the swept generation and has not read
		// it yet finds nothing there and reserves again (see takeFromPool).
		now := time.Now()
		if update.OneTimePreKeys != nil {
			_ = s.sweepStaleGenerations(ctx, userID, nonce, otkSegment, otkGen, k.otk.gen, now)
		}
		if update.KemOneTimePreKeys != nil {
			_ = s.sweepStaleGenerations(ctx, userID, nonce, kemSegment, kemGen, k.kem.gen, now)
		}
		return next.status(entry.device), nil
	}
	discard()
	return e2ee.KeyStatus{}, errors.New("e2ee/dynamodb: keys contention setting keys")
}

func (s *store) GetKeyStatus(ctx context.Context, address e2ee.DeviceAddress) (e2ee.KeyStatus, error) {
	entry, err := s.entry(ctx, address)
	if err != nil {
		return e2ee.KeyStatus{}, err
	}
	k, err := s.readKeys(ctx, address.UserID, entry.nonce)
	if err != nil {
		return e2ee.KeyStatus{}, err
	}
	return k.status(entry.device), nil
}

func (s *store) TakePreKeyBundle(ctx context.Context, address e2ee.DeviceAddress) (*e2ee.PreKeyBundle, error) {
	entry, err := s.entry(ctx, address)
	if err != nil {
		return nil, err
	}
	userID, nonce := address.UserID, entry.nonce

	// Each pool is taken on its own, the two side by side. A reservation
	// that lands returns the keys item as it stood, which is where the
	// signed prekey and last-resort key come from, so a take from a device
	// with anything left reads the keys item only when it reserves; a
	// take from a device with nothing left reads it once.
	var otkKey, otkKeys, kemKey, kemKeys map[string]types.AttributeValue
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		otkKey, otkKeys, err = s.takeFromPool(gctx, userID, nonce, otkPool)
		return err
	})
	g.Go(func() (err error) {
		kemKey, kemKeys, err = s.takeFromPool(gctx, userID, nonce, kemPool)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	var k *deviceKeys
	switch {
	case otkKeys != nil:
		k, err = keysFromItem(otkKeys)
	case kemKeys != nil:
		k, err = keysFromItem(kemKeys)
	default:
		k, err = s.readKeys(ctx, userID, nonce)
	}
	if err != nil {
		return nil, err
	}

	bundle := &e2ee.PreKeyBundle{
		Device:       entry.device.Clone(),
		SignedPreKey: k.signedPreKey,
		KemPreKey:    k.lastResort,
	}
	if otkKey != nil {
		id, err := parseUint(otkKey[attrKeyID])
		if err != nil {
			return nil, fmt.Errorf("pool key id: %w", err)
		}
		bundle.OneTimePreKey = &e2ee.OneTimePreKey{ID: uint32(id), PublicKey: asB(otkKey[attrPub])}
	}
	if kemKey != nil {
		id, err := parseUint(kemKey[attrKeyID])
		if err != nil {
			return nil, fmt.Errorf("pool key id: %w", err)
		}
		bundle.KemPreKey = &e2ee.KemSignedPreKey{ID: uint32(id), PublicKey: asB(kemKey[attrPub]), Signature: asB(kemKey[attrSig])}
	}
	return bundle, nil
}

// takeFromPool reserves the next index of one pool and reads the key at
// it. It returns the key's item, or nil when the pool has nothing left,
// and the keys item as it stood when the reservation landed (nil when
// nothing was reserved).
//
// The reservation is Signal-Server's paged-pool take: one ADD on the
// pool's index, conditioned on the index being below the count, with the
// item as it was returned. The write is the arbiter, so two takers never
// reserve one index, and nothing is read before the write, so a
// replacement that flips the generation between a read and a write, the
// race an earlier design defended against, has no gap to land in: the
// returned item names the generation the index was reserved on. What a
// replacement can still do is flip AND sweep between the reservation and
// the read of the key, in which case the index was of a retired
// generation and the key is gone; the read misses and the take reserves
// again, on the new generation, which charged nothing for the miss. A
// replacement that flipped but has not yet swept lets the read find the
// retired key, which is served: the device still holds it, it is served
// once, and the new pool is not charged for it.
func (s *store) takeFromPool(ctx context.Context, userID *commonpb.UserId, nonce []byte, p pool) (key, keys map[string]types.AttributeValue, err error) {
	for attempt := 0; attempt < maxTakeAttempts; attempt++ {
		out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                 aws.String(s.devicesTable),
			Key:                       keysKey(userID, nonce),
			ExpressionAttributeNames:  map[string]string{"#idx": p.idxAttr, "#n": p.nAttr},
			ExpressionAttributeValues: map[string]types.AttributeValue{":one": avNInt(1)},
			UpdateExpression:          aws.String("ADD #idx :one"),
			ConditionExpression:       aws.String("#idx < #n"),
			ReturnValues:              types.ReturnValueAllOld,
		})
		if err != nil {
			if isConditionalCheckFailed(err) {
				return nil, nil, nil // Nothing left, or no such device.
			}
			return nil, nil, err
		}
		if s.afterTakeReserve != nil {
			s.afterTakeReserve()
		}
		st, err := p.state(out.Attributes)
		if err != nil {
			return nil, nil, err
		}
		item, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(s.devicesTable),
			Key:            map[string]types.AttributeValue{attrPK: avS(userPK(userID)), attrSK: avS(poolSK(nonce, p.segment, st.gen, st.idx))},
			ConsistentRead: aws.Bool(true),
		})
		if err != nil {
			return nil, nil, err
		}
		if item.Item != nil {
			return item.Item, out.Attributes, nil
		}
		// Reserved on a generation that was replaced and swept in between.
	}
	return nil, nil, errors.New("e2ee/dynamodb: pool replaced repeatedly during take")
}

// poolHasID reports whether a pool generation holds a key with the id,
// served or not: a served key's private half is still held by the device,
// so its id is still taken. A pool is read whole for it; the check runs on
// a last-resort rotation, which is rare.
func (s *store) poolHasID(ctx context.Context, userID *commonpb.UserId, nonce []byte, p pool, gen string, id uint32) (bool, error) {
	input := &dynamodb.QueryInput{
		TableName:                aws.String(s.devicesTable),
		KeyConditionExpression:   aws.String("pk = :pk AND begins_with(sk, :prefix)"),
		FilterExpression:         aws.String("#kid = :id"),
		ExpressionAttributeNames: map[string]string{"#kid": attrKeyID},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     avS(userPK(userID)),
			":prefix": avS(poolPrefix(nonce, p.segment, gen)),
			":id":     avN(uint64(id)),
		},
		ProjectionExpression: aws.String(attrSK),
		ConsistentRead:       aws.Bool(true),
	}
	for {
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return false, err
		}
		if len(out.Items) > 0 {
			return true, nil
		}
		if len(out.LastEvaluatedKey) == 0 {
			return false, nil
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

// sweepStaleGenerations deletes, under one pool segment of a device, every
// generation that is not current: the one just replaced outright, and any
// other whose items were staged more than orphanGrace ago. The others are
// orphans, left by a replacement that failed or died between staging and
// its flip, or whose sweep failed; a generation staged within the grace
// may be one a concurrent replacement is about to flip to, and is left
// alone. This is Signal-Server's orphaned-page rule (delete pages that are
// not the current one and are older than a minimum age), run on every
// flip rather than by a command, so each replacement repairs what earlier
// ones leaked. Best effort, like the sweep it extends.
func (s *store) sweepStaleGenerations(ctx context.Context, userID *commonpb.UserId, nonce []byte, segment, current, replaced string, now time.Time) error {
	prefix := deviceKeyPrefix + hex.EncodeToString(nonce) + segment
	cutoff := now.Add(-orphanGrace).Unix()
	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.devicesTable),
		KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     avS(userPK(userID)),
			":prefix": avS(prefix),
		},
		ExpressionAttributeNames: map[string]string{"#sk": attrSK, "#w": attrWrittenAt},
		ProjectionExpression:     aws.String("#sk, #w"),
		ConsistentRead:           aws.Bool(true),
	}
	for {
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return err
		}
		var keys []map[string]types.AttributeValue
		for _, item := range out.Items {
			sk := asS(item[attrSK])
			gen, _, _ := strings.Cut(sk[len(prefix):], "#")
			stale := gen == replaced
			if !stale && gen != current {
				// An item without a stamp predates the stamp; treat it as
				// old enough.
				writtenAt, err := parseInt(item[attrWrittenAt])
				stale = err != nil || writtenAt <= cutoff
			}
			if stale {
				keys = append(keys, map[string]types.AttributeValue{attrPK: avS(userPK(userID)), attrSK: avS(sk)})
			}
		}
		if err := batchDelete(ctx, s.client, s.devicesTable, keys); err != nil {
			return err
		}
		if len(out.LastEvaluatedKey) == 0 {
			return nil
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

func (s *store) TakeTokens(ctx context.Context, kind, key string, cost int64, cfg e2ee.BucketConfig, now time.Time) (time.Duration, error) {
	if cost > cfg.Size {
		_, retry := cfg.Decide(e2ee.WindowCounts{}, now, cost)
		return retry, nil
	}
	itemKey := map[string]types.AttributeValue{attrPK: avS(limitKeyPrefix + kind + "#" + key), attrSK: avS(bucketSK)}
	index := cfg.WindowIndex(now)
	expiresAt := avNInt(now.Add(2 * cfg.Window()).Unix())

	for attempt := 0; attempt < maxBucketAttempts; attempt++ {
		// The take first: one ADD on the current window's count, conditioned
		// only on the item being on that window, returning the counts as
		// they now stand. No read precedes it and nothing contends with
		// it, so a key a thousand callers hit at once costs each of them
		// one write. What was added is then judged against both windows;
		// a take that does not fit gives its cost back, best effort and
		// only while the window is still this one, so a refused take
		// consumes nothing. Between the add and the give-back, other takers
		// see the cost as taken, which errs toward refusing.
		out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                aws.String(s.devicesTable),
			Key:                      itemKey,
			ExpressionAttributeNames: map[string]string{"#cw": attrCurWindow, "#cn": attrCurCount, "#exp": attrExpiresAt},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":w":    avNInt(index),
				":cost": avNInt(cost),
				":exp":  expiresAt,
			},
			UpdateExpression:    aws.String("ADD #cn :cost SET #exp = :exp"),
			ConditionExpression: aws.String("#cw = :w"),
			ReturnValues:        types.ReturnValueAllNew,
		})
		if err == nil {
			w, err := windowCountsFromItem(out.Attributes)
			if err != nil {
				return 0, err
			}
			w = w.At(cfg, now)
			w.Count -= cost
			ok, retry := cfg.Decide(w, now, cost)
			if ok {
				return 0, nil
			}
			_, _ = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName:                aws.String(s.devicesTable),
				Key:                      itemKey,
				ExpressionAttributeNames: map[string]string{"#cw": attrCurWindow, "#cn": attrCurCount},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":w":    avNInt(index),
					":back": avNInt(-cost),
				},
				UpdateExpression:    aws.String("ADD #cn :back"),
				ConditionExpression: aws.String("#cw = :w"),
			})
			return retry, nil
		}
		if !isConditionalCheckFailed(err) {
			return 0, err
		}

		// The item is on an earlier window, or there is none: the window
		// rolls. Once per key per window, so a read and a compare-and-set
		// are fine here; a lost race means another taker rolled it, and
		// the plain take above then applies.
		item, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(s.devicesTable),
			Key:            itemKey,
			ConsistentRead: aws.Bool(true),
		})
		if err != nil {
			return 0, err
		}
		var old e2ee.WindowCounts
		if item.Item != nil {
			if old, err = windowCountsFromItem(item.Item); err != nil {
				return 0, err
			}
			if old.Window == index {
				continue // Rolled by another taker since the add; take from it.
			}
		}
		w := old.At(cfg, now)
		ok, retry := cfg.Decide(w, now, cost)
		if !ok {
			return retry, nil
		}

		input := &dynamodb.UpdateItemInput{
			TableName:                aws.String(s.devicesTable),
			Key:                      itemKey,
			ExpressionAttributeNames: map[string]string{"#cw": attrCurWindow, "#cn": attrCurCount, "#pw": attrPrevWindow, "#pn": attrPrevCount, "#exp": attrExpiresAt},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":w":    avNInt(index),
				":cost": avNInt(cost),
				":pw":   avNInt(index - 1),
				":pn":   avNInt(w.PrevCount),
				":exp":  expiresAt,
			},
			UpdateExpression: aws.String("SET #cw = :w, #cn = :cost, #pw = :pw, #pn = :pn, #exp = :exp"),
		}
		if item.Item == nil {
			input.ConditionExpression = aws.String("attribute_not_exists(" + attrPK + ")")
		} else {
			input.ConditionExpression = aws.String("#cw = :old")
			input.ExpressionAttributeValues[":old"] = avNInt(old.Window)
		}
		if _, err := s.client.UpdateItem(ctx, input); err != nil {
			if isConditionalCheckFailed(err) {
				continue // Rolled by another taker between the read and the write.
			}
			return 0, err
		}
		return 0, nil
	}
	return 0, errors.New("e2ee/dynamodb: limit contention")
}

// windowCountsFromItem reads a limit item's counts. The previous window's
// count is kept only when it is the window before the current one.
func windowCountsFromItem(item map[string]types.AttributeValue) (e2ee.WindowCounts, error) {
	var w e2ee.WindowCounts
	var err error
	if w.Window, err = parseInt(item[attrCurWindow]); err != nil {
		return w, fmt.Errorf("limit window: %w", err)
	}
	if w.Count, err = parseInt(item[attrCurCount]); err != nil {
		return w, fmt.Errorf("limit count: %w", err)
	}
	if av, ok := item[attrPrevWindow]; ok {
		prevWindow, err := parseInt(av)
		if err != nil {
			return w, fmt.Errorf("limit previous window: %w", err)
		}
		if prevWindow == w.Window-1 {
			if w.PrevCount, err = parseInt(item[attrPrevCount]); err != nil {
				return w, fmt.Errorf("limit previous count: %w", err)
			}
		}
	}
	return w, nil
}

// clearPrefix deletes every item of a partition whose sort key begins with
// prefix (every item when prefix is empty) and that every filter admits.
func (s *store) clearPrefix(ctx context.Context, table, pk, prefix string, filters ...func(sk string) bool) error {
	input := &dynamodb.QueryInput{
		TableName:                 aws.String(table),
		KeyConditionExpression:    aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": avS(pk)},
		ProjectionExpression:      aws.String(attrSK),
		ConsistentRead:            aws.Bool(true),
	}
	if prefix != "" {
		input.KeyConditionExpression = aws.String("pk = :pk AND begins_with(sk, :prefix)")
		input.ExpressionAttributeValues[":prefix"] = avS(prefix)
	}
	for {
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return err
		}
		var keys []map[string]types.AttributeValue
	items:
		for _, item := range out.Items {
			sk := asS(item[attrSK])
			for _, f := range filters {
				if !f(sk) {
					continue items
				}
			}
			keys = append(keys, map[string]types.AttributeValue{attrPK: avS(pk), attrSK: avS(sk)})
		}
		if err := batchDelete(ctx, s.client, table, keys); err != nil {
			return err
		}
		if len(out.LastEvaluatedKey) == 0 {
			return nil
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

// Keys and encodings.

func userPK(userID *commonpb.UserId) string {
	return userKeyPrefix + hex.EncodeToString(userID.GetValue())
}

func rosterKey(userID *commonpb.UserId) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{attrPK: avS(userPK(userID)), attrSK: avS(rosterSK)}
}

func keysSK(nonce []byte) string {
	return deviceKeyPrefix + hex.EncodeToString(nonce) + keysSKSuffix
}

func poolPrefix(nonce []byte, segment, gen string) string {
	return deviceKeyPrefix + hex.EncodeToString(nonce) + segment + gen + "#"
}

// poolSK is the key of the pool item at an upload index. The index is
// zero-padded so a generation's items sort in upload order under their
// prefix; nothing parses it back, a take reads the index off the keys item.
func poolSK(nonce []byte, segment, gen string, idx int64) string {
	return poolPrefix(nonce, segment, gen) + fmt.Sprintf("%06d", idx)
}

func mailboxPK(userID *commonpb.UserId, nonce []byte) string {
	return mailboxKeyPrefix + hex.EncodeToString(userID.GetValue()) + "#" + hex.EncodeToString(nonce)
}

func newGeneration() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func rosterEntryItem(e *rosterEntry) types.AttributeValue {
	d := e.device
	m := map[string]types.AttributeValue{
		attrRegistrationID: avN(uint64(d.RegistrationID)),
		attrIdentityKey:    avB(d.IdentityKey),
		attrIdentityKeySig: avB(d.IdentityKeySignature),
		attrAccountKey:     avB(d.AccountKey),
		attrRegisteredAt:   avNInt(d.RegisteredAt.UnixNano()),
		attrNonce:          avB(e.nonce),
		attrIdempotencyKey: avB(e.idem),
	}
	if d.LastSeen != nil {
		m[attrLastSeen] = avNInt(d.LastSeen.UnixNano())
	}
	if len(d.Capabilities) > 0 {
		caps := make([]types.AttributeValue, 0, len(d.Capabilities))
		for _, c := range d.Capabilities {
			caps = append(caps, avNInt(int64(c)))
		}
		m[attrCapabilities] = &types.AttributeValueMemberL{Value: caps}
	}
	if d.AppInstall != "" {
		m[attrAppInstall] = avS(d.AppInstall)
	}
	return &types.AttributeValueMemberM{Value: m}
}

func rosterEntryFromItem(userID *commonpb.UserId, id uint32, av types.AttributeValue) (*rosterEntry, error) {
	m, ok := av.(*types.AttributeValueMemberM)
	if !ok {
		return nil, fmt.Errorf("roster entry %d: expected map, got %T", id, av)
	}
	regID, err := parseUint(m.Value[attrRegistrationID])
	if err != nil {
		return nil, fmt.Errorf("roster entry %d registration id: %w", id, err)
	}
	registeredAt, err := parseInt(m.Value[attrRegisteredAt])
	if err != nil {
		return nil, fmt.Errorf("roster entry %d registered_at: %w", id, err)
	}
	d := &e2ee.Device{
		Address:              e2ee.DeviceAddress{UserID: &commonpb.UserId{Value: append([]byte(nil), userID.Value...)}, DeviceID: id},
		RegistrationID:       uint32(regID),
		IdentityKey:          asB(m.Value[attrIdentityKey]),
		IdentityKeySignature: asB(m.Value[attrIdentityKeySig]),
		AccountKey:           asB(m.Value[attrAccountKey]),
		RegisteredAt:         time.Unix(0, registeredAt).UTC(),
	}
	if av, ok := m.Value[attrLastSeen]; ok {
		n, err := parseInt(av)
		if err != nil {
			return nil, fmt.Errorf("roster entry %d last_seen: %w", id, err)
		}
		t := time.Unix(0, n).UTC()
		d.LastSeen = &t
	}
	if l, ok := m.Value[attrCapabilities].(*types.AttributeValueMemberL); ok {
		for _, av := range l.Value {
			n, err := parseInt(av)
			if err != nil {
				return nil, fmt.Errorf("roster entry %d capability: %w", id, err)
			}
			d.Capabilities = append(d.Capabilities, e2eepb.Device_Capability(n))
		}
	}
	d.AppInstall = asS(m.Value[attrAppInstall])
	return &rosterEntry{device: d, nonce: asB(m.Value[attrNonce]), idem: asB(m.Value[attrIdempotencyKey])}, nil
}

func entriesOf(m *types.AttributeValueMemberM) map[string]types.AttributeValue {
	if m == nil {
		return nil
	}
	return m.Value
}

func putSignedPreKey(item map[string]types.AttributeValue, k *e2ee.SignedPreKey) {
	if k == nil {
		return
	}
	item[attrSignedPreKeyID] = avN(uint64(k.ID))
	item[attrSignedPreKeyPub] = avB(k.PublicKey)
	item[attrSignedPreKeySig] = avB(k.Signature)
}

func putLastResort(item map[string]types.AttributeValue, k *e2ee.KemSignedPreKey) {
	item[attrLastResortID] = avN(uint64(k.ID))
	item[attrLastResortPub] = avB(k.PublicKey)
	item[attrLastResortSig] = avB(k.Signature)
}

func keysFromItem(item map[string]types.AttributeValue) (*deviceKeys, error) {
	k := &deviceKeys{}
	var err error
	if k.version, err = parseUint(item[attrVersion]); err != nil {
		return nil, fmt.Errorf("keys version: %w", err)
	}
	if k.otk, err = otkPool.state(item); err != nil {
		return nil, err
	}
	if k.kem, err = kemPool.state(item); err != nil {
		return nil, err
	}
	spkID, err := parseUint(item[attrSignedPreKeyID])
	if err != nil {
		return nil, fmt.Errorf("keys signed prekey id: %w", err)
	}
	k.signedPreKey = &e2ee.SignedPreKey{ID: uint32(spkID), PublicKey: asB(item[attrSignedPreKeyPub]), Signature: asB(item[attrSignedPreKeySig])}
	lrID, err := parseUint(item[attrLastResortID])
	if err != nil {
		return nil, fmt.Errorf("keys last-resort id: %w", err)
	}
	k.lastResort = &e2ee.KemSignedPreKey{ID: uint32(lrID), PublicKey: asB(item[attrLastResortPub]), Signature: asB(item[attrLastResortSig])}
	return k, nil
}

// Batch helpers.

func batchPut(ctx context.Context, client *dynamodb.Client, table string, items []map[string]types.AttributeValue) error {
	requests := make([]types.WriteRequest, 0, len(items))
	for _, item := range items {
		requests = append(requests, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
	}
	return batchWrite(ctx, client, table, requests)
}

func batchDelete(ctx context.Context, client *dynamodb.Client, table string, keys []map[string]types.AttributeValue) error {
	requests := make([]types.WriteRequest, 0, len(keys))
	for _, key := range keys {
		requests = append(requests, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: key}})
	}
	return batchWrite(ctx, client, table, requests)
}

// batchWrite issues requests in batches of maxBatchWrite, one after another,
// resending what DynamoDB leaves unprocessed after a jittered wait (see
// maxBatchRounds), and fails the write when a batch does not drain within
// its rounds. Sequential because its callers write one partition (a
// device's pool, a sweep), whose per-second budget concurrent batches would
// only meet sooner.
func batchWrite(ctx context.Context, client *dynamodb.Client, table string, requests []types.WriteRequest) error {
	for start := 0; start < len(requests); start += maxBatchWrite {
		end := min(start+maxBatchWrite, len(requests))
		if err := batchWriteOne(ctx, client, table, requests[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// batchWriteConcurrently is batchWrite with its batches issued side by
// side, at most concurrency at a time, for requests spread over many
// partitions (a delivery's mailboxes). The first error ends the call; the
// batches already issued may have landed.
func batchWriteConcurrently(ctx context.Context, client *dynamodb.Client, table string, requests []types.WriteRequest, concurrency int) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for start := 0; start < len(requests); start += maxBatchWrite {
		batch := requests[start:min(start+maxBatchWrite, len(requests))]
		g.Go(func() error { return batchWriteOne(gctx, client, table, batch) })
	}
	return g.Wait()
}

// batchWriteOne issues one batch of at most maxBatchWrite requests and
// resends its unprocessed leftovers until they drain or its rounds run out.
func batchWriteOne(ctx context.Context, client *dynamodb.Client, table string, batch []types.WriteRequest) error {
	backoff := batchBackoffBase
	for round := 0; len(batch) > 0; round++ {
		if round == maxBatchRounds {
			return errBatchUnprocessed
		}
		if round > 0 {
			if err := sleepWithJitter(ctx, &backoff, batchBackoffMax); err != nil {
				return err
			}
		}
		out, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{table: batch},
		})
		if err != nil {
			return err
		}
		batch = out.UnprocessedItems[table]
	}
	return nil
}

// errBatchUnprocessed is a batch that DynamoDB kept returning partly
// unprocessed for every round it was given: the partition is throttled.
var errBatchUnprocessed = errors.New("e2ee/dynamodb: batch not processed within its rounds")

// Attribute helpers.

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avN(v uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
}
func avNFloat(v float64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatFloat(v, 'f', -1, 64)}
}
func avNInt(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}
func avB(v []byte) types.AttributeValue { return &types.AttributeValueMemberB{Value: v} }
func avBool(v bool) types.AttributeValue {
	return &types.AttributeValueMemberBOOL{Value: v}
}

func asS(av types.AttributeValue) string {
	if s, ok := av.(*types.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

func asB(av types.AttributeValue) []byte {
	if b, ok := av.(*types.AttributeValueMemberB); ok {
		return append([]byte(nil), b.Value...)
	}
	return nil
}

func asBool(av types.AttributeValue) bool {
	if b, ok := av.(*types.AttributeValueMemberBOOL); ok {
		return b.Value
	}
	return false
}

func parseInt(av types.AttributeValue) (int64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseInt(n.Value, 10, 64)
}

func parseFloat(av types.AttributeValue) (float64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number, got %T", av)
	}
	return strconv.ParseFloat(n.Value, 64)
}

func parseUint(av types.AttributeValue) (uint64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseUint(n.Value, 10, 64)
}

func isConditionalCheckFailed(err error) bool {
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}

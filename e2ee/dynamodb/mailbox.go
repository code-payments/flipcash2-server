package dynamodb

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	e2eepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/e2ee/v1"

	"github.com/code-payments/flipcash2-server/e2ee"
)

// mailboxDelivery is the part of one Deliver call bound for one mailbox.
type mailboxDelivery struct {
	pk string
	// indexes into the call's envelopes, in input order.
	indexes []int
	// first is the first position reserved for the call in this mailbox;
	// the envelopes take first, first+1, ... in input order.
	first uint64
}

func (s *store) Deliver(ctx context.Context, envelopes []*e2ee.Envelope) ([]*e2ee.Envelope, error) {
	if len(envelopes) == 0 {
		return nil, nil
	}

	// Resolve every recipient to its mailbox partition first: a recipient
	// that is not a current device fails the whole call before any write.
	rosters := make(map[string]*roster)
	byMailbox := make(map[string]*mailboxDelivery)
	var order []string
	for i, e := range envelopes {
		userKey := string(e.Recipient.UserID.GetValue())
		r, ok := rosters[userKey]
		if !ok {
			var err error
			if r, err = s.readRoster(ctx, e.Recipient.UserID); err != nil {
				return nil, err
			}
			rosters[userKey] = r
		}
		entry, ok := r.devices[e.Recipient.DeviceID]
		if !ok {
			return nil, e2ee.ErrDeviceNotFound
		}
		pk := mailboxPK(e.Recipient.UserID, entry.nonce)
		m, ok := byMailbox[pk]
		if !ok {
			m = &mailboxDelivery{pk: pk}
			byMailbox[pk] = m
			order = append(order, pk)
		}
		m.indexes = append(m.indexes, i)
	}

	// Reserve positions: one unconditional ADD per mailbox, side by side.
	// Nothing is read first and nothing can lose a race, so fan-in to one
	// mailbox never contends. The cost is that a call that fails after
	// this point leaves what it reserved as gaps, which the sequence
	// contract allows: a client that sees one drains and finds nothing
	// there.
	g, gctx := errgroup.WithContext(ctx)
	for _, pk := range order {
		m := byMailbox[pk]
		g.Go(func() error {
			var size int64
			for _, i := range m.indexes {
				size += int64(len(envelopes[i].Content))
			}
			last, err := s.reserve(gctx, m.pk, int64(len(m.indexes)), size)
			if err != nil {
				return err
			}
			m.first = last - uint64(len(m.indexes)) + 1
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	stored := make([]*e2ee.Envelope, len(envelopes))
	puts := make([]types.WriteRequest, 0, len(envelopes))
	for _, pk := range order {
		m := byMailbox[pk]
		for j, i := range m.indexes {
			stored[i] = s.placed(envelopes[i], m.first+uint64(j))
			puts = append(puts, envelopePut(pk, stored[i]))
		}
	}

	// The envelopes go in with BatchWriteItem, 25 to a call, the calls side
	// by side (deliverConcurrency): a DM, at most 9 mailboxes, is one call;
	// a full group's 250 envelopes are ten. A batch is a plain write, half
	// the cost of a transactional one, and nothing is atomic across
	// envelopes: a call that fails partway may have stored some of them,
	// which is Signal-Server's behaviour too (it inserts per device queue
	// with no transaction), and the caller's retry stores the whole call
	// again, at later positions, which the recipient absorbs. What a batch
	// cannot carry is a condition, so the puts are unconditional; the
	// positions were reserved a moment ago and nothing else writes at them.
	if err := batchWriteConcurrently(ctx, s.client, s.mailboxesTable, puts, deliverConcurrency); err != nil {
		return nil, err
	}
	return stored, nil
}

// reserve advances a mailbox's counter by n positions, and its size by n
// envelopes of size bytes, and returns the last position reserved. The
// first reservation creates the counter item.
func (s *store) reserve(ctx context.Context, pk string, n, size int64) (uint64, error) {
	out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                aws.String(s.mailboxesTable),
		Key:                      map[string]types.AttributeValue{attrPK: avS(pk), attrSK: avS(counterSK)},
		ExpressionAttributeNames: map[string]string{"#seq": attrSeq, "#count": attrCount, "#bytes": attrBytes},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":n":     avNInt(n),
			":bytes": avNInt(size),
		},
		UpdateExpression: aws.String("ADD #seq :n, #count :n, #bytes :bytes"),
		ReturnValues:     types.ReturnValueAllNew,
	})
	if err != nil {
		return 0, err
	}
	last, err := parseUint(out.Attributes[attrSeq])
	if err != nil {
		return 0, fmt.Errorf("mailbox counter: %w", err)
	}
	return last, nil
}

// placed is e at its assigned position, with its id.
func (s *store) placed(e *e2ee.Envelope, sequence uint64) *e2ee.Envelope {
	out := e.Clone()
	out.Sequence = sequence
	out.ID = e2ee.NewEnvelopeID(sequence)
	return out
}

// envelopePut is the batch put of a placed envelope. Unconditional, since a
// batch carries no condition; the position was reserved for this envelope
// and nothing else writes at it.
func envelopePut(pk string, e *e2ee.Envelope) types.WriteRequest {
	return types.WriteRequest{PutRequest: &types.PutRequest{Item: envelopeItem(pk, e)}}
}

// sleepWithJitter waits backoff plus jitter, doubling backoff for the next
// wait up to ceiling, and returns early if ctx ends. The batch loops' waits
// are sized to per-second throttling (see maxBatchRounds).
func sleepWithJitter(ctx context.Context, backoff *time.Duration, ceiling time.Duration) error {
	wait := *backoff + time.Duration(rand.Int64N(int64(*backoff)))
	*backoff = min(*backoff*2, ceiling)
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *store) GetEnvelopes(ctx context.Context, address e2ee.DeviceAddress, afterSequence uint64, limit int, maxBytes int64) ([]*e2ee.Envelope, error) {
	entry, err := s.entry(ctx, address)
	if err != nil {
		return nil, err
	}
	pk := mailboxPK(address.UserID, entry.nonce)

	// The envelope range after the cursor, ascending, one server page at a
	// time until limit or maxBytes is met: a page of large envelopes can
	// exceed a Query's 1 MB result before it reaches the limit, and the
	// filter (TTL deletion is lazy, so a lapsed envelope may still be
	// there) can leave a page short. The byte budget is checked as items
	// arrive, so a Query page that crosses it is the last one read rather
	// than the first of several read for nothing.
	input := &dynamodb.QueryInput{
		TableName:                aws.String(s.mailboxesTable),
		KeyConditionExpression:   aws.String("pk = :pk AND sk BETWEEN :lo AND :hi"),
		FilterExpression:         aws.String("attribute_not_exists(#exp) OR #exp > :now"),
		ExpressionAttributeNames: map[string]string{"#exp": attrExpiresAt},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":  avS(pk),
			":lo":  avS(envelopeSK(afterSequence + 1)),
			":hi":  avS(envelopeSK(^uint64(0))),
			":now": avNInt(time.Now().Unix()),
		},
		ConsistentRead: aws.Bool(true),
	}
	var out []*e2ee.Envelope
	var bytes int64
	for {
		if limit > 0 {
			input.Limit = aws.Int32(int32(limit - len(out)))
		}
		page, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			e, err := envelopeFromItem(item)
			if err != nil {
				return nil, err
			}
			e.Recipient = address.Clone()
			out = append(out, e)
			if bytes += int64(len(e.Content)); maxBytes > 0 && bytes > maxBytes {
				return out, nil // The envelope that crossed the budget is the page's last.
			}
		}
		if len(page.LastEvaluatedKey) == 0 || (limit > 0 && len(out) >= limit) {
			return out, nil
		}
		input.ExclusiveStartKey = page.LastEvaluatedKey
	}
}

func (s *store) AckEnvelopes(ctx context.Context, address e2ee.DeviceAddress, ids [][]byte) ([]*e2ee.Envelope, error) {
	entry, err := s.entry(ctx, address)
	if err != nil {
		return nil, err
	}
	pk := mailboxPK(address.UserID, entry.nonce)

	// Each id names its position, so each acknowledgement is one keyed
	// delete, conditioned on the id's random tail so a fabricated id (or a
	// stale one under a reused position, which cannot happen since
	// positions are never reused, but costs nothing to refuse) removes
	// nothing. The old item comes back with the delete. Independent, so
	// they run concurrently, bounded.
	results := make([]*e2ee.Envelope, len(ids))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(ackConcurrency)
	launched := make(map[string]struct{}, len(ids))
	for i, id := range ids {
		seq, ok := e2ee.EnvelopeSequence(id)
		if !ok {
			continue
		}
		// An id named twice is deleted once, in its first slot, so the
		// result keeps input order whichever goroutine would have won.
		if _, dup := launched[string(id)]; dup {
			continue
		}
		launched[string(id)] = struct{}{}
		g.Go(func() error {
			out, err := s.client.DeleteItem(gctx, &dynamodb.DeleteItemInput{
				TableName:                aws.String(s.mailboxesTable),
				Key:                      map[string]types.AttributeValue{attrPK: avS(pk), attrSK: avS(envelopeSK(seq))},
				ExpressionAttributeNames: map[string]string{"#id": attrID},
				ConditionExpression:      aws.String("#id = :id"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":id": avB(id),
				},
				ReturnValues: types.ReturnValueAllOld,
			})
			if err != nil {
				if isConditionalCheckFailed(err) {
					return nil // Not in the mailbox: already acknowledged, or never there.
				}
				return err
			}
			e, err := envelopeFromItem(out.Attributes)
			if err != nil {
				return err
			}
			e.Recipient = address.Clone()
			results[i] = e
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	acked := make([]*e2ee.Envelope, 0, len(results))
	var size int64
	for _, e := range results {
		if e != nil {
			acked = append(acked, e)
			size += int64(len(e.Content))
		}
	}

	// What the call removed comes off the mailbox's size, folded into one
	// write per call and best effort: the counters bound the mailbox from
	// above (see table.go) and an ack that could not subtract leaves them
	// high, which is the safe direction. The envelopes are gone either way.
	if len(acked) > 0 {
		_, _ = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                aws.String(s.mailboxesTable),
			Key:                      map[string]types.AttributeValue{attrPK: avS(pk), attrSK: avS(counterSK)},
			ExpressionAttributeNames: map[string]string{"#count": attrCount, "#bytes": attrBytes},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":n":     avNInt(-int64(len(acked))),
				":bytes": avNInt(-size),
			},
			UpdateExpression: aws.String("ADD #count :n, #bytes :bytes"),
		})
	}
	return acked, nil
}

// Keys and encodings.

func envelopeSK(sequence uint64) string {
	return envelopeSKPrefix + fmt.Sprintf("%020d", sequence)
}

func envelopeItem(pk string, e *e2ee.Envelope) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		attrPK:           avS(pk),
		attrSK:           avS(envelopeSK(e.Sequence)),
		attrID:           avB(e.ID),
		attrChat:         avB(e.ChatID.GetValue()),
		attrType:         avNInt(int64(e.Type)),
		attrServerTS:     avNInt(e.ServerTS.UnixNano()),
		attrLowPriority:  avBool(e.LowPriority),
		attrContentHint:  avNInt(int64(e.ContentHint)),
		attrWantsReceipt: avBool(e.WantsDeliveryReceipt),
	}
	if !e.ExpiresAt.IsZero() {
		item[attrExpiresAt] = avNInt(e.ExpiresAt.Unix())
	}
	if e.Ephemeral {
		item[attrEphemeral] = avBool(true)
	}
	if len(e.Content) > 0 {
		item[attrContent] = avB(e.Content)
	}
	if e.Source != nil {
		item[attrSourceUser] = avB(e.Source.UserID.GetValue())
		item[attrSourceDevice] = avN(uint64(e.Source.DeviceID))
	}
	if e.ClientTS != nil {
		item[attrClientTS] = avNInt(e.ClientTS.UnixNano())
	}
	if e.DeliveryReceipt != nil {
		acks := make([]types.AttributeValue, 0, len(e.DeliveryReceipt.Acknowledgements))
		for _, a := range e.DeliveryReceipt.Acknowledgements {
			tss := make([]types.AttributeValue, 0, len(a.ClientTS))
			for _, ts := range a.ClientTS {
				tss = append(tss, avNInt(ts.UnixNano()))
			}
			acks = append(acks, &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				attrAckRecipient: addressItem(a.Recipient),
				attrAckClientTS:  &types.AttributeValueMemberL{Value: tss},
			}})
		}
		item[attrReceipt] = &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			attrAcks: &types.AttributeValueMemberL{Value: acks},
		}}
	}
	if e.SourceDevice != nil {
		// The stamp is a roster entry without its nonce or key; the
		// handler has already dropped last_seen and the app install.
		stamp := &rosterEntry{device: e.SourceDevice.Clone()}
		stamp.device.LastSeen = nil
		m := rosterEntryItem(stamp).(*types.AttributeValueMemberM)
		delete(m.Value, attrNonce)
		delete(m.Value, attrIdempotencyKey)
		m.Value[attrAddressUser] = avB(e.SourceDevice.Address.UserID.GetValue())
		m.Value[attrAddressDevice] = avN(uint64(e.SourceDevice.Address.DeviceID))
		item[attrSourceRecord] = m
	}
	return item
}

// envelopeFromItem decodes an envelope item. The recipient is the mailbox's
// address, not stored on the item; the caller sets it.
func envelopeFromItem(item map[string]types.AttributeValue) (*e2ee.Envelope, error) {
	seq, err := sequenceFromSK(asS(item[attrSK]))
	if err != nil {
		return nil, err
	}
	typ, err := parseInt(item[attrType])
	if err != nil {
		return nil, fmt.Errorf("envelope type: %w", err)
	}
	serverTS, err := parseInt(item[attrServerTS])
	if err != nil {
		return nil, fmt.Errorf("envelope server_ts: %w", err)
	}
	hint, err := parseInt(item[attrContentHint])
	if err != nil {
		return nil, fmt.Errorf("envelope content_hint: %w", err)
	}

	e := &e2ee.Envelope{
		ID:                   asB(item[attrID]),
		Sequence:             seq,
		ChatID:               &commonpb.ChatId{Value: asB(item[attrChat])},
		Type:                 e2eepb.Envelope_Type(typ),
		Content:              asB(item[attrContent]),
		ServerTS:             time.Unix(0, serverTS).UTC(),
		LowPriority:          asBool(item[attrLowPriority]),
		ContentHint:          e2eepb.Envelope_ContentHint(hint),
		WantsDeliveryReceipt: asBool(item[attrWantsReceipt]),
		Ephemeral:            asBool(item[attrEphemeral]),
	}
	if av, ok := item[attrExpiresAt]; ok {
		n, err := parseInt(av)
		if err != nil {
			return nil, fmt.Errorf("envelope expires_at: %w", err)
		}
		e.ExpiresAt = time.Unix(n, 0).UTC()
	}
	if av, ok := item[attrSourceUser]; ok {
		deviceID, err := parseUint(item[attrSourceDevice])
		if err != nil {
			return nil, fmt.Errorf("envelope source device: %w", err)
		}
		e.Source = &e2ee.DeviceAddress{UserID: &commonpb.UserId{Value: asB(av)}, DeviceID: uint32(deviceID)}
	}
	if av, ok := item[attrClientTS]; ok {
		n, err := parseInt(av)
		if err != nil {
			return nil, fmt.Errorf("envelope client_ts: %w", err)
		}
		t := time.Unix(0, n).UTC()
		e.ClientTS = &t
	}
	if av, ok := item[attrReceipt].(*types.AttributeValueMemberM); ok {
		r := &e2ee.DeliveryReceipt{}
		if l, ok := av.Value[attrAcks].(*types.AttributeValueMemberL); ok {
			for _, ackAV := range l.Value {
				m, ok := ackAV.(*types.AttributeValueMemberM)
				if !ok {
					return nil, fmt.Errorf("receipt acknowledgement: expected map, got %T", ackAV)
				}
				recipient, err := addressFromItem(m.Value[attrAckRecipient])
				if err != nil {
					return nil, err
				}
				ack := e2ee.Acknowledgement{Recipient: recipient}
				if tss, ok := m.Value[attrAckClientTS].(*types.AttributeValueMemberL); ok {
					for _, tsAV := range tss.Value {
						n, err := parseInt(tsAV)
						if err != nil {
							return nil, fmt.Errorf("receipt client_ts: %w", err)
						}
						ack.ClientTS = append(ack.ClientTS, time.Unix(0, n).UTC())
					}
				}
				r.Acknowledgements = append(r.Acknowledgements, ack)
			}
		}
		e.DeliveryReceipt = r
	}
	if av, ok := item[attrSourceRecord].(*types.AttributeValueMemberM); ok {
		a, err := addressFromItem(av)
		if err != nil {
			return nil, err
		}
		entry, err := rosterEntryFromItem(a.UserID, a.DeviceID, av)
		if err != nil {
			return nil, err
		}
		e.SourceDevice = entry.device
	}
	return e, nil
}

func addressItem(a e2ee.DeviceAddress) types.AttributeValue {
	return &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
		attrAddressUser:   avB(a.UserID.GetValue()),
		attrAddressDevice: avN(uint64(a.DeviceID)),
	}}
}

func addressFromItem(av types.AttributeValue) (e2ee.DeviceAddress, error) {
	m, ok := av.(*types.AttributeValueMemberM)
	if !ok {
		return e2ee.DeviceAddress{}, fmt.Errorf("expected address map, got %T", av)
	}
	deviceID, err := parseUint(m.Value[attrAddressDevice])
	if err != nil {
		return e2ee.DeviceAddress{}, fmt.Errorf("address device: %w", err)
	}
	return e2ee.DeviceAddress{UserID: &commonpb.UserId{Value: asB(m.Value[attrAddressUser])}, DeviceID: uint32(deviceID)}, nil
}

func sequenceFromSK(sk string) (uint64, error) {
	if !strings.HasPrefix(sk, envelopeSKPrefix) {
		return 0, fmt.Errorf("malformed envelope sk %q", sk)
	}
	seq, err := strconv.ParseUint(sk[len(envelopeSKPrefix):], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("malformed envelope sk %q: %w", sk, err)
	}
	return seq, nil
}

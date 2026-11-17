package dynamodb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	"github.com/code-payments/flipcash2-server/paymentrequest"
)

// The payment request store is one table:
//
//	payment_requests  pk = "req#<request ID hex>", sk = requestSK
//	                  (#meta), one item per request: type (N, see the type
//	                  values below), creator and chat (B; the chat is the
//	                  16-byte group chat ID), created_at (epoch nanos,
//	                  exactly: a creator's share is paid at created_at to the
//	                  nanosecond, which PaymentRequest.Validate checks),
//	                  details (B: the type's message marshalled: for a split
//	                  the paymentrequestpb.Split, shares' paid included,
//	                  rewritten whole whenever a share is paid; for a
//	                  collection the paymentrequestpb.Collection, written at
//	                  creation and never again), status (N, see the status
//	                  values below), version (N), and closed_at (epoch nanos)
//	                  once the request is final. A split also has intent_ids
//	                  (M: each debtor's user ID, hex, to their share's
//	                  payment intent ID, B; written at creation and never
//	                  again), and a collection payer_count (N: how many
//	                  members have contributed, 0 at creation).
//
// A split is the only item in its partition. A collection keeps, beside its
// #meta, two more kinds of item, read in the same partition and written with
// #meta in one transaction where they change together:
//
//	payer#<user ID hex>     one per member issued a payment intent ID
//	                        (GetOrCreatePayerIntent): intent_id (B), and payer
//	                        (B, the marshalled paymentrequestpb.Payer, its
//	                        status the contribution's Paid) once they have
//	                        contributed. It is written alone when the ID is
//	                        issued, and that moves nothing on #meta: a member
//	                        holding an ID is not a Payer until payer is set.
//	paid#<inv>#<user hex>   one per contribution, the paid section of
//	                        GetPayers: payer (B), as on payer#. <inv> is the
//	                        contribution's paid_at as 16 lowercase hex digits
//	                        of ^sortableNanos(paid_at), so the sort keys read
//	                        ascending are paid_at descending, then user ID
//	                        ascending (hex keeps byte order), the order
//	                        paymentrequest.PaidBefore defines. Every key is the
//	                        same length, and a page resumes strictly after the
//	                        cursor's Payer as the Query's ExclusiveStartKey.
//
// RecordContribution is one TransactWriteItems over all three: #meta
// (version + 1 and payer_count + 1, conditioned on the version and status it
// read), payer# (payer set, conditioned on its intent_id being the payment's
// and payer being unset), and the paid# put (conditioned on not existing).
// Any condition lost cancels all three, and the change is decided again from
// a fresh read, which tells a contribution already recorded (a no-op) from a
// race with another writer of #meta (retried). The close and the expiry
// worker update #meta's own attributes in place (UpdateItem SET and REMOVE,
// never a whole-item put), so they leave payer_count alone.
//
// Neither payer# nor paid# items carry chat, the section sort attributes or
// the queue attributes, so they never reach the indexes below. The sort key
// is the table's from the start, since a key schema cannot be changed once
// the table exists; any further item type kept in a request's partition takes
// its own sk prefix and likewise omits the index attributes.
//
// The table is deployed as payment_requests_v2. The first table keyed a
// request on its raw binary ID; the prefixed string key leaves room for
// partitions of other kinds beside requests', and since a key schema cannot
// change, it took a new table, with the first table's items copied across.
//
// type says which of the request's type fields is set, and so which message
// details holds; it is the only thing that says so, since every type stores its
// message in the same attribute. Every reader refuses a value it does not know
// rather than guess: a server that predates a type fails reads of it instead
// of serving it as a split.
//
// A chat's requests are listed (GetChatPage) from two sparse GSIs, one per
// section, each hashed on chat so a page is one key-range Query:
//
//	by_chat_open      ranged on open_sort, present exactly while the request
//	                  is stored open: ends_at (sortableNanos) ‖ ID, read
//	                  ascending.
//	by_chat_resolved  ranged on resolved_sort, present exactly once the
//	                  request is final: closed_at (sortableNanos) ‖ ID, read
//	                  descending.
//
// The sort keys carry the ID after the time so that they are unique and the
// order total: many requests can share an ends_at or a closed_at, and a cursor
// (paymentrequest.PageCursor, the last request's time and ID) maps to an
// exclusive bound on the sort key (> in the open index, < in the resolved one)
// that resumes strictly after the request it names, even inside a tie. Every
// write that makes a request final REMOVEs open_sort and SETs resolved_sort in
// the same update, so a request is in exactly one of the two indexes at any
// moment. Both indexes project the whole item, so a page needs no further
// reads; they are eventually consistent, as the Store allows.
//
// The expiry queue is a third sparse GSI over the same table, exactly as
// poll's tally queue is (see poll/dynamodb): the queue attributes
// (expiry_queue, expiry_due_at, expiry_attempts) are present exactly while a
// request is stored open. Create sets them, in the put that creates the
// request, and the write that makes it final (its last share paid, a close,
// or Expire) removes them, so a request is enqueued and dequeued atomically.
//
// A collection with no ends_at is created with neither open_sort nor the
// queue attributes: while open it is in no index, and never expires. Its
// close sets resolved_sort like any other's, so it is then listed resolved.
//
// Any other item type ever written into this table must omit chat and the
// section sort attributes (open_sort, resolved_sort) and the queue attributes,
// or it leaks into the indexes.
//
// Every change to a request is a read, a decision in Go by the same model
// helpers the memory store uses (PaymentRequest.Share, IsOpenAt,
// AllSharesPaid, Status.IsFinal, EndsAt), and one UpdateItem conditioned on
// the version and status it read (a contribution's is the transaction
// above). The status is in the condition because
// Expire changes it without moving the version. A lost condition retries from
// the item the failure returns (strongly consistent, it is the item as it
// stood), after a short jittered, doubling wait, within a bounded number of
// attempts: concurrent payments of a request's shares all contend on its one
// item.
const (
	// openIndex and resolvedIndex are the per-chat section GSIs backing
	// GetChatPage.
	openIndex     = "by_chat_open"
	resolvedIndex = "by_chat_resolved"

	// expiryQueueIndex is the sparse GSI backing GetDueForExpiry.
	expiryQueueIndex = "expiry_queue"

	// expiryQueuePK is the expiry queue's one index partition. Like poll's
	// tally queue it is not sharded: each request costs it a handful of writes
	// in its life (enqueue, a claim per attempt, dequeue), far below what one
	// GSI partition takes at any rate of request creation in sight. Should
	// that change, it is sharded by filing each request under one of several
	// hash keys derived from its ID, with the worker querying each.
	expiryQueuePK = "queue#expiry"

	// requestKeyPrefix prefixes a request's partition key, so the table can
	// hold partitions of other kinds beside requests' without their keys
	// colliding.
	requestKeyPrefix = "req#"

	// requestSK is the sort key of a request's own item.
	requestSK = "#meta"

	// payerSKPrefix and contributionSKPrefix prefix the sort keys of a
	// collection's payer# and paid# items.
	payerSKPrefix        = "payer#"
	contributionSKPrefix = "paid#"

	attrPK             = "pk"              // S, requestPK of the request ID
	attrSK             = "sk"              // S, requestSK for the request itself
	attrType           = "type"            // N, see the type values
	attrCreator        = "creator"         // B
	attrChat           = "chat"            // B, the group chat ID (GSI hash of both section indexes)
	attrCreatedAt      = "created_at"      // N, Unix nanos
	attrDetails        = "details"         // B, the type's marshalled message
	attrIntentIDs      = "intent_ids"      // M, hex debtor user ID → B payment intent ID
	attrStatus         = "status"          // N, see the status values
	attrVersion        = "version"         // N
	attrClosedAt       = "closed_at"       // N, Unix nanos; present only once final
	attrOpenSort       = "open_sort"       // B, sortKey(ends_at, ID); present only while open
	attrResolvedSort   = "resolved_sort"   // B, sortKey(closed_at, ID); present only once final
	attrExpiryQueue    = "expiry_queue"    // S, expiryQueuePK (GSI hash); present only while queued
	attrExpiryDueAt    = "expiry_due_at"   // N, Unix nanos (GSI range); when the request is next due
	attrExpiryAttempts = "expiry_attempts" // N, failed attempts so far
	attrPayerCount     = "payer_count"     // N, a collection's contributions; #meta only
	attrIntentID       = "intent_id"       // B, a payer's issued intent ID; payer# only
	attrPayer          = "payer"           // B, marshalled Payer; payer# once contributed, and paid#

	// maxWriteAttempts bounds how many times one change is tried against
	// concurrent writers before it fails, and writeBackoffBase and
	// writeBackoffMax shape the wait between tries.
	maxWriteAttempts = 32
	writeBackoffBase = 2 * time.Millisecond
	writeBackoffMax  = 100 * time.Millisecond
)

// The stored type values, by stable number: which of PaymentRequest's type
// fields is set, and so which message details holds. A new type takes the next
// number.
const (
	typeSplit      = 1
	typeCollection = 2
)

// The stored status values, by stable number.
const (
	statusOpen      = 0
	statusCompleted = 1
	statusClosed    = 2
	statusExpired   = 3
)

// expressionNames aliases the attributes the fixed expressions here name, so
// none can collide with a DynamoDB reserved word.
var expressionNames = map[string]string{
	"#pk":       attrPK,
	"#q":        attrExpiryQueue,
	"#due":      attrExpiryDueAt,
	"#attempts": attrExpiryAttempts,
}

// names returns the subset of expressionNames an expression uses: DynamoDB
// refuses a name an expression does not use.
func names(aliases ...string) map[string]string {
	out := make(map[string]string, len(aliases))
	for _, alias := range aliases {
		out[alias] = expressionNames[alias]
	}
	return out
}

// errLostRace is returned by casWrite when the request changed since it was
// read.
var errLostRace = errors.New("payment request changed since it was read")

type store struct {
	client *dynamodb.Client
	table  string
}

// NewInDynamoDB returns a paymentrequest.Store backed by the given DynamoDB
// table. Use CreateTables to provision it.
func NewInDynamoDB(client *dynamodb.Client, table string) paymentrequest.Store {
	return &store{
		client: client,
		table:  table,
	}
}

func (s *store) Create(ctx context.Context, r *paymentrequest.PaymentRequest) error {
	if err := r.Validate(); err != nil {
		return err
	}

	item := map[string]types.AttributeValue{
		attrPK:        avS(requestPK(r.ID)),
		attrSK:        avS(requestSK),
		attrCreator:   avB(r.Creator.Value),
		attrChat:      avB(r.ChatID.Value),
		attrCreatedAt: avNInt(r.CreatedAt.UnixNano()),
		attrStatus:    avNInt(statusOpen),
		attrVersion:   avNInt(1),
	}
	switch {
	case r.Split != nil:
		details, err := proto.Marshal(r.Split)
		if err != nil {
			return fmt.Errorf("marshalling split: %w", err)
		}
		intentIDs := make(map[string]types.AttributeValue, len(r.PaymentIntentIDs))
		for userID, intentID := range r.PaymentIntentIDs {
			intentIDs[hex.EncodeToString([]byte(userID))] = avB(intentID)
		}
		item[attrType] = avNInt(typeSplit)
		item[attrDetails] = avB(details)
		item[attrIntentIDs] = &types.AttributeValueMemberM{Value: intentIDs}
	case r.Collection != nil:
		details, err := proto.Marshal(r.Collection)
		if err != nil {
			return fmt.Errorf("marshalling collection: %w", err)
		}
		item[attrType] = avNInt(typeCollection)
		item[attrDetails] = avB(details)
		item[attrPayerCount] = avNInt(0)
	}
	// A request with no ends_at is in neither the open section nor the expiry
	// queue while open.
	if endsAt, ok := r.EndsAt(); ok {
		item[attrOpenSort] = avB(sortKey(endsAt, r.ID.Value))
		item[attrExpiryQueue] = avS(expiryQueuePK)
		item[attrExpiryDueAt] = avNInt(paymentrequest.ExpiryDueAt(r).UnixNano())
		item[attrExpiryAttempts] = avNInt(0)
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                aws.String(s.table),
		Item:                     item,
		ConditionExpression:      aws.String("attribute_not_exists(#pk)"),
		ExpressionAttributeNames: names("#pk"),
	})
	if isConditionFailed(err) {
		return paymentrequest.ErrExists
	}
	return err
}

func (s *store) Get(ctx context.Context, id *paymentrequestpb.PaymentRequestId) (*paymentrequest.PaymentRequest, error) {
	item, err := s.read(ctx, id)
	if err != nil {
		return nil, err
	}
	return requestFromItem(item)
}

func (s *store) GetChatPage(ctx context.Context, chatID *commonpb.ChatId, section paymentrequest.Section, cursor *paymentrequest.PageCursor, limit int) ([]*paymentrequest.PaymentRequest, error) {
	// Each section is its own index, read in its order: a cursor is an
	// exclusive bound on the sort key in the direction of the read.
	var (
		index    string
		sortAttr string
		forward  bool
		afterOp  string
	)
	switch section {
	case paymentrequest.SectionOpen:
		index, sortAttr, forward, afterOp = openIndex, attrOpenSort, true, ">"
	case paymentrequest.SectionResolved:
		index, sortAttr, forward, afterOp = resolvedIndex, attrResolvedSort, false, "<"
	default:
		return nil, fmt.Errorf("unknown section %s", section)
	}

	condition := "#chat = :chat"
	exprNames := map[string]string{"#chat": attrChat}
	values := map[string]types.AttributeValue{":chat": avB(chatID.GetValue())}
	if cursor != nil {
		condition += " AND #sort " + afterOp + " :after"
		exprNames["#sort"] = sortAttr
		values[":after"] = avB(sortKey(cursor.At, cursor.ID))
	}

	var (
		page     []*paymentrequest.PaymentRequest
		startKey map[string]types.AttributeValue
	)
	for {
		input := &dynamodb.QueryInput{
			TableName:                 aws.String(s.table),
			IndexName:                 aws.String(index),
			KeyConditionExpression:    aws.String(condition),
			ExpressionAttributeNames:  exprNames,
			ExpressionAttributeValues: values,
			ScanIndexForward:          aws.Bool(forward),
			ExclusiveStartKey:         startKey,
		}
		if limit > 0 {
			input.Limit = aws.Int32(int32(limit - len(page)))
		}
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			r, err := requestFromItem(item)
			if err != nil {
				return nil, err
			}
			page = append(page, r)
		}
		if len(out.LastEvaluatedKey) == 0 || (limit > 0 && len(page) >= limit) {
			return page, nil
		}
		startKey = out.LastEvaluatedKey
	}
}

func (s *store) PayShare(ctx context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId, paid *paymentrequestpb.Paid, now time.Time) (*paymentrequest.PaymentRequest, bool, bool, error) {
	if err := paymentrequest.ValidatePaidShare(paid); err != nil {
		return nil, false, false, err
	}

	item, err := s.read(ctx, id)
	if err != nil {
		return nil, false, false, err
	}
	backoff := writeBackoffBase
	for range maxWriteAttempts {
		r, err := requestFromItem(item)
		if err != nil {
			return nil, false, false, err
		}

		// The rules are the memory store's, decided on the same helpers.
		share := r.Share(userID)
		if share == nil {
			return nil, false, false, paymentrequest.ErrNotAShare
		}
		if err := paymentrequest.ValidatePaymentIntent(r, userID, paid); err != nil {
			return nil, false, false, err
		}
		if !paymentrequest.PaidShareSupersedes(share.Paid, paid) {
			return r, false, false, nil
		}
		if paid.GetManual() != nil && !r.IsOpenAt(now) {
			return nil, false, false, paymentrequest.ErrNotOpen
		}

		readVersion, readStatus := r.Version, r.Status
		share.Paid = proto.Clone(paid).(*paymentrequestpb.Paid)
		r.Version++
		details, err := proto.Marshal(r.Split)
		if err != nil {
			return nil, false, false, fmt.Errorf("marshalling split: %w", err)
		}
		ch := &change{set: map[string]types.AttributeValue{
			attrDetails: avB(details),
			attrVersion: avNInt(int64(r.Version)),
		}}
		completed := r.Status == paymentrequest.StatusOpen && r.AllSharesPaid()
		if completed {
			closedAt := paid.PaidAt.AsTime()
			r.Status = paymentrequest.StatusCompleted
			r.ClosedAt = &closedAt
			ch.finalize(r)
		}

		item, err = s.casWrite(ctx, r.ID, readVersion, readStatus, ch)
		if err == nil {
			return r, true, completed, nil
		}
		if !errors.Is(err, errLostRace) {
			return nil, false, false, err
		}
		if err := wait(ctx, &backoff); err != nil {
			return nil, false, false, err
		}
	}
	return nil, false, false, fmt.Errorf("pay share exhausted retries for payment request %s", hex.EncodeToString(id.GetValue()))
}

func (s *store) Close(ctx context.Context, id *paymentrequestpb.PaymentRequestId, now time.Time) (*paymentrequest.PaymentRequest, bool, error) {
	item, err := s.read(ctx, id)
	if err != nil {
		return nil, false, err
	}
	backoff := writeBackoffBase
	for range maxWriteAttempts {
		r, err := requestFromItem(item)
		if err != nil {
			return nil, false, err
		}
		if !r.IsOpenAt(now) {
			return r, false, nil
		}

		readVersion, readStatus := r.Version, r.Status
		closedAt := now
		r.Status = paymentrequest.StatusClosed
		r.ClosedAt = &closedAt
		r.Version++
		ch := &change{set: map[string]types.AttributeValue{
			attrVersion: avNInt(int64(r.Version)),
		}}
		ch.finalize(r)

		item, err = s.casWrite(ctx, r.ID, readVersion, readStatus, ch)
		if err == nil {
			return r, true, nil
		}
		if !errors.Is(err, errLostRace) {
			return nil, false, err
		}
		if err := wait(ctx, &backoff); err != nil {
			return nil, false, err
		}
	}
	return nil, false, fmt.Errorf("close exhausted retries for payment request %s", hex.EncodeToString(id.GetValue()))
}

func (s *store) Expire(ctx context.Context, id *paymentrequestpb.PaymentRequestId) (bool, error) {
	item, err := s.read(ctx, id)
	if err != nil {
		return false, err
	}
	backoff := writeBackoffBase
	for range maxWriteAttempts {
		r, err := requestFromItem(item)
		if err != nil {
			return false, err
		}
		if r.Status.IsFinal() {
			// Every write that makes a request final dequeues it, so this is
			// only ever a request expired, closed or completed already; should
			// one somehow still be queued, it is dequeued here.
			if _, queued := item[attrExpiryQueue]; queued {
				if err := s.dequeue(ctx, id); err != nil {
					return false, err
				}
			}
			return false, nil
		}

		endsAt, ok := r.EndsAt()
		if !ok {
			// A request with no ends_at never expires, and is never queued;
			// should one somehow be, it is dequeued here.
			if _, queued := item[attrExpiryQueue]; queued {
				if err := s.dequeue(ctx, id); err != nil {
					return false, err
				}
			}
			return false, nil
		}

		// The version does not move: expiry is the clock's, and a reader was
		// shown the request expired at this version already.
		readVersion, readStatus := r.Version, r.Status
		r.Status = paymentrequest.StatusExpired
		r.ClosedAt = &endsAt
		ch := &change{set: map[string]types.AttributeValue{}}
		ch.finalize(r)

		item, err = s.casWrite(ctx, r.ID, readVersion, readStatus, ch)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, errLostRace) {
			return false, err
		}
		if err := wait(ctx, &backoff); err != nil {
			return false, err
		}
	}
	return false, fmt.Errorf("expire exhausted retries for payment request %s", hex.EncodeToString(id.GetValue()))
}

func (s *store) GetDueForExpiry(ctx context.Context, asOf time.Time, limit int) ([]*paymentrequest.ExpiryTask, error) {
	input := &dynamodb.QueryInput{
		TableName:                aws.String(s.table),
		IndexName:                aws.String(expiryQueueIndex),
		KeyConditionExpression:   aws.String("#q = :q AND #due <= :asOf"),
		ExpressionAttributeNames: names("#q", "#due"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":q":    avS(expiryQueuePK),
			":asOf": avNInt(asOf.UnixNano()),
		},
		// The range key is the due time, so the query is already soonest-first.
	}
	if limit > 0 {
		input.Limit = aws.Int32(int32(limit))
	}
	out, err := s.client.Query(ctx, input)
	if err != nil {
		return nil, err
	}

	tasks := make([]*paymentrequest.ExpiryTask, 0, len(out.Items))
	for _, item := range out.Items {
		attempts, err := parseInt(item[attrExpiryAttempts])
		if err != nil {
			return nil, err
		}
		dueAt, err := parseInt(item[attrExpiryDueAt])
		if err != nil {
			return nil, err
		}
		id, err := requestIDFromPK(item)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, &paymentrequest.ExpiryTask{
			ID:            id,
			Attempts:      uint32(attempts),
			NextAttemptAt: time.Unix(0, dueAt).UTC(),
		})
	}
	return tasks, nil
}

func (s *store) GetExpiryQueueStats(ctx context.Context, asOf time.Time) (*paymentrequest.ExpiryQueueStats, error) {
	// One page of the due range, soonest first, projecting only the due time:
	// its first item is the most overdue request, and its length the depth,
	// capped by the page limit. Requests not yet due sit past the range and
	// are never read.
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                aws.String(s.table),
		IndexName:                aws.String(expiryQueueIndex),
		KeyConditionExpression:   aws.String("#q = :q AND #due <= :asOf"),
		ProjectionExpression:     aws.String("#due"),
		ExpressionAttributeNames: names("#q", "#due"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":q":    avS(expiryQueuePK),
			":asOf": avNInt(asOf.UnixNano()),
		},
		Limit: aws.Int32(paymentrequest.MaxExpiryQueueStatsDepth),
	})
	if err != nil {
		return nil, err
	}

	stats := &paymentrequest.ExpiryQueueStats{DueDepth: uint64(len(out.Items))}
	if len(out.Items) > 0 {
		nanos, err := parseInt(out.Items[0][attrExpiryDueAt])
		if err != nil {
			return nil, err
		}
		stats.OldestDueAt = time.Unix(0, nanos).UTC()
	}
	return stats, nil
}

func (s *store) ClaimForExpiry(ctx context.Context, id *paymentrequestpb.PaymentRequestId, asOf, until time.Time) (bool, error) {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(s.table),
		Key:              requestKey(id),
		UpdateExpression: aws.String("SET #due = :until"),
		// Claim only a request that is still queued and still due: a dequeued
		// request was made final, and one pushed into the future was claimed
		// or delayed by another server first.
		ConditionExpression:      aws.String("attribute_exists(#q) AND #due <= :asOf"),
		ExpressionAttributeNames: names("#q", "#due"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":until": avNInt(until.UnixNano()),
			":asOf":  avNInt(asOf.UnixNano()),
		},
	})
	if isConditionFailed(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *store) DelayExpiry(ctx context.Context, id *paymentrequestpb.PaymentRequestId, nextAttemptAt time.Time) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(s.table),
		Key:              requestKey(id),
		UpdateExpression: aws.String("SET #due = :due ADD #attempts :one"),
		// A request that left the queue (it was made final) has nothing to
		// reschedule.
		ConditionExpression:      aws.String("attribute_exists(#q)"),
		ExpressionAttributeNames: names("#q", "#due", "#attempts"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":due": avNInt(nextAttemptAt.UnixNano()),
			":one": avNInt(1),
		},
	})
	if isConditionFailed(err) {
		return nil
	}
	return err
}

func (s *store) GetOrCreatePayerIntent(ctx context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId, candidate []byte) (*paymentrequest.PayerIntent, error) {
	if err := paymentrequest.ValidatePayerIntentCandidate(candidate); err != nil {
		return nil, err
	}

	// Requests are never deleted and their type and creator never change, so
	// what this read decides holds for the write below without conditioning
	// it on #meta.
	item, err := s.read(ctx, id)
	if err != nil {
		return nil, err
	}
	r, err := requestFromItem(item)
	if err != nil {
		return nil, err
	}
	switch {
	case !r.IsCollection():
		return nil, paymentrequest.ErrNotACollection
	case proto.Equal(r.Creator, userID):
		return nil, paymentrequest.ErrNotAPayer
	}

	// An ID already issued is read, never written over: a plain read never
	// conflicts with the payer's contribution being recorded in a transaction
	// on the same item, where a write would be refused with a
	// TransactionConflictException. A contribution needs the ID, so one can
	// be in flight only once the ID exists, and the write below, made only
	// while there is none, never meets one.
	switch current, err := s.GetPayerIntent(ctx, id, userID); {
	case err == nil:
		return current, nil
	case !errors.Is(err, paymentrequest.ErrNotFound):
		return nil, err
	}

	// The first call's candidate is the ID; a concurrent first call loses the
	// condition and is answered with the item as it stood.
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]types.AttributeValue{
			attrPK:       avS(requestPK(id)),
			attrSK:       avS(payerSK(userID)),
			attrIntentID: avB(candidate),
		},
		ConditionExpression:                 aws.String("attribute_not_exists(#pk)"),
		ExpressionAttributeNames:            names("#pk"),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if current, failed := conditionFailedItem(err); failed {
		if len(current) == 0 {
			// ALL_OLD is always returned for an item that exists; read it.
			return s.GetPayerIntent(ctx, id, userID)
		}
		return payerIntentFromItem(current)
	}
	if err != nil {
		return nil, err
	}
	return &paymentrequest.PayerIntent{IntentID: append([]byte(nil), candidate...)}, nil
}

func (s *store) GetPayerIntent(ctx context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId) (*paymentrequest.PayerIntent, error) {
	item, err := s.readPayer(ctx, id, userID)
	if err != nil {
		return nil, err
	}
	return payerIntentFromItem(item)
}

func (s *store) GetCollectionPayer(ctx context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId) (*paymentrequestpb.Payer, error) {
	item, err := s.readPayer(ctx, id, userID)
	if err != nil {
		return nil, err
	}
	if _, ok := item[attrPayer]; !ok {
		// Issued an ID, not yet contributed with it: not a Payer.
		return nil, paymentrequest.ErrNotFound
	}
	return payerFromItem(item)
}

func (s *store) GetCollectionPayersPage(ctx context.Context, id *paymentrequestpb.PaymentRequestId, cursor *paymentrequest.PayerCursor, limit int) ([]*paymentrequestpb.Payer, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		KeyConditionExpression: aws.String("#pk = :pk AND begins_with(#sk, :prefix)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": attrPK,
			"#sk": attrSK,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     avS(requestPK(id)),
			":prefix": avS(contributionSKPrefix),
		},
		// Ascending sort keys are the paid section's order (see the layout).
		ScanIndexForward: aws.Bool(true),
		ConsistentRead:   aws.Bool(true),
	}
	if cursor != nil {
		input.ExclusiveStartKey = map[string]types.AttributeValue{
			attrPK: avS(requestPK(id)),
			attrSK: avS(contributionSK(cursor.PaidAt, cursor.UserID)),
		}
	}

	var page []*paymentrequestpb.Payer
	for {
		if limit > 0 {
			input.Limit = aws.Int32(int32(limit - len(page)))
		}
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			payer, err := payerFromItem(item)
			if err != nil {
				return nil, err
			}
			page = append(page, payer)
		}
		if len(out.LastEvaluatedKey) == 0 || (limit > 0 && len(page) >= limit) {
			return page, nil
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

func (s *store) RecordContribution(ctx context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId, paid *paymentrequestpb.Paid, _ time.Time) (*paymentrequest.PaymentRequest, bool, error) {
	if err := paymentrequest.ValidateContribution(paid); err != nil {
		return nil, false, err
	}
	intentID := paid.GetIntent().GetValue()

	item, err := s.read(ctx, id)
	if err != nil {
		return nil, false, err
	}
	backoff := writeBackoffBase
	for range maxWriteAttempts {
		r, err := requestFromItem(item)
		if err != nil {
			return nil, false, err
		}
		if !r.IsCollection() {
			return nil, false, paymentrequest.ErrNotACollection
		}

		// The rules are the memory store's: the payment must name the ID
		// issued to the payer, and a payer who has contributed is a repeat.
		record, err := s.GetPayerIntent(ctx, id, userID)
		switch {
		case errors.Is(err, paymentrequest.ErrNotFound):
			return nil, false, paymentrequest.ErrWrongIntent
		case err != nil:
			return nil, false, err
		}
		if !bytes.Equal(record.IntentID, intentID) {
			return nil, false, paymentrequest.ErrWrongIntent
		}
		if record.Paid != nil {
			return r, false, nil
		}

		readVersion, readStatus := r.Version, r.Status
		payer, err := proto.Marshal(r.ContributionPayer(userID, paid))
		if err != nil {
			return nil, false, fmt.Errorf("marshalling payer: %w", err)
		}
		r.Version++
		r.PayerCount++

		_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{Update: &types.Update{
					TableName:           aws.String(s.table),
					Key:                 requestKey(id),
					UpdateExpression:    aws.String("SET #version = :version, #count = :count"),
					ConditionExpression: aws.String("#version = :readVersion AND #status = :readStatus"),
					ExpressionAttributeNames: map[string]string{
						"#version": attrVersion,
						"#count":   attrPayerCount,
						"#status":  attrStatus,
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":version":     avNInt(int64(r.Version)),
						":count":       avNInt(int64(r.PayerCount)),
						":readVersion": avNInt(int64(readVersion)),
						":readStatus":  avNInt(storedStatus(readStatus)),
					},
				}},
				{Update: &types.Update{
					TableName:           aws.String(s.table),
					Key:                 payerKey(id, userID),
					UpdateExpression:    aws.String("SET #payer = :payer"),
					ConditionExpression: aws.String("#intent = :intent AND attribute_not_exists(#payer)"),
					ExpressionAttributeNames: map[string]string{
						"#payer":  attrPayer,
						"#intent": attrIntentID,
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":payer":  avB(payer),
						":intent": avB(intentID),
					},
				}},
				{Put: &types.Put{
					TableName: aws.String(s.table),
					Item: map[string]types.AttributeValue{
						attrPK:    avS(requestPK(id)),
						attrSK:    avS(contributionSK(paid.PaidAt.AsTime(), userID.Value)),
						attrPayer: avB(payer),
					},
					ConditionExpression:      aws.String("attribute_not_exists(#pk)"),
					ExpressionAttributeNames: names("#pk"),
				}},
			},
		})
		if err == nil {
			return r, true, nil
		}
		var canceled *types.TransactionCanceledException
		if !errors.As(err, &canceled) {
			return nil, false, err
		}

		// A condition lost or a conflicting write: decide again from a fresh
		// read of both, which tells a contribution recorded meanwhile (the
		// no-op) from another change to #meta (a retry).
		if err := wait(ctx, &backoff); err != nil {
			return nil, false, err
		}
		if item, err = s.read(ctx, id); err != nil {
			return nil, false, err
		}
	}
	return nil, false, fmt.Errorf("record contribution exhausted retries for payment request %s", hex.EncodeToString(id.GetValue()))
}

// readPayer returns a collection payer's payer# item, strongly consistent, or
// ErrNotFound when no intent ID was issued to them.
func (s *store) readPayer(ctx context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId) (map[string]types.AttributeValue, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.table),
		Key:            payerKey(id, userID),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, paymentrequest.ErrNotFound
	}
	return out.Item, nil
}

// payerIntentFromItem decodes a payer# item.
func payerIntentFromItem(item map[string]types.AttributeValue) (*paymentrequest.PayerIntent, error) {
	intentID, ok := item[attrIntentID].(*types.AttributeValueMemberB)
	if !ok {
		return nil, fmt.Errorf("%s is missing or not binary", attrIntentID)
	}
	record := &paymentrequest.PayerIntent{IntentID: intentID.Value}
	if _, ok := item[attrPayer]; ok {
		payer, err := payerFromItem(item)
		if err != nil {
			return nil, err
		}
		record.Paid = payer.GetPaid()
	}
	return record, nil
}

// payerFromItem decodes the Payer a payer# item (once contributed) or a paid#
// item holds.
func payerFromItem(item map[string]types.AttributeValue) (*paymentrequestpb.Payer, error) {
	b, ok := item[attrPayer].(*types.AttributeValueMemberB)
	if !ok {
		return nil, fmt.Errorf("%s is missing or not binary", attrPayer)
	}
	payer := &paymentrequestpb.Payer{}
	if err := proto.Unmarshal(b.Value, payer); err != nil {
		return nil, fmt.Errorf("unmarshalling payer: %w", err)
	}
	return payer, nil
}

// dequeue removes a request's queue attributes, leaving the rest of it alone.
func (s *store) dequeue(ctx context.Context, id *paymentrequestpb.PaymentRequestId) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                aws.String(s.table),
		Key:                      requestKey(id),
		UpdateExpression:         aws.String("REMOVE #q, #due, #attempts"),
		ConditionExpression:      aws.String("attribute_exists(#pk)"),
		ExpressionAttributeNames: names("#pk", "#q", "#due", "#attempts"),
	})
	if isConditionFailed(err) {
		return nil
	}
	return err
}

// read returns a request's item, strongly consistent, or ErrNotFound.
func (s *store) read(ctx context.Context, id *paymentrequestpb.PaymentRequestId) (map[string]types.AttributeValue, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.table),
		Key:            requestKey(id),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, paymentrequest.ErrNotFound
	}
	return out.Item, nil
}

// change is one write of a request: the attributes it sets and those it
// removes.
type change struct {
	set    map[string]types.AttributeValue
	remove []string
}

// finalize adds to the change what makes r, whose Status and ClosedAt it
// carries, final: its status and closed_at, its move from the open section's
// index to the resolved one's, and its leaving the expiry queue.
func (c *change) finalize(r *paymentrequest.PaymentRequest) {
	c.set[attrStatus] = avNInt(storedStatus(r.Status))
	c.set[attrClosedAt] = avNInt(r.ClosedAt.UnixNano())
	c.set[attrResolvedSort] = avB(sortKey(*r.ClosedAt, r.ID.Value))
	c.remove = append(c.remove, attrOpenSort, attrExpiryQueue, attrExpiryDueAt, attrExpiryAttempts)
}

// casWrite applies ch to the request, conditioned on it still being at the
// version and status it was read at. When the condition fails it returns
// errLostRace with the item as it now stands, to decide again from; a request
// that no longer exists is ErrNotFound (requests are never deleted, so that
// is a fault).
func (s *store) casWrite(ctx context.Context, id *paymentrequestpb.PaymentRequestId, readVersion uint64, readStatus paymentrequest.Status, ch *change) (map[string]types.AttributeValue, error) {
	exprNames := map[string]string{
		"#version": attrVersion,
		"#status":  attrStatus,
	}
	values := map[string]types.AttributeValue{
		":readVersion": avNInt(int64(readVersion)),
		":readStatus":  avNInt(storedStatus(readStatus)),
	}

	update := ""
	i := 0
	for attr, value := range ch.set {
		alias := "#a" + strconv.Itoa(i)
		placeholder := ":v" + strconv.Itoa(i)
		i++
		// Alias every attribute rather than special-casing the two the
		// condition already names: an expression may alias one attribute
		// twice.
		exprNames[alias] = attr
		values[placeholder] = value
		if update == "" {
			update = "SET "
		} else {
			update += ", "
		}
		update += alias + " = " + placeholder
	}
	for j, attr := range ch.remove {
		alias := "#r" + strconv.Itoa(j)
		exprNames[alias] = attr
		if j == 0 {
			if update != "" {
				update += " "
			}
			update += "REMOVE "
		} else {
			update += ", "
		}
		update += alias
	}

	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                           aws.String(s.table),
		Key:                                 requestKey(id),
		UpdateExpression:                    aws.String(update),
		ConditionExpression:                 aws.String("#version = :readVersion AND #status = :readStatus"),
		ExpressionAttributeNames:            exprNames,
		ExpressionAttributeValues:           values,
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err == nil {
		return nil, nil
	}
	if current, failed := conditionFailedItem(err); failed {
		if len(current) == 0 {
			return nil, paymentrequest.ErrNotFound
		}
		return current, errLostRace
	}
	var conflict *types.TransactionConflictException
	if errors.As(err, &conflict) {
		// A transaction held the item (a contribution's, see
		// RecordContribution): decide again from a fresh read.
		current, err := s.read(ctx, id)
		if err != nil {
			return nil, err
		}
		return current, errLostRace
	}
	return nil, err
}

// wait sleeps for a jittered *backoff, doubling it up to writeBackoffMax for
// the next wait, or returns ctx's error should it end first.
func wait(ctx context.Context, backoff *time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(*backoff + rand.N(*backoff)):
	}
	*backoff = min(2**backoff, writeBackoffMax)
	return nil
}

func requestFromItem(item map[string]types.AttributeValue) (*paymentrequest.PaymentRequest, error) {
	id, err := requestIDFromPK(item)
	if err != nil {
		return nil, err
	}
	r := &paymentrequest.PaymentRequest{
		ID:      id,
		Creator: &commonpb.UserId{Value: asB(item[attrCreator])},
		ChatID:  &commonpb.ChatId{Value: asB(item[attrChat])},
	}

	// Decide the type before reading details, and refuse one this server does
	// not know: details is every type's attribute, so another type's message
	// could otherwise be read as a split.
	storedType, err := parseInt(item[attrType])
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", attrType, err)
	}
	details, ok := item[attrDetails].(*types.AttributeValueMemberB)
	if !ok {
		return nil, fmt.Errorf("%s is missing or not binary", attrDetails)
	}
	switch storedType {
	case typeSplit:
		split := &paymentrequestpb.Split{}
		if err := proto.Unmarshal(details.Value, split); err != nil {
			return nil, fmt.Errorf("unmarshalling split: %w", err)
		}
		intentIDs, err := intentIDsFromItem(item[attrIntentIDs])
		if err != nil {
			return nil, err
		}
		r.Split = split
		r.PaymentIntentIDs = intentIDs
	case typeCollection:
		collection := &paymentrequestpb.Collection{}
		if err := proto.Unmarshal(details.Value, collection); err != nil {
			return nil, fmt.Errorf("unmarshalling collection: %w", err)
		}
		payerCount, err := parseInt(item[attrPayerCount])
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", attrPayerCount, err)
		}
		r.Collection = collection
		r.PayerCount = uint32(payerCount)
	default:
		return nil, fmt.Errorf("unexpected payment request type %d", storedType)
	}

	createdAt, err := parseInt(item[attrCreatedAt])
	if err != nil {
		return nil, err
	}
	stored, err := parseInt(item[attrStatus])
	if err != nil {
		return nil, err
	}
	status, err := statusFromStored(stored)
	if err != nil {
		return nil, err
	}
	version, err := parseInt(item[attrVersion])
	if err != nil {
		return nil, err
	}

	r.CreatedAt = time.Unix(0, createdAt).UTC()
	r.Status = status
	r.Version = uint64(version)
	if av, ok := item[attrClosedAt]; ok {
		nanos, err := parseInt(av)
		if err != nil {
			return nil, err
		}
		closedAt := time.Unix(0, nanos).UTC()
		r.ClosedAt = &closedAt
	}
	return r, nil
}

// intentIDsFromItem decodes the intent_ids map: each debtor's user ID,
// hex, to their share's payment intent ID.
func intentIDsFromItem(av types.AttributeValue) (map[string][]byte, error) {
	m, ok := av.(*types.AttributeValueMemberM)
	if !ok {
		return nil, fmt.Errorf("%s is missing or not a map", attrIntentIDs)
	}
	intentIDs := make(map[string][]byte, len(m.Value))
	for key, value := range m.Value {
		userID, err := hex.DecodeString(key)
		if err != nil {
			return nil, fmt.Errorf("decoding %s key: %w", attrIntentIDs, err)
		}
		intentID, ok := value.(*types.AttributeValueMemberB)
		if !ok {
			return nil, fmt.Errorf("%s value is not binary", attrIntentIDs)
		}
		intentIDs[string(userID)] = intentID.Value
	}
	return intentIDs, nil
}

func storedStatus(status paymentrequest.Status) int64 {
	switch status {
	case paymentrequest.StatusOpen:
		return statusOpen
	case paymentrequest.StatusCompleted:
		return statusCompleted
	case paymentrequest.StatusClosed:
		return statusClosed
	case paymentrequest.StatusExpired:
		return statusExpired
	default:
		panic(fmt.Sprintf("unexpected payment request status %s", status))
	}
}

func statusFromStored(stored int64) (paymentrequest.Status, error) {
	switch stored {
	case statusOpen:
		return paymentrequest.StatusOpen, nil
	case statusCompleted:
		return paymentrequest.StatusCompleted, nil
	case statusClosed:
		return paymentrequest.StatusClosed, nil
	case statusExpired:
		return paymentrequest.StatusExpired, nil
	default:
		return 0, fmt.Errorf("unexpected payment request status %d", stored)
	}
}

// sortKey is a section index's sort key for a request at at (its ends_at or
// closed_at) with ID id: sortableNanos(at) then the ID, so that byte order is
// the order of (at, ID), and unique.
func sortKey(at time.Time, id []byte) []byte {
	key := make([]byte, 8, 8+len(id))
	binary.BigEndian.PutUint64(key, sortableNanos(at))
	return append(key, id...)
}

// sortableNanos is t in Unix nanos with the sign bit flipped, so that the
// unsigned big-endian bytes DynamoDB compares binary keys by order times as
// the signed nanos do, those before 1970 included.
func sortableNanos(t time.Time) uint64 {
	return uint64(t.UnixNano()) ^ (1 << 63)
}

// requestPK is the partition key of a request's items.
func requestPK(id *paymentrequestpb.PaymentRequestId) string {
	return requestKeyPrefix + hex.EncodeToString(id.GetValue())
}

// requestKey is the key of a request's own item.
func requestKey(id *paymentrequestpb.PaymentRequestId) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK: avS(requestPK(id)),
		attrSK: avS(requestSK),
	}
}

// payerSK is the sort key of a collection payer's payer# item.
func payerSK(userID *commonpb.UserId) string {
	return payerSKPrefix + hex.EncodeToString(userID.GetValue())
}

// payerKey is the key of a collection payer's payer# item.
func payerKey(id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK: avS(requestPK(id)),
		attrSK: avS(payerSK(userID)),
	}
}

// contributionSK is the sort key of a contribution's paid# item: paid_at
// inverted, as fixed-width hex, then the user ID, as hex, so that ascending
// keys are paid_at descending then user ID ascending (see the layout).
func contributionSK(paidAt time.Time, userID []byte) string {
	return fmt.Sprintf("%s%016x#%s", contributionSKPrefix, ^sortableNanos(paidAt), hex.EncodeToString(userID))
}

// requestIDFromPK recovers a request ID from an item's pk ("req#<hex>"), the
// inverse of requestPK.
func requestIDFromPK(item map[string]types.AttributeValue) (*paymentrequestpb.PaymentRequestId, error) {
	pk := asS(item[attrPK])
	encoded, ok := strings.CutPrefix(pk, requestKeyPrefix)
	if !ok {
		return nil, fmt.Errorf("unexpected pk %q", pk)
	}
	id, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding payment request id from pk %q: %w", pk, err)
	}
	return &paymentrequestpb.PaymentRequestId{Value: id}, nil
}

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avB(v []byte) types.AttributeValue { return &types.AttributeValueMemberB{Value: v} }
func avNInt(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

func asS(av types.AttributeValue) string {
	if s, ok := av.(*types.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

func asB(av types.AttributeValue) []byte {
	if b, ok := av.(*types.AttributeValueMemberB); ok {
		return b.Value
	}
	return nil
}

func parseInt(av types.AttributeValue) (int64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseInt(n.Value, 10, 64)
}

// isConditionFailed reports whether err is a failed condition on a single-item
// write.
func isConditionFailed(err error) bool {
	var failed *types.ConditionalCheckFailedException
	return errors.As(err, &failed)
}

// conditionFailedItem reports whether err is a failed condition on a
// single-item write and, if so, returns the item as it stood (nil when it did
// not exist), as returned by ReturnValuesOnConditionCheckFailure.
func conditionFailedItem(err error) (map[string]types.AttributeValue, bool) {
	var failed *types.ConditionalCheckFailedException
	if !errors.As(err, &failed) {
		return nil, false
	}
	return failed.Item, true
}

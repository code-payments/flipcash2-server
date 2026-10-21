package dynamodb

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
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
//	payment_requests  pk = the request ID (B, its 16 bytes), sk = requestSK
//	                  (#meta), one item per request: type (N, see the type
//	                  values below), creator and chat (B; the chat is the
//	                  16-byte group chat ID), created_at (epoch nanos,
//	                  exactly: a creator's share is paid at created_at to the
//	                  nanosecond, which PaymentRequest.Validate checks), split
//	                  (the marshalled paymentrequestpb.Split, shares' paid
//	                  included, rewritten whole whenever a share is paid),
//	                  payment_intent_ids (M: each debtor's user ID, hex, to
//	                  their share's payment intent ID, B; written at creation
//	                  and never again), status (N, see the status values
//	                  below), version (N), and closed_at (epoch nanos) once
//	                  the request is final.
//
// The request is the only item in its partition today. The sort key is there
// so a type that outgrows one item (a request's shares or contributions as
// items of their own) can keep them beside it, read with it in one Query and
// written with it in one transaction; a key schema cannot be changed once the
// table exists. Such an item takes its own sk and must omit the index
// attributes below.
//
// type says which of the request's type fields is set, and every reader
// refuses a value it does not know rather than guess: a server that predates a
// type fails reads of it instead of serving it as a split.
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
// Any other item type ever written into this table must omit chat and the
// section sort attributes (open_sort, resolved_sort) and the queue attributes,
// or it leaks into the indexes.
//
// Every change to a request is a read, a decision in Go by the same model
// helpers the memory store uses (PaymentRequest.Share, IsOpenAt,
// AllSharesPaid, Status.IsFinal, EndsAt), and one UpdateItem conditioned on
// the version and status it read. The status is in the condition because
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

	// requestSK is the sort key of a request's own item.
	requestSK = "#meta"

	attrPK             = "pk"                 // B, the request ID
	attrSK             = "sk"                 // S, requestSK for the request itself
	attrType           = "type"               // N, see the type values
	attrCreator        = "creator"            // B
	attrChat           = "chat"               // B, the group chat ID (GSI hash of both section indexes)
	attrCreatedAt      = "created_at"         // N, Unix nanos
	attrSplit          = "split"              // B, the marshalled Split
	attrIntentIDs      = "payment_intent_ids" // M, hex debtor user ID → B payment intent ID
	attrStatus         = "status"             // N, see the status values
	attrVersion        = "version"            // N
	attrClosedAt       = "closed_at"          // N, Unix nanos; present only once final
	attrOpenSort       = "open_sort"          // B, sortKey(ends_at, ID); present only while open
	attrResolvedSort   = "resolved_sort"      // B, sortKey(closed_at, ID); present only once final
	attrExpiryQueue    = "expiry_queue"       // S, expiryQueuePK (GSI hash); present only while queued
	attrExpiryDueAt    = "expiry_due_at"      // N, Unix nanos (GSI range); when the request is next due
	attrExpiryAttempts = "expiry_attempts"    // N, failed attempts so far

	// maxWriteAttempts bounds how many times one change is tried against
	// concurrent writers before it fails, and writeBackoffBase and
	// writeBackoffMax shape the wait between tries.
	maxWriteAttempts = 32
	writeBackoffBase = 2 * time.Millisecond
	writeBackoffMax  = 100 * time.Millisecond
)

// The stored type values, by stable number: which of PaymentRequest's type
// fields is set. A new type takes the next number.
const (
	typeSplit = 1
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
	split, err := proto.Marshal(r.Split)
	if err != nil {
		return fmt.Errorf("marshalling split: %w", err)
	}

	intentIDs := make(map[string]types.AttributeValue, len(r.PaymentIntentIDs))
	for userID, intentID := range r.PaymentIntentIDs {
		intentIDs[hex.EncodeToString([]byte(userID))] = avB(intentID)
	}

	item := map[string]types.AttributeValue{
		attrPK:             avB(r.ID.Value),
		attrSK:             avS(requestSK),
		attrType:           avNInt(typeSplit),
		attrCreator:        avB(r.Creator.Value),
		attrChat:           avB(r.ChatID.Value),
		attrCreatedAt:      avNInt(r.CreatedAt.UnixNano()),
		attrSplit:          avB(split),
		attrIntentIDs:      &types.AttributeValueMemberM{Value: intentIDs},
		attrStatus:         avNInt(statusOpen),
		attrVersion:        avNInt(1),
		attrOpenSort:       avB(sortKey(r.EndsAt(), r.ID.Value)),
		attrExpiryQueue:    avS(expiryQueuePK),
		attrExpiryDueAt:    avNInt(paymentrequest.ExpiryDueAt(r).UnixNano()),
		attrExpiryAttempts: avNInt(0),
	}

	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
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

func (s *store) PayShare(ctx context.Context, id *paymentrequestpb.PaymentRequestId, userID *commonpb.UserId, paid *paymentrequestpb.PaidShare, now time.Time) (*paymentrequest.PaymentRequest, bool, error) {
	if err := paymentrequest.ValidatePaidShare(paid); err != nil {
		return nil, false, err
	}

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

		// The rules are the memory store's, decided on the same helpers.
		share := r.Share(userID)
		if share == nil {
			return nil, false, paymentrequest.ErrNotAShare
		}
		if err := paymentrequest.ValidatePaymentIntent(r, userID, paid); err != nil {
			return nil, false, err
		}
		if !paymentrequest.PaidShareSupersedes(share.Paid, paid) {
			return r, false, nil
		}
		if paid.GetManual() != nil && !r.IsOpenAt(now) {
			return nil, false, paymentrequest.ErrNotOpen
		}

		readVersion, readStatus := r.Version, r.Status
		share.Paid = proto.Clone(paid).(*paymentrequestpb.PaidShare)
		r.Version++
		split, err := proto.Marshal(r.Split)
		if err != nil {
			return nil, false, fmt.Errorf("marshalling split: %w", err)
		}
		ch := &change{set: map[string]types.AttributeValue{
			attrSplit:   avB(split),
			attrVersion: avNInt(int64(r.Version)),
		}}
		if r.Status == paymentrequest.StatusOpen && r.AllSharesPaid() {
			closedAt := paid.PaidAt.AsTime()
			r.Status = paymentrequest.StatusCompleted
			r.ClosedAt = &closedAt
			ch.finalize(r)
		}

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
	return nil, false, fmt.Errorf("pay share exhausted retries for payment request %s", hex.EncodeToString(id.GetValue()))
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

		// The version does not move: expiry is the clock's, and a reader was
		// shown the request expired at this version already.
		readVersion, readStatus := r.Version, r.Status
		endsAt := r.EndsAt()
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
		tasks = append(tasks, &paymentrequest.ExpiryTask{
			ID:            &paymentrequestpb.PaymentRequestId{Value: asB(item[attrPK])},
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
		// A transaction held the item; nothing here writes one, but should
		// anything come to, decide again from a fresh read.
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
	// Decide the type before reading anything type-specific, and refuse one
	// this server does not know: an absent split would otherwise unmarshal as
	// an empty one.
	storedType, err := parseInt(item[attrType])
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", attrType, err)
	}
	if storedType != typeSplit {
		return nil, fmt.Errorf("unexpected payment request type %d", storedType)
	}
	splitBytes, ok := item[attrSplit].(*types.AttributeValueMemberB)
	if !ok {
		return nil, fmt.Errorf("%s is missing or not binary", attrSplit)
	}
	split := &paymentrequestpb.Split{}
	if err := proto.Unmarshal(splitBytes.Value, split); err != nil {
		return nil, fmt.Errorf("unmarshalling split: %w", err)
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

	intentIDs, err := intentIDsFromItem(item[attrIntentIDs])
	if err != nil {
		return nil, err
	}

	r := &paymentrequest.PaymentRequest{
		ID:               &paymentrequestpb.PaymentRequestId{Value: asB(item[attrPK])},
		Creator:          &commonpb.UserId{Value: asB(item[attrCreator])},
		CreatedAt:        time.Unix(0, createdAt).UTC(),
		ChatID:           &commonpb.ChatId{Value: asB(item[attrChat])},
		Split:            split,
		PaymentIntentIDs: intentIDs,
		Status:           status,
		Version:          uint64(version),
	}
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

// intentIDsFromItem decodes the payment_intent_ids map: each debtor's user ID,
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

// requestKey is the key of a request's own item.
func requestKey(id *paymentrequestpb.PaymentRequestId) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK: avB(id.GetValue()),
		attrSK: avS(requestSK),
	}
}

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avB(v []byte) types.AttributeValue { return &types.AttributeValueMemberB{Value: v} }
func avNInt(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
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

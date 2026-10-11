package dynamodb

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	pollpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/poll/v1"

	"github.com/code-payments/flipcash2-server/poll"
)

// The poll store is two tables:
//
//	polls       pk = "poll#<poll>", one item per poll: its chat, creator,
//	            definition (the marshalled PollDefinition, which never changes),
//	            created_at and ends_at (epoch nanos; ends_at is copied out of the
//	            definition so conditions and updates can use it), live, once
//	            sent the client message ID it went live by (cmid), status,
//	            and once closed its results (the marshalled Results).
//
//	            An unsent poll carries expires_at, its end in epoch seconds, as
//	            its TTL: it is discarded when it ends. TTL deletes lazily, so
//	            every read also drops an unsent poll past its end (see
//	            Poll.IsDiscardedAt). Going live removes it.
//
//	            The tally queue is a sparse GSI over the polls table, as blob
//	            finalization's is (see blob/dynamodb): the queue attributes are
//	            present exactly while a poll is live and not yet closed, so only
//	            queued polls appear in the index. Going live sets them, in the
//	            write that makes the poll live, and Close removes them in the
//	            write that closes it, so a poll is enqueued and dequeued
//	            atomically.
//
//	poll_votes  pk = "poll#<poll>", sk = "user#<voter>", one item per current
//	            vote: the selected options and voted_at (epoch nanos). A vote is
//	            a single put or delete of its item, and a poll's votes are one
//	            strongly consistent query of its partition, in voter order.
const (
	// tallyQueueIndex is the sparse GSI backing GetDueForTally.
	tallyQueueIndex = "tally_queue"

	// tallyQueuePK is the tally queue's one index partition. The queue is not
	// sharded: every queued poll is filed under this one hash key, so its
	// enqueues, claims, delays and dequeues all write that one GSI partition,
	// and the tally's poll is a single soonest-first Query of it. One GSI
	// partition takes on the order of 1,000 writes a second, and each poll
	// costs it a handful in its life (enqueue, a claim per attempt, dequeue),
	// far beyond any rate of poll creation in sight. Should that change, the
	// queue is sharded by filing each poll under one of several hash keys
	// derived from its ID, with the tally querying each.
	tallyQueuePK = "queue#tally"

	pollKeyPrefix = "poll#"
	userKeyPrefix = "user#"

	attrPK              = "pk"
	attrSK              = "sk"
	attrChat            = "chat"       // B
	attrCreator         = "creator"    // B
	attrDefinition      = "definition" // B, the marshalled PollDefinition
	attrCreatedAt       = "created_at" // N, Unix nanos
	attrEndsAt          = "ends_at"    // N, Unix nanos, copied out of the definition
	attrLive            = "live"       // BOOL
	attrClientMessageID = "cmid"       // B, the client message ID it went live by; present only once sent
	attrStatus          = "status"     // N, statusOpen or statusClosed
	attrResults         = "results"    // B, the marshalled Results; present only once closed
	attrExpiresAt       = "expires_at" // N, Unix seconds (TTL); present only while unsent

	attrTallyQueue    = "tally_queue"    // S, tallyQueuePK (GSI hash); present only while queued
	attrTallyDueAt    = "tally_due_at"   // N, Unix nanos (GSI range); when the poll is next due
	attrTallyAttempts = "tally_attempts" // N, failed attempts so far

	attrOptions = "options"  // L of N, ascending
	attrVotedAt = "voted_at" // N, Unix nanos

	// maxBatchGetKeys is DynamoDB's per-BatchGetItem key limit.
	maxBatchGetKeys = 100
)

// The stored status values. Only open and closed are stored (see poll.Status).
const (
	statusOpen   = 0
	statusClosed = 2
)

// expressionNames aliases every attribute an expression here names, so none
// can collide with a DynamoDB reserved word (status is one).
var expressionNames = map[string]string{
	"#pk":       attrPK,
	"#endsAt":   attrEndsAt,
	"#live":     attrLive,
	"#cmid":     attrClientMessageID,
	"#status":   attrStatus,
	"#results":  attrResults,
	"#expires":  attrExpiresAt,
	"#q":        attrTallyQueue,
	"#due":      attrTallyDueAt,
	"#attempts": attrTallyAttempts,
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

type store struct {
	client     *dynamodb.Client
	pollsTable string
	votesTable string
}

// NewInDynamoDB returns a poll.Store backed by the given DynamoDB tables. Use
// CreateTables to provision them.
func NewInDynamoDB(client *dynamodb.Client, pollsTable, votesTable string) poll.Store {
	return &store{
		client:     client,
		pollsTable: pollsTable,
		votesTable: votesTable,
	}
}

func (s *store) CreatePoll(ctx context.Context, p *poll.Poll) error {
	definition, err := proto.Marshal(p.Definition)
	if err != nil {
		return fmt.Errorf("marshalling poll definition: %w", err)
	}

	item := map[string]types.AttributeValue{
		attrPK:         avS(pollPK(p.ID)),
		attrChat:       avB(p.ChatID.Value),
		attrCreator:    avB(p.CreatorID.Value),
		attrDefinition: avB(definition),
		attrCreatedAt:  avNInt(p.CreatedAt.UnixNano()),
		attrEndsAt:     avNInt(p.EndsAt().UnixNano()),
		attrLive:       avBool(p.Live),
		attrStatus:     avNInt(statusOpen),
	}
	if p.Live {
		item[attrTallyQueue] = avS(tallyQueuePK)
		item[attrTallyDueAt] = avNInt(poll.TallyDueAt(p).UnixNano())
		item[attrTallyAttempts] = avNInt(0)
	} else {
		item[attrExpiresAt] = avNInt(expiresAt(p.EndsAt()))
	}

	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                aws.String(s.pollsTable),
		Item:                     item,
		ConditionExpression:      aws.String("attribute_not_exists(#pk)"),
		ExpressionAttributeNames: names("#pk"),
	})
	if isConditionFailed(err) {
		return poll.ErrPollExists
	}
	return err
}

func (s *store) GetPoll(ctx context.Context, pollID *pollpb.PollId) (*poll.Poll, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.pollsTable),
		Key:            pollKey(pollID),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, poll.ErrPollNotFound
	}
	p, err := pollFromItem(out.Item)
	if err != nil {
		return nil, err
	}
	if p.IsDiscardedAt(time.Now()) {
		return nil, poll.ErrPollNotFound
	}
	return p, nil
}

func (s *store) GetPolls(ctx context.Context, pollIDs []*pollpb.PollId) (map[string]*poll.Poll, error) {
	seen := make(map[string]bool, len(pollIDs))
	keys := make([]map[string]types.AttributeValue, 0, len(pollIDs))
	for _, id := range pollIDs {
		if seen[string(id.Value)] {
			continue
		}
		seen[string(id.Value)] = true
		keys = append(keys, pollKey(id))
	}

	items, err := s.batchGet(ctx, s.pollsTable, keys)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	out := make(map[string]*poll.Poll, len(items))
	for _, item := range items {
		p, err := pollFromItem(item)
		if err != nil {
			return nil, err
		}
		if p.IsDiscardedAt(now) {
			continue
		}
		out[string(p.ID.Value)] = p
	}
	return out, nil
}

func (s *store) MarkLive(ctx context.Context, pollID *pollpb.PollId, clientMessageID *messagingpb.ClientMessageId, now time.Time) (bool, error) {
	// The due time is computed from the stored end in the update itself, so
	// going live is one write with no read before it. The condition admits
	// only an unsent poll, never a retry by the same send: that would enqueue
	// the poll again, closed or not.
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                aws.String(s.pollsTable),
		Key:                      pollKey(pollID),
		ConditionExpression:      aws.String("attribute_exists(#pk) AND #live = :false AND #endsAt > :now"),
		UpdateExpression:         aws.String("SET #live = :true, #cmid = :cmid, #q = :q, #due = #endsAt + :delay, #attempts = :zero REMOVE #expires"),
		ExpressionAttributeNames: names("#pk", "#live", "#cmid", "#endsAt", "#q", "#due", "#attempts", "#expires"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":false": avBool(false),
			":true":  avBool(true),
			":cmid":  avB(clientMessageID.GetValue()),
			":now":   avNInt(now.UnixNano()),
			":q":     avS(tallyQueuePK),
			":delay": avNInt(int64(poll.TallyDelay)),
			":zero":  avNInt(0),
		},
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err == nil {
		return true, nil
	}
	old, failed := conditionFailedItem(err)
	if !failed {
		return false, err
	}
	// Missing, or unsent and ended (discarded): not found. Otherwise the poll
	// is live already, which is the no-op if this send made it live.
	if len(old) == 0 || !asBool(old[attrLive]) {
		return false, poll.ErrPollNotFound
	}
	if stored, ok := old[attrClientMessageID]; ok && bytes.Equal(asB(stored), clientMessageID.GetValue()) {
		return false, nil
	}
	return false, poll.ErrPollAlreadyLive
}

func (s *store) SetVote(ctx context.Context, pollID *pollpb.PollId, userID *commonpb.UserId, options []uint32, votedAt time.Time) error {
	key := voteKey(pollID, userID)
	if len(options) == 0 {
		_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(s.votesTable),
			Key:       key,
		})
		return err
	}

	normalized := poll.NormalizeSelection(options)
	list := make([]types.AttributeValue, len(normalized))
	for i, option := range normalized {
		list[i] = avNInt(int64(option))
	}
	key[attrOptions] = &types.AttributeValueMemberL{Value: list}
	key[attrVotedAt] = avNInt(votedAt.UnixNano())

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.votesTable),
		Item:      key,
	})
	return err
}

func (s *store) GetVotes(ctx context.Context, userID *commonpb.UserId, pollIDs []*pollpb.PollId) (map[string][]uint32, error) {
	seen := make(map[string]bool, len(pollIDs))
	keys := make([]map[string]types.AttributeValue, 0, len(pollIDs))
	for _, id := range pollIDs {
		if seen[string(id.Value)] {
			continue
		}
		seen[string(id.Value)] = true
		keys = append(keys, voteKey(id, userID))
	}

	items, err := s.batchGet(ctx, s.votesTable, keys)
	if err != nil {
		return nil, err
	}

	out := make(map[string][]uint32, len(items))
	for _, item := range items {
		pollID, err := pollIDFromPK(item)
		if err != nil {
			return nil, err
		}
		options, err := parseOptions(item[attrOptions])
		if err != nil {
			return nil, err
		}
		out[string(pollID.Value)] = options
	}
	return out, nil
}

func (s *store) GetVotesPage(ctx context.Context, pollID *pollpb.PollId, after *commonpb.UserId, limit int) ([]poll.Vote, error) {
	condition := "#pk = :pk AND begins_with(#sk, :prefix)"
	values := map[string]types.AttributeValue{
		":pk":     avS(pollPK(pollID)),
		":prefix": avS(userKeyPrefix),
	}
	if after != nil {
		condition = "#pk = :pk AND #sk > :after"
		values = map[string]types.AttributeValue{
			":pk":    avS(pollPK(pollID)),
			":after": avS(userSK(after)),
		}
	}

	var (
		votes    []poll.Vote
		startKey map[string]types.AttributeValue
	)
	for {
		input := &dynamodb.QueryInput{
			TableName:                 aws.String(s.votesTable),
			KeyConditionExpression:    aws.String(condition),
			ExpressionAttributeNames:  map[string]string{"#pk": attrPK, "#sk": attrSK},
			ExpressionAttributeValues: values,
			ConsistentRead:            aws.Bool(true),
			ExclusiveStartKey:         startKey,
		}
		if limit > 0 {
			input.Limit = aws.Int32(int32(limit - len(votes)))
		}
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			vote, err := voteFromItem(item)
			if err != nil {
				return nil, err
			}
			votes = append(votes, vote)
		}
		if len(out.LastEvaluatedKey) == 0 || (limit > 0 && len(votes) >= limit) {
			return votes, nil
		}
		startKey = out.LastEvaluatedKey
	}
}

func (s *store) GetDueForTally(ctx context.Context, asOf time.Time, limit int) ([]*poll.TallyTask, error) {
	input := &dynamodb.QueryInput{
		TableName:                aws.String(s.pollsTable),
		IndexName:                aws.String(tallyQueueIndex),
		KeyConditionExpression:   aws.String("#q = :q AND #due <= :asOf"),
		ExpressionAttributeNames: names("#q", "#due"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":q":    avS(tallyQueuePK),
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

	tasks := make([]*poll.TallyTask, 0, len(out.Items))
	for _, item := range out.Items {
		id, err := pollIDFromPK(item)
		if err != nil {
			return nil, err
		}
		attempts, err := parseInt(item[attrTallyAttempts])
		if err != nil {
			return nil, err
		}
		dueAt, err := parseInt(item[attrTallyDueAt])
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, &poll.TallyTask{
			ID:            id,
			Attempts:      uint32(attempts),
			NextAttemptAt: time.Unix(0, dueAt).UTC(),
		})
	}
	return tasks, nil
}

func (s *store) GetTallyQueueStats(ctx context.Context, asOf time.Time) (*poll.TallyQueueStats, error) {
	// One page of the due range, soonest first, projecting only the due time:
	// its first item is the most overdue poll, and its length the depth, capped
	// by the page limit. Polls not yet due sit past the range and are never
	// read.
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                aws.String(s.pollsTable),
		IndexName:                aws.String(tallyQueueIndex),
		KeyConditionExpression:   aws.String("#q = :q AND #due <= :asOf"),
		ProjectionExpression:     aws.String("#due"),
		ExpressionAttributeNames: names("#q", "#due"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":q":    avS(tallyQueuePK),
			":asOf": avNInt(asOf.UnixNano()),
		},
		Limit: aws.Int32(poll.MaxTallyQueueStatsDepth),
	})
	if err != nil {
		return nil, err
	}

	stats := &poll.TallyQueueStats{DueDepth: uint64(len(out.Items))}
	if len(out.Items) > 0 {
		nanos, err := parseInt(out.Items[0][attrTallyDueAt])
		if err != nil {
			return nil, err
		}
		stats.OldestDueAt = time.Unix(0, nanos).UTC()
	}
	return stats, nil
}

func (s *store) ClaimForTally(ctx context.Context, pollID *pollpb.PollId, asOf, until time.Time) (bool, error) {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(s.pollsTable),
		Key:              pollKey(pollID),
		UpdateExpression: aws.String("SET #due = :until"),
		// Claim only a poll that is still queued and still due: a dequeued poll
		// was closed, and one pushed into the future was claimed or delayed by
		// another server first.
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

func (s *store) DelayTally(ctx context.Context, pollID *pollpb.PollId, nextAttemptAt time.Time) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(s.pollsTable),
		Key:              pollKey(pollID),
		UpdateExpression: aws.String("SET #due = :due ADD #attempts :one"),
		// A poll that left the queue (a concurrent tally closed it) has nothing
		// to reschedule.
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

func (s *store) Close(ctx context.Context, pollID *pollpb.PollId, results *pollpb.Results) (bool, error) {
	encoded, err := proto.Marshal(results)
	if err != nil {
		return false, fmt.Errorf("marshalling poll results: %w", err)
	}

	_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                aws.String(s.pollsTable),
		Key:                      pollKey(pollID),
		ConditionExpression:      aws.String("#live = :true AND #status = :open"),
		UpdateExpression:         aws.String("SET #status = :closed, #results = :results REMOVE #q, #due, #attempts"),
		ExpressionAttributeNames: names("#live", "#status", "#results", "#q", "#due", "#attempts"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":true":    avBool(true),
			":open":    avNInt(statusOpen),
			":closed":  avNInt(statusClosed),
			":results": avB(encoded),
		},
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err == nil {
		return true, nil
	}
	old, failed := conditionFailedItem(err)
	if !failed {
		return false, err
	}
	if len(old) == 0 {
		return false, poll.ErrPollNotFound
	}
	p, err := pollFromItem(old)
	if err != nil {
		return false, err
	}
	switch {
	case p.IsDiscardedAt(time.Now()):
		return false, poll.ErrPollNotFound
	case !p.Live:
		return false, poll.ErrPollNotLive
	}
	// Live and not open: closed already, the no-op.
	return false, nil
}

// batchGet reads every item named by keys from table, strongly consistent, in
// as few BatchGetItem calls as DynamoDB allows, retrying unprocessed keys.
// Items that do not exist are absent from the result, which is in no
// particular order.
func (s *store) batchGet(ctx context.Context, table string, keys []map[string]types.AttributeValue) ([]map[string]types.AttributeValue, error) {
	var items []map[string]types.AttributeValue
	for start := 0; start < len(keys); start += maxBatchGetKeys {
		pending := keys[start:min(start+maxBatchGetKeys, len(keys))]
		for len(pending) > 0 {
			out, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					table: {Keys: pending, ConsistentRead: aws.Bool(true)},
				},
			})
			if err != nil {
				return nil, err
			}
			items = append(items, out.Responses[table]...)
			pending = out.UnprocessedKeys[table].Keys
		}
	}
	return items, nil
}

func pollFromItem(item map[string]types.AttributeValue) (*poll.Poll, error) {
	id, err := pollIDFromPK(item)
	if err != nil {
		return nil, err
	}
	definition := &pollpb.PollDefinition{}
	if err := proto.Unmarshal(asB(item[attrDefinition]), definition); err != nil {
		return nil, fmt.Errorf("unmarshalling poll definition: %w", err)
	}
	createdAt, err := parseInt(item[attrCreatedAt])
	if err != nil {
		return nil, err
	}
	status, err := parseInt(item[attrStatus])
	if err != nil {
		return nil, err
	}

	p := &poll.Poll{
		ID:         id,
		ChatID:     &commonpb.ChatId{Value: asB(item[attrChat])},
		CreatorID:  &commonpb.UserId{Value: asB(item[attrCreator])},
		Definition: definition,
		CreatedAt:  time.Unix(0, createdAt).UTC(),
		Live:       asBool(item[attrLive]),
	}
	if clientMessageID, ok := item[attrClientMessageID]; ok {
		p.ClientMessageID = &messagingpb.ClientMessageId{Value: asB(clientMessageID)}
	}
	switch status {
	case statusOpen:
		p.Status = poll.StatusOpen
	case statusClosed:
		p.Status = poll.StatusClosed
		results := &pollpb.Results{}
		if err := proto.Unmarshal(asB(item[attrResults]), results); err != nil {
			return nil, fmt.Errorf("unmarshalling poll results: %w", err)
		}
		p.Results = results
	default:
		return nil, fmt.Errorf("unexpected poll status %d", status)
	}
	return p, nil
}

func voteFromItem(item map[string]types.AttributeValue) (poll.Vote, error) {
	userID, err := userIDFromSK(item)
	if err != nil {
		return poll.Vote{}, err
	}
	options, err := parseOptions(item[attrOptions])
	if err != nil {
		return poll.Vote{}, err
	}
	votedAt, err := parseInt(item[attrVotedAt])
	if err != nil {
		return poll.Vote{}, err
	}
	return poll.Vote{
		UserID:  userID,
		Options: options,
		VotedAt: time.Unix(0, votedAt).UTC(),
	}, nil
}

func parseOptions(av types.AttributeValue) ([]uint32, error) {
	list, ok := av.(*types.AttributeValueMemberL)
	if !ok {
		return nil, fmt.Errorf("expected list attribute, got %T", av)
	}
	options := make([]uint32, len(list.Value))
	for i, v := range list.Value {
		n, err := parseInt(v)
		if err != nil {
			return nil, err
		}
		options[i] = uint32(n)
	}
	return options, nil
}

// expiresAt is the TTL of an unsent poll ending at endsAt: its end, in epoch
// seconds, rounded up so the item is never deleted before it ends.
func expiresAt(endsAt time.Time) int64 {
	seconds := endsAt.Unix()
	if endsAt.After(time.Unix(seconds, 0)) {
		seconds++
	}
	return seconds
}

func pollPK(pollID *pollpb.PollId) string { return pollKeyPrefix + hex.EncodeToString(pollID.Value) }
func userSK(userID *commonpb.UserId) string {
	return userKeyPrefix + hex.EncodeToString(userID.Value)
}

func pollKey(pollID *pollpb.PollId) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{attrPK: avS(pollPK(pollID))}
}

func voteKey(pollID *pollpb.PollId, userID *commonpb.UserId) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK: avS(pollPK(pollID)),
		attrSK: avS(userSK(userID)),
	}
}

// pollIDFromPK recovers a poll ID from an item's pk ("poll#<hex>"), the
// inverse of pollPK.
func pollIDFromPK(item map[string]types.AttributeValue) (*pollpb.PollId, error) {
	pk := asS(item[attrPK])
	encoded, ok := strings.CutPrefix(pk, pollKeyPrefix)
	if !ok {
		return nil, fmt.Errorf("unexpected pk %q", pk)
	}
	id, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding poll id from pk %q: %w", pk, err)
	}
	return &pollpb.PollId{Value: id}, nil
}

// userIDFromSK recovers a voter ID from a vote item's sk ("user#<hex>"), the
// inverse of userSK.
func userIDFromSK(item map[string]types.AttributeValue) (*commonpb.UserId, error) {
	sk := asS(item[attrSK])
	encoded, ok := strings.CutPrefix(sk, userKeyPrefix)
	if !ok {
		return nil, fmt.Errorf("unexpected sk %q", sk)
	}
	id, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding user id from sk %q: %w", sk, err)
	}
	return &commonpb.UserId{Value: id}, nil
}

func avS(v string) types.AttributeValue  { return &types.AttributeValueMemberS{Value: v} }
func avB(v []byte) types.AttributeValue  { return &types.AttributeValueMemberB{Value: v} }
func avBool(v bool) types.AttributeValue { return &types.AttributeValueMemberBOOL{Value: v} }
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

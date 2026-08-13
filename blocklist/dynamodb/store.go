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

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/blocklist"
)

// The blocklist store is a single table:
//
//	blocklist  pk = "user#<owner>", sk = "blocked#<blocked>" (one item per
//	           (owner, blocked user)). blocked_at is denormalized as an attribute.
//	           IsBlocked/Unblock are O(1) point operations on the base key. A GSI
//	           on (pk, blocked_at) lists an owner's blocklist most-recently-blocked
//	           first with true server-side pagination. GetBlocked queries the
//	           sort-key range spanning the candidate set (between the smallest and
//	           largest candidate) and filters to the exact candidates in memory —
//	           bounded to the blocked users within that range, cheap while
//	           blocklists stay small. The blocked user ID is recovered from the sk,
//	           so it is not stored as its own attribute.
//
//	           GetBlockers answers the reverse question — which of these owners
//	           have blocked one user — off the reverse GSI, intersecting one
//	           blocker set against the candidates in memory, with a keyed
//	           BatchGetItem fallback once that set outgrows a bounded probe. See
//	           there for why the index rather than the keys is the default.
//
//	           Each owner also has one metadata item (sk = "#meta") holding
//	           per-owner aggregates — currently just their blocklist size in a
//	           count attribute — maintained atomically with each Block/Unblock via
//	           a transaction. GetBlockedCount is an O(1) read of it, letting callers
//	           size their read strategy without counting the partition.
const (
	// gsiByBlockedAt orders an owner's blocklist by blocked_at. It reuses the
	// base table's pk as its hash key.
	gsiByBlockedAt = "by_blocked_at"

	// gsiByBlockedUser is the reverse of gsiByBlockedAt: it reuses the base
	// table's sk (the blocked user) as its hash key, so a query for one user
	// returns everyone who has blocked them, ordered by blocked_at. The blocker
	// is recovered from the base table's pk.
	gsiByBlockedUser = "by_blocked_user"

	// userKeyPrefix prefixes an owner ID in the pk; blockedKeyPrefix prefixes a
	// blocked user ID in the sk.
	userKeyPrefix    = "user#"
	blockedKeyPrefix = "blocked#"

	// metaSK is the sort key of an owner's metadata item: one item per owner in the
	// owner's partition, holding per-owner aggregates (currently their blocklist
	// size in a count attribute). Its leading "#" sorts it before the "blocked#"
	// key space — below the lower bound of a GetBlocked range scan — so it never
	// appears in one, and it carries no blocked_at, so it is absent from both GSIs
	// and from a GetBlocklist listing.
	metaSK = "#meta"

	attrPK        = "pk"
	attrSK        = "sk"
	attrBlockedAt = "blocked_at"
	attrCount     = "count"

	// blockersQueryLimit bounds the reverse-index probe in GetBlockers: at most
	// this many of a user's blockers are read before the strategy switches to the
	// keyed fallback. At roughly 100 bytes per KEYS_ONLY index entry a full probe
	// costs about 2.5 RCU, and a user blocked by fewer than this — nearly all of
	// them — is answered by that one query regardless of how large the chat is.
	//
	// The shared store suite's ManyBlockers test must block more than this many
	// users to reach the fallback path; raising this without raising that leaves
	// the fallback uncovered.
	blockersQueryLimit = 200

	// maxBatchGetKeys is DynamoDB's per-BatchGetItem key limit.
	maxBatchGetKeys = 100
)

type store struct {
	client *dynamodb.Client
	table  string
}

// NewInDynamoDB returns a blocklist.Store backed by the given DynamoDB table.
// Use CreateTable to provision it.
func NewInDynamoDB(client *dynamodb.Client, table string) blocklist.Store {
	return &store{
		client: client,
		table:  table,
	}
}

func (s *store) Block(ctx context.Context, ownerID, blockedID *commonpb.UserId, blockedAt time.Time) (bool, error) {
	// Add the entry and bump the owner's blocklist counter atomically. The entry
	// Put is conditioned on the entry not already existing, so a re-block leaves
	// the original entry (and its blocked_at) untouched; because that cancels the
	// whole transaction, it also does not double-count.
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String(s.table),
					Item: map[string]types.AttributeValue{
						attrPK:        avS(ownerPK(ownerID)),
						attrSK:        avS(blockedSK(blockedID)),
						attrBlockedAt: avN(uint64(blockedAt.UnixNano())),
					},
					ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
				},
			},
			s.countDelta(ownerID, 1),
		},
	})
	if err != nil {
		// The entry already existed: the condition canceled the transaction, so
		// nothing was written and the count is unchanged. A no-op re-block.
		if isTransactionConditionFailed(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *store) Unblock(ctx context.Context, ownerID, blockedID *commonpb.UserId) (bool, error) {
	// Remove the entry and decrement the owner's counter atomically. The Delete is
	// conditioned on the entry existing, so unblocking a user who isn't blocked
	// cancels the transaction and leaves the count untouched — a no-op.
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Delete: &types.Delete{
					TableName:           aws.String(s.table),
					Key:                 map[string]types.AttributeValue{attrPK: avS(ownerPK(ownerID)), attrSK: avS(blockedSK(blockedID))},
					ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s)", attrPK)),
				},
			},
			s.countDelta(ownerID, -1),
		},
	})
	if err != nil {
		// The entry did not exist: a no-op unblock, count unchanged.
		if isTransactionConditionFailed(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// countDelta builds a transaction item that adds delta to the count attribute on
// ownerID's metadata item, creating that item (with count from 0) on first use.
// It is paired with the conditional entry write in Block/Unblock so the count
// moves only when the entry set actually changes.
func (s *store) countDelta(ownerID *commonpb.UserId, delta int64) types.TransactWriteItem {
	return types.TransactWriteItem{
		Update: &types.Update{
			TableName: aws.String(s.table),
			Key: map[string]types.AttributeValue{
				attrPK: avS(ownerPK(ownerID)),
				attrSK: avS(metaSK),
			},
			UpdateExpression:          aws.String("ADD #count :delta"),
			ExpressionAttributeNames:  map[string]string{"#count": attrCount},
			ExpressionAttributeValues: map[string]types.AttributeValue{":delta": avNInt(delta)},
		},
	}
}

func (s *store) IsBlocked(ctx context.Context, ownerID, blockedID *commonpb.UserId) (bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.table),
		Key:                  map[string]types.AttributeValue{attrPK: avS(ownerPK(ownerID)), attrSK: avS(blockedSK(blockedID))},
		ProjectionExpression: aws.String(attrPK),
	})
	if err != nil {
		return false, err
	}
	return len(out.Item) > 0, nil
}

func (s *store) GetBlockedCount(ctx context.Context, ownerID *commonpb.UserId) (int, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:                aws.String(s.table),
		Key:                      map[string]types.AttributeValue{attrPK: avS(ownerPK(ownerID)), attrSK: avS(metaSK)},
		ProjectionExpression:     aws.String("#count"),
		ExpressionAttributeNames: map[string]string{"#count": attrCount},
	})
	if err != nil {
		return 0, err
	}
	if len(out.Item) == 0 {
		return 0, nil
	}
	n, err := parseInt(out.Item[attrCount])
	if err != nil {
		return 0, err
	}
	// The counter tracks net Block/Unblock activity; clamp defensively so removing
	// an entry that predates the counter can't surface a negative size.
	if n < 0 {
		return 0, nil
	}
	return int(n), nil
}

func (s *store) GetBlocked(ctx context.Context, ownerID *commonpb.UserId, candidateIDs []*commonpb.UserId) (map[string]bool, error) {
	if len(candidateIDs) == 0 {
		return nil, nil
	}

	// The exact candidate set to keep, plus the byte-smallest and byte-largest
	// candidate IDs. sk = "blocked#" + hex(id), and hex preserves byte order, so
	// these two IDs bound the sort-key range that can hold any candidate.
	wanted := make(map[string]struct{}, len(candidateIDs))
	minID, maxID := candidateIDs[0], candidateIDs[0]
	for _, c := range candidateIDs {
		wanted[string(c.Value)] = struct{}{}
		if bytes.Compare(c.Value, minID.Value) < 0 {
			minID = c
		}
		if bytes.Compare(c.Value, maxID.Value) > 0 {
			maxID = c
		}
	}

	// Query only the slice of the owner's blocklist whose sk falls within the
	// candidate range [min, max], then keep the exact candidates (the range can
	// also span non-candidate blocked users, which are filtered out here). This
	// reads at most the blocked users between the smallest and largest candidate —
	// never the whole partition — and while blocklists stay small it is one page
	// and a fraction of an RCU. blocked_at is unused, so only the sk is projected.
	blocked := make(map[string]bool)
	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		KeyConditionExpression: aws.String("#pk = :owner AND #sk BETWEEN :lo AND :hi"),
		ProjectionExpression:   aws.String("#sk"),
		ExpressionAttributeNames: map[string]string{
			"#pk": attrPK,
			"#sk": attrSK,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":owner": avS(ownerPK(ownerID)),
			":lo":    avS(blockedSK(minID)),
			":hi":    avS(blockedSK(maxID)),
		},
	}
	for {
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			id, err := blockedIDFromSK(item)
			if err != nil {
				return nil, err
			}
			if _, ok := wanted[string(id.Value)]; ok {
				blocked[string(id.Value)] = true
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
	return blocked, nil
}

func (s *store) GetBlockers(ctx context.Context, blockedID *commonpb.UserId, candidateOwnerIDs []*commonpb.UserId) (map[string]bool, error) {
	if len(candidateOwnerIDs) == 0 {
		return nil, nil
	}

	// Deduped once here: it bounds the fallback's key set (BatchGetItem rejects a
	// request carrying the same key twice, rather than collapsing it) and gives the
	// probe path its membership test.
	wanted := make(map[string]struct{}, len(candidateOwnerIDs))
	ownerIDs := make([]*commonpb.UserId, 0, len(candidateOwnerIDs))
	for _, ownerID := range candidateOwnerIDs {
		if _, dup := wanted[string(ownerID.Value)]; dup {
			continue
		}
		wanted[string(ownerID.Value)] = struct{}{}
		ownerIDs = append(ownerIDs, ownerID)
	}

	// Read the reverse index for blockedID — everyone who has blocked them — and
	// intersect with the candidates in memory. A Query is billed on the summed size
	// of what it reads before rounding to 4KB, so a whole blocker set of tens of
	// tiny KEYS_ONLY entries costs the 0.5 RCU minimum. The keyed fallback below is
	// billed per item instead (each key rounded to its own 4KB), making it cost
	// proportional to the candidate set — which for a group send is the membership.
	// So this path is what keeps a send's blocklist read independent of group size.
	//
	// The cost of this path scales with how blocked the sender is, which has nothing
	// to do with the chat and is unbounded — a mass-blocked account would otherwise
	// read its entire blocker list, over many pages, on every message it sends, all
	// against a single index partition. The limit caps that: past it, this probe is
	// abandoned for the fallback, whose cost is bounded by the candidates.
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                aws.String(s.table),
		IndexName:                aws.String(gsiByBlockedUser),
		KeyConditionExpression:   aws.String("#sk = :blocked"),
		ExpressionAttributeNames: map[string]string{"#sk": attrSK},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":blocked": avS(blockedSK(blockedID)),
		},
		Limit: aws.Int32(blockersQueryLimit),
	})
	if err != nil {
		return nil, err
	}

	// A start key means the partition did not drain within the limit, so the
	// blockers read so far are an arbitrary prefix: a candidate absent from it may
	// still be further in. Only a drained partition licenses concluding "absent
	// here means not blocking". (DynamoDB also returns a start key when it stops
	// exactly at the limit with nothing left, so a user blocked by precisely this
	// many falls back needlessly — one wasted query in a case that barely exists.)
	if len(out.LastEvaluatedKey) > 0 {
		return s.getBlockersByKey(ctx, blockedID, ownerIDs)
	}

	blockers := make(map[string]bool)
	for _, item := range out.Items {
		ownerID, err := blockerIDFromPK(item)
		if err != nil {
			return nil, err
		}
		if _, ok := wanted[string(ownerID.Value)]; ok {
			blockers[string(ownerID.Value)] = true
		}
	}
	return blockers, nil
}

// getBlockersByKey answers GetBlockers by addressing one exact base-table key per
// candidate owner — their partition, and within it the single entry that would
// record them blocking blockedID. It is the fallback for a user with more blockers
// than the reverse-index probe will read: the read is bounded by the candidate set
// however many blockers they have, at the cost of being billed per key rather than
// in aggregate. ownerIDs must already be deduped.
func (s *store) getBlockersByKey(ctx context.Context, blockedID *commonpb.UserId, ownerIDs []*commonpb.UserId) (map[string]bool, error) {
	keys := make([]map[string]types.AttributeValue, len(ownerIDs))
	for i, ownerID := range ownerIDs {
		keys[i] = map[string]types.AttributeValue{
			attrPK: avS(ownerPK(ownerID)),
			attrSK: avS(blockedSK(blockedID)),
		}
	}

	// Only the pk is projected: an item coming back at all is the answer, and its
	// pk is which owner it belongs to.
	blockers := make(map[string]bool)
	for start := 0; start < len(keys); start += maxBatchGetKeys {
		end := start + maxBatchGetKeys
		if end > len(keys) {
			end = len(keys)
		}
		req := map[string]types.KeysAndAttributes{
			s.table: {Keys: keys[start:end], ProjectionExpression: aws.String(attrPK)},
		}
		// Retry UnprocessedKeys until the batch drains.
		for len(req[s.table].Keys) > 0 {
			out, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
			if err != nil {
				return nil, err
			}
			for _, item := range out.Responses[s.table] {
				ownerID, err := blockerIDFromPK(item)
				if err != nil {
					return nil, err
				}
				blockers[string(ownerID.Value)] = true
			}
			if unprocessed, ok := out.UnprocessedKeys[s.table]; ok && len(unprocessed.Keys) > 0 {
				req = map[string]types.KeysAndAttributes{s.table: unprocessed}
			} else {
				break
			}
		}
	}
	return blockers, nil
}

func (s *store) GetBlocklistPage(ctx context.Context, ownerID *commonpb.UserId, cursor *blocklist.Cursor, limit int) ([]*blocklist.BlockedUser, error) {
	// Query the GSI for one owner's entries, most recent first. blocked_at is
	// immutable, so no watermark bound is needed; the partition holds exactly the
	// owner's blocklist, so pages come back dense with no filter expression.
	input := &dynamodb.QueryInput{
		TableName:                aws.String(s.table),
		IndexName:                aws.String(gsiByBlockedAt),
		KeyConditionExpression:   aws.String("#pk = :owner"),
		ExpressionAttributeNames: map[string]string{"#pk": attrPK},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":owner": avS(ownerPK(ownerID)),
		},
		ScanIndexForward: aws.Bool(false),
	}
	if limit > 0 {
		input.Limit = aws.Int32(int32(limit))
	}

	// The cursor carries (blocked_at, user_id) explicitly, so the GSI start key is
	// built directly without a lookup. A GSI start key must include the GSI key
	// (pk, blocked_at) and the base table key (pk, sk).
	if cursor != nil {
		input.ExclusiveStartKey = map[string]types.AttributeValue{
			attrPK:        avS(ownerPK(ownerID)),
			attrSK:        avS(blockedSK(cursor.UserID)),
			attrBlockedAt: avN(uint64(cursor.BlockedAt.UnixNano())),
		}
	}

	out, err := s.client.Query(ctx, input)
	if err != nil {
		return nil, err
	}
	entries := make([]*blocklist.BlockedUser, 0, len(out.Items))
	for _, item := range out.Items {
		e, err := blockedUserFromItem(item)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// blockedUserFromItem builds a BlockedUser from a blocklist item. The blocked
// user ID is recovered from the item's sk rather than stored on its own.
func blockedUserFromItem(item map[string]types.AttributeValue) (*blocklist.BlockedUser, error) {
	userID, err := blockedIDFromSK(item)
	if err != nil {
		return nil, err
	}
	nanos, err := parseInt(item[attrBlockedAt])
	if err != nil {
		return nil, err
	}
	return &blocklist.BlockedUser{
		UserID:    userID,
		BlockedAt: time.Unix(0, nanos).UTC(),
	}, nil
}

func ownerPK(userID *commonpb.UserId) string { return userKeyPrefix + hex.EncodeToString(userID.Value) }
func blockedSK(userID *commonpb.UserId) string {
	return blockedKeyPrefix + hex.EncodeToString(userID.Value)
}

// blockerIDFromPK recovers an owner (blocker) ID from an item's pk
// ("user#<hex>"), the inverse of ownerPK. It is how GetBlockers attributes a
// result back to an owner: both of its read paths return items keyed by owner,
// and neither knows which owner an item belongs to until its pk is read.
func blockerIDFromPK(item map[string]types.AttributeValue) (*commonpb.UserId, error) {
	pk := asS(item[attrPK])
	encoded, ok := strings.CutPrefix(pk, userKeyPrefix)
	if !ok {
		return nil, fmt.Errorf("unexpected pk %q", pk)
	}
	id, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding user id from pk %q: %w", pk, err)
	}
	return &commonpb.UserId{Value: id}, nil
}

// blockedIDFromSK recovers a blocked user ID from an item's sk
// ("blocked#<hex>"), the inverse of blockedSK.
func blockedIDFromSK(item map[string]types.AttributeValue) (*commonpb.UserId, error) {
	sk := asS(item[attrSK])
	encoded, ok := strings.CutPrefix(sk, blockedKeyPrefix)
	if !ok {
		return nil, fmt.Errorf("unexpected sk %q", sk)
	}
	id, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding user id from sk %q: %w", sk, err)
	}
	return &commonpb.UserId{Value: id}, nil
}

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avN(v uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
}
func avNInt(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

func asS(av types.AttributeValue) string {
	if s, ok := av.(*types.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

func parseInt(av types.AttributeValue) (int64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseInt(n.Value, 10, 64)
}

// isTransactionConditionFailed reports whether err is a TransactWriteItems
// cancellation caused by a failed condition (as opposed to a conflict, capacity,
// or other cancellation) — i.e. the entry already existed on Block, or did not
// exist on Unblock, which callers translate into an idempotent no-op.
func isTransactionConditionFailed(err error) bool {
	var canceled *types.TransactionCanceledException
	if !errors.As(err, &canceled) {
		return false
	}
	for _, reason := range canceled.CancellationReasons {
		if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
			return true
		}
	}
	return false
}

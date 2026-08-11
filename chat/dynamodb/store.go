package dynamodb

import (
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

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// The chat store spans three tables:
//
//	chats     pk = "chat#<id>" (one item per chat). Canonical metadata: type,
//	          members (the DM participants; absent for groups), title (groups
//	          only), last_activity. GetChat is a point read and
//	          AdvanceLastActivity is an O(1) update of the source of truth.
//
//	dm_inbox  pk = "user#<id>", sk = "chat#<id>" (one item per (user, DM)). The
//	          per-user DM inbox index. A GSI on (feed, last_activity) — where
//	          feed = "user#<id>#<type>" partitions each user's inbox by chat
//	          type — lets one type's DMs be listed most-recently-active first
//	          with true server-side pagination and no filtering. last_activity
//	          and the participants are denormalized so the inbox renders from
//	          one query. AdvanceLastActivity fans the new last_activity out to
//	          each member's row (two for a DM), re-sorting the GSI.
//
//	group_members  pk = "chat#<id>", sk = "user#<id>" (one item per (group,
//	          member), kept as a tombstone after departure). Group membership,
//	          mutable — unlike a DM's inline member list. Group chats
//	          deliberately have no per-message inbox fan-out: a send touches
//	          only the canonical chats item, and a user's group feed is
//	          assembled at read time via the inverted gsiByUser. Joined members
//	          are enumerated densely via the sparse gsiByJoinedAt.
//
//	          Both GSIs are sparse, keyed by attributes only membership rows
//	          carry (user, joined_at) — that discipline is load-bearing: any
//	          new item type added to this table (e.g. a future per-group
//	          aggregates item) must omit those attributes or it leaks into the
//	          indexes.
const (
	// gsiByActivity is the legacy feed index on (pk, last_activity), spanning
	// all of a user's DM types. Superseded by gsiByTypeActivity; retained until
	// the feed backfill completes everywhere and the index is dropped from the
	// table.
	gsiByActivity = "by_activity"

	// gsiByTypeActivity orders one chat type's slice of a user's inbox by
	// last_activity, keyed by the composite feed attribute.
	gsiByTypeActivity = "by_type_activity"

	// gsiByUser is the inverted (user, chat) index on group_members, for
	// listing the group chats a user is a member of. It is keyed by the sparse
	// user attribute — the member's ID, written once and present on membership
	// rows (tombstones included) and nothing else — rather than the sk itself,
	// so non-membership items in the table stay out of the index.
	gsiByUser = "by_user"

	// gsiByJoinedAt is the sparse (chat, joined_at) index on group_members.
	// joined_at is present iff the member is currently joined (a left tombstone
	// carries left_at instead), so only joined members appear in the index:
	// enumerating a group's members is a dense query — no tombstone filtering,
	// no matter how churned the group — ordered by join time, newest first.
	gsiByJoinedAt = "by_joined_at"

	// chatKeyPrefix prefixes a chat ID in the chats table pk and the dm_inbox
	// sk. The chat ID is recovered from the key, so it is not stored as its own
	// attribute.
	chatKeyPrefix = "chat#"

	attrPK            = "pk"
	attrSK            = "sk"
	attrType          = "type"
	attrFeed          = "feed"
	attrMembers       = "members"
	attrTitle         = "title"
	attrState         = "state"
	attrUser          = "user" // member id, bare hex — see userIndexKey
	attrJoinedAt      = "joined_at"
	attrLeftAt        = "left_at"
	attrLastActivity  = "last_activity"
	attrLastMessageID = "last_message_id"
)

// Values of a group_members item's numeric state attribute, following the
// store's convention of encoding enums by stable number (see attrType). 0 is
// reserved as unspecified, per proto enum convention, so a future proto member
// state enum can adopt these values directly.
//
// A left member's item is a tombstone: it records that the user was formerly a
// member (which "never a member" cannot), and makes re-adding an idempotent
// update of the same key. Because the (chat, user) key is immutable across
// state changes, paginating members by sort key stays cursor-stable under
// concurrent joins and leaves.
const (
	memberStateJoined = 1
	memberStateLeft   = 2
)

type store struct {
	client            *dynamodb.Client
	chatsTable        string
	dmInboxTable      string
	groupMembersTable string
}

// NewInDynamoDB returns a chat.Store backed by the given DynamoDB tables. Use
// CreateTables to provision them.
func NewInDynamoDB(client *dynamodb.Client, chatsTable, dmInboxTable, groupMembersTable string) chat.Store {
	return &store{
		client:            client,
		chatsTable:        chatsTable,
		dmInboxTable:      dmInboxTable,
		groupMembersTable: groupMembersTable,
	}
}

// maxTransactWriteItems is DynamoDB's per-transaction item limit.
const maxTransactWriteItems = 100

// A group chat is created in one transaction covering the canonical item and
// every membership record, so the domain's cap on the initial member set must
// leave room for the canonical item within maxTransactWriteItems. Asserted
// rather than assumed, since the cap lives in the chat package and this is the
// constraint it exists to respect: raising it past the ceiling fails to compile
// here, because the difference underflows uint.
const _ uint = maxTransactWriteItems - (chat.MaxGroupChatCreationMembers + 1)

func (s *store) PutChat(ctx context.Context, c *chat.Chat) error {
	if chat.IsGroupChatID(c.ID) != (c.Type == chatpb.ChatType_GROUP) {
		return fmt.Errorf("chat id length does not match chat type")
	}
	if c.Type == chatpb.ChatType_GROUP {
		return s.putGroupChat(ctx, c)
	}
	return s.putDmChat(ctx, c)
}

// putDmChat creates a DM chat: the canonical metadata item plus each
// participant's dm_inbox row, in one transaction. The canonical item's
// condition enforces uniqueness for the whole write.
func (s *store) putDmChat(ctx context.Context, c *chat.Chat) error {
	if len(c.Members) == 0 {
		return chat.ErrNoMembers
	}

	transactItems := []types.TransactWriteItem{
		{Put: &types.Put{
			TableName:           aws.String(s.chatsTable),
			Item:                s.chatItem(c),
			ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
		}},
	}
	for _, member := range c.Members {
		transactItems = append(transactItems, types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String(s.dmInboxTable),
				Item:      s.dmInboxItem(c, member),
			},
		})
	}

	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		if isChatExistsCancellation(err) {
			return chat.ErrChatExists
		}
		return err
	}
	return nil
}

// putGroupChat creates a group chat: the canonical metadata item plus every
// initial membership record, in one transaction. The canonical item's condition
// enforces uniqueness for the whole write, so a duplicate reports ErrChatExists
// with no membership written.
func (s *store) putGroupChat(ctx context.Context, c *chat.Chat) error {
	// Collapse duplicates before building the transaction: two actions on one
	// item is a validation error, not a cancellation, so a repeated member would
	// fail the write outright rather than being absorbed the way AddGroupMembers
	// absorbs it.
	members := dedupeUserIDs(c.Members)
	if len(members) == 0 {
		return chat.ErrNoMembers
	}
	if len(members) > chat.MaxGroupChatCreationMembers {
		return chat.ErrTooManyMembers
	}

	now := time.Now().UTC()
	transactItems := []types.TransactWriteItem{
		{Put: &types.Put{
			TableName:           aws.String(s.chatsTable),
			Item:                s.chatItem(c),
			ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
		}},
	}
	for _, member := range members {
		transactItems = append(transactItems, types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String(s.groupMembersTable),
				Item:      s.groupMemberItem(c.ID, member, now),
			},
		})
	}
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		if isChatExistsCancellation(err) {
			return chat.ErrChatExists
		}
		return err
	}
	return nil
}

// dedupeUserIDs returns the distinct user IDs in order of first appearance.
func dedupeUserIDs(userIDs []*commonpb.UserId) []*commonpb.UserId {
	seen := make(map[string]struct{}, len(userIDs))
	out := make([]*commonpb.UserId, 0, len(userIDs))
	for _, userID := range userIDs {
		if _, dup := seen[string(userID.Value)]; dup {
			continue
		}
		seen[string(userID.Value)] = struct{}{}
		out = append(out, userID)
	}
	return out
}

func (s *store) AddGroupMembers(ctx context.Context, chatID *commonpb.ChatId, userIDs []*commonpb.UserId) error {
	if !chat.IsGroupChatID(chatID) {
		return fmt.Errorf("not a group chat id")
	}

	// Existence gate so a typo'd chat ID can't accrete orphaned membership
	// rows. Non-transactional: the canonical item is never deleted, so a chat
	// that exists here still exists during the writes below.
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.chatsTable),
		Key:                  map[string]types.AttributeValue{attrPK: avS(chatPK(chatID))},
		ProjectionExpression: aws.String(attrPK),
	})
	if err != nil {
		return err
	}
	if len(out.Item) == 0 {
		return chat.ErrChatNotFound
	}

	return s.addGroupMembers(ctx, chatID, userIDs)
}

// addGroupMembers upserts joined membership records without checking that the
// chat exists. Each member is an independent conditional update: an
// already-joined member is left untouched (preserving their original
// joined_at), while a new or departed member is (re)joined with a fresh join
// time. joined_at is present iff joined — the sparse gsiByJoinedAt keys off its
// presence — so rejoining also clears the tombstone's left_at.
func (s *store) addGroupMembers(ctx context.Context, chatID *commonpb.ChatId, userIDs []*commonpb.UserId) error {
	for _, userID := range userIDs {
		_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(s.groupMembersTable),
			Key: map[string]types.AttributeValue{
				attrPK: avS(chatPK(chatID)),
				attrSK: avS(userPK(userID)),
			},
			UpdateExpression: aws.String(fmt.Sprintf(
				"SET #state = :joined, #user = :user, %s = :now REMOVE %s", attrJoinedAt, attrLeftAt,
			)),
			ConditionExpression:      aws.String("attribute_not_exists(#state) OR #state <> :joined"),
			ExpressionAttributeNames: map[string]string{"#state": attrState, "#user": attrUser},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":joined": avN(memberStateJoined),
				":user":   avS(userIndexKey(userID)),
				":now":    avN(uint64(time.Now().UTC().UnixNano())),
			},
		})
		if err != nil && !isConditionalCheckFailed(err) {
			return err
		}
	}
	return nil
}

func (s *store) RemoveGroupMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) error {
	if !chat.IsGroupChatID(chatID) {
		return fmt.Errorf("not a group chat id")
	}

	// Tombstone, don't delete: the item keeps recording that the user was
	// formerly a member. Only a joined member transitions — the condition makes
	// removing a non-member (or an unknown user) a no-op rather than an upsert
	// of a malformed tombstone. Removing joined_at drops the member from the
	// sparse gsiByJoinedAt.
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.groupMembersTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(userPK(userID)),
		},
		UpdateExpression: aws.String(fmt.Sprintf(
			"SET #state = :left, %s = :now REMOVE %s", attrLeftAt, attrJoinedAt,
		)),
		ConditionExpression:      aws.String("#state = :joined"),
		ExpressionAttributeNames: map[string]string{"#state": attrState},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":joined": avN(memberStateJoined),
			":left":   avN(memberStateLeft),
			":now":    avN(uint64(time.Now().UTC().UnixNano())),
		},
	})
	if err != nil && !isConditionalCheckFailed(err) {
		return err
	}
	return nil
}

// GetChatByID is a single point read of the canonical chats item. A group's
// membership lives in group_members and is deliberately not joined in here:
// enumerating it is a paged query, and no metadata read should pay for it
// implicitly (see GetMembers).
func (s *store) GetChatByID(ctx context.Context, chatID *commonpb.ChatId) (*chat.Chat, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.chatsTable),
		Key:       map[string]types.AttributeValue{attrPK: avS(chatPK(chatID))},
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, chat.ErrChatNotFound
	}
	return chatFromItem(chatID, out.Item)
}

func (s *store) GetDmFeedPage(ctx context.Context, userID *commonpb.UserId, chatType chatpb.ChatType, snapshot time.Time, cursor *chat.DmFeedCursor, limit int) ([]*chat.Chat, error) {
	// Constrain the GSI range key to the snapshot window: only inbox rows whose
	// last_activity is at or before the watermark. The composite feed hash key
	// scopes the query to one chat type, so pages come back dense — no filter
	// expression. Descending order (most recent first) is fixed for the feed.
	input := &dynamodb.QueryInput{
		TableName:                aws.String(s.dmInboxTable),
		IndexName:                aws.String(gsiByTypeActivity),
		KeyConditionExpression:   aws.String(fmt.Sprintf("#feed = :f AND %s <= :snap", attrLastActivity)),
		ExpressionAttributeNames: map[string]string{"#feed": attrFeed},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":f":    avS(feedPK(userID, chatType)),
			":snap": avN(uint64(snapshot.UnixNano())),
		},
		ScanIndexForward: aws.Bool(false),
	}
	if limit > 0 {
		input.Limit = aws.Int32(int32(limit))
	}

	// The cursor carries (last_activity, chat_id) explicitly, so the GSI start
	// key is built directly without a lookup. A GSI start key must include the
	// GSI key (feed, last_activity) and the base table key (pk, sk).
	if cursor != nil {
		input.ExclusiveStartKey = map[string]types.AttributeValue{
			attrPK:           avS(userPK(userID)),
			attrSK:           avS(chatSK(cursor.ChatID)),
			attrFeed:         avS(feedPK(userID, chatType)),
			attrLastActivity: avN(uint64(cursor.LastActivity.UnixNano())),
		}
	}

	out, err := s.client.Query(ctx, input)
	if err != nil {
		return nil, err
	}
	chats := make([]*chat.Chat, 0, len(out.Items))
	for _, item := range out.Items {
		chatID, err := chatIDFromSK(item)
		if err != nil {
			return nil, err
		}
		c, err := chatFromItem(chatID, item)
		if err != nil {
			return nil, err
		}
		chats = append(chats, c)
	}
	return chats, nil
}

func (s *store) GetMembers(ctx context.Context, chatID *commonpb.ChatId) ([]*commonpb.UserId, error) {
	if chat.IsGroupChatID(chatID) {
		return s.getGroupMembers(ctx, chatID)
	}
	c, err := s.GetChatByID(ctx, chatID)
	if err != nil {
		return nil, err
	}
	return c.Members, nil
}

// getGroupMembers enumerates a group's joined members via the sparse
// gsiByJoinedAt: tombstones have no joined_at, so they are not in the index and
// every page is dense regardless of how churned the group is. Newest joiner
// first, matching the index's feed ordering.
func (s *store) getGroupMembers(ctx context.Context, chatID *commonpb.ChatId) ([]*commonpb.UserId, error) {
	members := make([]*commonpb.UserId, 0)
	var startKey map[string]types.AttributeValue
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(s.groupMembersTable),
			IndexName:                 aws.String(gsiByJoinedAt),
			KeyConditionExpression:    aws.String("#pk = :pk"),
			ExpressionAttributeNames:  map[string]string{"#pk": attrPK},
			ExpressionAttributeValues: map[string]types.AttributeValue{":pk": avS(chatPK(chatID))},
			ScanIndexForward:          aws.Bool(false),
			ExclusiveStartKey:         startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			userID, err := userIDFromSK(item)
			if err != nil {
				return nil, err
			}
			members = append(members, userID)
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}

	// A memberless group and a nonexistent chat both query empty; the contract
	// distinguishes them, so consult the canonical item (whose absence is
	// ErrChatNotFound) only on that ambiguous path.
	if len(members) == 0 {
		if _, err := s.GetChatByID(ctx, chatID); err != nil {
			return nil, err
		}
	}
	return members, nil
}

func (s *store) IsMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	if chat.IsGroupChatID(chatID) {
		out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(s.groupMembersTable),
			Key: map[string]types.AttributeValue{
				attrPK: avS(chatPK(chatID)),
				attrSK: avS(userPK(userID)),
			},
			ProjectionExpression:     aws.String("#state"),
			ExpressionAttributeNames: map[string]string{"#state": attrState},
		})
		if err != nil {
			return false, err
		}
		if len(out.Item) == 0 {
			return false, nil
		}
		state, err := parseN(out.Item[attrState])
		if err != nil {
			return false, err
		}
		return state == memberStateJoined, nil
	}

	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.dmInboxTable),
		Key:                  map[string]types.AttributeValue{attrPK: avS(userPK(userID)), attrSK: avS(chatSK(chatID))},
		ProjectionExpression: aws.String(attrPK),
	})
	if err != nil {
		return false, err
	}
	return len(out.Item) > 0, nil
}

func (s *store) AdvanceLastMessage(ctx context.Context, chatID *commonpb.ChatId, messageID *messagingpb.MessageId, ts time.Time) (bool, []*commonpb.UserId, error) {
	// Load the canonical record for the current value and the member set to
	// fan out to.
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.chatsTable),
		Key:            map[string]types.AttributeValue{attrPK: avS(chatPK(chatID))},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return false, nil, err
	}
	if len(out.Item) == 0 {
		return false, nil, chat.ErrChatNotFound
	}
	cur, err := parseInt(out.Item[attrLastActivity])
	if err != nil {
		return false, nil, err
	}
	// Members are returned to the caller regardless of whether the activity
	// advances, so parse them before the no-op short-circuit.
	members := membersFromItem(out.Item)
	if ts.UnixNano() <= cur {
		return false, members, nil // No-op: stored value is already at or after ts.
	}

	// Bump the canonical value (conditioned so it only moves forward) and mirror
	// it onto each member's inbox row so the GSI re-sorts. last_activity and
	// last_message_id move together: both describe the same newest message.
	setExpr := fmt.Sprintf("SET %s = :ts, %s = :mid", attrLastActivity, attrLastMessageID)
	condExpr := fmt.Sprintf("%s < :ts", attrLastActivity)
	values := func() map[string]types.AttributeValue {
		return map[string]types.AttributeValue{
			":ts":  avN(uint64(ts.UnixNano())),
			":mid": avN(messageID.Value),
		}
	}
	transactItems := []types.TransactWriteItem{
		{Update: &types.Update{
			TableName:                 aws.String(s.chatsTable),
			Key:                       map[string]types.AttributeValue{attrPK: avS(chatPK(chatID))},
			UpdateExpression:          aws.String(setExpr),
			ConditionExpression:       aws.String(condExpr),
			ExpressionAttributeValues: values(),
		}},
	}
	for _, member := range members {
		transactItems = append(transactItems, types.TransactWriteItem{
			Update: &types.Update{
				TableName:        aws.String(s.dmInboxTable),
				Key:              map[string]types.AttributeValue{attrPK: avS(userPK(member)), attrSK: avS(chatSK(chatID))},
				UpdateExpression: aws.String(setExpr),
				// Each inbox row advances only if the new value is strictly
				// newer. Also guards against upserting a malformed row if the
				// member's row were somehow missing.
				ConditionExpression:       aws.String(condExpr),
				ExpressionAttributeValues: values(),
			},
		})
	}

	_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: transactItems})
	if err != nil {
		// A concurrent advance moved last_activity to/past ts; treat as no-op.
		// last_activity is a derived value that self-heals on the next bump.
		if isTransactionCanceled(err) {
			return false, members, nil
		}
		return false, nil, err
	}
	return true, members, nil
}

func (s *store) chatItem(c *chat.Chat) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		attrPK:           avS(chatPK(c.ID)),
		attrType:         avN(uint64(c.Type)),
		attrLastActivity: avN(uint64(c.LastActivity.UnixNano())),
	}
	// A group's membership lives in group_members, not on the canonical item —
	// an inline list could not hold a large group. Title is group-only.
	if c.Type == chatpb.ChatType_GROUP {
		if c.Title != "" {
			item[attrTitle] = avS(c.Title)
		}
	} else {
		item[attrMembers] = membersAttr(c.Members)
	}
	if c.LastMessageID != nil {
		item[attrLastMessageID] = avN(c.LastMessageID.Value)
	}
	return item
}

// groupMemberItem is a joined membership record, as written at group creation.
// It matches what addGroupMembers converges to: state joined, user set (the
// gsiByUser hash key), joined_at present (the sparse gsiByJoinedAt keys off its
// presence), no left_at.
func (s *store) groupMemberItem(chatID *commonpb.ChatId, member *commonpb.UserId, joinedAt time.Time) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK:       avS(chatPK(chatID)),
		attrSK:       avS(userPK(member)),
		attrUser:     avS(userIndexKey(member)),
		attrState:    avN(memberStateJoined),
		attrJoinedAt: avN(uint64(joinedAt.UnixNano())),
	}
}

func (s *store) dmInboxItem(c *chat.Chat, member *commonpb.UserId) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		attrPK:           avS(userPK(member)),
		attrSK:           avS(chatSK(c.ID)),
		attrType:         avN(uint64(c.Type)),
		attrFeed:         avS(feedPK(member, c.Type)),
		attrMembers:      membersAttr(c.Members),
		attrLastActivity: avN(uint64(c.LastActivity.UnixNano())),
	}
	if c.LastMessageID != nil {
		item[attrLastMessageID] = avN(c.LastMessageID.Value)
	}
	return item
}

// chatFromItem builds a Chat from a chats or dm_inbox item. The chat ID is not
// stored on the item; it is recovered from the item's key by the caller and
// passed in.
func chatFromItem(chatID *commonpb.ChatId, item map[string]types.AttributeValue) (*chat.Chat, error) {
	typeVal, err := parseN(item[attrType])
	if err != nil {
		return nil, err
	}
	nanos, err := parseInt(item[attrLastActivity])
	if err != nil {
		return nil, err
	}
	c := &chat.Chat{
		ID:           &commonpb.ChatId{Value: append([]byte(nil), chatID.Value...)},
		Type:         protoChatType(uint64(typeVal)),
		Members:      membersFromItem(item),
		Title:        asS(item[attrTitle]),
		LastActivity: time.Unix(0, nanos).UTC(),
	}
	// last_message_id is absent until the chat's first message.
	if _, ok := item[attrLastMessageID]; ok {
		id, err := parseN(item[attrLastMessageID])
		if err != nil {
			return nil, err
		}
		c.LastMessageID = &messagingpb.MessageId{Value: id}
	}
	return c, nil
}

func membersFromItem(item map[string]types.AttributeValue) []*commonpb.UserId {
	list := asL(item[attrMembers])
	members := make([]*commonpb.UserId, len(list))
	for i, av := range list {
		members[i] = &commonpb.UserId{Value: append([]byte(nil), asB(av)...)}
	}
	return members
}

func membersAttr(members []*commonpb.UserId) types.AttributeValue {
	values := make([]types.AttributeValue, len(members))
	for i, m := range members {
		values[i] = avB(m.Value)
	}
	return &types.AttributeValueMemberL{Value: values}
}

// userKeyPrefix prefixes a user ID in the dm_inbox pk and the group_members
// sk, mirroring chatKeyPrefix.
const userKeyPrefix = "user#"

func chatPK(chatID *commonpb.ChatId) string { return chatKeyPrefix + hex.EncodeToString(chatID.Value) }
func chatSK(chatID *commonpb.ChatId) string { return chatKeyPrefix + hex.EncodeToString(chatID.Value) }
func userPK(userID *commonpb.UserId) string { return userKeyPrefix + hex.EncodeToString(userID.Value) }

// userIndexKey is the gsiByUser hash key: the member's ID in bare hex, with no
// "user#" prefix. The prefix disambiguates a key space shared by several entity
// types — the group_members sk needs it, as does every pk here — but this
// attribute's name already fixes its type, so the prefix would say nothing the
// name doesn't. Query gsiByUser with this, never with userPK: the two encode the
// same ID differently, and a mismatch is a query that returns nothing rather
// than an error.
func userIndexKey(userID *commonpb.UserId) string { return hex.EncodeToString(userID.Value) }

// feedPK is the composite hash key of gsiByTypeActivity: one chat type's slice
// of a user's inbox. The chat type is encoded by its stable proto enum number.
func feedPK(userID *commonpb.UserId, chatType chatpb.ChatType) string {
	return fmt.Sprintf("%s#%d", userPK(userID), chatType)
}

// chatIDFromSK recovers a chat ID from a dm_inbox item's sk ("chat#<hex>"),
// the inverse of chatSK.
func chatIDFromSK(item map[string]types.AttributeValue) (*commonpb.ChatId, error) {
	sk := asS(item[attrSK])
	encoded, ok := strings.CutPrefix(sk, chatKeyPrefix)
	if !ok {
		return nil, fmt.Errorf("unexpected sk %q", sk)
	}
	id, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding chat id from sk %q: %w", sk, err)
	}
	return &commonpb.ChatId{Value: id}, nil
}

// userIDFromSK recovers a user ID from a group_members item's sk
// ("user#<hex>"), the inverse of userPK.
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

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avB(v []byte) types.AttributeValue {
	return &types.AttributeValueMemberB{Value: append([]byte(nil), v...)}
}
func avN(v uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
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

func asL(av types.AttributeValue) []types.AttributeValue {
	if l, ok := av.(*types.AttributeValueMemberL); ok {
		return l.Value
	}
	return nil
}

func parseN(av types.AttributeValue) (uint64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseUint(n.Value, 10, 64)
}

func parseInt(av types.AttributeValue) (int64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseInt(n.Value, 10, 64)
}

func isTransactionCanceled(err error) bool {
	var tce *types.TransactionCanceledException
	return errors.As(err, &tce)
}

// conditionalCheckFailedCode is the CancellationReason code DynamoDB reports for
// the item whose condition expression failed.
const conditionalCheckFailedCode = "ConditionalCheckFailed"

// isChatExistsCancellation reports whether a chat-creation transaction was
// cancelled because the canonical item already existed.
//
// A cancellation alone does not mean that: DynamoDB also cancels for
// TransactionConflict, throttling, and insufficient capacity, none of which say
// anything about the chat existing. Reporting those as ErrChatExists would be
// worse than a plain error, because callers treat ErrChatExists as the benign
// steady state and swallow it — a throttled creation would look like success
// with nothing written. So the decision is made on the reason for the canonical
// item specifically. Reasons are positional over the request's items, and every
// creation transaction here puts the canonical item first; items that were fine
// carry the code "None".
func isChatExistsCancellation(err error) bool {
	var tce *types.TransactionCanceledException
	if !errors.As(err, &tce) || len(tce.CancellationReasons) == 0 {
		return false
	}
	return aws.ToString(tce.CancellationReasons[0].Code) == conditionalCheckFailedCode
}

func isConditionalCheckFailed(err error) bool {
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}

func protoChatType(v uint64) chatpb.ChatType {
	return chatpb.ChatType(v)
}

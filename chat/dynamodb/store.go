package dynamodb

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// The chat store spans eight tables:
//
//	chats     pk = "chat#<id>" (one item per chat). Canonical metadata: type,
//	          members (the DM participants; absent for groups), title, creator
//	          and picture (groups only), last_activity. GetChat is a point read and
//	          AdvanceLastActivity is an O(1) update of the source of truth.
//
//	dm_inbox  pk = "user#<id>", sk = "chat#<id>" (one item per (user, DM)). The
//	          per-user DM inbox index. A GSI on (feed, last_activity) — where
//	          feed = "user#<id>#<type>" partitions each user's inbox by chat
//	          type — lets one type's DMs be listed most-recently-active first
//	          with true server-side pagination and no filtering. last_activity
//	          and the participants are denormalized so the inbox renders from
//	          one query. AdvanceLastActivity fans the new last_activity out to
//	          each member's row (two for a DM), re-sorting the GSI. The row
//	          of a member excluded from the feed (see chat.Store.PutChat)
//	          carries neither feed nor last_activity, so it is in neither GSI:
//	          it records membership alone, and no advance ever touches it.
//
//	group_members  pk = "chat#<id>", sk = "user#<id>" (one item per (group,
//	          member), kept as a tombstone after departure). Group membership,
//	          mutable — unlike a DM's inline member list. Group chats
//	          deliberately have no per-message inbox fan-out: a send touches
//	          only the canonical chats item, and a user's group feed is
//	          assembled at read time via the inverted gsiByUser. Joined members
//	          are enumerated from the partition itself, which tombstone expiry
//	          keeps near-dense (see getGroupMembers).
//
//	          The partition also holds one aggregates item, sk = "#meta" (see
//	          skMeta), carrying the group's roster summary: member_count and
//	          version (see chat.RosterSummary), maintained by compare-and-set
//	          in the same transaction as every membership transition, so a
//	          group's size and version are a point read rather than an
//	          enumeration. "#" sorts before "user#", so it is the head of the
//	          partition.
//
//	          Both GSIs are sparse, keyed by attributes only membership rows
//	          carry (user, joined_at) — that discipline is load-bearing: the
//	          #meta item, and any new item type added to this table, must omit
//	          those attributes or it leaks into the indexes.
//
//	chat_user_state  pk = "user#<id>", sk = "chat#<id>" (one item per (user,
//	          chat) the user has ever set state on; see chat.Store).
//	          What a chat holds about one user, independent of membership:
//	          today a mute (muted_until) and the state's version. Keyed by
//	          user so that the user's own read — their state across a page of
//	          chats, the one they expect to reflect the mute they just set —
//	          is a strongly consistent query on their partition, bounded to
//	          the page's key range, rather than a key probe per chat.
//
//	          The chat-scoped read the push fan-out needs — who has this chat
//	          muted right now — has two shapes, and the fan-out picks by size.
//	          The sparse gsiByMuted on (chat, muted_until) — chat is the raw
//	          chat ID as a binary attribute, the sk's bytes without the prefix
//	          or hex, since an index key is never parsed and the smaller key
//	          is copied into every index entry; muted_until is present only
//	          while a mute is recorded — holds exactly the recorded mutes, and a
//	          range on muted_until yields the active ones as one set, billed
//	          by what it returns: the read for a chat few have muted. The
//	          inverted gsiUserStateByUser on (chat, pk) orders a chat's records
//	          by user, the same "user#<id>" order a group's membership
//	          partition uses, so a chat most have muted is walked as two
//	          cursors over one order, roster and records, never holding
//	          either whole; it carries every record with the full item
//	          projected, so any state added later is readable per chat
//	          without a new index, and a mute is picked out by a filter. Both
//	          are eventually consistent, which that read tolerates. An
//	          indefinite mute is stored as muteForeverUntil so it sorts above
//	          any "now".
//
//	          Which shape is cheaper is decided from a count of the chat's
//	          recorded mutes, kept on one aggregates item per chat, pk =
//	          "chat#<id>", sk = "#meta" (see skMeta), and moved in the same
//	          transaction as the record whenever a mute is first recorded or
//	          cleared — never when one is replaced, and never when a timed one
//	          lapses, so it bounds the active mutes from above. The #meta item
//	          carries neither chat nor muted_until, which is what keeps it out
//	          of both indexes.
//
//	          Every write moves version by exactly one and fails its
//	          condition — returning the current item — when it would be a
//	          no-op, so the no-op path costs no second read and never creates
//	          an item. Nothing deletes an item: a cleared mute drops
//	          muted_until (and so its gsiByMuted entry) and keeps the record
//	          and its version.
//
//	chat_activity  pk = "chat#<id>", sk = "user#<id>" (one item per (group,
//	          user) the user has sent in; see chat.RecentSender). A group's
//	          activity records, independent of membership: last_sent_at
//	          (epoch ms), activity_score (epoch ms; see
//	          chat.NextActivityScore) and expires_at (DynamoDB TTL, epoch
//	          seconds, ActivityRetention after the last send). Keyed by
//	          (chat, user) so a send is a read and a conditional update of
//	          one known item, with no duplicate rows, while lsiByLastSentAt
//	          orders the partition by recency, which is the read: a group's
//	          recent senders are one eventually consistent query, billed by
//	          the page.
//
//	          lsiByActivityScore orders the same partition by
//	          activity_score, a frequency-weighted ordering that every
//	          recorded send maintains (see RecordSend), read by
//	          GetActiveSenders. The score is epoch-ms-denominated, equal to
//	          last_sent_at for a user with one recorded send and running
//	          ahead of it (possibly past now) as their sends accumulate, so a
//	          row written before scores existed, which is missing from the
//	          index, was given activity_score = last_sent_at by a one-off
//	          backfill and compares correctly with the rest; one that
//	          somehow still lacks it reads and scores as if it had it, but
//	          GetActiveSenders does not find it.
//
//	chat_key_envelopes  pk = "user#<id>", sk = "chat#<id>" (one item per
//	          (user, private group) the user holds a key envelope for; see
//	          chat.KeyEnvelope). The envelope as stored: scheme, nonce,
//	          ciphertext, and wrapped_by, the user who stored it. Keyed by
//	          user, like chat_user_state, since every read is a user's read
//	          of their own envelope: a strongly consistent point read. There
//	          is no index and no read by chat, because nothing needs one: a
//	          group's size is unbounded and nothing enumerates its envelopes.
//
//	          A write is one conditional put of the one item, refused when
//	          the stored envelope is one the user wrapped themself
//	          (wrapped_by is the user), and the refusal returns the stored
//	          item, so the caller learns which envelope stands without a
//	          read. An item is deleted when its user leaves the group, in
//	          the transaction that records the departure (see
//	          RemoveGroupMember), so no departed user keeps one. No item
//	          expires.
//
//	chat_lobbies  pk = "user#<id>", sk = "chat#<id>" (one item per (user,
//	          private group) the user is waiting in the lobby of; see
//	          chat.LobbyEntry), with entered_at (epoch nanos). gsiLobbyByChat
//	          on (sk, entered_at) pages a group's lobby earliest first: the
//	          sk is the chat, so no attribute repeats it, and the index is
//	          sparse all the same because entered_at, its range key, is on
//	          entries alone. Keyed
//	          by user like chat_user_state, so that a user's own entries are
//	          one strongly consistent query with no index: in_lobby and
//	          EnterLobby's response today, and the listing of the lobbies a
//	          user waits in when that RPC comes (decided). The cost is the
//	          creator's page, which a chat-keyed table with an LSI could have
//	          served strongly consistent and here comes off a GSI that trails
//	          writes briefly, as the proto allows; the creator reconciles
//	          against the LobbyUpdates they receive.
//
//	          Two aggregates items count the entries, both sk = "#meta" (see
//	          skMeta) with lobby_count: pk = "chat#<id>" is the lobby's size,
//	          pk = "user#<id>" the lobbies the user waits in. Neither carries
//	          entered_at, so neither is in the index. Every
//	          write of an entry moves both counts in the same transaction,
//	          and an entry is made conditionally on both counts being under
//	          their caps and on the user's group_members row not being
//	          joined (see EnterLobby), so the caps hold under concurrent
//	          entries without a read and a member never gains an entry. An
//	          admission (see AdmitFromLobby) is the membership transition's
//	          transaction carrying the entry's removal, its counts and the
//	          key envelope, so the two tables agree in both directions.
//
//	chat_featured_groups  pk = "user#<id>", sk = "pos#<nn>" (one item per
//	          position in the user's featured groups; see
//	          chat.FeaturedGroups), with chat, the group's raw ID (B), and
//	          version, the list's version as of the write that put the item.
//	          The position is zero-padded (see featuredSK), so the sk order
//	          is the list order and the list is one query of the
//	          partition, in order, with nothing to sort: eventually
//	          consistent for a reader, strongly consistent for the replace
//	          that decides against it (see SetFeaturedGroups). One
//	          aggregates item heads the partition, sk = "#meta" (see skMeta),
//	          with the list's version and featured_count.
//
//	          A replace is one transaction: the #meta item compare-and-set on
//	          version, a put of every position the new list fills and a
//	          delete of every position past it that the old list filled, so
//	          it is at most one item more than the longer of the two lists,
//	          and needs no read of the items it replaces. The compare-and-set
//	          is what makes it whole: without it, a short list landing after
//	          a long one would delete only the positions it knew of and leave
//	          the rest. Every replace rewrites every position, stamping each
//	          with the new version, which is what lets a read tell a list
//	          from a mix of two: a query is consistent per item, not as a
//	          set, so the read is verified against #meta and taken again when
//	          it straddles a write (see readFeaturedGroups).
//
//	          There is no index. The chat attribute is raw so that a
//	          (chat, pk) GSI, asking who features a group, can be added
//	          later and fill from the items as they stand; #meta omits it,
//	          which keeps it out of such an index.
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
	// carries left_at instead), so only joined members appear in the index, in
	// join order. It is what pages a large group's roster newest-first without
	// reading the whole partition (see GetGroupRosterPage); the whole-roster
	// reads (GetMembers, GetGroupRoster) walk the base partition instead.
	gsiByJoinedAt = "by_joined_at"

	// gsiByMuted is the sparse (chat, muted_until) index on chat_user_state:
	// the users with a mute recorded on a chat, in order of when it ends.
	// KEYS_ONLY, since the user is in the projected pk and the query wants
	// nothing else.
	gsiByMuted = "by_muted"

	// gsiUserStateByUser is the inverted (chat, user) index on
	// chat_user_state: a chat's records in user order, full item projected.
	gsiUserStateByUser = "by_user"

	// lsiByLastSentAt is the (chat, last_sent_at) LSI on chat_activity: a
	// group's activity records in recency order (see GetRecentSenders).
	lsiByLastSentAt = "by_last_sent_at"

	// lsiByActivityScore is the (chat, activity_score) LSI on chat_activity:
	// a group's activity records in activity-score order (see
	// GetActiveSenders). It is sparse on activity_score, which records
	// written before scores existed lacked until backfilled.
	lsiByActivityScore = "by_activity_score"

	// gsiLobbyByChat is the (sk, entered_at) index on chat_lobbies: a group's
	// lobby in entry order (see GetLobbyPage), hashed on the sk, which is the
	// chat. It is sparse on entered_at, which only entries carry: the #meta
	// items must omit it.
	gsiLobbyByChat = "by_chat"

	// chatKeyPrefix prefixes a chat ID in the chats table pk, the dm_inbox sk
	// and the chat_user_state sk. The chat ID is recovered from the key, so it
	// is not stored as its own attribute — except in chat_user_state, where a
	// record also carries it raw, as the binary chat attribute that keys both
	// of that table's indexes (see attrChat).
	chatKeyPrefix = "chat#"

	// skMeta is the sort key of a group's aggregates item in group_members.
	skMeta = "#meta"

	attrPK                   = "pk"
	attrSK                   = "sk"
	attrType                 = "type"
	attrFeed                 = "feed"
	attrMembers              = "members"
	attrExcludedFromFeed     = "excluded_from_feed" // DM chats item: binary set of the members the DM excludes from the feed (see chat.Store.PutChat), absent when none
	attrTitle                = "title"
	attrIsStaffOnly          = "is_staff_only"
	attrMinListenerBalance   = "min_listener_balance" // map: see minimumBalanceAttr
	attrIsCreatorOnlySpeaker = "is_creator_only_speaker"
	attrMinSpeakerBalance    = "min_speaker_balance" // map: see minimumBalanceAttr
	attrIsPrivate            = "is_private"
	attrCreator              = "creator"
	attrDescription          = "description"   // group chats item: absent when the group has none
	attrProfilePictureBlobID = "picture"       // group chats item: the profile picture's ORIGINAL, named before cover pictures existed
	attrCoverPictureBlobID   = "cover_picture" // group chats item: the cover picture's ORIGINAL
	attrState                = "state"
	attrUser                 = "user" // member id, bare hex — see userIndexKey
	attrJoinedAt             = "joined_at"
	attrLeftAt               = "left_at"
	attrExpiresAt            = "expires_at" // DynamoDB TTL, epoch seconds: group_members tombstones (see tombstoneTTL) and chat_activity records (see chat.ActivityRetention)
	attrLastActivity         = "last_activity"
	attrLastMessageID        = "last_message_id"
	attrMemberCount          = "member_count" // #meta item: joined member count
	attrVersion              = "version"
	attrChat                 = "chat"           // chat_user_state records: the raw chat ID bytes (B), keying gsiByMuted and gsiUserStateByUser
	attrMutedUntil           = "muted_until"    // chat_user_state: epoch seconds, present only while a mute is recorded — see muteForeverUntil
	attrMutedCount           = "muted_count"    // chat_user_state #meta item: records with a mute recorded
	attrLastSentAt           = "last_sent_at"   // chat_activity: epoch ms of the latest recorded send
	attrActivityScore        = "activity_score" // chat_activity: epoch ms, see chat.NextActivityScore; absent on records written before scores
	attrScheme               = "scheme"         // chat_key_envelopes: chatpb.KeyEnvelope_Scheme, by number
	attrNonce                = "nonce"          // chat_key_envelopes: the envelope's nonce (B)
	attrCiphertext           = "ciphertext"     // chat_key_envelopes: the wrapped chat key (B)
	attrWrappedBy            = "wrapped_by"     // chat_key_envelopes: the raw ID of the user who stored the envelope (B)
	attrEnteredAt            = "entered_at"     // chat_lobbies: epoch nanos of the entry, keying gsiLobbyByChat
	attrLobbyCount           = "lobby_count"    // chat_lobbies #meta items: a lobby's size (chat# pk) or a user's lobbies (user# pk)
	attrFeaturedCount        = "featured_count" // chat_featured_groups #meta item: how many groups the list holds

	// Keys of the min_listener_balance map.
	attrBalanceCurrency     = "currency"
	attrBalanceNativeAmount = "amount"
	attrBalanceMints        = "mints"
)

// A membership transition is a two-item transaction — the membership record and
// the group's #meta roster summary — and every transition in a group contends
// on that one summary item, twice over: DynamoDB cancels the losers of a
// concurrent write to it with TransactionConflict, which the SDK does not
// retry, and the summary's compare-and-set fails when a concurrent writer got
// there first. Both are absorbed here with a bounded number of attempts; the
// former backs off (short, jittered, doubling), the latter retries at once
// from the value the failure returned. The ceiling is what one item's
// transactional throughput allows; a group whose churn exceeds it is the
// signal to revisit the summary's design.
const (
	maxMembershipAttempts = 8
	membershipBackoffBase = 10 * time.Millisecond
	membershipBackoffMax  = 250 * time.Millisecond

	codeTransactionConflict = "TransactionConflict"
)

// Values of a group_members item's numeric state attribute, following the
// store's convention of encoding enums by stable number (see attrType). 0 is
// reserved as unspecified, per proto enum convention, so a future proto member
// state enum can adopt these values directly.
//
// A left member's item is a tombstone: it makes re-adding an idempotent update
// of the same key, and carries the version of the departure (see
// transitionMembership) so a reader following the user's transitions can tell
// a delayed copy of the join it undid from news. Because the (chat, user) key
// is immutable across state changes, paginating members by sort key stays
// cursor-stable under concurrent joins and leaves.
//
// Tombstones are short-lived: the departure stamps expires_at and DynamoDB TTL
// sweeps the item after tombstoneTTL, while a rejoin clears the stamp so an
// active membership never expires. The version only matters for as long as a
// copy of the departure's event could still be in flight — seconds, since a
// forward times out in one and a full outbox drops rather than queues — and
// the TTL is generous against that, so the user's slice of the table stays
// bounded by their current memberships plus recent churn, however many groups
// they have passed through. TTL is garbage collection, not semantics: an
// expired item is returned until swept, so readers trust state, never the
// clock. "Formerly a member" is thus not durable here; anything that must
// outlive a membership belongs in a table of its own.
const (
	memberStateJoined = 1
	memberStateLeft   = 2

	// tombstoneTTL is how long after a departure its tombstone is eligible for
	// the TTL sweep. The sweep itself runs within a couple of days of that, so
	// the value states what the mechanism needs rather than when the row goes.
	tombstoneTTL = time.Hour
)

type store struct {
	client              *dynamodb.Client
	chatsTable          string
	dmInboxTable        string
	groupMembersTable   string
	userStateTable      string
	activityTable       string
	keyEnvelopesTable   string
	lobbiesTable        string
	featuredGroupsTable string

	exclusions chat.FeedExclusions
}

// NewInDynamoDB returns a chat.Store backed by the given DynamoDB tables,
// creating every DM with a user in excludedFromFeed excluding them from the
// feed (see chat.FeedExclusions); nil excludes no one. Use CreateTables to
// provision the tables.
func NewInDynamoDB(client *dynamodb.Client, chatsTable, dmInboxTable, groupMembersTable, userStateTable, activityTable, keyEnvelopesTable, lobbiesTable, featuredGroupsTable string, excludedFromFeed []*commonpb.UserId) chat.Store {
	return &store{
		exclusions:          chat.NewFeedExclusions(excludedFromFeed),
		client:              client,
		chatsTable:          chatsTable,
		dmInboxTable:        dmInboxTable,
		groupMembersTable:   groupMembersTable,
		userStateTable:      userStateTable,
		activityTable:       activityTable,
		keyEnvelopesTable:   keyEnvelopesTable,
		lobbiesTable:        lobbiesTable,
		featuredGroupsTable: featuredGroupsTable,
	}
}

// maxTransactWriteItems is DynamoDB's per-transaction item limit.
const maxTransactWriteItems = 100

// A group chat is created in one transaction covering the canonical item, the
// #meta counter, and every membership record, so the domain's cap on the initial
// member set must leave room for those two items within maxTransactWriteItems.
// Asserted rather than assumed, since the cap lives in the chat package and this
// is the constraint it exists to respect: raising it past the ceiling fails to
// compile here, because the difference underflows uint.
const _ uint = maxTransactWriteItems - (chat.MaxGroupChatCreationMembers + 2)

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
// condition enforces uniqueness for the whole write. The members the store
// excludes from the feed, if the DM has any, are recorded on the
// canonical item as a binary set, where every advance reads them, and each
// gets a row that records membership alone (see dmInboxItem). A set cannot be
// empty, so a DM excluding no one has no attribute at all.
func (s *store) putDmChat(ctx context.Context, c *chat.Chat) error {
	if len(c.Members) == 0 {
		return chat.ErrNoMembers
	}

	excluded := s.exclusions.For(c)
	canonical := s.chatItem(c)
	if len(excluded) > 0 {
		canonical[attrExcludedFromFeed] = avBS(excluded)
	}
	transactItems := []types.TransactWriteItem{
		{Put: &types.Put{
			TableName:           aws.String(s.chatsTable),
			Item:                canonical,
			ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
		}},
	}
	for _, member := range c.Members {
		isExcluded := slices.ContainsFunc(excluded, func(u *commonpb.UserId) bool { return bytes.Equal(u.Value, member.Value) })
		transactItems = append(transactItems, types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String(s.dmInboxTable),
				Item:      s.dmInboxItem(c, member, isExcluded),
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

// putGroupChat creates a group chat: the canonical metadata item, the #meta
// counter seeded with the initial member count, and every initial membership
// record, in one transaction. The canonical item's condition enforces uniqueness
// for the whole write, so a duplicate reports ErrChatExists with nothing else
// written.
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
		{Put: &types.Put{
			TableName: aws.String(s.groupMembersTable),
			Item:      s.groupMetaItem(c.ID, uint64(len(members))),
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

func (s *store) AddGroupMembers(ctx context.Context, chatID *commonpb.ChatId, userIDs []*commonpb.UserId) (bool, chat.RosterSummary, error) {
	if !chat.IsGroupChatID(chatID) {
		return false, chat.RosterSummary{}, fmt.Errorf("not a group chat id")
	}

	return s.addGroupMembers(ctx, chatID, userIDs)
}

// addGroupMembers upserts joined membership records. A typo'd chat ID cannot
// accrete orphaned rows: the roster read the writes start from is the
// existence gate (see readRosterSummaryForWrite), and the transition below
// never runs without it. Each member is an independent conditional transition: an
// already-joined member is left untouched (preserving their original
// joined_at), while a new or departed member is (re)joined with a fresh join
// time. joined_at is present iff joined — the sparse gsiByJoinedAt keys off its
// presence — so rejoining also clears the tombstone's left_at, and its
// expires_at, so the rejoined row cannot be swept.
//
// The roster summary is read once, up front, and threaded through the
// transitions: each successful one advances the local copy, so a batch costs
// one read however many members it joins (see transitionMembership).
func (s *store) addGroupMembers(ctx context.Context, chatID *commonpb.ChatId, userIDs []*commonpb.UserId) (bool, chat.RosterSummary, error) {
	roster, err := s.readRosterSummaryForWrite(ctx, chatID)
	if err != nil {
		return false, chat.RosterSummary{}, err
	}

	changed := false
	for _, userID := range userIDs {
		joined, err := s.transitionMembership(ctx, chatID, s.joinUpdate(chatID, userID), 1, &roster, nil)
		if err != nil {
			return changed, roster, err
		}
		changed = changed || joined
	}
	return changed, roster, nil
}

// joinUpdate is the membership transition that (re)joins userID to chatID
// (see addGroupMembers), for transitionMembership to carry.
func (s *store) joinUpdate(chatID *commonpb.ChatId, userID *commonpb.UserId) *types.Update {
	return &types.Update{
		TableName: aws.String(s.groupMembersTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(userPK(userID)),
		},
		UpdateExpression: aws.String(fmt.Sprintf(
			"SET #state = :joined, #user = :user, %s = :now, %s = :version REMOVE %s, %s", attrJoinedAt, attrVersion, attrLeftAt, attrExpiresAt,
		)),
		ConditionExpression:      aws.String("attribute_not_exists(#state) OR #state <> :joined"),
		ExpressionAttributeNames: map[string]string{"#state": attrState, "#user": attrUser},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":joined": avN(memberStateJoined),
			":user":   avS(userIndexKey(userID)),
			":now":    avN(uint64(time.Now().UTC().UnixNano())),
		},
	}
}

func (s *store) RemoveGroupMember(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, discardKeyEnvelope bool) (bool, chat.RosterSummary, error) {
	if !chat.IsGroupChatID(chatID) {
		return false, chat.RosterSummary{}, fmt.Errorf("not a group chat id")
	}

	roster, err := s.readRosterSummaryForWrite(ctx, chatID)
	if err != nil {
		return false, chat.RosterSummary{}, err
	}

	// Tombstone, don't delete: the item keeps the departure's version for as
	// long as it could matter, then expires (see tombstoneTTL). Only a joined
	// member transitions — the condition makes removing a non-member (or an
	// unknown user) a no-op rather than an upsert of a malformed tombstone.
	// Removing joined_at drops the member from the sparse gsiByJoinedAt.
	now := time.Now().UTC()
	leave := &types.Update{
		TableName: aws.String(s.groupMembersTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(userPK(userID)),
		},
		UpdateExpression: aws.String(fmt.Sprintf(
			"SET #state = :left, %s = :now, %s = :version, %s = :expires REMOVE %s", attrLeftAt, attrVersion, attrExpiresAt, attrJoinedAt,
		)),
		ConditionExpression:      aws.String("#state = :joined"),
		ExpressionAttributeNames: map[string]string{"#state": attrState},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":joined":  avN(memberStateJoined),
			":left":    avN(memberStateLeft),
			":now":     avN(uint64(now.UnixNano())),
			":expires": avN(uint64(now.Add(tombstoneTTL).Unix())),
		},
	}
	// The departing user's key envelope goes in the same transaction, so it
	// is deleted iff the departure happens. The delete is unconditional: a
	// user with no envelope leaves all the same.
	var alongside []types.TransactWriteItem
	if discardKeyEnvelope {
		alongside = append(alongside, types.TransactWriteItem{Delete: &types.Delete{
			TableName: aws.String(s.keyEnvelopesTable),
			Key:       keyEnvelopeKey(chatID, userID),
		}})
	}
	changed, err := s.transitionMembership(ctx, chatID, leave, -1, &roster, alongside)
	return changed, roster, err
}

// readRosterSummaryForWrite is the strongly consistent read of the #meta item
// that a membership write starts from. Every group has one — creation seeds
// it in the same transaction as the canonical item, and nothing deletes it —
// so the read is also the write path's existence check: a missing item is a
// chat that does not exist, and a write against a known group pays no separate
// read to learn that.
func (s *store) readRosterSummaryForWrite(ctx context.Context, chatID *commonpb.ChatId) (chat.RosterSummary, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.groupMembersTable),
		Key:            map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(skMeta)},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return chat.RosterSummary{}, err
	}
	if len(out.Item) == 0 {
		return chat.RosterSummary{}, chat.ErrChatNotFound
	}
	return rosterSummaryFromItem(out.Item)
}

// transitionMembership applies one member's conditional state change together
// with the group's next roster summary, in a single transaction: the summary
// moves iff the membership does, so the two can never disagree. The
// transition's own condition is what decides whether anything happens — when
// it fails (the member is already in the target state) the whole transaction
// cancels and that is reported as the no-op it is.
//
// The summary is a compare-and-set rather than a blind increment: the #meta
// item is set to *roster advanced by the transition (count by delta, version
// by one), conditioned on its version still being roster.Version. That is
// what lets the row be stamped with the version that moved it and the caller
// know the exact summary each write produced, with no read afterward. On
// success *roster is advanced to match; the next transition in a batch chains
// from it without another read.
//
// alongside are further writes the transition carries: they commit iff it
// does, and are not made when it is a no-op. One may carry a condition of its
// own: when it fails, the whole transaction cancels, nothing is retried, and
// the failure is reported as an alongsideConditionFailed naming the item, for
// the caller to translate (see AdmitFromLobby). An unconditional one can
// cancel the transaction only by losing to a concurrent write on its own
// item.
//
// Cancellation reasons are positional over the transaction's items: [0] is the
// membership transition, [1] the summary, and the rest alongside's. A failed
// [1] condition means a
// concurrent writer moved the version — the failed item is returned with the
// cancellation, so *roster is refreshed from it and the transition retried
// with no extra read. Losing the write itself to a concurrent transaction on
// the same item, any of them, surfaces as TransactionConflict and is retried
// with backoff. Both share the attempt budget (see maxMembershipAttempts).
// The transition's own no-op is judged first, then the summary, then
// alongside's conditions: a no-op is a no-op whatever else failed, and a
// stale summary is retried before an alongside condition is believed, since
// the retry re-evaluates it.
func (s *store) transitionMembership(ctx context.Context, chatID *commonpb.ChatId, transition *types.Update, delta int64, roster *chat.RosterSummary, alongside []types.TransactWriteItem) (bool, error) {
	backoff := membershipBackoffBase
	for attempt := 0; ; attempt++ {
		next := chat.RosterSummary{
			MemberCount: uint64(int64(roster.MemberCount) + delta),
			Version:     roster.Version + 1,
		}
		transition.ExpressionAttributeValues[":version"] = avN(next.Version)
		transactItems := []types.TransactWriteItem{
			{Update: transition},
			{Update: &types.Update{
				TableName: aws.String(s.groupMembersTable),
				Key: map[string]types.AttributeValue{
					attrPK: avS(chatPK(chatID)),
					attrSK: avS(skMeta),
				},
				UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :count, %s = :version", attrMemberCount, attrVersion)),
				ConditionExpression: aws.String(fmt.Sprintf("%s = :expected", attrVersion)),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":count":    avN(next.MemberCount),
					":version":  avN(next.Version),
					":expected": avN(roster.Version),
				},
				ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
			}},
		}
		transactItems = append(transactItems, alongside...)

		_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: transactItems})
		if err == nil {
			*roster = next
			return true, nil
		}
		reasons, ok := cancellationReasons(err)
		if !ok || len(reasons) != len(transactItems) {
			return false, err
		}
		codes := make([]string, len(reasons))
		for i, reason := range reasons {
			codes[i] = aws.ToString(reason.Code)
		}

		// The budget applies to the retries alone: a no-op is a no-op on the
		// last attempt too, and is reported as one, not as an exhausted retry.
		exhausted := attempt+1 >= maxMembershipAttempts
		switch {
		case codes[0] == conditionalCheckFailedCode:
			// Already in the target state: nothing happened, nothing to record.
			// Checked first — if the summary is also stale, there is still no
			// transition, and the summary in hand is what the caller reports.
			return false, nil
		case codes[1] == conditionalCheckFailedCode:
			// A concurrent writer moved the summary. Its current value came back
			// with the failure; chain from it and go again, immediately — this is
			// a lost race, not a throttled write. Nothing deletes the item, so
			// its absence here is a broken invariant, not a state to retry from.
			if exhausted {
				return false, fmt.Errorf("membership transition for chat %x: %w", chatID.Value, err)
			}
			if len(reasons[1].Item) == 0 {
				return false, fmt.Errorf("chat %x lost its %s item during a membership transition", chatID.Value, skMeta)
			}
			current, err := rosterSummaryFromItem(reasons[1].Item)
			if err != nil {
				return false, err
			}
			*roster = current
		case slices.Contains(codes[2:], conditionalCheckFailedCode):
			return false, alongsideConditionFailed{Index: slices.Index(codes[2:], conditionalCheckFailedCode)}
		case isTransactionConflict(codes):
			if exhausted {
				return false, fmt.Errorf("membership transition for chat %x: %w", chatID.Value, err)
			}
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(backoff + rand.N(backoff)):
			}
			backoff = min(2*backoff, membershipBackoffMax)
		default:
			return false, err
		}
	}
}

// SetGroupPicture writes (or, for a nil blobID, removes) the picture attribute
// on the canonical item. The update is conditioned on the item existing so a
// typo'd chat ID cannot upsert a picture-only phantom record; the failed
// condition is what reports ErrChatNotFound.
func (s *store) SetGroupPicture(ctx context.Context, chatID *commonpb.ChatId, blobID *blobpb.BlobId) error {
	if !chat.IsGroupChatID(chatID) {
		return fmt.Errorf("not a group chat id")
	}

	input := &dynamodb.UpdateItemInput{
		TableName:           aws.String(s.chatsTable),
		Key:                 map[string]types.AttributeValue{attrPK: avS(chatPK(chatID))},
		ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s)", attrPK)),
	}
	if blobID == nil {
		input.UpdateExpression = aws.String(fmt.Sprintf("REMOVE %s", attrProfilePictureBlobID))
	} else {
		input.UpdateExpression = aws.String(fmt.Sprintf("SET %s = :picture", attrProfilePictureBlobID))
		input.ExpressionAttributeValues = map[string]types.AttributeValue{":picture": avB(blobID.Value)}
	}

	_, err := s.client.UpdateItem(ctx, input)
	if err != nil {
		if isConditionalCheckFailed(err) {
			return chat.ErrChatNotFound
		}
		return err
	}
	return nil
}

// EditGroup is one update of the canonical item that SETs exactly the
// attributes the edit names, so two edits of different fields never clobber
// each other, conditioned on the item existing as SetGroupPicture is. An
// empty description is REMOVEd rather than stored, so a group without one
// never carries the attribute, whichever way it came to have none.
func (s *store) EditGroup(ctx context.Context, chatID *commonpb.ChatId, edit chat.GroupEdit) error {
	if !chat.IsGroupChatID(chatID) {
		return fmt.Errorf("not a group chat id")
	}
	if edit.IsEmpty() {
		return fmt.Errorf("edit names nothing")
	}

	var sets, removes []string
	values := make(map[string]types.AttributeValue)
	if edit.Title != nil {
		sets = append(sets, fmt.Sprintf("%s = :title", attrTitle))
		values[":title"] = avS(*edit.Title)
	}
	if edit.Description != nil {
		if *edit.Description == "" {
			removes = append(removes, attrDescription)
		} else {
			sets = append(sets, fmt.Sprintf("%s = :description", attrDescription))
			values[":description"] = avS(*edit.Description)
		}
	}
	if edit.ProfilePictureBlobID != nil {
		sets = append(sets, fmt.Sprintf("%s = :picture", attrProfilePictureBlobID))
		values[":picture"] = avB(edit.ProfilePictureBlobID.Value)
	}
	if edit.CoverPictureBlobID != nil {
		sets = append(sets, fmt.Sprintf("%s = :cover_picture", attrCoverPictureBlobID))
		values[":cover_picture"] = avB(edit.CoverPictureBlobID.Value)
	}

	var clauses []string
	if len(sets) > 0 {
		clauses = append(clauses, "SET "+strings.Join(sets, ", "))
	}
	if len(removes) > 0 {
		clauses = append(clauses, "REMOVE "+strings.Join(removes, ", "))
	}
	input := &dynamodb.UpdateItemInput{
		TableName:           aws.String(s.chatsTable),
		Key:                 map[string]types.AttributeValue{attrPK: avS(chatPK(chatID))},
		UpdateExpression:    aws.String(strings.Join(clauses, " ")),
		ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s)", attrPK)),
	}
	// An edit that only removes binds no values, and DynamoDB refuses an empty
	// value map.
	if len(values) > 0 {
		input.ExpressionAttributeValues = values
	}
	_, err := s.client.UpdateItem(ctx, input)
	if err != nil {
		if isConditionalCheckFailed(err) {
			return chat.ErrChatNotFound
		}
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

// getGroupMembers enumerates a group's joined members whole: the page walk
// (see GetGroupMembersPage) drained in one call. A memberless group and a
// nonexistent chat both walk empty; the contract distinguishes them, so the
// canonical item (whose absence is ErrChatNotFound) is consulted only on that
// ambiguous path.
func (s *store) getGroupMembers(ctx context.Context, chatID *commonpb.ChatId) ([]*commonpb.UserId, error) {
	page, err := s.GetGroupMembersPage(ctx, chatID, nil, 0)
	if err != nil {
		return nil, err
	}
	if len(page.Users) == 0 {
		if _, err := s.GetChatByID(ctx, chatID); err != nil {
			return nil, err
		}
	}
	return page.Users, nil
}

// GetGroupMembersPage walks a group's base-table partition rather than the
// sparse gsiByJoinedAt. The partition holds the membership rows plus the
// #meta item, which the key condition's sort-key prefix excludes, and the
// tombstones, which a filter drops; tombstones expire (see tombstoneTTL), so
// the partition is dense up to the group's recent churn and the filter scans
// little. The sort key is "user#<hex>", so the partition's order is ascending
// user-ID bytes — the order the contract promises, and the one
// gsiUserStateByUser ranges a chat's mutes in. Reading the table leaves a
// strongly consistent read available if a caller ever needs one; paging in
// join order is the index's job (see GetGroupRosterPage).
//
// The cursor is resumed as an exclusive start key built from the user alone:
// a Query's start key need not name an existing item, so a cursor that has
// since left the group (or never was in it) resumes just as well. The filter
// runs after DynamoDB applies the limit, so a page is assembled from as many
// queries as it takes to collect limit joined rows, and each is asked for
// only what the page still lacks.
func (s *store) GetGroupMembersPage(ctx context.Context, chatID *commonpb.ChatId, after *commonpb.UserId, limit int) (chat.MembersPage, error) {
	if !chat.IsGroupChatID(chatID) {
		return chat.MembersPage{}, fmt.Errorf("not a group chat id")
	}

	var startKey map[string]types.AttributeValue
	if after != nil {
		startKey = map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(userPK(after)),
		}
	}

	page := chat.MembersPage{Users: make([]*commonpb.UserId, 0)}
	for {
		input := &dynamodb.QueryInput{
			TableName:                aws.String(s.groupMembersTable),
			KeyConditionExpression:   aws.String("#pk = :pk AND begins_with(#sk, :user)"),
			FilterExpression:         aws.String("#state = :joined"),
			ExpressionAttributeNames: map[string]string{"#pk": attrPK, "#sk": attrSK, "#state": attrState},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":     avS(chatPK(chatID)),
				":user":   avS(userKeyPrefix),
				":joined": avN(memberStateJoined),
			},
			ExclusiveStartKey: startKey,
		}
		if limit > 0 {
			input.Limit = aws.Int32(int32(limit - len(page.Users)))
		}
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return chat.MembersPage{}, err
		}
		for _, item := range out.Items {
			userID, err := userIDFromSK(item)
			if err != nil {
				return chat.MembersPage{}, err
			}
			page.Users = append(page.Users, userID)
			if limit > 0 && len(page.Users) == limit {
				// Whether any member follows is unknown without reading on,
				// so the cursor is handed out regardless and the next page may
				// come back empty and final.
				page.Next = userID
				return page, nil
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			return page, nil
		}
		startKey = out.LastEvaluatedKey
	}
}

// GetGroupRoster is one strongly consistent Query of the group's whole base
// partition, with no sort-key condition: the #meta item comes back at the
// head ("#" sorts before "user#") and the membership rows follow. Tombstones
// are dropped here rather than by a filter, since a filter would need to
// spare the #meta item too. A partition with no #meta item is a group that
// does not exist; the membership rows cannot exist without it (see
// readRosterSummaryForWrite).
//
// A Query is consistent per item, not as a set: each item reflects the writes
// committed before that item was read, but a transition — one transaction
// over a row and #meta — can commit between the head and the rows, so the
// rows may run ahead of the summary they came with. Which way the skew runs
// is fixed by the sort order (the summary is read first, so it is never the
// newer side), and every transition leaves a mark the summary contradicts: a
// join writes a row stamped with a version above the summary's, and a leave
// tombstones a row the summary still counts. So a read whose joined rows
// number member_count and all carry a version at or below the summary's saw
// no transition, and is exactly the roster at that version; one that fails
// the check is re-read, a bounded number of times, and the last read is
// returned as it is if the roster is churning faster than that. No second
// read of the summary is needed for this, and it costs nothing when the
// roster is quiet.
func (s *store) GetGroupRoster(ctx context.Context, chatID *commonpb.ChatId) (chat.RosterSummary, []chat.GroupMember, error) {
	if !chat.IsGroupChatID(chatID) {
		return chat.RosterSummary{}, nil, fmt.Errorf("not a group chat id")
	}

	var (
		summary chat.RosterSummary
		members []chat.GroupMember
	)
	for attempt := 1; ; attempt++ {
		var err error
		summary, members, err = s.readGroupRoster(ctx, chatID)
		if err != nil {
			return chat.RosterSummary{}, nil, err
		}
		if rosterAgrees(summary, members) || attempt == maxRosterReadAttempts {
			return summary, members, nil
		}
	}
}

// maxRosterReadAttempts bounds how many times GetGroupRoster re-reads a
// roster whose rows disagree with the summary they came with (see
// GetGroupRoster). One transition landing mid-read is a race lost once; a
// roster that loses it this many times running is churning faster than a
// snapshot is worth, and the client's version merge covers the rest.
const maxRosterReadAttempts = 3

// rosterAgrees reports whether members are the roster at exactly summary's
// version: as many as it counts, none placed by a later transition.
func rosterAgrees(summary chat.RosterSummary, members []chat.GroupMember) bool {
	if uint64(len(members)) != summary.MemberCount {
		return false
	}
	for _, m := range members {
		if m.Version > summary.Version {
			return false
		}
	}
	return true
}

// readGroupRoster is one pass of GetGroupRoster's Query.
func (s *store) readGroupRoster(ctx context.Context, chatID *commonpb.ChatId) (chat.RosterSummary, []chat.GroupMember, error) {
	var (
		summary chat.RosterSummary
		found   bool
		members = make([]chat.GroupMember, 0)
	)
	var startKey map[string]types.AttributeValue
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(s.groupMembersTable),
			KeyConditionExpression:    aws.String("#pk = :pk"),
			ExpressionAttributeNames:  map[string]string{"#pk": attrPK},
			ExpressionAttributeValues: map[string]types.AttributeValue{":pk": avS(chatPK(chatID))},
			ExclusiveStartKey:         startKey,
			ConsistentRead:            aws.Bool(true),
		})
		if err != nil {
			return chat.RosterSummary{}, nil, err
		}
		for _, item := range out.Items {
			if asS(item[attrSK]) == skMeta {
				if summary, err = rosterSummaryFromItem(item); err != nil {
					return chat.RosterSummary{}, nil, err
				}
				found = true
				continue
			}
			state, err := parseN(item[attrState])
			if err != nil {
				return chat.RosterSummary{}, nil, err
			}
			if state != memberStateJoined {
				continue
			}
			member, err := groupMemberFromItem(item)
			if err != nil {
				return chat.RosterSummary{}, nil, err
			}
			members = append(members, member)
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	if !found {
		return chat.RosterSummary{}, nil, chat.ErrChatNotFound
	}
	return summary, members, nil
}

// GetGroupRosterPage is a descending range on the sparse gsiByJoinedAt: the
// index holds exactly the joined members, in join order, so the page is
// billed by what it returns and no tombstone is read or filtered. The index
// projects every attribute, so each member's version stamp comes with the
// row. A page resumes from an exclusive start key built from the position
// alone — the index key plus the base table key, all three of which a
// position names — so a cursor need not name an item still in the index. Two
// members joined in the same nanosecond are ordered within the index by the
// base table key, which is not the descending user-ID order the contract
// promises; a page boundary inside such a tie could skip or repeat one of
// the pair, which the contract accepts (see chat.RosterPosition).
func (s *store) GetGroupRosterPage(ctx context.Context, chatID *commonpb.ChatId, after *chat.RosterPosition, limit int) ([]chat.GroupMember, error) {
	if !chat.IsGroupChatID(chatID) {
		return nil, fmt.Errorf("not a group chat id")
	}

	var startKey map[string]types.AttributeValue
	if after != nil {
		startKey = map[string]types.AttributeValue{
			attrPK:       avS(chatPK(chatID)),
			attrSK:       avS(userPK(after.UserID)),
			attrJoinedAt: avN(uint64(after.JoinedAt.UnixNano())),
		}
	}

	members := make([]chat.GroupMember, 0)
	for {
		input := &dynamodb.QueryInput{
			TableName:                 aws.String(s.groupMembersTable),
			IndexName:                 aws.String(gsiByJoinedAt),
			KeyConditionExpression:    aws.String("#pk = :pk"),
			ExpressionAttributeNames:  map[string]string{"#pk": attrPK},
			ExpressionAttributeValues: map[string]types.AttributeValue{":pk": avS(chatPK(chatID))},
			ExclusiveStartKey:         startKey,
			ScanIndexForward:          aws.Bool(false),
		}
		if limit > 0 {
			input.Limit = aws.Int32(int32(limit - len(members)))
		}
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			member, err := groupMemberFromItem(item)
			if err != nil {
				return nil, err
			}
			members = append(members, member)
			if limit > 0 && len(members) == limit {
				return members, nil
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			return members, nil
		}
		startKey = out.LastEvaluatedKey
	}
}

// GetGroupMemberRecords is one strongly consistent keyed batch read of the
// user's row in each given group's partition: the same item IsMember reads,
// whole. A tombstone comes back like any item and is dropped here, as is an
// absent row.
func (s *store) GetGroupMemberRecords(ctx context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (map[string]chat.GroupMember, error) {
	seen := make(map[string]struct{}, len(chatIDs))
	var keys []map[string]types.AttributeValue
	for _, chatID := range chatIDs {
		if !chat.IsGroupChatID(chatID) {
			return nil, fmt.Errorf("not a group chat id")
		}
		if _, dup := seen[string(chatID.Value)]; dup {
			continue
		}
		seen[string(chatID.Value)] = struct{}{}
		keys = append(keys, map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(userPK(userID))})
	}

	out := make(map[string]chat.GroupMember, len(keys))
	err := s.batchGet(ctx, s.groupMembersTable, keys, "", nil, true, func(item map[string]types.AttributeValue) error {
		state, err := parseN(item[attrState])
		if err != nil {
			return err
		}
		if state != memberStateJoined {
			return nil
		}
		chatID, err := chatIDFromPK(item)
		if err != nil {
			return err
		}
		member, err := groupMemberFromItem(item)
		if err != nil {
			return err
		}
		out[string(chatID.Value)] = member
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetGroupMembersByID is one eventually consistent batch read of the users'
// rows in the group's partition, keeping the joined ones.
func (s *store) GetGroupMembersByID(ctx context.Context, chatID *commonpb.ChatId, userIDs []*commonpb.UserId) (map[string]chat.GroupMember, error) {
	if !chat.IsGroupChatID(chatID) {
		return nil, fmt.Errorf("not a group chat id")
	}

	seen := make(map[string]struct{}, len(userIDs))
	keys := make([]map[string]types.AttributeValue, 0, len(userIDs))
	for _, userID := range userIDs {
		if _, dup := seen[string(userID.Value)]; dup {
			continue
		}
		seen[string(userID.Value)] = struct{}{}
		keys = append(keys, map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(userPK(userID))})
	}

	out := make(map[string]chat.GroupMember, len(keys))
	err := s.batchGet(ctx, s.groupMembersTable, keys, "", nil, false, func(item map[string]types.AttributeValue) error {
		state, err := parseN(item[attrState])
		if err != nil {
			return err
		}
		if state != memberStateJoined {
			return nil
		}
		member, err := groupMemberFromItem(item)
		if err != nil {
			return err
		}
		out[string(member.UserID.Value)] = member
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// groupMemberFromItem decodes a joined membership row: the user from the sort
// key, the join time, and the version stamp of the transition that wrote it —
// absent on a row written at the group's creation, which reads as zero (see
// transitionMembership).
func groupMemberFromItem(item map[string]types.AttributeValue) (chat.GroupMember, error) {
	userID, err := userIDFromSK(item)
	if err != nil {
		return chat.GroupMember{}, err
	}
	joinedAt, err := parseN(item[attrJoinedAt])
	if err != nil {
		return chat.GroupMember{}, fmt.Errorf("member %s joined_at: %w", asS(item[attrSK]), err)
	}
	var version uint64
	if _, ok := item[attrVersion]; ok {
		if version, err = parseN(item[attrVersion]); err != nil {
			return chat.GroupMember{}, err
		}
	}
	return chat.GroupMember{
		UserID:   userID,
		JoinedAt: time.Unix(0, int64(joinedAt)).UTC(),
		Version:  version,
	}, nil
}

// GetGroupMembershipsForUser lists the user's membership records via the
// inverted gsiByUser. The index is keyed by the sparse user attribute, which
// tombstones keep (unlike joined_at), so departed memberships are in the user's
// slice alongside joined ones, and come back with their state.
func (s *store) GetGroupMembershipsForUser(ctx context.Context, userID *commonpb.UserId) ([]chat.GroupMembership, error) {
	return s.queryGroupMemberships(ctx, userID, nil, nil, false)
}

// queryGroupMemberships is the gsiByUser query behind
// GetGroupMembershipsForUser, optionally bounded to chat IDs in [lo, hi]. The
// index's range key is the chat key, whose hex encoding preserves the ID's
// byte order, so the bound is a key condition: rows outside it are never
// scanned, and never billed. Both bounds are nil for the user's whole slice.
//
// joinedOnly drops the tombstones with a filter expression. A filter runs
// after the scan, so it changes nothing about what is read or billed — only
// what comes back over the wire — which is why a caller after current
// memberships alone (the feed) asks for it, and a caller following the user's
// transitions does not: a tombstone's version is what tells a delayed copy of
// the join it undid from news (see chat.GroupMembership). The index projects
// every attribute, so each row's state and version stamp (see
// transitionMembership) come back with it at no extra cost. A row written at
// the group's creation carries no stamp and reads as version zero.
func (s *store) queryGroupMemberships(ctx context.Context, userID *commonpb.UserId, lo, hi *commonpb.ChatId, joinedOnly bool) ([]chat.GroupMembership, error) {
	keyCondition := "#user = :user"
	names := map[string]string{"#user": attrUser}
	values := map[string]types.AttributeValue{":user": avS(userIndexKey(userID))}
	if lo != nil && hi != nil {
		keyCondition += " AND #pk BETWEEN :lo AND :hi"
		names["#pk"] = attrPK
		values[":lo"] = avS(chatPK(lo))
		values[":hi"] = avS(chatPK(hi))
	}
	var filter *string
	if joinedOnly {
		filter = aws.String("#state = :joined")
		names["#state"] = attrState
		values[":joined"] = avN(memberStateJoined)
	}

	memberships := make([]chat.GroupMembership, 0)
	var startKey map[string]types.AttributeValue
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(s.groupMembersTable),
			IndexName:                 aws.String(gsiByUser),
			KeyConditionExpression:    aws.String(keyCondition),
			FilterExpression:          filter,
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: values,
			ExclusiveStartKey:         startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			chatID, err := chatIDFromPK(item)
			if err != nil {
				return nil, err
			}
			state, err := parseN(item[attrState])
			if err != nil {
				return nil, err
			}
			var version uint64
			if _, ok := item[attrVersion]; ok {
				if version, err = parseN(item[attrVersion]); err != nil {
					return nil, err
				}
			}
			memberships = append(memberships, chat.GroupMembership{
				ChatID:  chatID,
				Joined:  state == memberStateJoined,
				Version: version,
			})
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	return memberships, nil
}

// membershipChatIDs projects memberships onto their chat IDs.
func membershipChatIDs(memberships []chat.GroupMembership) []*commonpb.ChatId {
	chatIDs := make([]*commonpb.ChatId, len(memberships))
	for i, m := range memberships {
		chatIDs[i] = m.ChatID
	}
	return chatIDs
}

// GetGroupChatsForUser is the joined-only membership query followed by the
// canonical read of each ID: the feed wants current memberships alone, so the
// tombstones are dropped in the store rather than carried back to be skipped.
func (s *store) GetGroupChatsForUser(ctx context.Context, userID *commonpb.UserId) ([]*chat.Chat, error) {
	memberships, err := s.queryGroupMemberships(ctx, userID, nil, nil, true)
	if err != nil {
		return nil, err
	}
	return s.batchGetChats(ctx, membershipChatIDs(memberships))
}

// GetGroupChatsForUserByIDs checks membership by querying the user's slice of
// the inverted membership index — the same read GetGroupMembershipsForUser makes —
// rather than by a keyed read of each given ID's membership record. A query is
// billed on the bytes it scans, and membership rows are small, so the user's
// whole membership costs a fraction of what one keyed read per ID would: a
// batch get charges every item the full minimum, however small. The query
// only loses once a user is in thousands of groups, and it is one request
// either way.
//
// The query is bounded to the range of chat IDs asked about, so only the
// user's memberships that could match are scanned. Group IDs are random, so a
// page of many IDs spans most of the key space and the bound prunes little;
// for a few IDs — a refill after a dropped one, say — it prunes nearly
// everything. It never costs more than the unbounded query.
//
// The membership read comes first, and only the given IDs it confirms are
// then read from the chats table — so nothing is fetched for an ID the user
// is not (or no longer) in, which is what a stale or tampered feed token asks
// about.
func (s *store) GetGroupChatsForUserByIDs(ctx context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) ([]*chat.Chat, error) {
	for _, chatID := range chatIDs {
		if !chat.IsGroupChatID(chatID) {
			return nil, fmt.Errorf("not a group chat id")
		}
	}
	if len(chatIDs) == 0 {
		return []*chat.Chat{}, nil
	}

	lo, hi := chatIDs[0], chatIDs[0]
	for _, chatID := range chatIDs[1:] {
		if bytes.Compare(chatID.Value, lo.Value) < 0 {
			lo = chatID
		}
		if bytes.Compare(chatID.Value, hi.Value) > 0 {
			hi = chatID
		}
	}
	memberships, err := s.queryGroupMemberships(ctx, userID, lo, hi, true)
	if err != nil {
		return nil, err
	}
	joined := make(map[string]struct{}, len(memberships))
	for _, m := range memberships {
		joined[string(m.ChatID.Value)] = struct{}{}
	}

	wanted := make([]*commonpb.ChatId, 0, len(chatIDs))
	for _, chatID := range chatIDs {
		if _, ok := joined[string(chatID.Value)]; ok {
			wanted = append(wanted, chatID)
		}
	}
	return s.batchGetChats(ctx, wanted)
}

func (s *store) GetGroupChatsByID(ctx context.Context, chatIDs []*commonpb.ChatId) (map[string]*chat.Chat, error) {
	for _, chatID := range chatIDs {
		if !chat.IsGroupChatID(chatID) {
			return nil, fmt.Errorf("not a group chat id")
		}
	}

	seen := make(map[string]struct{}, len(chatIDs))
	var keys []map[string]types.AttributeValue
	for _, chatID := range chatIDs {
		if _, dup := seen[string(chatID.Value)]; dup {
			continue
		}
		seen[string(chatID.Value)] = struct{}{}
		keys = append(keys, map[string]types.AttributeValue{attrPK: avS(chatPK(chatID))})
	}

	chats := make(map[string]*chat.Chat, len(keys))
	err := s.batchGet(ctx, s.chatsTable, keys, "", nil, false, func(item map[string]types.AttributeValue) error {
		chatID, err := chatIDFromPK(item)
		if err != nil {
			return err
		}
		c, err := chatFromItem(chatID, item)
		if err != nil {
			return err
		}
		chats[string(chatID.Value)] = c
		return nil
	})
	if err != nil {
		return nil, err
	}
	return chats, nil
}

// maxBatchGetKeys is DynamoDB's per-request BatchGetItem key limit.
const maxBatchGetKeys = 100

// batchGetChats reads the canonical item of each given chat (duplicates
// collapsed), omitting chats that do not exist, in no particular order.
func (s *store) batchGetChats(ctx context.Context, chatIDs []*commonpb.ChatId) ([]*chat.Chat, error) {
	seen := make(map[string]struct{}, len(chatIDs))
	var keys []map[string]types.AttributeValue
	for _, chatID := range chatIDs {
		if _, dup := seen[string(chatID.Value)]; dup {
			continue
		}
		seen[string(chatID.Value)] = struct{}{}
		keys = append(keys, map[string]types.AttributeValue{attrPK: avS(chatPK(chatID))})
	}

	chats := make([]*chat.Chat, 0, len(keys))
	err := s.batchGet(ctx, s.chatsTable, keys, "", nil, false, func(item map[string]types.AttributeValue) error {
		chatID, err := chatIDFromPK(item)
		if err != nil {
			return err
		}
		c, err := chatFromItem(chatID, item)
		if err != nil {
			return err
		}
		chats = append(chats, c)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return chats, nil
}

// batchGet reads the given keys from table in BatchGetItem chunks, retrying
// UnprocessedKeys until each chunk drains, and calls fn on every item found.
// projection and names are an optional ProjectionExpression and its attribute
// name aliases; an empty projection reads whole items. consistent asks for a
// strongly consistent read, at twice the cost of the default. Items come back
// in no particular order.
func (s *store) batchGet(
	ctx context.Context,
	table string,
	keys []map[string]types.AttributeValue,
	projection string,
	names map[string]string,
	consistent bool,
	fn func(item map[string]types.AttributeValue) error,
) error {
	for start := 0; start < len(keys); start += maxBatchGetKeys {
		end := min(start+maxBatchGetKeys, len(keys))

		attrs := types.KeysAndAttributes{Keys: keys[start:end], ConsistentRead: aws.Bool(consistent)}
		if projection != "" {
			attrs.ProjectionExpression = aws.String(projection)
			attrs.ExpressionAttributeNames = names
		}
		req := map[string]types.KeysAndAttributes{table: attrs}
		for len(req[table].Keys) > 0 {
			resp, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
			if err != nil {
				return err
			}
			for _, item := range resp.Responses[table] {
				if err := fn(item); err != nil {
					return err
				}
			}
			if unprocessed, ok := resp.UnprocessedKeys[table]; ok && len(unprocessed.Keys) > 0 {
				req = map[string]types.KeysAndAttributes{table: unprocessed}
			} else {
				break
			}
		}
	}
	return nil
}

// GetGroupRosterSummary is a point read of the group's #meta item. Every group
// has one (see readRosterSummaryForWrite), so a missing item is a chat that
// does not exist.
func (s *store) GetGroupRosterSummary(ctx context.Context, chatID *commonpb.ChatId) (chat.RosterSummary, error) {
	if !chat.IsGroupChatID(chatID) {
		return chat.RosterSummary{}, fmt.Errorf("not a group chat id")
	}

	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.groupMembersTable),
		Key:       map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(skMeta)},
	})
	if err != nil {
		return chat.RosterSummary{}, err
	}
	if len(out.Item) == 0 {
		return chat.RosterSummary{}, chat.ErrChatNotFound
	}
	return rosterSummaryFromItem(out.Item)
}

// GetGroupRosterSummaries is one batched keyed read of the given groups' #meta
// items. A group whose item is absent — one that predates the item, or one
// that does not exist — is simply absent from the result; unlike the single
// read, no canonical item is consulted to tell the two apart.
func (s *store) GetGroupRosterSummaries(ctx context.Context, chatIDs []*commonpb.ChatId) (map[string]chat.RosterSummary, error) {
	seen := make(map[string]struct{}, len(chatIDs))
	var keys []map[string]types.AttributeValue
	for _, chatID := range chatIDs {
		if !chat.IsGroupChatID(chatID) {
			return nil, fmt.Errorf("not a group chat id")
		}
		if _, dup := seen[string(chatID.Value)]; dup {
			continue
		}
		seen[string(chatID.Value)] = struct{}{}
		keys = append(keys, map[string]types.AttributeValue{attrPK: avS(chatPK(chatID)), attrSK: avS(skMeta)})
	}

	out := make(map[string]chat.RosterSummary, len(keys))
	err := s.batchGet(ctx, s.groupMembersTable, keys, "", nil, false, func(item map[string]types.AttributeValue) error {
		chatID, err := chatIDFromPK(item)
		if err != nil {
			return err
		}
		summary, err := rosterSummaryFromItem(item)
		if err != nil {
			return err
		}
		out[string(chatID.Value)] = summary
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *store) GetGroupRules(ctx context.Context, chatID *commonpb.ChatId) (chat.ChatRules, error) {
	if !chat.IsGroupChatID(chatID) {
		return chat.ChatRules{}, fmt.Errorf("not a group chat id")
	}

	// Only the attributes the rules are projected from: the type, the
	// attributes that stand for a rule, the creator a CreatorRequirement
	// names, and whether the group is private. The rest of the record — title,
	// picture, activity — is neither fetched nor deserialized.
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.chatsTable),
		Key:                  map[string]types.AttributeValue{attrPK: avS(chatPK(chatID))},
		ProjectionExpression: aws.String("#type, #staff, #balance, #creatorOnly, #speakerBalance, #creator, #private"),
		ExpressionAttributeNames: map[string]string{
			"#type":           attrType,
			"#staff":          attrIsStaffOnly,
			"#balance":        attrMinListenerBalance,
			"#creatorOnly":    attrIsCreatorOnlySpeaker,
			"#speakerBalance": attrMinSpeakerBalance,
			"#creator":        attrCreator,
			"#private":        attrIsPrivate,
		},
	})
	if err != nil {
		return chat.ChatRules{}, err
	}
	if len(out.Item) == 0 {
		return chat.ChatRules{}, chat.ErrChatNotFound
	}
	typeVal, err := parseN(out.Item[attrType])
	if err != nil {
		return chat.ChatRules{}, err
	}
	listenerBalance, err := minimumBalanceFromItem(out.Item, attrMinListenerBalance)
	if err != nil {
		return chat.ChatRules{}, err
	}
	speakerBalance, err := minimumBalanceFromItem(out.Item, attrMinSpeakerBalance)
	if err != nil {
		return chat.ChatRules{}, err
	}
	c := &chat.Chat{
		Type:                   protoChatType(uint64(typeVal)),
		IsStaffOnly:            asBool(out.Item[attrIsStaffOnly]),
		MinimumListenerBalance: listenerBalance,
		IsCreatorOnlySpeaker:   asBool(out.Item[attrIsCreatorOnlySpeaker]),
		MinimumSpeakerBalance:  speakerBalance,
		IsPrivate:              asBool(out.Item[attrIsPrivate]),
	}
	// creator is absent for groups written before it was recorded.
	if creator := asB(out.Item[attrCreator]); len(creator) > 0 {
		c.CreatorID = &commonpb.UserId{Value: append([]byte(nil), creator...)}
	}
	return c.ChatRules(), nil
}

// rosterSummaryFromItem reads a #meta item. version is absent on an item
// backfilled by hand without one, and reads as zero — the value creation seeds.
func rosterSummaryFromItem(item map[string]types.AttributeValue) (chat.RosterSummary, error) {
	count, err := parseN(item[attrMemberCount])
	if err != nil {
		return chat.RosterSummary{}, err
	}
	var version uint64
	if _, ok := item[attrVersion]; ok {
		if version, err = parseN(item[attrVersion]); err != nil {
			return chat.RosterSummary{}, err
		}
	}
	return chat.RosterSummary{MemberCount: count, Version: version}, nil
}

// IsMember is a strongly consistent point read in both layouts: the group's
// membership row, or the user's DM inbox row. It is the membership authority
// behind every access gate and is asked about a chat the user just joined or
// created more often than any other — the join's response is the cue to open
// the chat — so it must see that write, not a replica that has yet to.
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
			ConsistentRead:           aws.Bool(true),
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
		ConsistentRead:       aws.Bool(true),
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
	excluded := asBS(out.Item[attrExcludedFromFeed])
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
		// An excluded member's row has no activity to move, and its condition
		// could never hold: it would cancel every advance of the chat.
		if slices.ContainsFunc(excluded, func(u []byte) bool { return bytes.Equal(u, member.Value) }) {
			continue
		}
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
	// an inline list could not hold a large group. Title, the staff-only flag,
	// the minimum listener balance, the creator-only speaker flag, the minimum
	// speaker balance, the private flag, the creator and the picture are
	// group-only;
	// each is written only when set, so an absent attribute (including on every
	// item written before it existed) reads as its zero value.
	if c.Type == chatpb.ChatType_GROUP {
		if c.Title != "" {
			item[attrTitle] = avS(c.Title)
		}
		if c.IsStaffOnly {
			item[attrIsStaffOnly] = avBool(true)
		}
		if c.MinimumListenerBalance != nil {
			item[attrMinListenerBalance] = minimumBalanceAttr(c.MinimumListenerBalance)
		}
		if c.IsCreatorOnlySpeaker {
			item[attrIsCreatorOnlySpeaker] = avBool(true)
		}
		if c.MinimumSpeakerBalance != nil {
			item[attrMinSpeakerBalance] = minimumBalanceAttr(c.MinimumSpeakerBalance)
		}
		if c.IsPrivate {
			item[attrIsPrivate] = avBool(true)
		}
		if c.CreatorID != nil {
			item[attrCreator] = avB(c.CreatorID.Value)
		}
		if c.Description != "" {
			item[attrDescription] = avS(c.Description)
		}
		if c.ProfilePictureBlobID != nil {
			item[attrProfilePictureBlobID] = avB(c.ProfilePictureBlobID.Value)
		}
		if c.CoverPictureBlobID != nil {
			item[attrCoverPictureBlobID] = avB(c.CoverPictureBlobID.Value)
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

// groupMetaItem is a group's aggregates item as written at creation: the
// initial joined member count and a roster version of zero. It carries neither
// user nor joined_at, so it stays out of both sparse GSIs.
func (s *store) groupMetaItem(chatID *commonpb.ChatId, memberCount uint64) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK:          avS(chatPK(chatID)),
		attrSK:          avS(skMeta),
		attrMemberCount: avN(memberCount),
		attrVersion:     avN(0),
	}
}

// dmInboxItem is member's dm_inbox row for the DM c. The row of a member the
// DM excludes from the feed (excluded) records membership alone: without feed
// and last_activity, the keys of both GSIs, it stays out of them, so creating
// it writes the base table only, where the sort key is a chat ID and a busy
// member's rows spread across the partition rather than all landing at the
// newest end of one index key.
func (s *store) dmInboxItem(c *chat.Chat, member *commonpb.UserId, excluded bool) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		attrPK:      avS(userPK(member)),
		attrSK:      avS(chatSK(c.ID)),
		attrType:    avN(uint64(c.Type)),
		attrMembers: membersAttr(c.Members),
	}
	if excluded {
		return item
	}
	item[attrFeed] = avS(feedPK(member, c.Type))
	item[attrLastActivity] = avN(uint64(c.LastActivity.UnixNano()))
	if c.LastMessageID != nil {
		item[attrLastMessageID] = avN(c.LastMessageID.Value)
	}
	return item
}

// chatFromItem builds a Chat from a chats or dm_inbox item. The chat ID is not
// stored on the item; it is recovered from the item's key by the caller and
// passed in. RosterSummary is set from the inline member list, which is
// complete for a DM and empty for a group — a group's summary is its own read,
// see GetGroupRosterSummary.
func chatFromItem(chatID *commonpb.ChatId, item map[string]types.AttributeValue) (*chat.Chat, error) {
	typeVal, err := parseN(item[attrType])
	if err != nil {
		return nil, err
	}
	nanos, err := parseInt(item[attrLastActivity])
	if err != nil {
		return nil, err
	}
	// min_listener_balance and min_speaker_balance are absent for DMs and for
	// groups without one.
	listenerBalance, err := minimumBalanceFromItem(item, attrMinListenerBalance)
	if err != nil {
		return nil, err
	}
	speakerBalance, err := minimumBalanceFromItem(item, attrMinSpeakerBalance)
	if err != nil {
		return nil, err
	}
	members := membersFromItem(item)
	c := &chat.Chat{
		ID:                     &commonpb.ChatId{Value: append([]byte(nil), chatID.Value...)},
		Type:                   protoChatType(uint64(typeVal)),
		Members:                members,
		RosterSummary:          chat.RosterSummary{MemberCount: uint64(len(members))},
		Title:                  asS(item[attrTitle]),
		Description:            asS(item[attrDescription]),
		IsStaffOnly:            asBool(item[attrIsStaffOnly]),
		MinimumListenerBalance: listenerBalance,
		IsCreatorOnlySpeaker:   asBool(item[attrIsCreatorOnlySpeaker]),
		MinimumSpeakerBalance:  speakerBalance,
		IsPrivate:              asBool(item[attrIsPrivate]),
		LastActivity:           time.Unix(0, nanos).UTC(),
	}
	// creator is absent for DMs and for groups written before it was recorded.
	if creator := asB(item[attrCreator]); len(creator) > 0 {
		c.CreatorID = &commonpb.UserId{Value: append([]byte(nil), creator...)}
	}
	// picture and cover_picture are absent for DMs and for groups without one.
	if picture := asB(item[attrProfilePictureBlobID]); len(picture) > 0 {
		c.ProfilePictureBlobID = &blobpb.BlobId{Value: append([]byte(nil), picture...)}
	}
	if cover := asB(item[attrCoverPictureBlobID]); len(cover) > 0 {
		c.CoverPictureBlobID = &blobpb.BlobId{Value: append([]byte(nil), cover...)}
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

// minimumBalanceAttr encodes a group's minimum listener or speaker balance as
// one map attribute, so the requirement is present or absent as a whole: the currency
// code, the native amount (a decimal number, written at full float precision),
// and the mint list — omitted when empty, the encoding of "any mint".
func minimumBalanceAttr(b *chat.MinimumBalance) types.AttributeValue {
	m := map[string]types.AttributeValue{
		attrBalanceCurrency:     avS(b.Currency),
		attrBalanceNativeAmount: avF(b.NativeAmount),
	}
	if len(b.Mints) > 0 {
		mints := make([]types.AttributeValue, len(b.Mints))
		for i, mint := range b.Mints {
			mints[i] = avB(mint.Value)
		}
		m[attrBalanceMints] = &types.AttributeValueMemberL{Value: mints}
	}
	return &types.AttributeValueMemberM{Value: m}
}

// minimumBalanceFromItem is the inverse of minimumBalanceAttr for the
// requirement stored under attr: nil, without error, when the item carries
// none.
func minimumBalanceFromItem(item map[string]types.AttributeValue, attr string) (*chat.MinimumBalance, error) {
	m, ok := item[attr].(*types.AttributeValueMemberM)
	if !ok {
		return nil, nil
	}
	amount, err := parseF(m.Value[attrBalanceNativeAmount])
	if err != nil {
		return nil, fmt.Errorf("parsing %s.%s: %w", attr, attrBalanceNativeAmount, err)
	}
	b := &chat.MinimumBalance{
		Currency:     asS(m.Value[attrBalanceCurrency]),
		NativeAmount: amount,
	}
	for _, av := range asL(m.Value[attrBalanceMints]) {
		b.Mints = append(b.Mints, &commonpb.PublicKey{Value: append([]byte(nil), asB(av)...)})
	}
	return b, nil
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
// the inverse of chatSK; chatIDFromPK does the same for a group_members item's
// pk, the inverse of chatPK.
func chatIDFromSK(item map[string]types.AttributeValue) (*commonpb.ChatId, error) {
	return chatIDFromKey(asS(item[attrSK]))
}

func chatIDFromPK(item map[string]types.AttributeValue) (*commonpb.ChatId, error) {
	return chatIDFromKey(asS(item[attrPK]))
}

func chatIDFromKey(key string) (*commonpb.ChatId, error) {
	encoded, ok := strings.CutPrefix(key, chatKeyPrefix)
	if !ok {
		return nil, fmt.Errorf("unexpected chat key %q", key)
	}
	id, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding chat id from key %q: %w", key, err)
	}
	return &commonpb.ChatId{Value: id}, nil
}

// userIDFromSK recovers a user ID from a group_members item's sk
// ("user#<hex>"), the inverse of userPK; userIDFromPK does the same for a
// chat_user_state item's pk.
func userIDFromSK(item map[string]types.AttributeValue) (*commonpb.UserId, error) {
	return userIDFromKey(asS(item[attrSK]))
}

func userIDFromPK(item map[string]types.AttributeValue) (*commonpb.UserId, error) {
	return userIDFromKey(asS(item[attrPK]))
}

func userIDFromKey(key string) (*commonpb.UserId, error) {
	encoded, ok := strings.CutPrefix(key, userKeyPrefix)
	if !ok {
		return nil, fmt.Errorf("unexpected user key %q", key)
	}
	id, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding user id from key %q: %w", key, err)
	}
	return &commonpb.UserId{Value: id}, nil
}

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avB(v []byte) types.AttributeValue {
	return &types.AttributeValueMemberB{Value: append([]byte(nil), v...)}
}

// avBS encodes user IDs as a binary set. userIDs must be non-empty and
// distinct: DynamoDB rejects an empty set and one naming a value twice.
func avBS(userIDs []*commonpb.UserId) types.AttributeValue {
	values := make([][]byte, len(userIDs))
	for i, userID := range userIDs {
		values[i] = append([]byte(nil), userID.Value...)
	}
	return &types.AttributeValueMemberBS{Value: values}
}
func avN(v uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
}
func avInt(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}
func avBool(v bool) types.AttributeValue { return &types.AttributeValueMemberBOOL{Value: v} }

// avF encodes a float as a DynamoDB number: the shortest plain-decimal string
// that round-trips the value exactly (no exponent, no padding).
func avF(v float64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatFloat(v, 'f', -1, 64)}
}

func asS(av types.AttributeValue) string {
	if s, ok := av.(*types.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

// asBool returns the attribute's value, or false when it is absent or not a
// BOOL.
func asBool(av types.AttributeValue) bool {
	if b, ok := av.(*types.AttributeValueMemberBOOL); ok {
		return b.Value
	}
	return false
}

func asB(av types.AttributeValue) []byte {
	if b, ok := av.(*types.AttributeValueMemberB); ok {
		return b.Value
	}
	return nil
}

func asBS(av types.AttributeValue) [][]byte {
	if bs, ok := av.(*types.AttributeValueMemberBS); ok {
		return bs.Value
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

func parseF(av types.AttributeValue) (float64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseFloat(n.Value, 64)
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
	reasons, ok := cancellationReasons(err)
	return ok && len(reasons) > 0 && aws.ToString(reasons[0].Code) == conditionalCheckFailedCode
}

// cancellationReasons returns a cancelled transaction's per-item reasons, in
// the order of the request's items (code "None" for items that were fine; Item
// set on a failed condition when that item asked for it), and false when err
// is not a transaction cancellation.
func cancellationReasons(err error) ([]types.CancellationReason, bool) {
	var tce *types.TransactionCanceledException
	if !errors.As(err, &tce) {
		return nil, false
	}
	return tce.CancellationReasons, true
}

// isTransactionConflict reports whether any item of a cancelled transaction
// lost to a concurrent write on the same item.
func isTransactionConflict(codes []string) bool {
	return slices.Contains(codes, codeTransactionConflict)
}

func isConditionalCheckFailed(err error) bool {
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}

func protoChatType(v uint64) chatpb.ChatType {
	return chatpb.ChatType(v)
}

// muteForeverUntil encodes an indefinite mute in muted_until: the epoch
// seconds of chat.MaxMuteUntil, which a timed mute may not reach (see
// chat.Mute), so the two never collide — and above any "now" the fan-out
// will ever ask about, so the gsiByMuted range needs no second case. It is a
// storage encoding only: nothing outside this store sees the value.
var muteForeverUntil = uint64(chat.MaxMuteUntil.Unix())

// A mute first recorded or cleared is a two-item transaction — the user's
// record and the chat's #meta muted count — on the membership-transition
// pattern (see maxMembershipAttempts): a lost condition retries at once from
// the item the failure returned, and a TransactionConflict backs off. A mute
// replaced is a single conditional update of the record alone, since the
// count does not move.
const maxUserStateAttempts = maxMembershipAttempts

func (s *store) SetMute(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, mute chat.Mute) (chat.ViewerState, bool, error) {
	mute, err := mute.Normalize()
	if err != nil {
		return chat.ViewerState{}, false, err
	}
	until := muteForeverUntil
	if !mute.Forever {
		until = uint64(mute.Until.Unix())
	}

	// current is the record as the last failed write returned it, nil until
	// one has: every failure hands the item back, so the loop never reads,
	// and each attempt starts from the case the record is known to be in.
	var current *chat.ViewerState
	backoff := membershipBackoffBase
	for attempt := 0; attempt < maxUserStateAttempts; attempt++ {
		if current == nil || current.Mute != nil {
			// Replace: a recorded mute that differs is one write of the record
			// alone. It is also the probe when nothing is known yet — the
			// failure returns the item, which decides between the other two
			// cases without a read: the mute already recorded is the no-op,
			// and none recorded is a first mute, counted below.
			out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName:                aws.String(s.userStateTable),
				Key:                      userStateKey(chatID, userID),
				UpdateExpression:         aws.String(fmt.Sprintf("SET %s = :until ADD #version :one", attrMutedUntil)),
				ConditionExpression:      aws.String(fmt.Sprintf("attribute_exists(%s) AND %s <> :until", attrMutedUntil, attrMutedUntil)),
				ExpressionAttributeNames: map[string]string{"#version": attrVersion},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":until": avN(until),
					":one":   avN(1),
				},
				ReturnValues:                        types.ReturnValueAllNew,
				ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
			})
			if err == nil {
				state, err := viewerStateFromItem(out.Attributes)
				return state, true, err
			}
			var ccf *types.ConditionalCheckFailedException
			if !errors.As(err, &ccf) {
				return chat.ViewerState{}, false, err
			}
			state, err := viewerStateFromItem(ccf.Item)
			if err != nil {
				return chat.ViewerState{}, false, err
			}
			current = &state
			if current.Mute != nil {
				return *current, false, nil
			}
		}

		// A first mute: record it and count it together. The record's version
		// is compared so the state returned is exactly what was written; a
		// record that appeared or moved meanwhile fails the condition and
		// returns the item, and the next attempt starts from the case it is
		// in — a mute now recorded goes back to the replace above, a record
		// that moved and holds none comes straight back here at its new
		// version — rather than probing again.
		record := &types.Update{
			TableName:                aws.String(s.userStateTable),
			Key:                      userStateKey(chatID, userID),
			UpdateExpression:         aws.String(fmt.Sprintf("SET %s = :chat, %s = :until ADD #version :one", attrChat, attrMutedUntil)),
			ConditionExpression:      aws.String(fmt.Sprintf("attribute_not_exists(%s) AND (attribute_not_exists(#version) OR #version = :version)", attrMutedUntil)),
			ExpressionAttributeNames: map[string]string{"#version": attrVersion},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":chat":    avB(chatID.Value),
				":until":   avN(until),
				":one":     avN(1),
				":version": avN(current.Version),
			},
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}
		_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
			{Update: record},
			{Update: s.mutedCountUpdate(chatID, 1)},
		}})
		if err == nil {
			return chat.ViewerState{Mute: &mute, Version: current.Version + 1}, true, nil
		}
		reasons, ok := cancellationReasons(err)
		if !ok || len(reasons) != 2 {
			return chat.ViewerState{}, false, err
		}
		codes := []string{aws.ToString(reasons[0].Code), aws.ToString(reasons[1].Code)}
		switch {
		case codes[0] == conditionalCheckFailedCode:
			// Lost the race to the user's own other write: go again at once
			// from the item the failure returned.
			state, err := viewerStateFromItem(reasons[0].Item)
			if err != nil {
				return chat.ViewerState{}, false, err
			}
			current = &state
			continue
		case isTransactionConflict(codes):
			// The record was not evaluated, so what is known of it still
			// holds; the same attempt goes again after a pause.
			select {
			case <-ctx.Done():
				return chat.ViewerState{}, false, ctx.Err()
			case <-time.After(backoff + rand.N(backoff)):
			}
			backoff = min(2*backoff, membershipBackoffMax)
			continue
		default:
			return chat.ViewerState{}, false, err
		}
	}
	return chat.ViewerState{}, false, fmt.Errorf("recording mute for user %x on chat %x: attempts exhausted", userID.Value, chatID.Value)
}

func (s *store) ClearMute(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (chat.ViewerState, bool, error) {
	backoff := membershipBackoffBase
	for attempt := 0; attempt < maxUserStateAttempts; attempt++ {
		// The record's version is compared so the state returned is exactly
		// what was written, with no read after; the condition on muted_until
		// is what makes no mute recorded the no-op, and keeps that no-op from
		// upserting an item.
		item, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(s.userStateTable),
			Key:            userStateKey(chatID, userID),
			ConsistentRead: aws.Bool(true),
		})
		if err != nil {
			return chat.ViewerState{}, false, err
		}
		current, err := viewerStateFromItem(item.Item)
		if err != nil {
			return chat.ViewerState{}, false, err
		}
		if current.Mute == nil {
			return current, false, nil
		}

		record := &types.Update{
			TableName:                aws.String(s.userStateTable),
			Key:                      userStateKey(chatID, userID),
			UpdateExpression:         aws.String(fmt.Sprintf("REMOVE %s ADD #version :one", attrMutedUntil)),
			ConditionExpression:      aws.String(fmt.Sprintf("attribute_exists(%s) AND #version = :version", attrMutedUntil)),
			ExpressionAttributeNames: map[string]string{"#version": attrVersion},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":one":     avN(1),
				":version": avN(current.Version),
			},
		}
		_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
			{Update: record},
			{Update: s.mutedCountUpdate(chatID, -1)},
		}})
		if err == nil {
			return chat.ViewerState{Version: current.Version + 1}, true, nil
		}
		reasons, ok := cancellationReasons(err)
		if !ok || len(reasons) != 2 {
			return chat.ViewerState{}, false, err
		}
		codes := []string{aws.ToString(reasons[0].Code), aws.ToString(reasons[1].Code)}
		switch {
		case codes[0] == conditionalCheckFailedCode:
			continue
		case isTransactionConflict(codes):
			select {
			case <-ctx.Done():
				return chat.ViewerState{}, false, ctx.Err()
			case <-time.After(backoff + rand.N(backoff)):
			}
			backoff = min(2*backoff, membershipBackoffMax)
			continue
		default:
			return chat.ViewerState{}, false, err
		}
	}
	return chat.ViewerState{}, false, fmt.Errorf("clearing mute for user %x on chat %x: attempts exhausted", userID.Value, chatID.Value)
}

// mutedCountUpdate moves a chat's #meta muted count by delta, creating the
// item on first use. The item carries neither chat nor muted_until, so it
// stays out of both indexes (see gsiUserStateByUser).
func (s *store) mutedCountUpdate(chatID *commonpb.ChatId, delta int64) *types.Update {
	return &types.Update{
		TableName: aws.String(s.userStateTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(skMeta),
		},
		UpdateExpression:          aws.String(fmt.Sprintf("ADD %s :delta", attrMutedCount)),
		ExpressionAttributeValues: map[string]types.AttributeValue{":delta": avInt(delta)},
	}
}

// GetMutedCount reads the chat's #meta muted count eventually consistent, at
// half the cost of a strong read: it feeds a choice of read shape, where a
// count one transition behind picks the same shape as the current one would
// in every case but a tie, and either shape is correct.
func (s *store) GetMutedCount(ctx context.Context, chatID *commonpb.ChatId) (uint64, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.userStateTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(skMeta),
		},
		ProjectionExpression: aws.String(attrMutedCount),
	})
	if err != nil {
		return 0, err
	}
	av, ok := out.Item[attrMutedCount]
	if !ok {
		return 0, nil
	}
	count, err := parseInt(av)
	if err != nil {
		return 0, fmt.Errorf("parsing %s: %w", attrMutedCount, err)
	}
	// The count is moved only alongside the record it counts, so it cannot
	// go negative; clamp anyway rather than hand a caller a wrapped value.
	return uint64(max(count, 0)), nil
}

// GetMutedUsersPage ranges gsiUserStateByUser over [lo, hi] in the index's
// user order, filtering to the records whose mute is active at now. The
// index's range key is the record's pk ("user#<hex>"), whose hex encoding
// preserves the ID's byte order, so the bound is a key condition: records
// outside it are never scanned, and never billed. The filter runs after the
// read, so the range is billed for every record in it, muted or not — the
// shape the contract describes.
func (s *store) GetMutedUsersPage(ctx context.Context, chatID *commonpb.ChatId, now time.Time, lo, hi *commonpb.UserId) ([]*commonpb.UserId, error) {
	nowSecs := max(now.Unix(), 0)

	condition := "#chat = :chat"
	names := map[string]string{"#chat": attrChat, "#until": attrMutedUntil}
	values := map[string]types.AttributeValue{
		":chat": avB(chatID.Value),
		":now":  avN(uint64(nowSecs)),
	}
	switch {
	case lo != nil && hi != nil:
		condition += " AND #pk BETWEEN :lo AND :hi"
		names["#pk"] = attrPK
		values[":lo"] = avS(userPK(lo))
		values[":hi"] = avS(userPK(hi))
	case lo != nil:
		condition += " AND #pk >= :lo"
		names["#pk"] = attrPK
		values[":lo"] = avS(userPK(lo))
	case hi != nil:
		condition += " AND #pk <= :hi"
		names["#pk"] = attrPK
		values[":hi"] = avS(userPK(hi))
	}

	users := make([]*commonpb.UserId, 0)
	var startKey map[string]types.AttributeValue
	for {
		res, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(s.userStateTable),
			IndexName:                 aws.String(gsiUserStateByUser),
			KeyConditionExpression:    aws.String(condition),
			FilterExpression:          aws.String("#until > :now"),
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: values,
			ExclusiveStartKey:         startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range res.Items {
			userID, err := userIDFromPK(item)
			if err != nil {
				return nil, err
			}
			users = append(users, userID)
		}
		if len(res.LastEvaluatedKey) == 0 {
			return users, nil
		}
		startKey = res.LastEvaluatedKey
	}
}

// GetViewerStates reads the user's partition, strongly consistent, bounded to
// the sort-key range the requested chats span: every requested sk lies
// between the least and the greatest of them, so the query cannot miss one,
// and it skips whatever the user has set on chats outside that span. Rows
// inside the span that were not asked for are dropped in memory. A single
// chat is a point read instead, which is cheaper than a query of one.
func (s *store) GetViewerStates(ctx context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (map[string]chat.ViewerState, error) {
	wanted := make(map[string]struct{}, len(chatIDs))
	var lo, hi string
	for _, chatID := range chatIDs {
		sk := chatSK(chatID)
		wanted[sk] = struct{}{}
		if lo == "" || sk < lo {
			lo = sk
		}
		if sk > hi {
			hi = sk
		}
	}
	out := make(map[string]chat.ViewerState, len(wanted))
	if len(wanted) == 0 {
		return out, nil
	}

	if len(wanted) == 1 {
		res, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(s.userStateTable),
			Key:            userStateKey(chatIDs[0], userID),
			ConsistentRead: aws.Bool(true),
		})
		if err != nil {
			return nil, err
		}
		if len(res.Item) == 0 {
			return out, nil
		}
		state, err := viewerStateFromItem(res.Item)
		if err != nil {
			return nil, err
		}
		out[string(chatIDs[0].Value)] = state
		return out, nil
	}

	var startKey map[string]types.AttributeValue
	for {
		res, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                aws.String(s.userStateTable),
			KeyConditionExpression:   aws.String("#pk = :pk AND #sk BETWEEN :lo AND :hi"),
			ExpressionAttributeNames: map[string]string{"#pk": attrPK, "#sk": attrSK},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": avS(userPK(userID)),
				":lo": avS(lo),
				":hi": avS(hi),
			},
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range res.Items {
			if _, ok := wanted[asS(item[attrSK])]; !ok {
				continue
			}
			chatID, err := chatIDFromSK(item)
			if err != nil {
				return nil, err
			}
			state, err := viewerStateFromItem(item)
			if err != nil {
				return nil, err
			}
			out[string(chatID.Value)] = state
		}
		if len(res.LastEvaluatedKey) == 0 {
			return out, nil
		}
		startKey = res.LastEvaluatedKey
	}
}

// GetMutedUsers queries gsiByMuted with a key range above now, so it reads
// only the active mutes and never touches a cleared or lapsed one. The index
// is eventually consistent, as the contract allows.
func (s *store) GetMutedUsers(ctx context.Context, chatID *commonpb.ChatId, now time.Time, limit int) ([]*commonpb.UserId, error) {
	nowSecs := now.Unix()
	if nowSecs < 0 {
		nowSecs = 0
	}

	users := make([]*commonpb.UserId, 0)
	var startKey map[string]types.AttributeValue
	for {
		input := &dynamodb.QueryInput{
			TableName:                aws.String(s.userStateTable),
			IndexName:                aws.String(gsiByMuted),
			KeyConditionExpression:   aws.String("#chat = :chat AND #until > :now"),
			ExpressionAttributeNames: map[string]string{"#chat": attrChat, "#until": attrMutedUntil},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":chat": avB(chatID.Value),
				":now":  avN(uint64(nowSecs)),
			},
			ExclusiveStartKey: startKey,
		}
		if limit > 0 {
			input.Limit = aws.Int32(int32(limit - len(users)))
		}
		res, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range res.Items {
			userID, err := userIDFromPK(item)
			if err != nil {
				return nil, err
			}
			users = append(users, userID)
		}
		if limit > 0 && len(users) >= limit {
			return users[:limit], nil
		}
		if len(res.LastEvaluatedKey) == 0 {
			return users, nil
		}
		startKey = res.LastEvaluatedKey
	}
}

func userStateKey(chatID *commonpb.ChatId, userID *commonpb.UserId) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK: avS(userPK(userID)),
		attrSK: avS(chatSK(chatID)),
	}
}

// viewerStateFromItem decodes a chat_user_state item. A missing version reads
// as zero and a missing muted_until as no mute; muted_until at or above
// muteForeverUntil is the indefinite mute.
func viewerStateFromItem(item map[string]types.AttributeValue) (chat.ViewerState, error) {
	var state chat.ViewerState
	if av, ok := item[attrVersion]; ok {
		version, err := parseN(av)
		if err != nil {
			return chat.ViewerState{}, fmt.Errorf("parsing %s: %w", attrVersion, err)
		}
		state.Version = version
	}
	if av, ok := item[attrMutedUntil]; ok {
		until, err := parseN(av)
		if err != nil {
			return chat.ViewerState{}, fmt.Errorf("parsing %s: %w", attrMutedUntil, err)
		}
		if until >= muteForeverUntil {
			state.Mute = &chat.Mute{Forever: true}
		} else {
			state.Mute = &chat.Mute{Until: time.Unix(int64(until), 0).UTC()}
		}
	}
	return state, nil
}

// RecordSend reads the user's chat_activity item and, unless the read already
// shows the send throttled, writes the next last_sent_at and activity_score
// in one update conditioned on the last_sent_at it read (or on there being
// none). The score needs the prior one, which an update expression cannot
// compute, hence the read; the condition makes the read-compute-write atomic
// across writers, because every recorded send moves last_sent_at forward by
// at least ActivityRecordInterval and nothing moves it back, so a value that
// still matches proves nothing was recorded since the read.
//
// A writer that loses checks its send once more against the item the failure
// returned, with no second read. Usually the winner's send throttles it and
// it is done; when it is at least ActivityRecordInterval later than the
// winner's, it is the newest send and is written once more, scored against
// the winner's. Should that write lose too, another send has landed since,
// and the send goes unrecorded, as a throttled one does: a record is best
// effort, and its user's next send repairs it.
//
// The read is eventually consistent, at half a strong read's cost: a stale
// one only shows an older send, so a throttled answer from it is right, and a
// write from it fails its condition and is checked again against the current
// item.
// last_sent_at and activity_score are epoch milliseconds, which keeps the
// throttle's arithmetic and both indexes' orders exact.
func (s *store) RecordSend(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, sentAt time.Time) (bool, error) {
	if !chat.IsGroupChatID(chatID) {
		return false, fmt.Errorf("not a group chat id")
	}
	sentAtMillis := sentAt.UnixMilli()
	if sentAtMillis <= 0 {
		return false, fmt.Errorf("send time %v is not after the epoch", sentAt)
	}
	sentAt = time.UnixMilli(sentAtMillis).UTC()
	key := map[string]types.AttributeValue{
		attrPK: avS(chatPK(chatID)),
		attrSK: avS(userPK(userID)),
	}

	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:                aws.String(s.activityTable),
		Key:                      key,
		ProjectionExpression:     aws.String("#sent, #score"),
		ExpressionAttributeNames: map[string]string{"#sent": attrLastSentAt, "#score": attrActivityScore},
	})
	if err != nil {
		return false, err
	}
	item := out.Item

	for range maxRecordSendAttempts {
		var prior, lastSentAt time.Time
		values := map[string]types.AttributeValue{
			":sent":    avInt(sentAtMillis),
			":expires": avInt(sentAt.Add(chat.ActivityRetention).Unix()),
		}
		condition := "attribute_not_exists(#sent)"
		if av, ok := item[attrLastSentAt]; ok {
			lastSentMillis, err := parseInt(av)
			if err != nil {
				return false, fmt.Errorf("parsing %s: %w", attrLastSentAt, err)
			}
			if lastSentMillis > sentAtMillis-chat.ActivityRecordInterval.Milliseconds() {
				return false, nil
			}
			score, err := activityScoreFromItem(item)
			if err != nil {
				return false, err
			}
			lastSentAt = time.UnixMilli(lastSentMillis).UTC()
			prior = chat.EffectiveActivityScore(score, lastSentAt)
			condition = "#sent = :read"
			values[":read"] = av
		}
		values[":score"] = avInt(chat.NextActivityScore(prior, lastSentAt, sentAt).UnixMilli())

		_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:           aws.String(s.activityTable),
			Key:                 key,
			UpdateExpression:    aws.String("SET #sent = :sent, #score = :score, #expires = :expires"),
			ConditionExpression: aws.String(condition),
			ExpressionAttributeNames: map[string]string{
				"#sent":    attrLastSentAt,
				"#score":   attrActivityScore,
				"#expires": attrExpiresAt,
			},
			ExpressionAttributeValues:           values,
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		})
		if err == nil {
			return true, nil
		}
		var ccf *types.ConditionalCheckFailedException
		if !errors.As(err, &ccf) {
			return false, err
		}
		item = ccf.Item
	}
	return false, nil
}

// maxRecordSendAttempts bounds RecordSend's writes: the first, and one more
// after a lost race (see RecordSend).
const maxRecordSendAttempts = 2

// activityScoreFromItem decodes a chat_activity item's activity_score, the
// zero time when it has none (a record written before scores existed).
func activityScoreFromItem(item map[string]types.AttributeValue) (time.Time, error) {
	av, ok := item[attrActivityScore]
	if !ok {
		return time.Time{}, nil
	}
	scoreMillis, err := parseInt(av)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing %s: %w", attrActivityScore, err)
	}
	return time.UnixMilli(scoreMillis).UTC(), nil
}

// GetRecentSenders queries lsiByLastSentAt descending, eventually
// consistent at half the cost of a strong read (the index is local, so a
// strong one is available if a reader ever needs it). Every item in the
// partition is an activity record carrying last_sent_at, so the index holds
// them all.
func (s *store) GetRecentSenders(ctx context.Context, chatID *commonpb.ChatId, limit int) ([]chat.RecentSender, error) {
	return s.querySenders(ctx, chatID, lsiByLastSentAt, limit)
}

// GetActiveSenders queries lsiByActivityScore descending, eventually
// consistent like GetRecentSenders. The index is sparse on activity_score, so
// a record written before scores existed and never backfilled is missing
// from it.
func (s *store) GetActiveSenders(ctx context.Context, chatID *commonpb.ChatId, limit int) ([]chat.RecentSender, error) {
	return s.querySenders(ctx, chatID, lsiByActivityScore, limit)
}

// querySenders reads a group's activity records from one of the two LSIs on
// chat_activity, in descending order of its sort key. Each index projects the
// other's sort key, so a record's send time and score both come from the
// index and nothing is fetched from the table.
func (s *store) querySenders(ctx context.Context, chatID *commonpb.ChatId, index string, limit int) ([]chat.RecentSender, error) {
	if !chat.IsGroupChatID(chatID) {
		return nil, fmt.Errorf("not a group chat id")
	}

	senders := make([]chat.RecentSender, 0)
	var startKey map[string]types.AttributeValue
	for {
		input := &dynamodb.QueryInput{
			TableName:                aws.String(s.activityTable),
			IndexName:                aws.String(index),
			KeyConditionExpression:   aws.String("#pk = :pk"),
			ProjectionExpression:     aws.String("#sk, #sent, #score"),
			ExpressionAttributeNames: map[string]string{"#pk": attrPK, "#sk": attrSK, "#sent": attrLastSentAt, "#score": attrActivityScore},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": avS(chatPK(chatID)),
			},
			ScanIndexForward:  aws.Bool(false),
			ExclusiveStartKey: startKey,
		}
		if limit > 0 {
			input.Limit = aws.Int32(int32(limit - len(senders)))
		}
		res, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range res.Items {
			userID, err := userIDFromSK(item)
			if err != nil {
				return nil, err
			}
			sentAtMillis, err := parseInt(item[attrLastSentAt])
			if err != nil {
				return nil, fmt.Errorf("parsing %s: %w", attrLastSentAt, err)
			}
			score, err := activityScoreFromItem(item)
			if err != nil {
				return nil, err
			}
			lastSentAt := time.UnixMilli(sentAtMillis).UTC()
			senders = append(senders, chat.RecentSender{
				UserID:        userID,
				LastSentAt:    lastSentAt,
				ActivityScore: chat.EffectiveActivityScore(score, lastSentAt),
			})
		}
		if limit > 0 && len(senders) >= limit {
			return senders[:limit], nil
		}
		if len(res.LastEvaluatedKey) == 0 {
			return senders, nil
		}
		startKey = res.LastEvaluatedKey
	}
}

// GetLastSentAt is one eventually consistent GetItem of the user's activity
// record, projecting its send time.
func (s *store) GetLastSentAt(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (time.Time, bool, error) {
	if !chat.IsGroupChatID(chatID) {
		return time.Time{}, false, fmt.Errorf("not a group chat id")
	}

	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.activityTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(chatPK(chatID)),
			attrSK: avS(userPK(userID)),
		},
		ProjectionExpression:     aws.String("#sent"),
		ExpressionAttributeNames: map[string]string{"#sent": attrLastSentAt},
	})
	if err != nil {
		return time.Time{}, false, err
	}
	if len(out.Item) == 0 {
		return time.Time{}, false, nil
	}
	sentAtMillis, err := parseInt(out.Item[attrLastSentAt])
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parsing %s: %w", attrLastSentAt, err)
	}
	return time.UnixMilli(sentAtMillis).UTC(), true, nil
}

func (s *store) SetKeyEnvelope(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, envelope chat.KeyEnvelope) (chat.KeyEnvelope, error) {
	if !chat.IsGroupChatID(chatID) {
		return chat.KeyEnvelope{}, fmt.Errorf("not a group chat id")
	}
	if envelope.WrappedBy == nil {
		return chat.KeyEnvelope{}, fmt.Errorf("key envelope has no wrapper")
	}

	// An envelope the user wrapped themself stands: the put is refused when
	// one is stored, and the refusal returns it.
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(s.keyEnvelopesTable),
		Item:                keyEnvelopeItem(chatID, userID, envelope),
		ConditionExpression: aws.String("attribute_not_exists(#pk) OR #wrappedBy <> :user"),
		ExpressionAttributeNames: map[string]string{
			"#pk":        attrPK,
			"#wrappedBy": attrWrappedBy,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":user": avB(userID.Value),
		},
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err == nil {
		return envelope.Clone(), nil
	}
	var ccf *types.ConditionalCheckFailedException
	if !errors.As(err, &ccf) {
		return chat.KeyEnvelope{}, err
	}
	return keyEnvelopeFromItem(ccf.Item)
}

func (s *store) GetKeyEnvelope(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (chat.KeyEnvelope, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.keyEnvelopesTable),
		Key:            keyEnvelopeKey(chatID, userID),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return chat.KeyEnvelope{}, err
	}
	if len(out.Item) == 0 {
		return chat.KeyEnvelope{}, chat.ErrKeyEnvelopeNotFound
	}
	return keyEnvelopeFromItem(out.Item)
}

// keyEnvelopeKey is the key of userID's envelope for chatID in
// chat_key_envelopes.
func keyEnvelopeKey(chatID *commonpb.ChatId, userID *commonpb.UserId) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK: avS(userPK(userID)),
		attrSK: avS(chatSK(chatID)),
	}
}

// keyEnvelopeItem is userID's envelope for chatID as stored, key included.
func keyEnvelopeItem(chatID *commonpb.ChatId, userID *commonpb.UserId, envelope chat.KeyEnvelope) map[string]types.AttributeValue {
	item := keyEnvelopeKey(chatID, userID)
	item[attrScheme] = avN(uint64(envelope.Scheme))
	item[attrNonce] = avB(envelope.Nonce)
	item[attrCiphertext] = avB(envelope.Ciphertext)
	item[attrWrappedBy] = avB(envelope.WrappedBy.Value)
	return item
}

// keyEnvelopeFromItem decodes a chat_key_envelopes item. Every attribute is
// written with every item, so one missing its wrapper is a corrupt item
// rather than a default.
func keyEnvelopeFromItem(item map[string]types.AttributeValue) (chat.KeyEnvelope, error) {
	scheme, err := parseN(item[attrScheme])
	if err != nil {
		return chat.KeyEnvelope{}, err
	}
	wrappedBy := asB(item[attrWrappedBy])
	if len(wrappedBy) == 0 {
		return chat.KeyEnvelope{}, fmt.Errorf("key envelope item has no wrapper")
	}
	return chat.KeyEnvelope{
		Scheme:     chatpb.KeyEnvelope_Scheme(scheme),
		Nonce:      bytes.Clone(asB(item[attrNonce])),
		Ciphertext: bytes.Clone(asB(item[attrCiphertext])),
		WrappedBy:  &commonpb.UserId{Value: bytes.Clone(wrappedBy)},
	}, nil
}

// alongsideConditionFailed reports that a conditional write carried alongside
// a membership transition failed its condition (see transitionMembership).
// Index is the write's position in alongside.
type alongsideConditionFailed struct {
	Index int
}

func (e alongsideConditionFailed) Error() string {
	return fmt.Sprintf("write %d alongside the membership transition failed its condition", e.Index)
}

// EnterLobby is one transaction over the entry, both counts and the user's
// membership record: the entry is put conditionally on not existing, each
// count is moved conditionally on being under its cap, and the membership
// row in group_members is checked to be absent or not joined, so a member, a
// lobby at its cap, or a user at theirs, cancels the whole write and nothing
// is recorded. The membership check is what keeps an entry from landing for
// a user whose admission committed between the caller's gate and this write.
// The cancellation's per-item reasons tell the cases apart with no read: a
// joined member is ErrAlreadyMember, judged first; an entry that exists is
// the no-op, returned from the item the failure hands back, judged before
// the caps so a repeat never reports a full lobby; a failed lobby count is
// ErrLobbyFull, judged before the user's; a failed user count is
// ErrTooManyLobbies. A TransactionConflict — a concurrent write to one of the
// items, which the counts make likely in a busy lobby — is retried with
// backoff.
func (s *store) EnterLobby(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, limits chat.LobbyLimits) (chat.LobbyEntry, bool, error) {
	if !chat.IsGroupChatID(chatID) {
		return chat.LobbyEntry{}, false, fmt.Errorf("not a group chat id")
	}

	entry := chat.LobbyEntry{
		UserID:    &commonpb.UserId{Value: append([]byte(nil), userID.Value...)},
		EnteredAt: time.Now().UTC(),
	}
	item := lobbyKey(chatID, userID)
	item[attrEnteredAt] = avN(uint64(entry.EnteredAt.UnixNano()))
	transactItems := []types.TransactWriteItem{
		{Put: &types.Put{
			TableName:                           aws.String(s.lobbiesTable),
			Item:                                item,
			ConditionExpression:                 aws.String("attribute_not_exists(#pk)"),
			ExpressionAttributeNames:            map[string]string{"#pk": attrPK},
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}},
		{Update: s.lobbyCountUpdate(chatPK(chatID), 1, limits.LobbySize)},
		{Update: s.lobbyCountUpdate(userPK(userID), 1, limits.LobbiesPerUser)},
		{ConditionCheck: &types.ConditionCheck{
			TableName: aws.String(s.groupMembersTable),
			Key: map[string]types.AttributeValue{
				attrPK: avS(chatPK(chatID)),
				attrSK: avS(userPK(userID)),
			},
			ConditionExpression:       aws.String("attribute_not_exists(#state) OR #state <> :joined"),
			ExpressionAttributeNames:  map[string]string{"#state": attrState},
			ExpressionAttributeValues: map[string]types.AttributeValue{":joined": avN(memberStateJoined)},
		}},
	}

	backoff := membershipBackoffBase
	for attempt := 0; ; attempt++ {
		_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: transactItems})
		if err == nil {
			return entry, true, nil
		}
		reasons, ok := cancellationReasons(err)
		if !ok || len(reasons) != len(transactItems) {
			return chat.LobbyEntry{}, false, err
		}
		codes := make([]string, len(reasons))
		for i, reason := range reasons {
			codes[i] = aws.ToString(reason.Code)
		}
		switch {
		case codes[3] == conditionalCheckFailedCode:
			return chat.LobbyEntry{}, false, chat.ErrAlreadyMember
		case codes[0] == conditionalCheckFailedCode:
			existing, err := lobbyEntryFromItem(reasons[0].Item)
			return existing, false, err
		case codes[1] == conditionalCheckFailedCode:
			return chat.LobbyEntry{}, false, chat.ErrLobbyFull
		case codes[2] == conditionalCheckFailedCode:
			return chat.LobbyEntry{}, false, chat.ErrTooManyLobbies
		case isTransactionConflict(codes):
			if attempt+1 >= maxMembershipAttempts {
				return chat.LobbyEntry{}, false, fmt.Errorf("entering lobby of chat %x: %w", chatID.Value, err)
			}
			select {
			case <-ctx.Done():
				return chat.LobbyEntry{}, false, ctx.Err()
			case <-time.After(backoff + rand.N(backoff)):
			}
			backoff = min(2*backoff, membershipBackoffMax)
		default:
			return chat.LobbyEntry{}, false, err
		}
	}
}

// LeaveLobby is the inverse transaction: the entry deleted conditionally on
// existing, both counts moved down. An entry that does not exist cancels it,
// which is the no-op; a TransactionConflict is retried with backoff.
func (s *store) LeaveLobby(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId) (bool, error) {
	if !chat.IsGroupChatID(chatID) {
		return false, fmt.Errorf("not a group chat id")
	}

	transactItems := append([]types.TransactWriteItem{{Delete: s.lobbyEntryDelete(chatID, userID)}}, s.lobbyCountDecrements(chatID, userID)...)
	backoff := membershipBackoffBase
	for attempt := 0; ; attempt++ {
		_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: transactItems})
		if err == nil {
			return true, nil
		}
		reasons, ok := cancellationReasons(err)
		if !ok || len(reasons) != len(transactItems) {
			return false, err
		}
		codes := make([]string, len(reasons))
		for i, reason := range reasons {
			codes[i] = aws.ToString(reason.Code)
		}
		switch {
		case codes[0] == conditionalCheckFailedCode:
			return false, nil
		case isTransactionConflict(codes):
			if attempt+1 >= maxMembershipAttempts {
				return false, fmt.Errorf("leaving lobby of chat %x: %w", chatID.Value, err)
			}
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(backoff + rand.N(backoff)):
			}
			backoff = min(2*backoff, membershipBackoffMax)
		default:
			return false, err
		}
	}
}

// GetLobbyEntries reads the user's partition as GetViewerStates does: one
// strongly consistent point read for a single chat, else one strongly
// consistent query bounded to the sort-key range the requested chats span,
// with the rows not asked for dropped in memory.
func (s *store) GetLobbyEntries(ctx context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (map[string]chat.LobbyEntry, error) {
	wanted := make(map[string]struct{}, len(chatIDs))
	var lo, hi string
	for _, chatID := range chatIDs {
		sk := chatSK(chatID)
		wanted[sk] = struct{}{}
		if lo == "" || sk < lo {
			lo = sk
		}
		if sk > hi {
			hi = sk
		}
	}
	out := make(map[string]chat.LobbyEntry, len(wanted))
	if len(wanted) == 0 {
		return out, nil
	}

	if len(wanted) == 1 {
		res, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(s.lobbiesTable),
			Key:            lobbyKey(chatIDs[0], userID),
			ConsistentRead: aws.Bool(true),
		})
		if err != nil {
			return nil, err
		}
		if len(res.Item) == 0 {
			return out, nil
		}
		entry, err := lobbyEntryFromItem(res.Item)
		if err != nil {
			return nil, err
		}
		out[string(chatIDs[0].Value)] = entry
		return out, nil
	}

	var startKey map[string]types.AttributeValue
	for {
		res, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                aws.String(s.lobbiesTable),
			KeyConditionExpression:   aws.String("#pk = :pk AND #sk BETWEEN :lo AND :hi"),
			ExpressionAttributeNames: map[string]string{"#pk": attrPK, "#sk": attrSK},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": avS(userPK(userID)),
				":lo": avS(lo),
				":hi": avS(hi),
			},
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range res.Items {
			if _, ok := wanted[asS(item[attrSK])]; !ok {
				continue
			}
			chatID, err := chatIDFromSK(item)
			if err != nil {
				return nil, err
			}
			entry, err := lobbyEntryFromItem(item)
			if err != nil {
				return nil, err
			}
			out[string(chatID.Value)] = entry
		}
		if len(res.LastEvaluatedKey) == 0 {
			return out, nil
		}
		startKey = res.LastEvaluatedKey
	}
}

// GetLobbyPage is an ascending range on gsiLobbyByChat, which holds exactly
// the lobby's entries in entry order, so the page is billed by what it
// returns. The index projects every attribute, so the user comes with the
// row's key. A page resumes from an exclusive start key built from the
// position alone — the table key plus entered_at, which a position and the
// chat name — so a cursor need not name an entry still in the index
// (see GetGroupRosterPage for the same shape, and chat.LobbyPosition for the
// tie order the index may not share). The read is eventually consistent, a
// GSI's; see the table's doc for why that is the read that pays.
func (s *store) GetLobbyPage(ctx context.Context, chatID *commonpb.ChatId, after *chat.LobbyPosition, limit int) ([]chat.LobbyEntry, error) {
	if !chat.IsGroupChatID(chatID) {
		return nil, fmt.Errorf("not a group chat id")
	}

	var startKey map[string]types.AttributeValue
	if after != nil {
		startKey = lobbyKey(chatID, after.UserID)
		startKey[attrEnteredAt] = avN(uint64(after.EnteredAt.UnixNano()))
	}

	entries := make([]chat.LobbyEntry, 0)
	for {
		input := &dynamodb.QueryInput{
			TableName:                 aws.String(s.lobbiesTable),
			IndexName:                 aws.String(gsiLobbyByChat),
			KeyConditionExpression:    aws.String("#sk = :chat"),
			ExpressionAttributeNames:  map[string]string{"#sk": attrSK},
			ExpressionAttributeValues: map[string]types.AttributeValue{":chat": avS(chatSK(chatID))},
			ExclusiveStartKey:         startKey,
		}
		if limit > 0 {
			input.Limit = aws.Int32(int32(limit - len(entries)))
		}
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			entry, err := lobbyEntryFromItem(item)
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
			if limit > 0 && len(entries) == limit {
				return entries, nil
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			return entries, nil
		}
		startKey = out.LastEvaluatedKey
	}
}

// AdmitFromLobby is the join transition (see addGroupMembers) carrying the
// lobby's side of the admission alongside: the entry's conditional delete,
// its two count decrements and an unconditional put of the key envelope, so
// the four commit with the membership or not at all. A failed delete
// condition is the user not waiting, reported as ErrNotInLobby. A join that
// is a no-op — the user was a member already — writes nothing, the entry
// included: the caller gates on membership before asking, so an entry left
// behind this way is a race's residue the user clears by leaving the lobby.
func (s *store) AdmitFromLobby(ctx context.Context, chatID *commonpb.ChatId, userID *commonpb.UserId, envelope chat.KeyEnvelope) (bool, chat.RosterSummary, error) {
	if !chat.IsGroupChatID(chatID) {
		return false, chat.RosterSummary{}, fmt.Errorf("not a group chat id")
	}
	if envelope.WrappedBy == nil {
		return false, chat.RosterSummary{}, fmt.Errorf("key envelope has no wrapper")
	}

	roster, err := s.readRosterSummaryForWrite(ctx, chatID)
	if err != nil {
		return false, chat.RosterSummary{}, err
	}

	alongside := append([]types.TransactWriteItem{{Delete: s.lobbyEntryDelete(chatID, userID)}}, s.lobbyCountDecrements(chatID, userID)...)
	alongside = append(alongside, types.TransactWriteItem{Put: &types.Put{
		TableName: aws.String(s.keyEnvelopesTable),
		Item:      keyEnvelopeItem(chatID, userID, envelope),
	}})
	changed, err := s.transitionMembership(ctx, chatID, s.joinUpdate(chatID, userID), 1, &roster, alongside)
	var failed alongsideConditionFailed
	if errors.As(err, &failed) && failed.Index == 0 {
		return false, roster, chat.ErrNotInLobby
	}
	return changed, roster, err
}

// lobbyKey is the key of userID's entry in chatID's lobby in chat_lobbies.
func lobbyKey(chatID *commonpb.ChatId, userID *commonpb.UserId) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK: avS(userPK(userID)),
		attrSK: avS(chatSK(chatID)),
	}
}

// lobbyEntryDelete removes userID's entry in chatID's lobby, conditionally on
// its existing, so the transaction it rides tells a user who was waiting from
// one who was not.
func (s *store) lobbyEntryDelete(chatID *commonpb.ChatId, userID *commonpb.UserId) *types.Delete {
	return &types.Delete{
		TableName:                aws.String(s.lobbiesTable),
		Key:                      lobbyKey(chatID, userID),
		ConditionExpression:      aws.String("attribute_exists(#pk)"),
		ExpressionAttributeNames: map[string]string{"#pk": attrPK},
	}
}

// lobbyCountDecrements move the lobby's size and the user's lobby count down
// by one, for the transaction that removes an entry.
func (s *store) lobbyCountDecrements(chatID *commonpb.ChatId, userID *commonpb.UserId) []types.TransactWriteItem {
	return []types.TransactWriteItem{
		{Update: s.lobbyCountUpdate(chatPK(chatID), -1, 0)},
		{Update: s.lobbyCountUpdate(userPK(userID), -1, 0)},
	}
}

// lobbyCountUpdate moves the lobby_count of the #meta item under pk by delta,
// creating the item on first use. An increment is conditioned on the count
// being under limit, so the transaction it rides cancels at the cap. The item
// carries no entered_at, so it stays out of gsiLobbyByChat.
func (s *store) lobbyCountUpdate(pk string, delta int64, limit int) *types.Update {
	update := &types.Update{
		TableName: aws.String(s.lobbiesTable),
		Key: map[string]types.AttributeValue{
			attrPK: avS(pk),
			attrSK: avS(skMeta),
		},
		UpdateExpression:          aws.String(fmt.Sprintf("ADD %s :delta", attrLobbyCount)),
		ExpressionAttributeValues: map[string]types.AttributeValue{":delta": avInt(delta)},
	}
	if delta > 0 {
		update.ConditionExpression = aws.String(fmt.Sprintf("attribute_not_exists(%s) OR %s < :limit", attrLobbyCount, attrLobbyCount))
		update.ExpressionAttributeValues[":limit"] = avInt(int64(limit))
	}
	return update
}

// lobbyEntryFromItem reads a chat_lobbies entry: the user from its pk, and
// when they entered.
func lobbyEntryFromItem(item map[string]types.AttributeValue) (chat.LobbyEntry, error) {
	userID, err := userIDFromPK(item)
	if err != nil {
		return chat.LobbyEntry{}, err
	}
	nanos, err := parseN(item[attrEnteredAt])
	if err != nil {
		return chat.LobbyEntry{}, fmt.Errorf("reading lobby entry: %w", err)
	}
	return chat.LobbyEntry{UserID: userID, EnteredAt: time.Unix(0, int64(nanos)).UTC()}, nil
}

// featuredSKPrefix prefixes a position in the chat_featured_groups sk.
const featuredSKPrefix = "pos#"

// A position is written in two digits, so the list may not outgrow them; and
// a replace is one transaction of at most chat.MaxFeaturedGroups positions
// and the #meta item. Asserted rather than assumed, as for group creation:
// raising the cap past either fails to compile here.
const (
	_ uint = 100 - chat.MaxFeaturedGroups
	_ uint = maxTransactWriteItems - (chat.MaxFeaturedGroups + 1)
)

// A replace contends only with the same user's other replaces, on the #meta
// pattern (see maxMembershipAttempts): a lost compare-and-set reads again and
// retries at once, and a TransactionConflict backs off.
const maxFeaturedAttempts = maxMembershipAttempts

// maxFeaturedReads bounds how many times a read of a list is taken again
// when it straddles a replace (see readFeaturedGroups). A replace is one
// small transaction, so a read that straddles three in a row is a user
// replacing their list continuously.
const maxFeaturedReads = 3

// featuredSK is the sk of the given position in a user's featured groups,
// zero-padded so the sk order is the list order.
func featuredSK(position int) string {
	return fmt.Sprintf("%s%02d", featuredSKPrefix, position)
}

func (s *store) SetFeaturedGroups(ctx context.Context, userID *commonpb.UserId, chatIDs []*commonpb.ChatId) (chat.FeaturedGroups, bool, error) {
	if err := chat.ValidateFeaturedGroups(chatIDs); err != nil {
		return chat.FeaturedGroups{}, false, err
	}

	backoff := membershipBackoffBase
	for attempt := 0; attempt < maxFeaturedAttempts; attempt++ {
		// The list is read first, strongly consistent, both to answer the
		// no-op without a write and for the version the write is compared
		// against and the positions it must clear. A stale read would be
		// wrong here, not just late: it could answer a list it no longer
		// holds as the no-op and skip the write. The items are never read
		// for the write's sake beyond that: a replace overwrites every
		// position it fills.
		current, err := s.readFeaturedGroups(ctx, userID, true)
		if err != nil {
			return chat.FeaturedGroups{}, false, err
		}
		if current.Equal(chatIDs) {
			return current, false, nil
		}

		next := current.Version + 1
		condition := "#version = :version"
		values := map[string]types.AttributeValue{
			":next":  avN(next),
			":count": avN(uint64(len(chatIDs))),
		}
		if current.Version == 0 {
			// Version zero is a list never written, so no #meta item; the
			// first write must find none, or it lost to another first write.
			condition = "attribute_not_exists(#version)"
		} else {
			values[":version"] = avN(current.Version)
		}

		transactItems := []types.TransactWriteItem{{Update: &types.Update{
			TableName: aws.String(s.featuredGroupsTable),
			Key: map[string]types.AttributeValue{
				attrPK: avS(userPK(userID)),
				attrSK: avS(skMeta),
			},
			UpdateExpression:          aws.String(fmt.Sprintf("SET #version = :next, %s = :count", attrFeaturedCount)),
			ConditionExpression:       aws.String(condition),
			ExpressionAttributeNames:  map[string]string{"#version": attrVersion},
			ExpressionAttributeValues: values,
		}}}
		for position, chatID := range chatIDs {
			transactItems = append(transactItems, types.TransactWriteItem{Put: &types.Put{
				TableName: aws.String(s.featuredGroupsTable),
				Item: map[string]types.AttributeValue{
					attrPK:      avS(userPK(userID)),
					attrSK:      avS(featuredSK(position)),
					attrChat:    avB(chatID.Value),
					attrVersion: avN(next),
				},
			}})
		}
		for position := len(chatIDs); position < len(current.ChatIDs); position++ {
			transactItems = append(transactItems, types.TransactWriteItem{Delete: &types.Delete{
				TableName: aws.String(s.featuredGroupsTable),
				Key: map[string]types.AttributeValue{
					attrPK: avS(userPK(userID)),
					attrSK: avS(featuredSK(position)),
				},
			}})
		}

		_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: transactItems})
		if err == nil {
			featured := chat.FeaturedGroups{Version: next}
			for _, chatID := range chatIDs {
				featured.ChatIDs = append(featured.ChatIDs, &commonpb.ChatId{Value: append([]byte(nil), chatID.Value...)})
			}
			return featured, true, nil
		}
		reasons, ok := cancellationReasons(err)
		if !ok || len(reasons) != len(transactItems) {
			return chat.FeaturedGroups{}, false, err
		}
		codes := make([]string, len(reasons))
		for i, reason := range reasons {
			codes[i] = aws.ToString(reason.Code)
		}
		switch {
		case codes[0] == conditionalCheckFailedCode:
			// Another replace landed since the read: read again, which may
			// find the list already what this one asks for.
			continue
		case isTransactionConflict(codes):
			select {
			case <-ctx.Done():
				return chat.FeaturedGroups{}, false, ctx.Err()
			case <-time.After(backoff + rand.N(backoff)):
			}
			backoff = min(2*backoff, membershipBackoffMax)
			continue
		default:
			return chat.FeaturedGroups{}, false, err
		}
	}
	return chat.FeaturedGroups{}, false, fmt.Errorf("replacing featured groups for user %x: attempts exhausted", userID.Value)
}

// GetFeaturedGroups reads eventually consistent, at half the cost: its
// readers display the list (see chat.Store).
func (s *store) GetFeaturedGroups(ctx context.Context, userID *commonpb.UserId) (chat.FeaturedGroups, error) {
	return s.readFeaturedGroups(ctx, userID, false)
}

// readFeaturedGroups reads userID's featured groups: one query of their
// partition, strongly consistent when consistent is set, #meta at its head
// and the positions after it in order. A query is consistent per item, not
// as a set, and an eventually consistent one can also take items from
// replicas at different points, so either can return #meta from one side of
// a replace and some positions from the other. Every replace stamps every position it fills
// with its version, so the read is a list exactly when every position carries
// #meta's version and they number featured_count, contiguous from zero; it
// is taken again, up to maxFeaturedReads times, when it is not.
func (s *store) readFeaturedGroups(ctx context.Context, userID *commonpb.UserId, consistent bool) (chat.FeaturedGroups, error) {
	for attempt := 0; attempt < maxFeaturedReads; attempt++ {
		var items []map[string]types.AttributeValue
		paginator := dynamodb.NewQueryPaginator(s.client, &dynamodb.QueryInput{
			TableName:                 aws.String(s.featuredGroupsTable),
			KeyConditionExpression:    aws.String(fmt.Sprintf("%s = :pk", attrPK)),
			ExpressionAttributeValues: map[string]types.AttributeValue{":pk": avS(userPK(userID))},
			ConsistentRead:            aws.Bool(consistent),
		})
		for paginator.HasMorePages() {
			out, err := paginator.NextPage(ctx)
			if err != nil {
				return chat.FeaturedGroups{}, err
			}
			items = append(items, out.Items...)
		}

		featured, ok, err := featuredGroupsFromItems(items)
		if err != nil {
			return chat.FeaturedGroups{}, err
		}
		if ok {
			return featured, nil
		}
	}
	return chat.FeaturedGroups{}, fmt.Errorf("reading featured groups for user %x: every read straddled a replace", userID.Value)
}

// featuredGroupsFromItems assembles a list from a query of its partition,
// reporting ok false when the items are not one list (see
// readFeaturedGroups). No items at all is the list never written.
func featuredGroupsFromItems(items []map[string]types.AttributeValue) (chat.FeaturedGroups, bool, error) {
	if len(items) == 0 {
		return chat.FeaturedGroups{}, true, nil
	}
	if asS(items[0][attrSK]) != skMeta {
		// Positions with no #meta: the query ran alongside the first write.
		return chat.FeaturedGroups{}, false, nil
	}
	version, err := parseN(items[0][attrVersion])
	if err != nil {
		return chat.FeaturedGroups{}, false, fmt.Errorf("parsing featured groups version: %w", err)
	}
	count, err := parseN(items[0][attrFeaturedCount])
	if err != nil {
		return chat.FeaturedGroups{}, false, fmt.Errorf("parsing featured groups count: %w", err)
	}

	positions := items[1:]
	if uint64(len(positions)) != count {
		return chat.FeaturedGroups{}, false, nil
	}
	featured := chat.FeaturedGroups{Version: version}
	for i, item := range positions {
		if sk := asS(item[attrSK]); sk != featuredSK(i) {
			return chat.FeaturedGroups{}, false, nil
		}
		stamped, err := parseN(item[attrVersion])
		if err != nil {
			return chat.FeaturedGroups{}, false, fmt.Errorf("parsing featured group version: %w", err)
		}
		if stamped != version {
			return chat.FeaturedGroups{}, false, nil
		}
		chatID := asB(item[attrChat])
		if len(chatID) == 0 {
			return chat.FeaturedGroups{}, false, fmt.Errorf("featured group at %s has no chat", asS(item[attrSK]))
		}
		featured.ChatIDs = append(featured.ChatIDs, &commonpb.ChatId{Value: chatID})
	}
	return featured, true, nil
}

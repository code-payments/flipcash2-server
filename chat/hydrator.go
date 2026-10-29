package chat

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/redact"
)

// Hydrator builds chats' proto metadata (see hydrate): the one projection of
// a chat's record, and of what is read beside it, that every carrier of
// chatpb.Metadata returns. It is the Chat service's, and is its own type so
// that another domain showing chats it has chosen (see PublicGroupsMetadata)
// shows them exactly as the Chat service would, without holding the service.
//
// It holds no state of its own, only the readers a hydration asks, so the
// parent builds one and hands it to every server that shows chats.
type Hydrator struct {
	log *zap.Logger

	blocklist BlocklistReader
	chats     Store
	media     Media
	messaging MessagingReader
	profiles  ProfileReader

	// rules projects what a chat's metadata shows as its rules (see
	// RuleEvaluator.RulesOf), which for a DM is decided per read.
	rules *RuleEvaluator

	// teamUserID is the Flipcash team account (see flipcashteam), whose DMs are
	// never end-to-end encrypted (see useE2ee). The Never speaker rule they
	// carry is the RuleEvaluator's, built with the same account (see
	// RuleEvaluator.RulesOf). Nil when there is none.
	teamUserID *commonpb.UserId
}

func NewHydrator(
	log *zap.Logger,

	blocklist BlocklistReader,
	chats Store,
	media Media,
	messaging MessagingReader,
	profiles ProfileReader,

	rules *RuleEvaluator,

	teamUserID *commonpb.UserId,
) *Hydrator {
	return &Hydrator{
		log: log,

		blocklist: blocklist,
		chats:     chats,
		media:     media,
		messaging: messaging,
		profiles:  profiles,

		rules: rules,

		teamUserID: teamUserID,
	}
}

// A public list is a list of groups shown to anyone, the same for every
// viewer: a user's featured groups (see featured.go), and a currency's top
// holder chats (see the community package). Every group in one is projected
// the same way (see publicGroupsMetadata).

// PublicGroupsMetadata returns the given groups, in the given order, as a
// public list shows them (see publicGroupsMetadata), reading their records
// first. A group with no record, or a private one, is left out. The read is
// eventually consistent (see Store.GetGroupChatsByID), so a group created a
// moment ago may be left out until it catches up. It returns an error if any
// ID is not a group chat ID.
//
// It is how another domain shows a list of groups it has chosen: the
// projection is the chat domain's, so a list built elsewhere looks exactly
// like one built here.
func (h *Hydrator) PublicGroupsMetadata(ctx context.Context, chatIDs []*commonpb.ChatId) ([]*chatpb.Metadata, error) {
	if len(chatIDs) == 0 {
		return []*chatpb.Metadata{}, nil
	}
	records, err := h.chats.GetGroupChatsByID(ctx, chatIDs)
	if err != nil {
		return nil, err
	}
	return h.publicGroupsMetadata(ctx, h.log, chatIDs, records)
}

// publicGroupsMetadata projects a public list, in its order, from the
// groups' records: each as a list view shows it, with no viewer (so nothing
// about anyone's place in the group) and no reading (so no messaging state),
// which is the same for every caller. A group with no record is left out.
//
// A private group is left out too. Every list refuses or never names one,
// and IsPrivate never changes, so none is expected; the check is what keeps
// a record that broke that from reaching a caller with no standing to see
// it, and is logged when it does.
func (h *Hydrator) publicGroupsMetadata(ctx context.Context, log *zap.Logger, chatIDs []*commonpb.ChatId, records map[string]*Chat) ([]*chatpb.Metadata, error) {
	chats := make([]*Chat, 0, len(chatIDs))
	for _, chatID := range chatIDs {
		c, ok := records[string(chatID.Value)]
		if !ok {
			continue
		}
		if c.IsPrivate {
			log.Warn("Private group in a public list", zap.String("chat_id", model.ChatIDString(chatID)))
			continue
		}
		chats = append(chats, c)
	}
	if len(chats) == 0 {
		return []*chatpb.Metadata{}, nil
	}
	return h.hydrate(ctx, nil, ListenerStanding{}, ReadingDenied, listDetail, chats)
}

// metadataDetail is how much of a chat's record hydrate returns.
type metadataDetail int

const (
	// fullDetail is the whole record, and the chat's pin summary beside it,
	// for a caller showing one chat — GetChat, a creation, an edit, a join, a
	// lobby — or carrying it to a client that may.
	fullDetail metadataDetail = iota

	// listDetail is the record as a list view shows it: everything but the
	// group's cover picture, which only its profile view shows, and the
	// chat's pin summary, which only the conversation shows. A feed page is
	// many chats a client renders as rows, and the cover's renditions cost
	// the server a resolve and a download URL per group, and the pin summary
	// a read of the chat's pins and of its latest pinned message, for fields
	// no row renders. The description stays: a row may show it, and it is
	// short text read with the record, costing nothing to carry. The proto
	// allows a list view to omit the cover and the pin summary (see
	// chat.v1.Metadata.cover_picture and pin_summary), and tells the client to
	// fetch them with GetChat and never to let a list result clear what it
	// holds.
	listDetail
)

// hydrate builds the proto metadata for a set of chats as a viewer of the given
// standing sees them, batching the reads across the whole set: every chat's
// last message in one call, every group's roster summary in one call, every
// hydrated member's pointers in one call, every chat's head event sequence in
// one call, every member's display name in one call, and every DM member's
// phone number in one call. The calls are independent and run concurrently, so
// a page costs the slowest of them rather than their sum.
//
// The standing is the viewer's towards every chat in the set (see ListenerStanding),
// and the reading is what the viewer's read of every chat in the set is
// answered with (see ListenerStanding.Reading). Together they decide what is hydrated
// at all: what they withhold is never read, not read and dropped. Most
// callers hydrate chats the viewer is a member of — a feed built from their
// memberships, a join or creation that just landed — and pass memberListenerStanding
// and ReadingFull. GetChat hydrates a group for whoever asks, and passes what
// Access found under the mode the client asked for:
//
//   - A non-member has no hydrated member in a group: they are not on the
//     roster, and a pointer stored for them — a former member's — is not
//     theirs to see. A DM's members are its record's, whoever the viewer is.
//   - A viewer whose reading is denied gets no messaging state: no last
//     message, no head event sequence, no pointers, no pin summary. The
//     record's own fields — title, description, pictures, rules, roster
//     summary, last activity — are hydrated for anyone the caller admits to
//     the record at all.
//   - A viewer whose reading is redacted gets the messaging state with the
//     last message redacted (see redact.Message), after its media is
//     resolved, so the placeholder carries the blurhash a client renders, and
//     the latest pinned message redacted by the read that returns it (see
//     MessagingReader.Messages). The read fails rather than returning a
//     message that cannot be redacted.
//
// A DM's members are its two participants, carried on the canonical record. A
// group's roster lives in its own records and is not enumerated here: the
// metadata carries the viewer as its only member, and the group's roster
// summary — read as a batch across the set — says how large the roster really
// is. So the cost of a group in the set is fixed, whatever its size, and every
// caller passes chats straight from the store with no membership read of its
// own. The roster itself is a separate read (see GetRoster), and it is where
// a member's join time and version are read: the viewer's own entry here
// carries neither (see Member.joined_at), since nothing a client does with
// its own entry needs them, and reading them would cost a membership read
// per group on the page.
//
// Display names are populated for members of every chat: they are the public
// identifier a member is known by within the chat. Phone numbers are populated
// only for members of DM chats, so each party can resolve the other to a
// contact. Group chats deliberately do not expose member phone numbers.
//
// Pointers are populated for every hydrated member: both parties of a DM, and
// the viewer in a group. The viewer's own READ pointer is what lets a client
// compute its unread count. Other group members' pointers are stored but never
// surfaced: they are not broadcast in real time (see
// messaging.Server.AdvancePointer), and hydrating them would cost two keyed
// reads per member, growing with the roster.
//
// is_hidden is per-viewer: a DM is hidden from viewerID when the DM's peer (the
// member who is not the viewer) is on the viewer's blocklist. Every DM peer
// across the set is resolved against the viewer's blocklist in one batched read.
//
// use_e2ee is a DM's alone: it is set on every DM but the team account's (see
// useE2ee), decided per read off the members with nothing read and never
// stored on the chat, and a group never carries it.
//
// The viewer is nil for an unauthenticated read (see getPublicChat): its
// standing is never a member's and it is never shown a DM, so no per-viewer
// state is read on its behalf.
//
// in_lobby is per-viewer and a non-member's alone: whether the viewer is
// waiting in a private group's lobby (see lobby.go), read for every private
// group in the set when the viewer is not a member, one strongly consistent
// read across the set, and never for a member, who is past waiting, or for a
// group that is not private, which has no lobby. A nil viewer waits nowhere.
//
// viewer_state is per-viewer too, and a member's alone: it is what the chat
// holds about the viewer (see ViewerState) plus what they may do in it, and
// a non-member may do nothing and has no standing to see what the record
// still holds — a mute a departure failed to clear stays on the record, out
// of sight, until they rejoin. So it is read only for a member, one read
// across the set, and every member gets one: the record exactly, a lapsed
// mute included, or the zero record when they have written none, with their
// permissions computed off the record (see Chat.PermissionsFor and
// ViewerState.ToProto). A non-member gets none.
//
// A group's profile picture and cover picture are each stored as the blob
// holding its ORIGINAL; every picture across the set is expanded to its full
// rendition set, each with a short-lived download URL, in one batched read —
// so a client renders the group's avatar and banner without a follow-up
// GetBlobs. That is done without an ACL check here: a chat's pictures are
// granted to the chat and its public profile when they are set (see
// blob.Integration.SetAsChatMedia), and a non-member is shown them as part of
// the record that identifies the group (see GetChat). A picture whose
// original no longer resolves is left with its stored ORIGINAL for the client
// to treat as unavailable, rather than failing the whole read.
//
// The detail is how much of the record is returned (see metadataDetail): a
// feed asks for listDetail, and its chats carry no cover picture and no pin
// summary — the renditions and the pins are never read, not read and dropped
// — while every other caller asks for fullDetail.
//
// pin_summary is messaging state shown with one chat (see pin.go): read only
// under fullDetail for a viewer whose reading is not denied, and then set on
// every chat in the set, the zero summary (count 0, version 0, no latest) for
// one that has never had a pin. Every chat's summary is read in one batch,
// then every latest pinned message in one read under the reading, so a
// redacted viewer gets it redacted; the two are sequential, one goroutine
// beside the other reads. A latest pinned message the read does not return
// fails the hydration, as every pin the service shows carries its message
// (see pinsToProto); a deleted one is returned as its tombstone, which is not
// missing.
//
// holdings_summary is the group's holdings snapshot (see holdings.go), read
// for every group in the set gated on a currency (see GatedMint), one batched
// read across the set, whoever the viewer and whatever the detail: it is
// public, like the roster summary. A group with no snapshot, one whose
// snapshot is not listed (too few holders; see GroupHoldings.Listed), or one
// whose snapshot names a mint other than the one its record is gated on
// (which no refresh writes, since a group's mints are fixed at creation),
// carries none.
func (h *Hydrator) hydrate(ctx context.Context, viewerID *commonpb.UserId, standing ListenerStanding, reading Reading, detail metadataDetail, chats []*Chat) ([]*chatpb.Metadata, error) {
	var msgRefs []MessageRef
	var seqChatIDs []*commonpb.ChatId
	var pointerRefs []PointerRef
	var groupChatIDs []*commonpb.ChatId
	uniqueUserIDs := make(map[string]*commonpb.UserId)
	uniquePrivateProfileUserIds := make(map[string]*commonpb.UserId)
	dmPeerByChat := make(map[string]*commonpb.UserId)
	uniquePeerIDs := make(map[string]*commonpb.UserId)
	uniquePictureBlobIDs := make(map[string]*blobpb.BlobId)
	var lobbyChatIDs []*commonpb.ChatId
	var holdingsChatIDs []*commonpb.ChatId
	gatedMints := make(map[string]*commonpb.PublicKey)
	hydratedMembers := make([][]*commonpb.UserId, len(chats))
	// The pin summary is messaging state (see above), and the same for every
	// chat in the set, since the detail and the reading are.
	showPins := detail == fullDetail && reading != ReadingDenied
	for i, c := range chats {
		if mint, ok := GatedMint(c); ok {
			holdingsChatIDs = append(holdingsChatIDs, c.ID)
			gatedMints[string(c.ID.Value)] = mint
		}
		if c.IsPrivate && !standing.IsMember && viewerID != nil {
			lobbyChatIDs = append(lobbyChatIDs, c.ID)
		}
		// The members to hydrate: a DM's participants, or the viewer alone in a
		// group they are a member of (see above).
		members := c.Members
		if IsGroupChatID(c.ID) {
			members = nil
			if standing.IsMember {
				members = []*commonpb.UserId{viewerID}
			}
			groupChatIDs = append(groupChatIDs, c.ID)
		}
		hydratedMembers[i] = members

		// Messaging state is a reader's alone (see above).
		if reading != ReadingDenied {
			if len(members) > 0 {
				pointerRefs = append(pointerRefs, PointerRef{ChatID: c.ID, Members: members})
			}
			if c.LastMessageID != nil {
				msgRefs = append(msgRefs, MessageRef{ChatID: c.ID, MessageID: c.LastMessageID})
				// A chat's head is 0 unless it has at least one message, which is
				// exactly when it has a last message ID. Skip the rest: their head is
				// the proto default 0.
				seqChatIDs = append(seqChatIDs, c.ID)
			}
		}
		if c.ProfilePictureBlobID != nil {
			uniquePictureBlobIDs[string(c.ProfilePictureBlobID.Value)] = c.ProfilePictureBlobID
		}
		if c.CoverPictureBlobID != nil && detail == fullDetail {
			uniquePictureBlobIDs[string(c.CoverPictureBlobID.Value)] = c.CoverPictureBlobID
		}
		for _, m := range members {
			uniqueUserIDs[string(m.Value)] = m
			if c.Type == chatpb.ChatType_CONTACT_DM {
				uniquePrivateProfileUserIds[string(m.Value)] = m
			}
		}
		if IsDmChatType(c.Type) {
			for _, m := range c.Members {
				if !bytes.Equal(m.Value, viewerID.GetValue()) {
					dmPeerByChat[string(c.ID.Value)] = m
					uniquePeerIDs[string(m.Value)] = m
					break
				}
			}
		}
	}
	userIDs := make([]*commonpb.UserId, 0, len(uniqueUserIDs))
	for _, u := range uniqueUserIDs {
		userIDs = append(userIDs, u)
	}
	privateProfileUserIDs := make([]*commonpb.UserId, 0, len(uniquePrivateProfileUserIds))
	for _, u := range uniquePrivateProfileUserIds {
		privateProfileUserIDs = append(privateProfileUserIDs, u)
	}
	peerIDs := make([]*commonpb.UserId, 0, len(uniquePeerIDs))
	for _, u := range uniquePeerIDs {
		peerIDs = append(peerIDs, u)
	}
	pictureBlobIDs := make([]*blobpb.BlobId, 0, len(uniquePictureBlobIDs))
	for _, id := range uniquePictureBlobIDs {
		pictureBlobIDs = append(pictureBlobIDs, id)
	}

	// The reads are independent of one another, so they run concurrently: the
	// page waits for the slowest rather than the sum. Each goroutine writes only
	// its own result, and Wait is the barrier before any is read. The first
	// failure cancels the rest through the group's context.
	var (
		lastMessages           map[string]*messagingpb.Message
		rosterSummaries        map[string]RosterSummary
		pointers               map[string][]*messagingpb.Pointer
		latestEventSeqs        map[string]uint64
		phoneNumbersByUserId   map[string]*commonpb.PhoneNumber
		publicProfilesByUserId map[string]*profilepb.UserProfile
		blockedPeers           map[string]bool
		pictureRenditions      map[string][]*blobpb.Rendition
		viewerStates           map[string]ViewerState
		lobbyEntries           map[string]LobbyEntry
		holdings               map[string]GroupHoldings
		pinSummaries           map[string]PinSummary
		pinnedMessages         map[MessageKey]*messagingpb.Message
	)
	chatIDs := make([]*commonpb.ChatId, len(chats))
	for i, c := range chats {
		chatIDs[i] = c.ID
	}
	// A read with nothing to ask for is skipped, not made: a viewer's standing
	// may have withheld a whole class of state from the set.
	g, gctx := errgroup.WithContext(ctx)
	if len(msgRefs) > 0 {
		g.Go(func() (err error) {
			lastMessages, err = h.messaging.LastMessages(gctx, msgRefs)
			return err
		})
	}
	g.Go(func() (err error) {
		rosterSummaries, err = h.chats.GetGroupRosterSummaries(gctx, groupChatIDs)
		return err
	})
	if len(pointerRefs) > 0 {
		g.Go(func() (err error) {
			pointers, err = h.messaging.Pointers(gctx, pointerRefs)
			return err
		})
	}
	if len(seqChatIDs) > 0 {
		g.Go(func() (err error) {
			latestEventSeqs, err = h.messaging.LatestEventSequences(gctx, seqChatIDs)
			return err
		})
	}
	g.Go(func() (err error) {
		phoneNumbersByUserId, err = h.profiles.GetPhoneNumbers(gctx, privateProfileUserIDs)
		return err
	})
	g.Go(func() (err error) {
		publicProfilesByUserId, err = h.profiles.GetLimitedPublicProfilesForRow(gctx, userIDs)
		return err
	})
	if len(peerIDs) > 0 {
		g.Go(func() (err error) {
			blockedPeers, err = h.blocklist.GetBlocked(gctx, viewerID, peerIDs)
			return err
		})
	}
	if standing.IsMember {
		g.Go(func() (err error) {
			viewerStates, err = h.chats.GetViewerStates(gctx, viewerID, chatIDs)
			return err
		})
	}
	if len(pictureBlobIDs) > 0 {
		g.Go(func() (err error) {
			pictureRenditions, err = h.media.ResolveRenditions(gctx, pictureBlobIDs)
			return err
		})
	}
	if len(lobbyChatIDs) > 0 {
		g.Go(func() (err error) {
			lobbyEntries, err = h.chats.GetLobbyEntries(gctx, viewerID, lobbyChatIDs)
			return err
		})
	}
	if len(holdingsChatIDs) > 0 {
		g.Go(func() (err error) {
			holdings, err = h.chats.GetGroupHoldings(gctx, holdingsChatIDs)
			return err
		})
	}
	if showPins && len(chatIDs) > 0 {
		// The latest pinned messages are named by the summaries, so the two
		// reads are one after the other, together beside the rest.
		g.Go(func() (err error) {
			pinSummaries, err = h.chats.GetPinSummaries(gctx, chatIDs)
			if err != nil {
				return err
			}
			var refs []MessageRef
			for _, chatID := range chatIDs {
				if latest := pinSummaries[string(chatID.Value)].Latest; latest != nil {
					refs = append(refs, MessageRef{ChatID: chatID, MessageID: latest.MessageID})
				}
			}
			pinnedMessages, err = readPinnedMessages(gctx, h.messaging, reading, nil, refs)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	metadata := make([]*chatpb.Metadata, len(chats))
	for i, c := range chats {
		key := string(c.ID.Value)
		md := c.ToProto()
		if standing.IsMember {
			md.ViewerState = viewerStates[key].ToProto(c.PermissionsFor(viewerID, true))
		}
		if IsGroupChatID(c.ID) {
			// ToProto projects the canonical record, which carries neither a
			// group's members nor its summary. The members are the hydrated ones
			// — the viewer, or no one (see above). Every group in the set exists,
			// but the batch read is eventually consistent, so a group written
			// moments ago may be absent and read as a zero summary, and one moved
			// moments ago may read one transition behind. A caller that has
			// just written the group or its roster holds the authoritative
			// summary and should overwrite this one with it.
			md.Members = make([]*chatpb.Member, 0, len(hydratedMembers[i]))
			for _, m := range hydratedMembers[i] {
				md.Members = append(md.Members, &chatpb.Member{UserId: &commonpb.UserId{Value: append([]byte(nil), m.Value...)}})
			}
			md.RosterSummary = rosterSummaries[key].ToProto()
		}
		md.LastMessage = lastMessages[key]
		if reading == ReadingRedacted && md.LastMessage != nil {
			redacted, err := redact.Message(c.ID, md.LastMessage)
			if err != nil {
				return nil, fmt.Errorf("redacting last message of chat %s: %w", base64.StdEncoding.EncodeToString(c.ID.Value), err)
			}
			md.LastMessage = redacted
		}
		md.LatestEventSequence = latestEventSeqs[key]
		if showPins {
			summary, err := pinSummaryToProto(pinnedMessages, c.ID, pinSummaries[key])
			if err != nil {
				return nil, fmt.Errorf("pin summary of chat %s: %w", base64.StdEncoding.EncodeToString(c.ID.Value), err)
			}
			md.PinSummary = summary
		}
		if peer, ok := dmPeerByChat[key]; ok {
			md.IsHidden = blockedPeers[string(peer.Value)]
		}
		if _, ok := lobbyEntries[key]; ok {
			md.InLobby = true
		}
		if snapshot, ok := holdings[key]; ok && snapshot.Listed() && bytes.Equal(snapshot.Mint.Value, gatedMints[key].GetValue()) {
			md.HoldingsSummary = snapshot.ToProto()
		}
		if IsDmChatType(c.Type) {
			md.UseE2Ee = useE2ee(c, h.teamUserID)
			md.Rules = h.rules.RulesOf(c)
		}
		// ToProto seeds each picture with its stored ORIGINAL; swap in the full
		// resolved set when the original is servable, else leave that seed.
		if c.ProfilePictureBlobID != nil {
			if renditions, ok := pictureRenditions[string(c.ProfilePictureBlobID.Value)]; ok {
				md.ProfilePicture.Renditions = renditions
			}
		}
		switch detail {
		case fullDetail:
			if c.CoverPictureBlobID != nil {
				if renditions, ok := pictureRenditions[string(c.CoverPictureBlobID.Value)]; ok {
					md.CoverPicture.Renditions = renditions
				}
			}
		case listDetail:
			md.CoverPicture = nil
		}
		assignPointers(md.Members, pointers[key])
		for _, m := range md.Members {
			if err := assignProfile(m, md.Type, publicProfilesByUserId, phoneNumbersByUserId); err != nil {
				return nil, err
			}
		}
		metadata[i] = md
	}
	return metadata, nil
}

// assignProfile fills a member's profile from a batch of public profiles and,
// for a contact DM's member, their phone number. Every chat member is a user
// the profile domain knows, so a missing profile is a data integrity problem
// rather than something to paper over with an invented one.
//
// The batch returns one proto per user, but each member row is filled in per
// chat, so every row gets its own copy. Sharing one would let a contact DM's
// phone number show up on that same user's row in a chat of another type.
// Today's callers pass a single chat or a single-type feed page, so the copy
// is what keeps that true of any future caller.
func assignProfile(m *chatpb.Member, chatType chatpb.ChatType, publicProfiles map[string]*profilepb.UserProfile, phoneNumbers map[string]*commonpb.PhoneNumber) error {
	publicProfile, ok := publicProfiles[string(m.UserId.Value)]
	if !ok {
		return fmt.Errorf("no public profile for chat member %s", base64.StdEncoding.EncodeToString(m.UserId.Value))
	}
	profile := proto.Clone(publicProfile).(*profilepb.UserProfile)
	if chatType == chatpb.ChatType_CONTACT_DM {
		profile.PhoneNumber = phoneNumbers[string(m.UserId.Value)]
	}
	m.UserProfile = profile
	return nil
}

// assignPointers distributes a chat's pointers onto the matching member entries
// by user ID. SENT pointers are never shared with the chat, so they are dropped
// defensively; each member is left with its DELIVERED and/or READ pointers.
func assignPointers(members []*chatpb.Member, pointers []*messagingpb.Pointer) {
	if len(pointers) == 0 {
		return
	}
	byUser := make(map[string][]*messagingpb.Pointer, len(members))
	for _, p := range pointers {
		if p.Type == messagingpb.Pointer_SENT {
			continue
		}
		byUser[string(p.UserId.Value)] = append(byUser[string(p.UserId.Value)], p)
	}
	for _, m := range members {
		m.Pointers = byUser[string(m.UserId.Value)]
	}
}

// useE2ee reports whether a DM's messages are to be end-to-end encrypted (see
// chat.v1.Metadata.use_e2ee): exactly when none of its members is the
// Flipcash team account. The team's DMs are opened and written to by the
// server (see flipcashteam), which holds no keys for it, and no one else may
// send in them (see RuleEvaluator.RulesOf). teamUserID is nil when there is no
// team account.
func useE2ee(c *Chat, teamUserID *commonpb.UserId) bool {
	if !IsDmChatType(c.Type) || len(c.Members) == 0 {
		return false
	}
	if teamUserID == nil {
		return true
	}
	for _, m := range c.Members {
		if bytes.Equal(m.Value, teamUserID.Value) {
			return false
		}
	}
	return true
}

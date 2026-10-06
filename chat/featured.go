package chat

import (
	"bytes"
	"fmt"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// A user's featured groups are an ordered list of group chats they show on
// their full profile. The list is the user's alone and says nothing about the
// groups: featuring a group needs no membership, leaving a group does not
// remove it, and nothing is recorded against the group. It is written whole (see
// Store.SetFeaturedGroups), so a reorder, an addition and a removal are the
// same write, and read whole. Whether each group still exists, and which a
// viewer may be shown, is decided when the list is read for display, not
// stored.

// MaxFeaturedGroups is the most groups a user may feature. A profile shows
// them all, so it is small; it is also what keeps a replace within one
// store transaction.
const MaxFeaturedGroups = 10

// FeaturedGroups is a user's featured groups as stored: the groups in the
// user's order, and the list's version, which moves by exactly one with each
// write that changes the list and never otherwise (state, not delta, as a
// roster's). A user who has never set a list reads as no groups at version
// zero.
type FeaturedGroups struct {
	ChatIDs []*commonpb.ChatId
	Version uint64
}

// Equal reports whether the two lists name the same groups in the same
// order, whatever their versions.
func (f FeaturedGroups) Equal(chatIDs []*commonpb.ChatId) bool {
	if len(f.ChatIDs) != len(chatIDs) {
		return false
	}
	for i := range chatIDs {
		if !bytes.Equal(f.ChatIDs[i].Value, chatIDs[i].Value) {
			return false
		}
	}
	return true
}

// ValidateFeaturedGroups returns an error unless chatIDs is a list a store
// accepts as a user's featured groups: at most MaxFeaturedGroups group chat
// IDs, none repeated. An empty list is valid and clears the featured
// groups. A repeat is refused rather than collapsed, since which of its
// positions to keep is the caller's choice. It does not check that the groups exist.
func ValidateFeaturedGroups(chatIDs []*commonpb.ChatId) error {
	if len(chatIDs) > MaxFeaturedGroups {
		return fmt.Errorf("%d featured groups exceeds the limit of %d", len(chatIDs), MaxFeaturedGroups)
	}
	seen := make(map[string]struct{}, len(chatIDs))
	for _, chatID := range chatIDs {
		if !IsGroupChatID(chatID) {
			return fmt.Errorf("featured chat %x is not a group chat id", chatID.GetValue())
		}
		if _, ok := seen[string(chatID.Value)]; ok {
			return fmt.Errorf("featured group %x is repeated", chatID.Value)
		}
		seen[string(chatID.Value)] = struct{}{}
	}
	return nil
}

package e2ee

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// DmChatIDSize is the length of a DM chat ID, the same as chat.DmChatIDSize:
// a chat ID's length is its type family's discriminator across the system,
// and an E2EE DM is a DM.
const DmChatIDSize = 32

// dmChatIDDomain namespaces the E2EE DM chat ID hash, so an encrypted DM
// between two users is a different chat from their plaintext DMs, which hash
// under chat's domains ("flipcash:chat:dm", "flipcash:chat:dm:<type>"). The
// two families can never alias: the inputs after the domain are fixed-width
// member sets, so distinct domains of distinct lengths give distinct
// preimages.
//
// This derivation is the package's stand-in for a chat record. There is no
// E2EE chat type in chat.v1 yet; when one lands, the DM's ID becomes
// chat.MustDeriveDmChatID under that type and this function is retired
// (nothing minted under it is meant to survive: the package is not yet
// integrated with anything that would remember it).
const dmChatIDDomain = "flipcash:chat:dm:e2ee"

// DeriveDmChatID returns the ID of the E2EE DM between two users. It is
// symmetric and stable, as chat.MustDeriveDmChatID is: the byte-sorted,
// de-duplicated member set hashed under dmChatIDDomain, so either user can
// name the chat without a lookup and the server can check a send's chat
// against its recipients (see Server.SendEnvelopes). A self-DM collapses to
// one member.
//
// It panics if either user ID is not model.UserIDSize wide, a programming
// error: user IDs are validated to width before they reach here.
func DeriveDmChatID(a, b *commonpb.UserId) *commonpb.ChatId {
	for _, u := range []*commonpb.UserId{a, b} {
		if len(u.GetValue()) != model.UserIDSize {
			panic(fmt.Sprintf("user id must be %d bytes, got %d", model.UserIDSize, len(u.GetValue())))
		}
	}

	members := [][]byte{a.Value, b.Value}
	sort.Slice(members, func(i, j int) bool {
		return bytes.Compare(members[i], members[j]) < 0
	})
	if bytes.Equal(members[0], members[1]) {
		members = members[:1]
	}

	h := sha256.New()
	h.Write([]byte(dmChatIDDomain))
	for _, m := range members {
		h.Write(m)
	}
	return &commonpb.ChatId{Value: h.Sum(nil)}
}

// IsDmChatID reports whether chatID has a DM's width. Anything else (a
// group's 16 bytes) names a chat this package does not serve.
func IsDmChatID(chatID *commonpb.ChatId) bool {
	return len(chatID.GetValue()) == DmChatIDSize
}

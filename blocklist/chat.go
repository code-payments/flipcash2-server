package blocklist

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// chatBlocklistReader adapts a blocklist Store to chat.BlocklistReader, the read
// slice the Chat service uses to compute per-viewer hidden state. It lives here
// (rather than in chat) because chat must not import blocklist; the dependency
// is one-way.
type chatBlocklistReader struct {
	store Store
}

// NewChatBlocklistReader returns a chat.BlocklistReader backed by the given
// blocklist store, for wiring the Chat service.
func NewChatBlocklistReader(store Store) chat.BlocklistReader {
	return &chatBlocklistReader{store: store}
}

func (r *chatBlocklistReader) GetBlocked(ctx context.Context, ownerID *commonpb.UserId, candidateIDs []*commonpb.UserId) (map[string]bool, error) {
	return r.store.GetBlocked(ctx, ownerID, candidateIDs)
}

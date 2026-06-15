package profile

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// chatProfileReader adapts a profile Store to chat.ProfileReader, the read slice
// the Chat service uses to hydrate member profiles. It lives here (rather than
// in chat) because chat must not import profile; the dependency is one-way.
type chatProfileReader struct {
	store Store
}

// NewChatProfileReader returns a chat.ProfileReader backed by the given profile
// store, for wiring the Chat service.
func NewChatProfileReader(store Store) chat.ProfileReader {
	return &chatProfileReader{store: store}
}

func (r *chatProfileReader) GetPhoneNumbers(ctx context.Context, userIDs []*commonpb.UserId) (map[string]*phonepb.PhoneNumber, error) {
	// Only numbers the user has enabled for payment are shared into the chat.
	return r.store.GetPhoneNumbersForPayment(ctx, userIDs)
}

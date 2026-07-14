package profile

import (
	"context"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// chatProfileReader adapts a profile Store to chat.ProfileReader, the read slice
// the Chat service uses to hydrate member profiles. It lives here (rather than
// in chat) because chat must not import profile; the dependency is one-way.
type chatProfileReader struct {
	store Store
	media Media
}

// NewChatProfileReader returns a chat.ProfileReader backed by the given profile
// store and blob storage, for wiring the Chat service. The media dependency is what
// lets member avatars come back with resolved blob metadata, so chat itself never
// touches blob storage.
func NewChatProfileReader(store Store, media Media) chat.ProfileReader {
	return &chatProfileReader{store: store, media: media}
}

func (r *chatProfileReader) GetPhoneNumbers(ctx context.Context, userIDs []*commonpb.UserId) (map[string]*phonepb.PhoneNumber, error) {
	// Only numbers the user has enabled for payment are shared into the chat.
	return r.store.GetPhoneNumbersForPayment(ctx, userIDs)
}

func (r *chatProfileReader) GetDisplayNames(ctx context.Context, userIDs []*commonpb.UserId) (map[string]string, error) {
	return r.store.GetDisplayNames(ctx, userIDs)
}

func (r *chatProfileReader) GetProfilePictures(ctx context.Context, userIDs []*commonpb.UserId) (map[string]*blobpb.Media, error) {
	blobIDs, err := r.store.GetProfilePictures(ctx, userIDs)
	if err != nil {
		return nil, err
	}
	if len(blobIDs) == 0 {
		return nil, nil
	}

	// Only the ORIGINAL is stored; the server derives no renditions yet.
	pictures := make(map[string]*blobpb.Media, len(blobIDs))
	toHydrate := make([]*blobpb.Media, 0, len(blobIDs))
	for userID, blobID := range blobIDs {
		picture := &blobpb.Media{
			Renditions: []*blobpb.Rendition{{
				Role:   blobpb.Rendition_ORIGINAL,
				BlobId: blobID,
			}},
		}
		pictures[userID] = picture
		toHydrate = append(toHydrate, picture)
	}

	// One batch for every member's avatar, rather than a resolve per member.
	if err := hydratePictures(ctx, r.media, toHydrate...); err != nil {
		return nil, err
	}
	return pictures, nil
}

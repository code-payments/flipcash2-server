package messaging

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/blob"
)

// Media is the blob-side integration messaging uses for media: it shares the
// blobs a message references into the chat on send (ShareIntoChat) and resolves
// their metadata on read (Resolve). It is implemented by blob.Integration.
//
// ShareIntoChat returns blob.ErrBlobNotShareable when a referenced blob may not be
// attached (unknown, not owned by the sender, or not a READY original), in which
// case nothing is granted. Resolve performs no authorization — the caller has
// already gated on chat membership — and returns metadata keyed by
// string(BlobId.Value), omitting unknown or not-yet-READY ids.
type Media interface {
	ShareIntoChat(ctx context.Context, sharerID *commonpb.UserId, chatID *commonpb.ChatId, blobIDs []*blobpb.BlobId) error
	Resolve(ctx context.Context, ids []*blobpb.BlobId) (map[string]*blobpb.BlobMetadata, error)
}

// validClientMedia reports whether a media content is one a client may author:
// at least one item, each carrying exactly one ORIGINAL rendition identified by a
// blob id and no server-resolved metadata (the server fills Blob on read, never
// the client). Ownership and readiness of the blobs are checked separately, when
// they are shared into the chat.
func validClientMedia(media *messagingpb.MediaContent) bool {
	if media == nil || len(media.Items) == 0 {
		return false
	}
	for _, item := range media.Items {
		if len(item.Renditions) != 1 {
			return false
		}
		r := item.Renditions[0]
		if r.Role != messagingpb.MediaItemRendition_ORIGINAL || r.BlobId == nil || r.Blob != nil {
			return false
		}
	}
	return true
}

// shareMessageMedia grants the chat read access to the blobs referenced by
// content, after verifying the sender owns each and it is a READY original. It
// reports denied=true when the media is not shareable (the caller returns its
// DENIED result) and a ready-to-return Internal error on an unexpected failure.
// Non-media content is a no-op. It runs before the message is persisted and
// broadcast, so the grants are durable before any recipient can resolve them.
func (s *Server) shareMessageMedia(ctx context.Context, log *zap.Logger, senderID *commonpb.UserId, chatID *commonpb.ChatId, content []*messagingpb.Content) (denied bool, err error) {
	blobIDs := mediaBlobIDs(content)
	if len(blobIDs) == 0 {
		return false, nil
	}
	switch err := s.media.ShareIntoChat(ctx, senderID, chatID, blobIDs); {
	case errors.Is(err, blob.ErrBlobNotShareable):
		return true, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure sharing media into chat")
		return false, status.Error(codes.Internal, "")
	}
	return false, nil
}

// mediaBlobIDs returns the blob ids a message references — whether the media is
// the message body or the body of a reply — or nil when there is no media. It
// assumes the content already passed clientAllowedContent.
func mediaBlobIDs(content []*messagingpb.Content) []*blobpb.BlobId {
	if len(content) != 1 {
		return nil
	}
	body := content[0]
	if reply, ok := body.Type.(*messagingpb.Content_Reply); ok {
		if len(reply.Reply.Content) != 1 {
			return nil
		}
		body = reply.Reply.Content[0]
	}
	media, ok := body.Type.(*messagingpb.Content_Media)
	if !ok {
		return nil
	}
	var ids []*blobpb.BlobId
	for _, item := range media.Media.Items {
		for _, r := range item.Renditions {
			if r.BlobId != nil {
				ids = append(ids, r.BlobId)
			}
		}
	}
	return ids
}

// hydrateMedia fills each media rendition's Blob with freshly resolved metadata
// (mime type, size, a short-lived download URL, dimensions). The caller has
// already gated on chat membership, and every blob in a stored message was granted
// to the chat when it was sent, so this resolves without re-checking the ACL. A
// blob that no longer resolves (e.g. a later takedown left it non-READY) is left
// with a nil Blob for the client to treat as unavailable.
func hydrateMedia(ctx context.Context, resolver Media, protos []*messagingpb.Message) error {
	renditions := mediaRenditions(protos)
	if len(renditions) == 0 {
		return nil
	}

	ids := make([]*blobpb.BlobId, 0, len(renditions))
	seen := make(map[string]struct{}, len(renditions))
	for _, r := range renditions {
		key := string(r.BlobId.Value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ids = append(ids, r.BlobId)
	}

	metadata, err := resolver.Resolve(ctx, ids)
	if err != nil {
		return err
	}
	for _, r := range renditions {
		if m, ok := metadata[string(r.BlobId.Value)]; ok {
			r.Blob = m
		}
	}
	return nil
}

// mediaRenditions returns every media rendition carrying a blob id across the
// messages — whether the media is a message body or a reply body — so its Blob can
// be hydrated in place.
func mediaRenditions(protos []*messagingpb.Message) []*messagingpb.MediaItemRendition {
	var out []*messagingpb.MediaItemRendition
	for _, msg := range protos {
		for _, c := range msg.Content {
			media := mediaOf(c)
			if media == nil {
				continue
			}
			for _, item := range media.Items {
				for _, r := range item.Renditions {
					if r.BlobId != nil {
						out = append(out, r)
					}
				}
			}
		}
	}
	return out
}

// mediaOf returns the MediaContent a content element carries — directly or as a
// reply body — or nil if there is none.
func mediaOf(c *messagingpb.Content) *messagingpb.MediaContent {
	switch t := c.Type.(type) {
	case *messagingpb.Content_Media:
		return t.Media
	case *messagingpb.Content_Reply:
		return mediaOf(t.Reply.Content[0])
	}
	return nil
}

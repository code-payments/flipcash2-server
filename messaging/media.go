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
// their rendition sets on read (ResolveRenditions). It is implemented by
// blob.Integration.
//
// ShareIntoChat returns blob.ErrBlobNotShareable when a referenced blob may not be
// attached (unknown, not owned by the sender, or not a READY original), in which
// case nothing is granted. ResolveRenditions performs no authorization — the caller
// has already gated on chat membership — and returns each original's full rendition
// set keyed by string(BlobId.Value), omitting unknown or not-yet-READY ids.
type Media interface {
	ShareIntoChat(ctx context.Context, sharerID *commonpb.UserId, chatID *commonpb.ChatId, blobIDs []*blobpb.BlobId) error
	ResolveRenditions(ctx context.Context, ids []*blobpb.BlobId) (map[string][]*blobpb.Rendition, error)
}

// validClientMedia reports whether a media content is one a client may author:
// at least one item, each one a client may reference (see
// validClientMediaItem), and a caption, if any, with no link previews (a
// preview belongs to a text message or a reply's text body; see
// clientAllowedContent). Ownership and readiness of the blobs are checked
// separately, when they are shared into the chat.
func validClientMedia(media *messagingpb.MediaContent) bool {
	if media == nil || len(media.Items) == 0 {
		return false
	}
	for _, item := range media.Items {
		if !validClientMediaItem(item) {
			return false
		}
	}
	return len(media.GetCaption().GetLinkPreviews()) == 0
}

// validClientMediaItem reports whether a media item is one a client may
// reference, as a media message's item or a link preview's image: exactly one
// ORIGINAL rendition identified by a blob id and no server-resolved metadata
// (the server fills Blob on read, never the client).
func validClientMediaItem(item *blobpb.Media) bool {
	if len(item.GetRenditions()) != 1 {
		return false
	}
	r := item.Renditions[0]
	return r.Role == blobpb.Rendition_ORIGINAL && r.BlobId != nil && r.Blob == nil
}

// shareMessageMedia grants the chat read access to the blobs referenced by
// content — a media message's items and any link preview's image alike — after
// verifying the sender owns each and it is a READY original. It reports
// denied=true when the media is not shareable (the caller returns its DENIED
// result) and a ready-to-return Internal error on an unexpected failure.
// Content referencing no blob is a no-op. It runs before the message is
// persisted and broadcast, so the grants are durable before any recipient can
// resolve them.
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

// mediaBlobIDs returns the blob ids a message references (see mediaItemsOf),
// or nil when it references none. It assumes the content already passed
// clientAllowedContent.
func mediaBlobIDs(content []*messagingpb.Content) []*blobpb.BlobId {
	if len(content) != 1 {
		return nil
	}
	var ids []*blobpb.BlobId
	for _, item := range mediaItemsOf(content[0]) {
		for _, r := range item.Renditions {
			if r.BlobId != nil {
				ids = append(ids, r.BlobId)
			}
		}
	}
	return ids
}

// hydrateMedia replaces each media item's rendition list with the full set the
// server derived — the ORIGINAL plus every derived rendition (display, thumbnail),
// each carrying freshly resolved metadata (mime type, size, dimensions, a
// short-lived download URL). A stored message carries only the ORIGINAL the client
// sent; this expands it on read. The caller has already gated on chat membership,
// and every blob in a stored message was granted to the chat when it was sent, so
// this resolves without re-checking the ACL. An item whose original no longer
// resolves (e.g. a later takedown left it non-READY) is left with its stored
// ORIGINAL rendition for the client to treat as unavailable.
func hydrateMedia(ctx context.Context, resolver Media, protos []*messagingpb.Message) error {
	items := mediaItems(protos)
	if len(items) == 0 {
		return nil
	}

	// Resolve by each item's ORIGINAL id, de-duplicated so a page that repeats an
	// image costs a single lookup.
	ids := make([]*blobpb.BlobId, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		id := originalBlobID(item)
		if id == nil {
			continue
		}
		key := string(id.Value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ids = append(ids, id)
	}

	resolved, err := resolver.ResolveRenditions(ctx, ids)
	if err != nil {
		return err
	}
	for _, item := range items {
		id := originalBlobID(item)
		if id == nil {
			continue
		}
		if renditions, ok := resolved[string(id.Value)]; ok {
			item.Renditions = renditions
		}
	}
	return nil
}

// mediaItems returns every media item across the messages (see mediaItemsOf)
// so each item's rendition list can be expanded in place.
func mediaItems(protos []*messagingpb.Message) []*blobpb.Media {
	var out []*blobpb.Media
	for _, msg := range protos {
		for _, c := range msg.Content {
			out = append(out, mediaItemsOf(c)...)
		}
	}
	return out
}

// originalBlobID returns the id of the item's ORIGINAL rendition — the handle the
// client sent and the one the server resolves the rendition set from — or nil if
// the item carries no ORIGINAL.
func originalBlobID(item *blobpb.Media) *blobpb.BlobId {
	for _, r := range item.GetRenditions() {
		if r.Role == blobpb.Rendition_ORIGINAL && r.BlobId != nil {
			return r.BlobId
		}
	}
	return nil
}

// mediaItemsOf returns every media item a content element carries, in place —
// a media message's items, the image of each of a text's link previews — looking
// through a reply to its body; nil if there is none. These are the items whose
// blobs a send shares into the chat and a read resolves, so the two agree on
// what a message's media is. Encrypted content carries none the server sees.
func mediaItemsOf(c *messagingpb.Content) []*blobpb.Media {
	switch t := c.Type.(type) {
	case *messagingpb.Content_Media:
		return t.Media.GetItems()
	case *messagingpb.Content_Text:
		var out []*blobpb.Media
		for _, p := range t.Text.GetLinkPreviews() {
			if p.GetImage() != nil {
				out = append(out, p.Image)
			}
		}
		return out
	case *messagingpb.Content_Reply:
		if len(t.Reply.GetContent()) != 1 {
			return nil
		}
		return mediaItemsOf(t.Reply.Content[0])
	}
	return nil
}

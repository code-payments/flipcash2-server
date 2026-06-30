package blob

import (
	"bytes"
	"context"
	"errors"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// ErrBlobNotShareable is returned by Media.ShareIntoChat when a referenced blob
// cannot be attached to a chat — it does not exist, is not owned by the sharer,
// is not a READY original, or is not an image. When it is returned none of the
// blobs are granted.
var ErrBlobNotShareable = errors.New("blob not shareable")

// Media is the blob-side integration the messaging service uses to attach blobs
// to chat messages: it validates and grants read access on send (ShareIntoChat),
// and resolves the blobs' metadata on read (Resolve).
type Media struct {
	blobs   Store
	storage ObjectStorage
	access  AccessStore
}

// NewMedia returns a Media backed by the given blob metadata store, object
// storage, and ACL store.
func NewMedia(blobs Store, storage ObjectStorage, access AccessStore) *Media {
	return &Media{blobs: blobs, storage: storage, access: access}
}

// ShareIntoChat attaches blobs to a chat: it verifies that sharerID owns every
// blob in blobIDs and that each is a READY image original, then grants the chat
// read access to each. It is all-or-nothing — if any blob fails validation
// nothing is granted and ErrBlobNotShareable is returned — and idempotent, so a
// re-sent message re-grants harmlessly. An empty blobIDs is a no-op.
//
// Only the owner may introduce a blob into a chat: a BlobId is a bearer
// capability, so without the ownership check a member could attach a blob they
// merely learned the id of. Only a READY original is servable and grantable
// (renditions inherit their original's grants), so a pending, rejected, or
// rendition blob is rejected; and chat media is images only today, so a
// non-image blob is rejected too.
func (m *Media) ShareIntoChat(ctx context.Context, sharerID *commonpb.UserId, chatID *commonpb.ChatId, blobIDs []*blobpb.BlobId) error {
	if len(blobIDs) == 0 {
		return nil
	}

	records, err := m.blobs.GetByIDs(ctx, blobIDs)
	if err != nil {
		return err
	}
	byID := make(map[string]*Blob, len(records))
	for _, b := range records {
		byID[string(b.ID.Value)] = b
	}

	// Validate every requested blob before granting any, so a single bad id in the
	// batch grants nothing.
	for _, id := range blobIDs {
		b, ok := byID[string(id.Value)]
		if !ok {
			return ErrBlobNotShareable
		}
		if b.Owner == nil || !bytes.Equal(b.Owner.Value, sharerID.Value) {
			return ErrBlobNotShareable
		}
		if b.Rendition != RenditionOriginal || b.ParentID != nil {
			return ErrBlobNotShareable
		}
		// Media in a chat is images only today; reject any other content kind (a
		// future video/file blob) rather than attaching something the chat cannot
		// render or serve.
		if !SupportedImageMimeTypes[b.MimeType] {
			return ErrBlobNotShareable
		}
		if b.State != StateReady {
			return ErrBlobNotShareable
		}
	}

	chat := PrincipalForChat(chatID)
	for _, id := range blobIDs {
		if err := m.access.Grant(ctx, &Grant{BlobID: id, Principal: chat, Permission: PermissionRead}); err != nil {
			return err
		}
	}
	return nil
}

// Resolve returns the server-authoritative metadata — mime type, size, image
// dimensions, and a freshly minted, short-lived download URL — for each READY
// blob among ids, keyed by string(BlobId.Value). Unknown or not-yet-READY ids are
// omitted. An empty input yields a nil map.
//
// It performs NO authorization. Callers must only pass ids they have already
// established the reader may see — e.g. blob ids drawn from a chat message the
// reader is a member of, which were granted to the chat when the message was sent.
func (m *Media) Resolve(ctx context.Context, ids []*blobpb.BlobId) (map[string]*blobpb.BlobMetadata, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	records, err := m.blobs.GetByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}

	out := make(map[string]*blobpb.BlobMetadata, len(records))
	for _, record := range records {
		// Only a READY blob is servable; a pending/rejected one has no metadata to
		// surface, so it is left for the client to treat as unavailable.
		if record.State != StateReady {
			continue
		}
		metadata, err := buildMetadata(ctx, m.storage, record)
		if err != nil {
			return nil, err
		}
		out[string(record.ID.Value)] = metadata
	}
	return out, nil
}

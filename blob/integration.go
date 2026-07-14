package blob

import (
	"bytes"
	"context"
	"errors"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// ErrBlobNotShareable is returned by Integration.ShareIntoChat when a referenced
// blob cannot be attached to a chat — it does not exist, is not owned by the
// sharer, is not a READY original, or is not an image. When it is returned none of
// the blobs are granted.
//
// It is deliberately coarse: a chat share is all-or-nothing over a batch, so
// there is no single blob whose specific failure could be reported. Surfaces that
// attach exactly one blob (SetAsProfilePicture) get the granular errors below
// instead, since they can act on the distinction.
var ErrBlobNotShareable = errors.New("blob not shareable")

// The granular reasons a blob cannot be attached to a surface. They exist because
// SetAsProfilePicture attaches a single blob and its caller must tell the client
// which of these happened — whether to retry (not ready) or upload again
// (rejected).
var (
	// ErrBlobNotFound means the blob does not exist OR is not owned by the caller.
	// The two are deliberately indistinguishable: a BlobId is a bearer capability,
	// so confirming that someone else's id exists would leak its existence.
	ErrBlobNotFound = errors.New("blob not found")

	// ErrBlobNotReady means the blob's bytes are still being processed. This is
	// transient — the caller may retry once the blob reaches READY.
	ErrBlobNotReady = errors.New("blob not ready")

	// ErrBlobRejected means the blob failed validation or moderation. This is
	// terminal for that id, since the bytes behind it are immutable: to try again
	// the client must upload a new blob.
	ErrBlobRejected = errors.New("blob rejected")

	// ErrBlobInvalid means the blob is READY but unusable on this surface — it is a
	// server-derived rendition rather than an original, or it is not an image.
	ErrBlobInvalid = errors.New("blob invalid")
)

// Integration is the surface other domains (messaging and profile today) use to
// attach blobs to a resource they own: it validates and grants read access when
// the blob is attached (ShareIntoChat, SetAsProfilePicture), and resolves the
// blobs' metadata on read (Resolve).
type Integration struct {
	blobs   Store
	storage ObjectStorage
	access  AccessStore
}

// NewIntegration returns an Integration backed by the given blob metadata store,
// object storage, and ACL store.
func NewIntegration(blobs Store, storage ObjectStorage, access AccessStore) *Integration {
	return &Integration{blobs: blobs, storage: storage, access: access}
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
func (i *Integration) ShareIntoChat(ctx context.Context, sharerID *commonpb.UserId, chatID *commonpb.ChatId, blobIDs []*blobpb.BlobId) error {
	if len(blobIDs) == 0 {
		return nil
	}

	records, err := i.blobs.GetByIDs(ctx, blobIDs)
	if err != nil {
		return err
	}
	byID := make(map[string]*Blob, len(records))
	for _, b := range records {
		byID[string(b.ID.Value)] = b
	}

	// Validate every requested blob before granting any, so a single bad id in the
	// batch grants nothing. The specific reason is collapsed into ErrBlobNotShareable
	// because the share covers a batch: there is no one blob to attribute it to.
	for _, id := range blobIDs {
		if err := validateAttachable(byID[string(id.Value)], sharerID, chatMedia); err != nil {
			return ErrBlobNotShareable
		}
	}

	chat := PrincipalForChat(chatID)
	for _, id := range blobIDs {
		if err := i.access.Grant(ctx, &Grant{BlobID: id, Principal: chat, Permission: PermissionRead}); err != nil {
			return err
		}
	}
	return nil
}

// SetAsProfilePicture attaches a blob to ownerID's public profile: it verifies
// that ownerID owns the blob and that it is a READY image original, then grants
// the profile read access to it. It is idempotent, so re-setting the same picture
// re-grants harmlessly.
//
// Granting the profile — rather than each viewer — is what makes a profile picture
// public: every caller is covered by the profile principal (see ProfileResolver),
// so exactly the blobs granted to it are readable through it. Grants are never
// revoked, so a picture stays readable through the profile once set.
//
// The ownership check matters for the same reason it does on a chat share: a
// BlobId is a bearer capability, so without it a user could publish a blob they
// merely learned the id of — including one that was never moderated for them.
//
// It returns one of ErrBlobNotFound, ErrBlobNotReady, ErrBlobRejected, or
// ErrBlobInvalid when the blob cannot back a picture. Nothing is granted then.
func (i *Integration) SetAsProfilePicture(ctx context.Context, ownerID *commonpb.UserId, blobID *blobpb.BlobId) error {
	records, err := i.blobs.GetByIDs(ctx, []*blobpb.BlobId{blobID})
	if err != nil {
		return err
	}

	var record *Blob
	for _, b := range records {
		if bytes.Equal(b.ID.Value, blobID.Value) {
			record = b
		}
	}
	if err := validateAttachable(record, ownerID, imagesOnly); err != nil {
		return err
	}

	return i.access.Grant(ctx, &Grant{
		BlobID:     blobID,
		Principal:  PrincipalForProfile(ownerID),
		Permission: PermissionRead,
	})
}

// mimeTypeFilter reports whether a surface accepts content of the given MIME type.
//
// Each attach point supplies its own, because what a surface can carry is a property
// OF THAT SURFACE, not of blob storage: a chat message can hold anything the client
// renders inline, while a profile picture is a picture by definition. The two
// coincide today — both images only — but keeping them separate is what stops a
// future "chats support video" from silently making a video an acceptable avatar.
type mimeTypeFilter func(mimeType string) bool

// imagesOnly accepts still images and nothing else. This is what a profile picture
// takes, permanently: widening chat media must not widen this.
func imagesOnly(mimeType string) bool {
	return SupportedImageMimeTypes[mimeType]
}

// chatMedia accepts what a chat message may carry. Images only today; when video or
// audio lands, its MIME types are admitted HERE — together with the rendition and
// moderation paths those kinds need — rather than by widening what counts as an
// image.
func chatMedia(mimeType string) bool {
	return SupportedImageMimeTypes[mimeType]
}

// validateAttachable reports whether record may be attached to a surface by owner,
// as one of the granular Err* sentinels. A nil record — an id with no blob behind
// it — is reported as not-found, as is a blob owned by someone else. Which content
// types the surface will take is its own decision, supplied as accepts.
//
// The remaining checks are shared by every attach point because they are what make a
// blob safe to publish at all: only a READY original is servable and grantable, since
// renditions inherit their original's grants and only a READY blob has passed
// moderation.
func validateAttachable(record *Blob, owner *commonpb.UserId, accepts mimeTypeFilter) error {
	if record == nil {
		return ErrBlobNotFound
	}
	if record.Owner == nil || !bytes.Equal(record.Owner.Value, owner.Value) {
		return ErrBlobNotFound
	}
	// Settle the blob's state before judging its contents: a rejected blob is
	// terminal (the client must upload again) and a pending one may yet become
	// usable, so neither is "invalid" — which is reserved for a blob that is READY
	// and still cannot back this surface.
	switch record.State {
	case StateReady:
	case StateRejected:
		return ErrBlobRejected
	default:
		return ErrBlobNotReady
	}

	if record.Rendition != RenditionOriginal || record.ParentID != nil {
		return ErrBlobInvalid
	}
	// Reject content this surface cannot render or serve rather than attaching it.
	if !accepts(record.MimeType) {
		return ErrBlobInvalid
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
func (i *Integration) Resolve(ctx context.Context, ids []*blobpb.BlobId) (map[string]*blobpb.BlobMetadata, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	records, err := i.blobs.GetByIDs(ctx, ids)
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
		metadata, err := buildMetadata(ctx, i.storage, record)
		if err != nil {
			return nil, err
		}
		out[string(record.ID.Value)] = metadata
	}
	return out, nil
}

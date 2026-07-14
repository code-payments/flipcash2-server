package profile

import (
	"context"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// Media is the blob storage surface the profile service depends on, satisfied by
// blob.Integration. It is declared here, rather than depending on the blob server
// directly, so the dependency runs one way: profile states what it needs of blob
// storage and the wiring supplies it.
type Media interface {
	// SetAsProfilePicture validates that ownerID owns the blob and that it can back
	// a picture, then grants ownerID's public profile read access to it. It returns
	// one of blob.ErrBlobNotFound, blob.ErrBlobNotReady, blob.ErrBlobRejected, or
	// blob.ErrBlobInvalid when the blob cannot be used.
	SetAsProfilePicture(ctx context.Context, ownerID *commonpb.UserId, blobID *blobpb.BlobId) error

	// Resolve returns the server-authoritative metadata — including a freshly minted,
	// short-lived download URL — for each READY blob among ids, keyed by
	// string(BlobId.Value). It performs no authorization.
	Resolve(ctx context.Context, ids []*blobpb.BlobId) (map[string]*blobpb.BlobMetadata, error)
}

// hydratePicture fills each of the picture's renditions with freshly resolved blob
// metadata (mime type, size, dimensions, and a short-lived download URL), so a
// client can fetch the bytes without a second round trip to GetBlobs.
//
// A profile picture is public, so this deliberately resolves without consulting the
// ACL — there is no reader for whom it would resolve differently. Clients still need
// GetBlobs (with an AccessContext naming the profile) to re-mint a URL once this one
// expires, since download URLs are not cacheable.
//
// A blob that no longer resolves (e.g. a takedown left it non-READY) is left with a
// nil Blob for the client to treat as unavailable, rather than failing the whole
// profile read.
// It is variadic and resolves every rendition of every picture in ONE batch, so
// hydrating a whole chat's member avatars costs a single Resolve rather than one per
// member.
func hydratePictures(ctx context.Context, resolver Media, pictures ...*blobpb.Media) error {
	var renditions []*blobpb.Rendition
	for _, picture := range pictures {
		for _, r := range picture.GetRenditions() {
			if r.BlobId != nil {
				renditions = append(renditions, r)
			}
		}
	}
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

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

	// ResolveRenditions returns each original's full rendition set — the ORIGINAL
	// plus every derived rendition, each with a freshly minted, short-lived download
	// URL — keyed by string(BlobId.Value). It performs no authorization.
	ResolveRenditions(ctx context.Context, ids []*blobpb.BlobId) (map[string][]*blobpb.Rendition, error)
}

// hydratePictures replaces each picture's rendition list with the full set the
// server derived — the ORIGINAL plus every derived rendition, each with freshly
// resolved metadata (mime type, size, dimensions, and a short-lived download URL) —
// so a client can fetch the bytes without a second round trip to GetBlobs. A stored
// picture carries only the ORIGINAL; this expands it on read.
//
// A profile picture is public, so this deliberately resolves without consulting the
// ACL — there is no reader for whom it would resolve differently. Clients still need
// GetBlobs (with an AccessContext naming the profile) to re-mint a URL once this one
// expires, since download URLs are not cacheable.
//
// A picture whose original no longer resolves (e.g. a takedown left it non-READY) is
// left with its stored ORIGINAL for the client to treat as unavailable, rather than
// failing the whole profile read. It is variadic and resolves every picture in ONE
// batch, so hydrating a whole chat's member avatars costs a single lookup rather
// than one per member. A nil picture (a user with none) is skipped.
func hydratePictures(ctx context.Context, resolver Media, pictures ...*blobpb.Media) error {
	ids := make([]*blobpb.BlobId, 0, len(pictures))
	seen := make(map[string]struct{}, len(pictures))
	for _, picture := range pictures {
		id := originalBlobID(picture)
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
	if len(ids) == 0 {
		return nil
	}

	resolved, err := resolver.ResolveRenditions(ctx, ids)
	if err != nil {
		return err
	}
	for _, picture := range pictures {
		id := originalBlobID(picture)
		if id == nil {
			continue
		}
		if renditions, ok := resolved[string(id.Value)]; ok {
			picture.Renditions = renditions
		}
	}
	return nil
}

// originalBlobID returns the id of the picture's ORIGINAL rendition — the handle
// the server resolves the rendition set from — or nil if there is no ORIGINAL (a
// nil or empty picture).
func originalBlobID(picture *blobpb.Media) *blobpb.BlobId {
	for _, r := range picture.GetRenditions() {
		if r.Role == blobpb.Rendition_ORIGINAL && r.BlobId != nil {
			return r.BlobId
		}
	}
	return nil
}

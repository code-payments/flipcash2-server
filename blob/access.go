package blob

import (
	"context"
	"errors"
	"fmt"
	"maps"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/chat"
)

// ErrInvalidGrant is returned by AccessStore methods when a grant — or the key
// used to look one up — is not well-formed: a missing blob id, an unknown
// principal type, an empty principal id, or an unknown permission.
var ErrInvalidGrant = errors.New("invalid grant")

// PrincipalType identifies the kind of subject a grant is made to. It is the
// extension point for "where a blob can be accessed from": each access surface
// (a chat today; a feed, a profile, a public link later) is a principal type
// with its own server-side membership check. The constants are persisted by
// value, so their numbering is significant and must not be reordered.
type PrincipalType int

const (
	PrincipalTypeUnknown PrincipalType = iota

	// PrincipalTypeUser is a single user, identified by their user id. A user
	// principal is resolved by direct identity, with no membership lookup.
	PrincipalTypeUser

	// PrincipalTypeChat is every current member of a chat, identified by the chat
	// id. A chat principal is resolved against live chat membership, so a single
	// grant covers the whole chat and stays correct as membership changes.
	PrincipalTypeChat

	// PrincipalTypeProfile is a user's public profile, identified by that user's
	// id. It covers every caller — a profile is public — so the grant alone gates
	// the read: exactly the blobs granted to the profile are readable through it.
	PrincipalTypeProfile
)

// Principal is the typed subject a grant is made to. ID is the type-specific
// identifier bytes: a user id for PrincipalTypeUser, a chat id for
// PrincipalTypeChat, and the profile owner's user id for PrincipalTypeProfile.
type Principal struct {
	Type PrincipalType
	ID   []byte
}

// PrincipalForUser returns the principal for a single user.
func PrincipalForUser(userID *commonpb.UserId) Principal {
	return Principal{Type: PrincipalTypeUser, ID: userID.Value}
}

// PrincipalForChat returns the principal for the members of a chat.
func PrincipalForChat(chatID *commonpb.ChatId) Principal {
	return Principal{Type: PrincipalTypeChat, ID: chatID.Value}
}

// PrincipalForProfile returns the principal for the public profile of a user.
func PrincipalForProfile(userID *commonpb.UserId) Principal {
	return Principal{Type: PrincipalTypeProfile, ID: userID.Value}
}

func (p Principal) validate() error {
	switch p.Type {
	case PrincipalTypeUser, PrincipalTypeChat, PrincipalTypeProfile:
	default:
		return fmt.Errorf("%w: unknown principal type %d", ErrInvalidGrant, p.Type)
	}
	if len(p.ID) == 0 {
		return fmt.Errorf("%w: empty principal id", ErrInvalidGrant)
	}
	return nil
}

// Permission is what a grant authorizes a principal to do with a blob. READ is
// the only permission today; WRITE, DELETE, and SHARE can be added without
// changing the store. Persisted by value, so the numbering is significant.
type Permission int

const (
	PermissionUnknown Permission = iota

	// PermissionRead authorizes resolving the blob and minting a download URL for
	// it — the access GetBlobs grants.
	PermissionRead
)

func (p Permission) validate() error {
	switch p {
	case PermissionRead:
		return nil
	default:
		return fmt.Errorf("%w: unknown permission %d", ErrInvalidGrant, p)
	}
}

// Grant authorizes a Principal to exercise a Permission on a blob. It is the ACL
// entry: its existence is the authorization and it carries no other state. A
// blob's Owner holds every permission implicitly and needs no grant. Grants are
// made against the ORIGINAL blob id; server-derived renditions inherit their
// original's grants.
type Grant struct {
	BlobID     *blobpb.BlobId
	Principal  Principal
	Permission Permission
}

// Validate reports whether the grant is well-formed. Stores call it before
// persisting or looking up a grant so a malformed principal or permission can
// never be written or silently miss.
func (g *Grant) Validate() error {
	if g == nil || g.BlobID == nil || len(g.BlobID.Value) == 0 {
		return fmt.Errorf("%w: missing blob id", ErrInvalidGrant)
	}
	if err := g.Principal.validate(); err != nil {
		return err
	}
	return g.Permission.validate()
}

// AccessStore persists blob ACL grants. A grant's existence authorizes its
// principal to exercise its permission on the blob; there is no other state.
//
// The store resolves a grant by its exact (blob, principal, permission) key and
// performs no membership resolution, so it never depends on the chat — or any
// other principal — subsystem. Authorizing a concrete user against a non-user
// principal (e.g. checking chat membership for a PrincipalTypeChat grant) is the
// caller's responsibility.
type AccessStore interface {
	// Grant records that the grant's principal may exercise its permission on its
	// blob. It is idempotent: re-granting the same (blob, principal, permission)
	// is a no-op. It returns ErrInvalidGrant if the grant is not well-formed.
	Grant(ctx context.Context, g *Grant) error

	// HasGrant reports whether a grant exists for the exact (blob, principal,
	// permission) triple. A missing grant is (false, nil), not an error. It
	// returns ErrInvalidGrant if the lookup key is not well-formed.
	HasGrant(ctx context.Context, blobID *blobpb.BlobId, p Principal, perm Permission) (bool, error)

	// Revoke removes a grant. It is idempotent: revoking a grant that does not
	// exist is a no-op. It returns ErrInvalidGrant if the key is not well-formed.
	Revoke(ctx context.Context, blobID *blobpb.BlobId, p Principal, perm Permission) error
}

// PrincipalResolver reports whether a concrete user is covered by a principal —
// that is, whether a grant made to that principal authorizes the user. A
// PrincipalTypeUser principal is covered by identity; a group principal such as
// PrincipalTypeChat is covered by live membership.
//
// It is the single extension point the read path consults to resolve a grant's
// principal, so supporting a new access surface is a matter of adding a
// PrincipalType and teaching the resolver about it — the server does not change.
// Defining it here, rather than depending on the chat (or any future scope)
// subsystem, keeps blob decoupled from what backs each principal type; the
// wiring supplies a resolver that knows how to resolve them (e.g. mapping a
// PrincipalTypeChat to chat membership).
type PrincipalResolver interface {
	Covers(ctx context.Context, principal Principal, user *commonpb.UserId) (bool, error)
}

// CompositeResolver is the top-level PrincipalResolver: it routes each principal
// to the domain resolver registered for its type. The server holds one of these,
// so adding an access surface is a matter of registering its resolver here — the
// read path does not change. A principal whose type has no registered resolver
// is not covered, mirroring how an unknown access scope is treated.
type CompositeResolver struct {
	byType map[PrincipalType]PrincipalResolver
}

// NewCompositeResolver returns a CompositeResolver that dispatches a principal to
// the resolver registered for its PrincipalType. The routing table is copied, so
// later mutation of the passed map does not affect the resolver.
func NewCompositeResolver(byType map[PrincipalType]PrincipalResolver) PrincipalResolver {
	routes := make(map[PrincipalType]PrincipalResolver, len(byType))
	maps.Copy(routes, byType)
	return &CompositeResolver{byType: routes}
}

// Covers routes principal to the resolver registered for its type and returns
// that resolver's decision. A principal whose type has no registered resolver is
// not covered (false, nil) — an unroutable scope authorizes nothing.
func (r *CompositeResolver) Covers(ctx context.Context, principal Principal, user *commonpb.UserId) (bool, error) {
	resolver, ok := r.byType[principal.Type]
	if !ok {
		return false, nil
	}
	return resolver.Covers(ctx, principal, user)
}

// ChatResolver is the PrincipalResolver for chat-scoped grants: a user is
// covered by a PrincipalTypeChat principal iff they are a member of the chat the
// principal identifies. It resolves membership directly against the chat store.
type ChatResolver struct {
	chats chat.Store
}

// NewChatResolver returns a ChatResolver backed by the given chat store.
func NewChatResolver(chats chat.Store) PrincipalResolver {
	return &ChatResolver{chats: chats}
}

// Covers reports whether user is covered by principal. A PrincipalTypeChat
// principal is covered iff user is a member of the chat identified by the
// principal id. Any other principal type is outside this resolver's scope — it
// is chat-only — so it reports not-covered rather than guessing; supporting
// another scope is a matter of layering a different resolver, not changing this
// one.
func (r *ChatResolver) Covers(ctx context.Context, principal Principal, user *commonpb.UserId) (bool, error) {
	switch principal.Type {
	case PrincipalTypeChat:
		return r.chats.IsMember(ctx, &commonpb.ChatId{Value: principal.ID}, user)
	default:
		return false, nil
	}
}

// ProfileResolver is the PrincipalResolver for profile-scoped grants: a profile
// is public, so every caller is covered by a PrincipalTypeProfile principal —
// there is no membership to resolve, and it therefore needs no store.
//
// Coverage being universal is precisely why the grant carries the whole decision
// here: only the blobs granted to a profile resolve through it, so a picture
// stops being readable the moment it is superseded and its grant revoked. Read
// authorization still requires both halves (see Server.canRead), so this is not a
// blanket authorization of any blob id.
type ProfileResolver struct{}

// NewProfileResolver returns a ProfileResolver.
func NewProfileResolver() PrincipalResolver {
	return &ProfileResolver{}
}

// Covers reports whether user is covered by principal. Every user is covered by a
// PrincipalTypeProfile principal, since a profile is public. Any other principal
// type is outside this resolver's scope, so it reports not-covered rather than
// guessing.
func (r *ProfileResolver) Covers(_ context.Context, principal Principal, _ *commonpb.UserId) (bool, error) {
	switch principal.Type {
	case PrincipalTypeProfile:
		return true, nil
	default:
		return false, nil
	}
}

package profile

import (
	"context"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// FirstUsernameHandler is told when a user is given a handle while holding
// none: claimed with SetUsername, or assigned by default with a display name
// (see SetDisplayNameWithDefaultUsername). A handle is never released without
// another taking its place, so this is the first handle the user ever holds.
//
// It is called on the request path, after the handle is written, and cannot
// fail the request: an implementation returns promptly and does its work
// elsewhere. It may be called more than once for a user, as when two claims by
// them race, so what it does is idempotent per user. username is in canonical
// form, as held.
type FirstUsernameHandler interface {
	OnFirstUsername(ctx context.Context, userID *commonpb.UserId, username string)
}

// ServerOption configures a Server beyond its dependencies.
type ServerOption func(*Server)

// WithFirstUsernameHandler has the Server tell h about every user's first
// handle (see FirstUsernameHandler). Nil, the default, tells no one.
func WithFirstUsernameHandler(h FirstUsernameHandler) ServerOption {
	return func(s *Server) { s.firstUsername = h }
}

// onFirstUsername tells the handler, if there is one, that userID was given
// username while holding none.
func (s *Server) onFirstUsername(ctx context.Context, userID *commonpb.UserId, username string) {
	if s.firstUsername != nil {
		s.firstUsername.OnFirstUsername(ctx, userID, username)
	}
}

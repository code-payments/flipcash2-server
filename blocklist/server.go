package blocklist

import (
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	blocklistpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blocklist/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
)

// maxGetBlocklistPageSize bounds a single GetBlocklist page. It matches the
// max_items on GetBlocklistResponse.blocked_users, so a page never exceeds what
// the response is allowed to carry.
const maxGetBlocklistPageSize = 100

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts  account.Store
	blocklist Store

	blocklistpb.UnimplementedBlocklistServer
}

func NewServer(log *zap.Logger, authz auth.Authorizer, accounts account.Store, blocklist Store) *Server {
	return &Server{
		log:       log,
		authz:     authz,
		accounts:  accounts,
		blocklist: blocklist,
	}
}

func (s *Server) BlockUser(ctx context.Context, req *blocklistpb.BlockUserRequest) (*blocklistpb.BlockUserResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("blocked_user_id", model.UserIDString(req.UserId)),
	)

	// A user cannot block themselves. Checked before existence: the caller is
	// authenticated, so self is known to exist, and this is the more specific
	// result.
	if bytes.Equal(userID.Value, req.UserId.Value) {
		return &blocklistpb.BlockUserResponse{Result: blocklistpb.BlockUserResponse_CANNOT_BLOCK_SELF}, nil
	}

	exists, err := s.userExists(ctx, req.UserId)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking whether user to block exists")
		return nil, status.Error(codes.Internal, "")
	}
	if !exists {
		return &blocklistpb.BlockUserResponse{Result: blocklistpb.BlockUserResponse_USER_NOT_FOUND}, nil
	}

	// Idempotent: blocking an already-blocked user is a no-op and still OK.
	if _, err := s.blocklist.Block(ctx, userID, req.UserId, time.Now().UTC()); err != nil {
		log.With(zap.Error(err)).Warn("Failure blocking user")
		return nil, status.Error(codes.Internal, "")
	}

	return &blocklistpb.BlockUserResponse{Result: blocklistpb.BlockUserResponse_OK}, nil
}

func (s *Server) UnblockUser(ctx context.Context, req *blocklistpb.UnblockUserRequest) (*blocklistpb.UnblockUserResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("blocked_user_id", model.UserIDString(req.UserId)),
	)

	// Idempotent: unblocking a user who isn't blocked (including oneself, who can
	// never be blocked) is a no-op and still OK. Existence is not checked — a
	// user can always clear an entry from their own list.
	if _, err := s.blocklist.Unblock(ctx, userID, req.UserId); err != nil {
		log.With(zap.Error(err)).Warn("Failure unblocking user")
		return nil, status.Error(codes.Internal, "")
	}

	return &blocklistpb.UnblockUserResponse{Result: blocklistpb.UnblockUserResponse_OK}, nil
}

func (s *Server) IsBlocked(ctx context.Context, req *blocklistpb.IsBlockedRequest) (*blocklistpb.IsBlockedResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("blocked_user_id", model.UserIDString(req.UserId)),
	)

	blocked, err := s.blocklist.IsBlocked(ctx, userID, req.UserId)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking whether user is blocked")
		return nil, status.Error(codes.Internal, "")
	}

	return &blocklistpb.IsBlockedResponse{
		Result:    blocklistpb.IsBlockedResponse_OK,
		IsBlocked: blocked,
	}, nil
}

func (s *Server) GetBlocklist(ctx context.Context, req *blocklistpb.GetBlocklistRequest) (*blocklistpb.GetBlocklistResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	limit := maxGetBlocklistPageSize
	if pageSize := req.GetQueryOptions().GetPageSize(); pageSize > 0 && int(pageSize) < limit {
		limit = int(pageSize)
	}

	// The cursor is carried opaquely in the paging token. blocked_at is immutable
	// once written, so the cursor alone (no snapshot watermark) is enough to page
	// consistently.
	var cursor *Cursor
	if token := req.GetQueryOptions().GetPagingToken(); token != nil {
		c, ok := decodeBlocklistToken(token)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid paging token")
		}
		cursor = c
	}

	// Fetch one extra to detect whether a further page remains.
	entries, err := s.blocklist.GetBlocklistPage(ctx, userID, cursor, limit+1)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting blocklist")
		return nil, status.Error(codes.Internal, "")
	}

	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}

	blockedUsers := make([]*blocklistpb.BlockedUser, len(entries))
	for i, e := range entries {
		blockedUsers[i] = e.ToProto()
	}

	resp := &blocklistpb.GetBlocklistResponse{
		Result:       blocklistpb.GetBlocklistResponse_OK,
		BlockedUsers: blockedUsers,
		HasMore:      hasMore,
	}
	// Advance the cursor to the last returned entry. An empty page has nothing to
	// resume from, so the token is omitted.
	if n := len(entries); n > 0 {
		last := entries[n-1]
		resp.PagingToken = encodeBlocklistToken(&Cursor{BlockedAt: last.BlockedAt, UserID: last.UserID})
	}
	return resp, nil
}

// userExists reports whether userID belongs to a known account. A user exists
// once it has at least one bound public key, mirroring how the resolver treats a
// user with no keys as unresolvable.
func (s *Server) userExists(ctx context.Context, userID *commonpb.UserId) (bool, error) {
	pubKeys, err := s.accounts.GetPubKeys(ctx, userID)
	if err != nil {
		return false, err
	}
	return len(pubKeys) > 0, nil
}

// blocklistTokenLen is the byte length of an encoded GetBlocklist paging token:
// the cursor's blocked_at as a big-endian int64 unix-nanos, followed by the
// cursor's user ID.
const blocklistTokenLen = 8 + model.UserIDSize

// encodeBlocklistToken serializes the resume cursor into an opaque paging token
// for the client to echo on the next request.
func encodeBlocklistToken(cursor *Cursor) *commonpb.PagingToken {
	buf := make([]byte, blocklistTokenLen)
	binary.BigEndian.PutUint64(buf[0:8], uint64(cursor.BlockedAt.UnixNano()))
	copy(buf[8:8+model.UserIDSize], cursor.UserID.Value)
	return &commonpb.PagingToken{Value: buf}
}

// decodeBlocklistToken reverses encodeBlocklistToken. ok is false if the token
// is nil or not the expected length (e.g. a client-fabricated value).
func decodeBlocklistToken(token *commonpb.PagingToken) (*Cursor, bool) {
	if token == nil || len(token.Value) != blocklistTokenLen {
		return nil, false
	}
	return &Cursor{
		BlockedAt: time.Unix(0, int64(binary.BigEndian.Uint64(token.Value[0:8]))).UTC(),
		UserID:    &commonpb.UserId{Value: append([]byte(nil), token.Value[8:8+model.UserIDSize]...)},
	}, true
}

package blocklist

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	blocklistpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blocklist/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// BlockedUser is one entry in a user's blocklist: a blocked user and the time
// they were blocked.
//
// It holds only the state owned by the blocklist domain. The richer profile of
// the blocked user (display name, picture, ...) lives in other domains and is
// not carried here; the proto surface intentionally exposes only the identity
// and the blocked-at timestamp.
type BlockedUser struct {
	UserID    *commonpb.UserId
	BlockedAt time.Time
}

// Clone returns a deep copy of the entry.
func (b *BlockedUser) Clone() *BlockedUser {
	return &BlockedUser{
		UserID:    &commonpb.UserId{Value: append([]byte(nil), b.UserID.Value...)},
		BlockedAt: b.BlockedAt,
	}
}

// ToProto projects the entry onto a blocklistpb.BlockedUser.
func (b *BlockedUser) ToProto() *blocklistpb.BlockedUser {
	return &blocklistpb.BlockedUser{
		UserId:    &commonpb.UserId{Value: append([]byte(nil), b.UserID.Value...)},
		BlockedAt: timestamppb.New(b.BlockedAt),
	}
}

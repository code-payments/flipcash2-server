package chat

import (
	"bytes"
	"slices"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// StoreOption configures a Store beyond its backing storage. Every Store
// implementation takes these at construction and applies them through
// StoreOptions, so they mean the same thing in every backend.
type StoreOption func(*StoreOptions)

// StoreOptions is what a Store's options configured it with.
type StoreOptions struct {
	// excludedFromFeed are the users every DM is created excluding from the
	// feed (see WithExcludedFromFeed), each named once.
	excludedFromFeed []*commonpb.UserId
}

// NewStoreOptions applies opts, in order, to empty options.
func NewStoreOptions(opts ...StoreOption) StoreOptions {
	var o StoreOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// WithExcludedFromFeed names users that every DM created with them excludes
// from the feed (see Store.PutChat). It is for an account in a DM with every
// user whose feed nobody reads, today the Flipcash team account (see
// flipcashteam): a DM created with it in the feed puts every one of its DMs
// under one feed key ordered by time, which every creation and every message
// then writes at the same end of. Configuring it on the store is what keeps
// any path that creates a DM from forgetting to. Nil user IDs are ignored, so
// a parent with no team account configured may pass one, and a user named
// more than once is excluded once. Options given more than once add up.
func WithExcludedFromFeed(userIDs ...*commonpb.UserId) StoreOption {
	return func(o *StoreOptions) {
		for _, userID := range userIDs {
			if userID == nil || containsUser(o.excludedFromFeed, userID) {
				continue
			}
			o.excludedFromFeed = append(o.excludedFromFeed, &commonpb.UserId{Value: bytes.Clone(userID.Value)})
		}
	}
}

// ExcludedFromFeed returns the members a DM being created is to exclude from
// the feed: those of c's members the options name, each once, in the order c
// lists them. It is empty when there are none, and for every group, which
// keeps no per-member feed copies to exclude anyone from.
func (o StoreOptions) ExcludedFromFeed(c *Chat) []*commonpb.UserId {
	if len(o.excludedFromFeed) == 0 || IsGroupChatID(c.ID) {
		return nil
	}
	var excluded []*commonpb.UserId
	for _, member := range c.Members {
		if containsUser(o.excludedFromFeed, member) && !containsUser(excluded, member) {
			excluded = append(excluded, member)
		}
	}
	return excluded
}

// containsUser reports whether userIDs names userID.
func containsUser(userIDs []*commonpb.UserId, userID *commonpb.UserId) bool {
	return slices.ContainsFunc(userIDs, func(u *commonpb.UserId) bool {
		return bytes.Equal(u.Value, userID.Value)
	})
}

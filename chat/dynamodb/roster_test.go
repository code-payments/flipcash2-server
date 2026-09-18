package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
)

// TestRosterAgrees pins the invariant GetGroupRoster verifies a whole read
// against: a transition that lands between the summary and the rows always
// leaves a mark — a join, a row stamped above the summary's version; a leave,
// one fewer joined row than the summary counts — and a read with neither mark
// is the roster at exactly that version.
func TestRosterAgrees(t *testing.T) {
	member := func(version uint64) chat.GroupMember {
		return chat.GroupMember{UserID: model.MustGenerateUserID(), Version: version}
	}
	summary := chat.RosterSummary{MemberCount: 3, Version: 7}

	// Quiet: three rows, none newer than the summary (creation rows read as
	// zero).
	require.True(t, rosterAgrees(summary, []chat.GroupMember{member(0), member(5), member(7)}))

	// A join straddled the read: its row is counted but stamped after.
	require.False(t, rosterAgrees(summary, []chat.GroupMember{member(0), member(5), member(8)}))
	// A join and a leave straddled it: the count balances, the stamp does not.
	require.False(t, rosterAgrees(summary, []chat.GroupMember{member(0), member(8), member(7)}))

	// A leave straddled the read: the summary still counts the tombstoned row.
	require.False(t, rosterAgrees(summary, []chat.GroupMember{member(0), member(5)}))

	// An empty roster agrees with a zero count at any version.
	require.True(t, rosterAgrees(chat.RosterSummary{MemberCount: 0, Version: 4}, nil))
}

package dynamodb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/poll"
)

// TestCmidTTL pins the two floors under how long a client message ID's
// marker is kept (see cmidTTL).
func TestCmidTTL(t *testing.T) {
	// messagingpb.ClientMessageId promises clients that the server recognizes
	// an ID for at least 30 days after its send: a sender's retry replays
	// their message, and anyone else's send carrying it is DENIED.
	require.GreaterOrEqual(t, cmidTTL, 30*24*time.Hour)

	// The marker outlives every retry of a poll's one send that
	// poll.PrepareSend lets through, which is what keeps a poll to one
	// message.
	require.GreaterOrEqual(t, cmidTTL, poll.SendRetryWindow)
}

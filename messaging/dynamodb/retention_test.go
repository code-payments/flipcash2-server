package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/poll"
)

// TestCmidTTL_CoversPollSendRetries pins that a client message ID's marker
// outlives every retry of a poll's one send that poll.PrepareSend lets
// through, which is what keeps a poll to one message.
func TestCmidTTL_CoversPollSendRetries(t *testing.T) {
	require.GreaterOrEqual(t, cmidTTL, poll.SendRetryWindow)
}

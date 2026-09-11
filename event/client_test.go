package event

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ocp_retry "github.com/code-payments/ocp-server/retry"
)

// TestForwardRetryStrategies pins which forwarding failures are retried: only
// a transport-level Unavailable, where the receiver provably never ran the
// delivery. An expired deadline is ambiguous and must not be re-sent, or every
// stream on the receiver could see the event twice.
func TestForwardRetryStrategies(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		attempts uint
	}{
		{"unavailable is retried to the limit", status.Error(codes.Unavailable, "connection refused"), 3},
		{"deadline is not retried", status.Error(codes.DeadlineExceeded, "context deadline exceeded"), 1},
		{"cancellation is not retried", status.Error(codes.Canceled, ""), 1},
		{"a denied result is not retried", errors.New("rpc forward result DENIED"), 1},
		{"an internal failure is not retried", status.Error(codes.Internal, ""), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls uint
			attempts, err := ocp_retry.Retry(func() error {
				calls++
				return tc.err
			}, forwardRetryStrategies()...)
			require.Error(t, err)
			require.Equal(t, tc.attempts, attempts)
			require.Equal(t, tc.attempts, calls)
		})
	}

	// Success on a later attempt still returns cleanly.
	var calls uint
	attempts, err := ocp_retry.Retry(func() error {
		calls++
		if calls < 2 {
			return status.Error(codes.Unavailable, "")
		}
		return nil
	}, forwardRetryStrategies()...)
	require.NoError(t, err)
	require.Equal(t, uint(2), attempts)
}

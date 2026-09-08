package internalrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/code-payments/flipcash2-server/cluster"
	ocp_headers "github.com/code-payments/ocp-server/grpc/headers"
)

func TestPool_ConnReuse(t *testing.T) {
	pool := NewPool(zap.NewNop())
	defer pool.Close()

	// grpc.NewClient dials lazily, so pooling is observable without a peer.
	conn1, err := pool.Conn("10.0.0.1:8085")
	require.NoError(t, err)
	conn2, err := pool.Conn("10.0.0.1:8085")
	require.NoError(t, err)
	require.Same(t, conn1, conn2)

	other, err := pool.Conn("10.0.0.2:8085")
	require.NoError(t, err)
	require.NotSame(t, conn1, other)
}

func TestPool_ClosedConnRefused(t *testing.T) {
	pool := NewPool(zap.NewNop())
	_, err := pool.Conn("10.0.0.1:8085")
	require.NoError(t, err)

	pool.Close()

	// A conn created after Close would be tracked by nothing: refuse it.
	_, err = pool.Conn("10.0.0.2:8085")
	require.ErrorIs(t, err, ErrPoolClosed)

	pool.Close() // Idempotent.
}

// ctxWithAPIKeyHeader builds a context whose ASCII header map carries the key,
// as an authenticated internal RPC's would; withoutHeader mimics an
// unauthenticated caller that set no header at all (which reads back as "").
func ctxWithAPIKeyHeader(t *testing.T, apiKey string) context.Context {
	ctx, err := ocp_headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)
	require.NoError(t, ocp_headers.SetASCIIHeader(ctx, APIKeyHeaderName, apiKey))
	return ctx
}

func TestAuthenticator_FailsClosed(t *testing.T) {
	noHeaderCtx, err := ocp_headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)

	// A misconfigured empty key (unset config value) must not admit callers
	// presenting no header: the absent header reads back as "".
	misconfigured := NewAuthenticator("")
	allowed, err := misconfigured.Allow(noHeaderCtx)
	require.NoError(t, err)
	require.False(t, allowed)
	allowed, err = misconfigured.Allow(ctxWithAPIKeyHeader(t, ""))
	require.NoError(t, err)
	require.False(t, allowed)

	auth := NewAuthenticator("", "real-key")

	allowed, err = auth.Allow(ctxWithAPIKeyHeader(t, "real-key"))
	require.NoError(t, err)
	require.True(t, allowed)

	for _, presented := range []string{"", "wrong-key"} {
		allowed, err = auth.Allow(ctxWithAPIKeyHeader(t, presented))
		require.NoError(t, err)
		require.False(t, allowed)
	}
	allowed, err = auth.Allow(noHeaderCtx)
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestRedirectAddress(t *testing.T) {
	address, ok := RedirectAddress(&cluster.NotOwnerError{
		Redirect: &cluster.Member{InstanceID: "instance-a", Address: "10.0.0.1:8085"},
	})
	require.True(t, ok)
	require.Equal(t, "10.0.0.1:8085", address)

	// A redirect-less NotOwnerError means no healthy owner is known: fall
	// back, don't forward.
	_, ok = RedirectAddress(&cluster.NotOwnerError{})
	require.False(t, ok)

	_, ok = RedirectAddress(errors.New("unrelated"))
	require.False(t, ok)
	_, ok = RedirectAddress(nil)
	require.False(t, ok)
}

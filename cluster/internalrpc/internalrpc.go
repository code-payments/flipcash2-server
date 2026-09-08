// Package internalrpc is the server-to-server RPC plumbing for the fleet: a
// per-peer connection pool, API-key authentication for internal-only
// endpoints, and helpers for forwarding a request to the member that owns its
// key. It carries no domain knowledge — event forwarding and cluster ownership
// forwarding ride the same machinery.
package internalrpc

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/code-payments/flipcash2-server/cluster"
	ocp_headers "github.com/code-payments/ocp-server/grpc/headers"
	ocp_validation "github.com/code-payments/ocp-server/grpc/protobuf/validation"
)

// APIKeyHeaderName carries the internal RPC API key. Internal endpoints are
// reachable on the public gRPC surface, so every internal RPC must present a
// key and every internal handler must verify one.
const APIKeyHeaderName = "x-flipcash-internal-rpc-api-key"

const cleanupInterval = time.Minute

// ErrPoolClosed is returned by Conn after Close: a connection created past
// Close would never be closed or reaped.
var ErrPoolClosed = errors.New("internal rpc connection pool is closed")

// Pool caches one gRPC client connection per peer address. Connections are
// created lazily, shared by all callers, and reaped only once they report
// broken (peer addresses die with their instances, so idle-but-healthy conns
// are cheap and worth keeping).
type Pool struct {
	log *zap.Logger

	mu     sync.RWMutex
	conns  map[string]*grpc.ClientConn
	closed bool

	closeOnce sync.Once
	done      chan struct{}
}

// NewPool creates a connection pool and starts its background reaper.
func NewPool(log *zap.Logger) *Pool {
	p := &Pool{
		log:   log,
		conns: make(map[string]*grpc.ClientConn),
		done:  make(chan struct{}),
	}
	go p.periodicallyCleanupConns()
	return p
}

// Conn returns the pooled connection for the address, creating it if needed.
func (p *Pool) Conn(address string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	existing, ok := p.conns[address]
	p.mu.RUnlock()
	if ok {
		return existing, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Checked under the same lock as the insert: a conn created after Close
	// would be tracked by nothing and never closed or reaped.
	if p.closed {
		return nil, ErrPoolClosed
	}

	existing, ok = p.conns[address]
	if ok {
		return existing, nil
	}

	conn, err := grpc.NewClient(
		address,

		grpc.WithTransportCredentials(insecure.NewCredentials()),

		grpc.WithChainUnaryInterceptor(
			ocp_validation.UnaryClientInterceptor(p.log),
			ocp_headers.UnaryClientInterceptor(),
		),
		grpc.WithChainStreamInterceptor(
			ocp_validation.StreamClientInterceptor(p.log),
			ocp_headers.StreamClientInterceptor(),
		),
	)
	if err != nil {
		return nil, err
	}

	p.conns[address] = conn
	return conn, nil
}

// Close shuts down every pooled connection and stops the reaper. Subsequent
// (or racing) Conn calls return ErrPoolClosed.
func (p *Pool) Close() {
	p.closeOnce.Do(func() {
		close(p.done)

		p.mu.Lock()
		defer p.mu.Unlock()
		p.closed = true
		for address, conn := range p.conns {
			conn.Close()
			delete(p.conns, address)
		}
	})
}

func (p *Pool) periodicallyCleanupConns() {
	for {
		select {
		case <-p.done:
			return
		case <-time.After(cleanupInterval):
		}

		p.mu.Lock()
		for address, conn := range p.conns {
			switch conn.GetState() {
			case connectivity.TransientFailure, connectivity.Shutdown:
				conn.Close()
				delete(p.conns, address)
			}
		}
		p.mu.Unlock()
	}
}

// WithAPIKey returns ctx with headers initialized (if they aren't already) and
// the internal API key header set — the client side of internal RPC auth.
func WithAPIKey(ctx context.Context, apiKey string) (context.Context, error) {
	if !ocp_headers.AreHeadersInitialized(ctx) {
		var err error
		ctx, err = ocp_headers.ContextWithHeaders(ctx)
		if err != nil {
			return nil, err
		}
	}
	if err := ocp_headers.SetASCIIHeader(ctx, APIKeyHeaderName, apiKey); err != nil {
		return nil, err
	}
	return ctx, nil
}

// Authenticator verifies internal API keys on incoming RPCs. It holds the set
// of currently accepted keys, which may exceed one so keys can be rotated
// across a deploy without rejecting in-flight traffic.
type Authenticator struct {
	allowed map[string]struct{}
}

// NewAuthenticator creates an authenticator accepting the given keys. Empty
// keys are discarded: an absent header reads back as the empty string, so
// accepting "" (e.g. from an unset config value) would leave internal
// endpoints open to unauthenticated callers on the public gRPC surface. An
// authenticator left with no keys denies everything — a misconfiguration
// fails closed and loud, never open.
func NewAuthenticator(apiKeys ...string) *Authenticator {
	allowed := make(map[string]struct{}, len(apiKeys))
	for _, key := range apiKeys {
		if key == "" {
			continue
		}
		allowed[key] = struct{}{}
	}
	return &Authenticator{allowed: allowed}
}

// Allow reports whether the RPC presented an accepted internal API key. A
// malformed header is an error; an absent header (which reads as "") or a
// well-formed but unaccepted key is (false, nil) so handlers can return their
// endpoint's denial shape.
func (a *Authenticator) Allow(ctx context.Context) (bool, error) {
	headerValue, err := ocp_headers.GetASCIIHeaderByName(ctx, APIKeyHeaderName)
	if err != nil {
		return false, err
	}
	if headerValue == "" {
		return false, nil
	}
	_, ok := a.allowed[headerValue]
	return ok, nil
}

// RedirectAddress extracts the forwarding target from an Ownership.Do error.
// ok is true only when the error is a NotOwnerError naming a member: forward
// the request to the returned address (and report a failed forward via
// Ownership.NoteUnreachable). ok is false for a redirect-less NotOwnerError —
// no healthy owner is known, so use the store-serialized fallback — and for
// every other error.
func RedirectAddress(err error) (address string, ok bool) {
	var notOwner *cluster.NotOwnerError
	if !errors.As(err, &notOwner) || notOwner.Redirect == nil {
		return "", false
	}
	return notOwner.Redirect.Address, true
}

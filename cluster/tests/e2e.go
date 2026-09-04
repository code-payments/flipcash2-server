package tests

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	"github.com/code-payments/flipcash2-server/cluster"
	"github.com/code-payments/flipcash2-server/cluster/internalrpc"
	ocp_headers "github.com/code-payments/ocp-server/grpc/headers"
	ocp_testutil "github.com/code-payments/ocp-server/testutil"
)

// The end-to-end suite runs multiple real gRPC servers, each carrying the full
// cluster stack (membership, router, ownership, internal-RPC forwarding)
// around a minimal owner-serialized counter service — a stand-in for a phase-4
// consumer like chat. Requests enter at any server: the owner serves, a
// non-owner forwards one hop to the claim holder with internal API-key auth,
// and when no healthy owner is known the request falls back to the shared
// durable store, which serializes on its own (the store-is-the-availability-
// floor invariant). The suite drives happy-path traffic, a graceful deploy
// with traffic in flight, an ungraceful crash, and degraded mode, asserting
// above all that no acknowledged write is ever lost.

const (
	e2eCounterMethod   = "/flipcash.cluster.test.Counter/Apply"
	e2eForwardedHeader = "x-flipcash-cluster-forwarded"
	e2eAPIKey          = "e2e-internal-api-key"
)

// RunE2ETests runs the end-to-end suite against a cluster.Store
// implementation, calling teardown between tests.
func RunE2ETests(t *testing.T, s cluster.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s cluster.Store){
		testE2EHappyPath,
		testE2EGracefulDeploy,
		testE2ECrashFailover,
		testE2EDegradedFallback,
	} {
		tf(t, s)
		teardown()
	}
}

// durableStore simulates the domain's own serializing data store (DynamoDB in
// production): correct under concurrent writers with no owner at all.
// Ownership is the accelerator above it, never the gatekeeper.
type durableStore struct {
	mu     sync.Mutex
	counts map[string]uint64
}

func newDurableStore() *durableStore {
	return &durableStore{counts: make(map[string]uint64)}
}

func (d *durableStore) increment(key string) uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.counts[key]++
	return d.counts[key]
}

func (d *durableStore) get(key string) uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.counts[key]
}

// counterService is the test consumer: Apply(key) increments the key's counter
// under ownership when possible, forwarding or degrading otherwise. The
// response's Hops trace which servers touched the request (a "fallback@"
// prefix marks the degraded path) and Nonce carries the counter value.
type counterService struct {
	name      string
	ownership *cluster.Ownership
	pool      *internalrpc.Pool
	auth      *internalrpc.Authenticator
	durable   *durableStore

	mu  sync.Mutex
	hot map[string]uint64
}

type e2eCounterServer interface {
	Apply(ctx context.Context, req *commonpb.UserId) (*eventpb.TestEvent, error)
}

func (c *counterService) Apply(ctx context.Context, req *commonpb.UserId) (*eventpb.TestEvent, error) {
	forwarded := c.isForwarded(ctx)
	if forwarded {
		// Internal hop: enforce fleet auth exactly as a production internal
		// endpoint must.
		if allowed, err := c.auth.Allow(ctx); err != nil || !allowed {
			return nil, status.Error(codes.PermissionDenied, "invalid internal RPC API key")
		}
	}

	key := req.Value
	keyStr := string(key)

	var resp *eventpb.TestEvent
	err := c.ownership.Do(ctx, testNamespace, key, func(_ context.Context, claim *cluster.Claim) error {
		// Owner path: write through the durable store (the store stays the
		// serializer; ownership buys locality), tracking the hot cache warmed
		// by OnAcquired.
		v := c.durable.increment(keyStr)
		c.mu.Lock()
		c.hot[keyStr] = v
		c.mu.Unlock()
		resp = &eventpb.TestEvent{Hops: []string{c.name}, Nonce: v}
		return nil
	})
	if err == nil {
		return resp, nil
	}

	var notOwner *cluster.NotOwnerError
	if !errors.As(err, &notOwner) {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if forwarded || notOwner.Redirect == nil {
		// Degraded mode: no healthy owner is known (or a forward raced a
		// handoff) — the store path keeps the key available.
		v := c.durable.increment(keyStr)
		return &eventpb.TestEvent{Hops: []string{"fallback@" + c.name}, Nonce: v}, nil
	}

	// Forward one hop to the claim holder.
	fwdCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fwdCtx, err = internalrpc.WithAPIKey(fwdCtx, e2eAPIKey)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := ocp_headers.SetASCIIHeader(fwdCtx, e2eForwardedHeader, "true"); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	conn, err := c.pool.Conn(notOwner.Redirect.Address)
	if err != nil {
		c.ownership.NoteUnreachable(notOwner.Redirect.InstanceID)
		return nil, status.Error(codes.Unavailable, "owner unreachable")
	}

	out := new(eventpb.TestEvent)
	if err := conn.Invoke(fwdCtx, e2eCounterMethod, req, out); err != nil {
		// A failed forward is the suspicion signal: corroborated with a stale
		// heartbeat it lets takeover beat the full liveness window.
		c.ownership.NoteUnreachable(notOwner.Redirect.InstanceID)
		return nil, status.Error(codes.Unavailable, "forward failed")
	}
	out.Hops = append([]string{c.name}, out.Hops...)
	return out, nil
}

func (c *counterService) isForwarded(ctx context.Context) bool {
	value, err := ocp_headers.GetASCIIHeaderByName(ctx, e2eForwardedHeader)
	return err == nil && value == "true"
}

func (c *counterService) onAcquired(_ context.Context, key []byte) {
	// Warm state on acquire: the new owner picks the counter up exactly where
	// the durable store left it.
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hot[string(key)] = c.durable.get(string(key))
}

func (c *counterService) onReleased(_ context.Context, key []byte) {
	// Write-through leaves nothing to flush; just evict.
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.hot, string(key))
}

func e2eApplyHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(commonpb.UserId)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(e2eCounterServer).Apply(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: e2eCounterMethod}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(e2eCounterServer).Apply(ctx, req.(*commonpb.UserId))
	}
	return interceptor(ctx, in, info, handler)
}

var e2eCounterServiceDesc = grpc.ServiceDesc{
	ServiceName: "flipcash.cluster.test.Counter",
	HandlerType: (*e2eCounterServer)(nil),
	Methods:     []grpc.MethodDesc{{MethodName: "Apply", Handler: e2eApplyHandler}},
	Streams:     []grpc.StreamDesc{},
	Metadata:    "cluster/tests/e2e.go",
}

// e2eNode is one simulated server process: a real gRPC server on a real
// listener, plus the full cluster runtime.
type e2eNode struct {
	name       string
	member     *cluster.Member
	membership *cluster.Membership
	router     *cluster.Router
	ownership  *cluster.Ownership
	counter    *counterService
	conn       *grpc.ClientConn
	stopServer func()
	stopped    bool
}

func startE2ENode(t *testing.T, s cluster.Store, name string, durable *durableStore) *e2eNode {
	log := zap.NewNop()

	conn, serv, err := ocp_testutil.NewServer(log)
	require.NoError(t, err)

	n := &e2eNode{
		name: name,
		conn: conn,
		member: &cluster.Member{
			InstanceID: name,
			Address:    conn.Target(),
			Labels:     map[string]string{"role": "all"},
		},
	}
	n.membership = cluster.NewMembership(log, s, n.member, fastMembershipConfig())
	n.router = cluster.NewRouter(n.membership, nil)
	n.ownership = cluster.NewOwnership(log, n.membership, n.router, s, fastOwnershipConfig())
	n.counter = &counterService{
		name:      name,
		ownership: n.ownership,
		pool:      internalrpc.NewPool(log),
		auth:      internalrpc.NewAuthenticator(e2eAPIKey),
		durable:   durable,
		hot:       make(map[string]uint64),
	}
	n.ownership.RegisterNamespace(testNamespace, cluster.NamespaceHooks{
		OnAcquired: n.counter.onAcquired,
		OnReleased: n.counter.onReleased,
	})

	serv.RegisterService(func(g *grpc.Server) {
		g.RegisterService(&e2eCounterServiceDesc, n.counter)
	})
	stop, err := serv.Serve()
	require.NoError(t, err)
	n.stopServer = stop

	ctx := context.Background()
	require.NoError(t, n.membership.Start(ctx))
	n.ownership.Start(ctx)

	t.Cleanup(func() { n.crash() })
	return n
}

// deploy runs the graceful shutdown flow: drain (leave candidacy, hand off
// claims), stop serving, deregister.
func (n *e2eNode) deploy(t *testing.T) {
	require.NoError(t, n.ownership.Drain(context.Background()))
	// Let in-flight forwards complete and peer routing converge off us before
	// the (hard) test-server stop, mirroring production's graceful gRPC stop.
	time.Sleep(100 * time.Millisecond)
	n.crashServerOnly()
	require.NoError(t, n.membership.Deregister(context.Background()))
	n.ownership.Stop()
	n.stopped = true
}

// crash kills the node with no drain and no deregistration: claims are left
// behind for liveness expiry and suspicion to displace.
func (n *e2eNode) crash() {
	if n.stopped {
		return
	}
	n.crashServerOnly()
	n.ownership.Stop()
	n.membership.Stop()
	n.stopped = true
}

func (n *e2eNode) crashServerOnly() {
	n.stopServer()
	n.conn.Close()
	n.counter.pool.Close()
}

func e2eKey() []byte {
	id := uuid.New()
	return id[:]
}

func applyOnce(ctx context.Context, conn *grpc.ClientConn, key []byte) (*eventpb.TestEvent, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	out := new(eventpb.TestEvent)
	if err := conn.Invoke(ctx, e2eCounterMethod, &commonpb.UserId{Value: key}, out); err != nil {
		return nil, err
	}
	return out, nil
}

func applyWithRetry(t *testing.T, conn *grpc.ClientConn, key []byte) *eventpb.TestEvent {
	var resp *eventpb.TestEvent
	require.Eventually(t, func() bool {
		var err error
		resp, err = applyOnce(context.Background(), conn, key)
		return err == nil
	}, 10*time.Second, 25*time.Millisecond)
	return resp
}

func waitE2ELive(t *testing.T, n *e2eNode, count int) {
	require.Eventually(t, func() bool {
		return len(n.membership.Live()) == count
	}, 10*time.Second, 10*time.Millisecond)
}

func testE2EHappyPath(t *testing.T, s cluster.Store) {
	t.Run("testE2EHappyPath", func(t *testing.T) {
		durable := newDurableStore()
		nodes := []*e2eNode{
			startE2ENode(t, s, "node-1", durable),
			startE2ENode(t, s, "node-2", durable),
			startE2ENode(t, s, "node-3", durable),
		}
		for _, n := range nodes {
			waitE2ELive(t, n, len(nodes))
		}

		const keys, appliesPerKey = 16, 5
		for i := range keys {
			key := e2eKey()

			owners := make(map[string]bool)
			for j := range appliesPerKey {
				// Requests enter at any server, exactly as behind a
				// round-robin LB.
				resp := applyWithRetry(t, nodes[(i+j)%len(nodes)].conn, key)

				// The counter is exact and the routing cost is at most the
				// designed single forward hop.
				require.EqualValues(t, j+1, resp.Nonce)
				require.LessOrEqual(t, len(resp.Hops), 2)
				owners[resp.Hops[len(resp.Hops)-1]] = true
			}

			// Exactly one server served the key no matter where requests
			// entered, and it never needed the degraded path.
			require.Len(t, owners, 1)
			for owner := range owners {
				require.NotContains(t, owner, "fallback@")
			}
			require.EqualValues(t, appliesPerKey, durable.get(string(key)))
		}
	})
}

func testE2EGracefulDeploy(t *testing.T, s cluster.Store) {
	t.Run("testE2EGracefulDeploy", func(t *testing.T) {
		durable := newDurableStore()
		n1 := startE2ENode(t, s, "node-1", durable)
		n2 := startE2ENode(t, s, "node-2", durable)
		n3 := startE2ENode(t, s, "node-3", durable)
		for _, n := range []*e2eNode{n1, n2, n3} {
			waitE2ELive(t, n, 3)
		}

		// A key owned by the node being deployed, seeded so the claim exists.
		var key []byte
		for {
			key = e2eKey()
			owner, err := n2.router.Owner(context.Background(), testNamespace, key)
			require.NoError(t, err)
			if owner.InstanceID == "node-1" {
				break
			}
		}
		seed := applyWithRetry(t, n2.conn, key)
		require.Equal(t, "node-1", seed.Hops[len(seed.Hops)-1])

		// Hammer the key through the surviving nodes while node-1 deploys.
		var successes, stop atomic.Uint64
		var wg sync.WaitGroup
		for _, entry := range []*e2eNode{n2, n3} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for stop.Load() == 0 {
					if _, err := applyOnce(context.Background(), entry.conn, key); err == nil {
						successes.Add(1)
					}
					time.Sleep(2 * time.Millisecond)
				}
			}()
		}

		time.Sleep(100 * time.Millisecond)
		n1.deploy(t)
		startE2ENode(t, s, "node-1b", durable)
		time.Sleep(500 * time.Millisecond)
		stop.Store(1)
		wg.Wait()

		// The chat stayed lively straight through the deploy...
		require.Positive(t, successes.Load())
		// ...and no acknowledged write was lost (client-side timeouts may
		// undercount, never overcount).
		require.GreaterOrEqual(t, durable.get(string(key)), successes.Load()+1)

		// Ownership moved off the deployed incarnation, whose claim was
		// released eagerly — never left for liveness expiry.
		claim, err := s.GetClaim(context.Background(), testNamespace, key)
		if err == nil {
			require.NotEqual(t, "node-1", claim.OwnerInstanceID)
		} else {
			require.ErrorIs(t, err, cluster.ErrClaimNotFound)
		}

		// The replacement incarnation joins the fleet and serves.
		waitE2ELive(t, n2, 3)
		resp := applyWithRetry(t, n2.conn, key)
		require.NotEqual(t, "node-1", resp.Hops[len(resp.Hops)-1])
	})
}

func testE2ECrashFailover(t *testing.T, s cluster.Store) {
	t.Run("testE2ECrashFailover", func(t *testing.T) {
		durable := newDurableStore()
		n1 := startE2ENode(t, s, "node-1", durable)
		n2 := startE2ENode(t, s, "node-2", durable)
		waitE2ELive(t, n1, 2)
		waitE2ELive(t, n2, 2)

		var key []byte
		for {
			key = e2eKey()
			owner, err := n2.router.Owner(context.Background(), testNamespace, key)
			require.NoError(t, err)
			if owner.InstanceID == "node-1" {
				break
			}
		}
		seed := applyWithRetry(t, n2.conn, key)
		require.Equal(t, "node-1", seed.Hops[len(seed.Hops)-1])
		require.EqualValues(t, 1, seed.Nonce)

		// Hard crash: no drain, no deregistration, claim left behind.
		n1.crash()

		// Traffic through the survivor recovers: failed forwards report the
		// corpse unreachable, suspicion corroborates, and node-2 displaces the
		// claim — well before test timeouts, and the counter continues from
		// the durable value with nothing lost.
		resp := applyWithRetry(t, n2.conn, key)
		require.Equal(t, "node-2", resp.Hops[len(resp.Hops)-1])
		require.EqualValues(t, durable.get(string(key)), resp.Nonce)

		// The takeover advanced the fence past the crashed incarnation's.
		claim, err := s.GetClaim(context.Background(), testNamespace, key)
		require.NoError(t, err)
		require.Equal(t, "node-2", claim.OwnerInstanceID)
		require.EqualValues(t, 2, claim.Fence)

		// Steady state after failover: served locally by the new owner.
		resp = applyWithRetry(t, n2.conn, key)
		require.Equal(t, []string{"node-2"}, resp.Hops)
	})
}

func testE2EDegradedFallback(t *testing.T, s cluster.Store) {
	t.Run("testE2EDegradedFallback", func(t *testing.T) {
		durable := newDurableStore()
		n1 := startE2ENode(t, s, "node-1", durable)
		waitE2ELive(t, n1, 1)

		// Drain the only node: no eligible owner exists anywhere, but the
		// endpoint keeps serving through the store path.
		require.NoError(t, n1.ownership.Drain(context.Background()))

		key := e2eKey()
		resp := applyWithRetry(t, n1.conn, key)
		require.Equal(t, []string{"fallback@node-1"}, resp.Hops)
		require.EqualValues(t, 1, resp.Nonce)
		require.EqualValues(t, 1, durable.get(string(key)))
	})
}

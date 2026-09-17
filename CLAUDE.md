# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Flipcash server is a Go monolith providing gRPC services and workers that power the Flipcash self-custodial mobile wallet. It is built on top of the **Open Code Protocol (OCP)** and extends it through integration interfaces to provide Flipcash-specific functionality.

## Commands

### Testing

```bash
# Run unit tests (uses in-memory stores)
make test

# Run integration tests (requires Docker; spins up Postgres and DynamoDB Local containers)
make test-integration

# Run tests for a specific package
go test -cover -count=1 ./push/...

# Run a single test
go test -run TestPush_MemoryServer ./push/memory/

# Run one package's integration tests
go test -tags integration -count=1 ./chat/dynamodb/...
```

Build tags in use:
- `integration` — every `postgres/` and `dynamodb/` store test. CI (`.github/workflows/test.yml`, Go 1.27.x) runs `make test-integration`, so these must pass, not just `make test`.
- `claude` — `moderation/claude/client_test.go`; hits the real Anthropic API and `t.Fatal`s without `ANTHROPIC_API_KEY`. Some cases also need fixture files pointed at by `MODERATION_DISPLAY_NAME_FIXTURES`, `MODERATION_USERNAME_FIXTURES`, `MODERATION_GROUP_TITLE_FIXTURES` (deliberately untracked; see `.gitignore`).
- `androidIntegration` — `iap/android/verifier_test.go`, needs real Google credentials.

Integration-test mechanics worth knowing:
- Postgres: `database/prisma/test.NewTestEnv()` starts a container and applies migrations by shelling out to `go run github.com/steebchen/prisma-client-go migrate deploy` in `database/`. `make test-integration` pins that module with `go get` first.
- DynamoDB: `database/dynamodb/test.NewTestEnv()` starts `amazon/dynamodb-local` with a 120s auto-kill, so a single package's integration run must finish inside that window.
- The only `t.Skip` in the tree is `intent/integration_test.go` (contact send feature disabled).

### Database

```bash
# Navigate to database directory for all Prisma commands
cd database

# Generate Prisma Go client (after schema changes)
make generate

# Create a new migration (without applying it)
make migrate

# Apply pending migrations (production/staging)
make deploy

# View database contents in Prisma Studio
make studio

# Run a local test database (for manual testing, not automated tests)
make db
```

`database/Makefile` loads `database/.env` (copy `example.env`) for `DATABASE_URL`.

### Linting

This project uses standard Go tooling:

```bash
# Format code
go fmt ./...

# Run go vet
go vet ./...
```

### Local dev / operator CLIs (`cmd/`, untracked)

`.gitignore` ignores `cmd*/` and `main.go`, so anything under `cmd/` is local-only and never in git. Don't rely on it existing in a fresh clone, and don't try to commit it.

## Architecture

### Monolithic Design with OCP Integration

This is a **library package**: the server implementations are designed to be instantiated and registered with a gRPC server in a parent application.

The codebase extends OCP by implementing integration interfaces:
- `intent.Integration` - Controls allowed intent types (OpenAccounts, SendPublicPayment, ReceivePaymentsPublicly); denies private payments. `OnSuccess` hooks inject chat messages after DM payments. Also validates tip amounts against `tip/` presets.
- `antispam.Integration` - Enforces registration requirements (IAP-gated) and anti-spam policies for account opens, currency launches, payments, and swaps
- `swap.Integration` - Sends localized push notifications when swaps are submitted/finalized (buy/sell confirmations, currency gain notifications to holders)
- `geyser.Integration` - Detects external on-chain deposits and sends push notifications (filters spam deposits under $0.01)
- `moderation.Integration` - Validates moderation attestations on swaps and currency creation
- `task.Executor` - OCP task runtime handling exactly two task types (`SendContactDmPaymentMessage`, `SendTipDmPaymentMessage`). Delivery is at-least-once and concurrent; idempotency comes from using the task UUID as the client message ID.
- `blob.Worker` - implements OCP `worker.Runtime`; one worker per content kind drains the finalization queue.

### Domain-Driven Package Structure

Each package represents a bounded context with clear responsibilities:

**Core Services (gRPC Servers):**
- `account/` - User registration, login, public key management, user flags (flags also carry tip presets to clients)
- `activity/` - Activity feed for payments, deposits, withdrawals, gift cards
- `chat/` - Group/DM chat metadata, membership, feeds (DynamoDB-backed). See "Chat, messaging and events" below.
- `messaging/` - Message persistence, delivery/read pointers, reactions, typing notifications (DynamoDB-backed); `sender.go` is the transport-free engine for server-initiated messages (used by `task/`)
- `blob/` - User-uploaded media (images only today). Two S3 buckets (upload → origin) plus CloudFront signed download URLs; the server never proxies bytes. Flow: `GetUploadPolicy` → `InitiateExternalUpload` → client uploads → `CompleteExternalUpload` (queues finalization, returns PROCESSING) → poll `GetBlobs`. `finalizer.go` owns the checkpointed pipeline (inspect → moderate → copy → WebP renditions). Reads: owner always; others need an `AccessContext` whose principal (User, Chat, UserProfile, ChatProfile) holds a grant *and* covers the caller (`access.go`). `image.go` rejects EXIF/privacy metadata. `integration.go` (`ShareIntoChat`, `SetAsProfilePicture`, `SetAsChatPicture`, `ResolveRenditions`) is the API other domains use.
- `blocklist/` - Per-user block lists. `chat/` must not import it, so `blocklist/chat.go` adapts the store to `chat.BlocklistReader`; messaging uses `GetBlockers` to filter DM event fan-out.
- `contact/` - Contact list sync (hashed phone numbers, XOR-of-SHA256 checksums, streaming delta/full uploads); maps contacts to Flipcash users and their DM chat IDs
- `event/` - Real-time event streaming with bidirectional gRPC streams. See below.
- `push/` - Push notification management (FCM for iOS/Android), with category/group-key support; chunked batch sends; sets the app badge from `badge/`
- `profile/` - User profile management (display names, phone, email, pictures, DM fee)
- `moderation/` - Text/image content moderation with signed Ed25519 attestations; providers in subpackages: `claude/` (Anthropic API), `hive/` (Hive API), `composite/` (chains providers), `noop/` (tests)
- `resolver/` - Resolves phone numbers to payment addresses (public keys) for registered users
- `settings/` - User settings: locale (BCP 47) and region (currency code), used to localize pushes
- `thirdparty/` - Issues signed JWTs for third-party API access (Coinbase)
- `iap/` - In-app purchase verification (Apple/Google)
- `email/` - Email verification via Twilio
- `phone/` - Phone verification via Twilio

**Integration Packages (OCP Hooks):**
- `intent/`, `antispam/`, `swap/`, `geyser/`, `moderation/`, `task/`

**Infrastructure & Supporting Packages:**
- `auth/` - Ed25519 signature-based authentication/authorization
- `badge/` - Per-user app-icon badge count ("notifications since last open", not live unread). DynamoDB, one item per user, atomic `Increment`/`IncrementBatch`/`Reset`.
- `balance/` - `GetTotalUsdfBalance` via one batched OCP `GetBalances` RPC; `ErrNotFound` when the user has no bound key (chat rules treat that as zero). Used by chat minimum-balance rules and profile.
- `cluster/` - Multi-server coordination on DynamoDB (`cluster_members`, `cluster_claims`, `cluster_subscriptions`). Four decoupled layers, documented in `cluster/model.go`: membership (liveness = heartbeat *counter* movement, never wall clocks), routing (HRW with placement/override label selectors), ownership (lazy claims with a monotonic fence; `NotOwnerError` redirects — an accelerator, never an availability gate; claim rows are intentionally permanent, no TTL), subscriptions (non-exclusive per-topic interest, 64 shards). `internalrpc/` is the connection pool plus `x-flipcash-internal-rpc-api-key` auth and redirect forwarding.
- `database/` - Postgres client + Prisma schema management; `database/dynamodb/` for DynamoDB client and test env
- `model/` - Domain models and utilities
- `localization/` - Locale-aware fiat currency formatting (symbol mapping, RTL handling) via `golang.org/x/text`
- `social/x/` - X (Twitter) API v2 client for profile fetching
- `protoutil/` - gRPC stream helpers (bounded receive with timeout, keep-alive monitoring) and proto comparison
- `rpc/` - Shared RPC constants (user-agent)
- `tip/` - Fiat tip preset table per currency (`Minimum/Low/Medium/High`); enforced on tip intents and DM-fee validation
- `testutil/` - In-memory gRPC server helpers for tests (`RunGRPCServer` only; auth helpers live in each package's `tests/`)

### Repository Pattern

Each domain package follows this structure:
```
domain/
  server.go          # gRPC service implementation
  store.go           # Store interface definition
  model.go           # Domain models
  memory/            # In-memory implementation (for unit tests)
    store.go
  postgres/          # PostgreSQL implementation (production for most domains)
    store.go
  dynamodb/          # DynamoDB implementation (chat, messaging, blob, badge, blocklist, cluster)
    store.go
    table.go         # Table/GSI definitions + CreateTables(...)
    common_test.go   # //go:build integration: TestMain that starts dynamotest and creates tables
  cache/             # Optional caching decorator over another Store
  tests/             # Shared test suites
    server.go / store.go
```

Storage implementations are swappable via interfaces. Tests are written against the `Store` interface and run against all implementations (`memory/`, `postgres/`, `dynamodb/`, `cache/` where applicable). **Add new test cases to the shared `tests/` suite**, not to a single backend's driver file; the per-backend `*_test.go` files are thin wrappers that construct a store and call `tests.RunStoreTests` / `tests.RunServerTests` with a `reset()` teardown.

**Storage backend split:** Most domains use Postgres. Chat, messaging, blob, badge, blocklist and cluster use **DynamoDB** via `aws-sdk-go-v2`. DynamoDB table definitions live in each package's `dynamodb/table.go`, not in Prisma. Several tables use **sparse GSIs** (e.g. `group_members.by_user` / `by_joined_at`): any new item type written into that partition must omit the indexed attributes or it leaks into the index.

### Chat, messaging and events

This is the most intricate part of the codebase and where most current work happens. The "why" lives in doc comments on the types and stores; read those before changing behaviour.

**Chat IDs.** Length is the type discriminator: 32 bytes = DM (SHA-256 over a domain-separated, sorted, deduped member set, so creation is idempotent and order-independent), 16 bytes = group (server-derived: a truncated, domain-separated SHA-256 over the creator's user ID and the request's required `IdempotencyKey`, so a retried `StartChat` names the same group and is answered from the existing record; stamped as a version 8 UUID so every group ID is UUID-shaped, though the ID is opaque and nothing parses it). DM paths must reject 16-byte IDs and vice versa. `CONTACT_DM` uses the bare legacy hash domain; other DM types append their enum number. A chat type of `UNKNOWN` falls back to `CONTACT_DM` for legacy clients.

**Chat storage (`chat/dynamodb`).** Tables: `chats` (metadata), `dm_inbox` (per-user DM feed rows; GSI `by_type_activity` on a composite `feed` key, legacy `by_activity` GSI still maintained), `group_members` (pk chat, sk user; plus one `#meta` item per group holding `member_count`/`version`, CAS-updated in the same transaction as each transition, with bounded retries on contention). `Chat.Members` is populated only for DMs; groups return empty `Members` and a `RosterSummary{MemberCount, Version}`. Version is *state, not a delta*: each real transition bumps it by exactly one and no-ops leave it alone; clients keep the greater version. DM sends fan `last_activity` into each member's inbox row; **group sends never fan out** — the group feed is assembled at read time with order computed once and a window of chat IDs carried in the paging token (`maxGroupFeedChats = 1000`), re-checking membership per page.

**Tombstones.** A departed group member's row stays with `state=2`, `left_at`, a 1h TTL and the departure's roster version, so re-joins are idempotent updates and delayed duplicates of an undone join can be distinguished from news. Readers trust `state`, never the clock. "Formerly a member" is not durable.

**Rules (`chat/rules.go`).** The proto is the single vocabulary: what clients see in `Metadata.rules` is exactly what `RuleEvaluator` evaluates. Only listener rules exist today (staff-only, minimum USD balance; a positive minimum balance is required); speaker rules are rejected. Rules are immutable after creation, so `chat/cache` holds them forever. Unknown rule kind = error (admit no one), never a pass. Rules are evaluated against current state, not join-time state.

**Group management.** `StartChat` / `JoinChat` / `LeaveChat` exist and are gated by the `requireStaffForGroupManagementRPC` config flag (a staff flag, not a client-version gate — nothing in these packages gates on client version). DMs always deny join/leave. Membership is checked before rules so a re-join is a no-op. Every real transition publishes a `RosterUpdate` to both the user and chat topics. Creation validates rules, checks the creator satisfies them, moderates the title and attaches the picture *before* the record is written.

**`chat/cache`** caches only what is fixed at creation: DM membership (positives only), DM member lists, group rules. Group membership is never cached.

**Messaging storage (`messaging/dynamodb`).** `messages` uses one partition per chat (sk `#counter` / `msg#` / `evt#` / `cmid#`) so a send is a single transaction and `evt#` is a gapless, strongly consistent range for delta catch-up. `GetDelta` pages 100 at a time and returns `RESET_REQUIRED` past 1000 events. `message_pointers`: DMs use a **single `#ptrs` item per chat** with per-member attribute suffixes; groups use one `ptr#<user>` item per member. Group pointer advances are stored but never broadcast (N² fan-out). Idempotency markers (`cmid#`) carry a TTL. Reactions span three tables: `message_reactions` is chat-keyed (`agg#<seq>#<emoji hex>` per emoji with count, **per-emoji version** and a bounded sample; `meta#<seq>` holding the message's own state, today its active-emoji count) so a page of summaries is one range query — a **strongly consistent** one, so a reader who reacts and refreshes never finds their emoji absent with no version to explain it; `message_reactors` is **message-keyed** (`user#<user>#<emoji>`, one row per current reaction, deleted on remove, no tombstone) with the `by_version` LSI on `emoji_version` so an emoji's reactors page most-recent-first under a strongly consistent read; `message_self_reactions` is **viewer-keyed** (pk chat+user, sk `<seq>#<emoji hex>`, the add's version; **groups only** — a DM's overlay is answered from the sample, so a DM writes no row and `GetSelfReactions` refuses a DM ID) so a viewer's own reactions across a page of messages — the `reacted_by_self` overlay for groups — are one strongly consistent range query on their own partition, billed by what the viewer reacted rather than a key probe per aggregate on the page (a GSI would be eventually consistent and outside the transaction; a prefix in `message_reactions` would double the write load on the chat partition that already takes every aggregate CAS). Every add/remove is one transaction that writes the reactor row and its viewer-keyed copy and compare-and-sets the aggregate (and the meta row when an emoji activates or empties) to the exact next state, on the `group_members` `#meta` pattern: no read-back, exact type cap and sample eviction, lost CAS retried from the returned item, transaction conflicts backed off. Reactor rows are stamped with the version that added them; that version is the ordering key and the paging cursor, never `reacted_ts`. `GetReactors` reads the aggregate version *before* the page so the page is never older than the version.

**Permissions.** One definition lives in `chat.Access` (`chat/access.go`); the parent builds a single instance and injects it into both the chat and messaging servers, so the admission cache and its TTL are shared. Three gates: `CanListen` (member, or a non-member of a *group* who satisfies its listener rules right now — a qualifying user can preview a group before joining), `IsMember` (pointer advances, add/remove reaction: writes that are a member's alone), `CanSpeak` (sends, edits, deletes, typing: member plus listener and speaker rules). A member's read is answered on membership alone and never evaluates a rule; a non-member's read evaluates the listener rules, with a positive-only 30s admission cache per (group, user). A group with **no** listener rules admits no non-member (only a rule can admit one; the store does not require rules, so legacy groups may carry none). A DM never reaches the rules fallback. Membership is meant to track listener rules (a member who stops satisfying one gets removed) — not built yet. `GetChat` returns a group's record to any registered user; the viewer's `Standing` decides how much `hydrate` reads (no hydrated member for a non-member, no messaging state for a viewer who cannot read). DMs stay member-only.

**Event delivery (`event/`).** `Bus` runs each handler on its own goroutine. The stream registry is sharded 64 ways. Two cluster namespaces: `user-events` and `chat-events` (groups only; DM updates fan out per member on the user bus, group updates publish once on the chat topic). Stream keys are prefixed because user and group IDs are both 16 bytes. A session's chat topics **follow membership and are version-gated**: a roster transition applies only if newer than the last one applied for that chat, seeded at stream open from membership records including tombstones. A reconcile sweep re-reads each streaming user's membership every 5 minutes (1s tick, 8 workers, quota-paced). Per-peer outboxes are 4096 deep and drop on overflow; a lagging stream is closed with `ErrStreamLagging`. `ForwardEvents` (cross-server, API-key authed) delivers locally only and never re-resolves ownership.

### Authentication Flow

All gRPC services use a two-phase auth pattern:
1. **Authentication** - `auth.Authenticator` verifies Ed25519 signatures
2. **Authorization** - `auth.Authorizer` looks up UserID from public key and checks permissions

The `auth` field in requests is zeroed out during verification to prevent tampering.

### Event Streaming Architecture

The event system supports multi-server deployments:
- Services publish events to `event.Bus` (e.g., `messaging/event.go` publishes `ChatUpdate` events for real-time message delivery)
- `event.Server` maintains bidirectional gRPC streams with clients
- **Rendezvous records** track which server instance hosts each user's stream
- Events are forwarded across server instances using internal gRPC RPCs (`cluster/internalrpc`)
- Internal RPCs use API key authentication

### Database Schema

Prisma schema is located at `database/prisma/schema.prisma`:
- **User** - Core user entity with display name, phone, email, flags
- **PublicKey** - Ed25519 public keys (1:1 with User)
- **PushToken** - FCM push tokens per app installation
- **Iap** - In-app purchase records
- **Rendezvous** - Event stream location tracking
- **ContactList / ContactListEntry** - Synced contact phone hashes and checksums
- **XProfile** - Twitter/X integration

Database access uses both:
- **Prisma Client Go** - Type-safe queries
- **pgx/v5** - Raw SQL with connection pooling for complex queries

Transactions use `database.ExecuteTxWithinCtx(ctx, func(txCtx context.Context) error { ... })`

Chat, messaging, blob, badge, blocklist and cluster data live in **DynamoDB**, not Postgres — table definitions are in the respective `dynamodb/table.go` files.

## Testing Patterns

### Unit Tests (No Build Tag)

Use in-memory stores for fast, isolated tests:
```go
func TestPush_MemoryServer(t *testing.T) {
    testStore := memory.NewInMemory()
    teardown := func() {
        testStore.(*memory.memory).reset()
    }
    tests.RunServerTests(t, testStore, teardown)
}
```

### Integration Tests (`//go:build integration`)

Postgres-backed tests spin up a Postgres container via Docker and run Prisma migrations:
```go
//go:build integration

func TestMain(m *testing.M) {
    env, err := prismatest.NewTestEnv()  // Starts Docker, runs migrations
    // ...
}

func TestPush_PostgresStore(t *testing.T) {
    pool, _ := pgxpool.New(context.Background(), testEnv.DatabaseUrl)
    pg.SetupGlobalPgxPool(pool)
    testStore := postgres.NewInPostgres(pool)
    tests.RunStoreTests(t, testStore, teardown)
}
```

DynamoDB-backed tests use a DynamoDB Local container. Each `dynamodb/` package has its own `common_test.go` with a `TestMain` that creates one shared env, then each test calls that package's `CreateTables(...)` with test table names and resets via the unexported `store.reset()` (scan + delete):
```go
//go:build integration

func TestMain(m *testing.M) {
    env, err := dynamotest.NewTestEnv()  // github.com/code-payments/flipcash2-server/database/dynamodb/test
    // ...
}
```

Integration tests require Docker and use `github.com/ory/dockertest/v3` to manage containers.

### gRPC Server Tests

Use `testutil.RunGRPCServer()` to create in-memory gRPC connections via `bufconn`:
```go
cc := testutil.RunGRPCServer(t, log,
    testutil.WithService(func(s *grpc.Server) {
        pb.RegisterMyServiceServer(s, myServer)
    }),
    testutil.WithUnaryServerInterceptor(myInterceptor),
)
client := pb.NewMyServiceClient(cc)
```

## Protobuf APIs

Two protobuf dependencies (see `go.mod` for current versions):
- **flipcash2-protobuf-api** - Flipcash-specific services (Account, Activity, Chat, Messaging, Contact, Event, Push, Moderation, Resolver, Settings, ThirdParty, Blob, Blocklist, etc.)
- **ocp-protobuf-api** - Open Code Protocol definitions for blockchain interactions

`go.mod` sometimes carries pseudo-versions of `ocp-server` / `ocp-protobuf-api` and, during OCP migrations, a temporary local `replace` directive. Check for a `replace` before assuming a build failure is local.

## Important Conventions

### Public Key Format
Public keys are stored as raw bytes (32 bytes for Ed25519), not base58-encoded strings.

### User IDs
UserIDs are UUIDs stored as byte arrays in `commonpb.UserId.Value`.

### Error Handling
Use `google.golang.org/grpc/status` for gRPC errors:
```go
return status.Error(codes.InvalidArgument, "invalid request")
```

### Logging
Use `go.uber.org/zap` for structured logging. Loggers are passed to constructors.

### Database NULL Values
Prisma uses pointers for optional fields: `*string`, `*int`, etc.

### Doc comments are the spec
Design rationale lives in package, type and store doc comments (e.g. `cluster/model.go`, `chat/model.go`, `chat/rules.go`, `blob/s3/storage.go`), not in a wiki. Intentional gaps are recorded in prose there ("not read today", "no group carries a speaker rule yet") rather than as `TODO`s (the membership-tracks-rules gap is described in prose on `chat.Access` and at the top of `messaging/access.go`). When changing behaviour, update the comment that explains it.

### Concurrency and idempotency
Prefer a DB constraint plus error translation over pre-emptive locking when races are rare in practice. DynamoDB writes in chat/messaging are conditional and monotonic so retries and at-least-once task delivery are safe; keep that property when adding writes.

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
```

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

### Linting

This project uses standard Go tooling:

```bash
# Format code
go fmt ./...

# Run go vet
go vet ./...
```

## Architecture

### Monolithic Design with OCP Integration

This is a **library package**: the server implementations are designed to be instantiated and registered with a gRPC server in a parent application.

The codebase extends OCP by implementing integration interfaces:
- `intent.Integration` - Controls allowed intent types (OpenAccounts, SendPublicPayment, ReceivePaymentsPublicly); denies private payments. `OnSuccess` hooks inject chat messages after DM payments.
- `antispam.Integration` - Enforces registration requirements (IAP-gated) and anti-spam policies for account opens, currency launches, payments, and swaps
- `swap.Integration` - Sends localized push notifications when swaps are submitted/finalized (buy/sell confirmations, currency gain notifications to holders)
- `geyser.Integration` - Detects external on-chain deposits and sends push notifications (filters spam deposits under $0.01)
- `moderation.Integration` - Validates moderation attestations on swaps and currency creation

### Domain-Driven Package Structure

Each package represents a bounded context with clear responsibilities:

**Core Services (gRPC Servers):**
- `account/` - User registration, login, public key management, user flags
- `activity/` - Activity feed for payments, deposits, withdrawals, gift cards
- `chat/` - Group/DM chat metadata, membership, DM feed pagination (DynamoDB-backed)
- `messaging/` - Message persistence, delivery/read pointers, typing notifications (DynamoDB-backed); `sender.go` is the engine for server-initiated messages (e.g., payment messages injected into DMs)
- `contact/` - Contact list sync (hashed phone numbers, XOR-of-SHA256 checksums, streaming delta/full uploads); maps contacts to Flipcash users and their DM chat IDs
- `event/` - Real-time event streaming with bidirectional gRPC streams
- `push/` - Push notification management (FCM for iOS/Android), with category/group-key support
- `profile/` - User profile management (display names, phone, email)
- `moderation/` - Text/image content moderation with signed Ed25519 attestations; providers in subpackages: `claude/` (Anthropic API), `hive/` (Hive API), `composite/` (chains providers), `noop/` (tests)
- `resolver/` - Resolves phone numbers to payment addresses (public keys) for registered users
- `settings/` - User settings: locale (BCP 47) and region (currency code), used to localize pushes
- `thirdparty/` - Issues signed JWTs for third-party API access (Coinbase)
- `iap/` - In-app purchase verification (Apple/Google)
- `email/` - Email verification via Twilio
- `phone/` - Phone verification via Twilio

**Integration Packages (OCP Hooks):**
- `intent/`, `antispam/`, `swap/`, `geyser/`, `moderation/`

**Infrastructure & Supporting Packages:**
- `auth/` - Ed25519 signature-based authentication/authorization
- `database/` - Postgres client + Prisma schema management; `database/dynamodb/` for DynamoDB client and test env
- `model/` - Domain models and utilities
- `localization/` - Locale-aware fiat currency formatting (symbol mapping, RTL handling) via `golang.org/x/text`
- `social/x/` - X (Twitter) API v2 client for profile fetching
- `protoutil/` - gRPC stream helpers (bounded receive with timeout, keep-alive monitoring) and proto comparison
- `rpc/` - Shared RPC constants (user-agent)
- `testutil/` - In-memory gRPC server helpers for tests

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
  dynamodb/          # DynamoDB implementation (production for chat/ and messaging/)
    store.go
    table.go         # Table/GSI definitions
  cache/             # Optional caching decorator over another Store (chat membership)
  tests/             # Shared test suites
    server.go / store.go
```

Storage implementations are swappable via interfaces. Tests are written against the `Store` interface and run against all implementations (`memory/`, `postgres/`, `dynamodb/` where applicable).

**Storage backend split:** Most domains use Postgres. High-volume chat data (`chat/`, `messaging/`) uses **DynamoDB** via `aws-sdk-go-v2` (e.g., a `chats` table for canonical metadata plus a `dm_inbox` table with a GSI on (user, last_activity) for sorted DM feed pagination). DM chat IDs are derived deterministically from the two member user IDs, so DM creation is idempotent.

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
- Events are forwarded across server instances using internal gRPC RPCs
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

Chat/messaging data lives in **DynamoDB**, not Postgres — table definitions are in the respective `dynamodb/table.go` files.

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

DynamoDB-backed tests (chat/, messaging/) use a DynamoDB Local container:
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
- **flipcash2-protobuf-api** - Flipcash-specific services (Account, Activity, Chat, Messaging, Contact, Event, Push, Moderation, Resolver, Settings, ThirdParty, etc.)
- **ocp-protobuf-api** - Open Code Protocol definitions for blockchain interactions

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

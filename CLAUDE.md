# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Flipcash server is a Go monolith providing gRPC services and workers that power the Flipcash self-custodial mobile wallet. It is built on top of the **Open Code Protocol (OCP)** and extends it through integration interfaces to provide Flipcash-specific functionality.

## Commands

### Testing

```bash
# Run unit tests (uses in-memory stores)
make test

# Run integration tests (requires Docker, uses Postgres)
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

This is a **library package**, not a standalone executable. The server implementations are designed to be instantiated and registered with a gRPC server in a parent application. There is no `main.go` in this repository.

The codebase extends OCP by implementing integration interfaces:
- `intent.Integration` - Controls allowed intent types (OpenAccounts, SendPublicPayment, etc.)
- `antispam.Integration` - Enforces registration requirements and anti-spam policies
- `swap.Integration` - Triggers notifications when swaps complete
- `geyser.Integration` - Detects external deposits and sends notifications
- `airdrop.Integration` - Welcome bonus logic (currently disabled)

### Domain-Driven Package Structure

Each package represents a bounded context with clear responsibilities:

**Core Services (gRPC Servers):**
- `account/` - User registration, login, public key management, user flags
- `activity/` - Activity feed for payments, deposits, withdrawals, gift cards
- `event/` - Real-time event streaming with bidirectional gRPC streams
- `push/` - Push notification management (FCM for iOS/Android)
- `profile/` - User profile management (display names, phone, email)
- `iap/` - In-app purchase verification (Apple/Google)
- `email/` - Email verification via Twilio
- `phone/` - Phone verification via Twilio

**Integration Packages (OCP Hooks):**
- `intent/`, `antispam/`, `swap/`, `geyser/`, `airdrop/`

**Infrastructure:**
- `auth/` - Ed25519 signature-based authentication/authorization
- `database/` - Postgres client and Prisma schema management
- `model/` - Domain models and utilities

### Repository Pattern

Each domain package follows this structure:
```
domain/
  server.go          # gRPC service implementation
  store.go           # Store interface definition
  model.go           # Domain models
  memory/            # In-memory implementation (for unit tests)
    store.go
  postgres/          # PostgreSQL implementation (production)
    store.go
  tests/             # Shared test suites
    server_test.go
```

Storage implementations are swappable via interfaces. Tests are written against the `Store` interface and run against both `memory/` and `postgres/` implementations.

### Authentication Flow

All gRPC services use a two-phase auth pattern:
1. **Authentication** - `auth.Authenticator` verifies Ed25519 signatures
2. **Authorization** - `auth.Authorizer` looks up UserID from public key and checks permissions

The `auth` field in requests is zeroed out during verification to prevent tampering.

### Event Streaming Architecture

The event system supports multi-server deployments:
- Services publish events to `event.Bus`
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
- **XProfile** - Twitter/X integration

Database access uses both:
- **Prisma Client Go** - Type-safe queries
- **pgx/v5** - Raw SQL with connection pooling for complex queries

Transactions use `database.ExecuteTxWithinCtx(ctx, func(txCtx context.Context) error { ... })`

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

Integration tests spin up a Postgres container via Docker and run Prisma migrations:
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

Two protobuf dependencies:
- **flipcash2-protobuf-api** (v0.1.0) - Flipcash-specific services (Account, Activity, Event, Push, etc.)
- **ocp-protobuf-api** (v0.2.0) - Open Code Protocol definitions for blockchain interactions

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

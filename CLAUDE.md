# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Flipcash server is a Go monolith providing gRPC services and workers that power the Flipcash self-custodial mobile wallet. It is built on top of the **Open Code Protocol (OCP)** and extends it through integration interfaces. It is a **library package**: servers and workers are instantiated and registered by a parent application, which also does the wiring (shared instances, required constructor arguments).

## Commands

```bash
make test                                        # unit tests (in-memory stores)
make test-integration                            # integration tests (Docker: Postgres + DynamoDB Local)
go test -cover -count=1 ./push/...               # one package
go test -run TestPush_MemoryServer ./push/memory/  # one test
go test -tags integration -count=1 ./chat/dynamodb/...  # one package's integration tests
go fmt ./... && go vet ./...                     # lint
```

Build tags:
- `integration` — every `postgres/` and `dynamodb/` store test. CI (`.github/workflows/test.yml`) runs `make test-integration`, so these must pass, not just `make test`.
- `claude` — `moderation/claude/client_test.go`; hits the real Anthropic API, needs `ANTHROPIC_API_KEY` (some cases also need untracked fixture files named by `MODERATION_*_FIXTURES` env vars).
- `androidIntegration` — `iap/android/verifier_test.go`, needs real Google credentials.
- `cdpsandbox` — `cdp/customers/sandbox_test.go` and `kyc/coinbase/memory/sandbox_test.go`; call Coinbase's real Sandbox, need `CDP_SANDBOX_KEY_FILE`. They send only Coinbase's documented magic values and delete every customer they create; keep it that way.

Integration-test mechanics:
- Postgres: `database/prisma/test.NewTestEnv()` starts a container and applies migrations by shelling out to `go run github.com/steebchen/prisma-client-go migrate deploy` in `database/`. `make test-integration` pins that module with `go get` first.
- DynamoDB: `database/dynamodb/test.NewTestEnv()` starts `amazon/dynamodb-local` with a 120s auto-kill, so a single package's integration run must finish inside that window.

Database (run from `database/`; loads `database/.env`, copy `example.env`, for `DATABASE_URL`): `make generate` (Prisma Go client after schema changes), `make migrate` (create a migration without applying), `make deploy` (apply pending), `make studio`, `make db` (local Postgres for manual testing, not used by tests).

`.gitignore` ignores `cmd*/` and `main.go`: anything under `cmd/` is local-only. Don't rely on it existing and don't try to commit it.

## Architecture

### OCP integrations

- `intent.Integration` — which intents are allowed and their app metadata (DM, group and contact-DM payments, reaction boosts, requested payments, return payments); schedules the task that posts each payment's cash message. Destinations are read only through `intent.GetDirectPayment`.
- `task.Executor` — runs the tasks scheduled above (cash messages, boost recording, requested-payment and return recording). Delivery is at-least-once and concurrent; idempotency comes from using the task UUID as the client message ID. Malformed input is returned and retried, never dropped.
- `antispam.Integration` — registration (IAP-gated) and anti-spam policy for opens, launches, payments and swaps.
- `swap.Integration` — localized pushes on swap submit/finalize.
- `geyser.Integration` — external deposit detection and pushes (ignores deposits under $0.01).
- `moderation.Integration` — validates signed moderation attestations; each attestation names the content kind it was moderated as, and must match the kind OCP uses the field as.
- Workers (OCP `worker.Runtime`): `blob.Worker` (one per content kind), `poll.Worker` (tally), `paymentrequest.Worker` (expiry).

### Packages

gRPC services: `account/` (registration, keys, user flags), `activity/` (feed), `chat/` (chat metadata, membership, feeds, rules, private groups, lobby, invites, moderation, pins, mute), `messaging/` (messages, pointers, reactions and boosts, payment return offers, typing; `sender.go` is the transport-free engine for server-initiated messages), `blob/` (user media, S3 + CloudFront signed URLs, end-to-end encrypted blobs), `poll/`, `paymentrequest/`, `blocklist/`, `contact/` (deprecated), `event/` (bidirectional event streams, chat previews), `push/`, `profile/`, `moderation/` (providers in `claude/`, `hive/`, `composite/`, `noop/`), `resolver/`, `settings/`, `thirdparty/` (Coinbase JWTs for the apps), `kyc/coinbase/` (KYC via Coinbase's Customers API; the package doc in `server.go` is the spec), `community/`, `reporting/` (stub), `iap/`, `email/`, `phone/`.

Supporting: `auth/`, `flipcashteam/` (the Flipcash team account), `badge/`, `balance/` (OCP balance reads), `cdp/` (CDP JWTs and the Customers API client, with an in-process fake in `cdp/customers/customerstest`), `cluster/` (multi-server coordination on DynamoDB; design in `cluster/model.go`; `internalrpc/` for API-key-authed forwarding), `database/`, `model/`, `localization/`, `social/x/`, `protoutil/`, `redact/`, `rpc/`, `send/` (per-currency presets; `MinimumTransferValue` is a currency's minor unit), `testutil/`.

### Repository pattern

```
domain/
  server.go          # gRPC service
  store.go           # Store interface
  model.go           # domain models
  memory/            # in-memory store (unit tests)
  postgres/          # Postgres store (most domains)
  dynamodb/          # DynamoDB store; table.go holds table/GSI defs + CreateTables(...);
                     # common_test.go (//go:build integration) starts dynamotest
  cache/             # optional caching decorator
  tests/             # shared test suites: RunStoreTests / RunServerTests
```

**Add new test cases to the shared `tests/` suite**, not to one backend's driver file; per-backend `*_test.go` files are thin wrappers that build a store and call the suite with a `reset()` teardown.

**Storage split:** most domains use Postgres (Prisma schema in `database/prisma/schema.prisma`; Prisma Client Go plus pgx/v5 for raw SQL; transactions via `database.ExecuteTxWithinCtx`). Chat, messaging, blob, badge, blocklist, cluster, poll, payment requests and Coinbase KYC use **DynamoDB** (`aws-sdk-go-v2`), with table definitions in each package's `dynamodb/table.go`, not Prisma.

DynamoDB hazards:
- Many tables use **sparse GSIs**: any new item type written into an indexed partition (e.g. a `#meta` item) must omit the indexed attributes or it leaks into the index.
- `CreateTables` leaves an existing table as is, so a new GSI on a live table is added out of band by infrastructure, and must be ACTIVE before code that reads it ships. An LSI exists only from table creation.
- `#meta` items with a `version` compare-and-set are the standard pattern for counts and summaries updated in the same transaction as the change.

### Cross-cutting invariants

- **Chat ID length is the type discriminator:** 32 bytes = DM (hash of the member set), 16 bytes = group. DM paths reject 16-byte IDs and vice versa. Group, poll and payment request IDs are derived from the creator and the request's `IdempotencyKey`, so a retried create is answered from the stored record.
- **Permissions live in one place, `chat.Access`** (`chat/access.go`), a single instance shared by the chat, messaging, poll, payment request, blob and event servers. Gates: `CanListen`, `IsMember`, `CanSpeak` / `SpeakerStanding` (which also says whether the chat takes plaintext or encrypted content). Nothing outside `chat/` should decide what makes a chat take encrypted content. Members are grandfathered: rules are evaluated against current state, and nothing removes a member who stops satisfying them.
- **Versions are state, not deltas:** a real change moves a version by exactly one, a no-op never moves it, and an event is published only by the write that made the change, never on a no-op or a replayed retry. Clients apply a change only at their version + 1 and refetch on a gap.
- **Writes are conditional and monotonic** so retries and at-least-once delivery are safe. Prefer a DB constraint plus error translation over pre-emptive locking.
- **The team account** (`flipcashteam`) is identified by a `teamUserID` passed as a **required** constructor argument (chat stores, `chat.NewRuleEvaluator`, `chat.NewServer`, `chat.NewHydrator`, `messaging.NewSender`, `intent.NewIntegration`), since chat and messaging cannot import `flipcashteam`. It is write-only: nobody messages or pays it, and it gets no events, pushes or pointer/inbox writes.
- **Encryption:** DMs and keyed private groups take encrypted content only; public groups take plaintext only. The server never reads ciphertext. Server-written messages (`messaging.Sender`) are plaintext.
- **Redaction** happens at one chokepoint per server (`messaging.Server.present`, `chat.Server.hydrate`'s last-message step, `redact.ChatUpdate` for streams), after media hydration. A message that cannot be redacted fails the read; it never leaks.
- **Events:** DM updates fan out per member on the user topic; group updates publish once on the chat topic. Group sends never fan out to the roster; the group feed is assembled at read time and pushes walk the roster in pages.
- **Credentials are random and stored, never derived:** payment request share intent IDs and return intent IDs are drawn at random so no one else can use them up, and are never on a read, event or push beyond what the proto names.
- `chat/cache` caches only what is fixed at creation (DM membership, a private group's rules). Public group rules, group membership, viewer state, key envelopes, lobbies and invites are never cached.

### Security-sensitive spots

- **KYC (`kyc/coinbase/`) stores and logs nothing a user submits.** No request-logging interceptor may run over that service; logs carry IDs and Coinbase's error status/type/correlation ID only, never a value or Coinbase's free-text message. Coinbase permanently blocks a customer reported as underage, so pre-checks must never let a bad date of birth through. The KYC CDP key must be separate from the one `thirdparty.GetJwt` signs with.
- `thirdparty.GetJwt` signs only an allowlist of Coinbase requests (`thirdparty/coinbase.go`); a new Coinbase call an app makes must be added before that app ships.
- **Invite tokens** (`chat/invite.go`) are never stored or logged; logs carry the invite ID only.

### Authentication

Two phases: `auth.Authenticator` verifies the Ed25519 signature (the request's `auth` field is zeroed during verification), then `auth.Authorizer` maps the public key to a user ID and checks permissions. Public keys are stored as raw 32 bytes; user IDs are UUIDs as bytes in `commonpb.UserId.Value`.

## Testing patterns

- Unit tests: in-memory stores with a `reset()` teardown, calling the shared `tests.Run*Tests`.
- Postgres integration: `TestMain` calls `prismatest.NewTestEnv()`; tests build a `pgxpool`, call `pg.SetupGlobalPgxPool`, and run the shared suite.
- DynamoDB integration: each `dynamodb/` package's `common_test.go` starts one `dynamotest.NewTestEnv()` (`github.com/code-payments/flipcash2-server/database/dynamodb/test`); each test calls `CreateTables(...)` with test table names and resets with the store's unexported `reset()`.
- gRPC servers: `testutil.RunGRPCServer(t, log, testutil.WithService(...), testutil.WithUnaryServerInterceptor(...))` over `bufconn`. Auth helpers live in each package's `tests/`.

## Protobuf APIs

- **flipcash2-protobuf-api** — Flipcash services.
- **ocp-protobuf-api** — OCP definitions.

`go.mod` sometimes carries pseudo-versions of `ocp-server` / `ocp-protobuf-api` and, during migrations, a temporary local `replace` directive. Check for a `replace` before assuming a build failure is local. Handlers rely on the proto's validation rules (enforced by OCP's validation interceptor) and don't repeat those checks.

## Conventions

- **Doc comments are the spec.** Design rationale and intentional gaps live in package, type and store doc comments (e.g. `cluster/model.go`, `chat/model.go`, `chat/rules.go`, `kyc/coinbase/server.go`), recorded in prose rather than as `TODO`s. Read them before changing behaviour, and update the comment that explains a behaviour when you change it.
- Errors: `google.golang.org/grpc/status` with `codes`.
- Logging: `go.uber.org/zap`, loggers passed to constructors.
- Prisma optional fields are pointers (`*string`, `*int`).

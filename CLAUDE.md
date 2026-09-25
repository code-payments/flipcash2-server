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
- `blob/` - User-uploaded media (images only today). Two S3 buckets (upload → origin) plus CloudFront signed download URLs; the server never proxies bytes. Flow: `GetUploadPolicy` → `InitiateExternalUpload` → client uploads → `CompleteExternalUpload` (queues finalization, returns PROCESSING) → poll `GetBlobs`. `finalizer.go` owns the checkpointed pipeline (inspect → moderate → copy → WebP renditions). Reads: owner always; others need an `AccessContext` whose principal (User, Chat, UserProfile, ChatProfile) holds a grant *and* covers the caller (`access.go`). `image.go` rejects EXIF/privacy metadata. `integration.go` (`ShareIntoChat`, `SetAsProfilePicture`, `SetAsChatPicture`, `ResolveRenditions`) is the API other domains use. **End-to-end encrypted blobs** (`encrypted.go`; `Blob.EncryptedFor` is a `*Principal` — the surface the READY grant is made to, `PrincipalForChat` for a DM — so the pipeline, stores (`encrypted_for_type` N + `encrypted_for_id` B) and read paths are surface-agnostic and only `initiateEncryptedUpload` maps a proto arm to a principal and its admission gate; `ContentKindEncrypted`, its own finalization queue and so its own `Worker` instance in the parent) are reserved with `InitiateExternalUploadRequest.end_to_end_encrypted_for.chat`: mime must be `application/octet-stream`, size ≤ `MaxEncryptedBlobSizeBytes` (the largest plaintext ceiling across encryptable kinds, today the image one; no allowance for the nonce and tag), and the caller must be a member of a **DM** (`blob.DMMembership`, satisfied by `chat.NewBlobDMMembership`, which owns the 32-byte discriminator check) or the reservation is `DENIED`. Finalization (`finalizeEncrypted`; the `Finalizer` takes the `AccessStore` for this) checks only the declared size (`TOO_LARGE` otherwise), promotes, **grants the DM read access, then** reaches READY, so READY implies granted and a rejected blob is never granted: no inspection, moderation or renditions. Before READY the other member sees nothing through `AccessContext.chat` (a reference that outruns READY reads as missing, which is why the sender waits for READY before sending). Metadata carries `kind.encrypted`. Every attach surface refuses one (`validateAttachable`) and `ResolveRenditions` omits it; only `EncryptedContent` in that DM, which the server never reads, references it. Policy always advertises `UploadPolicy.encrypted`; its image bounds (1600px) are advisory by construction.
- `blocklist/` - Per-user block lists. `chat/` must not import it, so `blocklist/chat.go` adapts the store to `chat.BlocklistReader`; messaging uses `GetBlockers` to filter DM event fan-out.
- `contact/` - Contact list sync (hashed phone numbers, XOR-of-SHA256 checksums, streaming delta/full uploads); maps contacts to Flipcash users and their DM chat IDs
- `event/` - Real-time event streaming with bidirectional gRPC streams. See below.
- `push/` - Push notification management (FCM for iOS/Android), with category/group-key support; chunked batch sends; sets the app badge from `badge/`. Chat message pushes are a `ChatMessagePush` built once per message (`BuildContactDmPush` / `BuildTipDmPush` / `BuildGroupChatPush`) and sent per recipient page (`Send`). Bodies are capped at 100 runes (`truncatePushBody`). A push whose full message would measure over `maxChatPushBytes` (3500; `payloadSize` measures the larger of the Android data and the APNs JSON, encoded exactly as `FCMPusher` sends it) carries only `ChatMetadata.message_id` instead, for the client to fetch; decided once per message at build time, and the muted copy follows
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
- `balance/` - `GetTotalUsdfBalance` (quarks, no rate) and `GetTotalFiatValue` (one currency code, OCP's valuation) via one batched OCP `GetBalances` RPC; `ErrNotFound` when the user has no bound key (chat rules treat that as zero); `ErrUnsupportedCurrency` when OCP returns no value for the code. Used by chat minimum-balance rules and profile.
- `cluster/` - Multi-server coordination on DynamoDB (`cluster_members`, `cluster_claims`, `cluster_subscriptions`). Four decoupled layers, documented in `cluster/model.go`: membership (liveness = heartbeat *counter* movement, never wall clocks), routing (HRW with placement/override label selectors), ownership (lazy claims with a monotonic fence; `NotOwnerError` redirects — an accelerator, never an availability gate; claim rows are intentionally permanent, no TTL), subscriptions (non-exclusive per-topic interest, 64 shards). `internalrpc/` is the connection pool plus `x-flipcash-internal-rpc-api-key` auth and redirect forwarding.
- `database/` - Postgres client + Prisma schema management; `database/dynamodb/` for DynamoDB client and test env
- `model/` - Domain models and utilities
- `localization/` - Locale-aware fiat currency formatting (symbol mapping, RTL handling) via `golang.org/x/text`
- `social/x/` - X (Twitter) API v2 client for profile fetching
- `protoutil/` - gRPC stream helpers (bounded receive with timeout, keep-alive monitoring) and proto comparison
- `redact/` - Shape-only placeholders for message content a viewer may see the shape of but not read (`Text`, `Content`, `Message`); pure functions of (chat ID, message seq, shape). See "Redacted reads" below.
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

**Chat storage (`chat/dynamodb`).** Tables: `chats` (metadata), `dm_inbox` (per-user DM feed rows; GSI `by_type_activity` on a composite `feed` key, legacy `by_activity` GSI still maintained), `group_members` (pk chat, sk user; plus one `#meta` item per group holding `member_count`/`version`, CAS-updated in the same transaction as each transition, with bounded retries on contention). `Chat.Members` is populated only for DMs; groups return empty `Members` and a `RosterSummary{MemberCount, Version}`. Version is *state, not a delta*: each real transition bumps it by exactly one and no-ops leave it alone; clients keep the greater version. DM sends fan `last_activity` into each member's inbox row; **group sends never fan out** — the group feed is assembled at read time with order computed once and a window of chat IDs carried in the paging token (`maxGroupFeedChats = 1000`), re-checking membership per page. A fourth table, `chat_user_state` (pk user, sk chat), holds `chat.ViewerState`: what a chat records about one user independent of membership — today a mute (`muted_until`, epoch seconds, present only while a mute is recorded; an indefinite mute is a store-internal far-future sentinel) and a per-row `version` with the same state-not-delta rule. Rows are sparse, never deleted, and survive leaving the chat. The viewer-state methods (`SetMute`, `ClearMute`, `GetViewerStates`, `GetMutedUsers`, `GetMutedUsersPage`, `GetMutedCount`) are part of `chat.Store` itself, not a separate interface. `GetViewerStates` is one strongly consistent `Query` on the user's partition bounded to the requested key range (a page of chats costs a few RCU, not one per key). The chat-scoped read has **two shapes, chosen by size**: `GetMutedUsers` is a key-range `Query` on the sparse `by_muted` GSI (chat, `muted_until`), billed by the active mutes it returns; `GetMutedUsersPage` ranges the inverted `by_user` GSI (chat, user; every record, full projection so future states need no new index) over an inclusive `[lo, hi]` user-ID key range in the same `user#` order as a group's roster, so a fan-out holding one roster page (`GetGroupMembersPage`, a cursor walk of the `group_members` partition in ascending user-ID order) asks for the mutes between that page's first and last user and never holds either whole. `GetMutedCount` reads a per-chat `#meta` item (pk `chat#`, sk `#meta`; carries neither `chat` nor `muted_until`, which keeps it out of both indexes) holding the number of records with a mute *recorded* — moved in the same transaction as a first mute or a clear, never by a replace or a lapse, so it bounds active mutes from above; the fan-out compares it to the roster size to pick a shape. Both GSIs are eventually consistent. A replaced mute is one conditional `UpdateItem`; a first mute or a clear is a two-item transaction on the `#meta` pattern (version compared, lost race retried from the returned item, `TransactionConflict` backed off). A no-op never creates an item.

**Tombstones.** A departed group member's row stays with `state=2`, `left_at`, a 1h TTL and the departure's roster version, so re-joins are idempotent updates and delayed duplicates of an undone join can be distinguished from news. Readers trust `state`, never the clock. "Formerly a member" is not durable.

**Rules (`chat/rules.go`).** The proto is the single vocabulary: what clients see in `Metadata.rules` is exactly what `RuleEvaluator` evaluates. Only listener rules exist today (staff-only, minimum balance in any ISO 4217 currency; a positive minimum balance is required); speaker rules are rejected. A USD minimum is compared in quarks against the USDF total; any other currency goes through OCP's `currency_codes` valuation (`balance.Client.GetTotalFiatValue`) with half a minor unit of slack, and a currency OCP cannot price is `INVALID_RULES` at creation and an evaluation error (never a pass) afterwards. Rules are immutable after creation, so `chat/cache` holds them forever. Unknown rule kind = error (admit no one), never a pass. Rules are evaluated against current state, not join-time state.

**Group management.** `StartChat` / `JoinChat` / `LeaveChat` exist and are gated by the `requireStaffForGroupManagementRPC` config flag (a staff flag, not a client-version gate — nothing in these packages gates on client version). DMs always deny join/leave. Membership is checked before rules so a re-join is a no-op. Every real transition publishes a `RosterUpdate` to both the user and chat topics. Creation validates rules, checks the creator satisfies them, moderates the title and attaches the picture *before* the record is written.

**Edit (`chat/edit.go`).** `EditChat` changes a group's title and/or picture. **Only the creator, while a member, may edit** (`Chat.PermissionsFor`: `CanEdit = isMember && IsCreator`; DMs and legacy groups with no recorded creator are never editable; a departed creator is `DENIED` until they rejoin). It is *not* behind the staff gate. Fields equal to the record are dropped first, so a no-op request (or one that sets nothing) is `OK` from the record with nothing moderated, attached, written or published. What remains runs in `StartChat`'s order — title moderation (`TITLE_MODERATED`), picture attach via `SetAsChatPicture` (`PICTURE_BLOB_NOT_ACCEPTED`), then one `Store.EditGroup` write (`GroupEdit`, nil = unchanged, SETs only the named attributes so concurrent edits of different fields never clobber; an empty edit is an error; `SetGroupPicture` remains the only way to *clear* a picture). A real change publishes **one event on the chat topic, excluding no one** (the editor's devices included), with one `MetadataUpdate` per field: `TitleChanged` / `PictureChanged` (the hydrated rendition set). `redact.ChatUpdate` passes both through.

**Permissions (`ViewerState.permissions`).** Server-computed per read from the record and membership (`Chat.PermissionsFor`), never stored, and carried on **every** `ViewerState` carrier (`ToProto(permissions)`: hydrate, mute/unmute responses, `ViewerStateChanged`; the clear-on-leave publishes with none). `hydrate` **always sets `viewer_state` for a member** (the zero record with permissions when they have written none) and **never for a non-member**, whatever record persists for them (the read is skipped for a non-member's standing). The version does **not** move when permissions change with membership (decided: a creator's leave/rejoin is announced by `RosterUpdate`s carrying fresh metadata, not by a version bump), so the same version can carry different `can_edit` before and after the viewer's own transition.

**Roster read (`chat/roster.go`).** `GetRoster` pages a chat's members most-recently-joined first (`RosterPosition`: `(joined_at desc, user_id desc)`, total so a cursor resumes strictly after a member), every page carrying the `RosterSummary`. Two shapes chosen by size, like the mute fan-out: at or below `rosterWholeReadCap` (1000) the roster is read **whole and strongly consistent** (`GetGroupRoster`, one `Query` of the base partition that returns the `#meta` item at its head; a Query is consistent per item, not as a set, so the read is verified — joined rows == `member_count` and no row version above the summary's proves no transition straddled it — and re-read up to 3× otherwise, so the page is exactly the roster at that version), sorted and sliced per page; above it, `GetGroupRosterPage` ranges the sparse `by_joined_at` GSI descending (eventually consistent, billed by the page, `limit+1` for an exact `has_more`) beside a separate summary read. The proto is written for the weaker shape and the client never learns which it got. The token (`[version][chat ID padded to 16][joined_at nanos][user ID]`) is bound to the chat and refused elsewhere. Gate is `StandingWithChat(...).CanListen` (a member, or a qualifying non-member; a preview-only viewer is `DENIED`). The whole RPC has an operator kill switch, the `disableGetRoster` constructor flag: when set, every call is refused with `UNAVAILABLE` after authorization and before any read. Pointers are hydrated for a DM's participants only, never for group members. **`Member.joined_at` / `Member.version`** are the membership row's join time and version stamp, set for group members on roster pages and on `RosterUpdate.MemberJoined` (`announcedMember`: one strongly consistent `GetGroupMemberRecords` point read after a real join/creation, so the announcement carries the record the write produced), **unset on `Metadata.members`** (the viewer's own entry needs neither, and `hydrate` reads no membership rows) and never for a DM's participants. A departure's version is the `roster_summary.version` on its `MemberLeft`, so a client merges pages against the stream per member by version.

**Mute (`chat/mute.go`).** `MuteChat` / `UnmuteChat` record the caller's mute on any chat they are a *member* of (membership alone, never the rules; DMs included), gated as `NOT_FOUND` / `DENIED` via the canonical record then `IsMember`. A timed mute must end in the future, checked in the handler rather than by proto validation because `MuteState` also appears in responses. The store's no-op-aware write decides whether anything changed; only a real change publishes `MetadataUpdate.ViewerStateChanged` on the **user topic only** (never the chat topic), and the response carries the same `ViewerState` so the calling device applies it at its version. `LeaveChat` clears the caller's mute **best effort** after the departure lands (`clearMuteOnLeave`, run on every OK leave so a retry repairs a failed clear; a failure is logged, never fails the RPC, and a real clear publishes `ViewerStateChanged` like `UnmuteChat`); the record and its version persist, and a departed member cannot mute or unmute until they rejoin. `hydrate` reads `viewer_state` for every chat on the page for a member (one `GetViewerStates` read), never for a non-member. The projection is **the record exactly, lapsed mute included**: a version names one state, so every carrier of it must agree, and whether a timed mute is still in force is the client's call against its own clock (the server applies `Mute.Active` only in the push fan-out). A member always gets a `viewer_state` (see "Permissions" above); a non-member never does. **Pushes still reach muted recipients on Android, never on iOS**: the fan-out (`messaging/push.go`, see "Push fan-out" below) splits each page's recipients into a `push.ChatRecipients{Unmuted, Muted}`; `ChatMessagePush.Send` sends the unmuted half with the badge resolver and the muted half as a payload cloned at build time with `ChatMetadata.muted = true` and **no badge bump**. `FCMPusher.SendPushesWithBadges` sees that flag and drops every `FCM_APNS` token from the send (`withoutTokenType`), so a muted recipient's iOS devices get nothing at all — iOS cannot suppress an alert APNs already has — while their Android devices get the flagged copy the client keeps quiet. The mute read has two shapes chosen once per message from `GetMutedCount`: at or below `defaultPushMutedWholeSetCap` the active set is read whole (`GetMutedUsers`) and held for every page; above it each page reads `GetMutedUsersPage` over its own `[first, last]` user range and intersects with the page (a muted record of a departed user or non-member is in the range but never a recipient). The mute lookup **fails open** (everyone gets the plain push) where the blocklist lookup fails closed, because a suppressed push would lose the delivery the flag exists to preserve. The FCM shape of the muted batch is still an alert; the client suppresses it.

**Push fan-out (`messaging/push.go`).** A group send publishes once on the chat topic and never loads the roster; the pushes it earns run detached (`pushSentMessages`) and **walk the roster in pages** (`GetGroupMembersPage`, `defaultPushPageSize` 2000, `WithPushPageSize` for tests), running the whole pipeline per page: drop the sender, `GetBlockers` for the page (fails closed for that page only), mute split, then `ChatMessagePush.Send` (its own token lookup, badge batch and FCM batches). What is the same for every recipient — the sender profile, the rendered push including any currency-name lookup, the mute shape — is resolved once per message (`messagePush.prepare`) before the walk, via the `push.Build*Push` constructors. Pages are read sequentially but **sent concurrently**: each page's send runs on its own goroutine under a slot from the `Sender`'s pool (`pushPageSlots`, `defaultPushPageConcurrency` 4, `WithPushPageConcurrency` for tests), taken by the reader before it launches the page and before it reads the next, so a walk holds at most one page more than it has in send, and the pool is **shared by every fan-out the `Sender` runs** — it is the cross-message throttle, not just per-walk parallelism. `walkGroup` waits for its launched pages before returning. A DM is a walk of one page, its inline pair, sent inline and outside the pool so a large group's walk never delays it. Budgets: `pushStepTimeout` (15s) bounds each step (setup, each page read, each page send) on its own; `pushBudget` (1 min) bounds a whole update, slot waits included. A failed page read, or a budget that runs out waiting for a slot, ends the walk (the cursor is what failed to arrive, or the page that never launched — logged); a failed page send costs that page only. The cursor is a user ID, which a future durable worker can checkpoint to resume without re-sending pages already out. Not built: a read-ahead of the next page, a once-per-message blocker set.

**`chat/cache`** caches only what is fixed at creation: DM membership (positives only), DM member lists, group rules. Group membership is never cached, and neither is viewer state: every `SetMute`/`ClearMute`/`GetViewerStates`/`GetMuted*` call passes straight through.

**Messaging storage (`messaging/dynamodb`).** `messages` uses one partition per chat (sk `#counter` / `msg#` / `evt#` / `cmid#`) so a send is a single transaction and `evt#` is a gapless, strongly consistent range for delta catch-up. `GetDelta` pages 100 at a time and returns `RESET_REQUIRED` past 1000 events. `message_pointers`: DMs use a **single `#ptrs` item per chat** with per-member attribute suffixes; groups use one `ptr#<user>` item per member. Group pointer advances are stored but never broadcast (N² fan-out). Idempotency markers (`cmid#`) carry a TTL. Reactions span three tables: `message_reactions` is chat-keyed (`agg#<seq>#<emoji hex>` per emoji with count, **per-emoji version** and a bounded sample; `meta#<seq>` holding the message's own state, today its active-emoji count) so a page of summaries is one range query — a **strongly consistent** one, so a reader who reacts and refreshes never finds their emoji absent with no version to explain it; `message_reactors` is **message-keyed** (`user#<user>#<emoji>`, one row per current reaction, deleted on remove, no tombstone) with the `by_version` LSI on `emoji_version` so an emoji's reactors page most-recent-first under a strongly consistent read; `message_self_reactions` is **viewer-keyed** (pk chat+user, sk `<seq>#<emoji hex>`, the add's version and `reacted_ts`, i.e. the viewer's own reactor entry; **groups only** — a DM's overlay is answered from the sample, so a DM writes no row and `GetSelfReactions` refuses a DM ID) so a viewer's own reactions across a page of messages — the `self_reactor` overlay for groups — are one strongly consistent range query on their own partition, billed by what the viewer reacted rather than a key probe per aggregate on the page (a GSI would be eventually consistent and outside the transaction; a prefix in `message_reactions` would double the write load on the chat partition that already takes every aggregate CAS). Every add/remove is one transaction that writes the reactor row and its viewer-keyed copy and compare-and-sets the aggregate (and the meta row when an emoji activates or empties) to the exact next state, on the `group_members` `#meta` pattern: no read-back, exact type cap and sample eviction, lost CAS retried from the returned item, transaction conflicts backed off. Reactor rows are stamped with the version that added them; that version is the ordering key and the paging cursor, never `reacted_ts`. `GetReactors` reads the aggregate version *before* the page so the page is never older than the version. A summary's **wire order** is decided at projection, not by the store: `ReactionSummary.ToProto` sorts by `messaging.ReactionLess` (count descending, ties by the emoji's UTF-8 bytes, so the order is total for a given state and identical from every backend); stores still return their own by-emoji order and nothing downstream relies on it.

**Encrypted content (`EncryptedContent`).** DMs only. `clientAllowedContent` accepts it top-level only (never as a reply body; a reply is encrypted whole), and `SendMessage` / `EditMessage` answer `ENCRYPTION_NOT_ALLOWED` for a group ID *after* the speaker gate, so a non-member is still `DENIED`. The server never reads the ciphertext: nothing inside is validated (a reply wrapped inside is never checked against the thread; a `MediaContent` wrapped inside names end-to-end encrypted blobs the sender uploaded for the DM, see `blob/`, which messaging never shares, hydrates or sees — the sender fills the ORIGINAL rendition's metadata itself), it is replyable/reactable/editable/deletable like text, except that an edit never downgrades it (encrypted → plaintext is `CANNOT_EDIT`, judged on `GetMessage`, which is strongly consistent so the check and the edit's `expected_event_sequence` guard together close the race; plaintext → encrypted is allowed), `redact.Content` passes it through (only a DM's members can reach it), and a DM push gets the generic body "Sent you a message" with the message in the payload when it fits under `maxChatPushBytes`, and only `ChatMetadata.message_id` otherwise.

**`Metadata.use_e2ee` (transitional).** Set by `hydrate` alone, per read and never stored: exactly when the chat is a DM and **every member is a staff user** (`useE2ee`; the account store's `IsStaff` per DM member across the page, resolved concurrently in `staffFlags`). Groups never carry it. Nothing else publishes DM metadata, so this is the only place the flag is decided; once E2EE launches the client is expected to ignore it and it will be deprecated.

**Permissions.** One definition lives in `chat.Access` (`chat/access.go`); the parent builds a single instance and injects it into both the chat and messaging servers, so the admission cache and its TTL are shared. Three gates: `CanListen` (member, or a non-member of a *group* who satisfies its listener rules right now — a qualifying user can preview a group before joining), `IsMember` (pointer advances, add/remove reaction: writes that are a member's alone), `CanSpeak` (sends, edits, deletes, typing: member plus listener and speaker rules). A caller already holding the canonical record uses `StandingWithChat` / `IsMemberWithChat` (`GetChat`, the mute gate, chat previews): a DM's membership is decided off the record's inline members with no membership read, a group's is still the store's. A member's read is answered on membership alone and never evaluates a rule; a non-member's read evaluates the listener rules, with a positive-only 30s admission cache per (group, user). A group with **no** listener rules admits no non-member (only a rule can admit one; the store does not require rules, so legacy groups may carry none). A DM never reaches the rules fallback. Membership is meant to track listener rules (a member who stops satisfying one gets removed) — not built yet. `GetChat` returns a group's record to any registered user; the viewer's `Standing` decides how much `hydrate` reads (no hydrated member for a non-member, no messaging state for a viewer who cannot read). DMs stay member-only. `GetChat` also takes no auth at all: the group's **public view** (`getPublicChat`, standing from `Access.PublicStanding`, off the record with nothing read), which is exactly a registered non-member's REDACTED read with no per-viewer fields; any other mode, or any DM ID, is `DENIED` before the record is read.

**Redacted reads (`redact/`, `messaging.v1.ViewMode`).** `Standing` has three levels: `IsMember`, `CanListen` (full read), `CanPreview` (redacted read: a non-member of a group that carries ≥1 listener rule, whatever the rules say of them; `CanListen` implies it). Every message read (`GetMessage`, `GetMessages`, `GetDelta`, and `GetChat` for `last_message`) carries a `view_mode`; `Standing.Reading(mode)` resolves it to denied / full / redacted and **never widens** the standing: `FULL` (proto default, legacy contract) is full or `DENIED`; `FULL_OR_REDACTED` is the most the standing allows; `REDACTED` is a placeholder for anyone who may read the chat at all, members included, and **never evaluates the rules** (`Access.Standing` skips the valuation and the admission cache), so a discovery screen pays nothing per group. Redaction happens at one chokepoint per server — `messaging.Server.present` and the last-message step of `chat.Server.hydrate` — **after** media hydration, so placeholders keep dimensions and blurhash and drop only the download URL; a message that cannot be redacted fails the read (`Internal`), never leaks. `redact.Message`/`redact.Content` are allowlist-built: text/captions/reply bodies become shape-only placeholders seeded by (chat ID, message seq); cash, system and deleted content pass through; reactions are not carried (no message read carries them). The reaction reads (`GetReactionSummary`, `GetReactionSummaries`, `GetReactors`) take no mode and answer anyone with `CanPreview`, gated by `overlayStanding` (a REDACTED-mode standing: no rule evaluated, no admission remembered); a non-member's `self_reactor` is never looked up. Pointer RPCs stay a member's. The event stream serves a redacted viewer only through a **chat preview** (see "Event delivery"): `redact.ChatUpdate` is the stream's chokepoint, walking every message an update carries (event-log mutations, a metadata refresh's `last_message`, a join's `metadata.last_message`) and passing the overlays through.

**Event delivery (`event/`).** `Bus` runs each handler on its own goroutine. The stream registry is sharded 64 ways. Two cluster namespaces: `user-events` and `chat-events` (groups only; DM updates fan out per member on the user bus, group updates publish once on the chat topic). Stream keys are prefixed because user and group IDs are both 16 bytes. A session's chat topics **follow membership and are version-gated**: a roster transition applies only if newer than the last one applied for that chat, seeded at stream open from membership records including tombstones. A reconcile sweep re-reads each streaming user's membership every 5 minutes (1s tick, 8 workers, quota-paced). Per-peer outboxes are 4096 deep and drop on overflow; a lagging stream is closed with `ErrStreamLagging`. `ForwardEvents` (cross-server, API-key authed) delivers locally only and never re-resolves ownership.

**Chat previews (`event/chat_preview.go`).** `StreamEvents` with `Params.chat_preview` set streams one **group's** `ChatUpdate`s under a `ViewMode`, and nothing else, for a **server-fixed window** (`defaultChatPreviewLifetime` 5 min, `WithChatPreviewLifetime` for tests) that ends with `STREAM_EXPIRED` whatever the client does; pongs keep it healthy, not alive. A user stream (no target) is unchanged. A DM is `DENIED` to everyone before its record is read. A preview is a **non-member's alone**: a member is `DENIED` whatever the mode (their user stream already carries the chat; this is stricter than the proto's "the server does not refuse it"). The viewer's `Standing` is resolved **once, at open**, through the shared `chat.Access` (the event server takes it at construction) with the same NOT_FOUND / DENIED rules as any read, and is trusted for the window: nothing is re-evaluated mid-stream, so a non-member who stops qualifying, or who joins, keeps the preview until it expires and is refused the next one. Registration is by the chat key alone, so previews are not in `sessions` and never move with `followMembership`; the viewer's own roster transitions are excluded from the chat topic and never appear on a preview (they ride the user stream). Every event passes `chatPreview.shape`, which drops anything that is not a `ChatUpdate` for that chat and redacts via `redact.ChatUpdate` when the reading is redacted; a shape failure ends the stream (`Internal`). Previews do not reset the badge. The proto's rate limit on opening previews is not built.

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

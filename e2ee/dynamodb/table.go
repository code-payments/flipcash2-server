package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// The e2ee store is two tables, both keyed by (pk, sk), with no index: every
// read is key-addressable and strongly consistent, which is what a key
// directory and a mailbox want.
//
//	e2ee_devices  pk = "user#<user>". One partition per user holding:
//
//	  sk = "#roster"                    The user's device roster, ONE item: a
//	                                    map of DeviceId -> the device's fixed
//	                                    record (registration ID, identity key
//	                                    and its certification, account key,
//	                                    registered_at, last_seen, capabilities,
//	                                    app install), the mailbox nonce (see
//	                                    below) and the idempotency key that
//	                                    registered it, plus a version.
//	                                    Five devices at ~300 bytes each fit one
//	                                    item, so lowest-free-id, the device cap,
//	                                    the duplicate-identity-key check and
//	                                    idempotency are one compare-and-set on
//	                                    it, GetDevices for a roster of users is
//	                                    one read per user, and a send's
//	                                    device-mismatch check is that same read.
//	                                    Registration and removal are UpdateItem
//	                                    on one map entry under a version
//	                                    condition, never a whole-item Put, so a
//	                                    concurrent last_seen touch (no version)
//	                                    is never clobbered.
//
//	  sk = "dev#<nonce>#keys"           A device's replaceable keys: signed
//	                                    prekey, last-resort KEM prekey, and
//	                                    per one-time pool its current
//	                                    generation, the number of keys
//	                                    uploaded (n) and the index of the
//	                                    next to serve (idx), plus a version
//	                                    for SetKeys' compare-and-set. A
//	                                    pool's count is n - idx.
//	  sk = "dev#<nonce>#otk#<gen>#<idx>" One one-time curve prekey at its
//	                                    upload index; the prekey id is an
//	                                    attribute (kid).
//	  sk = "dev#<nonce>#kem#<gen>#<idx>" One one-time KEM prekey (with sig).
//
//	                                    The nonce is 8 random bytes minted per
//	                                    registration and carried on the roster
//	                                    entry, so a device's keys are addressed
//	                                    by registration rather than by
//	                                    DeviceId: a reinstall under a reused id
//	                                    cannot see, or be seen through, its
//	                                    predecessor's keys. A pool is written
//	                                    under a fresh random generation and the
//	                                    keys item then flipped to it, because a
//	                                    full pool and the flip exceed a
//	                                    transaction's 100 items. Items of any
//	                                    other generation are unreachable: the
//	                                    one a flip replaced is swept at once,
//	                                    and every flip also sweeps any other
//	                                    generation under the nonce staged more
//	                                    than orphanGrace ago (written_at on
//	                                    each item), which is what a failed or
//	                                    interrupted replacement leaves behind.
//	                                    Both sweeps are best effort; the next
//	                                    flip repairs what one misses. A
//	                                    registration that never reaches the
//	                                    roster, or an unregistration whose
//	                                    detached sweep fails, still leaks its
//	                                    pools until a sweeper exists.
//	                                    Serving a prekey is Signal-Server's
//	                                    paged-pool take: one ADD of the pool's
//	                                    idx on the keys item, conditioned on
//	                                    idx < n, then a read of the key at the
//	                                    index the write returned. The write is
//	                                    the arbiter, so no two takes reserve
//	                                    one index, and nothing is read before
//	                                    it. Served keys stay in place until
//	                                    their generation is replaced and
//	                                    swept.
//
//	e2ee_devices   pk = "limit#<kind>#<key>"
//
//	  sk = "#bucket"                    One rate limit's sliding window (see
//	                                    e2ee.BucketConfig): the current
//	                                    window's index and count (cur_w,
//	                                    cur_n), the previous window's
//	                                    (prev_w, prev_n), and expires_at two
//	                                    windows out, so a key nobody is
//	                                    taking from costs nothing. A take is
//	                                    one ADD on cur_n conditioned on
//	                                    cur_w being the caller's window,
//	                                    judged on what comes back; a take
//	                                    that does not fit ADDs its cost
//	                                    back. Only the roll to a new window,
//	                                    once per key per window, reads and
//	                                    compare-and-sets. Nothing else
//	                                    contends: a key many callers hit at
//	                                    once costs each of them one write.
//
//	e2ee_mailboxes  pk = "mbox#<user>#<nonce>". One partition per registration:
//
//	  sk = "#counter"                   The last Sequence reserved (seq), and
//	                                    how many envelopes the mailbox holds
//	                                    (count) and their content bytes
//	                                    (bytes), all moved by unconditional
//	                                    ADD. A delivery reserves its positions
//	                                    with one ADD per mailbox, never a
//	                                    compare-and-set, so fan-in to one
//	                                    mailbox never contends; a delivery
//	                                    that fails after reserving leaves its
//	                                    positions as gaps, which the sequence
//	                                    contract allows. count and bytes drift
//	                                    the same way and upward only (an ack
//	                                    subtracts what it removed, best
//	                                    effort; a lapse or an expiry subtracts
//	                                    nothing), so they bound the mailbox
//	                                    from above and are read only to decide
//	                                    that it is too large. Nothing enforces
//	                                    a bound yet.
//	  sk = "env#<seq>"                  One envelope, at its position, written
//	                                    after the reservation by BatchWriteItem,
//	                                    25 to a call and the calls side by side
//	                                    (deliverConcurrency): a DM is one call.
//	                                    Plain writes, half a transaction's cost,
//	                                    with no atomicity across envelopes and
//	                                    no condition (a batch carries none; the
//	                                    position was reserved for it), as
//	                                    Signal-Server inserts per device queue:
//	                                    a call that fails partway may have
//	                                    stored some envelopes, and the caller's
//	                                    retry stores the whole call again. A
//	                                    receipt envelope carries its
//	                                    acknowledgements as a nested list. Its
//	                                    id embeds the sequence
//	                                    (e2ee.NewEnvelopeID), so an
//	                                    acknowledgement is a keyed Delete
//	                                    conditioned on the id's random tail,
//	                                    returning the old item: no index and no
//	                                    read. TTL on expires_at is the
//	                                    envelope's ExpiresAt, which the handler
//	                                    sets: the retention, or an ephemeral
//	                                    envelope's window; reads filter it too,
//	                                    since TTL deletion is lazy. Nothing is
//	                                    deduplicated: a retried send is a
//	                                    second envelope at a later position.
//
//	                                    The nonce in the key is what unregistration
//	                                    relies on: the partition is not deleted
//	                                    (it can be thousands of items), it is
//	                                    orphaned, swept best effort and expired
//	                                    by TTL, and a re-registration under the
//	                                    same DeviceId opens a new one at
//	                                    sequence 1.
const (
	userKeyPrefix    = "user#"
	rosterSK         = "#roster"
	deviceKeyPrefix  = "dev#"
	keysSKSuffix     = "#keys"
	otkSegment       = "#otk#"
	kemSegment       = "#kem#"
	limitKeyPrefix   = "limit#"
	bucketSK         = "#bucket"
	mailboxKeyPrefix = "mbox#"
	counterSK        = "#counter"
	envelopeSKPrefix = "env#"

	attrPK        = "pk"
	attrSK        = "sk"
	attrVersion   = "version"
	attrExpiresAt = "expires_at"

	// Roster item.
	attrDevices = "devices"
	// Roster entry (map) attributes.
	attrRegistrationID = "reg_id"
	attrIdentityKey    = "identity_key"
	attrIdentityKeySig = "identity_key_sig"
	attrAccountKey     = "account_key"
	attrRegisteredAt   = "registered_at"
	attrLastSeen       = "last_seen"
	attrNonce          = "nonce"
	attrIdempotencyKey = "idem"
	attrCapabilities   = "caps"
	attrAppInstall     = "push"

	// Bucket item.
	attrCurWindow  = "cur_w"
	attrCurCount   = "cur_n"
	attrPrevWindow = "prev_w"
	attrPrevCount  = "prev_n"

	// Keys item.
	attrSignedPreKeyID  = "spk_id"
	attrSignedPreKeyPub = "spk_pub"
	attrSignedPreKeySig = "spk_sig"
	attrLastResortID    = "lr_id"
	attrLastResortPub   = "lr_pub"
	attrLastResortSig   = "lr_sig"
	attrOtkGen          = "otk_gen"
	attrKemGen          = "kem_gen"
	attrOtkIdx          = "otk_idx"
	attrOtkN            = "otk_n"
	attrKemIdx          = "kem_idx"
	attrKemN            = "kem_n"

	// Pool items.
	attrKeyID     = "kid"
	attrPub       = "pub"
	attrSig       = "sig"
	attrWrittenAt = "written_at"

	// Counter item.
	attrSeq   = "seq"
	attrCount = "count"
	attrBytes = "bytes"

	// Envelope item.
	attrID            = "id"
	attrChat          = "chat"
	attrSourceUser    = "src_user"
	attrSourceDevice  = "src_device"
	attrType          = "type"
	attrContent       = "content"
	attrClientTS      = "client_ts"
	attrServerTS      = "server_ts"
	attrLowPriority   = "low_priority"
	attrContentHint   = "content_hint"
	attrReceipt       = "receipt"
	attrSourceRecord  = "source_record"
	attrWantsReceipt  = "wants_receipt"
	attrEphemeral     = "ephemeral"
	attrAcks          = "acks"
	attrAckRecipient  = "recipient"
	attrAckClientTS   = "client_ts"
	attrAddressUser   = "user"
	attrAddressDevice = "device"

	// Attempt budgets for the compare-and-set loops (a user's roster, a
	// device's keys: one item that few writers touch, so a lost race is
	// rare and an immediate retry wins). A limit's loop contends only on
	// the roll to a new window, which one taker wins and the rest then
	// take from.
	maxRosterAttempts = 8
	maxKeysAttempts   = 8
	maxBucketAttempts = 4

	// A take never contends: its reservation is an unconditional ADD. What
	// it can lose is the key it reserved, to a replacement that flipped and
	// swept the generation between the reservation and the read, and it
	// then reserves again on the new generation. Replacements are minutes
	// apart at the fastest, so a second miss is a fault, not a race.
	maxTakeAttempts = 4

	// The wait between rounds of a batch write or read that DynamoDB
	// returned partly unprocessed, and how many rounds a batch gets. A
	// partial batch is the partition over its per-second budget (a user's
	// devices share one, so two pools uploaded together can exceed it), and
	// throttling is metered per second, so the wait is a throttling wait,
	// not a compare-and-set's; the docs require backoff here, since an
	// immediate resend meets the same budget. The SDK retries a wholly
	// rejected call on its own; this is for the leftovers of an accepted one.
	maxBatchRounds = 8

	// DynamoDB limits.
	maxBatchWrite = 25

	// deliverConcurrency bounds the batches one Deliver call has in flight:
	// a DM is one batch, a full group's 250 envelopes are ten, each batch
	// spread over its own mailbox partitions.
	deliverConcurrency = 8

	// ackConcurrency bounds the parallel deletes of one AckEnvelopes call.
	ackConcurrency = 16

	// sweepTimeout bounds an unregistration's detached sweep of the
	// device's keys and mailbox.
	sweepTimeout = time.Minute
)

// The batch loops' waits (see maxBatchRounds), and the age past which a
// non-current pool generation is an orphan rather than one being staged
// (see sweepStaleGenerations; staging takes seconds, so an hour is
// generous). Variables so a test can shrink them; nothing else assigns
// them.
var (
	batchBackoffBase = 25 * time.Millisecond
	batchBackoffMax  = time.Second
	orphanGrace      = time.Hour
)

// CreateTables provisions the devices and mailboxes tables with on-demand
// billing, and TTL on expires_at on both (envelopes and dedupe-free
// buckets; pool items carry none, see sweepStaleGenerations). It is
// idempotent and blocks until both tables are ACTIVE.
func CreateTables(ctx context.Context, client *dynamodb.Client, devicesTable, mailboxesTable string) error {
	for _, table := range []string{devicesTable, mailboxesTable} {
		input := &dynamodb.CreateTableInput{
			TableName:   aws.String(table),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
			},
		}
		if _, err := client.CreateTable(ctx, input); err != nil {
			var inUse *types.ResourceInUseException
			if errors.As(err, &inUse) {
				continue // Already exists.
			}
			return err
		}
		if err := dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		}, 2*time.Minute); err != nil {
			return err
		}
	}
	if err := ensureTTL(ctx, client, devicesTable, attrExpiresAt); err != nil {
		return err
	}
	return ensureTTL(ctx, client, mailboxesTable, attrExpiresAt)
}

// ensureTTL idempotently enables DynamoDB TTL on table's attr.
func ensureTTL(ctx context.Context, client *dynamodb.Client, table, attr string) error {
	desc, err := client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(table),
	})
	if err != nil {
		return err
	}
	if d := desc.TimeToLiveDescription; d != nil {
		switch d.TimeToLiveStatus {
		case types.TimeToLiveStatusEnabled, types.TimeToLiveStatusEnabling:
			return nil
		}
	}

	_, err = client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(table),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: aws.String(attr),
		},
	})
	return err
}

// reset deletes every item from both tables, for tests.
func (s *store) reset() {
	ctx := context.Background()
	for _, table := range []string{s.devicesTable, s.mailboxesTable} {
		if err := clearTable(ctx, s.client, table); err != nil {
			panic(err)
		}
	}
}

func clearTable(ctx context.Context, client *dynamodb.Client, table string) error {
	var startKey map[string]types.AttributeValue
	for {
		out, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:            aws.String(table),
			ProjectionExpression: aws.String(attrPK + ", " + attrSK),
			ExclusiveStartKey:    startKey,
		})
		if err != nil {
			return err
		}
		keys := make([]map[string]types.AttributeValue, 0, len(out.Items))
		for _, item := range out.Items {
			keys = append(keys, map[string]types.AttributeValue{attrPK: item[attrPK], attrSK: item[attrSK]})
		}
		if err := batchDelete(ctx, client, table, keys); err != nil {
			return err
		}
		if len(out.LastEvaluatedKey) == 0 {
			return nil
		}
		startKey = out.LastEvaluatedKey
	}
}

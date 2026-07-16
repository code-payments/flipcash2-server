package dynamodb

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"

	"github.com/code-payments/flipcash2-server/blob"
)

// The blob store uses a single table, one item per blob keyed by
// pk = "blob#<id hex>". An original's derived renditions are recorded as a manifest
// on the original's own item (attrRenditions), so the whole set resolves in the one
// read that fetches the original — there is no by-parent index. The child rendition
// items still exist and carry parent_id (for ACL inheritance), but they are reached
// by their own id, never enumerated by parent. A BlobId is an opaque capability, so
// point and batch reads go straight to the partition by id and are not scoped to an
// owner.
const (
	attrPK            = "pk"
	attrParentID      = "parent_id"       // parent (ORIGINAL) id, hex; absent on ORIGINALs
	attrRendition     = "rendition"       // RenditionType, N
	attrUserID        = "user_id"         // owner id, hex
	attrState         = "state"           // blob.State, N
	attrStorageKey    = "storage_key"     // S
	attrMimeType      = "mime_type"       // S
	attrSizeBytes     = "size_bytes"      // N
	attrImageWidth    = "image_width"     // N, present only on READY images
	attrImageHeight   = "image_height"    // N, present only on READY images
	attrImageBlurhash = "image_blurhash"  // S, present only on READY images
	attrImageHasAlpha = "image_has_alpha" // BOOL, present only on READY images
	attrExpiresAt     = "expires_at"      // N, Unix seconds; TTL on non-READY blobs
	attrCreatedAt     = "created_at"      // N, Unix nanos; stamped once at creation

	attrRejectionReason = "rejection_reason" // N, present only on REJECTED blobs
	attrFlaggedCategory = "flagged_category" // N, present only on REJECTED-by-moderation blobs

	attrRenditions = "renditions" // S (JSON manifest), present only on ORIGINALs with generated renditions

	// The finalization queues are a sparse GSI over the blobs table: the queue
	// attributes are present exactly while a blob has uploaded bytes awaiting
	// processing, so only queued blobs appear in the index. There is one queue —
	// one index partition, see finalizeQueuePK — per content kind, each drained
	// by its own worker. Reaching a terminal state removes the attributes in the
	// same UpdateItem as the transition, dequeuing atomically.
	attrFinalizeQueue      = "finalize_queue"      // S, finalizeQueuePK(kind) (GSI hash); present only while queued
	attrFinalizeDueAt      = "finalize_due_at"     // N, Unix nanos (GSI range); when the task is next due
	attrFinalizeAttempts   = "finalize_attempts"   // N, failed attempts so far
	attrFinalizeEnqueuedAt = "finalize_enqueued_at" // N, Unix nanos; set on first mark, never reset (backs the max-age gauge)

	// finalizationQueueIndex is the sparse GSI backing GetDueForFinalization.
	finalizationQueueIndex = "finalization_queue"

	// finalizeQueuePrefix prefixes a queue's partition value; the content kind's
	// stable numeric value completes it (see finalizeQueuePK).
	finalizeQueuePrefix = "queue#"

	blobKeyPrefix = "blob#"

	// batchGetMaxKeys is the DynamoDB BatchGetItem per-request key limit.
	batchGetMaxKeys = 100

	// pendingBlobTTL is how long a blob record lives before DynamoDB reclaims it
	// via TTL. It is stamped at creation and cleared once the blob reaches the
	// durable READY state, so only abandoned uploads (and rejected tombstones)
	// ever expire; READY blobs persist.
	pendingBlobTTL = 7 * 24 * time.Hour
)

type store struct {
	client *dynamodb.Client
	table  string
}

// NewInDynamoDB returns a blob.Store backed by the given DynamoDB table. Use
// CreateTables to provision it.
func NewInDynamoDB(client *dynamodb.Client, table string) blob.Store {
	return &store{
		client: client,
		table:  table,
	}
}

func (s *store) CreatePending(ctx context.Context, b *blob.Blob) error {
	item := toItem(b)
	now := time.Now()
	item[attrCreatedAt] = avUnixNanos(now)
	// A never-completed reservation should not live forever; give the record a
	// TTL that Advance clears once the blob reaches READY.
	item[attrExpiresAt] = avUnix(now.Add(pendingBlobTTL))

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(s.table),
		Item:                item,
		ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return blob.ErrExists
		}
		return err
	}
	return nil
}

func (s *store) GetByID(ctx context.Context, id *blobpb.BlobId) (*blob.Blob, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key:       map[string]types.AttributeValue{attrPK: avS(blobPK(id))},
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, blob.ErrNotFound
	}
	return fromItem(out.Item)
}

func (s *store) GetByIDs(ctx context.Context, ids []*blobpb.BlobId) ([]*blob.Blob, error) {
	pks := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		pk := blobPK(id)
		if _, ok := seen[pk]; ok {
			continue
		}
		seen[pk] = struct{}{}
		pks = append(pks, pk)
	}

	res := make([]*blob.Blob, 0, len(pks))

	for start := 0; start < len(pks); start += batchGetMaxKeys {
		end := min(start+batchGetMaxKeys, len(pks))

		keys := make([]map[string]types.AttributeValue, 0, end-start)
		for _, pk := range pks[start:end] {
			keys = append(keys, map[string]types.AttributeValue{attrPK: avS(pk)})
		}

		// BatchGetItem may return UnprocessedKeys under throttling; loop until the
		// whole chunk is drained.
		for len(keys) > 0 {
			out, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					s.table: {Keys: keys},
				},
			})
			if err != nil {
				return nil, err
			}

			for _, item := range out.Responses[s.table] {
				b, err := fromItem(item)
				if err != nil {
					return nil, err
				}
				res = append(res, b)
			}

			if unprocessed, ok := out.UnprocessedKeys[s.table]; ok && len(unprocessed.Keys) > 0 {
				keys = unprocessed.Keys
			} else {
				keys = nil
			}
		}
	}

	return res, nil
}

func (s *store) AttachRenditions(ctx context.Context, id *blobpb.BlobId, refs []blob.RenditionRef) error {
	// A rendition normally shares the original's mime type, BlurHash, and alpha, which
	// are already stored on the original item; read them so marshalRenditions can omit
	// matching entries and only persist values that genuinely differ. Projecting pk too
	// distinguishes a missing item (no attributes) from an original that simply lacks
	// those fields.
	current, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.table),
		Key:                  map[string]types.AttributeValue{attrPK: avS(blobPK(id))},
		ProjectionExpression: aws.String(fmt.Sprintf("%s, %s, %s, %s", attrPK, attrMimeType, attrImageBlurhash, attrImageHasAlpha)),
	})
	if err != nil {
		return err
	}
	if len(current.Item) == 0 {
		return blob.ErrNotFound
	}

	manifest, err := marshalRenditions(refs, renditionDefaults{
		mimeType: stringAttr(current.Item, attrMimeType),
		blurhash: stringAttr(current.Item, attrImageBlurhash),
		hasAlpha: boolAttr(current.Item, attrImageHasAlpha),
	})
	if err != nil {
		return err
	}

	_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(s.table),
		Key:              map[string]types.AttributeValue{attrPK: avS(blobPK(id))},
		UpdateExpression: aws.String(fmt.Sprintf("SET %s = :manifest", attrRenditions)),
		// The original must exist; overwrite any manifest already there so a replayed
		// generation is idempotent.
		ConditionExpression:       aws.String(fmt.Sprintf("attribute_exists(%s)", attrPK)),
		ExpressionAttributeValues: map[string]types.AttributeValue{":manifest": avS(manifest)},
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return blob.ErrNotFound
		}
		return err
	}
	return nil
}

func (s *store) Advance(ctx context.Context, id *blobpb.BlobId, to blob.State, image *blob.ImageMetadata) (bool, error) {
	if to == blob.StateRejected {
		return false, blob.ErrCannotAdvanceToRejected
	}

	names := map[string]string{"#state": attrState}
	values := map[string]types.AttributeValue{
		":to":       avInt(int(to)),
		":ready":    avInt(int(blob.StateReady)),
		":rejected": avInt(int(blob.StateRejected)),
	}

	update := "SET #state = :to"
	if image != nil {
		update += fmt.Sprintf(", %s = :w, %s = :h, %s = :b, %s = :a", attrImageWidth, attrImageHeight, attrImageBlurhash, attrImageHasAlpha)
		values[":w"] = avInt(int(image.Width))
		values[":h"] = avInt(int(image.Height))
		values[":b"] = avS(image.Blurhash)
		values[":a"] = avBool(image.HasAlpha)
	}
	// READY is the durable terminal state: clear the TTL so the blob is never
	// reclaimed, and dequeue it from the finalization queue — the work is done.
	// Non-terminal records keep the TTL and expire if they never reach READY.
	if to == blob.StateReady {
		update += fmt.Sprintf(" REMOVE %s, %s, %s, %s, %s", attrExpiresAt, attrFinalizeQueue, attrFinalizeDueAt, attrFinalizeAttempts, attrFinalizeEnqueuedAt)
	}

	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(s.table),
		Key:              map[string]types.AttributeValue{attrPK: avS(blobPK(id))},
		UpdateExpression: aws.String(update),
		// Advance strictly forward and never out of a terminal state.
		ConditionExpression:       aws.String(fmt.Sprintf("attribute_exists(%s) AND #state <> :ready AND #state <> :rejected AND #state < :to", attrPK)),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		// Distinguish "no such blob" from "already at/past the target" on failure.
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			// No old item means the blob does not exist; otherwise it was already at
			// or past the target (or terminal) and advancing is an idempotent no-op.
			if len(ccf.Item) == 0 {
				return false, blob.ErrNotFound
			}
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *store) Reject(ctx context.Context, id *blobpb.BlobId, rejection *blob.RejectionMetadata) (bool, error) {
	names := map[string]string{"#state": attrState}
	values := map[string]types.AttributeValue{
		":to":       avInt(int(blob.StateRejected)),
		":ready":    avInt(int(blob.StateReady)),
		":rejected": avInt(int(blob.StateRejected)),
	}

	update := "SET #state = :to"
	if rejection != nil {
		update += fmt.Sprintf(", %s = :reason, %s = :cat", attrRejectionReason, attrFlaggedCategory)
		values[":reason"] = avInt(int(rejection.Reason))
		values[":cat"] = avInt(int(rejection.FlaggedCategory))
	}
	// Rejection is terminal: dequeue the blob from the finalization queue along
	// with the transition.
	update += fmt.Sprintf(" REMOVE %s, %s, %s, %s", attrFinalizeQueue, attrFinalizeDueAt, attrFinalizeAttempts, attrFinalizeEnqueuedAt)
	// REJECTED keeps the TTL set at creation: a rejected record is a tombstone the
	// client can read for the reason, then DynamoDB reclaims it. Only READY clears
	// the TTL (in Advance).

	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(s.table),
		Key:              map[string]types.AttributeValue{attrPK: avS(blobPK(id))},
		UpdateExpression: aws.String(update),
		// Reject only a non-terminal blob; never overwrite a terminal state.
		ConditionExpression:       aws.String(fmt.Sprintf("attribute_exists(%s) AND #state <> :ready AND #state <> :rejected", attrPK)),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		// Distinguish "no such blob" from "already terminal" on failure.
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			// No old item means the blob does not exist; otherwise it was already
			// terminal and rejecting is an idempotent no-op.
			if len(ccf.Item) == 0 {
				return false, blob.ErrNotFound
			}
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *store) MarkForFinalization(ctx context.Context, id *blobpb.BlobId, kind blob.ContentKind, nextAttemptAt time.Time) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key:       map[string]types.AttributeValue{attrPK: avS(blobPK(id))},
		// Re-marking resets the due time (and the queue partition, should the kind
		// differ) but preserves the attempt count and the original enqueue time
		// (if_not_exists), so a client re-completing cannot wipe the backoff
		// bookkeeping or hide the entry's age.
		UpdateExpression: aws.String("SET #q = :q, #due = :due, #attempts = if_not_exists(#attempts, :zero), #enq = if_not_exists(#enq, :enq)"),
		// Only a live, non-terminal blob is queued: the work behind a terminal one
		// is already done.
		ConditionExpression: aws.String(fmt.Sprintf("attribute_exists(%s) AND #state <> :ready AND #state <> :rejected", attrPK)),
		ExpressionAttributeNames: map[string]string{
			"#q":        attrFinalizeQueue,
			"#due":      attrFinalizeDueAt,
			"#attempts": attrFinalizeAttempts,
			"#enq":      attrFinalizeEnqueuedAt,
			"#state":    attrState,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":q":        avS(finalizeQueuePK(kind)),
			":due":      avUnixNanos(nextAttemptAt),
			":zero":     avInt(0),
			":enq":      avUnixNanos(time.Now()),
			":ready":    avInt(int(blob.StateReady)),
			":rejected": avInt(int(blob.StateRejected)),
		},
		// Distinguish "no such blob" from "already terminal" on failure.
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			if len(ccf.Item) == 0 {
				return blob.ErrNotFound
			}
			// The blob is terminal; marking is an idempotent no-op.
			return nil
		}
		return err
	}
	return nil
}

func (s *store) GetDueForFinalization(ctx context.Context, kind blob.ContentKind, asOf time.Time, limit int) ([]*blob.FinalizationTask, error) {
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		IndexName:              aws.String(finalizationQueueIndex),
		KeyConditionExpression: aws.String("#q = :q AND #due <= :asOf"),
		ExpressionAttributeNames: map[string]string{
			"#q":   attrFinalizeQueue,
			"#due": attrFinalizeDueAt,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":q":    avS(finalizeQueuePK(kind)),
			":asOf": avUnixNanos(asOf),
		},
		// The range key is the due time, so the query is already soonest-first.
		Limit: aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, err
	}

	tasks := make([]*blob.FinalizationTask, 0, len(out.Items))
	for _, item := range out.Items {
		idBytes, err := hex.DecodeString(strings.TrimPrefix(stringAttr(item, attrPK), blobKeyPrefix))
		if err != nil {
			return nil, fmt.Errorf("invalid %s attribute: %w", attrPK, err)
		}
		attempts, err := intAttr(item, attrFinalizeAttempts)
		if err != nil {
			return nil, err
		}
		dueAtNanos, err := uint64Attr(item, attrFinalizeDueAt)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, &blob.FinalizationTask{
			ID:            &blobpb.BlobId{Value: idBytes},
			Attempts:      uint32(attempts),
			NextAttemptAt: time.Unix(0, int64(dueAtNanos)),
		})
	}
	return tasks, nil
}

func (s *store) GetFinalizationQueueStats(ctx context.Context, kind blob.ContentKind) (*blob.FinalizationQueueStats, error) {
	// One walk of the kind's (sparse, shallow) queue partition yields both
	// gauges: the item count is the depth, and the minimum enqueue time is the
	// oldest entry. Only the enqueue attribute is projected, so the walk ships
	// almost nothing; the pagination loop is only for correctness should a
	// backlog ever exceed one page.
	stats := &blob.FinalizationQueueStats{}
	var startKey map[string]types.AttributeValue
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(s.table),
			IndexName:              aws.String(finalizationQueueIndex),
			KeyConditionExpression: aws.String("#q = :q"),
			ProjectionExpression:   aws.String("#enq"),
			ExpressionAttributeNames: map[string]string{
				"#q":   attrFinalizeQueue,
				"#enq": attrFinalizeEnqueuedAt,
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":q": avS(finalizeQueuePK(kind)),
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}

		stats.Depth += uint64(len(out.Items))
		for _, item := range out.Items {
			enqueuedAtNanos, err := uint64Attr(item, attrFinalizeEnqueuedAt)
			if err != nil {
				return nil, err
			}
			enqueuedAt := time.Unix(0, int64(enqueuedAtNanos))
			if stats.OldestEnqueuedAt.IsZero() || enqueuedAt.Before(stats.OldestEnqueuedAt) {
				stats.OldestEnqueuedAt = enqueuedAt
			}
		}

		if len(out.LastEvaluatedKey) == 0 {
			return stats, nil
		}
		startKey = out.LastEvaluatedKey
	}
}

func (s *store) ClaimForFinalization(ctx context.Context, id *blobpb.BlobId, asOf, until time.Time) (bool, error) {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(s.table),
		Key:              map[string]types.AttributeValue{attrPK: avS(blobPK(id))},
		UpdateExpression: aws.String("SET #due = :until"),
		// Claim only a blob that is still queued and still due: a dequeued blob was
		// finalized, and one pushed into the future was claimed or delayed by
		// another worker first.
		ConditionExpression: aws.String("attribute_exists(#q) AND #due <= :asOf"),
		ExpressionAttributeNames: map[string]string{
			"#q":   attrFinalizeQueue,
			"#due": attrFinalizeDueAt,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":until": avUnixNanos(until),
			":asOf":  avUnixNanos(asOf),
		},
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *store) DelayFinalization(ctx context.Context, id *blobpb.BlobId, nextAttemptAt time.Time) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(s.table),
		Key:              map[string]types.AttributeValue{attrPK: avS(blobPK(id))},
		UpdateExpression: aws.String("SET #due = :due ADD #attempts :one"),
		// A blob that left the queue (a concurrent finalize drove it terminal) has
		// nothing to reschedule.
		ConditionExpression: aws.String("attribute_exists(#q)"),
		ExpressionAttributeNames: map[string]string{
			"#q":        attrFinalizeQueue,
			"#due":      attrFinalizeDueAt,
			"#attempts": attrFinalizeAttempts,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":due": avUnixNanos(nextAttemptAt),
			":one": avInt(1),
		},
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return nil
		}
		return err
	}
	return nil
}

func toItem(b *blob.Blob) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		attrPK:         avS(blobPK(b.ID)),
		attrRendition:  avInt(int(b.Rendition)),
		attrUserID:     avS(hex.EncodeToString(b.Owner.Value)),
		attrState:      avInt(int(b.State)),
		attrStorageKey: avS(b.StorageKey),
		attrMimeType:   avS(b.MimeType),
		attrSizeBytes:  avUint64(b.SizeBytes),
	}
	if b.ParentID != nil {
		item[attrParentID] = avS(hex.EncodeToString(b.ParentID.Value))
	}
	if b.Image != nil {
		item[attrImageWidth] = avInt(int(b.Image.Width))
		item[attrImageHeight] = avInt(int(b.Image.Height))
		item[attrImageBlurhash] = avS(b.Image.Blurhash)
		item[attrImageHasAlpha] = avBool(b.Image.HasAlpha)
	}
	return item
}

func fromItem(item map[string]types.AttributeValue) (*blob.Blob, error) {
	// The id is not stored on its own; it is the pk with the type prefix stripped.
	idBytes, err := hex.DecodeString(strings.TrimPrefix(stringAttr(item, attrPK), blobKeyPrefix))
	if err != nil {
		return nil, fmt.Errorf("invalid %s attribute: %w", attrPK, err)
	}
	ownerBytes, err := hexAttr(item, attrUserID)
	if err != nil {
		return nil, err
	}
	rendition, err := intAttr(item, attrRendition)
	if err != nil {
		return nil, err
	}
	state, err := intAttr(item, attrState)
	if err != nil {
		return nil, err
	}
	sizeBytes, err := uint64Attr(item, attrSizeBytes)
	if err != nil {
		return nil, err
	}

	b := &blob.Blob{
		ID:         &blobpb.BlobId{Value: idBytes},
		Rendition:  blob.RenditionType(rendition),
		Owner:      &commonpb.UserId{Value: ownerBytes},
		State:      blob.State(state),
		StorageKey: stringAttr(item, attrStorageKey),
		MimeType:   stringAttr(item, attrMimeType),
		SizeBytes:  sizeBytes,
	}

	if _, ok := item[attrParentID]; ok {
		parentBytes, err := hexAttr(item, attrParentID)
		if err != nil {
			return nil, err
		}
		b.ParentID = &blobpb.BlobId{Value: parentBytes}
	}

	if _, ok := item[attrImageBlurhash]; ok {
		width, err := intAttr(item, attrImageWidth)
		if err != nil {
			return nil, err
		}
		height, err := intAttr(item, attrImageHeight)
		if err != nil {
			return nil, err
		}
		b.Image = &blob.ImageMetadata{
			Width:    uint32(width),
			Height:   uint32(height),
			Blurhash: stringAttr(item, attrImageBlurhash),
			HasAlpha: boolAttr(item, attrImageHasAlpha),
		}
	}

	if raw, ok := item[attrRenditions].(*types.AttributeValueMemberS); ok {
		// A rendition normally shares the original's mime type, BlurHash, and alpha,
		// stored once on the original item rather than per entry.
		def := renditionDefaults{mimeType: b.MimeType}
		if b.Image != nil {
			def.blurhash = b.Image.Blurhash
			def.hasAlpha = b.Image.HasAlpha
		}
		refs, err := unmarshalRenditions(raw.Value, def)
		if err != nil {
			return nil, err
		}
		b.Renditions = refs
	}

	if _, ok := item[attrRejectionReason]; ok {
		reason, err := intAttr(item, attrRejectionReason)
		if err != nil {
			return nil, err
		}
		b.Rejection = &blob.RejectionMetadata{Reason: blob.RejectionReason(reason)}
		// flagged_category is present only for a moderation rejection.
		if _, ok := item[attrFlaggedCategory]; ok {
			category, err := intAttr(item, attrFlaggedCategory)
			if err != nil {
				return nil, err
			}
			b.Rejection.FlaggedCategory = moderationpb.FlaggedCategory(category)
		}
	}

	return b, nil
}

// renditionRefItem is the JSON shape a manifest entry is stored as. It flattens
// the reused ImageMetadata (w/h/alpha) so the serialized form stays a compact flat
// object, and holds the blob id as hex. MimeType, BlurHash, and HasAlpha are stored
// only when they differ from the original's: each is normally shared across an
// original's renditions, so an entry that matches the original omits it (see
// marshalRenditions) and it is rehydrated from the original on read. HasAlpha is a
// pointer so an omitted value (matches the original) is distinct from a stored false
// (genuinely differs), which a bare bool with omitempty could not express; MimeType
// and BlurHash use "" as that sentinel, as neither is ever legitimately empty.
type renditionRefItem struct {
	ID         string `json:"id"`
	Rendition  int    `json:"role"`
	MimeType   string `json:"mime,omitempty"`
	SizeBytes  uint64 `json:"size"`
	StorageKey string `json:"key"`
	Width      uint32 `json:"w"`
	Height     uint32 `json:"h"`
	Blurhash   string `json:"bh,omitempty"`
	HasAlpha   *bool  `json:"a,omitempty"`
}

// renditionDefaults are the original's values that a rendition normally inherits.
// A manifest entry stores each field only when it diverges from these; the omitted
// ones are rehydrated from here on read. Bundling them keeps the two adjacent string
// fields from being transposed at a call site.
type renditionDefaults struct {
	mimeType string
	blurhash string
	hasAlpha bool
}

// marshalRenditions serializes a rendition manifest to the JSON stored under
// attrRenditions. An empty manifest serializes to "[]". A rendition normally shares
// the original's mime type, BlurHash, and alpha, so an entry matching def omits that
// field and is rehydrated from the original on read; only a field that genuinely
// differs is stored.
func marshalRenditions(refs []blob.RenditionRef, def renditionDefaults) (string, error) {
	items := make([]renditionRefItem, len(refs))
	for i, ref := range refs {
		items[i] = renditionRefItem{
			ID:         hex.EncodeToString(ref.ID.Value),
			Rendition:  int(ref.Rendition),
			SizeBytes:  ref.SizeBytes,
			StorageKey: ref.StorageKey,
		}
		if ref.MimeType != def.mimeType {
			items[i].MimeType = ref.MimeType
		}
		if ref.Image != nil {
			items[i].Width = ref.Image.Width
			items[i].Height = ref.Image.Height
			if ref.Image.Blurhash != def.blurhash {
				items[i].Blurhash = ref.Image.Blurhash
			}
			if ref.Image.HasAlpha != def.hasAlpha {
				hasAlpha := ref.Image.HasAlpha
				items[i].HasAlpha = &hasAlpha
			}
		}
	}
	encoded, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// unmarshalRenditions reverses marshalRenditions, rebuilding the manifest (with
// its reused ImageMetadata) from the stored JSON. def holds the original's values;
// an entry that omitted a field (because it matched) is rehydrated from def, while an
// entry that stored a differing value keeps that.
func unmarshalRenditions(encoded string, def renditionDefaults) ([]blob.RenditionRef, error) {
	var items []renditionRefItem
	if err := json.Unmarshal([]byte(encoded), &items); err != nil {
		return nil, fmt.Errorf("invalid rendition manifest: %w", err)
	}
	refs := make([]blob.RenditionRef, len(items))
	for i, item := range items {
		idBytes, err := hex.DecodeString(item.ID)
		if err != nil {
			return nil, fmt.Errorf("invalid rendition id in manifest: %w", err)
		}
		mimeType := item.MimeType
		if mimeType == "" {
			mimeType = def.mimeType
		}
		blurhash := item.Blurhash
		if blurhash == "" {
			blurhash = def.blurhash
		}
		hasAlpha := def.hasAlpha
		if item.HasAlpha != nil {
			hasAlpha = *item.HasAlpha
		}
		refs[i] = blob.RenditionRef{
			ID:         &blobpb.BlobId{Value: idBytes},
			Rendition:  blob.RenditionType(item.Rendition),
			MimeType:   mimeType,
			SizeBytes:  item.SizeBytes,
			StorageKey: item.StorageKey,
			Image: &blob.ImageMetadata{
				Width:    item.Width,
				Height:   item.Height,
				Blurhash: blurhash,
				HasAlpha: hasAlpha,
			},
		}
	}
	return refs, nil
}

func blobPK(id *blobpb.BlobId) string { return blobKeyPrefix + hex.EncodeToString(id.Value) }

// finalizeQueuePK is the queue partition a content kind's blobs wait in, e.g.
// "queue#1" for images. The kind's numeric value is persisted, matching how the
// other internal enums (state, rendition) are stored, so it must stay stable —
// which blob.ContentKind already guarantees.
//
// Each kind's queue is deliberately a single partition: entries are ephemeral
// (a blob dequeues as soon as it finalizes), so the partition holds only
// in-flight work, and its write budget sustains hundreds of finalizations per
// second — orders of magnitude above expected load. If a kind ever runs hot,
// shard ADDITIVELY: keep this value as shard 0 and add "queue#<kind>#1..N-1"
// beside it, so in-flight entries under the old key stay drainable and no
// migration is needed.
func finalizeQueuePK(kind blob.ContentKind) string {
	return finalizeQueuePrefix + strconv.Itoa(int(kind))
}

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avInt(v int) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.Itoa(v)}
}
func avUint64(v uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
}
func avUnix(t time.Time) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(t.Unix(), 10)}
}
func avUnixNanos(t time.Time) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(t.UnixNano(), 10)}
}
func avBool(v bool) types.AttributeValue { return &types.AttributeValueMemberBOOL{Value: v} }

func stringAttr(item map[string]types.AttributeValue, name string) string {
	if av, ok := item[name].(*types.AttributeValueMemberS); ok {
		return av.Value
	}
	return ""
}

func boolAttr(item map[string]types.AttributeValue, name string) bool {
	if av, ok := item[name].(*types.AttributeValueMemberBOOL); ok {
		return av.Value
	}
	return false
}

func hexAttr(item map[string]types.AttributeValue, name string) ([]byte, error) {
	decoded, err := hex.DecodeString(stringAttr(item, name))
	if err != nil {
		return nil, fmt.Errorf("invalid hex attribute %q: %w", name, err)
	}
	return decoded, nil
}

func intAttr(item map[string]types.AttributeValue, name string) (int, error) {
	av, ok := item[name].(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("missing or non-numeric attribute %q", name)
	}
	return strconv.Atoi(av.Value)
}

func uint64Attr(item map[string]types.AttributeValue, name string) (uint64, error) {
	av, ok := item[name].(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("missing or non-numeric attribute %q", name)
	}
	return strconv.ParseUint(av.Value, 10, 64)
}

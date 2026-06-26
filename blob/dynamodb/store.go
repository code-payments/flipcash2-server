package dynamodb

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/blob"
)

// The blob store uses a single table, one item per blob keyed by
// pk = "blob#<id hex>". The renditions_by_parent GSI (hash = parent_id) lets all
// of an ORIGINAL's server-derived renditions be queried by the original's id; it
// is sparse — only rendition items carry parent_id, so ORIGINALs never appear in
// it. A BlobId is an opaque capability, so point and batch reads go straight to
// the partition by id and are not scoped to an owner.
const (
	attrPK            = "pk"
	attrID            = "id"             // blob id, hex
	attrParentID      = "parent_id"      // parent (ORIGINAL) id, hex; absent on ORIGINALs
	attrRendition     = "rendition"      // RenditionType, N
	attrUserID        = "user_id"        // owner id, hex
	attrState         = "state"          // blob.State, N
	attrStorageKey    = "storage_key"    // S
	attrMimeType      = "mime_type"      // S
	attrSizeBytes     = "size_bytes"     // N
	attrImageWidth    = "image_width"    // N, present only on READY images
	attrImageHeight   = "image_height"   // N, present only on READY images
	attrImageBlurhash = "image_blurhash" // S, present only on READY images
	attrExpiresAt     = "expires_at"     // N, Unix seconds; TTL on non-READY blobs

	renditionsByParentGSI = "renditions_by_parent"

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
	// A never-completed reservation should not live forever; give the record a
	// TTL that Advance clears once the blob reaches READY.
	item[attrExpiresAt] = avUnix(time.Now().Add(pendingBlobTTL))

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

func (s *store) GetRenditions(ctx context.Context, parentID *blobpb.BlobId) ([]*blob.Blob, error) {
	var res []*blob.Blob

	var startKey map[string]types.AttributeValue
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(s.table),
			IndexName:              aws.String(renditionsByParentGSI),
			KeyConditionExpression: aws.String(fmt.Sprintf("%s = :parent", attrParentID)),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":parent": avS(hex.EncodeToString(parentID.Value)),
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}

		for _, item := range out.Items {
			b, err := fromItem(item)
			if err != nil {
				return nil, err
			}
			res = append(res, b)
		}

		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}

	return res, nil
}

func (s *store) Advance(ctx context.Context, id *blobpb.BlobId, to blob.State, image *blob.ImageMetadata) (bool, error) {
	names := map[string]string{"#state": attrState}
	values := map[string]types.AttributeValue{
		":to":       avInt(int(to)),
		":ready":    avInt(int(blob.StateReady)),
		":rejected": avInt(int(blob.StateRejected)),
	}

	update := "SET #state = :to"
	if image != nil {
		update += fmt.Sprintf(", %s = :w, %s = :h, %s = :b", attrImageWidth, attrImageHeight, attrImageBlurhash)
		values[":w"] = avInt(int(image.Width))
		values[":h"] = avInt(int(image.Height))
		values[":b"] = avS(image.Blurhash)
	}
	// READY is the durable terminal state: clear the TTL so the blob is never
	// reclaimed. Non-terminal and rejected records keep it and expire if they
	// never reach READY.
	if to == blob.StateReady {
		update += fmt.Sprintf(" REMOVE %s", attrExpiresAt)
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

func toItem(b *blob.Blob) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		attrPK:         avS(blobPK(b.ID)),
		attrID:         avS(hex.EncodeToString(b.ID.Value)),
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
	}
	return item
}

func fromItem(item map[string]types.AttributeValue) (*blob.Blob, error) {
	idBytes, err := hexAttr(item, attrID)
	if err != nil {
		return nil, err
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
		}
	}

	return b, nil
}

func blobPK(id *blobpb.BlobId) string { return blobKeyPrefix + hex.EncodeToString(id.Value) }

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

func stringAttr(item map[string]types.AttributeValue, name string) string {
	if av, ok := item[name].(*types.AttributeValueMemberS); ok {
		return av.Value
	}
	return ""
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

package dynamodb

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"

	"github.com/code-payments/flipcash2-server/blob"
)

// The access store uses a single ACL table, one item per access-control entry
// (ACE) keyed by pk = "blob#<id hex>" (the blob the entry is on) and
// sk = "<effect>#<perm>#<ptype>#<principal id hex>" (the effect a principal has
// on a permission). An entry's existence is the authorization, so the item
// carries no other attributes and the table has no secondary indexes. Reads are
// exact-key point gets, so the store performs no membership resolution and never
// depends on the chat subsystem. Entries are durable: unlike pending blobs they
// carry no TTL and persist until explicitly revoked.
//
// Only ALLOW effects are written today — the domain AccessStore is allow-only —
// but the effect leads the sort key so a future DENY effect lands as sibling
// entries (distinct items, point-gettable and queryable via
// begins_with(sk, "<deny>#")) with no change to this table.
const (
	attrSK = "sk" // composite "<effect>#<perm>#<ptype>#<principal id hex>", the entry

	// effectAllow is the only effect written today. It leads the sort key, ahead
	// of the permission and principal, so a future DENY effect slots in as sibling
	// entries without a migration.
	effectAllow = 1
)

type accessStore struct {
	client *dynamodb.Client
	table  string
}

// NewAccessInDynamoDB returns a blob.AccessStore backed by the given DynamoDB
// ACL table. Use CreateTables to provision it.
func NewAccessInDynamoDB(client *dynamodb.Client, table string) blob.AccessStore {
	return &accessStore{
		client: client,
		table:  table,
	}
}

func (s *accessStore) Grant(ctx context.Context, g *blob.Grant) error {
	if err := g.Validate(); err != nil {
		return err
	}

	// The entry is keyless beyond its identity, so an unconditional PutItem is the
	// idempotent grant: re-granting overwrites the same item with itself.
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      aceKey(g.BlobID, g.Principal, g.Permission),
	})
	return err
}

func (s *accessStore) HasGrant(ctx context.Context, blobID *blobpb.BlobId, p blob.Principal, perm blob.Permission) (bool, error) {
	if err := (&blob.Grant{BlobID: blobID, Principal: p, Permission: perm}).Validate(); err != nil {
		return false, err
	}

	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.table),
		Key:                  aceKey(blobID, p, perm),
		ProjectionExpression: aws.String(attrPK),
	})
	if err != nil {
		return false, err
	}
	return len(out.Item) > 0, nil
}

func (s *accessStore) Revoke(ctx context.Context, blobID *blobpb.BlobId, p blob.Principal, perm blob.Permission) error {
	if err := (&blob.Grant{BlobID: blobID, Principal: p, Permission: perm}).Validate(); err != nil {
		return err
	}

	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.table),
		Key:       aceKey(blobID, p, perm),
	})
	return err
}

func (s *accessStore) reset() {
	if err := clearTable(context.Background(), s.client, s.table, []string{attrPK, attrSK}); err != nil {
		panic(err)
	}
}

// aceKey builds the full primary key (pk + sk) for an access-control entry. It is
// the item itself on a Put (an entry has no other attributes) and the lookup key
// on a Get/Delete.
func aceKey(blobID *blobpb.BlobId, p blob.Principal, perm blob.Permission) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK: avS(blobPK(blobID)),
		attrSK: avS(aceSK(p, perm)),
	}
}

// aceSK encodes the sort key for an access-control entry: the effect, then the
// permission, principal type, and principal id. Only effectAllow is written
// today; leading with the effect leaves room for a future DENY without a
// migration.
func aceSK(p blob.Principal, perm blob.Permission) string {
	return fmt.Sprintf("%d#%d#%d#%s", effectAllow, int(perm), int(p.Type), hex.EncodeToString(p.ID))
}

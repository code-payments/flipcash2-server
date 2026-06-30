package memory

import (
	"context"
	"fmt"
	"sync"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"

	"github.com/code-payments/flipcash2-server/blob"
)

type accessMemory struct {
	sync.Mutex

	// grants is the set of present grant keys; a grant carries no state beyond
	// its existence, so membership in this set is the authorization.
	grants map[string]struct{}
}

// NewInMemoryAccessStore returns an in-memory blob.AccessStore for tests.
func NewInMemoryAccessStore() blob.AccessStore {
	return &accessMemory{
		grants: make(map[string]struct{}),
	}
}

func (m *accessMemory) Grant(_ context.Context, g *blob.Grant) error {
	if err := g.Validate(); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	m.grants[grantKey(g.BlobID, g.Principal, g.Permission)] = struct{}{}
	return nil
}

func (m *accessMemory) HasGrant(_ context.Context, blobID *blobpb.BlobId, p blob.Principal, perm blob.Permission) (bool, error) {
	if err := (&blob.Grant{BlobID: blobID, Principal: p, Permission: perm}).Validate(); err != nil {
		return false, err
	}

	m.Lock()
	defer m.Unlock()

	_, ok := m.grants[grantKey(blobID, p, perm)]
	return ok, nil
}

func (m *accessMemory) Revoke(_ context.Context, blobID *blobpb.BlobId, p blob.Principal, perm blob.Permission) error {
	if err := (&blob.Grant{BlobID: blobID, Principal: p, Permission: perm}).Validate(); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	delete(m.grants, grantKey(blobID, p, perm))
	return nil
}

func (m *accessMemory) reset() {
	m.Lock()
	defer m.Unlock()

	m.grants = make(map[string]struct{})
}

// grantKey derives a unique map key for the (blob, principal, permission)
// triple. The permission and principal type are scalar values and the id bytes
// are length-delimited (via the %q on the raw bytes), so no two distinct triples
// collide.
func grantKey(blobID *blobpb.BlobId, p blob.Principal, perm blob.Permission) string {
	return fmt.Sprintf("%q|%d|%d|%q", blobID.Value, int(perm), int(p.Type), p.ID)
}

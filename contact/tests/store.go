package tests

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/contact"
)

// CreateUserFunc creates a user and returns their UserId.
type CreateUserFunc func(t *testing.T) *commonpb.UserId

func RunStoreTests(t *testing.T, s contact.Store, createUser CreateUserFunc, teardown func()) {
	for _, tf := range []func(t *testing.T, s contact.Store, createUser CreateUserFunc){
		testStore_GetChecksum_Empty,
		testStore_ApplyDelta_FirstUpload,
		testStore_ApplyDelta_RetryIsIdempotent,
		testStore_ApplyDelta_ChecksumDrift,
		testStore_ApplyDelta_TooManyContacts,
		testStore_Replace_BasicAndIdempotent,
		testStore_Replace_TooManyContacts,
		testStore_DeltaAfterReplace,
		testStore_GetHashes_AfterReplace,
		testStore_GetHashes_EmptyReplaceKeepsRow,
		testStore_GetHashes_AfterDelta,
		testStore_GetUserIdsByPhoneHash,
	} {
		tf(t, s, createUser)
		teardown()
	}
}

func testStore_GetChecksum_Empty(t *testing.T, s contact.Store, createUser CreateUserFunc) {
	ctx := context.Background()
	userID := createUser(t)

	_, err := s.GetChecksum(ctx, userID)
	require.ErrorIs(t, err, contact.ErrNotFound)

	_, err = s.GetHashes(ctx, userID)
	require.ErrorIs(t, err, contact.ErrNotFound)
}

func testStore_ApplyDelta_FirstUpload(t *testing.T, s contact.Store, createUser CreateUserFunc) {
	ctx := context.Background()
	userID := createUser(t)

	zero := contact.ZeroChecksum()
	h1 := hash("a")
	h2 := hash("b")
	newChecksum := xor(zero, h1, h2)

	require.NoError(t, s.ApplyDelta(ctx, userID, []*commonpb.Hash{h1, h2}, nil, zero, newChecksum))

	got, err := s.GetChecksum(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, newChecksum.Value, got.Value)
}

func testStore_ApplyDelta_RetryIsIdempotent(t *testing.T, s contact.Store, createUser CreateUserFunc) {
	ctx := context.Background()
	userID := createUser(t)

	zero := contact.ZeroChecksum()
	h1 := hash("a")
	h2 := hash("b")
	newChecksum := xor(zero, h1, h2)

	require.NoError(t, s.ApplyDelta(ctx, userID, []*commonpb.Hash{h1, h2}, nil, zero, newChecksum))
	// Replaying the same request with the original old_checksum should
	// be a no-op (the stored checksum already equals new_checksum).
	require.NoError(t, s.ApplyDelta(ctx, userID, []*commonpb.Hash{h1, h2}, nil, zero, newChecksum))

	got, err := s.GetChecksum(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, newChecksum.Value, got.Value)
}

func testStore_ApplyDelta_ChecksumDrift(t *testing.T, s contact.Store, createUser CreateUserFunc) {
	ctx := context.Background()
	userID := createUser(t)

	zero := contact.ZeroChecksum()
	h1 := hash("a")
	checksumAfterH1 := xor(zero, h1)

	require.NoError(t, s.ApplyDelta(ctx, userID, []*commonpb.Hash{h1}, nil, zero, checksumAfterH1))

	// Apply a delta whose old_checksum is wrong and whose new_checksum
	// is also not the stored one — should drift.
	bogusOld := hash("wrong-old")
	bogusNew := hash("wrong-new")
	require.ErrorIs(t,
		s.ApplyDelta(ctx, userID, []*commonpb.Hash{hash("c")}, nil, bogusOld, bogusNew),
		contact.ErrChecksumDrift,
	)

	got, err := s.GetChecksum(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, checksumAfterH1.Value, got.Value)
}

func testStore_ApplyDelta_TooManyContacts(t *testing.T, s contact.Store, createUser CreateUserFunc) {
	ctx := context.Background()
	userID := createUser(t)

	// Bulk-seed to the cap via Replace, then attempt a delta that would push
	// it over.
	hashes := make([]*commonpb.Hash, contact.MaxContactsPerUser)
	for i := range hashes {
		hashes[i] = hash(string(rune(i)) + "-seed")
	}
	checksum := xor(append([]*commonpb.Hash{contact.ZeroChecksum()}, hashes...)...)
	require.NoError(t, s.Replace(ctx, userID, hashes, checksum))

	extra := hash("one-too-many")
	newChecksum := xor(checksum, extra)

	require.ErrorIs(t,
		s.ApplyDelta(ctx, userID, []*commonpb.Hash{extra}, nil, checksum, newChecksum),
		contact.ErrTooManyContacts,
	)

	got, err := s.GetChecksum(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, checksum.Value, got.Value)
}

func testStore_Replace_BasicAndIdempotent(t *testing.T, s contact.Store, createUser CreateUserFunc) {
	ctx := context.Background()
	userID := createUser(t)

	h1 := hash("a")
	h2 := hash("b")
	checksum := xor(contact.ZeroChecksum(), h1, h2)

	require.NoError(t, s.Replace(ctx, userID, []*commonpb.Hash{h1, h2}, checksum))

	got, err := s.GetChecksum(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, checksum.Value, got.Value)

	// Replacing again with the same set should succeed and yield the same
	// stored checksum.
	require.NoError(t, s.Replace(ctx, userID, []*commonpb.Hash{h1, h2}, checksum))
	got, err = s.GetChecksum(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, checksum.Value, got.Value)

	// Replacing with a completely different set should atomically swap.
	h3 := hash("c")
	newChecksum := xor(contact.ZeroChecksum(), h3)
	require.NoError(t, s.Replace(ctx, userID, []*commonpb.Hash{h3}, newChecksum))
	got, err = s.GetChecksum(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, newChecksum.Value, got.Value)
}

func testStore_Replace_TooManyContacts(t *testing.T, s contact.Store, createUser CreateUserFunc) {
	ctx := context.Background()
	userID := createUser(t)

	hashes := make([]*commonpb.Hash, contact.MaxContactsPerUser+1)
	for i := range hashes {
		hashes[i] = hash(string(rune(i)) + "-overflow")
	}
	checksum := xor(append([]*commonpb.Hash{contact.ZeroChecksum()}, hashes...)...)

	require.ErrorIs(t, s.Replace(ctx, userID, hashes, checksum), contact.ErrTooManyContacts)

	// The store should be untouched — no contact list row was created.
	_, err := s.GetChecksum(ctx, userID)
	require.ErrorIs(t, err, contact.ErrNotFound)
}

func testStore_DeltaAfterReplace(t *testing.T, s contact.Store, createUser CreateUserFunc) {
	ctx := context.Background()
	userID := createUser(t)

	h1 := hash("a")
	h2 := hash("b")
	afterReplace := xor(contact.ZeroChecksum(), h1, h2)
	require.NoError(t, s.Replace(ctx, userID, []*commonpb.Hash{h1, h2}, afterReplace))

	// Remove h1, add h3 — the new checksum is the post-replace XOR'd
	// with both transitions.
	h3 := hash("c")
	afterDelta := xor(afterReplace, h1, h3)
	require.NoError(t, s.ApplyDelta(
		ctx, userID,
		[]*commonpb.Hash{h3}, []*commonpb.Hash{h1},
		afterReplace, afterDelta,
	))

	got, err := s.GetChecksum(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, afterDelta.Value, got.Value)
}

func testStore_GetHashes_AfterReplace(t *testing.T, s contact.Store, createUser CreateUserFunc) {
	ctx := context.Background()
	userID := createUser(t)

	h1 := hash("a")
	h2 := hash("b")
	h3 := hash("c")
	checksum := xor(contact.ZeroChecksum(), h1, h2, h3)

	require.NoError(t, s.Replace(ctx, userID, []*commonpb.Hash{h1, h2, h3}, checksum))

	got, err := s.GetHashes(ctx, userID)
	require.NoError(t, err)
	require.ElementsMatch(t, hashValues(h1, h2, h3), hashValues(got...))
}

func testStore_GetHashes_EmptyReplaceKeepsRow(t *testing.T, s contact.Store, createUser CreateUserFunc) {
	ctx := context.Background()
	userID := createUser(t)

	// Replace with an empty set — the parent row should exist (with the zero
	// checksum) but contain no entries.
	require.NoError(t, s.Replace(ctx, userID, nil, contact.ZeroChecksum()))

	got, err := s.GetHashes(ctx, userID)
	require.NoError(t, err)
	require.Empty(t, got)

	// GetChecksum should also return the stored zero checksum, not ErrNotFound.
	checksum, err := s.GetChecksum(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, make([]byte, contact.ChecksumSize), checksum.Value)
}

func testStore_GetHashes_AfterDelta(t *testing.T, s contact.Store, createUser CreateUserFunc) {
	ctx := context.Background()
	userID := createUser(t)

	h1 := hash("a")
	h2 := hash("b")
	h3 := hash("c")
	h4 := hash("d")

	// Seed with {h1, h2} via the first delta.
	afterFirst := xor(contact.ZeroChecksum(), h1, h2)
	require.NoError(t, s.ApplyDelta(
		ctx, userID,
		[]*commonpb.Hash{h1, h2}, nil,
		contact.ZeroChecksum(), afterFirst,
	))

	got, err := s.GetHashes(ctx, userID)
	require.NoError(t, err)
	require.ElementsMatch(t, hashValues(h1, h2), hashValues(got...))

	// Remove h1, add h3 and h4.
	afterSecond := xor(afterFirst, h1, h3, h4)
	require.NoError(t, s.ApplyDelta(
		ctx, userID,
		[]*commonpb.Hash{h3, h4}, []*commonpb.Hash{h1},
		afterFirst, afterSecond,
	))

	got, err = s.GetHashes(ctx, userID)
	require.NoError(t, err)
	require.ElementsMatch(t, hashValues(h2, h3, h4), hashValues(got...))
}

func testStore_GetUserIdsByPhoneHash(t *testing.T, s contact.Store, createUser CreateUserFunc) {
	ctx := context.Background()

	h1 := hash("a")
	h2 := hash("b")
	h3 := hash("c")

	// Unknown hash and empty store returns nothing.
	got, err := s.GetUserIdsByPhoneHash(ctx, h1)
	require.NoError(t, err)
	require.Empty(t, got)

	userA := createUser(t)
	userB := createUser(t)
	userC := createUser(t)

	// userA has {h1, h2}, userB has {h1, h3}, userC has {h2} only.
	require.NoError(t, s.Replace(ctx, userA, []*commonpb.Hash{h1, h2}, xor(contact.ZeroChecksum(), h1, h2)))
	require.NoError(t, s.Replace(ctx, userB, []*commonpb.Hash{h1, h3}, xor(contact.ZeroChecksum(), h1, h3)))
	require.NoError(t, s.Replace(ctx, userC, []*commonpb.Hash{h2}, xor(contact.ZeroChecksum(), h2)))

	got, err = s.GetUserIdsByPhoneHash(ctx, h1)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{userA.Value, userB.Value}, userIdValues(got))

	got, err = s.GetUserIdsByPhoneHash(ctx, h2)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{userA.Value, userC.Value}, userIdValues(got))

	got, err = s.GetUserIdsByPhoneHash(ctx, h3)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{userB.Value}, userIdValues(got))

	got, err = s.GetUserIdsByPhoneHash(ctx, hash("unknown"))
	require.NoError(t, err)
	require.Empty(t, got)

	// Removing h1 from userA via delta should leave only userB matching h1.
	newChecksum := xor(xor(contact.ZeroChecksum(), h1, h2), h1)
	require.NoError(t, s.ApplyDelta(
		ctx, userA,
		nil, []*commonpb.Hash{h1},
		xor(contact.ZeroChecksum(), h1, h2), newChecksum,
	))

	got, err = s.GetUserIdsByPhoneHash(ctx, h1)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{userB.Value}, userIdValues(got))
}

func userIdValues(ids []*commonpb.UserId) [][]byte {
	out := make([][]byte, len(ids))
	for i, id := range ids {
		out[i] = id.Value
	}
	return out
}

func hashValues(hashes ...*commonpb.Hash) [][]byte {
	out := make([][]byte, len(hashes))
	for i, h := range hashes {
		out[i] = h.Value
	}
	return out
}

func hash(s string) *commonpb.Hash {
	sum := sha256.Sum256([]byte(s))
	return &commonpb.Hash{Value: sum[:]}
}

func xor(hashes ...*commonpb.Hash) *commonpb.Hash {
	out := make([]byte, contact.ChecksumSize)
	for _, h := range hashes {
		for i := range contact.ChecksumSize {
			out[i] ^= h.Value[i]
		}
	}
	return &commonpb.Hash{Value: out}
}

package tests

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	contactpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/contact/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/contact"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/testutil"
)

var serverTestPepper = []byte("test-pepper")

func RunServerTests(t *testing.T, accounts account.Store, profiles profile.Store, s contact.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, accounts account.Store, profiles profile.Store, s contact.Store){
		testServer_CheckSync,
		testServer_DeltaUpload_Success,
		testServer_DeltaUpload_Mismatch,
		testServer_DeltaUpload_Drift,
		testServer_FullUpload_Success,
		testServer_FullUpload_Mismatch,
		testServer_Unauthorized,
		testServer_Unregistered,
		testServer_GetFlipcashContacts_NotFound,
		testServer_GetFlipcashContacts_ChecksumDrift,
		testServer_GetFlipcashContacts_OK,
	} {
		tf(t, accounts, profiles, s)
		teardown()
	}
}

type serverFixture struct {
	t      *testing.T
	client contactpb.ContactListClient
	keys   model.KeyPair
	userID *commonpb.UserId
	store  contact.Store
}

func newServerFixture(t *testing.T, accounts account.Store, profiles profile.Store, store contact.Store) *serverFixture {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))
	server := contact.NewServer(log, authz, accounts, profiles, store, serverTestPepper)

	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()

	_, err := accounts.Bind(ctx, userID, keys.Proto())
	require.NoError(t, err)
	require.NoError(t, accounts.SetRegistrationFlag(ctx, userID, true))

	cc := testutil.RunGRPCServer(t, log,
		testutil.WithService(func(s *grpc.Server) {
			contactpb.RegisterContactListServer(s, server)
		}),
	)
	return &serverFixture{
		t:      t,
		client: contactpb.NewContactListClient(cc),
		keys:   keys,
		userID: userID,
		store:  store,
	}
}

func testServer_CheckSync(t *testing.T, accounts account.Store, profiles profile.Store, store contact.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles, store)

	// No state yet — server checksum is zero, OUT_OF_SYNC vs a non-zero
	// client checksum.
	clientChecksum := phoneSha256("+12223334444")
	req := &contactpb.CheckSyncRequest{ClientChecksum: clientChecksum}
	require.NoError(t, f.keys.Auth(req, &req.Auth))

	resp, err := f.client.CheckSync(ctx, req)
	require.NoError(t, err)
	require.Equal(t, contactpb.CheckSyncResponse_OUT_OF_SYNC, resp.Result)
	require.Equal(t, zeroChecksum(), resp.ServerChecksum.Value)

	// Seed the store so the server checksum matches what the client sends.
	require.NoError(t, store.Replace(ctx, f.userID, []*commonpb.Hash{hmacHash("+12223334444")}, clientChecksum))

	req = &contactpb.CheckSyncRequest{ClientChecksum: clientChecksum}
	require.NoError(t, f.keys.Auth(req, &req.Auth))
	resp, err = f.client.CheckSync(ctx, req)
	require.NoError(t, err)
	require.Equal(t, contactpb.CheckSyncResponse_OK, resp.Result)
	require.Equal(t, clientChecksum.Value, resp.ServerChecksum.Value)
}

func testServer_DeltaUpload_Success(t *testing.T, accounts account.Store, profiles profile.Store, store contact.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles, store)

	adds := []*phonepb.PhoneNumber{
		{Value: "+12223334444"},
		{Value: "+15556667777"},
	}
	zero := contact.ZeroChecksum()
	newChecksum := xorChecksums(zero, phoneSha256("+12223334444"), phoneSha256("+15556667777"))

	req := &contactpb.DeltaUploadRequest{
		Adds:        adds,
		OldChecksum: zero,
		NewChecksum: newChecksum,
	}
	require.NoError(t, f.keys.Auth(req, &req.Auth))

	resp, err := f.client.DeltaUpload(ctx, req)
	require.NoError(t, err)
	require.Equal(t, contactpb.DeltaUploadResponse_OK, resp.Result)

	stored, err := store.GetChecksum(ctx, f.userID)
	require.NoError(t, err)
	require.Equal(t, newChecksum.Value, stored.Value)
}

func testServer_DeltaUpload_Mismatch(t *testing.T, accounts account.Store, profiles profile.Store, store contact.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles, store)

	// Client claims a new_checksum that does not match the XOR-derivation
	// from old_checksum + adds. Server should reject with CHECKSUM_MISMATCH
	// and leave the store untouched.
	bogusNewChecksum := &commonpb.Hash{Value: make([]byte, contact.ChecksumSize)}
	for i := range bogusNewChecksum.Value {
		bogusNewChecksum.Value[i] = 0xAB
	}
	req := &contactpb.DeltaUploadRequest{
		Adds:        []*phonepb.PhoneNumber{{Value: "+12223334444"}},
		OldChecksum: contact.ZeroChecksum(),
		NewChecksum: bogusNewChecksum,
	}
	require.NoError(t, f.keys.Auth(req, &req.Auth))

	resp, err := f.client.DeltaUpload(ctx, req)
	require.NoError(t, err)
	require.Equal(t, contactpb.DeltaUploadResponse_CHECKSUM_MISMATCH, resp.Result)

	_, err = store.GetChecksum(ctx, f.userID)
	require.ErrorIs(t, err, contact.ErrNotFound)
}

func testServer_DeltaUpload_Drift(t *testing.T, accounts account.Store, profiles profile.Store, store contact.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles, store)

	// Seed the store to a known non-zero checksum.
	initial := phoneSha256("+19998887777")
	require.NoError(t, store.Replace(ctx, f.userID, []*commonpb.Hash{hmacHash("+19998887777")}, initial))

	// Client thinks the prior state is zero — but server has `initial`.
	// The delta is internally consistent (XOR-derivable), but the CAS
	// fails because stored != old_checksum and stored != new_checksum.
	zero := contact.ZeroChecksum()
	adds := []*phonepb.PhoneNumber{{Value: "+12223334444"}}
	newChecksum := xorChecksums(zero, phoneSha256("+12223334444"))

	req := &contactpb.DeltaUploadRequest{
		Adds:        adds,
		OldChecksum: zero,
		NewChecksum: newChecksum,
	}
	require.NoError(t, f.keys.Auth(req, &req.Auth))

	resp, err := f.client.DeltaUpload(ctx, req)
	require.NoError(t, err)
	require.Equal(t, contactpb.DeltaUploadResponse_CHECKSUM_DRIFT, resp.Result)

	stored, err := store.GetChecksum(ctx, f.userID)
	require.NoError(t, err)
	require.Equal(t, initial.Value, stored.Value)
}

func testServer_FullUpload_Success(t *testing.T, accounts account.Store, profiles profile.Store, store contact.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles, store)

	// Stream two messages — the second carries the expected_checksum.
	phones1 := []*phonepb.PhoneNumber{{Value: "+12223334444"}}
	phones2 := []*phonepb.PhoneNumber{{Value: "+15556667777"}}
	expected := xorChecksums(contact.ZeroChecksum(), phoneSha256("+12223334444"), phoneSha256("+15556667777"))

	stream, err := f.client.FullUpload(ctx)
	require.NoError(t, err)

	r1 := &contactpb.FullUploadRequest{Phones: phones1, ExpectedChecksum: contact.ZeroChecksum()}
	require.NoError(t, f.keys.Auth(r1, &r1.Auth))
	require.NoError(t, stream.Send(r1))

	r2 := &contactpb.FullUploadRequest{Phones: phones2, ExpectedChecksum: expected}
	require.NoError(t, f.keys.Auth(r2, &r2.Auth))
	require.NoError(t, stream.Send(r2))

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, contactpb.FullUploadResponse_OK, resp.Result)

	stored, err := store.GetChecksum(ctx, f.userID)
	require.NoError(t, err)
	require.Equal(t, expected.Value, stored.Value)
}

func testServer_FullUpload_Mismatch(t *testing.T, accounts account.Store, profiles profile.Store, store contact.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles, store)

	// Send phones that don't XOR to the supplied expected_checksum.
	bogus := &commonpb.Hash{Value: make([]byte, contact.ChecksumSize)}
	for i := range bogus.Value {
		bogus.Value[i] = 0xCD
	}

	stream, err := f.client.FullUpload(ctx)
	require.NoError(t, err)

	r := &contactpb.FullUploadRequest{
		Phones:           []*phonepb.PhoneNumber{{Value: "+12223334444"}},
		ExpectedChecksum: bogus,
	}
	require.NoError(t, f.keys.Auth(r, &r.Auth))
	require.NoError(t, stream.Send(r))

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, contactpb.FullUploadResponse_CHECKSUM_MISMATCH, resp.Result)

	// Store should be untouched.
	_, err = store.GetChecksum(ctx, f.userID)
	require.ErrorIs(t, err, contact.ErrNotFound)
}

func testServer_Unauthorized(t *testing.T, accounts account.Store, profiles profile.Store, store contact.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles, store)

	other := model.MustGenerateKeyPair()

	req := &contactpb.CheckSyncRequest{ClientChecksum: contact.ZeroChecksum()}
	require.NoError(t, other.Auth(req, &req.Auth))

	_, err := f.client.CheckSync(ctx, req)
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	dreq := &contactpb.DeltaUploadRequest{
		OldChecksum: contact.ZeroChecksum(),
		NewChecksum: contact.ZeroChecksum(),
	}
	require.NoError(t, other.Auth(dreq, &dreq.Auth))
	_, err = f.client.DeltaUpload(ctx, dreq)
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// FullUpload auth check happens once we receive the first message —
	// the stream send may succeed but CloseAndRecv surfaces the error.
	stream, err := f.client.FullUpload(ctx)
	require.NoError(t, err)
	freq := &contactpb.FullUploadRequest{
		Phones:           []*phonepb.PhoneNumber{{Value: "+12223334444"}},
		ExpectedChecksum: phoneSha256("+12223334444"),
	}
	require.NoError(t, other.Auth(freq, &freq.Auth))
	sendErr := stream.Send(freq)
	if sendErr != nil && sendErr != io.EOF {
		t.Fatalf("stream.Send: unexpected error %v", sendErr)
	}
	_, err = stream.CloseAndRecv()
	require.Error(t, err)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

func testServer_Unregistered(t *testing.T, accounts account.Store, profiles profile.Store, store contact.Store) {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))
	server := contact.NewServer(log, authz, accounts, profiles, store, serverTestPepper)

	// Bind a user without setting the registration flag.
	userID := model.MustGenerateUserID()
	keys := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, userID, keys.Proto())
	require.NoError(t, err)

	cc := testutil.RunGRPCServer(t, log,
		testutil.WithService(func(s *grpc.Server) {
			contactpb.RegisterContactListServer(s, server)
		}),
	)
	client := contactpb.NewContactListClient(cc)

	// CheckSync → DENIED
	checkReq := &contactpb.CheckSyncRequest{ClientChecksum: contact.ZeroChecksum()}
	require.NoError(t, keys.Auth(checkReq, &checkReq.Auth))
	checkResp, err := client.CheckSync(ctx, checkReq)
	require.NoError(t, err)
	require.Equal(t, contactpb.CheckSyncResponse_DENIED, checkResp.Result)

	// DeltaUpload → DENIED
	deltaReq := &contactpb.DeltaUploadRequest{
		OldChecksum: contact.ZeroChecksum(),
		NewChecksum: contact.ZeroChecksum(),
	}
	require.NoError(t, keys.Auth(deltaReq, &deltaReq.Auth))
	deltaResp, err := client.DeltaUpload(ctx, deltaReq)
	require.NoError(t, err)
	require.Equal(t, contactpb.DeltaUploadResponse_DENIED, deltaResp.Result)

	// FullUpload → DENIED (returned in-stream via SendAndClose).
	stream, err := client.FullUpload(ctx)
	require.NoError(t, err)
	freq := &contactpb.FullUploadRequest{
		Phones:           []*phonepb.PhoneNumber{{Value: "+12223334444"}},
		ExpectedChecksum: phoneSha256("+12223334444"),
	}
	require.NoError(t, keys.Auth(freq, &freq.Auth))
	require.NoError(t, stream.Send(freq))
	fullResp, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, contactpb.FullUploadResponse_DENIED, fullResp.Result)
}

func testServer_GetFlipcashContacts_NotFound(t *testing.T, accounts account.Store, profiles profile.Store, store contact.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles, store)

	// No FullUpload / DeltaUpload yet — no contact list row.
	req := &contactpb.GetFlipcashContactsRequest{Checksum: contact.ZeroChecksum()}
	require.NoError(t, f.keys.Auth(req, &req.Auth))

	stream, err := f.client.GetFlipcashContacts(ctx, req)
	require.NoError(t, err)
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, contactpb.GetFlipcashContactsResponse_NOT_FOUND, resp.Result)

	// Stream ends.
	_, err = stream.Recv()
	require.Equal(t, io.EOF, err)
}

func testServer_GetFlipcashContacts_ChecksumDrift(t *testing.T, accounts account.Store, profiles profile.Store, store contact.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles, store)

	// Seed the store with a known checksum.
	stored := phoneSha256("+19998887777")
	require.NoError(t, store.Replace(ctx, f.userID, []*commonpb.Hash{hmacHash("+19998887777")}, stored))

	// Client sends a different checksum.
	req := &contactpb.GetFlipcashContactsRequest{Checksum: contact.ZeroChecksum()}
	require.NoError(t, f.keys.Auth(req, &req.Auth))

	stream, err := f.client.GetFlipcashContacts(ctx, req)
	require.NoError(t, err)
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, contactpb.GetFlipcashContactsResponse_CHECKSUM_DRIFT, resp.Result)

	_, err = stream.Recv()
	require.Equal(t, io.EOF, err)
}

func testServer_GetFlipcashContacts_OK(t *testing.T, accounts account.Store, profiles profile.Store, store contact.Store) {
	ctx := context.Background()
	f := newServerFixture(t, accounts, profiles, store)

	// Set up: 3 contacts in caller's list; 2 of them are Flipcash users.
	phoneA := "+11111111111"
	phoneB := "+12222222222"
	phoneC := "+13333333333"
	phoneD := "+14444444444" // a Flipcash user *not* in the caller's contacts; must not leak.

	hashA := hmacHash(phoneA)
	hashB := hmacHash(phoneB)
	hashC := hmacHash(phoneC)

	// phoneA and phoneB are linked to other Flipcash users.
	flipcashUserA := model.MustGenerateUserID()
	flipcashUserB := model.MustGenerateUserID()
	flipcashUserD := model.MustGenerateUserID()
	_, err := accounts.Bind(ctx, flipcashUserA, model.MustGenerateKeyPair().Proto())
	require.NoError(t, err)
	_, err = accounts.Bind(ctx, flipcashUserB, model.MustGenerateKeyPair().Proto())
	require.NoError(t, err)
	_, err = accounts.Bind(ctx, flipcashUserD, model.MustGenerateKeyPair().Proto())
	require.NoError(t, err)
	require.NoError(t, profiles.SetDisplayName(ctx, flipcashUserA, "A"))
	require.NoError(t, profiles.SetDisplayName(ctx, flipcashUserB, "B"))
	require.NoError(t, profiles.SetDisplayName(ctx, flipcashUserD, "D"))
	require.NoError(t, profiles.LinkPhoneNumber(ctx, flipcashUserA, phoneA, hashA))
	require.NoError(t, profiles.LinkPhoneNumber(ctx, flipcashUserB, phoneB, hashB))
	require.NoError(t, profiles.LinkPhoneNumber(ctx, flipcashUserD, phoneD, hmacHash(phoneD)))

	// Caller's contact list = [A, B, C]. C is not on Flipcash.
	checksum := xorChecksums(
		contact.ZeroChecksum(),
		phoneSha256(phoneA), phoneSha256(phoneB), phoneSha256(phoneC),
	)
	require.NoError(t, store.Replace(ctx, f.userID, []*commonpb.Hash{hashA, hashB, hashC}, checksum))

	req := &contactpb.GetFlipcashContactsRequest{Checksum: checksum}
	require.NoError(t, f.keys.Auth(req, &req.Auth))

	stream, err := f.client.GetFlipcashContacts(ctx, req)
	require.NoError(t, err)

	var got []string
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, contactpb.GetFlipcashContactsResponse_OK, resp.Result)
		for _, c := range resp.Contacts {
			got = append(got, c.Phone.Value)
		}
	}

	require.ElementsMatch(t, []string{phoneA, phoneB}, got)
}

func zeroChecksum() []byte {
	return make([]byte, contact.ChecksumSize)
}

func phoneSha256(phone string) *commonpb.Hash {
	sum := sha256.Sum256([]byte(phone))
	return &commonpb.Hash{Value: sum[:]}
}

func hmacHash(phone string) *commonpb.Hash {
	mac := hmac.New(sha256.New, serverTestPepper)
	mac.Write([]byte(phone))
	return &commonpb.Hash{Value: mac.Sum(nil)}
}

func xorChecksums(hashes ...*commonpb.Hash) *commonpb.Hash {
	out := make([]byte, contact.ChecksumSize)
	for _, h := range hashes {
		for i := range contact.ChecksumSize {
			out[i] ^= h.Value[i]
		}
	}
	return &commonpb.Hash{Value: out}
}

package contact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	contactpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/contact/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
	phone_hash "github.com/code-payments/flipcash2-server/phone/hash"
	"github.com/code-payments/flipcash2-server/profile"
)

const (
	MaxFullUploadStreamMessages  = 11
	GetFlipcashContactsBatchSize = 1000
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	accounts account.Store
	profiles profile.Store
	contacts Store

	hashPepper []byte

	contactpb.UnimplementedContactListServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	profiles profile.Store,
	contacts Store,
	hashPepper []byte,
) *Server {
	return &Server{
		log:        log,
		authz:      authz,
		accounts:   accounts,
		profiles:   profiles,
		contacts:   contacts,
		hashPepper: hashPepper,
	}
}

func (s *Server) CheckSync(ctx context.Context, req *contactpb.CheckSyncRequest) (*contactpb.CheckSyncResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if ok, err := s.isRegistered(ctx, log, userID); err != nil {
		return nil, err
	} else if !ok {
		return &contactpb.CheckSyncResponse{Result: contactpb.CheckSyncResponse_DENIED}, nil
	}

	stored, err := s.contacts.GetChecksum(ctx, userID)
	if errors.Is(err, ErrNotFound) {
		stored = ZeroChecksum()
	} else if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting contact list checksum")
		return nil, status.Error(codes.Internal, "")
	}

	result := contactpb.CheckSyncResponse_OK
	if !bytes.Equal(stored.Value, req.ClientChecksum.Value) {
		result = contactpb.CheckSyncResponse_OUT_OF_SYNC
	}

	return &contactpb.CheckSyncResponse{
		Result:         result,
		ServerChecksum: stored,
	}, nil
}

func (s *Server) DeltaUpload(ctx context.Context, req *contactpb.DeltaUploadRequest) (*contactpb.DeltaUploadResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if ok, err := s.isRegistered(ctx, log, userID); err != nil {
		return nil, err
	} else if !ok {
		return &contactpb.DeltaUploadResponse{Result: contactpb.DeltaUploadResponse_DENIED}, nil
	}

	// Verify that new_checksum = old_checksum XOR (SHA256 of each add) XOR
	// (SHA256 of each remove). XOR is its own inverse so we can apply both
	// add and remove SHA256 hashes the same way; a mismatch means the
	// client's delta is internally inconsistent.
	expectedNew := make([]byte, ChecksumSize)
	copy(expectedNew, req.OldChecksum.Value)
	xorPhoneHashes(expectedNew, req.Adds)
	xorPhoneHashes(expectedNew, req.Removes)
	if !bytes.Equal(expectedNew, req.NewChecksum.Value) {
		return &contactpb.DeltaUploadResponse{Result: contactpb.DeltaUploadResponse_CHECKSUM_MISMATCH}, nil
	}

	addHashes := s.secureHashPhones(req.Adds)
	removeHashes := s.secureHashPhones(req.Removes)

	switch err := s.contacts.ApplyDelta(ctx, userID, addHashes, removeHashes, req.OldChecksum, req.NewChecksum); {
	case err == nil:
		return &contactpb.DeltaUploadResponse{Result: contactpb.DeltaUploadResponse_OK}, nil
	case errors.Is(err, ErrChecksumDrift):
		return &contactpb.DeltaUploadResponse{Result: contactpb.DeltaUploadResponse_CHECKSUM_DRIFT}, nil
	case errors.Is(err, ErrTooManyContacts):
		return &contactpb.DeltaUploadResponse{Result: contactpb.DeltaUploadResponse_TOO_MANY_CONTACTS}, nil
	default:
		log.With(zap.Error(err)).Warn("Failure applying contact list delta")
		return nil, status.Error(codes.Internal, "")
	}
}

func (s *Server) FullUpload(stream contactpb.ContactList_FullUploadServer) error {
	ctx := stream.Context()

	var userID *commonpb.UserId
	var expectedChecksum *commonpb.Hash
	var allHashes []*commonpb.Hash

	// XOR-of-SHA256 over the phones we've received so far. Compared to the
	// client-supplied expected_checksum at end-of-stream.
	computedChecksum := make([]byte, ChecksumSize)

	for i := 0; ; i++ {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if i >= MaxFullUploadStreamMessages {
			return status.Error(codes.InvalidArgument, "too many streamed messages")
		}

		authorized, err := s.authz.Authorize(ctx, req, &req.Auth)
		if err != nil {
			return err
		}
		if userID == nil {
			userID = authorized

			log := s.log.With(zap.String("user_id", model.UserIDString(userID)))
			if ok, err := s.isRegistered(ctx, log, userID); err != nil {
				return err
			} else if !ok {
				return stream.SendAndClose(&contactpb.FullUploadResponse{Result: contactpb.FullUploadResponse_DENIED})
			}
		} else if !bytes.Equal(userID.Value, authorized.Value) {
			return status.Error(codes.PermissionDenied, "auth identity changed mid-stream")
		}

		allHashes = append(allHashes, s.secureHashPhones(req.Phones)...)
		xorPhoneHashes(computedChecksum, req.Phones)

		if req.ExpectedChecksum != nil {
			expectedChecksum = req.ExpectedChecksum
		}
	}

	if userID == nil {
		return status.Error(codes.InvalidArgument, "empty stream")
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if expectedChecksum == nil {
		return status.Error(codes.InvalidArgument, "missing expected_checksum")
	}

	if !bytes.Equal(computedChecksum, expectedChecksum.Value) {
		return stream.SendAndClose(&contactpb.FullUploadResponse{
			Result: contactpb.FullUploadResponse_CHECKSUM_MISMATCH,
		})
	}

	if len(allHashes) > MaxContactsPerUser {
		return stream.SendAndClose(&contactpb.FullUploadResponse{Result: contactpb.FullUploadResponse_TOO_MANY_CONTACTS})
	}

	switch err := s.contacts.Replace(ctx, userID, allHashes, expectedChecksum); {
	case err == nil:
		return stream.SendAndClose(&contactpb.FullUploadResponse{Result: contactpb.FullUploadResponse_OK})
	case errors.Is(err, ErrTooManyContacts):
		return stream.SendAndClose(&contactpb.FullUploadResponse{Result: contactpb.FullUploadResponse_TOO_MANY_CONTACTS})
	default:
		log.With(zap.Error(err)).Warn("Failure replacing contact list")
		return status.Error(codes.Internal, "")
	}
}

func (s *Server) GetFlipcashContacts(req *contactpb.GetFlipcashContactsRequest, stream contactpb.ContactList_GetFlipcashContactsServer) error {
	ctx := stream.Context()

	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	if ok, err := s.isRegistered(ctx, log, userID); err != nil {
		return err
	} else if !ok {
		return stream.Send(&contactpb.GetFlipcashContactsResponse{
			Result: contactpb.GetFlipcashContactsResponse_DENIED,
		})
	}

	stored, err := s.contacts.GetChecksum(ctx, userID)
	switch {
	case errors.Is(err, ErrNotFound):
		return stream.Send(&contactpb.GetFlipcashContactsResponse{
			Result: contactpb.GetFlipcashContactsResponse_NOT_FOUND,
		})
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting contact list checksum")
		return status.Error(codes.Internal, "")
	}

	if !bytes.Equal(stored.Value, req.Checksum.Value) {
		return stream.Send(&contactpb.GetFlipcashContactsResponse{
			Result: contactpb.GetFlipcashContactsResponse_CHECKSUM_DRIFT,
		})
	}

	hashes, err := s.contacts.GetHashes(ctx, userID)
	if errors.Is(err, ErrNotFound) {
		return stream.Send(&contactpb.GetFlipcashContactsResponse{
			Result: contactpb.GetFlipcashContactsResponse_NOT_FOUND,
		})
	} else if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting contact list hashes")
		return status.Error(codes.Internal, "")
	}

	if len(hashes) == 0 {
		return stream.Send(&contactpb.GetFlipcashContactsResponse{
			Result: contactpb.GetFlipcashContactsResponse_NOT_FOUND,
		})
	}

	matches, err := s.profiles.GetPhonesByHashesForPayment(ctx, hashes)
	if errors.Is(err, profile.ErrNotFound) {
		return stream.Send(&contactpb.GetFlipcashContactsResponse{
			Result: contactpb.GetFlipcashContactsResponse_NOT_FOUND,
		})
	} else if err != nil {
		log.With(zap.Error(err)).Warn("Failure looking up phones by hash")
		return status.Error(codes.Internal, "")
	}

	if len(matches) == 0 {
		return stream.Send(&contactpb.GetFlipcashContactsResponse{
			Result: contactpb.GetFlipcashContactsResponse_NOT_FOUND,
		})
	}

	for start := 0; start < len(matches); start += GetFlipcashContactsBatchSize {
		end := start + GetFlipcashContactsBatchSize
		if end > len(matches) {
			end = len(matches)
		}
		batch := matches[start:end]

		contacts := make([]*contactpb.FlipcashContact, len(batch))
		for i, m := range batch {
			contacts[i] = &contactpb.FlipcashContact{
				Phone:    m.PhoneNumber,
				DmChatId: chat.MustDeriveDmChatID(userID, m.UserID),
			}
		}

		if err := stream.Send(&contactpb.GetFlipcashContactsResponse{
			Result:   contactpb.GetFlipcashContactsResponse_OK,
			Contacts: contacts,
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) isRegistered(ctx context.Context, log *zap.Logger, userID *commonpb.UserId) (bool, error) {
	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting user registration status")
		return false, status.Error(codes.Internal, "")
	}
	return isRegistered, nil
}

func (s *Server) secureHashPhones(phones []*phonepb.PhoneNumber) []*commonpb.Hash {
	if len(phones) == 0 {
		return nil
	}
	out := make([]*commonpb.Hash, len(phones))
	for i, p := range phones {
		out[i] = phone_hash.Secure(p, s.hashPepper)
	}
	return out
}

func xorPhoneHashes(dst []byte, phones []*phonepb.PhoneNumber) {
	for _, p := range phones {
		sum := sha256.Sum256([]byte(p.Value))
		for i := range ChecksumSize {
			dst[i] ^= sum[i]
		}
	}
}

// Package reporting implements the Reporting gRPC service, which lets users
// flag a user, chat, message or blob for review.
//
// Reports are advisory: filing one has no client-visible effect and no outcome
// is surfaced back to the reporter. Today the service is a stub that records
// nothing — a report is logged with who filed it and what it targets, and
// nothing else happens. Persistence and any review workflow are not built.
package reporting

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	reportingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/reporting/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/model"
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	reportingpb.UnimplementedReportingServer
}

func NewServer(log *zap.Logger, authz auth.Authorizer) *Server {
	return &Server{
		log:   log,
		authz: authz,
	}
}

// Report logs the report and returns OK. The target is not checked for
// existence and nothing is stored, so every well-formed report from an
// authorized caller succeeds, including a repeat of an earlier one.
func (s *Server) Report(ctx context.Context, req *reportingpb.ReportRequest) (*reportingpb.ReportResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	switch target := req.Target.(type) {
	case *reportingpb.ReportRequest_UserId:
		log = log.With(
			zap.String("target", "user"),
			zap.String("reported_user_id", model.UserIDString(target.UserId)),
		)
	case *reportingpb.ReportRequest_ChatId:
		log = log.With(
			zap.String("target", "chat"),
			zap.String("reported_chat_id", model.ChatIDString(target.ChatId)),
		)
	case *reportingpb.ReportRequest_Message:
		log = log.With(
			zap.String("target", "message"),
			zap.String("reported_chat_id", model.ChatIDString(target.Message.GetChatId())),
			zap.Uint64("reported_message_id", target.Message.GetMessageId().GetValue()),
		)
	case *reportingpb.ReportRequest_BlobId:
		log = log.With(
			zap.String("target", "blob"),
			zap.String("reported_blob_id", blob.IDString(target.BlobId)),
		)
	default:
		return nil, status.Error(codes.InvalidArgument, "target is required")
	}

	log.With(zap.String("description", req.Description)).Info("Received report")

	return &reportingpb.ReportResponse{Result: reportingpb.ReportResponse_OK}, nil
}

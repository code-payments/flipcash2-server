package reporting

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	reportingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/reporting/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/account/memory"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/testutil"
)

type serverEnv struct {
	t      *testing.T
	ctx    context.Context
	client reportingpb.ReportingClient

	keys model.KeyPair
}

func newServerEnv(t *testing.T) *serverEnv {
	ctx := context.Background()
	log := zaptest.NewLogger(t)

	accounts := memory.NewInMemory()
	authz := account.NewAuthorizer(log, accounts, auth.NewKeyPairAuthenticator(log))

	keys := model.MustGenerateKeyPair()
	_, err := accounts.Bind(ctx, model.MustGenerateUserID(), keys.Proto())
	require.NoError(t, err)

	server := NewServer(log, authz)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		reportingpb.RegisterReportingServer(s, server)
	}))

	return &serverEnv{
		t:      t,
		ctx:    ctx,
		client: reportingpb.NewReportingClient(cc),
		keys:   keys,
	}
}

func (e *serverEnv) report(req *reportingpb.ReportRequest) (*reportingpb.ReportResponse, error) {
	require.NoError(e.t, e.keys.Auth(req, &req.Auth))
	return e.client.Report(e.ctx, req)
}

func TestServer_Report_AllTargets(t *testing.T) {
	e := newServerEnv(t)

	chatID := &commonpb.ChatId{Value: make([]byte, 16)}
	for name, req := range map[string]*reportingpb.ReportRequest{
		"user":    {Target: &reportingpb.ReportRequest_UserId{UserId: model.MustGenerateUserID()}},
		"chat":    {Target: &reportingpb.ReportRequest_ChatId{ChatId: chatID}},
		"message": {Target: &reportingpb.ReportRequest_Message{Message: &reportingpb.ReportRequest_ReportedMessage{ChatId: chatID, MessageId: &messagingpb.MessageId{Value: 7}}}},
		"blob":    {Target: &reportingpb.ReportRequest_BlobId{BlobId: &blobpb.BlobId{Value: make([]byte, 16)}}},
	} {
		t.Run(name, func(t *testing.T) {
			req.Description = "spam"
			resp, err := e.report(req)
			require.NoError(t, err)
			require.Equal(t, reportingpb.ReportResponse_OK, resp.Result)

			// Repeating a report is a no-op and still OK.
			req.Auth, req.Description = nil, ""
			resp, err = e.report(req)
			require.NoError(t, err)
			require.Equal(t, reportingpb.ReportResponse_OK, resp.Result)
		})
	}
}

func TestServer_Report_NoTarget(t *testing.T) {
	e := newServerEnv(t)

	_, err := e.report(&reportingpb.ReportRequest{Description: "nothing"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestServer_Report_Unauthorized(t *testing.T) {
	e := newServerEnv(t)

	// A key with no bound account is authenticated but not authorized.
	e.keys = model.MustGenerateKeyPair()
	_, err := e.report(&reportingpb.ReportRequest{
		Target: &reportingpb.ReportRequest_UserId{UserId: model.MustGenerateUserID()},
	})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

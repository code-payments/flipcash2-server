package messaging

import (
	"bytes"
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// GetCashMessageReturnStatus reports whether the payment of a cash message
// offering a return (CashContent.payment_return.offer) can still be returned,
// and gives the message's single_receiver, alone, the intent ID the return
// must use (see CashMessageReturn and intent.Integration's return payment
// validation). The ID is never in the message itself, so this is the only way
// a client learns it.
//
// Gate: membership alone (Access.IsMember), so a non-member, however the
// chat's rules would treat them, is DENIED before the message is read. A
// missing message, one sent with no offer, and one whose content no longer
// carries the offer (a deleted message's tombstone, whose state the store
// keeps) are all NOT_FOUND: intent validation refuses a return of any of
// them, so there is nothing to report. The status is the store's strongly
// consistent read, so a return whose task has marked the offer reads RETURNED
// on every later call; a return paid and not yet recorded still reads
// AVAILABLE, and a second payment under the ID cannot go through anyway.
func (s *Server) GetCashMessageReturnStatus(ctx context.Context, req *messagingpb.GetCashMessageReturnStatusRequest) (*messagingpb.GetCashMessageReturnStatusResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	isMember, err := s.isMember(ctx, log, req.ChatId, userID)
	if err != nil {
		return nil, err
	}
	if !isMember {
		return &messagingpb.GetCashMessageReturnStatusResponse{Result: messagingpb.GetCashMessageReturnStatusResponse_DENIED}, nil
	}

	msg, state, err := s.messages.GetCashMessageReturn(ctx, req.ChatId, req.MessageId)
	switch {
	case errors.Is(err, ErrMessageNotFound):
		return &messagingpb.GetCashMessageReturnStatusResponse{Result: messagingpb.GetCashMessageReturnStatusResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting cash message return")
		return nil, status.Error(codes.Internal, "")
	}

	var cash *messagingpb.CashContent
	if len(msg.Content) == 1 {
		cash = msg.Content[0].GetCash()
	}
	if state == nil || cash.GetPaymentReturn().GetOffer() == nil {
		return &messagingpb.GetCashMessageReturnStatusResponse{Result: messagingpb.GetCashMessageReturnStatusResponse_NOT_FOUND}, nil
	}

	resp := &messagingpb.GetCashMessageReturnStatusResponse{
		Result: messagingpb.GetCashMessageReturnStatusResponse_OK,
		Status: messagingpb.GetCashMessageReturnStatusResponse_AVAILABLE,
	}
	if state.Returned {
		resp.Status = messagingpb.GetCashMessageReturnStatusResponse_RETURNED
	}
	if bytes.Equal(cash.GetSingleReceiver().GetValue(), userID.Value) {
		resp.PaymentIntentId = state.IntentID
	}
	return resp, nil
}

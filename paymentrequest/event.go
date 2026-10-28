package paymentrequest

import (
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/model"
)

// Payment request updates are announced on the request's chat topic, once,
// excluding no one, so the actor's own devices hear them too, and the delivery
// layer decides who receives them: the chat's members, and a chat preview
// reading the chat in full. A redacted preview never sees one, since
// redact.ChatUpdate carries only what it knows to redact.
//
// A request is announced whole when it is created (PublishCreated), and every
// change after that as what it did at the version it produced
// (PublishSharePaid, PublishClosed): clients apply a change to a copy at the
// version before it and refetch on a gap, so each change must be announced at
// most once and only by the write that made it, never on a no-op, whose
// version may be some other change's. Publishing is best effort: it queues
// the event and cannot fail, and a client reconciles a lost one by a refetch.
//
// Expiry is never announced: it is the clock's, moves no version, and every
// reader derives it from ends_at.

// PublishCreated announces r, just created, whole, as a reader sees it at now.
func PublishCreated(chatEventBus *event.Bus[*commonpb.ChatId, *eventpb.ChatEvent], r *PaymentRequest, now time.Time) {
	publish(chatEventBus, r, now, &paymentrequestpb.PaymentRequestUpdate{
		PaymentRequestId: proto.Clone(r.ID).(*paymentrequestpb.PaymentRequestId),
		Kind:             &paymentrequestpb.PaymentRequestUpdate_Created{Created: r.ToProto(now)},
	})
}

// PublishSharePaid announces the payment, or marking, of userID's share of r,
// as Store.PayShare returned it from a write that changed it: the share as it
// now stands and, when the write completed the request (PayShare's completed),
// the request's completion.
func PublishSharePaid(chatEventBus *event.Bus[*commonpb.ChatId, *eventpb.ChatEvent], r *PaymentRequest, userID *commonpb.UserId, completed bool, now time.Time) {
	updates := []*paymentrequestpb.PaymentRequestUpdate_Update{{
		Kind: &paymentrequestpb.PaymentRequestUpdate_Update_SharePaymentUpdate{SharePaymentUpdate: &paymentrequestpb.PaymentRequestUpdate_SharePaymentUpdate{
			Share: proto.Clone(r.Share(userID)).(*paymentrequestpb.Share),
		}},
	}}
	if completed {
		updates = append(updates, statusUpdate(paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_COMPLETED, r))
	}
	publishUpdates(chatEventBus, r, now, updates)
}

// PublishClosed announces the close of r, as Store.Close returned it from a
// write that changed it.
func PublishClosed(chatEventBus *event.Bus[*commonpb.ChatId, *eventpb.ChatEvent], r *PaymentRequest, now time.Time) {
	publishUpdates(chatEventBus, r, now, []*paymentrequestpb.PaymentRequestUpdate_Update{
		statusUpdate(paymentrequestpb.PaymentRequestStatus_PAYMENT_REQUEST_STATUS_CLOSED, r),
	})
}

// statusUpdate is r leaving OPEN for status, at its stored ClosedAt, which
// the write that finalized it set.
func statusUpdate(status paymentrequestpb.PaymentRequestStatus, r *PaymentRequest) *paymentrequestpb.PaymentRequestUpdate_Update {
	return &paymentrequestpb.PaymentRequestUpdate_Update{
		Kind: &paymentrequestpb.PaymentRequestUpdate_Update_StatusUpdate{StatusUpdate: &paymentrequestpb.PaymentRequestUpdate_StatusUpdate{
			Status:   status,
			ClosedAt: timestamppb.New(*r.ClosedAt),
		}},
	}
}

// publishUpdates announces updates as the one change to r at its version.
func publishUpdates(chatEventBus *event.Bus[*commonpb.ChatId, *eventpb.ChatEvent], r *PaymentRequest, now time.Time, updates []*paymentrequestpb.PaymentRequestUpdate_Update) {
	publish(chatEventBus, r, now, &paymentrequestpb.PaymentRequestUpdate{
		PaymentRequestId: proto.Clone(r.ID).(*paymentrequestpb.PaymentRequestId),
		Kind: &paymentrequestpb.PaymentRequestUpdate_Updates_{Updates: &paymentrequestpb.PaymentRequestUpdate_Updates{
			Version: r.Version,
			Updates: updates,
		}},
	})
}

func publish(chatEventBus *event.Bus[*commonpb.ChatId, *eventpb.ChatEvent], r *PaymentRequest, now time.Time, update *paymentrequestpb.PaymentRequestUpdate) {
	chatEventBus.OnEvent(r.ChatID, &eventpb.ChatEvent{
		ChatId: r.ChatID,
		Event: &eventpb.Event{
			Id: model.MustGenerateEventID(),
			Ts: timestamppb.New(now),
			Type: &eventpb.Event_ChatUpdate{ChatUpdate: &eventpb.ChatUpdate{
				Chat: r.ChatID,
				PaymentRequestUpdates: &paymentrequestpb.PaymentRequestUpdateBatch{
					PaymentRequestUpdates: []*paymentrequestpb.PaymentRequestUpdate{update},
				},
			}},
		},
	})
}

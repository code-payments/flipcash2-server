package task

import (
	"context"
	"fmt"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/event"
	"github.com/code-payments/flipcash2-server/intent"
	"github.com/code-payments/flipcash2-server/messaging"
	"github.com/code-payments/flipcash2-server/paymentrequest"
	ocp_data "github.com/code-payments/ocp-server/ocp/data"
	ocp_task "github.com/code-payments/ocp-server/ocp/data/task"
	ocp_integration "github.com/code-payments/ocp-server/ocp/integration"
)

// Executor executes Flipcash-defined tasks whose execution is guaranteed by
// the OCP task system. Tasks are delivered at least once and may execute
// concurrently on any process, so every handler must be idempotent.
type Executor struct {
	accounts account.Store
	chats    chat.Store
	requests paymentrequest.Store
	sender   *messaging.Sender
	ocpData  ocp_data.Provider

	chatEventBus *event.Bus[*commonpb.ChatId, *eventpb.ChatEvent]
}

// NewExecutor constructs the task executor. requests is the payment request
// store, written to mark a share paid by a requested payment, and
// chatEventBus the chat-keyed event bus the mark is announced on (see
// recordRequestedPayment).
func NewExecutor(
	accounts account.Store,
	chats chat.Store,
	requests paymentrequest.Store,
	sender *messaging.Sender,
	ocpData ocp_data.Provider,
	chatEventBus *event.Bus[*commonpb.ChatId, *eventpb.ChatEvent],
) ocp_integration.TaskExecutor {
	return &Executor{
		accounts:     accounts,
		chats:        chats,
		requests:     requests,
		sender:       sender,
		ocpData:      ocpData,
		chatEventBus: chatEventBus,
	}
}

func (e *Executor) Execute(ctx context.Context, record *ocp_task.Record) error {
	switch record.Type {
	case intent.TaskTypeSendContactDmPaymentMessage:
		return e.sendContactDmPaymentMessage(ctx, record)
	case intent.TaskTypeSendDmPaymentMessage:
		return e.sendDmPaymentMessage(ctx, record)
	case intent.TaskTypeSendGroupPaymentMessage:
		return e.sendGroupPaymentMessage(ctx, record)
	case intent.TaskTypeRecordReactionBoost:
		return e.recordReactionBoost(ctx, record)
	case intent.TaskTypeRecordRequestedPayment:
		return e.recordRequestedPayment(ctx, record)
	default:
		return fmt.Errorf("unknown task type %d", record.Type)
	}
}

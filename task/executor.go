package task

import (
	"context"
	"fmt"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/intent"
	"github.com/code-payments/flipcash2-server/messaging"
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
	sender   *messaging.Sender
	ocpData  ocp_data.Provider
}

func NewExecutor(
	accounts account.Store,
	chats chat.Store,
	sender *messaging.Sender,
	ocpData ocp_data.Provider,
) ocp_integration.TaskExecutor {
	return &Executor{
		accounts: accounts,
		chats:    chats,
		sender:   sender,
		ocpData:  ocpData,
	}
}

func (e *Executor) Execute(ctx context.Context, record *ocp_task.Record) error {
	switch record.Type {
	case intent.TaskTypeSendContactDmPaymentMessage:
		return e.sendContactDmPaymentMessage(ctx, record)
	case intent.TaskTypeSendTipDmPaymentMessage:
		return e.sendTipDmPaymentMessage(ctx, record)
	default:
		return fmt.Errorf("unknown task type %d", record.Type)
	}
}

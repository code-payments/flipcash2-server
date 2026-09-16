package messaging

import (
	"go.uber.org/zap"

	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"

	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/chat"
)

type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	chats    chat.Store
	media    Media
	messages Store

	access *chat.Access

	sender *Sender

	messagingpb.UnimplementedMessagingServer
}

func NewServer(
	log *zap.Logger,

	authz auth.Authorizer,

	chats chat.Store,
	media Media,
	messages Store,

	access *chat.Access,

	sender *Sender,
) *Server {
	return &Server{
		log: log,

		authz: authz,

		chats:    chats,
		media:    media,
		messages: messages,

		access: access,

		sender: sender,
	}
}

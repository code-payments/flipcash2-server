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
	messages Store
	media    Media

	sender *Sender

	messagingpb.UnimplementedMessagingServer
}

func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	chats chat.Store,
	messages Store,
	media Media,
	sender *Sender,
) *Server {
	return &Server{
		log:      log,
		authz:    authz,
		chats:    chats,
		messages: messages,
		media:    media,
		sender:   sender,
	}
}

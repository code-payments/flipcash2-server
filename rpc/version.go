package rpc

import (
	"context"

	ocp_client "github.com/code-payments/ocp-server/grpc/client"
)

// GetClientVersion extracts the client's version from the gRPC user-agent, or
// returns nil if no parseable Flipcash user-agent is present.
//
// A nil version means the caller is unidentifiable, not that it is new. Gating
// on a version should treat nil as the older behaviour, so a client that cannot
// be placed is never handed something it may not understand.
func GetClientVersion(ctx context.Context) *ocp_client.Version {
	var clientVersion *ocp_client.Version
	if userAgent, err := ocp_client.GetUserAgent(ctx, UserAgentName); err == nil {
		clientVersion = &userAgent.Version
	}
	if userAgent, err := ocp_client.GetUserAgent(ctx, UserAgentName+"/Core"); err == nil {
		clientVersion = &userAgent.Version
	}
	return clientVersion
}

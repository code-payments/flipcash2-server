package model

import (
	"fmt"

	"github.com/google/uuid"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
)

// MustGenerateEventID mints a fresh event ID: a random UUID.
func MustGenerateEventID() *eventpb.EventId {
	id, err := uuid.NewRandom()
	if err != nil {
		panic(fmt.Sprintf("failed to generate event id: %v", err))
	}
	return &eventpb.EventId{Id: id[:]}
}

// EventIDString renders an event ID for logging.
func EventIDString(id *eventpb.EventId) string {
	if id == nil {
		return "<nil>"
	}
	uuidValue, err := uuid.FromBytes(id.GetId())
	if err != nil {
		return fmt.Sprintf("<invalid: %v>", err)
	}
	return uuidValue.String()
}

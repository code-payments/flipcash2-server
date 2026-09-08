package event

import (
	"fmt"

	"github.com/google/uuid"

	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"
)

func MustGenerateEventID() *eventpb.EventId {
	id, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}
	return &eventpb.EventId{Id: id[:]}
}

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

type KeyAndEvent[Key, Event any] struct {
	Key   Key
	Event Event
}

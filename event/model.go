package event

type KeyAndEvent[Key, Event any] struct {
	Key   Key
	Event Event
}

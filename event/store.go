package event

import (
	"context"
	"errors"
	"time"
)

var (
	ErrRendezvousExists   = errors.New("rendezvous already exists")
	ErrRendezvousNotFound = errors.New("rendezvous not found")
)

type Store interface {
	// CreateRendezvous creates a new rendezvous for an event stream
	CreateRendezvous(ctx context.Context, rendezvous *Rendezvous) error

	// GetRendezvous gets an event stream rendezvous for a given key
	GetRendezvous(ctx context.Context, key string) (*Rendezvous, error)

	// ExtendRendezvousxpiry extends a rendezvous' expiry for a given key and address
	ExtendRendezvousExpiry(ctx context.Context, key, address string, expiresAt time.Time) error

	// DeleteRendezvous deletes an event stream rendezvous for a given key and address
	DeleteRendezvous(ctx context.Context, key, address string) error
}

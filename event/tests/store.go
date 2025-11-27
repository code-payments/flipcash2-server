package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/code-payments/flipcash2-server/event"
)

func RunStoreTests(t *testing.T, s event.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s event.Store){
		testEventStore_RendezvousHappyPath,
		testEventStore_RendezvousExpiredRecord,
	} {
		tf(t, s)
		teardown()
	}
}

func testEventStore_RendezvousHappyPath(t *testing.T, s event.Store) {
	ctx := context.Background()

	record := &event.Rendezvous{
		Key:       "key",
		Address:   "localhost:1234",
		ExpiresAt: time.Now().Add(time.Second),
	}
	cloned := record.Clone()

	require.NoError(t, s.DeleteRendezvous(ctx, record.Key, record.Address))
	_, err := s.GetRendezvous(ctx, record.Key)
	require.Equal(t, event.ErrRendezvousNotFound, err)
	require.Equal(t, event.ErrRendezvousNotFound, s.ExtendRendezvousExpiry(ctx, record.Key, record.Address, time.Now().Add(time.Minute)))

	require.NoError(t, s.CreateRendezvous(ctx, record))

	actual, err := s.GetRendezvous(ctx, record.Key)
	require.NoError(t, err)
	assertEquivalentRendezvous(t, cloned, actual)

	time.Sleep(time.Millisecond)
	record.Address = "localhost:5678"
	record.ExpiresAt = time.Now().Add(2 * time.Second)
	cloned = record.Clone()
	require.Equal(t, event.ErrRendezvousExists, s.CreateRendezvous(ctx, record))

	time.Sleep(time.Second)
	require.NoError(t, s.CreateRendezvous(ctx, record))

	actual, err = s.GetRendezvous(ctx, record.Key)
	require.NoError(t, err)
	assertEquivalentRendezvous(t, cloned, actual)

	record.ExpiresAt = record.ExpiresAt.Add(10 * time.Minute)
	cloned = record.Clone()
	require.NoError(t, s.ExtendRendezvousExpiry(ctx, record.Key, record.Address, record.ExpiresAt))

	actual, err = s.GetRendezvous(ctx, record.Key)
	require.NoError(t, err)
	assertEquivalentRendezvous(t, cloned, actual)

	require.NoError(t, s.DeleteRendezvous(ctx, record.Key, "localhost:8888"))

	actual, err = s.GetRendezvous(ctx, record.Key)
	require.NoError(t, err)
	assertEquivalentRendezvous(t, cloned, actual)

	require.NoError(t, s.DeleteRendezvous(ctx, record.Key, record.Address))

	_, err = s.GetRendezvous(ctx, record.Key)
	require.Equal(t, event.ErrRendezvousNotFound, err)
}

func testEventStore_RendezvousExpiredRecord(t *testing.T, s event.Store) {
	ctx := context.Background()

	record := &event.Rendezvous{
		Key:       "key",
		Address:   "localhost:1234",
		ExpiresAt: time.Now().Add(100 * time.Millisecond),
	}
	require.NoError(t, s.CreateRendezvous(ctx, record))

	time.Sleep(200 * time.Millisecond)

	_, err := s.GetRendezvous(ctx, record.Key)
	require.Equal(t, event.ErrRendezvousNotFound, err)
	require.Equal(t, event.ErrRendezvousNotFound, s.ExtendRendezvousExpiry(ctx, record.Key, record.Address, time.Now().Add(time.Minute)))

	require.NoError(t, s.DeleteRendezvous(ctx, record.Key, record.Address))
}

func assertEquivalentRendezvous(t *testing.T, obj1, obj2 *event.Rendezvous) {
	require.Equal(t, obj1.Key, obj2.Key)
	require.Equal(t, obj1.Address, obj2.Address)
	require.Equal(t, obj1.ExpiresAt.Unix(), obj2.ExpiresAt.Unix())
}

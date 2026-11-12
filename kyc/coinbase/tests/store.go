package tests

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/kyc/coinbase"
	"github.com/code-payments/flipcash2-server/model"
)

// RunStoreTests runs the shared coinbase.Store test suite against s. teardown
// is called between tests to reset the store.
func RunStoreTests(t *testing.T, s coinbase.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, s coinbase.Store){
		testStore_RoundTrip,
		testStore_NotFound,
		testStore_FirstRecordStands,
		testStore_RepeatPut,
		testStore_PerUserIsolation,
		testStore_InvalidRecords,
		testStore_ReturnsCopies,
		testStore_ConcurrentPuts,
	} {
		tf(t, s)
		teardown()
	}
}

// byIDTimeout bounds how long a by-customer read may trail a write: the
// DynamoDB store reads a GSI, which is eventually consistent.
const (
	byIDTimeout  = 5 * time.Second
	byIDInterval = 20 * time.Millisecond
)

func newCustomerID() string { return "customer_" + uuid.NewString() }

func newRecord(userID *commonpb.UserId) *coinbase.CustomerRecord {
	return &coinbase.CustomerRecord{
		UserID:     userID,
		CustomerID: newCustomerID(),
		// Nanoseconds set, so a store that kept less would fail the round trip.
		CreatedAt: time.Unix(1_700_000_000, 123_456_789),
	}
}

func requireRecordEqual(t *testing.T, want, got *coinbase.CustomerRecord) {
	t.Helper()
	require.NotNil(t, got)
	require.Equal(t, want.UserID.Value, got.UserID.Value)
	require.Equal(t, want.CustomerID, got.CustomerID)
	require.True(t, want.CreatedAt.Equal(got.CreatedAt), "created at: want %v, got %v", want.CreatedAt, got.CreatedAt)
}

// requireByID waits for GetCustomerByID to find customerID and requires the
// record it finds to equal want.
func requireByID(t *testing.T, s coinbase.Store, customerID string, want *coinbase.CustomerRecord) {
	t.Helper()
	requireRecordEqual(t, want, eventuallyByID(t, s, customerID))
}

// eventuallyByID waits for GetCustomerByID to find customerID and returns the
// record it finds.
func eventuallyByID(t *testing.T, s coinbase.Store, customerID string) *coinbase.CustomerRecord {
	t.Helper()
	// The condition runs off the test goroutine, so it records what it read
	// and the assertions run after.
	var (
		mu  sync.Mutex
		got *coinbase.CustomerRecord
		err error
	)
	require.Eventually(t, func() bool {
		record, readErr := s.GetCustomerByID(context.Background(), customerID)
		if errors.Is(readErr, coinbase.ErrNotFound) {
			return false
		}
		mu.Lock()
		defer mu.Unlock()
		got, err = record, readErr
		return true
	}, byIDTimeout, byIDInterval)
	mu.Lock()
	defer mu.Unlock()
	require.NoError(t, err)
	return got
}

func testStore_RoundTrip(t *testing.T, s coinbase.Store) {
	ctx := context.Background()
	record := newRecord(model.MustGenerateUserID())

	stored, created, err := s.PutCustomer(ctx, record)
	require.NoError(t, err)
	require.True(t, created)
	requireRecordEqual(t, record, stored)

	got, err := s.GetCustomer(ctx, record.UserID)
	require.NoError(t, err)
	requireRecordEqual(t, record, got)

	requireByID(t, s, record.CustomerID, record)

	// A time with a monotonic reading and a location also reads back equal.
	now := newRecord(model.MustGenerateUserID())
	now.CreatedAt = time.Now()
	_, created, err = s.PutCustomer(ctx, now)
	require.NoError(t, err)
	require.True(t, created)
	got, err = s.GetCustomer(ctx, now.UserID)
	require.NoError(t, err)
	requireRecordEqual(t, now, got)
}

func testStore_NotFound(t *testing.T, s coinbase.Store) {
	ctx := context.Background()

	_, err := s.GetCustomer(ctx, model.MustGenerateUserID())
	require.ErrorIs(t, err, coinbase.ErrNotFound)

	_, err = s.GetCustomerByID(ctx, newCustomerID())
	require.ErrorIs(t, err, coinbase.ErrNotFound)

	// A string that is not a customer ID is not found either.
	for _, id := range []string{"", "customer_", "not-a-customer", strings.ToUpper(newCustomerID())} {
		_, err = s.GetCustomerByID(ctx, id)
		require.ErrorIs(t, err, coinbase.ErrNotFound, "%q", id)
	}
}

func testStore_FirstRecordStands(t *testing.T, s coinbase.Store) {
	ctx := context.Background()
	user := model.MustGenerateUserID()
	first := newRecord(user)

	_, created, err := s.PutCustomer(ctx, first)
	require.NoError(t, err)
	require.True(t, created)

	second := newRecord(user)
	second.CreatedAt = first.CreatedAt.Add(time.Hour)
	stored, created, err := s.PutCustomer(ctx, second)
	require.NoError(t, err)
	require.False(t, created)
	requireRecordEqual(t, first, stored)

	got, err := s.GetCustomer(ctx, user)
	require.NoError(t, err)
	requireRecordEqual(t, first, got)

	// Once the first is visible by ID, the second, never written, is not.
	requireByID(t, s, first.CustomerID, first)
	_, err = s.GetCustomerByID(ctx, second.CustomerID)
	require.ErrorIs(t, err, coinbase.ErrNotFound)
}

func testStore_RepeatPut(t *testing.T, s coinbase.Store) {
	ctx := context.Background()
	record := newRecord(model.MustGenerateUserID())

	_, created, err := s.PutCustomer(ctx, record)
	require.NoError(t, err)
	require.True(t, created)

	stored, created, err := s.PutCustomer(ctx, record)
	require.NoError(t, err)
	require.False(t, created)
	requireRecordEqual(t, record, stored)
}

func testStore_PerUserIsolation(t *testing.T, s coinbase.Store) {
	ctx := context.Background()
	a := newRecord(model.MustGenerateUserID())
	b := newRecord(model.MustGenerateUserID())

	_, created, err := s.PutCustomer(ctx, a)
	require.NoError(t, err)
	require.True(t, created)

	// a's record is not b's.
	_, err = s.GetCustomer(ctx, b.UserID)
	require.ErrorIs(t, err, coinbase.ErrNotFound)

	_, created, err = s.PutCustomer(ctx, b)
	require.NoError(t, err)
	require.True(t, created)

	got, err := s.GetCustomer(ctx, a.UserID)
	require.NoError(t, err)
	requireRecordEqual(t, a, got)
	got, err = s.GetCustomer(ctx, b.UserID)
	require.NoError(t, err)
	requireRecordEqual(t, b, got)

	requireByID(t, s, a.CustomerID, a)
	requireByID(t, s, b.CustomerID, b)
}

func testStore_InvalidRecords(t *testing.T, s coinbase.Store) {
	ctx := context.Background()

	for name, mutate := range map[string]func(r *coinbase.CustomerRecord) *coinbase.CustomerRecord{
		"nil record":    func(*coinbase.CustomerRecord) *coinbase.CustomerRecord { return nil },
		"nil user id":   func(r *coinbase.CustomerRecord) *coinbase.CustomerRecord { r.UserID = nil; return r },
		"empty user id": func(r *coinbase.CustomerRecord) *coinbase.CustomerRecord { r.UserID = &commonpb.UserId{}; return r },
		"short user id": func(r *coinbase.CustomerRecord) *coinbase.CustomerRecord {
			r.UserID.Value = r.UserID.Value[:15]
			return r
		},
		"long user id": func(r *coinbase.CustomerRecord) *coinbase.CustomerRecord {
			r.UserID.Value = append(r.UserID.Value, 0)
			return r
		},
		"empty customer id": func(r *coinbase.CustomerRecord) *coinbase.CustomerRecord { r.CustomerID = ""; return r },
		"no prefix": func(r *coinbase.CustomerRecord) *coinbase.CustomerRecord {
			r.CustomerID = strings.TrimPrefix(r.CustomerID, "customer_")
			return r
		},
		"uppercase customer id": func(r *coinbase.CustomerRecord) *coinbase.CustomerRecord {
			r.CustomerID = strings.ToUpper(r.CustomerID)
			return r
		},
		"customer id with space": func(r *coinbase.CustomerRecord) *coinbase.CustomerRecord {
			r.CustomerID = " " + r.CustomerID
			return r
		},
		"customer id too long": func(r *coinbase.CustomerRecord) *coinbase.CustomerRecord { r.CustomerID += "0"; return r },
		"customer id path": func(r *coinbase.CustomerRecord) *coinbase.CustomerRecord {
			r.CustomerID = "customer_../../v2/other"
			return r
		},
		"zero created at": func(r *coinbase.CustomerRecord) *coinbase.CustomerRecord { r.CreatedAt = time.Time{}; return r },
	} {
		t.Run(name, func(t *testing.T) {
			record := mutate(newRecord(model.MustGenerateUserID()))

			require.ErrorIs(t, record.Validate(), coinbase.ErrInvalidRecord)

			stored, created, err := s.PutCustomer(ctx, record)
			require.ErrorIs(t, err, coinbase.ErrInvalidRecord)
			require.False(t, created)
			require.Nil(t, stored)

			// Nothing was written for the user.
			if record != nil && len(record.UserID.GetValue()) == model.UserIDSize {
				_, err = s.GetCustomer(ctx, record.UserID)
				require.ErrorIs(t, err, coinbase.ErrNotFound)
			}
		})
	}

	require.NoError(t, newRecord(model.MustGenerateUserID()).Validate())
}

func testStore_ReturnsCopies(t *testing.T, s coinbase.Store) {
	ctx := context.Background()
	user := model.MustGenerateUserID()
	// Each record gets its own copy of the user ID, so mutating one leaves
	// user as it was for the reads.
	userCopy := func() *commonpb.UserId { return &commonpb.UserId{Value: append([]byte(nil), user.Value...)} }
	record := newRecord(userCopy())
	want := record.Clone()

	stored, _, err := s.PutCustomer(ctx, record)
	require.NoError(t, err)

	// Neither the record passed in nor the one handed back is the store's.
	record.UserID.Value[0] ^= 0xff
	record.CustomerID = newCustomerID()
	record.CreatedAt = record.CreatedAt.Add(time.Hour)
	stored.UserID.Value[1] ^= 0xff
	stored.CustomerID = newCustomerID()

	got, err := s.GetCustomer(ctx, user)
	require.NoError(t, err)
	requireRecordEqual(t, want, got)

	// Nor is one read back, by user or by ID.
	got.UserID.Value[2] ^= 0xff
	got.CustomerID = newCustomerID()
	byID := eventuallyByID(t, s, want.CustomerID)
	byID.UserID.Value[3] ^= 0xff
	byID.CustomerID = newCustomerID()

	// A losing put's answer is a copy too.
	loser, created, err := s.PutCustomer(ctx, newRecord(userCopy()))
	require.NoError(t, err)
	require.False(t, created)
	loser.UserID.Value[4] ^= 0xff
	loser.CustomerID = newCustomerID()

	got, err = s.GetCustomer(ctx, user)
	require.NoError(t, err)
	requireRecordEqual(t, want, got)
	requireByID(t, s, want.CustomerID, want)
}

func testStore_ConcurrentPuts(t *testing.T, s coinbase.Store) {
	ctx := context.Background()
	user := model.MustGenerateUserID()

	const n = 10
	var (
		wg      sync.WaitGroup
		results = make([]*coinbase.CustomerRecord, n)
		created = make([]bool, n)
		errs    = make([]error, n)
	)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], created[i], errs[i] = s.PutCustomer(ctx, newRecord(user))
		}()
	}
	wg.Wait()

	var winners int
	for i := range n {
		require.NoError(t, errs[i])
		if created[i] {
			winners++
		}
	}
	require.Equal(t, 1, winners)

	stored, err := s.GetCustomer(ctx, user)
	require.NoError(t, err)
	for i := range n {
		requireRecordEqual(t, stored, results[i])
	}
	requireByID(t, s, stored.CustomerID, stored)
}

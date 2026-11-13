package tests

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	kyccoinbasepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/coinbase/v1"
	profilepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/profile/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/cdp/customers"
	"github.com/code-payments/flipcash2-server/cdp/customers/customerstest"
	"github.com/code-payments/flipcash2-server/kyc/coinbase"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	"github.com/code-payments/flipcash2-server/testutil"
)

// RunServerTests runs the shared Kyc server test suite over accounts and
// store, each test against a fresh fake of Coinbase's Customers API. teardown
// is called between tests to reset the store.
func RunServerTests(t *testing.T, accounts account.Store, store coinbase.Store, teardown func()) {
	for _, tf := range []func(t *testing.T, accounts account.Store, store coinbase.Store){
		testServer_Auth,
		testServer_StaffGate,
		testServer_GetBeforeStart,
		testServer_Start,
		testServer_StartConcurrent,
		testServer_StartLostCreate,
		testServer_StartRecordedCustomerStands,
		testServer_StartRequesterIP,
		testServer_CustomerNotStarted,
		testServer_SubmitBeforeStart,
		testServer_SubmitInvalid,
		testServer_SubmitPhoneNumber,
		testServer_SubmitEmail,
		testServer_SubmitDuplicates,
		testServer_SubmitEmpty,
		testServer_Approve,
		testServer_ScreenAtATime,
		testServer_Decline,
		testServer_Block,
		testServer_CoinbaseFailures,
		testServer_SubmitUpdateFailures,
		testServer_SubmitForbidden,
		testServer_StartForbidden,
		testServer_NoSubmittedValueLogged,
	} {
		tf(t, accounts, store)
		teardown()
	}
}

const product = kyccoinbasepb.Product_PRODUCT_OFFRAMP

// stubIP is the requester address the stub resolver returns, from a
// documentation range.
const stubIP = "203.0.113.7"

// Placeholders for what a user submits. None is one of Coinbase's Sandbox
// magic values (the phone numbers +15555550100 to +15555550102 among them),
// so the fake approves them. The SSN starts with 9, which no SSN does. The
// names, street, second line, city and email are made-up words found nowhere
// else, so testServer_NoSubmittedValueLogged can search for them as they
// are; the state and postal code are too short to search bare, and it
// searches them quoted.
const (
	testFirstName  = "Zorvald"
	testLastName   = "Quexley"
	testLine1      = "4821 Vellichor Ave"
	testLine2      = "Unit Brindlewick 9"
	testCity       = "Ostrowind"
	testState      = "WY"
	testPostalCode = "83014"
	testEmail      = "zorvald.quexley@example.com"
	testPhone      = "+15555550142"
	testSsn        = "912345678"
	testSsnDashed  = "912-34-5678"
)

// decliningPhone is Coinbase's Sandbox magic phone number that declines a
// review, which the fake follows.
const decliningPhone = "+15555550101"

// otherPhone is a number no user is linked to unless a test links it; like
// testPhone it is no magic value.
const otherPhone = "+15555550177"

// otherEmail is an address no user is linked to unless a test links it; like
// testEmail it is made-up words found nowhere else.
const otherEmail = "brindlow.vesk@example.com"

// eightFields are the fields a user is asked for when they start.
var eightFields = []kyccoinbasepb.Field{
	kyccoinbasepb.Field_FIELD_LEGAL_NAME,
	kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH,
	kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
	kyccoinbasepb.Field_FIELD_SSN,
	kyccoinbasepb.Field_FIELD_EMAIL,
	kyccoinbasepb.Field_FIELD_PHONE_NUMBER,
	kyccoinbasepb.Field_FIELD_PURPOSE_OF_ACCOUNT,
	kyccoinbasepb.Field_FIELD_SOURCE_OF_FUNDS,
}

// identityFields are the fields Coinbase verifies, and so rejects on a
// decline; the two due-diligence answers are not among them.
var identityFields = []kyccoinbasepb.Field{
	kyccoinbasepb.Field_FIELD_LEGAL_NAME,
	kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH,
	kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
	kyccoinbasepb.Field_FIELD_SSN,
	kyccoinbasepb.Field_FIELD_EMAIL,
	kyccoinbasepb.Field_FIELD_PHONE_NUMBER,
}

type serverEnv struct {
	t   *testing.T
	ctx context.Context

	accounts *staffAccounts
	profiles *linkedProfiles
	store    coinbase.Store
	fake     *customerstest.Server
	resolver *stubResolver
	client   kyccoinbasepb.KycClient
	logs     *observer.ObservedLogs

	// phone is the number newUser links to each user it makes, as phone
	// verification would: the one individual() submits, unless a suite that
	// submits another sets it. No email address is linked to a user until a
	// test links one (linkedProfiles.linkEmail): the env's server takes an
	// address as given unless a test builds it to require a verified one.
	phone string

	// now is the server's clock, fixed for the env.
	now time.Time
}

func newServerEnv(t *testing.T, accounts account.Store, store coinbase.Store, opts ...coinbase.ServerOption) *serverEnv {
	t.Helper()

	fake := customerstest.NewServer()
	t.Cleanup(fake.Close)

	env := newServerEnvOn(t, accounts, store, fake.Client(), opts...)
	env.fake = fake
	return env
}

// newServerEnvOn builds an env whose server calls Coinbase through
// cdpCustomers: the fake's client, or a real one. The env's fake is nil. opts
// are passed to the server after the env's clock.
func newServerEnvOn(t *testing.T, accounts account.Store, store coinbase.Store, cdpCustomers *customers.Client, opts ...coinbase.ServerOption) *serverEnv {
	t.Helper()

	// Everything the process logs is observed in full, at every level, so a
	// test can assert what was and was not logged: the server's own logs, and
	// those of what sees a request before its handler does (the gRPC
	// harness's interceptors, the authorizer and its authenticator).
	observed, logs := observer.New(zapcore.DebugLevel)
	log := zap.New(zapcore.NewTee(zaptest.NewLogger(t).Core(), observed))

	env := &serverEnv{
		t:        t,
		ctx:      context.Background(),
		accounts: newStaffAccounts(accounts),
		profiles: newLinkedProfiles(),
		store:    store,
		resolver: &stubResolver{addr: netip.MustParseAddr(stubIP)},
		logs:     logs,
		phone:    testPhone,
		now:      time.Now().UTC(),
	}

	opts = append([]coinbase.ServerOption{coinbase.WithClock(func() time.Time { return env.now })}, opts...)

	authz := account.NewAuthorizer(log, env.accounts, auth.NewKeyPairAuthenticator(log))
	serv := coinbase.NewServer(log, authz, env.accounts, env.profiles, store, cdpCustomers, env.resolver.resolve, opts...)
	cc := testutil.RunGRPCServer(t, log, testutil.WithService(func(s *grpc.Server) {
		kyccoinbasepb.RegisterKycServer(s, serv)
	}))
	env.client = kyccoinbasepb.NewKycClient(cc)
	return env
}

type testUser struct {
	id      *commonpb.UserId
	keyPair model.KeyPair
}

func (e *serverEnv) newUser(staff bool) testUser {
	e.t.Helper()
	u := testUser{id: model.MustGenerateUserID(), keyPair: model.MustGenerateKeyPair()}
	_, err := e.accounts.Bind(e.ctx, u.id, u.keyPair.Proto())
	require.NoError(e.t, err)
	e.accounts.setStaff(u.id, staff)
	e.profiles.link(u.id, e.phone)
	return u
}

func (e *serverEnv) get(u testUser) (*kyccoinbasepb.GetKycStateResponse, error) {
	req := &kyccoinbasepb.GetKycStateRequest{Product: product}
	require.NoError(e.t, u.keyPair.Auth(req, &req.Auth))
	return e.client.GetKycState(e.ctx, req)
}

func (e *serverEnv) mustGet(u testUser) *kyccoinbasepb.KycState {
	e.t.Helper()
	resp, err := e.get(u)
	require.NoError(e.t, err)
	require.Equal(e.t, kyccoinbasepb.GetKycStateResponse_OK, resp.Result)
	require.NotNil(e.t, resp.KycState)
	return resp.KycState
}

func (e *serverEnv) start(u testUser) (*kyccoinbasepb.StartKycResponse, error) {
	req := &kyccoinbasepb.StartKycRequest{Product: product}
	require.NoError(e.t, u.keyPair.Auth(req, &req.Auth))
	return e.client.StartKyc(e.ctx, req)
}

func (e *serverEnv) mustStart(u testUser) *kyccoinbasepb.KycState {
	e.t.Helper()
	resp, err := e.start(u)
	require.NoError(e.t, err)
	require.Equal(e.t, kyccoinbasepb.StartKycResponse_OK, resp.Result)
	require.NotNil(e.t, resp.KycState)
	return resp.KycState
}

func (e *serverEnv) submit(u testUser, req *kyccoinbasepb.SubmitKycRequest) (*kyccoinbasepb.SubmitKycResponse, error) {
	req.Product = product
	require.NoError(e.t, u.keyPair.Auth(req, &req.Auth))
	return e.client.SubmitKyc(e.ctx, req)
}

func (e *serverEnv) mustSubmit(u testUser, req *kyccoinbasepb.SubmitKycRequest) *kyccoinbasepb.KycState {
	e.t.Helper()
	resp, err := e.submit(u, req)
	require.NoError(e.t, err)
	require.Equal(e.t, kyccoinbasepb.SubmitKycResponse_OK, resp.Result)
	require.NotNil(e.t, resp.KycState)
	require.Empty(e.t, resp.InvalidFields)
	require.Empty(e.t, resp.InvalidTermsOfServiceAcceptances)
	return resp.KycState
}

// customerID returns the Coinbase customer the store records for u.
func (e *serverEnv) customerID(u testUser) string {
	e.t.Helper()
	record, err := e.store.GetCustomer(e.ctx, u.id)
	require.NoError(e.t, err)
	return record.CustomerID
}

func (e *serverEnv) inspect(u testUser) customerstest.Snapshot {
	e.t.Helper()
	snap, ok := e.fake.Inspect(e.customerID(u))
	require.True(e.t, ok)
	return snap
}

func (e *serverEnv) requireNoRecord(u testUser) {
	e.t.Helper()
	_, err := e.store.GetCustomer(e.ctx, u.id)
	require.ErrorIs(e.t, err, coinbase.ErrNotFound)
}

// dateOfBirth is a date of birth age years before the env's clock, on 4 March.
func (e *serverEnv) dateOfBirth(age int) *kyccoinbasepb.DateOfBirth {
	return &kyccoinbasepb.DateOfBirth{Year: uint32(e.now.Year() - age), Month: 3, Day: 4}
}

func (e *serverEnv) individual() *kyccoinbasepb.Individual {
	return &kyccoinbasepb.Individual{
		LegalName:        &kyccoinbasepb.LegalName{FirstName: testFirstName, LastName: testLastName},
		DateOfBirth:      e.dateOfBirth(30),
		Address:          testAddress(),
		Ssn:              &kyccoinbasepb.Ssn{Value: testSsn},
		Email:            &commonpb.EmailAddress{Value: testEmail},
		PhoneNumber:      &commonpb.PhoneNumber{Value: testPhone},
		PurposeOfAccount: kyccoinbasepb.PurposeOfAccount_PURPOSE_OF_ACCOUNT_PAYMENTS_TO_FRIENDS,
		SourceOfFunds:    kyccoinbasepb.SourceOfFunds_SOURCE_OF_FUNDS_SALARY,
	}
}

func testAddress() *kyccoinbasepb.Address {
	return &kyccoinbasepb.Address{
		Line1:       testLine1,
		City:        testCity,
		State:       testState,
		PostalCode:  testPostalCode,
		CountryCode: "US",
	}
}

func w9() *kyccoinbasepb.TaxAttestation {
	return &kyccoinbasepb.TaxAttestation{
		Form:             &kyccoinbasepb.TaxAttestation_UsW9_{UsW9: &kyccoinbasepb.TaxAttestation_UsW9{}},
		EdeliveryConsent: true,
	}
}

func acceptance(versionID, language string) *kyccoinbasepb.TermsOfServiceAcceptance {
	return &kyccoinbasepb.TermsOfServiceAcceptance{VersionId: versionID, Language: &commonpb.Locale{Value: language}}
}

// requirements is a KycState's requirements by kind.
type requirements struct {
	fields map[kyccoinbasepb.Field]kyccoinbasepb.Requirement_Reason
	terms  map[string]*kyccoinbasepb.Requirement
	forms  map[kyccoinbasepb.TaxForm]kyccoinbasepb.Requirement_Reason
}

func requirementsOf(t *testing.T, state *kyccoinbasepb.KycState) requirements {
	t.Helper()
	r := requirements{
		fields: map[kyccoinbasepb.Field]kyccoinbasepb.Requirement_Reason{},
		terms:  map[string]*kyccoinbasepb.Requirement{},
		forms:  map[kyccoinbasepb.TaxForm]kyccoinbasepb.Requirement_Reason{},
	}
	for _, req := range state.Requirements {
		switch kind := req.Kind.(type) {
		case *kyccoinbasepb.Requirement_Field:
			r.fields[kind.Field] = req.Reason
		case *kyccoinbasepb.Requirement_TermsOfService:
			r.terms[kind.TermsOfService.VersionId] = req
		case *kyccoinbasepb.Requirement_TaxForm:
			r.forms[kind.TaxForm] = req.Reason
		default:
			t.Fatalf("unexpected requirement kind %T", kind)
		}
	}
	return r
}

func reasons(fields []kyccoinbasepb.Field, reason kyccoinbasepb.Requirement_Reason) map[kyccoinbasepb.Field]kyccoinbasepb.Requirement_Reason {
	out := map[kyccoinbasepb.Field]kyccoinbasepb.Requirement_Reason{}
	for _, f := range fields {
		out[f] = reason
	}
	return out
}

// requireStartedState requires the state of a user who has just started:
// ACTION_REQUIRED, the eight fields and the W-9 missing, and no terms.
func requireStartedState(t *testing.T, state *kyccoinbasepb.KycState) {
	t.Helper()
	require.Equal(t, product, state.Product)
	require.Equal(t, kyccoinbasepb.KycState_ACTION_REQUIRED, state.Status)
	r := requirementsOf(t, state)
	require.Equal(t, reasons(eightFields, kyccoinbasepb.Requirement_MISSING), r.fields)
	require.Equal(t, map[kyccoinbasepb.TaxForm]kyccoinbasepb.Requirement_Reason{
		kyccoinbasepb.TaxForm_TAX_FORM_US_W9: kyccoinbasepb.Requirement_MISSING,
	}, r.forms)
	require.Empty(t, r.terms)
	require.Len(t, state.Requirements, 9)
}

// requireTermsOnly requires ACTION_REQUIRED listing the fake's one terms
// version and nothing else.
func requireTermsOnly(t *testing.T, state *kyccoinbasepb.KycState) {
	t.Helper()
	require.Equal(t, kyccoinbasepb.KycState_ACTION_REQUIRED, state.Status)
	require.Len(t, state.Requirements, 1)
	r := requirementsOf(t, state)
	terms, ok := r.terms[customerstest.TosVersionID]
	require.True(t, ok)
	require.Equal(t, kyccoinbasepb.Requirement_MISSING, terms.Reason)
	tos := terms.GetTermsOfService()
	require.Equal(t, customerstest.TosURL, tos.Url)
	require.Len(t, tos.Languages, 1)
	require.Equal(t, "en", tos.Languages[0].Value)
}

func requireStatus(t *testing.T, want kyccoinbasepb.KycState_Status, state *kyccoinbasepb.KycState) {
	t.Helper()
	require.Equal(t, want.String(), state.Status.String())
}

// requireNotLogged requires that no entry logged, by the server or by what
// runs before its handlers, mentions any of values, in its message or any
// field (see renderEntry and mentions).
func requireNotLogged(t *testing.T, logs *observer.ObservedLogs, values ...string) {
	t.Helper()
	for _, entry := range logs.All() {
		requireNotMentioned(t, fmt.Sprintf("log entry %q", entry.Message), renderEntry(t, entry), values)
	}
}

// requireNotMentioned requires that text mentions none of values.
func requireNotMentioned(t *testing.T, where, text string, values []string) {
	t.Helper()
	for _, v := range values {
		require.False(t, mentions(text, v), "%s mentions submitted value %q", where, v)
	}
}

// mentions reports whether text contains value, judged with all whitespace
// removed from both, so that a value is found however a renderer spaced it
// (protobuf's text form inserts spaces at random).
func mentions(text, value string) bool {
	return strings.Contains(withoutSpace(text), withoutSpace(value))
}

func withoutSpace(s string) string {
	return strings.Join(strings.Fields(s), "")
}

// renderEntry is everything an observed log entry says: its message, its
// fields as fmt prints them, and the entry as zap's JSON encoder writes it.
// Between them a value is found whether a field held it as a string, an
// error, a Stringer (a proto message), or a struct zap reflects on. A string
// field that reads as base64 is rendered decoded as well: a request logged as
// its marshalled bytes, encoded, holds every string it carries in the clear.
func renderEntry(t *testing.T, entry observer.LoggedEntry) string {
	t.Helper()
	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	buf, err := encoder.EncodeEntry(entry.Entry, entry.Context)
	require.NoError(t, err)
	defer buf.Free()
	text := entry.Message + " " + fmt.Sprint(entry.ContextMap()) + " " + buf.String()
	for _, field := range entry.Context {
		if field.Type != zapcore.StringType {
			continue
		}
		for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
			if decoded, err := encoding.DecodeString(field.String); err == nil {
				text += " " + string(decoded)
			}
		}
	}
	return text
}

// staffAccounts is an account.Store whose staff flag a test sets: the
// in-memory store answers IsStaff false for everyone and has no setter.
type staffAccounts struct {
	account.Store

	mu        sync.Mutex
	staff     map[string]bool
	failStaff bool
}

func newStaffAccounts(db account.Store) *staffAccounts {
	return &staffAccounts{Store: db, staff: make(map[string]bool)}
}

func (a *staffAccounts) setStaff(userID *commonpb.UserId, isStaff bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.staff[string(userID.Value)] = isStaff
}

func (a *staffAccounts) setFailStaff(fail bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failStaff = fail
}

func (a *staffAccounts) IsStaff(_ context.Context, userID *commonpb.UserId) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failStaff {
		return false, errors.New("staff flag unavailable")
	}
	return a.staff[string(userID.Value)], nil
}

// linkedProfiles is the profile store the env's server reads what a user has
// verified from: their linked phone number and email address. A user holds
// what a test links to them, holds neither, or is unknown to the store. Only
// GetFullProfile is implemented: the embedded Store is nil, so any other read
// the server came to make panics.
//
// It is not a real profile store because one links a number or an address to
// a single user at a time, and these tests have many users submit the same
// placeholders.
type linkedProfiles struct {
	profile.Store

	mu       sync.Mutex
	contacts map[string]linkedContact
	reads    int
	err      error
}

// linkedContact is what one user's profile has linked, each empty for none.
// emailUnverified marks emailAddress as linked unverified.
type linkedContact struct {
	phoneNumber     string
	emailAddress    string
	emailUnverified bool
}

func newLinkedProfiles() *linkedProfiles {
	return &linkedProfiles{contacts: make(map[string]linkedContact)}
}

// link gives the user a profile whose linked number is phoneNumber, or one
// with no number linked when it is empty. A linked email address is kept.
func (p *linkedProfiles) link(userID *commonpb.UserId, phoneNumber string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	contact := p.contacts[string(userID.Value)]
	contact.phoneNumber = phoneNumber
	p.contacts[string(userID.Value)] = contact
}

// linkEmail gives the user a profile whose linked email address is
// emailAddress, verified, or one with no address linked when it is empty. A
// linked number is kept.
func (p *linkedProfiles) linkEmail(userID *commonpb.UserId, emailAddress string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	contact := p.contacts[string(userID.Value)]
	contact.emailAddress = emailAddress
	contact.emailUnverified = false
	p.contacts[string(userID.Value)] = contact
}

// linkUnverifiedEmail is linkEmail for an address linked unverified.
func (p *linkedProfiles) linkUnverifiedEmail(userID *commonpb.UserId, emailAddress string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	contact := p.contacts[string(userID.Value)]
	contact.emailAddress = emailAddress
	contact.emailUnverified = true
	p.contacts[string(userID.Value)] = contact
}

// forget leaves the store with no profile for the user.
func (p *linkedProfiles) forget(userID *commonpb.UserId) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.contacts, string(userID.Value))
}

// setErr makes every read fail with err, until set to nil.
func (p *linkedProfiles) setErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// readCount is the number of profile reads made, failed ones included.
func (p *linkedProfiles) readCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reads
}

// GetFullProfile returns a profile carrying the linked number and address
// alone, and those only as private fields, as the real stores do.
func (p *linkedProfiles) GetFullProfile(_ context.Context, id *commonpb.UserId, includePrivateProfile bool) (*profilepb.UserProfile, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reads++
	if p.err != nil {
		return nil, p.err
	}
	contact, ok := p.contacts[string(id.Value)]
	if !ok {
		return nil, profile.ErrNotFound
	}
	userProfile := &profilepb.UserProfile{}
	if includePrivateProfile && contact.phoneNumber != "" {
		userProfile.PhoneNumber = &commonpb.PhoneNumber{Value: contact.phoneNumber}
	}
	if includePrivateProfile && contact.emailAddress != "" {
		userProfile.EmailAddress = &commonpb.EmailAddress{Value: contact.emailAddress}
		userProfile.IsEmailAddressVerified = !contact.emailUnverified
	}
	return userProfile, nil
}

// stubResolver resolves every request to the address or error a test sets.
type stubResolver struct {
	mu   sync.Mutex
	addr netip.Addr
	err  error
}

func (r *stubResolver) set(addr netip.Addr, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addr, r.err = addr, err
}

func (r *stubResolver) resolve(context.Context) (netip.Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.addr, r.err
}

// forgetfulStore answers ErrNotFound to the first GetCustomer, as a store
// would to a StartKyc racing the one that records the user's customer.
type forgetfulStore struct {
	coinbase.Store

	mu     sync.Mutex
	forgot bool
}

func (s *forgetfulStore) GetCustomer(ctx context.Context, userID *commonpb.UserId) (*coinbase.CustomerRecord, error) {
	s.mu.Lock()
	forget := !s.forgot
	s.forgot = true
	s.mu.Unlock()
	if forget {
		return nil, coinbase.ErrNotFound
	}
	return s.Store.GetCustomer(ctx, userID)
}

func testServer_Auth(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)

	t.Run("unknown key", func(t *testing.T) {
		stranger := testUser{id: model.MustGenerateUserID(), keyPair: model.MustGenerateKeyPair()}

		_, err := env.get(stranger)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		_, err = env.start(stranger)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		_, err = env.submit(stranger, &kyccoinbasepb.SubmitKycRequest{Individual: env.individual()})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	})

	t.Run("bad signature", func(t *testing.T) {
		u := env.newUser(true)
		req := &kyccoinbasepb.SubmitKycRequest{Product: product}
		require.NoError(t, u.keyPair.Auth(req, &req.Auth))
		req.Individual = env.individual() // changed after signing
		_, err := env.client.SubmitKyc(env.ctx, req)
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		env.requireNoRecord(u)
	})

	require.Zero(t, env.fake.Requests())
}

func testServer_StaffGate(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)

	t.Run("not staff", func(t *testing.T) {
		u := env.newUser(false)

		getResp, err := env.get(u)
		require.NoError(t, err)
		require.Equal(t, kyccoinbasepb.GetKycStateResponse_DENIED, getResp.Result)
		require.Nil(t, getResp.KycState)

		startResp, err := env.start(u)
		require.NoError(t, err)
		require.Equal(t, kyccoinbasepb.StartKycResponse_DENIED, startResp.Result)
		require.Nil(t, startResp.KycState)

		submitResp, err := env.submit(u, &kyccoinbasepb.SubmitKycRequest{
			Individual:      env.individual(),
			TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()},
		})
		require.NoError(t, err)
		require.Equal(t, kyccoinbasepb.SubmitKycResponse_DENIED, submitResp.Result)
		require.Nil(t, submitResp.KycState)

		require.Zero(t, env.fake.Requests())
		require.Empty(t, env.fake.CustomerIDs())
		env.requireNoRecord(u)
	})

	t.Run("staff flag unreadable", func(t *testing.T) {
		u := env.newUser(true)
		env.accounts.setFailStaff(true)
		defer env.accounts.setFailStaff(false)

		_, err := env.get(u)
		require.Equal(t, codes.Internal, status.Code(err))
		_, err = env.start(u)
		require.Equal(t, codes.Internal, status.Code(err))
		_, err = env.submit(u, &kyccoinbasepb.SubmitKycRequest{Individual: env.individual()})
		require.Equal(t, codes.Internal, status.Code(err))

		require.Zero(t, env.fake.Requests())
		env.requireNoRecord(u)
	})
}

func testServer_GetBeforeStart(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)

	state := env.mustGet(u)
	require.Equal(t, product, state.Product)
	requireStatus(t, kyccoinbasepb.KycState_NOT_STARTED, state)
	require.Empty(t, state.Requirements)

	require.Zero(t, env.fake.Requests())
	require.Empty(t, env.fake.CustomerIDs())
	env.requireNoRecord(u)
}

func testServer_Start(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)

	state := env.mustStart(u)
	requireStartedState(t, state)

	ids := env.fake.CustomerIDs()
	require.Len(t, ids, 1)
	record, err := env.store.GetCustomer(env.ctx, u.id)
	require.NoError(t, err)
	require.Equal(t, ids[0], record.CustomerID)
	require.True(t, env.now.Equal(record.CreatedAt))

	snap := env.inspect(u)
	require.Equal(t, stubIP, snap.RequesterIPAddress)
	require.False(t, snap.Blocked)

	// Starting again changes nothing: one read, no write.
	before := env.fake.Requests()
	again := env.mustStart(u)
	require.True(t, proto.Equal(state, again))
	require.Equal(t, 1, env.fake.Requests()-before)
	require.Equal(t, ids, env.fake.CustomerIDs())

	require.True(t, proto.Equal(state, env.mustGet(u)))
}

func testServer_StartConcurrent(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)

	const n = 8
	var wg sync.WaitGroup
	states := make([]*kyccoinbasepb.KycState, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := &kyccoinbasepb.StartKycRequest{Product: product}
			if err := u.keyPair.Auth(req, &req.Auth); err != nil {
				errs[i] = err
				return
			}
			resp, err := env.client.StartKyc(env.ctx, req)
			if err != nil {
				errs[i] = err
				return
			}
			states[i] = resp.KycState
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		requireStartedState(t, states[i])
	}
	ids := env.fake.CustomerIDs()
	require.Len(t, ids, 1)
	require.Equal(t, ids[0], env.customerID(u))
}

func testServer_StartLostCreate(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)

	// The create lands, but its response is lost.
	env.fake.FailNextAfterApply(1, 503)
	_, err := env.start(u)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Len(t, env.fake.CustomerIDs(), 1)
	env.requireNoRecord(u)

	// The retry creates under the same key and gets the same customer.
	requireStartedState(t, env.mustStart(u))
	ids := env.fake.CustomerIDs()
	require.Len(t, ids, 1)
	require.Equal(t, ids[0], env.customerID(u))
}

func testServer_StartRecordedCustomerStands(t *testing.T, accounts account.Store, store coinbase.Store) {
	forgetful := &forgetfulStore{Store: store}
	env := newServerEnv(t, accounts, forgetful)
	u := env.newUser(true)

	// The user already has a customer, created under some other key, which a
	// StartKyc racing this one recorded.
	recorded, err := env.fake.Client().Create(env.ctx, uuid.NewString(), nil)
	require.NoError(t, err)
	_, created, err := store.PutCustomer(env.ctx, &coinbase.CustomerRecord{UserID: u.id, CustomerID: recorded.ID, CreatedAt: env.now})
	require.NoError(t, err)
	require.True(t, created)

	requireStartedState(t, env.mustStart(u))

	// A second customer was created and is referred to by nothing; the
	// recorded one was started.
	ids := env.fake.CustomerIDs()
	require.Len(t, ids, 2)
	require.Equal(t, recorded.ID, env.customerID(u))
	snap, ok := env.fake.Inspect(recorded.ID)
	require.True(t, ok)
	require.Equal(t, stubIP, snap.RequesterIPAddress)

	var other string
	for _, id := range ids {
		if id != recorded.ID {
			other = id
		}
	}
	warnings := env.logs.FilterMessageSnippet("other than the one recorded").All()
	require.Len(t, warnings, 1)
	require.Equal(t, zapcore.WarnLevel, warnings[0].Level)
	fields := warnings[0].ContextMap()
	require.Equal(t, recorded.ID, fields["customer_id"])
	require.Equal(t, other, fields["created_customer_id"])
}

func testServer_StartRequesterIP(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)

	for _, tc := range []struct {
		name string
		addr netip.Addr
		err  error
	}{
		{"resolver fails", netip.Addr{}, errors.New("no forwarded address")},
		{"private address", netip.MustParseAddr("10.0.0.1"), nil},
		{"loopback address", netip.MustParseAddr("127.0.0.1"), nil},
		{"invalid address", netip.Addr{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := env.newUser(true)
			env.resolver.set(tc.addr, tc.err)
			defer env.resolver.set(netip.MustParseAddr(stubIP), nil)

			_, err := env.start(u)
			require.Equal(t, codes.Internal, status.Code(err))
			require.Zero(t, env.fake.Requests())
			require.Empty(t, env.fake.CustomerIDs())
			env.requireNoRecord(u)
		})
	}

	requireNotLogged(t, env.logs, "10.0.0.1", "127.0.0.1")
}

// testServer_CustomerNotStarted covers a user whose customer exists but has
// requested nothing: a start whose create was recorded but whose update
// failed.
func testServer_CustomerNotStarted(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)

	bare, err := env.fake.Client().Create(env.ctx, uuid.NewString(), nil)
	require.NoError(t, err)
	_, _, err = env.store.PutCustomer(env.ctx, &coinbase.CustomerRecord{UserID: u.id, CustomerID: bare.ID, CreatedAt: env.now})
	require.NoError(t, err)

	requireStatus(t, kyccoinbasepb.KycState_NOT_STARTED, env.mustGet(u))

	resp, err := env.submit(u, &kyccoinbasepb.SubmitKycRequest{Individual: env.individual()})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_NOT_STARTED, resp.Result)
	require.Nil(t, resp.KycState)

	// The start resolves the address before its update; a failure sends
	// nothing.
	env.resolver.set(netip.Addr{}, errors.New("no forwarded address"))
	before := env.fake.Requests()
	_, err = env.start(u)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Equal(t, 1, env.fake.Requests()-before)
	requireStatus(t, kyccoinbasepb.KycState_NOT_STARTED, env.mustGet(u))

	env.resolver.set(netip.MustParseAddr(stubIP), nil)
	requireStartedState(t, env.mustStart(u))
	require.Equal(t, []string{bare.ID}, env.fake.CustomerIDs())
	require.Equal(t, stubIP, env.inspect(u).RequesterIPAddress)
}

func testServer_SubmitBeforeStart(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)

	resp, err := env.submit(u, &kyccoinbasepb.SubmitKycRequest{
		Individual:      env.individual(),
		TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()},
	})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_NOT_STARTED, resp.Result)
	require.Nil(t, resp.KycState)
	require.Zero(t, env.fake.Requests())
	env.requireNoRecord(u)
}

func testServer_SubmitInvalid(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)
	started := env.mustStart(u)

	type invalid struct {
		fields map[kyccoinbasepb.Field]kyccoinbasepb.SubmitKycResponse_InvalidField_Reason
		terms  map[string]kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_Reason
	}
	check := func(t *testing.T, req *kyccoinbasepb.SubmitKycRequest, want invalid, wantRequests int) {
		t.Helper()
		before := env.fake.Requests()
		resp, err := env.submit(u, req)
		require.NoError(t, err)
		require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
		require.Nil(t, resp.KycState)

		gotFields := map[kyccoinbasepb.Field]kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{}
		for _, f := range resp.InvalidFields {
			gotFields[f.Field] = f.Reason
		}
		gotTerms := map[string]kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_Reason{}
		for _, a := range resp.InvalidTermsOfServiceAcceptances {
			gotTerms[a.VersionId] = a.Reason
		}
		if want.fields == nil {
			want.fields = map[kyccoinbasepb.Field]kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{}
		}
		if want.terms == nil {
			want.terms = map[string]kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_Reason{}
		}
		require.Equal(t, want.fields, gotFields)
		require.Equal(t, want.terms, gotTerms)
		require.Equal(t, wantRequests, env.fake.Requests()-before)
	}
	dobField := func(reason kyccoinbasepb.SubmitKycResponse_InvalidField_Reason) invalid {
		return invalid{fields: map[kyccoinbasepb.Field]kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{
			kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH: reason,
		}}
	}
	addressField := func(reason kyccoinbasepb.SubmitKycResponse_InvalidField_Reason) invalid {
		return invalid{fields: map[kyccoinbasepb.Field]kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{
			kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS: reason,
		}}
	}

	// Born on this very date 18 years ago: not yet 18 everywhere on Earth.
	underage := &kyccoinbasepb.DateOfBirth{Year: uint32(env.now.Year() - 18), Month: uint32(env.now.Month()), Day: uint32(env.now.Day())}

	// Refused before sending: only the state read reaches Coinbase.
	for _, tc := range []struct {
		name       string
		individual *kyccoinbasepb.Individual
		want       invalid
	}{
		{"underage", &kyccoinbasepb.Individual{DateOfBirth: underage}, dobField(kyccoinbasepb.SubmitKycResponse_InvalidField_UNDERAGE)},
		{"impossible date", &kyccoinbasepb.Individual{DateOfBirth: &kyccoinbasepb.DateOfBirth{Year: 2000, Month: 2, Day: 30}}, dobField(kyccoinbasepb.SubmitKycResponse_InvalidField_MALFORMED)},
		{"110 or older", &kyccoinbasepb.Individual{DateOfBirth: env.dateOfBirth(111)}, dobField(kyccoinbasepb.SubmitKycResponse_InvalidField_MALFORMED)},
		{"non-US country", &kyccoinbasepb.Individual{Address: &kyccoinbasepb.Address{Line1: testLine1, City: testCity, State: "ON", PostalCode: testPostalCode, CountryCode: "CA"}}, addressField(kyccoinbasepb.SubmitKycResponse_InvalidField_UNSUPPORTED_REGION)},
		{"not a state", &kyccoinbasepb.Individual{Address: &kyccoinbasepb.Address{Line1: testLine1, City: testCity, State: "ZZ", PostalCode: testPostalCode, CountryCode: "US"}}, addressField(kyccoinbasepb.SubmitKycResponse_InvalidField_MALFORMED)},
		{"email coinbase refuses", &kyccoinbasepb.Individual{Email: &commonpb.EmailAddress{Value: "test.user@example..com"}}, invalid{fields: map[kyccoinbasepb.Field]kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{
			kyccoinbasepb.Field_FIELD_EMAIL: kyccoinbasepb.SubmitKycResponse_InvalidField_MALFORMED,
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check(t, &kyccoinbasepb.SubmitKycRequest{Individual: tc.individual}, tc.want, 1)
		})
	}

	t.Run("acceptance before the address", func(t *testing.T) {
		// Not listed, so it is sent, and Coinbase's refusal is read as
		// NOT_ASKED.
		check(t, &kyccoinbasepb.SubmitKycRequest{
			TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "en")},
		}, invalid{terms: map[string]kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_Reason{
			customerstest.TosVersionID: kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_NOT_ASKED,
		}}, 2)
	})

	// Nothing was applied: the state is still the one the start left.
	require.True(t, proto.Equal(started, env.mustGet(u)))

	// With the address in, the terms are listed.
	state := env.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{Address: testAddress()}})
	require.Contains(t, requirementsOf(t, state).terms, customerstest.TosVersionID)

	t.Run("language the version is not published in", func(t *testing.T) {
		check(t, &kyccoinbasepb.SubmitKycRequest{
			TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "fr")},
		}, invalid{terms: map[string]kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_Reason{
			customerstest.TosVersionID: kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_UNSUPPORTED_LANGUAGE,
		}}, 1)
	})

	t.Run("language in another case than listed", func(t *testing.T) {
		// Coinbase compares the language as written, so the server does too.
		check(t, &kyccoinbasepb.SubmitKycRequest{
			TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "EN")},
		}, invalid{terms: map[string]kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_Reason{
			customerstest.TosVersionID: kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_UNSUPPORTED_LANGUAGE,
		}}, 1)
	})

	t.Run("both lists", func(t *testing.T) {
		check(t, &kyccoinbasepb.SubmitKycRequest{
			Individual:                &kyccoinbasepb.Individual{DateOfBirth: underage},
			TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "fr")},
		}, invalid{
			fields: map[kyccoinbasepb.Field]kyccoinbasepb.SubmitKycResponse_InvalidField_Reason{
				kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH: kyccoinbasepb.SubmitKycResponse_InvalidField_UNDERAGE,
			},
			terms: map[string]kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_Reason{
				customerstest.TosVersionID: kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_UNSUPPORTED_LANGUAGE,
			},
		}, 1)
	})

	require.True(t, proto.Equal(state, env.mustGet(u)))
	require.False(t, env.inspect(u).Blocked)
}

// testServer_SubmitPhoneNumber covers the phone number's check against the
// user's profile: a number must be the one linked to it.
func testServer_SubmitPhoneNumber(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)
	started := env.mustStart(u)

	phoneOnly := func(phoneNumber string) *kyccoinbasepb.SubmitKycRequest {
		return &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{
			PhoneNumber: &commonpb.PhoneNumber{Value: phoneNumber},
		}}
	}
	// requireNotVerified requires req to be refused on the phone number
	// alone, with only the state read reaching Coinbase.
	requireNotVerified := func(t *testing.T, req *kyccoinbasepb.SubmitKycRequest) {
		t.Helper()
		before := env.fake.Requests()
		resp, err := env.submit(u, req)
		require.NoError(t, err)
		require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
		require.Nil(t, resp.KycState)
		require.Empty(t, resp.InvalidTermsOfServiceAcceptances)
		require.Len(t, resp.InvalidFields, 1)
		require.Equal(t, kyccoinbasepb.Field_FIELD_PHONE_NUMBER, resp.InvalidFields[0].Field)
		require.Equal(t, kyccoinbasepb.SubmitKycResponse_InvalidField_NOT_VERIFIED, resp.InvalidFields[0].Reason)
		require.Equal(t, 1, env.fake.Requests()-before)
	}

	t.Run("another number than the one linked", func(t *testing.T) {
		requireNotVerified(t, phoneOnly(otherPhone))
		// With everything else valid beside it, nothing of the submission is
		// sent.
		individual := env.individual()
		individual.PhoneNumber = &commonpb.PhoneNumber{Value: otherPhone}
		requireNotVerified(t, &kyccoinbasepb.SubmitKycRequest{Individual: individual, TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()}})
		require.Empty(t, env.inspect(u).Individual.FullSsn)
	})

	t.Run("compared as written", func(t *testing.T) {
		// One digit off, and the linked number with a digit more.
		requireNotVerified(t, phoneOnly(testPhone[:len(testPhone)-1]+"3"))
		requireNotVerified(t, phoneOnly(testPhone+"0"))
	})

	t.Run("no number linked", func(t *testing.T) {
		env.profiles.link(u.id, "")
		defer env.profiles.link(u.id, testPhone)
		requireNotVerified(t, phoneOnly(testPhone))
	})

	t.Run("no profile", func(t *testing.T) {
		env.profiles.forget(u.id)
		defer env.profiles.link(u.id, testPhone)
		requireNotVerified(t, phoneOnly(testPhone))
	})

	t.Run("another user's number", func(t *testing.T) {
		other := env.newUser(true)
		env.profiles.link(other.id, otherPhone)
		requireNotVerified(t, phoneOnly(otherPhone))
	})

	t.Run("listed with the other fields refused, in field order", func(t *testing.T) {
		resp, err := env.submit(u, &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{
			DateOfBirth: &kyccoinbasepb.DateOfBirth{Year: 2000, Month: 2, Day: 30},
			Email:       &commonpb.EmailAddress{Value: "test.user@example..com"},
			PhoneNumber: &commonpb.PhoneNumber{Value: otherPhone},
		}})
		require.NoError(t, err)
		require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
		var fields []kyccoinbasepb.Field
		for _, f := range resp.InvalidFields {
			fields = append(fields, f.Field)
		}
		require.Equal(t, []kyccoinbasepb.Field{
			kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH,
			kyccoinbasepb.Field_FIELD_EMAIL,
			kyccoinbasepb.Field_FIELD_PHONE_NUMBER,
		}, fields)
	})

	t.Run("the profile read fails", func(t *testing.T) {
		env.profiles.setErr(errors.New("profile store unavailable"))
		defer env.profiles.setErr(nil)
		before := env.fake.Requests()
		_, err := env.submit(u, phoneOnly(testPhone))
		require.Equal(t, codes.Internal, status.Code(err))
		// The state read alone: nothing was sent.
		require.Equal(t, 1, env.fake.Requests()-before)
		require.Empty(t, env.inspect(u).Individual.PhoneNumber)
	})

	// Nothing above was applied, and Coinbase holds no number.
	require.True(t, proto.Equal(started, env.mustGet(u)))
	require.Empty(t, env.inspect(u).Individual.PhoneNumber)

	t.Run("the profile is read only for a submission that sets a number", func(t *testing.T) {
		// A failing store proves it: a submission without a number passes.
		env.profiles.setErr(errors.New("profile store unavailable"))
		defer env.profiles.setErr(nil)
		reads := env.profiles.readCount()
		env.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{
			LegalName: &kyccoinbasepb.LegalName{FirstName: testFirstName, LastName: testLastName},
		}})
		_, err := env.get(u)
		require.NoError(t, err)
		require.Equal(t, reads, env.profiles.readCount())
	})

	t.Run("the linked number is submitted", func(t *testing.T) {
		reads := env.profiles.readCount()
		env.mustSubmit(u, phoneOnly(testPhone))
		require.Equal(t, reads+1, env.profiles.readCount())
		require.Equal(t, testPhone, env.inspect(u).Individual.PhoneNumber)
	})

	t.Run("a number linked since is the one that passes", func(t *testing.T) {
		// The number is judged against the profile as it is now, whatever
		// Coinbase was given before.
		env.profiles.link(u.id, otherPhone)
		defer env.profiles.link(u.id, testPhone)
		requireNotVerified(t, phoneOnly(testPhone))
		env.mustSubmit(u, phoneOnly(otherPhone))
		require.Equal(t, otherPhone, env.inspect(u).Individual.PhoneNumber)
	})
}

// testServer_SubmitEmail covers the email address's check against the user's
// profile, which the server makes only when built to require a verified
// address (coinbase.WithVerifiedEmailRequired).
func testServer_SubmitEmail(t *testing.T, accounts account.Store, store coinbase.Store) {
	emailOnly := func(email string) *kyccoinbasepb.SubmitKycRequest {
		return &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{
			Email: &commonpb.EmailAddress{Value: email},
		}}
	}
	// A linked address Coinbase would not take: an empty domain label.
	const malformedEmail = "zorvald.quexley@example..com"

	t.Run("not required", func(t *testing.T) {
		env := newServerEnv(t, accounts, store)
		u := env.newUser(true)
		env.mustStart(u)

		// An address is taken as given from a user with none linked.
		env.mustSubmit(u, emailOnly(testEmail))
		require.Equal(t, testEmail, env.inspect(u).Individual.Email)

		// And from one with another linked, with no profile read at all,
		// which a failing store proves.
		env.profiles.linkEmail(u.id, testEmail)
		env.profiles.setErr(errors.New("profile store unavailable"))
		defer env.profiles.setErr(nil)
		reads := env.profiles.readCount()
		env.mustSubmit(u, emailOnly(otherEmail))
		require.Equal(t, reads, env.profiles.readCount())
		require.Equal(t, otherEmail, env.inspect(u).Individual.Email)
	})

	t.Run("required", func(t *testing.T) {
		env := newServerEnv(t, accounts, store, coinbase.WithVerifiedEmailRequired(true))
		u := env.newUser(true)
		env.profiles.linkEmail(u.id, testEmail)
		started := env.mustStart(u)

		// requireNotVerified requires req to be refused on the email address
		// alone, with only the state read reaching Coinbase.
		requireNotVerified := func(t *testing.T, req *kyccoinbasepb.SubmitKycRequest) {
			t.Helper()
			before := env.fake.Requests()
			resp, err := env.submit(u, req)
			require.NoError(t, err)
			require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
			require.Nil(t, resp.KycState)
			require.Empty(t, resp.InvalidTermsOfServiceAcceptances)
			require.Len(t, resp.InvalidFields, 1)
			require.Equal(t, kyccoinbasepb.Field_FIELD_EMAIL, resp.InvalidFields[0].Field)
			require.Equal(t, kyccoinbasepb.SubmitKycResponse_InvalidField_NOT_VERIFIED, resp.InvalidFields[0].Reason)
			require.Equal(t, 1, env.fake.Requests()-before)
		}

		t.Run("another address than the one linked", func(t *testing.T) {
			requireNotVerified(t, emailOnly(otherEmail))
			// With everything else valid beside it, nothing of the submission
			// is sent.
			individual := env.individual()
			individual.Email = &commonpb.EmailAddress{Value: otherEmail}
			requireNotVerified(t, &kyccoinbasepb.SubmitKycRequest{Individual: individual, TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()}})
			require.Empty(t, env.inspect(u).Individual.FullSsn)
		})

		t.Run("compared as written", func(t *testing.T) {
			// No case is folded, in the local part or the domain, and the
			// linked address with a letter more is another address.
			requireNotVerified(t, emailOnly("Zorvald.quexley@example.com"))
			requireNotVerified(t, emailOnly("zorvald.quexley@Example.com"))
			requireNotVerified(t, emailOnly(testEmail+"m"))
		})

		t.Run("no address linked", func(t *testing.T) {
			env.profiles.linkEmail(u.id, "")
			defer env.profiles.linkEmail(u.id, testEmail)
			requireNotVerified(t, emailOnly(testEmail))
		})

		t.Run("the linked address, unverified", func(t *testing.T) {
			env.profiles.linkUnverifiedEmail(u.id, testEmail)
			defer env.profiles.linkEmail(u.id, testEmail)
			requireNotVerified(t, emailOnly(testEmail))
		})

		t.Run("no profile", func(t *testing.T) {
			env.profiles.forget(u.id)
			defer func() {
				env.profiles.link(u.id, testPhone)
				env.profiles.linkEmail(u.id, testEmail)
			}()
			requireNotVerified(t, emailOnly(testEmail))
		})

		t.Run("another user's address", func(t *testing.T) {
			other := env.newUser(true)
			env.profiles.linkEmail(other.id, otherEmail)
			requireNotVerified(t, emailOnly(otherEmail))
		})

		t.Run("an address coinbase would not take is malformed, linked or not", func(t *testing.T) {
			// It is listed once, as MALFORMED, and is not judged against the
			// profile: no profile is read for it, which a failing store proves.
			env.profiles.linkEmail(u.id, malformedEmail)
			defer env.profiles.linkEmail(u.id, testEmail)
			env.profiles.setErr(errors.New("profile store unavailable"))
			defer env.profiles.setErr(nil)
			for _, email := range []string{malformedEmail, "zorvald@example..org"} {
				resp, err := env.submit(u, emailOnly(email))
				require.NoError(t, err)
				require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
				require.Len(t, resp.InvalidFields, 1)
				require.Equal(t, kyccoinbasepb.Field_FIELD_EMAIL, resp.InvalidFields[0].Field)
				require.Equal(t, kyccoinbasepb.SubmitKycResponse_InvalidField_MALFORMED, resp.InvalidFields[0].Reason)
			}
		})

		t.Run("listed with the other fields refused, in field order, off one read", func(t *testing.T) {
			reads := env.profiles.readCount()
			resp, err := env.submit(u, &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{
				DateOfBirth: &kyccoinbasepb.DateOfBirth{Year: 2000, Month: 2, Day: 30},
				Email:       &commonpb.EmailAddress{Value: otherEmail},
				PhoneNumber: &commonpb.PhoneNumber{Value: otherPhone},
			}})
			require.NoError(t, err)
			require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
			require.Len(t, resp.InvalidFields, 3)
			require.Equal(t, kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH, resp.InvalidFields[0].Field)
			require.Equal(t, kyccoinbasepb.SubmitKycResponse_InvalidField_MALFORMED, resp.InvalidFields[0].Reason)
			require.Equal(t, kyccoinbasepb.Field_FIELD_EMAIL, resp.InvalidFields[1].Field)
			require.Equal(t, kyccoinbasepb.SubmitKycResponse_InvalidField_NOT_VERIFIED, resp.InvalidFields[1].Reason)
			require.Equal(t, kyccoinbasepb.Field_FIELD_PHONE_NUMBER, resp.InvalidFields[2].Field)
			require.Equal(t, kyccoinbasepb.SubmitKycResponse_InvalidField_NOT_VERIFIED, resp.InvalidFields[2].Reason)
			require.Equal(t, reads+1, env.profiles.readCount())
		})

		t.Run("the profile read fails", func(t *testing.T) {
			env.profiles.setErr(errors.New("profile store unavailable"))
			defer env.profiles.setErr(nil)
			before := env.fake.Requests()
			_, err := env.submit(u, emailOnly(testEmail))
			require.Equal(t, codes.Internal, status.Code(err))
			// The state read alone: nothing was sent.
			require.Equal(t, 1, env.fake.Requests()-before)
		})

		// Nothing above was applied, and Coinbase holds no address.
		require.True(t, proto.Equal(started, env.mustGet(u)))
		require.Empty(t, env.inspect(u).Individual.Email)

		t.Run("the linked address is submitted", func(t *testing.T) {
			reads := env.profiles.readCount()
			env.mustSubmit(u, emailOnly(testEmail))
			require.Equal(t, reads+1, env.profiles.readCount())
			require.Equal(t, testEmail, env.inspect(u).Individual.Email)
		})

		t.Run("with the linked number, off one read", func(t *testing.T) {
			reads := env.profiles.readCount()
			env.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{
				Email:       &commonpb.EmailAddress{Value: testEmail},
				PhoneNumber: &commonpb.PhoneNumber{Value: testPhone},
			}})
			require.Equal(t, reads+1, env.profiles.readCount())
		})

		t.Run("an address linked since is the one that passes", func(t *testing.T) {
			// The address is judged against the profile as it is now,
			// whatever Coinbase was given before.
			env.profiles.linkEmail(u.id, otherEmail)
			defer env.profiles.linkEmail(u.id, testEmail)
			requireNotVerified(t, emailOnly(testEmail))
			env.mustSubmit(u, emailOnly(otherEmail))
			require.Equal(t, otherEmail, env.inspect(u).Individual.Email)
		})
	})
}

func testServer_SubmitDuplicates(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)
	env.mustStart(u)
	before := env.fake.Requests()

	_, err := env.submit(u, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{
			acceptance(customerstest.TosVersionID, "en"),
			acceptance(customerstest.TosVersionID, "en"),
		},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = env.submit(u, &kyccoinbasepb.SubmitKycRequest{
		TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9(), w9()},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	require.Equal(t, before, env.fake.Requests())
}

func testServer_SubmitEmpty(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)
	started := env.mustStart(u)

	for _, req := range []*kyccoinbasepb.SubmitKycRequest{
		{},
		{Individual: &kyccoinbasepb.Individual{}},
	} {
		before := env.fake.Requests()
		state := env.mustSubmit(u, req)
		require.True(t, proto.Equal(started, state))
		require.Equal(t, 1, env.fake.Requests()-before)
	}
}

func testServer_Approve(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)
	env.mustStart(u)

	// Identity and the W-9 together: the terms are listed now that the
	// address is in, and nothing else.
	state := env.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
		Individual:      env.individual(),
		TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()},
	})
	requireTermsOnly(t, state)

	// The acceptance names the language exactly as the state lists it.
	state = env.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "en")},
	})
	requireStatus(t, kyccoinbasepb.KycState_IN_REVIEW, state)
	require.Empty(t, state.Requirements)
	requireStatus(t, kyccoinbasepb.KycState_IN_REVIEW, env.mustGet(u))

	require.Equal(t, 1, env.fake.CompleteReviews())
	state = env.mustGet(u)
	requireStatus(t, kyccoinbasepb.KycState_VERIFIED, state)
	require.Empty(t, state.Requirements)

	// What Coinbase received.
	stamped := env.now.Add(-10 * time.Second)
	snap := env.inspect(u)
	ind := snap.Individual
	require.Equal(t, testFirstName, ind.FirstName)
	require.Equal(t, testLastName, ind.LastName)
	require.Equal(t, testSsnDashed, ind.FullSsn)
	require.Equal(t, &customers.DateOfBirth{
		Year:  fmt.Sprintf("%04d", env.now.Year()-30),
		Month: "03",
		Day:   "04",
	}, ind.DateOfBirth)
	require.NotNil(t, ind.Address)
	require.NotNil(t, ind.Address.Line2)
	require.Equal(t, "", *ind.Address.Line2)
	require.Equal(t, testLine1, ind.Address.Line1)
	require.Equal(t, testCity, ind.Address.City)
	require.Equal(t, testState, ind.Address.State)
	require.Equal(t, testPostalCode, ind.Address.PostCode)
	require.Equal(t, "US", ind.Address.CountryCode)
	require.Equal(t, testEmail, ind.Email)
	require.Equal(t, testPhone, ind.PhoneNumber)
	require.Equal(t, "payments_to_friends", ind.PurposeOfAccount)
	require.Equal(t, "salary", ind.SourceOfFunds)

	accepted, ok := snap.TosAccepted[customerstest.TosVersionID]
	require.True(t, ok)
	require.Equal(t, "en", accepted.Language)
	require.True(t, stamped.Equal(accepted.AcceptedAt), "acceptance stamped %v, want %v", accepted.AcceptedAt, stamped)

	require.NotNil(t, snap.W9)
	require.True(t, stamped.Equal(snap.W9.AcceptedAt), "w-9 stamped %v, want %v", snap.W9.AcceptedAt, stamped)
	require.True(t, snap.W9.EdeliveryConsent)
	require.False(t, snap.W9.IsExemptBackupWithholding)
	require.Equal(t, stubIP, snap.RequesterIPAddress)

	// Accepting a version already accepted changes nothing.
	state = env.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "en")},
	})
	requireStatus(t, kyccoinbasepb.KycState_VERIFIED, state)

	// In another case than it was listed in, the acceptance is refused as it
	// would have been the first time: the version is no longer listed, so it
	// is sent as given, and Coinbase compares the language as written.
	resp, err := env.submit(u, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "EN")},
	})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
	require.Empty(t, resp.InvalidFields)
	require.Len(t, resp.InvalidTermsOfServiceAcceptances, 1)
	require.Equal(t, customerstest.TosVersionID, resp.InvalidTermsOfServiceAcceptances[0].VersionId)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_UNSUPPORTED_LANGUAGE, resp.InvalidTermsOfServiceAcceptances[0].Reason)
	requireStatus(t, kyccoinbasepb.KycState_VERIFIED, env.mustGet(u))

	// Sent again beside something Coinbase refuses, the accepted version is
	// not blamed for the refusal: Coinbase does not say what it refused, and
	// nothing is named.
	env.fake.RefuseNextUpdate()
	resp, err = env.submit(u, &kyccoinbasepb.SubmitKycRequest{
		Individual:                &kyccoinbasepb.Individual{Email: env.individual().Email},
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "en")},
	})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
	require.Empty(t, resp.InvalidFields)
	require.Empty(t, resp.InvalidTermsOfServiceAcceptances)
	requireStatus(t, kyccoinbasepb.KycState_VERIFIED, env.mustGet(u))

	// Sent again with nothing but a version never asked for, it is named
	// beside that one: neither is listed, and Coinbase refuses the submission
	// whole without saying which it would not take.
	const neverAsked = "placeholder_version_never_asked"
	resp, err = env.submit(u, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{
			acceptance(customerstest.TosVersionID, "en"),
			acceptance(neverAsked, "en"),
		},
	})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
	require.Empty(t, resp.InvalidFields)
	require.Len(t, resp.InvalidTermsOfServiceAcceptances, 2)
	for i, versionID := range []string{customerstest.TosVersionID, neverAsked} {
		require.Equal(t, versionID, resp.InvalidTermsOfServiceAcceptances[i].VersionId)
		require.Equal(t, kyccoinbasepb.SubmitKycResponse_InvalidTermsOfServiceAcceptance_NOT_ASKED, resp.InvalidTermsOfServiceAcceptances[i].Reason)
	}
	requireStatus(t, kyccoinbasepb.KycState_VERIFIED, env.mustGet(u))
}

func testServer_ScreenAtATime(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)
	env.mustStart(u)

	full := env.individual()
	for _, step := range []struct {
		name  string
		req   *kyccoinbasepb.SubmitKycRequest
		field kyccoinbasepb.Field
	}{
		{"legal name", &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{LegalName: full.LegalName}}, kyccoinbasepb.Field_FIELD_LEGAL_NAME},
		{"date of birth", &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{DateOfBirth: full.DateOfBirth}}, kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH},
		{"address", &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{Address: full.Address}}, kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS},
		{"ssn", &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{Ssn: full.Ssn}}, kyccoinbasepb.Field_FIELD_SSN},
		{"w-9", &kyccoinbasepb.SubmitKycRequest{TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()}}, kyccoinbasepb.Field_FIELD_UNKNOWN},
		{"email", &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{Email: full.Email}}, kyccoinbasepb.Field_FIELD_EMAIL},
		{"phone", &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{PhoneNumber: full.PhoneNumber}}, kyccoinbasepb.Field_FIELD_PHONE_NUMBER},
		{"purpose", &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{PurposeOfAccount: full.PurposeOfAccount}}, kyccoinbasepb.Field_FIELD_PURPOSE_OF_ACCOUNT},
		{"source", &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{SourceOfFunds: full.SourceOfFunds}}, kyccoinbasepb.Field_FIELD_SOURCE_OF_FUNDS},
	} {
		state := env.mustSubmit(u, step.req)
		requireStatus(t, kyccoinbasepb.KycState_ACTION_REQUIRED, state)
		r := requirementsOf(t, state)
		if step.field != kyccoinbasepb.Field_FIELD_UNKNOWN {
			require.NotContains(t, r.fields, step.field, step.name)
		} else {
			require.Empty(t, r.forms, step.name)
		}
	}

	state := env.mustGet(u)
	requireTermsOnly(t, state)

	state = env.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "en")},
	})
	requireStatus(t, kyccoinbasepb.KycState_IN_REVIEW, state)

	require.Equal(t, 1, env.fake.CompleteReviews())
	requireStatus(t, kyccoinbasepb.KycState_VERIFIED, env.mustGet(u))
}

func testServer_Decline(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)
	env.mustStart(u)

	// Everything in one request, the acceptance included: Coinbase judges it
	// against the address the same request carries.
	declining := env.individual()
	declining.Ssn = &kyccoinbasepb.Ssn{Value: strings.ReplaceAll(customerstest.SsnDecline, "-", "")}
	state := env.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
		Individual:                declining,
		TermsOfServiceAcceptances: []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "en")},
		TaxAttestations:           []*kyccoinbasepb.TaxAttestation{w9()},
	})
	requireStatus(t, kyccoinbasepb.KycState_IN_REVIEW, state)

	require.Equal(t, 1, env.fake.CompleteReviews())
	state = env.mustGet(u)
	requireStatus(t, kyccoinbasepb.KycState_ACTION_REQUIRED, state)
	r := requirementsOf(t, state)
	require.Equal(t, reasons(identityFields, kyccoinbasepb.Requirement_REJECTED), r.fields)
	require.Empty(t, r.terms)
	require.Empty(t, r.forms)

	// Correcting the SSN, with a new W-9 since it certifies the SSN.
	state = env.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
		Individual:      &kyccoinbasepb.Individual{Ssn: &kyccoinbasepb.Ssn{Value: testSsn}},
		TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()},
	})
	requireStatus(t, kyccoinbasepb.KycState_IN_REVIEW, state)

	require.Equal(t, 1, env.fake.CompleteReviews())
	requireStatus(t, kyccoinbasepb.KycState_VERIFIED, env.mustGet(u))
	snap := env.inspect(u)
	require.Equal(t, testSsnDashed, snap.Individual.FullSsn)
	require.NotNil(t, snap.W9)
}

func testServer_Block(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)
	u := env.newUser(true)
	env.mustStart(u)

	blocking := env.individual()
	blocking.Ssn = &kyccoinbasepb.Ssn{Value: strings.ReplaceAll(customerstest.SsnBlock, "-", "")}
	resp, err := env.submit(u, &kyccoinbasepb.SubmitKycRequest{
		Individual:      blocking,
		TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()},
	})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_BLOCKED, resp.Result)
	require.Nil(t, resp.KycState)
	require.True(t, env.inspect(u).Blocked)

	state := env.mustGet(u)
	requireStatus(t, kyccoinbasepb.KycState_BLOCKED, state)
	require.Empty(t, state.Requirements)

	before := env.fake.Requests()
	state = env.mustStart(u)
	requireStatus(t, kyccoinbasepb.KycState_BLOCKED, state)
	require.Empty(t, state.Requirements)
	require.Equal(t, 1, env.fake.Requests()-before)

	before = env.fake.Requests()
	resp, err = env.submit(u, &kyccoinbasepb.SubmitKycRequest{Individual: env.individual()})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_BLOCKED, resp.Result)
	require.Nil(t, resp.KycState)
	require.Equal(t, 1, env.fake.Requests()-before)
}

func testServer_CoinbaseFailures(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)

	t.Run("start", func(t *testing.T) {
		u := env.newUser(true)
		for _, code := range []int{503, 429} {
			env.fake.FailNext(1, code)
			_, err := env.start(u)
			require.Equal(t, codes.Internal, status.Code(err))
			env.requireNoRecord(u)
		}
		requireStartedState(t, env.mustStart(u))
	})

	t.Run("get and submit", func(t *testing.T) {
		u := env.newUser(true)
		started := env.mustStart(u)

		for _, code := range []int{503, 429, 500} {
			env.fake.FailNext(1, code)
			_, err := env.get(u)
			require.Equal(t, codes.Internal, status.Code(err))

			env.fake.FailNext(1, code)
			_, err = env.submit(u, &kyccoinbasepb.SubmitKycRequest{
				Individual:      env.individual(),
				TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()},
			})
			require.Equal(t, codes.Internal, status.Code(err))

			require.True(t, proto.Equal(started, env.mustGet(u)))
		}

		requireTermsOnly(t, env.mustSubmit(u, &kyccoinbasepb.SubmitKycRequest{
			Individual:      env.individual(),
			TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()},
		}))
	})

	t.Run("recorded customer coinbase does not know", func(t *testing.T) {
		u := env.newUser(true)
		_, _, err := env.store.PutCustomer(env.ctx, &coinbase.CustomerRecord{
			UserID:     u.id,
			CustomerID: "customer_" + uuid.NewString(),
			CreatedAt:  env.now,
		})
		require.NoError(t, err)

		_, err = env.get(u)
		require.Equal(t, codes.Internal, status.Code(err))
		_, err = env.start(u)
		require.Equal(t, codes.Internal, status.Code(err))
		_, err = env.submit(u, &kyccoinbasepb.SubmitKycRequest{Individual: env.individual()})
		require.Equal(t, codes.Internal, status.Code(err))
	})
}

// submission is a whole submission of the env's placeholders with a W-9.
func (e *serverEnv) submission() *kyccoinbasepb.SubmitKycRequest {
	return &kyccoinbasepb.SubmitKycRequest{
		Individual:      e.individual(),
		TaxAttestations: []*kyccoinbasepb.TaxAttestation{w9()},
	}
}

// testServer_SubmitUpdateFailures covers SubmitKyc's update failing after
// its read of the customer succeeded.
func testServer_SubmitUpdateFailures(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)

	// Whatever the failure, one that may pass if retried included, the RPC
	// fails with Internal.
	for _, httpStatus := range []int{503, 429, 500, 404} {
		t.Run(fmt.Sprintf("refused %d before applied", httpStatus), func(t *testing.T) {
			u := env.newUser(true)
			started := env.mustStart(u)

			env.fake.FailNextUpdate(1, httpStatus)
			before := env.fake.Requests()
			_, err := env.submit(u, env.submission())
			require.Equal(t, codes.Internal, status.Code(err))
			// The read, then the update that failed.
			require.Equal(t, 2, env.fake.Requests()-before)

			// Nothing was applied.
			require.Empty(t, env.inspect(u).Individual.FullSsn)
			require.Nil(t, env.inspect(u).W9)
			require.True(t, proto.Equal(started, env.mustGet(u)))

			requireTermsOnly(t, env.mustSubmit(u, env.submission()))
		})
	}

	t.Run("applied but the response lost", func(t *testing.T) {
		u := env.newUser(true)
		env.mustStart(u)

		env.fake.FailNextUpdateAfterApply(1, 503)
		_, err := env.submit(u, env.submission())
		require.Equal(t, codes.Internal, status.Code(err))

		// Coinbase holds what was submitted.
		snap := env.inspect(u)
		require.Equal(t, testSsnDashed, snap.Individual.FullSsn)
		require.Equal(t, testFirstName, snap.Individual.FirstName)
		require.Equal(t, testEmail, snap.Individual.Email)
		require.NotNil(t, snap.Individual.Address)
		require.Equal(t, testLine1, snap.Individual.Address.Line1)
		require.NotNil(t, snap.W9)
		require.Equal(t, stubIP, snap.RequesterIPAddress)

		// Submitting the same again succeeds, and reports the state it leaves.
		requireTermsOnly(t, env.mustSubmit(u, env.submission()))
		requireTermsOnly(t, env.mustGet(u))
		require.Equal(t, testSsnDashed, env.inspect(u).Individual.FullSsn)
	})
}

// testServer_SubmitForbidden covers Coinbase refusing SubmitKyc's update with
// 403 forbidden after its read of the customer showed no block.
func testServer_SubmitForbidden(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)

	t.Run("blocked between the read and the update", func(t *testing.T) {
		u := env.newUser(true)
		env.mustStart(u)

		env.fake.BlockOnNextUpdate()
		before := env.fake.Requests()
		resp, err := env.submit(u, env.submission())
		require.NoError(t, err)
		require.Equal(t, kyccoinbasepb.SubmitKycResponse_BLOCKED, resp.Result)
		require.Nil(t, resp.KycState)
		// The read, the refused update, and the read again.
		require.Equal(t, 3, env.fake.Requests()-before)

		snap := env.inspect(u)
		require.True(t, snap.Blocked)
		require.Empty(t, snap.Individual.FullSsn, "the refused update was applied")

		entries := env.logs.FilterMessageSnippet("the customer is blocked").All()
		require.Len(t, entries, 1)
		require.Equal(t, zapcore.InfoLevel, entries[0].Level)
		require.Contains(t, entries[0].ContextMap()["error"], "http 403 forbidden")

		requireStatus(t, kyccoinbasepb.KycState_BLOCKED, env.mustGet(u))

		// A retry is answered BLOCKED off the read, unsent.
		before = env.fake.Requests()
		resp, err = env.submit(u, env.submission())
		require.NoError(t, err)
		require.Equal(t, kyccoinbasepb.SubmitKycResponse_BLOCKED, resp.Result)
		require.Equal(t, 1, env.fake.Requests()-before)
	})

	t.Run("forbidden but not blocked", func(t *testing.T) {
		u := env.newUser(true)
		started := env.mustStart(u)

		env.fake.FailNextUpdate(1, 403)
		before := env.fake.Requests()
		_, err := env.submit(u, env.submission())
		require.Equal(t, codes.Internal, status.Code(err))
		require.Equal(t, 3, env.fake.Requests()-before)

		require.False(t, env.inspect(u).Blocked)
		require.True(t, proto.Equal(started, env.mustGet(u)))
		requireTermsOnly(t, env.mustSubmit(u, env.submission()))
	})

	for _, httpStatus := range []int{503, 404} {
		t.Run(fmt.Sprintf("read again fails with %d", httpStatus), func(t *testing.T) {
			u := env.newUser(true)
			env.mustStart(u)

			// The first read passes the block by; the read again takes the
			// failure behind it.
			env.fake.BlockOnNextUpdate()
			env.fake.FailNext(1, httpStatus)
			before := env.fake.Requests()
			_, err := env.submit(u, env.submission())
			require.Equal(t, codes.Internal, status.Code(err))
			require.Equal(t, 3, env.fake.Requests()-before)

			// The block landed all the same.
			requireStatus(t, kyccoinbasepb.KycState_BLOCKED, env.mustGet(u))
			resp, err := env.submit(u, env.submission())
			require.NoError(t, err)
			require.Equal(t, kyccoinbasepb.SubmitKycResponse_BLOCKED, resp.Result)
		})
	}
}

// partlyStarted records for u a customer with only part of the product's
// capabilities requested, as a user who started before the product's set grew
// would have, and returns the customer's ID. The server never leaves a
// customer so itself: StartKyc requests the whole set at once.
func (e *serverEnv) partlyStarted(u testUser) string {
	e.t.Helper()
	bare, err := e.fake.Client().Create(e.ctx, uuid.NewString(), nil)
	require.NoError(e.t, err)
	_, _, err = e.store.PutCustomer(e.ctx, &coinbase.CustomerRecord{UserID: u.id, CustomerID: bare.ID, CreatedAt: e.now})
	require.NoError(e.t, err)
	_, err = e.fake.Client().Update(e.ctx, bare.ID, "", &customers.CustomerInput{
		Capabilities: map[customers.CapabilityName]customers.CapabilityRequest{
			customers.CapabilityCustodyCrypto: {Requested: true},
		},
		Compliance: &customers.Compliance{RequesterIPAddress: stubIP},
	})
	require.NoError(e.t, err)
	return bare.ID
}

// testServer_StartForbidden covers a user Coinbase has blocked before all of
// the product was requested: a blocked customer's requested capabilities are
// inactive, the rest stay unrequested, and Coinbase refuses every update,
// the one that would request the rest included.
func testServer_StartForbidden(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)

	t.Run("blocked with part of the product never requested", func(t *testing.T) {
		u := env.newUser(true)
		customerID := env.partlyStarted(u)
		requireStatus(t, kyccoinbasepb.KycState_NOT_STARTED, env.mustGet(u))

		// Coinbase blocks the customer, refusing an update.
		env.fake.BlockOnNextUpdate()
		_, err := env.fake.Client().Update(env.ctx, customerID, "", &customers.CustomerInput{
			Compliance: &customers.Compliance{RequesterIPAddress: stubIP},
		})
		refusal, ok := customers.AsError(err)
		require.True(t, ok, "%v", err)
		require.Equal(t, 403, refusal.StatusCode)

		// Every RPC says so off its read, and nothing is sent.
		blocked := env.mustGet(u)
		requireStatus(t, kyccoinbasepb.KycState_BLOCKED, blocked)
		require.Empty(t, blocked.Requirements)

		before := env.fake.Requests()
		started := env.mustStart(u)
		requireStatus(t, kyccoinbasepb.KycState_BLOCKED, started)
		require.Empty(t, started.Requirements)
		require.Equal(t, 1, env.fake.Requests()-before)

		before = env.fake.Requests()
		resp, err := env.submit(u, env.submission())
		require.NoError(t, err)
		require.Equal(t, kyccoinbasepb.SubmitKycResponse_BLOCKED, resp.Result)
		require.Nil(t, resp.KycState)
		require.Equal(t, 1, env.fake.Requests()-before)

		require.Empty(t, env.logs.FilterMessageSnippet("Start refused by coinbase").All())
	})

	t.Run("blocked between the read and the update", func(t *testing.T) {
		u := env.newUser(true)
		env.partlyStarted(u)

		env.fake.BlockOnNextUpdate()
		before := env.fake.Requests()
		started := env.mustStart(u)
		requireStatus(t, kyccoinbasepb.KycState_BLOCKED, started)
		require.Empty(t, started.Requirements)
		// The read, the refused update, and the read again.
		require.Equal(t, 3, env.fake.Requests()-before)
		require.True(t, env.inspect(u).Blocked)

		entries := env.logs.FilterMessageSnippet("Start refused by coinbase; the customer is blocked").All()
		require.Len(t, entries, 1)
		require.Equal(t, zapcore.InfoLevel, entries[0].Level)
		require.Contains(t, entries[0].ContextMap()["error"], "http 403 forbidden")

		requireStatus(t, kyccoinbasepb.KycState_BLOCKED, env.mustGet(u))

		// A retry is answered off the read, unsent.
		before = env.fake.Requests()
		requireStatus(t, kyccoinbasepb.KycState_BLOCKED, env.mustStart(u))
		require.Equal(t, 1, env.fake.Requests()-before)
	})

	t.Run("forbidden but not blocked", func(t *testing.T) {
		u := env.newUser(true)

		env.fake.FailNextUpdate(1, 403)
		before := env.fake.Requests()
		_, err := env.start(u)
		require.Equal(t, codes.Internal, status.Code(err))
		// The create, the refused update, and the read again.
		require.Equal(t, 3, env.fake.Requests()-before)

		require.False(t, env.inspect(u).Blocked)
		requireStatus(t, kyccoinbasepb.KycState_NOT_STARTED, env.mustGet(u))
		requireStartedState(t, env.mustStart(u))
	})

	for _, httpStatus := range []int{503, 404} {
		t.Run(fmt.Sprintf("read again fails with %d", httpStatus), func(t *testing.T) {
			u := env.newUser(true)
			env.partlyStarted(u)

			// The first read passes the block by; the read again takes the
			// failure behind it.
			env.fake.BlockOnNextUpdate()
			env.fake.FailNext(1, httpStatus)
			before := env.fake.Requests()
			_, err := env.start(u)
			require.Equal(t, codes.Internal, status.Code(err))
			require.Equal(t, 3, env.fake.Requests()-before)

			// The block landed all the same.
			requireStatus(t, kyccoinbasepb.KycState_BLOCKED, env.mustGet(u))
			requireStatus(t, kyccoinbasepb.KycState_BLOCKED, env.mustStart(u))
		})
	}
}

// testServer_NoSubmittedValueLogged drives submissions Coinbase accepts,
// refuses, fails and blocks, and those refused before sending, then requires
// that no log entry, gRPC status or response mentions any submitted value.
func testServer_NoSubmittedValueLogged(t *testing.T, accounts account.Store, store coinbase.Store) {
	env := newServerEnv(t, accounts, store)

	// Every submission carries the env's placeholders, a second address line,
	// and a date of birth whose month and day have one digit, so its padded
	// and unpadded forms differ.
	full := func() *kyccoinbasepb.Individual {
		individual := env.individual()
		individual.DateOfBirth = &kyccoinbasepb.DateOfBirth{Year: 1987, Month: 7, Day: 9}
		individual.Address.Line2 = testLine2
		return individual
	}
	const malformedEmail = "zorvald.quexley@example..com"

	// Each submitted value, in the forms a leak would take. The state and
	// postal code are searched quoted, as a proto's text form, JSON, and
	// Coinbase's message quoting its request hold them. The date of birth is
	// searched as ISO and unpadded dates and keyed by its year, as the
	// proto's text form, the proto as JSON and Coinbase's JSON hold it. The
	// terms version ID and language are not personal and a response names a
	// refused version by design, so they are not searched.
	submitted := []string{
		testSsn, testSsnDashed,
		testFirstName, testLastName,
		testLine1, testLine2, testCity,
		`"` + testState + `"`, `"` + testPostalCode + `"`,
		testEmail, malformedEmail, otherEmail,
		testPhone, decliningPhone, otherPhone,
		"1987-07-09", "1987-7-9", "year:1987", `"year":1987`, `"year":"1987"`,
		stubIP,
	}

	// answers holds every response and error the RPCs returned, as text.
	var answers []string
	answer := func(resp proto.Message, err error) {
		if err != nil {
			answers = append(answers, err.Error())
			return
		}
		answers = append(answers, prototext.Format(resp), protojson.Format(resp))
	}
	acceptEn := []*kyccoinbasepb.TermsOfServiceAcceptance{acceptance(customerstest.TosVersionID, "en")}
	w9s := []*kyccoinbasepb.TaxAttestation{w9()}

	// Accepted, then approved.
	approved := env.newUser(true)
	answer(env.start(approved))
	resp, err := env.submit(approved, &kyccoinbasepb.SubmitKycRequest{Individual: full(), TermsOfServiceAcceptances: acceptEn, TaxAttestations: w9s})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_OK, resp.Result)
	answer(resp, err)

	// Accepted, then declined on the phone number.
	declined := env.newUser(true)
	env.profiles.link(declined.id, decliningPhone)
	answer(env.start(declined))
	declining := full()
	declining.PhoneNumber = &commonpb.PhoneNumber{Value: decliningPhone}
	resp, err = env.submit(declined, &kyccoinbasepb.SubmitKycRequest{Individual: declining, TermsOfServiceAcceptances: acceptEn, TaxAttestations: w9s})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_OK, resp.Result)
	answer(resp, err)

	require.Equal(t, 2, env.fake.CompleteReviews())
	got, err := env.get(approved)
	require.NoError(t, err)
	requireStatus(t, kyccoinbasepb.KycState_VERIFIED, got.KycState)
	answer(got, err)
	got, err = env.get(declined)
	require.NoError(t, err)
	requireStatus(t, kyccoinbasepb.KycState_ACTION_REQUIRED, got.KycState)
	answer(got, err)

	// Refused before sending, on the email.
	refused := env.newUser(true)
	answer(env.start(refused))
	malformed := full()
	malformed.Email = &commonpb.EmailAddress{Value: malformedEmail}
	resp, err = env.submit(refused, &kyccoinbasepb.SubmitKycRequest{Individual: malformed, TaxAttestations: w9s})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
	require.Len(t, resp.InvalidFields, 1)
	answer(resp, err)

	// Refused before sending, on a phone number other than the one linked,
	// and failed on the profile read that judges one.
	unlinked := full()
	unlinked.PhoneNumber = &commonpb.PhoneNumber{Value: otherPhone}
	resp, err = env.submit(refused, &kyccoinbasepb.SubmitKycRequest{Individual: unlinked, TaxAttestations: w9s})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
	require.Len(t, resp.InvalidFields, 1)
	answer(resp, err)
	env.profiles.setErr(errors.New("profile store unavailable"))
	_, err = env.submit(refused, &kyccoinbasepb.SubmitKycRequest{Individual: full(), TaxAttestations: w9s})
	require.Equal(t, codes.Internal, status.Code(err))
	answer(nil, err)
	env.profiles.setErr(nil)

	// Where a verified email address is required: refused before sending on
	// an address other than the one linked, and failed on the profile read
	// that judges one. That server logs apart, and its logs are searched too.
	strict := newServerEnv(t, accounts, store, coinbase.WithVerifiedEmailRequired(true))
	emailUser := strict.newUser(true)
	strict.profiles.linkEmail(emailUser.id, otherEmail)
	answer(strict.start(emailUser))
	resp, err = strict.submit(emailUser, &kyccoinbasepb.SubmitKycRequest{Individual: full(), TaxAttestations: w9s})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
	require.Len(t, resp.InvalidFields, 1)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_InvalidField_NOT_VERIFIED, resp.InvalidFields[0].Reason)
	answer(resp, err)
	strict.profiles.setErr(errors.New("profile store unavailable"))
	_, err = strict.submit(emailUser, &kyccoinbasepb.SubmitKycRequest{Individual: &kyccoinbasepb.Individual{Email: full().Email}})
	require.Equal(t, codes.Internal, status.Code(err))
	answer(nil, err)
	strict.profiles.setErr(nil)
	require.NotEmpty(t, strict.logs.FilterMessageSnippet("Failed to get user profile").All())
	requireNotLogged(t, strict.logs, submitted...)

	// Refused by Coinbase: an acceptance it does not list, since it has no
	// address yet. Beside other things the refusal names nothing, and alone
	// it names the acceptance.
	resp, err = env.submit(refused, &kyccoinbasepb.SubmitKycRequest{
		Individual:                &kyccoinbasepb.Individual{LegalName: full().LegalName, Ssn: full().Ssn},
		TermsOfServiceAcceptances: acceptEn,
	})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
	require.Empty(t, resp.InvalidFields)
	require.Empty(t, resp.InvalidTermsOfServiceAcceptances)
	answer(resp, err)
	resp, err = env.submit(refused, &kyccoinbasepb.SubmitKycRequest{TermsOfServiceAcceptances: acceptEn})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
	require.Len(t, resp.InvalidTermsOfServiceAcceptances, 1)
	answer(resp, err)

	// Refused by Coinbase with a message quoting everything submitted.
	env.fake.RefuseNextUpdate()
	resp, err = env.submit(refused, &kyccoinbasepb.SubmitKycRequest{Individual: full(), TaxAttestations: w9s})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION, resp.Result)
	answer(resp, err)

	// An update that fails, and one refused 403 since a block landed.
	env.fake.FailNextUpdate(1, 500)
	_, err = env.submit(refused, &kyccoinbasepb.SubmitKycRequest{Individual: full(), TaxAttestations: w9s})
	require.Equal(t, codes.Internal, status.Code(err))
	answer(nil, err)
	env.fake.BlockOnNextUpdate()
	resp, err = env.submit(refused, &kyccoinbasepb.SubmitKycRequest{Individual: full(), TaxAttestations: w9s})
	require.NoError(t, err)
	require.Equal(t, kyccoinbasepb.SubmitKycResponse_BLOCKED, resp.Result)
	answer(resp, err)

	// Refused before the handler runs. A submission that breaks the proto's
	// rules is the validation interceptor's to refuse, and one whose signature
	// does not verify the authenticator's; both hold the whole request.
	invalid := full()
	invalid.Ssn = &kyccoinbasepb.Ssn{Value: testSsnDashed}
	_, err = env.submit(refused, &kyccoinbasepb.SubmitKycRequest{Individual: invalid, TaxAttestations: w9s})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	answer(nil, err)
	unverified := &kyccoinbasepb.SubmitKycRequest{Product: product, TaxAttestations: w9s}
	require.NoError(t, refused.keyPair.Auth(unverified, &unverified.Auth))
	unverified.Individual = full() // changed after signing
	_, err = env.client.SubmitKyc(env.ctx, unverified)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	answer(nil, err)

	// Each path logged, so there is something to search.
	for _, msg := range []string{
		"Failed to submit to coinbase",
		"Failed to get user profile",
		"the customer is blocked",
		"dropping invalid request",
	} {
		require.NotEmpty(t, env.logs.FilterMessageSnippet(msg).All(), msg)
	}

	requireNotLogged(t, env.logs, submitted...)
	for _, text := range answers {
		requireNotMentioned(t, "an rpc's answer", text, submitted)
	}

	// The search finds what it looks for: had the server logged a request,
	// the address, or Coinbase's message refusing a submission, the search
	// would have caught each value.
	env.fake.RefuseNextUpdate()
	_, err = env.fake.Client().Update(env.ctx, env.customerID(approved), "", &customers.CustomerInput{
		Individual: &customers.IndividualInput{
			FullSsn:     testSsnDashed,
			DateOfBirth: &customers.DateOfBirth{Year: "1987", Month: "07", Day: "09"},
		},
		Compliance: &customers.Compliance{RequesterIPAddress: stubIP},
	})
	coinbaseErr, ok := customers.AsError(err)
	require.True(t, ok, "%v", err)
	canaryCore, canaryLogs := observer.New(zapcore.DebugLevel)
	canary := zap.New(canaryCore)
	canary.Info("request", zap.Any("req", &kyccoinbasepb.SubmitKycRequest{Individual: full()}), zap.Stringer("ip", netip.MustParseAddr(stubIP)))
	canary.Info("coinbase refused", zap.String("message", coinbaseErr.Message))
	var canaryText string
	for _, entry := range canaryLogs.All() {
		canaryText += renderEntry(t, entry)
	}
	for _, v := range []string{
		testSsn, testSsnDashed,
		testFirstName, testLastName,
		testLine1, testLine2, testCity,
		`"` + testState + `"`, `"` + testPostalCode + `"`,
		testEmail, testPhone,
		"year:1987", `"year":"1987"`,
		stubIP,
	} {
		require.True(t, mentions(canaryText, v), "the search misses %q", v)
	}

	// Likewise had a request been logged as its marshalled bytes in base64,
	// as the signature verifier once logged one it could not verify.
	marshalled, err := proto.Marshal(&kyccoinbasepb.SubmitKycRequest{Individual: full()})
	require.NoError(t, err)
	encodedCore, encodedLogs := observer.New(zapcore.DebugLevel)
	zap.New(encodedCore).Info("unverified", zap.String("proto_message", base64.StdEncoding.EncodeToString(marshalled)))
	encodedText := renderEntry(t, encodedLogs.All()[0])
	for _, v := range []string{
		testSsn,
		testFirstName, testLastName,
		testLine1, testLine2, testCity,
		testEmail, testPhone,
	} {
		require.True(t, mentions(encodedText, v), "the search misses %q in a base64 field", v)
	}
}

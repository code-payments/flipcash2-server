// Package coinbase verifies a Flipcash user's identity (KYC) with Coinbase's
// Customers API, for the features Flipcash builds on Coinbase, starting with
// cashing out to the user's own bank account. It serves the
// flipcash.kyc.coinbase.v1 Kyc service.
//
// The verification is Coinbase's. The server collects what a user submits,
// checks what Coinbase cannot be trusted to refuse kindly, forwards it, and
// reads back where the user stands. Nothing a user submits is stored or
// logged: the store (Store) holds only the link between a user and their
// Coinbase customer, and Coinbase is read every time a state is needed, so no
// copy here can fall behind it. Logs carry the user ID, the customer ID, the
// product and Coinbase's error identifiers (status, type, correlation ID),
// never a request, a submitted value, an IP address or Coinbase's free-text
// error message, which may echo what was submitted. An INVALID_SUBMISSION is
// not logged at all, whether the server or Coinbase refused it.
//
// # RPCs
//
// GetKycState reads the caller's state for a product: NOT_STARTED with no
// Coinbase call when they have no customer, and otherwise the customer read
// from Coinbase. It tells Coinbase nothing and writes nothing. The state is
// never cached: each call for a user who has a customer is one read of
// Coinbase, a client's polls while IN_REVIEW included.
//
// StartKyc is what first tells Coinbase about a user. It creates a bare
// customer, carrying nothing about the user, under an idempotency key derived
// from the user ID, and records the link; a start that races another, or
// retries one whose response was lost, ends with the one customer. It then
// requests the product's capabilities with the requester's IP, which is what
// makes Coinbase list what the user has to provide. A product already started
// is answered with its state and nothing written, and so is a BLOCKED caller,
// whether or not all of the product was ever requested (a block is judged
// first; see deriveState). A caller blocked between the server's read and its
// update is answered with their BLOCKED state too: Coinbase refuses the
// update with 403 forbidden, and the customer is read again to tell, as for
// SubmitKyc below.
//
// SubmitKyc forwards any part of what verification needs (information about
// the user, acceptances of terms, tax attestations) as one update, with the
// requester's IP. It is refused with NOT_STARTED before the product is
// started and with BLOCKED, unsent, once Coinbase has blocked the user. A
// submission that sets nothing is answered with the state and not sent. What
// the server can judge before sending is judged first (see "Phone number and
// email" below for the checks that read anything), and an invalid submission is
// answered INVALID_SUBMISSION with nothing sent; a submission Coinbase itself
// refuses is answered the same way, attributed as well as Coinbase's answer
// allows. A user Coinbase blocks on the submission is
// answered BLOCKED, and so is one blocked between the server's read and its
// update: Coinbase refuses an update with 403 forbidden once a capability is
// permanently inactive, so on a 403 the customer is read again and BLOCKED
// answered when the state read is BLOCKED. Otherwise, and when that read
// fails, the RPC fails with Internal, as it does on any failure of Coinbase.
//
// Every RPC is behind a staff gate for now (staffOnly): a caller who is not
// staff is answered DENIED with nothing read from the store and nothing sent
// to Coinbase. It is transitional while the feature is built, and is removed
// by deleting its check.
//
// # Rules
//
// The server is glue over rules kept as pure functions, each file documenting
// its own:
//
//   - product.go: the Coinbase capabilities each product needs.
//   - state.go: a user's KycState for a product, derived from their customer.
//   - validate.go: the checks of a submission made before anything is sent
//     that read nothing (date of birth, address, email).
//   - submission.go: the shape of a submission (one acceptance per version,
//     one attestation per form), terms acceptances judged against the state,
//     the conversion to Coinbase's update, and reading Coinbase's refusal of
//     one.
//   - start.go: the customer's idempotency key, the update that starts a
//     product, and which requester IPs may be sent.
//
// # Phone number and email
//
// A phone number a user submits must be the one they verified with Flipcash:
// the number linked to their profile by phone verification, compared exactly
// as written (both are E.164). Any other number, and any number from a user
// with no linked number or no profile, is refused NOT_VERIFIED with the other
// checks made before sending, so only a number the user has shown they hold
// reaches Coinbase.
//
// An email address is held to the same rule only while the server is built
// to require it (WithVerifiedEmailRequired, off unless set): it must then be
// the address linked to the user's profile by email verification, compared
// exactly as written, no case folded, and any other address, and any address
// from a user with none linked or no profile, is refused NOT_VERIFIED too,
// the field telling the two apart. While it is off, an address is taken as
// given, linked or not. An address checkEmail refuses is MALFORMED either way
// and is not judged against the profile, a field being listed once, so a user
// whose linked address Coinbase would not take must link another.
//
// The profile is read (profile.Store.GetFullProfile) once for a submission,
// and only for one that sets a phone number, or an email address to judge;
// and by user ID with the values compared here, rather than looked up by the
// number or the address: a submitted value is then never a query's parameter,
// and cannot reach a database's logs or the text of an error. A failed read
// is Internal, with nothing sent.
//
// Both are judged when they are submitted and not again: Coinbase keeps what
// it was given when the user later links another number or address to their
// profile or unlinks one, and an address submitted while none was required
// is not judged when the requirement is turned on.
//
// # Requester IP
//
// Coinbase requires the end user's IP address on every write that starts or
// submits a verification, as a compliance signal, so it must be the user's
// own and not a proxy's. The server takes it from an IPResolver of OCP's
// gRPC client package (grpc/client), which the parent application passes:
// client.ResolveIPAddr reads x-forwarded-for from the right, past the number
// of trusted proxies the process is configured with (one by default; the
// gRPC app's trusted_proxies setting), since everything to their left is the
// client's to forge, and client.ForwardedForResolver is the same reading for
// a parent that must state its own number.
//
// The server refuses any address no end user connects from
// (validRequesterIP: private, loopback, link-local, multicast, unspecified,
// carrier-grade NAT, NAT64 and the other special-purpose ranges it names;
// the documentation ranges, which tests send, are accepted): such an address
// means it was read from the wrong hop, and a wrong signal must fail loudly
// rather than reach Coinbase. A resolver that fails, or an address refused,
// is a deployment fault: the RPC fails with Internal before anything about
// the request is sent, and on a first StartKyc before any customer is
// created. The resolver's error is logged, so it must not name the address
// or the header it was read from; OCP's resolvers never do.
//
// # Not built
//
//   - A webhook receiver. Coinbase's events are at-least-once hints to read a
//     customer again, so a receiver would only re-read and announce; until
//     then clients poll, and Store.GetCustomerByID, which maps an event's
//     customer back to a user, is unused.
//   - A push or event on a state change: clients poll GetKycState.
//   - Rate limiting of the RPCs.
//   - Deleting the Coinbase customer when a Flipcash account is deleted.
//   - The bank payout itself, which builds on a verified user.
//
// # Wiring
//
// The parent application creates the table (dynamodb.CreateTables), builds a
// customers.Client with a CDP API key of its own, separate from the one
// ThirdParty.GetJwt signs with (that key mints tokens the app uses directly;
// this one never leaves the server), passes the profile store phone and
// email verification link numbers and addresses in and the IPResolver that
// fits its ingress (client.ResolveIPAddr behind OCP's gRPC app), says whether
// a submitted email address must be the verified one
// (WithVerifiedEmailRequired, from its own configuration), and registers the
// service with NewServer.
//
// It must also run no interceptor over this service that logs requests: an
// interceptor sees a SubmitKycRequest, the user's SSN included, before any
// code here does, so the promise that nothing submitted is logged is the
// parent's to keep as well. OCP's validation interceptor
// (grpc/protobuf/validation), which OCP's gRPC app installs over every
// service and which the handlers rely on to have refused a request that
// breaks the proto's rules, logs the validation error alone: the field and
// the rule it broke, never a value. That holds from the ocp-server version
// go.mod requires; earlier ones logged the failed request in full.
//
// The same goes for what the handlers themselves call first. The Authorizer
// the parent passes verifies each request's signature with OCP's
// RPCSignatureVerifier (auth.NewKeyPairAuthenticator), which holds the whole
// request and, from the ocp-server version go.mod requires, logs nothing of
// one whose signature does not verify; earlier ones logged that request
// whole, base64-encoded.
package coinbase

import (
	"context"
	"errors"
	"net/http"
	"net/netip"
	"slices"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	kyccoinbasepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/coinbase/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/cdp/customers"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/profile"
	ocp_client "github.com/code-payments/ocp-server/grpc/client"
)

// ServerOption configures a Server at construction.
type ServerOption func(*Server)

// WithClock sets the clock that judges dates of birth and stamps acceptances,
// attestations and new records. For tests.
func WithClock(now func() time.Time) ServerOption {
	return func(s *Server) { s.now = now }
}

// WithVerifiedEmailRequired sets whether an email address a user submits must
// be the one they verified with Flipcash, as a phone number always must (see
// "Phone number and email" in the package comment). It is off unless set, and
// an address is then taken as given.
func WithVerifiedEmailRequired(required bool) ServerOption {
	return func(s *Server) { s.requireVerifiedEmail = required }
}

// Server serves the Kyc service. See the package comment.
type Server struct {
	log *zap.Logger

	authz    auth.Authorizer
	accounts account.Store
	profiles profile.Store

	store        Store
	cdpCustomers *customers.Client
	requesterIP  ocp_client.IPResolver

	// requireVerifiedEmail is whether a submitted email address must be the
	// one linked to the user's profile (WithVerifiedEmailRequired).
	requireVerifiedEmail bool

	now func() time.Time

	kyccoinbasepb.UnimplementedKycServer
}

// NewServer returns a Kyc server. Every argument is required: profiles is the
// store phone and email verification link a user's number and address in (see
// "Phone number and email" in the package comment), cdpCustomers is a client
// for the Customers API under the server's own CDP API key, and requesterIP
// resolves the end user's address behind each request
// (ocp_client.ResolveIPAddr behind OCP's gRPC app; see "Requester IP" in the
// package comment).
func NewServer(
	log *zap.Logger,
	authz auth.Authorizer,
	accounts account.Store,
	profiles profile.Store,
	store Store,
	cdpCustomers *customers.Client,
	requesterIP ocp_client.IPResolver,
	opts ...ServerOption,
) *Server {
	s := &Server{
		log:          log,
		authz:        authz,
		accounts:     accounts,
		profiles:     profiles,
		store:        store,
		cdpCustomers: cdpCustomers,
		requesterIP:  requesterIP,
		now:          time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// GetKycState returns the caller's state for a product.
func (s *Server) GetKycState(ctx context.Context, req *kyccoinbasepb.GetKycStateRequest) (*kyccoinbasepb.GetKycStateResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}
	log := s.requestLog(userID, req.Product)

	if allowed, err := s.staffOnly(ctx, log, userID); err != nil {
		return nil, err
	} else if !allowed {
		return &kyccoinbasepb.GetKycStateResponse{Result: kyccoinbasepb.GetKycStateResponse_DENIED}, nil
	}

	record, err := s.store.GetCustomer(ctx, userID)
	if errors.Is(err, ErrNotFound) {
		state, err := s.deriveState(log, req.Product, nil)
		if err != nil {
			return nil, err
		}
		return &kyccoinbasepb.GetKycStateResponse{KycState: state}, nil
	} else if err != nil {
		log.Warn("Failed to get coinbase customer record", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get customer record")
	}
	log = log.With(zap.String("customer_id", record.CustomerID))

	customer, err := s.cdpCustomers.Get(ctx, record.CustomerID)
	if err != nil {
		return nil, s.coinbaseFailure(log, "Failed to get coinbase customer", err)
	}
	state, err := s.deriveState(log, req.Product, customer)
	if err != nil {
		return nil, err
	}
	return &kyccoinbasepb.GetKycStateResponse{KycState: state}, nil
}

// StartKyc starts the caller's verification for a product.
func (s *Server) StartKyc(ctx context.Context, req *kyccoinbasepb.StartKycRequest) (*kyccoinbasepb.StartKycResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}
	log := s.requestLog(userID, req.Product)

	if allowed, err := s.staffOnly(ctx, log, userID); err != nil {
		return nil, err
	} else if !allowed {
		return &kyccoinbasepb.StartKycResponse{Result: kyccoinbasepb.StartKycResponse_DENIED}, nil
	}

	// Checked before anything is written, so a product that cannot be
	// started creates no customer.
	capabilities, ok := productCapabilities(req.Product)
	if !ok {
		log.Warn("Product has no capability set")
		return nil, status.Error(codes.Internal, "product not supported")
	}

	var (
		ip       netip.Addr
		haveIP   bool
		customer *customers.Customer
	)

	record, err := s.store.GetCustomer(ctx, userID)
	switch {
	case err == nil:
		log = log.With(zap.String("customer_id", record.CustomerID))
		customer, err = s.cdpCustomers.Get(ctx, record.CustomerID)
		if err != nil {
			return nil, s.coinbaseFailure(log, "Failed to get coinbase customer", err)
		}

	case errors.Is(err, ErrNotFound):
		// The address is checked before the customer is created, so a
		// request that cannot go on creates nothing.
		ip, err = s.resolveRequesterIP(ctx, log)
		if err != nil {
			return nil, err
		}
		haveIP = true

		// A bare customer: the create carries nothing about the user and no
		// IP, so every retry sends the same body under the same key.
		created, err := s.cdpCustomers.Create(ctx, customerIdempotencyKey(userID), nil)
		if err != nil {
			return nil, s.coinbaseFailure(log, "Failed to create coinbase customer", err)
		}

		stored, _, err := s.store.PutCustomer(ctx, &CustomerRecord{
			UserID:     userID,
			CustomerID: created.ID,
			CreatedAt:  s.now(),
		})
		if err != nil {
			log.Warn("Failed to put coinbase customer record", zap.String("customer_id", created.ID), zap.Error(err))
			return nil, status.Error(codes.Internal, "failed to put customer record")
		}
		record = stored
		log = log.With(zap.String("customer_id", record.CustomerID))

		customer = created
		if record.CustomerID != created.ID {
			// Possible only if Coinbase forgot the idempotency key between two
			// attempts. The recorded customer stands; the one just created is
			// referred to by nothing.
			log.Warn("Created a coinbase customer other than the one recorded; going on with the recorded one",
				zap.String("created_customer_id", created.ID))
			customer, err = s.cdpCustomers.Get(ctx, record.CustomerID)
			if err != nil {
				return nil, s.coinbaseFailure(log, "Failed to get coinbase customer", err)
			}
		}

	default:
		log.Warn("Failed to get coinbase customer record", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get customer record")
	}

	state, err := s.deriveState(log, req.Product, customer)
	if err != nil {
		return nil, err
	}
	if state.Status != kyccoinbasepb.KycState_NOT_STARTED {
		return &kyccoinbasepb.StartKycResponse{KycState: state}, nil
	}

	if !haveIP {
		ip, err = s.resolveRequesterIP(ctx, log)
		if err != nil {
			return nil, err
		}
	}
	input, err := startInput(capabilities, ip)
	if err != nil {
		log.Warn("Failed to build start update", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to start verification")
	}
	updated, err := s.cdpCustomers.Update(ctx, record.CustomerID, "", input)
	if err != nil {
		if e, ok := customers.AsError(err); ok && e.StatusCode == http.StatusForbidden {
			// Coinbase refuses an update with 403 once a capability is
			// permanently inactive, so a block may have landed since the
			// read above. The customer is read again to tell, and a caller
			// found blocked is answered with their state, as one the read
			// above found blocked is.
			after, readErr := s.stateAfterForbidden(ctx, log.With(zap.NamedError("start_error", err)), req.Product, record.CustomerID)
			if readErr != nil {
				return nil, readErr
			}
			if after.Status == kyccoinbasepb.KycState_BLOCKED {
				log.Info("Start refused by coinbase; the customer is blocked", zap.Error(err))
				return &kyccoinbasepb.StartKycResponse{KycState: after}, nil
			}
		}
		return nil, s.coinbaseFailure(log, "Failed to start coinbase verification", err)
	}

	state, err = s.deriveState(log, req.Product, updated)
	if err != nil {
		return nil, err
	}
	return &kyccoinbasepb.StartKycResponse{KycState: state}, nil
}

// SubmitKyc submits part or all of what the caller's verification needs.
func (s *Server) SubmitKyc(ctx context.Context, req *kyccoinbasepb.SubmitKycRequest) (*kyccoinbasepb.SubmitKycResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}
	log := s.requestLog(userID, req.Product)

	if allowed, err := s.staffOnly(ctx, log, userID); err != nil {
		return nil, err
	} else if !allowed {
		return &kyccoinbasepb.SubmitKycResponse{Result: kyccoinbasepb.SubmitKycResponse_DENIED}, nil
	}

	if err := validateSubmission(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	record, err := s.store.GetCustomer(ctx, userID)
	if errors.Is(err, ErrNotFound) {
		return &kyccoinbasepb.SubmitKycResponse{Result: kyccoinbasepb.SubmitKycResponse_NOT_STARTED}, nil
	} else if err != nil {
		log.Warn("Failed to get coinbase customer record", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get customer record")
	}
	log = log.With(zap.String("customer_id", record.CustomerID))

	customer, err := s.cdpCustomers.Get(ctx, record.CustomerID)
	if err != nil {
		return nil, s.coinbaseFailure(log, "Failed to get coinbase customer", err)
	}
	state, err := s.deriveState(log, req.Product, customer)
	if err != nil {
		return nil, err
	}
	switch state.Status {
	case kyccoinbasepb.KycState_NOT_STARTED:
		return &kyccoinbasepb.SubmitKycResponse{Result: kyccoinbasepb.SubmitKycResponse_NOT_STARTED}, nil
	case kyccoinbasepb.KycState_BLOCKED:
		return &kyccoinbasepb.SubmitKycResponse{Result: kyccoinbasepb.SubmitKycResponse_BLOCKED}, nil
	}

	if isEmptySubmission(req) {
		return &kyccoinbasepb.SubmitKycResponse{KycState: state}, nil
	}

	now := s.now()
	invalidFields, err := s.checkVerifiedContact(ctx, log, userID, req.Individual, checkIndividual(req.Individual, now))
	if err != nil {
		return nil, err
	}
	invalidTerms, accepted, unlisted := checkTermsAcceptances(req.TermsOfServiceAcceptances, customer, now)
	if len(invalidFields) > 0 || len(invalidTerms) > 0 {
		return &kyccoinbasepb.SubmitKycResponse{
			Result:                           kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION,
			InvalidFields:                    invalidFields,
			InvalidTermsOfServiceAcceptances: invalidTerms,
		}, nil
	}

	ip, err := s.resolveRequesterIP(ctx, log)
	if err != nil {
		return nil, err
	}
	input, err := toCustomerInput(req, accepted, now, ip)
	if err != nil {
		log.Warn("Failed to build submission update", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to build submission")
	}

	updated, err := s.cdpCustomers.Update(ctx, record.CustomerID, "", input)
	if err != nil {
		if refusedTerms, ok := submissionRefusal(err, accepted, unlisted, submitsAcceptancesAlone(req)); ok {
			return &kyccoinbasepb.SubmitKycResponse{
				Result:                           kyccoinbasepb.SubmitKycResponse_INVALID_SUBMISSION,
				InvalidTermsOfServiceAcceptances: refusedTerms,
			}, nil
		}
		if e, ok := customers.AsError(err); ok && e.StatusCode == http.StatusForbidden {
			// Coinbase refuses an update with 403 once a capability is
			// permanently inactive, so a block may have landed since the
			// read above. The customer is read again to tell.
			after, readErr := s.stateAfterForbidden(ctx, log.With(zap.NamedError("submit_error", err)), req.Product, record.CustomerID)
			if readErr != nil {
				return nil, readErr
			}
			if after.Status == kyccoinbasepb.KycState_BLOCKED {
				log.Info("Submission refused by coinbase; the customer is blocked", zap.Error(err))
				return &kyccoinbasepb.SubmitKycResponse{Result: kyccoinbasepb.SubmitKycResponse_BLOCKED}, nil
			}
		}
		return nil, s.coinbaseFailure(log, "Failed to submit to coinbase", err)
	}

	state, err = s.deriveState(log, req.Product, updated)
	if err != nil {
		return nil, err
	}
	if state.Status == kyccoinbasepb.KycState_BLOCKED {
		return &kyccoinbasepb.SubmitKycResponse{Result: kyccoinbasepb.SubmitKycResponse_BLOCKED}, nil
	}
	return &kyccoinbasepb.SubmitKycResponse{KycState: state}, nil
}

// checkVerifiedContact judges the contact details a submission sets against
// what the user has verified with Flipcash (see "Phone number and email" in
// the package comment): the phone number always, and the email address while
// a verified one is required and checkIndividual has not already refused it.
// It returns invalid, checkIndividual's list, with each one refused added,
// which keeps the list in enum order: checkIndividual judges no field past
// the email, and the email comes before the phone number.
//
// The profile is read once, and only when there is something to judge. A user
// the profile store does not know has verified nothing. A failed read is
// Internal, logged without anything submitted (the profile is read by user
// ID).
func (s *Server) checkVerifiedContact(
	ctx context.Context,
	log *zap.Logger,
	userID *commonpb.UserId,
	individual *kyccoinbasepb.Individual,
	invalid []*kyccoinbasepb.SubmitKycResponse_InvalidField,
) ([]*kyccoinbasepb.SubmitKycResponse_InvalidField, error) {
	phoneNumber := individual.GetPhoneNumber()
	email := individual.GetEmail()
	judgeEmail := s.requireVerifiedEmail && email != nil && !slices.ContainsFunc(invalid, func(f *kyccoinbasepb.SubmitKycResponse_InvalidField) bool {
		return f.Field == kyccoinbasepb.Field_FIELD_EMAIL
	})
	if phoneNumber == nil && !judgeEmail {
		return invalid, nil
	}

	userProfile, err := s.profiles.GetFullProfile(ctx, userID, true)
	if errors.Is(err, profile.ErrNotFound) {
		userProfile = nil
	} else if err != nil {
		log.Warn("Failed to get user profile", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get user profile")
	}

	if judgeEmail && !isLinked(userProfile.GetEmailAddress().GetValue(), email.GetValue()) {
		invalid = append(invalid, &kyccoinbasepb.SubmitKycResponse_InvalidField{
			Field:  kyccoinbasepb.Field_FIELD_EMAIL,
			Reason: kyccoinbasepb.SubmitKycResponse_InvalidField_NOT_VERIFIED,
		})
	}
	if phoneNumber != nil && !isLinked(userProfile.GetPhoneNumber().GetValue(), phoneNumber.GetValue()) {
		invalid = append(invalid, &kyccoinbasepb.SubmitKycResponse_InvalidField{
			Field:  kyccoinbasepb.Field_FIELD_PHONE_NUMBER,
			Reason: kyccoinbasepb.SubmitKycResponse_InvalidField_NOT_VERIFIED,
		})
	}
	return invalid, nil
}

// isLinked reports whether submitted is the value linked to a profile,
// compared as written. Nothing linked matches nothing.
func isLinked(linked, submitted string) bool {
	return linked != "" && linked == submitted
}

// stateAfterForbidden reads a customer again after Coinbase refused an update
// to it with 403 forbidden, and returns the state derived from the read, for
// the caller to tell a block (BLOCKED) from any other refusal. log carries
// the refusal (its error string, which is safe to log; see customers.Error).
// A failed read and a failed derivation are both Internal.
func (s *Server) stateAfterForbidden(ctx context.Context, log *zap.Logger, product kyccoinbasepb.Product, customerID string) (*kyccoinbasepb.KycState, error) {
	customer, err := s.cdpCustomers.Get(ctx, customerID)
	if err != nil {
		return nil, s.coinbaseFailure(log, "Failed to get coinbase customer after a forbidden update", err)
	}
	return s.deriveState(log, product, customer)
}

// requestLog is the logger for one request: the user and the product, and
// nothing from the request beyond them.
func (s *Server) requestLog(userID *commonpb.UserId, product kyccoinbasepb.Product) *zap.Logger {
	return s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("product", product.String()),
	)
}

// staffOnly is the transitional staff gate every RPC passes before it reads
// the store or calls Coinbase: false for a caller who is not staff, who is
// answered DENIED. Removing the gate is deleting its check in each RPC.
func (s *Server) staffOnly(ctx context.Context, log *zap.Logger, userID *commonpb.UserId) (bool, error) {
	isStaff, err := s.accounts.IsStaff(ctx, userID)
	if err != nil {
		log.Warn("Failed to get staff flag", zap.Error(err))
		return false, status.Error(codes.Internal, "failed to get staff flag")
	}
	return isStaff, nil
}

// deriveState derives the state and logs the notes deriveState makes, which
// name only Coinbase's own identifiers. Its error is Internal.
func (s *Server) deriveState(log *zap.Logger, product kyccoinbasepb.Product, customer *customers.Customer) (*kyccoinbasepb.KycState, error) {
	state, notes, err := deriveState(product, customer)
	if err != nil {
		log.Warn("Failed to derive kyc state", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to derive state")
	}
	if len(notes) > 0 {
		log.Warn("Coinbase customer holds what this server does not model", zap.Strings("notes", notes))
	}
	return state, nil
}

// resolveRequesterIP resolves the end user's address and checks it may be
// sent to Coinbase. Either failing is a deployment fault, Internal, logged
// without the address.
func (s *Server) resolveRequesterIP(ctx context.Context, log *zap.Logger) (netip.Addr, error) {
	ip, err := s.requesterIP(ctx)
	if err != nil {
		log.Warn("Failed to resolve requester ip address; check the resolver against the ingress", zap.Error(err))
		return netip.Addr{}, status.Error(codes.Internal, "failed to resolve requester ip address")
	}
	if !validRequesterIP(ip) {
		log.Warn("Requester ip address is not one an end user connects from; check the resolver against the ingress")
		return netip.Addr{}, status.Error(codes.Internal, "failed to resolve requester ip address")
	}
	return ip, nil
}

// coinbaseFailure logs a failed Coinbase call and returns its gRPC error,
// Internal whatever the failure: one that may pass if retried (a timeout, a
// 5xx, a rate limit) is not told apart from one that will not. The error's
// string is safe to log (see customers.Error).
func (s *Server) coinbaseFailure(log *zap.Logger, msg string, err error) error {
	log.Warn(msg, zap.Error(err))
	return status.Error(codes.Internal, "coinbase request failed")
}

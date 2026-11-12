// Package customerstest is an in-process fake of Coinbase's Customers API
// for tests: an httptest server speaking the same REST API as Coinbase, so
// tests drive the real customers.Client against it rather than a mock.
//
// The fake reproduces the behaviour the server depends on, as documented by
// Coinbase and observed in the CDP Sandbox on 2026-11-11 (see customer.go
// for the state model). It is deterministic: customer and correlation IDs
// come from counters, time from an injectable clock, and verification never
// completes on its own. Identity submitted for review stays pending until the
// test calls CompleteReviews (or CompleteReview), so a test can observe "in
// review" before "verified". Outcomes follow Coinbase's Sandbox magic values
// (SsnApprove, SsnDecline, SsnBlock and the rest), except that data with no
// magic value is approved, where real Coinbase would verify it.
//
// Every request must carry a valid CDP JWT signed by the fake's own key
// (KeyID, PrivateKey) whose uri claim names the request, so the client's
// signing is exercised too. FailNext, FailNextAfterApply, FailNextUpdate,
// FailNextUpdateAfterApply, BlockOnNextUpdate and RefuseNextUpdate inject
// failures and refusals, through one queue.
package customerstest

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/code-payments/flipcash2-server/cdp/customers"
)

const (
	basePath      = "/platform"
	customersPath = basePath + "/v2/customers"

	// KeyID is the fake's CDP API key ID.
	KeyID = "organizations/fake-org/apiKeys/fake-key"
)

// Server is a fake Customers API. All methods are safe for concurrent use,
// including alongside requests in flight.
type Server struct {
	httpServer *httptest.Server
	publicKey  ed25519.PublicKey
	privateKey ed25519.PrivateKey
	now        func() time.Time

	mu              sync.Mutex
	customers       map[string]*customer
	deleted         map[string]bool
	nextCustomer    int
	nextCorrelation int
	idempotency     map[string]idempotentResponse
	failures        []failure
	requests        int
}

type idempotentResponse struct {
	bodyHash [32]byte
	status   int
	body     []byte
}

// failureKind is what a queued injection does to the request it applies to.
type failureKind int

const (
	// failStatus answers statusCode as a proxy or an overloaded Coinbase
	// would (writeInjectedFailure), before or after the request is applied.
	failStatus failureKind = iota
	// failBlock blocks the update's customer and refuses the update 403
	// forbidden.
	failBlock
	// failRefuse refuses the update 400 invalid_request, quoting its body.
	failRefuse
)

type failure struct {
	kind       failureKind
	statusCode int
	afterApply bool
	// updateOnly makes the injection apply to an update (a PUT of a
	// customer) alone.
	updateOnly bool
}

// appliesTo reports whether f applies to r.
func (f failure) appliesTo(r *http.Request) bool {
	return !f.updateOnly || (r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, customersPath+"/"))
}

// Option configures a Server.
type Option func(*Server)

// WithClock sets the fake's clock, which stamps customers, judges whether
// an acceptance lies in the future and computes ages. JWT lifetimes are
// always judged against the real clock, since the client signs with it.
func WithClock(now func() time.Time) Option {
	return func(s *Server) { s.now = now }
}

// NewServer starts a fake. Close it when done.
func NewServer(opts ...Option) *Server {
	// A fixed seed keeps the fake deterministic; the key secures nothing.
	seed := sha256.Sum256([]byte("customerstest fake cdp api key"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])

	s := &Server{
		publicKey:   privateKey.Public().(ed25519.PublicKey),
		privateKey:  privateKey,
		now:         time.Now,
		customers:   make(map[string]*customer),
		deleted:     make(map[string]bool),
		idempotency: make(map[string]idempotentResponse),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.httpServer = httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	return s
}

// Close shuts the server down.
func (s *Server) Close() {
	s.httpServer.Close()
}

// BaseURL is the base URL to give customers.NewClient, the equivalent of
// customers.SandboxBaseURL.
func (s *Server) BaseURL() string {
	return s.httpServer.URL + basePath
}

// KeyID is the API key ID the fake accepts.
func (s *Server) KeyID() string {
	return KeyID
}

// PrivateKey is the API key the fake accepts.
func (s *Server) PrivateKey() ed25519.PrivateKey {
	return s.privateKey
}

// HTTPClient is an http.Client for the fake.
func (s *Server) HTTPClient() *http.Client {
	return s.httpServer.Client()
}

// Client returns a customers.Client for the fake.
func (s *Server) Client() *customers.Client {
	c, err := customers.NewClient(s.HTTPClient(), s.BaseURL(), KeyID, s.privateKey)
	if err != nil {
		panic(err)
	}
	return c
}

// The injections below share one queue and apply in the order they were
// set: each request takes the injection at the head of the queue when that
// injection applies to it, and otherwise passes untouched, leaving the
// injection at the head. FailNext and FailNextAfterApply apply to every
// request; the others to an update (a PUT of a customer) alone, so a read
// passes them by. No injection is recorded under an idempotency key (see
// idempotent).

// FailNext makes the next n requests fail with statusCode before they are
// applied, as a proxy or an overloaded Coinbase would, with a Coinbase error
// body of the matching type (500 internal_server_error, 502 bad_gateway,
// 503 service_unavailable, 429 rate_limit_exceeded, otherwise none).
// Failures queue behind any already set.
func (s *Server) FailNext(n int, statusCode int) {
	s.inject(n, failure{statusCode: statusCode})
}

// FailNextAfterApply makes the next n requests take effect and then answer
// statusCode as FailNext would: a write that landed but whose response was
// lost. A create retried under its idempotency key then returns the customer
// the lost one made.
func (s *Server) FailNextAfterApply(n int, statusCode int) {
	s.inject(n, failure{statusCode: statusCode, afterApply: true})
}

// FailNextUpdate is FailNext for updates alone: the next n updates fail with
// statusCode before they are applied, and reads, creates and deletes pass
// them by.
func (s *Server) FailNextUpdate(n int, statusCode int) {
	s.inject(n, failure{statusCode: statusCode, updateOnly: true})
}

// FailNextUpdateAfterApply is FailNextAfterApply for updates alone: the next
// n updates take effect and then answer statusCode as FailNext would.
func (s *Server) FailNextUpdateAfterApply(n int, statusCode int) {
	s.inject(n, failure{statusCode: statusCode, afterApply: true, updateOnly: true})
}

// BlockOnNextUpdate makes the next update be refused 403 forbidden, applying
// nothing it carries, and blocks its customer from then on as a block magic
// value would. It models a block that lands between a caller's read of a
// customer and its update: Coinbase answers an update with 403 forbidden when
// one or more capabilities are permanently inactive.
func (s *Server) BlockOnNextUpdate() {
	s.inject(1, failure{kind: failBlock, updateOnly: true})
}

// RefuseNextUpdate makes the next update be refused 400 invalid_request,
// applying nothing, with an errorMessage that quotes everything the update
// submitted. It stands for a refusal the fake does not otherwise model, whose
// free-text message quotes what was submitted, as Coinbase's can, for tests
// that such a message goes nowhere.
func (s *Server) RefuseNextUpdate() {
	s.inject(1, failure{kind: failRefuse, updateOnly: true})
}

func (s *Server) inject(n int, f failure) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < n; i++ {
		s.failures = append(s.failures, f)
	}
}

// Requests is the number of requests the fake has received, failed ones
// included.
func (s *Server) Requests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests
}

// CustomerIDs lists the customers that exist (not deleted), sorted.
func (s *Server) CustomerIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.customers))
	for id := range s.customers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// CompleteReviews completes every review that can complete (see
// CompleteReview) and returns how many did.
func (s *Server) CompleteReviews() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.customers {
		if c.completeReview() {
			c.updatedAt = s.now()
			n++
		}
	}
	return n
}

// CompleteReview completes one customer's identity review, as Coinbase does
// asynchronously a moment after the identity is submitted, and reports
// whether there was one to complete. A review completes only once every
// identity field is submitted and some are pending: they are then all
// rejected if a decline magic value is among them, and otherwise cleared.
func (s *Server) CompleteReview(customerID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.customers[customerID]
	if !ok || !c.completeReview() {
		return false
	}
	c.updatedAt = s.now()
	return true
}

// Snapshot is what the fake holds for a customer, including what Coinbase
// never returns on a read, for tests to assert what was submitted.
type Snapshot struct {
	Individual customers.IndividualInput
	// TosAccepted is every acceptance recorded, by version ID.
	TosAccepted map[string]customers.TosAcceptance
	// W9 is the W-9 on file, nil when none is or it was voided.
	W9 *customers.UsW9Attestation
	// RequesterIPAddress is the last one a write carried.
	RequesterIPAddress string
	// Blocked is a terminal block (block magic value or age).
	Blocked bool
}

// Inspect returns what the fake holds for a customer.
func (s *Server) Inspect(customerID string) (Snapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.customers[customerID]
	if !ok {
		return Snapshot{}, false
	}
	return c.snapshot(), true
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.requests++

	var fail *failure
	if len(s.failures) > 0 && s.failures[0].appliesTo(r) {
		f := s.failures[0]
		s.failures = s.failures[1:]
		fail = &f
	}
	if fail != nil && fail.kind == failStatus && !fail.afterApply {
		s.writeInjectedFailure(w, fail.statusCode)
		return
	}

	if !s.authorized(r) {
		s.writeError(w, http.StatusUnauthorized, "unauthorized", "Unauthorized.")
		return
	}

	if fail != nil && fail.kind != failStatus {
		s.refuseUpdate(r, body, fail.kind).write(w)
		return
	}

	resp := s.route(r, body)

	if fail != nil {
		s.writeInjectedFailure(w, fail.statusCode)
		return
	}
	resp.write(w)
}

// response is a response held until it is written, so that one can be
// recorded for idempotent replay or dropped for an injected failure.
type response struct {
	status int
	body   []byte
}

func (r response) write(w http.ResponseWriter) {
	if r.body != nil {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(r.status)
	if r.body != nil {
		_, _ = w.Write(r.body)
	}
}

func (s *Server) route(r *http.Request, body []byte) response {
	path := r.URL.Path
	switch {
	case path == customersPath:
		if r.Method != http.MethodPost {
			return s.errorResponse(http.StatusMethodNotAllowed, "invalid_request", "Method not allowed.")
		}
		return s.idempotent(r, body, func() response { return s.create(body) })
	case strings.HasPrefix(path, customersPath+"/"):
		id := strings.TrimPrefix(path, customersPath+"/")
		if !customerIDPattern.MatchString(id) {
			return s.errorResponse(http.StatusBadRequest, "invalid_request", "Invalid request. Please check the request body and parameters.")
		}
		switch r.Method {
		case http.MethodGet:
			return s.get(id)
		case http.MethodPut:
			return s.idempotent(r, body, func() response { return s.update(id, body) })
		case http.MethodDelete:
			return s.idempotent(r, body, func() response { return s.delete(id) })
		}
		return s.errorResponse(http.StatusMethodNotAllowed, "invalid_request", "Method not allowed.")
	}
	return response{status: http.StatusNotFound, body: []byte("404 page not found\n")}
}

// refuseUpdate answers an update that a queued block or refusal applies to,
// applying nothing the update carries: for a block, the customer is blocked
// and the update refused 403 forbidden; for a refusal, the update is refused
// 400 invalid_request with its body quoted. An unknown customer is 404
// not_found either way.
func (s *Server) refuseUpdate(r *http.Request, body []byte, kind failureKind) response {
	c, ok := s.customers[strings.TrimPrefix(r.URL.Path, customersPath+"/")]
	if !ok {
		return s.notFound()
	}
	if kind == failBlock {
		c.blocked = true
		c.updatedAt = s.now()
		return s.errorResponse(http.StatusForbidden, "forbidden", "This customer cannot be modified at this time.")
	}
	var compact bytes.Buffer
	if json.Compact(&compact, body) != nil {
		compact.Reset()
		compact.Write(body)
	}
	return s.errorResponse(http.StatusBadRequest, "invalid_request", "Invalid customer data provided: "+compact.String())
}

// idempotent replays the response recorded for a request sent again under
// the same X-Idempotency-Key with the same body, and refuses the key reused
// with a different body (422 idempotency_error), as Coinbase documents ("the
// exact same response as the first request") and as the Sandbox does. Every
// response the handler produces is recorded, refusals included, so a request
// refused for its content and then corrected must be sent under a new key.
// Keys are scoped to the method and path.
//
// Injected failures are not recorded: one before the request is applied
// never reaches the handler and leaves the key unused, and one after it
// stands in front of the response the handler produced, which is recorded
// and which a retry gets. A queued block or refusal (BlockOnNextUpdate,
// RefuseNextUpdate) never reaches the handler either.
func (s *Server) idempotent(r *http.Request, body []byte, handle func() response) response {
	key := r.Header.Get("X-Idempotency-Key")
	if key == "" {
		return handle()
	}
	var compact bytes.Buffer
	if len(body) > 0 && json.Compact(&compact, body) != nil {
		compact.Reset()
		compact.Write(body)
	}
	hash := sha256.Sum256(compact.Bytes())

	scope := r.Method + " " + r.URL.Path + " " + key
	if prior, ok := s.idempotency[scope]; ok {
		if prior.bodyHash != hash {
			return s.errorResponse(http.StatusUnprocessableEntity, "idempotency_error",
				fmt.Sprintf("Idempotency key '%s' was already used with a different request payload. Please try again with a new idempotency key.", key))
		}
		return response{status: prior.status, body: prior.body}
	}
	resp := handle()
	s.idempotency[scope] = idempotentResponse{bodyHash: hash, status: resp.status, body: resp.body}
	return resp
}

func (s *Server) create(body []byte) response {
	req, errResp := s.decodeRequest(body)
	if errResp != nil {
		return *errResp
	}
	if req.Type == nil || *req.Type != "individual" {
		return s.errorResponse(http.StatusBadRequest, "invalid_request", `type: value is not one of the allowed values ["individual"]`)
	}

	s.nextCustomer++
	now := s.now()
	c := newCustomer(fmt.Sprintf("customer_00000000-0000-4000-8000-%012x", s.nextCustomer), now)
	if errResp := s.apply(c, req, now); errResp != nil {
		s.nextCustomer--
		return *errResp
	}
	s.customers[c.id] = c
	return s.customerResponse(http.StatusCreated, c)
}

func (s *Server) get(id string) response {
	c, ok := s.customers[id]
	if !ok {
		return s.notFound()
	}
	return s.customerResponse(http.StatusOK, c)
}

func (s *Server) update(id string, body []byte) response {
	c, ok := s.customers[id]
	if !ok {
		return s.notFound()
	}
	req, errResp := s.decodeRequest(body)
	if errResp != nil {
		return *errResp
	}
	if req.Type != nil && *req.Type != "individual" {
		return s.errorResponse(http.StatusBadRequest, "invalid_request", `type: value is not one of the allowed values ["individual"]`)
	}
	next := c.clone()
	if errResp := s.apply(next, req, s.now()); errResp != nil {
		return *errResp
	}
	s.customers[id] = next
	return s.customerResponse(http.StatusOK, next)
}

func (s *Server) delete(id string) response {
	if _, ok := s.customers[id]; ok {
		delete(s.customers, id)
		s.deleted[id] = true
		return response{status: http.StatusNoContent}
	}
	if s.deleted[id] {
		return response{status: http.StatusNoContent}
	}
	return s.notFound()
}

func (s *Server) notFound() response {
	return s.errorResponse(http.StatusNotFound, "not_found", "Customer not found.")
}

func (s *Server) customerResponse(status int, c *customer) response {
	body, err := json.Marshal(c.toWire())
	if err != nil {
		panic(err)
	}
	return response{status: status, body: body}
}

func (s *Server) errorResponse(status int, errorType, message string) response {
	s.nextCorrelation++
	body, _ := json.Marshal(map[string]string{
		"errorType":     errorType,
		"errorMessage":  message,
		"correlationId": fmt.Sprintf("fake%012x-IAD", s.nextCorrelation),
		"errorLink":     "https://docs.cdp.coinbase.com/api-reference/v2/errors#" + strings.ReplaceAll(errorType, "_", "-"),
	})
	return response{status: status, body: body}
}

func (s *Server) writeError(w http.ResponseWriter, status int, errorType, message string) {
	s.errorResponse(status, errorType, message).write(w)
}

func (s *Server) writeInjectedFailure(w http.ResponseWriter, statusCode int) {
	switch statusCode {
	case http.StatusInternalServerError:
		s.writeError(w, statusCode, "internal_server_error", "An internal server error occurred. Please try again later.")
	case http.StatusBadGateway:
		s.writeError(w, statusCode, "bad_gateway", "Bad gateway.")
	case http.StatusServiceUnavailable:
		s.writeError(w, statusCode, "service_unavailable", "Service unavailable. Please try again later.")
	case http.StatusTooManyRequests:
		s.writeError(w, statusCode, "rate_limit_exceeded", "Rate limit exceeded.")
	default:
		w.WriteHeader(statusCode)
	}
}

// authorized verifies the request's bearer JWT the way CDP does: EdDSA
// signed by the fake's key, kid and sub naming it, issued by "cdp", inside
// its validity window (at most two minutes), and bound to this request by
// its uri claim, "METHOD host/path" with no query string.
func (s *Server) authorized(r *http.Request) bool {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !ed25519.Verify(s.publicKey, []byte(parts[0]+"."+parts[1]), signature) {
		return false
	}
	var header struct {
		Typ, Alg, Kid, Nonce string
	}
	var claims struct {
		Iss, Sub, URI string
		Nbf, Exp      int64
	}
	if !decodeSegment(parts[0], &header) || !decodeSegment(parts[1], &claims) {
		return false
	}
	now := time.Now().Unix()
	return header.Typ == "JWT" && header.Alg == "EdDSA" && header.Kid == KeyID && header.Nonce != "" &&
		claims.Iss == "cdp" && claims.Sub == KeyID &&
		claims.Nbf <= now+5 && now < claims.Exp && claims.Exp-claims.Nbf <= 120 &&
		claims.URI == r.Method+" "+r.Host+r.URL.Path
}

func decodeSegment(segment string, out any) bool {
	data, err := base64.RawURLEncoding.DecodeString(segment)
	return err == nil && json.Unmarshal(data, out) == nil
}

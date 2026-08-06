package bridge

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// Webhook event categories this integration consumes.
const (
	EventCategoryDrain    = "liquidation_address.drain"
	EventCategoryCustomer = "customer"

	// EventCategoryExternalAccount uses Bridge's documented spelling (three
	// c's). EventCategoryExternalAccountAlt is registered alongside it in
	// case the payloads use the conventional spelling.
	EventCategoryExternalAccount    = "external_acccount"
	EventCategoryExternalAccountAlt = "external_account"
)

// Event type suffixes, formatted as "<category>.<mutation>".
const (
	EventTypeCreated          = "created"
	EventTypeUpdated          = "updated"
	EventTypeStatusTransition = "updated.status_transitioned"
	EventTypeDeleted          = "deleted"
)

const (
	webhookSignatureHeader = "X-Webhook-Signature"

	// defaultMaxEventAge follows Bridge's guidance to disregard events older
	// than ~10 minutes; retries are re-signed with fresh timestamps.
	defaultMaxEventAge = 10 * time.Minute

	// maxClockSkew tolerates slightly future-dated timestamps.
	maxClockSkew = time.Minute

	// maxWebhookBodyBytes bounds request bodies; events carry full resource
	// objects but nothing near this size.
	maxWebhookBodyBytes = 1 << 20 // 1 MiB
)

// Event is Bridge's webhook event envelope. EventObject mirrors the
// corresponding API resource; consumers should treat events as hints and
// re-fetch authoritative state before applying changes.
//
// EventObject and EventObjectChanges can carry PII (e.g. customer events
// include submitted identity data) and must never be logged or persisted.
type Event struct {
	APIVersion         string          `json:"api_version"`
	EventID            string          `json:"event_id"`
	EventSequence      int64           `json:"event_sequence,omitempty"`
	EventDeveloperID   string          `json:"event_developer_id"`
	EventCategory      string          `json:"event_category"`
	EventType          string          `json:"event_type"`
	EventObjectID      string          `json:"event_object_id"`
	EventObjectStatus  string          `json:"event_object_status,omitempty"`
	EventObject        json.RawMessage `json:"event_object"`
	EventObjectChanges json.RawMessage `json:"event_object_changes,omitempty"`
	EventCreatedAt     string          `json:"event_created_at"`
}

// EventHandler processes a verified webhook event. Returning an error yields
// a 500 response, prompting Bridge to retry the delivery later. Handlers must
// not log or persist the event's object payloads, which can carry PII; log
// only identifiers (event ID, category, type, object ID).
type EventHandler func(ctx context.Context, event *Event) error

// WebhookHandler verifies and dispatches Bridge webhook deliveries. Events
// with valid signatures are routed to the handler registered for their
// category; verified events with no registered handler are acknowledged and
// dropped so new Bridge event categories can never break the receiver.
type WebhookHandler struct {
	publicKey   *rsa.PublicKey
	maxEventAge time.Duration
	handlers    map[string]EventHandler
	now         func() time.Time
	log         *zap.Logger
}

type WebhookOption func(*WebhookHandler)

// WithMaxEventAge overrides the replay-protection window.
func WithMaxEventAge(age time.Duration) WebhookOption {
	return func(h *WebhookHandler) {
		h.maxEventAge = age
	}
}

// WithWebhookLogger sets the logger for dropped and failed events.
func WithWebhookLogger(log *zap.Logger) WebhookOption {
	return func(h *WebhookHandler) {
		h.log = log
	}
}

// withNow overrides the clock for tests.
func withNow(now func() time.Time) WebhookOption {
	return func(h *WebhookHandler) {
		h.now = now
	}
}

// NewWebhookHandler creates a webhook receiver verifying deliveries against
// the endpoint's RSA public key (PEM, as returned when enabling the webhook
// endpoint). All consumed categories start with no-op handlers; replace them
// with RegisterHandler.
func NewWebhookHandler(publicKeyPEM []byte, opts ...WebhookOption) (*WebhookHandler, error) {
	block, _ := pem.Decode(publicKeyPEM)
	if block == nil {
		return nil, fmt.Errorf("bridge: webhook public key is not valid PEM")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("bridge: parsing webhook public key: %w", err)
	}
	rsaKey, ok := parsed.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("bridge: webhook public key is %T, expected RSA", parsed)
	}

	noop := func(context.Context, *Event) error { return nil }
	h := &WebhookHandler{
		publicKey:   rsaKey,
		maxEventAge: defaultMaxEventAge,
		handlers: map[string]EventHandler{
			EventCategoryDrain:              noop,
			EventCategoryCustomer:           noop,
			EventCategoryExternalAccount:    noop,
			EventCategoryExternalAccountAlt: noop,
		},
		now: time.Now,
		log: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h, nil
}

// RegisterHandler sets the handler for an event category, replacing the no-op
// default. Not safe to call after the handler starts serving.
func (h *WebhookHandler) RegisterHandler(category string, handler EventHandler) {
	h.handlers[category] = handler
}

func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBodyBytes))
	if err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	if err := h.verifySignature(r.Header.Get(webhookSignatureHeader), body); err != nil {
		http.Error(w, "invalid signature", http.StatusBadRequest)
		return
	}

	var event Event
	if err := json.Unmarshal(body, &event); err != nil {
		http.Error(w, "invalid event", http.StatusBadRequest)
		return
	}

	handler, ok := h.handlers[event.EventCategory]
	if !ok {
		// Verified but unconsumed category: acknowledge so Bridge does not
		// retry deliveries we will never process.
		h.log.Debug("dropping webhook event for unconsumed category",
			zap.String("event_id", event.EventID),
			zap.String("event_category", event.EventCategory))
		w.WriteHeader(http.StatusOK)
		return
	}

	if err := handler(r.Context(), &event); err != nil {
		h.log.Warn("webhook event handler failed",
			zap.String("event_id", event.EventID),
			zap.String("event_category", event.EventCategory),
			zap.String("event_type", event.EventType),
			zap.Error(err))
		http.Error(w, "handler error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// verifySignature checks the X-Webhook-Signature header ("t=<ms>,v0=<base64>")
// against the request body: RSA PKCS1v15 over SHA-256 of "timestamp.body",
// with replay protection on the timestamp.
func (h *WebhookHandler) verifySignature(header string, body []byte) error {
	if header == "" {
		return fmt.Errorf("bridge: missing %s header", webhookSignatureHeader)
	}

	var timestampMs int64
	var signatures [][]byte
	for _, part := range strings.Split(header, ",") {
		key, value, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found {
			continue
		}
		switch key {
		case "t":
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return fmt.Errorf("bridge: invalid signature timestamp")
			}
			timestampMs = parsed
		case "v0":
			decoded, err := base64.StdEncoding.DecodeString(value)
			if err != nil {
				return fmt.Errorf("bridge: invalid signature encoding")
			}
			signatures = append(signatures, decoded)
		}
	}
	if timestampMs == 0 || len(signatures) == 0 {
		return fmt.Errorf("bridge: malformed %s header", webhookSignatureHeader)
	}

	age := h.now().Sub(time.UnixMilli(timestampMs))
	if age > h.maxEventAge || age < -maxClockSkew {
		return fmt.Errorf("bridge: signature timestamp outside allowed window")
	}

	// Bridge signs the SHA-256 digest of "timestamp.body" as if it were the
	// message, so the signature effectively covers the double hash: their
	// documented Go/TS/Python/Java verifiers all hash the payload and then
	// verify the digest with a hashing verifier. Their Ruby sample instead
	// verifies over the single hash, so both forms are accepted.
	digest := sha256.Sum256(append([]byte(strconv.FormatInt(timestampMs, 10)+"."), body...))
	doubleDigest := sha256.Sum256(digest[:])

	for _, signature := range signatures {
		if rsa.VerifyPKCS1v15(h.publicKey, crypto.SHA256, digest[:], signature) == nil {
			return nil
		}
		if rsa.VerifyPKCS1v15(h.publicKey, crypto.SHA256, doubleDigest[:], signature) == nil {
			return nil
		}
	}
	return fmt.Errorf("bridge: signature verification failed")
}

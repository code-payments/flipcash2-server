package bridge

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type webhookTestEnv struct {
	handler *WebhookHandler
	key     *rsa.PrivateKey
	now     time.Time
}

func newWebhookTestEnv(t *testing.T, opts ...WebhookOption) *webhookTestEnv {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pkix, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	publicKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pkix})

	now := time.Now()
	opts = append(opts, withNow(func() time.Time { return now }))
	handler, err := NewWebhookHandler(publicKeyPEM, opts...)
	require.NoError(t, err)

	return &webhookTestEnv{handler: handler, key: key, now: now}
}

// sign produces an X-Webhook-Signature header over body. doubleHash selects
// which of the two tolerated digest interpretations to sign.
func (e *webhookTestEnv) sign(t *testing.T, body []byte, timestampMs int64, doubleHash bool) string {
	digest := sha256.Sum256(append([]byte(strconv.FormatInt(timestampMs, 10)+"."), body...))
	hashed := digest
	if doubleHash {
		hashed = sha256.Sum256(digest[:])
	}
	signature, err := rsa.SignPKCS1v15(rand.Reader, e.key, crypto.SHA256, hashed[:])
	require.NoError(t, err)
	return fmt.Sprintf("t=%d,v0=%s", timestampMs, base64.StdEncoding.EncodeToString(signature))
}

func (e *webhookTestEnv) post(t *testing.T, body []byte, signatureHeader string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/webhooks/bridge", bytes.NewReader(body))
	if signatureHeader != "" {
		req.Header.Set(webhookSignatureHeader, signatureHeader)
	}
	recorder := httptest.NewRecorder()
	e.handler.ServeHTTP(recorder, req)
	return recorder
}

func drainEventBody(id string) []byte {
	return []byte(fmt.Sprintf(`{
		"api_version": "v0",
		"event_id": "%s",
		"event_category": "liquidation_address.drain",
		"event_type": "liquidation_address.drain.updated.status_transitioned",
		"event_object_id": "drain-1",
		"event_object": {"id": "drain-1", "state": "payment_processed"},
		"event_created_at": "2026-07-28T12:00:00.000Z"
	}`, id))
}

func TestWebhookHandler_DispatchesVerifiedEvent(t *testing.T) {
	for _, doubleHash := range []bool{false, true} {
		env := newWebhookTestEnv(t)

		var received *Event
		env.handler.RegisterHandler(EventCategoryDrain, func(_ context.Context, event *Event) error {
			received = event
			return nil
		})

		body := drainEventBody("evt-1")
		recorder := env.post(t, body, env.sign(t, body, env.now.UnixMilli(), doubleHash))

		assert.Equal(t, http.StatusOK, recorder.Code, "doubleHash=%v", doubleHash)
		require.NotNil(t, received)
		assert.Equal(t, "evt-1", received.EventID)
		assert.Equal(t, EventCategoryDrain, received.EventCategory)
		assert.Equal(t, "drain-1", received.EventObjectID)

		var object Drain
		require.NoError(t, json.Unmarshal(received.EventObject, &object))
		assert.Equal(t, DrainStatePaymentProcessed, object.State)
	}
}

func TestWebhookHandler_NoopDefaultsAcknowledge(t *testing.T) {
	env := newWebhookTestEnv(t)

	// No handlers registered: consumed categories fall through to the no-op
	// default and are acknowledged.
	body := drainEventBody("evt-noop")
	recorder := env.post(t, body, env.sign(t, body, env.now.UnixMilli(), false))
	assert.Equal(t, http.StatusOK, recorder.Code)
}

func TestWebhookHandler_UnknownCategoryAcknowledged(t *testing.T) {
	env := newWebhookTestEnv(t)

	body := []byte(`{"event_id": "evt-2", "event_category": "card_transaction", "event_object": {}}`)
	recorder := env.post(t, body, env.sign(t, body, env.now.UnixMilli(), false))
	assert.Equal(t, http.StatusOK, recorder.Code)
}

func TestWebhookHandler_HandlerErrorReturns500(t *testing.T) {
	env := newWebhookTestEnv(t)
	env.handler.RegisterHandler(EventCategoryDrain, func(context.Context, *Event) error {
		return errors.New("transient store failure")
	})

	body := drainEventBody("evt-3")
	recorder := env.post(t, body, env.sign(t, body, env.now.UnixMilli(), false))
	assert.Equal(t, http.StatusInternalServerError, recorder.Code)
}

func TestWebhookHandler_RejectsTamperedBody(t *testing.T) {
	env := newWebhookTestEnv(t)

	body := drainEventBody("evt-4")
	header := env.sign(t, body, env.now.UnixMilli(), false)

	tampered := bytes.Replace(body, []byte("payment_processed"), []byte("payment_submitted"), 1)
	recorder := env.post(t, tampered, header)
	assert.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestWebhookHandler_RejectsStaleAndFutureTimestamps(t *testing.T) {
	env := newWebhookTestEnv(t)
	body := drainEventBody("evt-5")

	stale := env.now.Add(-11 * time.Minute).UnixMilli()
	recorder := env.post(t, body, env.sign(t, body, stale, false))
	assert.Equal(t, http.StatusBadRequest, recorder.Code)

	future := env.now.Add(2 * time.Minute).UnixMilli()
	recorder = env.post(t, body, env.sign(t, body, future, false))
	assert.Equal(t, http.StatusBadRequest, recorder.Code)

	// Within the window on both sides.
	recent := env.now.Add(-9 * time.Minute).UnixMilli()
	recorder = env.post(t, body, env.sign(t, body, recent, false))
	assert.Equal(t, http.StatusOK, recorder.Code)
}

func TestWebhookHandler_RejectsMissingOrMalformedHeader(t *testing.T) {
	env := newWebhookTestEnv(t)
	body := drainEventBody("evt-6")

	assert.Equal(t, http.StatusBadRequest, env.post(t, body, "").Code)
	assert.Equal(t, http.StatusBadRequest, env.post(t, body, "t=abc,v0=zzz").Code)
	assert.Equal(t, http.StatusBadRequest, env.post(t, body, "v0=AAAA").Code)
	assert.Equal(t, http.StatusBadRequest, env.post(t, body, fmt.Sprintf("t=%d", env.now.UnixMilli())).Code)
}

func TestWebhookHandler_RejectsWrongKeySignature(t *testing.T) {
	env := newWebhookTestEnv(t)

	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	other := &webhookTestEnv{key: otherKey, now: env.now}

	body := drainEventBody("evt-7")
	recorder := env.post(t, body, other.sign(t, body, env.now.UnixMilli(), false))
	assert.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestWebhookHandler_RejectsNonPost(t *testing.T) {
	env := newWebhookTestEnv(t)

	req := httptest.NewRequest(http.MethodGet, "/webhooks/bridge", nil)
	recorder := httptest.NewRecorder()
	env.handler.ServeHTTP(recorder, req)
	assert.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
}

func TestNewWebhookHandler_RejectsBadKeys(t *testing.T) {
	_, err := NewWebhookHandler([]byte("not pem"))
	assert.Error(t, err)

	_, err = NewWebhookHandler(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: []byte("garbage")}))
	assert.Error(t, err)
}

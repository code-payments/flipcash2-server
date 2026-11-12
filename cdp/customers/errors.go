package customers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// ErrorType is Coinbase's machine-readable error code (`errorType`). The
// constants are the ones a customers caller branches on; any other value
// Coinbase returns is kept on Error.Type as is.
type ErrorType string

const (
	// ErrorTypeInvalidRequest: the request failed validation (a field's
	// format, a Terms of Service version the customer is not required to
	// accept, a missing requester IP, ...). Error.Message says which, and
	// may quote what was submitted.
	ErrorTypeInvalidRequest ErrorType = "invalid_request"
	// ErrorTypeUnsupportedTosLanguage: an acceptance named a language its
	// Terms of Service version is not published in.
	ErrorTypeUnsupportedTosLanguage ErrorType = "unsupported_tos_language"
	// ErrorTypeNotFound: no such customer (or it was deleted).
	ErrorTypeNotFound ErrorType = "not_found"
	// ErrorTypeRateLimitExceeded: slow down and retry.
	ErrorTypeRateLimitExceeded ErrorType = "rate_limit_exceeded"
	// ErrorTypeIdempotency: the idempotency key was used before with a
	// different request.
	ErrorTypeIdempotency ErrorType = "idempotency_error"
	// ErrorTypeUnauthorized: the bearer JWT was refused.
	ErrorTypeUnauthorized ErrorType = "unauthorized"
)

// Error is a response from Coinbase other than success.
//
// Error() names the status, the type and the correlation ID and nothing
// else. Message is Coinbase's human-readable detail, which can quote values
// that were submitted (a name, an SSN's shape, a Terms of Service version),
// so it is kept apart for the caller to decide whether to log; it is never
// part of Error().
type Error struct {
	// StatusCode is the HTTP status.
	StatusCode int
	// Type is Coinbase's `errorType`, empty when the body was not a
	// Coinbase error (a proxy's 502 page, say).
	Type ErrorType
	// Message is Coinbase's `errorMessage`. May contain submitted values.
	Message string
	// CorrelationID is Coinbase's `correlationId`, for their support.
	CorrelationID string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("cdp customers: http %d", e.StatusCode)
	if e.Type != "" {
		s += " " + string(e.Type)
	}
	if e.CorrelationID != "" {
		s += " (correlation id " + e.CorrelationID + ")"
	}
	return s
}

// ErrInvalidArgument wraps every error for a request the client refused to
// send (a malformed customer ID or idempotency key, a tax attestation with
// no form). Nothing reached Coinbase.
var ErrInvalidArgument = errors.New("cdp customers: invalid argument")

// ErrMalformedResponse wraps the error for a successful response whose body
// could not be read as a customer. Whether the write it answered landed is
// unknown; retrying it under the same idempotency key is safe.
var ErrMalformedResponse = errors.New("cdp customers: malformed response")

// AsError returns err's *Error, if it is (or wraps) one.
func AsError(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// HasType reports whether err is a Coinbase error of type t.
func HasType(err error, t ErrorType) bool {
	e, ok := AsError(err)
	return ok && e.Type == t
}

// IsNotFound reports whether err says the customer does not exist.
func IsNotFound(err error) bool {
	e, ok := AsError(err)
	return ok && (e.Type == ErrorTypeNotFound || (e.Type == "" && e.StatusCode == http.StatusNotFound))
}

// IsInvalidRequest reports whether err is Coinbase's generic validation
// failure. Error.Message says what failed.
func IsInvalidRequest(err error) bool {
	return HasType(err, ErrorTypeInvalidRequest)
}

// IsUnsupportedTosLanguage reports whether err refused a Terms of Service
// acceptance for its language.
func IsUnsupportedTosLanguage(err error) bool {
	return HasType(err, ErrorTypeUnsupportedTosLanguage)
}

// IsRateLimited reports whether err is Coinbase asking the caller to slow
// down. It is retryable.
func IsRateLimited(err error) bool {
	e, ok := AsError(err)
	return ok && (e.Type == ErrorTypeRateLimitExceeded || e.StatusCode == http.StatusTooManyRequests)
}

// IsClientError reports whether err is a 4xx from Coinbase other than a
// rate limit, or a request the client refused to send: the request was
// wrong, and sending it again unchanged will fail the same way.
func IsClientError(err error) bool {
	if errors.Is(err, ErrInvalidArgument) {
		return true
	}
	e, ok := AsError(err)
	return ok && e.StatusCode >= 400 && e.StatusCode < 500 && !IsRateLimited(err)
}

// IsRetryable reports whether err may succeed if the request is sent again
// (under the same idempotency key, for a create): a 5xx, a rate limit, a
// transport failure or timeout, or a success whose body could not be read.
// A caller's own cancellation is not retryable, nor is a client error.
func IsRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrInvalidArgument) {
		return false
	}
	if e, ok := AsError(err); ok {
		return e.StatusCode >= 500 || IsRateLimited(err)
	}
	return true
}

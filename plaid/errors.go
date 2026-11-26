package plaid

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// ErrorType is Plaid's broad error category (`error_type`). Any value Plaid
// returns is kept on Error.Type as is; the constants are the ones callers
// branch on.
type ErrorType string

const (
	ErrorTypeInvalidRequest    ErrorType = "INVALID_REQUEST"
	ErrorTypeInvalidResult     ErrorType = "INVALID_RESULT"
	ErrorTypeInvalidInput      ErrorType = "INVALID_INPUT"
	ErrorTypeInstitutionError  ErrorType = "INSTITUTION_ERROR"
	ErrorTypeRateLimitExceeded ErrorType = "RATE_LIMIT_EXCEEDED"
	ErrorTypeAPIError          ErrorType = "API_ERROR"
	ErrorTypeItemError         ErrorType = "ITEM_ERROR"
	ErrorTypeOAuthError        ErrorType = "OAUTH_ERROR"
	ErrorTypeSandboxError      ErrorType = "SANDBOX_ERROR"
)

// Error codes (`error_code`) callers branch on. Plaid adds codes without
// notice, so a caller must handle codes it does not know.
const (
	// The Item no longer exists at Plaid: removed with /item/remove, by the
	// user at my.plaid.com, or on a data deletion request.
	CodeItemNotFound = "ITEM_NOT_FOUND"
	// The user must sign in again, through Link in update mode.
	CodeItemLoginRequired = "ITEM_LOGIN_REQUIRED"
	// The bank locked the user's login; they unlock it at the bank.
	CodeItemLocked = "ITEM_LOCKED"
	// No open account is left on the Item (closed, revoked, or churned).
	CodeNoAccounts = "NO_ACCOUNTS"
	// The user did not grant an OAuth permission the request needs.
	CodeAccessNotGranted = "ACCESS_NOT_GRANTED"

	// The public token is unknown, expired, or already exchanged.
	CodeInvalidPublicToken = "INVALID_PUBLIC_TOKEN"
	// The access token is unknown or from another environment.
	CodeInvalidAccessToken = "INVALID_ACCESS_TOKEN"
	// The account ID is not one of the Item's accounts (any more).
	CodeInvalidAccountID = "INVALID_ACCOUNT_ID"
	// The institution ID does not exist.
	CodeInvalidInstitution = "INVALID_INSTITUTION"

	CodeInstitutionDown          = "INSTITUTION_DOWN"
	CodeInstitutionNotResponding = "INSTITUTION_NOT_RESPONDING"
	CodeInstitutionNotAvailable  = "INSTITUTION_NOT_AVAILABLE"

	CodeInvalidAPIKeys                  = "INVALID_API_KEYS"
	CodeUnauthorizedEnvironment         = "UNAUTHORIZED_ENVIRONMENT"
	CodeUnauthorizedRouteAccess         = "UNAUTHORIZED_ROUTE_ACCESS"
	CodeInvalidProduct                  = "INVALID_PRODUCT"
	CodeInvalidLinkCustomization        = "INVALID_LINK_CUSTOMIZATION"
	CodeIncompatibleAPIVersion          = "INCOMPATIBLE_API_VERSION"
	CodeInvalidConfiguration            = "INVALID_CONFIGURATION"
	CodeInstitutionRegistrationRequired = "INSTITUTION_REGISTRATION_REQUIRED"

	CodeInternalServerError = "INTERNAL_SERVER_ERROR"
	CodePlannedMaintenance  = "PLANNED_MAINTENANCE"
)

// Error is a response from Plaid other than success, or an Item's error
// (Item.Error.Err).
//
// Error() names the status, type, code and request ID and nothing else, and
// is safe to log. Message (error_message) and DisplayMessage are Plaid's
// free text, which may change and is not for programmatic use; they are
// kept apart and are never part of Error().
type Error struct {
	// StatusCode is the HTTP status; 0 for an Item's error read from
	// /item/get.
	StatusCode int
	// Type is Plaid's error_type, empty when the body was not a Plaid
	// error (a proxy's 502 page, say).
	Type ErrorType
	// Code is Plaid's error_code.
	Code string
	// Message is Plaid's developer-facing error_message.
	Message string
	// DisplayMessage is Plaid's user-facing display_message; empty when
	// Plaid sent null.
	DisplayMessage string
	// RequestID is Plaid's request_id, for their support.
	RequestID string
}

func (e *Error) Error() string {
	s := "plaid:"
	if e.StatusCode != 0 {
		s += fmt.Sprintf(" http %d", e.StatusCode)
	}
	if e.Type != "" {
		s += " " + string(e.Type)
	}
	if e.Code != "" {
		s += " " + e.Code
	}
	if e.RequestID != "" {
		s += " (request id " + e.RequestID + ")"
	}
	return s
}

// ErrInvalidArgument wraps every error for a request the client refused to
// send. Nothing reached Plaid.
var ErrInvalidArgument = errors.New("plaid: invalid argument")

// ErrMalformedResponse wraps the error for a 200 response whose body could
// not be decoded. Whether a write it answered landed is unknown.
var ErrMalformedResponse = errors.New("plaid: malformed response")

// AsError returns err's *Error, if it is (or wraps) one.
func AsError(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// ErrorCode returns err's Plaid error_code, or "" when err is not a Plaid
// error.
func ErrorCode(err error) string {
	if e, ok := AsError(err); ok {
		return e.Code
	}
	return ""
}

// IsCode reports whether err is a Plaid error with error_code code.
func IsCode(err error, code string) bool {
	e, ok := AsError(err)
	return ok && e.Code == code
}

// IsItemNotFound reports whether the Item is gone at Plaid.
func IsItemNotFound(err error) bool { return IsCode(err, CodeItemNotFound) }

// IsItemLoginRequired reports whether the user must sign in again in update
// mode.
func IsItemLoginRequired(err error) bool { return IsCode(err, CodeItemLoginRequired) }

// IsItemLocked reports whether the bank locked the user's login.
func IsItemLocked(err error) bool { return IsCode(err, CodeItemLocked) }

// IsNoAccounts reports whether the Item has no open account left.
func IsNoAccounts(err error) bool { return IsCode(err, CodeNoAccounts) }

// IsInvalidPublicToken reports whether a public token is unknown, expired or
// already used.
func IsInvalidPublicToken(err error) bool { return IsCode(err, CodeInvalidPublicToken) }

// IsInvalidAccessToken reports whether an access token is unknown to Plaid
// (or from another environment). It is not how Plaid reports a removed
// Item, which is IsItemNotFound.
func IsInvalidAccessToken(err error) bool { return IsCode(err, CodeInvalidAccessToken) }

// IsRateLimited reports whether Plaid asked the caller to slow down: any
// RATE_LIMIT_EXCEEDED error (per Item, per endpoint, or per client), or a
// 429 with no Plaid body.
func IsRateLimited(err error) bool {
	e, ok := AsError(err)
	return ok && (e.Type == ErrorTypeRateLimitExceeded || e.StatusCode == http.StatusTooManyRequests)
}

// IsInstitutionDown reports whether the bank is temporarily unreachable
// through Plaid: INSTITUTION_DOWN, INSTITUTION_NOT_RESPONDING or
// INSTITUTION_NOT_AVAILABLE. Trying later may succeed.
func IsInstitutionDown(err error) bool {
	switch ErrorCode(err) {
	case CodeInstitutionDown, CodeInstitutionNotResponding, CodeInstitutionNotAvailable:
		return true
	}
	return false
}

// IsConfigError reports whether err says Flipcash's Plaid setup is wrong
// (keys, environment, products, Link customization, API version, or
// registration with a bank), not anything about the user: an operator must
// act, and no retry helps.
func IsConfigError(err error) bool {
	switch ErrorCode(err) {
	case CodeInvalidAPIKeys, CodeUnauthorizedEnvironment, CodeUnauthorizedRouteAccess,
		CodeInvalidProduct, CodeInvalidLinkCustomization, CodeIncompatibleAPIVersion,
		CodeInvalidConfiguration, CodeInstitutionRegistrationRequired:
		return true
	}
	return false
}

// IsRetryable reports whether err may succeed if the request is sent again
// later unchanged: a 5xx, a rate limit, a bank temporarily unreachable, a
// transport failure or timeout, or a 200 whose body could not be read. A
// caller's own cancellation is not retryable, nor is a refused argument or
// any other Plaid 4xx.
func IsRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrInvalidArgument) {
		return false
	}
	if e, ok := AsError(err); ok {
		return e.StatusCode >= 500 || IsRateLimited(err) || IsInstitutionDown(err)
	}
	return true
}

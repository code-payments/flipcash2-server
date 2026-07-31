package bridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// Error is a non-2xx response from the Bridge API.
//
// Only the structured code and message are retained; raw response bodies are
// dropped because validation errors can echo submitted PII.
type Error struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("bridge: http %d", e.StatusCode)
	if e.Code != "" {
		s += fmt.Sprintf(" (%s)", e.Code)
	}
	if e.Message != "" {
		s += ": " + e.Message
	}
	return s
}

// SanitizeError returns the form of err safe to report to metrics and logs:
// API errors keep only the status and machine-readable code, dropping the
// human-readable message, which may echo submitted PII in validation
// failures. Other errors pass through unchanged.
func SanitizeError(err error) error {
	var apiErr *Error
	if errors.As(err, &apiErr) {
		return &Error{StatusCode: apiErr.StatusCode, Code: apiErr.Code}
	}
	return err
}

func newError(statusCode int, body []byte) *Error {
	apiErr := &Error{StatusCode: statusCode}

	var parsed struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &parsed); err == nil {
		apiErr.Code = parsed.Code
		apiErr.Message = parsed.Message
	}
	return apiErr
}

// IsNotFound returns whether err is a Bridge 404.
func IsNotFound(err error) bool {
	var apiErr *Error
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound
}

// IsConflict returns whether err is a Bridge 409, e.g. creating a duplicate
// liquidation address for the same destination.
func IsConflict(err error) bool {
	var apiErr *Error
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict
}

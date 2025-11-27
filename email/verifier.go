package email

import (
	"context"
	"errors"
)

var (
	// ErrInvalidEmail is returned if the email address is invalid
	ErrInvalidEmail = errors.New("phone number is invalid")

	// ErrRateLimited indicates that the call was rate limited
	ErrRateLimited = errors.New("rate limited")

	// ErrInvalidVerificationCode is returned when the verification does not
	// match what's expected in a Confirm call
	ErrInvalidVerificationCode = errors.New("verification code does not match")

	// ErrNoVerification indicates that no verification is in progress for
	// the provided phone number in a Confirm call. Several reasons this
	// can occur include a verification being expired or having reached a
	// maximum check threshold.
	ErrNoVerification = errors.New("verification not in progress")
)

type Verifier interface {
	// SendCode sends a verification code via email to the provided email address.
	// If an active verification is already taking place, the existing code will
	// be resent. A unique ID for the verification is returned on success.
	SendCode(ctx context.Context, emailAddress, clientData string) (string, error)

	// Check verifies a email code sent to an email address.
	Check(ctx context.Context, emailAddress, code string) error

	// Cancel cancels an active verification. No error is returned if the
	// verification doesn't exist, previously canceled or successfully
	// completed.
	Cancel(ctx context.Context, id string) error

	// IsVerificationActive checks whether a verification is active or not
	IsVerificationActive(ctx context.Context, id string) (bool, error)

	// IsValidEmailAddress validates an email address is real and is able to
	// receive a verification code.
	IsValidEmailAddress(ctx context.Context, emailAddress string) (bool, error)
}

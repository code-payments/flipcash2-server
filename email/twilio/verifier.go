package twilio

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"

	"github.com/twilio/twilio-go"
	"github.com/twilio/twilio-go/client"
	verifyv2 "github.com/twilio/twilio-go/rest/verify/v2"

	"github.com/code-payments/ocp-server/pkg/metrics"
	"github.com/code-payments/flipcash2-server/email"
)

const (
	metricsStructName = "email.twilio.verifier"
)

const (
	// https://www.twilio.com/docs/api/errors/60200
	invalidParameterCode = 60200
	// https://www.twilio.com/docs/api/errors/60202
	maxCheckAttemptsCode = 60202
	// https://www.twilio.com/docs/api/errors/60203
	maxSendAttemptsCode = 60203
)

var (
	defaultChannel = "email"
)

var (
	statusApproved = "approved"
	statusCanceled = "canceled"
	statusPending  = "pending"
)

type verifier struct {
	client        *twilio.RestClient
	serviceSid    string
	baseVerifyURL string
}

// NewVerifier returns a new email verifier backed by Twilio
func NewVerifier(baseVerifyURL, accountSid, serviceSid, authToken string) email.Verifier {
	client := twilio.NewRestClientWithParams(twilio.ClientParams{
		Username: accountSid,
		Password: authToken,
	})

	return &verifier{
		client:        client,
		serviceSid:    serviceSid,
		baseVerifyURL: baseVerifyURL,
	}
}

// SendCode implements email.Verifier.SendCode
func (v *verifier) SendCode(ctx context.Context, emailAddress, clientData string) (string, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "SendCode")
	defer tracer.End()

	isValid, err := v.IsValidEmailAddress(ctx, emailAddress)
	if err != nil {
		return "", err
	} else if !isValid {
		return "", email.ErrInvalidEmail
	}

	var channelConfig interface{} = map[string]any{
		"substitutions": map[string]any{
			"recipient_email":          emailAddress,
			"verification_url":         v.baseVerifyURL,
			"url_safe_recipient_email": url.QueryEscape(emailAddress),
			"client_data":              url.QueryEscape(clientData),
		},
	}
	resp, err := v.client.VerifyV2.CreateVerification(v.serviceSid, &verifyv2.CreateVerificationParams{
		To:                   &emailAddress,
		Channel:              &defaultChannel,
		ChannelConfiguration: &channelConfig,
	})
	if err != nil {
		err = checkInvalidToParameterError(err, email.ErrInvalidEmail)
		err = checkMaxSendAttemptsError(err, email.ErrRateLimited)
		tracer.OnError(err)
		return "", err
	}

	if resp.Sid == nil {
		err = errors.New("sid not provided")
		tracer.OnError(err)
		return "", err
	}

	return *resp.Sid, nil
}

// Check implements email.Verifier.Check
func (v *verifier) Check(ctx context.Context, emailAddress, code string) error {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "Check")
	defer tracer.End()

	if !email.IsVerificationCode(code) {
		err := email.ErrInvalidVerificationCode
		tracer.OnError(err)
		return err
	}

	resp, err := v.client.VerifyV2.CreateVerificationCheck(v.serviceSid, &verifyv2.CreateVerificationCheckParams{
		To:   &emailAddress,
		Code: &code,
	})
	if err != nil {
		err = check404Error(err, email.ErrNoVerification)
		err = checkMaxCheckAttemptsError(err, email.ErrNoVerification)
		tracer.OnError(err)
		return err
	}

	if resp.Status == nil {
		err = errors.New("status not provided")
		tracer.OnError(err)
		return err
	}

	switch strings.ToLower(*resp.Status) {
	case statusApproved:
		err = nil
	case statusCanceled:
		err = email.ErrNoVerification
	default:
		err = email.ErrInvalidVerificationCode
	}
	tracer.OnError(err)
	return err
}

// Cancel implements email.Verifier.Cancel
func (v *verifier) Cancel(ctx context.Context, id string) error {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "Cancel")
	defer tracer.End()

	_, err := v.client.VerifyV2.UpdateVerification(v.serviceSid, id, &verifyv2.UpdateVerificationParams{
		Status: &statusCanceled,
	})

	if err != nil {
		err := check404Error(err, nil)
		tracer.OnError(err)
		return err
	}

	return nil
}

// IsVerificationActive implements email.Verifier.IsVerificationActive
func (v *verifier) IsVerificationActive(ctx context.Context, id string) (bool, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "IsVerificationActive")
	defer tracer.End()

	resp, err := v.client.VerifyV2.FetchVerification(v.serviceSid, id)
	if is404Error(err) {
		return false, nil
	} else if err != nil {
		tracer.OnError(err)
		return false, err
	}

	if resp.Status == nil {
		err = errors.New("status is not provided")
		tracer.OnError(err)
		return false, err
	}

	return *resp.Status == statusPending, nil
}

// IsValidEmailAddress implements email.Verifier.IsValidEmailAddress
func (v *verifier) IsValidEmailAddress(_ context.Context, emailAddress string) (bool, error) {
	return email.IsEmailAddress(emailAddress), nil
}

func check404Error(inError, outError error) error {
	if is404Error(inError) {
		return outError
	}
	return inError
}

func is404Error(err error) bool {
	twilioError, ok := err.(*client.TwilioRestError)
	if !ok {
		return false
	}

	return twilioError.Status == http.StatusNotFound
}

func checkMaxSendAttemptsError(inError, outError error) error {
	twilioError, ok := inError.(*client.TwilioRestError)
	if !ok {
		return inError
	}

	if twilioError.Status != http.StatusTooManyRequests {
		return inError
	}

	if twilioError.Code == maxSendAttemptsCode {
		return outError
	}
	return inError
}

func checkMaxCheckAttemptsError(inError, outError error) error {
	twilioError, ok := inError.(*client.TwilioRestError)
	if !ok {
		return inError
	}

	if twilioError.Status != http.StatusTooManyRequests {
		return inError
	}

	if twilioError.Code == maxCheckAttemptsCode {
		return outError
	}
	return inError
}

func checkInvalidToParameterError(inError, outError error) error {
	twilioError, ok := inError.(*client.TwilioRestError)
	if !ok {
		return inError
	}

	if twilioError.Status != http.StatusBadRequest {
		return inError
	}

	if twilioError.Code != invalidParameterCode {
		return inError
	}

	expectedMessage := strings.ToLower("Invalid parameter `To`")
	if strings.Contains(strings.ToLower(twilioError.Message), expectedMessage) {
		return outError
	}
	return inError
}

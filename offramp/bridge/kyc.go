package bridge

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/text/language"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	kycpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/v1"
	thirdpartypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/thirdparty/v1"
)

// Mapping between Bridge's customer model and the partner-neutral KYC protos.
// Bridge is the sole validator of submitted identity data; verification state
// is never persisted, and every KycState is derived fresh from a live
// customer.

// idempotencyKeyNamespace is the UUIDv5 namespace for Bridge idempotency keys.
var idempotencyKeyNamespace = uuid.MustParse("f00c3199-73b8-4b73-a1a4-a54f8f9d0d34")

// TOSLinkIdempotencyKey derives the idempotency key for a TOS link creation.
// TOS links are per-session resources and duplicates are inert, so the key
// only needs to hold within a transient-failure retry window: bucketing the
// timestamp to the minute dedupes quick retries, while a user re-entering the
// flow later gets a fresh acceptance session.
func TOSLinkIdempotencyKey(clientReferenceID string, now time.Time) string {
	name := fmt.Sprintf("tos-link:%s:%d", clientReferenceID, now.UTC().Truncate(time.Minute).Unix())
	return uuid.NewSHA1(idempotencyKeyNamespace, []byte(name)).String()
}

// CustomerIdempotencyKey derives the idempotency key for creating a user's
// customer from the submission payload: a retry of the same submission reuses
// its key and lands on the original customer, while any changed field (e.g. a
// correction after a validation rejection) mints a fresh one. The key cannot
// carry the one-customer-per-user invariant — Bridge honors a key for only 24
// hours and rejects reuse beyond that with a 422 — so uniqueness is enforced
// by the store mapping instead, which short-circuits submission before any
// customer creation.
func CustomerIdempotencyKey(clientReferenceID string, submission *kycpb.IndividualKycSubmission) string {
	serialized, err := proto.MarshalOptions{Deterministic: true}.Marshal(submission)
	if err != nil {
		serialized = []byte(submission.String())
	}
	digest := sha256.Sum256(serialized)
	name := fmt.Sprintf("customer:%s:%x", clientReferenceID, digest)
	return uuid.NewSHA1(idempotencyKeyNamespace, []byte(name)).String()
}

// KycStateFromCustomer maps a customer to the user's verification state.
func KycStateFromCustomer(customer *Customer) *kycpb.KycState {
	status := kycStatusFromCustomer(customer.Status)
	requirements, pendingChecks, blocked := requirementsFromCustomer(customer)

	state := &kycpb.KycState{
		Status:         status,
		NextStep:       nextStep(status, requirements, pendingChecks, blocked),
		Requirements:   requirements,
		Partner:        thirdpartypb.Partner_BRIDGE,
		SubmissionType: kycpb.SubmissionType_INDIVIDUAL,
		Features: &kycpb.KycState_BridgeFeatures{
			BridgeFeatures: &kycpb.BridgeFeatures{
				CanOfframp: canOfframp(customer),
			},
		},
	}
	if status == kycpb.KycStatus_REJECTED {
		state.RejectionReasons = rejectionReasonsFromCustomer(customer)
	}
	return state
}

// rejectionReasonsFromCustomer maps the customer-facing rejection reasons,
// most recent first. The developer_reason arm carries sensitive diagnostics
// and is deliberately never surfaced. Reasons are keyed on REJECTED status so
// stale entries on a customer who later recovered stay unsurfaced. Timestamps
// are Bridge's ISO 8601 UTC strings, so recency compares lexicographically.
func rejectionReasonsFromCustomer(customer *Customer) []*kycpb.KycState_RejectionReason {
	reasons := make([]RejectionReason, 0, len(customer.RejectionReasons))
	for _, reason := range customer.RejectionReasons {
		if reason.Reason != "" {
			reasons = append(reasons, reason)
		}
	}
	sort.SliceStable(reasons, func(i, j int) bool {
		return reasons[i].CreatedAt > reasons[j].CreatedAt
	})

	converted := make([]*kycpb.KycState_RejectionReason, len(reasons))
	for i, reason := range reasons {
		converted[i] = &kycpb.KycState_RejectionReason{Message: reason.Reason}
	}
	return converted
}

// canOfframp derives whether the user can cash out: a fully verified customer
// whose base endorsement (the rails offramp payouts use) is approved.
func canOfframp(customer *Customer) bool {
	base := customer.BaseEndorsement()
	return customer.Status == CustomerStatusActive &&
		base != nil && base.Status == EndorsementStatusApproved
}

func kycStatusFromCustomer(status string) kycpb.KycStatus {
	switch status {
	case CustomerStatusNotStarted:
		return kycpb.KycStatus_NOT_STARTED
	case CustomerStatusIncomplete, CustomerStatusAwaitingQuestionnaire, CustomerStatusAwaitingUBO:
		return kycpb.KycStatus_INCOMPLETE
	case CustomerStatusUnderReview:
		return kycpb.KycStatus_UNDER_REVIEW
	case CustomerStatusActive:
		return kycpb.KycStatus_ACTIVE
	case CustomerStatusRejected:
		return kycpb.KycStatus_REJECTED
	case CustomerStatusOffboarded:
		return kycpb.KycStatus_OFFBOARDED
	case CustomerStatusPaused:
		return kycpb.KycStatus_PAUSED
	default:
		return kycpb.KycStatus_KYC_STATUS_UNKNOWN
	}
}

// nextStep derives what the user should do, in precedence order: blockers
// (offboarding's permanent account closure, or a final verdict no
// resubmission can resolve — see blockingCodes) trump everything, anything
// actionable trumps waiting, and only a fully verified customer with no
// partner-side checks in flight has nothing to do. Rejection itself is not
// terminal — Bridge accepts corrections to a rejected submission at any time
// — so absent a blocker it stays actionable: the user corrects and
// resubmits, guided by requirements when Bridge enumerates them and by
// rejection_reasons otherwise.
func nextStep(status kycpb.KycStatus, requirements []*kycpb.KycState_Requirement, pendingChecks, blocked bool) kycpb.KycState_NextStep {
	if status == kycpb.KycStatus_OFFBOARDED || blocked {
		return kycpb.KycState_BLOCKED
	}
	if status == kycpb.KycStatus_REJECTED {
		return kycpb.KycState_PROVIDE_REQUIREMENTS
	}

	if len(requirements) > 0 {
		return kycpb.KycState_PROVIDE_REQUIREMENTS
	}

	if status == kycpb.KycStatus_ACTIVE && !pendingChecks {
		return kycpb.KycState_NONE
	}
	return kycpb.KycState_WAIT
}

// requirementsFromCustomer flattens the base endorsement's outstanding
// requirements into actionable entries, and separately reports whether any
// partner-side checks are still in flight and whether verification is
// blocked — by a final verdict, or by vocabulary the mapping does not
// recognize yet. Requirements carry only what the user can provide or fix;
// partner-side states surface as WAIT and blockers as BLOCKED via next_step
// instead (see KycState.requirements docs). Entries are not deduplicated by
// field; clients group them (see KycState.requirements docs).
func requirementsFromCustomer(customer *Customer) ([]*kycpb.KycState_Requirement, bool, bool) {
	var requirements []*kycpb.KycState_Requirement
	var pendingChecks, blocked bool
	appendRequirement := func(identifier rawIdentifier, kind kycpb.KycState_Requirement_Kind) {
		if isBlockingCode(identifier.value) {
			blocked = true
			return
		}
		if isVerdictCode(identifier.value) {
			return
		}
		if isPartnerSidePending(identifier.value) {
			pendingChecks = true
			return
		}
		field := identifier.field()
		// Unrecognized vocabulary is a blocker, not an actionable
		// requirement: without a mapping update we cannot direct the user to
		// a field, so presenting it as fixable would strand them.
		if field == kycpb.KycState_Requirement_FIELD_UNKNOWN {
			blocked = true
			return
		}
		requirements = append(requirements, &kycpb.KycState_Requirement{
			Field:    field,
			Kind:     kind,
			RawValue: identifier.value,
		})
	}

	if base := customer.BaseEndorsement(); base != nil && base.Requirements != nil {
		if len(base.Requirements.Pending) > 0 {
			pendingChecks = true
		}
		for _, identifier := range collectRawIdentifiers(base.Requirements.Missing) {
			appendRequirement(identifier, kycpb.KycState_Requirement_MISSING)
		}
		for _, identifier := range collectRawIdentifiers(base.Requirements.Issues) {
			appendRequirement(identifier, kycpb.KycState_Requirement_ISSUE)
		}
	}

	// awaiting_questionnaire is Bridge blocked on the enhanced-onboarding
	// questionnaire; guarantee an actionable requirement even when the
	// endorsement requirements don't enumerate the individual questions.
	if customer.Status == CustomerStatusAwaitingQuestionnaire {
		hasComplianceProfile := false
		for _, requirement := range requirements {
			if requirement.Field == kycpb.KycState_Requirement_COMPLIANCE_PROFILE {
				hasComplianceProfile = true
				break
			}
		}
		if !hasComplianceProfile {
			requirements = append(requirements, &kycpb.KycState_Requirement{
				Field:    kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
				Kind:     kycpb.KycState_Requirement_MISSING,
				RawValue: CustomerStatusAwaitingQuestionnaire,
			})
		}
	}

	// Bridge tracks terms-of-service acceptance on the customer, outside
	// endorsement requirements (e.g. when an updated version requires
	// re-acceptance of terms on an already-approved endorsement). The
	// endorsement requirements usually also enumerate acceptance as
	// terms_of_service_v1/v2; the synthetic fills the gap only when they
	// don't.
	if !customer.HasAcceptedTermsOfService {
		hasAgreement := false
		for _, requirement := range requirements {
			if requirement.Field == kycpb.KycState_Requirement_AGREEMENT {
				hasAgreement = true
				break
			}
		}
		if !hasAgreement {
			requirements = append(requirements, &kycpb.KycState_Requirement{
				Field:    kycpb.KycState_Requirement_AGREEMENT,
				Kind:     kycpb.KycState_Requirement_MISSING,
				RawValue: "terms_of_service",
			})
		}
	}
	return requirements, pendingChecks, blocked
}

// requirementFields maps Bridge's requirement and issue identifiers to the
// fields they re-collect. Identifiers not covered by an exact match fall
// through to prefix rules (Bridge suffixes issue codes, e.g.
// "government_id_verification_failed", "poa_name_mismatch"), then to
// FIELD_UNKNOWN with the identifier preserved in raw_value.
var requirementFields = map[string]kycpb.KycState_Requirement_Field{
	"first_name": kycpb.KycState_Requirement_LEGAL_NAME,
	"last_name":  kycpb.KycState_Requirement_LEGAL_NAME,
	"full_name":  kycpb.KycState_Requirement_LEGAL_NAME,

	"birth_date":         kycpb.KycState_Requirement_BIRTH_DATE,
	"date_of_birth":      kycpb.KycState_Requirement_BIRTH_DATE,
	"customer_too_young": kycpb.KycState_Requirement_BIRTH_DATE,
	"customer_over_age":  kycpb.KycState_Requirement_BIRTH_DATE,

	"residential_address":  kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,
	"address_of_residence": kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,

	"tax_identification_number": kycpb.KycState_Requirement_TAX_ID,
	"ssn":                       kycpb.KycState_Requirement_TAX_ID,

	"id_verification":             kycpb.KycState_Requirement_ID_DOCUMENT,
	"manual_government_id_review": kycpb.KycState_Requirement_ID_DOCUMENT,
	"has_valid_national_id":       kycpb.KycState_Requirement_ID_DOCUMENT,

	// Database verification failures name the mismatched field; a correction
	// resubmits that field's data.
	"database_check_failed_on_name_first":             kycpb.KycState_Requirement_LEGAL_NAME,
	"database_check_failed_on_name_middle":            kycpb.KycState_Requirement_LEGAL_NAME,
	"database_check_failed_on_name_last":              kycpb.KycState_Requirement_LEGAL_NAME,
	"database_check_failed_on_social_security_number": kycpb.KycState_Requirement_TAX_ID,
	"database_check_failed_on_tin_validation":         kycpb.KycState_Requirement_TAX_ID,
	"database_check_failed_on_house_number":           kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,
	"database_check_failed_on_street_name":            kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,
	"database_check_failed_on_street_type":            kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,
	"database_check_failed_on_city":                   kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,
	"database_check_failed_on_postal_code":            kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,
	"database_check_failed_on_subdivision":            kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,
	"database_check_failed_on_country_code":           kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,

	"email":         kycpb.KycState_Requirement_EMAIL,
	"email_address": kycpb.KycState_Requirement_EMAIL,

	"phone":        kycpb.KycState_Requirement_PHONE,
	"phone_number": kycpb.KycState_Requirement_PHONE,

	"account_purpose":               kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
	"source_of_funds":               kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
	"employment_status":             kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
	"expected_monthly_payments_usd": kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
	"acting_as_intermediary":        kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
	"most_recent_occupation":        kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
	"minimal_source_of_funds_data":  kycpb.KycState_Requirement_COMPLIANCE_PROFILE,

	// Answer-consistency risk flags; the correction is revising the
	// questionnaire answers.
	"unemployed_using_salary":             kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
	"funds_sourced_pension_or_retirement": kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
	"high_expected_monthly_payments":      kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
	"suspicious_source_of_funds":          kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
	"suspicious_primary_purpose":          kycpb.KycState_Requirement_COMPLIANCE_PROFILE,

	"terms_of_service": kycpb.KycState_Requirement_AGREEMENT,
}

var requirementFieldPrefixes = []struct {
	prefix string
	field  kycpb.KycState_Requirement_Field
}{
	{"government_id", kycpb.KycState_Requirement_ID_DOCUMENT},
	// Covers id_verification_failed, id_expired, id_front_photo, etc.
	{"id_", kycpb.KycState_Requirement_ID_DOCUMENT},
	{"proof_of_address", kycpb.KycState_Requirement_PROOF_OF_ADDRESS},
	{"poa_", kycpb.KycState_Requirement_PROOF_OF_ADDRESS},
	// Covers residence_address_invalid_postal_code, etc.
	{"residence_address", kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS},
	// Covers subdivision_not_ny_usa, subdivision_not_ak_usa, etc.
	{"subdivision_not_", kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS},
	// Covers min_age_18, min_age_60.
	{"min_age_", kycpb.KycState_Requirement_BIRTH_DATE},
	// Covers source_of_funds_questionnaire.
	{"source_of_funds", kycpb.KycState_Requirement_COMPLIANCE_PROFILE},
	{"terms_of_service", kycpb.KycState_Requirement_AGREEMENT},
	{"tos_", kycpb.KycState_Requirement_AGREEMENT},
	{"ssn_", kycpb.KycState_Requirement_TAX_ID},
	{"tax_identification", kycpb.KycState_Requirement_TAX_ID},
}

func requirementField(rawValue string) kycpb.KycState_Requirement_Field {
	normalized := strings.ToLower(rawValue)
	if field, ok := requirementFields[normalized]; ok {
		return field
	}
	for _, rule := range requirementFieldPrefixes {
		if strings.HasPrefix(normalized, rule.prefix) {
			return rule.field
		}
	}
	return kycpb.KycState_Requirement_FIELD_UNKNOWN
}

// CustomerRequestFromProtoSubmission converts an individual submission into the
// customer-creation payload, tagged with clientReferenceID (the user ID) for
// reconciliation. Everything converted here is PII in flight; the result must
// never be persisted or logged.
func CustomerRequestFromProtoSubmission(clientReferenceID string, submission *kycpb.IndividualKycSubmission) (*CreateCustomerRequest, error) {
	if len(submission.SignedAgreementTokens) == 0 {
		return nil, fmt.Errorf("missing signed agreement token")
	}

	address, err := addressFromProto(submission.Address)
	if err != nil {
		return nil, err
	}

	// The tax ID leads the identifying information, followed by each captured
	// ID document.
	taxIDType, err := taxIDTypeFromProto(submission.TaxId.Type)
	if err != nil {
		return nil, err
	}
	identifying := []IdentifyingInformation{{
		Type: taxIDType,
		// SSNs and ITINs are US-issued identifiers.
		IssuingCountry: "usa",
		Number:         submission.TaxId.Value,
	}}
	for _, document := range submission.IdentityDocuments {
		converted, err := identityDocumentFromProto(document)
		if err != nil {
			return nil, err
		}
		identifying = append(identifying, *converted)
	}

	var documents []Document
	for _, document := range submission.SupportingDocuments {
		documents = append(documents, supportingDocumentFromProto(document))
	}

	req := &CreateCustomerRequest{
		Type:       CustomerTypeIndividual,
		FirstName:  submission.Name.FirstName,
		MiddleName: submission.Name.MiddleName,
		LastName:   submission.Name.LastName,
		Email:      submission.Email.Value,
		Phone:      submission.Phone.Value,
		BirthDate:  dateFromProto(submission.BirthDate),

		// Tags the customer with the user ID for reconciliation, e.g.
		// recovering a mapping lost between customer creation and the store
		// write.
		ClientReferenceID: clientReferenceID,

		ResidentialAddress:     *address,
		IdentifyingInformation: identifying,
		Documents:              documents,

		// Consumes the token minted by the hosted TOS acceptance page; Bridge
		// takes a single agreement.
		SignedAgreementID: submission.SignedAgreementTokens[0].Value,

		// The base endorsement carries the payout rails offramp uses.
		Endorsements: []string{EndorsementBase},
	}
	applyComplianceProfile(&req.complianceAnswers, submission.GetBridgeCompliance())
	return req, nil
}

// IsEmptyUpdate reports whether the update provides nothing to change.
func IsEmptyUpdate(update *kycpb.IndividualKycUpdate) bool {
	return update.Name == nil &&
		update.BirthDate == nil &&
		update.Address == nil &&
		update.TaxId == nil &&
		len(update.IdentityDocuments) == 0 &&
		update.Email == nil &&
		update.Phone == nil &&
		len(update.SupportingDocuments) == 0 &&
		len(update.SignedAgreementTokens) == 0 &&
		update.GetBridgeCompliance() == nil
}

// UpdateCustomerRequestFromProtoUpdate converts a partial resubmission into the
// customer-update payload; only provided fields are set. As with submissions,
// the result is PII in flight and must never be persisted or logged.
func UpdateCustomerRequestFromProtoUpdate(update *kycpb.IndividualKycUpdate) (*UpdateCustomerRequest, error) {
	req := &UpdateCustomerRequest{}

	if update.Name != nil {
		req.FirstName = update.Name.FirstName
		req.MiddleName = update.Name.MiddleName
		req.LastName = update.Name.LastName
	}
	if update.BirthDate != nil {
		req.BirthDate = dateFromProto(update.BirthDate)
	}
	if update.Address != nil {
		address, err := addressFromProto(update.Address)
		if err != nil {
			return nil, err
		}
		req.ResidentialAddress = address
	}

	// A resubmitted tax ID leads the identifying information, as on creation.
	if update.TaxId != nil {
		taxIDType, err := taxIDTypeFromProto(update.TaxId.Type)
		if err != nil {
			return nil, err
		}
		req.IdentifyingInformation = append(req.IdentifyingInformation, IdentifyingInformation{
			Type:           taxIDType,
			IssuingCountry: "usa",
			Number:         update.TaxId.Value,
		})
	}
	for _, document := range update.IdentityDocuments {
		converted, err := identityDocumentFromProto(document)
		if err != nil {
			return nil, err
		}
		req.IdentifyingInformation = append(req.IdentifyingInformation, *converted)
	}

	if update.Email != nil {
		req.Email = update.Email.Value
	}
	if update.Phone != nil {
		req.Phone = update.Phone.Value
	}

	for _, document := range update.SupportingDocuments {
		req.Documents = append(req.Documents, supportingDocumentFromProto(document))
	}

	if len(update.SignedAgreementTokens) > 0 {
		req.SignedAgreementID = update.SignedAgreementTokens[0].Value
	}

	applyComplianceProfile(&req.complianceAnswers, update.GetBridgeCompliance())
	return req, nil
}

// SubmitKycResultFromError translates a customer-creation failure into a
// client-actionable result, or reports false for errors that should surface
// as internal failures.
func SubmitKycResultFromError(err error) (kycpb.SubmitKycResponse_Result, bool) {
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		return 0, false
	}
	// An invalid or expired signed_agreement_id means the TOS acceptance must
	// be redone.
	if strings.Contains(strings.ToLower(apiErr.Code+" "+apiErr.Message), "agreement") {
		return kycpb.SubmitKycResponse_AGREEMENT_EXPIRED, true
	}
	return 0, false
}

func addressFromProto(address *kycpb.Address) (*Address, error) {
	country, err := alpha3(address.Country)
	if err != nil {
		return nil, err
	}
	return &Address{
		StreetLine1: address.StreetLine_1,
		StreetLine2: address.StreetLine_2,
		City:        address.City,
		Subdivision: address.Subdivision,
		PostalCode:  address.PostalCode,
		Country:     country,
	}, nil
}

func identityDocumentFromProto(document *kycpb.IdentityDocument) (*IdentifyingInformation, error) {
	documentType, err := documentTypeFromProto(document.Type)
	if err != nil {
		return nil, err
	}
	issuingCountry, err := alpha3(document.IssuingCountry)
	if err != nil {
		return nil, err
	}

	info := &IdentifyingInformation{
		Type: documentType,
		// Bridge documents issuing_country in lowercase, unlike address
		// countries.
		IssuingCountry: strings.ToLower(issuingCountry),
		Number:         document.DocumentNumber,
		ImageFront:     dataURI(document.MimeType, document.FrontImage),
	}
	if len(document.BackImage) > 0 {
		info.ImageBack = dataURI(document.MimeType, document.BackImage)
	}
	if document.ExpirationDate != nil {
		info.Expiration = dateFromProto(document.ExpirationDate)
	}
	return info, nil
}

func supportingDocumentFromProto(document *kycpb.SupportingDocument) Document {
	purposes := make([]string, 0, len(document.Purposes))
	for _, purpose := range document.Purposes {
		purposes = append(purposes, documentPurposeFromProto(purpose))
	}
	return Document{
		Purposes:    purposes,
		File:        dataURI(document.MimeType, document.File),
		Description: document.Description,
	}
}

// applyComplianceProfile sets the enhanced-onboarding questionnaire answers,
// leaving unanswered questions unset. The proto enum values mirror Bridge's
// accepted answer lists, so answers convert by name.
func applyComplianceProfile(answers *complianceAnswers, profile *kycpb.BridgeComplianceProfile) {
	if profile == nil {
		return
	}
	if profile.AccountPurpose != kycpb.BridgeComplianceProfile_ACCOUNT_PURPOSE_UNKNOWN {
		if profile.AccountPurpose == kycpb.BridgeComplianceProfile_ACCOUNT_PURPOSE_OTHER {
			answers.AccountPurpose = "other"
		} else {
			answers.AccountPurpose = strings.ToLower(profile.AccountPurpose.String())
		}
		answers.AccountPurposeOther = profile.AccountPurposeOther
	}
	if profile.SourceOfFunds != kycpb.BridgeComplianceProfile_SOURCE_OF_FUNDS_UNKNOWN {
		answers.SourceOfFunds = strings.ToLower(profile.SourceOfFunds.String())
	}
	if profile.EmploymentStatus != kycpb.BridgeComplianceProfile_EMPLOYMENT_STATUS_UNKNOWN {
		answers.EmploymentStatus = strings.ToLower(profile.EmploymentStatus.String())
	}
	if volume := expectedMonthlyPaymentsFromProto(profile.ExpectedMonthlyVolume); volume != "" {
		answers.ExpectedMonthlyPaymentsUSD = volume
	}

	// The proto cannot distinguish an unanswered question from an answered
	// "no", so the answer is sent whenever the profile is provided.
	actingAsIntermediary := profile.ActingAsIntermediary
	answers.ActingAsIntermediary = &actingAsIntermediary

	answers.MostRecentOccupation = profile.Occupation
}

func taxIDTypeFromProto(taxIDType kycpb.TaxId_Type) (string, error) {
	switch taxIDType {
	case kycpb.TaxId_US_SSN:
		return IDTypeSSN, nil
	case kycpb.TaxId_US_ITIN:
		return IDTypeITIN, nil
	default:
		return "", fmt.Errorf("unsupported tax id type %s", taxIDType)
	}
}

func documentTypeFromProto(documentType kycpb.IdentityDocument_Type) (string, error) {
	switch documentType {
	case kycpb.IdentityDocument_DRIVERS_LICENSE:
		return IDTypeDriversLicense, nil
	case kycpb.IdentityDocument_PASSPORT:
		return IDTypePassport, nil
	case kycpb.IdentityDocument_STATE_ID:
		return IDTypeStateID, nil
	default:
		return "", fmt.Errorf("unsupported document type %s", documentType)
	}
}

func documentPurposeFromProto(purpose kycpb.SupportingDocument_Purpose) string {
	if purpose == kycpb.SupportingDocument_PURPOSE_OTHER {
		return "other"
	}
	return strings.ToLower(purpose.String())
}

func expectedMonthlyPaymentsFromProto(volume kycpb.BridgeComplianceProfile_ExpectedMonthlyVolume) string {
	switch volume {
	case kycpb.BridgeComplianceProfile_UNDER_5K_USD:
		return ExpectedMonthlyPaymentsUnder5K
	case kycpb.BridgeComplianceProfile_FROM_5K_TO_10K_USD:
		return ExpectedMonthlyPayments5KTo10K
	case kycpb.BridgeComplianceProfile_FROM_10K_TO_50K_USD:
		return ExpectedMonthlyPayments10KTo50K
	case kycpb.BridgeComplianceProfile_OVER_50K_USD:
		return ExpectedMonthlyPaymentsOver50K
	default:
		return ""
	}
}

// dateFromProto formats a date as Bridge's YYYY-MM-DD.
func dateFromProto(date *commonpb.Date) string {
	return fmt.Sprintf("%04d-%02d-%02d", date.Year, date.Month, date.Day)
}

// dataURI encodes a file as the base64 data URI Bridge accepts for images and
// documents.
func dataURI(mimeType string, data []byte) string {
	return "data:" + mimeType + ";base64," + base64.StdEncoding.EncodeToString(data)
}

// alpha3 converts an ISO 3166-1 alpha-2 country code to the alpha-3 form
// Bridge uses.
func alpha3(country *commonpb.CountryCode) (string, error) {
	region, err := language.ParseRegion(country.Value)
	if err != nil {
		return "", errors.New("invalid country code")
	}
	// ParseRegion also accepts private-use codes (e.g. "XX") and deprecated
	// aliases with no ISO3 mapping (e.g. "UK"), so both checks are needed.
	iso3 := region.ISO3()
	if !region.IsCountry() || len(iso3) != 3 || iso3 == "ZZZ" {
		return "", errors.New("unsupported country code")
	}
	return iso3, nil
}

// rawIdentifier is one identifier extracted from a provider-defined
// requirements payload, along with the nearest enclosing map key when there
// was one: issue entries can correlate an identifier to the field it concerns
// (e.g. {"id_front_photo": "id_expired"}).
type rawIdentifier struct {
	value string
	key   string
}

// field maps the identifier to the field it re-collects. The identifier
// itself wins; the correlating key breaks ties for unrecognized codes (e.g.
// {"birth_date": "implausible"}).
func (r rawIdentifier) field() kycpb.KycState_Requirement_Field {
	if field := requirementField(r.value); field != kycpb.KycState_Requirement_FIELD_UNKNOWN {
		return field
	}
	if r.key != "" {
		return requirementField(r.key)
	}
	return kycpb.KycState_Requirement_FIELD_UNKNOWN
}

// blockingCodes are identifiers whose verdict is final: no resubmission can
// resolve them (an incompatible developer or customer profile, a blocklisted
// person, an unsupported or prohibited region or activity), so they surface
// as BLOCKED via next_step rather than inviting hopeless corrections.
// developer_not_compatible additionally means the Flipcash Bridge account
// needs attention on our side.
var blockingCodes = map[string]struct{}{
	"developer_not_compatible":                        {},
	"blocklist_check_failed":                          {},
	"customer_not_compatible":                         {},
	"endorsement_not_available_in_customers_region":   {},
	"operates_in_prohibited_countries":                {},
	"high_risk_business_activities":                   {},
	"rejected_due_to_unsupported_geo":                 {},
	"rejected_due_to_unsupported_business_activities": {},
	"rejected_due_to_unidentifiable_business_entity":  {},
}

func isBlockingCode(rawValue string) bool {
	_, ok := blockingCodes[strings.ToLower(rawValue)]
	return ok
}

// verdictCodes are identifiers reporting the outcome of Bridge's decision
// rather than something to collect or wait on. Unlike blockingCodes the
// outcome is not final — a materially corrected resubmission can pass — and
// it is already conveyed by the customer status and rejection_reasons, so
// these are ignored: they name no field the user can fix and would only add
// noise to requirements.
var verdictCodes = map[string]struct{}{
	"manual_review_rejected": {},
}

func isVerdictCode(rawValue string) bool {
	normalized := strings.ToLower(rawValue)
	if _, ok := verdictCodes[normalized]; ok {
		return true
	}
	// The catch-all for decline reasons not classified as final, e.g.
	// rejected_due_to_inaccurate_onboarding_details.
	return strings.HasPrefix(normalized, "rejected_due_to_")
}

// bridgeSideCodes are requirement identifiers whose next move is Bridge's,
// not the user's: screenings not yet run, processing queues, and report
// matches awaiting compliance adjudication (a match is an input to review,
// not a verdict — concluded verdicts are covered by verdictCodes and the
// customer status).
var bridgeSideCodes = map[string]struct{}{
	"sanctions_screen":           {},
	"persona_sanctions_screen":   {},
	"post_processing":            {},
	"pending_rfi":                {},
	"adverse_media_report_match": {},
	"watchlist_report_match":     {},
}

// isPartnerSidePending reports whether an identifier's next move is Bridge's,
// not the user's: the bridge-side set, plus codes reporting a manual review in
// progress (e.g. "manual_government_id_review"). These ask nothing of the user
// — and resubmitting could restart a review — so they are excluded from
// requirements and surface as WAIT via next_step. Everything else stays
// user-actionable, including unrecognized codes. A concluded review reports
// differently ("manual_review_rejected"), so the suffix doesn't catch it.
func isPartnerSidePending(rawValue string) bool {
	normalized := strings.ToLower(rawValue)
	if _, ok := bridgeSideCodes[normalized]; ok {
		return true
	}
	return strings.HasSuffix(normalized, "_review")
}

// collectRawIdentifiers flattens a provider-defined requirements payload to
// its string leaves. Bridge's missing/issues shapes are underdocumented (a
// bare array of identifiers, grouping objects like {"all_of": [...]}, or
// field-correlation objects like {"id_front_photo": "id_expired"}, potentially
// nested), so any JSON is accepted and walked; map values are visited in
// sorted key order for deterministic output. Each leaf carries the nearest
// enclosing map key as its correlation — grouping keys resolve to no field,
// so they are harmless in that role.
func collectRawIdentifiers(raw json.RawMessage) []rawIdentifier {
	if len(raw) == 0 {
		return nil
	}
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil
	}

	var identifiers []rawIdentifier
	var walk func(node any, key string)
	walk = func(node any, key string) {
		switch v := node.(type) {
		case string:
			identifiers = append(identifiers, rawIdentifier{value: v, key: key})
		case []any:
			for _, item := range v {
				walk(item, key)
			}
		case map[string]any:
			keys := make([]string, 0, len(v))
			for k := range v {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				walk(v[k], k)
			}
		}
	}
	walk(parsed, "")
	return identifiers
}

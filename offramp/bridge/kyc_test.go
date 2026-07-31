package bridge

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	emailpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/email/v1"
	kycpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/v1"
	phonepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/phone/v1"
	thirdpartypb "github.com/code-payments/flipcash2-protobuf-api/generated/go/thirdparty/v1"
)

func TestKycStateFromCustomer_Verified(t *testing.T) {
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusActive,
		HasAcceptedTermsOfService: true,
		Endorsements: []Endorsement{{
			Name:   EndorsementBase,
			Status: EndorsementStatusApproved,
		}},
	})

	require.Equal(t, kycpb.KycStatus_ACTIVE, state.Status)
	require.Equal(t, kycpb.KycState_NONE, state.NextStep)
	require.Empty(t, state.Requirements)
	require.Equal(t, thirdpartypb.Partner_BRIDGE, state.Partner)
	require.Equal(t, kycpb.SubmissionType_INDIVIDUAL, state.SubmissionType)
	require.True(t, state.GetBridgeFeatures().CanOfframp)
}

func TestKycStateFromCustomer_UnderReview(t *testing.T) {
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusUnderReview,
		HasAcceptedTermsOfService: true,
		Endorsements: []Endorsement{{
			Name:   EndorsementBase,
			Status: EndorsementStatusIncomplete,
			Requirements: &EndorsementRequirements{
				Complete: []string{"first_name", "birth_date"},
				Pending:  []string{"id_verification"},
			},
		}},
	})

	require.Equal(t, kycpb.KycStatus_UNDER_REVIEW, state.Status)
	require.Equal(t, kycpb.KycState_WAIT, state.NextStep)
	require.False(t, state.GetBridgeFeatures().CanOfframp)

	// Pending checks are Bridge's work, not the user's: they are conveyed via
	// next_step, never as requirements.
	require.Empty(t, state.Requirements)
}

func TestKycStateFromCustomer_ActionNeeded(t *testing.T) {
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusIncomplete,
		HasAcceptedTermsOfService: true,
		Endorsements: []Endorsement{{
			Name:   EndorsementBase,
			Status: EndorsementStatusIncomplete,
			Requirements: &EndorsementRequirements{
				// A pending check alongside actionable requirements: anything
				// actionable trumps waiting.
				Pending: []string{"id_verification"},
				// Grouping-object shape for missing; issues mix bare
				// identifiers with field-correlation objects.
				Missing: json.RawMessage(`{"all_of": ["tax_identification_number", {"one_of": ["proof_of_address"]}]}`),
				Issues:  json.RawMessage(`["government_id_verification_failed", "poa_name_mismatch", "customer_too_young", "residence_address_invalid_postal_code", {"id_front_photo": "id_expired"}, {"birth_date": "implausible"}]`),
			},
		}},
	})

	require.Equal(t, kycpb.KycStatus_INCOMPLETE, state.Status)
	require.Equal(t, kycpb.KycState_PROVIDE_REQUIREMENTS, state.NextStep)
	require.False(t, state.GetBridgeFeatures().CanOfframp)

	type flattened struct {
		field kycpb.KycState_Requirement_Field
		kind  kycpb.KycState_Requirement_Kind
		raw   string
	}
	var got []flattened
	for _, r := range state.Requirements {
		got = append(got, flattened{r.Field, r.Kind, r.RawValue})
	}
	require.ElementsMatch(t, []flattened{
		{kycpb.KycState_Requirement_TAX_ID, kycpb.KycState_Requirement_MISSING, "tax_identification_number"},
		{kycpb.KycState_Requirement_PROOF_OF_ADDRESS, kycpb.KycState_Requirement_MISSING, "proof_of_address"},
		{kycpb.KycState_Requirement_ID_DOCUMENT, kycpb.KycState_Requirement_ISSUE, "government_id_verification_failed"},
		{kycpb.KycState_Requirement_PROOF_OF_ADDRESS, kycpb.KycState_Requirement_ISSUE, "poa_name_mismatch"},
		{kycpb.KycState_Requirement_BIRTH_DATE, kycpb.KycState_Requirement_ISSUE, "customer_too_young"},
		{kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS, kycpb.KycState_Requirement_ISSUE, "residence_address_invalid_postal_code"},
		// The identifier resolves the field directly.
		{kycpb.KycState_Requirement_ID_DOCUMENT, kycpb.KycState_Requirement_ISSUE, "id_expired"},
		// Only the correlating key resolves the field.
		{kycpb.KycState_Requirement_BIRTH_DATE, kycpb.KycState_Requirement_ISSUE, "implausible"},
	}, got)
}

func TestKycStateFromCustomer_Offboarded(t *testing.T) {
	// Offboarding is a permanent account closure: the only truly terminal
	// state.
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusOffboarded,
		HasAcceptedTermsOfService: true,
	})
	require.Equal(t, kycpb.KycStatus_OFFBOARDED, state.Status)
	require.Equal(t, kycpb.KycState_BLOCKED, state.NextStep)
	require.False(t, state.GetBridgeFeatures().CanOfframp)
	require.Empty(t, state.RejectionReasons)
}

func TestKycStateFromCustomer_Rejected(t *testing.T) {
	// Rejection is not terminal — Bridge accepts corrections — so the user is
	// directed to fix and resubmit even when Bridge enumerates no field-level
	// requirements. Customer-facing reasons are surfaced most recent first;
	// developer_reason is sensitive and never leaves the server.
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusRejected,
		HasAcceptedTermsOfService: true,
		RejectionReasons: []RejectionReason{
			{
				Reason:          "Your information could not be verified",
				DeveloperReason: "ID cannot be verified against third-party databases",
				CreatedAt:       "2026-02-19T19:01:59.529Z",
			},
			{
				Reason:          "Cannot validate ID -- upload a clear photo of the full ID",
				DeveloperReason: "Submission is blurry",
				CreatedAt:       "2026-03-01T08:30:00.000Z",
			},
		},
	})

	require.Equal(t, kycpb.KycStatus_REJECTED, state.Status)
	require.Equal(t, kycpb.KycState_PROVIDE_REQUIREMENTS, state.NextStep)
	require.False(t, state.GetBridgeFeatures().CanOfframp)

	require.Len(t, state.RejectionReasons, 2)
	require.Equal(t, "Cannot validate ID -- upload a clear photo of the full ID", state.RejectionReasons[0].Message)
	require.Equal(t, "Your information could not be verified", state.RejectionReasons[1].Message)
}

func TestKycStateFromCustomer_RejectedWithRequirements(t *testing.T) {
	// A rejection accompanied by endorsement issues points at the fields to
	// fix. Verdict codes report the decision itself — already conveyed by the
	// status and rejection_reasons — and are dropped.
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusRejected,
		HasAcceptedTermsOfService: true,
		Endorsements: []Endorsement{{
			Name:   EndorsementBase,
			Status: EndorsementStatusIncomplete,
			Requirements: &EndorsementRequirements{
				Issues: json.RawMessage(`["government_id_verification_failed", "manual_review_rejected", "rejected_due_to_inaccurate_onboarding_details"]`),
			},
		}},
		RejectionReasons: []RejectionReason{{
			Reason:    "Cannot validate ID -- upload a clear photo of the full ID",
			CreatedAt: "2026-03-01T08:30:00.000Z",
		}},
	})

	require.Equal(t, kycpb.KycStatus_REJECTED, state.Status)
	require.Equal(t, kycpb.KycState_PROVIDE_REQUIREMENTS, state.NextStep)

	require.Len(t, state.Requirements, 1)
	require.Equal(t, kycpb.KycState_Requirement_ID_DOCUMENT, state.Requirements[0].Field)
	require.Equal(t, kycpb.KycState_Requirement_ISSUE, state.Requirements[0].Kind)
	require.Len(t, state.RejectionReasons, 1)
}

func TestKycStateFromCustomer_RejectionReasonsNotRejected(t *testing.T) {
	// Rejection reasons are keyed on REJECTED status; stale reasons on a
	// recovered customer stay unsurfaced.
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusActive,
		HasAcceptedTermsOfService: true,
		Endorsements: []Endorsement{{
			Name:   EndorsementBase,
			Status: EndorsementStatusApproved,
		}},
		RejectionReasons: []RejectionReason{{
			Reason:    "Your information could not be verified",
			CreatedAt: "2026-02-19T19:01:59.529Z",
		}},
	})

	require.Equal(t, kycpb.KycState_NONE, state.NextStep)
	require.Empty(t, state.RejectionReasons)
}

func TestKycStateFromCustomer_TosReacceptance(t *testing.T) {
	// An otherwise verified customer needing agreement re-acceptance is
	// actionable, not done.
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusActive,
		HasAcceptedTermsOfService: false,
		Endorsements: []Endorsement{{
			Name:   EndorsementBase,
			Status: EndorsementStatusApproved,
		}},
	})

	require.Equal(t, kycpb.KycStatus_ACTIVE, state.Status)
	require.Equal(t, kycpb.KycState_PROVIDE_REQUIREMENTS, state.NextStep)

	require.Len(t, state.Requirements, 1)
	require.Equal(t, kycpb.KycState_Requirement_AGREEMENT, state.Requirements[0].Field)
	require.Equal(t, kycpb.KycState_Requirement_MISSING, state.Requirements[0].Kind)
}

func TestKycStateFromCustomer_TosReportedByEndorsement(t *testing.T) {
	// When the endorsement already enumerates the terms-of-service
	// requirement, the customer-level flag adds no synthetic duplicate.
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusIncomplete,
		HasAcceptedTermsOfService: false,
		Endorsements: []Endorsement{{
			Name:   EndorsementBase,
			Status: EndorsementStatusIncomplete,
			Requirements: &EndorsementRequirements{
				Missing: json.RawMessage(`["terms_of_service_v1"]`),
			},
		}},
	})

	require.Equal(t, kycpb.KycState_PROVIDE_REQUIREMENTS, state.NextStep)
	require.Len(t, state.Requirements, 1)
	require.Equal(t, kycpb.KycState_Requirement_AGREEMENT, state.Requirements[0].Field)
	require.Equal(t, "terms_of_service_v1", state.Requirements[0].RawValue)
}

func TestKycStateFromCustomer_ManualReview(t *testing.T) {
	// Review-marker issues report Bridge-side review in progress: they ask
	// nothing of the user and must not read as actionable.
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusUnderReview,
		HasAcceptedTermsOfService: true,
		Endorsements: []Endorsement{{
			Name:   EndorsementBase,
			Status: EndorsementStatusIncomplete,
			Requirements: &EndorsementRequirements{
				Issues: json.RawMessage(`["manual_government_id_review", "government_id_expiration_date_missing_manual_review"]`),
			},
		}},
	})

	require.Equal(t, kycpb.KycStatus_UNDER_REVIEW, state.Status)
	require.Equal(t, kycpb.KycState_WAIT, state.NextStep)
	require.Empty(t, state.Requirements)
}

func TestRequirementField(t *testing.T) {
	for rawValue, expected := range map[string]kycpb.KycState_Requirement_Field{
		"has_valid_national_id": kycpb.KycState_Requirement_ID_DOCUMENT,
		"id_not_expired":        kycpb.KycState_Requirement_ID_DOCUMENT,

		"min_age_18":         kycpb.KycState_Requirement_BIRTH_DATE,
		"min_age_60":         kycpb.KycState_Requirement_BIRTH_DATE,
		"customer_too_young": kycpb.KycState_Requirement_BIRTH_DATE,
		"customer_over_age":  kycpb.KycState_Requirement_BIRTH_DATE,

		"subdivision_not_ny_usa":                kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,
		"subdivision_not_ak_usa":                kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,
		"residence_address_invalid_city":        kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,
		"database_check_failed_on_street_name":  kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,
		"database_check_failed_on_postal_code":  kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,
		"database_check_failed_on_country_code": kycpb.KycState_Requirement_RESIDENTIAL_ADDRESS,

		"database_check_failed_on_name_last": kycpb.KycState_Requirement_LEGAL_NAME,

		"database_check_failed_on_social_security_number": kycpb.KycState_Requirement_TAX_ID,
		"database_check_failed_on_tin_validation":         kycpb.KycState_Requirement_TAX_ID,
		"tax_identification_number_not_compatible":        kycpb.KycState_Requirement_TAX_ID,

		"minimal_source_of_funds_data":        kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
		"source_of_funds_questionnaire":       kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
		"unemployed_using_salary":             kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
		"funds_sourced_pension_or_retirement": kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
		"high_expected_monthly_payments":      kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
		"suspicious_source_of_funds":          kycpb.KycState_Requirement_COMPLIANCE_PROFILE,
		"suspicious_primary_purpose":          kycpb.KycState_Requirement_COMPLIANCE_PROFILE,

		"terms_of_service_v2": kycpb.KycState_Requirement_AGREEMENT,

		// System-level codes have nothing to re-collect and stay unknown.
		"persona_sanctions_screen":                      kycpb.KycState_Requirement_FIELD_UNKNOWN,
		"blocklist_check_failed":                        kycpb.KycState_Requirement_FIELD_UNKNOWN,
		"manual_review_rejected":                        kycpb.KycState_Requirement_FIELD_UNKNOWN,
		"adverse_media_report_match":                    kycpb.KycState_Requirement_FIELD_UNKNOWN,
		"watchlist_report_match":                        kycpb.KycState_Requirement_FIELD_UNKNOWN,
		"customer_not_compatible":                       kycpb.KycState_Requirement_FIELD_UNKNOWN,
		"endorsement_not_available_in_customers_region": kycpb.KycState_Requirement_FIELD_UNKNOWN,
		"rejected_due_to_unsupported_geo":               kycpb.KycState_Requirement_FIELD_UNKNOWN,
		"has_base":                                      kycpb.KycState_Requirement_FIELD_UNKNOWN,
		"pending_rfi":                                   kycpb.KycState_Requirement_FIELD_UNKNOWN,
	} {
		require.Equal(t, expected, requirementField(rawValue), rawValue)
	}
}

func TestIsPartnerSidePending(t *testing.T) {
	for rawValue, pending := range map[string]bool{
		// Manual review in progress.
		"manual_government_id_review":   true,
		"manual_database_lookup_review": true,
		"government_id_expiration_date_missing_manual_review": true,
		// Bridge-side work, wherever it is reported.
		"sanctions_screen":           true,
		"persona_sanctions_screen":   true,
		"post_processing":            true,
		"pending_rfi":                true,
		"adverse_media_report_match": true,
		"watchlist_report_match":     true,
		// Verdicts and actionable codes stay user-facing.
		"manual_review_rejected":            false,
		"blocklist_check_failed":            false,
		"government_id_verification_failed": false,
		"poa_manipulated":                   false,
		"tax_identification_number":         false,
	} {
		require.Equal(t, pending, isPartnerSidePending(rawValue), rawValue)
	}
}

func TestKycStateFromCustomer_BridgeSideWork(t *testing.T) {
	// Screening and adjudication are Bridge's work, wherever they are
	// reported: they are excluded from requirements, and the user waits.
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusUnderReview,
		HasAcceptedTermsOfService: true,
		Endorsements: []Endorsement{{
			Name:   EndorsementBase,
			Status: EndorsementStatusIncomplete,
			Requirements: &EndorsementRequirements{
				Missing: json.RawMessage(`{"all_of": ["persona_sanctions_screen", "post_processing"]}`),
				Issues:  json.RawMessage(`["watchlist_report_match"]`),
			},
		}},
	})

	require.Equal(t, kycpb.KycStatus_UNDER_REVIEW, state.Status)
	require.Equal(t, kycpb.KycState_WAIT, state.NextStep)
	require.Empty(t, state.Requirements)
}

func TestKycStateFromCustomer_BlockingCodes(t *testing.T) {
	// Final verdicts — no resubmission can resolve them — block verification
	// outright, trumping any actionable requirements, and never surface as
	// requirements themselves.
	for _, code := range []string{
		"developer_not_compatible",
		"blocklist_check_failed",
		"customer_not_compatible",
		"endorsement_not_available_in_customers_region",
		"rejected_due_to_unsupported_geo",
	} {
		t.Run(code, func(t *testing.T) {
			state := KycStateFromCustomer(&Customer{
				Status:                    CustomerStatusRejected,
				HasAcceptedTermsOfService: true,
				Endorsements: []Endorsement{{
					Name:   EndorsementBase,
					Status: EndorsementStatusIncomplete,
					Requirements: &EndorsementRequirements{
						Issues: json.RawMessage(`["` + code + `", "government_id_verification_failed"]`),
					},
				}},
			})

			require.Equal(t, kycpb.KycState_BLOCKED, state.NextStep)
			require.Len(t, state.Requirements, 1)
			require.Equal(t, kycpb.KycState_Requirement_ID_DOCUMENT, state.Requirements[0].Field)
		})
	}
}

func TestKycStateFromCustomer_AwaitingQuestionnaire(t *testing.T) {
	// The questionnaire is the customer's to answer: the synthetic requirement
	// keeps the state actionable even when the endorsement requirements don't
	// enumerate the individual questions.
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusAwaitingQuestionnaire,
		HasAcceptedTermsOfService: true,
		Endorsements: []Endorsement{{
			Name:   EndorsementBase,
			Status: EndorsementStatusIncomplete,
		}},
	})

	require.Equal(t, kycpb.KycStatus_INCOMPLETE, state.Status)
	require.Equal(t, kycpb.KycState_PROVIDE_REQUIREMENTS, state.NextStep)
	require.False(t, state.GetBridgeFeatures().CanOfframp)

	require.Len(t, state.Requirements, 1)
	require.Equal(t, kycpb.KycState_Requirement_COMPLIANCE_PROFILE, state.Requirements[0].Field)
	require.Equal(t, kycpb.KycState_Requirement_MISSING, state.Requirements[0].Kind)
	require.Equal(t, "awaiting_questionnaire", state.Requirements[0].RawValue)
}

func TestKycStateFromCustomer_AwaitingQuestionnaire_ReportedRequirements(t *testing.T) {
	// No synthetic requirement when the endorsement already reports the
	// questionnaire fields.
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusAwaitingQuestionnaire,
		HasAcceptedTermsOfService: true,
		Endorsements: []Endorsement{{
			Name:   EndorsementBase,
			Status: EndorsementStatusIncomplete,
			Requirements: &EndorsementRequirements{
				Missing: json.RawMessage(`["source_of_funds", "account_purpose"]`),
			},
		}},
	})

	require.Equal(t, kycpb.KycStatus_INCOMPLETE, state.Status)
	require.Equal(t, kycpb.KycState_PROVIDE_REQUIREMENTS, state.NextStep)

	require.Len(t, state.Requirements, 2)
	for _, requirement := range state.Requirements {
		require.Equal(t, kycpb.KycState_Requirement_COMPLIANCE_PROFILE, requirement.Field)
	}
}

func TestKycStateFromCustomer_AwaitingUBO(t *testing.T) {
	// Business-only, but mapped defensively.
	state := KycStateFromCustomer(&Customer{
		Status:                    CustomerStatusAwaitingUBO,
		HasAcceptedTermsOfService: true,
	})
	require.Equal(t, kycpb.KycStatus_INCOMPLETE, state.Status)
	require.False(t, state.GetBridgeFeatures().CanOfframp)
}

func TestKycStateFromCustomer_UnknownValues(t *testing.T) {
	state := KycStateFromCustomer(&Customer{
		Status:                    "some_new_status",
		HasAcceptedTermsOfService: true,
		Endorsements: []Endorsement{{
			Name:   EndorsementBase,
			Status: EndorsementStatusIncomplete,
			Requirements: &EndorsementRequirements{
				Missing: json.RawMessage(`["some_new_requirement"]`),
			},
		}},
	})

	// Unknown vocabulary fails safe: without a mapping update we don't know
	// what to ask the user for, so an unrecognized requirement blocks rather
	// than masquerading as actionable.
	require.Equal(t, kycpb.KycStatus_KYC_STATUS_UNKNOWN, state.Status)
	require.Equal(t, kycpb.KycState_BLOCKED, state.NextStep)
	require.Empty(t, state.Requirements)
}

func TestCollectRawIdentifiers(t *testing.T) {
	for _, tc := range []struct {
		name     string
		raw      string
		expected []rawIdentifier
	}{
		{"empty", "", nil},
		{"null", "null", nil},
		{"bare array", `["a", "b"]`, []rawIdentifier{{value: "a"}, {value: "b"}}},
		{"grouping object", `{"all_of": ["a", "b"]}`,
			[]rawIdentifier{{value: "a", key: "all_of"}, {value: "b", key: "all_of"}}},
		{"nested groups", `{"all_of": ["a", {"one_of": ["b", "c"]}]}`,
			[]rawIdentifier{{value: "a", key: "all_of"}, {value: "b", key: "one_of"}, {value: "c", key: "one_of"}}},
		{"field correlation", `[{"id_front_photo": "id_expired"}]`,
			[]rawIdentifier{{value: "id_expired", key: "id_front_photo"}}},
		{"bare string", `"a"`, []rawIdentifier{{value: "a"}}},
		{"non-string leaves dropped", `[1, true, "a"]`, []rawIdentifier{{value: "a"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, collectRawIdentifiers(json.RawMessage(tc.raw)))
		})
	}
}

func TestAlpha3(t *testing.T) {
	for _, tc := range []struct {
		alpha2   string
		expected string
	}{
		{"US", "USA"},
		{"GB", "GBR"},
		{"DE", "DEU"},
	} {
		converted, err := alpha3(&commonpb.CountryCode{Value: tc.alpha2})
		require.NoError(t, err)
		require.Equal(t, tc.expected, converted)
	}

	_, err := alpha3(&commonpb.CountryCode{Value: "xx"})
	require.Error(t, err)
}

func TestCreateCustomerRequest_OptionalFields(t *testing.T) {
	userID := "client-ref-1234"
	submission := &kycpb.IndividualKycSubmission{
		Name:      &kycpb.LegalName{FirstName: "Jane", MiddleName: "Q", LastName: "Doe"},
		BirthDate: &commonpb.Date{Year: 1990, Month: 4, Day: 2},
		Address: &kycpb.Address{
			StreetLine_1: "123 Main St",
			StreetLine_2: "Apt 4",
			City:         "San Francisco",
			Subdivision:  "CA",
			PostalCode:   "94105",
			Country:      &commonpb.CountryCode{Value: "US"},
		},
		TaxId: &kycpb.TaxId{Type: kycpb.TaxId_US_ITIN, Value: "912345678"},
		IdentityDocuments: []*kycpb.IdentityDocument{{
			Type:           kycpb.IdentityDocument_PASSPORT,
			FrontImage:     []byte("passport-image"),
			MimeType:       "image/png",
			IssuingCountry: &commonpb.CountryCode{Value: "GB"},
			DocumentNumber: "P1234567",
			ExpirationDate: &commonpb.Date{Year: 2030, Month: 1, Day: 15},
		}},
		Email: &emailpb.EmailAddress{Value: "jane@example.com"},
		Phone: &phonepb.PhoneNumber{Value: "+12025550123"},
		SupportingDocuments: []*kycpb.SupportingDocument{{
			Purposes: []kycpb.SupportingDocument_Purpose{
				kycpb.SupportingDocument_PROOF_OF_ADDRESS,
				kycpb.SupportingDocument_PURPOSE_OTHER,
			},
			File:        []byte("%PDF-utility-bill"),
			MimeType:    "application/pdf",
			Description: "utility bill",
		}},
		SignedAgreementTokens: []*kycpb.SignedAgreementToken{{Value: "signed-agreement-id"}},
	}

	req, err := CustomerRequestFromProtoSubmission(userID, submission)
	require.NoError(t, err)

	require.Equal(t, "Q", req.MiddleName)
	require.Equal(t, "Apt 4", req.ResidentialAddress.StreetLine2)

	require.Len(t, req.IdentifyingInformation, 2)
	require.Equal(t, IDTypeITIN, req.IdentifyingInformation[0].Type)
	passport := req.IdentifyingInformation[1]
	require.Equal(t, IDTypePassport, passport.Type)
	require.Equal(t, "gbr", passport.IssuingCountry)
	require.Equal(t, "P1234567", passport.Number)
	require.Equal(t, "2030-01-15", passport.Expiration)
	require.Equal(t, "data:image/png;base64,"+base64.StdEncoding.EncodeToString([]byte("passport-image")), passport.ImageFront)
	require.Empty(t, passport.ImageBack)

	require.Len(t, req.Documents, 1)
	require.Equal(t, []string{"proof_of_address", "other"}, req.Documents[0].Purposes)
	require.Equal(t, "utility bill", req.Documents[0].Description)
	require.Equal(t, "data:application/pdf;base64,"+base64.StdEncoding.EncodeToString([]byte("%PDF-utility-bill")), req.Documents[0].File)

	// No compliance profile: the questionnaire fields stay unset.
	require.Empty(t, req.AccountPurpose)
	require.Nil(t, req.ActingAsIntermediary)
}

func TestCreateCustomerRequest_MissingAgreementToken(t *testing.T) {
	_, err := CustomerRequestFromProtoSubmission("client-ref-1234", &kycpb.IndividualKycSubmission{})
	require.Error(t, err)
}

func TestIsEmptyUpdate(t *testing.T) {
	require.True(t, IsEmptyUpdate(&kycpb.IndividualKycUpdate{}))

	for name, update := range map[string]*kycpb.IndividualKycUpdate{
		"name":       {Name: &kycpb.LegalName{FirstName: "Jane", LastName: "Doe"}},
		"birth date": {BirthDate: &commonpb.Date{Year: 1990, Month: 4, Day: 2}},
		"documents":  {IdentityDocuments: []*kycpb.IdentityDocument{{}}},
		"email":      {Email: &emailpb.EmailAddress{Value: "jane@example.com"}},
		"phone":      {Phone: &phonepb.PhoneNumber{Value: "+12025550123"}},
		"agreement":  {SignedAgreementTokens: []*kycpb.SignedAgreementToken{{Value: "token"}}},
		"compliance": {Compliance: &kycpb.IndividualKycUpdate_BridgeCompliance{BridgeCompliance: &kycpb.BridgeComplianceProfile{}}},
	} {
		t.Run(name, func(t *testing.T) {
			require.False(t, IsEmptyUpdate(update))
		})
	}
}

func TestUpdateCustomerRequestFromProtoUpdate(t *testing.T) {
	req, err := UpdateCustomerRequestFromProtoUpdate(&kycpb.IndividualKycUpdate{
		Name:      &kycpb.LegalName{FirstName: "Jane", LastName: "Doe"},
		BirthDate: &commonpb.Date{Year: 1990, Month: 4, Day: 2},
		Address: &kycpb.Address{
			StreetLine_1: "456 Oak Ave",
			City:         "Oakland",
			Subdivision:  "CA",
			PostalCode:   "94601",
			Country:      &commonpb.CountryCode{Value: "US"},
		},
		TaxId: &kycpb.TaxId{Type: kycpb.TaxId_US_SSN, Value: "123456789"},
		IdentityDocuments: []*kycpb.IdentityDocument{{
			Type:           kycpb.IdentityDocument_STATE_ID,
			FrontImage:     []byte("retaken"),
			MimeType:       "image/jpeg",
			IssuingCountry: &commonpb.CountryCode{Value: "US"},
		}},
		Email:                 &emailpb.EmailAddress{Value: "jane@example.com"},
		Phone:                 &phonepb.PhoneNumber{Value: "+12025550123"},
		SignedAgreementTokens: []*kycpb.SignedAgreementToken{{Value: "new-agreement"}},
	})
	require.NoError(t, err)

	require.Equal(t, "Jane", req.FirstName)
	require.Equal(t, "Doe", req.LastName)
	require.Equal(t, "1990-04-02", req.BirthDate)
	require.Equal(t, "jane@example.com", req.Email)
	require.Equal(t, "+12025550123", req.Phone)
	require.NotNil(t, req.ResidentialAddress)
	require.Equal(t, "USA", req.ResidentialAddress.Country)

	require.Len(t, req.IdentifyingInformation, 2)
	require.Equal(t, IDTypeSSN, req.IdentifyingInformation[0].Type)
	require.Equal(t, IDTypeStateID, req.IdentifyingInformation[1].Type)

	require.Equal(t, "new-agreement", req.SignedAgreementID)
}

func TestUpdateCustomerRequestFromProtoUpdate_Partial(t *testing.T) {
	// A document-only retake carries nothing else.
	req, err := UpdateCustomerRequestFromProtoUpdate(&kycpb.IndividualKycUpdate{
		IdentityDocuments: []*kycpb.IdentityDocument{{
			Type:           kycpb.IdentityDocument_DRIVERS_LICENSE,
			FrontImage:     []byte("retaken"),
			MimeType:       "image/jpeg",
			IssuingCountry: &commonpb.CountryCode{Value: "US"},
		}},
	})
	require.NoError(t, err)

	require.Empty(t, req.FirstName)
	require.Empty(t, req.BirthDate)
	require.Nil(t, req.ResidentialAddress)
	require.Empty(t, req.SignedAgreementID)
	require.Nil(t, req.ActingAsIntermediary)
	require.Len(t, req.IdentifyingInformation, 1)
}

func TestCustomerIdempotencyKey(t *testing.T) {
	submission := &kycpb.IndividualKycSubmission{
		Name: &kycpb.LegalName{FirstName: "Jane", LastName: "Doe"},
	}

	// Deterministic for the same payload; distinct across payloads and users.
	require.Equal(t, CustomerIdempotencyKey("user-1", submission), CustomerIdempotencyKey("user-1", submission))
	require.NotEqual(t, CustomerIdempotencyKey("user-1", submission), CustomerIdempotencyKey("user-2", submission))
	require.NotEqual(t,
		CustomerIdempotencyKey("user-1", submission),
		CustomerIdempotencyKey("user-1", &kycpb.IndividualKycSubmission{
			Name: &kycpb.LegalName{FirstName: "Janet", LastName: "Doe"},
		}))
}


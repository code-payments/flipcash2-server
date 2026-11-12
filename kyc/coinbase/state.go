package coinbase

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	kyccoinbasepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/kyc/coinbase/v1"

	"github.com/code-payments/flipcash2-server/cdp/customers"
)

// maxRequirements is the most requirements a KycState may list (its
// validation rule).
const maxRequirements = 64

// maxTermsLanguages is the most languages a TermsOfService may list (its
// validation rule).
const maxTermsLanguages = 64

// fieldOfKey maps each identity requirement Coinbase names to the field of
// Individual that meets it. Coinbase asks for some fields in parts, which all
// map to one Field. Any key not here (ssnLast4, citizenship, employmentStatus,
// occupation, expectedVolume, or one Coinbase adds) is something this API
// cannot collect, and is listed as FIELD_UNKNOWN, which tells a client to ask
// the user to update the app.
var fieldOfKey = map[customers.RequirementKey]kyccoinbasepb.Field{
	customers.RequirementFirstName:          kyccoinbasepb.Field_FIELD_LEGAL_NAME,
	customers.RequirementLastName:           kyccoinbasepb.Field_FIELD_LEGAL_NAME,
	customers.RequirementDateOfBirth:        kyccoinbasepb.Field_FIELD_DATE_OF_BIRTH,
	customers.RequirementAddressLine1:       kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
	customers.RequirementAddressLine2:       kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
	customers.RequirementAddressCity:        kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
	customers.RequirementAddressSubdivision: kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
	customers.RequirementAddressPostalCode:  kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
	customers.RequirementAddressCountryCode: kyccoinbasepb.Field_FIELD_RESIDENTIAL_ADDRESS,
	customers.RequirementFullSsn:            kyccoinbasepb.Field_FIELD_SSN,
	customers.RequirementEmail:              kyccoinbasepb.Field_FIELD_EMAIL,
	customers.RequirementPhoneNumber:        kyccoinbasepb.Field_FIELD_PHONE_NUMBER,
	customers.RequirementPurposeOfAccount:   kyccoinbasepb.Field_FIELD_PURPOSE_OF_ACCOUNT,
	customers.RequirementSourceOfFunds:      kyccoinbasepb.Field_FIELD_SOURCE_OF_FUNDS,
}

// deriveState derives a user's KycState for product from their Coinbase
// customer (nil when the user has none).
//
// notes says what Coinbase returned that this server does not model, for the
// caller to log. Each names only Coinbase's own identifiers (capability names
// and statuses, requirement keys and statuses, tax forms, terms version IDs
// and languages), never anything the user submitted, so each is safe to log.
//
// err is returned only for a product with no capability set.
//
// The status is the product's capability set S seen as one, as KycState's
// comment says, judged in this order:
//
//   - BLOCKED when any capability in S is inactive, with nothing listed,
//     whether or not the rest of S has been requested. Coinbase keeps
//     listing tos and taxAttestation as due on a blocked customer (observed
//     in the Sandbox), but the proto promises a blocked user an empty list,
//     since nothing they submit can change the outcome.
//   - NOT_STARTED until every capability in S is requested (no customer, or
//     a capability missing from the map, reads as not requested), with
//     nothing listed: StartKyc starts it.
//   - VERIFIED when every capability in S is active, whatever is listed (a
//     newer version of the terms, say, with a deadline).
//   - Otherwise ACTION_REQUIRED while something is listed and IN_REVIEW while
//     nothing is.
//
// A block is judged before NOT_STARTED because a blocked customer can never
// be started: Coinbase refuses every update to a customer with an inactive
// capability (403 forbidden), a request for the rest of S included, and
// leaves the capabilities never requested as unrequested (both observed in
// the Sandbox). Read as NOT_STARTED, such a user would be sent to a StartKyc
// that fails every time, and never be told they are blocked. With one
// product, whose set StartKyc requests whole, the shape arises only once the
// set grows.
//
// A block that lies wholly outside S (none of S inactive) does not block the
// product, though Coinbase would refuse an update that starts it; no customer
// has that shape while there is one product.
//
// A capability status this server does not know is neither active nor
// inactive, so it reads as pending, and is noted.
//
// What is listed is described on listRequirements.
func deriveState(product kyccoinbasepb.Product, customer *customers.Customer) (state *kyccoinbasepb.KycState, notes []string, err error) {
	set, ok := productCapabilities(product)
	if !ok {
		return nil, nil, fmt.Errorf("product %d has no capability set", product)
	}

	state = &kyccoinbasepb.KycState{
		Product: product,
		Status:  kyccoinbasepb.KycState_NOT_STARTED,
	}
	if customer == nil {
		return state, nil, nil
	}

	allRequested, anyInactive := true, false
	for _, name := range set {
		capability := customer.Capabilities[name]
		if !capability.Requested {
			allRequested = false
		}
		if capability.Status == customers.CapabilityInactive {
			anyInactive = true
		}
	}
	if !anyInactive && !allRequested {
		return state, nil, nil
	}

	allActive := true
	for _, name := range set {
		capability := customer.Capabilities[name]
		if !capability.Requested {
			// Only a blocked customer gets here with part of the set not
			// requested, and that part's status says nothing more.
			allActive = false
			continue
		}
		switch status := capability.Status; status {
		case customers.CapabilityActive:
		case customers.CapabilityInactive, customers.CapabilityPending, customers.CapabilityUnrequested:
			allActive = false
		default:
			allActive = false
			notes = append(notes, fmt.Sprintf("capability %s has status %s, which this server does not know; read as pending", quote(string(name)), quote(string(status))))
		}
	}
	if anyInactive {
		state.Status = kyccoinbasepb.KycState_BLOCKED
		return state, notes, nil
	}

	requirements, requirementNotes := listRequirements(set, customer.Requirements)
	notes = append(notes, requirementNotes...)
	state.Requirements = requirements
	switch {
	case allActive:
		state.Status = kyccoinbasepb.KycState_VERIFIED
	case len(requirements) > 0:
		state.Status = kyccoinbasepb.KycState_ACTION_REQUIRED
	default:
		state.Status = kyccoinbasepb.KycState_IN_REVIEW
	}
	return state, notes, nil
}

// listing is one requirement being assembled from the parts Coinbase lists
// for it.
type listing struct {
	reason   kyccoinbasepb.Requirement_Reason
	deadline *time.Time
}

// merge folds one part into l: the reason is REJECTED if any part is, else
// MISSING if any is, else UNKNOWN, and the deadline is the earliest any part
// has.
func (l *listing) merge(reason kyccoinbasepb.Requirement_Reason, deadline *time.Time) {
	if reasonRank(reason) > reasonRank(l.reason) {
		l.reason = reason
	}
	if deadline != nil && (l.deadline == nil || deadline.Before(*l.deadline)) {
		d := *deadline
		l.deadline = &d
	}
}

func reasonRank(r kyccoinbasepb.Requirement_Reason) int {
	switch r {
	case kyccoinbasepb.Requirement_REJECTED:
		return 2
	case kyccoinbasepb.Requirement_MISSING:
		return 1
	}
	return 0
}

func (l *listing) deadlineProto() *timestamppb.Timestamp {
	if l.deadline == nil {
		return nil
	}
	return timestamppb.New(*l.deadline)
}

// listRequirements lists what the user has to provide for the capability set
// S, from Coinbase's requirements map.
//
// A requirement counts for S when its impact is empty or names a capability
// in S; one whose impact lies wholly outside S is another product's and is not
// listed. Its status decides its reason: due and past_due are MISSING,
// rejected is REJECTED, and pending is not listed, since Coinbase is
// verifying it. Any other status is listed with reason UNKNOWN and noted,
// never dropped: a requirement left out could leave a user IN_REVIEW for good.
//
// Identity keys become Fields (fieldOfKey), each Field listed once with its
// parts merged (listing.merge). tos becomes one TermsOfService per version,
// and taxAttestation one TaxForm per form (see addTerms and addTaxForms).
//
// The order is fixed, so the same customer always reads the same: Fields by
// enum number with FIELD_UNKNOWN last, then terms in Coinbase's order, then
// tax forms likewise by enum number with TAX_FORM_UNKNOWN last. The list
// never passes the proto's cap; if it would, terms are cut, since there are at
// most nine Fields and two tax forms, and the cut is noted.
func listRequirements(set []customers.CapabilityName, requirements map[customers.RequirementKey]customers.Requirement) ([]*kyccoinbasepb.Requirement, []string) {
	var notes []string
	fields := map[kyccoinbasepb.Field]*listing{}
	forms := map[kyccoinbasepb.TaxForm]*listing{}
	var terms []*kyccoinbasepb.Requirement

	addField := func(field kyccoinbasepb.Field, reason kyccoinbasepb.Requirement_Reason, deadline *time.Time) {
		l, ok := fields[field]
		if !ok {
			l = &listing{reason: reason}
			fields[field] = l
		}
		l.merge(reason, deadline)
	}
	addForm := func(form kyccoinbasepb.TaxForm, reason kyccoinbasepb.Requirement_Reason, deadline *time.Time) {
		l, ok := forms[form]
		if !ok {
			l = &listing{reason: reason}
			forms[form] = l
		}
		l.merge(reason, deadline)
	}

	// Map order is random; walking the keys sorted keeps the notes in a
	// fixed order. The listing itself does not depend on it.
	keys := make([]customers.RequirementKey, 0, len(requirements))
	for key := range requirements {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	for _, key := range keys {
		r := requirements[key]
		if !impactsSet(r.Impact, set) {
			continue
		}

		var reason kyccoinbasepb.Requirement_Reason
		switch r.Status {
		case customers.RequirementDue, customers.RequirementPastDue:
			reason = kyccoinbasepb.Requirement_MISSING
		case customers.RequirementRejected:
			reason = kyccoinbasepb.Requirement_REJECTED
		case customers.RequirementPending:
			continue
		default:
			reason = kyccoinbasepb.Requirement_UNKNOWN
			notes = append(notes, fmt.Sprintf("requirement %s has status %s, which this server does not know; listed with reason UNKNOWN", quote(string(key)), quote(string(r.Status))))
		}

		switch key {
		case customers.RequirementTos:
			var unrepresentable bool
			var termsNotes []string
			terms, unrepresentable, termsNotes = addTerms(terms, r, reason)
			notes = append(notes, termsNotes...)
			if unrepresentable {
				addField(kyccoinbasepb.Field_FIELD_UNKNOWN, reason, r.Deadline)
			}
		case customers.RequirementTaxAttestation:
			if len(r.TaxForms) == 0 {
				notes = append(notes, "requirement \"taxAttestation\" names no tax form; listed as TAX_FORM_UNKNOWN")
				addForm(kyccoinbasepb.TaxForm_TAX_FORM_UNKNOWN, reason, r.Deadline)
				continue
			}
			for _, form := range r.TaxForms {
				switch form {
				case customers.TaxFormUsW9:
					addForm(kyccoinbasepb.TaxForm_TAX_FORM_US_W9, reason, r.Deadline)
				default:
					notes = append(notes, fmt.Sprintf("tax form %s is not one this server knows; listed as TAX_FORM_UNKNOWN", quote(string(form))))
					addForm(kyccoinbasepb.TaxForm_TAX_FORM_UNKNOWN, reason, r.Deadline)
				}
			}
		default:
			field, ok := fieldOfKey[key]
			if !ok {
				field = kyccoinbasepb.Field_FIELD_UNKNOWN
				notes = append(notes, fmt.Sprintf("requirement %s is not one this API can collect; listed as FIELD_UNKNOWN", quote(string(key))))
			}
			addField(field, reason, r.Deadline)
		}
	}

	var out []*kyccoinbasepb.Requirement
	for _, field := range sortedLast(fields, kyccoinbasepb.Field_FIELD_UNKNOWN) {
		l := fields[field]
		out = append(out, &kyccoinbasepb.Requirement{
			Kind:     &kyccoinbasepb.Requirement_Field{Field: field},
			Reason:   l.reason,
			Deadline: l.deadlineProto(),
		})
	}
	var formsOut []*kyccoinbasepb.Requirement
	for _, form := range sortedLast(forms, kyccoinbasepb.TaxForm_TAX_FORM_UNKNOWN) {
		l := forms[form]
		formsOut = append(formsOut, &kyccoinbasepb.Requirement{
			Kind:     &kyccoinbasepb.Requirement_TaxForm{TaxForm: form},
			Reason:   l.reason,
			Deadline: l.deadlineProto(),
		})
	}
	if room := maxRequirements - len(out) - len(formsOut); len(terms) > room {
		notes = append(notes, fmt.Sprintf("%d terms of service versions listed, more than fit; the last %d are left out", len(terms), len(terms)-room))
		terms = terms[:room]
	}
	out = append(out, terms...)
	out = append(out, formsOut...)
	return out, notes
}

// addTerms appends one requirement per Terms of Service version r lists, each
// version once (the first of a version ID wins), its deadline the version's
// own. A version Coinbase gives none is listed with none, since Coinbase
// requires such a version at once; the requirement's deadline is the earliest
// any of its versions has, and so is not that version's.
//
// A requirement must pass the proto's validation, so a language that does not
// fit commonpb.Locale, or past the first 64, is dropped and noted. A version
// that still cannot be represented (an ID or url out of bounds, no usable
// language) is not listed as terms: unrepresentable is set instead, and the
// caller lists FIELD_UNKNOWN, which tells the client to ask the user to update
// the app, rather than drop a requirement the user could never meet. So does
// a tos requirement naming no version at all.
func addTerms(terms []*kyccoinbasepb.Requirement, r customers.Requirement, reason kyccoinbasepb.Requirement_Reason) (out []*kyccoinbasepb.Requirement, unrepresentable bool, notes []string) {
	if len(r.TosVersions) == 0 {
		return terms, true, []string{"requirement \"tos\" names no terms of service version; listed as FIELD_UNKNOWN"}
	}
	seen := map[string]bool{}
	for _, v := range r.TosVersions {
		if seen[v.VersionID] {
			continue
		}
		seen[v.VersionID] = true

		var languages []*commonpb.Locale
		for _, language := range v.Languages {
			locale := &commonpb.Locale{Value: language}
			if locale.Validate() != nil {
				notes = append(notes, fmt.Sprintf("terms of service version %s lists language %s, which is not a locale; left out", quote(v.VersionID), quote(language)))
				continue
			}
			if len(languages) == maxTermsLanguages {
				notes = append(notes, fmt.Sprintf("terms of service version %s lists language %s past the first %d; left out", quote(v.VersionID), quote(language), maxTermsLanguages))
				continue
			}
			languages = append(languages, locale)
		}

		tos := &kyccoinbasepb.TermsOfService{
			VersionId: v.VersionID,
			Url:       v.URL,
			Languages: languages,
		}
		if tos.ValidateAll() != nil {
			notes = append(notes, fmt.Sprintf("terms of service version %s cannot be represented; listed as FIELD_UNKNOWN", quote(v.VersionID)))
			unrepresentable = true
			continue
		}

		var ts *timestamppb.Timestamp
		if v.Deadline != nil {
			ts = timestamppb.New(*v.Deadline)
		}
		terms = append(terms, &kyccoinbasepb.Requirement{
			Kind:     &kyccoinbasepb.Requirement_TermsOfService{TermsOfService: tos},
			Reason:   reason,
			Deadline: ts,
		})
	}
	return terms, unrepresentable, notes
}

// impactsSet reports whether a requirement with the given impact counts for
// the capability set: an empty impact counts for every set, and otherwise it
// must name a capability in it.
func impactsSet(impact []customers.CapabilityName, set []customers.CapabilityName) bool {
	if len(impact) == 0 {
		return true
	}
	for _, name := range impact {
		if slices.Contains(set, name) {
			return true
		}
	}
	return false
}

// sortedLast returns m's keys by enum number, with last (the enum's UNKNOWN)
// after the rest.
func sortedLast[K ~int32, V any](m map[K]V, last K) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if (keys[i] == last) != (keys[j] == last) {
			return keys[j] == last
		}
		return keys[i] < keys[j]
	})
	return keys
}

// maxQuoted bounds how much of a value from Coinbase a note quotes.
const maxQuoted = 64

// quote quotes a value Coinbase returned for a note, cut to maxQuoted bytes so
// that no note grows without bound.
func quote(s string) string {
	if len(s) > maxQuoted {
		return fmt.Sprintf("%q...", s[:maxQuoted])
	}
	return fmt.Sprintf("%q", s)
}

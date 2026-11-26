package plaid

import (
	"context"
	"fmt"
	"net/url"
	"time"
	"unicode/utf8"
)

// Flipcash's Link policy. It is fixed here rather than chosen per call, so
// every link token Flipcash creates asks for the same thing.
//
// A new link (CreateLinkToken):
//
//   - US banks only, in English;
//   - products ["auth"], with "identity" consented to up front
//     (additional_consented_products) so it can be added later without
//     sending the user back through Link;
//   - depository checking and savings accounts only (account_filters);
//   - accounts verified only by the user signing in to their bank: Instant
//     Auth, and Instant Match (sign-in plus account and routing numbers,
//     matched against the mask the bank returns). Instant Auth has no
//     switch: it is Link's default flow. Every flow that verifies without a
//     sign-in, or by deposits, is off: Auth Type Select, Automated
//     Micro-deposits, Instant Micro-deposits, Same-Day Micro-deposits, and
//     Database Match / Database Insights. Reroute to Credentials is FORCED,
//     so a user who reaches a manual entry point at a bank that supports
//     sign-in is sent to sign in.
//
// Two of those are not fully in this request's hands. Plaid documents that
// when the Dashboard's Account Verification pane is enabled, its settings
// override this auth object; and Database Match / Database Insights are
// deprecated in favour of Database Auth, which is switched on only in that
// pane. So the Plaid Dashboard must leave Database Auth and the
// micro-deposit flows off, and should not manage Auth through that pane.
//
// Update mode (CreateUpdateLinkToken) sends no products and no auth object
// (Plaid says update mode takes no product-specific parameters), sets
// update.account_selection_enabled so the user may add or untick accounts,
// and keeps the same account_filters, which Plaid accepts in update mode
// only with account selection on. It sends no webhook: Plaid ignores it in
// update mode, since an Item's webhook is changed with /item/webhook/update.
var (
	linkCountryCodes        = []string{"US"}
	linkProducts            = []string{ProductAuth}
	linkConsentedProducts   = []string{ProductIdentity}
	linkDepositorySubtypes  = []string{AccountSubtypeChecking, AccountSubtypeSavings}
	linkRerouteToCredential = "FORCED"
)

const linkLanguage = "en"

// maxClientNameLength is Plaid's limit on client_name; longer names show as
// "This Application".
const maxClientNameLength = 30

// Platform is the platform the app opening Link runs on. It decides how a
// bank that signs the user in through its own app or website (OAuth) sends
// them back to Link.
type Platform int

const (
	// PlatformIOS sends LinkConfig.RedirectURI, a universal link the iOS
	// app claims.
	PlatformIOS Platform = iota + 1
	// PlatformAndroid sends LinkConfig.AndroidPackageName; Plaid's SDK
	// handles the return itself.
	PlatformAndroid
)

// LinkConfig is the deployment's Link settings.
type LinkConfig struct {
	// ClientName is the app's name as Link shows it, 1 to 30 characters.
	ClientName string
	// RedirectURI is the OAuth redirect URI for iOS: absolute, with no
	// query, https in Production, and registered under Allowed redirect
	// URIs in Plaid's Dashboard. Empty sends none, and OAuth banks then
	// cannot be linked from iOS.
	RedirectURI string
	// AndroidPackageName is the Android app's package name, registered
	// under Allowed Android package names in Plaid's Dashboard. Empty sends
	// none, and OAuth banks then cannot be linked from Android.
	AndroidPackageName string
	// Webhook is where Plaid sends a new Item's webhooks; empty for none.
	Webhook string
}

func (lc LinkConfig) validate() error {
	if lc.ClientName == "" || utf8.RuneCountInString(lc.ClientName) > maxClientNameLength {
		return fmt.Errorf("%w: link client name must be 1 to %d characters", ErrInvalidArgument, maxClientNameLength)
	}
	if lc.RedirectURI != "" {
		u, err := url.Parse(lc.RedirectURI)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("%w: link redirect uri must be an absolute http(s) url with no query or fragment", ErrInvalidArgument)
		}
	}
	if lc.Webhook != "" {
		u, err := url.Parse(lc.Webhook)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			return fmt.Errorf("%w: link webhook must be an absolute http(s) url", ErrInvalidArgument)
		}
	}
	return nil
}

type linkTokenCreateRequest struct {
	credentials
	ClientName                  string                `json:"client_name"`
	Language                    string                `json:"language"`
	CountryCodes                []string              `json:"country_codes"`
	User                        linkTokenUser         `json:"user"`
	Products                    []string              `json:"products,omitempty"`
	AdditionalConsentedProducts []string              `json:"additional_consented_products,omitempty"`
	Webhook                     string                `json:"webhook,omitempty"`
	AccessToken                 string                `json:"access_token,omitempty"`
	RedirectURI                 string                `json:"redirect_uri,omitempty"`
	AndroidPackageName          string                `json:"android_package_name,omitempty"`
	AccountFilters              *linkAccountFilters   `json:"account_filters,omitempty"`
	Auth                        *linkTokenAuthOptions `json:"auth,omitempty"`
	Update                      *linkTokenUpdate      `json:"update,omitempty"`
}

type linkTokenUser struct {
	ClientUserID string `json:"client_user_id"`
}

type linkAccountFilters struct {
	Depository linkSubtypeFilter `json:"depository"`
}

type linkSubtypeFilter struct {
	AccountSubtypes []string `json:"account_subtypes"`
}

// linkTokenAuthOptions is /link/token/create's auth object. Every field is
// sent, false included, so no default (which Plaid lets an account manager
// change per team) applies.
type linkTokenAuthOptions struct {
	AuthTypeSelectEnabled         bool   `json:"auth_type_select_enabled"`
	AutomatedMicrodepositsEnabled bool   `json:"automated_microdeposits_enabled"`
	InstantMatchEnabled           bool   `json:"instant_match_enabled"`
	SameDayMicrodepositsEnabled   bool   `json:"same_day_microdeposits_enabled"`
	InstantMicrodepositsEnabled   bool   `json:"instant_microdeposits_enabled"`
	RerouteToCredentials          string `json:"reroute_to_credentials"`
	DatabaseMatchEnabled          bool   `json:"database_match_enabled"`
	DatabaseInsightsEnabled       bool   `json:"database_insights_enabled"`
}

type linkTokenUpdate struct {
	AccountSelectionEnabled bool `json:"account_selection_enabled"`
}

type linkTokenCreateResponse struct {
	LinkToken  string    `json:"link_token"`
	Expiration time.Time `json:"expiration"`
	RequestID  string    `json:"request_id"`
}

// CreateLinkToken creates a link token for linking a new bank, for the
// Flipcash user clientUserID (their user ID, never anything personal), on
// platform. See the Link policy above.
func (c *Client) CreateLinkToken(ctx context.Context, clientUserID string, platform Platform) (*LinkToken, error) {
	return trace(ctx, "CreateLinkToken", func() (*LinkToken, error) {
		if err := requireNonEmpty("client user id", clientUserID); err != nil {
			return nil, err
		}
		req, err := c.baseLinkTokenRequest(clientUserID, platform)
		if err != nil {
			return nil, err
		}
		req.Products = linkProducts
		req.AdditionalConsentedProducts = linkConsentedProducts
		req.Webhook = c.link.Webhook
		req.Auth = &linkTokenAuthOptions{
			InstantMatchEnabled:  true,
			RerouteToCredentials: linkRerouteToCredential,
		}
		return c.createLinkToken(ctx, req)
	})
}

// CreateUpdateLinkToken creates a link token for opening Link in update mode
// for the Item accessToken, with account selection on: the user signs in or
// consents again, and may share more of their accounts at the bank or stop
// sharing some. See the Link policy above.
//
// Plaid answers ITEM_NOT_FOUND for an Item that is gone.
func (c *Client) CreateUpdateLinkToken(ctx context.Context, clientUserID string, accessToken string, platform Platform) (*LinkToken, error) {
	return trace(ctx, "CreateUpdateLinkToken", func() (*LinkToken, error) {
		if err := requireNonEmpty("client user id", clientUserID); err != nil {
			return nil, err
		}
		if err := requireNonEmpty("access token", accessToken); err != nil {
			return nil, err
		}
		req, err := c.baseLinkTokenRequest(clientUserID, platform)
		if err != nil {
			return nil, err
		}
		req.AccessToken = accessToken
		req.Update = &linkTokenUpdate{AccountSelectionEnabled: true}
		return c.createLinkToken(ctx, req)
	})
}

// baseLinkTokenRequest is what both shapes share.
func (c *Client) baseLinkTokenRequest(clientUserID string, platform Platform) (*linkTokenCreateRequest, error) {
	req := &linkTokenCreateRequest{
		credentials:  c.credentials(),
		ClientName:   c.link.ClientName,
		Language:     linkLanguage,
		CountryCodes: linkCountryCodes,
		User:         linkTokenUser{ClientUserID: clientUserID},
		AccountFilters: &linkAccountFilters{
			Depository: linkSubtypeFilter{AccountSubtypes: linkDepositorySubtypes},
		},
	}
	switch platform {
	case PlatformIOS:
		req.RedirectURI = c.link.RedirectURI
	case PlatformAndroid:
		req.AndroidPackageName = c.link.AndroidPackageName
	default:
		return nil, fmt.Errorf("%w: unknown platform", ErrInvalidArgument)
	}
	return req, nil
}

func (c *Client) createLinkToken(ctx context.Context, req *linkTokenCreateRequest) (*LinkToken, error) {
	var res linkTokenCreateResponse
	if err := c.do(ctx, pathLinkTokenCreate, req, &res); err != nil {
		return nil, err
	}
	if res.LinkToken == "" {
		return nil, fmt.Errorf("%w: %s returned no link token (request id %s)", ErrMalformedResponse, pathLinkTokenCreate, res.RequestID)
	}
	return &LinkToken{Token: res.LinkToken, Expiration: res.Expiration, RequestID: res.RequestID}, nil
}

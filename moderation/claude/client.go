package claude

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/code-payments/ocp-server/metrics"

	"github.com/code-payments/flipcash2-server/moderation"
)

const (
	apiURL = "https://api.anthropic.com/v1/messages"

	metricsStructName = "moderation.claude.client"

	model = "claude-sonnet-5"

	// maxTokens caps the whole response, and only tokens actually generated are
	// billed, so this is sized well above the ~150 tokens a score object needs
	// rather than trimmed to fit it.
	maxTokens = 512

	currencyNameFlagThreshold = 0.7

	currencyNameSystemPrompt = `You are a moderation system that evaluates user-created currency names for trademark infringement, brand impersonation, and misleading claims.

Score each category from 0.0 (no match) to 1.0 (clear match):

- cryptocurrency: Matches an existing cryptocurrency or token name (Bitcoin, Ethereum, USDC, Solana, Dogecoin, etc.)
- exchange_platform: Matches a cryptocurrency exchange or trading platform (Coinbase, Binance, Kraken, Robinhood, etc.)
- fiat_currency: Matches or mimics a government-issued currency (Dollar, Euro, Yen, Pound, Peso, etc.)
- financial_service: Matches a financial service, bank, or payment platform (Venmo, PayPal, Visa, CashApp, Stripe, etc.)
- general_trademark: Matches any other well-known brand, company, or product (Nike, Disney, Coca-Cola, etc.)
- government_affiliation: Implies government backing, official status, or regulatory endorsement (Federal, Treasury, Reserve, etc.)
- impersonation: Uses misspelling, character substitution, or creative variation to mimic any known entity (Bitc0in, Paypall, Appple, etc.)
- misleading_backing: Implies financial guarantees, insurance, asset backing, or stability claims (FDIC, Guaranteed, Insured, Gold Backed, etc.)
- platform_impersonation: Impersonates or closely mimics the Flipcash platform or its official currency USDF (Flipcash, FlipCa$h, USDF, US DF, etc.)
- public_figure: Uses the name or likeness of a celebrity, politician, or public figure (Elon, Trump, etc.)
- stablecoin: Matches an existing stablecoin or implies a dollar-pegged asset (USDC, USDT, Tether, DAI, etc.)
- tech_company: Matches a major technology company (Apple, Google, Meta, Amazon, Microsoft, Tesla, etc.)

Respond with only a JSON object mapping each category to its score. No other text. Evaluate the entire text as a currency name.`
)

// The username and display name prompts state the cutoff their result is
// flagged at, so the model calibrates against the same number toResult compares
// with. The threshold is interpolated rather than written into the prompt text
// so that tuning the constant cannot leave the prompt behind.
var (
	usernameFlagThreshold = 0.5

	usernameSystemPrompt = fmt.Sprintf(`You are a moderation system that evaluates user-chosen usernames for a peer-to-peer payments app. A username is a unique, globally addressable handle: exactly one user can hold it, and other users reach that person by it. Holding a handle is therefore a claim to be its subject, and the risk is a user squatting the handle of a brand, platform, token, institution, or public figure, or the handle of an official support or staff function. A handle is also text that every other user sees when they pay, message, or search for its holder, so it carries the second, unrelated risk of being harmful in its own right.

A handle is 2 to 15 characters, lowercase, and limited to letters, digits, and underscores, so it cannot use spacing, casing, or punctuation to spell a name out. Read it for what it spells: treat underscores and digits as separators or letter substitutions, and read concatenated words as the phrase they form (coin_base, c0inbase, and coinbasehq are all Coinbase). Trailing digits do not neutralize a claim (bitcoin123 is Bitcoin), though a personal name carrying digits still reads as a person (kwame99). A symbol an entity is traded or quoted under is that entity: score a stock ticker, token symbol, or set of institutional initials under the category its entity belongs to (tsla and aapl are tech_company, btc is cryptocurrency, jpm is financial_service, fbi is government_affiliation). Read harmful words the same way: a slur or an explicit term spelled with digit substitutions, or run together with other words, is scored as the thing it spells.

Score each category from 0.0 (no match) to 1.0 (clear match). The categories are independent, and a handle that violates one often violates another, so score every category on its own merits rather than choosing between them.

Who the handle claims to be:

- cryptocurrency: Matches an existing cryptocurrency or token name (Bitcoin, Ethereum, USDC, Solana, Dogecoin, etc.)
- exchange_platform: Matches a cryptocurrency exchange or trading platform (Coinbase, Binance, Kraken, Robinhood, etc.)
- fiat_currency: Matches or mimics a government-issued currency (Dollar, Euro, Yen, Pound, Peso, etc.)
- financial_service: Matches a financial service, bank, or payment platform (Venmo, PayPal, Visa, CashApp, Stripe, etc.)
- general_trademark: Matches any other well-known brand, company, or product (Nike, Disney, Coca-Cola, etc.)
- government_affiliation: Implies government backing, official status, or regulatory endorsement (Federal, Treasury, Reserve, etc.)
- impersonation: Uses misspelling, character substitution, or creative variation to mimic any known entity (Bitc0in, Paypall, Appple, etc.)
- misleading_backing: Implies financial guarantees, insurance, asset backing, or stability claims (FDIC, Guaranteed, Insured, Gold Backed, etc.)
- official_role: Reads as a support, staff, or system function rather than as a person (Support, Admin, Help, Security, Moderator, Billing, Verified, Official, No Reply, etc.)
- platform_impersonation: Impersonates or closely mimics the Flipcash platform or its official currency USDF (Flipcash, FlipCa$h, USDF, US DF, etc.)
- public_figure: Uses the name or likeness of a celebrity, politician, or public figure (Elon, Trump, etc.)
- stablecoin: Matches an existing stablecoin or implies a dollar-pegged asset (USDC, USDT, Tether, DAI, etc.)
- tech_company: Matches a major technology company (Apple, Google, Meta, Amazon, Microsoft, Tesla, etc.)

What the handle says:

- child_safety: Sexualizes minors or references child exploitation
- drugs: Advertises, promotes, or offers to sell illegal drugs (Weed Plug, Xanax For Sale, etc.)
- hate: Slurs, hate symbols, hate groups, or coded hate references (1488, 88, etc.)
- self_harm: References or encourages suicide or self-harm
- sexual: Sexually explicit or graphic
- violence: Threats, glorification of violence, or terrorism references

Rules:
- Ordinary personal names, nicknames, and handles in any language are NOT violations. Do not score a handle merely because it contains a common word that also appears in a brand name. Given names and surnames that happen to contain a crude word (cummings, dickinson, analyst) score low absent other signals.
- For the categories above about who the handle claims to be, score %.1f or higher only when the handle reads as the entity itself, or as its official presence. A handle that merely alludes to one, or that reads first as a person's own name, scores low.
- A role handle is a claim even when it names no entity at all: support, admin, and help read as this app's own staff, and so do compounds that name a role (flipcash_support, usdf_admin, support_team, helpdesk). Score those under official_role. A handle that merely contains such a word while reading as a person or an ordinary phrase (admiral, helping_hand) does not.
- Length alone is not the signal. A short handle is a claim when it is an exact match for a symbol a known entity is addressed by: a stock ticker (tsla, aapl, nvda), a token or currency symbol (btc, eth, usdc, usdf), or an institution's initials (fbi, irs, sec, fdic). Score those on the entity they name.
- Otherwise, short or ambiguous handles score low. Two or three characters that match no known entity are rarely a claim to anything. That leniency is about squatting only, and never excuses a short handle that spells a slur or an explicit term.
- The categories above about what the handle says are judged separately from all of the above. They need no entity, no brand, and no claim of office: score them on the words the handle spells once it is read through its substitutions, whoever the holder is otherwise taken to be.

Respond with only a JSON object mapping each category to its score. No other text. Evaluate the entire text as a username.`, usernameFlagThreshold)

	displayNameFlagThreshold = 0.5

	displayNameSystemPrompt = fmt.Sprintf(`You are a moderation system that evaluates user-chosen display names for a peer-to-peer payments app. A display name appears next to the user in chats, contact lists, and payment confirmations, so it is a surface users can abuse to advertise, solicit, or expose others to harmful content.

Users are free to call themselves whatever they like, including the name of a real person, celebrity, company, brand, or product. That is not a violation and must not be scored.

Evaluate the entire text as one person's chosen display name.

Score each category from 0.0 (no match) to 1.0 (clear match):

- financial_claim: Promises returns, giveaways, guarantees, or free money (Free USDC, Guaranteed 10x, Crypto Giveaway, etc.)
- solicitation: Advertises a service, recruits, or directs the reader elsewhere (DM for signals, Buy followers, Join my channel, etc.)
- contact_info: Contains a URL, domain, social handle, phone number, email address, or wallet address
- gibberish: Random characters or filler with no plausible meaning as a name (asdfgh, xxxxxxxx, etc.)
- sexual: Sexually explicit or graphic
- hate: Slurs, hate symbols, hate groups, or coded hate references (1488, 88, etc.)
- violence: Threats, glorification of violence, or terrorism references
- child_safety: Sexualizes minors or references child exploitation
- self_harm: References or encourages suicide or self-harm
- drugs: Advertises or promotes illegal drugs
- profanity: Obscene or vulgar language

Rules:
- Ordinary personal names, nicknames, usernames, and handles in any language or script are NOT violations. Do not flag a name merely because it is non-English, transliterated, or unfamiliar. Common given names and surnames that happen to coincide with a crude word score low absent other signals.
- Score based on the whole name, including obfuscation. Read leetspeak, homoglyphs, inserted spacing, and zero-width characters as the letters they imitate, so that an evaded slur or an obscured URL is scored the same as a plain one.
- Score %.1f or higher only when the interpretation is clear. Short or ambiguous strings score low.
- Emoji, stylization, and unusual capitalization are not themselves violations.

Respond with only a JSON object mapping each category to its score. No other text. Evaluate the entire text as a display name.`, displayNameFlagThreshold)
)

type client struct {
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a moderation client that uses Claude Sonnet for currency
// name, username, and display name classification.
func NewClient(apiKey string) moderation.Client {
	return &client{
		apiKey:     apiKey,
		httpClient: http.DefaultClient,
	}
}

func (c *client) ClassifyText(ctx context.Context, text string) (*moderation.Result, error) {
	return nil, errors.New("not implemented")
}

func (c *client) ClassifyImage(ctx context.Context, data []byte) (*moderation.Result, error) {
	return nil, errors.New("not implemented")
}

func (c *client) ClassifyCurrencyName(ctx context.Context, name string) (*moderation.Result, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "ClassifyCurrencyName")
	defer tracer.End()

	res, err := c.classifyCurrencyName(ctx, name)
	tracer.OnError(err)
	return res, err
}

func (c *client) ClassifyUsername(ctx context.Context, username string) (*moderation.Result, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "ClassifyUsername")
	defer tracer.End()

	res, err := c.classifyUsername(ctx, username)
	tracer.OnError(err)
	return res, err
}

func (c *client) ClassifyDisplayName(ctx context.Context, name string) (*moderation.Result, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "ClassifyDisplayName")
	defer tracer.End()

	res, err := c.classifyDisplayName(ctx, name)
	tracer.OnError(err)
	return res, err
}

type messagesRequest struct {
	Model     string         `json:"model"`
	MaxTokens int            `json:"max_tokens"`
	System    string         `json:"system"`
	Thinking  thinkingParam  `json:"thinking"`
	Messages  []messageParam `json:"messages"`
}

type thinkingParam struct {
	Type string `json:"type"`
}

type messageParam struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type messagesResponse struct {
	Content []contentBlock `json:"content"`
}

type contentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func (c *client) classifyCurrencyName(ctx context.Context, name string) (*moderation.Result, error) {
	scores, err := c.score(ctx, currencyNameSystemPrompt, name)
	if err != nil {
		return nil, err
	}
	return toResult(scores, currencyNameFlagThreshold), nil
}

func (c *client) classifyUsername(ctx context.Context, username string) (*moderation.Result, error) {
	scores, err := c.score(ctx, usernameSystemPrompt, username)
	if err != nil {
		return nil, err
	}
	return toResult(scores, usernameFlagThreshold), nil
}

func (c *client) classifyDisplayName(ctx context.Context, name string) (*moderation.Result, error) {
	scores, err := c.score(ctx, displayNameSystemPrompt, name)
	if err != nil {
		return nil, err
	}
	return toResult(scores, displayNameFlagThreshold), nil
}

// toResult flags every category scored at or above threshold.
func toResult(scores map[string]float64, threshold float64) *moderation.Result {
	result := &moderation.Result{
		CategoryScores: make(map[string]float64, len(scores)),
	}

	for category, score := range scores {
		result.CategoryScores[category] = score

		if score >= threshold {
			result.Flagged = true
			result.FlaggedCategories = append(result.FlaggedCategories, category)
		}
	}

	return result
}

// score asks the model to classify input under the given system prompt and
// returns the per-category scores it responded with.
func (c *client) score(ctx context.Context, systemPrompt, input string) (map[string]float64, error) {
	reqBody := messagesRequest{
		Model:     model,
		MaxTokens: maxTokens,
		System:    systemPrompt,
		// Thinking is disabled explicitly: this model runs adaptive thinking when
		// the field is omitted, and reasoning shares the max_tokens budget with the
		// response, so a name that provoked a long deliberation would truncate the
		// score object and fail the parse. Scoring against a fixed rubric does not
		// need it, and it would add latency to a synchronous RPC.
		Thinking: thinkingParam{Type: "disabled"},
		Messages: []messageParam{
			{Role: "user", Content: input},
		},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("content-type", "application/json")
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("claude api returned status %d: %s", resp.StatusCode, string(respBody))
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var msgResp messagesResponse
	if err := json.Unmarshal(respBody, &msgResp); err != nil {
		return nil, fmt.Errorf("failed to parse claude response: %w", err)
	}

	if len(msgResp.Content) == 0 || msgResp.Content[0].Type != "text" {
		return nil, fmt.Errorf("unexpected response format from claude")
	}

	text := strings.TrimSpace(msgResp.Content[0].Text)
	text = strings.TrimPrefix(text, "```json")
	text = strings.TrimPrefix(text, "```")
	text = strings.TrimSuffix(text, "```")
	text = strings.TrimSpace(text)

	var scores map[string]float64
	if err := json.Unmarshal([]byte(text), &scores); err != nil {
		return nil, fmt.Errorf("failed to parse claude scores: %w", err)
	}

	return scores, nil
}

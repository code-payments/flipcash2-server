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

	displayNameFlagThreshold = 0.5

	displayNameSystemPrompt = `You are a moderation system that evaluates user-chosen display names for a peer-to-peer payments app. A display name appears next to the user in chats, contact lists, and payment confirmations, so it is a surface users can abuse to advertise, solicit, or expose others to harmful content.

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
- Only score above 0.5 when the interpretation is clear. Short or ambiguous strings score low.
- Emoji, stylization, and unusual capitalization are not themselves violations.

Respond with only a JSON object mapping each category to its score. No other text. Evaluate the entire text as a display name.`
)

type client struct {
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a moderation client that uses Claude Sonnet for currency
// name and display name classification.
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

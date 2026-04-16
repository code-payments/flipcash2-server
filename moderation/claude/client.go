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

	model     = "claude-haiku-4-5"
	maxTokens = 256

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

Respond with only a JSON object mapping each category to its score. No other text.`
)

type client struct {
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a moderation client uses Claude Haiku for currency name classification.
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

type messagesRequest struct {
	Model       string         `json:"model"`
	MaxTokens   int            `json:"max_tokens"`
	Temperature float64        `json:"temperature"`
	System      string         `json:"system"`
	Messages    []messageParam `json:"messages"`
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
	reqBody := messagesRequest{
		Model:       model,
		MaxTokens:   maxTokens,
		Temperature: 0.0,
		System:      currencyNameSystemPrompt,
		Messages: []messageParam{
			{Role: "user", Content: name},
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

	result := &moderation.Result{
		CategoryScores: make(map[string]float64),
	}

	for category, score := range scores {
		result.CategoryScores[category] = score
		if score >= currencyNameFlagThreshold {
			result.Flagged = true
			result.FlaggedCategories = append(result.FlaggedCategories, category)
		}
	}

	return result, nil
}

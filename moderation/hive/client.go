package hive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/code-payments/ocp-server/metrics"

	"github.com/code-payments/flipcash2-server/moderation"
)

const (
	apiUrl = "https://api.thehive.ai/api/v2/task/sync"

	metricsStructName = "moderation.hive.client"

	// textFlagThreshold is the minimum score for a text category to be
	// considered flagged. Any score above 1.0 is flagged.
	textFlagThreshold = 1.0

	// imageFlagThreshold is the minimum confidence score for an image
	// category to be considered flagged.
	//
	// Hive's recommendation is 0.9, but we're using a more conservative
	// value to start.
	imageFlagThreshold = 0.7

	// filterDetectedScore is the synthetic CategoryScores value used for
	// pattern-matching hits (IWF text filters, profanity, PII). These are
	// boolean matches without a confidence score, so we surface them at the
	// most-severe text moderation level (3).
	filterDetectedScore = 3.0

	// iwfMultiMatchThreshold is the minimum number of
	// iwf_keyword_list_multi_match hits required to flag, per IWF's guidance.
	iwfMultiMatchThreshold = 2

	iwfURLListMatch           = "iwf_url_list_match"
	iwfKeywordListSingleMatch = "iwf_keyword_list_single_match"
	iwfKeywordListMultiMatch  = "iwf_keyword_list_multi_match"

	profanityFilterType = "profanity"

	// minorSexualEscalationScore is the minimum sexual score that, together
	// with a minor-presence hit, flags the text as child exploitation. It sits
	// below textFlagThreshold on purpose: mildly sexual text that mentions a
	// minor is flagged even though neither signal would flag on its own.
	minorSexualEscalationScore = 1.0

	minorExplicitlyMentionedCategory = "minor_explicitly_mentioned"
	minorImplicitlyMentionedCategory = "minor_implicitly_mentioned"
	sexualCategory                   = "sexual"

	childExploitationCategory = "child_exploitation"
	profanityCategory         = "profanity"
	piiCategory               = "pii"
)

// textContextClasses are Hive text classes that describe the text rather than
// judge it, and so never flag on their own. Their scores are still recorded.
//
// The minor-presence classes are binary (0 or 3) and fire on any likely
// reference to someone under 18: "little boy", "teens", a school, or a name
// like "BadBoys". A mention of a minor is not a violation; it only escalates
// sexual text (see applyMinorSexualEscalation). Child exploitation itself is
// covered by Hive's child_exploitation class and the IWF text filters.
var textContextClasses = map[string]struct{}{
	minorExplicitlyMentionedCategory: {},
	minorImplicitlyMentionedCategory: {},
}

type client struct {
	apiKey     string
	apiUrl     string
	httpClient *http.Client
}

func NewClient(apiKey string) moderation.Client {
	return &client{
		apiKey:     apiKey,
		apiUrl:     apiUrl,
		httpClient: http.DefaultClient,
	}
}

func (c *client) ClassifyText(ctx context.Context, text string) (*moderation.Result, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "ClassifyText")
	defer tracer.End()

	res, err := c.classifyText(ctx, text)
	tracer.OnError(err)
	return res, err
}

func (c *client) classifyText(ctx context.Context, text string) (*moderation.Result, error) {
	form := url.Values{}
	form.Set("text_data", text)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiUrl, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Token "+c.apiKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	result, hiveResp, err := c.doClassify(req, textFlagThreshold, func(_ string) bool {
		return true
	}, func(category string) bool {
		_, ok := textContextClasses[category]
		return !ok
	})
	if err != nil {
		return nil, err
	}

	applyMinorSexualEscalation(result)
	applyIWFTextFilters(hiveResp, result)
	applyProfanityTextFilters(hiveResp, result)
	applyPIIEntities(hiveResp, result)

	if result.Flagged {
		return result, nil
	}

	// Required core categories that have good coverage across many languages
	for _, category := range []string{
		"sexual",
		"hate",
		"violence",
		"bullying",
		"spam",
	} {
		if _, ok := result.CategoryScores[category]; !ok {
			return nil, moderation.ErrUnsupportedLanguage
		}
	}

	return result, nil
}

func (c *client) ClassifyImage(ctx context.Context, data []byte) (*moderation.Result, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "ClassifyImage")
	defer tracer.End()

	res, err := c.classifyImage(ctx, data)
	tracer.OnError(err)
	return res, err
}

func (c *client) classifyImage(ctx context.Context, data []byte) (*moderation.Result, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	part, err := w.CreateFormFile("media", "image")
	if err != nil {
		return nil, err
	}
	if _, err := part.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiUrl, &buf)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Token "+c.apiKey)
	req.Header.Set("Content-Type", w.FormDataContentType())

	result, _, err := c.doClassify(req, imageFlagThreshold, func(category string) bool {
		_, ok := imageFlaggedCategories[strings.ToLower(category)]
		return ok
	}, func(_ string) bool {
		return true
	})
	return result, err
}

// doClassify sends req and converts Hive's response into a Result. Only
// categories passing categoryInclusionFunc are recorded, and only recorded
// categories passing categoryFlaggableFunc can flag.
func (c *client) doClassify(req *http.Request, flagThreshold float64, categoryInclusionFunc, categoryFlaggableFunc func(category string) bool) (*moderation.Result, *response, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("unexpected http status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	var hiveResp response
	if err := json.Unmarshal(body, &hiveResp); err != nil {
		return nil, nil, err
	}

	if len(hiveResp.Status) == 0 {
		return nil, nil, fmt.Errorf("empty response from hive")
	}

	if hiveResp.Status[0].Response.Code != 0 {
		return nil, nil, fmt.Errorf("hive returned error code: %d", hiveResp.Status[0].Response.Code)
	}

	result, err := hiveResp.toResult(flagThreshold, categoryInclusionFunc, categoryFlaggableFunc)
	if err != nil {
		return nil, nil, err
	}
	return result, &hiveResp, nil
}

func (c *client) ClassifyCurrencyName(ctx context.Context, name string) (*moderation.Result, error) {
	return nil, errors.New("not implemented")
}

func (c *client) ClassifyUsername(ctx context.Context, username string) (*moderation.Result, error) {
	return nil, errors.New("not implemented")
}

func (c *client) ClassifyDisplayName(ctx context.Context, name string) (*moderation.Result, error) {
	return nil, errors.New("not implemented")
}

func (c *client) ClassifyGroupTitle(ctx context.Context, title string) (*moderation.Result, error) {
	return nil, errors.New("not implemented")
}

type response struct {
	Status []taskStatus `json:"status"`
}

type taskStatus struct {
	Response taskResponse `json:"response"`
}

type taskResponse struct {
	Code        int          `json:"code"`
	Output      []taskOutput `json:"output"`
	TextFilters []textFilter `json:"text_filters"`
	PIIEntities []piiEntity  `json:"pii_entities"`
}

type taskOutput struct {
	Classes []classResult `json:"classes"`
}

type textFilter struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type piiEntity struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// classResult is a Hive moderation class score.
//
// Text moderation scores use a 0-3 severity scale:
//
//	-1 = unsupported language
//	 0 = benign
//	 1 = mildly concerning
//	 2 = moderately severe
//	 3 = most severe
//
// Image moderation scores use a 0-1 confidence scale.
type classResult struct {
	Class string  `json:"class"`
	Score float64 `json:"score"`
}

func (r *response) toResult(flagThreshold float64, categoryInclusionFunc, categoryFlaggableFunc func(category string) bool) (*moderation.Result, error) {
	result := &moderation.Result{
		CategoryScores: make(map[string]float64),
	}

	if len(r.Status) == 0 {
		return result, nil
	}

	for _, output := range r.Status[0].Response.Output {
		for _, class := range output.Classes {
			if !categoryInclusionFunc(class.Class) {
				continue
			}

			if class.Score == -1 {
				continue
			}

			result.CategoryScores[class.Class] = class.Score

			if class.Score > flagThreshold && categoryFlaggableFunc(class.Class) {
				result.Flagged = true
				result.FlaggedCategories = append(result.FlaggedCategories, class.Class)
			}
		}
	}

	return result, nil
}

// applyMinorSexualEscalation flags the result as child exploitation when Hive
// detected a minor and the text is at least mildly sexual. The minor-presence
// classes never flag alone (see textContextClasses); this is what they exist
// for.
func applyMinorSexualEscalation(result *moderation.Result) {
	if result.CategoryScores[minorExplicitlyMentionedCategory] <= 0 &&
		result.CategoryScores[minorImplicitlyMentionedCategory] <= 0 {
		return
	}

	if result.CategoryScores[sexualCategory] < minorSexualEscalationScore {
		return
	}

	result.Flagged = true
	result.CategoryScores[childExploitationCategory] = filterDetectedScore
	if !slices.Contains(result.FlaggedCategories, childExploitationCategory) {
		result.FlaggedCategories = append(result.FlaggedCategories, childExploitationCategory)
	}
}

// applyIWFTextFilters folds Hive's IWF text_filters matches into the result.
// Any IWF hit collapses to a single child_exploitation category at the highest
// severity score. iwf_keyword_list_multi_match requires >= 2 hits to flag, per
// IWF guidance; the URL and single-match lists flag on a single hit.
func applyIWFTextFilters(resp *response, result *moderation.Result) {
	if resp == nil || len(resp.Status) == 0 {
		return
	}

	counts := make(map[string]int)
	for _, filter := range resp.Status[0].Response.TextFilters {
		counts[filter.Type]++
	}

	if counts[iwfURLListMatch] < 1 &&
		counts[iwfKeywordListSingleMatch] < 1 &&
		counts[iwfKeywordListMultiMatch] < iwfMultiMatchThreshold {
		return
	}

	result.Flagged = true
	result.CategoryScores[childExploitationCategory] = filterDetectedScore
	if !slices.Contains(result.FlaggedCategories, childExploitationCategory) {
		result.FlaggedCategories = append(result.FlaggedCategories, childExploitationCategory)
	}
}

// applyProfanityTextFilters flags the result when Hive's profanity
// pattern-matcher returns any hits.
func applyProfanityTextFilters(resp *response, result *moderation.Result) {
	if resp == nil || len(resp.Status) == 0 {
		return
	}

	for _, filter := range resp.Status[0].Response.TextFilters {
		if filter.Type != profanityFilterType {
			continue
		}

		result.Flagged = true
		result.CategoryScores[profanityCategory] = filterDetectedScore
		if !slices.Contains(result.FlaggedCategories, profanityCategory) {
			result.FlaggedCategories = append(result.FlaggedCategories, profanityCategory)
		}
		return
	}
}

// applyPIIEntities flags the result when Hive detects any PII entities
// (emails, phone numbers, addresses).
func applyPIIEntities(resp *response, result *moderation.Result) {
	if resp == nil || len(resp.Status) == 0 {
		return
	}

	if len(resp.Status[0].Response.PIIEntities) == 0 {
		return
	}

	result.Flagged = true
	result.CategoryScores[piiCategory] = filterDetectedScore
	if !slices.Contains(result.FlaggedCategories, piiCategory) {
		result.FlaggedCategories = append(result.FlaggedCategories, piiCategory)
	}
}

var imageFlaggedCategories = map[string]struct{}{
	// --- Sexual content (explicit) ---
	"animal_genitalia_and_human": {},
	"animal_genitalia_only":      {},
	"animated_animal_genitalia":  {},
	"general_nsfw":               {},
	"yes_breast":                 {},
	"yes_female_nudity":          {},
	"yes_genitals":               {},
	"yes_male_nudity":            {},
	"yes_realistic_nsfw":         {},
	"yes_sex_toy":                {},
	"yes_sexual_activity":        {},
	"yes_sexual_intent":          {},
	"yes_undressed":              {},

	// --- Sexual content (suggestive) ---
	"general_suggestive":   {},
	"yes_bulge":            {},
	"yes_butt":             {},
	"yes_female_underwear": {},
	"yes_male_underwear":   {},
	"yes_negligee":         {},
	"yes_panties":          {},

	// --- Weapons ---
	"animated_gun":      {},
	"gun_in_hand":       {},
	"gun_not_in_hand":   {},
	"knife_in_hand":     {},
	"knife_not_in_hand": {},

	// --- Gore / violence ---
	"a_little_bloody":    {},
	"animated_corpse":    {},
	"hanging":            {},
	"human_corpse":       {},
	"noose":              {},
	"other_blood":        {},
	"very_bloody":        {},
	"yes_animal_abuse":   {},
	"yes_emaciated_body": {},
	"yes_fight":          {},
	"yes_self_harm":      {},

	// --- Hate symbols ---
	"yes_confederate":      {},
	"yes_confederate_flag": {},
	"yes_kkk":              {},
	"yes_middle_finger":    {},
	"yes_nazi":             {},
	"yes_terrorist":        {},

	// --- Drugs / illicit ---
	"illicit_injectables": {},
	"recreational_pills":  {},
	"yes_marijuana":       {},
	"yes_pills":           {},

	// --- Child safety / abuse signals ---
	"yes_child_safety": {},

	// --- Spam / phishing signals ---
	"yes_qr_code": {},
}

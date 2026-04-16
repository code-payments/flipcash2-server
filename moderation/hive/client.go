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
	// considered flagged. Any score above 0 (benign) is flagged.
	textFlagThreshold = 0.0

	// imageFlagThreshold is the minimum confidence score for an image
	// category to be considered flagged, per Hive's recommendation.
	imageFlagThreshold = 0.9

	// iwfDetectedScore is the synthetic CategoryScores value used when any IWF
	// text filter matches. IWF filters are pattern-matching results without a
	// confidence score, so we surface them at the most-severe text moderation
	// level (3).
	iwfDetectedScore = 3.0

	// iwfMultiMatchThreshold is the minimum number of
	// iwf_keyword_list_multi_match hits required to flag, per IWF's guidance.
	iwfMultiMatchThreshold = 2

	iwfURLListMatch           = "iwf_url_list_match"
	iwfKeywordListSingleMatch = "iwf_keyword_list_single_match"
	iwfKeywordListMultiMatch  = "iwf_keyword_list_multi_match"

	childExploitationCategory = "child_exploitation"
)

type client struct {
	apiKey     string
	httpClient *http.Client
}

func NewClient(apiKey string) moderation.Client {
	return &client{
		apiKey:     apiKey,
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

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiUrl, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Token "+c.apiKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	result, hiveResp, err := c.doClassify(req, textFlagThreshold, func(_ string) bool {
		return true
	})
	if err != nil {
		return nil, err
	}

	applyIWFTextFilters(hiveResp, result)

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

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiUrl, &buf)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Token "+c.apiKey)
	req.Header.Set("Content-Type", w.FormDataContentType())

	result, _, err := c.doClassify(req, imageFlagThreshold, func(category string) bool {
		_, ok := imageFlaggedCategories[strings.ToLower(category)]
		return ok
	})
	return result, err
}

func (c *client) doClassify(req *http.Request, flagThreshold float64, categoryInclusionFunc func(category string) bool) (*moderation.Result, *response, error) {
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

	result, err := hiveResp.toResult(flagThreshold, categoryInclusionFunc)
	if err != nil {
		return nil, nil, err
	}
	return result, &hiveResp, nil
}

func (c *client) ClassifyCurrencyName(ctx context.Context, name string) (*moderation.Result, error) {
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
}

type taskOutput struct {
	Classes []classResult `json:"classes"`
}

type textFilter struct {
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

func (r *response) toResult(flagThreshold float64, categoryInclusionFunc func(category string) bool) (*moderation.Result, error) {
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

			if class.Score > flagThreshold {
				result.Flagged = true
				result.FlaggedCategories = append(result.FlaggedCategories, class.Class)
			}
		}
	}

	return result, nil
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
	result.CategoryScores[childExploitationCategory] = iwfDetectedScore
	if !slices.Contains(result.FlaggedCategories, childExploitationCategory) {
		result.FlaggedCategories = append(result.FlaggedCategories, childExploitationCategory)
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
	"general_suggestive":     {},
	"kissing":                {},
	"licking":                {},
	"yes_bodysuit":           {},
	"yes_bra":                {},
	"yes_bulge":              {},
	"yes_butt":               {},
	"yes_cleavage":           {},
	"yes_female_swimwear":    {},
	"yes_female_underwear":   {},
	"yes_male_shirtless":     {},
	"yes_male_underwear":     {},
	"yes_miniskirt":          {},
	"yes_negligee":           {},
	"yes_panties":            {},
	"yes_sports_bra":         {},
	"yes_sportswear_bottoms": {},

	// --- Weapons ---
	"animated_gun":               {},
	"culinary_knife_in_hand":     {},
	"culinary_knife_not_in_hand": {},
	"gun_in_hand":                {},
	"gun_not_in_hand":            {},
	"knife_in_hand":              {},
	"knife_not_in_hand":          {},

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
	"yes_confederate":   {},
	"yes_kkk":           {},
	"yes_middle_finger": {},
	"yes_nazi":          {},
	"yes_terrorist":     {},

	// --- Drugs / illicit ---
	"animated_alcohol":     {},
	"illicit_injectables":  {},
	"medical_injectables":  {},
	"yes_alcohol":          {},
	"yes_drinking_alcohol": {},
	"yes_gambling":         {},
	"yes_marijuana":        {},
	"yes_pills":            {},
	"yes_smoking":          {},

	// --- Child safety / abuse signals ---
	"yes_child_present": {},
	"yes_child_safety":  {},

	// --- Spam / phishing signals ---
	"yes_qr_code": {},
}

package moderation

import (
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"
)

// HighestFlaggedCategory maps a moderation Result's flagged categories onto the
// proto FlaggedCategory vocabulary, returning the highest-scoring one that maps
// to a well-defined category (falling back to OTHER). It is the shared mapping
// used both by this service's responses and by callers that surface a moderation
// verdict elsewhere (e.g. a blob's rejection metadata).
func HighestFlaggedCategory(result *Result) moderationpb.FlaggedCategory {
	var highestScore float64
	highestFlaggedCategory := moderationpb.FlaggedCategory_OTHER
	for _, flaggedCategory := range result.FlaggedCategories {
		mapped := mapFlaggedCategory(flaggedCategory)
		if mapped == moderationpb.FlaggedCategory_OTHER {
			continue
		}
		score := result.CategoryScores[flaggedCategory]
		if score > highestScore {
			highestScore = score
			highestFlaggedCategory = mapped
		}
	}
	return highestFlaggedCategory
}

func mapFlaggedCategory(flaggedCategory string) moderationpb.FlaggedCategory {
	switch flaggedCategory {
	case
		"cryptocurrency",
		"exchange_platform",
		"fiat_currency",
		"financial_service",
		"general_trademark",
		"government_affiliation",
		"impersonation",
		"official_role",
		"platform_impersonation",
		"public_figure",
		"stablecoin",
		"tech_company":
		return moderationpb.FlaggedCategory_IMPERSONATION

	case
		"financial_claim",
		"misleading_backing":
		return moderationpb.FlaggedCategory_MISLEADING

	case
		"a_little_bloody",
		"animal_genitalia_and_human",
		"animal_genitalia_only",
		"animated_alcohol",
		"animated_animal_genitalia",
		"animated_corpse",
		"animated_gun",
		"bullying",
		"child_exploitation",
		"culinary_knife_in_hand",
		"culinary_knife_not_in_hand",
		"child_safety",
		"drugs",
		"general_nsfw",
		"general_suggestive",
		"gun_in_hand",
		"gun_not_in_hand",
		"hanging",
		"hate",
		"human_corpse",
		"illicit_injectables",
		"kissing",
		"knife_in_hand",
		"knife_not_in_hand",
		"licking",
		"medical_injectables",
		"minor_explicitly_mentioned",
		"minor_implicitly_mentioned",
		"noose",
		"other_blood",
		"profanity",
		"recreational_pills",
		"self_harm",
		"self_harm_intent",
		"sexual",
		"sexual_description",
		"very_bloody",
		"violence",
		"violent_description",
		"weapons",
		"yes_alcohol",
		"yes_animal_abuse",
		"yes_bodysuit",
		"yes_bra",
		"yes_breast",
		"yes_bulge",
		"yes_butt",
		"yes_child_safety",
		"yes_cleavage",
		"yes_confederate",
		"yes_confederate_flag",
		"yes_drinking_alcohol",
		"yes_emaciated_body",
		"yes_female_nudity",
		"yes_female_swimwear",
		"yes_female_underwear",
		"yes_fight",
		"yes_gambling",
		"yes_genitals",
		"yes_kkk",
		"yes_male_nudity",
		"yes_male_shirtless",
		"yes_male_underwear",
		"yes_marijuana",
		"yes_middle_finger",
		"yes_miniskirt",
		"yes_nazi",
		"yes_negligee",
		"yes_panties",
		"yes_pills",
		"yes_realistic_nsfw",
		"yes_self_harm",
		"yes_sex_toy",
		"yes_sexual_activity",
		"yes_sexual_intent",
		"yes_smoking",
		"yes_sports_bra",
		"yes_sportswear_bottoms",
		"yes_terrorist",
		"yes_undressed":
		return moderationpb.FlaggedCategory_NSFW

	case
		"contact_info",
		"gibberish",
		"phone_number",
		"promotions",
		"redirection",
		"solicitation",
		"spam",
		"yes_qr_code":
		return moderationpb.FlaggedCategory_SPAM
	}

	return moderationpb.FlaggedCategory_OTHER
}

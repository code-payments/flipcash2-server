package profile

import (
	"regexp"
	"strings"
)

// usernamePattern is the set of handles a user may hold, mirroring what
// profile.v1.Username enforces on the wire: X's character set, minus upper
// case, so a handle has exactly one spelling.
var usernamePattern = regexp.MustCompile(`^[a-z0-9_]{2,15}$`)

// NormalizeUsername puts a handle into the canonical form it is stored and
// compared in, so a lookup finds its holder regardless of the casing it was
// typed in.
func NormalizeUsername(username string) string {
	return strings.ToLower(username)
}

// ValidateUsername returns ErrInvalidUsername unless username is a handle in
// canonical form. Every store checks through here before persisting, so nothing
// but the canonical form is ever held — which is what makes a case-insensitive
// lookup enough to find a holder.
func ValidateUsername(username string) error {
	if !usernamePattern.MatchString(username) {
		return ErrInvalidUsername
	}
	return nil
}

var reservedUsernameSubstrings = []string{
	"flipcash",
	"usdf",
}

// reservedGlyphClasses gives, for each letter a reserved word can contain, the
// glyphs a handle can spell that letter with. Every class is a letter and the
// digits standing in for it, never two letters, which is what keeps the
// expansion below from reserving words nobody was hiding: a spelling that
// differs from its word at all differs by at least one digit, so no handle
// written in letters alone is caught by anything but itself. That is why "mail"
// being reserved leaves "mall" free.
var reservedGlyphClasses = map[byte]string{
	'a': "a4",
	'b': "b8",
	'e': "e3",
	'g': "g9",
	'i': "i1",
	'l': "l1",
	'o': "o0",
	's': "s5",
	't': "t7",
	'z': "z2",
}

// maxReservedSpellings bounds what one reserved word may expand to, since the
// count doubles with each ambiguous letter it contains. No entry comes close —
// "notifications" is the largest at 512 — and a test holds them all under it, so
// the cap is a backstop against a future entry rather than something reached.
const maxReservedSpellings = 4096

// expandReservedSpellings returns every way word can be spelled by substituting
// digits for its letters, itself included. A word past the cap is returned as
// itself alone, which protects it exactly and no less than a literal comparison
// would.
//
// Reserved words are ASCII by construction, so this indexes bytes.
func expandReservedSpellings(word string) []string {
	spellings := []string{""}
	for i := 0; i < len(word); i++ {
		glyphs, ok := reservedGlyphClasses[word[i]]
		if !ok {
			glyphs = word[i : i+1]
		}

		if len(spellings)*len(glyphs) > maxReservedSpellings {
			return []string{word}
		}

		grown := make([]string, 0, len(spellings)*len(glyphs))
		for _, spelling := range spellings {
			for j := 0; j < len(glyphs); j++ {
				grown = append(grown, spelling+string(glyphs[j]))
			}
		}
		spellings = grown
	}
	return spellings
}

// reservedExactWordSpellings and reservedSubstringSpellings hold the lists below
// as they are actually compared against: every spelling of every entry, worked
// out once at startup so that a claim costs a single lookup. The lists
// themselves stay in one plain spelling each, which is the one a person
// maintaining them should have to read.
var reservedExactWordSpellings = func() map[string]struct{} {
	spellings := make(map[string]struct{}, 32*len(reservedExactWords))
	for word := range reservedExactWords {
		for _, spelling := range expandReservedSpellings(withoutUnderscores(word)) {
			spellings[spelling] = struct{}{}
		}
	}
	return spellings
}()

var reservedSubstringSpellings = func() []string {
	var spellings []string
	for _, substring := range reservedUsernameSubstrings {
		spellings = append(spellings, expandReservedSpellings(substring)...)
	}
	return spellings
}()

// withoutUnderscores drops the one character that lets a handle spell a reserved
// word without being it. An underscore is silent, so "s_u_p_p_o_r_t" is the word
// it reads as and is compared as that word.
func withoutUnderscores(username string) string {
	return strings.ReplaceAll(username, "_", "")
}

var reservedExactWords = map[string]struct{}{
	// Marketing and static pages.
	"about": {}, "blog": {}, "brand": {}, "careers": {}, "changelog": {},
	"community": {}, "company": {}, "contact": {}, "events": {}, "faq": {},
	"features": {}, "help": {}, "investors": {}, "jobs": {}, "legal": {},
	"media": {}, "news": {}, "newsletter": {}, "partners": {}, "podcast": {},
	"press": {}, "pricing": {}, "privacy": {}, "roadmap": {}, "security": {},
	"status": {}, "subscribe": {}, "subscriptions": {}, "support": {},
	"terms": {}, "tos": {},

	// Account and auth flows. "me", "my" and "id" are short but are exactly the
	// kind of self-referential route that collides.
	"account": {}, "accounts": {}, "auth": {}, "authorize": {}, "confirm": {},
	"connect": {}, "deactivate": {}, "id": {}, "login": {}, "logout": {},
	"me": {}, "my": {}, "oauth": {}, "password": {}, "preferences": {},
	"profile": {}, "profiles": {}, "register": {}, "reset": {}, "session": {},
	"sessions": {}, "settings": {}, "sign_in": {}, "sign_up": {}, "signin": {},
	"signout": {}, "signup": {}, "sso": {}, "unsubscribe": {}, "user": {},
	"username": {}, "usernames": {}, "users": {}, "verify": {},
	"verification": {},

	// Words a self-custodial wallet must not let anyone be addressed as. These
	// are not route collisions: a handle here is the opening line of a
	// seed-phrase phish, and no user should ever receive a message from
	// "recovery" or "backup" in the first place.
	"2fa": {}, "backup": {}, "key": {}, "keys": {}, "magic": {}, "mfa": {},
	"mnemonic": {}, "otp": {}, "phrase": {}, "recover": {}, "recovery": {},
	"restore": {}, "secret": {}, "seed": {}, "seed_phrase": {}, "token": {},
	"tokens": {},

	// Product routes. These are the ones most likely to be added later, and the
	// ones a handle would most plausibly want — reserving them now is cheaper than
	// taking a handle back.
	"activity": {}, "app": {}, "apps": {}, "balance": {}, "buy": {}, "card": {},
	"cards": {}, "cash": {}, "chat": {}, "chats": {}, "checkout": {},
	"contacts": {}, "dashboard": {}, "deposit": {}, "discover": {}, "dm": {},
	"dms": {}, "download": {}, "downloads": {}, "earn": {}, "explore": {},
	"feed": {}, "gift": {}, "gifts": {}, "group": {}, "groups": {},
	"history": {}, "home": {}, "invite": {}, "invites": {}, "message": {},
	"messages": {}, "money": {}, "pay": {}, "payment": {}, "payments": {},
	"receive": {}, "referral": {}, "referrals": {}, "request": {},
	"rewards": {}, "search": {}, "sell": {}, "send": {}, "swap": {}, "tip": {},
	"tips": {}, "trade": {}, "transaction": {}, "transactions": {},
	"transfer": {}, "wallet": {}, "welcome": {}, "withdraw": {},

	// The routes a claimable link is served from, and the words a claim is
	// offered in. "go" and "get" are reserved for the short-link prefixes they
	// conventionally are, before anything is hosted under them.
	"claim": {}, "claims": {}, "code": {}, "codes": {}, "coupon": {},
	"get": {}, "go": {}, "link": {}, "links": {}, "promo": {}, "qr": {},
	"redeem": {}, "refer": {}, "scan": {}, "share": {}, "voucher": {},

	// The currency-launch surface, and the words money itself goes by. A handle
	// here reads as the market or as the money rather than as someone trading in
	// it.
	"analytics": {}, "chart": {}, "charts": {}, "coin": {}, "coins": {},
	"currencies": {}, "currency": {}, "dollar": {}, "dollars": {},
	"featured": {}, "leaderboard": {}, "market": {}, "markets": {}, "new": {},
	"popular": {}, "price": {}, "prices": {}, "stats": {}, "top": {},
	"trending": {},

	// Trust, safety and compliance. These are plausible routes, but the reason
	// to hold them is that a handle reporting fraud is a good way to commit it.
	"aml": {}, "appeal": {}, "appeals": {}, "banned": {}, "block": {},
	"blocked": {}, "compliance": {}, "dmca": {}, "fraud": {}, "kyc": {},
	"phishing": {}, "report": {}, "reports": {}, "safety": {}, "scam": {},
	"spam": {}, "trust": {},

	// Acquisition, platform and release channels.
	"alpha": {}, "android": {}, "beta": {}, "desktop": {}, "install": {},
	"ios": {}, "launch": {}, "mobile": {}, "onboarding": {}, "release": {},
	"releases": {}, "start": {}, "updates": {}, "version": {}, "waitlist": {},
	"web": {},

	// Technical routes, asset prefixes and crawler conventions.
	"404": {}, "500": {}, "api": {}, "assets": {}, "callback": {}, "cdn": {},
	"config": {}, "css": {}, "debug": {}, "default": {}, "demo": {}, "dev": {},
	"developer": {}, "developers": {}, "doc": {}, "docs": {},
	"documentation": {}, "embed": {}, "error": {}, "errors": {},
	"favicon": {}, "files": {}, "fonts": {}, "graphql": {}, "health": {},
	"healthz": {}, "images": {}, "img": {}, "index": {}, "internal": {},
	"js": {}, "manifest": {}, "metrics": {}, "ping": {}, "proxy": {},
	"public": {}, "redirect": {}, "robots": {}, "sandbox": {}, "sdk": {},
	"sitemap": {}, "staging": {}, "static": {}, "test": {}, "upload": {},
	"uploads": {}, "v1": {}, "v2": {}, "v3": {}, "webhook": {}, "webhooks": {},
	"widget": {}, "www": {},

	// Roles a handle must not claim to be. The username classifier scores these
	// as official_role too, but a fixed list costs nothing and does not depend on
	// a model being available or agreeing.
	"abuse": {}, "admin": {}, "admins": {}, "administrator": {}, "alert": {},
	"alerts": {}, "billing": {}, "bot": {}, "bots": {}, "email": {},
	"helpdesk": {}, "hostmaster": {}, "info": {}, "mail": {}, "marketing": {},
	"mod": {}, "moderator": {}, "moderators": {}, "mods": {}, "no_reply": {},
	"noreply": {}, "notification": {}, "notifications": {}, "official": {},
	"postmaster": {}, "root": {}, "sales": {}, "service": {}, "services": {},
	"staff": {}, "superuser": {}, "sysadmin": {}, "system": {}, "team": {},
	"verified": {}, "webmaster": {},

	// Values that leak out of buggy clients and serializers, and the names a
	// withheld or departed user is rendered under. None of them may resolve to a
	// real person's profile.
	"anon": {}, "anonymous": {}, "deleted": {}, "example": {}, "false": {},
	"guest": {}, "nan": {}, "nil": {}, "nobody": {}, "none": {}, "null": {},
	"placeholder": {}, "removed": {}, "true": {}, "unavailable": {},
	"undefined": {}, "unknown": {}, "void": {},

	// Broadcast words, so no one user can be addressed as all of them.
	"all": {}, "anyone": {}, "channel": {}, "channels": {}, "everybody": {},
	"everyone": {}, "here": {}, "online": {}, "somebody": {}, "someone": {},
}

// IsUsernameReserved reports whether username contains a word the platform keeps
// for itself, and so may not be claimed by anyone.
//
// A reserved word is recognized through every spelling a handle can hide it in:
// spaced out with underscores, or with digits standing in for its letters, so
// "s_u_p_p_o_r_t", "supp0rt" and "f1ipcash" are each the word they read as. A
// handle spelled in letters alone is only ever caught by the word it actually
// is.
//
// It expects a handle in canonical form, which is lower case, so the comparison
// needs no folding of its own — NormalizeUsername first if that is in doubt.
func IsUsernameReserved(username string) bool {
	spelling := withoutUnderscores(username)

	// An exact word is matched whole, so that reserving it does not take every
	// handle spelled around it too.
	if _, ok := reservedExactWordSpellings[spelling]; ok {
		return true
	}

	for _, reserved := range reservedSubstringSpellings {
		if strings.Contains(spelling, reserved) {
			return true
		}
	}

	return false
}

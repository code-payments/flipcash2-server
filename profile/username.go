package profile

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"regexp"
	"strconv"
	"strings"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

// maxUsernameLength is the longest handle a user may hold.
const maxUsernameLength = 15

// usernamePattern is the set of handles a user may hold, mirroring what
// profile.v1.Username enforces on the wire: X's character set, minus upper
// case, so a handle has exactly one spelling.
var usernamePattern = regexp.MustCompile(fmt.Sprintf(`^[a-z0-9_]{2,%d}$`, maxUsernameLength))

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
	"terms": {}, "termsofservice": {}, "tos": {},

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
	"feed": {}, "flipcard": {}, "flipcardcreator": {}, "flipcards": {},
	"gift": {}, "gifts": {}, "group": {}, "groups": {},
	"history": {}, "home": {}, "invite": {}, "invites": {}, "message": {},
	"messages": {}, "money": {}, "pay": {}, "payment": {}, "payments": {},
	"receive": {}, "referral": {}, "referrals": {}, "request": {},
	"rewards": {}, "search": {}, "sell": {}, "send": {}, "swap": {}, "tip": {},
	"tipcard": {}, "tipcardcreator": {}, "tips": {}, "trade": {},
	"transaction": {}, "transactions": {},
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
	"currencies": {}, "currency": {}, "currencycreator": {}, "dollar": {},
	"dollars": {}, "featured": {}, "leaderboard": {}, "market": {},
	"markets": {}, "new": {}, "popular": {}, "price": {}, "prices": {},
	"stats": {}, "top": {}, "trending": {},

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
	"config": {}, "css": {}, "data": {}, "debug": {}, "default": {},
	"demo": {}, "dev": {}, "developer": {}, "developers": {}, "doc": {},
	"docs": {}, "documentation": {}, "embed": {}, "error": {}, "errors": {},
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

// A user who sets a display name while holding no handle is given one derived
// from it, so that everyone is addressable from the start without having to
// claim a handle (which is balance-gated). Any display name counts, not only the
// first: a user whose earlier name could not be spelled as a handle, or found no
// number free, gets one from the next name that can, and a user who predates
// default handles gets one on their next rename. The default handle is the display
// name lowercased with spaces as underscores, always followed by a number from 2
// up: the bare handle is the valuable one, and stays free for whoever claims it
// through SetUsername. The number is the lowest one nobody holds at the time of
// assignment, so a number freed when its holder changes handle is handed out
// again before any higher one. Only contention bends that: an assignment that
// loses its number to a concurrent one of the same name retries on a random one
// of the lowest few free numbers, and failing those on a random number of four or
// five digits (see AssignDefaultUsername). The lowest-number search stops at
// maxLowDefaultUsernameNumber, so a name held that many times over is only ever
// given random numbers above it.

// ErrNoDefaultUsername is returned when an assignment claims no handle: every
// candidate it is allowed to consider is held, unusable, or lost to another
// holder while it tried.
var ErrNoDefaultUsername = errors.New("no default username available")

// defaultUsernameDisplayNamePattern is the set of display names a default handle
// is derived from. Anything else (punctuation, emoji, letters outside ASCII) has
// no faithful spelling as a handle, so such a user gets none.
var defaultUsernameDisplayNamePattern = regexp.MustCompile(`^[A-Za-z0-9 ]+$`)

// maxLowDefaultUsernameNumber is the highest number the lowest-number search
// considers. Past it, finding the lowest free number would take lookups large
// enough for Postgres to stop answering them from the index, so numbers above it
// are only handed out at random.
const maxLowDefaultUsernameNumber = 1_000

// minDefaultUsernameNumber is the lowest number a default handle carries, leaving
// the bare handle to whoever claims it.
const minDefaultUsernameNumber = 2

// firstDefaultUsernameBatchSize is how many numbers the lowest-number search's
// first lookup covers. Almost every base resolves in it.
const firstDefaultUsernameBatchSize = 100

// defaultUsernameBatchSizes is how many numbers each successive lookup covers,
// together exactly minDefaultUsernameNumber through maxLowDefaultUsernameNumber:
// the first batch, then the rest in one more lookup.
var defaultUsernameBatchSizes = []int{
	firstDefaultUsernameBatchSize,
	maxLowDefaultUsernameNumber - minDefaultUsernameNumber + 1 - firstDefaultUsernameBatchSize,
}

// maxLowestDefaultUsernameAttempts bounds how many times an assignment searches
// the low numbers, its first attempt included, before falling back to a random
// number. Each loss means another user committed the handle it wanted, so only
// that many users taking the same name at the same moment exhausts it.
const maxLowestDefaultUsernameAttempts = 5

// defaultUsernameRetrySpread is how many of the lowest free numbers a retry picks
// among. Assignments that just collided are likely to be retrying together, and
// would each find the same lowest number again; spreading them over a few keeps
// the next round from being the same race, at the cost of a number a little
// above the lowest for whoever was contended.
const defaultUsernameRetrySpread = 10

// maxRandomDefaultUsernameAttempts bounds how many random numbers are tried once
// the lowest free number cannot be had. With 98,999 numbers to draw from, a draw
// collides with a held handle only for a name that has nearly used them up.
const maxRandomDefaultUsernameAttempts = 5

// minRandomDefaultUsernameNumber and maxRandomDefaultUsernameNumber bound the
// numbers the random fallback draws: everything above what the lowest-number
// search covers, so a draw never lands on a number it just found held, up to five
// digits, which leaves the stem at least nine characters.
const (
	minRandomDefaultUsernameNumber = maxLowDefaultUsernameNumber + 1
	maxRandomDefaultUsernameNumber = 99_999
)

// HeldUsernamesFunc reports which of the given canonical handles currently have a
// holder. Handles nobody holds are absent from the returned set.
type HeldUsernamesFunc func(ctx context.Context, usernames []string) (map[string]struct{}, error)

// ClaimUsernameFunc gives the user being assigned a handle the given one. It
// returns ErrUsernameTaken when another user holds it, which AssignDefaultUsername
// answers by trying another number, and false when the user turned out to be no
// longer eligible for one (a concurrent write gave them a handle first), in which
// case nothing was claimed.
type ClaimUsernameFunc func(ctx context.Context, username string) (bool, error)

// AssignDefaultUsername claims a default handle for base and returns it, or ""
// when claim reported the user is no longer eligible for one.
//
// Its first attempt claims the lowest free number. Each time a concurrent
// assignment takes a number first it searches again, and claims one picked at
// random from the lowest defaultUsernameRetrySpread free numbers rather than the
// lowest, so racing assignments of the same name drift apart instead of meeting
// again. If it loses maxLowestDefaultUsernameAttempts times, or every number the
// search considers is held, it falls back to claiming random numbers from
// minRandomDefaultUsernameNumber to maxRandomDefaultUsernameNumber — drawn only
// from the digit counts whose cut of base leaves a usable stem, so that no draw is
// wasted on a number that could never be a handle.
//
// ErrNoDefaultUsername is returned when no handle could be claimed: no number
// considered yields a usable handle, or every one tried was lost to another
// holder. Either way the caller leaves the user without a handle rather than
// failing, since a contended name has all but run out of numbers anyway.
func AssignDefaultUsername(ctx context.Context, base string, held HeldUsernamesFunc, claim ClaimUsernameFunc) (string, error) {
	return assignDefaultUsername(ctx, base, held, claim, rand.IntN)
}

// assignDefaultUsername is AssignDefaultUsername with its source of randomness,
// which returns a number in [0, n), passed in.
func assignDefaultUsername(ctx context.Context, base string, held HeldUsernamesFunc, claim ClaimUsernameFunc, randomIntN func(n int) int) (string, error) {
	// try claims username, reporting whether the attempt is over (claimed, or
	// failed for a reason another number cannot fix).
	var claimed string
	try := func(username string) (bool, error) {
		ok, err := claim(ctx, username)
		if errors.Is(err, ErrUsernameTaken) {
			return false, nil
		} else if err != nil {
			return true, err
		}
		if ok {
			claimed = username
		}
		return true, nil
	}

	for attempt := range maxLowestDefaultUsernameAttempts {
		free, err := findFreeDefaultUsernames(ctx, base, held, defaultUsernameRetrySpread)
		if errors.Is(err, ErrNoDefaultUsername) {
			break
		} else if err != nil {
			return "", err
		}

		// An uncontended assignment always gets the lowest number; only one that
		// has already lost a race is spread.
		username := free[0]
		if attempt > 0 {
			username = free[randomIntN(len(free))]
		}

		done, err := try(username)
		if done {
			return claimed, err
		}
	}

	// Uniform over the usable ranges. A draw needs to be hard to collide with, not
	// hard to predict: a handle is public anyway. When no range is usable there is
	// nothing to draw, and the assignment ends here.
	ranges := randomDefaultUsernameRanges(base)
	var total int
	for _, r := range ranges {
		total += r.size()
	}
	for range maxRandomDefaultUsernameAttempts {
		if total == 0 {
			break
		}

		// A usable stem makes nearly every number in its range usable, but a stem
		// of digits can still spell a reserved word together with the number
		// ("5" and 4135 read as "sales"), which is skipped like a collision would
		// not be: it contends with nobody.
		username, ok := defaultUsernameCandidate(base, nthNumber(ranges, randomIntN(total)))
		if !ok {
			continue
		}

		done, err := try(username)
		if done {
			return claimed, err
		}
	}

	return "", ErrNoDefaultUsername
}

// DefaultUsernameBase returns the stem default handles for displayName are built
// from, or false when displayName is not eligible for one: it must contain only
// ASCII letters, digits and spaces, and must not name a reserved word.
//
// Leading and trailing spaces are dropped, and each run of spaces in between
// becomes a single underscore, so "Jeff  Yanta" cannot mint a handle that
// differs from "Jeff Yanta"'s by an extra underscore.
func DefaultUsernameBase(displayName string) (string, bool) {
	if !defaultUsernameDisplayNamePattern.MatchString(displayName) {
		return "", false
	}

	words := strings.Fields(strings.ToLower(displayName))
	if len(words) == 0 {
		return "", false
	}

	base := strings.Join(words, "_")
	if IsUsernameReserved(base) {
		return "", false
	}
	return base, true
}

// defaultUsernameCandidate returns the handle numbered n (n >= 2) for base, or
// false when that number has no usable handle: its stem is unusable (see
// defaultUsernameStem), or the handle as a whole spells a reserved word.
func defaultUsernameCandidate(base string, n int) (string, bool) {
	number := strconv.Itoa(n)

	stem, ok := defaultUsernameStem(base, len(number))
	if !ok {
		return "", false
	}
	return defaultUsernameCandidateWithStem(stem, number)
}

// defaultUsernameCandidateWithStem is defaultUsernameCandidate for a number whose
// usable stem is already known. The handle as a whole is still checked, since the
// number can complete a reserved word the stem alone does not spell.
func defaultUsernameCandidateWithStem(stem, number string) (string, bool) {
	candidate := stem + "_" + number
	if ValidateUsername(candidate) != nil || IsUsernameReserved(candidate) {
		return "", false
	}
	return candidate, true
}

// defaultUsernameStem returns the part of base a handle keeps ahead of a number
// with the given count of digits, or false when that leaves nothing usable.
//
// The stem is cut so the whole handle fits maxUsernameLength, which makes it a
// function of the digit count alone: "christopher_johnson" gives
// "christopher_j_2" but "christopher_10". An underscore left dangling by the cut
// is dropped, so the number is always set off by exactly one. A cut can also
// leave a stem that is reserved where the whole base was not ("developers_x" cut
// to "developers"), which makes every number of that length unusable.
func defaultUsernameStem(base string, digits int) (string, bool) {
	stem := base
	if maxStem := maxUsernameLength - 1 - digits; len(stem) > maxStem {
		stem = stem[:max(maxStem, 0)]
	}
	stem = strings.TrimRight(stem, "_")
	if stem == "" || IsUsernameReserved(stem) {
		return "", false
	}
	return stem, true
}

// numberRange is the inclusive range of numbers [lo, hi].
type numberRange struct {
	lo, hi int
}

func (r numberRange) size() int {
	return r.hi - r.lo + 1
}

// randomDefaultUsernameRanges returns the parts of the random fallback's range
// whose numbers leave base a usable stem. The stem depends only on how many
// digits a number has, so the range is split by digit count and each part kept
// or dropped whole.
func randomDefaultUsernameRanges(base string) []numberRange {
	var ranges []numberRange
	lo := minRandomDefaultUsernameNumber

	// Each power of ten bounds the numbers with one digit more than the last.
	for bound := 10; lo <= maxRandomDefaultUsernameNumber; bound *= 10 {
		if lo >= bound {
			continue
		}
		hi := min(bound-1, maxRandomDefaultUsernameNumber)

		if _, ok := defaultUsernameStem(base, len(strconv.Itoa(lo))); ok {
			ranges = append(ranges, numberRange{lo: lo, hi: hi})
		}
		lo = hi + 1
	}
	return ranges
}

// nthNumber returns the i'th number (from 0) across ranges taken in order.
func nthNumber(ranges []numberRange, i int) int {
	for _, r := range ranges {
		if i < r.size() {
			return r.lo + i
		}
		i -= r.size()
	}
	panic("index outside ranges")
}

// findFreeDefaultUsernames returns up to limit of the lowest-numbered default
// handles for base that held reports nobody holds, in ascending order of their
// number, or ErrNoDefaultUsername when there is none within the numbers it
// considers. They all come from the first batch with any free, so a search that
// finds the lowest costs no more lookups for the rest.
//
// The answer is only as fresh as held's view: another user can take a handle
// before the caller writes it, which the unique constraint on the handle catches
// and the caller answers by searching again.
func findFreeDefaultUsernames(ctx context.Context, base string, held HeldUsernamesFunc, limit int) ([]string, error) {
	// The stem depends only on a number's digit count, so it is worked out once
	// per count the search meets rather than once per number.
	type stemResult struct {
		stem string
		ok   bool
	}
	stems := make(map[int]stemResult, 4)

	n := minDefaultUsernameNumber
	for _, batchSize := range defaultUsernameBatchSizes {
		candidates := make([]string, 0, batchSize)
		for end := n + batchSize; n < end; n++ {
			number := strconv.Itoa(n)

			stem, known := stems[len(number)]
			if !known {
				stem.stem, stem.ok = defaultUsernameStem(base, len(number))
				stems[len(number)] = stem
			}
			if !stem.ok {
				continue
			}

			if candidate, ok := defaultUsernameCandidateWithStem(stem.stem, number); ok {
				candidates = append(candidates, candidate)
			}
		}
		if len(candidates) == 0 {
			continue
		}

		taken, err := held(ctx, candidates)
		if err != nil {
			return nil, err
		}

		var free []string
		for _, candidate := range candidates {
			if _, ok := taken[candidate]; ok {
				continue
			}
			free = append(free, candidate)
			if len(free) == limit {
				break
			}
		}
		if len(free) > 0 {
			return free, nil
		}
	}
	return nil, ErrNoDefaultUsername
}

// FirstUsernameHandler is told when a user is given a handle while holding
// none: claimed with SetUsername, or assigned by default with a display name
// (see SetDisplayNameWithDefaultUsername). A handle is never released without
// another taking its place, so this is the first handle the user ever holds.
//
// It is called on the request path, after the handle is written, and cannot
// fail the request: an implementation returns promptly and does its work
// elsewhere. It may be called more than once for a user, as when two claims by
// them race, so what it does is idempotent per user. username is in canonical
// form, as held.
type FirstUsernameHandler interface {
	OnFirstUsername(ctx context.Context, userID *commonpb.UserId, username string)
}

// ServerOption configures a Server beyond its dependencies.
type ServerOption func(*Server)

// WithFirstUsernameHandler has the Server tell h about every user's first
// handle (see FirstUsernameHandler). Nil, the default, tells no one.
func WithFirstUsernameHandler(h FirstUsernameHandler) ServerOption {
	return func(s *Server) { s.firstUsername = h }
}

// onFirstUsername tells the handler, if there is one, that userID was given
// username while holding none.
func (s *Server) onFirstUsername(ctx context.Context, userID *commonpb.UserId, username string) {
	if s.firstUsername != nil {
		s.firstUsername.OnFirstUsername(ctx, userID, username)
	}
}

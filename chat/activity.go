package chat

import (
	"math"
	"time"
)

// A group's activity score is a frequency-weighted ordering of its senders
// kept on each activity record beside the last send (see RecentSender): a
// record ranks above another when its recorded sends, each weighted and each
// decaying with ActivityScoreHalfLife, sum to more as of any one moment.
//
// A send is weighted by the quiet before it: the time since the user's
// previous recorded send over ActivityScoreFullWeightGap, at most one, and
// one for their first. So the score counts the days a user shows up rather
// than the messages they send: a day of chatting weighs about what one
// message that day would, and an hour-long burst barely more than its first
// message, however many sends the throttle (ActivityRecordInterval) lets
// through.
//
// It is stored as a time, not a weight: the moment at which a single send
// would rank equal to the record. A user with one recorded send scores their
// send time, and further sends move the score ahead of the last one,
// possibly past now. In that form the decay never moves a score: comparing
// two decayed sums at any moment compares their scores, because the moment
// cancels out of the comparison, so a store can keep the score as a sort key
// and rewrite it only on a recorded send. A record written before scores
// existed reads as scoring its last send, which is exactly its score had it
// had one send (see EffectiveActivityScore). A score is never below its last
// send, so a record never ranks below someone who sent once, after it.
//
// A user who sends every day settles about 6.8 days ahead of their last
// send, which is the most any steady pattern reaches at a 3-day half-life
// and a 1-day full-weight gap; one who sends weekly, about a day. That lead
// is how long they rank above someone who sends once after them.
// ActivityScoreMaxLead bounds it whatever the history; at these values no
// steady pattern reaches it.
//
// Nothing ranks by it yet.
const (
	// ActivityScoreHalfLife is how long a recorded send takes to count half
	// as much toward an activity score.
	ActivityScoreHalfLife = 72 * time.Hour

	// ActivityScoreFullWeightGap is the quiet before a send at which it
	// counts in full toward an activity score; a send after less counts in
	// proportion.
	ActivityScoreFullWeightGap = 24 * time.Hour

	// ActivityScoreMaxLead is the furthest an activity score may run ahead of
	// the send that set it.
	ActivityScoreMaxLead = 7 * 24 * time.Hour
)

// NextActivityScore returns an activity record's score after a send at sentAt
// is recorded on it, given its score before (see EffectiveActivityScore) and
// its last recorded send before, or the zero time for both for a user with no
// record. The result is at millisecond precision, like the record's send
// time, never before sentAt, and never below prior, so a score only moves
// forward with the sends that set it.
func NextActivityScore(prior, lastSentAt, sentAt time.Time) time.Time {
	t := float64(sentAt.UnixMilli())
	if prior.IsZero() {
		return time.UnixMilli(int64(t)).UTC()
	}
	s := float64(prior.UnixMilli())

	weight := min(1, float64(sentAt.Sub(lastSentAt))/float64(ActivityScoreFullWeightGap))
	next := math.Max(s, t)
	if weight > 0 {
		// τ·ln(e^(s/τ) + w·e^(t/τ)), arranged so neither exponential
		// overflows.
		tau := float64(ActivityScoreHalfLife.Milliseconds()) / math.Ln2
		u := t + tau*math.Log(weight)
		next = math.Max(s, u) + tau*math.Log1p(math.Exp(-math.Abs(s-u)/tau))
		next = math.Min(next, t+float64(ActivityScoreMaxLead.Milliseconds()))
		next = math.Max(next, math.Max(s, t))
	}
	return time.UnixMilli(int64(math.Round(next))).UTC()
}

// EffectiveActivityScore returns the score of an activity record whose last
// recorded send is lastSentAt and whose stored score is score, the zero time
// when it has none. A record with no score, written before scores existed,
// scores its last send. So does one whose score trails its last send, which
// only a writer that records sends without scoring them leaves behind, at the
// cost of the sends it did not score: a score is never below the last send.
func EffectiveActivityScore(score, lastSentAt time.Time) time.Time {
	if score.Before(lastSentAt) {
		return lastSentAt
	}
	return score
}

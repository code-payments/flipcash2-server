package chat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNextActivityScore(t *testing.T) {
	start := time.Unix(1_700_000_000, 0).UTC()
	day := 24 * time.Hour

	// A first send scores its own time, at millisecond precision.
	require.Equal(t, start, NextActivityScore(time.Time{}, time.Time{}, start))
	require.Equal(t, start.Add(time.Millisecond), NextActivityScore(time.Time{}, time.Time{}, start.Add(1_500*time.Microsecond)))

	// A send after no quiet counts for nothing.
	require.Equal(t, start, NextActivityScore(start, start, start))

	// A send after part of the full-weight gap counts in proportion: weight w
	// at t is one full send at t + half-life·log2(w), so a send after half
	// the gap counts as a full send one half-life before it.
	half := start.Add(ActivityScoreFullWeightGap / 2)
	equivalent := half.Add(-ActivityScoreHalfLife)
	requireWithinMilli(t,
		NextActivityScore(start, equivalent.Add(-ActivityScoreFullWeightGap), equivalent),
		NextActivityScore(start, start, half),
	)

	// A send long after the last counts almost alone.
	requireWithinMilli(t, start.Add(365*day), NextActivityScore(start, start, start.Add(365*day)))

	// The lead a steady pattern settles at, in days, after 120 days of it.
	settledLead := func(sendsPerDay int, spacing time.Duration) float64 {
		var score, last time.Time
		for d := range 120 {
			for k := range sendsPerDay {
				sentAt := start.Add(time.Duration(d)*day + time.Duration(k)*spacing)
				score = NextActivityScore(score, last, sentAt)
				require.False(t, score.Before(sentAt))
				require.False(t, score.After(sentAt.Add(ActivityScoreMaxLead)))
				last = sentAt
			}
		}
		return score.Sub(last).Hours() / 24
	}

	// One send a day settles about 6.8 days ahead of the last, and a daily
	// half-hour session, or sending all day, about the same: the score counts
	// days, not messages.
	daily := settledLead(1, 0)
	require.InDelta(t, 6.84, daily, 0.01)
	require.InDelta(t, daily, settledLead(30, ActivityRecordInterval), 0.05)
	require.InDelta(t, 6.4, settledLead(16, time.Hour), 0.05)

	// One send a week, about a day.
	var score, last time.Time
	for w := range 26 {
		sentAt := start.Add(time.Duration(w) * 7 * day)
		score, last = NextActivityScore(score, last, sentAt), sentAt
	}
	require.InDelta(t, 0.96, score.Sub(last).Hours()/24, 0.01)

	// A daily sender outranks a one-off sender who sent after them, until
	// their lead runs out.
	dailyLast := start.Add(119 * day)
	dailyScore := dailyLast.Add(time.Duration(daily * float64(day)))
	require.True(t, dailyScore.After(NextActivityScore(time.Time{}, time.Time{}, dailyLast.Add(6*day))))
	require.True(t, dailyScore.Before(NextActivityScore(time.Time{}, time.Time{}, dailyLast.Add(7*day))))

	// An hour-long burst at the throttle's rate barely counts past its first
	// send.
	score, last = time.Time{}, time.Time{}
	for k := range 60 {
		sentAt := start.Add(time.Duration(k) * ActivityRecordInterval)
		score, last = NextActivityScore(score, last, sentAt), sentAt
	}
	require.Less(t, score.Sub(last).Hours()/24, 0.15)

	// A score is held to ActivityScoreMaxLead ahead of the send that set it.
	require.Equal(t, start.Add(ActivityScoreMaxLead), NextActivityScore(start.Add(6*day+21*time.Hour), start.Add(-day), start))

	// But a score never moves backwards, even when it is already further
	// ahead of a new send than that.
	prior := start.Add(8 * day)
	require.Equal(t, prior, NextActivityScore(prior, start, start.Add(day)))
}

func TestEffectiveActivityScore(t *testing.T) {
	lastSentAt := time.Unix(1_700_000_000, 0).UTC()

	// No score: the last send.
	require.Equal(t, lastSentAt, EffectiveActivityScore(time.Time{}, lastSentAt))

	// A score trailing the last send is lifted to it.
	require.Equal(t, lastSentAt, EffectiveActivityScore(lastSentAt.Add(-time.Hour), lastSentAt))

	// A score at or ahead of it stands.
	require.Equal(t, lastSentAt, EffectiveActivityScore(lastSentAt, lastSentAt))
	require.Equal(t, lastSentAt.Add(time.Hour), EffectiveActivityScore(lastSentAt.Add(time.Hour), lastSentAt))
}

func requireWithinMilli(t *testing.T, want, got time.Time) {
	t.Helper()
	diff := got.Sub(want)
	require.True(t, diff >= -time.Millisecond && diff <= time.Millisecond, "got %v, want %v", got, want)
}

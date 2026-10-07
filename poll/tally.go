package poll

import (
	"context"
	"fmt"
	"math"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	pollpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/poll/v1"

	currency_lib "github.com/code-payments/ocp-server/currency"

	"github.com/code-payments/flipcash2-server/balance"
)

const (
	// minCountVoters is the fewest counted voters a COUNT poll shows totals
	// for (see pollpb.WithheldResults). Below it, totals would say too much
	// about how individual members voted.
	minCountVoters = 3

	// minBalanceVoters is the fewest counted voters holding any of the mint a
	// BALANCE poll shows totals for. It is higher than minCountVoters, and
	// counts only voters who add weight, since members' balances can often be
	// looked up: a total made of one or two balances names whose they are.
	minBalanceVoters = 5

	// balanceSignificantFigures is how many significant figures each BALANCE
	// amount is rounded to (see coarsen).
	balanceSignificantFigures = 2

	// tallyVotesPageSize is how many votes the tally reads at a time.
	tallyVotesPageSize = 100
)

// tally counts a poll's votes and returns its results. It reads the poll's
// votes whole and counts every one: only a member can vote (see Server.Vote),
// so a vote was a member's when it was cast, and it counts whether or not
// they are still one when the poll closes.
//
// A BALANCE poll's balances are read here too, which is when the poll's
// results are computed, not the instant it ended.
//
// It writes nothing, so a tally that fails part way is simply run again.
func tally(ctx context.Context, polls Store, balances *balance.Client, p *Poll) (*pollpb.Results, error) {
	votes, err := allVotes(ctx, polls, p)
	if err != nil {
		return nil, err
	}

	switch p.Definition.GetMode() {
	case pollpb.Mode_COUNT:
		return countResults(p, votes), nil
	case pollpb.Mode_BALANCE:
		parameters := p.Definition.GetBalanceParameters()
		voters := make([]*commonpb.UserId, len(votes))
		for i, vote := range votes {
			voters[i] = vote.UserID
		}
		values, err := balances.GetMintFiatValuesForUsers(ctx, currency_lib.Code(parameters.GetCurrency()), parameters.GetMint(), voters...)
		if err != nil {
			return nil, fmt.Errorf("reading voter balances: %w", err)
		}
		return balanceResults(p, votes, values), nil
	default:
		return nil, fmt.Errorf("unsupported poll mode %s", p.Definition.GetMode())
	}
}

// allVotes returns every vote cast in the poll.
func allVotes(ctx context.Context, polls Store, p *Poll) ([]Vote, error) {
	var (
		votes []Vote
		after *commonpb.UserId
	)
	for {
		page, err := polls.GetVotesPage(ctx, p.ID, after, tallyVotesPageSize)
		if err != nil {
			return nil, fmt.Errorf("reading votes: %w", err)
		}
		if len(page) == 0 {
			return votes, nil
		}
		after = page[len(page)-1].UserID
		votes = append(votes, page...)
	}
}

// countResults are a COUNT poll's results over its counted votes: each voter
// adds one to every option they chose. Too few voters withholds them.
func countResults(p *Poll, votes []Vote) *pollpb.Results {
	if len(votes) < minCountVoters {
		return withheldResults()
	}
	optionVoters := make([]uint64, p.Definition.GetOptionCount())
	for _, vote := range votes {
		for _, option := range vote.Options {
			if int(option) < len(optionVoters) {
				optionVoters[option]++
			}
		}
	}
	return &pollpb.Results{Type: &pollpb.Results_Count{Count: &pollpb.CountResults{
		TotalVoters:  uint64(len(votes)),
		OptionVoters: optionVoters,
	}}}
}

// balanceResults are a BALANCE poll's results over its counted votes, given
// each voter's holdings of the poll's mint valued in its currency (a voter
// absent from values holds none): each voter adds their full balance to every
// option they chose, and to the total once. Too few voters holding any of the
// mint withholds them.
//
// Every amount is coarsened (see coarsen), so an exact sum cannot be taken
// apart into the balances that make it, and carries no quarks: OCP reports a
// voter's holdings of a mint only valued, in the core mint or a currency,
// never in the mint's own quarks, so there are none to sum.
func balanceResults(p *Poll, votes []Vote, values map[string]float64) *pollpb.Results {
	var (
		weighted     int
		total        float64
		optionTotals = make([]float64, p.Definition.GetOptionCount())
	)
	for _, vote := range votes {
		value := values[string(vote.UserID.Value)]
		if value <= 0 {
			continue
		}
		weighted++
		total += value
		for _, option := range vote.Options {
			if int(option) < len(optionTotals) {
				optionTotals[option] += value
			}
		}
	}
	if weighted < minBalanceVoters {
		return withheldResults()
	}

	parameters := p.Definition.GetBalanceParameters()
	amount := func(value float64) *commonpb.CryptoPaymentAmount {
		return &commonpb.CryptoPaymentAmount{
			Currency:     parameters.GetCurrency(),
			NativeAmount: coarsen(value),
			Mint:         proto.Clone(parameters.GetMint()).(*commonpb.PublicKey),
		}
	}
	results := &pollpb.BalanceResults{Total: amount(total)}
	for _, value := range optionTotals {
		results.OptionBalances = append(results.OptionBalances, amount(value))
	}
	return &pollpb.Results{Type: &pollpb.Results_Balance{Balance: results}}
}

func withheldResults() *pollpb.Results {
	return &pollpb.Results{Type: &pollpb.Results_Withheld{Withheld: &pollpb.WithheldResults{}}}
}

// coarsen rounds a non-negative amount to balanceSignificantFigures
// significant figures, and anything not positive to zero.
func coarsen(value float64) float64 {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	// Scaling by a whole power of ten either way, never by its fraction, keeps
	// the result the nearest float to the rounded decimal: 16 / 1e-5 is
	// 1599999.9999999998, while 16 * 1e5 is 1600000.
	exponent := balanceSignificantFigures - 1 - int(math.Floor(math.Log10(value)))
	if exponent >= 0 {
		scale := math.Pow10(exponent)
		return math.Round(value*scale) / scale
	}
	scale := math.Pow10(-exponent)
	return math.Round(value/scale) * scale
}

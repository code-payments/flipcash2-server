package poll

import (
	"context"
	"fmt"

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
// option they chose, and to the total once.
//
// These totals are what the store records, but no reader is shown them: a
// reader sees only the options' ranking (see ResultsProto). That is why they
// are exact and never withheld, however few voters hold any of the mint: a
// ranking reveals no balance.
//
// No amount carries quarks: OCP reports a voter's holdings of a mint only
// valued, in the core mint or a currency, never in the mint's own quarks, so
// there are none to sum.
func balanceResults(p *Poll, votes []Vote, values map[string]float64) *pollpb.Results {
	var (
		total        float64
		optionTotals = make([]float64, p.Definition.GetOptionCount())
	)
	for _, vote := range votes {
		value := values[string(vote.UserID.Value)]
		if value <= 0 {
			continue
		}
		total += value
		for _, option := range vote.Options {
			if int(option) < len(optionTotals) {
				optionTotals[option] += value
			}
		}
	}

	parameters := p.Definition.GetBalanceParameters()
	amount := func(value float64) *commonpb.CryptoPaymentAmount {
		return &commonpb.CryptoPaymentAmount{
			Currency:     parameters.GetCurrency(),
			NativeAmount: value,
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

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
		holdings, err := balances.GetMintHoldingsForUsers(ctx, currency_lib.Code(parameters.GetCurrency()), parameters.GetMint(), voters...)
		if err != nil {
			return nil, fmt.Errorf("reading voter balances: %w", err)
		}
		return balanceResults(p, votes, holdings), nil
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
// adds one to every option they chose. They are never withheld, however few
// voted (see pollpb.WithheldResults, whose minimum the server chooses).
func countResults(p *Poll, votes []Vote) *pollpb.Results {
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
// each voter's holdings of the poll's mint (a voter absent from holdings holds
// none): each voter adds their full balance, both its quarks and its value in
// the poll's currency, to every option they chose, and to the total once.
// Every voter is counted in TotalVoters, including one who holds none.
//
// These totals are what the store records, but no reader is shown them: a
// reader sees only the options' ranking and the voter count (see
// ResultsProto). That is why they
// are exact and never withheld, however few voters hold any of the mint: a
// ranking reveals no balance.
//
// The quark sums cannot overflow: each is at most the voters' combined
// holdings of one mint, which its supply, a uint64, bounds.
func balanceResults(p *Poll, votes []Vote, holdings map[string]balance.MintHolding) *pollpb.Results {
	var (
		total        balance.MintHolding
		optionTotals = make([]balance.MintHolding, p.Definition.GetOptionCount())
	)
	add := func(sum *balance.MintHolding, holding balance.MintHolding) {
		sum.Quarks += holding.Quarks
		sum.FiatValue += holding.FiatValue
	}
	for _, vote := range votes {
		holding, ok := holdings[string(vote.UserID.Value)]
		if !ok {
			continue
		}
		add(&total, holding)
		for _, option := range vote.Options {
			if int(option) < len(optionTotals) {
				add(&optionTotals[option], holding)
			}
		}
	}

	parameters := p.Definition.GetBalanceParameters()
	amount := func(holding balance.MintHolding) *commonpb.CryptoPaymentAmount {
		return &commonpb.CryptoPaymentAmount{
			Currency:     parameters.GetCurrency(),
			NativeAmount: holding.FiatValue,
			Quarks:       holding.Quarks,
			Mint:         proto.Clone(parameters.GetMint()).(*commonpb.PublicKey),
		}
	}
	results := &pollpb.BalanceResults{
		Total:       amount(total),
		TotalVoters: uint64(len(votes)),
	}
	for _, holding := range optionTotals {
		results.OptionBalances = append(results.OptionBalances, amount(holding))
	}
	return &pollpb.Results{Type: &pollpb.Results_Balance{Balance: results}}
}

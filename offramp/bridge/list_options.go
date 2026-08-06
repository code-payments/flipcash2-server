package bridge

import (
	"net/url"
	"strconv"
)

const (
	// MaxPageSize is the largest page Bridge list endpoints return. The
	// default without an explicit limit is only 10.
	MaxPageSize = 100
)

// ListOption is a query parameter for Bridge list endpoints, which use
// cursor-based pagination ordered newest to oldest. At most one of
// WithStartingAfter/WithEndingBefore may be used per call.
type ListOption func(url.Values)

// WithLimit sets the page size (min 1, max MaxPageSize).
func WithLimit(limit int) ListOption {
	return func(v url.Values) {
		v.Set("limit", strconv.Itoa(limit))
	}
}

// WithStartingAfter pages toward older items, starting after the given
// object ID.
func WithStartingAfter(id string) ListOption {
	return func(v url.Values) {
		v.Set("starting_after", id)
	}
}

// WithEndingBefore pages toward newer items, ending before the given
// object ID.
func WithEndingBefore(id string) ListOption {
	return func(v url.Values) {
		v.Set("ending_before", id)
	}
}

// WithUpdatedAfter filters to objects updated after the given unix
// timestamp in milliseconds. Supported by drain listings; used by the
// reconciliation worker to sweep only recent changes.
func WithUpdatedAfter(unixMs int64) ListOption {
	return func(v url.Values) {
		v.Set("updated_after_ms", strconv.FormatInt(unixMs, 10))
	}
}

// WithTxHash filters drains to those matching a deposit or destination
// transaction hash.
func WithTxHash(txHash string) ListOption {
	return func(v url.Values) {
		v.Set("tx_hash", txHash)
	}
}

func listQuery(path string, opts []ListOption) string {
	if len(opts) == 0 {
		return path
	}
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	return path + "?" + values.Encode()
}

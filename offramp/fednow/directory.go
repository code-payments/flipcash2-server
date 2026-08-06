package fednow

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/code-payments/ocp-server/metrics"
)

const (
	// defaultListPageURL is the Federal Reserve page that links to the current
	// FedNow participant RTN file. The file itself has a date-stamped URL that
	// rotates on every update, so it is discovered from this page on each
	// refresh.
	defaultListPageURL = "https://www.frbservices.org/financial-services/fednow/organizations/terms-of-use"

	frbServicesBaseURL = "https://www.frbservices.org"

	metricsStructName = "offramp.fednow.directory"

	defaultTimeout = 30 * time.Second
)

// rtnFilePattern matches the date-stamped RTN list path linked from the
// participants page, e.g. /binaries/.../fednow/rtn-07202026.txt.
var rtnFilePattern = regexp.MustCompile(`/binaries/[^"'\s]*/fednow/rtn-\d+\.txt`)

// Directory is an in-memory lookup of FedNow participant routing numbers,
// refreshed from the Federal Reserve's published participant list.
//
// The published list identifies participating institutions by RTN only; it
// carries no per-account guarantees. Lookups are advisory for rail selection:
// a hit means instant payout is likely deliverable, a miss means fall back to
// ACH. The list is updated irregularly by the Fed, so refresh failures leave
// the previous data in place rather than clearing it.
type Directory struct {
	listPageURL string
	fileURL     string // optional override; skips discovery
	httpClient  *http.Client

	participants atomic.Pointer[map[string]struct{}]
	refreshedAt  atomic.Pointer[time.Time]
}

type Option func(*Directory)

// WithListPageURL overrides the page used to discover the current RTN file.
func WithListPageURL(url string) Option {
	return func(d *Directory) {
		d.listPageURL = url
	}
}

// WithFileURL pins the RTN file URL directly, skipping page discovery.
func WithFileURL(url string) Option {
	return func(d *Directory) {
		d.fileURL = url
	}
}

// WithHTTPClient overrides the underlying HTTP client.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(d *Directory) {
		d.httpClient = httpClient
	}
}

// NewDirectory returns an empty directory. Call Refresh before serving
// lookups; until then IsParticipant returns false for all inputs.
func NewDirectory(opts ...Option) *Directory {
	d := &Directory{
		listPageURL: defaultListPageURL,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// IsParticipant reports whether the routing number appears in the most
// recently loaded FedNow participant list. Returns false when the directory
// has never been successfully refreshed.
func (d *Directory) IsParticipant(routingNumber string) bool {
	participants := d.participants.Load()
	if participants == nil {
		return false
	}
	_, ok := (*participants)[routingNumber]
	return ok
}

// Size returns the number of loaded participant routing numbers.
func (d *Directory) Size() int {
	participants := d.participants.Load()
	if participants == nil {
		return 0
	}
	return len(*participants)
}

// LastRefreshedAt returns when the directory was last successfully refreshed,
// or the zero time if it never has been.
func (d *Directory) LastRefreshedAt() time.Time {
	if t := d.refreshedAt.Load(); t != nil {
		return *t
	}
	return time.Time{}
}

// Refresh fetches and atomically swaps in the current participant list. On
// any failure the previously loaded list is left untouched, so a transient
// fetch or format problem cannot wipe rail selection data.
func (d *Directory) Refresh(ctx context.Context) error {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "Refresh")
	defer tracer.End()

	err := d.refresh(ctx)
	tracer.OnError(err)
	return err
}

func (d *Directory) refresh(ctx context.Context) error {
	fileURL := d.fileURL
	if fileURL == "" {
		discovered, err := d.discoverFileURL(ctx)
		if err != nil {
			return err
		}
		fileURL = discovered
	}

	body, err := d.get(ctx, fileURL)
	if err != nil {
		return err
	}
	defer body.Close()

	participants, err := parseRTNList(body)
	if err != nil {
		return err
	}

	d.participants.Store(&participants)
	now := time.Now()
	d.refreshedAt.Store(&now)
	return nil
}

// discoverFileURL finds the current date-stamped RTN file link on the
// participants page.
func (d *Directory) discoverFileURL(ctx context.Context) (string, error) {
	body, err := d.get(ctx, d.listPageURL)
	if err != nil {
		return "", err
	}
	defer body.Close()

	page, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}

	match := rtnFilePattern.Find(page)
	if match == nil {
		return "", fmt.Errorf("fednow: no RTN file link found at %s", d.listPageURL)
	}
	return frbServicesBaseURL + string(match), nil
}

func (d *Directory) get(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("fednow: unexpected http status code %d from %s", resp.StatusCode, url)
	}
	return resp.Body, nil
}

// parseRTNList reads the participant file (one routing number per line),
// keeping only well-formed entries. Failing entirely on an empty result
// guards against silently loading an error page or a changed format.
func parseRTNList(r io.Reader) (map[string]struct{}, error) {
	participants := make(map[string]struct{})

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if IsValidRoutingNumber(line) {
			participants[line] = struct{}{}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if len(participants) == 0 {
		return nil, fmt.Errorf("fednow: participant list contained no valid routing numbers")
	}
	return participants, nil
}

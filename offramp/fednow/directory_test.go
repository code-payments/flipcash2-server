package fednow

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsValidRoutingNumber(t *testing.T) {
	for value, expected := range map[string]bool{
		"011001234":  true,  // from the Fed's published list
		"101019644":  true,  // Lead Bank (Bridge docs example)
		"121000248":  true,  // Wells Fargo
		"123456789":  false, // bad checksum
		"011001235":  false, // single digit typo
		"01100123":   false, // too short
		"0110012345": false, // too long
		"01100123a":  false, // non-digit
		"":           false,
	} {
		assert.Equal(t, expected, IsValidRoutingNumber(value), "routing number %q", value)
	}
}

func newTestDirectory(t *testing.T, pageHTML, rtnFile string) (*Directory, *int) {
	fileFetches := 0
	mux := http.NewServeMux()

	var server *httptest.Server
	mux.HandleFunc("/participants", func(w http.ResponseWriter, r *http.Request) {
		// Rewrite the page HTML so the discovered link points at this server.
		fmt.Fprint(w, pageHTML)
	})
	mux.HandleFunc("/binaries/content/assets/crsocms/financial-services/fednow/rtn-07202026.txt", func(w http.ResponseWriter, r *http.Request) {
		fileFetches++
		fmt.Fprint(w, rtnFile)
	})
	server = httptest.NewServer(mux)
	t.Cleanup(server.Close)

	directory := NewDirectory(
		WithListPageURL(server.URL+"/participants"),
		WithHTTPClient(server.Client()),
	)
	// Discovery produces frbServicesBaseURL-prefixed URLs; point the file URL
	// directly at the test server instead.
	directory.fileURL = server.URL + "/binaries/content/assets/crsocms/financial-services/fednow/rtn-07202026.txt"
	return directory, &fileFetches
}

func TestDirectory_RefreshAndLookup(t *testing.T) {
	directory, _ := newTestDirectory(t, "", "011001234\n101019644\n\nnot-a-number\n123456789\n")

	require.Zero(t, directory.Size())
	assert.False(t, directory.IsParticipant("011001234"))
	assert.True(t, directory.LastRefreshedAt().IsZero())

	require.NoError(t, directory.Refresh(context.Background()))

	assert.Equal(t, 2, directory.Size())
	assert.True(t, directory.IsParticipant("011001234"))
	assert.True(t, directory.IsParticipant("101019644"))
	assert.False(t, directory.IsParticipant("123456789")) // invalid checksum skipped
	assert.False(t, directory.IsParticipant("121000248")) // valid but not listed
	assert.False(t, directory.LastRefreshedAt().IsZero())
}

func TestDirectory_RefreshFailureKeepsExistingData(t *testing.T) {
	directory, _ := newTestDirectory(t, "", "011001234\n")
	require.NoError(t, directory.Refresh(context.Background()))
	require.Equal(t, 1, directory.Size())

	// Point at a URL that returns garbage with no valid routing numbers; the
	// previously loaded list must survive.
	goodFileURL := directory.fileURL
	badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "<html>maintenance page</html>")
	}))
	t.Cleanup(badServer.Close)
	directory.fileURL = badServer.URL

	require.Error(t, directory.Refresh(context.Background()))
	assert.Equal(t, 1, directory.Size())
	assert.True(t, directory.IsParticipant("011001234"))

	directory.fileURL = goodFileURL
	require.NoError(t, directory.Refresh(context.Background()))
	assert.Equal(t, 1, directory.Size())
}

func TestDirectory_DiscoverFileURL(t *testing.T) {
	pageHTML := `<html><body>
		<a href="/binaries/content/assets/crsocms/financial-services/fednow/rtn-07202026.txt">RTN list</a>
	</body></html>`
	directory, _ := newTestDirectory(t, pageHTML, "011001234\n")

	// Force the discovery path.
	directory.fileURL = ""

	url, err := directory.discoverFileURL(context.Background())
	require.NoError(t, err)
	assert.Equal(t, frbServicesBaseURL+"/binaries/content/assets/crsocms/financial-services/fednow/rtn-07202026.txt", url)
}

func TestDirectory_DiscoverFileURL_NoLink(t *testing.T) {
	directory, _ := newTestDirectory(t, "<html>nothing here</html>", "")
	directory.fileURL = ""

	_, err := directory.discoverFileURL(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no RTN file link")
}

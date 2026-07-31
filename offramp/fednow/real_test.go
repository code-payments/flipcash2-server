//go:build integration

package fednow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDirectory_RealFedEndpoint exercises discovery, download, and parsing
// against the live Federal Reserve participant list.
func TestDirectory_RealFedEndpoint(t *testing.T) {
	directory := NewDirectory()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	require.NoError(t, directory.Refresh(ctx))

	t.Logf("loaded %d participant routing numbers", directory.Size())

	// The network has thousands of participants; a drastically smaller result
	// suggests a parsing or format regression.
	assert.Greater(t, directory.Size(), 1000)

	// Spot-check an entry from the published list.
	assert.True(t, directory.IsParticipant("011001234"))

	// A checksum-valid routing number that should not be resolvable as a
	// FedNow participant (test/example RTN range).
	assert.False(t, directory.IsParticipant("999999992"))
}

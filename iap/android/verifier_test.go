//go:build androidIntegration

package android

import (
	"testing"

	"github.com/code-payments/flipcash2-server/iap/tests"
)

// todo: Update with Flipcash examples

func TestAndroidVerifier(t *testing.T) {
	// From real Android app on real environment.
	testPurchaseToken := "gcjkgkiehhchodpancdfjgfo.AO-J1OyEz6mLitFxK7gDOBN0iv4_9f5Xc6dIAdK_tLj2SGi9msJz-R5Xo3PcbC3fUYdG9SeQ6ngy2nwLe-LW2ORtPt6JQZte4w"

	// From test environment.
	//testPurchaseToken := "cmpkkdbgkebjhnalcgjinpba.AO-J1OzkqS9nR3iaT5C8C6HfVp_dqWvYoVjt8HACHXKDCXNioqPifcOxx3g33mZ36OAYqQvzxnUUX_YkNgRvlzSYQ7vBD6wRsQ"

	// TODO: Replace this with a real serviceAccount json.
	serviceAccount := []byte(`{}`)

	verifier := NewAndroidVerifier(
		serviceAccount,
		"xyz.flipchat.app",
	)

	// The test harness requires a MessageGenerator function. For Android receipts,
	// the concept of "message" doesn't strictly apply, so we provide a dummy function.
	messageGenerator := func() string {
		return "unused_in_android_verifier"
	}

	// validReceiptFunc simulates returning the Android app's receipt
	validReceiptFunc := func(_ string) (string, string) {
		return testPurchaseToken, "com.flipchat.iap.createaccount"
	}

	// No-op teardown.
	teardown := func() {}

	tests.RunGenericVerifierTests(t, verifier, messageGenerator, validReceiptFunc, teardown)
}

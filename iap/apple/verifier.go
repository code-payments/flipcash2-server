package apple

import (
	"context"

	"github.com/devsisters/go-applereceipt"
	"github.com/devsisters/go-applereceipt/applepki"

	"github.com/code-payments/ocp-server/pkg/metrics"

	"github.com/code-payments/flipcash2-server/iap"
)

const (
	metricsStructName = "iap.apple.verifier"
)

type AppleVerifier struct {
	// PackageName is the app's package name, e.g. "com.flipchat.app".
	packageName string
}

func NewAppleVerifier(pkgName string) iap.Verifier {
	return &AppleVerifier{
		packageName: pkgName,
	}
}

func (m *AppleVerifier) VerifyReceipt(ctx context.Context, receipt, product string) (bool, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "VerifyReceipt")
	defer tracer.End()

	res, err := func() (bool, error) {
		receipt, err := applereceipt.DecodeBase64(receipt, applepki.CertPool())
		if err != nil {
			return false, err
		}

		// Verify the bundle ID.
		if receipt.BundleIdentifier != m.packageName {
			return false, nil
		}

		// NOTE: this is omitted because Apple may not provide it as part of the envelope.
		// See https://developer.apple.com/library/archive/releasenotes/General/ValidateAppStoreReceipt/Chapters/ReceiptFields.html

		// Verify the that the receipt is for the correct product.
		//if receipt.InAppPurchaseReceipts[0].ProductIdentifier != product {
		//	return false, nil
		//}

		return true, nil
	}()

	tracer.OnError(err)

	return res, err
}

func (m *AppleVerifier) GetReceiptIdentifier(ctx context.Context, encodedReceipt string) ([]byte, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "GetReceiptIdentifier")
	defer tracer.End()

	res, err := func() ([]byte, error) {
		// TODO: adjust this so that verification and getting the identifier don't
		// require decoding the receipt twice. Once we know how to decode an Android
		// receipt we can do this.

		receipt, err := applereceipt.DecodeBase64(encodedReceipt, applepki.CertPool())
		if err != nil {
			return nil, err
		}

		return receipt.SHA1Hash, nil
	}()

	tracer.OnError(err)

	return res, err
}

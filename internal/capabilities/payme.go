package capabilities

import (
	"fmt"
	"net/url"

	"github.com/ipld/go-ipld-prime/datamodel"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
)

const PayMeAbility = "content/serve/payme"

type PayMeCaveats struct {
	Receipts []ucan.Link
	Endpoint *url.URL
}

func (pmc PayMeCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&pmc, PayMeCaveatsType(), captypes.Converters...)
}

var PayMeCaveatsReader = schema.Struct[PayMeCaveats](PayMeCaveatsType(), nil, captypes.Converters...)

type PayMeOk struct {
}

func (pmo PayMeOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&pmo, PayMeOkType(), captypes.Converters...)
}

type PayMeReceipt receipt.Receipt[PayMeOk, failure.Failure]

type PayMeReceiptReader receipt.ReceiptReader[PayMeOk, failure.Failure]

func NewPayMeReceiptReader() (PayMeReceiptReader, error) {
	return receipt.NewReceiptReader[PayMeOk, failure.Failure](paymeSchema)
}

var PayMeOkReader = schema.Struct[PayMeOk](PayMeOkType(), nil, captypes.Converters...)

// PayMe capability definition
// This capability allows a storage node to request recording egress for content it has served.
var PayMe = validator.NewCapability(
	PayMeAbility,
	schema.DIDString(),
	schema.Struct[PayMeCaveats](nil, nil, captypes.Converters...),
	func(claimed, delegated ucan.Capability[PayMeCaveats]) failure.Failure {
		if fail := equalWith(claimed, delegated); fail != nil {
			return fail
		}

		return nil
	},
)

// equalWith validates that the claimed capability's `with` field matches the delegated one.
func equalWith[Caveats any](claimed, delegated ucan.Capability[Caveats]) failure.Failure {
	if claimed.With() != delegated.With() {
		return schema.NewSchemaError(fmt.Sprintf(
			"Resource '%s' doesn't match delegated '%s'",
			claimed.With(), delegated.With(),
		))
	}

	return nil
}

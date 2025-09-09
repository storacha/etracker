package egress

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
)

const ConsolidateAbility = "space/egress/consolidate"

type ConsolidateCaveats struct {
	Cause ucan.Link
}

func (cc ConsolidateCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&cc, ConsolidateCaveatsType(), captypes.Converters...)
}

var ConsolidateCaveatsReader = schema.Struct[ConsolidateCaveats](ConsolidateCaveatsType(), nil, captypes.Converters...)

type Error struct {
	Receipt ucan.Link
	Error   string
}

type ConsolidateOk struct {
	Errors []Error
}

func (co ConsolidateOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&co, ConsolidateOkType(), captypes.Converters...)
}

type ConsolidateReceipt receipt.Receipt[ConsolidateOk, failure.Failure]

type ConsolidateReceiptReader receipt.ReceiptReader[ConsolidateOk, failure.Failure]

func NewConsolidateReceiptReader() (ConsolidateReceiptReader, error) {
	return receipt.NewReceiptReader[ConsolidateOk, failure.Failure](egressSchema)
}

var ConsolidateOkReader = schema.Struct[ConsolidateOk](ConsolidateOkType(), nil, captypes.Converters...)

// EgressTrack capability definition
// This capability allows a storage node to request recording egress for content it has served.
var Consolidate = validator.NewCapability(
	ConsolidateAbility,
	schema.DIDString(),
	schema.Struct[ConsolidateCaveats](nil, nil, captypes.Converters...),
	func(claimed, delegated ucan.Capability[ConsolidateCaveats]) failure.Failure {
		if fail := equalWith(claimed, delegated); fail != nil {
			return fail
		}

		return nil
	},
)

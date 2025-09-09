package egress

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

const TrackAbility = "space/egress/track"

type TrackCaveats struct {
	Receipts ucan.Link
	Endpoint *url.URL
}

func (tc TrackCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&tc, TrackCaveatsType(), captypes.Converters...)
}

var TrackCaveatsReader = schema.Struct[TrackCaveats](TrackCaveatsType(), nil, captypes.Converters...)

type TrackOk struct {
}

func (to TrackOk) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&to, TrackOkType(), captypes.Converters...)
}

type TrackReceipt receipt.Receipt[TrackOk, failure.Failure]

type TrackReceiptReader receipt.ReceiptReader[TrackOk, failure.Failure]

func NewTrackReceiptReader() (TrackReceiptReader, error) {
	return receipt.NewReceiptReader[TrackOk, failure.Failure](egressSchema)
}

var TrackOkReader = schema.Struct[TrackOk](TrackOkType(), nil, captypes.Converters...)

// EgressTrack capability definition
// This capability allows a storage node to request recording egress for content it has served.
var Track = validator.NewCapability(
	TrackAbility,
	schema.DIDString(),
	schema.Struct[TrackCaveats](nil, nil, captypes.Converters...),
	func(claimed, delegated ucan.Capability[TrackCaveats]) failure.Failure {
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

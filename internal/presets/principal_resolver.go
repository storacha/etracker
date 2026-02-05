package presets

import (
	"context"
	"errors"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/validator"
)

var principalMapping = map[string]string{
	"did:web:registrar.forge.storacha.network":        "did:key:z6MkkfWep96Dphp35s9VqSCD7h7G4R9R1QCR3K9TxpbSRrKf",
	"did:web:staging.registrar.warm.storacha.network": "did:key:z6MkuQ8PfSMrzXCwZkbQv662nZC4FGGm1aucbH256HXXZyxo",
	"did:web:indexer.forge.storacha.network":          "did:key:z6Mkj8WmJQRy5jEnFN97uuc2qsjFdsYCuD5wE384Z1AMCFN7",
	"did:web:staging.indexer.warm.storacha.network":   "did:key:z6Mkr4QkdinnXQmJ9JdnzwhcEjR8nMnuVPEwREyh9jp2Pb7k",
	"did:web:up.forge.storacha.network":               "did:key:z6MkgSttS3n3R56yGX2Eufvbwc58fphomhAsLoBCZpZJzQbr",
	"did:web:staging.up.warm.storacha.network":        "did:key:z6MkpR58oZpK7L3cdZZciKT25ynGro7RZm6boFouWQ7AzF7v",
}

type resolver struct {
	mapping map[did.DID]did.DID
}

func (r *resolver) ResolveDIDKey(ctx context.Context, input did.DID) (did.DID, validator.UnresolvedDID) {
	dk, ok := r.mapping[input]
	if !ok {
		return did.Undef, validator.NewDIDKeyResolutionError(input, errors.New("not found in mapping"))
	}
	return dk, nil
}

func NewPresetResolver() (validator.PrincipalResolver, error) {
	dmap := map[did.DID]did.DID{}
	for k, v := range principalMapping {
		dk, err := did.Parse(k)
		if err != nil {
			return nil, err
		}
		dv, err := did.Parse(v)
		if err != nil {
			return nil, err
		}
		dmap[dk] = dv
	}
	return &resolver{dmap}, nil
}

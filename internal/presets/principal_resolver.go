package presets

import (
	"context"
	"errors"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/validator"
)

var principalMapping = map[string]string{
	"did:web:staging.registrar.storacha.network": "did:key:z6MkuQ8PfSMrzXCwZkbQv662nZC4FGGm1aucbH256HXXZyxo",
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

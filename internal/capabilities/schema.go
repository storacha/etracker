package capabilities

import (
	_ "embed"
	"fmt"

	"github.com/ipld/go-ipld-prime/schema"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
)

//go:embed payme.ipldsch
var paymeSchema []byte

var paymeTS = mustLoadTS()

func mustLoadTS() *schema.TypeSystem {
	ts, err := captypes.LoadSchemaBytes(paymeSchema)
	if err != nil {
		panic(fmt.Errorf("loading index schema: %w", err))
	}
	return ts
}

func PayMeCaveatsType() schema.Type {
	return paymeTS.TypeByName("PayMeCaveats")
}

func PayMeOkType() schema.Type {
	return paymeTS.TypeByName("PayMeOk")
}

package capabilities

import (
	_ "embed"
	"fmt"

	ipldprime "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/schema"
)

//go:embed index.ipldsch
var paymeSchema []byte

var paymeTS = mustLoadTS()

func mustLoadTS() *schema.TypeSystem {
	ts, err := ipldprime.LoadSchemaBytes(paymeSchema)
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

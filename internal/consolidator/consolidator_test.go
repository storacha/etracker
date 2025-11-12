package consolidator

import (
	"context"
	"reflect"
	"testing"
	"unsafe"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld/block"
	"github.com/storacha/go-ucanto/core/ipld/codec/cbor"
	"github.com/storacha/go-ucanto/core/ipld/hash/sha256"
	"github.com/storacha/go-ucanto/core/receipt"
	rdm "github.com/storacha/go-ucanto/core/receipt/datamodel"
	"github.com/storacha/go-ucanto/core/receipt/ran"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/principal/ed25519/verifier"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateRetrievalReceipt(t *testing.T) {
	vCtx := validator.NewValidationContext(
		testutil.Service.Verifier(),
		content.Retrieve,
		validator.IsSelfIssued,
		func(context.Context, validator.Authorization[any]) validator.Revoked {
			return nil
		},
		validator.ProofUnavailable,
		verifier.Parse,
		validator.FailDIDKeyResolution,
		// ignore expiration and not valid before
		func(dlg delegation.Delegation) validator.InvalidProof {
			return nil
		},
	)

	space := testutil.RandomSigner(t)
	randBytes := testutil.RandomBytes(t, 256)
	blob := struct {
		bytes []byte
		cid   cid.Cid
	}{randBytes, cid.NewCidV1(cid.Raw, testutil.MultihashFromBytes(t, randBytes))}

	prf := delegation.FromDelegation(
		testutil.Must(
			delegation.Delegate(
				space,
				testutil.Alice,
				[]ucan.Capability[content.RetrieveCaveats]{
					ucan.NewCapability(
						content.RetrieveAbility,
						space.DID().String(),
						content.RetrieveCaveats{
							Blob:  content.BlobDigest{Digest: blob.cid.Hash()},
							Range: content.Range{Start: 0, End: uint64(len(blob.bytes) - 1)},
						},
					),
				},
			),
		)(t),
	)

	storageNode := testutil.RandomSigner(t)

	inv, err := invocation.Invoke(
		testutil.Alice,
		storageNode,
		content.Retrieve.New(
			space.DID().String(),
			content.RetrieveCaveats{
				Blob:  content.BlobDigest{Digest: blob.cid.Hash()},
				Range: content.Range{Start: 0, End: 1},
			},
		),
		delegation.WithProof(prf),
	)
	require.NoError(t, err)

	t.Run("successful validation", func(t *testing.T) {
		rcpt, err := receipt.Issue(
			storageNode,
			result.Ok[content.RetrieveOk, failure.IPLDBuilderFailure](content.RetrieveOk{}),
			ran.FromInvocation(inv),
		)
		require.NoError(t, err)

		cap, err := validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, vCtx)
		require.NoError(t, err)
		assert.Equal(t, content.RetrieveAbility, cap.Can())
	})

	t.Run("failure receipt", func(t *testing.T) {
		rcpt, err := receipt.Issue(
			storageNode,
			result.Error[content.RetrieveOk, failure.IPLDBuilderFailure](content.NewNotFoundError("not found")),
			ran.FromInvocation(inv),
		)
		require.NoError(t, err)

		_, err = validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, vCtx)
		assert.ErrorContains(t, err, "receipt is a failure receipt")
	})

	t.Run("wrong issuer", func(t *testing.T) {
		otherNode := testutil.RandomSigner(t)
		rcpt, err := receipt.Issue(
			otherNode,
			result.Ok[content.RetrieveOk, failure.IPLDBuilderFailure](content.RetrieveOk{}),
			ran.FromInvocation(inv),
		)
		require.NoError(t, err)

		_, err = validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, vCtx)
		assert.ErrorContains(t, err, "receipt is not issued by the requester node")
	})

	t.Run("invalid signature", func(t *testing.T) {
		// Issue an error receipt
		rcpt, err := receipt.Issue(
			storageNode,
			result.Error[content.RetrieveOk, failure.IPLDBuilderFailure](content.NewNotFoundError("not found")),
			ran.FromInvocation(inv),
		)
		require.NoError(t, err)

		// Tamper with the receipt to change its result
		tamperReceiptResult(t, rcpt)

		_, err = validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, vCtx)
		assert.ErrorContains(t, err, "receipt signature is invalid")
	})

	t.Run("missing invocation", func(t *testing.T) {
		rcpt, err := receipt.Issue(
			storageNode,
			result.Ok[content.RetrieveOk, failure.IPLDBuilderFailure](content.RetrieveOk{}),
			ran.FromLink(inv.Link()),
		)
		require.NoError(t, err)

		_, err = validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, vCtx)
		assert.ErrorContains(t, err, "original retrieve invocation must be attached to the receipt")
	})

	t.Run("wrong capability type", func(t *testing.T) {
		otherInv, err := invocation.Invoke(
			testutil.Alice,
			storageNode,
			ucan.NewCapability(
				"other/ability",
				space.DID().String(),
				ucan.NoCaveats{},
			),
			delegation.WithProof(prf),
		)
		require.NoError(t, err)

		rcpt, err := receipt.Issue(
			storageNode,
			result.Ok[content.RetrieveOk, failure.IPLDBuilderFailure](content.RetrieveOk{}),
			ran.FromInvocation(otherInv),
		)
		require.NoError(t, err)

		_, err = validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, vCtx)
		expectedErr := "original invocation is not a " + content.RetrieveAbility + " invocation, but a other/ability one"
		assert.ErrorContains(t, err, expectedErr)
	})

	t.Run("invalid delegation chain", func(t *testing.T) {
		// Bob invokes on the space, but the proof is from the space to Alice
		bobInvokes, err := invocation.Invoke(
			testutil.Bob,
			storageNode,
			content.Retrieve.New(
				space.DID().String(),
				content.RetrieveCaveats{
					Blob:  content.BlobDigest{Digest: blob.cid.Hash()},
					Range: content.Range{Start: 0, End: 1},
				},
			),
			delegation.WithProof(prf),
		)
		require.NoError(t, err)

		rcpt, err := receipt.Issue(
			storageNode,
			result.Ok[content.RetrieveOk, failure.IPLDBuilderFailure](content.RetrieveOk{}),
			ran.FromInvocation(bobInvokes),
		)
		require.NoError(t, err)

		_, err = validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, vCtx)
		assert.ErrorContains(t, err, "invalid delegation chain")
	})
}

// tamperReceiptResult adds a new root block to an existing receipt. The receipt in this block will be identical
// to the original one except for the result, which will always be an ok result.
func tamperReceiptResult(t *testing.T, rcpt receipt.AnyReceipt) {
	t.Helper()

	bs, err := blockstore.NewBlockStore()
	require.NoError(t, err)

	for b, err := range rcpt.Blocks() {
		require.NoError(t, err)
		require.NoError(t, bs.Put(b))
	}

	// Create a new receipt model with all fields unchanged except for the result
	okResult, err := content.RetrieveOk{}.ToIPLD()
	require.NoError(t, err)
	resultModel := rdm.ResultModel[ipld.Node, ipld.Node]{Ok: &okResult, Error: nil}

	issString := rcpt.Issuer().DID().String()

	var proofLinks []ipld.Link
	for _, prf := range rcpt.Proofs() {
		proofLinks = append(proofLinks, prf.Link())
	}

	outcomeModel := rdm.OutcomeModel[ipld.Node, ipld.Node]{
		Ran:  rcpt.Ran().Link(),
		Out:  resultModel,
		Fx:   rdm.EffectsModel{},
		Iss:  &issString,
		Meta: rdm.MetaModel{},
		Prf:  proofLinks,
	}

	receiptModel := rdm.ReceiptModel[ipld.Node, ipld.Node]{
		Ocm: outcomeModel,
		Sig: rcpt.Signature().Bytes(),
	}

	rt, err := block.Encode(&receiptModel, rdm.TypeSystem().TypeByName("Receipt"), cbor.Codec, sha256.Hasher)
	require.NoError(t, err)

	require.NoError(t, bs.Put(rt))

	// Supplant existing blocks and data
	rcptValue := reflect.ValueOf(rcpt).Elem()

	rcptRt := rcptValue.FieldByName("rt")
	rcptRtPtr := unsafe.Pointer(rcptRt.UnsafeAddr())
	*(*block.Block)(rcptRtPtr) = rt

	rcptBlks := rcptValue.FieldByName("blks")
	rcptBlksPtr := unsafe.Pointer(rcptBlks.UnsafeAddr())
	*(*blockstore.BlockReader)(rcptBlksPtr) = bs

	rcptData := rcptValue.FieldByName("data")
	rcptData = reflect.NewAt(rcptData.Type(), unsafe.Pointer(rcptData.UnsafeAddr())).Elem()
	rcptData.Set(reflect.ValueOf(&receiptModel))
}

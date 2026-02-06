package consolidator

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/storacha/etracker/internal/db/consumer"
	"github.com/storacha/go-libstoracha/capabilities/space/content"
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
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
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/absentee"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ consumer.ConsumerTable = (*mockConsumerTable)(nil)

type mockConsumerTable struct {
	t        *testing.T
	provider did.DID
}

func (m *mockConsumerTable) Get(ctx context.Context, space string) (consumer.Consumer, error) {
	s, err := did.Parse(space)
	if err != nil {
		return consumer.Consumer{}, err
	}

	return consumer.Consumer{
		ID:           s,
		Provider:     m.provider,
		Subscription: testutil.RandomCID(m.t).String(),
	}, nil
}

func (m *mockConsumerTable) ListByCustomer(ctx context.Context, customer did.DID) ([]did.DID, error) {
	return []did.DID{}, nil
}

func TestValidateRetrievalReceipt(t *testing.T) {
	consolidatorID := testutil.RandomSigner(t)

	// trust attestations from the upload service
	uploadServiceID := testutil.WebService
	attestDlg, err := delegation.Delegate(
		consolidatorID,
		uploadServiceID,
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability(
				ucancap.AttestAbility,
				consolidatorID.DID().String(),
				ucan.NoCaveats{},
			),
		},
		delegation.WithNoExpiration(),
	)
	require.NoError(t, err)

	knownProvider, err := did.Parse("did:web:up.test.storacha.network")
	require.NoError(t, err)

	consumerTable := &mockConsumerTable{t: t, provider: knownProvider}

	// Create a consolidator instance to test the validation context it creates works as expected
	c, err := New(
		consolidatorID,
		nil,
		nil,
		nil,
		consumerTable,
		[]string{knownProvider.String()},
		0,
		1,
		func(ctx context.Context, input did.DID) (did.DID, validator.UnresolvedDID) {
			if input.String() == uploadServiceID.DID().String() {
				return uploadServiceID.Unwrap().DID(), nil
			}

			return did.Undef, validator.NewDIDKeyResolutionError(input, fmt.Errorf("%s not found in mapping", input.String()))
		},
		[]delegation.Delegation{attestDlg},
	)
	require.NoError(t, err)

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

		cap, err := validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, c.retrieveValidationCtx, c.consumerTable, c.knownProviders)
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

		_, err = validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, c.retrieveValidationCtx, c.consumerTable, c.knownProviders)
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

		_, err = validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, c.retrieveValidationCtx, c.consumerTable, c.knownProviders)
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

		_, err = validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, c.retrieveValidationCtx, c.consumerTable, c.knownProviders)
		assert.ErrorContains(t, err, "receipt signature is invalid")
	})

	t.Run("missing invocation", func(t *testing.T) {
		rcpt, err := receipt.Issue(
			storageNode,
			result.Ok[content.RetrieveOk, failure.IPLDBuilderFailure](content.RetrieveOk{}),
			ran.FromLink(inv.Link()),
		)
		require.NoError(t, err)

		_, err = validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, c.retrieveValidationCtx, c.consumerTable, c.knownProviders)
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

		_, err = validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, c.retrieveValidationCtx, c.consumerTable, c.knownProviders)
		expectedErr := "original invocation is not a " + content.RetrieveAbility + " invocation, but a other/ability one"
		assert.ErrorContains(t, err, expectedErr)
	})

	t.Run("wrong space provider", func(t *testing.T) {
		rcpt, err := receipt.Issue(
			storageNode,
			result.Ok[content.RetrieveOk, failure.IPLDBuilderFailure](content.RetrieveOk{}),
			ran.FromInvocation(inv),
		)
		require.NoError(t, err)

		otherProvider, err := did.Parse("did:web:up.other.net")
		require.NoError(t, err)

		consumerTable := &mockConsumerTable{t: t, provider: otherProvider}

		_, err = validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, c.retrieveValidationCtx, consumerTable, c.knownProviders)
		assert.ErrorContains(t, err, "unknown space provider")
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

		_, err = validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, c.retrieveValidationCtx, c.consumerTable, c.knownProviders)
		assert.ErrorContains(t, err, "invalid delegation chain")
	})

	t.Run("ucan/attest delegation from trusted authority works", func(t *testing.T) {
		// Bob invokes on the space, but the proof is from the space to Alice
		// Alice delegates access to Bob's account
		bobAcc := absentee.From(testutil.Must(did.Parse("did:mailto:web.mail.bob"))(t))
		aliceToBobAcc, err := delegation.Delegate(
			testutil.Alice,
			bobAcc,
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
			delegation.WithProof(prf),
		)
		require.NoError(t, err)

		// Bob's account delegates to Bob's key
		bobAccToKey, err := delegation.Delegate(
			bobAcc,
			testutil.Bob,
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
			delegation.WithProof(delegation.FromDelegation(aliceToBobAcc)),
		)
		require.NoError(t, err)

		// The upload service is trusted to attest bob's delegation
		attestDlg, err := ucancap.Attest.Delegate(
			uploadServiceID,
			testutil.Bob,
			uploadServiceID.DID().String(),
			ucancap.AttestCaveats{
				Proof: bobAccToKey.Link(),
			},
		)
		require.NoError(t, err)

		// Bob's agent invokes space/content/retrieve on the space
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
			delegation.WithProof(
				delegation.FromDelegation(bobAccToKey),
				delegation.FromDelegation(attestDlg),
			),
		)
		require.NoError(t, err)

		rcpt, err := receipt.Issue(
			storageNode,
			result.Ok[content.RetrieveOk, failure.IPLDBuilderFailure](content.RetrieveOk{}),
			ran.FromInvocation(bobInvokes),
		)
		require.NoError(t, err)

		cap, err := validateRetrievalReceipt(context.Background(), storageNode.DID(), rcpt, c.retrieveValidationCtx, c.consumerTable, c.knownProviders)
		require.NoError(t, err)
		assert.Equal(t, content.RetrieveAbility, cap.Can())
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

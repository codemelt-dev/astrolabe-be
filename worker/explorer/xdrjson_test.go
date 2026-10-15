package explorer

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Operation details for types the real fixtures lack, including awkward
// shapes: bodiless operations, a non-struct body, binary data names and
// values, asset codes, liquidity pools, claimable balances, sponsorship
// keys and a large WASM upload.
func TestOperationDetails(t *testing.T) {
	a := keypair.MustRandom().Address()
	acct := xdr.MustAddress(a)
	muxed := xdr.MustMuxedAddress(a)
	usdc := xdr.MustNewCreditAsset("USDC", a)
	pool := xdr.PoolId{9, 9}
	cb := xdr.ClaimableBalanceId{Type: xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0, V0: &xdr.Hash{7}}
	wasm := make([]byte, 5000)
	cases := []struct {
		body xdr.OperationBody
		want []string // substrings
	}{
		{xdr.OperationBody{Type: xdr.OperationTypeInflation}, []string{`{}`}},
		{xdr.OperationBody{Type: xdr.OperationTypeEndSponsoringFutureReserves}, []string{`{}`}},
		{xdr.OperationBody{Type: xdr.OperationTypeAccountMerge, Destination: &muxed}, []string{`{"destination":"` + a + `"}`}},
		{xdr.OperationBody{Type: xdr.OperationTypeManageData, ManageDataOp: &xdr.ManageDataOp{DataName: "n\x00m", DataValue: &xdr.DataValue{0, 1}}},
			[]string{`"data_name":{"hex":"6e006d"}`, `"data_value":"0001"`}},
		{xdr.OperationBody{Type: xdr.OperationTypeManageData, ManageDataOp: &xdr.ManageDataOp{DataName: "gone"}}, []string{`"data_value":null`}},
		{xdr.OperationBody{Type: xdr.OperationTypeAllowTrust, AllowTrustOp: &xdr.AllowTrustOp{Trustor: acct,
			Asset: xdr.AssetCode{Type: xdr.AssetTypeAssetTypeCreditAlphanum4, AssetCode4: &xdr.AssetCode4{'A', 'B', 0, 0}}, Authorize: 1}},
			[]string{`"asset":"AB"`, `"authorize":1`}},
		{xdr.OperationBody{Type: xdr.OperationTypeChangeTrust, ChangeTrustOp: &xdr.ChangeTrustOp{Line: xdr.ChangeTrustAsset{
			Type: xdr.AssetTypeAssetTypePoolShare, LiquidityPool: &xdr.LiquidityPoolParameters{
				Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct, ConstantProduct: &xdr.LiquidityPoolConstantProductParameters{
					AssetA: xdr.MustNewNativeAsset(), AssetB: usdc, Fee: 30}}}, Limit: 100}},
			[]string{`"liquidity_pool":{"constant_product":{"asset_a":"native","asset_b":"USDC:` + a, `"limit":"100"`}},
		{xdr.OperationBody{Type: xdr.OperationTypeClawbackClaimableBalance, ClawbackClaimableBalanceOp: &xdr.ClawbackClaimableBalanceOp{BalanceId: cb}},
			[]string{`"balance_id":"B`}},
		{xdr.OperationBody{Type: xdr.OperationTypeLiquidityPoolDeposit, LiquidityPoolDepositOp: &xdr.LiquidityPoolDepositOp{
			LiquidityPoolId: pool, MaxAmountA: 1, MaxAmountB: 2, MinPrice: xdr.Price{N: 1, D: 2}, MaxPrice: xdr.Price{N: 3, D: 4}}},
			[]string{`"liquidity_pool_id":"0909`, `"min_price":{"n":1,"d":2}`}},
		{xdr.OperationBody{Type: xdr.OperationTypeRevokeSponsorship, RevokeSponsorshipOp: &xdr.RevokeSponsorshipOp{
			Type: xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry, LedgerKey: &xdr.LedgerKey{
				Type: xdr.LedgerEntryTypeTrustline, TrustLine: &xdr.LedgerKeyTrustLine{AccountId: acct, Asset: usdc.ToTrustLineAsset()}}}},
			[]string{`{"ledger_key":{"trust_line":{"account_id":"` + a + `","asset":"USDC:`}},
		{xdr.OperationBody{Type: xdr.OperationTypeSetTrustLineFlags, SetTrustLineFlagsOp: &xdr.SetTrustLineFlagsOp{Trustor: acct, Asset: usdc, SetFlags: 4}},
			[]string{`"set_flags":4`}},
		{xdr.OperationBody{Type: xdr.OperationTypeInvokeHostFunction, InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
			HostFunction: xdr.HostFunction{Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm, Wasm: &wasm}}},
			[]string{`{"host_function":{"wasm":{"size":5000,"sha256":"`, `"auth":[]`}},
		{xdr.OperationBody{Type: xdr.OperationTypeBumpSequence, BumpSequenceOp: &xdr.BumpSequenceOp{BumpTo: 9223372036854775807}},
			[]string{`{"bump_to":"9223372036854775807"}`}},
	}
	for _, c := range cases {
		got, err := OperationDetails(c.body)
		if err != nil {
			t.Fatalf("%v: %v", c.body.Type, err)
		}
		if !json.Valid([]byte(got)) || strings.Contains(got, `\u0000`) {
			t.Fatalf("%v: invalid jsonb %s", c.body.Type, got)
		}
		for _, w := range c.want {
			if !strings.Contains(got, w) {
				t.Errorf("%v: %s lacks %s", c.body.Type, got, w)
			}
		}
	}
	if _, err := OperationDetails(xdr.OperationBody{Type: 99}); err == nil {
		t.Fatal("unknown operation type accepted")
	}
}

// invoke_host_function keeps its host function and reduces "auth" to the
// authorizing addresses: the operation's source for source-account
// credentials, the credential's address otherwise; signatures, nonces and
// the invocation tree are left out.
func TestInvokeDetails(t *testing.T) {
	contract := xdr.ContractId{1, 2, 3}
	signer := keypair.MustRandom().Address()
	signerID := xdr.MustAddress(signer)
	fn := xdr.InvokeContractArgs{
		ContractAddress: xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contract},
		FunctionName:    "transfer",
		Args:            []xdr.ScVal{{Type: xdr.ScValTypeScvBool, B: new(bool)}},
	}
	invocation := xdr.SorobanAuthorizedInvocation{Function: xdr.SorobanAuthorizedFunction{
		Type: xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn, ContractFn: &fn}}
	op := xdr.InvokeHostFunctionOp{
		HostFunction: xdr.HostFunction{Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract, InvokeContract: &fn},
		Auth: []xdr.SorobanAuthorizationEntry{
			{Credentials: xdr.SorobanCredentials{Type: xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount}, RootInvocation: invocation},
			{Credentials: xdr.SorobanCredentials{Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress, Address: &xdr.SorobanAddressCredentials{
				Address:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &signerID},
				Nonce:     7,
				Signature: xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			}}, RootInvocation: invocation},
		},
	}
	source := keypair.MustRandom().Address()
	got, err := invokeDetails(op, source)
	if err != nil {
		t.Fatal(err)
	}
	full, err := OperationDetails(xdr.OperationBody{Type: xdr.OperationTypeInvokeHostFunction, InvokeHostFunctionOp: &op})
	if err != nil {
		t.Fatal(err)
	}
	var g, f map[string]json.RawMessage
	if err := json.Unmarshal([]byte(got), &g); err != nil || json.Unmarshal([]byte(full), &f) != nil {
		t.Fatalf("not JSON: %s (%v)", got, err)
	}
	if len(g) != 2 || string(g["host_function"]) != string(f["host_function"]) {
		t.Errorf("host function changed:\n%s\n%s", g["host_function"], f["host_function"])
	}
	if want := `["` + source + `","` + signer + `"]`; string(g["auth"]) != want {
		t.Errorf("auth %s, want %s", g["auth"], want)
	}
	if strings.Contains(got, "root_invocation") || strings.Contains(got, "nonce") || len(got) >= len(full) {
		t.Errorf("not slim: %s", got)
	}
}

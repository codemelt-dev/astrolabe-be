package explorer

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// The history rows besides the envelope's: activity, contract calls and
// creations, result codes and return values, on synthetic transactions.

func muxedOf(n byte, id uint64) xdr.MuxedAccount {
	return xdr.MuxedAccount{Type: xdr.CryptoKeyTypeKeyTypeMuxedEd25519,
		Med25519: &xdr.MuxedAccountMed25519{Id: xdr.Uint64(id), Ed25519: *testAccountID(n).Ed25519}}
}

// muxedAcct is account n as a plain (not muxed) MuxedAccount.
func muxedAcct(n byte) xdr.MuxedAccount {
	id := testAccountID(n)
	return id.ToMuxedAccount()
}

func accountAddr(n byte) xdr.ScAddress {
	id := testAccountID(n)
	return xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &id}
}

func contractAddr(n byte) xdr.ScAddress {
	id := testContractID(n)
	return xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &id}
}

func contractStr(n byte) string {
	id := testContractID(n)
	s, _ := strkey.Encode(strkey.VersionByteContract, id[:])
	return s
}

// testTx is a transaction as the ledger's reader yields it: source n, its
// operations, success or not, the operations' entry changes and (for
// Soroban) a return value, with meta v4.
func testTx(index uint32, source xdr.MuxedAccount, ops []xdr.Operation, success bool, opResults []xdr.OperationResult,
	opChanges []xdr.LedgerEntryChanges, before xdr.LedgerEntryChanges, ret *xdr.ScVal) ingest.LedgerTransaction {
	tx := xdr.Transaction{SourceAccount: source, Fee: 100, SeqNum: 1, Operations: ops}
	if ret != nil || (len(ops) == 1 && ops[0].Body.Type == xdr.OperationTypeInvokeHostFunction) {
		tx.Ext = xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{}}
	}
	code := xdr.TransactionResultCodeTxSuccess
	if !success {
		code = xdr.TransactionResultCodeTxFailed
	}
	metaOps := make([]xdr.OperationMetaV2, len(ops))
	for i := range metaOps {
		if i < len(opChanges) {
			metaOps[i].Changes = opChanges[i]
		}
	}
	meta := xdr.TransactionMetaV4{TxChangesBefore: before, Operations: metaOps}
	if tx.Ext.V == 1 {
		meta.SorobanMeta = &xdr.SorobanTransactionMetaV2{ReturnValue: ret}
	}
	return ingest.LedgerTransaction{
		Index:    index,
		Envelope: xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: &xdr.TransactionV1Envelope{Tx: tx}},
		Result: xdr.TransactionResultPair{Result: xdr.TransactionResult{FeeCharged: 100,
			Result: xdr.TransactionResultResult{Code: code, Results: &opResults}}},
		UnsafeMeta: xdr.TransactionMeta{V: 4, V4: &meta},
	}
}

func invokeContractOp(source *xdr.MuxedAccount, contract xdr.ScAddress, fn string, args ...xdr.ScVal) xdr.Operation {
	return xdr.Operation{SourceAccount: source, Body: xdr.OperationBody{Type: xdr.OperationTypeInvokeHostFunction,
		InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{HostFunction: xdr.HostFunction{
			Type:           xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{ContractAddress: contract, FunctionName: xdr.ScSymbol(fn), Args: args},
		}}}}
}

func invokeResult(code xdr.InvokeHostFunctionResultCode) xdr.OperationResult {
	r := xdr.InvokeHostFunctionResult{Code: code}
	if code == xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess {
		r.Success = &xdr.Hash{}
	}
	return xdr.OperationResult{Code: xdr.OperationResultCodeOpInner,
		Tr: &xdr.OperationResultTr{Type: xdr.OperationTypeInvokeHostFunction, InvokeHostFunctionResult: &r}}
}

func paymentResult(code xdr.PaymentResultCode) xdr.OperationResult {
	return xdr.OperationResult{Code: xdr.OperationResultCodeOpInner,
		Tr: &xdr.OperationResultTr{Type: xdr.OperationTypePayment, PaymentResult: &xdr.PaymentResult{Code: code}}}
}

// instanceEntry is a contract's instance entry: a Wasm contract's, or a
// Stellar Asset Contract's for asset.
func instanceEntry(t *testing.T, n byte, asset *xdr.Asset) xdr.LedgerEntry {
	t.Helper()
	if asset != nil {
		id, err := asset.ContractID(network.PublicNetworkPassphrase)
		if err != nil {
			t.Fatal(err)
		}
		var code, issuer string
		var typ string
		if err := asset.Extract(&typ, &code, &issuer); err != nil {
			t.Fatal(err)
		}
		data, err := sac.AssetToContractData(asset.Type == xdr.AssetTypeAssetTypeNative, code, issuer, id)
		if err != nil {
			t.Fatal(err)
		}
		return xdr.LedgerEntry{LastModifiedLedgerSeq: 10, Data: data}
	}
	hash := xdr.Hash(sha256.Sum256([]byte{n}))
	id := testContractID(n)
	return xdr.LedgerEntry{LastModifiedLedgerSeq: 10, Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.ContractDataEntry{
			Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &id},
			Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
			Durability: xdr.ContractDataDurabilityPersistent,
			Val: xdr.ScVal{Type: xdr.ScValTypeScvContractInstance, Instance: &xdr.ScContractInstance{
				Executable: xdr.ContractExecutable{Type: xdr.ContractExecutableTypeContractExecutableWasm, WasmHash: &hash}}},
		}}}
}

func TestOperationResultCode(t *testing.T) {
	for _, c := range []struct {
		r    xdr.OperationResult
		want string
	}{
		{paymentResult(xdr.PaymentResultCodePaymentSuccess), "payment_success"},
		{paymentResult(xdr.PaymentResultCodePaymentUnderfunded), "payment_underfunded"},
		{invokeResult(xdr.InvokeHostFunctionResultCodeInvokeHostFunctionTrapped), "invoke_host_function_trapped"},
		{xdr.OperationResult{Code: xdr.OperationResultCodeOpBadAuth}, "op_bad_auth"},
		{xdr.OperationResult{Code: xdr.OperationResultCodeOpNoAccount}, "op_no_account"},
		{xdr.OperationResult{Code: xdr.OperationResultCodeOpInner}, "op_inner"}, // malformed: no inner result
	} {
		if got := operationResultCode(c.r); got != c.want {
			t.Errorf("%+v: %q, want %q", c.r, got, c.want)
		}
	}
}

func TestReturnValue(t *testing.T) {
	seven := xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: ptr(xdr.Uint32(7))}
	call := []xdr.Operation{invokeContractOp(nil, contractAddr(1), "get")}
	ok := testTx(1, muxedAcct(1), call, true,
		[]xdr.OperationResult{invokeResult(xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess)}, nil, nil, &seven)
	if got, err := returnValueOf(ok); err != nil || got != `{"u32":7}` {
		t.Fatalf("v4: %q %v", got, err)
	}
	// Meta v3 keeps it in SorobanMeta too.
	v3 := ok
	v3.UnsafeMeta = xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{Operations: []xdr.OperationMeta{{}},
		SorobanMeta: &xdr.SorobanTransactionMeta{ReturnValue: seven}}}
	if got, err := returnValueOf(v3); err != nil || got != `{"u32":7}` {
		t.Fatalf("v3: %q %v", got, err)
	}
	failed := testTx(1, muxedAcct(1), call, false,
		[]xdr.OperationResult{invokeResult(xdr.InvokeHostFunctionResultCodeInvokeHostFunctionTrapped)}, nil, nil, &seven)
	classic := testTx(1, muxedAcct(1), []xdr.Operation{paymentOp(nil, muxedAcct(2))}, true,
		[]xdr.OperationResult{paymentResult(xdr.PaymentResultCodePaymentSuccess)}, nil, nil, nil)
	for name, tx := range map[string]ingest.LedgerTransaction{"failed": failed, "classic": classic} {
		if got, err := returnValueOf(tx); err != nil || got != "" {
			t.Errorf("%s: %q %v", name, got, err)
		}
	}
	// The operation row carries it, with the result code.
	rows, err := operationRowsOf(ok, 5)
	if err != nil || rows[0].ReturnValue != `{"u32":7}` || rows[0].ResultCode != "invoke_host_function_success" {
		t.Fatalf("rows %+v %v", rows, err)
	}
	if rows, err := operationRowsOf(failed, 5); err != nil || rows[0].ReturnValue != "" || rows[0].ResultCode != "invoke_host_function_trapped" {
		t.Fatalf("failed rows %+v %v", rows, err)
	}
}

func paymentOp(source *xdr.MuxedAccount, to xdr.MuxedAccount) xdr.Operation {
	return xdr.Operation{SourceAccount: source, Body: xdr.OperationBody{Type: xdr.OperationTypePayment,
		PaymentOp: &xdr.PaymentOp{Destination: to, Asset: xdr.MustNewCreditAsset("USD", testAddress(9)), Amount: 5}}}
}

// Addresses at any depth of an operation's body, muxed ones as their G…;
// asset issuers, claimable balances and pools are left out.
func TestWalkAddresses(t *testing.T) {
	cb := xdr.ClaimableBalanceId{Type: xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0, V0: &xdr.Hash{1}}
	muxed := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeMuxedAccount, MuxedAccount: &xdr.MuxedEd25519Account{Id: 9, Ed25519: *testAccountID(6).Ed25519}}
	call := invokeContractOp(nil, contractAddr(1), "transfer",
		xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: ptr(accountAddr(3))},
		xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: ptr(&xdr.ScVec{{Type: xdr.ScValTypeScvAddress, Address: ptr(contractAddr(4))}})},
		xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeClaimableBalance, ClaimableBalanceId: &cb}},
		xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &muxed},
	)
	call.Body.InvokeHostFunctionOp.Auth = []xdr.SorobanAuthorizationEntry{{Credentials: xdr.SorobanCredentials{
		Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress, Address: &xdr.SorobanAddressCredentials{Address: accountAddr(5)}}}}
	for _, c := range []struct {
		name string
		body xdr.OperationBody
		want []string
	}{
		{"payment to a muxed account, of an issued asset", paymentOp(nil, muxedOf(2, 77)).Body, []string{testAddress(2)}},
		{"contract call: contract, arguments, authorizer", call.Body,
			[]string{contractStr(1), testAddress(3), contractStr(4), testAddress(6), testAddress(5)}},
		{"change trust: the issuer is no participant", xdr.OperationBody{Type: xdr.OperationTypeChangeTrust,
			ChangeTrustOp: &xdr.ChangeTrustOp{Line: xdr.MustNewCreditAsset("USD", testAddress(9)).ToChangeTrustAsset(), Limit: 1}}, nil},
	} {
		var got []string
		if err := walkAddresses(reflect.ValueOf(c.body), func(a string) { got = append(got, a) }); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// A transaction's addresses with their roles: from the envelope, the
// operations' bodies, the entries it changed, and its transfers.
func TestActivity(t *testing.T) {
	opSource := muxedAcct(2)
	usd := xdr.MustNewCreditAsset("USD", testAddress(9))
	usdID, err := usd.ContractID(network.PublicNetworkPassphrase)
	if err != nil {
		t.Fatal(err)
	}
	sacBalance := xdr.LedgerEntry{Data: sac.BalanceToContractData(usdID, testContractID(8), 5)}
	offer := offerEntry() // seller 3
	cbEntry := xdr.LedgerEntry{Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeClaimableBalance, ClaimableBalance: &xdr.ClaimableBalanceEntry{
		BalanceId: xdr.ClaimableBalanceId{Type: xdr.ClaimableBalanceIdTypeClaimableBalanceIdTypeV0, V0: &xdr.Hash{1}}, Asset: usd, Amount: 1,
		Claimants: []xdr.Claimant{{Type: xdr.ClaimantTypeClaimantTypeV0, V0: &xdr.ClaimantV0{Destination: testAccountID(7),
			Predicate: xdr.ClaimPredicate{Type: xdr.ClaimPredicateTypeClaimPredicateUnconditional}}}},
	}}}
	tx := testTx(3, muxedOf(1, 5), []xdr.Operation{paymentOp(&opSource, muxedOf(4, 1))}, true,
		[]xdr.OperationResult{paymentResult(xdr.PaymentResultCodePaymentSuccess)},
		[]xdr.LedgerEntryChanges{append(append(updated(trustlineEntry(4, 1, 1), trustlineEntry(4, 6, 2)), created(offer)...),
			append(created(sacBalance), created(cbEntry)...)...)},
		updated(accountEntry(1, 10, 1), accountEntry(1, 9, 2)), nil)
	ops, err := operationRowsOf(tx, 50)
	if err != nil {
		t.Fatal(err)
	}
	set, err := activityOf(tx, ops, network.PublicNetworkPassphrase)
	if err != nil {
		t.Fatal(err)
	}
	usdContract, _ := strkey.Encode(strkey.VersionByteContract, usdID[:])
	rows := activityRows(50, []activitySet{set}, []TransferRow{
		{TxIndex: 3, From: testAddress(1), To: "B" + testAddress(1)[1:], Kind: "fee"}, // a claimable balance's B… is left out
		{TxIndex: 3, From: testAddress(2), To: testAddress(4)},
		{TxIndex: 9, From: testAddress(5), To: testAddress(5)}, // another transaction's
	})
	got := map[string]uint8{}
	for _, r := range rows {
		if r.TxIndex != 3 {
			t.Fatalf("row %+v", r)
		}
		got[r.Account] = r.Roles
	}
	want := map[string]uint8{
		testAddress(1): RoleSource | RoleFeePayer | RoleChanged | RoleTransfer,
		testAddress(2): RoleOpSource | RoleTransfer,
		testAddress(4): RoleParticipant | RoleChanged | RoleTransfer, // the payment's muxed destination, its trustline
		testAddress(3): RoleChanged,                                  // the offer's seller
		usdContract:    RoleChanged,                                  // the SAC's balance entry: the contract…
		contractStr(8): RoleChanged,                                  // …and its holder
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("activity\n got %v\nwant %v", got, want)
	}
	if !slices.IsSortedFunc(rows, func(a, b ActivityRow) int { return strings.Compare(a.Account, b.Account) }) {
		t.Fatal("rows not ordered by account")
	}

	// A fee bump: the payer is the fee payer, the inner source the source.
	inner := tx.Envelope.MustV1()
	bump := tx
	bump.Envelope = xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump, FeeBump: &xdr.FeeBumpTransactionEnvelope{
		Tx: xdr.FeeBumpTransaction{FeeSource: muxedAcct(6), Fee: 200,
			InnerTx: xdr.FeeBumpTransactionInnerTx{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: &inner}}}}
	set, err = activityOf(bump, ops, network.PublicNetworkPassphrase)
	if err != nil {
		t.Fatal(err)
	}
	if set.roles[testAddress(6)] != RoleFeePayer || set.roles[testAddress(1)]&(RoleSource|RoleFeePayer) != RoleSource {
		t.Fatalf("fee bump roles %v", set.roles)
	}
}

// Calls of invoke_contract operations, and the contracts a transaction
// created: by a create_contract operation, or deployed by the contract a
// call invoked (Wasm or Stellar Asset Contract).
func TestContractRows(t *testing.T) {
	ret := xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: ptr(contractAddr(2))}
	usd := xdr.MustNewCreditAsset("USD", testAddress(9))
	deployed, sacEntry := instanceEntry(t, 2, nil), instanceEntry(t, 0, &usd)
	call := testTx(1, muxedAcct(1), []xdr.Operation{invokeContractOp(nil, contractAddr(1), "deploy")}, true,
		[]xdr.OperationResult{invokeResult(xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess)},
		// The factory's own instance, only updated, is no creation.
		[]xdr.LedgerEntryChanges{append(append(created(deployed), created(sacEntry)...), updated(instanceEntry(t, 1, nil), instanceEntry(t, 1, nil))...)},
		nil, &ret)
	ops, err := operationRowsOf(call, 7)
	if err != nil {
		t.Fatal(err)
	}
	calls, creations, err := contractRowsOf(call, ops, network.PublicNetworkPassphrase)
	if err != nil {
		t.Fatal(err)
	}
	wantCall := ContractCallRow{TxIndex: 1, ContractID: contractStr(1), Function: "deploy", Caller: testAddress(1), Successful: true,
		ReturnValue: `{"address":"` + contractStr(2) + `"}`}
	if len(calls) != 1 || calls[0] != wantCall {
		t.Fatalf("calls %+v", calls)
	}
	hash := sha256.Sum256([]byte{2})
	usdID, _ := usd.ContractID(network.PublicNetworkPassphrase)
	usdContract, _ := strkey.Encode(strkey.VersionByteContract, usdID[:])
	want := []ContractCreationRow{
		{TxIndex: 1, ContractID: contractStr(2), Creator: testAddress(1), ViaContract: contractStr(1), Executable: "wasm", WasmHash: hash[:]},
		{TxIndex: 1, ContractID: usdContract, Creator: testAddress(1), ViaContract: contractStr(1), Executable: "stellar_asset", Asset: "USD:" + testAddress(9)},
	}
	// The SDK orders a transaction's changes by key.
	byID := func(a, b ContractCreationRow) int { return strings.Compare(a.ContractID, b.ContractID) }
	slices.SortFunc(creations, byID)
	slices.SortFunc(want, byID)
	if !reflect.DeepEqual(creations, want) {
		t.Fatalf("creations\n got %+v\nwant %+v", creations, want)
	}

	// create_contract: no call, the creation has no via_contract.
	create := xdr.Operation{Body: xdr.OperationBody{Type: xdr.OperationTypeInvokeHostFunction, InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
		HostFunction: xdr.HostFunction{Type: xdr.HostFunctionTypeHostFunctionTypeCreateContract, CreateContract: &xdr.CreateContractArgs{
			ContractIdPreimage: xdr.ContractIdPreimage{Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
				FromAddress: &xdr.ContractIdPreimageFromAddress{Address: accountAddr(1)}},
			Executable: xdr.ContractExecutable{Type: xdr.ContractExecutableTypeContractExecutableWasm, WasmHash: &xdr.Hash{2}},
		}}}}}
	tx := testTx(2, muxedAcct(3), []xdr.Operation{create}, true,
		[]xdr.OperationResult{invokeResult(xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess)},
		[]xdr.LedgerEntryChanges{created(deployed)}, nil, &ret)
	ops, _ = operationRowsOf(tx, 7)
	calls, creations, err = contractRowsOf(tx, ops, network.PublicNetworkPassphrase)
	if err != nil || len(calls) != 0 || len(creations) != 1 || creations[0].ViaContract != "" || creations[0].Creator != testAddress(3) {
		t.Fatalf("create_contract: calls %+v creations %+v %v", calls, creations, err)
	}

	// A failed call: the call is recorded, unsuccessful, without a value; nothing was created.
	failed := testTx(3, muxedAcct(1), []xdr.Operation{invokeContractOp(nil, contractAddr(1), "deploy")}, false,
		[]xdr.OperationResult{invokeResult(xdr.InvokeHostFunctionResultCodeInvokeHostFunctionTrapped)}, nil, nil, nil)
	ops, _ = operationRowsOf(failed, 7)
	calls, creations, err = contractRowsOf(failed, ops, network.PublicNetworkPassphrase)
	if err != nil || len(calls) != 1 || calls[0].Successful || calls[0].ReturnValue != "" || len(creations) != 0 {
		t.Fatalf("failed call: %+v %+v %v", calls, creations, err)
	}
}

// A contract's instance entry gets its executable, Wasm hash or asset; XLM's
// Stellar Asset Contract is recognized too (the SDK leaves it out).
func TestInstanceColumns(t *testing.T) {
	usd, native := xdr.MustNewCreditAsset("USD", testAddress(9)), xdr.MustNewNativeAsset()
	hash := sha256.Sum256([]byte{2})
	for _, c := range []struct {
		name                     string
		e                        xdr.LedgerEntry
		executable, wasm, assetV any
	}{
		{"wasm", instanceEntry(t, 2, nil), "wasm", hash[:], nil},
		{"usd sac", instanceEntry(t, 0, &usd), "stellar_asset", nil, "USD:" + testAddress(9)},
		{"xlm sac", instanceEntry(t, 0, &native), "stellar_asset", nil, "native"},
		{"other data", contractDataEntry(1, xdr.ContractDataDurabilityPersistent, 1, 1), nil, nil, nil},
	} {
		vals, err := contractDataValues(c.e, network.PublicNetworkPassphrase)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual([]any{vals[9], vals[10], vals[11]}, []any{c.executable, c.wasm, c.assetV}) {
			t.Errorf("%s: executable %v wasm %v asset %v", c.name, vals[9], vals[10], vals[11])
		}
	}
}

// A contract's balance of a Stellar Asset Contract gets its holder and
// amount, of XLM's contract too (the SDK leaves it out); the entry passed in
// is not changed.
func TestSACBalances(t *testing.T) {
	usd, native := xdr.MustNewCreditAsset("USD", testAddress(9)), xdr.MustNewNativeAsset()
	balance := func(asset xdr.Asset, amount int64) xdr.LedgerEntry {
		id, err := asset.ContractID(network.PublicNetworkPassphrase)
		if err != nil {
			t.Fatal(err)
		}
		holder := contractAddr(5)
		key := &xdr.ScVec{{Type: xdr.ScValTypeScvSymbol, Sym: ptr(xdr.ScSymbol("Balance"))}, {Type: xdr.ScValTypeScvAddress, Address: &holder}}
		val := &xdr.ScMap{
			{Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: ptr(xdr.ScSymbol("amount"))},
				Val: xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &xdr.Int128Parts{Lo: xdr.Uint64(amount)}}},
			{Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: ptr(xdr.ScSymbol("authorized"))}, Val: xdr.ScVal{Type: xdr.ScValTypeScvBool, B: ptr(true)}},
			{Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: ptr(xdr.ScSymbol("clawback"))}, Val: xdr.ScVal{Type: xdr.ScValTypeScvBool, B: ptr(false)}},
		}
		cid := xdr.ContractId(id)
		return xdr.LedgerEntry{LastModifiedLedgerSeq: 1, Data: xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeContractData, ContractData: &xdr.ContractDataEntry{
			Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &cid},
			Key:        xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &key},
			Durability: xdr.ContractDataDurabilityPersistent,
			Val:        xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &val},
		}}}
	}
	holder, err := contractStrkey(*contractAddr(5).ContractId)
	if err != nil {
		t.Fatal(err)
	}
	for name, e := range map[string]xdr.LedgerEntry{"usd": balance(usd, 70), "xlm": balance(native, 40)} {
		before := *e.Data.ContractData.Contract.ContractId
		vals, err := contractDataValues(e, network.PublicNetworkPassphrase)
		if err != nil {
			t.Fatal(err)
		}
		if vals[7] != holder || vals[8] == nil {
			t.Errorf("%s: balance_holder %v balance %v", name, vals[7], vals[8])
		}
		if *e.Data.ContractData.Contract.ContractId != before {
			t.Errorf("%s: the entry was changed", name)
		}
	}
}

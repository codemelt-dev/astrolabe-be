package explorer

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func ptr[T any](v T) *T { return &v }

func TestScValJSON(t *testing.T) {
	acct := xdr.MustAddress(keypair.MustRandom().Address())
	contract := xdr.ContractId{1, 2, 3}
	vec := &xdr.ScVec{{Type: xdr.ScValTypeScvU32, U32: ptr(xdr.Uint32(7))}, {Type: xdr.ScValTypeScvBool, B: ptr(true)}}
	m := &xdr.ScMap{{Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: ptr(xdr.ScSymbol("k"))}, Val: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}}
	cases := []struct {
		v    xdr.ScVal
		want string
	}{
		{xdr.ScVal{Type: xdr.ScValTypeScvBool, B: ptr(false)}, `{"bool":false}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvVoid}, `{"void":null}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: ptr(xdr.Uint32(4294967295))}, `{"u32":4294967295}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvI32, I32: ptr(xdr.Int32(-5))}, `{"i32":-5}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: ptr(xdr.Uint64(18446744073709551615))}, `{"u64":"18446744073709551615"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvI64, I64: ptr(xdr.Int64(-9223372036854775808))}, `{"i64":"-9223372036854775808"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvTimepoint, Timepoint: ptr(xdr.TimePoint(1700000000))}, `{"timepoint":"1700000000"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvU128, U128: &xdr.UInt128Parts{Hi: 1, Lo: 2}}, `{"u128":"18446744073709551618"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &xdr.Int128Parts{Hi: -1, Lo: 18446744073709551615}}, `{"i128":"-1"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &xdr.Int128Parts{Hi: 0, Lo: 1500000}}, `{"i128":"1500000"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvU256, U256: &xdr.UInt256Parts{HiHi: 0, HiLo: 0, LoHi: 1, LoLo: 0}}, `{"u256":"18446744073709551616"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvI256, I256: &xdr.Int256Parts{HiHi: -1, HiLo: 18446744073709551615, LoHi: 18446744073709551615, LoLo: 18446744073709551614}}, `{"i256":"-2"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: ptr(xdr.ScBytes{0, 255})}, `{"bytes":"00ff"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptr(xdr.ScString("hi \"you\"\n"))}, `{"string":"hi \"you\"\n"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptr(xdr.ScString("a\x00b"))}, `{"string_hex":"610062"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvString, Str: ptr(xdr.ScString("\xff"))}, `{"string_hex":"ff"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: ptr(xdr.ScSymbol("transfer"))}, `{"symbol":"transfer"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vec}, `{"vec":[{"u32":7},{"bool":true}]}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: ptr((*xdr.ScVec)(nil))}, `{"vec":null}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &m}, `{"map":[{"key":{"symbol":"k"},"val":{"void":null}}]}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &acct}}, `{"address":"` + acct.Address() + `"}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &contract}}, `{"address":"CAAQEAY`},
		{xdr.ScVal{Type: xdr.ScValTypeScvError, Error: &xdr.ScError{Type: xdr.ScErrorTypeSceContract, ContractCode: ptr(xdr.Uint32(5))}}, `{"error":{"type":"contract","code":5}}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvError, Error: &xdr.ScError{Type: xdr.ScErrorTypeSceWasmVm, Code: ptr(xdr.ScErrorCodeScecInvalidAction)}}, `{"error":{"type":"wasm_vm","code":"invalid_action"}}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance}, `{"ledger_key_contract_instance":null}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyNonce, NonceKey: &xdr.ScNonceKey{Nonce: -3}}, `{"ledger_key_nonce":{"nonce":"-3"}}`},
		{xdr.ScVal{Type: xdr.ScValTypeScvContractInstance, Instance: &xdr.ScContractInstance{
			Executable: xdr.ContractExecutable{Type: xdr.ContractExecutableTypeContractExecutableStellarAsset}}},
			`{"contract_instance":{"executable":"stellar_asset","storage":null}}`},
	}
	for _, c := range cases {
		got, err := ScValJSON(c.v)
		if err != nil {
			t.Fatalf("%v: %v", c.v.Type, err)
		}
		if !json.Valid([]byte(got)) || strings.Contains(got, `\u0000`) {
			t.Fatalf("%v: invalid jsonb %s", c.v.Type, got)
		}
		if !strings.HasPrefix(got, c.want) {
			t.Errorf("%v: got %s, want %s", c.v.Type, got, c.want)
		}
	}
	if _, err := ScValJSON(xdr.ScVal{Type: 99}); err == nil {
		t.Fatal("unknown ScVal type accepted")
	}
}

func TestSnakeCase(t *testing.T) {
	for in, want := range map[string]string{
		"ContractIdPreimage": "contract_id_preimage", "AccountId": "account_id", "HomeDomain": "home_domain",
		"InvokeHostFunctionOp": "invoke_host_function_op", "V0": "v0", "Ed25519": "ed25519", "SorobanAuthorizationEntry": "soroban_authorization_entry",
		"LedgerKeyContractData": "ledger_key_contract_data",
	} {
		if got := snakeCase(in); got != want {
			t.Errorf("snakeCase(%q) = %q, want %q", in, got, want)
		}
	}
	if cleanText("a\x00b\xff") != "ab�" || cleanText("ok") != "ok" {
		t.Fatal("cleanText")
	}
}

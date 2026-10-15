package explorer

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// The SDK has no ScVal → JSON converter, so this is ours. The
// shape follows stellar-xdr's JSON (what RPC returns for xdrFormat=json):
// every value is an object keyed by its type, 64-bit and wider integers are
// decimal strings, bytes are lower-case hex and addresses are strkeys:
//
//	{"symbol":"transfer"}  {"address":"GA…"}  {"i128":"-1500000"}
//	{"vec":[{"u32":1},{"bool":true}]}  {"map":[{"key":…,"val":…}]}
//	{"bytes":"00ff"}  {"string":"hi"}  {"void":null}
//
// Strings and symbols that are not valid UTF-8 (or contain NUL) become
// {"string_hex":"…"} / {"symbol_hex":"…"}, so nothing is lost and jsonb
// accepts every value.

// ScValJSON returns v as JSON.
func ScValJSON(v xdr.ScVal) (string, error) {
	b, err := appendScVal(nil, v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func appendScVal(dst []byte, v xdr.ScVal) ([]byte, error) {
	var err error
	switch v.Type {
	case xdr.ScValTypeScvBool:
		dst = append(dst, `{"bool":`...)
		dst = strconv.AppendBool(dst, v.MustB())
	case xdr.ScValTypeScvVoid:
		dst = append(dst, `{"void":null`...)
	case xdr.ScValTypeScvError:
		dst = append(dst, `{"error":`...)
		dst, err = appendScError(dst, v.MustError())
	case xdr.ScValTypeScvU32:
		dst = append(dst, `{"u32":`...)
		dst = strconv.AppendUint(dst, uint64(v.MustU32()), 10)
	case xdr.ScValTypeScvI32:
		dst = append(dst, `{"i32":`...)
		dst = strconv.AppendInt(dst, int64(v.MustI32()), 10)
	case xdr.ScValTypeScvU64:
		dst = append(dst, `{"u64":`...)
		dst = appendQuotedUint(dst, uint64(v.MustU64()))
	case xdr.ScValTypeScvI64:
		dst = append(dst, `{"i64":`...)
		dst = appendQuotedInt(dst, int64(v.MustI64()))
	case xdr.ScValTypeScvTimepoint:
		dst = append(dst, `{"timepoint":`...)
		dst = appendQuotedUint(dst, uint64(v.MustTimepoint()))
	case xdr.ScValTypeScvDuration:
		dst = append(dst, `{"duration":`...)
		dst = appendQuotedUint(dst, uint64(v.MustDuration()))
	case xdr.ScValTypeScvU128:
		p := v.MustU128()
		dst = append(dst, `{"u128":`...)
		dst = appendBig(dst, u128(p))
	case xdr.ScValTypeScvI128:
		p := v.MustI128()
		dst = append(dst, `{"i128":`...)
		dst = appendBig(dst, i128(p))
	case xdr.ScValTypeScvU256:
		p := v.MustU256()
		dst = append(dst, `{"u256":`...)
		dst = appendBig(dst, words(false, uint64(p.HiHi), uint64(p.HiLo), uint64(p.LoHi), uint64(p.LoLo)))
	case xdr.ScValTypeScvI256:
		p := v.MustI256()
		dst = append(dst, `{"i256":`...)
		dst = appendBig(dst, words(true, uint64(p.HiHi), uint64(p.HiLo), uint64(p.LoHi), uint64(p.LoLo)))
	case xdr.ScValTypeScvBytes:
		dst = append(dst, `{"bytes":`...)
		dst = appendHexString(dst, v.MustBytes())
	case xdr.ScValTypeScvString:
		dst = appendTaggedString(dst, "string", string(v.MustStr()))
		return dst, nil
	case xdr.ScValTypeScvSymbol:
		dst = appendTaggedString(dst, "symbol", string(v.MustSym()))
		return dst, nil
	case xdr.ScValTypeScvVec:
		dst = append(dst, `{"vec":`...)
		vec := v.MustVec()
		if vec == nil {
			dst = append(dst, "null"...)
			break
		}
		dst = append(dst, '[')
		for i, e := range *vec {
			if i > 0 {
				dst = append(dst, ',')
			}
			if dst, err = appendScVal(dst, e); err != nil {
				return nil, err
			}
		}
		dst = append(dst, ']')
	case xdr.ScValTypeScvMap:
		dst = append(dst, `{"map":`...)
		m := v.MustMap()
		if m == nil {
			dst = append(dst, "null"...)
			break
		}
		if dst, err = appendScMap(dst, *m); err != nil {
			return nil, err
		}
	case xdr.ScValTypeScvAddress:
		dst = append(dst, `{"address":`...)
		dst, err = appendScAddress(dst, v.MustAddress())
	case xdr.ScValTypeScvContractInstance:
		dst = append(dst, `{"contract_instance":`...)
		dst, err = appendContractInstance(dst, v.MustInstance())
	case xdr.ScValTypeScvLedgerKeyContractInstance:
		dst = append(dst, `{"ledger_key_contract_instance":null`...)
	case xdr.ScValTypeScvLedgerKeyNonce:
		dst = append(dst, `{"ledger_key_nonce":{"nonce":`...)
		dst = appendQuotedInt(dst, int64(v.MustNonceKey().Nonce))
		dst = append(dst, '}')
	case xdr.ScValTypeScvExecutableTag:
		dst = appendTaggedString(dst, "executable_tag", string(*v.ExecutableTag))
		return dst, nil
	default:
		return nil, fmt.Errorf("unknown ScVal type %d", v.Type)
	}
	if err != nil {
		return nil, err
	}
	return append(dst, '}'), nil
}

// appendTaggedString appends {"<tag>":"…"} or {"<tag>_hex":"…"}.
func appendTaggedString(dst []byte, tag, s string) []byte {
	dst = append(dst, '{')
	if textSafe(s) {
		dst = appendKey(dst, tag, true)
		dst = appendJSONString(dst, s)
	} else {
		dst = appendKey(dst, tag+"_hex", true)
		dst = appendHexString(dst, []byte(s))
	}
	return append(dst, '}')
}

func appendScMap(dst []byte, m xdr.ScMap) ([]byte, error) {
	var err error
	dst = append(dst, '[')
	for i, e := range m {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, `{"key":`...)
		if dst, err = appendScVal(dst, e.Key); err != nil {
			return nil, err
		}
		dst = append(dst, `,"val":`...)
		if dst, err = appendScVal(dst, e.Val); err != nil {
			return nil, err
		}
		dst = append(dst, '}')
	}
	return append(dst, ']'), nil
}

func appendScAddress(dst []byte, a xdr.ScAddress) ([]byte, error) {
	s, err := a.String()
	if err != nil {
		return nil, err
	}
	return appendJSONString(dst, s), nil
}

func appendScError(dst []byte, e xdr.ScError) ([]byte, error) {
	dst = append(dst, `{"type":`...)
	dst = appendJSONString(dst, enumName(e.Type.String(), "ScErrorType", "Sce"))
	dst = append(dst, `,"code":`...)
	switch {
	case e.Type == xdr.ScErrorTypeSceContract && e.ContractCode != nil:
		dst = strconv.AppendUint(dst, uint64(*e.ContractCode), 10)
	case e.Code != nil:
		dst = appendJSONString(dst, enumName(e.Code.String(), "ScErrorCode", "Scec"))
	default:
		return nil, fmt.Errorf("malformed ScError %v", e.Type)
	}
	return append(dst, '}'), nil
}

func appendContractInstance(dst []byte, inst xdr.ScContractInstance) ([]byte, error) {
	var err error
	dst = append(dst, `{"executable":`...)
	if dst, err = appendContractExecutable(dst, inst.Executable); err != nil {
		return nil, err
	}
	dst = append(dst, `,"storage":`...)
	if inst.Storage == nil {
		dst = append(dst, "null"...)
	} else if dst, err = appendScMap(dst, *inst.Storage); err != nil {
		return nil, err
	}
	return append(dst, '}'), nil
}

func appendContractExecutable(dst []byte, e xdr.ContractExecutable) ([]byte, error) {
	switch e.Type {
	case xdr.ContractExecutableTypeContractExecutableWasm:
		dst = append(dst, `{"wasm":`...)
		h := e.MustWasmHash()
		dst = appendHexString(dst, h[:])
		return append(dst, '}'), nil
	case xdr.ContractExecutableTypeContractExecutableStellarAsset:
		return append(dst, `"stellar_asset"`...), nil
	case xdr.ContractExecutableTypeContractExecutableExternalRef:
		ref := e.MustExternalRef()
		var err error
		dst = append(dst, `{"external_ref":{"owner":`...)
		if dst, err = appendScAddress(dst, ref.ExecutableOwner); err != nil {
			return nil, err
		}
		dst = append(dst, `,"tag":`...)
		dst = appendXDRString(dst, string(ref.Tag))
		return append(dst, "}}"...), nil
	default:
		return nil, fmt.Errorf("unknown contract executable type %d", e.Type)
	}
}

// contractStrkey returns the C… strkey of a contract ID.
func contractStrkey(id xdr.ContractId) (string, error) {
	return strkey.Encode(strkey.VersionByteContract, id[:])
}

// enumName shortens a generated enum name: "ScErrorTypeSceWasmVm" with
// prefixes "ScErrorType" and "Sce" becomes "wasm_vm".
func enumName(s, typePrefix, valuePrefix string) string {
	s = strings.TrimPrefix(s, typePrefix)
	s = strings.TrimPrefix(s, valuePrefix)
	return snakeCase(s)
}

func u128(p xdr.UInt128Parts) *big.Int {
	return words(false, uint64(p.Hi), uint64(p.Lo))
}

func i128(p xdr.Int128Parts) *big.Int {
	return words(true, uint64(p.Hi), uint64(p.Lo))
}

// words assembles big-endian 64-bit words into an integer; with signed, the
// top word is two's complement.
func words(signed bool, ws ...uint64) *big.Int {
	n := new(big.Int)
	for _, w := range ws {
		n.Lsh(n, 64)
		n.Or(n, new(big.Int).SetUint64(w))
	}
	if signed && len(ws) > 0 && ws[0]>>63 == 1 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(64*len(ws))))
	}
	return n
}

func appendBig(dst []byte, n *big.Int) []byte {
	dst = append(dst, '"')
	dst = n.Append(dst, 10)
	return append(dst, '"')
}
